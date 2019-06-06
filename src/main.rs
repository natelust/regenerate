use crypto::digest::Digest;
use crypto::sha1::Sha1;
use fnv::FnvHashMap;
use fs_extra::dir::{copy, remove, CopyOptions};
use git2::Repository;
use log;
use log::{debug, info, warn};
use reqwest;
use reups::DBBuilderTrait;
use reups_lib as reups;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::iter::FromIterator;
use std::path::PathBuf;
use std::str;
use tempdir::TempDir;
use yaml_rust;

struct RepoSourceWrapper {
    remote_map: yaml_rust::yaml::Yaml,
    local_map: yaml_rust::yaml::Yaml,
}

impl RepoSourceWrapper {
    fn new(remote: yaml_rust::yaml::Yaml, local: &Option<PathBuf>) -> RepoSourceWrapper {
        let local_map = match local {
            Some(file) => yaml_rust::YamlLoader::load_from_str(&fs::read_to_string(file).unwrap())
                .unwrap()
                .remove(0),
            None => yaml_rust::yaml::Yaml::Hash(yaml_rust::yaml::Hash::new()),
        };
        RepoSourceWrapper {
            remote_map: remote,
            local_map,
        }
    }

    fn get_url(&self, product: &str) -> Option<&str> {
        if self
            .local_map
            .as_hash()
            .unwrap()
            .contains_key(&yaml_rust::Yaml::String(product.to_string()))
        {
            return match &self.local_map[product] {
                yaml_rust::yaml::Yaml::String(s) => Some(&s),
                yaml_rust::yaml::Yaml::Hash(hm) => Some(
                    hm[&yaml_rust::yaml::Yaml::String("url".to_string())]
                        .as_str()
                        .unwrap(),
                ),
                yaml_rust::yaml::Yaml::BadValue => None,
                _ => panic!("There should be no other types in remote product mapping"),
            };
        }
        match &self.remote_map[product] {
            yaml_rust::yaml::Yaml::String(s) => Some(&s),
            yaml_rust::yaml::Yaml::Hash(hm) => Some(
                hm[&yaml_rust::yaml::Yaml::String("url".to_string())]
                    .as_str()
                    .unwrap(),
            ),
            yaml_rust::yaml::Yaml::BadValue => None,
            _ => panic!("There should be no other types in remote product mapping"),
        }
    }
}

struct RegenOptions {
    branches: Option<Vec<String>>,
    local_yaml: Option<PathBuf>,
    clone_root: String,
    install_root: String,
    version: String,
    build_tool: String,
    tag: Option<String>,
    remote_package_url: String,
}

struct Regenerate<'a> {
    product_urls: RepoSourceWrapper,
    graph: reups::graph::Graph,
    db: &'a mut reups::DB,
    repo_map: HashMap<String, Repository>,
    branches: Vec<String>,
    options: RegenOptions,
    build_completed: HashSet<String>,
}

impl<'a> Regenerate<'a> {
    fn new(db: &'a mut reups::DB, options: RegenOptions) -> Result<Regenerate<'a>, String> {
        // get the mapping from defined url
        debug!("Fetching remote package list");
        let mut response = reqwest::get(options.remote_package_url.as_str()).unwrap();
        let mapping = if response.status().is_success() {
            let body = response.text().unwrap();
            let mut parsed = yaml_rust::YamlLoader::load_from_str(&body).unwrap();
            // This is not using multi paged yaml, so just take the first
            parsed.remove(0)
        } else {
            return Err("There was a problem fetch or parsing the remote map".to_string());
        };
        let repo_map = HashMap::new();
        let mut br = vec!["master".to_string()];
        if let Some(in_br) = options.branches.as_ref() {
            br = [&in_br[..], &br[..]].concat();
        }
        Ok(Regenerate {
            product_urls: RepoSourceWrapper::new(mapping, &options.local_yaml),
            db: db,
            graph: reups::graph::Graph::new(),
            repo_map,
            branches: br,
            options: options,
            build_completed: HashSet::new(),
        })
    }

    fn get_or_clone_repo(&mut self, product: &str) -> Result<(), String> {
        let repo_src = match self.product_urls.get_url(product) {
            Some(x) => x,
            None => return Err("No url for associated product".to_string()),
        };
        let mut on_disk = PathBuf::from(&self.options.clone_root);
        on_disk.push(product);
        let repo = match if on_disk.exists() {
            debug!(
                "Using repo found on disk for {} at {}",
                product,
                &on_disk.to_str().unwrap()
            );
            match Repository::open(&on_disk) {
                Ok(x) => Ok(x),
                Err(_) => {
                    warn!("There was a problem opening the on disk repo for {}, removing and re-cloning", product);
                    let _ = remove(&on_disk);
                    Repository::clone(repo_src, on_disk)
                        .or_else(|e| panic!("Failed to clone: {}", e))
                }
            }
        } else {
            debug!("Cloning {} from {}", product, repo_src);
            Repository::clone(repo_src, on_disk)
        } {
            Ok(repo) => repo,
            Err(e) => panic!("Failed to clone: {}", e),
        };
        self.repo_map.insert(product.to_string(), repo);
        Ok(())
    }

    fn checkout_branch(&self, repo_name: &str) -> Result<(), String> {
        let repo = self.repo_map.get(repo_name).unwrap();
        let mut success = false;
        for name in self.branches.iter() {
            debug!(
                "Trying to checkout {} in {}",
                name,
                repo.workdir().unwrap().to_str().unwrap()
            );
            let tree = match repo.revparse_single(name) {
                Ok(x) => x,
                Err(_) => continue,
            };
            match repo.checkout_tree(&tree, None) {
                Ok(_) => (),
                Err(_) => continue,
            };
            let head = match tree.kind() {
                Some(k) => match k {
                    git2::ObjectType::Tag => format!("refs/tags/{}", name),
                    _ => format!("refs/remotes/{}", name),
                },
                None => panic!("No target for specified name"),
            };
            match repo.set_head(&head) {
                Ok(x) => x,
                Err(e) => {
                    return Err(format!(
                        "Could not set {} to branch {} error {}",
                        repo_name, name, e
                    ))
                }
            }
            success = true;
            break;
        }
        match success {
            true => Ok(()),
            false => Err(format!("Could not find branch to checkout")),
        }
    }

    fn get_sha_of_head(&self, name: &str) -> Result<String, String> {
        let repo = self.repo_map.get(name).unwrap();

        let head = match repo.head() {
            Ok(v) => v,
            Err(e) => return Err(format!("{}", e)),
        };
        let target = head.target().unwrap();
        Ok(format!("{}", target))
    }

    fn graph_repo(&mut self, name: &str, node_type: reups::graph::NodeType) {
        let location = {
            let repo = self.repo_map.get(name).unwrap();
            self.graph
                .add_or_update_product(name.to_string(), node_type);
            repo.workdir().unwrap().clone().to_path_buf()
        };
        let mut table_file = location.clone();
        table_file.push(format!("ups/{}.table", name));
        let table =
            reups::table::Table::from_file(name.to_string(), table_file, location.to_path_buf())
                .unwrap();
        use reups::graph::NodeType;
        for (dep_map, node_type) in vec![
            &table.inexact.as_ref().unwrap().required,
            //&table.inexact.as_ref().unwrap().optional,
        ]
        .iter()
        .zip(vec![
            NodeType::Required,
            //   NodeType::Optional
        ]) {
            for (dep_name, _) in dep_map.iter() {
                let product_added = self.graph.has_product(dep_name);
                if !product_added {
                    let _ = self.get_or_clone_repo(dep_name);
                    let _ = self.checkout_branch(dep_name);
                    self.graph_repo(dep_name, node_type.clone())
                }
                let sha = self.get_sha_of_head(dep_name).unwrap();
                let _ = self
                    .graph
                    .connect_products(&name.to_string(), dep_name, sha);
            }
        }
    }

    fn make_product_id(&self, product: &str) -> Result<String, String> {
        let mut hasher = Sha1::new();
        for node in self.graph.dfs_post_order(product)? {
            let hashes = self.graph.product_versions(&self.graph.get_name(node));
            let hash = match hashes.len() {
                0 => {
                    let name = self.graph.get_name(node);
                    self.get_sha_of_head(&name).unwrap()
                }
                _ => hashes[0].clone(),
            };
            hasher.input(hash.as_bytes());
        }
        let id = hasher.result_str();
        Ok(id)
    }

    fn accumulate_env(
        &self,
        product: &str,
        product_repo: &str,
        products: &Vec<String>,
    ) -> Result<FnvHashMap<String, String>, String> {
        debug!("Building env for {}", product);
        let mut env_vars = FnvHashMap::default();
        for node_name in products.iter() {
            let node_id = self.make_product_id(node_name)?;
            // get the table for the node, this presupposes all products have been
            // declared except the product being installed
            let (table, db_path) = if node_name == product {
                let repo_pathbuf = PathBuf::from(product_repo);
                let mut table_path = repo_pathbuf.clone();
                table_path.push("ups");
                table_path.push(format!("{}.table", product));
                match reups::table::Table::from_file(
                    product.to_string(),
                    table_path.clone(),
                    repo_pathbuf,
                ) {
                    Ok(x) => (
                        x,
                        PathBuf::from(format!(
                            "LOCAL:{}",
                            table_path
                                .to_str()
                                .ok_or("cant convert table path to str")?
                        )),
                    ),
                    Err(e) => return Err(format!("{}", e)),
                }
            } else {
                (
                    self.db
                        .get_table_from_identity(node_name, &node_id)
                        .ok_or(format!(
                            "Issue looking up table for {}, was it declared?",
                            node_name
                        ))?,
                    self.db
                        .get_database_path_from_version(node_name, &self.options.version),
                )
            };
            reups::setup_table(
                &self.options.version,
                &table,
                &mut env_vars,
                true,
                &reups::SYSTEM_OS.to_string(),
                db_path,
                false,
            );
        }
        Ok(env_vars)
    }

    fn build_product(
        &mut self,
        product: &str,
        product_dir: &PathBuf,
        repo_path: &String,
        env_vars: &FnvHashMap<String, String>,
    ) {
        info!("Building {}", product);
        debug!("Using environment {:#?} for building", env_vars);
        let mut upstream = product_dir.clone();
        upstream.push("upstream");
        let tmp_dir = TempDir::new(product).unwrap();
        let tmp_dir_path = PathBuf::from(tmp_dir.path());
        let product_dir = if upstream.exists() {
            debug!("Product is a upstream build, copy to tmp directory");
            let _ = copy(product_dir, &tmp_dir_path, &CopyOptions::new());
            &tmp_dir_path
        } else {
            drop(tmp_dir);
            product_dir
        };

        for verb in ["fetch", "prep", "config", "build", "install"].iter() {
            debug!("Running build tool verb {}", verb);
            let output = std::process::Command::new(&self.options.build_tool)
                .args(&[
                    format!("PRODUCT={}", product),
                    format!("VERSION={}", self.options.version),
                    format!("FLAVOR={}", reups::SYSTEM_OS),
                    format!("PREFIX={}", &product_dir.to_str().unwrap()),
                    verb.to_string(),
                ])
                .current_dir(&repo_path)
                .envs(env_vars)
                .output();
            match output {
                Ok(o) => {
                    if !o.status.success() {
                        panic!("{:#?}", o);
                    } else {
                        println!("{:#?}", o);
                        ()
                    }
                }
                Err(e) => {
                    panic!("Building failed with error {}", e);
                    /*
                    println!(
                        "The output of the command is {}",
                        str::from_utf8(&output.as_ref().unwrap().stdout).unwrap()
                    );
                    panic!(
                        "The result of the command is {:#?}",
                        &output.as_ref().unwrap().status
                    );*/
                }
            }
        }
    }

    fn install_product(&mut self, product: &str) -> Result<(), String> {
        // clone product
        // checkout branch
        // graph repo (VERIFY BRANCH IS PRESENT IN AT LEAST ONE RPO)
        // make product id
        // verify product id is not in database, if so short circuit and declare
        // loop through graph dfs and build
        // create directory to install in
        // change to repo working dir
        // issue eupspkg build comamnds
        // declare to systemdb
        // declare to remote db?

        info!("Installing product {}", product);
        self.get_or_clone_repo(product)?;
        self.checkout_branch(product)?;
        self.graph_repo(product, reups::graph::NodeType::Required);
        self.install_product_impl(product)
    }

    fn install_product_impl(&mut self, product: &str) -> Result<(), String> {
        // short circuit if this has already been built
        if self.build_completed.contains(product) {
            return Ok(());
        }
        let product_id = self.make_product_id(product)?;
        let product_dir = if self.db.has_identity(product, &product_id) {
            info!(
                "Database has product {} with id {}, using that for the build",
                product, &product_id
            );
            // Get the path to an existing product if that is to be used
            PathBuf::from(
                self.db
                    .get_table_from_identity(product, &product_id)
                    .ok_or(format!(
                        "Error retrieving up table for {} in database",
                        product
                    ))?
                    .product_dir,
            )
        } else {
            info!("Doing a source build for {}", product);

            // record all dependencies into a vector, as it is cheaper to loop through
            // that than do a dfs iteration multiple times
            let mut names = vec![];
            let mut has_python = false;
            for node in self.graph.dfs_post_order(product)? {
                let node_name = self.graph.get_name(node);
                if node_name == "scipipe_conda" {
                    has_python = true
                }
                names.push(node_name);
            }
            // for now force the python env to be a dependency of everything except
            // the environment and base conda, this ensures the environment is setup
            // this is not a good long terms solution but is useful for just testing
            if !HashSet::<&&str>::from_iter(["miniconda_lsst", "scipipe_conda"].iter())
                .contains(&product)
                && !has_python
            {
                names.insert(0, "scipipe_conda".to_string())
            }

            debug!("Product {} has dependencies {:?}", product, &names);

            // make sure all the dependencies are already installed, making sure
            // to skip the product currently being installed (ie the last element
            // in the dfs
            for name in names.iter() {
                // this product will be in the dfs graph, so skip it and finish
                // this function
                info!("Procesing dependency {}", name);
                if name != product {
                    self.install_product_impl(&name)?;
                }
            }

            // determine the product directory to install to, and make sure it is
            // created
            let mut product_dir = PathBuf::from(&self.options.install_root);
            product_dir.push(product);
            product_dir.push(&self.options.version);
            product_dir = product_dir
                .canonicalize()
                .or_else(|e| return Err(format!("{}", e)))?;

            debug!(
                "Creating directory {} for {} installation",
                product_dir.to_str().unwrap(),
                product
            );
            match std::fs::create_dir_all(&product_dir) {
                Ok(_) => (),
                Err(e) => return Err(format!("{}", e)),
            }

            // get the path to the build directory
            let repo_path = self
                .repo_map
                .get(product)
                .ok_or("no product of specified name found")?
                .workdir()
                .ok_or("The speficied product has no working directory")?
                .canonicalize()
                .or_else(|_| return Err(format!("Problem expanding abs path for {}", product)))?
                .to_str()
                .ok_or("Problem turning path into str")?
                .to_string();
            // accumulate the environment varibales
            let env_vars = self.accumulate_env(product, &repo_path, &names)?;
            // remove and trace that this might have been previously prepaired
            let mut prep_path = PathBuf::from(&repo_path);
            prep_path.push("upstream");
            prep_path.push("prepared");
            if prep_path.exists() {
                let _ = std::fs::remove_file(prep_path);
            }
            // issue the build commands
            self.build_product(product, &product_dir, &repo_path, &env_vars);
            // remove the git folder form product_dir
            let mut git_path = product_dir.clone();
            git_path.push(".git");
            if git_path.exists() {
                debug!("Removing git directory from installation");
                match remove(git_path) {
                    Ok(_) => (),
                    Err(e) => return Err(format!("{}", e)),
                };
            }
            product_dir
        };
        // get the table for the product
        let product_pathbuf = PathBuf::from(&product_dir);
        let mut table_path = product_pathbuf.clone();
        table_path.push("ups");
        table_path.push(format!("{}.table", product));
        let table = match reups::table::Table::from_file(
            product.to_string(),
            table_path.clone(),
            product_pathbuf,
        ) {
            Ok(x) => x,
            Err(e) => return Err(format!("{}", e)),
        };

        // declare the results to the database
        let tmp_tag = match self.options.tag.as_ref() {
            Some(t) => Some(t.as_str()),
            None => None,
        };

        info!("Declaring {}", product);
        let declare_product = reups::DeclareInputs {
            product,
            prod_dir: &product_dir,
            version: &self.options.version,
            tag: tmp_tag,
            ident: Some(product_id.as_str()),
            flavor: Some(reups::SYSTEM_OS),
            table: Some(table),
            relative: false,
        };
        let res = self.db.declare(vec![declare_product], None);
        debug!("The results of declare are{:#?}", res);
        // add this product to the build completed set, so that when
        // multiple packages depend on this package it will not be
        // built twice
        self.build_completed.insert(product.to_string());
        Ok(())
    }
}

fn main() {
    let level = log::LevelFilter::Debug;
    let logger = reups::Logger::new(level, std::io::stdout());
    let _ = log::set_boxed_logger(logger);
    log::set_max_level(level);
    let mut db = reups::DBBuilder::new()
        .add_eups_user(false)
        .add_path_str("resources/test.json")
        .allow_empty(true)
        .build()
        .unwrap();
    let branch = "w.2019.20";
    let options = RegenOptions {
        branches: Some(vec![branch.to_string()]),
        local_yaml: Some(PathBuf::from("resources/local_repo_list.yaml")),
        clone_root: "resources/clones/".to_string(),
        install_root: "resources/install/".to_string(),
        version: "test_version".to_string(),
        build_tool: "eupspkg.sh".to_string(),
        tag: Some("build_tag".to_string()),
        remote_package_url: "https://raw.githubusercontent.com/lsst/repos/master/etc/repos.yaml"
            .to_string(),
    };
    let mut app = match Regenerate::new(&mut db, options) {
        Ok(x) => x,
        Err(msg) => {
            println!("{}", msg);
            return;
        }
    };
    let repo_name = "afw";
    match app.install_product(repo_name) {
        Ok(_) => println!("yay"),
        Err(e) => println!("{}", e),
    }
}
