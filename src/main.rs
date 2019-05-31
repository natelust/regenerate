use crypto::digest::Digest;
use crypto::sha1::Sha1;
use git2::{Error, Repository};
use reqwest;
use reups::DBBuilderTrait;
use reups_lib as reups;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
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
}

struct Regenerate<'a> {
    product_urls: RepoSourceWrapper,
    graph: RefCell<Option<reups::graph::Graph<'a>>>,
    db: reups::DB,
    repo_map: RefCell<HashMap<String, Repository>>,
    branches: Vec<String>,
    options: RegenOptions,
    build_completed: HashSet<String>,
}

impl<'a> Regenerate<'a> {
    fn new(options: RegenOptions) -> Result<Regenerate<'a>, String> {
        // get the mapping from defined url
        let remote_map_url = "https://raw.githubusercontent.com/lsst/repos/master/etc/repos.yaml";
        let mut response = reqwest::get(remote_map_url).unwrap();
        let mapping = if response.status().is_success() {
            let body = response.text().unwrap();
            let mut parsed = yaml_rust::YamlLoader::load_from_str(&body).unwrap();
            // This is not using multi paged yaml, so just take the first
            parsed.remove(0)
        } else {
            return Err("There was a problem fetch or parsing the remote map".to_string());
        };
        let db = reups::DBBuilder::new().allow_empty(true).build().unwrap();
        let repo_map = HashMap::new();
        let mut br = vec!["master".to_string()];
        if let Some(in_br) = options.branches.as_ref() {
            br = [&in_br[..], &br[..]].concat();
        }
        Ok(Regenerate {
            product_urls: RepoSourceWrapper::new(mapping, &options.local_yaml),
            db: db,
            graph: RefCell::new(None),
            repo_map: RefCell::new(repo_map),
            branches: br,
            options: options,
            build_completed: HashSet::new(),
        })
    }

    fn get_or_clone_repo(&self, product: &str) -> Result<(), String> {
        let repo_src = match self.product_urls.get_url(product) {
            Some(x) => x,
            None => return Err("No url for associated product".to_string()),
        };
        let mut on_disk = PathBuf::from(&self.options.clone_root);
        on_disk.push(product);
        let repo = match if on_disk.exists() {
            Repository::open(on_disk)
        } else {
            Repository::clone(repo_src, on_disk)
        } {
            Ok(repo) => repo,
            Err(e) => panic!("Failed to clone: {}", e),
        };
        self.repo_map.borrow_mut().insert(product.to_string(), repo);
        Ok(())
    }

    fn checkout_branch(&self, repo_name: &str) -> Result<(), String> {
        let repo_rc = self.repo_map.borrow();
        let repo = repo_rc.get(repo_name).unwrap();
        let mut success = false;
        for name in self.branches.iter() {
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
        let repo_rc = self.repo_map.borrow();
        let repo = repo_rc.get(name).unwrap();

        let head = match repo.head() {
            Ok(v) => v,
            Err(e) => return Err(format!("{}", e)),
        };
        let target = head.target().unwrap();
        Ok(format!("{}", target))
    }

    fn graph_repo(&'a self, name: &str, node_type: reups::graph::NodeType) {
        if self.graph.borrow().is_none() {
            let _ = self
                .graph
                .replace(Some(reups::graph::Graph::<'a>::new(&self.db)));
        }
        let location = {
            let repo_rc = self.repo_map.borrow();
            let repo = repo_rc.get(name).unwrap();
            let mut graph_rc = self.graph.borrow_mut();
            let graph = graph_rc.as_mut().unwrap();
            graph.add_or_update_product(name.to_string(), node_type);
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
                let product_added = { self.graph.borrow().as_ref().unwrap().has_product(dep_name) };
                if !product_added {
                    let _ = self.get_or_clone_repo(dep_name);
                    let _ = self.checkout_branch(dep_name);
                    self.graph_repo(dep_name, node_type.clone())
                }
                let sha = self.get_sha_of_head(dep_name).unwrap();
                {
                    let mut graph_rc = self.graph.borrow_mut();
                    let graph = graph_rc.as_mut().unwrap();
                    let _ = graph.connect_products(&name.to_string(), dep_name, sha);
                }
            }
        }
    }

    fn make_product_id(&self, product: &str) -> Result<String, String> {
        let graph_rc = self.graph.borrow();
        let graph = graph_rc.as_ref().unwrap();
        let mut hasher = Sha1::new();
        for node in graph.dfs_post_order(product)? {
            let hashes = graph.product_versions(&graph.get_name(node));
            let hash = match hashes.len() {
                0 => {
                    let name = graph.get_name(node);
                    self.get_sha_of_head(&name).unwrap()
                }
                _ => hashes[0].clone(),
            };
            hasher.input(hash.as_bytes());
        }
        let id = hasher.result_str();
        Ok(id)
    }

    fn install_product(&'a self, product: &str) -> Result<(), String> {
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

        self.get_or_clone_repo(product)?;
        self.checkout_branch(product)?;
        self.graph_repo(product, reups::graph::NodeType::Required);
        self.install_product_impl(product)
    }

    fn install_product_impl(&self, product: &str) -> Result<(), String> {
        // short circuit if this has already been built
        if self.build_completed.contains(product) {
            return Ok(());
        }
        let product_id = self.make_product_id(product)?;
        let product_dir = if !self.db.has_identity(product, &product_id) {
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
            // loop through all dependencies
            let graph_rc = self.graph.borrow();
            let graph = graph_rc.as_ref().unwrap();
            for node in graph.dfs_post_order(product)? {
                let name = &graph.get_name(node);
                // this product will be in the dfs graph, so skip it and finish
                // this function
                if name != product {
                    self.install_product_impl(name)?;
                }
            }
            let mut product_dir = PathBuf::from(&self.options.install_root);
            product_dir.push(product);
            product_dir.push(&self.options.version);
            match std::fs::create_dir_all(&product_dir) {
                Ok(_) => (),
                Err(e) => return Err(format!("{}", e)),
            }
            // issue the build command
            //
            product_dir
        };
        Ok(())
    }
}

fn main() {
    let branch = "w.2019.20";
    //let branch = "origin/u/nlust/tickets/DM-10785";
    let _version = "test";
    let options = RegenOptions {
        branches: Some(vec![branch.to_string()]),
        local_yaml: Some(PathBuf::from("resources/local_repo_list.yaml")),
        clone_root: "resources/clones/".to_string(),
        install_root: "resources/install/".to_string(),
        version: "version".to_string(),
        build_tool: "eupspkg.sh".to_string(),
    };
    let app = match Regenerate::new(options) {
        Ok(x) => x,
        Err(msg) => {
            println!("{}", msg);
            return;
        }
    };
    let repo_name = "pipe_tasks";
    let repo = app.get_or_clone_repo(repo_name);
    println!("{:?}", repo);
    match repo {
        Ok(_) => {
            app.checkout_branch(repo_name)
                .unwrap_or_else(|e| panic!("issue chekcing out branch {}", e));
            //println!("{}", repo.head().unwrap().target().unwrap());
            app.graph_repo(repo_name, reups::graph::NodeType::Required);
            app.make_product_id(repo_name);
        }
        Err(e) => {
            println!("{}", e);
        }
    };
}
