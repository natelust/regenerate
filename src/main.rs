use git2::Repository;
use std::path::PathBuf;
use std::collections::HashMap;
use reqwest;
use yaml_rust;

struct RepoSourceWrapper {
    remote_map: yaml_rust::yaml::Yaml
}

impl RepoSourceWrapper {
    fn new(remote: yaml_rust::yaml::Yaml) -> RepoSourceWrapper {
        RepoSourceWrapper { remote_map: remote }
    }

    fn get_url(&self, product: &str) -> Option<&str> {
         match &self.remote_map[product] {
            yaml_rust::yaml::Yaml::String(s) => Some(&s),
            yaml_rust::yaml::Yaml::Hash(hm) => {
                Some(hm[&yaml_rust::yaml::Yaml::String("url".to_string())].as_str().unwrap())
            },
            yaml_rust::yaml::Yaml::BadValue => None,
            _ => panic!("There should be no other types in remote product mapping")
        }

    }
}

struct Regenerate{
    product_urls: RepoSourceWrapper,
    checkout_root: String
}

impl Regenerate {
    fn new() -> Result<Regenerate, String> {
        let mut hash = HashMap::new();
        hash.insert("pipe_tasks".to_string(), "https://github.com/lsst/pipe_tasks.git".to_string());
        // get the mapping from defined url
        let remote_map_url = "https://raw.githubusercontent.com/lsst/repos/master/etc/repos.yaml";
        let mut response = reqwest::get(remote_map_url).unwrap();
        let mapping = if response.status().is_success() {
            let body = response.text().unwrap();
            let mut parsed = yaml_rust::YamlLoader::load_from_str(&body).unwrap();
            // This is not using multi paged yaml, so just take the first
            parsed.remove(0)
        } else {
            return Err("There was a problem fetch or parsing the remote map".to_string())
        };
        Ok(Regenerate {
            product_urls: RepoSourceWrapper::new(mapping),
            checkout_root: "resources".to_string()
        })
    }

    fn get_or_clone_repo(&self, product: &str) -> Result<Repository, git2::Error> {
        let repo_src = match self.product_urls.get_url(product) {
            Some(x) => x,
            None => return Err(git2::Error::from_str("No url for associated product"))
        };
        let mut on_disk = PathBuf::from(&self.checkout_root);
        on_disk.push(product);
        let repo = match if on_disk.exists() {
            Repository::open(on_disk)
        } else {
            Repository::clone(repo_src, on_disk)
        } {
            Ok(repo) => repo,
            Err(e) => panic!("Failed to clone: {}", e),
        };
        Ok(repo)
    }

    fn checkout_branch(&self, repo: &Repository, name: &str) -> Result<(), git2::Error> {
        let tree = repo.revparse_single(name)?;
        repo.checkout_tree(&tree, None)?;
        repo.set_head(&format!("refs/remotes/{}", name))?;
        Ok(())
    }
}

fn main() {
    let app = match Regenerate::new() {
        Ok(x) => x,
        Err(msg) => {println!("{}", msg); return}
    };
    let branch = "origin/u/nlust/tickets/DM-10785";
    let repo = app.get_or_clone_repo("pipe_tasks");
    match repo {
        Ok(repo) => {app.checkout_branch(&repo, branch)
            .unwrap_or_else(|e| {panic!("issue chekcing out branch {}", e)});
        },
        Err(e) => {println!("{}", e);}
    };

}
