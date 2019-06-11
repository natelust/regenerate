mod regenerate;
mod repo_wrapper;
use regenerate::*;

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
