use std::fs;

pub struct RepoSourceWrapper {
    remote_map: yaml_rust::yaml::Yaml,
    local_map: yaml_rust::yaml::Yaml,
}

impl RepoSourceWrapper {
    pub fn new(remote: yaml_rust::yaml::Yaml, local: &Option<crate::PathBuf>) -> RepoSourceWrapper {
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

    pub fn get_url(&self, product: &str) -> Option<&str> {
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

    pub fn has_ref(&self, product: &str) -> Option<String> {
        let matcher = |map: &yaml_rust::Yaml| match &map[product] {
            yaml_rust::yaml::Yaml::Hash(hm) => {
                match hm.get(&yaml_rust::yaml::Yaml::String("ref".to_string())) {
                    Some(v) => Some(v.as_str().unwrap().to_string()),
                    None => None,
                }
            }
            _ => None,
        };
        for map in [&self.local_map, &self.remote_map].iter() {
            if map
                .as_hash()
                .unwrap()
                .contains_key(&yaml_rust::Yaml::String(product.to_string()))
            {
                return matcher(map);
            }
        }
        None
    }
}
