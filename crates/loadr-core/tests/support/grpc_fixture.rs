//! Helpers for integration tests that exercise the engine with an in-memory
//! protocol handler. Fixtures use short request syntax for readability; this
//! helper expands every request into a valid reflection-backed gRPC request
//! before it goes through the public configuration loader.

pub fn load(yaml: &str) -> loadr_config::Loaded {
    let mut value: serde_yaml::Value = serde_yaml::from_str(yaml).expect("parse fixture YAML");
    expand_requests(&mut value);
    let yaml = serde_yaml::to_string(&value).expect("serialize gRPC fixture");
    loadr_config::load_str(&yaml, &loadr_config::LoadOptions::new()).expect("parse")
}

fn expand_requests(value: &mut serde_yaml::Value) {
    match value {
        serde_yaml::Value::Mapping(mapping) => {
            let url_key = serde_yaml::Value::String("url".to_string());
            let grpc_key = serde_yaml::Value::String("grpc".to_string());
            if let Some(serde_yaml::Value::String(url)) = mapping.get_mut(&url_key) {
                if let Some(rest) = url.strip_prefix("http://") {
                    *url = format!("grpc://{rest}");
                }
                mapping.entry(grpc_key).or_insert_with(|| {
                    serde_yaml::from_str(
                        "reflection: true\nservice: loadr.test.Mock\nmethod: Call\nmessage: {}\n",
                    )
                    .expect("static gRPC fixture")
                });
            }
            for child in mapping.values_mut() {
                expand_requests(child);
            }
        }
        serde_yaml::Value::Sequence(sequence) => {
            for child in sequence {
                expand_requests(child);
            }
        }
        _ => {}
    }
}
