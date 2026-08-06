//! Every shipped example must load and validate without errors.

use loadr_config::{LoadOptions, Severity};

#[test]
fn all_examples_validate() {
    let examples = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples")
        .canonicalize()
        .expect("examples directory");
    let mut checked = 0;
    for entry in std::fs::read_dir(&examples).expect("read examples") {
        let path = entry.expect("entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
            continue;
        }
        let mut opts = LoadOptions::new();
        opts.check_files = true; // referenced files must ship with the repo
        let loaded = loadr_config::load_file(&path, &opts)
            .unwrap_or_else(|e| panic!("{} failed to load: {e}", path.display()));
        let errors: Vec<_> = loaded
            .diagnostics
            .iter()
            .filter(|d| d.severity == Severity::Error)
            .collect();
        assert!(
            errors.is_empty(),
            "{} has validation errors: {errors:?}",
            path.display()
        );
        checked += 1;
    }
    assert_eq!(checked, 3, "expected the three focused gRPC examples");
}
