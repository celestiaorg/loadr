//! # loadr-config
//!
//! YAML test definition schema, parsing, validation, `${...}` interpolation and
//! JSON Schema generation for [loadr](https://loadr.io).
//!
//! The main entry points are [`load_file`] / [`load_str`] (parse + env overrides
//! + validation) and [`json_schema`] (editor support).

pub mod diagnostics;
pub mod duration;
pub mod merge;
pub mod plan;
pub mod template;
pub mod threshold;
pub mod validate;

use std::path::{Path, PathBuf};

use thiserror::Error;

pub use diagnostics::{Diagnostic, Severity, SpanIndex};
pub use duration::Dur;
pub use plan::*;
pub use template::{Part, Template, TemplateError};
pub use threshold::{Agg, MetricSelector, Op, ThresholdExpr};
pub use validate::{validate, ValidateOptions, BUILTIN_METRICS};

/// Errors from loading a test definition.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot read {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("YAML syntax error: {0}")]
    Syntax(String),
    /// Deserialization failed; carries a located, suggestion-enriched diagnostic.
    #[error("{0}")]
    Deserialize(Diagnostic),
    #[error("unknown environment `{requested}`; available: {}", available.join(", "))]
    UnknownEnv {
        requested: String,
        available: Vec<String>,
    },
    #[error("test definition has {} validation error(s)", .0.iter().filter(|d| d.severity == Severity::Error).count())]
    Invalid(Vec<Diagnostic>),
}

/// Options for [`load_file`] / [`load_str`].
#[derive(Debug, Clone, Default)]
pub struct LoadOptions {
    /// Environment override to apply (`env.<name>` block).
    pub env: Option<String>,
    /// Verify referenced files exist (resolved against the test file's directory).
    pub check_files: bool,
    /// Treat validation errors as fatal (default true). Warnings never fail.
    pub deny_errors: bool,
}

impl LoadOptions {
    pub fn new() -> Self {
        LoadOptions {
            env: None,
            check_files: false,
            deny_errors: true,
        }
    }
}

/// A successfully loaded test definition plus any validation warnings.
#[derive(Debug)]
pub struct Loaded {
    pub plan: TestPlan,
    pub diagnostics: Vec<Diagnostic>,
    /// Directory used to resolve relative paths (test file's parent, or CWD).
    pub base_dir: PathBuf,
}

/// Load a test definition from a file.
pub fn load_file(path: &Path, opts: &LoadOptions) -> Result<Loaded, ConfigError> {
    let source = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let base_dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    load_str_with_base(&source, &base_dir, opts)
}

/// Load a test definition from a string (paths resolve against the CWD).
pub fn load_str(source: &str, opts: &LoadOptions) -> Result<Loaded, ConfigError> {
    load_str_with_base(source, Path::new("."), opts)
}

/// Load a test definition from a string, resolving paths against `base_dir`.
pub fn load_str_with_base(
    source: &str,
    base_dir: &Path,
    opts: &LoadOptions,
) -> Result<Loaded, ConfigError> {
    // 1. Syntax pass: YAML must parse.
    let mut doc: serde_yaml::Value =
        serde_yaml::from_str(source).map_err(|e| ConfigError::Syntax(e.to_string()))?;

    // 2. Environment override.
    if let Some(env_name) = &opts.env {
        merge::apply_env(&mut doc, env_name).map_err(|available| ConfigError::UnknownEnv {
            requested: env_name.clone(),
            available,
        })?;
    }

    // 3. Typed deserialization with path tracking for precise diagnostics.
    let index = SpanIndex::build(source);
    let plan: TestPlan = {
        let de = doc;
        let mut track = serde_path_to_error::Track::new();
        let tracked = serde_path_to_error::Deserializer::new(de, &mut track);
        match TestPlan::deserialize(tracked) {
            Ok(p) => p,
            Err(e) => {
                let path = track.path().to_string();
                let path = if path == "." { String::new() } else { path };
                let mut diag =
                    Diagnostic::error(path.clone(), prettify_serde_message(&e.to_string()));
                if let Some(s) = unknown_field_suggestion(&e.to_string()) {
                    diag = diag.with_suggestion(s);
                }
                return Err(ConfigError::Deserialize(diag.locate(&index)));
            }
        }
    };

    // 4. Semantic validation.
    let vopts = ValidateOptions {
        check_files_relative_to: opts.check_files.then(|| base_dir.to_path_buf()),
    };
    let diagnostics = validate(&plan, Some(source), &vopts);
    let has_errors = diagnostics.iter().any(|d| d.severity == Severity::Error);
    if has_errors && opts.deny_errors {
        return Err(ConfigError::Invalid(diagnostics));
    }

    Ok(Loaded {
        plan,
        diagnostics,
        base_dir: base_dir.to_path_buf(),
    })
}

use serde::Deserialize;

/// Strip serde noise like trailing "at line x column y" (we locate separately).
fn prettify_serde_message(msg: &str) -> String {
    let msg = msg
        .split(" at line ")
        .next()
        .unwrap_or(msg)
        .trim()
        .to_string();
    msg
}

/// For `unknown field` errors, compute a did-you-mean suggestion from the
/// "expected one of ..." list serde provides.
fn unknown_field_suggestion(msg: &str) -> Option<String> {
    let rest = msg.strip_prefix("unknown field `").or_else(|| {
        msg.find("unknown field `")
            .map(|i| &msg[i + "unknown field `".len()..])
    })?;
    let (field, rest) = rest.split_once('`')?;
    let expected_start = rest.find("expected one of ")?;
    let list = &rest[expected_start + "expected one of ".len()..];
    let candidates: Vec<&str> = list
        .split(',')
        .map(|s| s.trim().trim_matches('`'))
        .filter(|s| !s.is_empty())
        .collect();
    diagnostics::did_you_mean(field, candidates)
}

/// Generate the JSON Schema for the loadr YAML format.
pub fn json_schema() -> serde_json::Value {
    let mut settings = schemars::generate::SchemaSettings::draft07();
    settings.meta_schema = Some("http://json-schema.org/draft-07/schema#".into());
    let generator = schemars::SchemaGenerator::new(settings);
    let schema = generator.into_root_schema_for::<TestPlan>();
    let mut value = serde_json::to_value(schema).unwrap_or_default();
    if let Some(obj) = value.as_object_mut() {
        obj.insert("title".into(), "loadr test definition".into());
        obj.insert(
            "$id".into(),
            "https://loadr.io/schemas/loadr-test.schema.json".into(),
        );
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_str_happy_path() {
        let yaml = r#"
name: t
scenarios:
  s:
    executor: constant-vus
    vus: 1
    duration: 1s
    flow:
      - request:
          url: grpc://e.com:443
          grpc: { reflection: true, service: test.Service, method: Call }
"#;
        let loaded = load_str(yaml, &LoadOptions::new()).unwrap();
        assert_eq!(loaded.plan.name.as_deref(), Some("t"));
    }

    #[test]
    fn unknown_field_gets_suggestion_and_location() {
        let yaml = "scenariosss:\n  s: {}\n";
        let err = load_str(yaml, &LoadOptions::new()).unwrap_err();
        match err {
            ConfigError::Deserialize(d) => {
                assert!(d.message.contains("unknown field"));
                assert_eq!(d.suggestion.as_deref(), Some("did you mean `scenarios`?"));
            }
            other => panic!("expected Deserialize, got {other:?}"),
        }
    }

    #[test]
    fn type_error_carries_path() {
        let yaml = r#"
scenarios:
  s:
    executor: constant-vus
    vus: "lots"
    duration: 1s
    flow: [ { request: { url: https://e.com/ } } ]
"#;
        let err = load_str(yaml, &LoadOptions::new()).unwrap_err();
        match err {
            ConfigError::Deserialize(d) => {
                assert!(d.path.contains("scenarios.s.vus"), "path: {}", d.path);
                assert!(d.line.is_some());
            }
            other => panic!("expected Deserialize, got {other:?}"),
        }
    }

    #[test]
    fn env_override_applied() {
        let yaml = r#"
defaults: { http: { base_url: https://prod.example.com } }
env:
  staging:
    defaults: { http: { base_url: https://staging.example.com } }
scenarios:
  s:
    executor: constant-vus
    vus: 1
    duration: 1s
    flow:
      - request:
          url: grpc://e.com:443
          grpc: { reflection: true, service: test.Service, method: Call }
"#;
        let mut opts = LoadOptions::new();
        opts.env = Some("staging".into());
        let loaded = load_str(yaml, &opts).unwrap();
        assert_eq!(
            loaded.plan.defaults.http.base_url.as_deref(),
            Some("https://staging.example.com")
        );

        opts.env = Some("nope".into());
        match load_str(yaml, &opts).unwrap_err() {
            ConfigError::UnknownEnv { available, .. } => {
                assert_eq!(available, vec!["staging".to_string()]);
            }
            other => panic!("expected UnknownEnv, got {other:?}"),
        }
    }

    #[test]
    fn invalid_plan_fails_when_denying_errors() {
        let yaml = "name: empty\n";
        match load_str(yaml, &LoadOptions::new()).unwrap_err() {
            ConfigError::Invalid(diags) => {
                assert!(diags.iter().any(|d| d.path == "scenarios"));
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn schema_generates() {
        let schema = json_schema();
        assert_eq!(schema["title"], "loadr test definition");
        let props = schema["properties"].as_object().expect("properties");
        for key in ["scenarios", "thresholds", "defaults", "outputs", "data"] {
            assert!(props.contains_key(key), "schema missing `{key}`");
        }
    }
}
