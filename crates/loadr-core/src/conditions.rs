//! Assertion/check evaluation against protocol responses.

use std::collections::BTreeMap;
use std::sync::Arc;

use loadr_config::{Condition, FailureAction};

use crate::extract::xpath_eval;
use crate::protocol::{GrpcProtobufFieldCheck, ProtocolResponse};

/// A compiled condition (regexes/paths parsed at plan compile time).
#[derive(Debug)]
pub struct CompiledCondition {
    pub name: String,
    pub on_failure: FailureAction,
    kind: Kind,
}

#[derive(Debug)]
enum Kind {
    Status {
        equals: Option<i64>,
        one_of: Option<Vec<i64>>,
        matches: Option<regex::Regex>,
    },
    BodyContains {
        value: String,
        negate: bool,
    },
    BodyMatches {
        pattern: regex::Regex,
        negate: bool,
    },
    Jsonpath {
        path: serde_json_path::JsonPath,
        equals: Option<serde_json::Value>,
        exists: bool,
    },
    ProtobufField {
        id: Option<u32>,
        field: Arc<str>,
        equals: Option<serde_json::Value>,
        exists: bool,
        failure_groups: Option<BTreeMap<i64, String>>,
    },
    Xpath {
        expression: String,
        equals: Option<String>,
        exists: bool,
    },
    Duration {
        max_ms: f64,
    },
    Size {
        min: Option<u64>,
        max: Option<u64>,
        equals: Option<u64>,
    },
    Header {
        header: String,
        equals: Option<String>,
        contains: Option<String>,
        exists: bool,
    },
    Js {
        expression: String,
    },
}

/// The result of evaluating one condition.
#[derive(Debug, Clone)]
pub struct ConditionResult {
    pub name: String,
    pub pass: bool,
    /// Human-readable failure detail.
    pub detail: Option<String>,
    pub on_failure: FailureAction,
    /// JS conditions are deferred to the script engine.
    pub needs_js: Option<String>,
    /// Optional bounded grouping key for a failed check sample.
    pub failure_group: Option<ConditionFailureGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConditionFailureGroup {
    pub code: Option<i64>,
    pub label: String,
}

impl CompiledCondition {
    pub fn compile(spec: &Condition) -> Result<Self, String> {
        let name = spec.display_name();
        let on_failure = spec.on_failure();
        let kind = match spec {
            Condition::Status {
                equals,
                one_of,
                matches,
                ..
            } => Kind::Status {
                equals: *equals,
                one_of: one_of.clone(),
                matches: matches
                    .as_ref()
                    .map(|m| regex::Regex::new(m))
                    .transpose()
                    .map_err(|e| format!("condition `{name}`: {e}"))?,
            },
            Condition::BodyContains { value, negate, .. } => Kind::BodyContains {
                value: value.clone(),
                negate: *negate,
            },
            Condition::BodyMatches {
                pattern, negate, ..
            } => Kind::BodyMatches {
                pattern: regex::Regex::new(pattern)
                    .map_err(|e| format!("condition `{name}`: {e}"))?,
                negate: *negate,
            },
            Condition::Jsonpath {
                expression,
                equals,
                exists,
                ..
            } => Kind::Jsonpath {
                path: serde_json_path::JsonPath::parse(expression)
                    .map_err(|e| format!("condition `{name}`: {e}"))?,
                equals: equals.clone(),
                exists: exists.unwrap_or(true),
            },
            Condition::ProtobufField {
                field,
                equals,
                exists,
                failure_groups,
                ..
            } => Kind::ProtobufField {
                id: None,
                field: Arc::from(field.as_str()),
                equals: equals.clone(),
                exists: exists.unwrap_or(true),
                failure_groups: failure_groups.clone(),
            },
            Condition::Xpath {
                expression,
                equals,
                exists,
                ..
            } => Kind::Xpath {
                expression: expression.clone(),
                equals: equals.clone(),
                exists: exists.unwrap_or(true),
            },
            Condition::Duration { max, .. } => Kind::Duration {
                max_ms: max.as_duration().as_secs_f64() * 1000.0,
            },
            Condition::Size {
                min, max, equals, ..
            } => Kind::Size {
                min: *min,
                max: *max,
                equals: *equals,
            },
            Condition::Header {
                header,
                equals,
                contains,
                exists,
                ..
            } => Kind::Header {
                header: header.clone(),
                equals: equals.clone(),
                contains: contains.clone(),
                exists: exists.unwrap_or(true),
            },
            Condition::Js { expression, .. } => Kind::Js {
                expression: expression.clone(),
            },
        };
        Ok(CompiledCondition {
            name,
            on_failure,
            kind,
        })
    }

    pub fn evaluate(&self, response: &ProtocolResponse) -> ConditionResult {
        let mut result = ConditionResult {
            name: self.name.clone(),
            pass: true,
            detail: None,
            on_failure: self.on_failure,
            needs_js: None,
            failure_group: None,
        };
        let fail = |result: &mut ConditionResult, detail: String| {
            result.pass = false;
            result.detail = Some(detail);
        };
        match &self.kind {
            Kind::Status {
                equals,
                one_of,
                matches,
            } => {
                let status = response.status;
                if let Some(e) = equals {
                    if status != *e {
                        fail(&mut result, format!("expected status {e}, got {status}"));
                    }
                }
                if let Some(set) = one_of {
                    if !set.contains(&status) {
                        fail(
                            &mut result,
                            format!("expected status in {set:?}, got {status}"),
                        );
                    }
                }
                if let Some(re) = matches {
                    if !re.is_match(&status.to_string()) {
                        fail(
                            &mut result,
                            format!("status {status} does not match /{re}/"),
                        );
                    }
                }
            }
            Kind::BodyContains { value, negate } => {
                let contains = response.body_text().contains(value.as_str());
                if contains == *negate {
                    fail(
                        &mut result,
                        if *negate {
                            format!("body unexpectedly contains {value:?}")
                        } else {
                            format!("body does not contain {value:?}")
                        },
                    );
                }
            }
            Kind::BodyMatches { pattern, negate } => {
                let matched = pattern.is_match(&response.body_text());
                if matched == *negate {
                    fail(
                        &mut result,
                        if *negate {
                            format!("body unexpectedly matches /{pattern}/")
                        } else {
                            format!("body does not match /{pattern}/")
                        },
                    );
                }
            }
            Kind::Jsonpath {
                path,
                equals,
                exists,
            } => {
                let body: serde_json::Value =
                    serde_json::from_slice(&response.body).unwrap_or(serde_json::Value::Null);
                let nodes = path.query(&body);
                let first = nodes.first();
                match (exists, first) {
                    (true, None) => fail(&mut result, "no JSONPath match".to_string()),
                    (false, Some(v)) => {
                        fail(&mut result, format!("unexpected JSONPath match: {v}"))
                    }
                    (true, Some(v)) => {
                        if let Some(expected) = equals {
                            if *v != *expected {
                                fail(&mut result, format!("expected {expected}, got {v}"));
                            }
                        }
                    }
                    (false, None) => {}
                }
            }
            Kind::ProtobufField {
                id, failure_groups, ..
            } => {
                let outcome = id.and_then(|id| {
                    response
                        .grpc_protobuf_outcomes
                        .iter()
                        .find(|outcome| outcome.id == id)
                });
                match outcome {
                    Some(outcome) => {
                        result.pass = outcome.pass;
                        result.detail.clone_from(&outcome.detail);
                        if !outcome.pass {
                            result.failure_group = failure_groups.as_ref().map(|groups| {
                                if outcome.missing {
                                    ConditionFailureGroup {
                                        code: None,
                                        label: "missing".to_string(),
                                    }
                                } else if let Some(code) = outcome.actual_code {
                                    match groups.get(&code) {
                                        Some(label) => ConditionFailureGroup {
                                            code: Some(code),
                                            label: label.clone(),
                                        },
                                        None => ConditionFailureGroup {
                                            code: None,
                                            label: "other".to_string(),
                                        },
                                    }
                                } else {
                                    ConditionFailureGroup {
                                        code: None,
                                        label: "other".to_string(),
                                    }
                                }
                            });
                        }
                    }
                    None => {
                        fail(
                            &mut result,
                            "no protobuf response message was available".to_string(),
                        );
                        if failure_groups.is_some() {
                            result.failure_group = Some(ConditionFailureGroup {
                                code: None,
                                label: "no_response".to_string(),
                            });
                        }
                    }
                }
            }
            Kind::Xpath {
                expression,
                equals,
                exists,
            } => match xpath_eval(&response.body_text(), expression) {
                Err(e) => fail(&mut result, format!("xpath error: {e}")),
                Ok(found) => match (exists, found) {
                    (true, None) => fail(&mut result, "no XPath match".to_string()),
                    (false, Some(v)) => fail(&mut result, format!("unexpected XPath match: {v}")),
                    (true, Some(v)) => {
                        if let Some(expected) = equals {
                            if v != *expected {
                                fail(&mut result, format!("expected {expected:?}, got {v:?}"));
                            }
                        }
                    }
                    (false, None) => {}
                },
            },
            Kind::Duration { max_ms } => {
                let actual = response.timings.duration_ms;
                if actual > *max_ms {
                    fail(
                        &mut result,
                        format!("duration {actual:.1}ms exceeds {max_ms:.0}ms"),
                    );
                }
            }
            Kind::Size { min, max, equals } => {
                let size = response.body.len() as u64;
                if let Some(e) = equals {
                    if size != *e {
                        fail(&mut result, format!("expected size {e}, got {size}"));
                    }
                }
                if let Some(m) = min {
                    if size < *m {
                        fail(&mut result, format!("size {size} below minimum {m}"));
                    }
                }
                if let Some(m) = max {
                    if size > *m {
                        fail(&mut result, format!("size {size} above maximum {m}"));
                    }
                }
            }
            Kind::Header {
                header,
                equals,
                contains,
                exists,
            } => {
                let value = response.header(header);
                match (exists, value) {
                    (true, None) => fail(&mut result, format!("missing header `{header}`")),
                    (false, Some(v)) => {
                        fail(&mut result, format!("unexpected header `{header}: {v}`"))
                    }
                    (true, Some(v)) => {
                        if let Some(e) = equals {
                            if v != e {
                                fail(
                                    &mut result,
                                    format!("header `{header}` is {v:?}, expected {e:?}"),
                                );
                            }
                        }
                        if let Some(c) = contains {
                            if !v.contains(c.as_str()) {
                                fail(
                                    &mut result,
                                    format!(
                                        "header `{header}` is {v:?}, expected to contain {c:?}"
                                    ),
                                );
                            }
                        }
                    }
                    (false, None) => {}
                }
            }
            Kind::Js { expression } => {
                // Evaluated by the caller via the script engine.
                result.needs_js = Some(expression.clone());
            }
        }
        result
    }

    /// Whether evaluating this condition needs the response body
    /// materialized (used to gate gRPC's lazy decode). Negated match, not a
    /// positive list: an unhandled future `Kind` then defaults to "reads
    /// body", the safe choice, instead of silently skipping needed data.
    pub fn reads_body(&self) -> bool {
        !matches!(
            self.kind,
            Kind::Status { .. }
                | Kind::ProtobufField { .. }
                | Kind::Duration { .. }
                | Kind::Header { .. }
        )
    }

    /// Bind a protobuf condition to its request-local result slot and return
    /// the handler specification. Non-protobuf conditions return `None`.
    pub fn bind_protobuf_check(&mut self, id: u32) -> Option<GrpcProtobufFieldCheck> {
        match &mut self.kind {
            Kind::ProtobufField {
                id: slot,
                field,
                equals,
                exists,
                failure_groups,
            } => {
                *slot = Some(id);
                Some(GrpcProtobufFieldCheck {
                    id,
                    field: field.clone(),
                    equals: equals.clone(),
                    exists: *exists,
                    group_failures: failure_groups.is_some(),
                })
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn compile(yaml: &str) -> CompiledCondition {
        let spec: Condition = serde_yaml::from_str(yaml).expect("spec");
        CompiledCondition::compile(&spec).expect("compile")
    }

    fn response(status: i64, body: &str) -> ProtocolResponse {
        ProtocolResponse {
            status,
            body: Bytes::from(body.to_string()),
            protocol_version: "HTTP/1.1".into(),
            ..Default::default()
        }
    }

    #[test]
    fn status_conditions() {
        let c = compile("{ type: status, equals: 200 }");
        assert!(c.evaluate(&response(200, "")).pass);
        assert!(!c.evaluate(&response(404, "")).pass);

        let c = compile("{ type: status, one_of: [200, 201, 204] }");
        assert!(c.evaluate(&response(204, "")).pass);
        assert!(!c.evaluate(&response(500, "")).pass);

        let c = compile(r#"{ type: status, matches: "2.." }"#);
        assert!(c.evaluate(&response(299, "")).pass);
        assert!(!c.evaluate(&response(301, "")).pass);
    }

    #[test]
    fn body_conditions() {
        let c = compile("{ type: body_contains, value: Welcome }");
        assert!(c.evaluate(&response(200, "Welcome home")).pass);
        let r = c.evaluate(&response(200, "Goodbye"));
        assert!(!r.pass);
        assert!(r.detail.unwrap().contains("does not contain"));

        let c = compile("{ type: body_contains, value: error, negate: true }");
        assert!(c.evaluate(&response(200, "all good")).pass);
        assert!(!c.evaluate(&response(200, "error: boom")).pass);

        let c = compile(r#"{ type: body_matches, pattern: "id=\\d+" }"#);
        assert!(c.evaluate(&response(200, "id=42")).pass);
        assert!(!c.evaluate(&response(200, "id=none")).pass);
    }

    #[test]
    fn jsonpath_conditions() {
        let c = compile(r#"{ type: jsonpath, expression: "$.ok", equals: true }"#);
        assert!(c.evaluate(&response(200, r#"{"ok":true}"#)).pass);
        assert!(!c.evaluate(&response(200, r#"{"ok":false}"#)).pass);

        let c = compile(r#"{ type: jsonpath, expression: "$.missing", exists: false }"#);
        assert!(c.evaluate(&response(200, r#"{"ok":true}"#)).pass);
        assert!(!c.evaluate(&response(200, r#"{"missing":1}"#)).pass);
    }

    #[test]
    fn protobuf_field_outcomes_use_bounded_failure_groups() {
        let spec: Condition = serde_yaml::from_str(
            r#"{ type: protobuf_field, name: admission, field: code, equals: 0, failure_groups: { 18: WrongShard } }"#,
        )
        .expect("spec");
        let mut condition = CompiledCondition::compile(&spec).expect("compile");
        condition.bind_protobuf_check(7).expect("protobuf check");

        let mut response = response(0, "");
        response
            .grpc_protobuf_outcomes
            .push(crate::protocol::GrpcProtobufFieldOutcome {
                id: 7,
                pass: false,
                detail: Some("expected 0, got 18".to_string()),
                actual_code: Some(18),
                missing: false,
            });
        let result = condition.evaluate(&response);
        assert!(!result.pass);
        assert_eq!(
            result.failure_group,
            Some(ConditionFailureGroup {
                code: Some(18),
                label: "WrongShard".to_string(),
            })
        );

        response.grpc_protobuf_outcomes[0].actual_code = Some(999);
        let result = condition.evaluate(&response);
        assert_eq!(result.failure_group.unwrap().label, "other");

        response.grpc_protobuf_outcomes.clear();
        let result = condition.evaluate(&response);
        assert_eq!(result.failure_group.unwrap().label, "no_response");
    }

    #[test]
    fn xpath_conditions() {
        let c = compile(r#"{ type: xpath, expression: "//name", equals: alpha }"#);
        assert!(c.evaluate(&response(200, "<r><name>alpha</name></r>")).pass);
        assert!(!c.evaluate(&response(200, "<r><name>beta</name></r>")).pass);
    }

    #[test]
    fn duration_and_size() {
        let c = compile("{ type: duration, max: 100ms }");
        let mut r = response(200, "x");
        r.timings.duration_ms = 50.0;
        assert!(c.evaluate(&r).pass);
        r.timings.duration_ms = 150.0;
        assert!(!c.evaluate(&r).pass);

        let c = compile("{ type: size, min: 2, max: 5 }");
        assert!(c.evaluate(&response(200, "abc")).pass);
        assert!(!c.evaluate(&response(200, "a")).pass);
        assert!(!c.evaluate(&response(200, "abcdef")).pass);
    }

    #[test]
    fn header_conditions() {
        let c = compile("{ type: header, header: content-type, contains: json }");
        let mut r = response(200, "{}");
        r.headers
            .push(("Content-Type".into(), "application/json".into()));
        assert!(c.evaluate(&r).pass);
        let r2 = response(200, "{}");
        assert!(!c.evaluate(&r2).pass);
    }

    #[test]
    fn js_condition_defers() {
        let c = compile(r#"{ type: js, expression: "response.status === 200" }"#);
        let r = c.evaluate(&response(200, ""));
        assert_eq!(r.needs_js.as_deref(), Some("response.status === 200"));
        assert!(r.pass, "pass until JS says otherwise");
    }

    #[test]
    fn failure_action_propagates() {
        let c = compile("{ type: status, equals: 200, on_failure: abort_iteration }");
        let r = c.evaluate(&response(500, ""));
        assert_eq!(r.on_failure, FailureAction::AbortIteration);
    }

    #[test]
    fn reads_body_classifies_by_kind() {
        // Status/duration/header never touch the body.
        assert!(!compile("{ type: status, equals: 200 }").reads_body());
        assert!(!compile("{ type: duration, max: 100ms }").reads_body());
        assert!(!compile("{ type: header, header: content-type, exists: true }").reads_body());
        assert!(!compile("{ type: protobuf_field, field: code, equals: 0 }").reads_body());

        // Everything else does (including `js`, since we can't know in
        // advance whether the expression touches `response.body`).
        assert!(compile("{ type: body_contains, value: x }").reads_body());
        assert!(compile(r#"{ type: body_matches, pattern: "x" }"#).reads_body());
        assert!(compile(r#"{ type: jsonpath, expression: "$.x" }"#).reads_body());
        assert!(compile(r#"{ type: xpath, expression: "//x" }"#).reads_body());
        assert!(compile("{ type: size, max: 10 }").reads_body());
        assert!(compile(r#"{ type: js, expression: "true" }"#).reads_body());
    }
}
