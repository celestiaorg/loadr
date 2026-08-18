//! Build metadata shared by the loadr binary and its embedded interfaces.

/// The Cargo package version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The 12-character Git commit embedded at build time, or `unknown` when the
/// source revision could not be determined.
pub const GIT_REVISION: &str = env!("LOADR_GIT_REVISION");

/// The user-facing version and revision, for example `1.0.0 (31412fc46bfb)`.
pub const VERSION_WITH_REVISION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("LOADR_GIT_REVISION"),
    ")"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revision_has_expected_shape() {
        assert!(
            GIT_REVISION == "unknown"
                || (GIT_REVISION.len() == 12
                    && GIT_REVISION.bytes().all(|byte| byte.is_ascii_hexdigit())
                    && GIT_REVISION == GIT_REVISION.to_ascii_lowercase()),
            "unexpected revision: {GIT_REVISION}"
        );
    }

    #[test]
    fn display_version_contains_version_and_revision() {
        assert_eq!(VERSION_WITH_REVISION, format!("{VERSION} ({GIT_REVISION})"));
    }
}
