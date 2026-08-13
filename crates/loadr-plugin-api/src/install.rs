//! Resolving, downloading and verifying plugins from a signed plugin index.
//!
//! `loadr plugin install <name>` resolves a short plugin name against a JSON
//! index ([`PluginIndex`]), picks the artifact matching the running host's
//! target triple and ABI, downloads + sha256-verifies it, unpacks it to a
//! temporary directory and hands the directory off to
//! [`crate::PluginRegistry::install_from_dir`].
//!
//! All network I/O goes through the [`Fetcher`] seam so the resolution and
//! verification logic can be unit-tested without touching the network.
//!
//! The index format (kept in sync with the release CI that produces it):
//!
//! ```json
//! {
//!   "schema": 1,
//!   "plugins": {
//!     "mongo": {
//!       "kind": "protocol",
//!       "description": "…",
//!       "latest": "1.0.0",
//!       "versions": {
//!         "1.0.0": {
//!           "min_loadr_abi": "1.0",
//!           "artifacts": {
//!             "x86_64-unknown-linux-gnu": {
//!               "url": "https://…/mongo-x86_64-unknown-linux-gnu.tar.gz",
//!               "sha256": "…",
//!               "entry": "libloadr_plugin_mongo.so"
//!             }
//!           }
//!         }
//!       }
//!     }
//!   }
//! }
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::abi::LOADR_PLUGIN_ABI_VERSION;
use crate::error::PluginError;
use crate::manifest::PluginManifest;
use crate::registry::PluginRegistry;

/// The canonical, trusted plugin index published on `main`.
pub const DEFAULT_INDEX_URL: &str =
    "https://raw.githubusercontent.com/levantar-ai/loadr/main/plugins/index.json";

/// Environment variable overriding [`DEFAULT_INDEX_URL`].
pub const INDEX_ENV: &str = "LOADR_PLUGIN_INDEX";

/// Resolve the index URL to use: explicit `flag`, then `$LOADR_PLUGIN_INDEX`,
/// then [`DEFAULT_INDEX_URL`].
pub fn index_url(flag: Option<&str>) -> String {
    if let Some(u) = flag {
        return u.to_string();
    }
    if let Ok(u) = std::env::var(INDEX_ENV) {
        if !u.is_empty() {
            return u;
        }
    }
    DEFAULT_INDEX_URL.to_string()
}

/// The host's Rust target triple (e.g. `x86_64-unknown-linux-gnu`), used to
/// pick the matching artifact from the index. Resolved at compile time from
/// the standard `cfg` values.
pub fn host_target() -> &'static str {
    // Built from cfg!() so it reflects the binary that's running, matching the
    // triple keys produced by the release CI.
    const ARCH: &str = if cfg!(target_arch = "x86_64") {
        "x86_64"
    } else if cfg!(target_arch = "aarch64") {
        "aarch64"
    } else if cfg!(target_arch = "arm") {
        "arm"
    } else {
        "unknown"
    };
    const TRIPLE: &str = match () {
        _ if cfg!(all(
            target_arch = "x86_64",
            target_os = "linux",
            target_env = "gnu"
        )) =>
        {
            "x86_64-unknown-linux-gnu"
        }
        _ if cfg!(all(
            target_arch = "x86_64",
            target_os = "linux",
            target_env = "musl"
        )) =>
        {
            "x86_64-unknown-linux-musl"
        }
        _ if cfg!(all(
            target_arch = "aarch64",
            target_os = "linux",
            target_env = "gnu"
        )) =>
        {
            "aarch64-unknown-linux-gnu"
        }
        _ if cfg!(all(
            target_arch = "aarch64",
            target_os = "linux",
            target_env = "musl"
        )) =>
        {
            "aarch64-unknown-linux-musl"
        }
        _ if cfg!(all(target_arch = "x86_64", target_os = "macos")) => "x86_64-apple-darwin",
        _ if cfg!(all(target_arch = "aarch64", target_os = "macos")) => "aarch64-apple-darwin",
        _ if cfg!(all(target_arch = "x86_64", target_os = "windows")) => "x86_64-pc-windows-msvc",
        _ if cfg!(all(target_arch = "aarch64", target_os = "windows")) => "aarch64-pc-windows-msvc",
        _ => "unknown",
    };
    // Suppress unused warning when none of the specific arms above matched.
    let _ = ARCH;
    TRIPLE
}

/// A parsed plugin index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginIndex {
    pub schema: u32,
    #[serde(default)]
    pub plugins: BTreeMap<String, IndexEntry>,
}

/// A single plugin's entry in the index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexEntry {
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub description: String,
    /// The newest published version.
    pub latest: String,
    #[serde(default)]
    pub versions: BTreeMap<String, IndexVersion>,
}

/// One published version of a plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexVersion {
    /// Minimum host ABI this build is compatible with, as `"<major>.<minor>"`.
    pub min_loadr_abi: String,
    #[serde(default)]
    pub artifacts: BTreeMap<String, IndexArtifact>,
}

/// A per-target downloadable artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexArtifact {
    pub url: String,
    pub sha256: String,
    /// The artifact filename the manifest's `entry` must resolve to once
    /// installed (e.g. `libloadr_plugin_mongo.so`).
    pub entry: String,
}

impl PluginIndex {
    /// Parse an index from JSON, rejecting unknown schema versions.
    pub fn parse(json: &[u8]) -> Result<PluginIndex, PluginError> {
        let index: PluginIndex = serde_json::from_slice(json)
            .map_err(|e| PluginError::Other(format!("invalid plugin index: {e}")))?;
        if index.schema != 1 {
            return Err(PluginError::Other(format!(
                "unsupported plugin index schema {} (this loadr understands schema 1)",
                index.schema
            )));
        }
        Ok(index)
    }

    /// Names of all plugins matching `term` (case-insensitive substring of the
    /// name or description), sorted.
    pub fn search(&self, term: &str) -> Vec<&str> {
        let needle = term.to_lowercase();
        self.plugins
            .iter()
            .filter(|(name, e)| {
                name.to_lowercase().contains(&needle)
                    || e.description.to_lowercase().contains(&needle)
            })
            .map(|(name, _)| name.as_str())
            .collect()
    }

    /// Resolve a plugin name (+ optional version + target) to the artifact to
    /// download, applying the ABI compatibility check.
    pub fn resolve(
        &self,
        name: &str,
        version: Option<&str>,
        target: &str,
    ) -> Result<Resolved, PluginError> {
        let entry = self.plugins.get(name).ok_or_else(|| {
            PluginError::Other(format!(
                "plugin `{name}` is not in the index (try `loadr plugin search {name}`)"
            ))
        })?;
        let version = version.unwrap_or(&entry.latest).to_string();
        let ver = entry.versions.get(&version).ok_or_else(|| {
            let mut available: Vec<&str> = entry.versions.keys().map(String::as_str).collect();
            available.sort_unstable();
            PluginError::Other(format!(
                "plugin `{name}` has no version `{version}` (available: {})",
                available.join(", ")
            ))
        })?;
        if !abi_compatible(&ver.min_loadr_abi)? {
            return Err(PluginError::Other(format!(
                "plugin `{name}` v{version} needs loadr plugin ABI >= {} but this loadr provides {}.0; \
                 upgrade loadr or pick another version",
                ver.min_loadr_abi, LOADR_PLUGIN_ABI_VERSION
            )));
        }
        let artifact = ver.artifacts.get(target).ok_or_else(|| {
            let mut targets: Vec<&str> = ver.artifacts.keys().map(String::as_str).collect();
            targets.sort_unstable();
            PluginError::Other(format!(
                "plugin `{name}` v{version} has no artifact for target `{target}` \
                 (available: {})",
                targets.join(", ")
            ))
        })?;
        Ok(Resolved {
            name: name.to_string(),
            version,
            kind: entry.kind.clone(),
            target: target.to_string(),
            artifact: artifact.clone(),
        })
    }
}

/// The outcome of [`PluginIndex::resolve`]: a concrete artifact to fetch.
#[derive(Debug, Clone)]
pub struct Resolved {
    pub name: String,
    pub version: String,
    pub kind: String,
    pub target: String,
    pub artifact: IndexArtifact,
}

/// Whether the running host ABI satisfies a `min_loadr_abi` requirement.
/// The requirement is `"<major>.<minor>"`; only the major component is a hard
/// compatibility gate (the native loader does the precise layout check), so a
/// host major >= the required major is accepted.
pub fn abi_compatible(min_loadr_abi: &str) -> Result<bool, PluginError> {
    let major: u32 = min_loadr_abi
        .split('.')
        .next()
        .unwrap_or(min_loadr_abi)
        .parse()
        .map_err(|_| {
            PluginError::Other(format!(
                "malformed min_loadr_abi `{min_loadr_abi}` in index"
            ))
        })?;
    Ok(LOADR_PLUGIN_ABI_VERSION >= major)
}

/// Lowercase hex sha256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Verify `bytes` hash to `expected` (case-insensitive hex). Errors otherwise.
pub fn verify_sha256(bytes: &[u8], expected: &str) -> Result<(), PluginError> {
    let actual = sha256_hex(bytes);
    if actual.eq_ignore_ascii_case(expected) {
        Ok(())
    } else {
        Err(PluginError::Other(format!(
            "sha256 mismatch: expected {expected}, downloaded artifact hashed to {actual}"
        )))
    }
}

/// The seam over network downloads, mocked in tests.
pub trait Fetcher {
    /// Fetch the full body at `url` (following redirects). `https` is required
    /// for real implementations.
    fn fetch(&self, url: &str) -> Result<Vec<u8>, PluginError>;
}

/// Unpack a downloaded archive (`.tar.gz`/`.tgz` or `.zip`, inferred from
/// `url`) into `dest`. The archive is expected to contain a `plugin.toml` and
/// the plugin artifact, either at the root or under a single top-level
/// directory.
pub fn unpack_archive(url: &str, bytes: &[u8], dest: &Path) -> Result<(), PluginError> {
    let lower = url.to_lowercase();
    if lower.ends_with(".zip") {
        unpack_zip(bytes, dest)
    } else if lower.ends_with(".tar.gz") || lower.ends_with(".tgz") {
        unpack_tar_gz(bytes, dest)
    } else {
        Err(PluginError::Other(format!(
            "unsupported archive type for `{url}` (expected .tar.gz, .tgz or .zip)"
        )))
    }
}

fn unpack_tar_gz(bytes: &[u8], dest: &Path) -> Result<(), PluginError> {
    use flate2::read::GzDecoder;
    use tar::Archive;
    std::fs::create_dir_all(dest).map_err(|e| PluginError::io(dest, e))?;
    let mut archive = Archive::new(GzDecoder::new(bytes));
    for entry in archive
        .entries()
        .map_err(|e| PluginError::Other(format!("reading tar archive: {e}")))?
    {
        let mut entry = entry.map_err(|e| PluginError::Other(format!("reading tar entry: {e}")))?;
        let path = entry
            .path()
            .map_err(|e| PluginError::Other(format!("bad tar entry path: {e}")))?
            .into_owned();
        let Some(rel) = flatten_entry_path(&path) else {
            continue;
        };
        let out = dest.join(&rel);
        if let Some(parent) = out.parent() {
            std::fs::create_dir_all(parent).map_err(|e| PluginError::io(parent, e))?;
        }
        entry
            .unpack(&out)
            .map_err(|e| PluginError::Other(format!("extracting `{}`: {e}", rel.display())))?;
    }
    Ok(())
}

fn unpack_zip(bytes: &[u8], dest: &Path) -> Result<(), PluginError> {
    use std::io::Read as _;
    std::fs::create_dir_all(dest).map_err(|e| PluginError::io(dest, e))?;
    let reader = std::io::Cursor::new(bytes);
    let mut zip = zip::ZipArchive::new(reader)
        .map_err(|e| PluginError::Other(format!("reading zip archive: {e}")))?;
    for i in 0..zip.len() {
        let mut file = zip
            .by_index(i)
            .map_err(|e| PluginError::Other(format!("reading zip entry: {e}")))?;
        let Some(name) = file.enclosed_name() else {
            continue;
        };
        let Some(rel) = flatten_entry_path(&name) else {
            continue;
        };
        if file.is_dir() {
            continue;
        }
        let out = dest.join(&rel);
        if let Some(parent) = out.parent() {
            std::fs::create_dir_all(parent).map_err(|e| PluginError::io(parent, e))?;
        }
        let mut buf = Vec::with_capacity(file.size() as usize);
        file.read_to_end(&mut buf)
            .map_err(|e| PluginError::Other(format!("extracting `{}`: {e}", rel.display())))?;
        std::fs::write(&out, &buf).map_err(|e| PluginError::io(&out, e))?;
    }
    Ok(())
}

/// Strip a leading single top-level directory and reject path traversal,
/// yielding the path relative to the plugin root. `plugin.toml` and the
/// artifact end up at the destination root.
fn flatten_entry_path(path: &Path) -> Option<PathBuf> {
    use std::path::Component;
    let mut comps: Vec<&std::ffi::OsStr> = Vec::new();
    for c in path.components() {
        match c {
            Component::Normal(s) => comps.push(s),
            // Reject `..`, absolute roots, prefixes — never escape `dest`.
            _ => return None,
        }
    }
    if comps.is_empty() {
        return None;
    }
    // Drop a single archive top-level dir (e.g. `mongo-1.0.0/plugin.toml`).
    let skip = usize::from(comps.len() > 1);
    let rel: PathBuf = comps[skip..].iter().collect();
    if rel.as_os_str().is_empty() {
        None
    } else {
        Some(rel)
    }
}

/// Download, verify, unpack and install a resolved index artifact.
///
/// After unpacking, the manifest's `entry` is reconciled with the index
/// artifact's declared `entry` filename (renaming the unpacked artifact if
/// needed) so the installed plugin matches the host's per-platform naming.
pub fn install_resolved(
    resolved: &Resolved,
    fetcher: &dyn Fetcher,
    plugins_dir: &Path,
) -> Result<PluginManifest, PluginError> {
    let bytes = fetcher.fetch(&resolved.artifact.url)?;
    verify_sha256(&bytes, &resolved.artifact.sha256)?;
    let tmp = tempfile::Builder::new()
        .prefix("loadr-plugin-")
        .tempdir()
        .map_err(|e| PluginError::Other(format!("creating temp dir: {e}")))?;
    let staging = tmp.path().join("unpacked");
    unpack_archive(&resolved.artifact.url, &bytes, &staging)?;
    reconcile_entry(&staging, &resolved.artifact.entry)?;
    PluginRegistry::install_from_dir(&staging, plugins_dir)
}

/// Install from an already-downloaded archive (used for `<url>`/`<file>` and
/// github sources). `archive_name` drives archive-type inference (`.tar.gz`
/// vs `.zip`).
pub fn install_archive_bytes(
    archive_name: &str,
    bytes: &[u8],
    plugins_dir: &Path,
) -> Result<PluginManifest, PluginError> {
    let tmp = tempfile::Builder::new()
        .prefix("loadr-plugin-")
        .tempdir()
        .map_err(|e| PluginError::Other(format!("creating temp dir: {e}")))?;
    let staging = tmp.path().join("unpacked");
    unpack_archive(archive_name, bytes, &staging)?;
    PluginRegistry::install_from_dir(&staging, plugins_dir)
}

/// Ensure the unpacked plugin's artifact file is named to match the manifest's
/// `entry`. If the index declares a different filename than what the archive
/// shipped, the artifact is renamed; the manifest's `entry` is authoritative.
fn reconcile_entry(staging: &Path, index_entry: &str) -> Result<(), PluginError> {
    let manifest = PluginManifest::load(staging)?;
    let want = manifest
        .entry
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    // The index `entry` and the manifest `entry` should agree; if the archive
    // shipped the file under the index name but the manifest expects another
    // (or vice-versa), rename so the manifest's `entry` resolves.
    let have = staging.join(index_entry);
    let target = staging.join(&want);
    if have.is_file() && have != target && !target.exists() {
        std::fs::rename(&have, &target).map_err(|e| PluginError::io(&target, e))?;
    }
    if !target.is_file() {
        return Err(PluginError::Other(format!(
            "unpacked archive is missing the plugin artifact `{want}`"
        )));
    }
    Ok(())
}

/// Remove an installed plugin directory by name. Returns whether anything was
/// removed.
pub fn remove(plugins_dir: &Path, name: &str) -> Result<bool, PluginError> {
    let dir = plugins_dir.join(name);
    if !dir.join("plugin.toml").is_file() {
        return Ok(false);
    }
    std::fs::remove_dir_all(&dir).map_err(|e| PluginError::io(&dir, e))?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    const INDEX: &str = r#"{
      "schema": 1,
      "plugins": {
        "mongo": {
          "kind": "protocol",
          "description": "MongoDB protocol plugin",
          "latest": "1.0.0",
          "versions": {
            "1.0.0": {
              "min_loadr_abi": "1.0",
              "artifacts": {
                "x86_64-unknown-linux-gnu": { "url": "https://example.test/mongo-x86_64-unknown-linux-gnu.tar.gz", "sha256": "aa", "entry": "libloadr_plugin_mongo.so" },
                "x86_64-pc-windows-msvc": { "url": "https://example.test/mongo-x86_64-pc-windows-msvc.zip", "sha256": "bb", "entry": "loadr_plugin_mongo.dll" }
              }
            },
            "3.0.0": {
              "min_loadr_abi": "3.0",
              "artifacts": {
                "x86_64-unknown-linux-gnu": { "url": "https://example.test/mongo-3-x86_64-unknown-linux-gnu.tar.gz", "sha256": "cc", "entry": "libloadr_plugin_mongo.so" }
              }
            }
          }
        }
      }
    }"#;

    #[test]
    fn parses_and_rejects_bad_schema() {
        let idx = PluginIndex::parse(INDEX.as_bytes()).expect("parses");
        assert_eq!(idx.schema, 1);
        assert!(idx.plugins.contains_key("mongo"));

        let bad = br#"{"schema": 99, "plugins": {}}"#;
        let err = PluginIndex::parse(bad).expect_err("rejects schema 99");
        assert!(err.to_string().contains("schema"), "{err}");
    }

    #[test]
    fn resolve_latest_for_host_target() {
        let idx = PluginIndex::parse(INDEX.as_bytes()).unwrap();
        let r = idx
            .resolve("mongo", None, "x86_64-unknown-linux-gnu")
            .expect("resolves latest abi-compatible");
        assert_eq!(r.version, "1.0.0");
        assert_eq!(r.artifact.entry, "libloadr_plugin_mongo.so");
        assert_eq!(r.artifact.sha256, "aa");
    }

    #[test]
    fn resolve_missing_plugin_and_target() {
        let idx = PluginIndex::parse(INDEX.as_bytes()).unwrap();
        let e = idx
            .resolve("nope", None, "x86_64-unknown-linux-gnu")
            .expect_err("unknown plugin");
        assert!(e.to_string().contains("not in the index"), "{e}");

        let e = idx
            .resolve("mongo", Some("1.0.0"), "sparc-unknown-none")
            .expect_err("no artifact for target");
        assert!(e.to_string().contains("no artifact for target"), "{e}");
    }

    #[test]
    fn resolve_abi_mismatch_is_clear() {
        let idx = PluginIndex::parse(INDEX.as_bytes()).unwrap();
        // 3.0.0 needs ABI >= 3 but the host provides 2.
        let e = idx
            .resolve("mongo", Some("3.0.0"), "x86_64-unknown-linux-gnu")
            .expect_err("abi too new");
        assert!(e.to_string().contains("ABI"), "{e}");
    }

    #[test]
    fn search_matches_name_and_description() {
        let idx = PluginIndex::parse(INDEX.as_bytes()).unwrap();
        assert_eq!(idx.search("mon"), vec!["mongo"]);
        assert_eq!(idx.search("MONGODB"), vec!["mongo"]);
        assert!(idx.search("postgres").is_empty());
    }

    #[test]
    fn abi_compatible_major_gate() {
        assert!(abi_compatible("1.0").unwrap());
        assert!(abi_compatible("1.5").unwrap());
        assert!(abi_compatible("2.0").unwrap());
        assert!(!abi_compatible("3.0").unwrap());
        assert!(abi_compatible("malformed").is_err());
    }

    #[test]
    fn sha256_verifies_and_rejects() {
        let data = b"hello loadr";
        let hash = sha256_hex(data);
        verify_sha256(data, &hash).expect("matches");
        verify_sha256(data, &hash.to_uppercase()).expect("case-insensitive");
        let err = verify_sha256(data, "deadbeef").expect_err("mismatch");
        assert!(err.to_string().contains("sha256 mismatch"), "{err}");
    }

    #[test]
    fn flatten_strips_top_dir_and_rejects_traversal() {
        assert_eq!(
            flatten_entry_path(Path::new("mongo-1.0.0/plugin.toml")),
            Some(PathBuf::from("plugin.toml"))
        );
        assert_eq!(
            flatten_entry_path(Path::new("plugin.toml")),
            Some(PathBuf::from("plugin.toml"))
        );
        assert_eq!(flatten_entry_path(Path::new("../../etc/passwd")), None);
        assert_eq!(flatten_entry_path(Path::new("/abs/path")), None);
    }

    // A Fetcher that serves a canned tar.gz built in-memory, exercising the
    // full download -> verify -> unpack -> install_from_dir happy path without
    // any network.
    struct CannedFetcher {
        url: String,
        bytes: Vec<u8>,
    }

    impl Fetcher for CannedFetcher {
        fn fetch(&self, url: &str) -> Result<Vec<u8>, PluginError> {
            assert_eq!(url, self.url);
            Ok(self.bytes.clone())
        }
    }

    fn build_tar_gz() -> Vec<u8> {
        use flate2::write::GzEncoder;
        use flate2::Compression;

        let manifest = r#"[plugin]
name = "demo"
version = "0.1.0"
kind = "output"
type = "native"
entry = "libloadr_plugin_demo.so"
"#;
        let artifact = b"\x7fELF-not-really";

        let buf = Vec::new();
        let enc = GzEncoder::new(buf, Compression::fast());
        let mut tar = tar::Builder::new(enc);

        let add = |tar: &mut tar::Builder<GzEncoder<Vec<u8>>>, name: &str, data: &[u8]| {
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            tar.append_data(&mut header, format!("demo-0.1.0/{name}"), data)
                .unwrap();
        };
        add(&mut tar, "plugin.toml", manifest.as_bytes());
        add(&mut tar, "libloadr_plugin_demo.so", artifact);
        let enc = tar.into_inner().unwrap();
        enc.finish().unwrap()
    }

    #[test]
    fn install_resolved_happy_path() {
        let bytes = build_tar_gz();
        let url = "https://example.test/demo-x86_64-unknown-linux-gnu.tar.gz".to_string();
        let resolved = Resolved {
            name: "demo".into(),
            version: "0.1.0".into(),
            kind: "output".into(),
            target: "x86_64-unknown-linux-gnu".into(),
            artifact: IndexArtifact {
                url: url.clone(),
                sha256: sha256_hex(&bytes),
                entry: "libloadr_plugin_demo.so".into(),
            },
        };
        let fetcher = CannedFetcher { url, bytes };
        let dir = tempfile::tempdir().unwrap();
        let manifest = install_resolved(&resolved, &fetcher, dir.path()).expect("installs");
        assert_eq!(manifest.name, "demo");
        assert!(dir
            .path()
            .join("demo")
            .join("libloadr_plugin_demo.so")
            .is_file());
        assert!(dir.path().join("demo").join("plugin.toml").is_file());
    }

    #[test]
    fn install_resolved_sha_mismatch_fails() {
        let bytes = build_tar_gz();
        let url = "https://example.test/demo.tar.gz".to_string();
        let resolved = Resolved {
            name: "demo".into(),
            version: "0.1.0".into(),
            kind: "output".into(),
            target: "x86_64-unknown-linux-gnu".into(),
            artifact: IndexArtifact {
                url: url.clone(),
                sha256: "00".into(),
                entry: "libloadr_plugin_demo.so".into(),
            },
        };
        let fetcher = CannedFetcher { url, bytes };
        let dir = tempfile::tempdir().unwrap();
        let err = install_resolved(&resolved, &fetcher, dir.path()).expect_err("bad sha");
        assert!(err.to_string().contains("sha256 mismatch"), "{err}");
    }

    #[test]
    fn remove_deletes_installed_dir() {
        let dir = tempfile::tempdir().unwrap();
        let pdir = dir.path().join("demo");
        std::fs::create_dir_all(&pdir).unwrap();
        std::fs::write(
            pdir.join("plugin.toml"),
            r#"[plugin]
name = "demo"
version = "0.1.0"
kind = "output"
type = "native"
entry = "x.so"
"#,
        )
        .unwrap();
        std::fs::write(pdir.join("x.so"), b"x").unwrap();
        assert!(remove(dir.path(), "demo").unwrap());
        assert!(!pdir.exists());
        assert!(!remove(dir.path(), "demo").unwrap(), "second remove no-op");
    }
}
