use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const REVISION_ENV: &str = "LOADR_GIT_REVISION";

fn main() {
    println!("cargo:rerun-if-env-changed={REVISION_ENV}");

    let manifest_dir = PathBuf::from(
        env::var_os("CARGO_MANIFEST_DIR").expect("Cargo must set CARGO_MANIFEST_DIR"),
    );
    let vcs_info = manifest_dir.join(".cargo_vcs_info.json");
    println!("cargo:rerun-if-changed={}", vcs_info.display());

    let revision = revision_from_env()
        .or_else(|| revision_from_vcs_info(&vcs_info))
        .or_else(|| revision_from_git(&manifest_dir))
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env={REVISION_ENV}={revision}");
}

fn revision_from_env() -> Option<String> {
    let raw = match env::var(REVISION_ENV) {
        Ok(raw) => raw,
        Err(env::VarError::NotPresent) => return None,
        Err(env::VarError::NotUnicode(_)) => {
            println!("cargo:warning={REVISION_ENV} is not valid UTF-8; ignoring it");
            return None;
        }
    };

    match normalize_revision(&raw) {
        Some(revision) => Some(revision),
        None => {
            println!(
                "cargo:warning={REVISION_ENV} must contain at least 12 hexadecimal characters; ignoring it"
            );
            None
        }
    }
}

fn revision_from_vcs_info(path: &Path) -> Option<String> {
    let contents = fs::read(path).ok()?;
    let value: serde_json::Value = serde_json::from_slice(&contents).ok()?;
    value
        .pointer("/git/sha1")
        .and_then(serde_json::Value::as_str)
        .and_then(normalize_revision)
}

fn revision_from_git(manifest_dir: &Path) -> Option<String> {
    let workspace_root = manifest_dir.parent()?.parent()?;
    if !workspace_root.join(".git").exists() {
        return None;
    }

    watch_git_path(workspace_root, "HEAD");
    watch_git_path(workspace_root, "packed-refs");
    if let Some(reference) = git_output(workspace_root, &["symbolic-ref", "-q", "HEAD"]) {
        watch_git_path(workspace_root, &reference);
    }

    git_output(workspace_root, &["rev-parse", "HEAD"])
        .as_deref()
        .and_then(normalize_revision)
}

fn watch_git_path(workspace_root: &Path, name: &str) {
    let Some(path) = git_output(workspace_root, &["rev-parse", "--git-path", name]) else {
        return;
    };
    let path = PathBuf::from(path);
    let path = if path.is_absolute() {
        path
    } else {
        workspace_root.join(path)
    };
    println!("cargo:rerun-if-changed={}", path.display());
}

fn git_output(workspace_root: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(workspace_root)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn normalize_revision(value: &str) -> Option<String> {
    let value = value.trim();
    if value.len() < 12 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    Some(value[..12].to_ascii_lowercase())
}
