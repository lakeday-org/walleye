//! Embeds the digest of the source tree this binary was compiled from.
//!
//! An image build copies the workspace (`Cargo.toml`, `Cargo.lock`,
//! `rust-toolchain.toml`, `crates/`, `vendor/`) into its context, labels the
//! image with the digest of exactly that tree, and compiles with a cache of
//! earlier builds' artifacts. Cargo decides what to rebuild from file mtimes,
//! so a tree whose files are older than the cached artifacts can ship a binary
//! compiled from different sources than the label names.
//!
//! This script is part of that same freshness decision: it re-runs only when
//! cargo sees a file under the tree change, so a binary built from stale
//! artifacts carries the stale digest, and a build that compares
//! `walleye-node --version` with its label refuses it. It hashes only when
//! `WALLEYE_EMBED_SOURCE_DIGEST=1`, which the image build sets; other builds
//! embed `unset`.
//!
//! The digest is the one `fly/tenant/cell/source-digest.mjs` computes over the
//! build context in the lakeday repository: every regular file, visited in
//! byte order of name at each level, hashed as
//! `<relative path>\0<length>\0<bytes>`. Symlinks and special files are
//! refused, as they are there.
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

/// The top of the tree an image build copies, in byte order.
const TREE: [&str; 5] = [
    "Cargo.lock",
    "Cargo.toml",
    "crates",
    "rust-toolchain.toml",
    "vendor",
];

fn main() {
    println!("cargo:rerun-if-env-changed=WALLEYE_EMBED_SOURCE_DIGEST");
    let digest = if std::env::var("WALLEYE_EMBED_SOURCE_DIGEST").as_deref() == Ok("1") {
        let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("workspace root");
        for top in TREE {
            println!("cargo:rerun-if-changed={}", root.join(top).display());
        }
        tree_digest(&root)
    } else {
        "unset".to_owned()
    };
    println!("cargo:rustc-env=WALLEYE_SOURCE_SHA256={digest}");
}

fn tree_digest(root: &Path) -> String {
    let mut hash = Sha256::new();
    for top in TREE {
        let path = root.join(top);
        if path.is_dir() {
            visit(root, &path, &mut hash);
        } else if path.is_file() {
            add(root, &path, &mut hash);
        }
    }
    hex::encode(hash.finalize())
}

fn visit(root: &Path, dir: &Path, hash: &mut Sha256) {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("read {}: {error}", dir.display()))
        .map(|entry| entry.expect("directory entry"))
        .collect();
    entries.sort_by(|a, b| {
        a.file_name()
            .as_encoded_bytes()
            .cmp(b.file_name().as_encoded_bytes())
    });
    for entry in entries {
        let path = entry.path();
        let kind = entry.file_type().expect("file type");
        // Build output and version control are not source, and an image
        // build never copies them.
        if kind.is_dir() && matches!(entry.file_name().to_str(), Some("target" | ".git")) {
            continue;
        }
        if kind.is_dir() {
            visit(root, &path, hash);
        } else if kind.is_file() {
            add(root, &path, hash);
        } else {
            panic!(
                "source tree must not contain symlinks or special files: {}",
                path.display()
            );
        }
    }
}

fn add(root: &Path, path: &Path, hash: &mut Sha256) {
    let relative = path.strip_prefix(root).expect("inside the tree");
    let name = relative
        .components()
        .map(|part| part.as_os_str().to_str().expect("UTF-8 path"))
        .collect::<Vec<_>>()
        .join("/");
    let data =
        std::fs::read(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    hash.update(format!("{name}\0{}\0", data.len()).as_bytes());
    hash.update(&data);
}
