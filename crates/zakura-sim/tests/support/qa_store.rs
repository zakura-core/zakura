//! The QA store's typed planted inputs, for the ignored tools that rewrite
//! them. The store is a local copy of the `zakura-sim-corpus` Space, and
//! `ZAKURA_QA_STORE` names it. No test reads it.

use std::{env, fs, path::PathBuf};

/// The variable that names the QA store's local copy.
pub const STORE_ENV: &str = "ZAKURA_QA_STORE";

/// The path of `fixtures/<name>` in the QA store, after creating its
/// directory.
///
/// # Panics
///
/// If `ZAKURA_QA_STORE` is unset or empty, or the directory can't be
/// created.
pub fn fixture(name: &str) -> PathBuf {
    let root = env::var_os(STORE_ENV)
        .filter(|root| !root.is_empty())
        .unwrap_or_else(|| {
            panic!(
                "set {STORE_ENV} to the QA store's local copy, which a pull from the Space fills"
            )
        });
    let dir = PathBuf::from(root).join("fixtures");
    fs::create_dir_all(&dir).unwrap_or_else(|error| panic!("{}: {error}", dir.display()));
    dir.join(name)
}
