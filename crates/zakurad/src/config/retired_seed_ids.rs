//! Rewrite a config file whose `network.zakura.bootstrap_peers` name default seeds
//! by their retired v2 node IDs.

use std::{
    fs,
    io::{self, Write as _},
    path::{Path, PathBuf},
};

use toml_edit::{DocumentMut, Value};
use zakura_network::zakura::replace_retired_seed_node_id;

/// Suffix of the copy of the original file kept beside a rewritten config.
const BACKUP_SUFFIX: &str = ".pre-pq-seed-ids.bak";

/// What [`rewrite_config_file`] did.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RewriteOutcome {
    /// No entry names a default seed by a retired node ID, so the file was not written.
    Unchanged,
    /// The file now names the seeds by their current IDs.
    Rewrote {
        /// The file that was rewritten, after following symlinks.
        path: PathBuf,
        /// The `(old, new)` entries, in file order.
        replaced: Vec<(String, String)>,
        /// The copy of the original file.
        backup: PathBuf,
    },
}

/// Why [`rewrite_config_file`] left the file as it was.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RewriteError {
    #[error("could not access {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("could not parse {path:?} as TOML: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml_edit::TomlError,
    },
    #[error("the rewritten {path:?} would load differently from the original beyond the seed IDs")]
    Unverified { path: PathBuf },
}

/// Replaces default seeds' retired node IDs in the config file that the loader reads
/// for `path`, keeping everything else in the file byte for byte.
///
/// Before writing, it copies the original to `<file>.pre-pq-seed-ids.bak`, unless that
/// copy already exists. The new contents replace the file in one rename, so a crash
/// leaves either the old or the new file. A symlink is followed and kept.
pub(crate) fn rewrite_config_file(path: &Path) -> Result<RewriteOutcome, RewriteError> {
    let io_error = |path: &Path| {
        let path = path.to_path_buf();
        move |source| RewriteError::Io { path, source }
    };

    let path = super::loader::resolve_config_file(path.to_path_buf()).map_err(io_error(path))?;
    let path = fs::canonicalize(&path).map_err(io_error(&path))?;
    let original = fs::read_to_string(&path).map_err(io_error(&path))?;
    // The loader accepts a UTF-8 byte order mark, which TOML parsers reject.
    let (bom, original) = match original.strip_prefix('\u{feff}') {
        Some(rest) => ("\u{feff}", rest),
        None => ("", original.as_str()),
    };

    let Some((rewritten, replaced)) =
        replace_in_document(original).map_err(|source| RewriteError::Parse {
            path: path.clone(),
            source,
        })?
    else {
        return Ok(RewriteOutcome::Unchanged);
    };
    if !loads_as_original_with_ids_replaced(original, &rewritten) {
        return Err(RewriteError::Unverified { path });
    }

    let backup = with_suffix(&path, BACKUP_SUFFIX);
    if !backup.exists() {
        fs::copy(&path, &backup).map_err(io_error(&backup))?;
    }
    replace_file(&path, format!("{bom}{rewritten}").as_bytes()).map_err(io_error(&path))?;

    Ok(RewriteOutcome::Rewrote {
        path,
        replaced,
        backup,
    })
}

/// Returns `text` with retired seed IDs replaced in `network.zakura.bootstrap_peers`,
/// and the `(old, new)` entries, or `None` when there is nothing to replace.
fn replace_in_document(
    text: &str,
) -> Result<Option<(String, Vec<(String, String)>)>, toml_edit::TomlError> {
    let mut document = text.parse::<DocumentMut>()?;

    // Table-like access covers `[network.zakura]` tables, dotted keys and inline tables.
    let Some(peers) = document
        .get_mut("network")
        .and_then(|network| network.as_table_like_mut())
        .and_then(|network| network.get_mut("zakura"))
        .and_then(|zakura| zakura.as_table_like_mut())
        .and_then(|zakura| zakura.get_mut("bootstrap_peers"))
        .and_then(|peers| peers.as_array_mut())
    else {
        return Ok(None);
    };

    let mut replaced = Vec::new();
    for value in peers.iter_mut() {
        let Value::String(formatted) = value else {
            continue;
        };
        let entry = formatted.value().clone();
        let Some(current) = replace_retired_seed_node_id(&entry) else {
            continue;
        };
        *value = replacement_value(value, &entry, &current);
        replaced.push((entry, current));
    }

    if replaced.is_empty() {
        return Ok(None);
    }
    Ok(Some((document.to_string(), replaced)))
}

/// Returns whether `rewritten` parses, with the parser the config loader uses, to exactly
/// what `original` parses to once retired seed IDs in `network.zakura.bootstrap_peers` are
/// replaced. This guards the file against any change the edit should not make.
fn loads_as_original_with_ids_replaced(original: &str, rewritten: &str) -> bool {
    let (Ok(mut expected), Ok(rewritten)) = (
        toml::from_str::<toml::Table>(original),
        toml::from_str::<toml::Table>(rewritten),
    ) else {
        return false;
    };
    let peers = expected
        .get_mut("network")
        .and_then(toml::Value::as_table_mut)
        .and_then(|network| network.get_mut("zakura"))
        .and_then(toml::Value::as_table_mut)
        .and_then(|zakura| zakura.get_mut("bootstrap_peers"))
        .and_then(toml::Value::as_array_mut);
    for peer in peers.into_iter().flatten() {
        if let Some(current) = peer.as_str().and_then(replace_retired_seed_node_id) {
            *peer = toml::Value::String(current);
        }
    }
    expected == rewritten
}

/// Returns `original`, whose string is `entry`, holding `current` instead, with the same
/// quoting and the same whitespace and comments around it.
fn replacement_value(original: &Value, entry: &str, current: &str) -> Value {
    let entry_id = entry.split_once('@').map_or(entry, |(id, _)| id);
    let current_id = current.split_once('@').map_or(current, |(id, _)| id);

    // Swap the ID inside the literal as written, so a single-quoted string stays one.
    // An escaped literal does not contain the ID verbatim, so it is written plainly.
    let mut value = match original {
        Value::String(formatted) => formatted
            .display_repr()
            .replace(entry_id, current_id)
            .parse::<Value>()
            .ok()
            .filter(|value| value.as_str() == Some(current)),
        _ => None,
    }
    .unwrap_or_else(|| Value::from(current));

    *value.decor_mut() = original.decor().clone();
    value
}

/// Writes `contents` to a sibling of `path` with `path`'s permissions, then renames it
/// over `path`.
fn replace_file(path: &Path, contents: &[u8]) -> io::Result<()> {
    let permissions = fs::metadata(path)?.permissions();
    // A per-process name, so two processes sharing a config cannot interleave writes.
    let temp = with_suffix(path, &format!(".tmp.{}", std::process::id()));
    let write = || -> io::Result<()> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp)?;
        file.set_permissions(permissions)?;
        file.write_all(contents)?;
        file.sync_all()?;
        fs::rename(&temp, path)
    };
    write().inspect_err(|_| {
        let _ = fs::remove_file(&temp);
    })
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut path = path.as_os_str().to_owned();
    path.push(suffix);
    PathBuf::from(path)
}

#[cfg(test)]
mod tests;
