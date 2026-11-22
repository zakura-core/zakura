//! Replace the default seeds' retired v2 node IDs in `zakura.bootstrap_peers`, both in
//! the loaded config and in the config file it came from.

use std::{
    fs,
    io::{self, Write as _},
    ops::Range,
    path::{Path, PathBuf},
};

use toml::{
    de::{DeTable, DeValue},
    Spanned,
};
use zakura_network::zakura::{replace_retired_seed_node_id, ZakuraConfig};

/// Suffix of the copy of the original file kept beside a rewritten config.
const BACKUP_SUFFIX: &str = ".pre-pq-seed-ids.bak";

/// Switches each `zakura.bootstrap_peers` entry that names a default seed by its retired
/// node ID to the seed's current ID, in `zakura` and in the file at `config_path`.
///
/// The file keeps every other byte, and its original is kept beside it. Nothing here can
/// stop startup: if the file cannot be rewritten, this warns and the node still dials the
/// current IDs. It also warns about each replacement the file did not account for, such
/// as one from an environment override, so the operator can fix its source.
pub(crate) fn replace_retired_seed_ids(config_path: Option<&Path>, zakura: &mut ZakuraConfig) {
    let rewritten = match config_path.map(rewrite_config_file) {
        None | Some(Ok(RewriteOutcome::Unchanged)) => Vec::new(),
        Some(Ok(RewriteOutcome::Rewrote {
            path,
            replaced,
            backup,
        })) => {
            info!(
                ?path,
                ?backup,
                ?replaced,
                "rewrote zakura.bootstrap_peers in the config file with the default seeds' \
                 current node IDs; the original is in the backup"
            );
            replaced
        }
        Some(Err(error)) => {
            warn!(
                %error,
                "could not rewrite the config file's retired default seed node IDs"
            );
            Vec::new()
        }
    };

    for replacement in zakura.replace_retired_seed_node_ids() {
        if !rewritten.contains(&replacement) {
            let (retired, current) = replacement;
            warn!(
                %retired,
                %current,
                "zakura.bootstrap_peers names a default seed by the node ID it had before \
                 p2p-v2/4; dialing its current ID instead. Update the entry where it is set"
            );
        }
    }
}

/// What [`rewrite_config_file`] did.
#[derive(Debug, PartialEq, Eq)]
enum RewriteOutcome {
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
enum RewriteError {
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
        source: toml::de::Error,
    },
    #[error("{path:?} is read-only")]
    ReadOnly { path: PathBuf },
    #[error("the rewritten {path:?} would load differently from the original beyond the seed IDs")]
    Unverified { path: PathBuf },
}

/// Replaces default seeds' retired node IDs in the config file that the loader reads for
/// `path`, changing only the characters of each replaced ID.
///
/// A read-only file is left alone. Before writing, the original is copied to
/// `<file>.pre-pq-seed-ids.bak`, unless that copy already exists. The new contents
/// replace the file in one rename and keep its permissions and owner, so a crash leaves
/// either the old or the new file. A symlink is followed and kept.
fn rewrite_config_file(path: &Path) -> Result<RewriteOutcome, RewriteError> {
    let io_error = |path: &Path| {
        let path = path.to_path_buf();
        move |source| RewriteError::Io { path, source }
    };

    let path = super::loader::resolve_config_file(path.to_path_buf()).map_err(io_error(path))?;
    let path = fs::canonicalize(&path).map_err(io_error(&path))?;
    let metadata = fs::metadata(&path).map_err(io_error(&path))?;
    let original = fs::read_to_string(&path).map_err(io_error(&path))?;
    // The loader accepts a UTF-8 byte order mark, which TOML parsers reject.
    let (bom, original) = match original.strip_prefix('\u{feff}') {
        Some(rest) => ("\u{feff}", rest),
        None => ("", original.as_str()),
    };

    let (rewritten, replaced) =
        replace_in_text(original).map_err(|source| RewriteError::Parse {
            path: path.clone(),
            source,
        })?;
    if replaced.is_empty() {
        return Ok(RewriteOutcome::Unchanged);
    }
    if metadata.permissions().readonly() {
        return Err(RewriteError::ReadOnly { path });
    }
    if !loads_as_original_with_ids_replaced(original, &rewritten) {
        return Err(RewriteError::Unverified { path });
    }

    let backup = with_suffix(&path, BACKUP_SUFFIX);
    let created_backup = !backup.exists();
    if created_backup {
        fs::copy(&path, &backup).map_err(io_error(&backup))?;
    }
    if let Err(error) = replace_file(&path, format!("{bom}{rewritten}").as_bytes(), &metadata) {
        // A backup of a file that was never rewritten would only mislead.
        if created_backup {
            let _ = fs::remove_file(&backup);
        }
        return Err(io_error(&path)(error));
    }

    Ok(RewriteOutcome::Rewrote {
        path,
        replaced,
        backup,
    })
}

/// Returns `text` with retired seed IDs replaced in `network.zakura.bootstrap_peers`, and
/// the `(old, new)` entries. Only the characters of each replaced ID change.
fn replace_in_text(text: &str) -> Result<(String, Vec<(String, String)>), toml::de::Error> {
    fn table<'a, 'i>(value: Option<&'a Spanned<DeValue<'i>>>) -> Option<&'a DeTable<'i>> {
        match value?.get_ref() {
            DeValue::Table(table) => Some(table),
            _ => None,
        }
    }

    let document = DeTable::parse(text)?;
    // Tables, dotted keys and inline tables all parse to nested tables with source spans.
    let peers = table(document.get_ref().get("network"))
        .and_then(|network| table(network.get("zakura")))
        .and_then(|zakura| match zakura.get("bootstrap_peers")?.get_ref() {
            DeValue::Array(peers) => Some(peers),
            _ => None,
        });

    let mut edits: Vec<(Range<usize>, String)> = Vec::new();
    let mut replaced = Vec::new();
    for peer in peers.into_iter().flat_map(|peers| peers.iter()) {
        let DeValue::String(entry) = peer.get_ref() else {
            continue;
        };
        let Some(current) = replace_retired_seed_node_id(entry) else {
            continue;
        };
        let literal = literal_with_id_replaced(&text[peer.span()], entry, &current);
        edits.push((peer.span(), literal));
        replaced.push((entry.to_string(), current));
    }

    let mut rewritten = text.to_owned();
    // Later spans first, so earlier spans keep their offsets.
    for (span, literal) in edits.into_iter().rev() {
        rewritten.replace_range(span, &literal);
    }
    Ok((rewritten, replaced))
}

/// Returns the TOML string `literal`, whose value is `entry`, holding `current` instead.
///
/// The ID is swapped inside the literal as written, so its quoting stays. A literal that
/// does not spell the ID exactly once, such as one using escapes, is written plainly.
fn literal_with_id_replaced(literal: &str, entry: &str, current: &str) -> String {
    let id = |entry: &str| entry.split_once('@').map_or(entry, |(id, _)| id).to_owned();
    let (entry_id, current_id) = (id(entry), id(current));
    if literal.matches(&entry_id).count() == 1 {
        literal.replacen(&entry_id, &current_id, 1)
    } else {
        format!("{current:?}")
    }
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

/// Writes `contents` to a sibling of `path` with the permissions and owner in `metadata`,
/// then renames it over `path`. Fails rather than change the file's owner.
fn replace_file(path: &Path, contents: &[u8], metadata: &fs::Metadata) -> io::Result<()> {
    // A per-process name, so two processes sharing a config cannot interleave writes.
    let temp = with_suffix(path, &format!(".tmp.{}", std::process::id()));
    let write = || -> io::Result<()> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp)?;
        file.set_permissions(metadata.permissions())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            std::os::unix::fs::fchown(&file, Some(metadata.uid()), Some(metadata.gid()))?;
        }
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
