//! Replace the default seeds' retired v2 node IDs in `zakura.bootstrap_peers`, both in
//! the loaded config and in the config file it came from.

use std::{
    fs,
    io::{self, Read as _, Seek as _, SeekFrom, Write as _},
    ops::Range,
    path::{Path, PathBuf},
};

use toml::{
    de::{DeTable, DeValue},
    Spanned,
};
use zakura_network::zakura::{retired_seed_replacements, ZakuraConfig};

/// Switches each `zakura.bootstrap_peers` entry that names a default seed by its retired
/// node ID to the seed's current ID, in `zakura` and in the file at `config_path`.
///
/// Nothing here can stop startup: if the file cannot be rewritten, this warns and the node
/// still dials the current IDs. It also warns about each replacement the file did not
/// account for, such as one from an environment override, so the operator can fix its
/// source.
pub(crate) fn replace_retired_seed_ids(config_path: Option<&Path>, zakura: &mut ZakuraConfig) {
    let rewritten = match config_path.map(rewrite_config_file) {
        None | Some(Ok(RewriteOutcome::Unchanged)) => Vec::new(),
        Some(Ok(RewriteOutcome::Rewrote { path, replaced })) => {
            info!(
                ?path,
                ?replaced,
                "rewrote default seeds' retired node IDs in zakura.bootstrap_peers in the \
                 config file with their current IDs"
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
        /// The config file the loader read.
        path: PathBuf,
        /// The `(old, new)` entries, in file order.
        replaced: Vec<(String, String)>,
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
    #[error("{path:?} changed while it was being rewritten")]
    Changed { path: PathBuf },
}

/// One node ID to overwrite in the file.
struct IdEdit {
    /// The ID's bytes in the file.
    range: Range<usize>,
    /// The current ID, as long as the retired one it replaces.
    current_id: String,
}

/// Overwrites default seeds' retired node IDs in the config file that the loader reads for
/// `path` with their current IDs, which are exactly as long, and changes nothing else.
///
/// The file is written in place, so it keeps its inode, and with it its owner,
/// permissions, access control lists and other attributes. It is read and written through
/// one handle, and the write is skipped if the content changed since it was read, so a
/// config installed meanwhile is never overwritten. A read-only file is left alone. A crash
/// mid-write can leave some IDs replaced and others not; both forms load.
fn rewrite_config_file(path: &Path) -> Result<RewriteOutcome, RewriteError> {
    let path = super::loader::resolve_config_file(path.to_path_buf())
        .map_err(|source| io_error(path, source))?;
    let original = fs::read_to_string(&path).map_err(|source| io_error(&path, source))?;

    let (edits, replaced) = id_edits(&original).map_err(|source| RewriteError::Parse {
        path: path.clone(),
        source,
    })?;
    if edits.is_empty() {
        return Ok(RewriteOutcome::Unchanged);
    }
    let mut rewritten = original.clone();
    for edit in &edits {
        rewritten.replace_range(edit.range.clone(), &edit.current_id);
    }
    if !loads_as_original_with_replacements(&original, &rewritten, &replaced) {
        return Err(RewriteError::Unverified { path });
    }

    write_if_unchanged(&path, &original, &edits)?;
    Ok(RewriteOutcome::Rewrote { path, replaced })
}

/// Applies `edits` to the file at `path` if it is writable and still holds `original`.
///
/// The check and the writes go through one handle, so if the file was replaced since it
/// was read, the new file is never written.
fn write_if_unchanged(path: &Path, original: &str, edits: &[IdEdit]) -> Result<(), RewriteError> {
    let mut file = fs::File::options()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|source| io_error(path, source))?;
    let metadata = file.metadata().map_err(|source| io_error(path, source))?;
    if metadata.permissions().readonly() {
        return Err(RewriteError::ReadOnly {
            path: path.to_path_buf(),
        });
    }
    let mut current = String::new();
    file.read_to_string(&mut current)
        .map_err(|source| io_error(path, source))?;
    if current != original {
        return Err(RewriteError::Changed {
            path: path.to_path_buf(),
        });
    }
    write_edits(&mut file, edits).map_err(|source| io_error(path, source))
}

/// Returns where each retired seed ID in `network.zakura.bootstrap_peers` sits in `text`,
/// with its replacement, and the `(old, new)` entries those edits make.
///
/// An entry whose ID is not spelled verbatim, exactly once and as long as its replacement,
/// such as one using escapes, is left for the operator, and the startup warning names it.
fn id_edits(text: &str) -> Result<(Vec<IdEdit>, Vec<(String, String)>), toml::de::Error> {
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
    let peers: Vec<(Range<usize>, &str)> = peers
        .into_iter()
        .flat_map(|peers| peers.iter())
        .filter_map(|peer| match peer.get_ref() {
            DeValue::String(entry) => Some((peer.span(), entry.as_ref())),
            _ => None,
        })
        .collect();
    let entries: Vec<&str> = peers.iter().map(|(_, entry)| *entry).collect();

    let mut edits = Vec::new();
    let mut replaced = Vec::new();
    for ((span, entry), current) in peers.into_iter().zip(retired_seed_replacements(&entries)) {
        let Some(current) = current else {
            continue;
        };
        let id = |entry: &str| entry.split_once('@').map_or(entry, |(id, _)| id).to_owned();
        let (retired_id, current_id) = (id(entry), id(&current));
        let mut matches = text[span.clone()].match_indices(&retired_id);
        let (Some((offset, _)), None) = (matches.next(), matches.next()) else {
            continue;
        };
        if retired_id.len() != current_id.len() {
            continue;
        }
        let start = span.start + offset;
        edits.push(IdEdit {
            range: start..start + retired_id.len(),
            current_id,
        });
        replaced.push((entry.to_owned(), current));
    }
    Ok((edits, replaced))
}

/// Returns whether `rewritten` parses, with the parser the config loader uses, to exactly
/// what `original` parses to once its `network.zakura.bootstrap_peers` entries are changed
/// by `replaced`, in order. This guards the file against any change the edit should not
/// make.
fn loads_as_original_with_replacements(
    original: &str,
    rewritten: &str,
    replaced: &[(String, String)],
) -> bool {
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
    let mut replaced = replaced.iter().peekable();
    for peer in peers.into_iter().flatten() {
        if let Some((_, current)) = replaced.next_if(|(old, _)| peer.as_str() == Some(old)) {
            *peer = toml::Value::String(current.clone());
        }
    }
    replaced.peek().is_none() && expected == rewritten
}

/// Overwrites each edit's bytes in `file` and flushes the file to disk.
fn write_edits(file: &mut fs::File, edits: &[IdEdit]) -> io::Result<()> {
    for edit in edits {
        let offset = u64::try_from(edit.range.start).map_err(io::Error::other)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(edit.current_id.as_bytes())?;
    }
    file.sync_all()
}

fn io_error(path: &Path, source: io::Error) -> RewriteError {
    RewriteError::Io {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests;
