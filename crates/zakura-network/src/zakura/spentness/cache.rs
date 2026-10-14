//! Content-addressed artifact files. Every load reverifies the complete file.

use std::{
    fs::{self, File},
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use zakura_chain::{
    common::atomic_write,
    parameters::spentness_hints::{Commitment, VerifiedArtifact},
};

use crate::{zakura::ZakuraPeerId, BoxError};

/// Return the content-addressed path for an artifact.
pub fn artifact_path(cache: &Path, commitment: &Commitment) -> PathBuf {
    cache.join(commitment.file_name())
}

/// Partial files retained for one digest, so disk use stays below this many artifacts.
pub(super) const MAX_PARTIALS_PER_DIGEST: usize = 3;
const PARTIAL_SUFFIX: &str = ".part";

/// Return the resumable download path, bound to one expected digest and one peer.
pub(super) fn partial_path(cache: &Path, commitment: &Commitment, peer: &ZakuraPeerId) -> PathBuf {
    cache.join(format!(
        "{}.{}{PARTIAL_SUFFIX}",
        commitment.digest_hex(),
        hex::encode(peer.digest())
    ))
}

/// Return the digest named by a partial file, if `name` is one.
fn partial_digest(name: &str) -> Option<&str> {
    let (digest, peer) = name.strip_suffix(PARTIAL_SUFFIX)?.split_once('.')?;
    let is_hex = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_hexdigit());
    (digest.len() == 64 && is_hex(digest) && is_hex(peer)).then_some(digest)
}

/// List partial files in `cache` with their digests and lengths.
fn partials(cache: &Path) -> io::Result<Vec<(String, PathBuf, u64)>> {
    let mut partials = Vec::new();
    for entry in fs::read_dir(cache)? {
        let entry = entry?;
        let Some(digest) = entry
            .file_name()
            .to_str()
            .and_then(partial_digest)
            .map(str::to_owned)
        else {
            continue;
        };
        partials.push((digest, entry.path(), entry.metadata()?.len()));
    }
    Ok(partials)
}

/// Delete every partial file for `commitment`.
pub(super) fn remove_partials(cache: &Path, commitment: &Commitment) -> io::Result<()> {
    let digest = commitment.digest_hex();
    for (_, path, _) in partials(cache)?.into_iter().filter(|(d, ..)| *d == digest) {
        remove_if_present(&path)?;
    }
    Ok(())
}

/// Delete the shortest partial files for `commitment` so that `next` can start
/// without exceeding [`MAX_PARTIALS_PER_DIGEST`].
pub(super) fn make_room_for_partial(
    cache: &Path,
    commitment: &Commitment,
    next: &Path,
) -> io::Result<()> {
    let digest = commitment.digest_hex();
    let mut others: Vec<_> = partials(cache)?
        .into_iter()
        .filter(|(d, path, _)| *d == digest && path != next)
        .collect();
    let excess = others.len().saturating_sub(MAX_PARTIALS_PER_DIGEST - 1);
    others.sort_by_key(|(.., len)| *len);
    for (_, path, _) in others.into_iter().take(excess) {
        remove_if_present(&path)?;
    }
    Ok(())
}

/// Delete partial files for digests outside `missing`.
///
/// Those digests are either held verified or no longer supported.
fn remove_stale_partials(cache: &Path, missing: &[&Commitment]) -> io::Result<()> {
    let missing: Vec<_> = missing.iter().map(|pin| pin.digest_hex()).collect();
    for (digest, path, _) in partials(cache)? {
        if !missing.contains(&digest) {
            remove_if_present(&path)?;
        }
    }
    Ok(())
}

fn remove_if_present(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

/// Load and reverify an exact content-addressed cache entry.
pub fn load(cache: &Path, commitment: &Commitment) -> Result<VerifiedArtifact, BoxError> {
    let file = File::open(artifact_path(cache, commitment))?;
    Ok(VerifiedArtifact::read(file, commitment)?)
}

/// Durably publish verified owned bytes, including the parent directory entry.
pub fn publish(cache: &Path, artifact: &VerifiedArtifact) -> Result<PathBuf, BoxError> {
    let path = artifact_path(cache, artifact.commitment());
    atomic_write(path.clone(), artifact.bytes())??;
    Ok(path)
}

/// Load every cached artifact that still matches a supported commitment.
///
/// Missing or corrupt entries are skipped; the downloader can replace them.
/// Partial files for digests that are held or unsupported are deleted.
pub(super) fn load_supported(
    cache: &Path,
    commitments: &[Commitment],
) -> Vec<Arc<VerifiedArtifact>> {
    let mut missing = Vec::new();
    let artifacts = commitments
        .iter()
        .filter_map(|commitment| match load(cache, commitment) {
            Ok(artifact) => Some(Arc::new(artifact)),
            Err(error) => {
                tracing::debug!(
                    %error,
                    digest = %commitment.digest_hex(),
                    "spentness cache entry unavailable"
                );
                missing.push(commitment);
                None
            }
        })
        .collect();
    if let Err(error) = remove_stale_partials(cache, &missing) {
        tracing::debug!(%error, "could not remove stale spentness partial files");
    }
    artifacts
}

#[cfg(test)]
mod tests {
    use zakura_chain::parameters::spentness_hints::{encode, ParsedArtifact};

    use super::*;

    #[test]
    fn cache_reverification_rejects_corruption() {
        let directory = tempfile::tempdir().unwrap();
        let bytes = encode([1; 32], 1, [2; 32], [false, true]).unwrap();
        let parsed = ParsedArtifact::read(bytes.as_slice()).unwrap();
        let commitment = parsed.commitment().clone();
        let verified = parsed.verify(&commitment).unwrap();
        let path = publish(directory.path(), &verified).unwrap();
        assert!(load(directory.path(), &commitment)
            .unwrap()
            .retains(1)
            .unwrap());
        std::fs::write(path, b"corrupt").unwrap();
        assert!(load(directory.path(), &commitment).is_err());
    }

    fn commitment(terminal_height: u32) -> Commitment {
        let bytes = encode([1; 32], terminal_height, [2; 32], [false, true]).unwrap();
        ParsedArtifact::read(bytes.as_slice())
            .unwrap()
            .commitment()
            .clone()
    }

    fn write_partial(cache: &Path, commitment: &Commitment, peer: u8, len: usize) -> PathBuf {
        let peer = ZakuraPeerId::new(vec![peer; 32]).unwrap();
        let path = partial_path(cache, commitment, &peer);
        std::fs::write(&path, vec![0; len]).unwrap();
        path
    }

    #[test]
    fn a_new_source_evicts_the_shortest_partials() {
        let directory = tempfile::tempdir().unwrap();
        let cache = directory.path();
        let pin = commitment(1);
        let shortest = write_partial(cache, &pin, 1, 1);
        let longer = write_partial(cache, &pin, 2, 2);
        let longest = write_partial(cache, &pin, 3, 3);
        let next = partial_path(cache, &pin, &ZakuraPeerId::new(vec![4; 32]).unwrap());

        make_room_for_partial(cache, &pin, &next).unwrap();
        assert!(!shortest.exists());
        assert!(longer.exists() && longest.exists());

        // A source that resumes its own partial evicts nothing.
        make_room_for_partial(cache, &pin, &longest).unwrap();
        assert!(longer.exists() && longest.exists());
    }

    #[test]
    fn loading_removes_partials_for_held_and_unsupported_digests() {
        let directory = tempfile::tempdir().unwrap();
        let cache = directory.path();
        let bytes = encode([1; 32], 1, [2; 32], [false, true]).unwrap();
        let parsed = ParsedArtifact::read(bytes.as_slice()).unwrap();
        let held = parsed.commitment().clone();
        publish(cache, &parsed.verify(&held).unwrap()).unwrap();
        let missing = commitment(2);
        let unsupported = commitment(3);
        let held_partial = write_partial(cache, &held, 1, 1);
        let missing_partial = write_partial(cache, &missing, 1, 1);
        let unsupported_partial = write_partial(cache, &unsupported, 1, 1);
        let unrelated = cache.join("notes.part");
        std::fs::write(&unrelated, b"operator file").unwrap();

        let loaded = load_supported(cache, &[held.clone(), missing]);
        assert_eq!(loaded.len(), 1);
        assert!(!held_partial.exists());
        assert!(missing_partial.exists());
        assert!(!unsupported_partial.exists());
        assert!(
            unrelated.exists(),
            "only files named like partials are removed"
        );
        assert!(load(cache, &held).is_ok());
    }
}
