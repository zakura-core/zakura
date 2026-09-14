//! Content-addressed artifact files. Every load reverifies the complete file.

use std::{
    fs::File,
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

/// Return the resumable download path, bound to one expected digest and one peer.
pub(super) fn partial_path(cache: &Path, commitment: &Commitment, peer: &ZakuraPeerId) -> PathBuf {
    cache.join(format!(
        "{}.{}.part",
        commitment.digest_hex(),
        hex::encode(peer.digest())
    ))
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
pub(super) fn load_supported(
    cache: &Path,
    commitments: &[Commitment],
) -> Vec<Arc<VerifiedArtifact>> {
    commitments
        .iter()
        .filter_map(|commitment| match load(cache, commitment) {
            Ok(artifact) => Some(Arc::new(artifact)),
            Err(error) => {
                tracing::debug!(
                    %error,
                    digest = %commitment.digest_hex(),
                    "spentness cache entry unavailable"
                );
                None
            }
        })
        .collect()
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
}
