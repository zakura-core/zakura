//! Download admission uses actual proof of work and the candidate's own parent branch.
//!
//! No sampled target or best-tip-relative floor can exclude a valid difficulty decline.
//! Unknown ancestry stays subject to the downloader's existing capacity and timeout bounds;
//! it cannot authorize height-based changes to shared sync throttling.

use std::{sync::Arc, time::Duration};

use tower::{util::BoxCloneService, Service, ServiceExt};
use zakura_chain::{block, parameters::Network};
use zakura_state as zs;

use crate::BoxError;

/// Bounds state readiness and the parent-context read together.
const ADMISSION_TIMEOUT: Duration = Duration::from_secs(10);

/// Checks downloaded headers without waiting for their parents to commit.
#[derive(Clone)]
pub(super) struct WorkAdmission {
    state: BoxCloneService<zs::Request, zs::Response, BoxError>,
}

impl std::fmt::Debug for WorkAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WorkAdmission")
    }
}

impl WorkAdmission {
    pub(super) fn new<S>(state: S) -> Self
    where
        S: Service<zs::Request, Response = zs::Response, Error = BoxError> + Send + Clone + 'static,
        S::Future: Send + 'static,
    {
        Self {
            state: BoxCloneService::new(state),
        }
    }

    /// Returns an authenticated height only when the committed parent context is
    /// complete and the header's difficulty and time agree with that branch.
    /// `None` permits bounded recovery and full verification, never shared throttling.
    /// Unavailable state falls back to full verification without authenticating height.
    /// Invalid work rejects this attempt without caching the block as consensus-invalid.
    pub(super) async fn check(
        self,
        network: Network,
        block: Arc<block::Block>,
    ) -> Result<Option<block::Height>, BoxError> {
        let header = block.header.clone();
        let hash = block.hash();
        let height = block
            .coinbase_height()
            .ok_or("downloaded block has no height")?;
        let proof_header = header.clone();
        let valid = tokio::task::spawn_blocking(move || {
            zakura_consensus::proof_of_work_is_valid(&proof_header, &network, &height, &hash)
        })
        .await?;
        if !valid {
            return Err("downloaded header has invalid proof of work".into());
        }
        let response = tokio::time::timeout(
            ADMISSION_TIMEOUT,
            self.state
                .oneshot(zs::Request::CheckHeaderAdmission(header)),
        )
        .await;
        match response {
            Ok(Ok(zs::Response::HeaderAdmission(height))) => Ok(height),
            Ok(Err(error)) if error.is::<zakura_header_chain::ContextualValidationError>() => {
                Err(error)
            }
            unavailable => {
                // An unavailable optional read is not evidence against the block. Full
                // verification remains mandatory, and no authenticated height is returned.
                tracing::debug!(
                    ?unavailable,
                    "header admission context unavailable; deferring to full verification"
                );
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::serialization::ZcashDeserializeInto;

    #[tokio::test]
    async fn historical_work_passes_without_a_sampled_floor() {
        let block: Arc<block::Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        for height in [None, Some(block::Height(1))] {
            let admission = WorkAdmission::new(tower::service_fn(move |request| async move {
                assert!(matches!(request, zs::Request::CheckHeaderAdmission(_)));
                Ok::<_, BoxError>(zs::Response::HeaderAdmission(height))
            }));
            assert_eq!(
                admission
                    .check(Network::Mainnet, block.clone())
                    .await
                    .unwrap(),
                height
            );
        }
    }

    #[tokio::test]
    async fn invalid_proof_cannot_reach_parent_context_even_with_a_hard_target() {
        use zakura_chain::work::difficulty::{CompactDifficulty, ParameterDifficulty};
        let canonical: Arc<block::Block> = zakura_test::vectors::BLOCK_MAINNET_1687107_BYTES
            .zcash_deserialize_into()
            .unwrap();
        for target in [
            Network::Mainnet.target_difficulty_limit().to_compact(),
            CompactDifficulty::from_bytes_in_display_order(&0x03000001u32.to_be_bytes()).unwrap(),
        ] {
            let mut block = canonical.clone();
            Arc::make_mut(&mut Arc::make_mut(&mut block).header).difficulty_threshold = target;
            let admission = WorkAdmission::new(tower::service_fn(|_| async {
                panic!("invalid proof must be rejected before reading parent context");
                #[allow(unreachable_code)]
                Ok::<_, BoxError>(zs::Response::HeaderAdmission(None))
            }));
            assert!(admission
                .check(Network::Mainnet, block)
                .await
                .unwrap_err()
                .to_string()
                .contains("invalid proof of work"));
        }
    }
    #[tokio::test(start_paused = true)]
    async fn unavailable_parent_read_preserves_bounded_full_verification() {
        let block: Arc<block::Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let admission = WorkAdmission::new(tower::service_fn(|_| {
            std::future::pending::<Result<zs::Response, BoxError>>()
        }));
        assert_eq!(
            admission
                .check(Network::Mainnet, block.clone())
                .await
                .unwrap(),
            None
        );
        let admission = WorkAdmission::new(tower::service_fn(|_| async {
            Err::<zs::Response, BoxError>("state unavailable".into())
        }));
        assert_eq!(
            admission.check(Network::Mainnet, block).await.unwrap(),
            None
        );
    }
}
