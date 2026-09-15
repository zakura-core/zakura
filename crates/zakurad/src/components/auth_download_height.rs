//! Authenticates downloaded coinbase heights against committed parents.
//!
//! # Security
//!
//! A V5+ coinbase height is authorizing data and can change without changing the block hash. Download and
//! gossip paths inspect it before full validation, so a peer could rewrite it to influence their
//! height policies. A committed parent determines its child height.
//! This module centralizes that check for both paths.

use tower::{Service, ServiceExt};
use zakura_chain::block::{self, Height};
use zakura_state as zs;

/// Checks a claimed height against a committed parent on any stored chain.
/// Local lookup failures and unavailable parents do not prove supplier misconduct.
pub(crate) async fn parent_height_mismatch<S>(
    state: S,
    parent_hash: block::Hash,
    claimed: Option<Height>,
    best_tip: Option<(Height, block::Hash)>,
) -> Option<Height>
where
    S: Service<zs::Request, Response = zs::Response, Error = crate::BoxError>
        + Send
        + Clone
        + 'static,
    S::Future: Send,
{
    if parent_hash == block::Hash([0; 32]) {
        return None;
    }
    let parent_height = match best_tip.filter(|(_, hash)| *hash == parent_hash) {
        Some((height, _)) => height,
        None => {
            let response = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                state.oneshot(zs::Request::AnyChainHeight(parent_hash)),
            )
            .await;
            match response {
                Ok(Ok(zs::Response::AnyChainHeight(Some(height)))) => height,
                Ok(Ok(zs::Response::AnyChainHeight(None))) | Ok(Err(_)) | Err(_) => return None,
                Ok(Ok(_)) => unreachable!("AnyChainHeight returns a height response"),
            }
        }
    };
    let expected = (parent_height + 1)?;
    (claimed != Some(expected)).then_some(expected)
}

/// Clones `canonical` and rewrites its V5+ coinbase height without changing its block hash.
#[cfg(test)]
pub(crate) fn poison_coinbase_height(
    canonical: &zakura_chain::block::Block,
    height: Height,
) -> std::sync::Arc<zakura_chain::block::Block> {
    use std::sync::Arc;
    use zakura_chain::transparent;

    let mut poisoned = canonical.clone();

    let coinbase = Arc::make_mut(
        poisoned
            .transactions
            .first_mut()
            .expect("test block has a coinbase transaction"),
    );

    match coinbase
        .inputs_mut()
        .first_mut()
        .expect("coinbase transaction has an input")
    {
        transparent::Input::Coinbase {
            height: coinbase_height,
            ..
        } => *coinbase_height = height,
        transparent::Input::PrevOut { .. } => panic!("the first input is a coinbase input"),
    }

    Arc::new(poisoned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};

    #[tokio::test]
    async fn committed_parent_authenticates_low_high_and_missing_heights() {
        let parent: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1687107_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let parent_hash = parent.hash();
        let expected = (parent.coinbase_height().unwrap() + 1).unwrap();
        for claimed in [
            None,
            Some(Height(1)),
            Some(Height(4_000_000)),
            Some(expected),
        ] {
            let parent = parent.clone();
            let state = tower::service_fn(move |request| {
                assert!(
                    matches!(request, zs::Request::AnyChainHeight(hash) if hash == parent_hash)
                );
                let parent = parent.clone();
                async move { Ok(zs::Response::AnyChainHeight(parent.coinbase_height())) }
            });
            assert_eq!(
                parent_height_mismatch(state, parent_hash, claimed, None).await,
                (claimed != Some(expected)).then_some(expected)
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn unavailable_parent_and_local_failures_are_neutral() {
        let hash = block::Hash([42; 32]);
        let missing = tower::service_fn(|_| async { Ok(zs::Response::AnyChainHeight(None)) });
        assert_eq!(
            parent_height_mismatch(missing, hash, None, None).await,
            None
        );
        let failed = tower::service_fn(|_| async { Err("local state failure".into()) });
        assert_eq!(parent_height_mismatch(failed, hash, None, None).await, None);
        let pending = tower::service_fn(|_| std::future::pending());
        assert_eq!(
            parent_height_mismatch(pending, hash, None, None).await,
            None
        );
    }

    #[tokio::test]
    async fn tip_fast_path_checks_missing_height_without_state_access() {
        let hash = block::Hash([42; 32]);
        let state = tower::service_fn(|_| async {
            panic!("tip needs no lookup");
            #[allow(unreachable_code)]
            Ok(zs::Response::AnyChainHeight(None))
        });
        assert_eq!(
            parent_height_mismatch(state, hash, None, Some((Height(100), hash))).await,
            Some(Height(101))
        );
    }
}
