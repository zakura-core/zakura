//! A fixed Mainnet work floor for downloaded block admission.
//!
//! This is a resource policy, separate from consensus validation. If Mainnet
//! difficulty falls below this floor, otherwise valid downloaded blocks will be
//! refused until the policy is updated. Rejections must not score peers or mark
//! blocks consensus-invalid.

use zakura_chain::{block::Height, parameters::Network, work::difficulty::CompactDifficulty};

/// Last Mainnet checkpoint when the admission floor was introduced.
///
/// Historical downloads are exempt only until the committed tip reaches this
/// boundary. A peer's claimed height cannot disable the floor after that point.
pub(super) const MAINNET_WORK_FLOOR_HEIGHT: Height = Height(3_535_411);

/// Target of Mainnet block 3,538,452, observed on November 1, 2026 UTC.
///
/// Block hash:
/// `0000000000044089828cb43eee6f528cb2e333cc1380b4308fc07a7babd01bd1`.
/// Source: <https://ccnodes.net/explorer/v1/zcash/blocks>.
const MAINNET_REFERENCE_TARGET: CompactDifficulty =
    CompactDifficulty::from_le_bytes(0x1b6aac57_u32.to_le_bytes());

/// Allow one tenth of the reference difficulty: targets at most ten times larger.
const MAINNET_TARGET_MULTIPLIER: u32 = 10;

/// Whether a downloaded header's claimed target meets the admission work floor.
///
/// This does not verify the header hash or Equihash solution, or replace the
/// parent-dependent difficulty check. Historical Mainnet downloads remain exempt
/// while the committed tip is below the boundary or absent. This preserves
/// historical sync without trusting a peer's old height claim near the tip.
pub(super) fn has_sufficient_work(
    network: &Network,
    height: Height,
    committed_tip_height: Option<Height>,
    target: CompactDifficulty,
) -> bool {
    if height <= MAINNET_WORK_FLOOR_HEIGHT
        && committed_tip_height.is_none_or(|tip| tip < MAINNET_WORK_FLOOR_HEIGHT)
    {
        return true;
    }

    target_meets_work_floor(network, target)
}

/// Whether the claimed target can authorize Mainnet missing-parent recovery.
///
/// Recovery has no historical exemption: an unverified height must never
/// authorize an extra download. Other networks retain their existing rules.
pub(super) fn target_meets_work_floor(network: &Network, target: CompactDifficulty) -> bool {
    if !matches!(network, Network::Mainnet) {
        return true;
    }

    // The fixed reference is valid, and multiplying its 215-bit target by ten
    // fits in the 256-bit expanded target representation.
    let max_target = MAINNET_REFERENCE_TARGET
        .to_expanded()
        .expect("the fixed Mainnet reference target has a valid compact encoding")
        * MAINNET_TARGET_MULTIPLIER;

    target
        .to_expanded()
        .is_some_and(|target| target <= max_target)
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        task::{Context, Poll},
    };
    use zakura_chain::block;

    /// Holds verifier readiness until a test releases it.
    #[derive(Clone)]
    pub(crate) struct GatedBlockVerifier {
        ready: Arc<AtomicBool>,
        pub(crate) polled: Arc<tokio::sync::Notify>,
        waker: Arc<futures::task::AtomicWaker>,
        pub(crate) called: Arc<AtomicBool>,
    }

    impl GatedBlockVerifier {
        pub(crate) fn new() -> Self {
            Self {
                ready: Arc::new(AtomicBool::new(false)),
                polled: Arc::new(tokio::sync::Notify::new()),
                waker: Arc::new(futures::task::AtomicWaker::new()),
                called: Arc::new(AtomicBool::new(false)),
            }
        }

        pub(crate) fn release(&self) {
            self.ready.store(true, Ordering::SeqCst);
            self.waker.wake();
        }
    }

    impl tower::Service<zakura_consensus::Request> for GatedBlockVerifier {
        type Response = block::Hash;
        type Error = crate::BoxError;
        type Future = futures::future::Ready<Result<block::Hash, crate::BoxError>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.waker.register(cx.waker());
            self.polled.notify_one();
            if self.ready.load(Ordering::SeqCst) {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }

        fn call(&mut self, request: zakura_consensus::Request) -> Self::Future {
            self.called.store(true, Ordering::SeqCst);
            futures::future::ready(Ok(request.block().hash()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::work::difficulty::ParameterDifficulty as _;

    #[test]
    fn mainnet_work_floor_covers_the_default_semantic_path() {
        assert!(MAINNET_WORK_FLOOR_HEIGHT <= Network::Mainnet.checkpoint_list().max_height());
    }

    #[test]
    fn mainnet_work_floor_uses_the_committed_tip_despite_an_old_height_claim() {
        let easiest_target = Network::Mainnet.target_difficulty_limit().to_compact();
        for height in [Height(0), MAINNET_WORK_FLOOR_HEIGHT] {
            for tip in [
                MAINNET_WORK_FLOOR_HEIGHT,
                MAINNET_WORK_FLOOR_HEIGHT.next().unwrap(),
            ] {
                assert!(!has_sufficient_work(
                    &Network::Mainnet,
                    height,
                    Some(tip),
                    easiest_target,
                ));
            }
            assert!(has_sufficient_work(
                &Network::Mainnet,
                height,
                Some(MAINNET_WORK_FLOOR_HEIGHT.previous().unwrap()),
                easiest_target,
            ));
        }
        assert!(!target_meets_work_floor(&Network::Mainnet, easiest_target));
    }

    #[test]
    fn mainnet_work_floor_preserves_earlier_blocks_and_other_networks() {
        let easiest_target = Network::Mainnet.target_difficulty_limit().to_compact();
        for height in [Height(0), MAINNET_WORK_FLOOR_HEIGHT] {
            assert!(has_sufficient_work(
                &Network::Mainnet,
                height,
                None,
                easiest_target
            ));
        }
        let height = MAINNET_WORK_FLOOR_HEIGHT.next().unwrap();
        assert!(!has_sufficient_work(
            &Network::Mainnet,
            height,
            None,
            easiest_target
        ));
        for network in [
            Network::new_default_testnet(),
            Network::new_regtest(Default::default()),
        ] {
            assert!(has_sufficient_work(
                &network,
                height,
                Some(height),
                easiest_target
            ));
            assert!(target_meets_work_floor(&network, easiest_target));
        }
    }

    #[test]
    fn mainnet_work_floor_compares_expanded_targets_at_the_boundary() {
        let height = MAINNET_WORK_FLOOR_HEIGHT.next().unwrap();
        let max_target =
            MAINNET_REFERENCE_TARGET.to_expanded().unwrap() * MAINNET_TARGET_MULTIPLIER;
        let easiest_allowed_encoding = max_target.to_compact();
        let easier_encoding = CompactDifficulty::from_le_bytes(
            (u32::from_le_bytes(easiest_allowed_encoding.to_le_bytes()) + 1).to_le_bytes(),
        );

        assert!(has_sufficient_work(
            &Network::Mainnet,
            height,
            None,
            MAINNET_REFERENCE_TARGET
        ));
        assert!(has_sufficient_work(
            &Network::Mainnet,
            height,
            None,
            easiest_allowed_encoding
        ));
        assert!(!has_sufficient_work(
            &Network::Mainnet,
            height,
            None,
            easier_encoding
        ));
        assert!(!has_sufficient_work(
            &Network::Mainnet,
            height,
            None,
            CompactDifficulty::from_le_bytes([0; 4]),
        ));
    }
}
