//! Stack-4 (`bpv1-ports`) adapter tests:
//! - `BodyStorePort` (`MemoryBodyStore`): assembled `OwnedBlock` shares one
//!   `DecodedBlockPool` charge through final holder.
//! - `BlockVerifierPort` verdict classification (`decision-8`, `bpv1-43`):
//!   `PayloadMismatch` (tx root OR auth-data root) maps to `VerifierVerdict::RootMismatch`,
//!   distinct from `ConsensusInvalid` and `Duplicate`.
//! - `BlockFallbackPort` (`SharedBodyClaims`) + REAL native `block_sync` reactor:
//!   native block sync skips a BPv1-claimed hash, and immediately requests it when
//!   BPv1 releases the claim with `ClaimRelease::Fallback` (or when the deadline expires)!
//! - `admission_result`: maps headerchain's exact-header admission onto
//!   `HeaderAdmissionResult`.

use std::{num::NonZeroU64, time::Duration};

use zakura_chain::block::{self, genesis::regtest_genesis_block};
use zakura_header_chain::{
    BodyCommitmentKind, BodyRuleId, BodyVerificationClass, TransientBodyFailureKind,
};
use zakura_node_services::{
    block_memory::DecodedBlockPool,
    body_claims::SharedBodyClaims,
    header_chain::{ExactHeaderAdmission, PortError},
};
use zakura_rt::{sync::watch, time::Instant};

use super::{
    blocks::{ClaimRelease, HeaderAdmissionResult, VerifierVerdict},
    ports::*,
};
use crate::zakura::block_sync::{
    spawn_block_sync_reactor, BlockSizeEstimate, BlockSyncAction, BlockSyncBlockMeta,
    BlockSyncEvent, BlockSyncFrontiers, BlockSyncStartup, ZakuraBlockSyncConfig,
};

#[test]
fn header_admission_maps_every_headerchain_reply() {
    let parent_hash = block::Hash([7; 32]);
    let admitted = |already_known| {
        Ok(ExactHeaderAdmission::Admitted {
            height: block::Height(42),
            already_known,
        })
    };
    let cases = [
        (
            admitted(false),
            HeaderAdmissionResult::Admitted { height: 42 },
        ),
        // A header headerchain already retains still admits (`decision-2`).
        (
            admitted(true),
            HeaderAdmissionResult::Admitted { height: 42 },
        ),
        (
            Ok(ExactHeaderAdmission::MissingParent { parent_hash }),
            HeaderAdmissionResult::MissingParent { parent_hash },
        ),
        (
            Ok(ExactHeaderAdmission::PeerInvalid),
            HeaderAdmissionResult::PeerInvalid,
        ),
        (
            Ok(ExactHeaderAdmission::LocalError),
            HeaderAdmissionResult::LocalError,
        ),
        (Err(PortError::Timeout), HeaderAdmissionResult::LocalError),
    ];
    for (admission, expected) in cases {
        assert_eq!(admission_result(admission), expected);
    }
}

#[test]
fn verifier_verdict_classification_preserves_root_mismatch_distinct_from_invalid() {
    assert_eq!(
        classify_body_verification_class(BodyVerificationClass::PayloadMismatch(
            BodyCommitmentKind::TransactionMerkleRoot,
        )),
        VerifierVerdict::RootMismatch
    );
    assert_eq!(
        classify_body_verification_class(BodyVerificationClass::PayloadMismatch(
            BodyCommitmentKind::AuthDataRoot,
        )),
        VerifierVerdict::RootMismatch
    );
    assert_eq!(
        classify_body_verification_class(BodyVerificationClass::Duplicate),
        VerifierVerdict::Duplicate
    );
    assert_eq!(
        classify_body_verification_class(BodyVerificationClass::ConsensusInvalid(BodyRuleId::new(
            "block.subsidy"
        ),)),
        VerifierVerdict::Invalid
    );
    assert_eq!(
        classify_body_verification_class(BodyVerificationClass::Retryable(
            TransientBodyFailureKind::MissingContext,
        )),
        VerifierVerdict::LocalCapacity
    );
}

#[test]
fn memory_body_store_admits_retained_blocks_and_settles_through_last_holder() {
    // bpv1-52: a pool sized for non-finalized state retention admits more than
    // eight consecutive blocks while state still holds the earlier ones.
    use zakura_chain::serialization::{ZcashDeserialize, ZcashSerialize};
    let genesis_bytes = regtest_genesis_block().zcash_serialize_to_vec().unwrap();
    let pool = DecodedBlockPool::for_block_count(NonZeroU64::new(128 + 8).unwrap());
    let store = MemoryBodyStore::new(pool.clone());
    let mut held = Vec::new();
    for _ in 0..20 {
        let fresh_block = zakura_chain::block::Block::zcash_deserialize(genesis_bytes.as_slice())
            .expect("fresh unaliased block");
        let owned = store
            .admit_assembled(fresh_block)
            .expect("steady-state pool must admit >8 retained blocks without Capacity");
        held.push(owned);
    }
    assert_eq!(pool.reserved_blocks(), 20);
    // An assembled block shares one pool charge through its last handle (dual-4).
    let tx_handle = held[0].transaction(0).expect("genesis has coinbase");
    drop(held);
    assert_eq!(
        pool.reserved_blocks(),
        1,
        "transaction handle keeps decoded charge alive"
    );
    drop(tx_handle);
    assert_eq!(
        pool.reserved_blocks(),
        0,
        "final holder settles pool charge"
    );
}

// Block sync's test stack builds only outside the simulator.
#[cfg(not(zakura_sim))]
#[tokio::test]
async fn native_block_sync_skips_bpv1_claimed_hash_and_fetches_on_fallback_release() {
    let claims = SharedBodyClaims::new();
    let fallback_port: &dyn BlockFallbackPort = &claims;

    let genesis_hash = regtest_genesis_block().hash();
    let h1 = block::Hash([0x11; 32]);
    let h2 = block::Hash([0x22; 32]);

    // Claim h1 for BPv1 BEFORE starting native block sync!
    assert!(fallback_port.claim(h1, Instant::now() + Duration::from_secs(30)));

    let frontiers = BlockSyncFrontiers {
        finalized_height: block::Height(10),
        verified_block_tip: block::Height(10),
        verified_block_hash: genesis_hash,
    };
    let (_tip_tx, tip_rx) = watch::channel((block::Height(12), h2));
    let pool = DecodedBlockPool::for_block_count(NonZeroU64::new(8).unwrap());
    let mut startup = BlockSyncStartup::new(
        frontiers,
        (block::Height(12), h2),
        tip_rx,
        pool,
        ZakuraBlockSyncConfig::default(),
    )
    .with_body_claims(claims.clone());
    startup.committed_views = Some(crate::zakura::testkit::startup_committed_view(
        frontiers,
        (block::Height(12), h2),
    ));
    let shutdown = startup.shutdown.clone();

    let (handle, mut actions, task) =
        spawn_block_sync_reactor(startup).expect("block sync reactor spawns");

    // Each query for needed blocks (11..=12) gets h1 (claimed) and h2 (unclaimed).
    let mut answer_query = async || {
        while !matches!(
            tokio::time::timeout(Duration::from_secs(2), actions.recv())
                .await
                .expect("reactor emits QueryNeededBlocks")
                .expect("action stream open"),
            BlockSyncAction::QueryNeededBlocks { .. }
        ) {}
        let meta = |height, hash| BlockSyncBlockMeta {
            height: block::Height(height),
            hash,
            size: BlockSizeEstimate::Confirmed(1_000),
        };
        handle
            .send_control(BlockSyncEvent::NeededBlocks(vec![
                meta(11, h1),
                meta(12, h2),
            ]))
            .expect("send NeededBlocks");
    };

    answer_query().await;
    // Wait briefly for candidate state to update: height 11 MUST be skipped while claimed,
    // height 12 MUST be present!
    tokio::time::sleep(Duration::from_millis(40)).await;
    let candidates = handle.candidate_state();
    assert!(
        !candidates.missing_block_bodies.contains(&block::Height(11)),
        "08 §4: native block sync must skip BPv1-claimed hash h1"
    );
    assert!(
        candidates.missing_block_bodies.contains(&block::Height(12)),
        "unclaimed hash h2 remains eligible for native block sync"
    );

    // 2. Release h1 with Fallback -> native block sync immediately wakes and re-queries!
    assert!(fallback_port.release(h1, ClaimRelease::Fallback));
    answer_query().await;

    tokio::time::sleep(Duration::from_millis(40)).await;
    let candidates_after = handle.candidate_state();
    assert!(
        candidates_after
            .missing_block_bodies
            .contains(&block::Height(11)),
        "08 §4: native block sync schedules h1 immediately after BPv1 releases fallback claim"
    );

    shutdown.cancel();
    let _ = task.await;
}
