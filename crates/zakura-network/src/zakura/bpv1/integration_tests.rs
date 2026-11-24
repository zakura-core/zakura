//! Level-10 service-pair tests.
//!
//! Covers:
//! 1. A TINY block and a near-limit (~1.96 MB) `Profile::W2` block end-to-end
//!    across `Bpv1Service`, with clean drain.
//! 2. An empty body that completes at metadata admission.
//! 3. Valid body recovery after `VerifierVerdict::RootMismatch` (bad encoding
//!    retires immediately, releases `SharedBodyClaims` with `Fallback`, and
//!    wakes native `block_sync`).
//! 4. Legacy-first `Duplicate` verdict, which commits without a fallback.

use std::time::Duration;

use zakura_chain::{
    block::{self, Height},
    serialization::ZcashSerialize,
    transaction::Transaction,
};
use zakura_node_services::body_claims::SharedBodyClaims;
use zakura_quic::ConnLoad;
use zakura_rt::{
    sync::{watch, CancellationToken},
    time::Instant,
};

use super::{
    blocks::{BlockPhase, VerifierVerdict},
    profile::Profile,
    service::Bpv1Handle,
    test_support::{
        build_large_v6_tx, build_v6_tx, connect_pair, node_id, pair, ports, regtest_nu7,
        solved_block_of, start, wait_until, ScriptedVerifier,
    },
};

/// Level-10: a proposer publishes a solved block to one receiver through two
/// real services, for a TINY block and a near-limit 2 MB W2 block. The
/// receiver submits it once (`bpv1-44`), commits it, and releases its body
/// claim. Both wire traces conform, and teardown drains sessions and path
/// entries (`bpv1-62`, `bpv1-65`).
#[tokio::test]
async fn level10_proposer_to_receiver_end_to_end_conforms_and_drains() {
    // Two small transactions, then 32 of about 61 KB (about 1.96 MB).
    let small = |height| (1..=2).map(|tag| build_v6_tx(height, tag)).collect();
    let near_limit = |height| {
        (0..32u8)
            .map(|i| build_large_v6_tx(height, i.wrapping_add(1), 61_000))
            .collect()
    };
    let cases: [(Profile, Height, fn(Height) -> Vec<Transaction>); 2] = [
        (Profile::TINY, Height(10), small),
        (Profile::W2, Height(20), near_limit),
    ];
    for (profile, height, build_txs) in cases {
        proposer_to_receiver_end_to_end(profile, height, build_txs(height)).await;
    }
}

/// Runs one solved block from a proposer A to a receiver B.
async fn proposer_to_receiver_end_to_end(
    profile: Profile,
    height: Height,
    non_coinbase: Vec<Transaction>,
) {
    let (id_a, id_b) = (node_id(81), node_id(82));
    let shutdown = CancellationToken::new();
    let verifier_b = ScriptedVerifier::new(VerifierVerdict::Valid);
    let claims_b = SharedBodyClaims::new();
    let valid = ScriptedVerifier::new(VerifierVerdict::Valid);
    let ports_a = ports(height.0, valid, SharedBodyClaims::new());
    let ports_b = ports(height.0, verifier_b.clone(), claims_b.clone());
    let (svc_a, handle_a) = start(profile, regtest_nu7(), id_a, ports_a, &shutdown);
    let (svc_b, handle_b) = start(profile, regtest_nu7(), id_b, ports_b, &shutdown);
    // B's connection to A reports a path load (tool-9).
    let (_load_tx, load_b) = watch::channel(ConnLoad::with_path(
        Duration::from_millis(20),
        10_000_000,
        10_000_000,
    ));
    let cancel_a = CancellationToken::new();
    let cancel_b = CancellationToken::new();
    connect_pair(
        &svc_a,
        id_a,
        &svc_b,
        id_b,
        cancel_a.clone(),
        cancel_b.clone(),
        Some(load_b),
    );

    let solved = solved_block_of(&handle_a, height, non_coinbase);
    let expected_hash = solved.hash();
    assert!(
        solved.zcash_serialized_size() <= 2_000_000,
        "the block stays within the 2,000,000-byte consensus limit"
    );

    assert!(handle_a.publish_solved_block(&solved, height.0));
    let committed = format!("{profile:?}: B assembles, verifies, and commits the block");
    wait_until(&committed, || {
        handle_b.block_phase(&expected_hash) == Some(BlockPhase::Done)
    })
    .await;
    assert_eq!(
        verifier_b.seen(),
        vec![expected_hash],
        "bpv1-44: one verifier submission"
    );
    assert!(!claims_b.is_claimed(expected_hash, Instant::now()));
    assert_eq!(handle_b.path_entries(), 1, "B measured its path to A");

    cancel_a.cancel();
    cancel_b.cancel();
    wait_until(
        "bpv1-62: both sides drain their sessions, and bpv1-65: B drops its path entry",
        || handle_a.live_sessions() + handle_b.live_sessions() == 0 && handle_b.path_entries() == 0,
    )
    .await;
    shutdown.cancel();
}

/// An empty body has no parts, so B finishes the block once it admits
/// `HeaderMeta`.
#[tokio::test]
async fn level10_empty_body_completes_at_admission() {
    proposer_to_receiver_end_to_end(Profile::TINY, Height(30), Vec::new()).await;
}

/// Publishes a one-transaction block from A to a B whose verifier answers
/// `verdict`, and returns B's handle, B's body claims, and the block hash.
fn publish_to_verifier(
    verdict: VerifierVerdict,
    shutdown: &CancellationToken,
) -> (Bpv1Handle, SharedBodyClaims, block::Hash) {
    let height = Height(30);
    let claims_b = SharedBodyClaims::new();
    let valid = ScriptedVerifier::new(VerifierVerdict::Valid);
    let (handle_a, handle_b) = pair(
        Profile::TINY,
        ports(height.0, valid, SharedBodyClaims::new()),
        ports(height.0, ScriptedVerifier::new(verdict), claims_b.clone()),
        shutdown,
    );
    let solved = solved_block_of(&handle_a, height, vec![build_large_v6_tx(height, 1, 128)]);
    assert!(handle_a.publish_solved_block(&solved, height.0));
    (handle_b, claims_b, solved.hash())
}

#[tokio::test]
async fn level10_root_mismatch_fallback_and_legacy_first_duplicate_matrix() {
    let shutdown = CancellationToken::new();

    // 1. RootMismatch retires bad encoding immediately, releases SharedBodyClaims
    //    with Fallback wakeup, and settles retained part bytes back to 0.
    {
        let (handle_b, claims_b, hash) =
            publish_to_verifier(VerifierVerdict::RootMismatch, &shutdown);
        let mut wake_rx = claims_b.subscribe();
        // The claim bumps the wake generation once; the fallback release bumps it again.
        tokio::time::timeout(
            Duration::from_secs(3),
            wake_rx.wait_for(|generation| *generation >= 2),
        )
        .await
        .expect("RootMismatch must wake native fallback immediately")
        .unwrap();
        wait_until("bad encoding retires immediately on RootMismatch", || {
            handle_b.block_phase(&hash) == Some(BlockPhase::Retired)
        })
        .await;
        assert_eq!(
            handle_b.retained_part_bytes(),
            0,
            "retired bad encoding releases retained part memory"
        );
        assert!(
            !claims_b.is_claimed(hash, Instant::now()),
            "claim released for native sync recovery"
        );
    }

    // 2. Legacy-first delivery -> Duplicate verdict marks BlockPhase::Done cleanly
    //    without closing any peer or triggering fallback.
    {
        let (handle_b, claims_b, hash) = publish_to_verifier(VerifierVerdict::Duplicate, &shutdown);
        let wake_rx = claims_b.subscribe();
        wait_until("B commits the duplicate", || {
            handle_b.block_phase(&hash) == Some(BlockPhase::Done)
        })
        .await;
        assert_eq!(
            handle_b.live_sessions(),
            1,
            "Duplicate verdict does not fault any peer"
        );
        assert_eq!(
            *wake_rx.borrow(),
            1,
            "only the claim bumps the wake generation: Duplicate releases as Committed, not Fallback"
        );
    }

    shutdown.cancel();
}
