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
    block::{self, Block, Height},
    serialization::ZcashSerialize,
    transaction::Transaction,
};
use zakura_header_chain::{EngineSnapshot, Frontier};
use zakura_node_services::body_claims::SharedBodyClaims;
use zakura_quic::ConnLoad;
use zakura_rt::{
    sync::{watch, CancellationToken},
    time::Instant,
};

use crate::zakura::{
    transport::{framed_channel, FramedRecv, FramedSend, Peer, Service},
    wire_codec::{decode_frame, encode_frame},
    ServicePeerDirection, ZakuraPeerId,
};
use zakura_rt::collections::HashMap;

use super::{
    blocks::{BlockPhase, VerifierVerdict},
    codec::encode_body,
    commitment::{coinbase_paths, CoinbaseBinding},
    profile::Profile,
    service::{Bpv1Handle, Bpv1Ports},
    service_tests::engine_snapshot,
    test_support::{
        build_large_v6_tx, build_v6_tx, connect, connect_pair, node_id, pair, ports, regtest_nu7,
        solved_block_of, start, wait_until, ScriptedVerifier,
    },
    wire::{Bpv1Message, BPV1_CONTROL_KIND, BPV1_DATA_KIND, PROPAGATION_DEPTH, ZAKURA_CAP_BPV1},
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

    // The handler's disconnect cancels the connection and then removes the
    // peer before the session sees the cancel, so removal wins the race.
    cancel_a.cancel();
    svc_a.remove_peer(&ZakuraPeerId::from_node_id(id_b), 1);
    cancel_b.cancel();
    svc_b.remove_peer(&ZakuraPeerId::from_node_id(id_a), 2);
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

/// Services A and B and a raw session M on A, after M sent A the
/// `HeaderMeta` of A's own solved block and A admitted it and forwarded it
/// to B (bpv1-88, #2753).
struct PeerCopyFirst {
    handle_a: Bpv1Handle,
    handle_b: Bpv1Handle,
    /// A's control stream to M.
    m_control: FramedRecv,
    /// M's ends of the other streams. M's session ends when they drop.
    _m_streams: (FramedSend, FramedSend, FramedRecv),
    /// A's committed tip.
    snapshots_a: watch::Sender<Option<EngineSnapshot>>,
    solved: Block,
}

/// M is a modified peer that got A's solved body early, such as over legacy
/// relay before A commits. It rebuilds the `HeaderMeta` that names A from
/// the body alone and sends it to A before A's miner publishes the block.
async fn peer_copy_first(height: Height, shutdown: &CancellationToken) -> PeerCopyFirst {
    let (id_a, id_b, id_m) = (node_id(91), node_id(92), node_id(93));
    let valid = || ScriptedVerifier::new(VerifierVerdict::Valid);
    let parent = Frontier::new(Height(height.0 - 1), block::Hash([0xEE; 32]));
    let (snapshots_a, committed_snapshots) = watch::channel(Some(engine_snapshot(parent, parent)));
    let ports_a = Bpv1Ports {
        committed_snapshots,
        ..ports(height.0, valid(), SharedBodyClaims::new())
    };
    let (svc_a, handle_a) = start(Profile::TINY, regtest_nu7(), id_a, ports_a, shutdown);
    let (svc_b, handle_b) = start(
        Profile::TINY,
        regtest_nu7(),
        id_b,
        ports(height.0, valid(), SharedBodyClaims::new()),
        shutdown,
    );
    connect(&svc_a, id_a, &svc_b, id_b, shutdown);

    // M is a raw session on A's in-memory streams.
    let (m_ctrl_tx, a_ctrl_rx) = framed_channel(64);
    let (a_ctrl_tx, m_control) = framed_channel(64);
    let (m_data_tx, a_data_rx) = framed_channel(64);
    let (a_data_tx, m_data_rx) = framed_channel(256);
    let mut streams = HashMap::new();
    streams.insert(BPV1_CONTROL_KIND, (a_ctrl_rx, a_ctrl_tx));
    streams.insert(BPV1_DATA_KIND, (a_data_rx, a_data_tx));
    svc_a.add_peer(Peer::new_with_conn_id_and_direction(
        3,
        ZakuraPeerId::from_node_id(id_m),
        None,
        ZAKURA_CAP_BPV1,
        ServicePeerDirection::Inbound,
        streams,
        shutdown.child_token(),
    ));

    let solved = solved_block_of(&handle_a, height, vec![build_large_v6_tx(height, 1, 600)]);
    let block_id = solved.hash();
    let paths = coinbase_paths(&solved).expect("the solved block has a coinbase");
    let mut body = Vec::new();
    for tx in solved.transactions.iter().skip(1) {
        tx.zcash_serialize(&mut body)
            .expect("a v6 transaction serializes");
    }
    let tx_count = u32::try_from(solved.transactions.len()).expect("a test block is small");
    let encoded = encode_body(&Profile::TINY, &body, tx_count).expect("the body encodes");
    let meta = Bpv1Message::HeaderMeta {
        header: solved.header.zcash_serialize_to_vec().unwrap(),
        binding: CoinbaseBinding {
            coinbase: paths.coinbase,
            txid_siblings: paths.txid_siblings,
            auth_siblings: paths.auth_siblings,
            // `solved_block_of` builds every block on this light-client root.
            light_client_root: [0x99; 32],
        },
        coding: encoded.coding,
        part_root: encoded.part_root,
        proposer_node: id_a,
    };
    m_ctrl_tx
        .try_send(encode_frame(&meta).unwrap())
        .expect("A's control stream has room");
    let _m_streams = (m_ctrl_tx, m_data_tx, m_data_rx);
    wait_until("A admits M's copy and forwards it to B", || {
        handle_a.block_phase(&block_id) == Some(BlockPhase::Collecting)
            && handle_b.block_phase(&block_id) == Some(BlockPhase::Collecting)
    })
    .await;
    PeerCopyFirst {
        handle_a,
        handle_b,
        m_control,
        _m_streams,
        snapshots_a,
        solved,
    }
}

/// bpv1-88 (#2753): when A's miner publishes a block whose peer copy A
/// admitted first, A takes the copy over as its proposal. It reports the
/// publication, sends M `BlockDone` without a second `HeaderMeta`, and seeds
/// M and B; B assembles and commits the block. Without the takeover, A holds
/// no part, never sends `BlockDone`, and drops its seed plan, so B cannot get
/// the block from anyone before its deadline.
#[tokio::test]
async fn bpv1_88_a_peer_copy_of_a_solved_block_still_gets_seeded() {
    let height = Height(30);
    let shutdown = CancellationToken::new();
    let PeerCopyFirst {
        handle_a,
        handle_b,
        mut m_control,
        _m_streams: (_m_ctrl_tx, _m_data_tx, mut m_data),
        solved,
        ..
    } = peer_copy_first(height, &shutdown).await;
    let block_id = solved.hash();

    assert!(
        handle_a.publish_solved_block(&solved, height.0),
        "A proposes the block over BPv1"
    );
    assert_eq!(handle_a.block_phase(&block_id), Some(BlockPhase::Done));
    wait_until("B gets A's seeds and commits the block", || {
        handle_b.block_phase(&block_id) == Some(BlockPhase::Done)
    })
    .await;

    // A tells M it holds every part, and sends M no `HeaderMeta` (bpv1-4).
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(10), m_control.recv())
            .await
            .expect("A sends M its BlockDone")
            .expect("A's control stream stays open");
        match decode_frame::<Bpv1Message>(&frame).unwrap() {
            Bpv1Message::BlockDone { block_id: done } if done == block_id => break,
            Bpv1Message::HeaderMeta { .. } => panic!("A sent M's HeaderMeta back"),
            _ => {}
        }
    }
    // A seeds M too: its seeds come behind a flagged `Have` (decision-19).
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(10), m_data.recv())
            .await
            .expect("A seeds M")
            .expect("A's data stream stays open");
        if let Bpv1Message::Have { flags, .. } = decode_frame::<Bpv1Message>(&frame).unwrap() {
            assert!(
                flags.push(),
                "the proposer sends no plain Have (decision-20)"
            );
            break;
        }
    }
    shutdown.cancel();
}

/// bpv1-88: a peer copy that retired before A's miner published the block
/// stays retired, so A reports that BPv1 did not publish it.
#[tokio::test]
async fn bpv1_88_a_retired_peer_copy_reports_no_publication() {
    let height = Height(30);
    let shutdown = CancellationToken::new();
    let PeerCopyFirst {
        handle_a,
        _m_streams: _m,
        snapshots_a,
        solved,
        ..
    } = peer_copy_first(height, &shutdown).await;
    let block_id = solved.hash();

    // A committed tip past the propagation window retires the copy and keeps
    // its record.
    let tip = Frontier::new(
        Height(height.0 + PROPAGATION_DEPTH),
        block::Hash([0x64; 32]),
    );
    snapshots_a.send_replace(Some(engine_snapshot(tip, tip)));
    wait_until("A retires M's copy", || {
        handle_a.block_phase(&block_id) == Some(BlockPhase::Retired)
    })
    .await;

    assert!(
        !handle_a.publish_solved_block(&solved, height.0),
        "A reports that BPv1 did not publish the block"
    );
    assert_eq!(handle_a.block_phase(&block_id), Some(BlockPhase::Retired));
    shutdown.cancel();
}
