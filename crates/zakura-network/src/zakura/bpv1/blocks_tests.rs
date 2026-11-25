//! Stack-3 (`bpv1-blocks`) tests: level-4 proptest sequences & replay,
//! level-5 named scenarios, tool-4 deterministic
//! `MachineMesh` (up to 16 nodes), tool-6 adversaries, and tool-9 scripted paths.

use super::close::Bpv1CloseRow;
use std::time::Duration;

use proptest::prelude::*;
use zakura_chain::{
    block::{
        self, genesis::regtest_genesis_block, merkle::AuthDataRoot,
        ChainHistoryBlockTxAuthCommitmentHash, ChainHistoryMmrRootHash, Height,
    },
    parameters::NetworkUpgrade,
    serialization::ZcashSerialize,
    transaction::Transaction,
};
use zakura_node_services::body_claims::SharedBodyClaims;
use zakura_quic::NodeId;
use zakura_rt::{collections::HashSet, time::Instant};

use super::{
    blocks::*,
    codec::{encode_body, EncodedBody},
    commitment::commitment_script,
    paths::{PathDirection, PathEstimate, PeerPaths, DEFAULT_UNMEASURED_RATE_BPS},
    peer::{PartAuth, PeerConfig},
    ports::BlockFallbackPort,
    profile::{Coding, Profile},
    proposer::*,
    test_support::{
        admit_meta, admit_work, build_v6_coinbase, commit_work, make_envelope, node_id, Adversary,
        LinkProfile, MachineMesh, TEST_TIP_HASH,
    },
    tree::VerifiedPart,
    wire::PROPAGATION_DEPTH,
};

/// Charges the part buffer for `part`, as the IO shell does before proof,
/// then inserts it with no peer snapshot.
fn insert(
    table: &mut BlockTable,
    now: Instant,
    block_id: block::Hash,
    part: &VerifiedPart,
    from_peer: NodeId,
    auth: PartAuth,
) -> BlockStep {
    // An unadmitted block refuses the charge; the uncharged insert is then a no-op.
    let _ = table.charge_part(
        now,
        &block_id,
        part.index(),
        from_peer,
        part.payload().len() as u64,
    );
    table.step(
        now,
        BlockInput::InsertPart {
            block_id,
            part: part.clone(),
            from_peer,
            auth,
            peer_states: Vec::new(),
        },
    )
}

/// Admits a block at `height` proposed by `node_id(1)`.
fn admit_block(
    table: &mut BlockTable,
    now: Instant,
    block_id: block::Hash,
    height: u32,
    coding: Coding,
    part_root: [u8; 32],
) -> BlockStep {
    let envelope = make_envelope(block_id, coding, part_root, node_id(1));
    admit_meta(table, now, envelope, height, &[])
}

/// Admits a block at height 1 proposed by `node_id(1)`.
fn admit(
    table: &mut BlockTable,
    now: Instant,
    block_id: block::Hash,
    coding: Coding,
    part_root: [u8; 32],
) -> BlockStep {
    admit_block(table, now, block_id, 1, coding, part_root)
}

/// Records a pull path to `peer` with `rtt_ms` and `rate_in`.
fn observe(table: &mut BlockTable, now: Instant, peer: NodeId, rtt_ms: u64, rate_in: u64) {
    table.step(
        now,
        BlockInput::PathObserved {
            peer,
            estimate: PathEstimate {
                min_rtt: Some(Duration::from_millis(rtt_ms)),
                rate_in: Some(rate_in),
                rate_out: None,
            },
        },
    );
}

fn tick(table: &mut BlockTable, at: Instant, peer_states: &[PullPeerState]) -> BlockStep {
    table.step(
        at,
        BlockInput::Tick {
            peer_states: peer_states.to_vec(),
        },
    )
}

fn announce(
    table: &mut BlockTable,
    now: Instant,
    peer: NodeId,
    block_id: block::Hash,
    indices: Vec<u32>,
    peer_states: &[PullPeerState],
) -> BlockStep {
    table.step(
        now,
        BlockInput::PeerAnnounced {
            peer,
            block_id,
            indices,
            peer_states: peer_states.to_vec(),
        },
    )
}

fn release(
    table: &mut BlockTable,
    now: Instant,
    peer: NodeId,
    block_id: block::Hash,
    slots: Vec<u32>,
    reason: ReleaseReason,
    peer_states: &[PullPeerState],
) -> BlockStep {
    table.step(
        now,
        BlockInput::ReleaseClaims {
            peer,
            block_id,
            stripe: 0,
            slots,
            reason,
            peer_states: peer_states.to_vec(),
        },
    )
}

/// A pullable peer with no bytes ahead and room for 8 more parts.
fn pull_state(peer: NodeId) -> PullPeerState {
    PullPeerState {
        peer,
        ahead_bytes: 0,
        can_pull: true,
        window_remaining: 8,
    }
}

/// `pull_state(peer)` with room for `window` more parts.
fn pull_window(peer: NodeId, window: u32) -> PullPeerState {
    PullPeerState {
        window_remaining: window,
        ..pull_state(peer)
    }
}

/// Inserts the `k` source parts of one-stripe `encoded` from `node_id(2)`.
/// Returns the verifier and re-encode check work ids they start.
fn assemble(
    table: &mut BlockTable,
    now: Instant,
    block_id: block::Hash,
    encoded: &EncodedBody,
) -> (WorkId, WorkId) {
    let (mut verify, mut check) = (None, None);
    for part in &encoded.parts[..encoded.coding.k as usize] {
        for o in insert(table, now, block_id, part, node_id(2), PartAuth::Want(1)).outputs {
            match o {
                BlockOutput::SubmitVerifier { work_id, .. } => verify = Some(work_id),
                BlockOutput::StartStripeJob {
                    work_id,
                    kind: StripeJob::Check,
                    ..
                } => check = Some(work_id),
                _ => {}
            }
        }
    }
    (
        verify.expect("k source parts assemble the stripe at once"),
        check.expect("an all-systematic stripe queues its re-encode check"),
    )
}

/// Every slot the step grants `to`.
fn granted_slots(step: &BlockStep, to: NodeId) -> Vec<u32> {
    step.outputs
        .iter()
        .filter_map(|o| match o {
            BlockOutput::GrantPull {
                peer, requested, ..
            } if *peer == to => Some(requested.iter().collect::<Vec<u32>>()),
            _ => None,
        })
        .flatten()
        .collect()
}

/// Steps `MetaReceived` for `envelope` from `node_id(2)`.
fn receive(table: &mut BlockTable, now: Instant, envelope: Box<MetaEnvelope>) -> BlockStep {
    table.step(
        now,
        BlockInput::MetaReceived {
            envelope,
            from_peer: node_id(2),
            session_gen: 1,
        },
    )
}

/// Completes the step's header admission with `result`.
fn header_result(
    table: &mut BlockTable,
    now: Instant,
    received: &BlockStep,
    result: HeaderAdmissionResult,
) -> BlockStep {
    table.step(
        now,
        BlockInput::HeaderAdmitted {
            work_id: admit_work(received),
            result,
        },
    )
}

/// Records that `peer` flagged `indices` of `block_id` as pushes.
fn flag_push(
    table: &mut BlockTable,
    now: Instant,
    peer: NodeId,
    block_id: block::Hash,
    indices: Vec<u32>,
    relay: bool,
) -> BlockStep {
    table.step(
        now,
        BlockInput::PushFlagged {
            peer,
            block_id,
            indices,
            relay,
        },
    )
}

/// Completes the step's commitment check as valid, with no live sessions.
fn commitment_ok(table: &mut BlockTable, now: Instant, header_step: &BlockStep) -> BlockStep {
    table.step(
        now,
        BlockInput::CommitmentChecked {
            work_id: commit_work(header_step),
            ok: true,
            peers: Vec::new(),
            rng_seed: 0,
        },
    )
}

#[test]
fn scenario_missing_parent_then_parent_arrives_defers_commitment() {
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let now = Instant::now();
    let parent = block::Hash([0x01; 32]);
    let child = block::Hash([0x02; 32]);
    let coding = Profile::TINY.derive_coding(64, 2).unwrap();
    let s1 = receive(
        &mut table,
        now,
        make_envelope(child, coding, [0xAA; 32], node_id(1)),
    );

    // A missing parent parks the metadata and starts no commitment work (bpv1-1, -11).
    let s2 = header_result(
        &mut table,
        now,
        &s1,
        HeaderAdmissionResult::MissingParent {
            parent_hash: parent,
        },
    );
    assert!(!s2
        .outputs
        .iter()
        .any(|o| matches!(o, BlockOutput::VerifyCommitment { .. })));
    assert_eq!(table.pending_meta_len(), 1);

    // The parent wakes the child, which retries header admission.
    let s3 = table.step(
        now + Duration::from_millis(50),
        BlockInput::ParentAdmitted {
            hash: parent,
            height: 10,
        },
    );
    assert_eq!(table.pending_meta_len(), 0);
    let at = now + Duration::from_millis(60);
    let s4 = header_result(
        &mut table,
        at,
        &s3,
        HeaderAdmissionResult::Admitted {
            height: 11,
            parent_hash: TEST_TIP_HASH,
        },
    );
    let s5 = commitment_ok(&mut table, at, &s4);
    assert!(s5
        .events
        .contains(&BlockEvent::BlockAdmitted { block_id: child }));
    assert_eq!(table.block_phase(&child), Some(BlockPhase::Collecting));
    table.assert_invariants();
}

/// Sends `block_id`'s metadata from `peer` and answers its header admission
/// with a missing parent.
fn park_unknown_parent(
    table: &mut BlockTable,
    now: Instant,
    peer: NodeId,
    block_id: block::Hash,
) -> BlockStep {
    let coding = Profile::TINY.derive_coding(64, 2).unwrap();
    let received = table.step(
        now,
        BlockInput::MetaReceived {
            envelope: make_envelope(block_id, coding, [0xAA; 32], peer),
            from_peer: peer,
            session_gen: 1,
        },
    );
    let parked = header_result(
        table,
        now,
        &received,
        HeaderAdmissionResult::MissingParent {
            parent_hash: block::Hash([0xEE; 32]),
        },
    );
    table.assert_invariants();
    parked
}

/// A made-up header with an unknown parent costs its sender only bytes, so
/// waiting metadata is bounded per peer and per node (bpv1-11), and it never
/// costs viable metadata its place (bpv1-75). Here sixteen peers fill the
/// node-wide pending budget with unknown parents, and a new peer still gets
/// both a viable block admitted and its own missing-parent entry parked.
#[test]
fn bpv1_75_headers_with_unknown_parents_never_refuse_viable_metadata() {
    let now = Instant::now();
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let fake = |peer: u8, i: u8| {
        block::Hash([
            peer, i, 0xF1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
        ])
    };

    // One peer keeps at most its share waiting; the rest are refused neutrally.
    let flooder = node_id(100);
    for i in 0..10 {
        let step = park_unknown_parent(&mut table, now, flooder, fake(100, i));
        let refused = !rejected_sessions(&step, fake(100, i)).is_empty();
        assert_eq!(refused, usize::from(i) >= MAX_PENDING_META_PER_PEER);
        assert!(!step
            .outputs
            .iter()
            .any(|o| matches!(o, BlockOutput::ClosePeer { .. })));
    }
    assert_eq!(table.pending_meta_len(), MAX_PENDING_META_PER_PEER);

    // More flooders fill the node-wide pending budget.
    let flooders = (MAX_PENDING_META_COUNT / MAX_PENDING_META_PER_PEER) as u8;
    for peer in 101..100 + flooders {
        for i in 0..MAX_PENDING_META_PER_PEER as u8 {
            park_unknown_parent(&mut table, now, node_id(peer), fake(peer, i));
        }
    }
    assert_eq!(table.pending_meta_len(), MAX_PENDING_META_COUNT);

    // An honest peer's viable metadata is admitted despite the full budget.
    let honest = node_id(7);
    let coding = Profile::TINY.derive_coding(64, 2).unwrap();
    let viable = block::Hash([0x77; 32]);
    let received = table.step(
        now,
        BlockInput::MetaReceived {
            envelope: make_envelope(viable, coding, [0xAA; 32], honest),
            from_peer: honest,
            session_gen: 1,
        },
    );
    let admitted = header_result(
        &mut table,
        now,
        &received,
        HeaderAdmissionResult::Admitted {
            height: 11,
            parent_hash: TEST_TIP_HASH,
        },
    );
    let committed = commitment_ok(&mut table, now, &admitted);
    assert!(committed
        .events
        .contains(&BlockEvent::BlockAdmitted { block_id: viable }));

    // Its own missing-parent entry takes the place of a flooder's oldest.
    let honest_wait = block::Hash([0x78; 32]);
    let parked = park_unknown_parent(&mut table, now, honest, honest_wait);
    assert!(rejected_sessions(&parked, honest_wait).is_empty());
    assert_eq!(table.pending_meta_len(), MAX_PENDING_META_COUNT);
    let evicted: Vec<_> = parked
        .outputs
        .iter()
        .filter_map(|o| match o {
            BlockOutput::MetaRejected { peer, .. } => Some(*peer),
            _ => None,
        })
        .collect();
    assert_eq!(evicted.len(), 1);
    assert_ne!(evicted[0], honest);
    table.assert_invariants();
}

/// Returns the sessions the step closed with `row`.
fn closed_sessions(step: &BlockStep, row: Bpv1CloseRow) -> Vec<(NodeId, u64)> {
    let mut closed: Vec<(NodeId, u64)> = step
        .outputs
        .iter()
        .filter_map(|o| match o {
            BlockOutput::ClosePeer {
                peer,
                session_gen,
                row: r,
            } if *r == row => Some((*peer, *session_gen)),
            _ => None,
        })
        .collect();
    closed.sort();
    closed
}

/// A header that headerchain rejects as the sender's fault closes every
/// session that sent it, also after it waited for its parent: the block id
/// hashes the header bytes, so each sent the same invalid header.
#[test]
fn invalid_header_closes_every_session_that_sent_it() {
    let now = Instant::now();
    let coding = Profile::TINY.derive_coding(64, 2).unwrap();
    let first = (node_id(2), 1);
    let second = (node_id(3), 7);
    let mut both = vec![first, second];
    both.sort();
    let send_twice = |table: &mut BlockTable, block_id| {
        let s = receive(
            table,
            now,
            make_envelope(block_id, coding, [0xAA; 32], node_id(1)),
        );
        table.step(
            now,
            BlockInput::MetaReceived {
                envelope: make_envelope(block_id, coding, [0xAA; 32], node_id(1)),
                from_peer: second.0,
                session_gen: second.1,
            },
        );
        s
    };

    // Known parent: admission refuses the header at once.
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let direct = block::Hash([0x31; 32]);
    let s = send_twice(&mut table, direct);
    let refused = header_result(&mut table, now, &s, HeaderAdmissionResult::PeerInvalid);
    assert_eq!(
        closed_sessions(&refused, Bpv1CloseRow::Row3InvalidHeader),
        both
    );
    table.assert_invariants();

    // Unknown parent: the header waits, its parent arrives, and admission
    // then refuses it.
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let parent = block::Hash([0x30; 32]);
    let parked = block::Hash([0x32; 32]);
    let s = send_twice(&mut table, parked);
    header_result(
        &mut table,
        now,
        &s,
        HeaderAdmissionResult::MissingParent {
            parent_hash: parent,
        },
    );
    let woken = table.step(
        now,
        BlockInput::ParentAdmitted {
            hash: parent,
            height: 10,
        },
    );
    let refused = header_result(&mut table, now, &woken, HeaderAdmissionResult::PeerInvalid);
    assert_eq!(
        closed_sessions(&refused, Bpv1CloseRow::Row3InvalidHeader),
        both
    );
    assert_eq!(table.pending_meta_len(), 0);
    table.assert_invariants();
}

/// Returns the sessions the step rejected `block_id`'s metadata for.
fn rejected_sessions(step: &BlockStep, block_id: block::Hash) -> Vec<(NodeId, u64)> {
    let mut rejected: Vec<(NodeId, u64)> = step
        .outputs
        .iter()
        .filter_map(|o| match o {
            BlockOutput::MetaRejected {
                block_id: id,
                peer,
                session_gen,
            } if *id == block_id => Some((*peer, *session_gen)),
            _ => None,
        })
        .collect();
    rejected.sort();
    rejected
}

/// A `HeaderMeta` that a second session sends while the first copy waits
/// leaves that session a received marker. Every end that does not admit the
/// block rejects it for both sessions, so neither marker outlives the entry.
#[test]
fn duplicate_metadata_is_rejected_for_every_session_that_sent_it() {
    let now = Instant::now();
    let coding = Profile::TINY.derive_coding(64, 2).unwrap();
    let first = (node_id(2), 1);
    let second = (node_id(3), 7);
    let mut both = vec![first, second];
    both.sort();
    let send_twice = |table: &mut BlockTable, block_id| {
        let s = receive(
            table,
            now,
            make_envelope(block_id, coding, [0xAA; 32], node_id(1)),
        );
        let duplicate = table.step(
            now,
            BlockInput::MetaReceived {
                envelope: make_envelope(block_id, coding, [0xAA; 32], node_id(1)),
                from_peer: second.0,
                session_gen: second.1,
            },
        );
        assert!(duplicate.outputs.is_empty());
        s
    };

    // Missing parent: the pending entry ages out.
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let pending = block::Hash([0x21; 32]);
    let s = send_twice(&mut table, pending);
    header_result(
        &mut table,
        now,
        &s,
        HeaderAdmissionResult::MissingParent {
            parent_hash: block::Hash([0x20; 32]),
        },
    );
    let aged = tick(&mut table, now + PENDING_META_AGE, &[]);
    assert_eq!(rejected_sessions(&aged, pending), both);

    // Below the window: header admission ends the entry at once.
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    table.step(
        now,
        BlockInput::TipHeight {
            height: 100,
            hash: TEST_TIP_HASH,
        },
    );
    let old = block::Hash([0x22; 32]);
    let s = send_twice(&mut table, old);
    let refused = header_result(
        &mut table,
        now,
        &s,
        HeaderAdmissionResult::Admitted {
            height: 50,
            parent_hash: TEST_TIP_HASH,
        },
    );
    assert_eq!(rejected_sessions(&refused, old), both);
    table.assert_invariants();
}

/// decision-10, bpv1-41: `k` source parts assemble the block at once and
/// queue a re-encode check. A failed check falls back and retires the
/// encoding, so its parts stop circulating and no pull is planned.
#[test]
fn scenario_all_systematic_fast_path_and_reencode_failure_triggers_immediate_fallback() {
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let now = Instant::now();
    let block_id = block::Hash([0x33; 32]);
    let encoded = encode_body(&Profile::TINY, &[0x42; 64], 2).unwrap();
    assert_eq!(encoded.coding.stripes, 1);
    admit(&mut table, now, block_id, encoded.coding, encoded.part_root);
    let (_, check) = assemble(&mut table, now, block_id, &encoded);
    assert_eq!(table.block_phase(&block_id), Some(BlockPhase::Verifying));

    let fail = table.step(
        now,
        BlockInput::StripeJobDone {
            work_id: check,
            block_id,
            stripe: 0,
            result: Err(()),
        },
    );
    assert!(fail.events.contains(&BlockEvent::FallbackStarted {
        block_id,
        reason: FallbackReason::ReencodeFailed,
    }));
    assert_eq!(table.block_phase(&block_id), Some(BlockPhase::Retired));
    let p = node_id(2);
    let parity = vec![encoded.coding.k];
    let plan = announce(&mut table, now, p, block_id, parity, &[pull_state(p)]);
    assert!(plan.outputs.is_empty());
    table.assert_invariants();
}

/// bpv1-43, bpv1-45: a root mismatch falls back and a consensus-invalid
/// block retires. Both retire the encoding, release the body claim, and
/// close no relay.
#[test]
fn scenario_root_mismatch_falls_back_and_invalid_retires_without_faulting_relays() {
    let now = Instant::now();
    let encoded = encode_body(&Profile::TINY, &[0x55; 64], 2).unwrap();
    let block_id = block::Hash([0x41; 32]);
    let invalid = VerifierVerdict::Invalid;
    for (verdict, event, claim) in [
        (
            VerifierVerdict::RootMismatch,
            BlockEvent::FallbackStarted {
                block_id,
                reason: FallbackReason::RootMismatch,
            },
            ClaimRelease::Fallback,
        ),
        (
            invalid,
            BlockEvent::Retired { block_id },
            ClaimRelease::Retired,
        ),
    ] {
        let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
        admit(&mut table, now, block_id, encoded.coding, encoded.part_root);
        let (work_id, _) = assemble(&mut table, now, block_id, &encoded);
        let s = table.step(
            now,
            BlockInput::VerifierVerdict {
                work_id,
                block_id,
                verdict,
            },
        );
        assert!(s.events.contains(&event), "{event:?}");
        assert!(s.outputs.iter().any(|o| matches!(
            o,
            BlockOutput::ReleaseBodyClaim { block_id: b, reason } if *b == block_id && *reason == claim
        )));
        assert!(!s
            .outputs
            .iter()
            .any(|o| matches!(o, BlockOutput::ClosePeer { .. })));
        assert_eq!(table.block_phase(&block_id), Some(BlockPhase::Retired));
        table.assert_invariants();
    }
}

#[test]
fn scenario_equal_height_forks_and_stale_headerchain_completion() {
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let now = Instant::now();
    let fork_a = block::Hash([0x5A; 32]);
    let fork_b = block::Hash([0x5B; 32]);
    let coding = Profile::TINY.derive_coding(64, 2).unwrap();

    admit(&mut table, now, fork_a, coding, [1; 32]);
    admit(&mut table, now, fork_b, coding, [2; 32]);
    assert_eq!(table.block_phase(&fork_a), Some(BlockPhase::Collecting));
    assert_eq!(table.block_phase(&fork_b), Some(BlockPhase::Collecting));

    // Obsolete / stale headerchain completion is a pure no-op (bpv1-63).
    let stale = table.step(
        now,
        BlockInput::HeaderAdmitted {
            work_id: WorkId(999_999),
            result: HeaderAdmissionResult::PeerInvalid,
        },
    );
    assert!(stale.outputs.is_empty());
    assert!(stale.events.is_empty());
    table.assert_invariants();
}

/// decision-21: an announcer's pull cap is twice its bandwidth-delay product,
/// with a 2-part floor. With no estimate at all a peer routes on the
/// unmeasured median: 0 RTT, so the floor. A later announcer then takes the
/// parts a capped one left.
#[test]
fn pull_cap_is_two_bdp_with_a_two_part_floor() {
    let now = Instant::now();
    let block_id = block::Hash([0x60; 32]);
    let coding = Profile::W2.derive_coding(1_950_000, 100).unwrap();
    assert_eq!((coding.stripes, coding.k), (1, 64));
    let part = u64::from(coding.part_bytes);
    let peer = node_id(10);
    // (RTT ms, rate_in) -> cap. 4x the bandwidth at one RTT gets 4x the parts.
    for (estimate, cap) in [
        (Some((100, 80 * part)), 16),
        (Some((100, 20 * part)), 4),
        (Some((10, 200_000)), 2),
        (None, 2),
    ] {
        let mut table = BlockTable::new(BlockConfig::for_profile(Profile::W2));
        admit(&mut table, now, block_id, coding, [0x66; 32]);
        match estimate {
            Some((rtt, rate)) => observe(&mut table, now, peer, rtt, rate),
            None => {
                assert_eq!(table.paths().median_rtt(), Duration::ZERO);
                assert_eq!(
                    table.paths().median_rate(PathDirection::Pull),
                    DEFAULT_UNMEASURED_RATE_BPS
                );
            }
        }
        let all = (0..coding.k).collect();
        let s = announce(
            &mut table,
            now,
            peer,
            block_id,
            all,
            &[pull_window(peer, 32)],
        );
        assert_eq!(granted_slots(&s, peer).len(), cap, "{estimate:?}");
        table.assert_invariants();
    }

    // A 10 ms, 200 KB/s peer holds the floor of a k = 10 block; a 60 ms,
    // 10 MB/s peer announcing after it gets the other 8.
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::W2));
    let coding = Profile::W2.derive_coding(40_960, 10).unwrap();
    assert_eq!(coding.k, 10);
    admit(&mut table, now, block_id, coding, [0x67; 32]);
    let (thin, fat) = (node_id(20), node_id(21));
    observe(&mut table, now, thin, 10, 200_000);
    observe(&mut table, now, fat, 60, 10_000_000);
    let all: Vec<u32> = (0..coding.k).collect();
    let s = announce(
        &mut table,
        now,
        thin,
        block_id,
        all.clone(),
        &[pull_window(thin, 32)],
    );
    assert_eq!(granted_slots(&s, thin).len(), 2);
    let thin_after = PullPeerState {
        ahead_bytes: 2 * u64::from(coding.part_bytes),
        ..pull_window(thin, 30)
    };
    let later = now + Duration::from_millis(5);
    let s = announce(
        &mut table,
        later,
        fat,
        block_id,
        all,
        &[thin_after, pull_window(fat, 32)],
    );
    assert_eq!(
        granted_slots(&s, fat).len(),
        8,
        "the later announcer gets the rest"
    );
    table.assert_invariants();
}

#[test]
fn scenario_peer_paths_under_connection_churn() {
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let now = Instant::now();
    let mut live: HashSet<NodeId> = HashSet::new();

    for i in 1..=20u8 {
        let p = node_id(i);
        table.step(
            now,
            BlockInput::PathObserved {
                peer: p,
                estimate: PathEstimate {
                    min_rtt: Some(Duration::from_millis(u64::from(i) * 5)),
                    rate_in: Some(u64::from(i) * 100_000),
                    rate_out: Some(u64::from(i) * 100_000),
                },
            },
        );
        live.insert(p);
        assert_eq!(table.paths().len(), live.len(), "bpv1-65 entries == live");

        if i % 3 == 0 {
            let drop_p = node_id(i - 2);
            table.step(
                now,
                BlockInput::SessionEnded {
                    peer: drop_p,
                    peer_states: Vec::new(),
                },
            );
            live.remove(&drop_p);
            assert_eq!(table.paths().len(), live.len(), "bpv1-65 entries == live");
        }
    }
    for p in live.drain() {
        table.step(
            now,
            BlockInput::SessionEnded {
                peer: p,
                peer_states: Vec::new(),
            },
        );
    }
    assert_eq!(table.paths().len(), 0);
}

#[test]
fn scenario_monotonic_ladder_and_unmoved_reconstruction_deadline() {
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let t0 = Instant::now();
    let block_id = block::Hash([0x80; 32]);
    let encoded = encode_body(&Profile::TINY, &[0x11; 128], 2).unwrap();
    let coding = encoded.coding;
    admit(&mut table, t0, block_id, coding, encoded.part_root);

    // One stall interval without a part -> Repair(1) (decision-22).
    let base = stall_base(table.paths(), &coding);
    let r1 = tick(&mut table, t0 + base, &[]);
    assert!(r1
        .events
        .contains(&BlockEvent::RepairRound { block_id, round: 1 }));

    // Partial progress neither lowers the ladder nor moves the deadline (bpv1-57).
    insert(
        &mut table,
        t0 + Duration::from_secs(5),
        block_id,
        &encoded.parts[0],
        node_id(2),
        PartAuth::Want(1),
    );
    // The next stall, `base × 2` after the progress, is round 2: the ladder never decreases.
    let r2 = tick(&mut table, t0 + Duration::from_secs(5) + base * 2, &[]);
    assert!(
        r2.events
            .contains(&BlockEvent::RepairRound { block_id, round: 2 }),
        "ladder never decreases on progress"
    );

    // At the original deadline from admission the block falls back despite progress.
    let dl = tick(&mut table, t0 + RECONSTRUCTION_DEADLINE, &[]);
    assert!(dl.events.contains(&BlockEvent::FallbackStarted {
        block_id,
        reason: FallbackReason::Deadline,
    }));
    assert_eq!(table.block_phase(&block_id), Some(BlockPhase::Fallback));
    table.assert_invariants();
}

/// A body change starts a fresh encode, and a stale `EncodeDone` changes
/// nothing (bpv1-63). A solve whose coinbase lacks the commitment publishes
/// nothing. A committed solve admits the block before any seed (bpv1-8),
/// queues one seed batch per neighbour, and seeds no neighbour too slow to be
/// eligible (decision-19).
#[test]
fn scenario_proposer_reuse_recompute_entry_filter_solve_and_disconnect() {
    let (fast1, fast2, very_slow) = (node_id(2), node_id(3), node_id(4));
    let mut paths = PeerPaths::new();
    // The slow neighbour's rate is far below `SLOW_X ×` the median.
    for (peer, rtt, rate_out) in [
        (fast1, 20, 10_000_000),
        (fast2, 25, 9_000_000),
        (very_slow, 500, 50_000),
    ] {
        let estimate = PathEstimate {
            min_rtt: Some(Duration::from_millis(rtt)),
            rate_in: None,
            rate_out: Some(rate_out),
        };
        paths.observe(peer, estimate);
    }
    let config = ProposerConfig::new([0x11; 32], node_id(1));
    let body = vec![0xAA; 128];
    let body_identity = compute_body_identity(2, &body);
    let (mut proposer, commitment) = encoded_proposer(config, &Profile::TINY, &body, [0; 32]);

    let changed = proposer.step(ProposerInput::TemplateBuilt {
        template_id: TemplateId(2),
        body_identity: compute_body_identity(2, &[0xBB; 128]),
        light_client_root: [0; 32],
    });
    assert!(changed.events.iter().any(|e| matches!(
        e,
        ProposerEvent::EncodeStarted {
            template_id: TemplateId(2),
            ..
        }
    )));
    let stale = proposer.step(ProposerInput::EncodeDone {
        work_id: WorkId(999),
        encoded: encode_body(&Profile::TINY, &body, 2).unwrap(),
    });
    assert!(stale.outputs.is_empty());

    let block_id = block::Hash([0xCC; 32]);
    let peers = [fast1, fast2, very_slow];
    let pool_solve = proposer.step(ProposerInput::Solved {
        block_id,
        height: 100,
        header_bytes: vec![0x01; 32],
        coinbase_bytes: vec![0x00; 16],
        txid_siblings: Vec::new(),
        auth_siblings: Vec::new(),
        body_identity,
        peers: peers.to_vec(),
        paths: Box::new(paths.clone()),
    });
    assert!(pool_solve.outputs.is_empty());

    let c = commitment.commitment_hash;
    let lcr = commitment.light_client_root;
    let solved = solve(
        &mut proposer,
        block_id,
        c,
        lcr,
        body_identity,
        &peers,
        &paths,
    );
    assert!(solved.events.contains(&ProposerEvent::Solved { block_id }));
    let position = |want: fn(&ProposerOutput) -> bool| solved.outputs.iter().position(want);
    let first_seed =
        position(|o| matches!(o, ProposerOutput::QueueSeeds { .. })).expect("seed parts queued");
    let admit = position(|o| matches!(o, ProposerOutput::AdmitSolvedLocal { .. }))
        .expect("AdmitSolvedLocal queued");
    assert!(
        admit < first_seed,
        "AdmitSolvedLocal, which broadcasts HeaderMeta and BlockDone, comes before any seed"
    );
    let mut seeded = HashSet::new();
    for o in &solved.outputs {
        if let ProposerOutput::QueueSeeds { peer, .. } = o {
            assert_ne!(*peer, very_slow, "the slow neighbour gets no seed");
            assert!(seeded.insert(*peer), "one seed batch per neighbour");
        }
    }
}

/// A pool that keeps the commitment but rebuilds the coinbase as v5 gets no
/// BPv1 publication. Receivers require a v6 coinbase and close the sender of
/// any other version (decision-9), so the block goes out through existing
/// relay instead.
#[test]
fn proposer_does_not_publish_a_v5_coinbase_that_receivers_refuse() {
    let config = ProposerConfig::new([0x11; 32], node_id(1));
    let body = vec![0xAA; 128];
    let body_identity = compute_body_identity(2, &body);
    let (mut proposer, commitment) = encoded_proposer(config, &Profile::TINY, &body, [0; 32]);
    let peers = [node_id(2), node_id(3)];
    let mut paths = PeerPaths::new();
    for peer in peers {
        paths.observe(
            peer,
            PathEstimate {
                min_rtt: Some(Duration::from_millis(20)),
                rate_in: None,
                rate_out: Some(10_000_000),
            },
        );
    }

    let Transaction::V6 {
        lock_time,
        expiry_height,
        inputs,
        outputs,
        ..
    } = commit_coinbase(commitment.commitment_hash)
    else {
        unreachable!("commit_coinbase builds a v6 coinbase");
    };
    let v5 = Transaction::V5 {
        network_upgrade: NetworkUpgrade::Nu7,
        lock_time,
        expiry_height,
        inputs,
        outputs,
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    };
    let solved = proposer.step(ProposerInput::Solved {
        block_id: block::Hash([0xCC; 32]),
        height: 100,
        header_bytes: solved_header(&v5, commitment.light_client_root),
        coinbase_bytes: v5.zcash_serialize_to_vec().unwrap(),
        txid_siblings: Vec::new(),
        auth_siblings: Vec::new(),
        body_identity,
        peers: peers.to_vec(),
        paths: Box::new(paths),
    });
    assert!(solved.outputs.is_empty(), "a v5 coinbase publishes nothing");
    assert!(solved.events.is_empty());
}

#[test]
fn tool4_sixteen_node_mesh_seeded_pull_converges_with_adversary_and_wrong_estimate() {
    // 16 nodes:
    // - node 0 proposes;
    // - nodes 1..=4 are its entry peers, in a ring whose link from 4 is silent;
    // - nodes 5..=15 hang off the honest entry peers in a downstream ring, and
    //   node 5 holds a wrong fast estimate for a slow link (tool-9).
    let block_cfg = BlockConfig::for_profile(Profile::TINY);
    let peer_cfg = PeerConfig::for_profile(Profile::TINY);
    let mut mesh = MachineMesh::new(16, block_cfg);

    let fast = LinkProfile::new(Duration::from_millis(15), 1_000_000);
    let slow = LinkProfile::new(Duration::from_millis(80), 4_000);
    let fast_est = PathEstimate {
        min_rtt: Some(Duration::from_millis(30)),
        rate_in: Some(1_000_000),
        rate_out: Some(1_000_000),
    };
    let wrong_fast_est = PathEstimate {
        min_rtt: Some(Duration::from_millis(10)),
        rate_in: Some(5_000_000),
        rate_out: Some(5_000_000),
    };
    let est = Some(fast_est);

    for e in 1..=4 {
        mesh.connect(0, e, fast, fast, peer_cfg, est, est);
    }
    for e in 1..=4 {
        let (next, out) = if e == 4 {
            (1, fast.silent())
        } else {
            (e + 1, fast)
        };
        mesh.connect(e, next, out, fast, peer_cfg, est, est);
    }
    for d in 5..16 {
        let e1 = 1 + (d % 3);
        let e2 = 1 + ((d + 1) % 3);
        let (l1, est1) = if d == 5 {
            (slow, Some(wrong_fast_est))
        } else {
            (fast, est)
        };
        mesh.connect(e1, d, l1, fast, peer_cfg, est, est1);
        mesh.connect(e2, d, fast, fast, peer_cfg, est, est);
        if d > 5 {
            mesh.connect(d - 1, d, fast, fast, peer_cfg, est, est);
        }
    }

    // 256 bytes = 8 source parts: 2 stripes of k=4, n=6.
    let block_id = block::Hash([0xFA; 32]);
    let encoded = encode_body(&Profile::TINY, &[0x77; 256], 5).unwrap();
    assert_eq!(encoded.coding.stripes, 2);
    let envelope = make_envelope(
        block_id,
        encoded.coding,
        encoded.part_root,
        mesh.nodes[0].id,
    );
    mesh.propose(0, envelope, encoded.parts);

    let deadline = mesh.now + Duration::from_secs(5);
    mesh.run_until(deadline);
    mesh.assert_honest_links_open();

    // Holders pushed their pushed seeds on (decision-24).
    assert!(mesh.pushed_parts_accepted() > 0, "holders push");

    // Every node verifies well before the 15 s deadline, although node 4's
    // outgoing link is silent and node 5 trusts a wrong estimate.
    for idx in 0..16 {
        assert!(
            mesh.verified(idx, &block_id),
            "node {idx} failed to reconstruct block; phase={:?}",
            mesh.nodes[idx].table.block_phase(&block_id)
        );
    }
}

/// A TINY two-stripe block proposed by mesh node 0, seeded by its plan.
fn propose_two_stripe_block(mesh: &mut MachineMesh) -> block::Hash {
    let block_id = block::Hash([0xFB; 32]);
    let encoded = encode_body(&Profile::TINY, &[0x77; 256], 5).unwrap();
    assert_eq!(encoded.coding.stripes, 2);
    let envelope = make_envelope(
        block_id,
        encoded.coding,
        encoded.part_root,
        mesh.nodes[0].id,
    );
    mesh.propose(0, envelope, encoded.parts);
    block_id
}

/// cov-4 (bpv1-59, level-7): a lying announcer that never answers and one
/// that answers with nothing cannot stop honest nodes fetching the parts
/// honest peers announced. Each looks faster than every honest peer, and
/// neither is granted a slot again after it failed it.
#[test]
fn bpv1_59_lying_and_empty_announcers_cannot_stop_honest_fetches() {
    let peer_cfg = PeerConfig::for_profile(Profile::TINY);
    let mut mesh = MachineMesh::new(7, BlockConfig::for_profile(Profile::TINY));
    mesh.make_adversary(5, Adversary::LiesThenSilent);
    mesh.make_adversary(6, Adversary::LiesThenEmpty);

    let honest = LinkProfile::new(Duration::from_millis(100), 500_000);
    let near = LinkProfile::new(Duration::from_millis(5), 500_000);
    let honest_est = Some(PathEstimate {
        min_rtt: Some(Duration::from_millis(60)),
        rate_in: Some(500_000),
        rate_out: Some(500_000),
    });
    let lying_est = Some(PathEstimate {
        min_rtt: Some(Duration::from_millis(5)),
        rate_in: Some(50_000_000),
        rate_out: Some(50_000_000),
    });
    for e in [1, 2] {
        mesh.connect(0, e, honest, honest, peer_cfg, honest_est, honest_est);
    }
    mesh.connect(1, 2, honest, honest, peer_cfg, honest_est, honest_est);
    // The entry peers' data reaches the victims after the adversaries
    // announce, so the victims pull before any holder push arrives.
    let slow_data = LinkProfile {
        data_delay: Duration::from_millis(150),
        ..honest
    };
    for victim in [3, 4] {
        for e in [1, 2] {
            mesh.connect(
                e, victim, slow_data, honest, peer_cfg, honest_est, honest_est,
            );
        }
        for adversary in [5, 6] {
            mesh.connect(victim, adversary, near, near, peer_cfg, lying_est, None);
        }
    }

    let block_id = propose_two_stripe_block(&mut mesh);
    let start = mesh.now;
    mesh.run_until(start + Duration::from_secs(5));
    mesh.assert_honest_links_open();

    for idx in 0..5 {
        assert!(mesh.verified(idx, &block_id), "honest node {idx} assembled");
    }
    for victim in [3, 4] {
        let node = &mesh.nodes[victim];
        let first_repair = node
            .block_events
            .iter()
            .find(
                |(_, e)| matches!(e, BlockEvent::RepairRound { block_id: b, .. } if *b == block_id),
            )
            .map(|(at, _)| *at);
        for adversary in [5, 6] {
            let liar = mesh.nodes[adversary].id;
            assert!(
                node.grants.iter().any(|(_, peer, ..)| *peer == liar),
                "node {victim} tried adversary {adversary}"
            );
            let mut granted = HashSet::new();
            for (at, peer, b, stripe, slots) in &node.grants {
                if *peer != liar || *b != block_id || first_repair.is_some_and(|r| *at >= r) {
                    continue;
                }
                for slot in slots.iter() {
                    assert!(
                        granted.insert((*stripe, slot)),
                        "node {victim} re-granted failed slot {stripe}/{slot} to adversary {adversary}"
                    );
                }
            }
        }
    }
}

/// cov-5 (bpv1-57, level-7): when no node can finish a block, each falls back
/// exactly once, at its reconstruction deadline. Partial progress, inserted
/// parts and repair rounds, never moves the deadline.
#[test]
fn bpv1_57_partial_progress_falls_back_once_at_the_unmoved_deadline() {
    let peer_cfg = PeerConfig::for_profile(Profile::TINY);
    let mut mesh = MachineMesh::new(4, BlockConfig::for_profile(Profile::TINY));
    let link = LinkProfile::new(Duration::from_millis(20), 500_000);
    // The proposer's data links carry two seed parts each, then go silent:
    // the entry peers jointly hold stripe 0 and nothing of stripe 1.
    let starved = LinkProfile {
        data_frames: Some(2),
        ..link
    };
    for e in [1, 2] {
        mesh.connect(0, e, starved, link, peer_cfg, None, None);
        mesh.connect(e, 3, link, link, peer_cfg, None, None);
    }
    mesh.connect(1, 2, link, link, peer_cfg, None, None);

    let block_id = propose_two_stripe_block(&mut mesh);
    let start = mesh.now;
    mesh.run_until(start + RECONSTRUCTION_DEADLINE + Duration::from_secs(5));

    for idx in 1..4 {
        let node = &mesh.nodes[idx];
        assert!(!mesh.verified(idx, &block_id), "node {idx} cannot assemble");
        let at = |want: fn(&BlockEvent) -> bool| -> Vec<Instant> {
            node.block_events
                .iter()
                .filter(|(_, e)| want(e))
                .map(|(at, _)| *at)
                .collect()
        };
        let admitted = at(|e| matches!(e, BlockEvent::BlockAdmitted { .. }));
        let fallbacks = at(|e| matches!(e, BlockEvent::FallbackStarted { .. }));
        assert_eq!(admitted.len(), 1, "node {idx} admitted once");
        assert_eq!(fallbacks.len(), 1, "node {idx} fell back exactly once");
        let waited = fallbacks[0].saturating_duration_since(admitted[0]);
        assert!(
            waited >= RECONSTRUCTION_DEADLINE
                && waited <= RECONSTRUCTION_DEADLINE + Duration::from_millis(100),
            "node {idx} fell back {waited:?} after admission"
        );
    }
    // The entry peers made progress before the deadline.
    assert!(mesh.nodes[1]
        .block_events
        .iter()
        .any(|(_, e)| matches!(e, BlockEvent::PartInserted { .. })));
}

/// bpv1-28 budgets pulls, so it binds only a block that still pulls. A part
/// that arrives while a stripe decodes is kept, and the reconstruction
/// deadline then ends the decode. The block in `Fallback` keeps its verified
/// parts to serve and pulls nothing, so it may hold more than `k` (#2714).
#[test]
fn bpv1_28_pull_budget_binds_only_a_collecting_block() {
    let now = Instant::now();
    let encoded = encode_body(&Profile::TINY, &[0x5A; 128], 2).unwrap();
    let k = encoded.coding.k as usize;
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let block_id = block::Hash([0x27; 32]);
    admit(&mut table, now, block_id, encoded.coding, encoded.part_root);
    // `k - 1` source parts and one parity part start a decode.
    let mut parts: Vec<_> = encoded.parts[..k - 1].to_vec();
    parts.push(encoded.parts[k].clone());
    let mut decode = None;
    for part in &parts {
        for o in insert(
            &mut table,
            now,
            block_id,
            part,
            node_id(2),
            PartAuth::Want(1),
        )
        .outputs
        {
            if let BlockOutput::StartStripeJob {
                work_id,
                kind: StripeJob::Decode,
                ..
            } = o
            {
                decode = Some(work_id);
            }
        }
    }
    assert!(decode.is_some(), "a parity part starts a decode");
    insert(
        &mut table,
        now,
        block_id,
        &encoded.parts[k + 1],
        node_id(3),
        PartAuth::Want(1),
    );
    table.assert_invariants();
    tick(&mut table, now + RECONSTRUCTION_DEADLINE, &[]);
    assert_eq!(table.block_phase(&block_id), Some(BlockPhase::Fallback));
    table.assert_invariants();
}

/// An empty body has no parts, so admission hands the block to the verifier
/// at once: no pull, no `StripeDone`, and `BlockDone` after assembly, as for
/// any assembled block. A `Have` for it plans nothing.
#[test]
fn empty_body_assembles_at_admission_without_parts() {
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let now = Instant::now();
    let block_id = block::Hash([0xE0; 32]);
    let encoded = encode_body(&Profile::TINY, &[], 1).unwrap();
    assert!(encoded.parts.is_empty());
    let sender = node_id(2);
    let admitted = admit(&mut table, now, block_id, encoded.coding, encoded.part_root);
    let position = |want: fn(&BlockOutput) -> bool| admitted.outputs.iter().position(want);
    let publish = position(|o| matches!(o, BlockOutput::PublishAdmitted { .. }))
        .expect("admission publishes the block");
    let submit = position(|o| {
        matches!(o, BlockOutput::SubmitVerifier { stripe_sources, .. } if stripe_sources.is_empty())
    })
    .expect("admission submits the block with no stripes");
    assert!(
        publish < submit,
        "peers learn the block before it assembles"
    );
    assert!(!admitted.outputs.iter().any(|o| matches!(
        o,
        BlockOutput::GrantPull { .. }
            | BlockOutput::PartReady { .. }
            | BlockOutput::NotifyStripeDone { .. }
            | BlockOutput::NotifyBlockDone { .. }
    )));
    assert_eq!(table.block_phase(&block_id), Some(BlockPhase::Verifying));

    // A `Have` naming an index the layout lacks plans nothing.
    let announced = announce(
        &mut table,
        now,
        sender,
        block_id,
        vec![0],
        &[pull_state(sender)],
    );
    assert!(announced.outputs.is_empty());

    let BlockOutput::SubmitVerifier { work_id, .. } = admitted.outputs[submit] else {
        unreachable!("`submit` indexes a SubmitVerifier");
    };
    let assembled = table.step(now, BlockInput::BlockAssembled { work_id, block_id });
    assert!(assembled
        .outputs
        .iter()
        .any(|o| matches!(o, BlockOutput::NotifyBlockDone { block_id: id } if *id == block_id)));
    let verdict = table.step(
        now,
        BlockInput::VerifierVerdict {
            work_id,
            block_id,
            verdict: VerifierVerdict::Valid,
        },
    );
    assert!(verdict.events.contains(&BlockEvent::Outcome {
        block_id,
        outcome: BlockOutcome::Normal,
    }));
    assert_eq!(table.block_phase(&block_id), Some(BlockPhase::Done));
    table.assert_invariants();
}

/// An empty block crosses a three-hop line with control messages only:
/// every node assembles it from `HeaderMeta`, and no data frame crosses any
/// link.
#[test]
fn empty_body_crosses_a_line_with_no_data_frames() {
    let peer_cfg = PeerConfig::for_profile(Profile::TINY);
    let mut mesh = MachineMesh::new(4, BlockConfig::for_profile(Profile::TINY));
    let link = LinkProfile::new(Duration::from_millis(20), 500_000);
    for a in 0..3 {
        mesh.connect(a, a + 1, link, link, peer_cfg, None, None);
    }
    let block_id = block::Hash([0xE1; 32]);
    let encoded = encode_body(&Profile::TINY, &[], 1).unwrap();
    let envelope = make_envelope(
        block_id,
        encoded.coding,
        encoded.part_root,
        mesh.nodes[0].id,
    );
    mesh.propose(0, envelope, encoded.parts);
    let start = mesh.now;
    mesh.run_until(start + Duration::from_secs(1));

    for idx in 1..4 {
        assert!(
            mesh.verified(idx, &block_id),
            "node {idx} assembled the block"
        );
        assert!(mesh.nodes[idx]
            .block_events
            .iter()
            .any(|(_, e)| matches!(e, BlockEvent::Assembled { .. })));
    }
    assert_eq!(mesh.data_frames(), 0, "no Part, Have, or WantEnd crossed");
    assert!(mesh.nodes.iter().all(|node| node.grants.is_empty()));
    mesh.assert_honest_links_open();
}

proptest! {
    /// Level-4 / tool-2: the fuzz replay asserts every invariant after each step.
    #[test]
    fn block_replay_holds_invariants(bytes in prop::collection::vec(any::<u8>(), 0..192)) {
        replay_bpv1_block_bytes(&bytes);
    }
}

/// The expected transaction path depth is total over every `u32` count.
#[test]
fn bpv1_2_tx_path_depth_is_total_over_u32() {
    use super::commitment::expected_tx_path_depth;

    assert_eq!(expected_tx_path_depth(0), 0);
    assert_eq!(expected_tx_path_depth(1), 0);
    assert_eq!(expected_tx_path_depth(2), 1);
    assert_eq!(expected_tx_path_depth(u32::MAX), 32);
}

/// An invalid coding tuple closes its sender under row 4 before any header
/// port call.
#[test]
fn bpv1_61_invalid_layout_closes_row_4_before_header_admission() {
    use super::close::Bpv1CloseRow;
    let mut bad_coding = Profile::TINY.derive_coding(128, 2).unwrap();
    bad_coding.n = bad_coding.k; // breaks parity formula
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let step = receive(
        &mut table,
        Instant::now(),
        make_envelope(block::Hash([10; 32]), bad_coding, [0x11; 32], node_id(1)),
    );
    assert!(
        !step
            .outputs
            .iter()
            .any(|o| matches!(o, BlockOutput::AdmitHeader { .. })),
        "invalid layout must create zero header-port calls"
    );
    assert!(step.outputs.iter().any(|o| matches!(
        o,
        BlockOutput::ClosePeer {
            row: Bpv1CloseRow::Row4BadMetaOrCommitment,
            ..
        }
    )));
}

/// cov-8 (bpv1-10): metadata whose header the headerchain refuses never
/// reaches the commitment check.
#[test]
fn bpv1_10_refused_header_never_reaches_the_commitment_check() {
    let coding = Profile::TINY.derive_coding(128, 2).unwrap();
    let now = Instant::now();
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let received = receive(
        &mut table,
        now,
        make_envelope(block::Hash([0x10; 32]), coding, [0x22; 32], node_id(1)),
    );
    let refused = header_result(
        &mut table,
        now,
        &received,
        HeaderAdmissionResult::PeerInvalid,
    );
    assert!(!refused
        .outputs
        .iter()
        .any(|o| matches!(o, BlockOutput::VerifyCommitment { .. })));
    table.assert_invariants();
}

/// The window excludes `height + PROPAGATION_DEPTH == top`, and the node
/// rechecks it when the commitment check completes.
#[test]
fn bpv1_7_window_excludes_its_edge_and_rechecks_after_the_commitment() {
    let h = |b: u8| block::Hash([b; 32]);
    let coding = Profile::TINY.derive_coding(128, 2).unwrap();
    let now = Instant::now();
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let _ = table.step(
        now,
        BlockInput::TipHeight {
            height: 116,
            hash: TEST_TIP_HASH,
        },
    );
    let s1 = receive(
        &mut table,
        now,
        make_envelope(h(20), coding, [0x22; 32], node_id(1)),
    );
    // height 100 + propagation_depth 16 == 116 == top -> must NOT admit (bpv1-7).
    let s2 = header_result(
        &mut table,
        now,
        &s1,
        HeaderAdmissionResult::Admitted {
            height: 100,
            parent_hash: TEST_TIP_HASH,
        },
    );
    assert!(
        !s2.outputs
            .iter()
            .any(|o| matches!(o, BlockOutput::VerifyCommitment { .. })),
        "height + PROPAGATION_DEPTH == top must be excluded"
    );
    assert!(s2
        .outputs
        .iter()
        .any(|o| matches!(o, BlockOutput::MetaRejected { .. })));

    // Tip advancing between HeaderAdmitted and CommitmentChecked rejects neutrally.
    let r1 = receive(
        &mut table,
        now,
        make_envelope(h(21), coding, [0x33; 32], node_id(1)),
    );
    let r2 = header_result(
        &mut table,
        now,
        &r1,
        HeaderAdmissionResult::Admitted {
            height: 101,
            parent_hash: TEST_TIP_HASH,
        },
    );
    // The tip advances so 101 + 16 == 117 <= top(120) before CommitmentChecked returns.
    let _ = table.step(
        now,
        BlockInput::TipHeight {
            height: 120,
            hash: TEST_TIP_HASH,
        },
    );
    let r3 = commitment_ok(&mut table, now, &r2);
    assert!(
        !r3.outputs
            .iter()
            .any(|o| matches!(o, BlockOutput::PublishAdmitted { .. })),
        "window recheck at commitment completion must drop out-of-window block"
    );
}

/// The table stores no part proven under another `part_root`.
#[test]
fn bpv1_12_part_under_another_part_root_is_not_stored() {
    let encoded_a = encode_body(&Profile::TINY, &[0xAA; 128], 2).unwrap();
    let encoded_b = encode_body(&Profile::TINY, &[0xBB; 128], 2).unwrap();
    let now = Instant::now();
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let b_id = block::Hash([30; 32]);
    admit(&mut table, now, b_id, encoded_a.coding, encoded_a.part_root);
    let bad_insert = insert(
        &mut table,
        now,
        b_id,
        &encoded_b.parts[0],
        node_id(2),
        PartAuth::Want(1),
    );
    assert!(
        bad_insert.outputs.is_empty() && table.get_part(&b_id, 0).is_none(),
        "BlockTable must reject VerifiedPart verified under a different part_root"
    );
}

/// A peer's copy whose commitment check finishes after this node admitted
/// the same block from its own miner never replaces the solved block.
#[test]
fn local_admission_during_a_peer_copy_check_keeps_the_solved_block() {
    let encoded = encode_body(&Profile::TINY, &[0x22; 128], 2).unwrap();
    let block_id = block::Hash([42; 32]);
    let envelope = || make_envelope(block_id, encoded.coding, encoded.part_root, node_id(1));
    let now = Instant::now();
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let received = receive(&mut table, now, envelope());
    let checking = header_result(
        &mut table,
        now,
        &received,
        HeaderAdmissionResult::Admitted {
            height: 1,
            parent_hash: TEST_TIP_HASH,
        },
    );
    let _ = table.step(
        now,
        BlockInput::BlockAdmittedLocal {
            height: 1,
            envelope: envelope(),
            parts: encoded.parts.clone(),
        },
    );
    let late = commitment_ok(&mut table, now, &checking);
    table.assert_invariants();
    assert!(
        late.outputs.is_empty(),
        "the late copy publishes, claims and retires nothing: {:?}",
        late.outputs
    );
    assert_eq!(table.block_phase(&block_id), Some(BlockPhase::Done));
}

/// decision-20: the proposer announces nothing for its own block.
#[test]
fn decision20_proposer_admission_announces_nothing() {
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let encoded = encode_body(&Profile::TINY, &[0x22; 128], 2).unwrap();
    let step = table.step(
        Instant::now(),
        BlockInput::BlockAdmittedLocal {
            height: 51,
            envelope: make_envelope(
                block::Hash([41; 32]),
                encoded.coding,
                encoded.part_root,
                node_id(1),
            ),
            parts: encoded.parts.clone(),
        },
    );
    assert!(
        !step
            .outputs
            .iter()
            .any(|o| matches!(o, BlockOutput::PartReady { .. })),
        "bpv1-9: the proposer emits no readiness for its own block"
    );
}

/// decision-20: a `BlockDone` peer holds every part, so a repair round asks
/// it even without a `Have`; normal pulls never do.
#[test]
fn decision20_repair_asks_block_done_peers_and_normal_pulls_do_not() {
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let now = Instant::now();
    let block_id = block::Hash([0xE1; 32]);
    let coding = Profile::TINY.derive_coding(128, 2).unwrap();
    admit(&mut table, now, block_id, coding, [0xE1; 32]);
    let done = node_id(30);
    let states = vec![pull_state(done)];
    let s = table.step(
        now,
        BlockInput::PeerBlockDone {
            peer: done,
            block_id,
            peer_states: states.clone(),
        },
    );
    assert!(
        granted_slots(&s, done).is_empty(),
        "a normal pull asked a BlockDone peer"
    );
    let mut repaired = false;
    let mut granted_before_repair = false;
    let mut granted_after_repair = false;
    for ms in (10..20_000).step_by(10) {
        let s = tick(&mut table, now + Duration::from_millis(ms), &states);
        repaired |= s
            .events
            .iter()
            .any(|e| matches!(e, BlockEvent::RepairRound { .. }));
        let granted = !granted_slots(&s, done).is_empty();
        granted_before_repair |= granted && !repaired;
        table.assert_invariants();
        if repaired && granted {
            granted_after_repair = true;
            break;
        }
        assert!(
            !granted_before_repair,
            "a BlockDone peer was asked before repair"
        );
    }
    assert!(repaired, "the stalled block entered repair");
    assert!(granted_after_repair, "repair asked the BlockDone peer");
}

/// Unresolved fallback blocks yield their slot to a new block at the
/// concurrency cap, lowest first.
#[test]
fn bpv1_57_fallback_blocks_yield_to_a_new_block_at_the_cap() {
    let h = |b: u8| block::Hash([b; 32]);
    let now = Instant::now();
    let coding = Profile::TINY.derive_coding(128, 2).unwrap();
    let mut cfg = BlockConfig::for_profile(Profile::TINY);
    cfg.max_concurrent_blocks = 2;
    let mut table = BlockTable::new(cfg);
    table.step(
        now,
        BlockInput::TipHeight {
            height: 99,
            hash: TEST_TIP_HASH,
        },
    );
    let late = now + RECONSTRUCTION_DEADLINE + Duration::from_secs(1);
    for i in 0..2u8 {
        admit_block(
            &mut table,
            now,
            h(60 + i),
            100 + u32::from(i),
            coding,
            [0x55; 32],
        );
        // The deadline sends the block to fallback, where it stays.
        tick(&mut table, late, &[]);
        assert_eq!(table.block_phase(&h(60 + i)), Some(BlockPhase::Fallback));
    }
    let b3 = h(62);
    admit_block(&mut table, late, b3, 102, coding, [0x55; 32]);
    assert_eq!(
        table.block_phase(&b3),
        Some(BlockPhase::Collecting),
        "unresolved Fallback entries must be evictable under concurrent cap"
    );
    assert_eq!(table.block_phase(&h(60)), Some(BlockPhase::Retired));
}

/// A Valid verdict is terminal over a late failed stripe check: the block
/// starts no fallback and no second verification (bpv1-44). The failed check
/// still stops coded propagation (bpv1-41), so the encoding retires and its
/// parts stop circulating.
#[test]
fn bpv1_44_valid_verdict_is_terminal_over_a_late_check_failure() {
    let now = Instant::now();
    let encoded = encode_body(&Profile::TINY, &[0x55; 128], 2).unwrap();
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let b_ok = block::Hash([70; 32]);
    admit(&mut table, now, b_ok, encoded.coding, encoded.part_root);
    let (verify, check) = assemble(&mut table, now, b_ok, &encoded);
    // The verifier commits Valid before the background check returns.
    table.step(
        now,
        BlockInput::VerifierVerdict {
            work_id: verify,
            block_id: b_ok,
            verdict: VerifierVerdict::Valid,
        },
    );
    assert_eq!(table.block_phase(&b_ok), Some(BlockPhase::Done));
    let late = table.step(
        now,
        BlockInput::StripeJobDone {
            work_id: check,
            block_id: b_ok,
            stripe: 0,
            result: Err(()),
        },
    );
    assert!(
        !late
            .events
            .iter()
            .any(|e| matches!(e, BlockEvent::FallbackStarted { .. })),
        "committed Done block is terminal over late CheckDone failure"
    );
    assert_eq!(table.block_phase(&b_ok), Some(BlockPhase::Retired));
    assert!(table.get_part(&b_ok, 0).is_none());
    table.assert_invariants();
}

/// `C` commits neither a timestamp nor the light-client root, so a template
/// refresh for the same body reuses the encoding and issues the same `C`. A
/// solve on either template publishes with the light-client root its header
/// commits (check-3). `C` names no peer (decision-19).
#[test]
fn bpv1_2_reused_encoding_keeps_c_and_each_solve_finds_its_light_client_root() {
    let peers = [node_id(2), node_id(3), node_id(4)];
    let body = vec![0x77; 128];
    let body_identity = compute_body_identity(2, &body);
    let (lcr_old, lcr_new) = ([0xA1; 32], [0xA2; 32]);
    let config = ProposerConfig::new([0x22; 32], node_id(1));
    let (mut proposer, commitment) = encoded_proposer(config, &Profile::TINY, &body, lcr_old);
    let c = commitment.commitment_hash;

    // A template on a new parent reuses the encoding and issues the same `C`.
    let refreshed = proposer.step(ProposerInput::TemplateBuilt {
        template_id: TemplateId(2),
        body_identity,
        light_client_root: lcr_new,
    });
    assert_eq!(set_commitment(&refreshed.outputs).commitment_hash, c);

    // A solve on either template publishes that template's light-client root.
    let paths = PeerPaths::new();
    for (block, lcr) in [(88u8, lcr_old), (89, lcr_new)] {
        let block_id = block::Hash([block; 32]);
        let solved = solve(
            &mut proposer,
            block_id,
            c,
            lcr,
            body_identity,
            &peers,
            &paths,
        );
        let published = solved
            .outputs
            .iter()
            .find_map(|o| match o {
                ProposerOutput::AdmitSolvedLocal { envelope, .. } => {
                    Some(envelope.binding.light_client_root)
                }
                _ => None,
            })
            .expect("an issued commitment still matches its solve");
        assert_eq!(published, lcr);
    }

    // A header that commits a root no template had is not published.
    let block_id = block::Hash([90; 32]);
    let unknown = solve(
        &mut proposer,
        block_id,
        c,
        [0xA3; 32],
        body_identity,
        &peers,
        &paths,
    );
    assert!(unknown.outputs.is_empty());
}

/// A proposer that built template 1 for `body` under `light_client_root`
/// and encoded it on `profile`. Returns the proposer and its commitment.
fn encoded_proposer(
    config: ProposerConfig,
    profile: &Profile,
    body: &[u8],
    light_client_root: [u8; 32],
) -> (ProposerState, PreparedCommitment) {
    let mut proposer = ProposerState::new(config);
    let built = proposer.step(ProposerInput::TemplateBuilt {
        template_id: TemplateId(1),
        body_identity: compute_body_identity(2, body),
        light_client_root,
    });
    let work_id = built
        .outputs
        .iter()
        .find_map(|o| match o {
            ProposerOutput::StartEncode { work_id, .. } => Some(*work_id),
            _ => None,
        })
        .expect("a new body starts an encode");
    let done = proposer.step(ProposerInput::EncodeDone {
        work_id,
        encoded: encode_body(profile, body, 2).unwrap(),
    });
    let commitment = set_commitment(&done.outputs);
    (proposer, commitment)
}

/// Steps `Solved` for a block at height 100 whose coinbase commits
/// `commitment_hash` and whose header commits `light_client_root`.
fn solve(
    proposer: &mut ProposerState,
    block_id: block::Hash,
    commitment_hash: [u8; 32],
    light_client_root: [u8; 32],
    body_identity: [u8; 32],
    peers: &[NodeId],
    paths: &PeerPaths,
) -> ProposerStep {
    let coinbase = commit_coinbase(commitment_hash);
    proposer.step(ProposerInput::Solved {
        block_id,
        height: 100,
        header_bytes: solved_header(&coinbase, light_client_root),
        coinbase_bytes: coinbase.zcash_serialize_to_vec().unwrap(),
        txid_siblings: Vec::new(),
        auth_siblings: Vec::new(),
        body_identity,
        peers: peers.to_vec(),
        paths: Box::new(paths.clone()),
    })
}

/// The commitment of the only `SetCommitment` in `outputs`.
fn set_commitment(outputs: &[ProposerOutput]) -> PreparedCommitment {
    outputs
        .iter()
        .find_map(|o| match o {
            ProposerOutput::SetCommitment { commitment, .. } => Some(commitment.clone()),
            _ => None,
        })
        .expect("the step issued a commitment")
}

/// A one-transaction v6 coinbase at height 100 whose only output commits `c`.
fn commit_coinbase(c: [u8; 32]) -> Transaction {
    build_v6_coinbase(Height(100), &commitment_script(c), 0, vec![], &[])
}

/// Serialized header of a block holding only `coinbase`, whose
/// `hashBlockCommitments` commits `light_client_root`.
fn solved_header(coinbase: &Transaction, light_client_root: [u8; 32]) -> Vec<u8> {
    let block_commitment = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
        &ChainHistoryMmrRootHash::from(light_client_root),
        &std::iter::once(coinbase).collect::<AuthDataRoot>(),
    );
    let mut header = *regtest_genesis_block().header;
    *header.commitment_bytes = <[u8; 32]>::from(block_commitment);
    header.zcash_serialize_to_vec().unwrap()
}

/// bpv1-59, bpv1-64: a `Late` or `Failed` release excludes the peer
/// from that slot, so the slot moves to the slower announcer. A `Refused`
/// release is local, so the peer is granted the slot again once it can pull.
/// Only `Late` emits `PullLate`, and only once per slot.
#[test]
fn released_slot_moves_to_another_announcer_unless_refused() {
    let coding = Profile::TINY.derive_coding(128, 2).unwrap();
    let block_id = block::Hash([0xA1; 32]);
    let (fast, backup) = (node_id(2), node_id(3));
    let both = [pull_state(fast), pull_state(backup)];
    for reason in [
        ReleaseReason::Late,
        ReleaseReason::Failed,
        ReleaseReason::Refused,
    ] {
        let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
        let now = Instant::now();
        admit(&mut table, now, block_id, coding, [0xA1; 32]);
        observe(&mut table, now, fast, 5, 10_000_000);
        observe(&mut table, now, backup, 40, 1_000_000);
        announce(&mut table, now, backup, block_id, vec![0], &[]);
        let s = announce(&mut table, now, fast, block_id, vec![0], &both);
        assert_eq!(
            granted_slots(&s, fast),
            vec![0],
            "the faster announcer wins"
        );

        // The release offers only the parked releasing peer, so nothing is granted.
        let parked = PullPeerState {
            can_pull: false,
            ..pull_state(fast)
        };
        let s = release(&mut table, now, fast, block_id, vec![0], reason, &[parked]);
        assert!(s.outputs.is_empty(), "{reason:?}");
        let late = BlockEvent::PullLate {
            peer: fast,
            block_id,
            stripe: 0,
            slot: 0,
        };
        assert_eq!(s.events.contains(&late), reason == ReleaseReason::Late);

        // The next re-plan offers both announcers.
        let s = table.step(
            now,
            BlockInput::PlanPulls {
                block_id,
                stripe: 0,
                peer_states: both.to_vec(),
            },
        );
        let (owner, excluded) = if reason == ReleaseReason::Refused {
            (fast, backup)
        } else {
            (backup, fast)
        };
        assert_eq!(granted_slots(&s, owner), vec![0], "{reason:?}");
        assert!(granted_slots(&s, excluded).is_empty(), "{reason:?}");

        // A second late release of the slot is not late again (bpv1-64).
        if reason == ReleaseReason::Late {
            let s = release(&mut table, now, backup, block_id, vec![0], reason, &both);
            assert!(!s
                .events
                .iter()
                .any(|e| matches!(e, BlockEvent::PullLate { .. })));
        }
        table.assert_invariants();
    }
}

/// Repair rounds repeat every `base × 2^r`,
/// with `base = SLOW_X ×` one part's estimated arrival, until the monotonic
/// deadline, which is the only bound and the only way into fallback. Only the
/// first round releases the body claim, so native block sync races.
#[test]
fn fix15_repair_rounds_repeat_until_the_deadline_falls_back() {
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let t0 = Instant::now();
    let block_id = block::Hash([0xA3; 32]);
    let coding = Profile::TINY.derive_coding(128, 2).unwrap();
    admit(&mut table, t0, block_id, coding, [0xA3; 32]);
    // A 500 ms median RTT makes the base a little over one second.
    observe(&mut table, t0, node_id(9), 500, 10_000_000);
    let base = stall_base(table.paths(), &coding);
    assert!(base > Duration::from_secs(1) && base < Duration::from_millis(1_100));

    let mut rounds = Vec::new();
    let mut fallback = None;
    let mut releases = Vec::new();
    let mut t = t0;
    while t <= t0 + RECONSTRUCTION_DEADLINE {
        let s = tick(&mut table, t, &[]);
        releases.extend(s.outputs.iter().filter_map(|o| match o {
            BlockOutput::ReleaseBodyClaim { reason, .. } => Some((*reason, t - t0)),
            _ => None,
        }));
        for ev in &s.events {
            match ev {
                BlockEvent::RepairRound { round, .. } => rounds.push((*round, t - t0)),
                BlockEvent::FallbackStarted { reason, .. } => {
                    assert!(fallback.is_none(), "fallback starts once");
                    fallback = Some((*reason, t - t0));
                }
                _ => {}
            }
        }
        table.assert_invariants();
        t += Duration::from_millis(250);
    }

    // Round r starts base × 2^(r−1) after the previous round, within one
    // 250 ms tick: about 1 s, 3 s, 7 s; the 15 s deadline comes before round 4.
    assert_eq!(rounds.len(), 3, "rounds {rounds:?}");
    let mut previous = Duration::ZERO;
    for (at, &(round, t)) in rounds.iter().enumerate() {
        let expected = previous + base * (1u32 << at);
        assert_eq!(usize::from(round), at + 1);
        assert!(
            t >= expected && t < expected + Duration::from_millis(250),
            "round {round} at {t:?}, expected {expected:?}"
        );
        previous = t;
    }
    assert_eq!(
        fallback,
        Some((FallbackReason::Deadline, RECONSTRUCTION_DEADLINE))
    );
    assert_eq!(
        releases,
        vec![(ClaimRelease::Repair, rounds[0].1)],
        "round 1 releases the claim; later rounds and fallback do not"
    );
}

/// fix-25: `GrantPull` carries the RTT and bandwidth its arrival used, and
/// `PartInserted` carries the part's sender and source.
#[test]
fn fix25_pull_and_part_events_carry_path_and_source() {
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let t0 = Instant::now();
    let block_id = block::Hash([0xA4; 32]);
    let encoded = encode_body(&Profile::TINY, &[0x44; 128], 2).unwrap();
    admit(&mut table, t0, block_id, encoded.coding, encoded.part_root);
    let peer = node_id(23);
    observe(&mut table, t0, peer, 40, 2_000_000);
    let s = announce(&mut table, t0, peer, block_id, vec![0], &[pull_state(peer)]);
    assert!(s.outputs.iter().any(|o| matches!(
        o,
        BlockOutput::GrantPull { peer: p, arrival, .. }
            if *p == peer
                && arrival.rtt == Duration::from_millis(40)
                && arrival.rate_bps == 2_000_000
    )));

    let inserted = |table: &mut BlockTable, t: Instant, i: usize, auth: PartAuth| {
        insert(table, t, block_id, &encoded.parts[i], peer, auth)
            .events
            .into_iter()
            .find_map(|e| match e {
                BlockEvent::PartInserted {
                    from_peer, source, ..
                } => Some((from_peer, source)),
                _ => None,
            })
    };
    // Unlisted as a pushed seed, a seed-slot part is a direct copy.
    assert_eq!(
        inserted(&mut table, t0, 0, PartAuth::Seed),
        Some((peer, PartSource::Direct))
    );
    assert_eq!(
        inserted(&mut table, t0, 1, PartAuth::Want(1)),
        Some((peer, PartSource::Pull))
    );
    let base = stall_base(table.paths(), &encoded.coding);
    tick(&mut table, t0 + base, &[]);
    assert_eq!(
        inserted(&mut table, t0 + base, 2, PartAuth::Want(1)),
        Some((peer, PartSource::Repair))
    );
}

/// Templates for one body that arrive during its encode share that encode,
/// and each template id gets its commitment when the encode finishes.
#[test]
fn fix14_templates_during_one_encode_share_it_and_each_get_a_commitment() {
    let local = node_id(1);
    let mut proposer = ProposerState::new(ProposerConfig::new([0x11; 32], local));
    let body = vec![0xAB; 128];
    let body_identity = compute_body_identity(2, &body);

    let mut encodes = Vec::new();
    for t in 1..=3u64 {
        let s = proposer.step(ProposerInput::TemplateBuilt {
            template_id: TemplateId(t),
            body_identity,
            light_client_root: [0u8; 32],
        });
        encodes.extend(s.outputs.iter().filter_map(|o| match o {
            ProposerOutput::StartEncode { work_id, .. } => Some(*work_id),
            _ => None,
        }));
    }
    assert_eq!(encodes.len(), 1, "one encode per body");

    let done = proposer.step(ProposerInput::EncodeDone {
        work_id: encodes[0],
        encoded: encode_body(&Profile::TINY, &body, 2).unwrap(),
    });
    let mut committed: Vec<u64> = done
        .outputs
        .iter()
        .filter_map(|o| match o {
            ProposerOutput::SetCommitment { template_id, .. } => Some(template_id.0),
            _ => None,
        })
        .collect();
    committed.sort_unstable();
    assert_eq!(committed, vec![1, 2, 3]);
}

/// Block-keyed state exists only for admitting, pending, or admitted
/// blocks. Ten thousand distinct metadata messages that header admission
/// rejects leave nothing behind, and the block's arrival time lives in its
/// table entry.
#[test]
fn fix6_rejected_metadata_leaves_no_state_and_arrival_lives_in_the_entry() {
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let t0 = Instant::now();
    let encoded = encode_body(&Profile::TINY, &[0x66; 128], 2).unwrap();
    let sender = node_id(2);
    let mut admissions = Vec::new();
    for i in 0..10_000u32 {
        let mut id = [0xF6; 32];
        id[..4].copy_from_slice(&i.to_le_bytes());
        let envelope = make_envelope(block::Hash(id), encoded.coding, encoded.part_root, sender);
        let s = receive(&mut table, t0, envelope);
        admissions.extend(s.outputs.iter().filter_map(|o| match o {
            BlockOutput::AdmitHeader { work_id, .. } => Some(*work_id),
            _ => None,
        }));
        table.assert_invariants();
    }
    assert!(admissions.len() <= MAX_ADMITTING_META_COUNT);
    for work_id in admissions {
        table.step(
            t0,
            BlockInput::HeaderAdmitted {
                work_id,
                result: HeaderAdmissionResult::LocalError,
            },
        );
    }
    table.assert_invariants();
    assert_eq!(table.admitting_meta_len(), 0);
    assert_eq!(table.pending_meta_len(), 0);
    assert!(table.active_admitted_blocks().is_empty());

    // An admitted block measures its first part from its own `HeaderMeta` arrival.
    let block_id = block::Hash([0xA6; 32]);
    admit(&mut table, t0, block_id, encoded.coding, encoded.part_root);
    let part = encoded.parts[0].clone();
    table
        .charge_part(
            t0 + Duration::from_secs(1),
            &block_id,
            part.index(),
            sender,
            part.payload().len() as u64,
        )
        .unwrap();
    let s = table.step(
        t0 + Duration::from_secs(3),
        BlockInput::InsertPart {
            block_id,
            part,
            from_peer: sender,
            auth: PartAuth::Want(1),
            peer_states: Vec::new(),
        },
    );
    assert!(s.events.contains(&BlockEvent::FirstPartStored {
        block_id,
        meta_to_first_part: Duration::from_secs(1),
        first_part_to_stored: Duration::from_secs(2),
    }));
}

/// fix-8: `BlockTable` charges each peer part before proof against the part
/// buffer budget. A refused part never enters the table, a failed proof or a
/// duplicate releases its charge, and retirement releases the rest.
#[test]
fn fix8_part_charge_before_proof_bounds_the_part_buffer() {
    let encoded = encode_body(&Profile::TINY, &[0x88; 128], 2).unwrap();
    let part_len = encoded.parts[0].payload().len() as u64;
    let mut cfg = BlockConfig::for_profile(Profile::TINY);
    cfg.part_buffer_bytes = 2 * part_len;
    let mut table = BlockTable::new(cfg);
    let t0 = Instant::now();
    let block_id = block::Hash([0xA8; 32]);
    admit(&mut table, t0, block_id, encoded.coding, encoded.part_root);
    let (a, b) = (node_id(31), node_id(32));
    // Each insert follows its own charge, so it charges nothing itself.
    let store = |table: &mut BlockTable, i: usize, from_peer: NodeId| {
        table.step(
            t0,
            BlockInput::InsertPart {
                block_id,
                part: encoded.parts[i].clone(),
                from_peer,
                auth: PartAuth::Want(1),
                peer_states: Vec::new(),
            },
        );
        table.assert_invariants();
    };

    assert_eq!(
        table.charge_part(t0, &block::Hash([0xEE; 32]), 0, a, part_len),
        Err(PartRefusal::NotAdmitted)
    );
    assert!(table.charge_part(t0, &block_id, 0, a, part_len).is_ok());
    assert!(table.charge_part(t0, &block_id, 1, a, part_len).is_ok());
    assert_eq!(
        table.charge_part(t0, &block_id, 2, a, part_len),
        Err(PartRefusal::LocalCapacity)
    );
    store(&mut table, 2, a);
    assert!(
        table.get_part(&block_id, 2).is_none(),
        "a refused part never enters the table"
    );

    table.step(
        t0,
        BlockInput::ReleasePartCharge {
            block_id,
            index: 1,
            from_peer: a,
        },
    );
    assert_eq!(table.part_buffer_bytes(), part_len, "failed proof releases");

    store(&mut table, 0, a);
    assert!(table.get_part(&block_id, 0).is_some());
    assert_eq!(
        table.part_buffer_bytes(),
        part_len,
        "a stored part keeps its charge"
    );

    assert!(table.charge_part(t0, &block_id, 0, b, part_len).is_ok());
    store(&mut table, 0, b);
    assert_eq!(table.part_buffer_bytes(), part_len, "a duplicate releases");

    assert!(table.charge_part(t0, &block_id, 1, b, part_len).is_ok());
    table.step(
        t0,
        BlockInput::TipHeight {
            height: 2 + super::wire::PROPAGATION_DEPTH,
            hash: TEST_TIP_HASH,
        },
    );
    // The invariants check that a retired block holds no charge.
    table.assert_invariants();
    assert_eq!(table.block_phase(&block_id), Some(BlockPhase::Retired));
}

/// Every input that raises `top` retires the blocks it pushes out of
/// the propagation window.
#[test]
fn fix2_raising_top_retires_out_of_window_blocks() {
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let t0 = Instant::now();
    let coding = Profile::TINY.derive_coding(128, 2).unwrap();
    let old = block::Hash([0xB1; 32]);
    table.step(
        t0,
        BlockInput::TipHeight {
            height: 49,
            hash: TEST_TIP_HASH,
        },
    );
    admit_block(&mut table, t0, old, 50, coding, [0x21; 32]);

    // Headerchain admits a parent past the window.
    let parent_height = 50 + PROPAGATION_DEPTH + 1;
    let step = table.step(
        t0,
        BlockInput::ParentAdmitted {
            hash: block::Hash([0xB2; 32]),
            height: parent_height,
        },
    );
    assert!(step.events.contains(&BlockEvent::Retired { block_id: old }));
    assert_eq!(table.block_phase(&old), Some(BlockPhase::Retired));

    // An admitted header proves only that headerchain retained it: a fork
    // or a future-dated header admits too. It moves no window, so a block
    // at the selected height still admits after it.
    table.step(
        t0,
        BlockInput::TipHeight {
            height: parent_height,
            hash: TEST_TIP_HASH,
        },
    );
    let high_height = parent_height + PROPAGATION_DEPTH;
    let high = make_envelope(block::Hash([0xB4; 32]), coding, [0x21; 32], node_id(1));
    let received = receive(&mut table, t0, high);
    let step = header_result(
        &mut table,
        t0,
        &received,
        HeaderAdmissionResult::Admitted {
            height: high_height,
            parent_hash: TEST_TIP_HASH,
        },
    );
    assert!(step
        .outputs
        .iter()
        .any(|o| matches!(o, BlockOutput::VerifyCommitment { .. })));
    let mid = block::Hash([0xB3; 32]);
    admit_block(&mut table, t0, mid, parent_height, coding, [0x21; 32]);
    assert_eq!(table.block_phase(&mid), Some(BlockPhase::Collecting));

    // The verified tip moving past the window retires the blocks below it.
    let step = table.step(
        t0,
        BlockInput::TipHeight {
            height: high_height,
            hash: TEST_TIP_HASH,
        },
    );
    assert!(step.events.contains(&BlockEvent::Retired { block_id: mid }));

    table.assert_invariants();
}

/// bpv1-76: while header sync runs far ahead of the verified tip, BPv1 refuses
/// metadata more than `PROPAGATION_DEPTH` above the verified tip without
/// faulting the sender, and admits it once the verified tip catches up.
#[test]
fn bpv1_76_metadata_far_above_the_verified_tip_belongs_to_block_sync() {
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let t0 = Instant::now();
    let coding = Profile::TINY.derive_coding(128, 2).unwrap();
    let verified = 100;
    table.step(
        t0,
        BlockInput::TipHeight {
            height: verified,
            hash: TEST_TIP_HASH,
        },
    );
    // Header sync is near the network tip.
    table.step(
        t0,
        BlockInput::ParentAdmitted {
            hash: block::Hash([0xC0; 32]),
            height: verified + 200,
        },
    );

    let far = make_envelope(block::Hash([0xC1; 32]), coding, [0x21; 32], node_id(1));
    let received = receive(&mut table, t0, far);
    let step = header_result(
        &mut table,
        t0,
        &received,
        HeaderAdmissionResult::Admitted {
            height: verified + 199,
            parent_hash: TEST_TIP_HASH,
        },
    );
    assert!(!step
        .outputs
        .iter()
        .any(|o| matches!(o, BlockOutput::VerifyCommitment { .. })));
    assert!(
        !step
            .outputs
            .iter()
            .any(|o| matches!(o, BlockOutput::ClosePeer { .. })),
        "a block above the gate is block sync's, not the sender's fault",
    );
    table.assert_invariants();

    // The verified tip catches up to within `PROPAGATION_DEPTH`.
    table.step(
        t0,
        BlockInput::TipHeight {
            height: verified + 199 - PROPAGATION_DEPTH,
            hash: TEST_TIP_HASH,
        },
    );
    admit_block(
        &mut table,
        t0,
        block::Hash([0xC2; 32]),
        verified + 199,
        coding,
        [0x21; 32],
    );
    assert_eq!(
        table.block_phase(&block::Hash([0xC2; 32])),
        Some(BlockPhase::Collecting)
    );
    table.assert_invariants();
}

/// bpv1-7: retirement and admission use one window, so a block admission
/// refuses is never kept, and a block it accepts is never retired.
#[test]
fn bpv1_7_retirement_matches_the_admission_window() {
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let t0 = Instant::now();
    let coding = Profile::TINY.derive_coding(128, 2).unwrap();
    table.step(
        t0,
        BlockInput::TipHeight {
            height: 49,
            hash: TEST_TIP_HASH,
        },
    );
    let edge = block::Hash([0xD1; 32]);
    admit_block(&mut table, t0, edge, 50, coding, [0x21; 32]);

    // `top = 50 + PROPAGATION_DEPTH - 1` keeps height 50 in the window.
    let step = table.step(
        t0,
        BlockInput::TipHeight {
            height: 50 + PROPAGATION_DEPTH - 1,
            hash: TEST_TIP_HASH,
        },
    );
    assert!(!step
        .events
        .contains(&BlockEvent::Retired { block_id: edge }));
    // At `top = 50 + PROPAGATION_DEPTH`, admission refuses height 50, so it retires.
    let step = table.step(
        t0,
        BlockInput::TipHeight {
            height: 50 + PROPAGATION_DEPTH,
            hash: TEST_TIP_HASH,
        },
    );
    assert!(step
        .events
        .contains(&BlockEvent::Retired { block_id: edge }));
    table.assert_invariants();
}

/// Native block sync committing a `Fallback` block frees its slot, so
/// the next admission evicts nothing.
#[test]
fn fix4_committed_fallback_block_frees_its_slot_without_eviction() {
    let mut cfg = BlockConfig::for_profile(Profile::TINY);
    cfg.max_concurrent_blocks = 1;
    let mut table = BlockTable::new(cfg);
    let t0 = Instant::now();
    let late = t0 + RECONSTRUCTION_DEADLINE;
    let coding = Profile::TINY.derive_coding(128, 2).unwrap();
    let fallen = block::Hash([0xC1; 32]);
    admit(&mut table, t0, fallen, coding, [0x41; 32]);
    tick(&mut table, late, &[]);
    assert_eq!(table.block_phase(&fallen), Some(BlockPhase::Fallback));

    // An unknown hash changes nothing.
    let step = table.step(
        late,
        BlockInput::FallbackDone {
            block_id: block::Hash([0xCF; 32]),
        },
    );
    assert!(step.events.is_empty() && step.outputs.is_empty());

    let step = table.step(late, BlockInput::FallbackDone { block_id: fallen });
    assert!(step
        .events
        .contains(&BlockEvent::Retired { block_id: fallen }));

    // The next block takes the freed slot; nothing is evicted.
    let next = admit(
        &mut table,
        late,
        block::Hash([0xC2; 32]),
        coding,
        [0x41; 32],
    );
    assert!(
        !next
            .events
            .iter()
            .any(|e| matches!(e, BlockEvent::Retired { .. })),
        "admission evicts nothing"
    );
}

/// decision-18: a padding source part (`wire_len = 0`) is held from
/// admission. The planner never requests it, and the stripe decodes and
/// assembles once its real parts reach `k`.
#[test]
fn padding_part_is_held_at_admission_and_never_pulled() {
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let now = Instant::now();
    let block_id = block::Hash([0x7A; 32]);
    // 160 bytes at `S = 32`: K = 5 over two stripes of k = 3, so stripe 1's
    // last source slot holds only padding.
    let body: Vec<u8> = (0..160u8).collect();
    let encoded = encode_body(&Profile::TINY, &body, 2).unwrap();
    let coding = encoded.coding;
    assert_eq!((coding.part_bytes, coding.stripes, coding.k), (32, 2, 3));
    let padding = coding.wire_index(1, 2).unwrap();
    assert!(coding.is_padding(padding));
    admit(&mut table, now, block_id, coding, encoded.part_root);

    let peer = node_id(30);
    // One RTT of this peer's transfer covers every part.
    observe(&mut table, now, peer, 50, 10_000_000);
    let states = vec![pull_window(peer, 32)];
    let planned = announce(
        &mut table,
        now,
        peer,
        block_id,
        (0..coding.total_parts().unwrap()).collect(),
        &states,
    );
    let mut requested = Vec::new();
    for o in &planned.outputs {
        if let BlockOutput::GrantPull {
            stripe,
            requested: bm,
            ..
        } = o
        {
            requested.extend(
                (0..coding.n)
                    .filter(|&s| bm.contains(s))
                    .map(|s| (*stripe, s)),
            );
        }
    }
    assert_eq!(requested.iter().filter(|(s, _)| *s == 0).count(), 3);
    assert_eq!(
        requested.iter().filter(|(s, _)| *s == 1).count(),
        2,
        "stripe 1 needs only its two real source parts"
    );
    assert!(!requested.contains(&(1, 2)), "padding is never requested");

    // Stripe 1 reaches `k` with its two real parts and checks with padding as zeros.
    for slot in 0..2 {
        let part = &encoded.parts[(coding.n + slot) as usize];
        insert(&mut table, now, block_id, part, peer, PartAuth::Want(1));
    }
    let job = table.step(
        now,
        BlockInput::PlanPulls {
            block_id,
            stripe: 1,
            peer_states: states,
        },
    );
    assert!(job.outputs.is_empty(), "a stripe at k plans nothing");
    table.assert_invariants();
}

/// The `Solved` outputs of a W2 proposer that encoded a `body_bytes` body and
/// solved it with `peers` live.
fn solved_with_paths(
    peers: &[NodeId],
    paths: &PeerPaths,
    body_bytes: usize,
    direct_copies: bool,
) -> Vec<ProposerOutput> {
    let config = ProposerConfig {
        direct_copies,
        ..ProposerConfig::new([0x19; 32], node_id(1))
    };
    let body = vec![0x5A; body_bytes];
    let (mut proposer, commitment) = encoded_proposer(config, &Profile::W2, &body, [0; 32]);
    let block_id = block::Hash([0x19; 32]);
    let c = commitment.commitment_hash;
    let body_identity = compute_body_identity(2, &body);
    let solved = solve(
        &mut proposer,
        block_id,
        c,
        [0; 32],
        body_identity,
        peers,
        paths,
    );
    assert!(solved.events.contains(&ProposerEvent::Solved { block_id }));
    solved.outputs
}

/// Every pushed seed in `plan` as `(holder, index)`.
fn pushed_seeds(plan: &SeedPlan) -> Vec<(NodeId, u32)> {
    plan.covering
        .iter()
        .filter(|seed| seed.pushed)
        .map(|seed| (seed.peer, seed.index))
        .collect()
}

/// Each seeded neighbour's pushed and other seeds, each increasing.
fn seed_lists(plan: &SeedPlan) -> Vec<(NodeId, Vec<u32>, Vec<u32>)> {
    let seeds: Vec<PlannedSeed> = plan.covering.iter().chain(&plan.direct).copied().collect();
    by_peer(&seeds)
        .into_iter()
        .map(|(peer, seeds)| {
            let (mut pushed, mut other): (Vec<u32>, Vec<u32>) = (Vec::new(), Vec::new());
            for seed in seeds {
                if seed.pushed {
                    pushed.push(seed.index);
                } else {
                    other.push(seed.index);
                }
            }
            pushed.sort_unstable();
            other.sort_unstable();
            (peer, pushed, other)
        })
        .collect()
}

/// The most pushed seeds any neighbour holds in one stripe.
fn most_pushed_in_a_stripe(plan: &SeedPlan, coding: &Coding) -> u32 {
    let mut counts: std::collections::HashMap<(NodeId, u32), u32> = Default::default();
    for (peer, index) in pushed_seeds(plan) {
        *counts
            .entry((peer, coding.split_index(index).unwrap().0))
            .or_default() += 1;
    }
    counts.values().copied().max().unwrap_or(0)
}

/// Paths with `rtt`, the proposer's `rate_out` to each peer, and each
/// peer's `rate_in` to the proposer, which stands in for its push rate.
fn plan_paths(peers: &[(NodeId, u64, u64)], rtt: Duration) -> PeerPaths {
    let mut paths = PeerPaths::new();
    for &(peer, rate_out, rate_in) in peers {
        paths.observe(
            peer,
            PathEstimate {
                min_rtt: Some(rtt),
                rate_in: Some(rate_in),
                rate_out: Some(rate_out),
            },
        );
    }
    paths
}

/// decision-19 seed plan: deal-1 pushes every part once within the `n − k`
/// cap, any one neighbour's loss leaves every stripe recoverable (bpv1-67),
/// and no neighbour gets an index twice.
#[test]
fn decision19_seed_plan_pushes_every_part_once_and_survives_one_loss() {
    let local = node_id(1);
    let coding = Profile::TINY.derive_coding(4_000, 2).unwrap();
    let real: Vec<u32> = (0..coding.total_parts().unwrap())
        .filter(|&i| !coding.is_padding(i))
        .collect();
    let parity = coding.n - coding.k;
    // Widening u32 to usize is lossless on supported targets.
    let enough = coding.n.div_ceil(parity) as usize;
    for count in 1..=6u8 {
        let peers: Vec<NodeId> = (2..2 + count).map(node_id).collect();
        let plan = seed_plan(&coding, local, &peers, &PeerPaths::new(), false);
        assert_eq!(plan.holders.len(), peers.len());
        let deal1: Vec<u32> = plan
            .covering
            .iter()
            .filter(|seed| seed.deal == SeedDeal::One)
            .map(|seed| seed.index)
            .collect();
        assert_eq!(deal1, real, "deal-1 gives every part once");
        assert!(most_pushed_in_a_stripe(&plan, &coding) <= parity);
        if peers.len() >= enough {
            assert_eq!(
                pushed_seeds(&plan).len(),
                real.len(),
                "every part is pushed"
            );
        } else {
            // Below `ceil(n / (n − k))` neighbours each fills its cap, and
            // the rest go out as other seeds.
            let pushed = pushed_seeds(&plan);
            for stripe in 0..coding.stripes {
                let in_stripe = |index: &u32| coding.split_index(*index).unwrap().0 == stripe;
                let real_in = real.iter().filter(|i| in_stripe(i)).count();
                // Widening u32 to usize is lossless on supported targets.
                let room = parity as usize * peers.len();
                assert_eq!(
                    pushed.iter().filter(|(_, i)| in_stripe(i)).count(),
                    real_in.min(room)
                );
            }
            assert!(pushed.len() < real.len());
        }
        let lists = seed_lists(&plan);
        for (peer, pushed, other) in &lists {
            assert!(peers.contains(peer));
            let mut all: Vec<u32> = pushed.iter().chain(other).copied().collect();
            all.sort_unstable();
            assert!(
                all.windows(2).all(|w| w[0] < w[1]),
                "a neighbour gets each index once"
            );
            assert!(all.iter().all(|&i| !coding.is_padding(i)));
        }
        if peers.len() >= 2 {
            for lost in &peers {
                for stripe in 0..coding.stripes {
                    let held = (0..coding.n)
                        .filter(|&slot| {
                            let index = coding.wire_index(stripe, slot).unwrap();
                            coding.is_padding(index)
                                || lists.iter().any(|(p, pushed, other)| {
                                    p != lost && (pushed.contains(&index) || other.contains(&index))
                                })
                        })
                        .count() as u32;
                    assert!(
                        held >= coding.k,
                        "losing one neighbour strands stripe {stripe}"
                    );
                }
            }
        }
    }
}

/// Each stripe's even share of pushed seeds:
/// `min(n − k, ceil(real / eligible))` (Adam's finding 2).
fn even_share(coding: &Coding, stripe: u32, eligible: usize) -> u32 {
    // A stripe has at most `MAX_N` parts, so the count fits u32.
    let real = (0..coding.n)
        .filter(|&slot| !coding.is_padding(coding.wire_index(stripe, slot).unwrap()))
        .count() as u32;
    // A test plan has far fewer than u32::MAX neighbours.
    (coding.n - coding.k).min(real.div_ceil(eligible as u32))
}

/// Adam's finding 2: a holder whose push rate is four times the others'
/// fills its even share of every stripe first, and takes no more.
#[test]
fn decision19_a_faster_holder_takes_only_its_even_share() {
    let local = node_id(1);
    let coding = Profile::W2.derive_coding(1_900_000, 2).unwrap();
    let fast = node_id(2);
    let mut peers = vec![(fast, 1_000_000_000, 40_000_000)];
    peers.extend((3..18).map(|b| (node_id(b), 1_000_000_000, 10_000_000)));
    let paths = plan_paths(&peers, Duration::from_millis(50));
    let ids: Vec<NodeId> = peers.iter().map(|p| p.0).collect();
    let plan = seed_plan(&coding, local, &ids, &paths, false);
    assert_eq!(plan.holders.len(), ids.len());
    let pushed = pushed_seeds(&plan);
    for stripe in 0..coding.stripes {
        let cap = even_share(&coding, stripe, ids.len());
        // A stripe has at most `MAX_N` parts, so the count fits u32.
        let in_stripe = |peer: NodeId| {
            pushed
                .iter()
                .filter(|&&(p, i)| p == peer && coding.split_index(i).unwrap().0 == stripe)
                .count() as u32
        };
        assert_eq!(in_stripe(fast), cap, "stripe {stripe}");
        assert!(ids.iter().all(|&peer| in_stripe(peer) <= cap));
    }
}

/// Adam's finding 2: with 32 eligible neighbours, a 2 MB W2 block leaves
/// no neighbour more than 3 pushed seeds of a stripe, and every part is
/// pushed.
#[test]
fn decision19_thirty_two_neighbours_hold_at_most_three_pushed_seeds_a_stripe() {
    let local = node_id(1);
    let coding = Profile::W2.derive_coding(2_000_000, 2).unwrap();
    let peers: Vec<(NodeId, u64, u64)> = (2..34)
        .map(|b| (node_id(b), 100_000_000, 25_000_000))
        .collect();
    let paths = plan_paths(&peers, Duration::from_millis(50));
    let ids: Vec<NodeId> = peers.iter().map(|p| p.0).collect();
    let plan = seed_plan(&coding, local, &ids, &paths, false);
    assert_eq!(plan.holders.len(), 32, "every neighbour is eligible");
    assert!(most_pushed_in_a_stripe(&plan, &coding) <= 3);
    let real = (0..coding.total_parts().unwrap())
        .filter(|&i| !coding.is_padding(i))
        .count();
    assert_eq!(pushed_seeds(&plan).len(), real, "every part is pushed");
}

proptest! {
    /// Adam's finding 2: whichever `floor((n − k) / cap)` holders withhold
    /// their pushes, the other holders still push at least `k` distinct
    /// parts of every stripe, counting padding as held.
    #[test]
    fn decision19_withholding_holders_never_strand_a_stripe(
        body_bytes in 1_000u64..400_000,
        rates in prop::collection::vec((1u64..=8, 1u64..=8), 1..40),
    ) {
        let local = node_id(1);
        let coding = Profile::W2.derive_coding(body_bytes, 2).unwrap();
        let peers: Vec<(NodeId, u64, u64)> = rates
            .iter()
            .enumerate()
            // At most 40 neighbours, so the index fits u8 past node 1.
            .map(|(i, &(out, push))| (node_id(2 + i as u8), out * 10_000_000, push * 5_000_000))
            .collect();
        let paths = plan_paths(&peers, Duration::from_millis(50));
        let ids: Vec<NodeId> = peers.iter().map(|p| p.0).collect();
        let plan = seed_plan(&coding, local, &ids, &paths, false);
        let eligible = plan.holders.len();
        let parity = coding.n - coding.k;
        // Widening u32 to usize is lossless on supported targets.
        prop_assume!(eligible >= coding.n.div_ceil(parity) as usize);
        let pushed = pushed_seeds(&plan);
        for stripe in 0..coding.stripes {
            let cap = even_share(&coding, stripe, eligible);
            // Widening u32 to usize is lossless on supported targets.
            let withheld = (parity / cap) as usize;
            let mut per_holder: Vec<u32> = plan
                .holders
                .iter()
                .map(|h| {
                    // A stripe has at most `MAX_N` parts, so the count fits u32.
                    pushed
                        .iter()
                        .filter(|&&(p, i)| p == h.peer && coding.split_index(i).unwrap().0 == stripe)
                        .count() as u32
                })
                .collect();
            prop_assert!(per_holder.iter().all(|&c| c <= cap));
            // The holders with the most pushed seeds withhold: the worst case.
            per_holder.sort_unstable_by(|a, b| b.cmp(a));
            let kept: u32 = per_holder.iter().skip(withheld).sum();
            // A stripe has at most `MAX_N` parts, so the count fits u32.
            let padding = (0..coding.n)
                .filter(|&slot| coding.is_padding(coding.wire_index(stripe, slot).unwrap()))
                .count() as u32;
            prop_assert!(kept + padding >= coding.k, "stripe {stripe}: {kept} + {padding}");
        }
    }
}

/// 05 §3 "One fast neighbour among slow ones", with feedback: every
/// neighbour may get a direct copy of any part it lacks until it holds `k`
/// parts of the stripe, the closest neighbour's copies come first, and the
/// proposer hands each session its copies with its seeds.
#[test]
fn decision19_each_neighbour_gets_copies_up_to_k_parts_a_stripe() {
    let local = node_id(1);
    let coding = Profile::W2.derive_coding(1_000_000, 2).unwrap();
    let fast = node_id(2);
    let mut peers = vec![(fast, 100_000_000, 100_000_000)];
    peers.extend((3..9).map(|b| (node_id(b), 5_000_000, 5_000_000)));
    let paths = plan_paths(&peers, Duration::from_millis(40));
    let ids: Vec<NodeId> = peers.iter().map(|p| p.0).collect();
    let plan = seed_plan(&coding, local, &ids, &paths, true);
    assert_eq!(plan.direct.first().map(|seed| seed.peer), Some(fast));
    assert!(plan.direct.iter().all(|seed| !seed.pushed));
    for &peer in &ids {
        for stripe in 0..coding.stripes {
            let in_stripe = |seed: &&PlannedSeed| {
                seed.peer == peer && coding.split_index(seed.index).unwrap().0 == stripe
            };
            let seeded: Vec<u32> = plan
                .covering
                .iter()
                .filter(in_stripe)
                .map(|seed| seed.index)
                .collect();
            let copied: Vec<u32> = plan
                .direct
                .iter()
                .filter(in_stripe)
                .map(|seed| seed.index)
                .collect();
            assert!(
                copied.iter().all(|index| !seeded.contains(index)),
                "a direct copy is a part the neighbour lacks"
            );
            // A stripe has at most `MAX_N` parts, so the counts fit u32.
            let real = (0..coding.n)
                .filter(|&slot| !coding.is_padding(coding.wire_index(stripe, slot).unwrap()))
                .count() as u32;
            let (seeded, copied) = (seeded.len() as u32, copied.len() as u32);
            assert_eq!(
                seeded + copied,
                seeded.max(coding.k.min(real)),
                "copies fill stripe {stripe} to k parts"
            );
        }
    }

    // The solve hands each session its copies with its seeds.
    let outputs = solved_with_paths(&ids, &paths, 1_000_000, true);
    let copies: usize = outputs
        .iter()
        .map(|o| match o {
            ProposerOutput::QueueSeeds { copies, .. } => copies.len(),
            _ => 0,
        })
        .sum();
    assert!(copies > 0, "the solve queues direct copies");
}

#[test]
fn default_proposer_seeds_the_mesh_without_direct_copies() {
    let peers: Vec<(NodeId, u64, u64)> = (2..81)
        .map(|id| (node_id(id), 50_000_000, 50_000_000))
        .collect();
    let ids: Vec<NodeId> = peers.iter().map(|peer| peer.0).collect();
    let paths = plan_paths(&peers, Duration::from_millis(40));
    let direct_copies = ProposerConfig::new([0x19; 32], node_id(1)).direct_copies;
    let outputs = solved_with_paths(&ids, &paths, 1_989_000, direct_copies);
    let mut holder_seeds = 0;
    for output in outputs {
        if let ProposerOutput::QueueSeeds { seeds, copies, .. } = output {
            assert!(
                copies.is_empty(),
                "the proposer queues no optional copies by default"
            );
            holder_seeds += seeds.iter().filter(|seed| seed.relay).count();
        }
    }
    assert!(
        holder_seeds > 0,
        "the proposer still sends relay-flagged seeds"
    );
}

/// 05 §3 "Small block, 50 neighbours": nearly every neighbour gets the whole
/// block from the proposer, by its own seeds and direct copies.
#[test]
fn decision19_small_block_reaches_nearly_every_neighbour_whole() {
    let local = node_id(1);
    let coding = Profile::W2.derive_coding(20_000, 2).unwrap();
    let peers: Vec<(NodeId, u64, u64)> = (2..52)
        .map(|b| (node_id(b), 50_000_000, 50_000_000))
        .collect();
    let paths = plan_paths(&peers, Duration::from_millis(50));
    let ids: Vec<NodeId> = peers.iter().map(|p| p.0).collect();
    let plan = seed_plan(&coding, local, &ids, &paths, true);
    let whole = seed_lists(&plan)
        .iter()
        .filter(|(_, pushed, other)| {
            // A stripe has at most `MAX_N` parts, so the count fits u32.
            (0..coding.stripes).all(|stripe| {
                pushed
                    .iter()
                    .chain(other)
                    .filter(|&&i| coding.split_index(i).unwrap().0 == stripe)
                    .count() as u32
                    >= coding.k
            })
        })
        .count();
    assert!(whole >= 45, "{whole} of 50 neighbours get the block whole");
}

/// decision-21: `transfer = max(t_last − t_sent − min_rtt, t_last − t_first)`;
/// `bw` is the windowed max of samples and `rate_in`, and one fast response
/// lifts the pull cap.
#[test]
fn decision21_rate_samples_set_bw_and_lift_the_pull_cap() {
    let t0 = Instant::now();
    let peer = node_id(40);
    let mut paths = PeerPaths::new();
    paths.observe(
        peer,
        PathEstimate {
            min_rtt: Some(Duration::from_millis(100)),
            rate_in: Some(100_000),
            rate_out: None,
        },
    );
    // 1 MB over 200 ms after one RTT: 1 MB / 100 ms = 10 MB/s.
    let sample = |at: Instant, bytes: u64| super::paths::RateSample {
        bytes,
        sent_at: at,
        first_at: at + Duration::from_millis(150),
        last_at: at + Duration::from_millis(200),
    };
    assert_eq!(paths.record_sample(peer, sample(t0, 1_000_000)), 10_000_000);
    let arrival = paths.pull_arrival(&peer, 0, 1, t0);
    assert_eq!(
        arrival.rate_bps, 10_000_000,
        "bw is the max of samples and rate_in"
    );
    // A lower sample does not lower the windowed max.
    assert_eq!(paths.record_sample(peer, sample(t0, 100_000)), 1_000_000);
    assert_eq!(paths.pull_arrival(&peer, 0, 1, t0).rate_bps, 10_000_000);
    // When the second term dominates: first and last 500 ms apart.
    let slow = super::paths::RateSample {
        bytes: 1_000_000,
        sent_at: t0,
        first_at: t0 + Duration::from_millis(100),
        last_at: t0 + Duration::from_millis(600),
    };
    assert_eq!(paths.record_sample(node_id(41), slow), 2_000_000);
    // Two windows later the old samples have expired; rate_in remains.
    let later = t0 + super::paths::RATE_WINDOW * 3;
    paths.record_sample(peer, sample(later, 10_000));
    assert_eq!(paths.pull_arrival(&peer, 0, 1, later).rate_bps, 100_000);
    // The session end drops the samples with the entry (bpv1-65).
    paths.remove(&peer);
    assert!(paths.get(&peer).is_none());

    // In the table, one fast response lifts the peer's cap.
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::W2));
    let block_id = block::Hash([0xF1; 32]);
    let coding = Profile::W2.derive_coding(1_950_000, 100).unwrap();
    let part = u64::from(coding.part_bytes);
    admit(&mut table, t0, block_id, coding, [0xF1; 32]);
    observe(&mut table, t0, peer, 100, part);
    let states = vec![pull_window(peer, 64)];
    let s = announce(
        &mut table,
        t0,
        peer,
        block_id,
        (0..coding.k).collect(),
        &states,
    );
    assert_eq!(granted_slots(&s, peer).len(), 2, "the 2-part floor");
    table.step(
        t0,
        BlockInput::RateSample {
            peer,
            sample: sample(t0, 10 * part),
        },
    );
    let s = release(
        &mut table,
        t0,
        peer,
        block_id,
        vec![0, 1],
        ReleaseReason::Refused,
        &states,
    );
    assert_eq!(
        granted_slots(&s, peer).len(),
        20,
        "2 x 100 parts/s x 100 ms after one fast response"
    );
    table.assert_invariants();
}

/// bpv1-28: a part outside the stripe's claims drops the excess claims, so
/// held + claims never exceeds `k`.
#[test]
fn unclaimed_arrival_drops_excess_claims() {
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let now = Instant::now();
    let block_id = block::Hash([0xC1; 32]);
    let encoded = encode_body(&Profile::TINY, &[0x5C; 128], 2).unwrap();
    let coding = encoded.coding;
    admit(&mut table, now, block_id, coding, encoded.part_root);
    let peers: Vec<NodeId> = (50..54).map(node_id).collect();
    let states: Vec<PullPeerState> = peers.iter().map(|&p| pull_state(p)).collect();
    let source: Vec<u32> = (0..coding.k).collect();
    let mut granted = 0;
    for &peer in &peers {
        let s = announce(&mut table, now, peer, block_id, source.clone(), &states);
        granted += peers
            .iter()
            .map(|&p| granted_slots(&s, p).len())
            .sum::<usize>();
    }
    // Padding parts are held from admission (decision-18).
    let padding = (0..coding.n).filter(|&i| coding.is_padding(i)).count();
    assert_eq!(
        (granted + padding) as u32,
        coding.k,
        "held padding and claims fill the stripe"
    );
    // A parity part nobody claimed arrives.
    let parity = coding.wire_index(0, coding.k).unwrap();
    insert(
        &mut table,
        now,
        block_id,
        &encoded.parts[parity as usize],
        node_id(60),
        PartAuth::Want(1),
    );
    table.assert_invariants();
}

/// decision-24: flagged pushes are pending claims held by the holder, so no
/// one is asked for them until they turn late; the late rule then releases
/// each once and names the holder.
#[test]
fn decision24_flagged_pushes_wait_for_late_then_release_once() {
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let now = Instant::now();
    let block_id = block::Hash([0xD5; 32]);
    let coding = Profile::TINY.derive_coding(128, 2).unwrap();
    admit(&mut table, now, block_id, coding, [0xD5; 32]);
    let holder = node_id(21);
    let other = node_id(22);
    let states = vec![pull_state(holder), pull_state(other)];
    // Two slow neighbours lift the median, so the pushes turn late (about
    // 20 ms) well before the stall interval starts repair (about 510 ms).
    for (peer, rtt) in [
        (holder, 10),
        (other, 10),
        (node_id(23), 500),
        (node_id(24), 500),
    ] {
        observe(&mut table, now, peer, rtt, 10_000_000);
    }
    flag_push(&mut table, now, holder, block_id, vec![0, 1], false);
    let s = announce(&mut table, now, other, block_id, vec![0, 1], &states);
    assert!(
        granted_slots(&s, other).is_empty(),
        "a flagged push is pulled before it is late"
    );
    table.assert_invariants();

    // Repair rounds re-plan on their own, so count grants until the first one.
    let mut late = Vec::new();
    let mut push_late = 0;
    let mut granted = Vec::new();
    for ms in (10..1_000).step_by(10) {
        let s = tick(&mut table, now + Duration::from_millis(ms), &states);
        if s.events
            .iter()
            .any(|e| matches!(e, BlockEvent::RepairRound { .. }))
        {
            break;
        }
        for e in &s.events {
            match e {
                BlockEvent::PullLate { peer, slot, .. } if *peer == holder => late.push(*slot),
                BlockEvent::PushLate { peer, .. } if *peer == holder => push_late += 1,
                _ => {}
            }
        }
        assert!(
            granted_slots(&s, holder).is_empty(),
            "the holder is never asked for a part it pushes"
        );
        granted.extend(granted_slots(&s, other));
        table.assert_invariants();
    }
    assert_eq!(late, vec![0, 1], "each push turns late once");
    assert_eq!(push_late, 1, "one PushLate per late holder");
    // The pull cap may hold the second slot until the first arrives.
    assert!(!granted.is_empty() && granted.iter().all(|s| *s <= 1));
    let mut once = granted.clone();
    once.sort_unstable();
    once.dedup();
    assert_eq!(once.len(), granted.len(), "a late push is granted once");
}

/// bpv1-64: each flagged push turns late at its own time. A holder that
/// keeps flagging new indices never delays the release of one it withholds.
#[test]
fn decision24_later_flags_never_delay_a_withheld_push() {
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let now = Instant::now();
    let block_id = block::Hash([0xDB; 32]);
    let coding = Profile::TINY.derive_coding(2_048, 2).unwrap();
    admit(&mut table, now, block_id, coding, [0xDB; 32]);
    let holder = node_id(21);
    let states = vec![pull_state(holder), pull_state(node_id(22))];
    // Slow peers keep the stall interval, and so repair, far off.
    for (peer, rtt) in [(holder, 10), (node_id(23), 500), (node_id(24), 500)] {
        observe(&mut table, now, peer, rtt, 10_000_000);
    }
    let flag = |table: &mut BlockTable, at: Instant, stripe: u32| {
        flag_push(
            table,
            at,
            holder,
            block_id,
            vec![coding.wire_index(stripe, 0).unwrap()],
            false,
        );
    };
    // The holder flags stripe 0's first part and withholds it, then flags
    // one more stripe every 10 ms.
    flag(&mut table, now, 0);
    let mut released_at = None;
    for (i, ms) in (10..300).step_by(10).enumerate() {
        let at = now + Duration::from_millis(ms);
        let stripe = 1 + i as u32 % (coding.stripes - 1);
        flag(&mut table, at, stripe);
        let s = tick(&mut table, at, &states);
        if s.events
            .iter()
            .any(|e| matches!(e, BlockEvent::RepairRound { .. }))
        {
            break;
        }
        if s.events.iter().any(|e| {
            matches!(e, BlockEvent::PullLate { peer, stripe: 0, slot: 0, .. } if *peer == holder)
        }) {
            released_at = Some(ms);
            break;
        }
        table.assert_invariants();
    }
    // Its estimate is about 10 ms, so it turns late near 20 ms.
    assert!(
        released_at.is_some_and(|ms| ms <= 50),
        "the withheld push turned late at {released_at:?} ms"
    );
}

/// bpv1-71: a pushed seed goes on from whichever source delivered it first:
/// the proposer, a push, a pull, or a checked stripe's regeneration. Any
/// other part is only announced.
#[test]
fn decision24_pushed_seeds_go_on_from_any_source() {
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let t0 = Instant::now();
    let block_id = block::Hash([0xD6; 32]);
    let encoded = encode_body(&Profile::TINY, &[0x46; 128], 2).unwrap();
    let coding = encoded.coding;
    let proposer = node_id(1);
    admit(&mut table, t0, block_id, coding, encoded.part_root);
    // Stripe 0's slots 2 and its last parity slot are this node's pushed seeds.
    let wire = |slot: u32| coding.wire_index(0, slot).unwrap();
    let parity = wire(coding.n - 1);
    flag_push(
        &mut table,
        t0,
        proposer,
        block_id,
        vec![wire(2), parity],
        true,
    );
    flag_push(&mut table, t0, proposer, block_id, vec![wire(1)], false);
    let insert_slot = |table: &mut BlockTable, slot: u32, from_peer: NodeId, auth: PartAuth| {
        insert(
            table,
            t0,
            block_id,
            &encoded.parts[wire(slot) as usize],
            from_peer,
            auth,
        )
    };
    let source_and_push = |s: &BlockStep| {
        let source = s.events.iter().find_map(|e| match e {
            BlockEvent::PartInserted { source, .. } => Some(*source),
            _ => None,
        });
        let push = s.outputs.iter().find_map(|o| match o {
            BlockOutput::PartReady { push, .. } => Some(*push),
            _ => None,
        });
        (source, push)
    };
    assert_eq!(
        source_and_push(&insert_slot(&mut table, 0, node_id(5), PartAuth::Want(1))),
        (Some(PartSource::Pull), Some(false))
    );
    assert_eq!(
        source_and_push(&insert_slot(&mut table, 1, proposer, PartAuth::Seed)),
        (Some(PartSource::Direct), Some(false))
    );
    // The pushed seed arrives by push from another holder before the
    // proposer's copy: it still goes on.
    assert_eq!(
        source_and_push(&insert_slot(&mut table, 2, node_id(5), PartAuth::Push)),
        (Some(PartSource::Push), Some(true))
    );
    // Slot 3 completes the stripe's `k` source parts and starts the check.
    let s = insert_slot(&mut table, 3, node_id(6), PartAuth::Want(2));
    assert_eq!(source_and_push(&s), (Some(PartSource::Pull), Some(false)));
    let work_id = s
        .outputs
        .iter()
        .find_map(|o| match o {
            BlockOutput::StartStripeJob { work_id, .. } => Some(*work_id),
            _ => None,
        })
        .expect("the stripe reached k");
    // The pushed parity seed never arrived; the check regenerates it.
    let checked = table.step(
        t0,
        BlockInput::StripeJobDone {
            work_id,
            block_id,
            stripe: 0,
            result: Ok((0..coding.n)
                .map(|slot| encoded.parts[wire(slot) as usize].clone())
                .collect()),
        },
    );
    let ready: Vec<(Vec<u32>, bool)> = checked
        .outputs
        .iter()
        .filter_map(|o| match o {
            BlockOutput::PartReady { indices, push, .. } => Some((indices.clone(), *push)),
            _ => None,
        })
        .collect();
    let regenerated: Vec<u32> = (coding.k..coding.n - 1).map(wire).collect();
    assert_eq!(ready, vec![(regenerated, false), (vec![parity], true)]);
    table.assert_invariants();

    // A pushed seed held before the proposer's lists arrive goes on then.
    let block_id = block::Hash([0xDD; 32]);
    admit(&mut table, t0, block_id, coding, encoded.part_root);
    let s = insert(
        &mut table,
        t0,
        block_id,
        &encoded.parts[0],
        node_id(5),
        PartAuth::Want(3),
    );
    assert_eq!(source_and_push(&s), (Some(PartSource::Pull), Some(false)));
    let listed = flag_push(&mut table, t0, proposer, block_id, vec![0], true);
    let ready: Vec<(Vec<u32>, bool)> = listed
        .outputs
        .iter()
        .filter_map(|o| match o {
            BlockOutput::PartReady { indices, push, .. } => Some((indices.clone(), *push)),
            _ => None,
        })
        .collect();
    assert_eq!(ready, vec![(vec![0], true)]);
    table.assert_invariants();
}

/// 05 §3 "A holder push crosses a pull response or a direct copy of the same
/// index": the second copy drops as surplus, nothing closes, and retirement
/// counts it for `part.duplicates`.
#[test]
fn decision19_push_crossing_a_direct_copy_drops_as_surplus() {
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let t0 = Instant::now();
    let block_id = block::Hash([0xD8; 32]);
    let encoded = encode_body(&Profile::TINY, &[0x47; 128], 2).unwrap();
    let proposer = node_id(1);
    admit(&mut table, t0, block_id, encoded.coding, encoded.part_root);
    let part_1 = |table: &mut BlockTable, from_peer: NodeId, auth: PartAuth| {
        insert(table, t0, block_id, &encoded.parts[1], from_peer, auth)
    };
    let copy = part_1(&mut table, proposer, PartAuth::Seed);
    assert!(copy.events.iter().any(|e| matches!(
        e,
        BlockEvent::PartInserted {
            first: true,
            source: PartSource::Direct,
            ..
        }
    )));
    for (from, auth) in [
        (node_id(5), PartAuth::Push),
        (node_id(6), PartAuth::Want(1)),
    ] {
        let again = part_1(&mut table, from, auth);
        assert!(again
            .events
            .iter()
            .any(|e| matches!(e, BlockEvent::PartInserted { first: false, .. })));
        assert!(!again.outputs.iter().any(|o| matches!(
            o,
            BlockOutput::ClosePeer { .. } | BlockOutput::PartReady { .. }
        )));
    }
    table.assert_invariants();
    let retired = table.step(
        t0,
        BlockInput::ParentAdmitted {
            hash: block::Hash([0xDA; 32]),
            height: 1 + PROPAGATION_DEPTH + 1,
        },
    );
    assert!(retired
        .events
        .contains(&BlockEvent::PartDuplicates { block_id, count: 2 }));
}

// Catch-up handoff between block sync and BPv1 (#2718). Each test names the
// property it checks:
//
// - bpv1-76: BPv1 never admits or submits a block more than `PROPAGATION_DEPTH`
//   above the verified tip.
// - bpv1-82: every `Verifying` block reaches a verdict or its verification
//   deadline, then frees its slot.
// - bpv1-44: for a block BPv1 admits, block sync fetches no body until BPv1
//   releases the claim by fallback, repair, retirement, or commit.
// - bpv1-57: when BPv1 fails a block, it releases the claim at the deadline of
//   the block's phase, so block sync fetches it within one fetch.
// - bpv1-83: in-window metadata whose parent is the verified tip is never
//   refused because slots hold blocks waiting on parents.
// - bpv1-84: a retryable verdict on a block whose parent is not committed keeps
//   the body, and BPv1 resubmits it once the parent commits. A resubmission's
//   parent is always committed, so only a block's first submission can wait in
//   the state's orphan queue.
//
// `service_tests` runs the handoff through the service, and `ports_tests` checks
// that block sync wakes when a claim expires and that no retryable verdict
// reports `Invalid`.

/// Applies the step's body-claim outputs to `claims` at `now`, as the IO shell's
/// `BlockFallbackPort` does, so a test reads block sync's view of each claim.
fn apply_claims(claims: &SharedBodyClaims, now: Instant, step: &BlockStep) {
    for output in &step.outputs {
        match output {
            BlockOutput::ClaimBody { block_id, deadline } => {
                claims.claim_at(*block_id, *deadline, now);
            }
            BlockOutput::ExtendBodyClaim { block_id, deadline } => {
                claims.extend_at(*block_id, *deadline, now);
            }
            BlockOutput::ReleaseBodyClaim { block_id, reason } => {
                BlockFallbackPort::release(claims, *block_id, *reason);
            }
            _ => {}
        }
    }
}

/// The work ids of the step's verifier submissions.
fn submissions(step: &BlockStep) -> Vec<WorkId> {
    step.outputs
        .iter()
        .filter_map(|o| match o {
            BlockOutput::SubmitVerifier { work_id, .. } => Some(*work_id),
            _ => None,
        })
        .collect()
}

/// Offers metadata for `block_id` at `height` from its proposer `node_id(1)`,
/// as the shell would, and returns the last step. The block is admitted iff
/// that step holds `BlockEvent::BlockAdmitted`.
fn offer(
    table: &mut BlockTable,
    now: Instant,
    block_id: block::Hash,
    height: u32,
    coding: Coding,
    part_root: [u8; 32],
) -> BlockStep {
    offer_child(
        table,
        now,
        block_id,
        height,
        TEST_TIP_HASH,
        coding,
        part_root,
    )
}

/// [`offer`] for a header whose parent is `parent_hash`.
fn offer_child(
    table: &mut BlockTable,
    now: Instant,
    block_id: block::Hash,
    height: u32,
    parent_hash: block::Hash,
    coding: Coding,
    part_root: [u8; 32],
) -> BlockStep {
    let received = table.step(
        now,
        BlockInput::MetaReceived {
            envelope: make_envelope(block_id, coding, part_root, node_id(1)),
            from_peer: node_id(1),
            session_gen: 1,
        },
    );
    let admitted = header_result(
        table,
        now,
        &received,
        HeaderAdmissionResult::Admitted {
            height,
            parent_hash,
        },
    );
    let Some(work_id) = admitted.outputs.iter().find_map(|o| match o {
        BlockOutput::VerifyCommitment { work_id, .. } => Some(*work_id),
        _ => None,
    }) else {
        return admitted;
    };
    table.step(
        now,
        BlockInput::CommitmentChecked {
            work_id,
            ok: true,
            peers: Vec::new(),
            rng_seed: 7,
        },
    )
}

fn was_admitted(step: &BlockStep, block_id: block::Hash) -> bool {
    step.events
        .contains(&BlockEvent::BlockAdmitted { block_id })
}

/// bpv1-44: assembly extends the body claim to the verification deadline, so a
/// block that assembles just before its reconstruction deadline stays claimed
/// while it verifies. The commit releases the claim.
#[test]
fn bpv1_44_the_body_claim_covers_verification() {
    let t0 = Instant::now();
    let claims = SharedBodyClaims::new();
    let encoded = encode_body(&Profile::TINY, &[0x55; 128], 2).unwrap();
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let block_id = block::Hash([0xA1; 32]);
    table.step(
        t0,
        BlockInput::TipHeight {
            height: 9,
            hash: TEST_TIP_HASH,
        },
    );
    let admitted = admit_block(
        &mut table,
        t0,
        block_id,
        10,
        encoded.coding,
        encoded.part_root,
    );
    apply_claims(&claims, t0, &admitted);
    assert_eq!(claims.next_expiry(), Some(t0 + RECONSTRUCTION_DEADLINE));

    let assembled_at = t0 + RECONSTRUCTION_DEADLINE - Duration::from_secs(1);
    let mut work = Vec::new();
    for part in &encoded.parts[..encoded.coding.k as usize] {
        let step = insert(
            &mut table,
            assembled_at,
            block_id,
            part,
            node_id(2),
            PartAuth::Want(1),
        );
        apply_claims(&claims, assembled_at, &step);
        work.extend(submissions(&step));
    }
    let [work_id] = work[..] else {
        panic!("one submission, got {work:?}");
    };
    assert_eq!(table.block_phase(&block_id), Some(BlockPhase::Verifying));
    assert_eq!(claims.next_expiry(), Some(assembled_at + VERIFY_DEADLINE));

    // Past the reconstruction deadline, the verifying block keeps its claim, so
    // block sync still skips it.
    let later = t0 + RECONSTRUCTION_DEADLINE + Duration::from_secs(30);
    let ticked = tick(&mut table, later, &[]);
    apply_claims(&claims, later, &ticked);
    assert!(!ticked
        .events
        .iter()
        .any(|e| matches!(e, BlockEvent::FallbackStarted { .. })));
    assert!(claims.is_claimed(block_id, later));

    let committed = table.step(
        later,
        BlockInput::VerifierVerdict {
            work_id,
            block_id,
            verdict: VerifierVerdict::Valid,
        },
    );
    apply_claims(&claims, later, &committed);
    assert!(!claims.is_claimed(block_id, later));
    assert_eq!(table.block_phase(&block_id), Some(BlockPhase::Done));
    table.assert_invariants();
}

/// bpv1-76 and bpv1-82 during catch-up: the best header runs 20 blocks ahead of
/// the verified tip. BPv1 refuses the tip block, and four blocks inside both
/// windows wait on a verifier that never answers. At the verification deadline each
/// falls back, releases its claim, and frees its slot, and a late verdict
/// changes nothing.
#[test]
fn bpv1_82_verification_ends_by_its_deadline_and_frees_the_slot() {
    let t0 = Instant::now();
    let claims = SharedBodyClaims::new();
    let empty = encode_body(&Profile::TINY, &[], 1).unwrap();
    let (coding, root) = (empty.coding, empty.part_root);
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let verified = 100;
    table.step(
        t0,
        BlockInput::TipHeight {
            height: verified,
            hash: TEST_TIP_HASH,
        },
    );
    table.step(
        t0,
        BlockInput::ParentAdmitted {
            hash: block::Hash([0xB0; 32]),
            height: verified + 20,
        },
    );

    let tip = block::Hash([0xB1; 32]);
    let refused = offer(&mut table, t0, tip, verified + 20, coding, root);
    assert!(!was_admitted(&refused, tip), "bpv1-76: block sync owns it");

    // Each empty body assembles and submits at admission.
    let mut work = Vec::new();
    for i in 0..4u8 {
        let block_id = block::Hash([0xB2 + i; 32]);
        let step = offer(
            &mut table,
            t0,
            block_id,
            verified + 5 + u32::from(i),
            coding,
            root,
        );
        assert!(was_admitted(&step, block_id));
        apply_claims(&claims, t0, &step);
        work.extend(submissions(&step).into_iter().map(|w| (w, block_id)));
    }
    assert_eq!(work.len(), 4, "one submission per block");
    let blocked = block::Hash([0xBF; 32]);
    let full = offer(&mut table, t0, blocked, verified + 9, coding, root);
    assert!(!was_admitted(&full, blocked), "every slot is verifying");
    assert_eq!(table.next_deadline(), Some(t0 + VERIFY_DEADLINE));

    let before = tick(
        &mut table,
        t0 + VERIFY_DEADLINE - Duration::from_millis(1),
        &[],
    );
    assert!(before.events.is_empty() && before.outputs.is_empty());

    let at = t0 + VERIFY_DEADLINE;
    let expired = tick(&mut table, at, &[]);
    apply_claims(&claims, at, &expired);
    for &(_, block_id) in &work {
        assert!(expired.events.contains(&BlockEvent::FallbackStarted {
            block_id,
            reason: FallbackReason::VerifyDeadline,
        }));
        assert!(
            !claims.is_claimed(block_id, at),
            "bpv1-57: block sync fetches it"
        );
        assert_eq!(table.block_phase(&block_id), Some(BlockPhase::Fallback));
    }

    // bpv1-63: the verifier answering late changes nothing.
    let (late_work, late_block) = work[0];
    let late = table.step(
        at,
        BlockInput::VerifierVerdict {
            work_id: late_work,
            block_id: late_block,
            verdict: VerifierVerdict::Valid,
        },
    );
    assert!(late.events.is_empty() && late.outputs.is_empty());
    assert_eq!(table.block_phase(&late_block), Some(BlockPhase::Fallback));

    let freed = offer(&mut table, at, blocked, verified + 9, coding, root);
    assert!(
        was_admitted(&freed, blocked),
        "a fallback block yields its slot"
    );
    table.assert_invariants();
}

/// bpv1-83: with every slot verifying blocks that wait on their parents, the
/// block whose parent is the verified tip retires the highest of them. Other
/// metadata displaces nothing.
#[test]
fn bpv1_83_the_next_block_displaces_a_block_waiting_on_its_parent() {
    let t0 = Instant::now();
    let empty = encode_body(&Profile::TINY, &[], 1).unwrap();
    let (coding, root) = (empty.coding, empty.part_root);
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let verified = 100;
    table.step(
        t0,
        BlockInput::TipHeight {
            height: verified,
            hash: TEST_TIP_HASH,
        },
    );
    for i in 0..4u8 {
        let block_id = block::Hash([0xC2 + i; 32]);
        let step = offer(
            &mut table,
            t0,
            block_id,
            verified + 2 + u32::from(i),
            coding,
            root,
        );
        assert!(was_admitted(&step, block_id));
    }
    let highest = block::Hash([0xC5; 32]);

    let other = block::Hash([0xCE; 32]);
    let refused = offer(&mut table, t0, other, verified + 3, coding, root);
    assert!(!was_admitted(&refused, other));

    let next = block::Hash([0xC1; 32]);
    let step = offer(&mut table, t0, next, verified + 1, coding, root);
    assert!(was_admitted(&step, next));
    assert!(step
        .events
        .contains(&BlockEvent::Retired { block_id: highest }));
    assert!(step.outputs.iter().any(|o| matches!(
        o,
        BlockOutput::ReleaseBodyClaim { block_id, reason: ClaimRelease::Retired }
            if *block_id == highest
    )));
    assert_eq!(
        submissions(&step).len(),
        1,
        "the next block verifies at once"
    );
    table.assert_invariants();
}

/// bpv1-83 bounds what competing metadata at the next height can evict. Each
/// sibling retires one block above that height, a retired block never takes a
/// slot again, and siblings never displace each other.
#[test]
fn bpv1_83_siblings_of_the_next_block_evict_each_block_above_it_once() {
    let t0 = Instant::now();
    let empty = encode_body(&Profile::TINY, &[], 1).unwrap();
    let (coding, root) = (empty.coding, empty.part_root);
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let verified = 100;
    table.step(
        t0,
        BlockInput::TipHeight {
            height: verified,
            hash: TEST_TIP_HASH,
        },
    );
    let above: Vec<block::Hash> = (0..4u8).map(|i| block::Hash([0xF2 + i; 32])).collect();
    for (i, &block_id) in above.iter().enumerate() {
        let height = verified + 2 + u32::try_from(i).unwrap();
        assert!(was_admitted(
            &offer(&mut table, t0, block_id, height, coding, root),
            block_id
        ));
    }

    let mut retired = Vec::new();
    for i in 0..4u8 {
        let sibling = block::Hash([0xE0 + i; 32]);
        let step = offer(&mut table, t0, sibling, verified + 1, coding, root);
        assert!(was_admitted(&step, sibling));
        retired.extend(step.events.iter().filter_map(|e| match e {
            BlockEvent::Retired { block_id } => Some(*block_id),
            _ => None,
        }));
    }
    let highest_first: Vec<block::Hash> = above.iter().rev().copied().collect();
    assert_eq!(
        retired, highest_first,
        "each block above, highest first, once"
    );

    let fifth = block::Hash([0xE9; 32]);
    let refused = offer(&mut table, t0, fifth, verified + 1, coding, root);
    assert!(
        !was_admitted(&refused, fifth),
        "a sibling displaces no sibling"
    );
    assert!(!refused
        .events
        .iter()
        .any(|e| matches!(e, BlockEvent::Retired { .. })));

    // A retired block's metadata starts no admission, so it cannot thrash.
    let again = table.step(
        t0,
        BlockInput::MetaReceived {
            envelope: make_envelope(above[3], coding, root, node_id(1)),
            from_peer: node_id(1),
            session_gen: 1,
        },
    );
    assert!(again.outputs.is_empty() && again.events.is_empty());
    assert_eq!(table.block_phase(&above[3]), Some(BlockPhase::Retired));
    table.assert_invariants();
}

/// bpv1-84: a retryable verdict on a block whose parent is not committed keeps
/// the assembled body and the claim. BPv1 resubmits the same body once the
/// verified tip reaches the parent, and not before. A retryable verdict with the
/// parent committed falls back.
#[test]
fn bpv1_84_a_retryable_verdict_keeps_the_body_until_the_parent_commits() {
    let t0 = Instant::now();
    let claims = SharedBodyClaims::new();
    let encoded = encode_body(&Profile::TINY, &[0x66; 128], 2).unwrap();
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let block_id = block::Hash([0xD7; 32]);
    let verified = 100;
    table.step(
        t0,
        BlockInput::TipHeight {
            height: verified,
            hash: TEST_TIP_HASH,
        },
    );
    let admitted = admit_block(
        &mut table,
        t0,
        block_id,
        verified + 3,
        encoded.coding,
        encoded.part_root,
    );
    apply_claims(&claims, t0, &admitted);
    let mut sources = Vec::new();
    let mut first = None;
    for part in &encoded.parts[..encoded.coding.k as usize] {
        let step = insert(
            &mut table,
            t0,
            block_id,
            part,
            node_id(2),
            PartAuth::Want(1),
        );
        apply_claims(&claims, t0, &step);
        for o in step.outputs {
            if let BlockOutput::SubmitVerifier {
                work_id,
                stripe_sources,
                ..
            } = o
            {
                first = Some(work_id);
                sources.push(stripe_sources);
            }
        }
    }
    let first = first.expect("k source parts assemble the block");

    let parked = table.step(
        t0,
        BlockInput::VerifierVerdict {
            work_id: first,
            block_id,
            verdict: VerifierVerdict::LocalCapacity,
        },
    );
    apply_claims(&claims, t0, &parked);
    assert!(parked.events.contains(&BlockEvent::Parked { block_id }));
    assert!(parked.outputs.is_empty(), "no fallback and no release");
    assert_eq!(table.block_phase(&block_id), Some(BlockPhase::Verifying));
    assert!(claims.is_claimed(block_id, t0));
    table.assert_invariants();

    let grandparent = table.step(
        t0,
        BlockInput::TipHeight {
            height: verified + 1,
            hash: TEST_TIP_HASH,
        },
    );
    assert!(
        submissions(&grandparent).is_empty(),
        "the parent is not committed"
    );

    let parent = table.step(
        t0,
        BlockInput::TipHeight {
            height: verified + 2,
            hash: TEST_TIP_HASH,
        },
    );
    let mut resubmitted = None;
    for o in parent.outputs {
        if let BlockOutput::SubmitVerifier {
            work_id,
            stripe_sources,
            ..
        } = o
        {
            assert!(resubmitted.is_none(), "one resubmission");
            resubmitted = Some(work_id);
            sources.push(stripe_sources);
        }
    }
    let resubmitted = resubmitted.expect("the committed parent resubmits the block");
    assert_eq!(sources[0], sources[1], "BPv1 resubmits the body it kept");

    // The parent is committed now, so another retryable verdict falls back.
    let fallen = table.step(
        t0,
        BlockInput::VerifierVerdict {
            work_id: resubmitted,
            block_id,
            verdict: VerifierVerdict::LocalCapacity,
        },
    );
    apply_claims(&claims, t0, &fallen);
    assert!(fallen.events.contains(&BlockEvent::FallbackStarted {
        block_id,
        reason: FallbackReason::LocalCapacity,
    }));
    assert!(!claims.is_claimed(block_id, t0));
    table.assert_invariants();
}

/// bpv1-82: a parked block whose parent never commits falls back at its
/// verification deadline.
#[test]
fn bpv1_82_a_parked_block_falls_back_at_its_verification_deadline() {
    let t0 = Instant::now();
    let empty = encode_body(&Profile::TINY, &[], 1).unwrap();
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    table.step(
        t0,
        BlockInput::TipHeight {
            height: 100,
            hash: TEST_TIP_HASH,
        },
    );
    let block_id = block::Hash([0xE7; 32]);
    let step = offer(&mut table, t0, block_id, 105, empty.coding, empty.part_root);
    let [work_id] = submissions(&step)[..] else {
        panic!("an empty body submits at admission");
    };
    table.step(
        t0,
        BlockInput::VerifierVerdict {
            work_id,
            block_id,
            verdict: VerifierVerdict::LocalCapacity,
        },
    );
    assert_eq!(table.next_deadline(), Some(t0 + VERIFY_DEADLINE));
    let expired = tick(&mut table, t0 + VERIFY_DEADLINE, &[]);
    assert!(expired.events.contains(&BlockEvent::FallbackStarted {
        block_id,
        reason: FallbackReason::VerifyDeadline,
    }));
    assert!(expired.outputs.iter().any(|o| matches!(
        o,
        BlockOutput::ReleaseBodyClaim {
            reason: ClaimRelease::Fallback,
            ..
        }
    )));
    // A late tip move resubmits nothing.
    let moved = table.step(
        t0 + VERIFY_DEADLINE,
        BlockInput::TipHeight {
            height: 104,
            hash: TEST_TIP_HASH,
        },
    );
    assert!(submissions(&moved).is_empty());
    table.assert_invariants();
}

/// bpv1-44: a block that assembles after its claim expired, before the tick
/// that falls it back, claims nothing again. Block sync may already fetch the
/// body, so the verification claim only extends a live claim.
#[test]
fn bpv1_44_a_late_assembly_does_not_reclaim_an_expired_body() {
    let t0 = Instant::now();
    let claims = SharedBodyClaims::new();
    let encoded = encode_body(&Profile::TINY, &[0x57; 128], 2).unwrap();
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let block_id = block::Hash([0xA7; 32]);
    table.step(
        t0,
        BlockInput::TipHeight {
            height: 9,
            hash: TEST_TIP_HASH,
        },
    );
    let admitted = admit_block(
        &mut table,
        t0,
        block_id,
        10,
        encoded.coding,
        encoded.part_root,
    );
    apply_claims(&claims, t0, &admitted);

    let late = t0 + RECONSTRUCTION_DEADLINE;
    let mut work = Vec::new();
    for part in &encoded.parts[..encoded.coding.k as usize] {
        let step = insert(
            &mut table,
            late,
            block_id,
            part,
            node_id(2),
            PartAuth::Want(1),
        );
        apply_claims(&claims, late, &step);
        work.extend(submissions(&step));
    }
    assert_eq!(work.len(), 1, "the late body still verifies");
    assert!(
        !claims.is_claimed(block_id, late),
        "the expired claim stays expired"
    );
    table.assert_invariants();
}

/// bpv1-83: metadata one above the verified tip on another branch is not the
/// next block, so it displaces nothing. Headerchain admits retained forks.
#[test]
fn bpv1_83_a_fork_block_at_the_next_height_displaces_nothing() {
    let t0 = Instant::now();
    let empty = encode_body(&Profile::TINY, &[], 1).unwrap();
    let (coding, root) = (empty.coding, empty.part_root);
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    let verified = 100;
    table.step(
        t0,
        BlockInput::TipHeight {
            height: verified,
            hash: TEST_TIP_HASH,
        },
    );
    for i in 0..4u8 {
        let block_id = block::Hash([0x92 + i; 32]);
        let height = verified + 2 + u32::from(i);
        assert!(was_admitted(
            &offer(&mut table, t0, block_id, height, coding, root),
            block_id
        ));
    }

    let fork = block::Hash([0x9E; 32]);
    let other_parent = block::Hash([0x9F; 32]);
    let step = offer_child(
        &mut table,
        t0,
        fork,
        verified + 1,
        other_parent,
        coding,
        root,
    );
    assert!(!was_admitted(&step, fork));
    assert!(!step
        .events
        .iter()
        .any(|e| matches!(e, BlockEvent::Retired { .. })));

    let next = block::Hash([0x91; 32]);
    let step = offer(&mut table, t0, next, verified + 1, coding, root);
    assert!(was_admitted(&step, next), "the tip's child still displaces");
    table.assert_invariants();
}

/// bpv1-84: a parked block whose height the verified tip reaches on another
/// branch is not resubmitted. Its parent is not the tip, so it falls back to
/// block sync.
#[test]
fn bpv1_84_a_tip_on_another_branch_falls_back_a_parked_block() {
    let t0 = Instant::now();
    let empty = encode_body(&Profile::TINY, &[], 1).unwrap();
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::TINY));
    table.step(
        t0,
        BlockInput::TipHeight {
            height: 100,
            hash: TEST_TIP_HASH,
        },
    );
    let block_id = block::Hash([0xE8; 32]);
    let step = offer(&mut table, t0, block_id, 103, empty.coding, empty.part_root);
    let [work_id] = submissions(&step)[..] else {
        panic!("an empty body submits at admission");
    };
    let parked = table.step(
        t0,
        BlockInput::VerifierVerdict {
            work_id,
            block_id,
            verdict: VerifierVerdict::LocalCapacity,
        },
    );
    assert!(parked.events.contains(&BlockEvent::Parked { block_id }));

    let fork_tip = table.step(
        t0,
        BlockInput::TipHeight {
            height: 102,
            hash: block::Hash([0xEF; 32]),
        },
    );
    assert!(submissions(&fork_tip).is_empty());
    assert!(fork_tip.events.contains(&BlockEvent::FallbackStarted {
        block_id,
        reason: FallbackReason::LocalCapacity,
    }));
    table.assert_invariants();
}
