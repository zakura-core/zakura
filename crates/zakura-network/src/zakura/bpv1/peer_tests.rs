//! Stack-2 (`bpv1-peer`) tests: level-4 proptest sequences, level-5 named
//! scenarios, and the tool-3 two-peer link harness (honest pairs + tool-6
//! adversaries). The tool-7 faults run as external mut-4 source mutations.

use std::time::Duration;

use proptest::prelude::*;
use zakura_chain::block;
use zakura_quic::NodeId;
use zakura_rt::time::Instant;

use super::{
    blocks::{BlockConfig, BlockEvent, BlockInput, PartSource, ReleaseReason},
    close::Bpv1CloseRow,
    codec::encode_body,
    commitment::CoinbaseBinding,
    peer::*,
    profile::{Coding, Profile},
    test_support::{
        make_envelope, node_id, part_frame, LinkProfile, MachineMesh, MESH_HEIGHT, TEST_TIP_HASH,
    },
    tree::VerifiedPart,
    wire::{
        encode_part_frame, Bpv1Message, HaveFlags, HaveForm, WantBitmap, WantEndReason,
        MAX_WANTS_INFLIGHT, PROPAGATION_DEPTH,
    },
};

/// Whether `step` queues a `HeaderMeta` to the peer.
fn queues_meta(step: &PeerStep) -> bool {
    step.outputs.iter().any(|o| {
        matches!(o, PeerOutput::SendControl(msg) if matches!(**msg, Bpv1Message::HeaderMeta { .. }))
    })
}

pub(super) fn dummy_meta(block_id: block::Hash) -> Box<Bpv1Message> {
    Box::new(Bpv1Message::HeaderMeta {
        header: block_id.0.to_vec(),
        binding: CoinbaseBinding {
            coinbase: vec![0x01],
            txid_siblings: Vec::new(),
            auth_siblings: Vec::new(),
            light_client_root: [0u8; 32],
        },
        coding: tiny(),
        part_root: block_id.0,
        proposer_node: node_id(1),
    })
}

/// A TINY 64-byte block's `HeaderMeta` from `proposer`.
fn proposer_meta(block_id: block::Hash, proposer: NodeId) -> Box<Bpv1Message> {
    let mut meta = dummy_meta(block_id);
    if let Bpv1Message::HeaderMeta { proposer_node, .. } = meta.as_mut() {
        *proposer_node = proposer;
    }
    meta
}

/// The peer's `HeaderMeta` for `block_id`.
pub(super) fn recv_meta(block_id: block::Hash) -> PeerInput {
    PeerInput::RecvHeaderMeta {
        block_id,
        message: dummy_meta(block_id),
    }
}

/// An inbound `Have` of `indices` with `flags`.
pub(super) fn have_with(block_id: block::Hash, flags: HaveFlags, indices: Vec<u32>) -> PeerInput {
    PeerInput::RecvHave(RecvHave {
        block_id,
        flags,
        form: HaveForm::List,
        frame_bytes: 33 + HaveForm::List.indices_len(indices.len()),
        indices,
    })
}

/// The coding of the 64-byte TINY block most tests admit.
pub(super) fn tiny() -> Coding {
    Profile::TINY.derive_coding(64, 2).unwrap()
}

/// The admission of `block_id`, which `proposer` proposed on another node.
/// This node queues `HeaderMeta` to the peer.
fn block_admitted(block_id: block::Hash, coding: Coding, proposer: NodeId) -> BlockAdmitted {
    BlockAdmitted {
        block_id,
        coding,
        proposer_node: proposer,
        header_meta: Some(dummy_meta(block_id)),
        as_proposer: false,
        have_held_until: None,
    }
}

pub(super) fn admitted(block_id: block::Hash, coding: Coding, proposer: NodeId) -> PeerInput {
    PeerInput::BlockAdmitted(block_admitted(block_id, coding, proposer))
}

/// The admission of node 1's own block.
pub(super) fn proposing(block_id: block::Hash, coding: Coding) -> PeerInput {
    PeerInput::BlockAdmitted(BlockAdmitted {
        as_proposer: true,
        ..block_admitted(block_id, coding, node_id(1))
    })
}

/// A TINY session from node 1 to `remote` that stepped `admission`.
pub(super) fn session(remote: NodeId, now: Instant, admission: PeerInput) -> PeerState {
    let mut peer = PeerState::new(PeerConfig::for_profile(Profile::TINY), remote, 1);
    peer.step(now, admission);
    peer
}

/// A session from node 1 to node 2 with the 64-byte TINY block admitted;
/// node 3 proposed it.
pub(super) fn tiny_session(block_id: block::Hash, now: Instant) -> PeerState {
    session(node_id(2), now, admitted(block_id, tiny(), node_id(3)))
}

pub(super) fn bitmap(slots: &[u32]) -> WantBitmap {
    let mut bm = WantBitmap::EMPTY;
    for &slot in slots {
        bm.set(slot);
    }
    bm
}

/// The block table's grant of `slots` of `stripe`, due 50 ms after `now`.
pub(super) fn pull(block_id: block::Hash, stripe: u32, slots: &[u32], now: Instant) -> PeerInput {
    PeerInput::PullGranted(PullGranted {
        block_id,
        stripe,
        requested: bitmap(slots),
        arrival: super::paths::Arrival {
            at: now + Duration::from_millis(50),
            rtt: Duration::from_millis(20),
            rate_bps: 1_000_000,
        },
    })
}

/// The peer's `Want` for `slots` of `stripe`.
pub(super) fn recv_want(
    want_id: u32,
    block_id: block::Hash,
    stripe: u32,
    slots: &[u32],
) -> PeerInput {
    PeerInput::RecvWant(RecvWant {
        want_id,
        block_id,
        stripe,
        requested: bitmap(slots),
    })
}

fn ready(block_id: block::Hash, indices: Vec<u32>, push: bool) -> PeerInput {
    PeerInput::Ready {
        block_id,
        indices,
        push,
    }
}

/// The id of the `Want` that `step` sends, if any.
fn sent_want(step: &PeerStep) -> Option<u32> {
    step.outputs.iter().find_map(|o| match o {
        PeerOutput::SendControl(msg) => match **msg {
            Bpv1Message::Want { want_id, .. } => Some(want_id),
            _ => None,
        },
        _ => None,
    })
}

/// The token of the proof job that `step` starts, if any.
pub(super) fn verify_token(step: &PeerStep) -> Option<PartToken> {
    step.outputs.iter().find_map(|o| match o {
        PeerOutput::VerifyPart { token, .. } => Some(*token),
        _ => None,
    })
}

#[track_caller]
pub(super) fn assert_closed(peer: &PeerState, row: Bpv1CloseRow) {
    assert_eq!(peer.close_cause().map(|c| c.row), Some(row));
}

#[test]
fn scenario_both_peers_send_same_header_simultaneously() {
    let config = PeerConfig::for_profile(Profile::TINY);
    let now = Instant::now();
    let mut a = PeerState::new(config, node_id(2), 1);
    let mut b = PeerState::new(config, node_id(1), 1);
    let block_id = block::Hash([0xAA; 32]);

    // Both A and B admit the block independently and queue HeaderMeta before
    // receiving each other's HeaderMeta.
    assert!(queues_meta(
        &a.step(now, admitted(block_id, tiny(), node_id(3)))
    ));
    assert!(queues_meta(
        &b.step(now, admitted(block_id, tiny(), node_id(3)))
    ));

    // Both receive each other's HeaderMeta, and nothing closes.
    let ra = a.step(now, recv_meta(block_id));
    let rb = b.step(now, recv_meta(block_id));
    assert!(!a.is_closed());
    assert!(!b.is_closed());
    assert!(ra.events.contains(&PeerEvent::MetaReceived(block_id)));
    assert!(rb.events.contains(&PeerEvent::MetaReceived(block_id)));

    // A second HeaderMeta from B closes A with bpv1-close-5.
    let dup = a.step(now, recv_meta(block_id));
    assert_closed(&a, Bpv1CloseRow::Row5DuplicateHeaderMeta);
    assert!(dup
        .outputs
        .iter()
        .any(|o| matches!(o, PeerOutput::CloseProtocol(c) if c.row == Bpv1CloseRow::Row5DuplicateHeaderMeta)));
}

#[test]
fn scenario_part_after_want_end_closes_14_and_after_block_done_drops_surplus() {
    let now = Instant::now();
    let block_id = block::Hash([0x22; 32]);
    let mut peer = tiny_session(block_id, now);

    // Issue Want for slots 0 and 1.
    let want_id = sent_want(&peer.step(now, pull(block_id, 0, &[0, 1], now)))
        .expect("a granted pull sends a Want");

    // Local node finishes block and sends BlockDone; part 0 still arrives under live Want -> Drop(AfterStop).
    peer.step(now, PeerInput::LocalBlockDone { block_id });
    let late_valid = peer.step(now, tiny_part_prefix(block_id, 0));
    assert!(!peer.is_closed());
    assert!(late_valid.events.contains(&PeerEvent::PartDropped {
        block_id,
        index: 0,
        reason: DropReason::AfterStop,
    }));

    // WantEnd arrives with sent=1 (matching attributed=1).
    peer.step(
        now,
        PeerInput::RecvWantEnd(RecvWantEnd { want_id, sent: 1 }),
    );
    assert!(!peer.is_closed());
    assert_eq!(peer.live_wants_out(), 0);

    // Part 1 arrives after its WantEnd and closes 14.
    peer.step(now, tiny_part_prefix(block_id, 1));
    assert_closed(&peer, Bpv1CloseRow::Row14UnwantedOrDuplicatePart);
}

/// bpv1-58: a peer that stops reading control gets the repeat-stall close,
/// with no protocol close row, and its pull claims release at session end.
#[test]
fn control_stall_closes_neutrally_and_releases_claims() {
    let t0 = Instant::now();
    let block_id = block::Hash([0x34; 32]);
    let mut peer = tiny_session(block_id, t0);
    peer.step(t0, pull(block_id, 0, &[0], t0));

    let closed = peer.step(t0, PeerInput::ControlStalled);
    assert!(closed.outputs.contains(&PeerOutput::CloseNeutralStall));
    assert!(peer.is_closed_neutral());
    assert_eq!(peer.close_cause(), None);
    let ended = peer.step(t0, PeerInput::SessionEnd);
    assert!(ended
        .outputs
        .iter()
        .any(|o| matches!(o, PeerOutput::ReleaseClaims { .. })));
}

#[test]
fn scenario_unanswered_want_deadline_park_and_repeat_stall_close() {
    let t0 = Instant::now();
    let block_id = block::Hash([0x33; 32]);
    let mut peer = tiny_session(block_id, t0);
    peer.step(t0, pull(block_id, 0, &[0], t0));
    assert_eq!(peer.window_remaining(), PULL_WINDOW - 1);

    // 1. Late pull at SLOW_X * 50 ms = 100 ms: releases claim, no park/close (bpv1-64).
    let late = peer.step(t0 + Duration::from_millis(100), PeerInput::Tick);
    assert!(late
        .events
        .iter()
        .any(|e| matches!(e, PeerEvent::WantLate { released: 1, .. })));
    assert_eq!(peer.window_remaining(), PULL_WINDOW - 1);
    assert_eq!(peer.live_wants_out(), 1, "wire reservation stays live");
    assert!(!peer.is_parked(t0 + Duration::from_millis(100)));

    // 2. Hard deadline at ~8.001 s: releases claim; the wire reservation and its
    //    window share stay live (bpv1-31).
    let deadline = peer.step(t0 + Duration::from_secs(9), PeerInput::Tick);
    assert!(deadline
        .events
        .iter()
        .any(|e| matches!(e, PeerEvent::WantDeadline { released: 1, .. })));
    assert_eq!(peer.window_remaining(), PULL_WINDOW - 1);
    assert_eq!(peer.live_wants_out(), 1, "wire reservation stays live");
    assert!(!peer.is_closed());

    // 3. First liveness timeout (~32.004 s): parks session without closing (bpv1-58).
    let t_park = t0 + Duration::from_secs(34);
    let parked = peer.step(t_park, PeerInput::Tick);
    assert!(parked.events.contains(&PeerEvent::Parked));
    assert!(peer.is_parked(t_park));
    assert!(!peer.is_closed());

    // 4. Cooldown expires -> unparks.
    let t_unpark = t_park + LIVENESS_COOLDOWN;
    let unparked = peer.step(t_unpark, PeerInput::Tick);
    assert!(unparked.events.contains(&PeerEvent::Unparked));
    assert!(!peer.is_parked(t_unpark));

    // 5. A repeat stall with no progress closes the connection neutrally,
    //    with no protocol close row.
    let t_second = t_unpark + Duration::from_secs(34);
    let closed = peer.step(t_second, PeerInput::Tick);
    assert!(closed.outputs.contains(&PeerOutput::CloseNeutralStall));
    assert!(peer.is_closed_neutral());
    assert_eq!(
        peer.close_cause(),
        None,
        "stall close is neutral, not a protocol fault"
    );
}

#[test]
fn scenario_early_record_full_pauses_data_only_and_replays_on_admission() {
    let coding = tiny();
    let block_id = block::Hash([0x44; 32]);
    let mut config = PeerConfig::for_profile(Profile::TINY);
    let flag = have_with(block_id, HaveFlags::Push, vec![0, 1]);
    let PeerInput::RecvHave(RecvHave { frame_bytes, .. }) = &flag else {
        unreachable!("have_with builds a Have");
    };
    // The record holds exactly the seeds' flagged `Have` and two part frames.
    config.early_peer_bytes_cap =
        frame_bytes + part_frame(&coding, 0).len() + part_frame(&coding, 1).len();
    let mut peer = PeerState::new(config, node_id(2), 1);
    let t0 = Instant::now();

    // Proposer's HeaderMeta delayed on control so its flagged `Have` and
    // seed parts 0 and 1 arrive first.
    peer.step(t0, flag);
    let s1 = peer.step(t0, tiny_part_prefix(block_id, 0));
    assert!(!peer.is_data_paused());
    assert!(s1
        .events
        .iter()
        .any(|e| matches!(e, PeerEvent::EarlyHeld { .. })));

    let s2 = peer.step(t0, tiny_part_prefix(block_id, 1));
    assert!(peer.is_data_paused());
    assert!(s2.outputs.contains(&PeerOutput::PauseDataReads));

    // The control stream still works while data reads are paused (bpv1-54).
    let meta_rx = peer.step(
        t0,
        PeerInput::RecvHeaderMeta {
            block_id,
            message: proposer_meta(block_id, node_id(2)),
        },
    );
    assert!(meta_rx.events.contains(&PeerEvent::MetaReceived(block_id)));

    // Admission replays the held seed parts and resumes data reads.
    let admit = peer.step(
        t0,
        PeerInput::BlockAdmitted(BlockAdmitted {
            header_meta: None,
            ..block_admitted(block_id, coding, node_id(2))
        }),
    );
    assert!(!peer.is_data_paused());
    assert_eq!(peer.early_data_bytes(), 0);
    assert!(admit.outputs.contains(&PeerOutput::ResumeDataReads));
    let verify_jobs = admit
        .outputs
        .iter()
        .filter(|o| matches!(o, PeerOutput::VerifyPart { .. }))
        .count();
    assert_eq!(verify_jobs, 2, "both early seed parts replayed to verify");
}

#[test]
fn scenario_retired_block_allows_stripes_times_n_then_closes_15() {
    let config = PeerConfig::for_profile(Profile::TINY);
    let mut peer = PeerState::new(config, node_id(2), 1);
    let now = Instant::now();
    let block_id = block::Hash([0x55; 32]);
    let coding = tiny();
    let total = coding.total_parts().unwrap();

    peer.step(
        now,
        PeerInput::Retire(Retire {
            block_id,
            stripes: coding.stripes,
            n: coding.n,
        }),
    );

    for i in 0..total {
        let s = peer.step(now, tiny_part_prefix(block_id, i));
        assert!(!peer.is_closed());
        assert!(s.events.contains(&PeerEvent::PartDropped {
            block_id,
            index: i,
            reason: DropReason::RetiredLate,
        }));
    }

    // One more late part exhausts the retired counter and closes 15.
    peer.step(now, tiny_part_prefix(block_id, 0));
    assert_closed(&peer, Bpv1CloseRow::Row15RetiredCounterExhausted);
}

#[test]
fn scenario_stale_verify_token_is_noop() {
    let config = PeerConfig::for_profile(Profile::TINY);
    let mut peer = PeerState::new(config, node_id(2), 2);
    let now = Instant::now();
    let block_id = block::Hash([0x66; 32]);
    let encoded = encode_body(&Profile::TINY, &[0x11; 64], 2).unwrap();
    let stale_token = PartToken {
        session_gen: 1, // old session generation
        block_id,
        index: 0,
        attempt: 1,
    };
    let s1 = peer.step(
        now,
        PeerInput::PartVerified {
            token: stale_token,
            part: encoded.parts[0].clone(),
        },
    );
    assert!(s1.outputs.is_empty());
    let s2 = peer.step(now, PeerInput::PartFailed { token: stale_token });
    assert!(s2.outputs.is_empty());
    assert!(!peer.is_closed());
}

proptest! {
    /// Level-6 / tool-3 (bpv1-60, bpv1-29, bpv1-30): two conformant nodes never
    /// close each other, however their four streams interleave. Each stream
    /// direction draws its own delay. A is the proposer's only neighbour, so
    /// it pushes its pushed seeds to B over the link (decision-24), and it
    /// may retire the block early.
    #[test]
    fn conformant_two_node_link_never_closes_under_any_interleaving(
        delays_ms in prop::array::uniform4(0u64..60),
        retire_after_ms in prop::option::of(0u64..200),
    ) {
        let peer_cfg = PeerConfig::for_profile(Profile::TINY);
        let mut mesh = MachineMesh::new(3, BlockConfig::for_profile(Profile::TINY));
        let fast = LinkProfile::new(Duration::from_millis(5), 1_000_000);
        mesh.connect(2, 0, fast, fast, peer_cfg, None, None);
        let link = |ctrl: u64, data: u64| LinkProfile {
            ctrl_delay: Duration::from_millis(ctrl),
            data_delay: Duration::from_millis(data),
            ..LinkProfile::new(Duration::ZERO, 1_000_000)
        };
        let [a_ctrl, a_data, b_ctrl, b_data] = delays_ms;
        mesh.connect(
            0,
            1,
            link(a_ctrl, a_data),
            link(b_ctrl, b_data),
            peer_cfg,
            None,
            None,
        );

        // Node 2 proposes to A, which pushes to B.
        let block_id = block::Hash([0x99; 32]);
        let encoded = encode_body(&Profile::TINY, &[0x33; 128], 2).unwrap();
        let envelope = make_envelope(
            block_id,
            encoded.coding,
            encoded.part_root,
            mesh.nodes[2].id,
        );
        mesh.propose(2, envelope, encoded.parts);

        let start = mesh.now;
        if let Some(after) = retire_after_ms {
            mesh.run_until(start + Duration::from_millis(after));
            // A tip past the propagation window retires the block at A.
            mesh.step_table(0, BlockInput::TipHeight { height: MESH_HEIGHT + PROPAGATION_DEPTH + 1, hash: TEST_TIP_HASH });
        }
        mesh.run_until(start + Duration::from_secs(3));

        mesh.assert_honest_links_open();
        for node in &mesh.nodes {
            for state in node.peers.values() {
                prop_assert_eq!(state.live_wants_out(), 0, "every Want ended");
            }
        }
        if retire_after_ms.is_none() {
            prop_assert!(mesh.verified(1, &block_id), "B assembled the block");
            prop_assert!(mesh.pushed_parts_accepted() > 0, "A pushed to B");
        }
    }

    /// Level-4 / tool-2: the fuzz replay asserts every invariant after each
    /// step and the drain on `SessionEnd`.
    #[test]
    fn peer_replay_holds_invariants_and_drains(bytes in prop::collection::vec(any::<u8>(), 0..128)) {
        replay_bpv1_peer_bytes(&bytes);
    }
}

#[test]
fn scenario_partial_want_server_ends_early_without_closing() {
    let config = PeerConfig::for_profile(Profile::TINY);
    let now = Instant::now();
    let mut a = PeerState::new(config, node_id(2), 1);
    let mut b = PeerState::new(config, node_id(1), 1);
    let block_id = block::Hash([0xAB; 32]);
    let encoded = encode_body(&Profile::TINY, &[0x21; 64], 2).unwrap();
    // A sends HeaderMeta; B admits on its own and receives A's.
    assert!(queues_meta(
        &a.step(now, admitted(block_id, encoded.coding, node_id(3)))
    ));
    b.step(
        now,
        PeerInput::BlockAdmitted(BlockAdmitted {
            header_meta: None,
            ..block_admitted(block_id, encoded.coding, node_id(3))
        }),
    );
    b.step(now, recv_meta(block_id));

    // B asks for slots 0, 1 and 2.
    let want_id = sent_want(&b.step(now, pull(block_id, 0, &[0, 1, 2], now)))
        .expect("a granted pull sends a Want");

    // A holds only slot 0, so it serves one part and ends the Want early.
    let serve = a.step(now, recv_want(want_id, block_id, 0, &[0, 1, 2]));
    assert!(serve
        .outputs
        .iter()
        .any(|o| matches!(o, PeerOutput::ServeWant { want_id: id, .. } if *id == want_id)));
    a.step(
        now,
        PeerInput::ServePartQueued(ServePartQueued {
            want_id,
            block_id,
            index: 0,
        }),
    );
    let done = a.step(
        now,
        PeerInput::ServeDone(ServeDone {
            want_id,
            sent: 1,
            reason: WantEndReason::Served,
        }),
    );
    assert!(done
        .outputs
        .iter()
        .any(|o| matches!(o, PeerOutput::SendWantEnd { sent: 1, .. })));

    // B takes the part, then the end, and releases the two slots it never got.
    let prefix = b.step(
        now,
        PeerInput::RecvPartPrefix(RecvPartPrefix {
            block_id,
            index: 0,
            payload: encode_part_frame(block_id, &encoded.parts[0]).payload,
        }),
    );
    let token = verify_token(&prefix).expect("a wanted part goes to proof");
    b.step(
        now,
        PeerInput::PartVerified {
            token,
            part: encoded.parts[0].clone(),
        },
    );
    let end = b.step(
        now,
        PeerInput::RecvWantEnd(RecvWantEnd { want_id, sent: 1 }),
    );
    assert!(end.outputs.iter().any(|o| matches!(
        o,
        PeerOutput::WantCompleted { unreceived_slots, .. } if unreceived_slots == &[1, 2]
    )));
    assert!(!a.is_closed());
    assert!(!b.is_closed());
    assert_eq!(b.live_wants_out(), 0);
}

#[test]
fn scenario_retirement_purges_queued_writes_and_emits_retired_want_end() {
    let now = Instant::now();
    let block_id = block::Hash([0xBC; 32]);
    let coding = tiny();
    let mut peer = tiny_session(block_id, now);

    // Peer opens an incoming Want that is currently being served.
    let recv_w = peer.step(now, recv_want(7, block_id, 0, &[0]));
    assert!(recv_w
        .outputs
        .iter()
        .any(|o| matches!(o, PeerOutput::ServeWant { want_id: 7, .. })));
    assert_eq!(peer.live_wants_in(), 1);

    // Retire while Want 7 is still live: must purge queued parts and emit WantEnd(Retired).
    let ret = peer.step(
        now,
        PeerInput::Retire(Retire {
            block_id,
            stripes: coding.stripes,
            n: coding.n,
        }),
    );
    assert!(ret.outputs.contains(&PeerOutput::PurgeQueuedBlock {
        block_id,
        include_haves: true,
    }));
    assert!(ret.outputs.iter().any(|o| matches!(
        o,
        PeerOutput::SendWantEnd {
            want_id: 7,
            sent: 0,
            reason: WantEndReason::Retired,
            ..
        }
    )));
    assert_eq!(peer.live_wants_in(), 0);
}

/// An empty body has no parts or stripes. The session sends `BlockDone`
/// once the block assembles, and any `Have`, `Part`, `Want`, or `StripeDone`
/// for it names an index or stripe outside the layout and closes.
#[test]
fn empty_body_session_sends_block_done_and_closes_on_part_traffic() {
    let now = Instant::now();
    let block_id = block::Hash([0xE3; 32]);
    let coding = Profile::TINY.derive_coding(0, 1).unwrap();
    assert_eq!(coding.total_parts(), Some(0));
    let admitted_peer = || session(node_id(2), now, admitted(block_id, coding, node_id(3)));

    let mut p = PeerState::new(PeerConfig::for_profile(Profile::TINY), node_id(2), 1);
    assert!(queues_meta(
        &p.step(now, admitted(block_id, coding, node_id(3)))
    ));
    let done = p.step(now, PeerInput::LocalBlockDone { block_id });
    assert_eq!(
        done.outputs,
        vec![PeerOutput::SendControl(Box::new(Bpv1Message::BlockDone {
            block_id
        }))]
    );
    p.assert_invariants();

    let traffic = [
        (
            have_with(block_id, HaveFlags::Plain, vec![0]),
            Bpv1CloseRow::Row16BadHaveIndex,
        ),
        (
            have_with(block_id, HaveFlags::Relay, vec![0]),
            Bpv1CloseRow::Row16BadHaveIndex,
        ),
        (
            PeerInput::RecvPartPrefix(RecvPartPrefix {
                block_id,
                index: 0,
                payload: part_frame(&coding, 0),
            }),
            Bpv1CloseRow::Row13BadPartIndexOrProof,
        ),
        (
            recv_want(1, block_id, 0, &[0]),
            Bpv1CloseRow::Row8BadWantRange,
        ),
        (
            PeerInput::RecvStripeDone {
                block_id,
                stripe: 0,
            },
            Bpv1CloseRow::Row10BadStripeDone,
        ),
    ];
    for (input, row) in traffic {
        let mut p = admitted_peer();
        p.step(now, input);
        assert_closed(&p, row);
    }
}

#[test]
fn tool6_lying_relay_adversary_close_rows() {
    let now = Instant::now();
    let block_id = block::Hash([0xDE; 32]);
    let want_end = |sent| PeerInput::RecvWantEnd(RecvWantEnd { want_id: 1, sent });
    let stripe_done = || PeerInput::RecvStripeDone {
        block_id,
        stripe: 0,
    };
    let cases = [
        // Overlapping Wants.
        (
            vec![
                recv_want(1, block_id, 0, &[0]),
                recv_want(2, block_id, 0, &[0]),
            ],
            Bpv1CloseRow::Row9WantReservationViolation,
        ),
        // An out-of-range Want stripe.
        (
            vec![recv_want(1, block_id, tiny().stripes, &[0])],
            Bpv1CloseRow::Row8BadWantRange,
        ),
        // A duplicate StripeDone.
        (
            vec![stripe_done(), stripe_done()],
            Bpv1CloseRow::Row10BadStripeDone,
        ),
        // A WantEnd with sent > popcount.
        (
            vec![pull(block_id, 0, &[0], now), want_end(2)],
            Bpv1CloseRow::Row18BadWantEnd,
        ),
        // A WantEnd with sent < attributed.
        (
            vec![
                pull(block_id, 0, &[0, 1], now),
                tiny_part_prefix(block_id, 0),
                want_end(0),
            ],
            Bpv1CloseRow::Row18BadWantEnd,
        ),
    ];
    for (inputs, row) in cases {
        let mut p = tiny_session(block_id, now);
        for input in inputs {
            p.step(now, input);
        }
        assert_closed(&p, row);
    }

    // A forged proof (PartFailed) closes 13.
    let mut p = tiny_session(block_id, now);
    p.step(now, pull(block_id, 0, &[0], now));
    let rx = p.step(now, tiny_part_prefix(block_id, 0));
    let token = verify_token(&rx).expect("verify job emitted");
    p.step(now, PeerInput::PartFailed { token });
    assert_closed(&p, Bpv1CloseRow::Row13BadPartIndexOrProof);
}

/// bpv1-38: the peer's `BlockDone` cancels the pending `Have` batch, so an
/// idle session has no deadline.
#[test]
fn bpv1_38_block_done_cancels_the_pending_have_batch() {
    let now = Instant::now();
    let block_id = block::Hash([0xB1; 32]);
    let mut p = tiny_session(block_id, now);
    p.step(now, ready(block_id, vec![0, 1], false));
    assert!(
        p.next_deadline().is_some(),
        "pending Have schedules a batching deadline"
    );
    p.step(now, PeerInput::RecvBlockDone { block_id });
    assert_eq!(
        p.next_deadline(),
        None,
        "a BlockDone from the peer cancels the pending Have batch"
    );
}

/// bpv1-9: a block whose miner named this node as `proposer_node`, but that
/// this node admitted from a peer, is relayed like any other block: the node
/// announces and pushes its parts. Only the node that proposed it stays quiet.
#[test]
fn bpv1_9_a_node_named_as_proposer_that_did_not_propose_still_relays() {
    let now = Instant::now();
    let named = block::Hash([0xB9; 32]);
    let mut p = session(node_id(2), now, admitted(named, tiny(), node_id(1)));
    let step = p.step(now, ready(named, vec![0], true));
    assert!(
        step.outputs.iter().any(|output| matches!(
            output,
            PeerOutput::SendHave {
                flags: HaveFlags::Push,
                ..
            }
        )),
        "the named node pushes its pushed seeds"
    );
    p.step(now, ready(named, vec![1], false));
    assert!(
        p.next_deadline().is_some(),
        "the named node announces the parts it verified"
    );

    let proposed = block::Hash([0xBA; 32]);
    let mut p = session(node_id(2), now, proposing(proposed, tiny()));
    let step = p.step(now, ready(proposed, vec![0], true));
    assert!(
        !step
            .outputs
            .iter()
            .any(|output| matches!(output, PeerOutput::SendHave { .. })),
        "the proposer pushes nothing back"
    );
    p.step(now, ready(proposed, vec![1], false));
    assert_eq!(p.next_deadline(), None, "the proposer announces nothing");
}

/// bpv1-49: a grant the session refuses releases its slots at once.
#[test]
fn bpv1_49_refused_grant_releases_its_slots() {
    let config = PeerConfig::for_profile(Profile::TINY);
    let now = Instant::now();
    let block_id = block::Hash([0xB1; 32]);
    let mut p = PeerState::new(config, node_id(2), 1);
    // Block not admitted on this peer -> PullGranted is refused and must release [0, 2].
    let step = p.step(now, pull(block_id, 0, &[0, 2], now));
    assert!(
        step.outputs.iter().any(|o| matches!(
            o,
            PeerOutput::ReleaseClaims {
                block_id: bid,
                stripe: 0,
                slots,
                reason: ReleaseReason::Refused,
            } if *bid == block_id && slots == &vec![0, 2]
        )),
        "a refused grant releases its slots, got {:?}",
        step.outputs
    );
}

/// Each close cites the stream the violating message arrived on.
#[test]
fn scenario_close_cites_the_stream_it_read() {
    use super::wire::{BPV1_CONTROL_KIND, BPV1_DATA_KIND};

    let now = Instant::now();
    let block_id = block::Hash([0xC5; 32]);

    // A repeated `Have` index arrives on the data stream.
    let mut data = tiny_session(block_id, now);
    for _ in 0..2 {
        data.step(now, have_with(block_id, HaveFlags::Plain, vec![0]));
    }
    let cause = data.close_cause().expect("a repeated Have index closes");
    assert_eq!(cause.row, Bpv1CloseRow::Row16BadHaveIndex);
    assert_eq!(cause.stream_kind, BPV1_DATA_KIND);

    // A repeated `BlockDone` arrives on the control stream.
    let mut control = tiny_session(block_id, now);
    for _ in 0..2 {
        control.step(now, PeerInput::RecvBlockDone { block_id });
    }
    let cause = control.close_cause().expect("a repeated BlockDone closes");
    assert_eq!(cause.row, Bpv1CloseRow::Row11DuplicateBlockDone);
    assert_eq!(cause.stream_kind, BPV1_CONTROL_KIND);
}

/// `PULL_WINDOW` bounds the parts outstanding from
/// one peer even under a planner that grants every stripe.
#[test]
fn scenario_pull_window_bounds_outstanding_parts_under_greedy_planner() {
    let now = Instant::now();
    let block_id = block::Hash([0xF1; 32]);
    let coding = Profile::TINY.derive_coding(4096, 2).unwrap();
    let part_bytes = u64::from(coding.part_bytes);
    let mut peer = session(node_id(2), now, admitted(block_id, coding, node_id(3)));
    let full_stripe: Vec<u32> = (0..coding.n).collect();
    for stripe in 0..coding.stripes {
        peer.step(now, pull(block_id, stripe, &full_stripe, now));
        assert!(
            peer.outstanding_bytes() <= u64::from(PULL_WINDOW) * part_bytes,
            "stripe {stripe}: {} outstanding parts exceed PULL_WINDOW {PULL_WINDOW}",
            peer.outstanding_bytes() / part_bytes,
        );
    }
    assert!(
        peer.outstanding_bytes() >= (u64::from(PULL_WINDOW) - u64::from(coding.n)) * part_bytes,
        "grants keep filling the window until the next grant would exceed it"
    );
}

/// Every `Part` the early record refuses
/// emits `PartDropped` with its own reason.
#[test]
fn scenario_early_record_refusals_emit_part_dropped() {
    let mut config = PeerConfig::for_profile(Profile::TINY);
    config.early_peer_bytes_cap = 64;
    let now = Instant::now();
    let block_id = block::Hash([0x4E; 32]);
    let prefix = |index, frame_bytes| {
        PeerInput::RecvPartPrefix(RecvPartPrefix {
            block_id,
            index,
            payload: vec![0; frame_bytes],
        })
    };
    let dropped = |step: &PeerStep, index| {
        step.events.iter().find_map(|e| match e {
            PeerEvent::PartDropped {
                index: i, reason, ..
            } if *i == index => Some(*reason),
            _ => None,
        })
    };

    // A frame larger than the whole cap can never fit.
    let mut peer = PeerState::new(config, node_id(2), 1);
    let oversize = peer.step(now, prefix(0, 65));
    assert_eq!(dropped(&oversize, 0), Some(DropReason::EarlyOversize));

    // A frame that does not fit beside held entries is dropped as full.
    let mut peer = PeerState::new(config, node_id(2), 1);
    peer.step(now, prefix(0, 40));
    let full = peer.step(now, prefix(1, 40));
    assert!(peer.is_data_paused());
    assert_eq!(dropped(&full, 1), Some(DropReason::EarlyFull));
}

/// The `Have` row's declared `send_interval` holds for the sender's real
/// flush timer.
#[test]
fn have_flush_timer_honours_declared_have_cadence() {
    use super::wire::HAVE_RULE;
    use crate::zakura::MessageRole;

    let MessageRole::Announcement { cadence: have } = HAVE_RULE.role else {
        panic!("the Have row is an announcement");
    };

    let block_id = block::Hash([0x22; 32]);
    let coding = Profile::TINY.derive_coding(4096, 2).unwrap();
    let mut now = Instant::now();
    let mut peer = session(node_id(2), now, admitted(block_id, coding, node_id(3)));
    let mut last_have: Option<Instant> = None;
    for index in 0..8 {
        peer.step(now, ready(block_id, vec![index], false));
        now = peer.next_deadline().expect("a pending Have arms the timer");
        let step = peer.step(now, PeerInput::Tick);
        assert!(step
            .outputs
            .iter()
            .any(|o| matches!(o, PeerOutput::SendHave { .. })));
        if let Some(last) = last_have {
            assert!(
                now.saturating_duration_since(last) >= have.send_interval,
                "Haves {:?} apart; the row declares {:?}",
                now.saturating_duration_since(last),
                have.send_interval
            );
        }
        last_have = Some(now);
    }
}

#[test]
fn unwritten_want_ends_count_against_the_live_want_limit() {
    // A peer that stops reading data cannot grow the data queue with
    // `WantEnd`s. Each unwritten end holds its `Want`'s place until the
    // writer hands it to the transport.
    let config = PeerConfig::for_profile(Profile::TINY);
    let now = Instant::now();
    let unknown = block::Hash([0xAB; 32]);
    // Each `Want` asks for its own stripe: a `Want` whose end is unwritten is
    // still live, so a repeated set would overlap it.
    let want = |want_id| recv_want(want_id, unknown, want_id, &[0]);
    let hard_limit = MAX_WANTS_INFLIGHT * 2;

    let mut peer = PeerState::new(config, node_id(2), 1);
    for want_id in 1..=hard_limit {
        let step = peer.step(now, want(want_id));
        assert!(step
            .outputs
            .iter()
            .any(|o| matches!(o, PeerOutput::SendWantEnd { .. })));
    }
    assert!(!peer.is_closed());
    peer.step(now, PeerInput::WantEndWritten);
    peer.step(now, want(hard_limit + 1));
    assert!(!peer.is_closed(), "a written end frees its place");
    peer.step(now, want(hard_limit + 2));
    assert_closed(&peer, Bpv1CloseRow::Row9WantReservationViolation);
}

#[test]
fn refused_part_charge_clears_its_attempt_without_faulting_the_peer() {
    // The block table refuses a part before proof when its buffer is
    // full or the block left it. The attempt ends; the sender stays open.
    for reason in [DropReason::LocalCapacity, DropReason::NotAdmitted] {
        let now = Instant::now();
        let block_id = block::Hash([0xC8; 32]);
        let mut peer = tiny_session(block_id, now);
        peer.step(now, pull(block_id, 0, &[0], now));
        let rx = peer.step(now, tiny_part_prefix(block_id, 0));
        let token = verify_token(&rx).expect("verify job emitted");
        let refused = peer.step(now, PeerInput::PartRefused { token, reason });
        assert!(!peer.is_closed(), "{reason:?} never faults the sender");
        assert_eq!(
            refused.events,
            vec![PeerEvent::PartDropped {
                block_id,
                index: 0,
                reason,
            }]
        );
        // The attempt is gone: a late proof failure for it is a no-op.
        peer.step(now, PeerInput::PartFailed { token });
        assert!(!peer.is_closed(), "{reason:?} ends the attempt");
    }
}

/// bpv1-10: a malformed part prefix is refused by the cheap range,
/// receive-once and authorization checks before any proof work. The only
/// proof work a session starts is the shell's hashing for `VerifyPart`.
#[test]
fn bpv1_10_malformed_part_prefix_is_refused_before_proof_work() {
    let block_id = block::Hash([0x10; 32]);
    let coding = tiny();
    let now = Instant::now();
    // A session that admitted the block and holds a live `Want` for slot 0.
    let wanting = || {
        let mut peer = tiny_session(block_id, now);
        peer.step(now, pull(block_id, 0, &[0], now));
        peer
    };
    let proof_jobs = |step: &PeerStep| {
        step.outputs
            .iter()
            .filter(|o| matches!(o, PeerOutput::VerifyPart { .. }))
            .count()
    };

    // The wanted part starts exactly one proof job.
    let mut peer = wanting();
    assert_eq!(
        proof_jobs(&peer.step(now, tiny_part_prefix(block_id, 0))),
        1
    );
    // Its repeat is refused before a second one.
    assert_eq!(
        proof_jobs(&peer.step(now, tiny_part_prefix(block_id, 0))),
        0
    );
    assert_closed(&peer, Bpv1CloseRow::Row14UnwantedOrDuplicatePart);

    for (index, row) in [
        (
            coding.n * coding.stripes,
            Bpv1CloseRow::Row13BadPartIndexOrProof,
        ),
        (1, Bpv1CloseRow::Row14UnwantedOrDuplicatePart),
    ] {
        let mut peer = wanting();
        assert_eq!(
            proof_jobs(&peer.step(now, tiny_part_prefix(block_id, index))),
            0,
            "index {index}"
        );
        assert_eq!(
            peer.close_cause().map(|c| c.row),
            Some(row),
            "index {index}"
        );
        peer.assert_invariants();
    }

    // bpv1-66: a wanted part whose payload is not `wire_len(i)` bytes closes 13
    // before proof work, longer or shorter.
    for frame_len in [
        part_frame(&coding, 0).len() + 1,
        part_frame(&coding, 0).len() - 1,
    ] {
        let mut peer = wanting();
        let step = peer.step(
            now,
            PeerInput::RecvPartPrefix(RecvPartPrefix {
                block_id,
                index: 0,
                payload: vec![0; frame_len],
            }),
        );
        assert_eq!(proof_jobs(&step), 0, "frame {frame_len}");
        assert_eq!(
            peer.close_cause().map(|c| c.row),
            Some(Bpv1CloseRow::Row13BadPartIndexOrProof),
            "frame {frame_len}"
        );
    }
}

/// decision-19 / bpv1-69 / close-20: the proposer's flagged `Have` on its
/// own connection authorizes exactly the seeds it flags. Its push flags may
/// cover every index of a stripe, but its relay flags at most `n − k`. A
/// relay flag from anyone else closes 20.
#[test]
fn decision19_proposer_push_bound_is_n_and_relay_bound_n_minus_k() {
    let coding = tiny();
    let block_id = block::Hash([0x54; 32]);
    let now = Instant::now();
    let (n, bound) = (coding.n, coding.n - coding.k);
    let relay: Vec<u32> = (0..bound).collect();

    // The proposer, node 2, flags `n − k` relay seeds and the rest of
    // stripe 0 push only: every index of the stripe. The table learns the
    // relay seeds as this node's pushed seeds.
    let mut peer = push_session(node_id(2), block_id);
    let s = peer.step(now, have_with(block_id, HaveFlags::Relay, relay.clone()));
    assert!(s.outputs.contains(&PeerOutput::PushFlagged {
        block_id,
        indices: relay,
        relay: true,
    }));
    peer.step(
        now,
        have_with(block_id, HaveFlags::Push, (bound..n).collect()),
    );
    assert!(peer.close_cause().is_none());
    for index in 0..n {
        // Widening u32 to usize is lossless on supported targets.
        let (auth, _) = deliver(&mut peer, block_id, &session_part(index as usize), now);
        assert_eq!(auth, Some(PartAuth::Seed), "seed {index}");
    }
    assert!(peer.close_cause().is_none());
    peer.assert_invariants();

    // A seed the proposer did not flag and nobody wanted closes 14.
    let mut peer = push_session(node_id(2), block_id);
    peer.step(now, have_with(block_id, HaveFlags::Relay, vec![0]));
    peer.step(now, tiny_part_prefix(block_id, 1));
    assert_closed(&peer, Bpv1CloseRow::Row14UnwantedOrDuplicatePart);

    // One relay flag past `n − k`, or a holder's relay flag, closes 20.
    for (remote, relay) in [(node_id(2), (0..=bound).collect()), (node_id(3), vec![0])] {
        let mut peer = push_session(remote, block_id);
        peer.step(now, have_with(block_id, HaveFlags::Relay, relay));
        assert_closed(&peer, Bpv1CloseRow::Row20BadPushFlags);
    }
}

/// decision-19 / bpv1-25: the proposer flags each seed once, relay seeds
/// first, each `Have` ahead of its parts, and skips an index the peer
/// announced or that it already flagged.
#[test]
fn decision19_proposer_flags_its_seeds_ahead_of_them_once() {
    let encoded = encode_body(&Profile::TINY, &[0x5A; 64], 2).unwrap();
    let block_id = block::Hash([0x52; 32]);
    let t0 = Instant::now();
    let mut peer = session(node_id(2), t0, proposing(block_id, encoded.coding));
    // The peer announced part 3 before the proposer's `BlockDone` reached it.
    peer.step(t0, have_with(block_id, HaveFlags::Plain, vec![3]));
    let seeds = |peer: &mut PeerState, seeds: &[(u32, bool)]| {
        peer.step(
            t0,
            PeerInput::Seeds(Seeds {
                block_id,
                seeds: seeds
                    .iter()
                    .map(|&(index, relay)| Seed {
                        // Widening u32 to usize is lossless on supported targets.
                        part: encoded.parts[index as usize].clone(),
                        relay,
                    })
                    .collect(),
                copies: Vec::new(),
            }),
        )
        .outputs
        .into_iter()
        .map(|o| match o {
            PeerOutput::SendHave { flags, indices, .. } => (Some(flags), indices),
            PeerOutput::SendSeedPart { part, .. } => (None, vec![part.index()]),
            other => panic!("unexpected output {other:?}"),
        })
        .collect::<Vec<_>>()
    };
    assert_eq!(
        seeds(&mut peer, &[(2, false), (0, true), (3, false), (1, true)]),
        vec![
            (Some(HaveFlags::Relay), vec![0, 1]),
            (None, vec![0]),
            (None, vec![1]),
            (Some(HaveFlags::Push), vec![2]),
            (None, vec![2]),
        ]
    );
    assert!(
        seeds(&mut peer, &[(0, false)]).is_empty(),
        "a seed goes out once"
    );
    peer.assert_invariants();
}

/// decision-20 / bpv1-9: a proposer's session never announces its own block.
#[test]
fn decision20_proposer_session_never_sends_have() {
    let block_id = block::Hash([0x53; 32]);
    let mut now = Instant::now();
    let mut peer = session(node_id(2), now, proposing(block_id, tiny()));
    let s = peer.step(now, ready(block_id, vec![0, 1], false));
    assert!(s
        .outputs
        .iter()
        .all(|o| !matches!(o, PeerOutput::SendHave { .. })));
    now += Duration::from_secs(1);
    let s = peer.step(now, PeerInput::Tick);
    assert!(s
        .outputs
        .iter()
        .all(|o| !matches!(o, PeerOutput::SendHave { .. })));
    assert!(
        peer.next_deadline().is_none_or(|d| d > now),
        "no Have is pending"
    );
}

/// decision-21: a `Want` that received parts yields one delivery-rate sample
/// from the grant time and the first and last arrivals; an empty one yields none.
#[test]
fn decision21_want_end_with_parts_emits_one_rate_sample() {
    let coding = tiny();
    let block_id = block::Hash([0x54; 32]);
    let t0 = Instant::now();
    let mut peer = tiny_session(block_id, t0);
    let want = |peer: &mut PeerState, slots: &[u32]| {
        sent_want(&peer.step(t0, pull(block_id, 0, slots, t0))).expect("a Want goes out")
    };
    let samples = |s: &PeerStep| -> Vec<super::paths::RateSample> {
        s.outputs
            .iter()
            .filter_map(|o| match o {
                PeerOutput::RateSample(sample) => Some(*sample),
                _ => None,
            })
            .collect()
    };

    let want_id = want(&mut peer, &[0, 1]);
    let mut bytes = 0;
    for (index, ms) in [(0u32, 30u64), (1, 45)] {
        let payload = part_frame(&coding, index);
        bytes += payload.len() as u64;
        peer.step(
            t0 + Duration::from_millis(ms),
            PeerInput::RecvPartPrefix(RecvPartPrefix {
                block_id,
                index,
                payload,
            }),
        );
    }
    let end = peer.step(
        t0 + Duration::from_millis(46),
        PeerInput::RecvWantEnd(RecvWantEnd { want_id, sent: 2 }),
    );
    assert_eq!(
        samples(&end),
        vec![super::paths::RateSample {
            bytes,
            sent_at: t0,
            first_at: t0 + Duration::from_millis(30),
            last_at: t0 + Duration::from_millis(45),
        }]
    );
    let sample_at = end
        .outputs
        .iter()
        .position(|o| matches!(o, PeerOutput::RateSample(_)))
        .unwrap();
    let completed_at = end
        .outputs
        .iter()
        .position(|o| matches!(o, PeerOutput::WantCompleted { .. }))
        .unwrap();
    assert!(
        sample_at < completed_at,
        "the sample lands before the re-plan"
    );

    let want_id = want(&mut peer, &[2]);
    let end = peer.step(
        t0 + Duration::from_millis(60),
        PeerInput::RecvWantEnd(RecvWantEnd { want_id, sent: 0 }),
    );
    assert!(samples(&end).is_empty(), "an empty Want yields no sample");
}

/// A session from local node 1 to `remote` with the 64-byte TINY block
/// admitted; node 2 proposed it, and node 1 sent `HeaderMeta`.
fn push_session(remote: NodeId, block_id: block::Hash) -> PeerState {
    session(
        remote,
        Instant::now(),
        admitted(block_id, tiny(), node_id(2)),
    )
}

/// decision-24 / bpv1-71 / bpv1-72: a holder writes one flagged `Have`, then
/// the parts behind it, never to the proposer, and never twice.
#[test]
fn decision24_holder_flags_then_pushes_its_seeds_but_not_to_the_proposer() {
    let block_id = block::Hash([0x61; 32]);
    let now = Instant::now();

    let mut peer = push_session(node_id(3), block_id);
    let s = peer.step(now, ready(block_id, vec![0, 1], true));
    assert_eq!(
        s.outputs,
        vec![
            PeerOutput::SendHave {
                block_id,
                flags: HaveFlags::Push,
                form: HaveForm::smaller(tiny().total_parts().unwrap(), 2),
                indices: vec![0, 1],
            },
            PeerOutput::PushPart { block_id, index: 0 },
            PeerOutput::PushPart { block_id, index: 1 },
        ]
    );
    peer.assert_invariants();
    // A part already pushed is never pushed or announced again.
    assert!(peer
        .step(now, ready(block_id, vec![1], true))
        .outputs
        .is_empty());
    assert!(peer
        .step(now, ready(block_id, vec![1], false))
        .outputs
        .is_empty());
    // A part the peer announced is not pushed.
    peer.step(now, have_with(block_id, HaveFlags::Plain, vec![2]));
    assert!(peer
        .step(now, ready(block_id, vec![2], true))
        .outputs
        .is_empty());
}

/// decision-32: a pushed seed this node got from another peer, or rebuilt,
/// reaches the proposer as a plain `Have`, never as a push.
#[test]
fn decision32_a_pushed_seed_is_announced_to_the_proposer_not_pushed() {
    let block_id = block::Hash([0x6D; 32]);
    let now = Instant::now();

    let mut to_proposer = push_session(node_id(2), block_id);
    assert_eq!(
        to_proposer
            .step(now, ready(block_id, vec![0], true))
            .outputs,
        vec![PeerOutput::SendHave {
            block_id,
            flags: HaveFlags::Plain,
            form: HaveForm::smaller(tiny().total_parts().unwrap(), 1),
            indices: vec![0],
        }]
    );
    to_proposer.assert_invariants();
}

/// 03 §5.4: when the peer announces or sends a part this node pushes to
/// it, the writer skips the push if it is still queued.
#[test]
fn decision24_peer_announcing_or_sending_a_pushed_part_cancels_the_push() {
    let block_id = block::Hash([0x6A; 32]);
    let now = Instant::now();
    let mut peer = push_session(node_id(3), block_id);
    peer.step(now, ready(block_id, vec![0, 1], true));
    let s = peer.step(now, have_with(block_id, HaveFlags::Plain, vec![0]));
    assert_eq!(
        s.outputs,
        vec![
            PeerOutput::CancelQueuedPush { block_id, index: 0 },
            PeerOutput::PeerHas {
                block_id,
                indices: vec![0],
            },
        ]
    );
    // The peer pushes part 1 too, under its own flag.
    peer.step(now, have_with(block_id, HaveFlags::Push, vec![1]));
    let s = peer.step(now, tiny_part_prefix(block_id, 1));
    assert!(s
        .outputs
        .contains(&PeerOutput::CancelQueuedPush { block_id, index: 1 }));
    assert!(!peer.is_closed());
    peer.assert_invariants();
}

/// bpv1-69 / close-20: a flagged `Have` opens push slots that authorize
/// parts; more than `n − k` flags in a stripe close 20 before any part.
#[test]
fn decision24_flagged_have_opens_push_slots_up_to_n_minus_k() {
    let coding = tiny();
    let block_id = block::Hash([0x62; 32]);
    let now = Instant::now();
    let bound = coding.n - coding.k;
    let stripe0: Vec<u32> = (0..bound).collect();

    let mut peer = push_session(node_id(3), block_id);
    let s = peer.step(now, have_with(block_id, HaveFlags::Push, stripe0.clone()));
    assert!(s.outputs.contains(&PeerOutput::PushFlagged {
        block_id,
        indices: stripe0.clone(),
        relay: false,
    }));
    assert!(!s
        .outputs
        .iter()
        .any(|o| matches!(o, PeerOutput::PeerHas { .. })));
    let s = peer.step(now, tiny_part_prefix(block_id, 0));
    assert!(s.events.contains(&PeerEvent::PartPrefix {
        block_id,
        index: 0,
        auth: PartAuth::Push,
    }));
    // An unflagged index still needs a `Want`.
    peer.step(now, tiny_part_prefix(block_id, bound));
    assert_closed(&peer, Bpv1CloseRow::Row14UnwantedOrDuplicatePart);

    // One flag past the bound closes 20, across `Have`s too.
    let mut peer = push_session(node_id(3), block_id);
    peer.step(now, have_with(block_id, HaveFlags::Push, stripe0));
    peer.step(now, have_with(block_id, HaveFlags::Push, vec![bound]));
    assert_closed(&peer, Bpv1CloseRow::Row20BadPushFlags);
    let mut peer = push_session(node_id(3), block_id);
    peer.step(
        now,
        have_with(block_id, HaveFlags::Push, (0..=bound).collect()),
    );
    assert_closed(&peer, Bpv1CloseRow::Row20BadPushFlags);
}

/// bpv1-69: a pull grant never names an index the peer is pushing.
#[test]
fn decision24_no_want_names_an_open_push_slot() {
    let block_id = block::Hash([0x63; 32]);
    let now = Instant::now();
    let mut peer = push_session(node_id(3), block_id);
    peer.step(now, have_with(block_id, HaveFlags::Push, vec![0]));
    let s = peer.step(now, pull(block_id, 0, &[0, 1], now));
    assert!(s.outputs.contains(&PeerOutput::ReleaseClaims {
        block_id,
        stripe: 0,
        slots: vec![0, 1],
        reason: ReleaseReason::Failed,
    }));
    assert!(!s
        .outputs
        .iter()
        .any(|o| matches!(o, PeerOutput::SendControl(_))));
}

/// bpv1-72: more pushed seeds than one `Have` lists go out as several
/// flagged `Have`s, each ahead of its own parts, and every one encodes.
#[test]
fn decision24_flagged_have_is_chunked_at_the_have_limit() {
    use super::profile::W2_MAX_HAVE_INDICES;
    let coding = Coding {
        codec_id: 2,
        body_bytes: 200 * 64 * 4_096,
        tx_count: 2,
        part_bytes: 4_096,
        stripes: 200,
        k: 64,
        n: 86,
    };
    let block_id = block::Hash([0x69; 32]);
    let now = Instant::now();
    let mut peer = PeerState::new(PeerConfig::for_profile(Profile::W2), node_id(3), 1);
    peer.step(now, admitted(block_id, coding, node_id(2)));
    // `n − k` pushed seeds in every stripe: 4,400 in all.
    let pushed: Vec<u32> = (0..coding.stripes)
        .flat_map(|stripe| (coding.k..coding.n).map(move |slot| (stripe, slot)))
        .map(|(stripe, slot)| coding.wire_index(stripe, slot).unwrap())
        .collect();
    assert!(pushed.len() > W2_MAX_HAVE_INDICES);
    let s = peer.step(now, ready(block_id, pushed.clone(), true));
    let mut flagged: Vec<u32> = Vec::new();
    let mut sent: Vec<u32> = Vec::new();
    let mut haves = 0;
    for output in &s.outputs {
        match output {
            PeerOutput::SendHave {
                flags: HaveFlags::Push,
                form,
                indices,
                ..
            } => {
                haves += 1;
                assert!(
                    super::wire::DataQueueItem::Have {
                        block_id,
                        flags: HaveFlags::Push,
                        form: *form,
                        indices: indices.clone(),
                    }
                    .encode()
                    .is_some(),
                    "a flagged Have of {} indices does not encode",
                    indices.len()
                );
                flagged.extend(indices);
            }
            PeerOutput::PushPart { index, .. } => {
                assert!(flagged.contains(index), "part {index} before its flag");
                sent.push(*index);
            }
            _ => {}
        }
    }
    assert_eq!(haves, 2);
    assert_eq!(flagged, pushed);
    assert_eq!(sent, pushed);
    peer.assert_invariants();
}

/// decision-24: a flagged `Have` for an unadmitted block waits in the early
/// record and counts toward the bound at replay.
#[test]
fn decision24_flagged_have_for_unadmitted_block_waits_and_counts_at_replay() {
    let coding = tiny();
    let block_id = block::Hash([0x64; 32]);
    let now = Instant::now();
    let bound = coding.n - coding.k;
    let mut peer = PeerState::new(PeerConfig::for_profile(Profile::TINY), node_id(3), 1);
    let s = peer.step(now, have_with(block_id, HaveFlags::Push, vec![0]));
    assert!(s
        .events
        .iter()
        .any(|e| matches!(e, PeerEvent::EarlyHeld { .. })));
    let s = admit_tiny(&mut peer, block_id, now);
    assert!(s.outputs.contains(&PeerOutput::PushFlagged {
        block_id,
        indices: vec![0],
        relay: false,
    }));

    let mut peer = PeerState::new(PeerConfig::for_profile(Profile::TINY), node_id(3), 1);
    peer.step(
        now,
        have_with(block_id, HaveFlags::Push, (0..bound).collect()),
    );
    peer.step(now, have_with(block_id, HaveFlags::Push, vec![bound]));
    assert!(peer.close_cause().is_none());
    admit_tiny(&mut peer, block_id, now);
    assert_closed(&peer, Bpv1CloseRow::Row20BadPushFlags);
}

/// Admits the 64-byte TINY block that node 2 proposed; node 1 sends
/// `HeaderMeta` to the peer.
fn admit_tiny(peer: &mut PeerState, block_id: block::Hash, now: Instant) -> PeerStep {
    peer.step(now, admitted(block_id, tiny(), node_id(2)))
}

pub(super) fn tiny_part_prefix(block_id: block::Hash, index: u32) -> PeerInput {
    PeerInput::RecvPartPrefix(RecvPartPrefix {
        block_id,
        index,
        payload: part_frame(&tiny(), index),
    })
}

fn part_auth(step: &PeerStep) -> Option<PartAuth> {
    step.events.iter().find_map(|e| match e {
        PeerEvent::PartPrefix { auth, .. } => Some(*auth),
        _ => None,
    })
}

/// bpv1-60 / bpv1-48: a full early record refuses the holder's flagged
/// `Have`, and the block is admitted before the flagged part is read. The
/// part opens its own push slot instead of closing 14. The lost flags still
/// count toward `n − k` per stripe, so one more unasked part closes.
#[test]
fn bpv1_60_flagged_have_refused_by_a_full_early_record_never_closes() {
    let coding = tiny();
    let bound = coding.n - coding.k;
    assert!(bound >= 2 && coding.split_index(bound).unwrap().0 == 0);
    let have_bytes = 33 + HaveForm::List.indices_len(1);
    let mut config = PeerConfig::for_profile(Profile::TINY);
    // The record holds one flagged `Have` and its part, with no room for a
    // second `Have`.
    config.early_peer_bytes_cap = have_bytes + part_frame(&coding, 0).len() + have_bytes - 1;
    let mut peer = PeerState::new(config, node_id(3), 1);
    let now = Instant::now();
    let block_id = block::Hash([0x67; 32]);

    peer.step(now, have_with(block_id, HaveFlags::Push, vec![0]));
    peer.step(now, tiny_part_prefix(block_id, 0));
    assert!(!peer.is_data_paused());
    let refused = peer.step(now, have_with(block_id, HaveFlags::Push, vec![1]));
    assert!(refused.outputs.contains(&PeerOutput::PauseDataReads));
    peer.assert_invariants();

    let admit = admit_tiny(&mut peer, block_id, now);
    assert!(admit.outputs.contains(&PeerOutput::ResumeDataReads));
    assert_eq!(part_auth(&admit), Some(PartAuth::Push));

    // Reads resume: the part behind the refused flag arrives.
    let s = peer.step(now, tiny_part_prefix(block_id, 1));
    assert!(peer.close_cause().is_none(), "a conformant holder closed");
    assert_eq!(part_auth(&s), Some(PartAuth::Push));
    assert!(s
        .outputs
        .iter()
        .any(|o| matches!(o, PeerOutput::VerifyPart { .. })));
    peer.assert_invariants();

    // Stripe 0 now has `n − k` push slots, so it takes `Want`s again and
    // an unasked part closes 14.
    assert!(sent_want(&peer.step(now, pull(block_id, 0, &[2], now))).is_some());
    peer.step(now, tiny_part_prefix(block_id, 3));
    assert_closed(&peer, Bpv1CloseRow::Row14UnwantedOrDuplicatePart);
}

/// bpv1-48 / decision-33: a bitmap byte decodes to up to eight `u32`
/// indices, so the early record charges a held `Have` its decoded indices
/// when they outweigh its frame. A peer that sends 4,096-index bitmaps for
/// unknown blocks holds no more than the per-peer cap.
#[test]
fn bpv1_48_early_record_charges_a_have_its_decoded_indices() {
    let config = PeerConfig::for_profile(Profile::TINY);
    let cap = config.early_peer_bytes_cap;
    let mut peer = PeerState::new(config, node_id(3), 1);
    let now = Instant::now();
    // 512 bytes of set bits name 4,096 indices.
    let form = HaveForm::Bitmap { len: 512 };
    let indices: Vec<u32> = (0..4_096).collect();
    let frame_bytes = 33 + form.indices_len(indices.len());
    let mut held_indices = 0;
    for i in 0..=cap / frame_bytes {
        let mut id = [0x6A; 32];
        id[..8].copy_from_slice(&(i as u64).to_le_bytes());
        let step = peer.step(
            now,
            PeerInput::RecvHave(RecvHave {
                block_id: block::Hash(id),
                flags: HaveFlags::Plain,
                form,
                frame_bytes,
                indices: indices.clone(),
            }),
        );
        if step
            .events
            .iter()
            .any(|e| matches!(e, PeerEvent::EarlyHeld { .. }))
        {
            held_indices += indices.len();
        }
        peer.assert_invariants();
        if peer.is_data_paused() {
            break;
        }
    }
    assert!(peer.is_data_paused(), "the record never filled");
    let held_bytes = held_indices * std::mem::size_of::<u32>();
    assert!(
        held_bytes <= cap,
        "the early record holds {held_bytes} index bytes past its {cap}-byte cap"
    );
}

/// bpv1-60 / bpv1-14: the holder's flagged `Have` ages out of the early
/// record, or leaves it with rejected metadata, before the block is
/// admitted and before its part is read. The part still never closes, and
/// no `Want` goes to the holder for a stripe it may still push.
#[test]
fn bpv1_60_flagged_have_lost_to_age_or_rejected_meta_never_closes() {
    let t0 = Instant::now();
    for rejected in [false, true] {
        let block_id = block::Hash([0x68; 32]);
        let mut peer = PeerState::new(PeerConfig::for_profile(Profile::TINY), node_id(3), 1);
        peer.step(t0, have_with(block_id, HaveFlags::Push, vec![0]));
        let mut now = t0;
        if rejected {
            peer.step(now, recv_meta(block_id));
            peer.step(now, PeerInput::MetaRejected { block_id });
        } else {
            now = t0 + DEFAULT_EARLY_AGE;
            let aged = peer.step(now, PeerInput::Tick);
            assert!(aged
                .events
                .iter()
                .any(|e| matches!(e, PeerEvent::EarlyAged { .. })));
        }
        assert_eq!(peer.early_data_bytes(), 0);
        peer.assert_invariants();

        // Another peer's metadata admits the block.
        now += Duration::from_secs(1);
        admit_tiny(&mut peer, block_id, now);
        let s = peer.step(now, tiny_part_prefix(block_id, 0));
        assert!(peer.close_cause().is_none(), "a conformant holder closed");
        assert_eq!(part_auth(&s), Some(PartAuth::Push));
        // The holder may still push another part of stripe 0.
        assert!(
            sent_want(&peer.step(now, pull(block_id, 0, &[2], now))).is_none(),
            "a Want to the holder could cross its push"
        );
        peer.assert_invariants();
        peer.step(now, PeerInput::SessionEnd);
        peer.assert_drained();
    }
}

/// A node admits a `body_bytes` W2 block that node 250 proposed, with
/// `peer_count` live peers, and publishes it through the production router.
/// Returns the block, its coding, the deadline, and each peer's session.
fn paced_sessions(
    body_bytes: u64,
    peer_count: u8,
    now: Instant,
) -> (block::Hash, Coding, Instant, Vec<(NodeId, PeerState)>) {
    use super::{
        blocks::{BlockOutput, BlockTable},
        route::{route_block_output, BlockRoute},
        test_support::admit_meta,
    };
    let block_id = block::Hash([0x73; 32]);
    let coding = Profile::W2.derive_coding(body_bytes, 2).unwrap();
    let peers: Vec<NodeId> = (1..=peer_count).map(node_id).collect();
    let mut table = BlockTable::new(BlockConfig::for_profile(Profile::W2));
    let envelope = make_envelope(block_id, coding, [0x73; 32], node_id(250));
    let publish = admit_meta(&mut table, now, envelope, 1, &peers)
        .outputs
        .into_iter()
        .find(|o| matches!(o, BlockOutput::PublishAdmitted { .. }))
        .unwrap();
    let BlockRoute::Broadcast(broadcast) = route_block_output(publish) else {
        unreachable!("PublishAdmitted broadcasts");
    };
    let sessions = peers
        .into_iter()
        .map(|peer| {
            let mut state = PeerState::new(PeerConfig::for_profile(Profile::W2), peer, 1);
            state.step(now, broadcast.input_for(&peer));
            (peer, state)
        })
        .collect();
    let deadline = now + BlockConfig::for_profile(Profile::W2).have_pacing.delay;
    (block_id, coding, deadline, sessions)
}

/// The unflagged `Have` batches in `step`.
fn haves(step: &PeerStep) -> Vec<Vec<u32>> {
    step.outputs
        .iter()
        .filter_map(|o| match o {
            PeerOutput::SendHave {
                flags: HaveFlags::Plain,
                indices,
                ..
            } => Some(indices.clone()),
            _ => None,
        })
        .collect()
}

/// bpv1-73: with 80 peers and a 2 MB block, the 32 peers of the initial
/// group hear of parts at once. The rest hear nothing before the deadline,
/// then one `Have` each without the parts the skip rules drop.
#[test]
fn decision26_large_block_announces_to_32_then_the_rest_at_the_deadline() {
    let now = Instant::now();
    let (block_id, coding, deadline, mut sessions) = paced_sessions(1_900_000, 80, now);
    assert_eq!(coding.stripes, 1, "a 1.9 MB W2 block is one stripe");
    let real: Vec<u32> = (0..coding.total_parts().unwrap())
        .filter(|&i| !coding.is_padding(i))
        .collect();
    let mut early = 0;
    let mut delayed = Vec::new();
    for (i, (_, state)) in sessions.iter_mut().enumerate() {
        let mut sent = haves(&state.step(now, ready(block_id, real.clone(), false)));
        for t in [2, 100, 499] {
            sent.extend(haves(
                &state.step(now + Duration::from_millis(t), PeerInput::Tick),
            ));
        }
        if sent.is_empty() {
            delayed.push(i);
        } else {
            early += 1;
        }
        state.assert_invariants();
    }
    assert_eq!(
        early, 32,
        "only the initial group hears before the deadline"
    );

    // Meanwhile every peer announced part 0, and every fourth peer finished
    // the only stripe.
    for (i, (_, state)) in sessions.iter_mut().enumerate() {
        state.step(now, have_with(block_id, HaveFlags::Plain, vec![0]));
        if i % 4 == 0 {
            state.step(
                now,
                PeerInput::RecvStripeDone {
                    block_id,
                    stripe: 0,
                },
            );
        }
    }
    let expected: Vec<u32> = real.iter().copied().filter(|&index| index != 0).collect();
    let mut late = 0;
    for (i, (_, state)) in sessions.iter_mut().enumerate() {
        let s = state.step(deadline, PeerInput::Tick);
        assert!(state.close_cause().is_none());
        let sent = haves(&s);
        if !delayed.contains(&i) {
            assert!(sent.is_empty(), "the initial group has nothing held");
        } else if i % 4 == 0 {
            assert!(sent.is_empty(), "a peer that finished hears nothing");
        } else {
            late += 1;
            assert_eq!(
                sent,
                vec![expected.clone()],
                "one batched Have at the deadline"
            );
            assert!(s
                .events
                .contains(&PeerEvent::HaveSkippedAtDeadline { block_id, count: 1 }));
        }
        state.assert_invariants();
    }
    assert_eq!(
        late,
        delayed.iter().filter(|&&i| i % 4 != 0).count(),
        "one Have per delayed peer"
    );
}

/// bpv1-73: a delayed peer that finishes the block before the deadline
/// never hears of any part.
#[test]
fn decision26_delayed_peer_that_finishes_never_gets_a_have() {
    let now = Instant::now();
    let (block_id, _, deadline, mut sessions) = paced_sessions(1_900_000, 80, now);
    let mut checked = 0;
    for (_, state) in &mut sessions {
        let s = state.step(now, ready(block_id, vec![0, 1, 2], false));
        let s2 = state.step(now + Duration::from_millis(2), PeerInput::Tick);
        if !haves(&s).is_empty() || !haves(&s2).is_empty() {
            continue;
        }
        state.step(now, PeerInput::RecvBlockDone { block_id });
        assert!(haves(&state.step(deadline, PeerInput::Tick)).is_empty());
        checked += 1;
    }
    assert_eq!(checked, 48);
}

/// A session from node 1 to node 3 with the 64-byte TINY block admitted
/// outside its initial group: held indices go out at `deadline`.
fn delayed_session(block_id: block::Hash, now: Instant, deadline: Instant) -> PeerState {
    session(
        node_id(3),
        now,
        PeerInput::BlockAdmitted(BlockAdmitted {
            have_held_until: Some(deadline),
            ..block_admitted(block_id, tiny(), node_id(2))
        }),
    )
}

/// bpv1-73: at the deadline the skip rules rerun in full, so a held part of
/// a stripe the peer can now decode is skipped.
#[test]
fn decision26_deadline_skips_a_stripe_the_peer_can_decode() {
    let coding = tiny();
    let block_id = block::Hash([0x74; 32]);
    let now = Instant::now();
    let deadline = now + Duration::from_millis(500);
    let mut peer = delayed_session(block_id, now, deadline);
    let held = coding.wire_index(0, coding.n - 1).unwrap();
    let s = peer.step(now, ready(block_id, vec![held], false));
    assert!(haves(&s).is_empty());
    // The peer announces `k` other parts of stripe 0.
    peer.step(
        now,
        have_with(
            block_id,
            HaveFlags::Plain,
            (0..coding.k)
                .map(|slot| coding.wire_index(0, slot).unwrap())
                .collect(),
        ),
    );
    let s = peer.step(deadline, PeerInput::Tick);
    assert!(haves(&s).is_empty(), "the peer can decode stripe 0");
    assert!(s
        .events
        .contains(&PeerEvent::HaveSkippedAtDeadline { block_id, count: 1 }));
    peer.assert_invariants();
}

/// bpv1-73 / bpv1-72: a held index that this node later pushes leaves the
/// held set, so the deadline neither announces nor counts it as skipped.
#[test]
fn decision26_pushed_index_leaves_the_held_set() {
    let block_id = block::Hash([0x75; 32]);
    let now = Instant::now();
    let deadline = now + Duration::from_millis(500);
    let mut peer = delayed_session(block_id, now, deadline);
    for push in [false, true] {
        peer.step(now, ready(block_id, vec![0], push));
    }
    let s = peer.step(deadline, PeerInput::Tick);
    assert!(haves(&s).is_empty());
    assert!(
        !s.events
            .iter()
            .any(|e| matches!(e, PeerEvent::HaveSkippedAtDeadline { .. })),
        "a pushed index counted as skipped"
    );
    peer.assert_invariants();
}

/// decision-26: a small block's window covers 10 peers, so all of them
/// hear of parts at once.
#[test]
fn decision26_small_block_with_ten_peers_announces_to_all() {
    let now = Instant::now();
    let (block_id, _, _, mut sessions) = paced_sessions(10_000, 10, now);
    for (_, state) in &mut sessions {
        state.step(now, ready(block_id, vec![0], false));
        let s = state.step(now + Duration::from_millis(2), PeerInput::Tick);
        assert_eq!(haves(&s), vec![vec![0]]);
    }
}

/// The verified TINY part `index` of the 64-byte block `push_session` admits.
fn session_part(index: usize) -> VerifiedPart {
    encode_body(&Profile::TINY, &[0x5B; 64], 2).unwrap().parts[index].clone()
}

/// Delivers `part` from the session's peer and completes its proof. Returns
/// the part's authority and the outputs after proof.
fn deliver(
    peer: &mut PeerState,
    block_id: block::Hash,
    part: &VerifiedPart,
    now: Instant,
) -> (Option<PartAuth>, Vec<PeerOutput>) {
    let prefix = peer.step(
        now,
        PeerInput::RecvPartPrefix(RecvPartPrefix {
            block_id,
            index: part.index(),
            payload: encode_part_frame(block_id, part).payload,
        }),
    );
    let auth = part_auth(&prefix);
    let Some(token) = verify_token(&prefix) else {
        return (auth, prefix.outputs);
    };
    let verified = peer.step(
        now,
        PeerInput::PartVerified {
            token,
            part: part.clone(),
        },
    );
    (auth, verified.outputs)
}

/// 05 §3 "A holder push crosses a pull response": this node already holds
/// the part when the holder's push lands. The push still verifies and goes
/// to the block table as surplus; nothing closes.
#[test]
fn decision24_push_crossing_a_pull_never_closes() {
    let block_id = block::Hash([0x64; 32]);
    let now = Instant::now();
    let mut peer = push_session(node_id(3), block_id);
    peer.step(now, have_with(block_id, HaveFlags::Push, vec![0]));
    // The pull from another peer landed first.
    peer.step(now, ready(block_id, vec![0], false));
    let (auth, outputs) = deliver(&mut peer, block_id, &session_part(0), now);
    assert_eq!(auth, Some(PartAuth::Push));
    assert!(outputs
        .iter()
        .any(|o| matches!(o, PeerOutput::InsertPart { .. })));
    assert!(!peer.is_closed());
    peer.assert_invariants();
}

/// H admits the block from A, then the proposer P's relay flags and seeds
/// reach H ahead of P's `HeaderMeta`. Nothing closes, the seeds count as
/// P's, and H pushes its relay seeds on.
#[test]
fn proposer_seeds_ahead_of_its_header_meta_are_accepted_and_relayed() {
    let peer_cfg = PeerConfig::for_profile(Profile::TINY);
    let mut mesh = MachineMesh::new(4, BlockConfig::for_profile(Profile::TINY));
    let (p, a, h, x) = (0, 1, 2, 3);
    let fast = LinkProfile::new(Duration::from_millis(5), 1_000_000);
    let split = |ctrl: u64, data: u64| LinkProfile {
        ctrl_delay: Duration::from_millis(ctrl),
        data_delay: Duration::from_millis(data),
        ..fast
    };
    mesh.connect(p, a, fast, fast, peer_cfg, None, None);
    // P's metadata reaches H long after P's data; A's metadata reaches H
    // first, and A's data last.
    mesh.connect(p, h, split(300, 40), fast, peer_cfg, None, None);
    mesh.connect(a, h, split(5, 500), fast, peer_cfg, None, None);
    mesh.connect(h, x, fast, fast, peer_cfg, None, None);

    let block_id = block::Hash([0x9A; 32]);
    let encoded = encode_body(&Profile::TINY, &[0x3A; 128], 2).unwrap();
    let envelope = make_envelope(
        block_id,
        encoded.coding,
        encoded.part_root,
        mesh.nodes[p].id,
    );
    mesh.propose(p, envelope, encoded.parts);
    let start = mesh.now;
    mesh.run_until(start + Duration::from_secs(3));

    mesh.assert_honest_links_open();
    let proposer = mesh.nodes[p].id;
    let early_seeds = mesh.nodes[h]
        .block_events
        .iter()
        .filter(|(at, e)| {
            *at < start + Duration::from_millis(300)
                && matches!(
                    e,
                    BlockEvent::PartInserted { from_peer, source: PartSource::Seed, .. }
                        if *from_peer == proposer
                )
        })
        .count();
    assert!(early_seeds > 0, "H took P's seeds before P's HeaderMeta");
    assert!(
        !mesh.flagged(h, x, &block_id).is_empty(),
        "H pushed its relay seeds on"
    );
    for node in [a, h, x] {
        assert!(mesh.verified(node, &block_id), "node {node} assembled");
    }
}

/// decision-32: every neighbour announces to the proposer each part it
/// holds verified, received or reconstructed, that the proposer did not send
/// it, and never sends the proposer a part. The proposer keeps every link open.
#[test]
fn decision32_neighbours_announce_to_the_proposer_the_parts_it_did_not_send_them() {
    let peer_cfg = PeerConfig::for_profile(Profile::TINY);
    let mut mesh = MachineMesh::new(4, BlockConfig::for_profile(Profile::TINY));
    let p = 0;
    let fast = LinkProfile::new(Duration::from_millis(5), 1_000_000);
    for (a, b) in [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)] {
        mesh.connect(a, b, fast, fast, peer_cfg, None, None);
    }
    let block_id = block::Hash([0x9B; 32]);
    let encoded = encode_body(&Profile::TINY, &[0x3B; 256], 2).unwrap();
    let total = encoded.coding.total_parts().unwrap();
    let envelope = make_envelope(
        block_id,
        encoded.coding,
        encoded.part_root,
        mesh.nodes[p].id,
    );
    mesh.propose(p, envelope, encoded.parts);
    let start = mesh.now;
    mesh.run_until(start + Duration::from_secs(3));

    mesh.assert_honest_links_open();
    let mut heard_from_others = 0;
    for neighbour in 1..4 {
        assert!(mesh.verified(neighbour, &block_id));
        assert!(
            mesh.sent_parts(neighbour, p, &block_id).is_empty(),
            "neighbour {neighbour} never sends the proposer a part"
        );
        let from_proposer = mesh.sent_parts(p, neighbour, &block_id);
        let announced = mesh.announced(neighbour, p, &block_id);
        assert!(
            announced.iter().all(|index| !from_proposer.contains(index)),
            "neighbour {neighbour} announced a part the proposer sent it"
        );
        // A neighbour that decoded holds every part, so it announces every
        // index the proposer did not send it.
        let held: Vec<u32> = (0..total)
            .filter(|index| !from_proposer.contains(index))
            .collect();
        assert_eq!(
            announced, held,
            "neighbour {neighbour} announced every part the proposer lacks a record of sending"
        );
        heard_from_others += announced.len();
    }
    assert!(
        heard_from_others > 0,
        "some neighbour took a part from another peer"
    );
}

/// decision-32: the proposer's session takes a neighbour's plain `Have`s
/// without asking for parts or faulting the neighbour, before and after
/// it retires the block.
#[test]
fn decision32_the_proposer_hears_haves_without_closing() {
    let block_id = block::Hash([0x6C; 32]);
    let now = Instant::now();
    let mut proposer = session(node_id(2), now, proposing(block_id, tiny()));
    proposer.step(now, recv_meta(block_id));

    let step = proposer.step(now, have_with(block_id, HaveFlags::Plain, vec![0, 2]));
    assert!(step.events.contains(&PeerEvent::ProposerHeardHave {
        block_id,
        indices: vec![0, 2],
    }));
    assert!(!step.outputs.iter().any(|o| matches!(
        o,
        PeerOutput::PeerHas { .. } | PeerOutput::CloseProtocol(_) | PeerOutput::CloseNeutralStall
    )));
    proposer.assert_invariants();

    proposer.step(
        now,
        PeerInput::Retire(Retire {
            block_id,
            stripes: tiny().stripes,
            n: tiny().n,
        }),
    );
    proposer.step(now, have_with(block_id, HaveFlags::Plain, vec![1, 3]));
    assert!(proposer.close_cause().is_none());
    proposer.assert_invariants();
}

/// An index this session already announced with a plain `Have`, sent or
/// pending, never gets a second, flagged `Have` (bpv1-25, bpv1-72).
#[test]
fn a_pushed_seed_learned_after_its_plain_have_is_announced_once() {
    let block_id = block::Hash([0x67; 32]);
    let now = Instant::now();
    let haves = |outputs: &[PeerOutput]| -> Vec<(HaveFlags, Vec<u32>)> {
        outputs
            .iter()
            .filter_map(|o| match o {
                PeerOutput::SendHave { flags, indices, .. } => Some((*flags, indices.clone())),
                _ => None,
            })
            .collect()
    };

    // The plain `Have` went out before the relay flag made index 1 a
    // pushed seed: the push finds it announced and does nothing.
    let mut peer = push_session(node_id(3), block_id);
    let first = peer.step(now, ready(block_id, vec![1], false));
    let flushed = peer.step(now + super::wire::HAVE_FLUSH_INTERVAL, PeerInput::Tick);
    assert_eq!(
        [first.outputs, flushed.outputs].concat(),
        vec![PeerOutput::SendHave {
            block_id,
            flags: HaveFlags::Plain,
            form: HaveForm::smaller(tiny().total_parts().unwrap(), 1),
            indices: vec![1],
        }],
        "an unpushed part gets a plain Have and no push"
    );
    let later = peer.step(
        now + super::wire::HAVE_FLUSH_INTERVAL,
        ready(block_id, vec![1], true),
    );
    assert!(later.outputs.is_empty(), "{:?}", later.outputs);
    peer.assert_invariants();

    // The plain `Have` was still pending: the flagged `Have` replaces it.
    let mut peer = push_session(node_id(3), block_id);
    let first = peer.step(now, ready(block_id, vec![1], false));
    let pushed = peer.step(now, ready(block_id, vec![1], true));
    let flushed = peer.step(now + super::wire::HAVE_FLUSH_INTERVAL, PeerInput::Tick);
    let all: Vec<PeerOutput> = [first.outputs, pushed.outputs, flushed.outputs].concat();
    assert_eq!(haves(&all), vec![(HaveFlags::Push, vec![1])]);
    peer.assert_invariants();
}

/// bpv1-60 on the proposer's connection: its relay `Have` ages out of the
/// early record. Every seed of the stripe, up to `n`, still lands without a
/// close; the relay mark is lost, so this node only announces them.
#[test]
fn bpv1_60_lost_proposer_flags_admit_up_to_n_seeds() {
    let t0 = Instant::now();
    let block_id = block::Hash([0x69; 32]);
    // Node 2 proposed the block that `admit_tiny` admits.
    let mut peer = PeerState::new(PeerConfig::for_profile(Profile::TINY), node_id(2), 1);
    peer.step(t0, have_with(block_id, HaveFlags::Relay, vec![0, 1]));
    let now = t0 + DEFAULT_EARLY_AGE;
    peer.step(now, PeerInput::Tick);
    assert_eq!(peer.early_data_bytes(), 0);
    let admitted = admit_tiny(&mut peer, block_id, now);
    assert!(!admitted
        .outputs
        .iter()
        .any(|o| matches!(o, PeerOutput::PushFlagged { .. })));
    for index in 0..tiny().n {
        let s = peer.step(now, tiny_part_prefix(block_id, index));
        assert_eq!(part_auth(&s), Some(PartAuth::Seed), "seed {index}");
    }
    assert!(peer.close_cause().is_none());
    peer.assert_invariants();
}

/// A proposer session from node 1 to node 2 with its own `body` admitted,
/// holding `copies` of it.
fn copy_session(body: &[u8], copies: &[u32]) -> (PeerState, block::Hash, Coding) {
    let encoded = encode_body(&Profile::TINY, body, 2).unwrap();
    let block_id = block::Hash([0x70; 32]);
    let t0 = Instant::now();
    let mut peer = session(node_id(2), t0, proposing(block_id, encoded.coding));
    peer.step(
        t0,
        PeerInput::Seeds(Seeds {
            block_id,
            seeds: Vec::new(),
            copies: copies
                .iter()
                // Widening u32 to usize is lossless on supported targets.
                .map(|&index| encoded.parts[index as usize].clone())
                .collect(),
        }),
    );
    (peer, block_id, encoded.coding)
}

/// Steps `DataIdle` and returns the flagged `Have`'s indices and the copies
/// behind it, in write order.
fn release(peer: &mut PeerState) -> (Vec<u32>, Vec<u32>) {
    let s = peer.step(Instant::now(), PeerInput::DataIdle);
    let mut flagged = Vec::new();
    let mut parts = Vec::new();
    for output in s.outputs {
        match output {
            PeerOutput::SendHave { flags, indices, .. } => {
                assert!(parts.is_empty(), "the flagged Have leads its copies");
                assert_eq!(flags, HaveFlags::Push, "a copy carries push only");
                flagged.extend(indices);
            }
            PeerOutput::SendSeedPart { part, .. } => parts.push(part.index()),
            other => panic!("unexpected output {other:?}"),
        }
    }
    (flagged, parts)
}

/// decision-19: an idle writer takes copies in batches of at most
/// `COPY_BATCH`, each behind its own push-flagged `Have`, and no stripe
/// gets more than `k` parts.
#[test]
fn copies_go_out_in_flagged_batches_up_to_k_a_stripe() {
    // A TINY body large enough for several stripes of real parts.
    let body = [0x71; 2_000];
    let coding = encode_body(&Profile::TINY, &body, 2).unwrap().coding;
    assert!(coding.stripes >= 2);
    let all: Vec<u32> = (0..coding.total_parts().unwrap())
        .filter(|&i| !coding.is_padding(i))
        .collect();
    let (mut peer, _, _) = copy_session(&body, &all);
    let mut sent = Vec::new();
    while peer.has_copies() {
        let (flagged, parts) = release(&mut peer);
        assert!(parts.len() <= COPY_BATCH);
        assert_eq!(flagged, {
            let mut sorted = parts.clone();
            sorted.sort_unstable();
            sorted
        });
        sent.extend(parts);
    }
    assert!(!sent.is_empty());
    for stripe in 0..coding.stripes {
        // A stripe has at most `MAX_N` parts, so the count fits u32.
        let in_stripe = sent
            .iter()
            .filter(|&&i| coding.split_index(i).unwrap().0 == stripe)
            .count() as u32;
        // A stripe has at most `MAX_N` parts, so the count fits u32.
        let real = all
            .iter()
            .filter(|&&i| coding.split_index(i).unwrap().0 == stripe)
            .count() as u32;
        assert_eq!(in_stripe, coding.k.min(real), "stripe {stripe}");
    }
    peer.assert_invariants();
}

/// decision-19: a copy drops once the peer announced the part, finished
/// its stripe, or finished the block.
#[test]
fn copies_stop_once_the_peer_announces_or_finishes() {
    let now = Instant::now();
    // The peer announces part 0: its copy drops, part 1's goes out.
    let (mut peer, block_id, _) = copy_session(&[0x72; 64], &[0, 1]);
    peer.step(now, have_with(block_id, HaveFlags::Plain, vec![0]));
    assert_eq!(release(&mut peer), (vec![1], vec![1]));
    assert!(!peer.has_copies());

    // The peer finished stripe 0: no copy of it goes out.
    let (mut peer, block_id, _) = copy_session(&[0x72; 64], &[0, 1]);
    peer.step(
        now,
        PeerInput::RecvStripeDone {
            block_id,
            stripe: 0,
        },
    );
    assert_eq!(release(&mut peer), (Vec::new(), Vec::new()));
    assert!(!peer.has_copies());

    // The peer finished the block: the copies are gone at once.
    let (mut peer, block_id, _) = copy_session(&[0x72; 64], &[0, 1]);
    peer.step(now, PeerInput::RecvBlockDone { block_id });
    assert!(!peer.has_copies());
    peer.assert_invariants();
}

/// decision-19: the proposer flagged a copy and then skipped it, because
/// the receiver announced the part meanwhile. The receiver never closes,
/// and a later part of the stripe still lands.
#[test]
fn a_skipped_flagged_copy_never_closes_the_receiver() {
    let block_id = block::Hash([0x73; 32]);
    let now = Instant::now();
    // Node 2 proposed the block; this session is the receiver's.
    let mut peer = push_session(node_id(2), block_id);
    peer.step(now, have_with(block_id, HaveFlags::Push, vec![0, 1, 2]));
    let s = peer.step(now, tiny_part_prefix(block_id, 0));
    assert_eq!(part_auth(&s), Some(PartAuth::Seed));
    // Part 1 never comes; part 2 does.
    let s = peer.step(now, tiny_part_prefix(block_id, 2));
    assert_eq!(part_auth(&s), Some(PartAuth::Seed));
    peer.step(now + Duration::from_secs(5), PeerInput::Tick);
    assert!(peer.close_cause().is_none());
    assert!(!peer.is_closed());
    peer.assert_invariants();
    peer.step(now, PeerInput::SessionEnd);
    peer.assert_drained();
}

/// decision-19: a neighbour's direct copies start once its own seeds are
/// written, while a slower neighbour's seeds are still going out, and every
/// node still assembles the block.
#[test]
fn copies_start_before_a_slower_neighbours_seeds_finish() {
    let peer_cfg = PeerConfig::for_profile(Profile::TINY);
    let mut mesh = MachineMesh::new(3, BlockConfig::for_profile(Profile::TINY));
    mesh.direct_copies = true;
    let (p, fast, slow) = (0, 1, 2);
    let link = |rate: u64| LinkProfile::new(Duration::from_millis(10), rate);
    let estimate = |rate: u64| {
        Some(super::paths::PathEstimate {
            min_rtt: Some(Duration::from_millis(20)),
            rate_in: Some(rate),
            rate_out: Some(rate),
        })
    };
    mesh.connect(
        p,
        fast,
        link(1_000_000),
        link(1_000_000),
        peer_cfg,
        estimate(1_000_000),
        estimate(1_000_000),
    );
    mesh.connect(
        p,
        slow,
        link(600_000),
        link(600_000),
        peer_cfg,
        estimate(600_000),
        estimate(600_000),
    );
    mesh.connect(
        fast,
        slow,
        link(1_000_000),
        link(1_000_000),
        peer_cfg,
        None,
        None,
    );

    let block_id = block::Hash([0x74; 32]);
    let encoded = encode_body(&Profile::TINY, &[0x75; 2_000], 2).unwrap();
    let envelope = make_envelope(
        block_id,
        encoded.coding,
        encoded.part_root,
        mesh.nodes[p].id,
    );
    mesh.propose(p, envelope, encoded.parts);
    let fast_seeds_done = mesh.data_idle_at(p, fast);
    let slow_seeds_done = mesh.data_idle_at(p, slow);
    assert!(fast_seeds_done < slow_seeds_done);
    let start = mesh.now;
    mesh.run_until(start + Duration::from_secs(3));

    mesh.assert_honest_links_open();
    let first_copy = mesh.nodes[p]
        .copy_batches
        .iter()
        .find(|&&(_, to, _)| to == fast)
        .map(|&(at, ..)| at)
        .expect("the fast neighbour got direct copies");
    assert!(first_copy >= fast_seeds_done, "copies follow the own seeds");
    assert!(
        first_copy < slow_seeds_done,
        "copies need no global barrier"
    );
    for node in [fast, slow] {
        assert!(mesh.verified(node, &block_id), "node {node} assembled");
    }
}
