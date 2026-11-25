//! Level-9 BPv1 session timing on the simulator's scheduler (cov-6).
//!
//! Each session task sleeps until its `PeerState`'s own next deadline in
//! virtual time, as the IO shell does, so a missed wake shows up as a
//! deadlocked run. A stalled session parks and then closes neutrally
//! (`bpv1-58`) while a responsive session beside it completes its `Want`
//! on time (`bpv1-56`). Both drain on session end (`bpv1-62`).
#![cfg(zakura_sim)]

mod support;

use std::{cell::RefCell, rc::Rc, time::Duration};

use zakura_chain::block;
use zakura_network::zakura::bpv1::{
    encode_body, encode_part_frame, Arrival, BlockAdmitted, Bpv1Message, CoinbaseBinding,
    PeerConfig, PeerEvent, PeerInput, PeerOutput, PeerState, PeerStep, Profile, PullGranted,
    RecvPartPrefix, RecvWantEnd, VerifiedPart, WantBitmap, LIVENESS_COOLDOWN,
};
use zakura_quic::{NodeId, NodeSecretKey};
use zakura_rt::{
    sync::mpsc,
    time::{self, Instant},
};
use zakura_sim::{observe::Exit, IdleStatus, NoChecks, Observation, Outcome, Plan};

use support::{config, run};

fn node_id(b: u8) -> NodeId {
    NodeSecretKey::from_bytes(&[b; 32]).public()
}

/// A session with `peer` that has admitted `block_id`, sent its
/// `HeaderMeta`, and sent a `Want` for slot 0 of stripe 0. Returns the
/// session, the `Want`'s id, and the block's parts.
fn session_with_want(peer: u8, block_id: block::Hash) -> (PeerState, u32, Vec<VerifiedPart>) {
    let encoded = encode_body(&Profile::TINY, &[0x42; 64], 2).expect("TINY body encodes");
    let coding = encoded.coding;
    let now = Instant::now();
    let mut state = PeerState::new(PeerConfig::for_profile(Profile::TINY), node_id(peer), 1);
    let meta = Box::new(Bpv1Message::HeaderMeta {
        header: block_id.0.to_vec(),
        binding: CoinbaseBinding {
            coinbase: vec![0x01],
            txid_siblings: Vec::new(),
            auth_siblings: Vec::new(),
            light_client_root: [0u8; 32],
        },
        coding,
        part_root: encoded.part_root,
        proposer_node: node_id(9),
    });
    state.step(
        now,
        PeerInput::BlockAdmitted(BlockAdmitted {
            block_id,
            coding,
            proposer_node: node_id(9),
            header_meta: Some(meta),
            as_proposer: false,
            have_held_until: None,
        }),
    );
    let mut requested = WantBitmap::EMPTY;
    requested.set(0);
    let granted = state.step(
        now,
        PeerInput::PullGranted(PullGranted {
            block_id,
            stripe: 0,
            requested,
            arrival: Arrival {
                at: now + Duration::from_millis(50),
                rtt: Duration::from_millis(20),
                rate_bps: 1_000_000,
            },
        }),
    );
    let want_id = granted
        .outputs
        .iter()
        .find_map(|output| match output {
            PeerOutput::SendControl(msg) => match **msg {
                Bpv1Message::Want { want_id, .. } => Some(want_id),
                _ => None,
            },
            _ => None,
        })
        .expect("a granted pull sends a Want");
    (state, want_id, encoded.parts)
}

/// Steps `state`, records its events with the virtual instant, and returns
/// the step.
fn step(
    state: &mut PeerState,
    log: &RefCell<Vec<(Instant, PeerEvent)>>,
    input: PeerInput,
) -> PeerStep {
    let now = Instant::now();
    let step = state.step(now, input);
    state.assert_invariants();
    log.borrow_mut()
        .extend(step.events.iter().cloned().map(|event| (now, event)));
    step
}

#[test]
fn bpv1_56_58_62_stalled_session_parks_then_closes_beside_a_live_one() {
    let block_id = block::Hash([0x58; 32]);
    let stalled_log = Rc::new(RefCell::new(Vec::new()));
    let live_done = Rc::new(RefCell::new(None));
    let start = Rc::new(RefCell::new(None));

    let mut run_config = config(1, 1_000_000);
    // The machines and the TINY encoder need more than the support crate's
    // 64 KiB task stacks.
    run_config.scheduler.stack_size = 1 << 20;
    let report = run(
        run_config,
        Plan::from_fuzz(&[]),
        |sim| {
            let (log, begun) = (stalled_log.clone(), start.clone());
            sim.scheduler().spawn_root("stalled", async move {
                *begun.borrow_mut() = Some(Instant::now());
                let (mut state, _, _) = session_with_want(2, block_id);
                // The peer never answers: only the machine's own deadlines wake it.
                loop {
                    let deadline = state
                        .next_deadline()
                        .expect("an open session with a live Want has a deadline");
                    time::sleep_until(deadline).await;
                    let ticked = step(&mut state, &log, PeerInput::Tick);
                    if ticked.outputs.contains(&PeerOutput::CloseNeutralStall) {
                        break;
                    }
                }
                assert_eq!(state.close_cause(), None, "a stall is no protocol fault");
                step(&mut state, &log, PeerInput::SessionEnd);
                state.assert_drained();
            });

            let (to_peer, mut from_session) = mpsc::unbounded_channel::<u32>();
            let (to_session, mut from_peer) = mpsc::unbounded_channel::<PeerInput>();
            sim.scheduler().spawn_root("responder", async move {
                let parts = encode_body(&Profile::TINY, &[0x42; 64], 2)
                    .expect("TINY body encodes")
                    .parts;
                let frame = encode_part_frame(block_id, &parts[0]).payload;
                // Answers each `Want` with its one part and an end, 10 ms later.
                while let Some(want_id) = from_session.recv().await {
                    time::sleep(Duration::from_millis(10)).await;
                    let _ = to_session.send(PeerInput::RecvPartPrefix(RecvPartPrefix {
                        block_id,
                        index: 0,
                        payload: frame.clone(),
                    }));
                    let _ =
                        to_session.send(PeerInput::RecvWantEnd(RecvWantEnd { want_id, sent: 1 }));
                }
            });
            let done = live_done.clone();
            sim.scheduler().spawn_root("live", async move {
                let log = RefCell::new(Vec::new());
                let (mut state, want_id, parts) = session_with_want(3, block_id);
                let _ = to_peer.send(want_id);
                while let Some(input) = from_peer.recv().await {
                    let stepped = step(&mut state, &log, input);
                    for output in stepped.outputs {
                        match output {
                            PeerOutput::VerifyPart { token, .. } => {
                                step(
                                    &mut state,
                                    &log,
                                    PeerInput::PartVerified {
                                        token,
                                        part: parts[0].clone(),
                                    },
                                );
                            }
                            PeerOutput::WantCompleted { .. } => {
                                *done.borrow_mut() = Some(Instant::now());
                            }
                            _ => {}
                        }
                    }
                    if done.borrow().is_some() {
                        break;
                    }
                }
                drop(to_peer);
                step(&mut state, &log, PeerInput::SessionEnd);
                state.assert_drained();
            });
        },
        &mut NoChecks,
    );

    assert_eq!(
        report.end.outcome,
        Outcome::Idle {
            status: IdleStatus::Quiescent
        },
        "every session task finished; none waited on a missed deadline"
    );
    // Quiescence counts a panicked task as finished, so check each exit.
    for task in ["stalled.1", "responder.1", "live.1"] {
        let exit = report.log.entries().iter().find_map(|e| match &e.obs {
            Observation::TaskExited { task: t, exit } if t == task => Some(*exit),
            _ => None,
        });
        assert_eq!(exit, Some(Exit::Completed), "{task} completed");
    }

    let t0 = start.borrow().expect("the stalled session started");
    // bpv1-56: the live session finished on its peer's pace, while the
    // stalled one had not even fired its late pull.
    let finished = live_done.borrow().expect("the live Want completed");
    assert!(finished.saturating_duration_since(t0) < Duration::from_millis(50));

    // bpv1-58: late, deadline, park, unpark after the cooldown, then close.
    let log = stalled_log.borrow();
    let first = |want: fn(&PeerEvent) -> bool| {
        log.iter()
            .find(|(_, e)| want(e))
            .map(|(at, _)| *at)
            .expect("the stalled session emitted each stage")
    };
    let late = first(|e| matches!(e, PeerEvent::WantLate { .. }));
    let deadline = first(|e| matches!(e, PeerEvent::WantDeadline { .. }));
    let parked = first(|e| *e == PeerEvent::Parked);
    let unparked = first(|e| *e == PeerEvent::Unparked);
    assert!(late < deadline && deadline < parked && parked < unparked);
    assert!(unparked.saturating_duration_since(parked) >= LIVENESS_COOLDOWN);
}
