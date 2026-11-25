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
use zakura_header_chain::Frontier;
use zakura_network::zakura::bpv1::{
    encode_body, encode_part_frame, Arrival, BlockAdmitted, BlockPhase, Bpv1Message,
    CoinbaseBinding, PeerConfig, PeerEvent, PeerInput, PeerOutput, PeerState, PeerStep, Profile,
    PullGranted, RecvPartPrefix, RecvWantEnd, VerifiedPart, WantBitmap, LIVENESS_COOLDOWN,
    PROPAGATION_DEPTH,
};
use zakura_quic::{NodeId, NodeSecretKey};
use zakura_rt::{
    sync::mpsc,
    time::{self, Instant},
};
use zakura_sim::{
    harness::bpv1_mesh::{Answer, Mesh, MeshScenario, Tally, Via},
    observe::Exit,
    IdleStatus, NoChecks, Observation, Outcome, Plan,
};

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

// ============================================================================
// Catch-up handoff between block sync and BPv1 (#2718)
// ============================================================================

/// One mined block: its height, hash, and how far above the node's verified
/// tip it was when mirror 0 published it.
#[derive(Clone, Copy, Debug)]
struct Mined {
    height: block::Height,
    hash: block::Hash,
    gap: u32,
}

/// What a director task saw: its mined blocks, why it stopped early, the
/// tally, and both tips at the end.
struct Seen {
    mined: Vec<Mined>,
    failure: Option<String>,
    tally: Tally,
    node_tip: Frontier,
    tip: Frontier,
    phases: Vec<Option<BlockPhase>>,
}

/// How often a director checks the mesh, in virtual time.
const POLL: Duration = Duration::from_millis(10);

/// Waits until `done` holds, checking every [`POLL`], or fails after
/// `limit` of virtual time.
async fn until(what: &str, limit: Duration, mut done: impl FnMut() -> bool) -> Result<(), String> {
    let deadline = Instant::now() + limit;
    while !done() {
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting until {what}"));
        }
        time::sleep(POLL).await;
    }
    Ok(())
}

/// A director's view of the mesh: the mesh and the blocks it mined.
struct Director {
    mesh: Rc<Mesh>,
    mined: RefCell<Vec<Mined>>,
}

impl Director {
    /// Mines the next block and records its gap.
    async fn mine(&self) -> Mined {
        let gap = self.mesh.tip().height.0 + 1 - self.mesh.node_tip().height.0;
        let block = self.mesh.mine().await;
        let block = Mined {
            height: block.height,
            hash: block.hash(),
            gap,
        };
        self.mined.borrow_mut().push(block);
        block
    }

    /// Waits until the node's verified tip is `block`.
    async fn committed(&self, block: Mined) -> Result<(), String> {
        let what = format!("the node commits h{}", block.height.0);
        until(&what, Duration::from_secs(60), || {
            self.mesh.node_tip().hash == block.hash
        })
        .await
    }
}

/// Runs `script` on a mesh built from `scenario` and returns what it saw.
/// The mesh shuts down when the script returns, whether it failed or not.
fn run_mesh<F, Fut>(scenario: MeshScenario, script: F) -> Seen
where
    F: FnOnce(Rc<Director>) -> Fut + 'static,
    Fut: std::future::Future<Output = Result<(), String>> + 'static,
{
    let seen = Rc::new(RefCell::new(None));
    let run_config = zakura_sim::RunConfig {
        scheduler: zakura_scheduler::SchedulerConfig {
            workers: 2,
            ..zakura_scheduler::SchedulerConfig::default()
        },
        budget: 2_000_000,
        drain_bound: 80_000,
        ..zakura_sim::RunConfig::default()
    };
    let out = seen.clone();
    let report = run(
        run_config,
        Plan::from_fuzz(&[]),
        move |sim| {
            let director = Rc::new(Director {
                mesh: Mesh::build(sim, &scenario),
                mined: RefCell::default(),
            });
            sim.scheduler().spawn_root("director", async move {
                let failure = script(director.clone()).await.err();
                // Late answers, such as a duplicate delivery's, land first.
                time::sleep(Duration::from_secs(2)).await;
                let mesh = &director.mesh;
                let mined = director.mined.take();
                *out.borrow_mut() = Some(Seen {
                    phases: mined.iter().map(|block| mesh.phase(block.hash)).collect(),
                    mined,
                    failure,
                    tally: mesh.state.tally(),
                    node_tip: mesh.node_tip(),
                    tip: mesh.tip(),
                });
                mesh.shut_down();
            });
        },
        &mut NoChecks,
    );
    let seen = seen.borrow_mut().take();
    let seen =
        seen.unwrap_or_else(|| panic!("the director finished; the run ended {:?}", report.end));
    if let Some(failure) = &seen.failure {
        panic!("{failure}\n{}", summary(&seen));
    }
    seen
}

/// Each delivery of `hash` and the node's answer to it.
fn answers(seen: &Seen, hash: block::Hash) -> Vec<(Via, Answer)> {
    seen.tally
        .answers
        .iter()
        .filter(|(_, answered, _)| *answered == hash)
        .map(|(via, _, answer)| (*via, *answer))
        .collect()
}

/// One line per mined block, for a failure message.
fn summary(seen: &Seen) -> String {
    let mut lines = format!(
        "node at h{}, tip h{}, queue full {}, records {}\n",
        seen.node_tip.height.0, seen.tip.height.0, seen.tally.queue_full, seen.tally.records
    );
    for (block, phase) in seen.mined.iter().zip(&seen.phases) {
        lines.push_str(&format!(
            "h{} gap {} via {:?} bpv1 {} sync {} fetched {} verified {} phase {phase:?}\n",
            block.height.0,
            block.gap,
            seen.tally.committed.get(&block.hash),
            seen.tally.submissions(Via::Bpv1, block.hash),
            seen.tally.submissions(Via::BlockSync, block.hash),
            seen.tally.fetches(block.height),
            seen.tally.verified.get(&block.hash).copied().unwrap_or(0),
        ));
    }
    lines
}

/// The checks every mesh run passes: the node reached the tip, no body was
/// downloaded or verified twice, BPv1 submitted no hash twice, the orphan
/// queue never refused a block, and block sync recorded no failure.
fn assert_clean(seen: &Seen) {
    let summary = summary(seen);
    assert_eq!(
        seen.node_tip, seen.tip,
        "the node reaches the tip\n{summary}"
    );
    assert_eq!(seen.tally.queue_full, 0, "no QueueFull\n{summary}");
    assert_eq!(seen.tally.records, 0, "no body record\n{summary}");
    assert!(
        seen.tally.served.values().all(|&count| count == 1),
        "no body downloaded twice: {:?}\n{summary}",
        seen.tally.served
    );
    assert!(
        seen.tally.verified.values().all(|&count| count == 1),
        "no block verified twice\n{summary}"
    );
    for block in &seen.mined {
        assert!(
            seen.tally.submissions(Via::Bpv1, block.hash) <= 1,
            "BPv1 submits h{} at most once\n{summary}",
            block.height.0
        );
    }
}

/// A node that joins 200 blocks behind a two-mirror BPv1 mesh while mirror
/// 0 keeps mining. Mirror 0 mines when the node starts, and again each time
/// the node's verified tip first comes within 100, 40, 12 and 4 blocks of
/// the next height, then four more blocks once the node holds the tip.
///
/// A block more than `PROPAGATION_DEPTH` above the verified tip belongs to
/// block sync (bpv1-76). Every later block commits through BPv1, with one
/// submission and no block-sync fetch (bpv1-44), even when it arrives
/// before its parent commits: the verifier holds it until the parent
/// commits. So BPv1 resumes with the first block inside its window.
#[test]
fn bpv1_76_82_a_node_far_behind_catches_up_and_hands_off_to_bpv1() {
    let scenario = MeshScenario {
        blocks: 200,
        node_blocks: 0,
        mirrors: 2,
        body_bytes: 2_000,
        verify_delay: Duration::from_millis(20),
    };
    let seen = run_mesh(scenario, |director| async move {
        let mesh = &director.mesh;
        let mut gaps = [100, 40, 12, 4].into_iter().peekable();
        director.mine().await;
        let limit = Instant::now() + Duration::from_secs(600);
        while mesh.node_tip() != mesh.tip() || gaps.peek().is_some() {
            if Instant::now() >= limit {
                return Err("the node never caught up".to_owned());
            }
            let next = mesh.tip().height.0 + 1 - mesh.node_tip().height.0;
            if gaps.next_if(|&gap| next <= gap).is_some() {
                director.mine().await;
            }
            time::sleep(POLL).await;
        }
        for _ in 0..4 {
            let block = director.mine().await;
            director.committed(block).await?;
        }
        Ok(())
    });
    let summary = summary(&seen);
    assert_clean(&seen);
    for block in &seen.mined {
        let via = seen.tally.committed.get(&block.hash).copied();
        if block.gap > PROPAGATION_DEPTH {
            assert_eq!(
                via,
                Some(Via::BlockSync),
                "block sync owns h{} {} above the verified tip\n{summary}",
                block.height.0,
                block.gap
            );
            assert_eq!(seen.tally.submissions(Via::Bpv1, block.hash), 0);
        } else {
            assert_eq!(
                via,
                Some(Via::Bpv1),
                "BPv1 commits h{} {} above the verified tip\n{summary}",
                block.height.0,
                block.gap
            );
            assert_eq!(seen.tally.submissions(Via::Bpv1, block.hash), 1);
            assert_eq!(seen.tally.fetches(block.height), 0, "no fetch\n{summary}");
        }
    }
    assert!(
        seen.mined.iter().any(|block| block.gap > PROPAGATION_DEPTH),
        "some block arrives outside the window\n{summary}"
    );
    assert!(
        seen.mined
            .iter()
            .any(|block| block.gap > 1 && block.gap <= PROPAGATION_DEPTH),
        "some block arrives inside the window before its parent commits\n{summary}"
    );
}

/// A node at the tip of a two-mirror mesh.
fn at_tip() -> MeshScenario {
    MeshScenario {
        blocks: 20,
        node_blocks: 20,
        mirrors: 2,
        body_bytes: 2_000,
        verify_delay: Duration::from_millis(20),
    }
}

/// Mirror 0 withholds every part of a block it mines. BPv1 admits the block
/// and claims its body. Its first repair round releases the claim
/// (decision-22), and block sync fetches the body once and commits it. When
/// the parts arrive later, BPv1's copy is a duplicate the node doesn't
/// verify again. The next block commits through BPv1.
#[test]
fn bpv1_57_withheld_parts_fall_back_to_block_sync() {
    let seen = run_mesh(at_tip(), |director| async move {
        director.mesh.hold(0, true);
        let withheld = director.mine().await;
        director.committed(withheld).await?;
        director.mesh.hold(0, false);
        let next = director.mine().await;
        director.committed(next).await
    });
    let summary = summary(&seen);
    assert_clean(&seen);
    let [withheld, next] = seen.mined[..] else {
        panic!("the director mines two blocks\n{summary}");
    };
    assert_eq!(
        seen.tally.committed.get(&withheld.hash),
        Some(&Via::BlockSync),
        "block sync commits the withheld block\n{summary}"
    );
    assert_eq!(
        answers(&seen, withheld.hash),
        [
            (Via::BlockSync, Answer::Committed),
            (Via::Bpv1, Answer::Duplicate)
        ],
        "block sync commits the withheld block, and BPv1's late copy is a duplicate\n{summary}"
    );
    assert_eq!(seen.tally.fetches(withheld.height), 1, "{summary}");
    assert_eq!(
        seen.tally.committed.get(&next.hash),
        Some(&Via::Bpv1),
        "BPv1 commits the next block\n{summary}"
    );
    assert_eq!(seen.tally.fetches(next.height), 0, "{summary}");
}

/// Mirror 0 withholds a block's parts until the first repair round
/// releases the body claim and block sync fetches the body, then sends
/// them. BPv1 and block sync both deliver the block, and the first
/// submission wins: the node verifies the block once, commits it once, and
/// answers the other delivery as a duplicate.
#[test]
fn bpv1_57_a_repair_round_race_commits_once() {
    let seen = run_mesh(at_tip(), |director| async move {
        let mesh = &director.mesh;
        mesh.hold(0, true);
        let raced = director.mine().await;
        until("BPv1 claims the body", Duration::from_secs(10), || {
            mesh.node.claims.is_claimed(raced.hash, Instant::now())
        })
        .await?;
        until(
            "the first repair round releases the claim",
            Duration::from_secs(15),
            || !mesh.node.claims.is_claimed(raced.hash, Instant::now()),
        )
        .await?;
        until(
            "block sync fetches the body",
            Duration::from_secs(15),
            || mesh.state.tally().fetches(raced.height) > 0,
        )
        .await?;
        mesh.hold(0, false);
        director.committed(raced).await
    });
    let summary = summary(&seen);
    assert_clean(&seen);
    let raced = seen.mined[0];
    let answers = answers(&seen, raced.hash);
    let delivered = |via| answers.iter().filter(|answer| answer.0 == via).count();
    assert_eq!(
        (delivered(Via::Bpv1), delivered(Via::BlockSync)),
        (1, 1),
        "both paths deliver the block once: {answers:?}\n{summary}"
    );
    let mut outcomes: Vec<_> = answers.iter().map(|answer| answer.1).collect();
    outcomes.sort_by_key(|answer| *answer == Answer::Duplicate);
    assert_eq!(
        outcomes,
        [Answer::Committed, Answer::Duplicate],
        "the first delivery commits and the other is a duplicate\n{summary}"
    );
}
