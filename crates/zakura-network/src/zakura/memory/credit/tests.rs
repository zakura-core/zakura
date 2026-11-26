//! Tests drive [`CreditMachine`] directly: inputs in, decisions out, no sink.

use std::collections::VecDeque;

use proptest::prelude::*;

use super::*;

const MIB: u64 = 1024 * 1024;
const BASE_RECEIVE: u64 = 4 * MIB;
const BASE_SEND: u64 = 2 * MIB;
const PER_PEER_CAP: u64 = 16 * MIB;
const PEER_FLOOR: u64 = 2 * MIB;

/// A machine and the `set_limits` calls the test answered for it.
struct Harness {
    machine: CreditMachine,
    seq: u64,
}

impl Harness {
    fn new() -> Self {
        Self {
            machine: CreditMachine::new(BASE_RECEIVE, BASE_SEND),
            seq: 0,
        }
    }

    /// Answers `limits`, if any, with the next `set_limits` call.
    fn sent(&mut self, limits: Option<ConnLimits>) -> Option<ConnLimits> {
        if limits.is_some() {
            self.seq += 1;
            self.machine.limits_sent(self.seq);
        }
        limits
    }

    fn add(&mut self, bytes: u64) -> Option<ConnLimits> {
        let limits = self.machine.add_receive(bytes);
        self.sent(limits)
    }

    /// Finishes a request of `total` bytes with `unspent` of them leased, and
    /// returns whether the lease drains.
    fn finish(&mut self, total: u64, unspent: u64) -> bool {
        let finish = self.machine.finish_receive(total, Some(unspent));
        self.sent(finish.limits);
        finish.drains
    }

    fn tick(&mut self, load: CreditLoad) -> TickStep {
        let step = self.machine.tick(load, PER_PEER_CAP, PEER_FLOOR);
        if let SendStep::Shrink { limits, .. } = step.send {
            self.sent(Some(limits));
        }
        step
    }

    fn grown(&mut self, bytes: u64) -> Option<ConnLimits> {
        let limits = self.machine.send_grown(bytes);
        self.sent(limits)
    }
}

fn receive(count: usize) -> DrainRelease {
    DrainRelease {
        receive: count,
        ..DrainRelease::default()
    }
}

fn snapshot(limits_applied: u64, receive_credit_excess: u64) -> CreditLoad {
    CreditLoad {
        limits_applied,
        receive_credit_excess,
        ..CreditLoad::default()
    }
}

#[test]
fn receive_credit_raises_the_window_and_drains_unspent_credit_against_the_excess() {
    let mut harness = Harness::new();
    assert_eq!(
        harness.add(2 * MIB),
        Some(
            ConnLimits::default()
                .with_receive_window(BASE_RECEIVE + 2 * MIB)
                .with_send_window(BASE_SEND)
        )
    );

    // RangeUnavailable: the window drops at once, the lease drains.
    assert!(harness.finish(2 * MIB, 2 * MIB));
    assert_eq!(harness.machine.receive_window(), BASE_RECEIVE);
    let first_seq = harness.seq;

    harness.add(MIB);
    assert!(harness.finish(MIB, MIB));

    // The peer may still spend all 3 MiB, then 2 MiB: only both drains
    // together cover that.
    let none = DrainRelease::default();
    assert_eq!(harness.machine.drain(snapshot(harness.seq, 3 * MIB)), none);
    assert_eq!(harness.machine.drain(snapshot(harness.seq, 2 * MIB)), none);
    assert_eq!(harness.machine.drain(snapshot(harness.seq, MIB + 1)), none);
    // 1 MiB remains: the later drain covers it, so the oldest releases.
    assert_eq!(
        harness.machine.drain(snapshot(harness.seq, MIB)),
        receive(1)
    );
    // fix-11: a snapshot older than the shrink shows the excess before the
    // shrink, so a snapshot of the first shrink alone can't release the
    // second.
    assert_eq!(harness.machine.drain(snapshot(first_seq, 0)), none);
    assert_eq!(harness.machine.drain(snapshot(harness.seq, 0)), receive(1));
    assert!(!harness.machine.has_drains());
}

#[test]
fn close_releases_every_lease_and_every_later_one() {
    let mut harness = Harness::new();
    harness.add(MIB);
    assert!(harness.finish(MIB, MIB));
    let step = harness.tick(CreditLoad {
        cwnd: 4 * MIB,
        queued_send_bytes: 1,
        ..CreditLoad::default()
    });
    assert_eq!(step.send, SendStep::Grow(6 * MIB));
    harness.grown(6 * MIB);

    assert_eq!(
        harness.machine.close(),
        DrainRelease {
            receive: 1,
            send: 0,
            active_send: true,
        }
    );
    assert!(!harness.machine.has_drains());

    // After close, a handed lease releases at once and credit changes
    // nothing.
    assert_eq!(harness.add(MIB), None);
    assert_eq!(
        harness.machine.finish_receive(MIB, Some(MIB)),
        FinishReceive {
            limits: None,
            drains: false,
        }
    );
    assert_eq!(harness.machine.send_grown(MIB), None);
}

#[test]
fn send_window_grows_to_two_cwnd_and_drains_after_an_accepted_byte() {
    let mut harness = Harness::new();
    let waiting = CreditLoad {
        cwnd: 5 * MIB,
        queued_send_bytes: 64 * 1024,
        send_blocked_streams: 1,
        ..CreditLoad::default()
    };
    let step = harness.tick(waiting);
    // 2 × cwnd = 10 MiB within the 16 MiB cap: 8 MiB above the base.
    assert_eq!(step.send, SendStep::Grow(8 * MIB));
    assert_eq!(step.peer_cap, Some(PER_PEER_CAP));
    // A second tick before the answer asks for nothing more.
    assert_eq!(harness.tick(waiting).send, SendStep::Keep);
    assert!(harness.grown(8 * MIB).is_some());
    assert_eq!(harness.machine.send_window(), 10 * MIB);

    // Idle for SEND_IDLE_TICKS ticks: the window returns to the base.
    let idle = CreditLoad {
        cwnd: 5 * MIB,
        limits_applied: harness.seq,
        ..CreditLoad::default()
    };
    let mut shrunk = None;
    for _ in 0..SEND_IDLE_TICKS {
        if let SendStep::Shrink { bytes, .. } = harness.tick(idle).send {
            shrunk = Some(bytes);
        }
    }
    assert_eq!(shrunk, Some(8 * MIB), "the idle window shrinks");
    assert_eq!(harness.machine.send_window(), BASE_SEND);

    // The snapshot that reflects the shrink sets its baseline; a byte
    // accepted after it releases the lease.
    let reflected = CreditLoad {
        limits_applied: harness.seq,
        ..idle
    };
    assert_eq!(harness.tick(reflected).release, DrainRelease::default());
    assert_eq!(
        harness
            .tick(CreditLoad {
                accepted_send_bytes: 1,
                ..reflected
            })
            .release,
        DrainRelease {
            send: 1,
            ..DrainRelease::default()
        }
    );
}

/// fix-13: output that drains between ticks doesn't shrink the window, while
/// bytes keep flowing or for fewer than [`SEND_IDLE_TICKS`] idle ticks.
#[test]
fn a_short_pause_keeps_the_send_window() {
    let mut harness = Harness::new();
    let _ = harness.tick(CreditLoad {
        cwnd: 4 * MIB,
        queued_send_bytes: 1,
        ..CreditLoad::default()
    });
    harness.grown(6 * MIB);
    let window = harness.machine.send_window();

    // The queue reads empty while the connection keeps accepting bytes.
    for accepted in 1..=20 {
        let _ = harness.tick(CreditLoad {
            cwnd: 4 * MIB,
            accepted_send_bytes: accepted * MIB,
            ..CreditLoad::default()
        });
        assert_eq!(harness.machine.send_window(), window);
    }
    // A pause shorter than SEND_IDLE_TICKS keeps it too.
    for _ in 1..SEND_IDLE_TICKS {
        let _ = harness.tick(CreditLoad {
            cwnd: 4 * MIB,
            accepted_send_bytes: 20 * MIB,
            ..CreditLoad::default()
        });
        assert_eq!(harness.machine.send_window(), window);
    }
}

/// fix-14: an idle connection doesn't tick. It ticks while its send window is
/// above the base, watches snapshots while a lease drains, and sleeps again
/// once nothing is left.
#[test]
fn the_machine_ticks_only_while_it_has_credit_work() {
    let mut harness = Harness::new();
    let idle = CreditWake::Load { drains_seen: None };
    assert_eq!(harness.machine.wake(), idle);
    // A live request needs no snapshot.
    harness.add(MIB);
    assert_eq!(harness.machine.wake(), idle);
    let quiet = snapshot(harness.seq, 0);
    assert!(!idle.wakes_on(&quiet));
    assert!(idle.wakes_on(&CreditLoad {
        queued_send_bytes: 1,
        ..quiet
    }));

    // A drain watches for a snapshot that could release it.
    harness.finish(MIB, MIB);
    let drain_wake = harness.machine.wake();
    assert!(matches!(
        drain_wake,
        CreditWake::Load {
            drains_seen: Some(_)
        }
    ));
    assert!(drain_wake.wakes_on(&snapshot(harness.seq, 0)));
    assert_eq!(harness.machine.drain(snapshot(harness.seq, 0)), receive(1));
    assert_eq!(harness.machine.wake(), idle);

    // Waiting output grows the window, which ticks until it drains back.
    let _ = harness.tick(CreditLoad {
        cwnd: 4 * MIB,
        queued_send_bytes: 1,
        ..quiet
    });
    assert_eq!(harness.machine.wake(), CreditWake::Tick);
    harness.grown(6 * MIB);
    let idle_load = CreditLoad {
        cwnd: 4 * MIB,
        ..snapshot(harness.seq, 0)
    };
    for _ in 0..SEND_IDLE_TICKS {
        assert_eq!(harness.machine.wake(), CreditWake::Tick);
        let _ = harness.tick(idle_load);
    }
    let shrunk = CreditLoad {
        limits_applied: harness.seq,
        ..idle_load
    };
    let _ = harness.tick(shrunk);
    let drain_wake = harness.machine.wake();
    assert!(matches!(
        drain_wake,
        CreditWake::Load {
            drains_seen: Some(_)
        }
    ));
    // Only an accepted byte releases the send drain.
    assert!(!drain_wake.wakes_on(&shrunk));
    let accepted = CreditLoad {
        accepted_send_bytes: 1,
        ..shrunk
    };
    assert!(drain_wake.wakes_on(&accepted));
    let _ = harness.tick(accepted);
    assert_eq!(harness.machine.wake(), idle);

    let _ = harness.machine.close();
    assert_eq!(harness.machine.wake(), CreditWake::Never);
}

#[derive(Clone, Debug)]
enum Op {
    Add(u64),
    /// Finish the oldest live request, with this part unspent (per mille).
    Finish(u64),
    /// A snapshot `lag` set_limits calls old, with this receive excess. A
    /// tick's snapshot also carries a cwnd, queued output and newly accepted
    /// bytes.
    Load {
        lag: u64,
        excess: u64,
        tick: Option<(u64, u64, u64)>,
    },
    /// Answer an outstanding growth request.
    Grow(bool),
    Close,
}

/// An excess that is often zero, so drains release.
fn excess() -> impl Strategy<Value = u64> {
    prop_oneof![Just(0), 0..8 * MIB]
}

fn op() -> impl Strategy<Value = Op> {
    let tick = (0..8 * MIB, 0..2u64, 0..2u64);
    prop_oneof![
        3 => (1..4 * MIB).prop_map(Op::Add),
        3 => (0..=1000u64).prop_map(Op::Finish),
        6 => (0..3u64, excess(), proptest::option::of(tick))
            .prop_map(|(lag, excess, tick)| Op::Load { lag, excess, tick }),
        2 => any::<bool>().prop_map(Op::Grow),
        1 => Just(Op::Close),
    ]
}

/// The leases the test handed the machine, in queue order, as a shell would
/// hold them.
#[derive(Default)]
struct Ledger {
    /// Unspent receive leases: their bytes and, once a `set_limits` call
    /// applied their shrink, its sequence number.
    receive: VecDeque<(u64, Option<u64>)>,
    send: VecDeque<u64>,
    active_send: u64,
    requests: VecDeque<u64>,
    growth_asked: bool,
    accepted: u64,
}

impl Ledger {
    /// Takes the leases `release` names. A receive drain released against
    /// `load` must be one the snapshot reflects.
    fn release(
        &mut self,
        release: DrainRelease,
        load: Option<CreditLoad>,
    ) -> Result<(), TestCaseError> {
        prop_assert!(
            release.receive <= self.receive.len() && release.send <= self.send.len(),
            "released a lease it doesn't hold"
        );
        for (_, seq) in self.receive.drain(..release.receive) {
            if let Some(load) = load {
                prop_assert!(
                    seq.is_some_and(|seq| seq <= load.limits_applied),
                    "released a drain against a snapshot that misses its shrink"
                );
            }
        }
        self.send.drain(..release.send);
        if release.active_send {
            prop_assert!(self.active_send > 0);
            self.active_send = 0;
        }
        Ok(())
    }

    /// Bytes of held receive drains that a snapshot of call `applied` reflects.
    fn reflected_receive(&self, applied: u64) -> u64 {
        self.receive
            .iter()
            .filter(|(_, seq)| seq.is_some_and(|seq| seq <= applied))
            .map(|(bytes, _)| bytes)
            .sum()
    }
}

proptest! {
    /// After every input: the machine releases only leases it holds, once; it
    /// releases a receive drain only against a snapshot that reflects its
    /// shrink, and keeps reflected drains that cover the snapshot's excess; its
    /// queues match the leases a shell holds; its send window matches the
    /// lease it keeps active; it asks for one growth at a time; its last
    /// limits are its current limits; and once closed it holds nothing. Its
    /// wake ticks while the send window is above its base, never ticks an idle
    /// connection, watches snapshots while leases drain, and never matches the
    /// snapshot it just read (no busy loop).
    #[test]
    fn credit_machine_invariants(ops in proptest::collection::vec(op(), 1..160)) {
        let mut harness = Harness::new();
        let mut ledger = Ledger::default();
        let mut output_waits = false;
        for op in ops {
            let mut load_seen = None;
            let mut release = DrainRelease::default();
            let mut last_limits = None;
            let closing = matches!(op, Op::Close);
            let seq_before = harness.seq;
            match op {
                Op::Add(bytes) => {
                    ledger.requests.push_back(bytes);
                    last_limits = harness.add(bytes);
                }
                Op::Finish(per_mille) => {
                    let Some(total) = ledger.requests.pop_front() else {
                        continue;
                    };
                    let unspent = total * per_mille / 1000;
                    let finish = harness.machine.finish_receive(total, Some(unspent));
                    if finish.drains {
                        ledger.receive.push_back((unspent, None));
                    }
                    last_limits = harness.sent(finish.limits);
                }
                Op::Load { lag, excess, tick } => {
                    let mut load = snapshot(harness.seq.saturating_sub(lag), excess);
                    let step = match tick {
                        Some((cwnd, queued, accepted)) => {
                            ledger.accepted += accepted;
                            load.cwnd = cwnd;
                            load.queued_send_bytes = queued;
                            load.accepted_send_bytes = ledger.accepted;
                            harness.tick(load)
                        }
                        None => TickStep {
                            release: harness.machine.drain(load),
                            ..TickStep::default()
                        },
                    };
                    load_seen = Some(load);
                    release = step.release;
                    ledger.release(release, load_seen)?;
                    if let Some(cap) = step.peer_cap {
                        prop_assert!(cap <= PER_PEER_CAP);
                    }
                    match step.send {
                        SendStep::Keep => {}
                        SendStep::Grow(bytes) => {
                            prop_assert!(bytes > 0 && !ledger.growth_asked);
                            ledger.growth_asked = true;
                        }
                        SendStep::Shrink { bytes, limits } => {
                            prop_assert!(bytes > 0 && bytes <= ledger.active_send);
                            ledger.active_send -= bytes;
                            ledger.send.push_back(bytes);
                            last_limits = Some(limits);
                        }
                    }
                }
                Op::Grow(granted) => {
                    if !ledger.growth_asked {
                        continue;
                    }
                    ledger.growth_asked = false;
                    if granted {
                        last_limits = harness.grown(MIB);
                        if last_limits.is_some() {
                            ledger.active_send += MIB;
                        }
                    } else {
                        harness.machine.send_refused();
                    }
                }
                Op::Close => {
                    release = harness.machine.close();
                    ledger.release(release, None)?;
                }
            }
            // The op's set_limits call applied every shrink it made.
            if harness.seq > seq_before {
                for (_, seq) in ledger.receive.iter_mut().filter(|(_, seq)| seq.is_none()) {
                    *seq = Some(harness.seq);
                }
            }
            if let Some(limits) = last_limits {
                prop_assert_eq!(limits, harness.machine.limits());
            }
            if let Some(load) = load_seen.filter(|_| release.receive > 0) {
                prop_assert!(ledger.reflected_receive(load.limits_applied) >= load.receive_credit_excess);
            }
            prop_assert_eq!(harness.machine.receive_drains.len(), ledger.receive.len());
            prop_assert_eq!(harness.machine.send_drains.len(), ledger.send.len());
            prop_assert_eq!(harness.machine.send_window(), BASE_SEND + ledger.active_send);
            if let Some(load) = load_seen {
                output_waits = load.output_waits();
            }
            let wake = harness.machine.wake();
            let drains = !ledger.receive.is_empty() || !ledger.send.is_empty();
            if harness.machine.has_drains() || ledger.active_send > 0 || output_waits {
                prop_assert_ne!(wake, CreditWake::Load { drains_seen: None });
            }
            if ledger.active_send > 0 && wake != CreditWake::Never {
                prop_assert_eq!(wake, CreditWake::Tick);
            }
            if !drains && ledger.active_send == 0 && !output_waits {
                prop_assert_ne!(wake, CreditWake::Tick, "an idle connection ticks");
            }
            if let (Some(load), CreditWake::Load { .. }) = (load_seen, wake) {
                prop_assert!(!wake.wakes_on(&load), "the wake matches the snapshot it read");
            }
            if closing {
                prop_assert_eq!(wake, CreditWake::Never);
                prop_assert!(ledger.receive.is_empty() && ledger.send.is_empty());
                prop_assert_eq!(ledger.active_send, 0);
                prop_assert!(!harness.machine.has_drains());
            }
        }
    }
}
