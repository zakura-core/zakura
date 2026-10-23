//! Fake action sources, for testing the step loop before real QUIC, storage
//! and peers plug in (M2, M4, M5).
//!
//! Each fake is a cheap handle around shared state. Tasks hold one handle and
//! the run holds another as its [`ActionSource`]. Every event goes to the log
//! as an [`Observation::Fake`].

use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
    time::Duration,
};

use zakura_scheduler::{hooks, SimInstant};

use crate::{ActionSource, Boundary, Identity, Observation, ObservationLog};

/// The virtual time now. Fakes are only used inside a run.
fn now() -> SimInstant {
    hooks::now().expect("fake sources are used only inside a run")
}

/// `t` plus `d`, saturating at the largest instant.
fn after(t: SimInstant, d: Duration) -> SimInstant {
    t.checked_add(d)
        .unwrap_or(SimInstant::from_start(Duration::MAX))
}

/// Logs a fake event.
fn event(log: &mut ObservationLog, source: &str, event: String) {
    log.push(Observation::Fake {
        source: source.to_owned(),
        event,
    });
}

/// A fake network. A datagram sent at `t` can be delivered from `t + min`
/// and must be delivered or dropped by `t + max`. Drops are allowed while
/// the drop budget lasts.
#[derive(Clone, Debug)]
pub struct FakeNet(Rc<RefCell<Net>>);

#[derive(Debug)]
struct Net {
    min: Duration,
    max: Duration,
    drops_left: u32,
    /// Datagrams sent on each link so far.
    sent: BTreeMap<String, u64>,
    in_flight: Vec<Datagram>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Datagram {
    link: String,
    seq: u64,
    opens: SimInstant,
    deadline: SimInstant,
}

impl FakeNet {
    /// A network whose delivery window is `min..=max` after sending, and
    /// that may drop `drops` datagrams.
    ///
    /// # Panics
    ///
    /// If `min` is after `max`. Time would stop at the deadline before
    /// delivery opened, and the run would look idle with a datagram in
    /// flight.
    pub fn new(min: Duration, max: Duration, drops: u32) -> Self {
        assert!(min <= max, "a FakeNet's min delay must not exceed its max");
        FakeNet(Rc::new(RefCell::new(Net {
            min,
            max,
            drops_left: drops,
            sent: BTreeMap::new(),
            in_flight: Vec::new(),
        })))
    }

    /// Sends a datagram on the directed `link`, such as `node->p1`, now.
    pub fn send(&self, link: &str) {
        let now = now();
        let mut net = self.0.borrow_mut();
        let seq = net.sent.entry(link.to_owned()).or_insert(0);
        *seq += 1;
        let seq = *seq;
        let (opens, deadline) = (after(now, net.min), after(now, net.max));
        net.in_flight.push(Datagram {
            link: link.to_owned(),
            seq,
            opens,
            deadline,
        });
    }

    /// How many datagrams are still in flight.
    pub fn in_flight(&self) -> usize {
        self.0.borrow().in_flight.len()
    }
}

impl ActionSource for FakeNet {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        let net = self.0.borrow();
        for (i, d) in net
            .in_flight
            .iter()
            .enumerate()
            .filter(|(_, d)| d.opens <= now)
        {
            // usize always fits in u64 on supported targets.
            let i = i as u64;
            out.push((Identity::deliver(&d.link, d.seq), 2 * i));
            if net.drops_left > 0 {
                out.push((Identity::drop(&d.link, d.seq), 2 * i + 1));
            }
        }
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        for d in &self.0.borrow().in_flight {
            if d.opens > now {
                out.push(Boundary::Opens(d.opens));
            }
            out.push(Boundary::Deadline(d.deadline));
        }
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        let mut net = self.0.borrow_mut();
        // `local` came from `enabled` at this step, so the index is in range.
        let d = net.in_flight.remove((local / 2) as usize);
        let what = if local % 2 == 1 {
            net.drops_left -= 1;
            "drop"
        } else {
            "deliver"
        };
        event(log, "net", format!("{what} {}:{}", d.link, d.seq));
    }
}

/// A fake storage service. A submitted operation completes with `success` or
/// `failure`, and must complete within the service's latency bound.
#[derive(Clone, Debug)]
pub struct FakeStorage(Rc<RefCell<Storage>>);

#[derive(Debug)]
struct Storage {
    service: String,
    latency: Duration,
    next: u64,
    ops: BTreeMap<u64, Op>,
}

#[derive(Debug, Default)]
struct Op {
    submitted: SimInstant,
    result: Option<&'static str>,
    waker: Option<Waker>,
}

/// The outcomes a fake storage operation can complete with.
const OUTCOMES: [&str; 2] = ["success", "failure"];

impl FakeStorage {
    /// A service named `service` that completes each operation within
    /// `latency`.
    pub fn new(service: &str, latency: Duration) -> Self {
        FakeStorage(Rc::new(RefCell::new(Storage {
            service: service.to_owned(),
            latency,
            next: 0,
            ops: BTreeMap::new(),
        })))
    }

    /// Submits an operation now. The future resolves to its outcome once the
    /// run completes it.
    pub fn submit(&self) -> Completion {
        let mut storage = self.0.borrow_mut();
        storage.next += 1;
        let seq = storage.next;
        storage.ops.insert(
            seq,
            Op {
                submitted: now(),
                ..Op::default()
            },
        );
        Completion {
            storage: self.clone(),
            seq,
        }
    }
}

/// A submitted fake storage operation.
#[derive(Debug)]
pub struct Completion {
    storage: FakeStorage,
    seq: u64,
}

impl Future for Completion {
    type Output = &'static str;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<&'static str> {
        let mut storage = self.storage.0.borrow_mut();
        let op = storage
            .ops
            .get_mut(&self.seq)
            .expect("an operation stays until its completion is polled ready");
        match op.result {
            Some(result) => {
                storage.ops.remove(&self.seq);
                Poll::Ready(result)
            }
            None => {
                op.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl ActionSource for FakeStorage {
    fn enabled(&self, _: SimInstant, out: &mut Vec<(Identity, u64)>) {
        let storage = self.0.borrow();
        for (seq, _) in storage.ops.iter().filter(|(_, op)| op.result.is_none()) {
            for (i, outcome) in (0..).zip(OUTCOMES) {
                out.push((
                    Identity::storage(&storage.service, *seq, outcome),
                    2 * seq + i,
                ));
            }
        }
    }

    fn boundaries(&self, _: SimInstant, out: &mut Vec<Boundary>) {
        let storage = self.0.borrow();
        for op in storage.ops.values().filter(|op| op.result.is_none()) {
            out.push(Boundary::Deadline(after(op.submitted, storage.latency)));
        }
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        let (seq, outcome) = (local / 2, OUTCOMES[usize::from(local % 2 == 1)]);
        let waker = {
            let mut storage = self.0.borrow_mut();
            let op = storage
                .ops
                .get_mut(&seq)
                .expect("`enabled` offered this operation at this step");
            op.result = Some(outcome);
            op.waker.take()
        };
        // Woken outside the borrow, since the waker can run scheduler code.
        if let Some(waker) = waker {
            waker.wake();
        }
        let service = self.0.borrow().service.clone();
        event(log, "storage", format!("{service}:{seq}:{outcome}"));
    }
}

/// A fake peer that takes scripted actions in order, at most `per_instant`
/// per virtual instant. Once that budget is spent, the next action opens one
/// second later, even if time moves sooner for another reason, so the node
/// can go idle and time can advance.
#[derive(Clone, Debug)]
pub struct FakePeer(Rc<RefCell<Peer>>);

#[derive(Debug)]
struct Peer {
    name: String,
    script: VecDeque<String>,
    per_instant: u32,
    /// The instant of the last action, and how many actions it took.
    spent: (SimInstant, u32),
    /// The earliest instant the next action may run. Spending the budget at
    /// `t` moves it to `t + 1 s`, however time moves in between.
    opens_at: SimInstant,
}

impl FakePeer {
    /// A peer named `name` that takes the actions in `script`, at most
    /// `per_instant` at one instant.
    ///
    /// # Panics
    ///
    /// If `per_instant` is 0, since such a peer could never act.
    pub fn new(name: &str, script: &[&str], per_instant: u32) -> Self {
        assert!(
            per_instant > 0,
            "a FakePeer needs a per_instant budget above 0"
        );
        FakePeer(Rc::new(RefCell::new(Peer {
            name: name.to_owned(),
            script: script.iter().map(|a| (*a).to_owned()).collect(),
            per_instant,
            spent: (SimInstant::ZERO, 0),
            opens_at: SimInstant::ZERO,
        })))
    }

    /// How many scripted actions are left.
    pub fn remaining(&self) -> usize {
        self.0.borrow().script.len()
    }
}

impl Peer {
    /// True if the next action may run at `now`.
    fn can_act(&self, now: SimInstant) -> bool {
        now >= self.opens_at
    }
}

impl ActionSource for FakePeer {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        let peer = self.0.borrow();
        if let Some(action) = peer.script.front().filter(|_| peer.can_act(now)) {
            out.push((Identity::peer(&peer.name, action), 0));
        }
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        let peer = self.0.borrow();
        if !peer.script.is_empty() && !peer.can_act(now) {
            out.push(Boundary::Opens(peer.opens_at));
        }
    }

    fn execute(&mut self, _: u64, log: &mut ObservationLog) {
        let now = now();
        let mut peer = self.0.borrow_mut();
        let action = peer
            .script
            .pop_front()
            .expect("`enabled` offered the next action at this step");
        peer.spent = match peer.spent {
            (at, n) if at == now => (at, n + 1),
            _ => (now, 1),
        };
        if peer.spent.1 >= peer.per_instant {
            peer.opens_at = after(now, Duration::from_secs(1));
        }
        let name = peer.name.clone();
        drop(peer);
        event(log, "peer", format!("{name}:{action}"));
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use zakura_scheduler::{At, Op as PointOp, Scheduler, SchedulerConfig, TaskId};

    use super::*;
    use crate::{
        action::{enabled_set, EnabledInputs, Offer},
        check::Phase,
    };

    fn ms(n: u64) -> SimInstant {
        SimInstant::from_start(Duration::from_millis(n))
    }

    /// `n` real task ids, which only a scheduler hands out.
    fn task_ids(n: usize) -> Vec<TaskId> {
        let mut sched = Scheduler::new(SchedulerConfig::default(), SimInstant::ZERO);
        let ids = (0..n)
            .map(|_| sched.spawn_root("t", async {}).task())
            .collect();
        sched.shutdown();
        ids
    }

    /// A fake-source state, generated without going through the fakes' own
    /// enabled logic.
    #[derive(Clone, Debug)]
    struct World {
        now: u64,
        ready: Vec<bool>,
        next_deadline: Option<u64>,
        /// (link, opens, window length) in milliseconds.
        datagrams: Vec<(u8, u64, u64)>,
        drops: u32,
        /// (submitted, completed) in milliseconds.
        ops: Vec<(u64, bool)>,
        latency: u64,
        script: Vec<u8>,
        per_instant: u32,
        /// When the peer's next action may run.
        opens_at: u64,
    }

    fn world() -> impl Strategy<Value = World> {
        (
            (0u64..20, proptest::collection::vec(any::<bool>(), 5)),
            proptest::option::of(0u64..20),
            proptest::collection::vec((0u8..3, 0u64..20, 0u64..10), 0..6),
            0u32..2,
            (
                proptest::collection::vec((0u64..20, any::<bool>()), 0..4),
                0u64..10,
            ),
            (proptest::collection::vec(0u8..3, 0..3), 1u32..3, 0u64..30),
        )
            .prop_map(
                |(
                    (now, ready),
                    next,
                    datagrams,
                    drops,
                    (ops, latency),
                    (script, per, opens_at),
                )| {
                    World {
                        now,
                        ready,
                        // A pending timer is never behind the clock.
                        next_deadline: next.map(|d| now + d),
                        datagrams,
                        drops,
                        ops,
                        latency,
                        script,
                        per_instant: per,
                        opens_at,
                    }
                },
            )
    }

    /// The fakes in `world`'s state.
    fn fakes(w: &World) -> (FakeNet, FakeStorage, FakePeer) {
        let mut sent = BTreeMap::new();
        let in_flight = w
            .datagrams
            .iter()
            .map(|(link, opens, len)| {
                let link = format!("l{link}");
                let seq = sent.entry(link.clone()).or_insert(0u64);
                *seq += 1;
                Datagram {
                    link,
                    seq: *seq,
                    opens: ms(*opens),
                    deadline: ms(opens + len),
                }
            })
            .collect();
        let net = FakeNet(Rc::new(RefCell::new(Net {
            min: Duration::ZERO,
            max: Duration::ZERO,
            drops_left: w.drops,
            sent,
            in_flight,
        })));
        let ops = (1..)
            .zip(&w.ops)
            .map(|(seq, (submitted, done))| {
                let op = Op {
                    submitted: ms(*submitted),
                    result: done.then_some("success"),
                    waker: None,
                };
                (seq, op)
            })
            .collect();
        let storage = FakeStorage(Rc::new(RefCell::new(Storage {
            service: "s".into(),
            latency: Duration::from_millis(w.latency),
            next: 0,
            ops,
        })));
        let peer = FakePeer(Rc::new(RefCell::new(Peer {
            name: "p1".into(),
            script: w.script.iter().map(|a| format!("a{a}")).collect(),
            per_instant: w.per_instant,
            spent: (SimInstant::ZERO, 0),
            opens_at: ms(w.opens_at),
        })));
        (net, storage, peer)
    }

    /// The enabled identities, derived from `w` by the rules in sim.md
    /// rather than by the fakes.
    fn model(w: &World) -> Vec<String> {
        let mut out: Vec<(u8, String, String)> = Vec::new();
        let ready: Vec<usize> = (0..w.ready.len()).filter(|i| w.ready[*i]).collect();
        for i in &ready {
            let name = format!("t{i}.1");
            out.push((0, name.clone(), format!("task:{name}:poll#1")));
        }
        let mut seqs = BTreeMap::new();
        let mut deadlines = Vec::new();
        let mut later = Vec::new();
        for (link, opens, len) in &w.datagrams {
            let seq = seqs.entry(*link).or_insert(0);
            *seq += 1;
            let key = format!("l{link}:{seq}");
            if *opens <= w.now {
                out.push((1, key.clone(), format!("deliver:{key}")));
                if w.drops > 0 {
                    out.push((2, key.clone(), format!("drop:{key}")));
                }
            } else {
                later.push(*opens);
            }
            deadlines.push(opens + len);
        }
        for (seq, (submitted, done)) in (1..).zip(&w.ops) {
            if !done {
                for outcome in ["success", "failure"] {
                    let key = format!("s:{seq}:{outcome}");
                    out.push((3, key.clone(), format!("storage:{key}")));
                }
                deadlines.push(submitted + w.latency);
            }
        }
        if let Some(action) = w.script.first() {
            if w.now >= w.opens_at {
                let key = format!("p1:a{action}");
                out.push((4, key.clone(), format!("peer:{key}")));
            } else {
                later.push(w.opens_at);
            }
        }
        if ready.is_empty() && deadlines.iter().all(|d| *d > w.now) {
            let next = w
                .next_deadline
                .into_iter()
                .chain(later)
                .chain(deadlines)
                .min();
            if let Some(t) = next {
                let ns = u128::from(t) * 1_000_000;
                out.push((5, ns.to_string(), format!("advance-time:{ns}")));
            }
        }
        out.sort();
        out.into_iter().map(|(_, _, shown)| shown).collect()
    }

    proptest! {
        #[test]
        fn enabled_matches_model(w in world()) {
            let ids = task_ids(w.ready.len());
            let ready: Vec<(Identity, TaskId)> = (0..w.ready.len())
                .filter(|i| w.ready[*i])
                .map(|i| {
                    let at = At { op: PointOp::Poll, n: 1, site: None };
                    (Identity::task(format!("t{i}.1"), at), ids[i])
                })
                .collect();
            let (net, storage, peer) = fakes(&w);
            let now = ms(w.now);
            let sources: [&dyn ActionSource; 3] = [&net, &storage, &peer];
            let offers: Vec<Offer> = sources
                .iter()
                .map(|s| {
                    let mut offer = Offer::default();
                    s.enabled(now, &mut offer.actions);
                    s.boundaries(now, &mut offer.boundaries);
                    offer
                })
                .collect();
            let enabled = enabled_set(&EnabledInputs {
                phase: Phase::Live,
                now,
                ready: &ready,
                next_deadline: w.next_deadline.map(ms),
                offers: &offers,
            })
            .expect("fakes never share an identity");
            let shown: Vec<String> = enabled.iter().map(|e| e.identity.to_string()).collect();
            prop_assert_eq!(shown, model(&w));
        }
    }
}
