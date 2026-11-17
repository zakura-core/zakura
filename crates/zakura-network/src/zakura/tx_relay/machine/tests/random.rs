//! Random runs of the machine.
//!
//! Each run gives the machine a few hundred random inputs from a handful of
//! peers over a few dozen ids, with the pool's answers and changes picked at
//! random. After every input the [`Bench`] checks the machine's invariants
//! and the tracker's record. Then the run drains: the peers that answer
//! answer every request, the pool grants every call and admits every body,
//! and time passes until nothing is due, which must leave nothing to fetch
//! unless the core stopped. Last, every session ends and every call and
//! verdict is answered, and the machine must hold no work at all.
//!
//! `RELAY_RUNS` sets how many runs, `RELAY_FIRST` the first seed, and
//! `RELAY_TRACE` prints each run's inputs and steps to stderr, with body `n`
//! named `tn`.

// The trace is for a developer chasing one failing seed.
#![allow(clippy::print_stderr)]

use std::{
    collections::BTreeMap,
    panic::{self, AssertUnwindSafe},
    time::Duration,
};

use zakura_chain::{
    block::Height,
    transaction::{UnminedTx, UnminedTxId},
};
use zakura_node_services::mempool::port::{
    ChainReason, Change, Charge, LocalFault, RejectionScope, RemovalReason, ReservationId,
};

use super::{body, script_len, Bench, SIZE};
use crate::zakura::tx_relay::machine::{
    peer::Hello, FanOut, PeerKey, RelayEffect, SessionId, Unreserved, Verdict, WaiterId,
    ADVERT_DELAY, MAX_REQUESTS_PER_PEER, MAX_SERVES, MAX_TX_BYTES,
};

/// The ids a run's peers announce.
const IDS: u32 = 24;

/// The peers. The last one is unconditional.
const PEERS: u64 = 5;

/// The peer whose bodies mostly earn a cooldown, so some peer earns a
/// second one after its first ends.
const BAD: u64 = 4;

/// The most sessions open at once.
const SESSIONS: usize = 7;

/// The coverage list: every kind the runs must reach, `;`-separated.
const REACHED: &str = "reserve; ask; submit; release; fetch failed; advert; inv taken;
    inv child waited; serve;
    answer not found; close Violation(HelloTwice); close Violation(BadHello);
    close Violation(NoHelloFirst); close Violation(Unrequested); close Violation(WrongSize);
    close Violation(WrongId); close Ban; close RepeatedCooldown; close Unresponsive;
    answer grant; answer cancelled grant; answer Pooled; answer Pending; answer Rejected;
    answer NoRoom; answer Inactive; answer Stopped; verdict Admitted;
    verdict Rejected cached, None; verdict Rejected uncached, None;
    verdict Rejected cached, Cooldown; verdict Rejected cached, Ban; verdict NoRoom;
    verdict LocalFault; verdict LocalFault(Stopped); request ended: body read;
    request ended: body not read; request ended: not found; request ended: session ended;
    added; removed Mined; removed Evicted; removed Reset; removed Deactivated; refused;
    resync; pool off; pool on";

/// A small deterministic generator: SplitMix64.
struct Rng(u64);

impl Rng {
    /// The next number.
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A number below `n`.
    fn below(&mut self, n: usize) -> usize {
        let n = u64::try_from(n.max(1)).expect("a small bound");
        usize::try_from(self.next() % n).expect("below a usize")
    }

    /// True one time in `n`.
    fn one_in(&mut self, n: usize) -> bool {
        self.below(n) == 0
    }

    /// One of `items`.
    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len())]
    }

    /// A duration below `millis` milliseconds.
    fn millis(&mut self, millis: usize) -> Duration {
        Duration::from_millis(u64::try_from(self.below(millis)).expect("small"))
    }
}

/// How a session's peer answers what it is asked for.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Peer {
    /// With the body, mostly, and now and then something wrong.
    Honest,
    /// Never.
    Silent,
    /// With `NotFound`.
    Liar,
}

/// One run: the machine through its bench, and the peers and pool around
/// it.
struct Run {
    rng: Rng,
    bench: Bench,
    bodies: Vec<UnminedTx>,
    peers: BTreeMap<SessionId, Peer>,
    /// Bodies the pool admitted and has not published yet.
    admitted: Vec<UnminedTxId>,
    /// Ids a tip reset took out of the pool, which it may add again.
    reset_out: Vec<UnminedTxId>,
    /// Whether the core may stop near the run's end.
    may_stop: bool,
    /// The core stopped: it answers every call and body `Stopped`, and
    /// publishes nothing more.
    stopped: bool,
    trace: bool,
}

impl Run {
    /// The run for `seed`. Some runs announce at once to everyone, the rest
    /// hold adverts outside small windows.
    fn new(seed: u64, trace: bool) -> Self {
        let mut rng = Rng(seed);
        let fan_out = if rng.one_in(4) {
            FanOut {
                advert_delay: Duration::ZERO,
                ..FanOut::default()
            }
        } else {
            FanOut {
                advert_delay: ADVERT_DELAY,
                window_small: 2,
                window_full: 1,
            }
        };
        let script = script_len();
        let bodies: Vec<UnminedTx> = (0..IDS).map(|n| body(n, script)).collect();
        let pooled: Vec<UnminedTxId> = bodies[..2].iter().map(UnminedTx::id).collect();
        let mut bench = Bench::with(fan_out, true, &pooled);
        // About one id in four spends outputs of an earlier one, which counts
        // while the pool holds it.
        for n in 1..bodies.len() {
            if rng.one_in(4) {
                let parent = bodies[rng.below(n)].id();
                bench.depends(bodies[n].id(), &[parent]);
            }
        }
        bench.steps = trace.then(Vec::new);
        let may_stop = rng.one_in(3);
        let (peers, admitted, reset_out) = (BTreeMap::new(), Vec::new(), Vec::new());
        let stopped = false;
        Self {
            rng,
            bench,
            bodies,
            peers,
            admitted,
            reset_out,
            may_stop,
            stopped,
            trace,
        }
    }

    /// A random id of the run.
    fn any_id(&mut self) -> UnminedTxId {
        self.bodies[self.rng.below(self.bodies.len())].id()
    }

    /// The body of `id`.
    fn body(&self, id: UnminedTxId) -> UnminedTx {
        let body = self.bodies.iter().find(|body| body.id() == id);
        body.expect("an id of the run").clone()
    }

    /// The name a trace gives `id`.
    fn name(&self, id: UnminedTxId) -> String {
        let n = self.bodies.iter().position(|body| body.id() == id);
        n.map_or_else(|| "?".to_owned(), |n| format!("t{n}"))
    }

    /// A random open session.
    fn session(&mut self) -> Option<SessionId> {
        let open: Vec<SessionId> = self.bench.tracker.open.keys().copied().collect();
        (!open.is_empty()).then(|| open[self.rng.below(open.len())])
    }

    /// The ids that went out to `session` in an `Inv` and still stand.
    fn sent(&self, session: SessionId) -> Vec<UnminedTxId> {
        let open = self.bench.tracker.open.get(&session);
        open.map(|open| open.sent.iter().copied().collect())
            .unwrap_or_default()
    }

    /// The requests on the wire, with their reservations.
    fn requests(&self) -> Vec<(SessionId, UnminedTxId, ReservationId)> {
        let requests = self.bench.tracker.requests.iter();
        requests
            .map(|((session, id), held)| (*session, *id, *held))
            .collect()
    }

    /// The bodies awaiting their verdicts, with their suppliers.
    fn bodies(&self) -> Vec<(WaiterId, UnminedTxId, PeerKey)> {
        let bodies = self.bench.tracker.bodies.iter();
        bodies
            .map(|(waiter, body)| (*waiter, body.id, body.peer))
            .collect()
    }

    /// A random scope a rejection is remembered for.
    fn scope(&mut self) -> RejectionScope {
        self.rng.pick(&[
            RejectionScope::ExactTip,
            RejectionScope::EffectsTip,
            RejectionScope::EffectsChain(ChainReason::Expired),
            RejectionScope::Uncached,
        ])
    }

    /// Whether the core has stopped: near the run's end, it stops now one
    /// time in `odds`.
    fn stops(&mut self, near_end: bool, odds: usize) -> bool {
        self.stopped |= near_end && self.may_stop && self.rng.one_in(odds);
        self.stopped
    }

    /// Prints what the run does next, when tracing.
    fn note(&self, what: impl FnOnce(&Self) -> String) {
        if self.trace {
            eprintln!("{:?} {}", self.bench.now, what(self));
        }
    }

    /// Prints the steps the bench took since the last call, when tracing.
    fn flush(&mut self) {
        let Some(steps) = self.bench.steps.as_mut().map(std::mem::take) else {
            return;
        };
        for (at, effects) in steps {
            let effects: Vec<String> = effects.iter().map(|effect| self.effect(effect)).collect();
            eprintln!("  {at:?} {}", effects.join(", "));
        }
    }

    /// An effect as a trace shows it.
    fn effect(&self, effect: &RelayEffect) -> String {
        let (kind, session, id) = match effect {
            RelayEffect::Ask { session, id } => ("Ask", session.0, *id),
            RelayEffect::Advert { session, id } => ("Advert", session.0, *id),
            RelayEffect::Serve { session, id } => ("Serve", session.0, *id),
            RelayEffect::NotFound { session, id } => ("NotFound", session.0, *id),
            RelayEffect::Reserve { call, id } => ("Reserve", call.0, *id),
            RelayEffect::Submit { waiter, body, .. } => ("Submit", waiter.0, body.id()),
            other => return format!("{other:?}"),
        };
        format!("{kind} {session} {}", self.name(id))
    }

    /// One random input, the core perhaps stopping near the end.
    fn input(&mut self, near_end: bool) {
        match self.rng.below(50) {
            0..=3 => self.connect(),
            4 => self.hello(),
            5..=12 => self.inv(),
            13..=19 => self.answer_request(),
            20 if self.rng.one_in(3) => self.unrequested(),
            21 | 22 => self.get_txs(),
            23 => self.end(),
            29..=34 => self.answer_call(near_end),
            35..=38 => self.verdict(near_end),
            39 => self.cancel(),
            // A stopped core publishes nothing more.
            40..=43 if self.stopped => self.tick(),
            40 | 41 => self.change(),
            42 => self.chain(),
            43 => self.toggle(),
            44 | 45 => self.take_inv(),
            46 => self.rtt(),
            47 if self.stops(near_end, 2) => {
                self.note(|_| "the IO sees the core stop".to_owned());
                self.bench.stop();
            }
            _ => self.tick(),
        }
        self.flush();
    }

    /// A peer opens a session, and usually sends its `Hello`: one in ten
    /// waits, one in ten is outside the protocol's limits.
    fn connect(&mut self) {
        if self.bench.tracker.open.len() >= SESSIONS {
            return self.tick();
        }
        let peer = 1 + u64::try_from(self.rng.below(5)).expect("small");
        let behaviour = self
            .rng
            .pick(&[Peer::Honest, Peer::Honest, Peer::Silent, Peer::Liar]);
        let serves = 1 + self.rng.below(MAX_SERVES);
        let (bytes, max) = (MAX_TX_BYTES, MAX_SERVES + 1);
        let bad = [(0, bytes), (max, bytes), (serves, bytes + 1)];
        let hello = match self.rng.below(10) {
            0 => None,
            1 => Some(self.rng.pick(&bad)),
            _ => Some((serves, bytes)),
        };
        let hello = hello.map(|(serves, max_tx_bytes)| Hello {
            serves,
            max_tx_bytes,
        });
        self.note(|_| format!("peer {peer} connects, {behaviour:?}, {hello:?}"));
        let session = self.bench.connect(peer, peer == PEERS);
        self.peers.insert(session, behaviour);
        if let Some(hello) = hello {
            self.bench.hello(session, hello);
        }
    }

    /// A session's peer sends a `Hello`: mostly one that waited, now and
    /// then a second one.
    fn hello(&mut self) {
        let open = self.bench.tracker.open.iter();
        let waiting: Vec<SessionId> = open
            .filter(|(_, open)| !open.hello)
            .map(|(s, _)| *s)
            .collect();
        let session = match self.session() {
            _ if !waiting.is_empty() && !self.rng.one_in(4) => self.rng.pick(&waiting),
            Some(session) => session,
            None => return self.tick(),
        };
        let serves = MAX_REQUESTS_PER_PEER + self.rng.below(3);
        self.note(|_| format!("{} sends a Hello", session.0));
        let max_tx_bytes = MAX_TX_BYTES;
        self.bench.hello(
            session,
            Hello {
                serves,
                max_tx_bytes,
            },
        );
    }

    /// A session's peer announces a few ids: some pooled, some it was sent,
    /// some twice, some at a wrong size.
    fn inv(&mut self) {
        let Some(session) = self.session() else {
            return self.tick();
        };
        let sent = self.sent(session);
        let pooled: Vec<UnminedTxId> = self.bench.pool.keys().copied().collect();
        let mut entries = Vec::new();
        for _ in 0..=self.rng.below(4) {
            let id = match self.rng.below(8) {
                0 if !pooled.is_empty() => self.rng.pick(&pooled),
                1 if !sent.is_empty() => self.rng.pick(&sent),
                _ => self.any_id(),
            };
            let size = self.rng.pick(&[SIZE + 1, MAX_TX_BYTES + 1]);
            let size = if self.rng.one_in(12) { size } else { SIZE };
            entries.push((id, size));
        }
        if self.rng.one_in(6) {
            entries.push(entries[0]);
        }
        self.note(|run| {
            let named: Vec<String> = entries
                .iter()
                .map(|(id, n)| format!("{} {n}", run.name(*id)))
                .collect();
            format!("{} announces {named:?}", session.0)
        });
        self.bench.inv_sized(session, &entries);
    }

    /// A peer answers one of its requests, as its behaviour says.
    fn answer_request(&mut self) {
        let requests = self.requests();
        if requests.is_empty() {
            return self.tick();
        }
        let (session, id, _) = requests[self.rng.below(requests.len())];
        let behaviour = self.peers.get(&session).copied().unwrap_or(Peer::Honest);
        self.note(|run| format!("{} answers {}, {behaviour:?}", session.0, run.name(id)));
        match (behaviour, self.rng.below(20)) {
            (Peer::Silent, _) => self.tick(),
            (Peer::Liar, _) | (Peer::Honest, 0..=2) => self.bench.not_found(session, id),
            (Peer::Honest, 3) => {
                self.bench.arrived(session, id, SIZE + 7);
            }
            (Peer::Honest, other) => {
                let other = if other == 4 { self.any_id() } else { id };
                let body = self.body(other);
                self.bench.deliver(session, id, body);
            }
        }
    }

    /// A peer answers something it was not asked for.
    fn unrequested(&mut self) {
        let Some(session) = self.session() else {
            return self.tick();
        };
        let id = self.any_id();
        self.note(|run| format!("{} answers {} unasked", session.0, run.name(id)));
        if self.rng.one_in(2) {
            self.bench.not_found(session, id);
        } else {
            self.bench.arrived(session, id, SIZE);
        }
    }

    /// A peer asks for an id: mostly one it was sent.
    fn get_txs(&mut self) {
        let Some(session) = self.session() else {
            return self.tick();
        };
        let sent = self.sent(session);
        let id = if !sent.is_empty() && !self.rng.one_in(3) {
            self.rng.pick(&sent)
        } else {
            self.any_id()
        };
        self.note(|run| format!("{} asks for {}", session.0, run.name(id)));
        self.bench.get_txs(session, id);
    }

    /// A session ends.
    fn end(&mut self) {
        let Some(session) = self.session() else {
            return self.tick();
        };
        self.note(|_| format!("{} ends", session.0));
        self.bench.end(session);
    }

    /// Time passes: mostly a little, sometimes past a deadline, rarely past
    /// a cooldown.
    fn tick(&mut self) {
        let by = match self.rng.below(20) {
            0 => Duration::from_secs(60) * u32::try_from(5 + self.rng.below(10)).expect("small"),
            1..=4 => self.rng.millis(30_000),
            _ => self.rng.millis(3_000),
        };
        self.note(|_| format!("{by:?} pass"));
        self.bench.tick(by);
    }

    /// The pool answers a reserve call. A pooled id is mostly `Pooled`.
    fn answer_call(&mut self, near_end: bool) {
        let calls = self.bench.calls();
        if calls.is_empty() {
            return self.tick();
        }
        let (call, id) = calls[self.rng.below(calls.len())];
        let pooled = self.bench.pool.contains_key(&id);
        let answer = match self.rng.below(25) {
            _ if self.stops(near_end, 8) => Err(Unreserved::Stopped),
            _ if pooled && !self.rng.one_in(4) => Err(Unreserved::Pooled),
            0 => self.bench.granted(true),
            1 => Err(Unreserved::Pooled),
            2 => Err(Unreserved::Pending),
            3 | 4 => Err(Unreserved::Rejected {
                scope: self.scope(),
            }),
            5 | 6 => Err(Unreserved::NoRoom),
            7 => Err(Unreserved::Inactive),
            _ => self.bench.granted(false),
        };
        self.note(|run| format!("call {} for {}: {answer:?}", call.0, run.name(id)));
        self.bench.answer(call, answer);
    }

    /// The pool answers a body. An admitted one is published at once or
    /// later.
    fn verdict(&mut self, near_end: bool) {
        let bodies = self.bodies();
        if bodies.is_empty() {
            return self.tick();
        }
        let (waiter, id, supplier) = bodies[self.rng.below(bodies.len())];
        let height = self.bench.tip.height.0;
        let rejected = |scope, charge, stale: bool| Verdict::Rejected {
            scope,
            charge,
            tip: Height(height - u32::from(stale)),
        };
        let verdict = match self.rng.below(20) {
            _ if self.stops(near_end, 3) => Verdict::LocalFault(LocalFault::Stopped),
            0..=14 if supplier == PeerKey(BAD) => {
                rejected(RejectionScope::ExactTip, Charge::Cooldown, false)
            }
            0..=9 => Verdict::Admitted,
            10..=14 => {
                let charges = [
                    Charge::None,
                    Charge::Cooldown,
                    Charge::Cooldown,
                    Charge::Ban,
                ];
                let charge = self.rng.pick(&charges);
                // Now and then decided at an older tip, which charges nothing.
                rejected(self.scope(), charge, self.rng.one_in(5))
            }
            15 => Verdict::NoRoom,
            _ => Verdict::LocalFault(self.rng.pick(&[
                LocalFault::VerifyTimeout,
                LocalFault::StateUnavailable,
                LocalFault::Deactivated,
                LocalFault::WrongTransaction,
            ])),
        };
        if verdict == Verdict::Admitted && self.bench.active && self.rng.one_in(2) {
            self.bench.add(&[id]);
        } else if verdict == Verdict::Admitted {
            self.admitted.push(id);
        }
        self.note(|run| format!("waiter {} of {}: {verdict:?}", waiter.0, run.name(id)));
        self.bench.verdict(waiter, verdict);
    }

    /// The pool cancels a fetch it no longer needs.
    fn cancel(&mut self) {
        let machine = &self.bench.machine;
        let held = self.requests().into_iter();
        let held: Vec<_> = held
            .filter(|(.., held)| machine.holds_reservation(*held))
            .collect();
        if held.is_empty() {
            return self.tick();
        }
        let (session, id, reservation) = held[self.rng.below(held.len())];
        self.note(|run| format!("the pool cancels {}'s fetch of {}", session.0, run.name(id)));
        self.bench.cancel(reservation);
    }

    /// The pool publishes a batch: admissions, a removal, a removal and
    /// re-admission of one id, or a refusal.
    fn change(&mut self) {
        let pooled: Vec<UnminedTxId> = self.bench.pool.keys().copied().collect();
        let reasons = [
            RemovalReason::Mined,
            RemovalReason::Conflicted,
            RemovalReason::Expired,
            RemovalReason::Evicted,
            RemovalReason::Invalidated,
        ];
        let changes = match self.rng.below(6) {
            0 | 1 if self.bench.active => {
                let mut ids = std::mem::take(&mut self.admitted);
                ids.push(self.any_id());
                if !self.reset_out.is_empty() {
                    let back = self.rng.below(self.reset_out.len());
                    ids.push(self.reset_out.swap_remove(back));
                }
                let mut fresh = Vec::new();
                for id in ids {
                    if !fresh.contains(&id) && !self.bench.pool.contains_key(&id) {
                        fresh.push(id);
                    }
                }
                fresh.into_iter().map(Change::Added).collect()
            }
            2 | 3 if !pooled.is_empty() => {
                let id = self.rng.pick(&pooled);
                vec![Change::Removed {
                    id,
                    reason: self.rng.pick(&reasons),
                }]
            }
            4 if !pooled.is_empty() && self.bench.active => {
                let id = self.rng.pick(&pooled);
                let reason = RemovalReason::Evicted;
                vec![Change::Removed { id, reason }, Change::Added(id)]
            }
            _ => vec![Change::Refused(self.any_id())],
        };
        self.note(|run| {
            let named = changes.iter().map(|change| match change {
                Change::Added(id) => format!("added {}", run.name(*id)),
                Change::Removed { id, reason } => format!("removed {} {reason:?}", run.name(*id)),
                Change::Refused(id) => format!("refused {}", run.name(*id)),
            });
            format!("the pool publishes {:?}", named.collect::<Vec<_>>())
        });
        self.bench.publish(changes);
    }

    /// The tip moves: a block, or a reset that sends the pool back through
    /// admission. Or the IO loses its place and resyncs, after changes it
    /// never read.
    fn chain(&mut self) {
        match self.rng.below(3) {
            0 => {
                self.note(|_| "the tip resets".to_owned());
                self.reset_out.extend(self.bench.pool.keys().copied());
                self.bench.new_tip(true);
            }
            1 => {
                self.note(|_| "the tip moves".to_owned());
                self.bench.new_tip(false);
            }
            _ => {
                if self.rng.one_in(2) && !self.bench.pool.is_empty() {
                    let gone = self.rng.below(self.bench.pool.len());
                    self.bench.pool.shift_remove_index(gone);
                }
                if self.bench.active && self.rng.one_in(2) {
                    let id = self.any_id();
                    self.bench.pool.insert(id, SIZE);
                }
                self.note(|run| {
                    let pool = run.bench.pool.keys().map(|id| run.name(*id));
                    format!("the IO resyncs to {:?}", pool.collect::<Vec<_>>())
                });
                self.bench.resync();
            }
        }
    }

    /// The pool turns off, now and then, or on again.
    fn toggle(&mut self) {
        if !self.bench.active {
            self.note(|_| "the pool turns on".to_owned());
            self.bench.turn_on();
        } else if self.rng.one_in(3) {
            self.note(|_| "the pool turns off".to_owned());
            self.bench.turn_off();
        } else {
            self.tick();
        }
    }

    /// A session's IO takes some of its queued adverts.
    fn take_inv(&mut self) {
        let Some(session) = self.session() else {
            return self.tick();
        };
        let taken = self.bench.take_inv(session, 1 + self.rng.below(4));
        self.note(|run| {
            let named = taken.iter().map(|id| run.name(*id));
            format!("{} is sent {:?}", session.0, named.collect::<Vec<_>>())
        });
    }

    /// A session's routine reports a round-trip time.
    fn rtt(&mut self) {
        let Some(session) = self.session() else {
            return self.tick();
        };
        let rtt = self.rng.millis(400);
        self.note(|_| format!("{} reports {rtt:?}", session.0));
        self.bench.rtt(session, rtt);
    }

    /// Ends the run. The IO sees a stopped core go away, or the pool comes
    /// back on, and with every peer that answers answering, everything
    /// settles: unless the core stopped, nothing may be left to fetch. Then
    /// every session ends, and nothing may be left.
    fn drain(&mut self) {
        self.note(|_| "the run drains".to_owned());
        if self.stopped {
            self.bench.stop();
        } else if !self.bench.active {
            self.bench.turn_on();
        }
        self.settle(true);
        if !self.stopped {
            for session in self.bench.tracker.open.keys() {
                let state = self.bench.machine.session(*session).expect("open");
                assert_eq!(state.pending.len(), 0, "{session:?} has ids left to fetch");
            }
            assert_eq!(self.bench.machine.wanted().len(), 0, "the table empties");
        }
        let open: Vec<SessionId> = self.bench.tracker.open.keys().copied().collect();
        for session in open {
            self.bench.end(session);
        }
        self.settle(false);
        self.bench.tracker.assert_drained();
        let machine = &self.bench.machine;
        assert!(
            machine.session_ids().next().is_none(),
            "every session ended"
        );
        assert_eq!(machine.calls_in_flight(), 0, "no call is left");
        assert_eq!(machine.waiters_len(), 0, "no body");
        assert_eq!(machine.reservations_held(), 0, "no reservation is left");
        let wanted = machine.wanted();
        assert_eq!(wanted.len(), 0, "no wanted entry is left");
        assert_eq!(machine.timers_len(), 0, "no timer is left");
        let reactor = machine.reactor();
        assert!(reactor.by_peer().is_empty() && reactor.go().is_empty());
        assert!(reactor.unsettled().is_empty() && reactor.ended_calls().is_empty());
        assert_eq!(machine.next_deadline(), None, "nothing is due");
    }

    /// Answers everything outstanding, then moves time on, until nothing
    /// is due. With `peers`, the peers that answer answer their requests.
    fn settle(&mut self, peers: bool) {
        for _ in 0..10_000 {
            self.flush();
            if let Some((call, id)) = self.bench.calls().first().copied() {
                let answer = if self.stopped {
                    Err(Unreserved::Stopped)
                } else if self.bench.pool.contains_key(&id) {
                    Err(Unreserved::Pooled)
                } else {
                    self.bench.granted(false)
                };
                self.note(|run| format!("call {} for {}: {answer:?}", call.0, run.name(id)));
                self.bench.answer(call, answer);
                continue;
            }
            let answers = |session: &SessionId| self.peers.get(session) != Some(&Peer::Silent);
            let mut requests = self.requests().into_iter();
            if let Some((session, id, _)) = requests.find(|(s, ..)| peers && answers(s)) {
                self.note(|run| format!("{} answers {}", session.0, run.name(id)));
                if self.peers.get(&session) == Some(&Peer::Liar) {
                    self.bench.not_found(session, id);
                } else {
                    let body = self.body(id);
                    self.bench.deliver(session, id, body);
                }
                continue;
            }
            if let Some((waiter, id, _)) = self.bodies().first().copied() {
                let verdict = if self.stopped {
                    Verdict::LocalFault(LocalFault::Stopped)
                } else {
                    self.bench.add(&[id]);
                    Verdict::Admitted
                };
                self.note(|run| format!("waiter {} of {}: {verdict:?}", waiter.0, run.name(id)));
                self.bench.verdict(waiter, verdict);
                continue;
            }
            if !self.admitted.is_empty() && !self.stopped {
                let admitted = std::mem::take(&mut self.admitted);
                self.bench.add(&admitted);
                continue;
            }
            self.note(|_| "time moves to the next deadline".to_owned());
            if !self.bench.next() {
                return;
            }
        }
        panic!("the run never settled");
    }
}

/// Over many random runs the machine keeps its invariants after every input,
/// its outputs agree with the run's record, and each run drains.
#[test]
fn random_runs_keep_the_invariants_and_drain() {
    let env = |name: &str| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
    };
    let runs: u64 = env("RELAY_RUNS").unwrap_or(300);
    let first: u64 = env("RELAY_FIRST").unwrap_or(0);
    let trace = std::env::var_os("RELAY_TRACE").is_some();
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    for seed in first..first + runs {
        let mut run = Run::new(seed, trace);
        run.note(|_| format!("seed {seed}"));
        let steps = 100 + run.rng.below(300);
        // A failing step is traced too.
        let done = panic::catch_unwind(AssertUnwindSafe(|| {
            for step in 0..steps {
                run.input(step * 10 >= steps * 9);
            }
            run.drain();
        }));
        if let Err(failure) = done {
            run.flush();
            eprintln!("seed {seed} failed: rerun it with RELAY_FIRST={seed} RELAY_RUNS=1");
            panic::resume_unwind(failure);
        }
        for (kind, count) in &run.bench.tracker.seen {
            *seen.entry(kind.clone()).or_default() += count;
        }
    }
    if trace {
        eprintln!("{seen:#?}");
    }
    // The runs reach every rule they check.
    if runs >= 300 {
        for kind in REACHED.split(';').map(str::trim) {
            let reached = seen.get(kind).is_some_and(|count| *count > 0);
            assert!(reached, "the runs never reached {kind}: {seen:#?}");
        }
    }
}
