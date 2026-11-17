//! The machine's tests.
//!
//! Every test here drives [`RelayMachine`], the one encoding of `tx-relay`'s
//! rules. None restates a rule to compare against.
//!
//! - [`random`] gives the machine random inputs. After each one it checks
//!   the machine's own invariants, and what a [`tracker::Tracker`] recorded
//!   of the run's inputs and the machine's outputs. At the end the run
//!   drains and must leave the machine empty of work.
//! - [`scenarios`] pins one rule each, by name.
//!
//! A [`Bench`] plays both the peers and the pool. It answers a reserve call
//! or a submission only as the test says, and publishes only the changes
//! the test asks for: it models none of the pool's rules.

use std::time::Duration;

use indexmap::IndexMap;
use zakura_chain::{
    amount::Amount,
    block::{self, Height},
    transaction::{LockTime, Transaction, UnminedTx, UnminedTxId},
    transparent,
};
use zakura_node_services::mempool::port::{
    ChainTip, Change, Charge, RejectionScope, RemovalReason, ReservationId,
};

use self::tracker::{Cause, Tracker};
use super::{
    peer::Hello, CallId, CloseReason, FanOut, Granted, PeerKey, RelayConfig, RelayEffect,
    RelayMachine, SessionId, SnapshotInfo, Unreserved, Verdict, WaiterId, MAX_REQUESTS_PER_PEER,
    MAX_TX_BYTES,
};

mod random;
mod scenarios;
mod tracker;

/// Every test transaction's size: a small transparent one.
pub(super) const SIZE: u32 = 300;

/// The height of the tip a bench starts at.
const HEIGHT: u32 = 100;

/// A transparent transaction with a lock script of `script` bytes, distinct
/// for each `n`.
pub(super) fn body(n: u32, script: usize) -> UnminedTx {
    UnminedTx::from(Transaction::V1 {
        inputs: Vec::new(),
        outputs: vec![transparent::Output {
            value: Amount::zero(),
            lock_script: transparent::Script::new(&vec![0; script]),
        }],
        lock_time: LockTime::Height(Height(n + 1)),
    })
}

/// The script length that makes [`body`] [`SIZE`] bytes.
pub(super) fn script_len() -> usize {
    (0..usize::try_from(SIZE).expect("small"))
        .find(|len| body(0, *len).size() == usize::try_from(SIZE).expect("small"))
        .expect("some script length gives the size")
}

/// The ids of bodies `ns`, of [`SIZE`] bytes each.
pub(super) fn ids(ns: impl IntoIterator<Item = u32>) -> Vec<UnminedTxId> {
    let script = script_len();
    ns.into_iter().map(|n| body(n, script).id()).collect()
}

/// A peer's `Hello`: it serves as many requests as one peer may have out,
/// and takes the protocol's largest transaction.
pub(super) fn hello() -> Hello {
    Hello {
        serves: MAX_REQUESTS_PER_PEER,
        max_tx_bytes: MAX_TX_BYTES,
    }
}

/// The pool state `sizes` describes, on, at one fixed tip.
pub(super) fn on(sizes: Vec<(UnminedTxId, u32)>) -> SnapshotInfo {
    SnapshotInfo {
        active: true,
        tip: tip(0),
        sizes,
    }
}

/// The tip at `height`.
fn tip(height: u32) -> ChainTip {
    let mut hash = [0; 32];
    hash[..4].copy_from_slice(&height.to_be_bytes());
    ChainTip {
        hash: block::Hash(hash),
        height: Height(height),
    }
}

/// One machine, with the test as its peers and its pool. Every input goes
/// through here: the bench hands it to the machine, records the step in its
/// [`Tracker`], and checks the machine's invariants and the record after it.
pub(super) struct Bench {
    pub(super) machine: RelayMachine,
    pub(super) tracker: Tracker,
    /// The test's clock.
    pub(super) now: Duration,
    /// The pool's transactions and their sizes, oldest first, as the test
    /// last published them. A test may change it without publishing, and
    /// then resyncs.
    pub(super) pool: IndexMap<UnminedTxId, u32>,
    /// Whether the pool is on, as the test last published it.
    pub(super) active: bool,
    /// The pool's tip, as the test last published it.
    pub(super) tip: ChainTip,
    /// The effects of the last input.
    pub(super) effects: Vec<RelayEffect>,
    /// Every step's time and effects, while a test traces them.
    pub(super) steps: Option<Vec<(Duration, Vec<RelayEffect>)>>,
    script: usize,
    next_reservation: u64,
}

impl Bench {
    /// A machine with the default fan-out, on an empty pool that is on.
    pub(super) fn new() -> Self {
        Self::with(FanOut::default(), true, &[])
    }

    /// A machine with `fan_out`, on a pool that holds `pooled` and is on if
    /// `active`.
    pub(super) fn with(fan_out: FanOut, active: bool, pooled: &[UnminedTxId]) -> Self {
        let tip = tip(HEIGHT);
        let pool: IndexMap<UnminedTxId, u32> = pooled.iter().map(|id| (*id, SIZE)).collect();
        let mut machine = RelayMachine::new(
            RelayConfig {
                fan_out,
                short_id_key: 7,
            },
            SnapshotInfo {
                active,
                tip,
                sizes: Vec::new(),
            },
            pool.iter().map(|(id, size)| (*id, *size)).collect(),
        );
        machine.set_best_tip(tip.height);
        Self {
            machine,
            tracker: Tracker::new(active, pooled),
            now: Duration::ZERO,
            pool,
            active,
            tip,
            effects: Vec::new(),
            steps: None,
            script: script_len(),
            next_reservation: 0,
        }
    }

    /// Body `n`, of [`SIZE`] bytes.
    pub(super) fn tx(&self, n: u32) -> UnminedTx {
        body(n, self.script)
    }

    /// The id of body `n`.
    pub(super) fn id(&self, n: u32) -> UnminedTxId {
        self.tx(n).id()
    }

    /// Records the step `cause` started, and checks the machine after it.
    fn took(&mut self, cause: Cause) {
        self.effects = self.machine.take_effects();
        if let Some(steps) = &mut self.steps {
            steps.push((self.now, self.effects.clone()));
        }
        self.tracker.step(&cause, &self.effects);
        self.machine.assert_invariants();
        self.tracker.check(&self.machine);
    }

    // The peers.

    /// A session of `peer` opens, unconditional if `exempt`.
    pub(super) fn connect(&mut self, peer: u64, exempt: bool) -> SessionId {
        let session = self.machine.connect(PeerKey(peer), exempt, self.now);
        self.tracker.opened(session, PeerKey(peer), exempt);
        self.took(Cause::Other);
        session
    }

    /// A session of `peer` opens and sends the usual `Hello`.
    pub(super) fn join(&mut self, peer: u64) -> SessionId {
        let session = self.connect(peer, false);
        self.hello(session, hello());
        session
    }

    /// `session`'s peer sends `hello`.
    pub(super) fn hello(&mut self, session: SessionId, hello: Hello) {
        self.machine.hello(session, hello, self.now);
        self.took(Cause::Hello(session));
    }

    /// `session`'s peer announces `ids`, each of [`SIZE`] bytes.
    pub(super) fn inv(&mut self, session: SessionId, ids: &[UnminedTxId]) {
        let entries: Vec<(UnminedTxId, u32)> = ids.iter().map(|id| (*id, SIZE)).collect();
        self.inv_sized(session, &entries);
    }

    /// `session`'s peer announces `entries`, at the sizes they give.
    pub(super) fn inv_sized(&mut self, session: SessionId, entries: &[(UnminedTxId, u32)]) {
        self.machine.inv(session, entries, self.now);
        let ids = entries.iter().map(|(id, _)| *id).collect();
        self.took(Cause::Inv(session, ids));
    }

    /// A body frame for `id` of `size` bytes arrives on `session`. Returns
    /// whether the machine reads it.
    pub(super) fn arrived(&mut self, session: SessionId, id: UnminedTxId, size: u32) -> bool {
        let read = self.machine.tx_arrived(session, id, size, self.now);
        self.took(Cause::Arrived(session, id, read));
        read
    }

    /// `session`'s peer answers the request for `id` with `body`, as the IO
    /// does: the machine judges the frame, then the body if it reads it.
    /// Returns whether it read the body.
    pub(super) fn deliver(&mut self, session: SessionId, id: UnminedTxId, body: UnminedTx) -> bool {
        let size = u32::try_from(body.size()).expect("a test body is small");
        if !self.arrived(session, id, size) {
            return false;
        }
        self.machine.tx(session, id, body, self.now);
        self.took(Cause::Body(session, id));
        true
    }

    /// `session`'s peer answers `NotFound` for `id`.
    pub(super) fn not_found(&mut self, session: SessionId, id: UnminedTxId) {
        self.machine.not_found(session, id, self.now);
        self.took(Cause::NotFound(session, id));
    }

    /// `session`'s peer asks for `id`.
    pub(super) fn get_txs(&mut self, session: SessionId, id: UnminedTxId) {
        self.machine.get_txs(session, id, self.now);
        self.took(Cause::GetTxs(session, id));
    }

    /// `session` ends.
    pub(super) fn end(&mut self, session: SessionId) {
        self.machine.end(session, self.now);
        self.took(Cause::End(session));
    }

    /// `session`'s IO takes up to `max` of its queued adverts into an `Inv`.
    pub(super) fn take_inv(&mut self, session: SessionId, max: usize) -> Vec<UnminedTxId> {
        let taken = self.machine.take_inv(session, max);
        let ids: Vec<UnminedTxId> = taken.into_iter().map(|(id, _)| id).collect();
        self.tracker.sent(session, &ids);
        self.machine.assert_invariants();
        ids
    }

    /// `session`'s routine reports its round-trip time.
    pub(super) fn rtt(&mut self, session: SessionId, rtt: Duration) {
        self.machine.observe_rtt(session, rtt, self.now);
        self.took(Cause::Other);
    }

    // The clock and the pool.

    /// `by` passes.
    pub(super) fn tick(&mut self, by: Duration) {
        self.now += by;
        self.machine.tick(self.now);
        self.took(Cause::Other);
    }

    /// Time moves to the machine's next deadline. Returns false if it has
    /// none.
    pub(super) fn next(&mut self) -> bool {
        let Some(at) = self.machine.next_deadline() else {
            return false;
        };
        assert!(at > self.now, "the next deadline is in the future");
        self.tick(at - self.now);
        true
    }

    /// The pool answers `call`.
    pub(super) fn answer(&mut self, call: CallId, answer: Result<Granted, Unreserved>) {
        self.machine.reserve_answer(call, answer, self.now);
        self.took(Cause::Answer(call, answer));
    }

    /// A fresh reservation.
    pub(super) fn reservation(&mut self) -> ReservationId {
        self.next_reservation += 1;
        ReservationId(self.next_reservation)
    }

    /// A grant of a fresh reservation, its fetch token fired if `cancelled`.
    pub(super) fn granted(&mut self, cancelled: bool) -> Result<Granted, Unreserved> {
        let reservation = self.reservation();
        Ok(Granted {
            reservation,
            cancelled,
        })
    }

    /// The pool grants `call` a fresh reservation.
    pub(super) fn grant(&mut self, call: CallId) -> ReservationId {
        let granted = self.granted(false);
        self.answer(call, granted);
        ReservationId(self.next_reservation)
    }

    /// The pool's verdict on `waiter`.
    pub(super) fn verdict(&mut self, waiter: WaiterId, verdict: Verdict) {
        self.machine.verdict(waiter, verdict, self.now);
        self.took(Cause::Verdict(waiter, verdict));
    }

    /// A rejection of `scope` that earns `charge`, decided at the tip.
    pub(super) fn rejection(&self, scope: RejectionScope, charge: Charge) -> Verdict {
        Verdict::Rejected {
            scope,
            charge,
            tip: self.tip.height,
        }
    }

    /// The pool cancels the fetch under `reservation`.
    pub(super) fn cancel(&mut self, reservation: ReservationId) {
        self.machine.fetch_cancelled(reservation, self.now);
        self.took(Cause::Other);
    }

    /// The pool applies `changes` and publishes them as one batch.
    pub(super) fn publish(&mut self, changes: Vec<Change>) {
        for change in &changes {
            match change {
                Change::Added(id) => {
                    self.pool.insert(*id, SIZE);
                }
                Change::Removed { id, .. } => {
                    self.pool.shift_remove(id);
                }
                Change::Refused(_) => {}
            }
        }
        // The sizes come from the snapshot after the batch.
        let sizes = changes
            .iter()
            .filter_map(|change| match change {
                Change::Added(id) => self.pool.get(id).map(|size| (*id, *size)),
                _ => None,
            })
            .collect();
        let snapshot = SnapshotInfo {
            active: self.active,
            tip: self.tip,
            sizes,
        };
        self.machine
            .pool_changed(changes.clone(), snapshot, self.now);
        self.took(Cause::Pool(changes, self.active));
    }

    /// The pool admits those of `ids` it does not hold.
    pub(super) fn add(&mut self, ids: &[UnminedTxId]) {
        let changes = ids
            .iter()
            .filter(|id| !self.pool.contains_key(*id))
            .map(|id| Change::Added(*id))
            .collect();
        self.publish(changes);
    }

    /// `id` leaves the pool for `reason`.
    pub(super) fn remove(&mut self, id: UnminedTxId, reason: RemovalReason) {
        self.publish(vec![Change::Removed { id, reason }]);
    }

    /// The tip moves up a block that mines nothing pooled. With `reset`,
    /// the tip is reset instead: every pooled transaction leaves, to be
    /// verified again.
    pub(super) fn new_tip(&mut self, reset: bool) {
        self.tip = tip(self.tip.height.0 + 1);
        self.machine.set_best_tip(self.tip.height);
        let changes = if reset {
            self.removing(RemovalReason::Reset)
        } else {
            Vec::new()
        };
        self.publish(changes);
    }

    /// The pool turns off, and everything leaves it.
    pub(super) fn turn_off(&mut self) {
        self.active = false;
        let changes = self.removing(RemovalReason::Deactivated);
        self.publish(changes);
    }

    /// The pool turns on again.
    pub(super) fn turn_on(&mut self) {
        self.active = true;
        self.publish(Vec::new());
    }

    /// Every pooled transaction leaving for `reason`, oldest first.
    fn removing(&self, reason: RemovalReason) -> Vec<Change> {
        self.pool
            .keys()
            .map(|id| Change::Removed { id: *id, reason })
            .collect()
    }

    /// The IO lost its place in the change log, and hands over the pool.
    pub(super) fn resync(&mut self) {
        let visible: Vec<(UnminedTxId, u32)> =
            self.pool.iter().map(|(id, size)| (*id, *size)).collect();
        let ids = visible.iter().map(|(id, _)| *id).collect();
        let snapshot = SnapshotInfo {
            active: self.active,
            tip: self.tip,
            sizes: Vec::new(),
        };
        self.machine.pool_resynced(visible, snapshot, self.now);
        self.took(Cause::Resync(ids, self.active));
    }

    /// The core stops for good.
    pub(super) fn stop(&mut self) {
        self.machine.pool_stopped(self.now);
        self.took(Cause::Stopped);
    }

    // What the last input did, and what is outstanding.

    /// The reserve calls the pool has not answered, oldest first.
    pub(super) fn calls(&self) -> Vec<(CallId, UnminedTxId)> {
        self.tracker
            .calls
            .iter()
            .map(|(call, id)| (*call, *id))
            .collect()
    }

    /// The unanswered reserve call for `id`.
    pub(super) fn call(&self, id: UnminedTxId) -> CallId {
        self.calls()
            .into_iter()
            .find_map(|(call, called)| (called == id).then_some(call))
            .unwrap_or_else(|| panic!("no reserve call for {id:?}"))
    }

    /// Whether the last input reserved `id`.
    pub(super) fn reserved(&self, id: UnminedTxId) -> bool {
        self.effects.iter().any(
            |effect| matches!(effect, RelayEffect::Reserve { id: called, .. } if *called == id),
        )
    }

    /// The requests the last input sent.
    pub(super) fn asked(&self) -> Vec<(SessionId, UnminedTxId)> {
        self.effects
            .iter()
            .filter_map(|effect| match effect {
                RelayEffect::Ask { session, id } => Some((*session, *id)),
                _ => None,
            })
            .collect()
    }

    /// The adverts the last input queued.
    pub(super) fn adverts(&self) -> Vec<(SessionId, UnminedTxId)> {
        self.effects
            .iter()
            .filter_map(|effect| match effect {
                RelayEffect::Advert { session, id } => Some((*session, *id)),
                _ => None,
            })
            .collect()
    }

    /// The sessions the last input closed, and why.
    pub(super) fn closes(&self) -> Vec<(SessionId, CloseReason)> {
        self.effects
            .iter()
            .filter_map(|effect| match effect {
                RelayEffect::Close { session, reason } => Some((*session, *reason)),
                _ => None,
            })
            .collect()
    }

    /// The submission the last input made, if it made one.
    pub(super) fn submitted(&self) -> Option<WaiterId> {
        self.effects.iter().find_map(|effect| match effect {
            RelayEffect::Submit { waiter, .. } => Some(*waiter),
            _ => None,
        })
    }

    // Common sequences.

    /// The pool grants the call for `id`, and the machine asks `session`.
    pub(super) fn fetch(&mut self, session: SessionId, id: UnminedTxId) -> ReservationId {
        let reservation = self.grant(self.call(id));
        assert_eq!(
            self.asked(),
            vec![(session, id)],
            "the grant sends the request"
        );
        reservation
    }

    /// `session`'s peer announces body `n`, and supplies it once asked, as
    /// soon as the machine reserves it. Returns the submission's waiter.
    pub(super) fn supply(&mut self, session: SessionId, n: u32) -> WaiterId {
        let tx = self.tx(n);
        let id = tx.id();
        self.inv(session, &[id]);
        for _ in 0..20 {
            if self.calls().iter().any(|(_, called)| *called == id) {
                break;
            }
            self.tick(Duration::from_millis(500));
        }
        self.fetch(session, id);
        assert!(self.deliver(session, id, tx), "the asked-for body is read");
        self.submitted().expect("a checked body is submitted")
    }
}
