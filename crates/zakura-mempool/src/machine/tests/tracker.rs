//! What a test records of its own inputs and the machine's outputs.
//!
//! The tracker never decides what the machine should do. It keeps simple
//! state that follows from what the test sent and what came back: the names
//! the machine handed out, the jobs it started, the rejections its verdicts
//! say it remembers, and the pool as the change log describes it. Each check compares two of those facts with each other,
//! or one of them with what the machine reports.

use std::collections::{BTreeMap, BTreeSet};

use zakura_chain::{
    transaction::{self, UnminedTx, UnminedTxId},
    transparent::OutPoint,
};
use zakura_node_services::mempool::port::{
    Change, ChangeSeq, Charge, RejectionScope, RemovalReason, ReservationId, Unreserved, Verdict,
};
use zakura_rt::collections::{HashMap, HashSet};

use super::{Machine, Tx};
use crate::machine::{Effect, JobId, Step, WaiterId};

/// The job a test is answering, as [`Tracker::answers`] found it.
struct Answering {
    job: u64,
    tx: UnminedTxId,
    /// Whether the answer is a pass, and the tip has not changed since the
    /// job started.
    passed: bool,
    /// The charge a failing answer carries, when the tip has not changed
    /// since the job started.
    charge: Option<Charge>,
}

/// One run's record.
#[derive(Default)]
pub(crate) struct Tracker {
    /// Every reservation the machine granted.
    reservations: HashSet<u64>,
    /// Every waiter a submission was given, with its transaction, and
    /// whether it has its verdict.
    waiters: BTreeMap<u64, (UnminedTxId, bool)>,
    /// Every job the machine ever started.
    started: HashSet<u64>,
    /// The running jobs, with their transaction.
    jobs: BTreeMap<u64, UnminedTxId>,
    /// The jobs that were running when the tip last changed. Each verified
    /// for another tip.
    stale: BTreeSet<u64>,
    /// Jobs the machine cancelled before the test answered them.
    cancelled: Vec<(u64, UnminedTxId)>,
    /// The body of every transaction the machine has started a job for,
    /// which is every transaction that was ever pooled.
    bodies: HashMap<UnminedTxId, UnminedTx>,
    /// The pool, replayed from the change log.
    pool: HashSet<UnminedTxId>,
    /// What the log says a reset took out of the pool. The machine verifies
    /// each again, and may add it again with nobody waiting for it.
    reset_out: HashSet<UnminedTxId>,
    /// How many changes the log holds.
    logged: u64,
    answering: Option<Answering>,
    /// The charges the failed verifications of each transaction carried.
    charges: HashMap<UnminedTxId, Vec<Charge>>,
    /// The transactions, by exact id, and the effects, by mined id, that a
    /// verification's verdict says the pool rejects until the tip changes.
    refused_exact: HashSet<UnminedTxId>,
    refused_effects: HashSet<transaction::Hash>,
    /// How often each kind of effect, change and answer happened.
    seen: BTreeMap<String, usize>,
}

impl Tracker {
    // What the test tells the tracker.

    /// The machine answered a reservation of `tx`. A rejection the pool
    /// remembers for the tip keeps `tx` from being fetched and verified again.
    pub(crate) fn reserve_answered(
        &mut self,
        tx: UnminedTxId,
        answer: &Result<ReservationId, Unreserved>,
    ) {
        let remembered =
            self.refused_exact.contains(&tx) || self.refused_effects.contains(&tx.mined_id());
        match answer {
            Ok(reservation) => {
                assert!(
                    !remembered,
                    "{tx:?} was reserved while its rejection is remembered for the tip"
                );
                assert!(
                    self.reservations.insert(reservation.0),
                    "{reservation:?} was granted twice"
                );
            }
            Err(_) if remembered => self.saw("a remembered rejection held"),
            Err(_) => {}
        }
    }

    /// A submission of `tx` waits for its verdict as `waiter`.
    pub(crate) fn waits(&mut self, waiter: WaiterId, tx: UnminedTxId) {
        assert!(
            self.waiters.insert(waiter.0, (tx, false)).is_none(),
            "{waiter:?} names two submissions"
        );
    }

    /// The tip is about to change. Every running job verified for the old
    /// one.
    pub(crate) fn tip_changed(&mut self) {
        self.stale.extend(self.jobs.keys().copied());
        self.forget_rejections();
    }

    /// The pool forgot every rejection it remembered for the tip: the tip
    /// changed, or the pool turned off.
    pub(crate) fn forget_rejections(&mut self) {
        self.refused_exact.clear();
        self.refused_effects.clear();
    }

    /// The test is about to answer `job`, with a pass if `passed`. `charge`
    /// is the charge a failing answer carries.
    pub(crate) fn answers(&mut self, job: JobId, passed: bool, charge: Option<Charge>) {
        self.cancelled.retain(|(cancelled, _)| *cancelled != job.0);
        let stale = self.stale.remove(&job.0);
        if stale && self.jobs.contains_key(&job.0) {
            self.saw("answer for another tip");
        }
        self.answering = self.jobs.remove(&job.0).map(|tx| Answering {
            job: job.0,
            tx,
            passed: passed && !stale,
            charge: charge.filter(|_| !stale),
        });
    }

    /// Counts one occurrence of `kind`.
    pub(crate) fn saw(&mut self, kind: impl Into<String>) {
        *self.seen.entry(kind.into()).or_default() += 1;
    }

    // What the tracker tells the test.

    /// The running jobs, oldest first.
    pub(crate) fn jobs(&self) -> Vec<(JobId, UnminedTxId)> {
        self.jobs
            .iter()
            .map(|(job, tx)| (JobId(*job), *tx))
            .collect()
    }

    /// Jobs the machine cancelled and the test has not answered since.
    pub(crate) fn cancelled(&self) -> &[(u64, UnminedTxId)] {
        &self.cancelled
    }

    /// The effects of the pool, as the log describes it.
    pub(crate) fn pooled_effects(&self) -> HashSet<transaction::Hash> {
        self.pool.iter().map(UnminedTxId::mined_id).collect()
    }

    /// Whether the log says the pool holds a transaction with the effects of
    /// `id`.
    pub(crate) fn pooled(&self, id: &UnminedTxId) -> bool {
        self.pool
            .iter()
            .any(|pooled| pooled.mined_id() == id.mined_id())
    }

    /// How often each kind happened.
    pub(crate) fn seen(&self) -> &BTreeMap<String, usize> {
        &self.seen
    }

    // The checks.

    /// Records the step one input caused, and checks it against the record
    /// so far.
    pub(crate) fn step(&mut self, step: &Step) {
        let answering = self.answering.take();
        // The transaction the answered job verified, and whether it passed.
        let decided = answering
            .as_ref()
            .map(|answering| (answering.tx, answering.passed));
        // Whether a submission waits for the answer. Nobody waits for a
        // pooled transaction that is verified again after a reset.
        let awaited = answering.as_ref().is_some_and(|answering| {
            self.waiters
                .values()
                .any(|(tx, answered)| *tx == answering.tx && !*answered)
        });
        let mut answered_job_cancelled = false;
        let mut verdicts: Vec<(UnminedTxId, &Verdict)> = Vec::new();
        for effect in &step.effects {
            match effect {
                Effect::Start { job, tx, .. } => {
                    assert!(self.started.insert(job.0), "{job:?} was started twice");
                    self.jobs.insert(job.0, tx.id());
                    self.bodies.insert(tx.id(), tx.clone());
                    self.saw(if self.reset_out.contains(&tx.id()) {
                        "start again after a reset"
                    } else {
                        "start"
                    });
                }
                Effect::Cancel(job) => {
                    // The job being answered can be cancelled in the same
                    // step: its deadline was due at the answer's time.
                    let answered = answering
                        .as_ref()
                        .is_some_and(|answering| answering.job == job.0);
                    match self.jobs.remove(&job.0) {
                        Some(tx) => self.cancelled.push((job.0, tx)),
                        None => assert!(answered, "{job:?} was cancelled but was not running"),
                    }
                    answered_job_cancelled |= answered;
                    self.saw("cancel");
                }
                Effect::CancelFetch(reservation) => {
                    assert!(
                        self.reservations.contains(&reservation.0),
                        "a fetch was cancelled under {reservation:?}, which was never granted"
                    );
                    self.saw("cancel fetch");
                }
                Effect::Verdict { waiter, verdict } => {
                    let Some((tx, answered)) = self.waiters.get_mut(&waiter.0) else {
                        panic!("a verdict for {waiter:?}, which names no submission");
                    };
                    assert!(!*answered, "{waiter:?} got a second verdict: {verdict:?}");
                    *answered = true;
                    verdicts.push((*tx, verdict));
                    let kind = match verdict {
                        Verdict::Admitted => "verdict Admitted".to_owned(),
                        Verdict::Rejected(_) => "verdict Rejected".to_owned(),
                        Verdict::NoRoom => "verdict NoRoom".to_owned(),
                        Verdict::LocalFault(fault) => format!("verdict LocalFault({fault:?})"),
                    };
                    self.saw(kind);
                }
            }
        }

        // A charge is what a failed verification earned its supplier. So a
        // failing answer's charge reaches every waiter of its transaction,
        // and no verdict carries a charge that no failed verification of its
        // transaction carried. A caller whose fetch outlived its admission
        // gets that admission's verdict later, with its charge.
        let failed = answering
            .filter(|_| awaited && !answered_job_cancelled)
            .and_then(|answering| answering.charge.map(|charge| (answering.tx, charge)));
        if let Some((tx, charge)) = failed {
            self.charges.entry(tx).or_default().push(charge);
        }
        for (tx, verdict) in &verdicts {
            if let Verdict::Rejected(rejection) = verdict {
                assert!(
                    rejection.charge == Charge::None
                        || self
                            .charges
                            .get(tx)
                            .is_some_and(|charges| charges.contains(&rejection.charge)),
                    "{tx:?} was charged {:?}, which no failed verification of it carried",
                    rejection.charge
                );
                // The answered job's rejection is the tip's. A verdict kept
                // for a body that came late may name an older tip.
                if decided.is_some_and(|(decided, _)| decided == *tx) {
                    match rejection.scope {
                        RejectionScope::ExactTip => {
                            self.refused_exact.insert(*tx);
                        }
                        RejectionScope::EffectsTip => {
                            self.refused_effects.insert(tx.mined_id());
                        }
                        _ => {}
                    }
                }
            }
        }
        if let Some((tx, charge)) = failed {
            let mut answered = 0;
            for (_, verdict) in verdicts.iter().filter(|(of, _)| *of == tx) {
                assert!(
                    matches!(verdict, Verdict::Rejected(rejection) if rejection.charge == charge),
                    "a verification of {tx:?} failed with {charge:?}, and a waiter got {verdict:?}"
                );
                answered += 1;
            }
            assert!(
                answered > 0,
                "a verification of {tx:?} failed and nobody was answered"
            );
        }

        let mut added = Vec::new();
        for change in &step.changes {
            self.logged += 1;
            match change {
                Change::Added(id) => {
                    assert!(
                        !self.pooled(id),
                        "the log adds {id:?}, and the pool already holds its effects"
                    );
                    self.pool.insert(*id);
                    added.push(*id);
                    self.saw("added");
                }
                Change::Removed { id, reason } => {
                    assert!(
                        self.pool.remove(id),
                        "the log removes {id:?}, which it never added"
                    );
                    if *reason == RemovalReason::Reset {
                        self.reset_out.insert(*id);
                    }
                    self.saw(format!("removed {reason:?}"));
                }
                Change::Refused(_) => self.saw("refused"),
            }
        }
        // A transaction enters the pool only in the step that answered a
        // passing verification of it for the tip, re-admissions after a reset
        // too. The log adds it there for the submission that step admits, or
        // because a reset took it out.
        for id in added {
            assert_eq!(
                decided,
                Some((id, true)),
                "the log adds {id:?}, and no verification of it passed for this tip"
            );
            let admitted = verdicts
                .iter()
                .any(|(tx, verdict)| *tx == id && matches!(verdict, Verdict::Admitted));
            let back = self.reset_out.remove(&id);
            assert!(
                admitted || back,
                "the log adds {id:?} and no submission of it was admitted"
            );
            if back && !admitted {
                self.saw("added again after a reset");
            }
        }
    }

    /// Compares the pool the log describes with the pool the machine holds
    /// and the snapshot it offers.
    pub(crate) fn check(&self, machine: &Machine) {
        let pool: HashSet<UnminedTxId> = machine.pooled_ids().into_iter().collect();
        assert_eq!(pool, self.pool, "the log leads to the pool");
        let snapshot = machine.snapshot(ChangeSeq(self.logged));
        assert_eq!(snapshot.active, machine.is_active());
        let offered: HashSet<UnminedTxId> = snapshot
            .transactions
            .iter()
            .map(|tx| tx.transaction.id())
            .collect();
        assert_eq!(
            offered.len(),
            snapshot.transactions.len(),
            "a snapshot offers each transaction once"
        );
        assert_eq!(offered, self.pool, "a snapshot offers exactly the pool");
        // Each pooled transaction depends on exactly the pooled transactions
        // whose outputs it spends.
        let pooled_effects = self.pooled_effects();
        for tx in &snapshot.transactions {
            let parents: BTreeSet<[u8; 32]> = tx
                .transaction
                .transaction()
                .spent_outpoints()
                .map(|outpoint| outpoint.hash)
                .filter(|parent| pooled_effects.contains(parent))
                .map(|parent| parent.0)
                .collect();
            let dependencies: BTreeSet<[u8; 32]> = snapshot
                .dependencies
                .dependencies()
                .get(&tx.transaction.id().mined_id())
                .map(|dependencies| dependencies.iter().map(|parent| parent.0).collect())
                .unwrap_or_default();
            assert_eq!(
                dependencies,
                parents,
                "a snapshot's dependencies of {:?}",
                tx.transaction.id()
            );
        }
    }

    /// The test connected blocks that mined `mined`. Nothing pooled is one
    /// of them, or spends an output one of them spent.
    pub(crate) fn connected(&self, mined: &[&Tx]) {
        let effects: HashSet<transaction::Hash> =
            mined.iter().map(|tx| tx.id().mined_id()).collect();
        let spent: HashSet<OutPoint> = mined
            .iter()
            .flat_map(|tx| tx.real.transaction.transaction().spent_outpoints())
            .collect();
        for id in &self.pool {
            assert!(
                !effects.contains(&id.mined_id()),
                "{id:?} is still pooled after a block mined it"
            );
            let body = self
                .bodies
                .get(id)
                .expect("a pooled transaction was verified, so its body was seen");
            assert!(
                !body
                    .transaction()
                    .spent_outpoints()
                    .any(|outpoint| spent.contains(&outpoint)),
                "{id:?} is still pooled after a block spent one of its inputs"
            );
        }
    }

    /// Every submission so far has its verdict.
    pub(crate) fn every_submission_is_answered(&self) {
        for (waiter, (tx, answered)) in &self.waiters {
            assert!(*answered, "waiter {waiter} of {tx:?} never got a verdict");
        }
    }

    /// The log says the pool is empty.
    pub(crate) fn pool_is_empty(&self) {
        assert!(
            self.pool.is_empty(),
            "the log leaves {} pooled",
            self.pool.len()
        );
    }
}
