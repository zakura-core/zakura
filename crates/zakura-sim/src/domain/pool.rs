//! The pool core's properties (M1.8).
//!
//! The pool core runs as one task behind the port, so a check can't read its
//! state between steps without a preemption point. The harness's own tasks
//! read the pool through the port as any caller would, and record what they
//! saw in a [`PoolEvidence`]. Each property's check reads that record. A
//! fact a task records is about a moment inside the run, so a check judges
//! every fact recorded so far, not only the last step's.
//!
//! Two properties are judged at the run's end too, through
//! [`end_violations`], because only a settled run shows what was still owed.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, MutexGuard},
};

use crate::{
    end::{Outcome, RunEnd},
    property::{
        CheckCtx, EndViolation, HasView, Kind, Property, PropertyId, Source, Status,
        TriggerHistory, Verdict,
    },
};

/// Every submission gets exactly one verdict from the core.
pub const VERDICT_FOR_EVERY_SUBMISSION: &str = "pool/verdict-for-every-submission";
/// A rejection charges exactly its body's peer supplier, never the reserving
/// source, another peer or a local submission, and a cooldown has started
/// when the caller gets the verdict.
pub const SUPPLIER_CHARGED: &str = "pool/supplier-charged";
/// No source holds more slots than the core's limits, counting each
/// reservation until its body is submitted and each body until its
/// verification ends.
pub const ADMISSIONS_WITHIN_LIMITS: &str = "pool/admissions-within-limits";
/// Every published snapshot is a consistent pool, with the dependency edges
/// its bodies imply.
pub const CONSISTENT: &str = "pool/consistent";
/// The change log leads to exactly each published snapshot, and each entry
/// follows from the ones before it.
pub const LOG_COMPLETE: &str = "pool/log-complete";
/// A reorg that leaves pooled transactions valid gives them back.
pub const SURVIVES_REORG: &str = "pool/survives-reorg";

/// Every property in this module, in report order.
pub const IDS: [&str; 6] = [
    VERDICT_FOR_EVERY_SUBMISSION,
    SUPPLIER_CHARGED,
    ADMISSIONS_WITHIN_LIMITS,
    CONSISTENT,
    LOG_COMPLETE,
    SURVIVES_REORG,
];

/// What the harness's tasks saw, in the order they saw it. Transactions are
/// named by their index in the harness's fixed universe.
#[derive(Debug, Default)]
pub struct PoolEvidence {
    /// Submissions sent to the core.
    pub submits_started: u64,
    /// Submissions the core answered with a verdict.
    pub submits_answered: u64,
    /// Submissions answered because the core dropped the reply while it
    /// still ran, which the port reports as `LocalFault(Stopped)`.
    pub stopped: Vec<usize>,
    /// Verifications that returned a failure.
    pub failed_outcomes: u64,
    /// Verifications that returned a failure that charges a cooldown.
    pub cooldown_failures: u64,
    /// For each rejection that charged a peer supplier a cooldown: the peer,
    /// and whether the port showed it cooling when the verdict arrived.
    pub charges: Vec<(String, bool)>,
    /// Cooldowns that fell on a supplier other than the reserving source.
    pub relayed_charges: u64,
    /// Invalid bodies this node submitted, which charge nobody.
    pub local_failures: u64,
    /// The peers that may be cooling down: each supplied an invalid body or
    /// reported a violation.
    pub may_cool: BTreeSet<String>,
    /// The first charge that fell on the wrong peer, or a verdict whose
    /// charge differs from what its body's script says.
    pub misattributed: Option<String>,
    /// Reservations granted and not yet submitted, failed or dropped: the
    /// transaction, and its source's label.
    pub held: BTreeMap<usize, String>,
    /// Transactions the verifier is checking: a submission's source label,
    /// or `None` for a revalidation.
    pub verifying: BTreeMap<usize, Option<String>>,
    /// The source label each transaction was last granted to.
    pub slot_source: BTreeMap<usize, String>,
    /// The most slots counted at once, in all.
    pub peak_total: u64,
    /// The most slots counted at once against one peer source.
    pub peak_source: u64,
    /// The first count past a limit.
    pub past_limit: Option<String>,
    /// The transactions the verifier passed.
    pub passed: BTreeSet<usize>,
    /// Pairs of transactions that spend the same chain output.
    pub conflict_pairs: Vec<(usize, usize)>,
    /// Snapshots the observer checked.
    pub snapshots_checked: u64,
    /// The first inconsistent snapshot.
    pub inconsistent: Option<String>,
    /// Snapshots whose change log the observer replayed.
    pub logs_checked: u64,
    /// Removals the change log carried.
    pub removals_seen: u64,
    /// Additions the change log carried.
    pub additions_seen: u64,
    /// The first snapshot the log did not lead to.
    pub log_mismatch: Option<String>,
    /// Reorgs that happened while the pool held visible transactions.
    pub reorgs_with_pool: u64,
    /// What was visible just before the last such reorg.
    pub before_reorg: BTreeSet<usize>,
    /// What the last snapshot the observer saw held.
    pub last_visible: BTreeSet<usize>,
    /// The transactions the current chain mined.
    pub mined: BTreeSet<usize>,
    /// The transactions each tip's chain mined, by the tip's hash.
    pub mined_at: BTreeMap<[u8; 32], BTreeSet<usize>>,
    /// Whether the chain script has finished.
    pub chain_done: bool,
    /// Each transaction's expiry height, if it has one.
    pub expiries: BTreeMap<usize, u32>,
    /// The height of the last snapshot's tip.
    pub tip: u32,
    /// How the run ended, once it has.
    pub end: Option<Outcome>,
}

impl PoolEvidence {
    /// Counts the slots certainly in use now, and records the first count
    /// past a limit.
    ///
    /// A reservation counts from the moment its caller holds it until the
    /// caller submits or drops it, and a submitted body while the verifier
    /// checks it. Both lie inside the time the core counts the slot, so the
    /// count never exceeds the core's own. A revalidation counts only
    /// toward the node's limit.
    pub fn count_slots(&mut self) {
        let mut by_source: BTreeMap<String, u64> = BTreeMap::new();
        for source in self.held.values().chain(self.verifying.values().flatten()) {
            *by_source.entry(source.clone()).or_default() += 1;
        }
        let total = u64::try_from(self.held.len() + self.verifying.len())
            .expect("the harness's universe is small");
        self.peak_total = self.peak_total.max(total);
        let mut past = None;
        for (source, count) in by_source.into_iter().filter(|(source, _)| source != LOCAL) {
            self.peak_source = self.peak_source.max(count);
            if count > LIMITS.per_source {
                past.get_or_insert(format!("{source} holds {count} slots"));
            }
        }
        if total > LIMITS.pending {
            past.get_or_insert(format!("the sources hold {total} slots"));
        }
        if self.past_limit.is_none() {
            self.past_limit = past;
        }
    }
}

/// The label of the submitter that is this node. It has no limit of its
/// own.
pub const LOCAL: &str = "local";

/// The evidence the harness's tasks share. Each task holds the lock only
/// between preemption points, so a check never finds it held.
pub type SharedEvidence = Arc<Mutex<PoolEvidence>>;

/// Locks `evidence`. No task panics while holding it.
pub fn lock(evidence: &SharedEvidence) -> MutexGuard<'_, PoolEvidence> {
    evidence
        .lock()
        .expect("no harness task panics while it holds the evidence lock")
}

/// The pool's limits the harness runs with, for the admission check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// The node-wide limit.
    pub pending: u64,
    /// One source's limit.
    pub per_source: u64,
}

/// The limits the harness runs the core with.
pub const LIMITS: Limits = Limits {
    pending: 6,
    per_source: 2,
};

/// What a pool check reads: the shared evidence.
pub type PoolView = SharedEvidence;

/// The six properties, in [`IDS`] order.
pub fn properties<S: HasView<View = PoolView>>() -> Vec<Property<S>> {
    vec![
        Property {
            id: PropertyId::new(VERDICT_FOR_EVERY_SUBMISSION),
            claim: "every submission gets exactly one verdict, and never a dropped reply while the core runs",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::View],
            status: Status::Active,
            check: check_verdicts::<S>,
            witness: Some(TriggerHistory::View(|view, _| {
                let evidence = lock(view);
                evidence.failed_outcomes > 0 && evidence.submits_started > 0
            })),
        },
        Property {
            id: PropertyId::new(SUPPLIER_CHARGED),
            claim: "a rejection charges exactly its body's peer supplier, never the reserving source, another peer or a local submission, and a cooldown has started when the caller gets the verdict",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::View],
            status: Status::Active,
            check: check_charges::<S>,
            witness: Some(TriggerHistory::View(|view, _| {
                let evidence = lock(view);
                evidence.relayed_charges > 0 || evidence.local_failures > 0
            })),
        },
        Property {
            id: PropertyId::new(ADMISSIONS_WITHIN_LIMITS),
            claim: "no peer source holds more slots than its limit, nor all sources more than the node's, counting reservations until submitted and bodies while verified",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::View],
            status: Status::Active,
            check: check_admissions::<S>,
            witness: Some(TriggerHistory::View(|view, _| {
                let evidence = lock(view);
                evidence.peak_total >= LIMITS.pending || evidence.peak_source >= LIMITS.per_source
            })),
        },
        Property {
            id: PropertyId::new(CONSISTENT),
            claim: "every published snapshot holds no two transactions that spend one output, no expired transaction, no transaction without its unmined parent, exactly the dependency edges its bodies imply, and no dependency group past three",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::View],
            status: Status::Active,
            check: check_consistent::<S>,
            witness: Some(TriggerHistory::View(|view, _| {
                let evidence = lock(view);
                evidence
                    .conflict_pairs
                    .iter()
                    .any(|(a, b)| evidence.passed.contains(a) && evidence.passed.contains(b))
            })),
        },
        Property {
            id: PropertyId::new(LOG_COMPLETE),
            claim: "replaying the change log from the start leads to exactly each published snapshot's transactions, and each entry is allowed by the entries before it",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::View],
            status: Status::Active,
            check: check_log::<S>,
            // A patched core can drop or repeat entries, so the trigger is
            // the first addition a publication carried.
            witness: Some(TriggerHistory::View(|view, _| lock(view).additions_seen > 0)),
        },
        Property {
            id: PropertyId::new(SURVIVES_REORG),
            claim: "after a reorg, every transaction that was visible before it and is still valid on the new chain is visible again once the run settles",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::View],
            status: Status::Active,
            check: check_reorg::<S>,
            witness: Some(TriggerHistory::View(|view, _| {
                lock(view).reorgs_with_pool > 0
            })),
        },
    ]
}

/// [`VERDICT_FOR_EVERY_SUBMISSION`] after a step: no reply was dropped.
/// The rest, that every submission was answered, waits for the run's end.
fn check_verdicts<S: HasView<View = PoolView>>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let view = cx.view();
    let evidence = lock(&view);
    if let Some(tx) = evidence.stopped.first() {
        return cx.violated(
            "a verdict for every submission while the core runs",
            format!("the core dropped the reply to transaction {tx}'s submission"),
        );
    }
    if evidence.submits_answered > 0 {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

/// [`SUPPLIER_CHARGED`] after a step.
fn check_charges<S: HasView<View = PoolView>>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let view = cx.view();
    let evidence = lock(&view);
    if let Some(problem) = &evidence.misattributed {
        return cx.violated(
            "each charge on its body's peer supplier alone",
            problem.clone(),
        );
    }
    if let Some((peer, _)) = evidence.charges.iter().find(|(_, cooling)| !cooling) {
        return cx.violated(
            "the charged supplier is cooling down when its verdict arrives",
            format!("{peer} was not cooling down"),
        );
    }
    if evidence.charges.is_empty() {
        Verdict::NotApplicable
    } else {
        Verdict::Holds
    }
}

/// [`ADMISSIONS_WITHIN_LIMITS`] after a step.
fn check_admissions<S: HasView<View = PoolView>>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let view = cx.view();
    let evidence = lock(&view);
    if let Some(count) = &evidence.past_limit {
        return cx.violated(
            format!(
                "at most {} slots per peer source and {} in all",
                LIMITS.per_source, LIMITS.pending
            ),
            count.clone(),
        );
    }
    if evidence.peak_total > 0 {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

/// [`CONSISTENT`] after a step.
fn check_consistent<S: HasView<View = PoolView>>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let view = cx.view();
    let evidence = lock(&view);
    if let Some(problem) = &evidence.inconsistent {
        return cx.violated("a consistent published pool", problem.clone());
    }
    if evidence.snapshots_checked > 0 {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

/// [`LOG_COMPLETE`] after a step.
fn check_log<S: HasView<View = PoolView>>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let view = cx.view();
    let evidence = lock(&view);
    if let Some(problem) = &evidence.log_mismatch {
        return cx.violated("a change log that leads to the snapshot", problem.clone());
    }
    if evidence.logs_checked > 0 {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

/// [`SURVIVES_REORG`] after a step: it applies once a reorg hit a pool with
/// visible transactions. The claim itself waits for the run's end.
fn check_reorg<S: HasView<View = PoolView>>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let view = cx.view();
    if lock(&view).reorgs_with_pool > 0 {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

/// Records how the run ended.
pub fn ended(evidence: &SharedEvidence, end: &RunEnd) {
    lock(evidence).end = Some(end.outcome);
}

/// The violations only a settled run's end shows: a submission still
/// waiting for its verdict, and a transaction a reorg did not give back.
///
/// A run that ended at a violation or a harness failure is not settled, so
/// it shows neither.
pub fn end_violations(evidence: &SharedEvidence) -> Vec<EndViolation> {
    let evidence = lock(evidence);
    if !matches!(evidence.end, Some(Outcome::Idle { .. } | Outcome::Budget)) {
        return Vec::new();
    }
    let mut violations = Vec::new();
    if evidence.submits_started != evidence.submits_answered {
        violations.push(EndViolation {
            property: PropertyId::new(VERDICT_FOR_EVERY_SUBMISSION),
            expected: "every submission answered once the run settles".to_owned(),
            actual: format!(
                "{} submissions sent and {} answered",
                evidence.submits_started, evidence.submits_answered
            ),
        });
    }
    if evidence.reorgs_with_pool > 0 {
        let missing: Vec<usize> = evidence
            .before_reorg
            .iter()
            .copied()
            .filter(|tx| !evidence.mined.contains(tx))
            .filter(|tx| {
                evidence
                    .expiries
                    .get(tx)
                    .is_none_or(|expiry| *expiry > evidence.tip)
            })
            .filter(|tx| !evidence.last_visible.contains(tx))
            .collect();
        if !missing.is_empty() {
            violations.push(EndViolation {
                property: PropertyId::new(SURVIVES_REORG),
                expected: "every still-valid transaction visible again after the reorg".to_owned(),
                actual: format!("transactions {missing:?} stayed out of the pool"),
            });
        }
    }
    violations
}
