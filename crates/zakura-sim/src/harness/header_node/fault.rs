//! Whether the node penalizes the peer behind each header fault its storage
//! reports, and keeps that peer out for its readmission cooldown, judged from
//! the log, the storage source's view-side [`FaultAttributions`] and the
//! tap's view-side [`CooldownEvidence`]. This is
//! `header-sync/header-fault-penalized`. hdr-3's
//! `fault-penalized-after-teardown` is its disconnect case: a disconnect
//! before the failure settles changes nothing it owes.
//!
//! [`owed`] applies production's rule for an automatic header-peer fault on
//! its own: the failure blames the header peer, that peer is the source the
//! request named, and the category is `malformed_protocol` or
//! `invalid_header`. Each such failure whose reply the reactor took owes, while
//! the reactor handles the call that took it, a matching `Misbehavior` and,
//! unless the node's configured cooldown is zero, the `CooldownStarted` that
//! follows it. Until that cooldown ends, the reactor admits no session from
//! the peer. [`FaultEvidence::check`] judges this as the run goes, and
//! [`FaultEvidence::penalized`] judges a finished run.

use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    rc::Rc,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use super::adoption::{settled, Acceptance};
use crate::{
    observe::{Entry, ReactorFact, StorageFact, StorageRequest, StorageResult},
    storage::{Blame, FaultAttributions, HEADER_SERVICE},
    Observation, RunEnd,
};

/// The node's storage instance in the header harnesses.
const NODE: u64 = 0;

/// A penalty the node owes for one failed operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwedPenalty {
    /// The failed operation's ID.
    pub op: u64,
    /// `prepare` or `apply`.
    pub stage: &'static str,
    /// The supplier the request named, as the hex of its digest.
    pub source: String,
    /// The session the work belonged to.
    pub session_id: u64,
    /// The `Misbehavior` reason its category calls for.
    pub reason: &'static str,
    /// The reactor port call that submitted the operation, if one did.
    pub call: Option<u64>,
}

/// The `Misbehavior` reason a failure owes, from production's rule applied
/// independently: `blame` names the header peer, that peer is `source`, the
/// request's supplier, and `category` is `malformed_protocol` or
/// `invalid_header`. `None` if it owes none.
pub fn owes_penalty(source: &str, category: &str, blame: &Blame) -> Option<&'static str> {
    let reason = match category {
        "malformed_protocol" => "malformed-message",
        "invalid_header" => "invalid-header",
        _ => return None,
    };
    matches!(blame, Blame::HeaderPeer(blamed) if blamed == source).then_some(reason)
}

/// Each penalty the node's storage facts in `entries` owe, in the order the
/// reactor took the failed replies. A failure whose reply the reactor never
/// took owes nothing, since the reactor never saw it ([`untaken`] lists
/// them). An error if a failure has no attribution in `attributions`, or
/// completes no logged request.
pub fn owed(
    entries: &[Entry],
    attributions: &FaultAttributions,
) -> Result<Vec<OwedPenalty>, String> {
    let judge = Judge::over(entries, attributions, &Cooldowns::Unchecked);
    match judge.error {
        Some(why) => Err(why),
        None => Ok(judge.owed),
    }
}

/// The operations in `entries` that failed with a fault that would owe a
/// penalty, but whose reply the reactor never took: it dropped or timed out
/// the call, or the log ends first. Whether the node should have penalized
/// them can't be judged from the reactor's side.
pub fn untaken(entries: &[Entry], attributions: &FaultAttributions) -> Result<Vec<u64>, String> {
    let judge = Judge::over(entries, attributions, &Cooldowns::Unchecked);
    match judge.error {
        Some(why) => Err(why),
        None => Ok(judge.untaken()),
    }
}

/// A logged prepare or apply request, as [`owed`] reads it.
#[derive(Clone, Debug)]
struct Request {
    stage: &'static str,
    source: String,
    session_id: u64,
    call: Option<u64>,
}

/// A `Misbehavior` the reactor reported while it handled one call: its
/// peer, reason and origin, and its event number.
#[derive(Clone, Debug)]
struct Reported {
    peer: String,
    reason: String,
    origin: Option<u64>,
    seq: u64,
}

/// What the reactor reported while it handled one call's completion.
#[derive(Debug, Default)]
struct Window {
    reported: Vec<Reported>,
    /// Set at the call's `Handled`, after which nothing joins it.
    handled: bool,
}

/// Whether the judge judges, and how it checks the cooldown after each
/// matched `Misbehavior`.
enum Cooldowns<'c> {
    /// It judges nothing and only collects what's owed and untaken, for
    /// [`owed`] and [`untaken`], so no earlier break cuts their lists short.
    Unchecked,
    /// Each matched `Misbehavior` must be followed by a `CooldownStarted` in
    /// `facts` that installs `cooldown`, unless `cooldown` is zero.
    Checked {
        facts: &'c [CooldownFact],
        cooldown: Duration,
    },
}

/// The penalty rule's state after the log entries it has read. It reads each
/// entry once, so the property's check costs nothing per step beyond the new
/// entries.
#[derive(Debug, Default)]
struct Judge {
    /// How many log entries it has read.
    read: usize,
    requests: BTreeMap<u64, Request>,
    /// Each failed operation whose reply the reactor hasn't taken, with its
    /// request and category.
    failed: BTreeMap<u64, (Request, String)>,
    /// Every penalty owed so far, in the order the replies were taken.
    owed: Vec<OwedPenalty>,
    /// The penalties in [`Judge::owed`] whose call's handling hasn't ended,
    /// by index.
    pending: Vec<usize>,
    /// What each call's handling reported, from its `CallEnded` on.
    windows: HashMap<u64, Window>,
    /// The call whose handling is running: its `CallEnded` came, and its
    /// `Handled` hasn't.
    handling: Option<u64>,
    /// Why the evidence can't be read, such as a failure with no
    /// attribution.
    error: Option<String>,
    /// The first break of the rule.
    broken: Option<String>,
}

impl Judge {
    /// A judge that has read all of `entries`.
    fn over(entries: &[Entry], attributions: &FaultAttributions, cooldowns: &Cooldowns) -> Self {
        let mut judge = Judge::default();
        judge.read(entries, attributions, cooldowns);
        judge
    }

    /// Reads the entries after the ones it already read, judging each owed
    /// penalty once its call's handling ends. Stops at the first break.
    fn read(&mut self, entries: &[Entry], attributions: &FaultAttributions, cooldowns: &Cooldowns) {
        let Some(new) = entries.get(self.read..) else {
            self.error
                .get_or_insert_with(|| "the log the penalty rule reads got shorter".to_owned());
            return;
        };
        self.read = entries.len();
        for entry in new {
            if self.failure().is_some() {
                return;
            }
            if let Err(why) = self.entry(entry, attributions) {
                self.error = Some(why);
                return;
            }
            self.judge(cooldowns);
        }
    }

    /// The evidence error or rule break that stopped it, if one did.
    fn failure(&self) -> Option<String> {
        self.error.clone().or_else(|| self.broken.clone())
    }

    /// Reads one log entry.
    fn entry(&mut self, entry: &Entry, attributions: &FaultAttributions) -> Result<(), String> {
        match &entry.obs {
            Observation::Storage {
                service,
                instance: NODE,
                fact,
                ..
            } if service == HEADER_SERVICE => self.storage(fact, attributions),
            Observation::Reactor { seq, event, .. } => {
                self.reactor(*seq, event);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Reads one of the node's header storage facts.
    fn storage(
        &mut self,
        fact: &StorageFact,
        attributions: &FaultAttributions,
    ) -> Result<(), String> {
        match fact {
            StorageFact::Submitted {
                id,
                request:
                    request @ (StorageRequest::PrepareHeaderTarget { owner, source, .. }
                    | StorageRequest::ApplyHeaderTarget { owner, source, .. }),
                call,
                ..
            } => {
                let stage = match request {
                    StorageRequest::PrepareHeaderTarget { .. } => "prepare",
                    _ => "apply",
                };
                self.requests.insert(
                    *id,
                    Request {
                        stage,
                        source: source.clone(),
                        session_id: owner.session_id,
                        call: *call,
                    },
                );
            }
            StorageFact::Completed {
                id,
                result:
                    StorageResult::PrepareFailed { category } | StorageResult::ApplyFailed { category },
                ..
            } => {
                let request = self.requests.get(id).cloned().ok_or_else(|| {
                    format!("UnknownOperation: operation {id} failed with no logged request")
                })?;
                if owes(attributions, *id, &request, category)?.is_some() {
                    self.failed.insert(*id, (request, category.clone()));
                }
            }
            StorageFact::ReplyTaken { id } => {
                if let Some((request, category)) = self.failed.remove(id) {
                    let reason = owes(attributions, *id, &request, &category)?
                        .expect("only a failure that owes a penalty is kept");
                    self.pending.push(self.owed.len());
                    self.owed.push(OwedPenalty {
                        op: *id,
                        stage: request.stage,
                        source: request.source,
                        session_id: request.session_id,
                        reason,
                        call: request.call,
                    });
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Reads one reactor fact, numbered `seq`, into its call's window.
    fn reactor(&mut self, seq: u64, event: &ReactorFact) {
        match event {
            // A dropped call is never handled.
            ReactorFact::CallEnded { call, end, .. } if end != "dropped" => {
                self.windows.entry(*call).or_default();
                self.handling = Some(*call);
            }
            ReactorFact::Handled { call, .. } => {
                if let Some(window) = self.windows.get_mut(call) {
                    window.handled = true;
                }
                if self.handling == Some(*call) {
                    self.handling = None;
                }
            }
            ReactorFact::Misbehavior {
                peer,
                reason,
                origin,
                ..
            } => {
                // The reactor handles one completion at a time, and the
                // tap's order puts each of its events between that call's
                // `CallEnded` and `Handled`.
                let window = self.handling.and_then(|call| self.windows.get_mut(&call));
                if let Some(window) = window {
                    window.reported.push(Reported {
                        peer: peer.clone(),
                        reason: reason.clone(),
                        origin: *origin,
                        seq,
                    });
                }
            }
            _ => {}
        }
    }

    /// Judges each pending penalty whose call's handling has ended. One
    /// that no reactor call submitted has no handling to penalize it, so it
    /// breaks at once.
    fn judge(&mut self, cooldowns: &Cooldowns) {
        if matches!(cooldowns, Cooldowns::Unchecked) {
            return;
        }
        let mut pending = std::mem::take(&mut self.pending);
        pending.retain(|index| {
            let penalty = &self.owed[*index];
            let Some(call) = penalty.call else {
                self.broken.get_or_insert_with(|| {
                    format!(
                        "UnpenalizedFault: {} operation {} failed with a header-peer fault of {} \
                         in session {}, and no reactor call submitted it, so no handling can \
                         penalize it",
                        penalty.stage, penalty.op, penalty.source, penalty.session_id
                    )
                });
                return false;
            };
            let Some(window) = self.windows.get(&call).filter(|window| window.handled) else {
                return true;
            };
            if let Err(why) = matched(penalty, window, cooldowns) {
                self.broken.get_or_insert(why);
            }
            false
        });
        self.pending = pending;
    }

    /// The failed operations that owe a penalty and whose reply the reactor
    /// hasn't taken, in order.
    fn untaken(&self) -> Vec<u64> {
        self.failed.keys().copied().collect()
    }

    /// Why the rule can't judge everything it read yet, if it can't: a
    /// penalty whose call's handling hasn't ended, or a failure whose reply
    /// the reactor hasn't taken.
    fn open(&self) -> Option<String> {
        if let Some(index) = self.pending.first() {
            let penalty = &self.owed[*index];
            return Some(format!(
                "{} operation {} failed with a header-peer fault of {} in session {}, and the \
                 reactor hasn't finished handling call {:?}",
                penalty.stage, penalty.op, penalty.source, penalty.session_id, penalty.call
            ));
        }
        self.failed.keys().next().map(|op| {
            format!(
                "UntakenFault: operation {op} failed with a header-peer fault whose reply the \
                 reactor never took"
            )
        })
    }
}

/// Checks that `window`, the handling of `penalty`'s call, reported a
/// `Misbehavior` naming the source's peer, the work's session as its origin
/// and the reason the category calls for, then the cooldown `cooldowns`
/// wants for it.
fn matched(penalty: &OwedPenalty, window: &Window, cooldowns: &Cooldowns) -> Result<(), String> {
    let what = format!(
        "{} operation {} failed with a header-peer fault of {} in session {}",
        penalty.stage, penalty.op, penalty.source, penalty.session_id
    );
    let reported: Vec<&Reported> = window
        .reported
        .iter()
        .filter(|reported| {
            reported.peer == penalty.source
                && reported.reason == penalty.reason
                && reported.origin == Some(penalty.session_id)
        })
        .collect();
    if reported.is_empty() {
        return Err(format!(
            "UnpenalizedFault: {what}, and the reactor reported no {} Misbehavior from that \
             session while it handled call {:?}",
            penalty.reason, penalty.call
        ));
    }
    let Cooldowns::Checked { facts, cooldown } = cooldowns else {
        return Ok(());
    };
    if cooldown.is_zero() {
        return Ok(());
    }
    let cooled = facts.iter().any(|fact| {
        matches!(fact, CooldownFact::Started { peer, origin, cooldown: started, after, .. }
            if *peer == penalty.source
                && *origin == penalty.session_id
                && started == cooldown
                && reported.iter().any(|reported| reported.seq == *after))
    });
    if cooled {
        Ok(())
    } else {
        Err(format!(
            "UncooledFault: {what}, and the reactor installed no {cooldown:?} readmission \
             cooldown for it after its Misbehavior"
        ))
    }
}

/// The first session the reactor admitted from a peer during that peer's
/// readmission cooldown, as a break, reading `facts` in the order the
/// reactor reported them.
fn readmitted(facts: &[CooldownFact]) -> Option<String> {
    let mut cooled: Vec<(&str, Duration, Duration)> = Vec::new();
    for fact in facts {
        match fact {
            CooldownFact::Started {
                peer, cooldown, at, ..
            } => cooled.push((peer, *at, at.saturating_add(*cooldown))),
            CooldownFact::Admitted {
                peer,
                session_id,
                at,
            } => {
                if let Some((_, from, until)) = cooled
                    .iter()
                    .find(|(cooled, _, until)| *cooled == peer.as_str() && at < until)
                {
                    return Some(format!(
                        "ReadmittedInCooldown: the reactor admitted session {session_id} from \
                         {peer} at {at:?}, inside the readmission cooldown it started at \
                         {from:?} until {until:?}"
                    ));
                }
            }
            CooldownFact::Refused { .. } => {}
        }
    }
    None
}

/// The reason failed operation `id`, from `request`, with `category`, owes,
/// by its attribution, or an error if it has none.
fn owes(
    attributions: &FaultAttributions,
    id: u64,
    request: &Request,
    category: &str,
) -> Result<Option<&'static str>, String> {
    let blame = attributions
        .get(NODE, id)
        .ok_or_else(|| format!("MissingAttribution: failed operation {id} has no attribution"))?;
    Ok(owes_penalty(&request.source, category, &blame))
}

/// One fact about the node's readmission cooldowns, kept out of the log.
/// Peers are written as the log writes them: the hex of their digest. `at`
/// is the virtual time since the run's start that the reactor reported it
/// at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CooldownFact {
    /// `CooldownStarted`, reported right after reactor event `after`, the
    /// `Misbehavior` it follows.
    Started {
        /// The cooled peer.
        peer: String,
        /// The session whose work showed the fault.
        origin: u64,
        /// How long the cooldown lasts.
        cooldown: Duration,
        /// When it started.
        at: Duration,
        /// The number of the last logged reactor event before it.
        after: u64,
    },
    /// `SessionRefused`.
    Refused {
        /// The refused peer.
        peer: String,
        /// The refused session.
        session_id: u64,
        /// When.
        at: Duration,
    },
    /// `SessionAdmitted`, which the refusal rule reads.
    Admitted {
        /// The admitted peer.
        peer: String,
        /// The admitted session.
        session_id: u64,
        /// When.
        at: Duration,
    },
}

/// What [`CooldownEvidence`] holds.
#[derive(Debug, Default)]
struct CooldownStore {
    facts: Vec<CooldownFact>,
    /// Set when the run's log froze. Facts after it are dropped.
    frozen: bool,
}

/// The reactor's cooldown facts, shared by its tap and the harness's view.
/// Cloning shares the store.
#[derive(Clone, Debug, Default)]
pub struct CooldownEvidence(Arc<Mutex<CooldownStore>>);

impl CooldownEvidence {
    /// Locks the store. Every change completes under the lock, and nothing
    /// else is called while it's held.
    fn store(&self) -> std::sync::MutexGuard<'_, CooldownStore> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records `fact`, unless the evidence is frozen.
    pub(super) fn record(&self, fact: CooldownFact) {
        let mut store = self.store();
        if !store.frozen {
            store.facts.push(fact);
        }
    }

    /// Stops recording facts, when the run's log freezes, as the adoption
    /// evidence does.
    pub(super) fn freeze(&self) {
        self.store().frozen = true;
    }

    /// Every fact so far, in the order the reactor reported them.
    pub fn facts(&self) -> Vec<CooldownFact> {
        self.store().facts.clone()
    }
}

/// The node's fault evidence: whom its storage failures blamed, the
/// cooldowns its reactor started and the sessions it admitted or refused,
/// the cooldown its config sets, and, once the run ends, whether it
/// settled. Cloning shares all of it.
#[derive(Clone, Debug, Default)]
pub struct FaultEvidence {
    attributions: FaultAttributions,
    cooldowns: CooldownEvidence,
    /// The node's configured readmission cooldown.
    cooldown: Duration,
    settled: Rc<std::cell::Cell<Option<bool>>>,
    /// The property's judge, which reads each log entry once.
    judge: Rc<RefCell<Judge>>,
}

/// What [`FaultEvidence::check`] found after a step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FaultCheck {
    /// No fault owes anything, and the reactor started no cooldown.
    Nothing,
    /// Everything judged so far holds, and nothing is open.
    Holds,
    /// Everything judged so far holds, but this is still open.
    Open(String),
    /// The rule broke.
    Broken(String),
}

impl FaultEvidence {
    /// Evidence that reads `attributions` and `cooldowns`, for a node whose
    /// config sets a readmission cooldown of `cooldown`.
    pub fn new(
        attributions: FaultAttributions,
        cooldowns: CooldownEvidence,
        cooldown: Duration,
    ) -> Self {
        FaultEvidence {
            attributions,
            cooldowns,
            cooldown,
            ..FaultEvidence::default()
        }
    }

    /// Whom each failed prepare and apply blamed.
    pub fn attributions(&self) -> &FaultAttributions {
        &self.attributions
    }

    /// The cooldowns the reactor started and the sessions it admitted or
    /// refused.
    pub fn cooldowns(&self) -> &CooldownEvidence {
        &self.cooldowns
    }

    /// Records how the run ended, and whether it [`settled`].
    pub fn end(&self, end: &RunEnd) {
        self.settled.set(Some(settled(end)));
    }

    /// `header-sync/header-fault-penalized` after a step, over `entries`,
    /// the run's log so far, which only ever grows: each penalty [`owed`],
    /// once the reactor has handled the call that took its failed reply,
    /// was matched by a `Misbehavior` naming the source's peer, the work's
    /// session as its origin and the reason the category calls for,
    /// reported strictly between that call's `CallEnded` and `Handled` in
    /// the tap's order. Unless the configured cooldown is zero, that
    /// `Misbehavior` is followed by a `CooldownStarted` for the same peer and
    /// session that installs the configured cooldown. No session from a
    /// peer is admitted while a cooldown started for it lasts.
    pub fn check(&self, entries: &[Entry]) -> FaultCheck {
        let facts = self.cooldowns.facts();
        let cooldowns = Cooldowns::Checked {
            facts: &facts,
            cooldown: self.cooldown,
        };
        let mut judge = self.judge.borrow_mut();
        judge.read(entries, &self.attributions, &cooldowns);
        if let Some(why) = judge.failure().or_else(|| readmitted(&facts)) {
            return FaultCheck::Broken(why);
        }
        if let Some(why) = judge.open() {
            return FaultCheck::Open(why);
        }
        let started = facts
            .iter()
            .any(|fact| matches!(fact, CooldownFact::Started { .. }));
        if judge.owed.is_empty() && !started {
            FaultCheck::Nothing
        } else {
            FaultCheck::Holds
        }
    }

    /// The same rule as [`FaultEvidence::check`] over a finished run's
    /// `entries`. A penalty still open when a settled run ends is a break.
    /// Open at any other end, or before the run has ended, it's
    /// inconclusive. With nothing else open, a fault the reactor never took
    /// ([`untaken`]) leaves the rule inconclusive too, so a node that
    /// abandons such work can't pass it.
    pub fn penalized(&self, entries: &[Entry]) -> Acceptance {
        let facts = self.cooldowns.facts();
        let cooldowns = Cooldowns::Checked {
            facts: &facts,
            cooldown: self.cooldown,
        };
        let judge = Judge::over(entries, &self.attributions, &cooldowns);
        if let Some(why) = judge.failure().or_else(|| readmitted(&facts)) {
            return Acceptance::Violated(why);
        }
        let Some(&index) = judge.pending.first() else {
            return match judge.open() {
                None => Acceptance::Holds,
                Some(why) => Acceptance::Inconclusive(why),
            };
        };
        let penalty = &judge.owed[index];
        let what = format!(
            "{} operation {} failed with a header-peer fault of {} in session {}, and the \
             reactor reported no {} Misbehavior from that session while it handled call {:?}",
            penalty.stage,
            penalty.op,
            penalty.source,
            penalty.session_id,
            penalty.reason,
            penalty.call
        );
        match self.settled.get() {
            Some(true) => Acceptance::Violated(format!("UnpenalizedFault: {what}")),
            Some(false) => Acceptance::Inconclusive(format!(
                "the run stopped while a task could be partway through handling it: {what}"
            )),
            None => Acceptance::Inconclusive(format!("the run hasn't ended: {what}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::{StorageBranch, StoragePoint, WorkOwner, WorkScope};

    /// A header-peer fault of `source`.
    fn blame(source: &str) -> Blame {
        Blame::HeaderPeer(source.to_owned())
    }

    /// The node's storage fact `fact`, its `seq`th.
    fn storage(seq: u64, fact: StorageFact) -> Entry {
        Entry {
            step: seq,
            obs: Observation::Storage {
                service: HEADER_SERVICE.to_owned(),
                instance: NODE,
                seq,
                txn: seq,
                fact,
            },
        }
    }

    /// A point with an empty hash.
    fn point(height: u32) -> StoragePoint {
        StoragePoint {
            height,
            hash: String::new(),
        }
    }

    /// Operation `id`: a prepare from `source` in `session_id` that reactor
    /// call `id` submitted, failing with `category` and taken by the
    /// reactor, as facts `id * 10` on.
    fn failed_prepare(id: u64, source: &str, session_id: u64, category: &str) -> Vec<Entry> {
        let owner = WorkOwner {
            kind: "header".to_owned(),
            scope: WorkScope {
                header_generation: 1,
                branch: StorageBranch {
                    anchor: String::new(),
                    target: String::new(),
                },
            },
            verified_generation: None,
            body_work_epoch: None,
            session_id,
            request_id: id,
        };
        vec![
            storage(
                id * 10,
                StorageFact::Submitted {
                    id,
                    request: StorageRequest::PrepareHeaderTarget {
                        owner,
                        source: source.to_owned(),
                        common_ancestor: point(0),
                        target: point(1),
                        headers: 1,
                    },
                    digest: String::new(),
                    call: Some(id),
                },
            ),
            storage(
                id * 10 + 1,
                StorageFact::Completed {
                    id,
                    outcome: "engine".to_owned(),
                    result: StorageResult::PrepareFailed {
                        category: category.to_owned(),
                    },
                    delivered: true,
                    generation: 0,
                    state: String::new(),
                },
            ),
            storage(id * 10 + 2, StorageFact::ReplyTaken { id }),
        ]
    }

    /// The reactor's `event`.
    fn reactor(event: ReactorFact) -> Entry {
        Entry {
            step: 0,
            obs: Observation::Reactor {
                instance: 0,
                seq: 0,
                storage_seq: 0,
                node: "node".to_owned(),
                event,
            },
        }
    }

    /// The reactor's `Misbehavior` against `peer` from session `origin`.
    fn misbehavior(peer: &str, origin: u64) -> Entry {
        reactor(ReactorFact::Misbehavior {
            peer: peer.to_owned(),
            reason: "invalid-header".to_owned(),
            origin: Some(origin),
            current: None,
        })
    }

    /// The reactor's handling of call `call`'s completion, during which it
    /// reported `during`.
    fn handled(call: u64, during: Vec<Entry>) -> Vec<Entry> {
        let mut entries = vec![reactor(ReactorFact::CallEnded {
            call,
            end: "replied".to_owned(),
            op: Some(call),
        })];
        entries.extend(during);
        entries.push(reactor(ReactorFact::Handled {
            call,
            panicked: false,
        }));
        entries
    }

    /// Fault evidence for a settled run, where each operation in `blamed`
    /// blamed its source.
    fn evidence(blamed: &[(u64, Blame)]) -> FaultEvidence {
        let attributions = FaultAttributions::default();
        for (id, blame) in blamed {
            attributions.insert(NODE, *id, blame.clone());
        }
        let evidence =
            FaultEvidence::new(attributions, CooldownEvidence::default(), Duration::ZERO);
        evidence.settled.set(Some(true));
        evidence
    }

    #[test]
    fn only_a_header_peer_fault_of_the_requests_source_owes_a_penalty() {
        assert_eq!(
            owes_penalty("aa", "invalid_header", &blame("aa")),
            Some("invalid-header")
        );
        assert_eq!(
            owes_penalty("aa", "malformed_protocol", &blame("aa")),
            Some("malformed-message")
        );
        // Another source, another kind of peer, no peer, or a category that
        // isn't the supplier's fault.
        assert_eq!(owes_penalty("aa", "invalid_header", &blame("bb")), None);
        assert_eq!(
            owes_penalty("aa", "invalid_header", &Blame::BodyPeer("aa".to_owned())),
            None
        );
        assert_eq!(owes_penalty("aa", "invalid_header", &Blame::None), None);
        assert_eq!(
            owes_penalty("aa", "local_resource_or_storage", &blame("aa")),
            None
        );
    }

    /// The mismatched-source control: a failure that blames a peer other
    /// than the request's source owes nothing, so its missing penalty is no
    /// break. The same log blaming the request's source owes one.
    #[test]
    fn a_fault_blaming_another_source_owes_nothing() {
        // Through the storage source's projection: a typed failure of a
        // request naming source A that blames source B keeps B's digest, and
        // owes nothing.
        use zakura_header_chain::{Attribution, ErrorCategory, SourceId};
        use zakura_storage::header::evidence::ResultIdentity;

        let digest = |byte: u8| -> String { format!("{byte:02x}").repeat(32) };
        let failed = |blamed: u8| ResultIdentity::PrepareFailed {
            category: ErrorCategory::InvalidHeader,
            attribution: Attribution::HeaderPeer(SourceId::from_digest([blamed; 32])),
        };
        let log = failed_prepare(1, &digest(0xaa), 3, "invalid_header");
        let projected = FaultAttributions::default();
        projected.record_result(NODE, 1, &failed(0xbb));
        assert_eq!(projected.get(NODE, 1), Some(blame(&digest(0xbb))));
        assert_eq!(owed(&log, &projected).unwrap(), Vec::new());
        projected.record_result(NODE, 1, &failed(0xaa));
        assert_eq!(owed(&log, &projected).unwrap().len(), 1);

        let log = failed_prepare(1, "aa", 3, "invalid_header");
        assert_eq!(
            evidence(&[(1, blame("bb"))]).penalized(&log),
            Acceptance::Holds
        );
        let owed = owed(&log, evidence(&[(1, blame("aa"))]).attributions()).unwrap();
        assert_eq!(
            owed,
            vec![OwedPenalty {
                op: 1,
                stage: "prepare",
                source: "aa".to_owned(),
                session_id: 3,
                reason: "invalid-header",
                call: Some(1),
            }]
        );
    }

    /// The penalty must come while the reactor handles the call that took
    /// the failed reply, against the work's session.
    #[test]
    fn the_penalty_comes_while_the_failing_call_is_handled() {
        let attributed = evidence(&[(1, blame("aa"))]);
        let broken = |log: &[Entry]| {
            matches!(attributed.penalized(log),
                Acceptance::Violated(why) if why.starts_with("UnpenalizedFault"))
        };
        let mut log = failed_prepare(1, "aa", 3, "invalid_header");
        // Another session's or peer's penalty during the handling.
        log.extend(handled(1, vec![misbehavior("aa", 4), misbehavior("bb", 3)]));
        assert!(broken(&log));
        // A matching penalty for a later completion, such as a malformed
        // page from the same session, isn't this failure's.
        log.extend(handled(2, vec![misbehavior("aa", 3)]));
        assert!(broken(&log));
        // A matching penalty before the call ended isn't either.
        let mut early = failed_prepare(1, "aa", 3, "invalid_header");
        early.push(misbehavior("aa", 3));
        early.extend(handled(1, Vec::new()));
        assert!(broken(&early));
        let mut log = failed_prepare(1, "aa", 3, "invalid_header");
        log.extend(handled(1, vec![misbehavior("aa", 3)]));
        assert_eq!(attributed.penalized(&log), Acceptance::Holds);
    }

    /// Each failure needs its own call's penalty.
    #[test]
    fn each_failure_needs_its_own_penalty() {
        let attributed = evidence(&[(1, blame("aa")), (2, blame("aa"))]);
        let mut log = failed_prepare(1, "aa", 3, "invalid_header");
        log.extend(failed_prepare(2, "aa", 3, "invalid_header"));
        log.extend(handled(1, vec![misbehavior("aa", 3)]));
        log.extend(handled(2, Vec::new()));
        assert!(matches!(
            attributed.penalized(&log),
            Acceptance::Violated(why) if why.contains("operation 2")
        ));
    }

    #[test]
    fn an_unpenalized_fault_is_inconclusive_until_a_settled_end() {
        let attributed = evidence(&[(1, blame("aa"))]);
        let log = failed_prepare(1, "aa", 3, "invalid_header");
        attributed.settled.set(None);
        assert!(matches!(
            attributed.penalized(&log),
            Acceptance::Inconclusive(_)
        ));
        attributed.settled.set(Some(false));
        assert!(matches!(
            attributed.penalized(&log),
            Acceptance::Inconclusive(_)
        ));
    }

    /// A fault whose reply the reactor never took owes no penalty, and leaves
    /// the rule inconclusive rather than holding.
    #[test]
    fn an_untaken_fault_is_inconclusive() {
        let attributed = evidence(&[(1, blame("aa"))]);
        let mut log = failed_prepare(1, "aa", 3, "invalid_header");
        log.pop();
        assert_eq!(owed(&log, attributed.attributions()).unwrap(), Vec::new());
        assert_eq!(untaken(&log, attributed.attributions()).unwrap(), vec![1]);
        assert!(matches!(
            attributed.penalized(&log),
            Acceptance::Inconclusive(why) if why.starts_with("UntakenFault")
        ));
        // A failure that owes nothing isn't listed.
        let unblamed = evidence(&[(1, blame("bb"))]);
        assert_eq!(
            untaken(&log, unblamed.attributions()).unwrap(),
            Vec::<u64>::new()
        );
        assert_eq!(unblamed.penalized(&log), Acceptance::Holds);
    }

    #[test]
    fn a_failure_with_no_attribution_is_a_break() {
        let log = failed_prepare(1, "aa", 3, "invalid_header");
        assert!(matches!(
            evidence(&[]).penalized(&log),
            Acceptance::Violated(why) if why.starts_with("MissingAttribution")
        ));
    }

    /// The reactor's `Misbehavior` against `peer` from session `origin`, as
    /// its event `seq`.
    fn misbehavior_at(seq: u64, peer: &str, origin: u64) -> Entry {
        let mut entry = misbehavior(peer, origin);
        if let Observation::Reactor { seq: at, .. } = &mut entry.obs {
            *at = seq;
        }
        entry
    }

    /// A cooldown of `peer` for session `origin`'s fault, started at `at`
    /// seconds right after reactor event `after`.
    fn started(peer: &str, origin: u64, cooldown: u64, at: u64, after: u64) -> CooldownFact {
        CooldownFact::Started {
            peer: peer.to_owned(),
            origin,
            cooldown: Duration::from_secs(cooldown),
            at: Duration::from_secs(at),
            after,
        }
    }

    /// A session `session_id` from `peer` admitted at `at` seconds.
    fn admitted(peer: &str, session_id: u64, at: u64) -> CooldownFact {
        CooldownFact::Admitted {
            peer: peer.to_owned(),
            session_id,
            at: Duration::from_secs(at),
        }
    }

    /// Fault evidence for a settled run of a node with a 60 s cooldown,
    /// where operation 1 blamed `aa` and the reactor reported `facts`.
    fn cooled(facts: Vec<CooldownFact>) -> FaultEvidence {
        let attributions = FaultAttributions::default();
        attributions.insert(NODE, 1, blame("aa"));
        let cooldowns = CooldownEvidence::default();
        for fact in facts {
            cooldowns.record(fact);
        }
        let evidence = FaultEvidence::new(attributions, cooldowns, Duration::from_secs(60));
        evidence.settled.set(Some(true));
        evidence
    }

    /// With a cooldown configured, the matched `Misbehavior` must be
    /// followed by the `CooldownStarted` it installs: same peer and session,
    /// the configured length, and right after that `Misbehavior`.
    #[test]
    fn a_penalty_starts_the_configured_cooldown_after_its_misbehavior() {
        let mut log = failed_prepare(1, "aa", 3, "invalid_header");
        log.extend(handled(1, vec![misbehavior_at(7, "aa", 3)]));
        let judged = |facts| cooled(facts).penalized(&log);
        assert_eq!(judged(vec![started("aa", 3, 60, 2, 7)]), Acceptance::Holds);
        for facts in [
            Vec::new(),
            vec![started("bb", 3, 60, 2, 7)],
            vec![started("aa", 4, 60, 2, 7)],
            vec![started("aa", 3, 30, 2, 7)],
            vec![started("aa", 3, 60, 2, 6)],
        ] {
            assert!(
                matches!(judged(facts.clone()),
                    Acceptance::Violated(why) if why.starts_with("UncooledFault")),
                "{facts:?}"
            );
        }
        // A zero cooldown installs nothing, so none is owed.
        let unconfigured = cooled(Vec::new());
        let unconfigured = FaultEvidence {
            cooldown: Duration::ZERO,
            ..unconfigured
        };
        assert_eq!(unconfigured.penalized(&log), Acceptance::Holds);
    }

    /// No session from a cooled peer is admitted until its cooldown ends.
    /// Another peer, or an admission from before the cooldown or from its
    /// end on, is fine.
    #[test]
    fn a_cooled_peer_is_not_readmitted_until_its_cooldown_ends() {
        let cooldown = || started("aa", 3, 60, 2, 7);
        for (facts, broken) in [
            (vec![cooldown(), admitted("aa", 4, 30)], true),
            (vec![cooldown(), admitted("aa", 4, 2)], true),
            (vec![cooldown(), admitted("aa", 4, 62)], false),
            (vec![cooldown(), admitted("bb", 4, 30)], false),
            (vec![admitted("aa", 4, 2), cooldown()], false),
        ] {
            let evidence = cooled(facts.clone());
            assert_eq!(
                matches!(evidence.penalized(&[]),
                    Acceptance::Violated(why) if why.starts_with("ReadmittedInCooldown")),
                broken,
                "{facts:?}"
            );
            assert_eq!(
                matches!(evidence.check(&[]), FaultCheck::Broken(_)),
                broken,
                "{facts:?}"
            );
        }
    }

    /// The property's check reads the log as it grows: a penalty is open
    /// until its call's handling ends, then holds or breaks for good.
    #[test]
    fn the_check_judges_each_penalty_once_its_handling_ends() {
        assert_eq!(cooled(Vec::new()).check(&[]), FaultCheck::Nothing);
        let evidence = cooled(vec![started("aa", 3, 60, 2, 7)]);
        let mut log = failed_prepare(1, "aa", 3, "invalid_header");
        log.extend(handled(1, vec![misbehavior_at(7, "aa", 3)]));
        let handled_at = log.len() - 1;
        assert!(matches!(
            evidence.check(&log[..handled_at]),
            FaultCheck::Open(_)
        ));
        assert_eq!(evidence.check(&log), FaultCheck::Holds);

        let evidence = cooled(Vec::new());
        assert!(matches!(
            evidence.check(&log[..handled_at]),
            FaultCheck::Open(_)
        ));
        assert!(matches!(
            evidence.check(&log),
            FaultCheck::Broken(why) if why.starts_with("UncooledFault")
        ));

        // A fault whose reply the reactor hasn't taken stays open.
        let evidence = cooled(Vec::new());
        let mut untaken = failed_prepare(1, "aa", 3, "invalid_header");
        untaken.pop();
        assert!(matches!(
            evidence.check(&untaken),
            FaultCheck::Open(why) if why.starts_with("UntakenFault")
        ));
    }

    /// [`owed`] and [`untaken`] list every failure even after an earlier
    /// one went unpenalized, which the penalty rule calls a break.
    #[test]
    fn the_lists_go_on_past_an_unpenalized_fault() {
        let attributions = FaultAttributions::default();
        for id in 1..=3 {
            attributions.insert(NODE, id, blame("aa"));
        }
        let mut log = failed_prepare(1, "aa", 3, "invalid_header");
        log.extend(handled(1, Vec::new()));
        log.extend(failed_prepare(2, "aa", 3, "invalid_header"));
        let mut third = failed_prepare(3, "aa", 3, "invalid_header");
        third.pop();
        log.extend(third);
        let evidence =
            FaultEvidence::new(attributions, CooldownEvidence::default(), Duration::ZERO);
        assert!(matches!(
            evidence.penalized(&log),
            Acceptance::Violated(why) if why.contains("operation 1")
        ));
        let owed: Vec<u64> = owed(&log, evidence.attributions())
            .unwrap()
            .iter()
            .map(|penalty| penalty.op)
            .collect();
        assert_eq!(owed, vec![1, 2]);
        assert_eq!(untaken(&log, evidence.attributions()).unwrap(), vec![3]);
    }
}
