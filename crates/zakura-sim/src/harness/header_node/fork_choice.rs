//! `header-sync/expected-chain-selected`'s selection rule, on the engine
//! snapshot records the node's controlled port makes: the engine as the
//! tap's harness found it before the run, and the engine after each
//! apply's and operator reset's run, whatever its result.
//!
//! At every record the engine's selected tip is a max-work eligible tip of
//! the headers it holds, and its score is that tip's work above the anchor.
//! Ties allow any max-work tip: production prefers the tip it already has,
//! which depends on arrival order, and arrival order isn't an oracle.
//!
//! The model never asks the engine's selector, nor reads a node's work.
//!
//! - **Work.** Every header a run can hold is [`HeaderFixture::extend`]'s
//!   child of its parent on one of the branches the scenario's chains use,
//!   so the check places each held header in that fixture tree from the
//!   anchor, and takes its height and its work above the anchor from the
//!   fixture and its branches' weights.
//! - **Eligibility.** A header is ineligible when it or an ancestor stands
//!   invalidated by the port's `Reset` facts and not reconsidered, which
//!   is the commit-contents check's reset model, or was admitted deferred.
//!   Admission defers a header whose fixture time is more than two hours
//!   past the clock of the first record that holds it (the protocol's
//!   full-validator time rule), and nothing here promotes it later.
//! - **What it holds.** The record's held headers are the engine's actual
//!   state, which is what selection reads. A record holding a header the
//!   fixture tree doesn't give isn't judged: what an apply inserts is
//!   `commit-only-authorized`'s.
//!
//! The snapshot is the engine's own projection of its metadata row, apart
//! from the durable state's copy of that row. A wrong score is this rule's
//! only where the projection made it: a score the snapshot copies from a
//! wrong row, along with the row's own tip, is the row's, which
//! `commit-only-authorized` judges on every run, harness commits included.
//! No run made the seed, so the seed's score is judged here whole. A
//! locator read leaves the engine unchanged, which
//! `commit-only-authorized` judges, so it makes no record.
//!
//! **Taint.** Whether a run left the engine's durable state as its rules
//! allow (its eligibility, validations, work, indexes, projection, and
//! the metadata row's own score) is `commit-only-authorized`'s
//! commit-contents check. Once that check finds a run corrupt, everything
//! selection reads is unreliable, so this check judges neither that run's
//! record nor any later one, and the property is `NotApplicable` from that
//! drain on: no later step counts as passed, and the corruption fails only
//! its owner.
//!
//! **Harness commits.** A harness control that commits straight into the
//! engine leaves an engine run too (`ControlledPort::harness_commit`), so
//! its record is judged like an apply's, and the commit-contents check's
//! taint covers it the same way.
//!
//! **Settled convergence.** In the fork fixture, the check also judges
//! each settled point against the heaviest mirror chain ([`settled`]).
//!
//! The check applies at every judged record. A broken rule is a claim
//! break, which the tap hands
//! [`Claim::ExpectedChainSelected`](super::claim::Claim) at its drain. A
//! reset run with no `Reset` fact is missing evidence, which fails the run.

use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex, PoisonError},
};

use chrono::{DateTime, Utc};
use zakura_chain::{block, work::difficulty::U256};
use zakura_header_chain::{DurableState, EngineSnapshot};
use zakura_network::zakura::{
    observe::{HeaderSyncObserver, ReactorEvent},
    ZakuraPeerId,
};
use zakura_storage::header::{
    controlled::{EngineRun, RunKind},
    fixture::HeaderFixture,
};

use crate::{observe::StorageFact, storage::hash};

/// What [`ForkChoiceEvidence`] keeps.
#[derive(Debug, Default)]
struct Store {
    /// The first drain that found the check applicable.
    applicable: Option<u64>,
    /// Whether a record since the last drain made it applicable.
    pending: bool,
    /// The first drain that found the check tainted.
    tainted: Option<u64>,
    /// Whether a run since the last drain tainted it.
    taint_pending: bool,
    /// The newest drain's settled point, if it found one: its step, and
    /// the convergence break, if any.
    settled: Option<(u64, Option<String>)>,
    /// How many settled points the drains judged.
    settled_points: u64,
    /// The mirrors whose own reactors admitted a session with the node
    /// ([`MirrorAdmits`]).
    mirrors_admitted: HashSet<String>,
}

/// When the fork-choice check first applied, shared by the tap and whoever
/// reads it. Cloning shares the store.
#[derive(Clone, Debug, Default)]
pub struct ForkChoiceEvidence(Arc<Mutex<Store>>);

impl ForkChoiceEvidence {
    /// Runs `change` on the store. Nothing else is called while the lock
    /// is held.
    fn with<R>(&self, change: impl FnOnce(&mut Store) -> R) -> R {
        change(&mut self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Whether a drain up to and including `step`'s found the check
    /// applicable.
    pub fn applicable_by(&self, step: u64) -> bool {
        self.with(|store| store.applicable.is_some_and(|found| found <= step))
    }

    /// Whether any drain found the check applicable.
    pub fn any(&self) -> bool {
        self.with(|store| store.applicable.is_some())
    }

    /// Whether a drain up to and including `step`'s found the check
    /// tainted, after which it judges nothing.
    pub fn tainted_by(&self, step: u64) -> bool {
        self.with(|store| store.tainted.is_some_and(|found| found <= step))
    }

    /// The settled point `step`'s drain found, if it found one: `Some(None)`
    /// if convergence held there, `Some(Some(detail))` if it broke. Whether
    /// no task can run after the step is the reader's to check.
    pub fn settled_at(&self, step: u64) -> Option<Option<String>> {
        self.with(|store| {
            store
                .settled
                .as_ref()
                .filter(|(found, _)| *found == step)
                .map(|(_, verdict)| verdict.clone())
        })
    }

    /// How many drains found every settled-point condition the tap reads,
    /// whether or not a task could still run after their steps.
    pub fn settled_points(&self) -> u64 {
        self.with(|store| store.settled_points)
    }

    /// The observer for the mirror named `mirror`, which records its own
    /// reactor admitting a session with `node`, the node under test
    /// ([`MirrorAdmits`]).
    pub fn mirror(&self, mirror: &str, node: ZakuraPeerId) -> Arc<dyn HeaderSyncObserver> {
        Arc::new(MirrorAdmits {
            mirror: mirror.to_owned(),
            node,
            evidence: self.clone(),
        })
    }

    /// How many mirrors' own reactors have admitted a session with the
    /// node.
    fn mirrors_admitted(&self) -> usize {
        self.with(|store| store.mirrors_admitted.len())
    }
}

/// A mirror's observer, which records the mirror's reactor admitting a
/// session with the node under test, so settled points count a mirror the
/// node itself refused ([`settled`]). It ignores every other event and adds
/// no scheduling point, so the mirror runs as it would unobserved.
#[derive(Debug)]
struct MirrorAdmits {
    /// The mirror's harness name.
    mirror: String,
    /// The node under test.
    node: ZakuraPeerId,
    evidence: ForkChoiceEvidence,
}

impl HeaderSyncObserver for MirrorAdmits {
    /// Records the mirror if `event` admits a session with the node.
    fn observe(&self, event: ReactorEvent) {
        if let ReactorEvent::SessionAdmitted { peer, .. } = event {
            if peer == self.node {
                self.evidence.with(|store| {
                    store.mirrors_admitted.insert(self.mirror.clone());
                });
            }
        }
    }
}

/// A held header as the model places it in the fixture tree.
#[derive(Clone, Copy, Debug)]
struct Placed {
    height: block::Height,
    /// Its work above the anchor.
    work: U256,
    eligible: bool,
}

/// A fixture child as the model builds it.
#[derive(Clone, Copy, Debug)]
struct Child {
    hash: block::Hash,
    /// Its own work.
    work: U256,
    time: DateTime<Utc>,
}

/// The check's state.
pub(super) struct ForkChoice {
    fixture: HeaderFixture,
    /// The fixture branches the scenario's chains use.
    branches: Vec<u8>,
    /// Each fixture child already built, by its parent and branch.
    children: HashMap<(block::Hash, u8), Child>,
    /// Whether admission deferred each header the judged records held, from
    /// the clock of the first record that held it.
    deferred: HashMap<block::Hash, bool>,
    /// Whether the commit-contents check found a run corrupt, after which
    /// no record is judged.
    tainted: bool,
    /// The newest record's selected tip.
    selected: block::Hash,
    /// The settled-convergence rule, in the fork fixture only.
    settled: Option<settled::Settled>,
    /// The headers the judged reset runs left invalidated, as hex.
    invalidated: HashSet<String>,
    /// The drain's `Reset` facts not yet matched to their runs, oldest
    /// first: whether each invalidated, and its target.
    resets: VecDeque<(bool, String)>,
    breaks: Vec<String>,
    /// The first missing-evidence failure.
    missing: Option<String>,
    evidence: ForkChoiceEvidence,
}

impl ForkChoice {
    /// A check on `fixture`'s headers on `branches`, keeping when it
    /// applied in `evidence`, that first judges the engine `seed` holds,
    /// with the snapshot it gives, at `clock`.
    pub(super) fn new(
        evidence: ForkChoiceEvidence,
        fixture: HeaderFixture,
        branches: impl IntoIterator<Item = u8>,
        seed: &DurableState,
        snapshot: &EngineSnapshot,
        clock: DateTime<Utc>,
    ) -> ForkChoice {
        // Branch 0 is every chain's main line.
        let mut branches: Vec<u8> = branches.into_iter().chain([0]).collect();
        branches.sort_unstable();
        branches.dedup();
        let mut check = ForkChoice {
            fixture,
            branches,
            children: HashMap::new(),
            deferred: HashMap::new(),
            tainted: false,
            selected: snapshot.frontiers.header_best.hash,
            settled: None,
            invalidated: HashSet::new(),
            resets: VecDeque::new(),
            breaks: Vec::new(),
            missing: None,
            evidence,
        };
        // No commit-contents check judged the seed's metadata row, so a
        // score the seed's snapshot copies from it is judged here.
        check.record("the engine before the run", seed, snapshot, clock, false);
        check
    }

    /// Judges settled convergence too, against `fork`'s mirrors.
    pub(super) fn settle_in(&mut self, fork: &settled::ForkFixture) {
        self.settled = Some(settled::Settled::new(&self.fixture, fork));
    }

    /// Notes `event`, which the reactor's own task reported when
    /// `on_reactor`, at `now`, for settled points.
    pub(super) fn observe(
        &mut self,
        event: &ReactorEvent,
        on_reactor: bool,
        now: zakura_scheduler::SimInstant,
    ) {
        if let (Some(settled), true) = (self.settled.as_mut(), on_reactor) {
            settled.observe(event, now);
        }
    }

    /// Judges whether `step`'s drain, with `outside` as the tap reads it,
    /// found a settled point, and keeps the verdict for the property, which
    /// reads it only if no task can run after the step. Nothing is judged
    /// once the check is tainted.
    pub(super) fn settle(&mut self, step: u64, outside: settled::Outside) {
        // No property checks step 0, the setup observation's, which may
        // also run before the run's clock does.
        let verdict = match (&mut self.settled, self.tainted) {
            (Some(settled), false) if step > 0 => {
                settled.judge(self.selected, outside, self.evidence.mirrors_admitted())
            }
            _ => None,
        };
        self.evidence.with(|store| {
            store.settled = verdict.map(|verdict| (step, verdict));
            if store.settled.is_some() {
                store.settled_points = store.settled_points.saturating_add(1);
            }
        });
    }

    /// Notes storage's `fact`: a reset's invalidation or reconsideration,
    /// which its run then applies.
    pub(super) fn storage(&mut self, fact: &StorageFact) {
        if let StorageFact::Reset { kind, target, .. } = fact {
            self.resets
                .push_back((kind == "invalidate", target.clone()));
        }
    }

    /// Judges the record one engine `run` left, after the drain's storage
    /// facts. `corrupt` says the commit-contents check found the run left
    /// the engine's durable state other than its rules allow, which taints
    /// this record and every later one.
    pub(super) fn run(&mut self, run: &EngineRun, corrupt: bool) {
        if corrupt && !self.tainted {
            self.tainted = true;
            self.evidence.with(|store| store.taint_pending = true);
        }
        let what = match &run.kind {
            RunKind::Apply(id, _) => format!("apply operation {}", id.0),
            RunKind::HarnessCommit(_) => "a harness commit".to_owned(),
            RunKind::Reset => {
                // Each reset run is its oldest unmatched `Reset` fact's, so
                // the invalidated set changes at the run that changed it.
                match self.resets.pop_front() {
                    Some((true, target)) => {
                        self.invalidated.insert(target);
                    }
                    Some((false, target)) => {
                        self.invalidated.remove(&target);
                    }
                    None => {
                        self.missing.get_or_insert(
                            "ForkChoiceJoin: a reset ran with no Reset fact".to_owned(),
                        );
                        return;
                    }
                }
                "a reset".to_owned()
            }
            RunKind::Locator(..) => return,
        };
        self.record(&what, &run.after, &run.snapshot, run.clock, true);
    }

    /// The breaks found since the last call, or the first missing evidence.
    /// Marks the check applicable at `step` if a record since the last call
    /// made it so.
    pub(super) fn drain(&mut self, step: u64) -> Result<Vec<String>, String> {
        // The setup observation drains at step 0, which no property checks,
        // so the seed record's breaks and applicability wait for step 1's.
        if step == 0 && self.missing.is_none() {
            return Ok(Vec::new());
        }
        // Marked even with evidence missing, so a break the tap takes ahead
        // of that failure keeps its trigger history.
        self.evidence.with(|store| {
            if std::mem::take(&mut store.pending) {
                store.applicable.get_or_insert(step);
            }
            if std::mem::take(&mut store.taint_pending) {
                store.tainted.get_or_insert(step);
            }
        });
        if let Some(missing) = &self.missing {
            return Err(missing.clone());
        }
        Ok(std::mem::take(&mut self.breaks))
    }

    /// The breaks found since the last call, taken even when
    /// [`drain`](Self::drain) failed with missing evidence, so the tap can
    /// report them ahead of that failure.
    pub(super) fn take_breaks(&mut self) -> Vec<String> {
        std::mem::take(&mut self.breaks)
    }

    /// Judges the record `what` left at `clock`: the engine holding
    /// `state`'s headers and giving `snapshot`. `row_judged` says the
    /// commit-contents check judged `state`'s metadata row, so a wrong
    /// score copied from it is that check's.
    fn record(
        &mut self,
        what: &str,
        state: &DurableState,
        snapshot: &EngineSnapshot,
        clock: DateTime<Utc>,
        row_judged: bool,
    ) {
        self.selected = snapshot.frontiers.header_best.hash;
        if self.tainted {
            return;
        }
        let Some(placed) = self.place(state, clock) else {
            return;
        };
        self.evidence.with(|store| store.pending = true);
        let best = placed
            .values()
            .filter(|at| at.eligible)
            .map(|at| at.work)
            .max();
        let tips: BTreeSet<String> = placed
            .iter()
            .filter(|(_, at)| at.eligible && Some(at.work) == best)
            .map(|(held, _)| hash(*held))
            .collect();
        let selected = snapshot.frontiers.header_best;
        let mut found = Vec::new();
        match placed.get(&selected.hash) {
            None => found.push(format!(
                "selected {}, which it doesn't hold",
                hash(selected.hash)
            )),
            Some(at) => {
                if !at.eligible || Some(at.work) != best {
                    found.push(format!(
                        "selected {} with work {} above the anchor, eligible {}, not one of the \
                         max-work eligible tips {tips:?} at work {best:?}",
                        hash(selected.hash),
                        at.work,
                        at.eligible
                    ));
                }
                if at.height != selected.height {
                    found.push(format!(
                        "selected {} at height {}, which the fixture puts at {}",
                        hash(selected.hash),
                        selected.height.0,
                        at.height.0
                    ));
                }
                let score = snapshot.header_best_score;
                let right =
                    score.tip_hash == selected.hash && score.suffix_work.as_u256() == at.work;
                // A score the snapshot copied from a wrong metadata row,
                // with the row's own tip, is the row's, which
                // `commit-only-authorized` owns.
                let row = &state.metadata;
                let copied = row_judged
                    && score == row.header_best_score
                    && selected == row.frontiers.header_best;
                if !right && !copied {
                    found.push(format!(
                        "scored the selected tip as {} at work {}, not {} at work {}",
                        hash(score.tip_hash),
                        score.suffix_work.as_u256(),
                        hash(selected.hash),
                        at.work
                    ));
                }
            }
        }
        for detail in found {
            self.breaks.push(format!(
                "ExpectedChainSelected: after {what}, the engine {detail}"
            ));
        }
    }

    /// Every header `state` holds, placed in the fixture tree from the
    /// anchor, or `None` if it holds one the tree doesn't give. A header no
    /// earlier record held takes its admission from `clock`.
    fn place(
        &mut self,
        state: &DurableState,
        clock: DateTime<Utc>,
    ) -> Option<HashMap<block::Hash, Placed>> {
        let nodes = &state.graph.nodes;
        let anchor = self.fixture.anchor();
        let mut placed = HashMap::new();
        let mut queue = VecDeque::new();
        if nodes.contains_key(&anchor.hash) {
            // The finalized anchor was admitted long before any clock here.
            let eligible = !self.invalidated.contains(&hash(anchor.hash));
            placed.insert(
                anchor.hash,
                Placed {
                    height: anchor.height,
                    work: U256::zero(),
                    eligible,
                },
            );
            queue.push_back(anchor.hash);
        }
        // Each step places a held header no earlier step placed, so the walk
        // ends after at most one step per held header.
        while let Some(parent) = queue.pop_front() {
            let at = placed[&parent];
            for branch in self.branches.clone() {
                let child = self.child(parent, at.height, branch);
                if !nodes.contains_key(&child.hash) || placed.contains_key(&child.hash) {
                    continue;
                }
                let deferred = *self
                    .deferred
                    .entry(child.hash)
                    .or_insert_with(|| deferred_at(child.time, clock));
                let eligible =
                    at.eligible && !deferred && !self.invalidated.contains(&hash(child.hash));
                placed.insert(
                    child.hash,
                    Placed {
                        height: at
                            .height
                            .next()
                            .expect("fixture heights stay far below the maximum"),
                        work: at.work.saturating_add(child.work),
                        eligible,
                    },
                );
                queue.push_back(child.hash);
            }
        }
        (placed.len() == nodes.len()).then_some(placed)
    }

    /// The fixture child of `parent`, at `height`, on `branch`, with its
    /// own work from the branch's target.
    fn child(&mut self, parent: block::Hash, height: block::Height, branch: u8) -> Child {
        let fixture = &self.fixture;
        *self.children.entry((parent, branch)).or_insert_with(|| {
            let header = fixture
                .extend(
                    zakura_header_chain::Frontier::new(height, parent),
                    1,
                    branch,
                )
                .remove(0);
            let work = fixture
                .target(branch)
                .to_work()
                .expect("every fixture target is a valid difficulty");
            Child {
                hash: header.hash(),
                work: U256::from(work),
                time: header.time,
            }
        })
    }
}

/// Whether admission at `clock` defers a header timed `time`: its time is
/// more than two hours past `clock`, the protocol's full-validator time
/// rule. Header times are whole seconds.
fn deferred_at(time: DateTime<Utc>, clock: DateTime<Utc>) -> bool {
    const TWO_HOURS: i64 = 2 * 60 * 60;
    DateTime::<Utc>::from_timestamp(time.timestamp().saturating_sub(TWO_HOURS), 0)
        .is_some_and(|deadline| deadline > clock)
}

pub mod settled;
#[cfg(test)]
mod tests;
