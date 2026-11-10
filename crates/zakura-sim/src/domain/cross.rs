//! Cross-service properties (properties.md, Cross-service sync): claims
//! about header sync and block sync together on one node, over one
//! engine.
//!
//! The checks judge the node's facts against a [`Dag`], the harness's own
//! record of every fixture block it made, never against the node's engine.
//! So a node that confuses two branches can't make its own check agree with
//! it.

use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use zakura_header_chain::Frontier;
use zakura_storage::block::fixture::FixtureBlock;

use crate::{
    harness::block_node::view::BlockSyncView,
    observe::{BlockSyncFact, StorageFact, StorageRequest, WorkOwner},
    property::{
        CheckCtx, HasView, Kind, Property, PropertyId, Source, Status, TriggerHistory, Verdict,
    },
    record::machine::{BodyAuthority, ReactorAction},
    storage::{hash, BLOCK_SERVICE},
    Observation, ObservationLog,
};

/// Every fixture block a run made: each one's height and parent, by hash,
/// above the anchor. Hashes are lowercase hex, as observations carry them.
#[derive(Clone, Debug)]
pub struct Dag {
    /// The anchor's hash, every chain's root.
    anchor: String,
    /// Each block's height and parent hash.
    parents: BTreeMap<String, (u32, String)>,
}

impl Dag {
    /// A DAG holding only `anchor`.
    pub fn new(anchor: Frontier) -> Self {
        Dag {
            anchor: hash(anchor.hash),
            parents: BTreeMap::new(),
        }
    }

    /// Adds `blocks`, a chain extending `parent`.
    pub fn extend(&mut self, parent: Frontier, blocks: &[FixtureBlock]) {
        let mut parent = hash(parent.hash);
        for block in blocks {
            let own = hash(block.hash());
            self.parents.insert(own.clone(), (block.height.0, parent));
            parent = own;
        }
    }

    /// The height of the block `hash`, if the run made it.
    pub fn height(&self, hash: &str) -> Option<u32> {
        if hash == self.anchor {
            return Some(0);
        }
        self.parents.get(hash).map(|(height, _)| *height)
    }

    /// The ancestor of the block `tip` at `height`, `tip` itself at its own
    /// height. `None` if the run never made `tip` or `height` is above it.
    pub fn ancestor(&self, tip: &str, height: u32) -> Option<&str> {
        let mut at = self.parents.get_key_value(tip).map(|(key, _)| key.as_str());
        if at.is_none() && tip == self.anchor {
            at = Some(self.anchor.as_str());
        }
        let mut at = at?;
        loop {
            let own = self.height(at)?;
            if own == height {
                return Some(at);
            }
            if own < height {
                return None;
            }
            at = self.parents.get(at).map(|(_, parent)| parent.as_str())?;
        }
    }

    /// Whether the chain ending at `tip` holds `ancestor`.
    pub fn descends(&self, tip: Frontier, ancestor: Frontier) -> bool {
        self.ancestor(&hash(tip.hash), ancestor.height.0) == Some(hash(ancestor.hash).as_str())
    }
}

/// The engine's committed view, as the harness read it after a step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewSample {
    /// Its state version.
    pub state_version: u64,
    /// Its body-work epoch.
    pub epoch: u64,
    /// Its selected header tip.
    pub header_best: Frontier,
    /// Its verified tip.
    pub verified_best: Frontier,
}

/// What the cross checks read: the DAG, the engine's views the harness
/// read, and the node's facts folded from the log.
#[derive(Debug)]
pub struct CrossModel {
    /// Every fixture block the run made.
    pub dag: Dag,
    /// Views read and not yet judged.
    samples: Vec<ViewSample>,
    /// The last view judged.
    last: Option<ViewSample>,
    /// Log entries folded so far.
    folded: usize,
    /// The target hash of the reactor's adopted body-work authority, and
    /// its epoch.
    adopted: Option<(String, u64)>,
    /// Each adopted target, in order, for the heights a reorganization
    /// replaced.
    targets: Vec<String>,
    /// Findings for [`TIPS_CONSISTENT`], not yet reported.
    tip_findings: Vec<Finding>,
    /// Findings for [`BLOCK_REQUESTS_FOLLOW_HEADERS`], not yet reported.
    request_findings: Vec<Finding>,
    /// Findings for [`HEADER_REORG_RETIRES_BLOCK_WORK`], not yet reported.
    submit_findings: Vec<Finding>,

    /// Views judged.
    pub views: u64,
    /// Requests judged.
    pub requests: u64,
    /// Submissions judged.
    pub submissions: u64,
    /// The judged count each [`Check`] last reported, so a check holds only
    /// at a step that judged something.
    reported: BTreeMap<Check, u64>,
    /// Whether a view's selected path replaced the previous view's.
    pub saw_replaced_selection: bool,
    /// Whether the node published a request for a height whose block an
    /// adopted reorganization replaced.
    pub saw_request_on_replaced_height: bool,
    /// Whether storage took a submission at a height whose block an adopted
    /// reorganization replaced.
    pub saw_submission_on_replaced_height: bool,
}

/// One of the cross checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Check {
    /// [`TIPS_CONSISTENT`], over views.
    Views,
    /// [`BLOCK_REQUESTS_FOLLOW_HEADERS`], over published requests.
    Requests,
    /// [`HEADER_REORG_RETIRES_BLOCK_WORK`], over submissions.
    Submissions,
}

/// A violation a cross check found.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Finding {
    expected: String,
    actual: String,
}

impl CrossModel {
    /// A model over `dag`, before any view.
    pub fn new(dag: Dag) -> Self {
        CrossModel {
            dag,
            samples: Vec::new(),
            last: None,
            folded: 0,
            adopted: None,
            targets: Vec::new(),
            tip_findings: Vec::new(),
            request_findings: Vec::new(),
            submit_findings: Vec::new(),
            views: 0,
            requests: 0,
            submissions: 0,
            reported: BTreeMap::new(),
            saw_replaced_selection: false,
            saw_request_on_replaced_height: false,
            saw_submission_on_replaced_height: false,
        }
    }

    /// Records `sample`, the engine's view after a step, if its state
    /// version is new.
    pub fn sample(&mut self, sample: ViewSample) {
        let newest = self.samples.last().or(self.last.as_ref());
        if newest.is_none_or(|newest| newest.state_version != sample.state_version) {
            self.samples.push(sample);
        }
    }

    /// Takes `check`'s findings, and whether it judged anything since it
    /// last reported.
    fn take(&mut self, check: Check) -> (Vec<Finding>, bool) {
        let (findings, judged) = match check {
            Check::Views => (&mut self.tip_findings, self.views),
            Check::Requests => (&mut self.request_findings, self.requests),
            Check::Submissions => (&mut self.submit_findings, self.submissions),
        };
        let findings = std::mem::take(findings);
        let last = self.reported.insert(check, judged).unwrap_or(0);
        (findings, judged > last)
    }

    /// Judges every view read since the last call, in order.
    fn judge_views(&mut self) {
        for sample in std::mem::take(&mut self.samples) {
            self.views += 1;
            for (name, tip) in [
                ("header tip", sample.header_best),
                ("verified tip", sample.verified_best),
            ] {
                if self.dag.height(&hash(tip.hash)) != Some(tip.height.0) {
                    self.tip_findings.push(Finding {
                        expected: format!("the view's {name} is a block the run made"),
                        actual: format!("state version {} names {tip:?}", sample.state_version),
                    });
                }
            }
            if let Some(last) = &self.last {
                let selection_kept = self.dag.descends(sample.header_best, last.header_best);
                let verified_kept = self.dag.descends(sample.verified_best, last.verified_best);
                let verified_left = sample.verified_best != last.verified_best
                    && !self.dag.descends(sample.header_best, sample.verified_best);
                self.saw_replaced_selection |= !selection_kept;
                if (!selection_kept || !verified_kept || verified_left)
                    && sample.epoch <= last.epoch
                {
                    self.tip_findings.push(Finding {
                        expected: "a view whose selected or verified path replaced the last \
                                   one's, or whose verified tip left the selected chain, starts \
                                   a new body-work epoch"
                            .to_owned(),
                        actual: format!(
                            "epoch {} after {} ({:?}, {:?}) moved to ({:?}, {:?}) at state \
                             version {}",
                            sample.epoch,
                            last.epoch,
                            last.header_best,
                            last.verified_best,
                            sample.header_best,
                            sample.verified_best,
                            sample.state_version
                        ),
                    });
                }
            }
            self.last = Some(sample);
        }
    }

    /// Folds every log entry not yet folded.
    fn catch_up(&mut self, log: &ObservationLog) {
        let entries = log.entries();
        while self.folded < entries.len() {
            match &entries[self.folded].obs {
                Observation::BlockSync { event, .. } => self.block_sync(event),
                Observation::Storage {
                    service,
                    fact:
                        StorageFact::BlockSubmitted {
                            request:
                                StorageRequest::SubmitBlock {
                                    owner,
                                    height: Some(height),
                                    hash,
                                    ..
                                },
                            ..
                        },
                    ..
                } if service == BLOCK_SERVICE => self.submission(owner, *height, hash),
                _ => {}
            }
            self.folded += 1;
        }
    }

    /// Folds a block-sync event.
    fn block_sync(&mut self, event: &BlockSyncFact) {
        match event {
            BlockSyncFact::Reactor {
                transition: ReactorAction::AuthorityChanged { authority, .. },
                ..
            } => self.adopt(authority),
            BlockSyncFact::Reactor { state, .. } if self.adopted.is_none() => {
                // The startup's authority, as the block-domain model reads
                // it.
                if let Some(authority) = &state.scope {
                    self.adopt(authority);
                }
            }
            BlockSyncFact::Request {
                owner,
                heights,
                step,
                ..
            } if step == "published" => self.request(owner, heights),
            _ => {}
        }
    }

    /// Adopts `authority`'s target.
    fn adopt(&mut self, authority: &BodyAuthority) {
        let target = authority.header.branch.target.clone();
        if self.targets.last() != Some(&target) {
            self.targets.push(target.clone());
        }
        self.adopted = Some((target, authority.body_work_epoch));
    }

    /// Whether an adopted reorganization replaced the block at `height`:
    /// two adopted targets disagree there.
    fn replaced(&self, height: u32) -> bool {
        let mut blocks = self
            .targets
            .iter()
            .filter_map(|target| self.dag.ancestor(target, height));
        blocks
            .next()
            .is_some_and(|first| blocks.any(|other| other != first))
    }

    /// The block at `height` on the adopted target's path, if the owner of
    /// epoch `epoch` works under the adopted authority's epoch.
    fn adopted_block(&self, epoch: Option<u64>, height: u32) -> Option<Option<&str>> {
        let (target, adopted) = self.adopted.as_ref()?;
        (epoch == Some(*adopted)).then(|| self.dag.ancestor(target, height))
    }

    /// Judges a published request.
    fn request(&mut self, owner: &WorkOwner, heights: &[u32]) {
        self.requests += 1;
        let branch = &owner.scope.branch;
        let anchor = self.dag.height(&branch.anchor);
        for &height in heights {
            self.saw_request_on_replaced_height |= self.replaced(height);
            let own = self.dag.ancestor(&branch.target, height);
            let within = own.is_some() && anchor.is_some_and(|anchor| height > anchor);
            let current = self.adopted_block(owner.body_work_epoch, height);
            if !within || current.is_some_and(|current| current != own) {
                self.request_findings.push(Finding {
                    expected: "every requested height lies on its owner's branch above its \
                               anchor, and, under the adopted epoch, names the adopted \
                               target's block"
                        .to_owned(),
                    actual: format!(
                        "request {} on session {} asked for height {height}: its branch \
                         holds {own:?} there, the adopted target {current:?}",
                        owner.request_id, owner.session_id
                    ),
                });
            }
        }
    }

    /// Judges a submission storage took.
    fn submission(&mut self, owner: &WorkOwner, height: u32, block: &str) {
        self.submissions += 1;
        self.saw_submission_on_replaced_height |= self.replaced(height);
        let own = self.dag.ancestor(&owner.scope.branch.target, height);
        let current = self.adopted_block(owner.body_work_epoch, height);
        if own != Some(block) || current.is_some_and(|current| current != Some(block)) {
            self.submit_findings.push(Finding {
                expected: "a submission's body is its owner's branch's block at its height, \
                           and, under the adopted epoch, the adopted target's"
                    .to_owned(),
                actual: format!(
                    "request {} on session {} submitted {block} at {height}: its branch holds \
                     {own:?} there, the adopted target {current:?}",
                    owner.request_id, owner.session_id
                ),
            });
        }
    }
}

/// A view that carries a [`CrossModel`] beside the node's block-sync view.
pub trait CrossView {
    /// The model, shared with every later view.
    fn cross(&self) -> Rc<RefCell<CrossModel>>;

    /// The node's block-sync view.
    fn block(&self) -> Rc<RefCell<BlockSyncView>>;
}

/// The engine's views agree with the fixture DAG: every tip is a block the
/// run made, and a view whose selected or verified path replaced the last
/// one's starts a new body-work epoch.
pub const TIPS_CONSISTENT: &str = "sync/tips-consistent";

/// Every block request the node publishes names heights on its owner's
/// branch, and under the adopted epoch the adopted target's blocks.
pub const BLOCK_REQUESTS_FOLLOW_HEADERS: &str = "sync/block-requests-follow-headers";

/// Every body block sync submits is its owner's branch's block, and under
/// the adopted epoch the adopted target's.
pub const HEADER_REORG_RETIRES_BLOCK_WORK: &str = "sync/header-reorg-retires-block-work";

/// What every cross property reads: storage's and block sync's facts, and
/// the view's model.
const NEEDS: &[Source] = &[Source::Storage, Source::View];

/// The cross properties `sync/storage` registers.
pub fn storage_properties<S>() -> Vec<Property<S>>
where
    S: HasView,
    S::View: CrossView,
{
    vec![
        Property {
            id: PropertyId::new(TIPS_CONSISTENT),
            claim: "every committed view's header tip and verified tip are blocks the run \
                    made, and a view whose selected path or verified path doesn't extend the \
                    last one's, or whose verified tip moved off its selected chain, starts a \
                    new body-work epoch",
            kind: Kind::Safety,
            assumes: &[],
            needs: NEEDS,
            status: Status::Active,
            check: check_tips_consistent::<S>,
            witness: Some(TriggerHistory::Log(epoch_changed)),
        },
        Property {
            id: PropertyId::new(BLOCK_REQUESTS_FOLLOW_HEADERS),
            claim: "every height a published block request names lies above its owner's \
                    anchor on its owner's branch, and a request under the adopted body-work \
                    epoch names the adopted target's block at each height",
            kind: Kind::Safety,
            assumes: &[],
            needs: NEEDS,
            status: Status::Draft,
            check: check_block_requests_follow_headers::<S>,
            witness: Some(TriggerHistory::Log(request_after_epoch_change)),
        },
        Property {
            id: PropertyId::new(HEADER_REORG_RETIRES_BLOCK_WORK),
            claim: "every body block sync submits is its owner's branch's block at its height, \
                    and a submission under the adopted body-work epoch is the adopted \
                    target's block",
            kind: Kind::Safety,
            assumes: &[],
            needs: NEEDS,
            status: Status::Draft,
            check: check_header_reorg_retires_block_work::<S>,
            witness: Some(TriggerHistory::Log(submission_after_epoch_change)),
        },
    ]
}

/// The model, caught up with the log and its views judged.
fn caught_up<S>(cx: &CheckCtx<'_, S>) -> Rc<RefCell<CrossModel>>
where
    S: HasView,
    S::View: CrossView,
{
    let model = cx.view().cross();
    {
        let mut model = model.borrow_mut();
        model.catch_up(cx.log);
        model.judge_views();
    }
    model
}

/// Reports `findings`: the first as the verdict, the rest recorded, or
/// `Holds` once `applicable`.
fn report<S: HasView>(
    cx: &mut CheckCtx<'_, S>,
    findings: Vec<Finding>,
    applicable: bool,
) -> Verdict {
    let mut findings = findings.into_iter();
    let Some(first) = findings.next() else {
        return if applicable {
            Verdict::Holds
        } else {
            Verdict::NotApplicable
        };
    };
    for finding in findings {
        cx.record_violation(finding.expected, finding.actual);
    }
    cx.violated(first.expected, first.actual)
}

/// Checks [`TIPS_CONSISTENT`] on each view read since the last check.
fn check_tips_consistent<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: CrossView,
{
    let model = caught_up(cx);
    let (findings, applicable) = model.borrow_mut().take(Check::Views);
    report(cx, findings, applicable)
}

/// Checks [`BLOCK_REQUESTS_FOLLOW_HEADERS`] on each request published.
fn check_block_requests_follow_headers<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: CrossView,
{
    let model = caught_up(cx);
    let (findings, applicable) = model.borrow_mut().take(Check::Requests);
    report(cx, findings, applicable)
}

/// Checks [`HEADER_REORG_RETIRES_BLOCK_WORK`] on each submission storage
/// took.
fn check_header_reorg_retires_block_work<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView,
    S::View: CrossView,
{
    let model = caught_up(cx);
    let (findings, applicable) = model.borrow_mut().take(Check::Submissions);
    report(cx, findings, applicable)
}

/// What the node did after the reactor adopted a new body-work epoch, as
/// the log alone shows it.
#[derive(Debug, Default)]
pub struct EpochHistory {
    /// The epoch of the reactor's latest `AuthorityChanged` that changed
    /// it.
    epoch: Option<u64>,
    /// Whether the reactor adopted a new body-work epoch: the engine
    /// published a view whose selected or verified path replaced the last
    /// one's.
    pub epoch_changed: bool,
    /// Whether the node published a request under the new epoch.
    pub request_after: bool,
    /// Whether storage took a submission under the new epoch.
    pub submission_after: bool,
}

impl EpochHistory {
    /// Folds every entry of `log`.
    pub fn of(log: &ObservationLog) -> Self {
        let mut history = EpochHistory::default();
        for entry in log.entries() {
            match &entry.obs {
                Observation::BlockSync {
                    event:
                        BlockSyncFact::Reactor {
                            transition:
                                ReactorAction::AuthorityChanged {
                                    authority,
                                    epoch_changed: true,
                                },
                            ..
                        },
                    ..
                } => {
                    history.epoch = Some(authority.body_work_epoch);
                    history.epoch_changed = true;
                }
                Observation::BlockSync {
                    event: BlockSyncFact::Request { owner, step, .. },
                    ..
                } if step == "published" => {
                    history.request_after |= history.current(owner);
                }
                Observation::Storage {
                    service,
                    fact:
                        StorageFact::BlockSubmitted {
                            request: StorageRequest::SubmitBlock { owner, .. },
                            ..
                        },
                    ..
                } if service == BLOCK_SERVICE => {
                    history.submission_after |= history.current(owner);
                }
                _ => {}
            }
        }
        history
    }

    /// Whether `owner` works under the latest new epoch.
    fn current(&self, owner: &WorkOwner) -> bool {
        self.epoch.is_some() && owner.body_work_epoch == self.epoch
    }
}

/// [`TIPS_CONSISTENT`]'s trigger history: the reactor adopted a new
/// body-work epoch, so a view replaced the last one's path.
fn epoch_changed(log: &ObservationLog) -> bool {
    EpochHistory::of(log).epoch_changed
}

/// [`BLOCK_REQUESTS_FOLLOW_HEADERS`]'s trigger history: the node published a
/// request under a new body-work epoch.
fn request_after_epoch_change(log: &ObservationLog) -> bool {
    EpochHistory::of(log).request_after
}

/// [`HEADER_REORG_RETIRES_BLOCK_WORK`]'s trigger history: storage took a
/// submission under a new body-work epoch.
fn submission_after_epoch_change(log: &ObservationLog) -> bool {
    EpochHistory::of(log).submission_after
}
