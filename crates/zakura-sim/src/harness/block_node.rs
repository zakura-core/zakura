//! Block-sync nodes: real block-sync reactors and services, joined by
//! in-memory links, with one node's block storage completed by the run.
//!
//! The node under test answers its action channel and serves its range reads
//! through a [`ControlledBlockService`] behind a [`PolicySource`], so the
//! `Plan` picks when and how each block operation completes. The service runs
//! the real header-chain engine and models only the verifier. Mirror peers
//! run the same block-sync code on a plain [`EnginePort`] whose full state
//! holds the whole fixture chain: [`MirrorStorage`] answers each of their
//! queries at once, and serves each range read when the run picks it, so a
//! [`policy`] hold can delay a mirror's serving too. Each actor's headers,
//! and each mirror's bodies, are committed before `Sim::new` by [`preload`].
//! [`policy`] holds individual operations, and gates [`control`] steps,
//! until chosen events happen. Control steps close and relink links, grow
//! or fork every actor's chain mid-run, and make a [`probe`], a scripted
//! peer, send frames.
//!
//! Mirrors hold every body, so they never ask the node for one. Only the
//! probe exercises the node's serving.
//!
//! A `Control` step can also tear the node down mid-run, or write its block
//! sync's owner state past the guard a property checks, as a bypass witness:
//! work inserted without its block service's answer, or a verdict
//! dispatched without its block service's commit.
//!
//! [`tap::BlockTap`] logs the node's block-sync events beside its storage
//! facts, pairs them, and folds them into a [`view::BlockSyncView`].

pub mod control;
mod identity;
mod pair;
pub mod policy;
pub mod probe;
pub mod relay;
pub mod tap;
pub mod view;

use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use zakura_header_chain::{ErrorCategory, Frontier};
use zakura_network::{
    zakura::{
        block_sync_observe::BlockSyncObserver, framed_channel, spawn_block_sync_reactor,
        BlockRangeRead, BlockRangeReadResult, BlockRangeSource, BlockSyncAction,
        BlockSyncFrontiers, BlockSyncHandle, BlockSyncService, BlockSyncStartup, CloseCauseReader,
        FramedSend, Peer, Service, ServicePeerDirection, StreamReader, ZakuraBlockSyncConfig,
        ZakuraPeerId, ZAKURA_CAP_BLOCK_SYNC_REGULATED, ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
        ZAKURA_STREAM_BLOCK_SYNC,
    },
    BoxError,
};
use zakura_node_services::header_chain::{ApplyHeaderTargetOutcome, Port, PreparedHeaderTarget};
use zakura_quic::NodeSecretKey;
use zakura_rt::sync::{mpsc, CancellationToken};
use zakura_scheduler::{hooks, SimInstant};
use zakura_storage::{
    block::{
        controlled::{BlockReplies, ControlledBlockService, HandleClosed},
        evidence::{OperationStage, RequestIdentity},
        fixture::{BlockFixture, BodySpec, FixtureBlock},
        ApplyLimits, BlockOutcome, HandleEvent, Verdict,
    },
    full_state::query_needed_blocks,
    header::adapter::EnginePort,
};

use self::{
    policy::{BlockKey, Hold, MirrorEvent, Predicates},
    probe::ProbeLog,
    tap::{BlockTap, TapTeardown},
};
use super::header_node::LINK_DEPTH;
use crate::{
    harness::policy::LinkEffect,
    observe::{StorageFact, StorageRequest, StorageResult},
    storage::{decode_block, BlockStorage, BLOCK_SERVICE},
    ActionSource, Boundary, Identity, Observation, ObservationLog, Sim,
};

/// Polls `future` once outside any task: before a run, or in the step loop.
/// Every engine port call finishes on its first poll.
fn ready<F: std::future::Future>(future: F) -> F::Output {
    match std::pin::pin!(future).poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    {
        std::task::Poll::Ready(output) => output,
        std::task::Poll::Pending => {
            panic!("an engine port call finishes on its first poll")
        }
    }
}

/// Commits `chain`'s headers to `engine`, and the first `bodies` of its
/// bodies to full state. Call it before `Sim::new`, as the header side's
/// `preload` is called.
///
/// # Panics
///
/// If the engine refuses the fixture headers or full state a body, which is
/// a fixture bug.
pub fn preload(engine: &EnginePort, fixture: &BlockFixture, chain: &[FixtureBlock], bodies: usize) {
    if !chain.is_empty() {
        let request = fixture.target_request(&engine.snapshot(), fixture.anchor(), chain, 1);
        let prepared =
            ready(engine.prepare_header_target(request)).expect("fixture headers prepare");
        assert_eq!(
            ready(engine.apply_header_target(prepared)).expect("fixture headers apply"),
            ApplyHeaderTargetOutcome::Applied,
            "a fresh engine applies a fixture chain"
        );
    }
    engine
        .preload_blocks(chain[..bodies].iter().map(|block| block.block.clone()))
        .expect("full state commits fixture bodies in order");
}

/// Regtest's last checkpoint: its checkpoint list holds only genesis.
const LAST_CHECKPOINT: zakura_chain::block::Height = zakura_chain::block::Height(0);

/// The apply limits zakurad's driver runs with by default on Regtest: the
/// checkpoint limit clamped to its one legal value, `sync.full_verify_
/// concurrency_limit`'s default 20, `sync.zakura_block_apply_concurrency_
/// limit`'s default 32 (`zakurad/src/components/sync.rs`), and
/// [`LAST_CHECKPOINT`].
pub fn default_limits() -> ApplyLimits {
    ApplyLimits::clamped(usize::MAX, 20, 32, LAST_CHECKPOINT)
}

/// Which outcomes the policy offers for operations no hold picks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Base {
    /// Every outcome the contract allows, for safety sweeps.
    Any,
    /// Each operation's success: an answer, an admission, an applied record,
    /// a served read or a commit, and a duplicate where a commit isn't
    /// legal, so a correct node converges.
    SuccessOnly,
}

impl Base {
    /// Whether the base offers `outcome` for an operation whose legal
    /// outcomes now are `legal`.
    pub fn offers(self, legal: &[BlockOutcome], outcome: BlockOutcome) -> bool {
        let success = |outcome: &BlockOutcome| {
            matches!(
                outcome,
                BlockOutcome::Answer
                    | BlockOutcome::Admit
                    | BlockOutcome::Apply
                    | BlockOutcome::Serve
                    | BlockOutcome::Verdict(Verdict::Committed)
            )
        };
        match self {
            Base::Any => true,
            Base::SuccessOnly => {
                success(&outcome)
                    || (outcome == BlockOutcome::Verdict(Verdict::Duplicate)
                        && !legal.iter().any(success))
            }
        }
    }
}

/// How the run completes the node's block operations and the mirrors' range
/// reads. The scenario records it, so a trace replays it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    /// The outcomes offered for the node's operations.
    pub base: Base,
    /// If set, each of the node's operations must complete within this many
    /// milliseconds of its current stage's start, and time stops there until
    /// it does.
    #[serde(default)]
    pub bound_ms: Option<u64>,
    /// Operations held apart from the base, each stage by the first hold
    /// that picks it. A mirror read no hold picks is served at once.
    #[serde(default)]
    pub holds: Vec<Hold>,
}

impl Policy {
    /// [`Base::SuccessOnly`] with no bound and no holds, the convergence
    /// policy.
    pub fn converging() -> Self {
        Policy {
            base: Base::SuccessOnly,
            bound_ms: None,
            holds: Vec::new(),
        }
    }
}

/// What the run did to a [`Topology`], shared with the [`PolicySource`] that
/// counts it for the link and chain triggers.
#[derive(Clone, Default)]
pub struct Effects {
    /// Every link made, in order.
    pub links: Rc<RefCell<Vec<Link>>>,
    /// Each actor whose chain grew or switched to a fork, once per change,
    /// in order.
    pub chains: Rc<RefCell<Vec<(LinkEffect, String)>>>,
    /// Each actor that prepared a chain change's headers, once per
    /// preparation, in order.
    pub prepared: Rc<RefCell<Vec<String>>>,
    /// Why a `Control` step couldn't take effect, which ends the run as a
    /// harness failure: the scenario asked for something that can't happen.
    pub failure: Rc<RefCell<Option<String>>>,
    /// How many consensus-invalid bodies the node's engine has recorded.
    /// Each one moves its header generation, which makes a preparation made
    /// before it stale.
    pub invalidated: Rc<Cell<u64>>,
}

/// The time since each operation's current stage began.
#[derive(Clone, Debug, Default)]
struct Stages(BTreeMap<BlockKey, SimInstant>);

impl Stages {
    /// Operation `key` starts a stage now.
    fn start(&mut self, key: BlockKey) {
        let now = hooks::now().expect("sources observe only inside a run");
        self.0.insert(key, now);
    }

    /// How long ago `key`'s stage began, at `now`.
    fn since(&self, key: BlockKey, now: SimInstant) -> Duration {
        self.0.get(&key).map_or(Duration::ZERO, |start| {
            now.since_start() - start.since_start()
        })
    }
}

/// A [`BlockStorage`] whose offers follow a [`Policy`]. It changes its
/// [`Predicates`] only in `observe`, so `enabled` and `boundaries` stay
/// pure. With a [`BlockTap`], it logs the node's block-sync events after its
/// storage facts.
pub struct PolicySource {
    storage: BlockStorage,
    tap: Option<BlockTap>,
    service: Arc<ControlledBlockService>,
    base: Base,
    bound: Option<Duration>,
    predicates: Rc<RefCell<Predicates>>,
    stages: Stages,
    effects: Effects,
    /// How many links are recorded as made.
    linked: usize,
    /// The connections recorded as closed.
    closed: BTreeSet<u64>,
    /// How many chain changes are recorded.
    changed: usize,
    /// How many preparations are recorded.
    prepared: usize,
    /// The node's `record_body_invalid` operations not yet completed.
    invalid_records: BTreeSet<u64>,
}

impl PolicySource {
    /// `service`'s storage under `policy`, with its checkpoint registered
    /// with `sim`. Its predicates count `effects` too.
    pub fn new(
        sim: &mut Sim,
        service: Arc<ControlledBlockService>,
        policy: Policy,
        effects: Effects,
    ) -> Self {
        PolicySource {
            storage: BlockStorage::new(sim, service.clone()),
            tap: None,
            service,
            base: policy.base,
            bound: policy.bound_ms.map(Duration::from_millis),
            predicates: Rc::new(RefCell::new(Predicates::new(policy.holds))),
            stages: Stages::default(),
            effects,
            linked: 0,
            closed: BTreeSet::new(),
            changed: 0,
            prepared: 0,
            invalid_records: BTreeSet::new(),
        }
    }

    /// The predicates, for a [`control::Control`] or a [`MirrorReadSource`]
    /// gated by the same counts.
    pub fn predicates(&self) -> Rc<RefCell<Predicates>> {
        self.predicates.clone()
    }

    /// This source, draining `tap` at each observation, after storage, so
    /// each event's storage watermark is already logged and the tap pairs
    /// the node's operations with its events.
    pub fn with_tap(self, tap: BlockTap) -> Self {
        PolicySource {
            tap: Some(tap),
            ..self
        }
    }
}

impl ActionSource for PolicySource {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        let pending = self.service.pending();
        let predicates = self.predicates.borrow();
        let mut offered = Vec::new();
        self.storage.enabled(now, &mut offered);
        out.extend(offered.into_iter().filter(|(_, local)| {
            let (id, outcome) = decode_block(*local);
            pending.iter().find(|op| op.id == id).is_some_and(|op| {
                let since = self.stages.since(BlockKey::Service(id.0), now);
                predicates.offers(self.base, op, since, outcome)
            })
        }));
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        self.storage.boundaries(now, out);
        let predicates = self.predicates.borrow();
        for op in self.service.pending() {
            let key = BlockKey::Service(op.id.0);
            predicates.boundaries(self.bound, key, self.stages.since(key, now), now, out);
        }
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        self.storage.execute(local, log);
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        let before = log.entries().len();
        let stored = self.storage.observe(log);
        // Drained even when storage failed, so the tap's evidence still
        // reaches the log.
        let tapped = match &mut self.tap {
            Some(tap) => {
                // The storage source appends only its own service's facts,
                // but a run may hold other services' storage too.
                tap.storage_facts(log.entries()[before..].iter().filter_map(
                    |entry| match &entry.obs {
                        Observation::Storage {
                            service, seq, fact, ..
                        } if service == BLOCK_SERVICE => Some((*seq, fact)),
                        _ => None,
                    },
                ));
                tap.observe(log)
            }
            None => Ok(()),
        };
        stored?;
        tapped?;
        let mut predicates = self.predicates.borrow_mut();
        // Only this source's storage and tap append here.
        for entry in &log.entries()[before..] {
            if let Observation::Storage { service, fact, .. } = &entry.obs {
                if service != BLOCK_SERVICE {
                    continue;
                }
                match fact {
                    StorageFact::BlockSubmitted { id, request, .. } => {
                        if matches!(
                            request,
                            StorageRequest::RecordBody { record, .. }
                                if record == "record_body_invalid"
                        ) {
                            self.invalid_records.insert(*id);
                        }
                        self.stages.start(BlockKey::Service(*id))
                    }
                    StorageFact::Admitted { id } => self.stages.start(BlockKey::Service(*id)),
                    StorageFact::BlockCompleted { id, result, .. }
                        if self.invalid_records.remove(id) =>
                    {
                        if matches!(
                            result,
                            StorageResult::Recorded { attempts }
                                if attempts.last().is_some_and(|last| last == "committed")
                        ) {
                            let invalidated = &self.effects.invalidated;
                            invalidated.set(invalidated.get() + 1);
                        }
                    }
                    _ => {}
                }
                predicates.record(fact);
            }
        }
        let links = self.effects.links.borrow();
        for link in &links[self.linked..] {
            predicates.record_link(LinkEffect::Linked, &link.peer);
        }
        self.linked = links.len();
        // A connection closes once, whoever cancels it and however often.
        for link in links.iter() {
            if link.is_closed() && self.closed.insert(link.conn_id) {
                predicates.record_link(LinkEffect::Closed, &link.peer);
            }
        }
        let chains = self.effects.chains.borrow();
        for (effect, actor) in &chains[self.changed..] {
            predicates.record_link(*effect, actor);
        }
        self.changed = chains.len();
        let prepared = self.effects.prepared.borrow();
        for actor in &prepared[self.prepared..] {
            predicates.record_link(LinkEffect::Prepared, actor);
        }
        self.prepared = prepared.len();
        match self.effects.failure.borrow().as_ref() {
            Some(failure) => Err(format!("ControlFailed: {failure}")),
            None => Ok(()),
        }
    }
}

/// Block sync's handle, set once its reactor starts and cleared when the
/// node is torn down. The node's service is built first, since its range
/// source goes into the reactor's startup.
#[derive(Debug, Default)]
pub struct LateHandle(Mutex<Option<BlockSyncHandle>>);

impl LateHandle {
    /// The handle's slot. No caller panics while it holds the lock.
    fn slot(&self) -> MutexGuard<'_, Option<BlockSyncHandle>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Sets the handle.
    ///
    /// # Panics
    ///
    /// If it's already set.
    pub fn set(&self, handle: BlockSyncHandle) {
        let mut slot = self.slot();
        assert!(slot.is_none(), "a node's reactor starts once");
        *slot = Some(handle);
    }

    /// Takes the handle out, so every later reply fails as a closed
    /// handle's does. The caller drops it outside the lock.
    fn clear(&self) -> Option<BlockSyncHandle> {
        self.slot().take()
    }
}

impl BlockReplies for LateHandle {
    /// Sends `event` through the handle, or fails once the node is torn
    /// down. Only the step loop completes operations, so no task holds the
    /// lock across a scheduling point.
    fn send(&self, event: HandleEvent) -> Result<(), HandleClosed> {
        match self.slot().as_ref() {
            Some(handle) => BlockReplies::send(handle, event),
            None => Err(HandleClosed),
        }
    }
}

/// One mirror range read the run hasn't let its caller take yet.
struct PendingRead {
    /// The mirror's name.
    mirror: String,
    /// The mirror's storage.
    engine: Arc<EnginePort>,
    /// The read.
    request: BlockRangeRead,
    /// Whether the mirror withholds the read's first height, so the run
    /// never serves it.
    withheld: bool,
    /// Whether the run has served it, which lets its caller read full state.
    served: bool,
    /// The caller's waker.
    waker: Option<Waker>,
}

/// Every mirror's range reads that wait for the run.
#[derive(Default)]
struct ReadQueue {
    /// The last read ID handed out. Reads count up from 1 across mirrors.
    last_id: u64,
    pending: BTreeMap<u64, PendingRead>,
    /// What happened since the source last drained it.
    events: Vec<MirrorEvent>,
    /// Everything that happened, in order.
    history: Vec<MirrorEvent>,
}

impl ReadQueue {
    fn push(&mut self, event: MirrorEvent) {
        self.events.push(event);
        self.history.push(event);
    }
}

/// The mirrors' range reads, shared by their storage and the
/// [`MirrorReadSource`] that serves them.
#[derive(Clone, Default)]
pub struct MirrorReads(Arc<Mutex<ReadQueue>>);

impl std::fmt::Debug for MirrorReads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let queue = self.queue();
        f.debug_struct("MirrorReads")
            .field("last_id", &queue.last_id)
            .field("pending", &queue.pending.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl MirrorReads {
    /// The queue. No caller panics while it holds the lock, and none holds
    /// it across a scheduling point.
    fn queue(&self) -> MutexGuard<'_, ReadQueue> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Everything that happened to the mirrors' reads, in order.
    pub fn history(&self) -> Vec<MirrorEvent> {
        self.queue().history.clone()
    }
}

/// A caller's wait for mirror read `id`.
struct MirrorRead {
    reads: MirrorReads,
    id: u64,
}

impl Future for MirrorRead {
    type Output = Result<BlockRangeReadResult, BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let read = {
            let mut queue = self.reads.queue();
            let read = queue
                .pending
                .get_mut(&self.id)
                .expect("a read stays queued until its caller takes it or drops it");
            if !read.served {
                read.waker = Some(cx.waker().clone());
                return Poll::Pending;
            }
            queue.push(MirrorEvent::Taken(self.id));
            queue
                .pending
                .remove(&self.id)
                .expect("the read was just found served")
        };
        // The caller reads full state itself: the engine's lock may be held
        // across a scheduling point, so only a task can wait for it.
        let request = read.request;
        let blocks = read.engine.read_block_range(
            request.start_height,
            request.count,
            request.max_body_bytes,
            || false,
        );
        Poll::Ready(
            blocks
                .map(|blocks| BlockRangeReadResult {
                    blocks,
                    lease: request.lease,
                })
                .map_err(BoxError::from),
        )
    }
}

impl Drop for MirrorRead {
    fn drop(&mut self) {
        let mut queue = self.reads.queue();
        if queue.pending.remove(&self.id).is_some() {
            queue.push(MirrorEvent::Dropped(self.id));
        }
    }
}

/// A mirror's storage: full state holding the whole fixture chain, which
/// answers each query at once. Its range reads wait for the run.
#[derive(Clone, Debug)]
pub struct MirrorStorage {
    name: String,
    engine: Arc<EnginePort>,
    reads: MirrorReads,
    /// Heights whose range reads the run never serves.
    withheld: Vec<u32>,
}

impl MirrorStorage {
    /// Storage for the mirror named `name` over `engine`, whose range reads
    /// join `reads`.
    pub fn new(name: &str, engine: Arc<EnginePort>, reads: MirrorReads) -> Self {
        MirrorStorage {
            name: name.to_owned(),
            engine,
            reads,
            withheld: Vec::new(),
        }
    }

    /// The same storage, except that the run never serves a range read that
    /// starts at one of `heights`. The read waits until its caller drops it.
    pub fn withholding(mut self, heights: &[u32]) -> Self {
        self.withheld = heights.to_vec();
        self
    }

    /// Spawns a task that answers each of `actions` until the channel
    /// closes, replying through `handle`. A mirror holds every body, so it
    /// needs none: its queries come back empty, and it never submits one.
    /// Records and misbehavior reports change nothing a mirror serves, so
    /// the task drops them.
    fn spawn(
        &self,
        sim: &mut Sim,
        mut actions: mpsc::Receiver<BlockSyncAction>,
        handle: BlockSyncHandle,
    ) {
        let engine = self.engine.clone();
        sim.scheduler().spawn_root("mirror-actions", async move {
            while let Some(action) = actions.recv().await {
                match action {
                    BlockSyncAction::QueryNeededBlocks {
                        query_id,
                        from,
                        limit,
                        scope,
                        ..
                    } => {
                        let event = match query_needed_blocks(from, limit, |from, limit| {
                            engine.missing_block_body_metadata(from, limit)
                        }) {
                            Ok((body_anchor, blocks)) => HandleEvent::NeededBlocks {
                                query_id,
                                scope,
                                body_anchor,
                                blocks,
                            },
                            Err(_) => HandleEvent::NeededBlocksFailed { query_id, scope },
                        };
                        if BlockReplies::send(&handle, event).is_err() {
                            return;
                        }
                    }
                    BlockSyncAction::SubmitBlock { block, .. } => {
                        panic!(
                            "a mirror holds every body, yet it submitted {}",
                            block.hash()
                        )
                    }
                    _ => {}
                }
            }
        });
    }
}

impl BlockRangeSource for MirrorStorage {
    /// Queues the read until the run serves it, then reads full state in
    /// the caller's task, uncancelled, as zakura-state's blocking read does.
    fn read(
        &self,
        request: BlockRangeRead,
    ) -> BoxFuture<'static, Result<BlockRangeReadResult, BoxError>> {
        let mut queue = self.reads.queue();
        queue.last_id += 1;
        let id = queue.last_id;
        queue.pending.insert(
            id,
            PendingRead {
                mirror: self.name.clone(),
                engine: self.engine.clone(),
                withheld: self.withheld.contains(&request.start_height.0),
                request,
                served: false,
                waker: None,
            },
        );
        queue.push(MirrorEvent::Submitted(id));
        Box::pin(MirrorRead {
            reads: self.reads.clone(),
            id,
        })
    }
}

/// Offers each queued mirror read as `storage:<mirror>:<id>:serve`, once the
/// policy's predicates let it be served. It records the reads' events in
/// those predicates.
pub struct MirrorReadSource {
    reads: MirrorReads,
    predicates: Rc<RefCell<Predicates>>,
    stages: Stages,
}

impl MirrorReadSource {
    /// A source for `reads`, gated and counted by `predicates`.
    pub fn new(reads: MirrorReads, predicates: Rc<RefCell<Predicates>>) -> Self {
        MirrorReadSource {
            reads,
            predicates,
            stages: Stages::default(),
        }
    }
}

impl ActionSource for MirrorReadSource {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        let queue = self.reads.queue();
        let predicates = self.predicates.borrow();
        for (id, read) in &queue.pending {
            let since = self.stages.since(BlockKey::Mirror(*id), now);
            if !read.served && !read.withheld && predicates.serves(*id, since) {
                out.push((Identity::storage(&read.mirror, *id, "serve"), *id));
            }
        }
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        let queue = self.reads.queue();
        let predicates = self.predicates.borrow();
        for (id, read) in &queue.pending {
            if !read.served {
                let key = BlockKey::Mirror(*id);
                predicates.boundaries(None, key, self.stages.since(key, now), now, out);
            }
        }
    }

    fn execute(&mut self, id: u64, _: &mut ObservationLog) {
        let mut queue = self.reads.queue();
        let read = queue
            .pending
            .get_mut(&id)
            .expect("`enabled` offered a queued read");
        read.served = true;
        if let Some(waker) = read.waker.take() {
            waker.wake();
        }
        queue.push(MirrorEvent::Served(id));
    }

    fn observe(&mut self, _: &mut ObservationLog) -> Result<(), String> {
        let events = std::mem::take(&mut self.reads.queue().events);
        let mut predicates = self.predicates.borrow_mut();
        for event in events {
            if let MirrorEvent::Submitted(id) = event {
                self.stages.start(BlockKey::Mirror(id));
            }
            predicates.record_mirror(event);
        }
        Ok(())
    }
}

/// Where a block node's storage comes from.
pub enum NodeStorage {
    /// The run completes its operations.
    Controlled {
        /// The service answering its actions and reads.
        service: Arc<ControlledBlockService>,
        /// The service's replies, set to the node's handle once it starts.
        replies: Arc<LateHandle>,
    },
    /// A mirror's storage.
    Mirror(MirrorStorage),
}

/// One block-sync actor: a real reactor and service on its storage.
pub struct BlockNode {
    /// The actor's role name, `node` or `mirror-<i>`, which `Control`
    /// actions name.
    pub name: String,
    /// Its peer identity, which its links present.
    pub id: ZakuraPeerId,
    /// Its engine, whose committed views its reactor follows.
    pub engine: Arc<EnginePort>,
    /// Cancels its reactor.
    shutdown: CancellationToken,
    /// Its handle and service, until a teardown drops them.
    live: RefCell<Option<Live>>,
    /// Its block service and the service's replies, if the run completes
    /// its operations.
    controlled: Option<(Arc<ControlledBlockService>, Arc<LateHandle>)>,
}

/// The handles to a node's block sync that the harness holds outside its
/// tasks.
struct Live {
    /// The reactor's handle.
    handle: BlockSyncHandle,
    /// The service its links join.
    service: BlockSyncService,
}

impl BlockNode {
    /// Starts a reactor and its service on `engine`'s committed views, as
    /// zakurad wires them: `config`, `engine`'s view watch as the
    /// committed views, the decoded-window cap above [`LAST_CHECKPOINT`],
    /// and `storage` answering its actions and range reads. Peer IDs come
    /// from `seed`.
    ///
    /// Unlike zakurad, it supplies no retention, so every actor serves as
    /// archive storage: no fixture chain is long enough to prune. An
    /// `observer`, such as a [`BlockTap`]'s, receives its block-sync events.
    pub fn spawn(
        sim: &mut Sim,
        name: &str,
        seed: u8,
        engine: &Arc<EnginePort>,
        storage: NodeStorage,
        config: ZakuraBlockSyncConfig,
        observer: Option<Arc<dyn BlockSyncObserver>>,
    ) -> Self {
        let view = engine.view();
        let frontiers = &view.frontiers;
        let mut startup = BlockSyncStartup::new_with_committed_views(
            BlockSyncFrontiers {
                finalized_height: frontiers.finalized.height,
                verified_block_tip: frontiers.verified_best.height,
                verified_block_hash: frontiers.verified_best.hash,
            },
            (frontiers.header_best.height, frontiers.header_best.hash),
            engine.subscribe_views(),
            config,
        );
        startup.max_checkpoint_height = Some(LAST_CHECKPOINT);
        startup.observer.0 = observer;
        let shutdown = startup.shutdown.clone();
        let source: Arc<dyn BlockRangeSource> = match &storage {
            NodeStorage::Controlled { service, .. } => Arc::new(service.range_source()),
            NodeStorage::Mirror(mirror) => Arc::new(mirror.clone()),
        };
        let (handle, actions, reactor) =
            spawn_block_sync_reactor(startup.with_range_source(source))
                .expect("a fixture startup is valid");
        sim.scheduler().spawn_root("block-node", async move {
            let _ = reactor.await;
        });
        let controlled = match storage {
            NodeStorage::Controlled { service, replies } => {
                replies.set(handle.clone());
                let actions_service = service.clone();
                sim.scheduler().spawn_root("block-actions", async move {
                    let _ = actions_service.spawn(actions).await;
                });
                Some((service, replies))
            }
            NodeStorage::Mirror(mirror) => {
                mirror.spawn(sim, actions, handle.clone());
                None
            }
        };
        BlockNode {
            name: name.to_owned(),
            id: peer_id(seed),
            engine: engine.clone(),
            shutdown,
            live: RefCell::new(Some(Live {
                service: BlockSyncService::new_with_handle(handle.clone()),
                handle,
            })),
            controlled,
        }
    }

    /// Whether a teardown dropped the node's handles.
    fn torn_down(&self) -> bool {
        self.live.borrow().is_none()
    }
}

/// The peer identity seeded by `seed`.
fn peer_id(seed: u8) -> ZakuraPeerId {
    let node = NodeSecretKey::from_bytes(&[seed; 32]).public();
    ZakuraPeerId::new(node.as_bytes().to_vec()).expect("a node id is a valid peer id")
}

/// One connection to the node under test, by its peer's index in a
/// [`Topology`], or the probe's.
#[derive(Clone, Debug)]
pub struct Link {
    /// The name of the node under test's peer on it.
    pub peer: String,
    /// The node that dialed, or `None` for the probe, which always dials.
    pub dialer: Option<usize>,
    /// The node that was dialed.
    pub listener: usize,
    /// The connection's ID, the same on both sides.
    pub conn_id: u64,
    /// Why the node under test's side closed, once it has.
    pub cause: CloseCauseReader,
    /// Each side's connection token, dialer first.
    tokens: [CancellationToken; 2],
}

impl Link {
    /// Closes the connection on both sides, as a transport does when either
    /// end goes away.
    pub fn close(&self) {
        for token in &self.tokens {
            token.cancel();
        }
    }

    /// Whether either side's connection token is cancelled: by
    /// [`Link::close`], or by a service that tears the connection down, as
    /// a routine that rejects a frame does.
    pub fn is_closed(&self) -> bool {
        self.tokens.iter().any(CancellationToken::is_cancelled)
    }
}

/// The fixture chain every actor follows, which `Control` steps grow and
/// fork. Cloning shares it, so a test can read it after a run.
#[derive(Clone)]
pub struct LiveChain {
    /// The fixture every block comes from.
    pub fixture: BlockFixture,
    /// Every new block's body.
    pub spec: BodySpec,
    blocks: Rc<RefCell<Vec<FixtureBlock>>>,
}

impl LiveChain {
    /// `blocks` above the fixture's genesis, each new block like `spec`.
    pub fn new(fixture: BlockFixture, blocks: Vec<FixtureBlock>, spec: BodySpec) -> Self {
        LiveChain {
            fixture,
            spec,
            blocks: Rc::new(RefCell::new(blocks)),
        }
    }

    /// The chain's blocks above genesis now.
    pub fn blocks(&self) -> Vec<FixtureBlock> {
        self.blocks.borrow().clone()
    }

    /// The chain's tip now.
    pub fn tip(&self) -> Frontier {
        self.blocks
            .borrow()
            .last()
            .map_or(self.fixture.anchor(), FixtureBlock::frontier)
    }

    /// The chain's frontier at `height`, which must be on it.
    fn at(&self, height: u32) -> Option<Frontier> {
        if height == 0 {
            return Some(self.fixture.anchor());
        }
        let index = usize::try_from(height - 1).ok()?;
        self.blocks.borrow().get(index).map(FixtureBlock::frontier)
    }
}

/// The probe's end of its link: it sends what `Control` steps script, and
/// its reader logs what the node sends.
struct ProbeEnd {
    /// The probe's sends to the node.
    send: FramedSend,
}

/// Every node in a run and the links between them.
pub struct Topology {
    /// The nodes, the node under test first.
    pub nodes: Vec<BlockNode>,
    effects: Effects,
    chain: LiveChain,
    /// The probe's end of its link, once linked.
    probe: RefCell<Option<ProbeEnd>>,
    /// The last connection and session ID handed out.
    last_id: Cell<u64>,
    /// Labels every frame on the node's links, if set.
    wire: Option<relay::Wire>,
    /// The node under test's tap, told of its teardown.
    tap: Option<TapTeardown>,
}

impl Topology {
    /// `nodes`, following `chain`, with no links yet. Each link it makes,
    /// and each chain change, goes in `effects`.
    pub fn new(nodes: Vec<BlockNode>, effects: Effects, chain: LiveChain) -> Self {
        Topology {
            nodes,
            effects,
            chain,
            probe: RefCell::new(None),
            last_id: Cell::new(0),
            wire: None,
            tap: None,
        }
    }

    /// This topology, telling `tap`, the node under test's, of its
    /// teardown.
    pub fn with_tap(self, tap: TapTeardown) -> Self {
        Topology {
            tap: Some(tap),
            ..self
        }
    }

    /// Puts a relay that labels every frame with `wire`'s role models on
    /// each link it makes from now on.
    pub fn with_wire(self, wire: relay::Wire) -> Self {
        Topology {
            wire: Some(wire),
            ..self
        }
    }

    /// A channel for frames from `sender` to `receiver` on connection `id`,
    /// through a relay if the topology labels its links: the sender's end,
    /// the receiver's, and the reader of a relay into the node.
    fn pipe(
        &self,
        id: u64,
        sender: usize,
        receiver: usize,
    ) -> (
        FramedSend,
        zakura_network::zakura::FramedRecv,
        Option<StreamReader>,
    ) {
        let Some(wire) = &self.wire else {
            let (send, recv) = framed_channel(LINK_DEPTH);
            return (send, recv, None);
        };
        let way = if receiver == 0 {
            relay::Way::ToNode
        } else {
            debug_assert_eq!(sender, 0, "one side of every link is the node under test");
            relay::Way::FromNode
        };
        wire.pipe(id, way, LINK_DEPTH)
    }

    /// The next connection and session ID.
    fn next_id(&self) -> u64 {
        let id = self.last_id.get() + 1;
        self.last_id.set(id);
        id
    }

    /// Admits `local`'s side of connection `id` to its service, from
    /// `remote`, with `streams`, whose receiving end `reader` reads if it
    /// has one, and returns why it closes.
    #[allow(clippy::too_many_arguments)]
    fn admit(
        &self,
        local: usize,
        remote: ZakuraPeerId,
        direction: ServicePeerDirection,
        id: u64,
        streams: (zakura_network::zakura::FramedRecv, FramedSend),
        reader: Option<StreamReader>,
        token: &CancellationToken,
    ) -> CloseCauseReader {
        let peer = Peer::new_versioned(
            id,
            remote,
            ZAKURA_CAP_BLOCK_SYNC_REGULATED,
            direction,
            ZAKURA_STREAM_BLOCK_SYNC,
            ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
            id,
            streams,
            token.clone(),
        );
        let cause = peer.close_cause_reader();
        if let Some(reader) = &reader {
            peer.read_through(reader);
        }
        self.nodes[local]
            .live
            .borrow()
            .as_ref()
            .expect("only setup and a relink add links, and a relink refuses a torn-down node")
            .service
            .add_peer(peer);
        cause
    }

    /// Connects `dialer` to `listener` with a new connection and session,
    /// each side's stream 6 at the regulated version over a pair of
    /// in-memory channels. One side must be the node under test.
    pub fn link(&self, dialer: usize, listener: usize) -> Link {
        let id = self.next_id();
        let (to_listener, from_dialer, listener_reader) = self.pipe(id, dialer, listener);
        let (to_dialer, from_listener, dialer_reader) = self.pipe(id, listener, dialer);
        let tokens = [CancellationToken::new(), CancellationToken::new()];
        let dialed = self.admit(
            dialer,
            self.nodes[listener].id.clone(),
            ServicePeerDirection::Outbound,
            id,
            (from_listener, to_listener),
            dialer_reader,
            &tokens[0],
        );
        let listened = self.admit(
            listener,
            self.nodes[dialer].id.clone(),
            ServicePeerDirection::Inbound,
            id,
            (from_dialer, to_dialer),
            listener_reader,
            &tokens[1],
        );
        let (peer, cause) = if dialer == 0 {
            (listener, dialed)
        } else {
            (dialer, listened)
        };
        if let Some(wire) = &self.wire {
            wire.watch(id, cause.clone());
        }
        let link = Link {
            peer: self.nodes[peer].name.clone(),
            dialer: Some(dialer),
            listener,
            conn_id: id,
            cause,
            tokens,
        };
        self.effects.links.borrow_mut().push(link.clone());
        link
    }

    /// Connects the probe, named `probe`, to the node under test, as the
    /// dialer, with its reader spawned on `sim` logging to `log`.
    pub fn link_probe(&self, sim: &mut Sim, log: ProbeLog) -> Link {
        let id = self.next_id();
        // The probe is never node 0, so `usize::MAX` stands for it.
        let (to_node, from_probe, reader) = self.pipe(id, usize::MAX, 0);
        let (to_probe, from_node, _) = self.pipe(id, 0, usize::MAX);
        let tokens = [CancellationToken::new(), CancellationToken::new()];
        let cause = self.admit(
            0,
            peer_id(PROBE_SEED),
            ServicePeerDirection::Inbound,
            id,
            (from_probe, to_probe),
            reader,
            &tokens[1],
        );
        if let Some(wire) = &self.wire {
            wire.watch(id, cause.clone());
        }
        log.spawn_reader(sim, from_node);
        *self.probe.borrow_mut() = Some(ProbeEnd { send: to_node });
        let link = Link {
            peer: PROBE.to_owned(),
            dialer: None,
            listener: 0,
            conn_id: id,
            cause,
            tokens,
        };
        self.effects.links.borrow_mut().push(link.clone());
        link
    }

    /// The newest link between the node under test and the peer named
    /// `peer`.
    pub fn link_to(&self, peer: &str) -> Option<Link> {
        self.effects
            .links
            .borrow()
            .iter()
            .rev()
            .find(|link| link.peer == peer)
            .cloned()
    }

    /// Links the mirror named `peer` again, in its newest link's
    /// direction. The probe links only once, and a torn-down node takes no
    /// links, so either relink is a failure.
    fn relink(&self, peer: &str) {
        let link = self
            .link_to(peer)
            .unwrap_or_else(|| panic!("the scenario links `{peer}`"));
        if self.nodes[0].torn_down() {
            self.fail("a torn-down node takes no links");
            return;
        }
        match link.dialer {
            Some(dialer) => {
                self.link(dialer, link.listener);
            }
            None => self.fail("the probe links only once"),
        }
    }

    /// Records why a `Control` step couldn't take effect.
    fn fail(&self, why: &str) {
        *self.effects.failure.borrow_mut() = Some(why.to_owned());
    }

    /// Tears the node under test down, as zakurad's shutdown ends block
    /// sync: tells its tap, closes every link, cancels its reactor, and
    /// drops every handle the harness holds outside its tasks. Its routines
    /// and reactor end, so the sequencer's inputs close and it releases what
    /// it holds. A second teardown is a failure.
    pub(crate) fn teardown(&self) {
        let node = &self.nodes[0];
        let Some(live) = node.live.borrow_mut().take() else {
            self.fail("the node is already torn down");
            return;
        };
        if let Some(tap) = &self.tap {
            tap.begin();
        }
        // Cancelling can be a scheduling point, where a relink would add a
        // link, so the links are copied first.
        let links = self.effects.links.borrow().clone();
        for link in &links {
            link.close();
        }
        node.shutdown.cancel();
        let replies = node
            .controlled
            .as_ref()
            .and_then(|(_, replies)| replies.clear());
        drop((live, replies));
    }

    /// The node under test's handle and its block service's owner state,
    /// for a bypass. Records a failure, and returns `None`, once it's torn
    /// down.
    fn bypass_parts(&self) -> Option<(BlockSyncHandle, Vec<RequestIdentity>)> {
        let node = &self.nodes[0];
        let Some(handle) = node.live.borrow().as_ref().map(|live| live.handle.clone()) else {
            self.fail("a torn-down node takes no bypass");
            return None;
        };
        let pending = self
            .node_service()
            .owner_state()
            .operations
            .into_iter()
            .filter(|op| !matches!(op.stage, OperationStage::Replied))
            .map(|op| op.request)
            .collect();
        Some((handle, pending))
    }

    /// The node under test's block service.
    fn node_service(&self) -> &ControlledBlockService {
        let (service, _) = self.nodes[0]
            .controlled
            .as_ref()
            .expect("the run completes the node under test's operations");
        service
    }

    /// The bypass witness for direct work insertion: answers the node's
    /// oldest pending needed-blocks query from its engine, as its block
    /// service would, but straight through its handle. Block sync's work
    /// queue takes work that its block service never answered, and the
    /// service's query stays pending. Records a failure if no query is
    /// pending. Only the step loop reads the node's engine, so only it may
    /// call this.
    pub(crate) fn insert_work(&self) {
        let Some((handle, pending)) = self.bypass_parts() else {
            return;
        };
        let Some((query_id, scope, from, limit)) =
            pending.into_iter().find_map(|request| match request {
                RequestIdentity::Query {
                    query_id,
                    scope,
                    from,
                    limit,
                    ..
                } => Some((query_id, scope, from, limit)),
                _ => None,
            })
        else {
            self.fail("no needed-blocks query is pending to answer");
            return;
        };
        let engine = &self.nodes[0].engine;
        let event = match query_needed_blocks(from, limit, |from, limit| {
            engine.missing_block_body_metadata(from, limit)
        }) {
            Ok((body_anchor, blocks)) => HandleEvent::NeededBlocks {
                query_id,
                scope,
                body_anchor,
                blocks,
            },
            Err(error) => {
                self.fail(&format!("the node's engine refused the query: {error:?}"));
                return;
            }
        };
        if BlockReplies::send(&handle, event).is_err() {
            self.fail("the node's reactor is gone");
        }
    }

    /// The bypass witness for an alternate storage dispatch: sends block
    /// sync a committed verdict for the node's oldest pending submission,
    /// straight through its handle, without its block service. The
    /// sequencer finishes a submission that its block service never
    /// committed, and the service still holds it. Records a failure if no
    /// submission is pending.
    pub(crate) fn dispatch_verdict(&self) {
        let Some((handle, pending)) = self.bypass_parts() else {
            return;
        };
        let Some(event) = pending.into_iter().find_map(|request| match request {
            RequestIdentity::Submit {
                owner,
                source,
                token,
                height: Some(height),
                hash,
                class,
                ..
            } => Some(HandleEvent::ApplyFinished {
                owner,
                source,
                token,
                height,
                hash,
                outcome: Verdict::Committed.outcome(owner, source, hash, class),
            }),
            _ => None,
        }) else {
            self.fail("no submission is pending to finish");
            return;
        };
        if BlockReplies::send(&handle, event).is_err() {
            self.fail("the node's reactor is gone");
        }
    }

    /// Sends `frames` from the probe, in order, stopping at the first the
    /// node's side no longer reads.
    async fn probe_send(&self, frames: &[probe::ProbeFrame]) {
        let genesis = self.chain.fixture.anchor().hash;
        let blocks = self.chain.blocks();
        let send = match self.probe.borrow().as_ref() {
            Some(probe) => probe.send.clone(),
            None => {
                *self.effects.failure.borrow_mut() =
                    Some("a probe step needs a linked probe".to_owned());
                return;
            }
        };
        for frame in frames {
            let frame = frame.frame(genesis, &blocks);
            if send.send(frame).await.is_err() {
                return;
            }
        }
    }

    /// Plans the chain change `action` names, a `Grow` or a `Reset`, and
    /// makes it the live chain. Records a failure, and returns `None`, if
    /// the change can't happen.
    pub(crate) fn plan_change(&self, action: &control::ControlAction) -> Option<ChainChange> {
        let tip = self.chain.tip();
        let change = match *action {
            control::ControlAction::Grow { len, .. } => ChainChange {
                parent: tip,
                blocks: self.chain.fixture.uniform(tip, len, self.chain.spec, 0),
                effect: LinkEffect::Grown,
            },
            control::ControlAction::Reset { fork, .. } => {
                // A fork from the tip itself is a growth, and resets nothing.
                if fork.at >= tip.height.0 {
                    *self.effects.failure.borrow_mut() = Some(format!(
                        "a fork at {} doesn't branch below the chain's tip {tip:?}",
                        fork.at
                    ));
                    return None;
                }
                // The chain holds every height below its tip.
                let parent = self.chain.at(fork.at)?;
                if fork.at.saturating_add(fork.len) <= tip.height.0 {
                    *self.effects.failure.borrow_mut() = Some(format!(
                        "a fork of {} blocks at {} doesn't outgrow the chain's tip {tip:?}",
                        fork.len, fork.at
                    ));
                    return None;
                }
                ChainChange {
                    parent,
                    blocks: self.chain.fixture.uniform(
                        parent,
                        fork.len,
                        self.chain.spec,
                        fork.branch,
                    ),
                    effect: LinkEffect::Reset,
                }
            }
            _ => unreachable!("only `Grow` and `Reset` change the chain"),
        };
        let mut chain = self.chain.blocks.borrow_mut();
        chain.truncate(usize::try_from(change.parent.height.0).expect("fixture heights fit usize"));
        chain.extend(change.blocks.iter().cloned());
        Some(change)
    }

    /// Makes `change` on each mirror: its headers on the mirror's engine,
    /// and its bodies in the mirror's full state. Mirror tasks hold their
    /// engine's lock across scheduling points, so only a task may call this.
    pub(crate) async fn change_mirrors(&self, change: &ChainChange) {
        for node in &self.nodes[1..] {
            let request = self.chain.fixture.target_request(
                &node.engine.snapshot(),
                change.parent,
                &change.blocks,
                1,
            );
            let applied = match node.engine.prepare_header_target(request).await {
                Ok(prepared) => node.engine.apply_header_target(prepared).await,
                Err(error) => Err(error),
            };
            let committed = node
                .engine
                .preload_blocks(change.blocks.iter().map(|block| block.block.clone()))
                .map_err(|error| format!("{error:?}"));
            let tip = node.engine.view().frontiers.verified_best;
            self.settle(node, change, applied, committed, tip);
        }
    }

    /// Prepares `change`'s headers on the node under test's engine, for
    /// [`apply_node`](Self::apply_node), and records the preparation for
    /// [`Trigger::Prepared`](crate::harness::policy::Trigger::Prepared). Records a failure,
    /// and returns `None`, if they don't prepare. Only the step loop reads
    /// the node's engine, so only it may call this.
    pub(crate) fn prepare_node(&self, change: &ChainChange) -> Option<NodeTarget> {
        let node = &self.nodes[0];
        let request = self.chain.fixture.target_request(
            &node.engine.snapshot(),
            change.parent,
            &change.blocks,
            1,
        );
        match ready(node.engine.prepare_header_target(request)) {
            Ok(prepared) => {
                self.effects.prepared.borrow_mut().push(node.name.clone());
                Some(NodeTarget {
                    prepared,
                    invalidated: self.effects.invalidated.get(),
                })
            }
            Err(error) => {
                let tip = node.engine.view().frontiers.header_best;
                self.settle(node, change, Err(error), Ok(()), tip);
                None
            }
        }
    }

    /// Applies `change`'s `target` headers on the node under test's
    /// engine, which reorganizes its selected chain through the real engine.
    /// The node must download the bodies. Returns `false` if a
    /// consensus-invalid body recorded since the preparation moved the
    /// header generation and made it stale, so the caller prepares it again,
    /// as header sync would. Records a failure if they don't apply for any
    /// other reason, any other stale preparation included: a body commit
    /// leaves the preparation valid, and nothing else changes the node's
    /// headers. Only the step loop reads the node's engine, so only it may
    /// call this.
    pub(crate) fn apply_node(&self, change: &ChainChange, target: NodeTarget) -> bool {
        let node = &self.nodes[0];
        let applied = ready(node.engine.apply_header_target(target.prepared));
        if target.invalidated != self.effects.invalidated.get()
            && applied
                .as_ref()
                .is_err_and(|error| error.category == ErrorCategory::StaleTargetOrGeneration)
        {
            return false;
        }
        let tip = node.engine.view().frontiers.header_best;
        self.settle(node, change, applied, Ok(()), tip);
        true
    }

    /// Records `change`'s effect on `node` if it applied and moved the
    /// actor's `tip` to the change's tip, and a failure if not.
    fn settle<E: std::fmt::Debug>(
        &self,
        node: &BlockNode,
        change: &ChainChange,
        applied: Result<ApplyHeaderTargetOutcome, E>,
        committed: Result<(), String>,
        tip: Frontier,
    ) {
        let target = change
            .blocks
            .last()
            .expect("a chain change has blocks")
            .frontier();
        match (applied, committed) {
            (Ok(ApplyHeaderTargetOutcome::Applied), Ok(())) if tip == target => {
                self.effects
                    .chains
                    .borrow_mut()
                    .push((change.effect, node.name.clone()));
            }
            (applied, committed) => {
                *self.effects.failure.borrow_mut() = Some(format!(
                    "moving {} to {target:?} gave {applied:?} and {committed:?}, with its tip at \
                     {tip:?}",
                    node.name
                ));
            }
        }
    }
}

/// A change to every actor's chain: `blocks`, extending `parent`. A
/// control task makes it on the mirrors, and the step loop on the node, in
/// the order its [`control::ControlAction`] names.
#[derive(Clone, Debug)]
pub(crate) struct ChainChange {
    parent: Frontier,
    blocks: Vec<FixtureBlock>,
    /// What the change records for each actor.
    pub(crate) effect: LinkEffect,
}

/// A chain change's headers, prepared on the node under test, for
/// [`Topology::apply_node`].
pub(crate) struct NodeTarget {
    prepared: PreparedHeaderTarget,
    /// How many consensus-invalid bodies the node's engine had recorded at
    /// the preparation.
    invalidated: u64,
}

/// The probe's name, which `Control` steps and link triggers use.
pub const PROBE: &str = "probe";

/// The probe's identity seed, apart from every node's.
const PROBE_SEED: u8 = 0xEE;

/// The verified tip `engine` publishes.
pub fn verified_tip(engine: &EnginePort) -> Frontier {
    engine.view().frontiers.verified_best
}
