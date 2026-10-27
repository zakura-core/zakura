//! Block-sync nodes: real block-sync reactors and services, joined by
//! in-memory links, with one node's block storage completed by the run.
//!
//! The node under test answers its action channel and serves its range reads
//! through a [`ControlledBlockService`] behind a [`PolicySource`], so the
//! `Plan` picks when and how each block operation completes. The service runs
//! the real header-chain engine and models only the verifier. Mirror peers
//! run the same block-sync code on a plain [`EnginePort`] whose full state
//! holds the whole fixture chain: [`MirrorStorage`] answers each of their
//! queries and reads on first poll. Each actor's headers, and each mirror's
//! bodies, are committed before `Sim::new` by [`preload`].
//!
//! Mirrors hold every body, so they never ask the node for one. The node's
//! serving is wired, but only a peer that lacks bodies would exercise it.

use std::{
    cell::Cell,
    future::ready as ready_future,
    sync::{Arc, OnceLock},
};

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use zakura_header_chain::Frontier;
use zakura_network::{
    zakura::{
        framed_channel, spawn_block_sync_reactor, BlockRangeRead, BlockRangeReadResult,
        BlockRangeSource, BlockSyncAction, BlockSyncFrontiers, BlockSyncHandle, BlockSyncService,
        BlockSyncStartup, Peer, Service, ServicePeerDirection, ZakuraBlockSyncConfig, ZakuraPeerId,
        ZAKURA_CAP_BLOCK_SYNC_REGULATED, ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
        ZAKURA_STREAM_BLOCK_SYNC,
    },
    BoxError,
};
use zakura_node_services::header_chain::{ApplyHeaderTargetOutcome, Port};
use zakura_quic::NodeSecretKey;
use zakura_rt::sync::{mpsc, CancellationToken};
use zakura_scheduler::SimInstant;
use zakura_storage::{
    block::{
        controlled::{BlockReplies, ControlledBlockService, HandleClosed},
        fixture::{BlockFixture, FixtureBlock},
        ApplyLimits, BlockOutcome, HandleEvent, Verdict,
    },
    full_state::query_needed_blocks,
    header::adapter::EnginePort,
};

use super::header_node::LINK_DEPTH;
use crate::{
    storage::{decode_block, BlockStorage},
    ActionSource, Boundary, Identity, ObservationLog, Sim,
};

/// Polls `future` once outside a run, where every engine port call finishes
/// on its first poll.
fn ready<F: std::future::Future>(future: F) -> F::Output {
    match std::pin::pin!(future).poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    {
        std::task::Poll::Ready(output) => output,
        std::task::Poll::Pending => {
            panic!("an engine port call outside a run finishes on its first poll")
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

/// Which outcomes the policy offers.
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

/// How the run completes the node's block operations. The scenario records
/// it, so a trace replays it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    /// The outcomes offered.
    pub base: Base,
}

impl Policy {
    /// [`Base::SuccessOnly`], the convergence policy.
    pub fn converging() -> Self {
        Policy {
            base: Base::SuccessOnly,
        }
    }
}

/// A [`BlockStorage`] whose offers follow a [`Policy`].
pub struct PolicySource {
    storage: BlockStorage,
    service: Arc<ControlledBlockService>,
    policy: Policy,
}

impl PolicySource {
    /// `service`'s storage under `policy`, with its checkpoint registered
    /// with `sim`.
    pub fn new(sim: &mut Sim, service: Arc<ControlledBlockService>, policy: Policy) -> Self {
        PolicySource {
            storage: BlockStorage::new(sim, service.clone()),
            service,
            policy,
        }
    }
}

impl ActionSource for PolicySource {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        let pending = self.service.pending();
        let mut offered = Vec::new();
        self.storage.enabled(now, &mut offered);
        out.extend(offered.into_iter().filter(|(_, local)| {
            let (id, outcome) = decode_block(*local);
            pending
                .iter()
                .find(|op| op.id == id)
                .is_some_and(|op| self.policy.base.offers(&op.enabled, outcome))
        }));
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        self.storage.boundaries(now, out);
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        self.storage.execute(local, log);
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        self.storage.observe(log)
    }
}

/// Block sync's handle, set once its reactor starts. The node's service is
/// built first, since its range source goes into the reactor's startup.
#[derive(Debug, Default)]
pub struct LateHandle(OnceLock<BlockSyncHandle>);

impl LateHandle {
    /// Sets the handle.
    ///
    /// # Panics
    ///
    /// If it's already set.
    pub fn set(&self, handle: BlockSyncHandle) {
        assert!(self.0.set(handle).is_ok(), "a node's reactor starts once");
    }
}

impl BlockReplies for LateHandle {
    fn send(&self, event: HandleEvent) -> Result<(), HandleClosed> {
        let handle = self
            .0
            .get()
            .expect("only block sync's actions draw replies, so its reactor started");
        BlockReplies::send(handle, event)
    }
}

/// A mirror's storage: full state holding the whole fixture chain, which
/// answers each query and read on first poll.
#[derive(Clone, Debug)]
pub struct MirrorStorage {
    engine: Arc<EnginePort>,
}

impl MirrorStorage {
    /// Storage over `engine`.
    pub fn new(engine: Arc<EnginePort>) -> Self {
        MirrorStorage { engine }
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
    /// Reads the range from full state, as zakura-state's blocking read
    /// does, uncancelled.
    fn read(
        &self,
        request: BlockRangeRead,
    ) -> BoxFuture<'static, Result<BlockRangeReadResult, BoxError>> {
        let read = self.engine.read_block_range(
            request.start_height,
            request.count,
            request.max_body_bytes,
            || false,
        );
        let lease = request.lease;
        Box::pin(ready_future(
            read.map(|blocks| BlockRangeReadResult { blocks, lease })
                .map_err(BoxError::from),
        ))
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
    /// The actor's role name, `node` or `mirror-<i>`.
    pub name: String,
    /// Its peer identity, which its links present.
    pub id: ZakuraPeerId,
    /// The reactor's handle.
    pub handle: BlockSyncHandle,
    service: BlockSyncService,
}

impl BlockNode {
    /// Starts a reactor and its service on `engine`'s committed views, as
    /// zakurad wires them: production config, `engine`'s view watch as the
    /// committed views, the decoded-window cap above [`LAST_CHECKPOINT`],
    /// and `storage` answering its actions and range reads. Peer IDs come
    /// from `seed`.
    ///
    /// Unlike zakurad, it supplies no retention, so every actor serves as
    /// archive storage: no fixture chain is long enough to prune.
    pub fn spawn(
        sim: &mut Sim,
        name: &str,
        seed: u8,
        engine: &EnginePort,
        storage: NodeStorage,
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
            ZakuraBlockSyncConfig::default(),
        );
        startup.max_checkpoint_height = Some(LAST_CHECKPOINT);
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
        match storage {
            NodeStorage::Controlled { service, replies } => {
                replies.set(handle.clone());
                sim.scheduler().spawn_root("block-actions", async move {
                    let _ = service.spawn(actions).await;
                });
            }
            NodeStorage::Mirror(mirror) => mirror.spawn(sim, actions, handle.clone()),
        }
        let node = NodeSecretKey::from_bytes(&[seed; 32]).public();
        BlockNode {
            name: name.to_owned(),
            id: ZakuraPeerId::new(node.as_bytes().to_vec()).expect("a node id is a valid peer id"),
            service: BlockSyncService::new_with_handle(handle.clone()),
            handle,
        }
    }
}

/// One connection between two nodes, by their index in a [`Topology`].
#[derive(Clone, Debug)]
pub struct Link {
    /// The name of the node under test's peer on it.
    pub peer: String,
    /// The node that dialed.
    pub dialer: usize,
    /// The node that was dialed.
    pub listener: usize,
    /// The connection's ID, the same on both sides.
    pub conn_id: u64,
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

    /// Whether either side's connection token is cancelled.
    pub fn is_closed(&self) -> bool {
        self.tokens.iter().any(CancellationToken::is_cancelled)
    }
}

/// Every node in a run and the links between them.
pub struct Topology {
    /// The nodes, the node under test first.
    pub nodes: Vec<BlockNode>,
    /// Every link made, in order.
    links: std::cell::RefCell<Vec<Link>>,
    /// The last connection and session ID handed out.
    last_id: Cell<u64>,
}

impl Topology {
    /// `nodes`, with no links yet.
    pub fn new(nodes: Vec<BlockNode>) -> Self {
        Topology {
            nodes,
            links: std::cell::RefCell::default(),
            last_id: Cell::new(0),
        }
    }

    /// Connects `dialer` to `listener` with a new connection and session,
    /// each side's stream 6 at the regulated version over a pair of
    /// in-memory channels.
    pub fn link(&self, dialer: usize, listener: usize) -> Link {
        let id = self.last_id.get() + 1;
        self.last_id.set(id);
        let (to_listener, from_dialer) = framed_channel(LINK_DEPTH);
        let (to_dialer, from_listener) = framed_channel(LINK_DEPTH);
        let tokens = [CancellationToken::new(), CancellationToken::new()];
        let sides = [
            (
                dialer,
                listener,
                ServicePeerDirection::Outbound,
                (from_listener, to_listener),
            ),
            (
                listener,
                dialer,
                ServicePeerDirection::Inbound,
                (from_dialer, to_dialer),
            ),
        ];
        for ((local, remote, direction, streams), token) in sides.into_iter().zip(&tokens) {
            self.nodes[local].service.add_peer(Peer::new_versioned(
                id,
                self.nodes[remote].id.clone(),
                ZAKURA_CAP_BLOCK_SYNC_REGULATED,
                direction,
                ZAKURA_STREAM_BLOCK_SYNC,
                ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
                id,
                streams,
                token.clone(),
            ));
        }
        let peer = if dialer == 0 { listener } else { dialer };
        let link = Link {
            peer: self.nodes[peer].name.clone(),
            dialer,
            listener,
            conn_id: id,
            tokens,
        };
        self.links.borrow_mut().push(link.clone());
        link
    }

    /// Every link made, in order.
    pub fn links(&self) -> Vec<Link> {
        self.links.borrow().clone()
    }
}

/// The verified tip `engine` publishes.
pub fn verified_tip(engine: &EnginePort) -> Frontier {
    engine.view().frontiers.verified_best
}
