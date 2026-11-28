//! BPv1 beside header sync and block sync on one node (#2718).
//!
//! The node under test runs its real header-sync reactor, its real
//! block-sync reactor, and a real [`Bpv1Service`], all over one
//! [`EnginePort`], as zakurad's `Zakura` stack wires them
//! (`bpv1_wiring.rs`): BPv1 admits headers through the engine, follows its
//! committed snapshots, and shares one [`SharedBodyClaims`] with block sync.
//! Each mirror runs the same three services on its own engine, which holds
//! the whole chain. Mirror 0 proposes: [`Mesh::mine`] builds the next tip
//! block with a v6 coinbase that commits to its BPv1 encoding, publishes it
//! over BPv1, and then commits it on every mirror, whose header sync and
//! block sync then serve it.
//!
//! The node's verifier and state are a model, [`NodeState`], because the
//! controlled block service has no second submitter. It follows the
//! verifier router and zakura-state for the paths this harness drives. A
//! submission of a committed block is a duplicate. A submission of a block
//! already verifying waits for that verification and is then a duplicate,
//! as the router's in-flight gate makes it (#2726). A block whose parent is
//! not committed waits in an orphan queue of at most
//! [`MAX_BLOCK_REORG_HEIGHT`] blocks, which block sync and BPv1 share, as
//! zakura-state's `MAX_QUEUED_BLOCKS` does. A submission that finds the
//! queue full gets `QueueFull`. Once the parent commits, verification takes
//! a fixed delay, so blocks commit one at a time. The model answers block sync's queries at
//! once, and drops its body records and misbehavior reports, which it only
//! counts.
//!
//! A [`Tally`] counts what the issue's properties judge: BPv1's and block
//! sync's submissions per hash, verifications per hash, the path that
//! committed each block, `QueueFull` answers, and the bodies the mirrors
//! served per height.
//!
//! Each link is one connection between the node and a mirror: header
//! sync's stream, block sync's stream, and BPv1's control and data streams,
//! over in-memory channels. A relay carries the mirror's BPv1 data stream
//! to the node, and [`Mesh::hold`] makes it hold every frame but
//! `HeaderMeta`, which withholds the parts of the block in flight.
//!
//! Nothing here touches production code: the harness builds every service
//! through its public startup, and the ports are the production adapters
//! except the verifier.

use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    num::NonZeroU64,
    rc::Rc,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use futures::future::BoxFuture;
use zakura_chain::{
    amount::Amount,
    block::{
        self, merkle::AuthDataRoot, Block, ChainHistoryBlockTxAuthCommitmentHash,
        ChainHistoryMmrRootHash, Height,
    },
    parameters::{
        constants::MAX_BLOCK_REORG_HEIGHT, testnet::ConfiguredActivationHeights, Network,
        NetworkUpgrade,
    },
    serialization::ZcashSerialize,
    transaction::{LockTime, Transaction},
    transparent::{self, Output, Script},
};
use zakura_header_chain::Frontier;
use zakura_network::{
    zakura::{
        bpv1::{
            wire::message_type, BlockPhase, BlockVerifierPort, HavePacing,
            HeaderChainAdmissionAdapter, MemoryBodyStore, PortFuture, Profile, VerifierVerdict,
            WorkId, BPV1_CONTROL_KIND, BPV1_DATA_KIND,
        },
        framed_channel, spawn_block_sync_reactor, BlockRangeRead, BlockRangeReadResult,
        BlockRangeSource, BlockSyncAction, BlockSyncFrontiers, BlockSyncService, BlockSyncStartup,
        Bpv1Handle, Bpv1Ports, Bpv1Service, FramedRecv, FramedSend, Peer, Service,
        ServicePeerDirection, SessionOrigin, ZakuraBlockSyncConfig, ZakuraBpv1Config, ZakuraPeerId,
        ZAKURA_CAP_BLOCK_SYNC_REGULATED, ZAKURA_CAP_BPV1, ZAKURA_CAP_HEADER_SYNC,
        ZAKURA_HEADER_SYNC_STREAM_VERSION, ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
        ZAKURA_STREAM_BLOCK_SYNC, ZAKURA_STREAM_HEADER_SYNC,
    },
    BoxError,
};
use zakura_node_services::{
    block_memory::{BlockHandle, DecodedBlockPool, OwnedBlock},
    body_claims::SharedBodyClaims,
    bpv1_proposer::Bpv1ProposerHook,
    header_chain::{ApplyHeaderTargetOutcome, Port},
};
use zakura_quic::{NodeId, NodeSecretKey};
use zakura_rt::{
    collections::HashMap,
    sync::{mpsc, watch, CancellationToken},
    time,
};
use zakura_storage::{
    block::{
        controlled::BlockReplies,
        fixture::{BlockFixture, BodySpec, FixtureBlock},
        ApplyClass, HandleEvent, RetryKind, Verdict,
    },
    full_state::query_needed_blocks,
    header::adapter::EnginePort,
};

use super::{
    block_node::preload,
    header_node::{DispatchFlag, HeaderNode, LINK_DEPTH},
};
use crate::Sim;

/// The orphan queue's bound: zakura-state's `MAX_QUEUED_BLOCKS`.
const MAX_QUEUED_BLOCKS: usize = MAX_BLOCK_REORG_HEIGHT as usize;

/// Decoded blocks BPv1's body store holds: zakurad's floor of 128 retained
/// blocks plus two per concurrent BPv1 block (`build_bpv1_service`).
const BPV1_POOL_BLOCKS: u64 = 128 + 2 * 4;

/// The light-client root every mined block commits to. The model checks no
/// history, so any value binds the header to the coinbase.
const LIGHT_CLIENT_ROOT: [u8; 32] = [0x99; 32];

/// A mesh: a node that starts `node_blocks` above genesis, and mirrors that
/// hold `blocks`.
#[derive(Clone, Debug)]
pub struct MeshScenario {
    /// Blocks above genesis every mirror holds at the start.
    pub blocks: u32,
    /// Blocks above genesis the node holds at the start, headers and bodies.
    pub node_blocks: u32,
    /// Mirrors, each linked once to the node. Mirror 0 proposes.
    pub mirrors: u8,
    /// Each starting block's serialized size in bytes.
    pub body_bytes: u32,
    /// How long the node's verifier takes for one block once its parent
    /// commits.
    pub verify_delay: Duration,
}

/// The path that delivered a block to the node's state.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Via {
    /// BPv1's verifier port.
    Bpv1,
    /// Block sync's `SubmitBlock` action.
    BlockSync,
}

/// The node state's answer to one submission.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Answer {
    /// This submission committed the block.
    Committed,
    /// The block was committed, or another submission's verification
    /// committed it.
    Duplicate,
    /// The orphan queue was full.
    QueueFull,
    /// The run shut the node down first.
    Shutdown,
}

/// What the node's state and the mirrors' serving saw.
#[derive(Clone, Debug, Default)]
pub struct Tally {
    /// Submissions per path and hash.
    pub submitted: HashMap<(Via, block::Hash), u32>,
    /// Each submission's answer, in answer order.
    pub answers: Vec<(Via, block::Hash, Answer)>,
    /// Verifications started per hash.
    pub verified: HashMap<block::Hash, u32>,
    /// The path whose submission committed each block.
    pub committed: HashMap<block::Hash, Via>,
    /// `QueueFull` answers.
    pub queue_full: u32,
    /// Blocks in the orphan queue now.
    pub queued: usize,
    /// The most blocks the orphan queue held at once.
    pub most_queued: usize,
    /// Bodies the mirrors served to the node per height. Every mirror holds
    /// one chain, so a height names one block.
    pub served: BTreeMap<u32, u32>,
    /// Body records and misbehavior reports the node's block sync sent.
    pub records: u32,
}

impl Tally {
    /// Submissions of `hash` through `via`.
    pub fn submissions(&self, via: Via, hash: block::Hash) -> u32 {
        self.submitted.get(&(via, hash)).copied().unwrap_or(0)
    }

    /// Bodies the mirrors served to the node at `height`.
    pub fn fetches(&self, height: Height) -> u32 {
        self.served.get(&height.0).copied().unwrap_or(0)
    }
}

/// Locks `mutex`, taking the value even if a panicking holder poisoned it.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The node's verifier and state. See the module doc. Clones share it.
#[derive(Clone, Debug)]
pub struct NodeState {
    engine: Arc<EnginePort>,
    delay: Duration,
    tally: Arc<Mutex<Tally>>,
    /// Each verifying block's answer, once it has one.
    verifying: Arc<Mutex<HashMap<block::Hash, watch::Receiver<Option<Answer>>>>>,
    shutdown: CancellationToken,
}

impl NodeState {
    /// A copy of the tally now.
    pub fn tally(&self) -> Tally {
        lock(&self.tally).clone()
    }

    fn count(&self, change: impl FnOnce(&mut Tally)) {
        change(&mut lock(&self.tally));
    }

    /// Whether the node's full state holds `hash`.
    fn committed(&self, hash: block::Hash) -> bool {
        self.engine.with_full_state(|chain| chain.contains(hash))
    }

    /// Submits `block` through `via` and waits for the state's answer. A
    /// block already verifying gets that verification's answer, as a
    /// duplicate if it committed.
    async fn submit(&self, block: BlockHandle, via: Via) -> Answer {
        let hash = block.hash();
        self.count(|tally| *tally.submitted.entry((via, hash)).or_default() += 1);
        let answer = self.answer(block, via).await;
        self.count(|tally| tally.answers.push((via, hash, answer)));
        answer
    }

    async fn answer(&self, block: BlockHandle, via: Via) -> Answer {
        let hash = block.hash();
        if self.committed(hash) {
            return Answer::Duplicate;
        }
        // The spawn is a preemption point, so it waits until the verifying
        // lock drops: a task the scheduler runs there may submit too.
        let (mut answer, send) = {
            let mut verifying = lock(&self.verifying);
            match verifying.get(&hash) {
                Some(answer) => (answer.clone(), None),
                None => {
                    let (send, answer) = watch::channel(None);
                    verifying.insert(hash, answer.clone());
                    (answer, Some(send))
                }
            }
        };
        let joined = send.is_none();
        if let Some(send) = send {
            let state = self.clone();
            drop(zakura_rt::task::spawn_named(
                "bpv1-mesh-verify",
                async move {
                    let answer = state.verify(block, via).await;
                    lock(&state.verifying).remove(&hash);
                    let _ = send.send(Some(answer));
                },
            ));
        }
        let answer = loop {
            if let Some(answer) = *answer.borrow_and_update() {
                break answer;
            }
            zakura_rt::select! {
                () = self.shutdown.cancelled() => return Answer::Shutdown,
                changed = answer.changed() => if changed.is_err() {
                    return Answer::Shutdown;
                },
            }
        };
        match (answer, joined) {
            (Answer::Committed, true) => Answer::Duplicate,
            (answer, _) => answer,
        }
    }

    /// Waits in the orphan queue until `block`'s parent commits, then
    /// verifies and commits it. Commits follow the chain one at a time, as
    /// zakura-state's do. A block committed since [`Self::answer`] checked
    /// is a duplicate: an earlier verification committed it and left the
    /// verifying map in between.
    async fn verify(&self, block: BlockHandle, via: Via) -> Answer {
        let hash = block.hash();
        if self.committed(hash) {
            return Answer::Duplicate;
        }
        self.count(|tally| *tally.verified.entry(hash).or_default() += 1);
        let parent = block.header().previous_block_hash;
        // Subscribed before the first check, so no commit slips between.
        let mut views = self.engine.subscribe_views();
        let mut queued = false;
        let answer = loop {
            if self.committed(parent) {
                break None;
            }
            if !queued {
                let full = {
                    let mut tally = lock(&self.tally);
                    let full = tally.queued >= MAX_QUEUED_BLOCKS;
                    if full {
                        tally.queue_full += 1;
                    } else {
                        tally.queued += 1;
                        tally.most_queued = tally.most_queued.max(tally.queued);
                    }
                    full
                };
                if full {
                    return Answer::QueueFull;
                }
                queued = true;
            }
            zakura_rt::select! {
                () = self.shutdown.cancelled() => break Some(Answer::Shutdown),
                changed = views.changed() => if changed.is_err() {
                    break Some(Answer::Shutdown);
                },
            }
        };
        if queued {
            self.count(|tally| tally.queued -= 1);
        }
        if let Some(answer) = answer {
            return answer;
        }
        zakura_rt::select! {
            () = self.shutdown.cancelled() => return Answer::Shutdown,
            () = time::sleep(self.delay) => {},
        }
        match self.engine.preload_blocks([block]) {
            Ok(()) => {
                self.count(|tally| {
                    tally.committed.insert(hash, via);
                });
                Answer::Committed
            }
            Err(error) => panic!("the node's state refused {hash:?} after its parent: {error}"),
        }
    }
}

/// BPv1's verifier port on the node: the node's state.
struct NodeVerifier(NodeState);

impl BlockVerifierPort for NodeVerifier {
    fn submit(&self, block: OwnedBlock, _work_id: WorkId) -> PortFuture<'_, VerifierVerdict> {
        let state = self.0.clone();
        Box::pin(async move {
            match state.submit(block.into(), Via::Bpv1).await {
                Answer::Committed => VerifierVerdict::Valid,
                Answer::Duplicate => VerifierVerdict::Duplicate,
                Answer::QueueFull | Answer::Shutdown => VerifierVerdict::LocalCapacity,
            }
        })
    }
}

/// BPv1's verifier port on a mirror, whose engine commits each mined block
/// itself: a block it holds is a duplicate, and any other commits at once.
struct MirrorVerifier(Arc<EnginePort>);

impl BlockVerifierPort for MirrorVerifier {
    fn submit(&self, block: OwnedBlock, _work_id: WorkId) -> PortFuture<'_, VerifierVerdict> {
        let engine = self.0.clone();
        Box::pin(async move {
            let hash = block.hash();
            if engine.with_full_state(|chain| chain.contains(hash)) {
                return VerifierVerdict::Duplicate;
            }
            match engine.preload_blocks([block]) {
                Ok(()) => VerifierVerdict::Valid,
                Err(_) => VerifierVerdict::LocalCapacity,
            }
        })
    }
}

/// A mirror's range reads: full state at once, counted per height.
#[derive(Debug)]
struct MirrorReads {
    engine: Arc<EnginePort>,
    tally: Arc<Mutex<Tally>>,
}

impl BlockRangeSource for MirrorReads {
    fn read(
        &self,
        request: BlockRangeRead,
    ) -> BoxFuture<'static, Result<BlockRangeReadResult, BoxError>> {
        let engine = self.engine.clone();
        let tally = self.tally.clone();
        // The read runs in the caller's task: only a task can wait for the
        // engine's lock.
        Box::pin(async move {
            let blocks = engine
                .read_block_range(
                    request.start_height,
                    request.count,
                    request.max_body_bytes,
                    || false,
                )
                .map_err(BoxError::from)?;
            let mut tally = lock(&tally);
            for (height, _) in &blocks {
                *tally.served.entry(height.0).or_default() += 1;
            }
            drop(tally);
            Ok(BlockRangeReadResult {
                blocks,
                lease: request.lease,
            })
        })
    }
}

/// One actor: header sync, block sync and BPv1 on one engine.
pub struct Actor {
    /// `node` or `mirror-<i>`.
    pub name: String,
    /// Its engine.
    pub engine: Arc<EnginePort>,
    /// Its BPv1 handle.
    pub bpv1: Bpv1Handle,
    /// The body claims its BPv1 and block sync share.
    pub claims: SharedBodyClaims,
    node_id: NodeId,
    header: HeaderNode,
    block: Arc<BlockSyncService>,
    bpv1_service: Arc<Bpv1Service>,
    block_shutdown: CancellationToken,
    bpv1_shutdown: CancellationToken,
}

impl Actor {
    fn peer_id(&self) -> ZakuraPeerId {
        ZakuraPeerId::from_node_id(self.node_id)
    }
}

/// The node under test, the mirrors, and the links between them. See the
/// module doc.
pub struct Mesh {
    /// The node under test.
    pub node: Actor,
    /// The mirrors. Mirror 0 proposes.
    pub mirrors: Vec<Actor>,
    /// The node's verifier and state.
    pub state: NodeState,
    fixture: BlockFixture,
    /// The mirrors' chain above genesis.
    chain: RefCell<Vec<FixtureBlock>>,
    /// Each mirror's hold on its BPv1 data stream to the node.
    holds: Vec<watch::Sender<bool>>,
    /// Each link's connection tokens.
    links: Vec<[CancellationToken; 2]>,
}

/// Regtest with NU7 active from height 1, so every mined block is a BPv1
/// block. It shares the fixture's genesis block, which is BPv1's chain ID.
fn bpv1_network() -> Network {
    Network::new_regtest(
        ConfiguredActivationHeights {
            nu7: Some(1),
            ..Default::default()
        }
        .into(),
    )
}

/// The node ID seeded by `seed`, which header sync and block sync also
/// derive their peer IDs from.
fn node_id(seed: u8) -> NodeId {
    NodeSecretKey::from_bytes(&[seed; 32]).public()
}

impl Mesh {
    /// Commits the scenario's chains, spawns every actor, and links each
    /// mirror to the node, the mirror dialing.
    ///
    /// # Panics
    ///
    /// If the scenario has no mirror, or the node starts above the mirrors.
    pub fn build(sim: &mut Sim, scenario: &MeshScenario) -> Rc<Mesh> {
        assert!(scenario.mirrors > 0, "mirror 0 proposes");
        assert!(scenario.node_blocks <= scenario.blocks);
        let fixture = BlockFixture::regtest();
        let chain = fixture.uniform(
            fixture.anchor(),
            scenario.blocks,
            BodySpec::exact(scenario.body_bytes),
            0,
        );
        let tally = Arc::new(Mutex::new(Tally::default()));
        let engine = |held: usize| {
            let engine = Arc::new(EnginePort::new(fixture.headers()));
            preload(&engine, &fixture, &chain[..held], held);
            engine
        };
        let node_engine = engine(scenario.node_blocks as usize);
        let state = NodeState {
            engine: node_engine.clone(),
            delay: scenario.verify_delay,
            tally: tally.clone(),
            verifying: Arc::default(),
            shutdown: CancellationToken::new(),
        };
        let node = spawn_actor(
            sim,
            &fixture,
            "node".to_owned(),
            1,
            node_engine,
            Arc::new(NodeVerifier(state.clone())),
            None,
            Some(state.clone()),
        );
        let mirrors: Vec<Actor> = (0..scenario.mirrors)
            .map(|index| {
                let engine = engine(chain.len());
                spawn_actor(
                    sim,
                    &fixture,
                    format!("mirror-{index}"),
                    index + 2,
                    engine.clone(),
                    Arc::new(MirrorVerifier(engine.clone())),
                    Some(Arc::new(MirrorReads {
                        engine,
                        tally: tally.clone(),
                    })),
                    None,
                )
            })
            .collect();
        let mut holds = Vec::new();
        let mut links = Vec::new();
        for (conn, mirror) in (1..).zip(&mirrors) {
            let (hold, held) = watch::channel(false);
            links.push(link(sim, conn, mirror, &node, held));
            holds.push(hold);
        }
        Rc::new(Mesh {
            node,
            mirrors,
            state,
            fixture,
            chain: RefCell::new(chain),
            holds,
            links,
        })
    }

    /// The mirrors' tip.
    pub fn tip(&self) -> Frontier {
        self.chain
            .borrow()
            .last()
            .map_or(self.fixture.anchor(), FixtureBlock::frontier)
    }

    /// The node's verified tip.
    pub fn node_tip(&self) -> Frontier {
        self.node.engine.view().frontiers.verified_best
    }

    /// The node's BPv1 phase of `hash`, if its block table holds it.
    pub fn phase(&self, hash: block::Hash) -> Option<BlockPhase> {
        self.node.bpv1.block_phase(&hash)
    }

    /// Holds, or releases, every BPv1 data frame but `HeaderMeta` from mirror
    /// `index` to the node.
    pub fn hold(&self, index: usize, held: bool) {
        self.holds[index].send_replace(held);
    }

    /// Mines the next tip block on mirror 0: a v6 coinbase that commits to
    /// the block's BPv1 encoding, and one transaction tagged by its height.
    /// Mirror 0 publishes it over BPv1, then every mirror commits it.
    ///
    /// # Panics
    ///
    /// If mirror 0 prepares no commitment, refuses to publish, or a mirror
    /// refuses the block.
    pub async fn mine(&self) -> FixtureBlock {
        let parent = self.tip();
        let height = parent.height.next().expect("mesh heights stay small");
        let mined = self.solved(parent, height);
        assert!(
            self.mirrors[0]
                .bpv1
                .publish_solved_block(&mined.block, height.0),
            "mirror 0 publishes its block"
        );
        for mirror in &self.mirrors {
            let engine = &mirror.engine;
            let request = self.fixture.target_request(
                &engine.snapshot(),
                parent,
                std::slice::from_ref(&mined),
                1,
            );
            let prepared = engine
                .prepare_header_target(request)
                .await
                .expect("a mirror prepares the mined header");
            assert_eq!(
                engine
                    .apply_header_target(prepared)
                    .await
                    .expect("a mirror applies the mined header"),
                ApplyHeaderTargetOutcome::Applied,
            );
            engine
                .preload_blocks([mined.block.clone()])
                .expect("a mirror commits the mined block");
        }
        self.chain.borrow_mut().push(mined.clone());
        mined
    }

    /// The block at `height` on `parent` whose coinbase carries the
    /// commitment mirror 0's BPv1 prepares for its body.
    fn solved(&self, parent: Frontier, height: Height) -> FixtureBlock {
        let tag = height.0.to_le_bytes();
        let tx = Transaction::V6 {
            network_upgrade: NetworkUpgrade::Nu7,
            lock_time: LockTime::min_lock_time_timestamp(),
            expiry_height: height,
            inputs: vec![transparent::Input::PrevOut {
                outpoint: transparent::OutPoint {
                    hash: zakura_chain::transaction::Hash([tag[0]; 32]),
                    index: height.0,
                },
                unlock_script: Script::new(&[0x51]),
                sequence: u32::MAX,
            }],
            outputs: vec![Output::new(
                Amount::try_from(1_000).expect("a valid amount"),
                Script::new(&[0x6a, 0x04, tag[0], tag[1], tag[2], tag[3]]),
            )],
            sapling_shielded_data: None,
            orchard_shielded_data: None,
            ironwood_shielded_data: None,
        };
        let body = tx
            .zcash_serialize_to_vec()
            .expect("a v6 transaction serializes");
        let payload = self.mirrors[0]
            .bpv1
            .prepare_template_commitment(LIGHT_CLIENT_ROOT, height, 2, &body)
            .expect("mirror 0 prepares a commitment for a post-NU7 template");
        let mut commitment = vec![0x6a, u8::try_from(payload.len()).expect("a short push")];
        commitment.extend_from_slice(&payload);
        let coinbase = Transaction::V6 {
            network_upgrade: NetworkUpgrade::Nu7,
            lock_time: LockTime::min_lock_time_timestamp(),
            expiry_height: height,
            inputs: vec![transparent::Input::Coinbase {
                height,
                data: vec![0x7a, 0x6b],
                sequence: u32::MAX,
            }],
            outputs: vec![
                Output::new(
                    Amount::try_from(1_000_000).expect("a valid amount"),
                    Script::new(&[0x51]),
                ),
                Output::new(
                    Amount::try_from(0).expect("a valid amount"),
                    Script::new(&commitment),
                ),
            ],
            sapling_shielded_data: None,
            orchard_shielded_data: None,
            ironwood_shielded_data: None,
        };
        let transactions = vec![Arc::new(coinbase), Arc::new(tx)];
        let mut header = *self
            .fixture
            .headers()
            .extend(parent, 1, 0)
            .pop()
            .expect("the fixture extends one header");
        header.merkle_root = transactions.iter().map(|tx| tx.as_ref()).collect();
        let auth_root: AuthDataRoot = transactions.iter().map(|tx| tx.as_ref()).collect();
        header.commitment_bytes =
            <[u8; 32]>::from(ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
                &ChainHistoryMmrRootHash::from(LIGHT_CLIENT_ROOT),
                &auth_root,
            ))
            .into();
        let block = Arc::new(Block {
            header: Arc::new(header),
            transactions,
        });
        let size = u32::try_from(block.zcash_serialized_size()).expect("a small block");
        FixtureBlock {
            block,
            height,
            hint: size,
        }
    }

    /// Stops every actor, as zakurad's shutdown does: closes every link,
    /// then cancels each actor's header sync, block sync, BPv1, and the
    /// node's state.
    pub fn shut_down(&self) {
        for tokens in &self.links {
            for token in tokens {
                token.cancel();
            }
        }
        for actor in std::iter::once(&self.node).chain(&self.mirrors) {
            actor.header.cancel();
            actor.block_shutdown.cancel();
            actor.bpv1_shutdown.cancel();
        }
        self.state.shutdown.cancel();
    }
}

/// Spawns one actor's header sync, block sync and BPv1 over `engine`, as
/// zakurad wires them. A mirror serves range reads from `reads`. The node
/// submits bodies to `state`.
#[allow(clippy::too_many_arguments)]
fn spawn_actor(
    sim: &mut Sim,
    fixture: &BlockFixture,
    name: String,
    seed: u8,
    engine: Arc<EnginePort>,
    verifier: Arc<dyn BlockVerifierPort>,
    reads: Option<Arc<dyn BlockRangeSource>>,
    state: Option<NodeState>,
) -> Actor {
    let view = engine.view();
    let frontiers = view.frontiers;
    let header = HeaderNode::spawn(
        sim,
        &name,
        seed,
        engine.clone() as Arc<dyn Port>,
        fixture.headers(),
        frontiers.header_best,
        DispatchFlag::default(),
        None,
    );
    let claims = SharedBodyClaims::new();
    // Block sync, as `BlockNode::spawn` starts it, sharing BPv1's claims.
    let config = ZakuraBlockSyncConfig::default();
    let mut startup = BlockSyncStartup::new_with_committed_views(
        BlockSyncFrontiers {
            finalized_height: frontiers.finalized.height,
            verified_block_tip: frontiers.verified_best.height,
            verified_block_hash: frontiers.verified_best.hash,
        },
        (frontiers.header_best.height, frontiers.header_best.hash),
        engine.subscribe_views(),
        config
            .decoded_block_pool(10_000)
            .expect("the fixture's decoded allowance fits"),
        config,
    )
    .with_body_claims(claims.clone());
    // Regtest's checkpoint list holds only genesis.
    startup.max_checkpoint_height = Some(Height(0));
    if let Some(reads) = reads {
        startup = startup.with_range_source(reads);
    }
    let block_shutdown = startup.shutdown.clone();
    let (handle, actions, reactor) =
        spawn_block_sync_reactor(startup).expect("a fixture startup is valid");
    sim.scheduler().spawn_root("block-node", async move {
        let _ = reactor.await;
    });
    spawn_actions(
        sim,
        engine.clone(),
        actions,
        handle.clone(),
        state,
        block_shutdown.clone(),
    );
    let block = Arc::new(BlockSyncService::new_with_handle(handle));
    // BPv1, as `build_bpv1_service` starts it.
    let bpv1_shutdown = CancellationToken::new();
    let ports = Bpv1Ports {
        header_admission: Arc::new(HeaderChainAdmissionAdapter::new(
            engine.clone() as Arc<dyn Port>
        )),
        body_store: Arc::new(MemoryBodyStore::new(DecodedBlockPool::for_block_count(
            NonZeroU64::new(BPV1_POOL_BLOCKS).expect("a non-zero pool"),
        ))),
        verifier,
        fallback: Arc::new(claims.clone()),
        committed_snapshots: engine.selected_chain(),
    };
    let (bpv1_service, bpv1) = Bpv1Service::start(
        ZakuraBpv1Config::default(),
        HavePacing::default(),
        Profile::W2,
        bpv1_network(),
        node_id(seed),
        ports,
        bpv1_shutdown.clone(),
    );
    Actor {
        name,
        engine,
        bpv1,
        claims,
        node_id: node_id(seed),
        header,
        block,
        bpv1_service,
        block_shutdown,
        bpv1_shutdown,
    }
}

/// Answers one actor's block-sync actions until the channel closes or
/// `shutdown` fires. Its handle keeps the sequencer's body input open, and
/// the sequencer keeps the action channel open, so only `shutdown` ends the
/// wait at an idle tip. Queries read full state at once. The node submits each body to `state` and
/// replies with its answer. A mirror holds every body, so it submits none.
fn spawn_actions(
    sim: &mut Sim,
    engine: Arc<EnginePort>,
    mut actions: mpsc::Receiver<BlockSyncAction>,
    handle: zakura_network::zakura::BlockSyncHandle,
    state: Option<NodeState>,
    shutdown: CancellationToken,
) {
    sim.scheduler().spawn_root("block-actions", async move {
        loop {
            let action = zakura_rt::select! {
                () = shutdown.cancelled() => return,
                action = actions.recv() => match action {
                    Some(action) => action,
                    None => return,
                },
            };
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
                BlockSyncAction::SubmitBlock {
                    owner,
                    source,
                    token,
                    block,
                } => {
                    let state = state
                        .clone()
                        .unwrap_or_else(|| panic!("a mirror submitted {}", block.hash()));
                    let handle = handle.clone();
                    drop(zakura_rt::task::spawn_named(
                        "bpv1-mesh-apply",
                        async move {
                            let hash = block.hash();
                            let height = block
                                .coinbase_height()
                                .expect("block sync checked the coinbase height");
                            let verdict = match state.submit(block, Via::BlockSync).await {
                                Answer::Committed => Verdict::Committed,
                                Answer::Duplicate => Verdict::Duplicate,
                                Answer::QueueFull => {
                                    Verdict::Retryable(RetryKind::ResourceExhausted)
                                }
                                Answer::Shutdown => return,
                            };
                            let _ = BlockReplies::send(
                                &handle,
                                HandleEvent::ApplyFinished {
                                    owner,
                                    source,
                                    token,
                                    height,
                                    hash,
                                    outcome: verdict.outcome(owner, source, hash, ApplyClass::Full),
                                },
                            );
                        },
                    ));
                }
                _ => {
                    if let Some(state) = &state {
                        state.count(|tally| tally.records += 1);
                    }
                }
            }
        }
    });
}

/// Links `mirror` to `node` as connection `conn`, the mirror dialing: a
/// session of header sync, one of block sync, and one of BPv1, each side's
/// sessions sharing its connection token. A relay carries the mirror's BPv1
/// data stream to the node, holding every frame but `HeaderMeta` while
/// `held` is set.
/// Returns the mirror's token and the node's.
fn link(
    sim: &mut Sim,
    conn: u64,
    mirror: &Actor,
    node: &Actor,
    held: watch::Receiver<bool>,
) -> [CancellationToken; 2] {
    let tokens = [CancellationToken::new(), CancellationToken::new()];
    let origin = SessionOrigin {
        connection: conn,
        quic_stream: None,
        generation: conn,
    };
    let (header_to_node, header_from_mirror) = framed_channel(LINK_DEPTH);
    let (header_to_mirror, header_from_node) = framed_channel(LINK_DEPTH);
    let (block_to_node, block_from_mirror) = framed_channel(LINK_DEPTH);
    let (block_to_mirror, block_from_node) = framed_channel(LINK_DEPTH);
    let (control_to_node, control_from_mirror) = framed_channel(64);
    let (control_to_mirror, control_from_node) = framed_channel(64);
    let (data_to_relay, data_from_mirror) = framed_channel(256);
    let (data_to_node, data_from_relay) = framed_channel(256);
    let (data_to_mirror, data_from_node) = framed_channel(256);
    relay(sim, data_from_mirror, data_to_node, held, tokens[1].clone());
    let sides = [
        (
            mirror,
            node,
            ServicePeerDirection::Outbound,
            (header_from_node, header_to_node),
            (block_from_node, block_to_node),
            [
                (BPV1_CONTROL_KIND, (control_from_node, control_to_node)),
                (BPV1_DATA_KIND, (data_from_node, data_to_relay)),
            ],
            &tokens[0],
        ),
        (
            node,
            mirror,
            ServicePeerDirection::Inbound,
            (header_from_mirror, header_to_mirror),
            (block_from_mirror, block_to_mirror),
            [
                (BPV1_CONTROL_KIND, (control_from_mirror, control_to_mirror)),
                (BPV1_DATA_KIND, (data_from_relay, data_to_mirror)),
            ],
            &tokens[1],
        ),
    ];
    for (local, remote, direction, header, block, bpv1, token) in sides {
        local.header.service().add_peer(
            Peer::new_versioned(
                conn,
                remote.peer_id(),
                ZAKURA_CAP_HEADER_SYNC,
                direction,
                ZAKURA_STREAM_HEADER_SYNC,
                ZAKURA_HEADER_SYNC_STREAM_VERSION,
                conn,
                header,
                token.clone(),
            )
            .with_origin(origin),
        );
        local.block.add_peer(Peer::new_versioned(
            conn,
            remote.peer_id(),
            ZAKURA_CAP_BLOCK_SYNC_REGULATED,
            direction,
            ZAKURA_STREAM_BLOCK_SYNC,
            ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
            conn,
            block,
            token.clone(),
        ));
        local.bpv1_service.add_peer(Peer::new_with_direction(
            remote.peer_id(),
            None,
            ZAKURA_CAP_BPV1,
            direction,
            HashMap::from(bpv1),
            token.clone(),
        ));
    }
    tokens
}

/// Carries frames from `from` to `to` in order, until `token` is cancelled
/// or either end closes. While `held` is set, the relay queues every frame
/// except `HeaderMeta`, which leads its block's traffic on the data stream, so
/// the node admits a held block but receives none of its parts. Lifting the
/// hold sends the queued frames in order.
fn relay(
    sim: &mut Sim,
    mut from: FramedRecv,
    to: FramedSend,
    mut held: watch::Receiver<bool>,
    token: CancellationToken,
) {
    sim.scheduler().spawn_root("bpv1-data-relay", async move {
        let mut queued = VecDeque::new();
        loop {
            if !*held.borrow_and_update() {
                while let Some(frame) = queued.pop_front() {
                    zakura_rt::select! {
                        () = token.cancelled() => return,
                        sent = to.send(frame) => if sent.is_err() {
                            return;
                        },
                    }
                }
            }
            let frame = zakura_rt::select! {
                () = token.cancelled() => return,
                changed = held.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    continue;
                }
                frame = from.recv() => match frame {
                    Some(frame) => frame,
                    None => return,
                },
            };
            if *held.borrow() && frame.message_type != message_type::HEADER_META {
                queued.push_back(frame);
                continue;
            }
            zakura_rt::select! {
                () = token.cancelled() => return,
                sent = to.send(frame) => if sent.is_err() {
                    return;
                },
            }
        }
    });
}
