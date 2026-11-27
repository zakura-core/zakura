//! Helpers shared by the BPv1 test modules, and the `tool-4` machine mesh.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    num::NonZeroU64,
    sync::Arc,
    time::Duration,
};

use zakura_chain::{
    block::{self, Block, Header, Height},
    parameters::Network,
    serialization::ZcashSerialize,
    transaction::Transaction,
};
use zakura_node_services::{
    block_memory::{DecodedBlockPool, OwnedBlock},
    body_claims::SharedBodyClaims,
};
use zakura_quic::{ConnLoad, NodeId};
use zakura_rt::{
    collections::{HashMap, HashSet},
    sync::{watch, CancellationToken, Mutex},
    time::Instant,
};

use super::{
    blocks::{
        BlockConfig, BlockEvent, BlockInput, BlockOutput, BlockPhase, BlockStep, BlockTable,
        HeaderAdmissionResult, MetaEnvelope, PullPeerState, VerifierVerdict, WorkId,
    },
    codec::decode_and_check_stripe,
    commitment::CoinbaseBinding,
    config::{HavePacing, ZakuraBpv1Config},
    paths::PathEstimate,
    peer::{
        PartAuth, PeerConfig, PeerEvent, PeerInput, PeerOutput, PeerState, PeerStep, RecvHave,
        RecvPartPrefix, RecvWant, RecvWantEnd, Seed, Seeds, ServeDone, ServePartQueued,
    },
    ports::{BlockVerifierPort, HeaderAdmissionPort, MemoryBodyStore, PortFuture},
    profile::{Coding, Profile},
    proposer::{queue_seeds, seed_plan, ProposerOutput},
    route::{
        charge_part, held_parts, pull_state, refused_grant, route_block_output, route_peer_output,
        BlockRoute, PeerRoute,
    },
    service::{Bpv1Handle, Bpv1Ports, Bpv1Service},
    tree::VerifiedPart,
    unknown_parent::UnknownParentVerdict,
    wire::{
        encode_part_frame, part_payload_len, Bpv1Message, HaveFlags, HaveForm, WantBitmap,
        WantEndReason, BPV1_CONTROL_KIND, BPV1_DATA_KIND, ZAKURA_CAP_BPV1,
    },
};
use crate::zakura::{
    transport::{framed_channel, Peer, Service},
    ServicePeerDirection, ZakuraPeerId,
};

use super::fuzz::chain::v6_tx;
pub(super) use super::fuzz::chain::{
    block_of, build_v6_coinbase, build_v6_tx, committed_coinbase, regtest_nu7, regtest_nu7_at,
};

// ============================================================================
// Identities, transactions, and ports
// ============================================================================

/// A zeroed `Part` frame payload of the exact length `coding` requires for
/// `index` (bpv1-66), or a bare 36-byte prefix when `index` is out of range.
pub(super) fn part_frame(coding: &Coding, index: u32) -> Vec<u8> {
    vec![0; part_payload_len(coding, index).unwrap_or(36)]
}

/// A node id of 32 `b` bytes. It skips key derivation, which costs milliseconds
/// in an unoptimized build. BPv1 machines only compare and hash node ids.
pub(super) fn node_id(b: u8) -> NodeId {
    NodeId::from_bytes(&[b; 32])
}

/// A v6 transaction at `height` whose `OP_RETURN` output carries
/// `payload_len` bytes of `tag`.
pub(super) fn build_large_v6_tx(height: Height, tag: u8, payload_len: usize) -> Transaction {
    let mut script_bytes = Vec::with_capacity(payload_len + 4);
    script_bytes.push(0x6a); // OP_RETURN
    script_bytes.push(0x4d); // OP_PUSHDATA2
    script_bytes.extend_from_slice(&(payload_len as u16).to_le_bytes());
    script_bytes.resize(script_bytes.len() + payload_len, tag);
    v6_tx(height, tag, 0, &script_bytes)
}

/// A solved block at `height` whose coinbase carries the commitment that
/// `proposer` prepares for one transaction tagged `tag`.
pub(super) fn solved_block(proposer: &Bpv1Handle, height: Height, tag: u8) -> Block {
    solved_block_of(proposer, height, vec![build_v6_tx(height, tag)])
}

/// A solved block at `height` holding `non_coinbase` after a coinbase that
/// carries the commitment `proposer` prepares for them.
pub(super) fn solved_block_of(
    proposer: &Bpv1Handle,
    height: Height,
    non_coinbase: Vec<Transaction>,
) -> Block {
    let non_coinbase: Vec<Arc<Transaction>> = non_coinbase.into_iter().map(Arc::new).collect();
    let mut body = Vec::new();
    for tx in &non_coinbase {
        tx.zcash_serialize(&mut body)
            .expect("a v6 transaction serializes");
    }
    // The coinbase is the one transaction outside the body.
    let tx_count = u32::try_from(non_coinbase.len() + 1).expect("a test block is small");
    let lcr = [0x99; 32];
    let commitment = proposer
        .prepare_commitment(lcr, height, tx_count, &body)
        .expect("a post-NU7 template gets a commitment");
    let mut txs = vec![Arc::new(committed_coinbase(height, &commitment.script))];
    txs.extend(non_coinbase);
    block_of(lcr, txs)
}

/// Header admission that admits every header at the given height.
pub(super) struct AcceptingHeaderAdmission(pub(super) u32);

impl HeaderAdmissionPort for AcceptingHeaderAdmission {
    fn admit(
        &self,
        header: Arc<Header>,
        _source_peer: NodeId,
        _session_gen: u64,
        _work_id: WorkId,
    ) -> PortFuture<'_, HeaderAdmissionResult> {
        let result = HeaderAdmissionResult::Admitted {
            height: self.0,
            parent_hash: header.previous_block_hash,
        };
        Box::pin(async move { result })
    }
}

/// A block-verifier port that answers every block with one verdict and
/// records the hash of each block it sees.
#[derive(Clone)]
pub(super) struct ScriptedVerifier {
    verdict: VerifierVerdict,
    seen: Arc<Mutex<Vec<block::Hash>>>,
}

impl ScriptedVerifier {
    pub(super) fn new(verdict: VerifierVerdict) -> Self {
        Self {
            verdict,
            seen: Arc::default(),
        }
    }

    /// The hashes of the blocks submitted so far, in submission order.
    pub(super) fn seen(&self) -> Vec<block::Hash> {
        self.seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl BlockVerifierPort for ScriptedVerifier {
    fn submit(&self, block: OwnedBlock, _work_id: WorkId) -> PortFuture<'_, VerifierVerdict> {
        self.seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(block.hash());
        let verdict = self.verdict;
        Box::pin(async move { verdict })
    }
}

/// A committed snapshot whose verified tip and best header are both at `height`.
fn snapshot_at(height: u32) -> zakura_header_chain::EngineSnapshot {
    let tip = zakura_header_chain::Frontier {
        height: Height(height),
        hash: block::Hash([0xEE; 32]),
    };
    zakura_header_chain::EngineSnapshot {
        mode: zakura_header_chain::EngineMode::Integrated,
        state_version: zakura_header_chain::StateVersion::new(1),
        header_generation: zakura_header_chain::HeaderGeneration::new(1),
        verified_generation: zakura_header_chain::VerifiedGeneration::new(1),
        frontiers: zakura_header_chain::FrontierSet {
            finalized: tip,
            header_best: tip,
            verified_best: tip,
        },
        header_best_score: zakura_header_chain::ChainScore::new(
            zakura_header_chain::SuffixWork::zero(),
            tip.hash,
        ),
        oldest_retained_height: tip.height,
        alarms: zakura_header_chain::AlarmSet::default(),
    }
}

/// Ports that admit every header at `height`, keep bodies in a four-block
/// pool, verify with `verifier`, and claim bodies in `claims`. The committed
/// snapshot puts the verified tip at the parent height.
pub(super) fn ports(
    height: u32,
    verifier: impl BlockVerifierPort,
    claims: SharedBodyClaims,
) -> Bpv1Ports {
    Bpv1Ports {
        header_admission: Arc::new(AcceptingHeaderAdmission(height)),
        body_store: Arc::new(MemoryBodyStore::new(DecodedBlockPool::for_block_count(
            NonZeroU64::new(4).expect("4 is non-zero"),
        ))),
        verifier: Arc::new(verifier),
        fallback: Arc::new(claims),
        committed_snapshots: watch::channel(Some(snapshot_at(height.saturating_sub(1)))).1,
    }
}

/// A header-admission and verifier port that never answers.
struct Pending;

impl HeaderAdmissionPort for Pending {
    fn admit(
        &self,
        _header: Arc<Header>,
        _source_peer: NodeId,
        _session_gen: u64,
        _work_id: WorkId,
    ) -> PortFuture<'_, HeaderAdmissionResult> {
        Box::pin(std::future::pending())
    }
}

impl BlockVerifierPort for Pending {
    fn submit(&self, _block: OwnedBlock, _work_id: WorkId) -> PortFuture<'_, VerifierVerdict> {
        Box::pin(std::future::pending())
    }
}

/// Ports whose header admission and verifier never answer.
impl Default for Bpv1Ports {
    fn default() -> Self {
        Self {
            header_admission: Arc::new(Pending),
            ..ports(0, Pending, SharedBodyClaims::default())
        }
    }
}

/// Starts a service with the default config and `Have` pacing.
pub(super) fn start(
    profile: Profile,
    network: Network,
    id: NodeId,
    ports: Bpv1Ports,
    shutdown: &CancellationToken,
) -> (Arc<Bpv1Service>, Bpv1Handle) {
    Bpv1Service::start(
        ZakuraBpv1Config::default(),
        HavePacing::default(),
        profile,
        network,
        id,
        ports,
        shutdown.clone(),
    )
}

/// Connects two services with sessions that end when `shutdown` ends.
pub(super) fn connect(
    a: &Arc<Bpv1Service>,
    id_a: NodeId,
    b: &Arc<Bpv1Service>,
    id_b: NodeId,
    shutdown: &CancellationToken,
) {
    connect_pair(
        a,
        id_a,
        b,
        id_b,
        shutdown.child_token(),
        shutdown.child_token(),
        None,
    );
}

/// Starts services A and B on regtest NU7 with `profile` and connects them.
pub(super) fn pair(
    profile: Profile,
    ports_a: Bpv1Ports,
    ports_b: Bpv1Ports,
    shutdown: &CancellationToken,
) -> (Bpv1Handle, Bpv1Handle) {
    let (id_a, id_b) = (node_id(1), node_id(2));
    let (svc_a, handle_a) = start(profile, regtest_nu7(), id_a, ports_a, shutdown);
    let (svc_b, handle_b) = start(profile, regtest_nu7(), id_b, ports_b, shutdown);
    connect(&svc_a, id_a, &svc_b, id_b, shutdown);
    (handle_a, handle_b)
}

/// Polls `done` every 15 ms and panics if it stays false for ten seconds.
pub(super) async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
}

/// Connects two services over in-memory control and data streams. B's
/// connection to A reports `load_b`.
pub(super) fn connect_pair(
    svc_a: &Arc<Bpv1Service>,
    id_a: NodeId,
    svc_b: &Arc<Bpv1Service>,
    id_b: NodeId,
    cancel_a: CancellationToken,
    cancel_b: CancellationToken,
    load_b: Option<watch::Receiver<ConnLoad>>,
) {
    let (a2b_ctrl_tx, a2b_ctrl_rx) = framed_channel(64);
    let (b2a_ctrl_tx, b2a_ctrl_rx) = framed_channel(64);
    let (a2b_data_tx, a2b_data_rx) = framed_channel(256);
    let (b2a_data_tx, b2a_data_rx) = framed_channel(256);

    let mut streams_a = HashMap::new();
    streams_a.insert(BPV1_CONTROL_KIND, (b2a_ctrl_rx, a2b_ctrl_tx));
    streams_a.insert(BPV1_DATA_KIND, (b2a_data_rx, a2b_data_tx));
    let mut streams_b = HashMap::new();
    streams_b.insert(BPV1_CONTROL_KIND, (a2b_ctrl_rx, b2a_ctrl_tx));
    streams_b.insert(BPV1_DATA_KIND, (a2b_data_rx, b2a_data_tx));

    let peer_a = Peer::new_with_conn_id_and_direction(
        1,
        ZakuraPeerId::from_node_id(id_b),
        None,
        ZAKURA_CAP_BPV1,
        ServicePeerDirection::Outbound,
        streams_a,
        cancel_a,
    );
    let peer_b = Peer::new_with_conn_id_and_direction(
        2,
        ZakuraPeerId::from_node_id(id_a),
        None,
        ZAKURA_CAP_BPV1,
        ServicePeerDirection::Inbound,
        streams_b,
        cancel_b,
    );
    let peer_b = match load_b {
        Some(load) => peer_b.with_load(load),
        None => peer_b,
    };
    svc_a.add_peer(peer_a);
    svc_b.add_peer(peer_b);
}

/// The verified tip hash, and the parent hash headerchain reports, in table
/// tests. A block one above the verified tip then extends it.
pub(super) const TEST_TIP_HASH: block::Hash = block::Hash([0; 32]);

/// A metadata envelope whose header bytes are the block hash.
pub(super) fn make_envelope(
    block_id: block::Hash,
    coding: Coding,
    part_root: [u8; 32],
    proposer_node: NodeId,
) -> Box<MetaEnvelope> {
    Box::new(MetaEnvelope {
        block_id,
        header_bytes: block_id.0.to_vec(),
        binding: CoinbaseBinding {
            coinbase: vec![0x01, 0x02, 0x03],
            txid_siblings: Vec::new(),
            auth_siblings: Vec::new(),
            light_client_root: [0u8; 32],
        },
        coding,
        part_root,
        proposer_node,
    })
}

/// Admits `envelope` into `table` as the shell would: `MetaReceived` from its
/// proposer, the header admitted at `height`, then a valid commitment with
/// `peers` as the live sessions. Returns the `CommitmentChecked` step.
pub(super) fn admit_meta(
    table: &mut BlockTable,
    now: Instant,
    envelope: Box<MetaEnvelope>,
    height: u32,
    peers: &[NodeId],
) -> BlockStep {
    let block_id = envelope.block_id;
    let from_peer = envelope.proposer_node;
    let s1 = table.step(
        now,
        BlockInput::MetaReceived {
            envelope,
            from_peer,
            session_gen: 1,
        },
    );
    let s2 = table.step(
        now,
        BlockInput::HeaderAdmitted {
            work_id: admit_work(&s1),
            result: HeaderAdmissionResult::Admitted {
                height,
                parent_hash: TEST_TIP_HASH,
            },
        },
    );
    let s3 = table.step(
        now,
        BlockInput::CommitmentChecked {
            work_id: commit_work(&s2),
            ok: true,
            peers: peers.to_vec(),
            rng_seed: 7,
        },
    );
    assert!(s3.events.contains(&BlockEvent::BlockAdmitted { block_id }));
    assert_claim_precedes_publication(&s3, block_id);
    table.assert_invariants();
    s3
}

/// The work id of the step's `AdmitHeader` output.
pub(super) fn admit_work(step: &BlockStep) -> WorkId {
    step.outputs
        .iter()
        .find_map(|o| match o {
            BlockOutput::AdmitHeader { work_id, .. } => Some(*work_id),
            _ => None,
        })
        .expect("the step admits a header")
}

/// The work id of the step's `VerifyCommitment` output.
pub(super) fn commit_work(step: &BlockStep) -> WorkId {
    step.outputs
        .iter()
        .find_map(|o| match o {
            BlockOutput::VerifyCommitment { work_id, .. } => Some(*work_id),
            _ => None,
        })
        .expect("the step verifies a commitment")
}

/// 06 §6: BPv1 claims the body before it publishes `BlockAdmitted`, so native
/// block sync never sees an admitted body unclaimed.
fn assert_claim_precedes_publication(step: &BlockStep, block_id: block::Hash) {
    let position = |want: fn(&BlockOutput, block::Hash) -> bool| {
        step.outputs.iter().position(|o| want(o, block_id))
    };
    let claim =
        position(|o, id| matches!(o, BlockOutput::ClaimBody { block_id, .. } if *block_id == id))
            .expect("admission claims the body");
    let publish = position(|o, id| {
        matches!(o, BlockOutput::PublishAdmitted { admitted, .. } if admitted.block_id == id)
    })
    .expect("admission publishes BlockAdmitted");
    assert!(claim < publish, "the claim precedes BlockAdmitted");
}

// ============================================================================
// Tool-4: deterministic machine mesh (level-7)
// ============================================================================

/// The admitted height the mesh's header admission reports.
pub(super) const MESH_HEIGHT: u32 = 10;

/// Interval at which the mesh ticks every machine.
const MESH_TICK: Duration = Duration::from_millis(50);

#[derive(Clone, Debug)]
enum MeshFrame {
    Control(Bpv1Message),
    /// A `HeaderMeta`, which travels on the data stream (bpv1-8).
    HeaderMeta(Bpv1Message),
    Part {
        block_id: block::Hash,
        part: VerifiedPart,
    },
    Have {
        block_id: block::Hash,
        flags: HaveFlags,
        form: HaveForm,
        indices: Vec<u32>,
    },
    WantEnd {
        want_id: u32,
        sent: u16,
    },
}

#[derive(Clone, Debug)]
struct ScheduledDelivery {
    deliver_at: Instant,
    seq: u64,
    from: usize,
    to: usize,
    frame: MeshFrame,
}

impl Ord for ScheduledDelivery {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.deliver_at
            .cmp(&other.deliver_at)
            .then_with(|| self.seq.cmp(&other.seq))
    }
}

impl PartialOrd for ScheduledDelivery {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ScheduledDelivery {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for ScheduledDelivery {}

/// One direction of a mesh link. Each stream is FIFO; the control and data
/// streams have their own delay, so they interleave freely.
#[derive(Copy, Clone, Debug)]
pub(super) struct LinkProfile {
    pub(super) ctrl_delay: Duration,
    pub(super) data_delay: Duration,
    pub(super) rate_bps: u64,
    /// Data frames the link carries before it goes silent; `None` never does.
    pub(super) data_frames: Option<u32>,
    /// Whether the link carries nothing.
    pub(super) silent: bool,
}

impl LinkProfile {
    /// A link with one delay on both streams.
    pub(super) const fn new(delay: Duration, rate_bps: u64) -> Self {
        Self {
            ctrl_delay: delay,
            data_delay: delay,
            rate_bps,
            data_frames: None,
            silent: false,
        }
    }

    /// This link, carrying nothing.
    pub(super) const fn silent(self) -> Self {
        Self {
            silent: true,
            ..self
        }
    }
}

/// A scripted hostile node (`tool-6`). It runs no machines.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum Adversary {
    /// Announces every part of a block once `HeaderMeta` reaches it, then
    /// never answers a `Want`.
    LiesThenSilent,
    /// Announces every part, then ends each `Want` with nothing sent.
    LiesThenEmpty,
}

/// One mesh node: a production `BlockTable` and one `PeerState` per link.
pub(super) struct MeshNode {
    pub(super) id: NodeId,
    pub(super) table: BlockTable,
    pub(super) peers: BTreeMap<usize, PeerState>,
    pub(super) adversary: Option<Adversary>,
    /// When each block reached the verifier.
    pub(super) verified: HashMap<block::Hash, Instant>,
    /// Every table event, with the instant it happened.
    pub(super) block_events: Vec<(Instant, BlockEvent)>,
    /// Every pull grant: when, to whom, and which slots of which stripe.
    pub(super) grants: Vec<(Instant, NodeId, block::Hash, u32, WantBitmap)>,
    /// Every batch of direct copies released: when, to which node, and how
    /// many (decision-19).
    pub(super) copy_batches: Vec<(Instant, usize, usize)>,
    ctrl_ready_at: HashMap<usize, Instant>,
    data_ready_at: HashMap<usize, Instant>,
}

/// What crossed each directed link, for the wire checks.
#[derive(Default)]
struct WireLedger {
    meta: HashSet<(usize, usize, block::Hash)>,
    parts: HashSet<(usize, usize, block::Hash, u32)>,
    ends: HashSet<(usize, usize, u32)>,
    data_frames: HashMap<(usize, usize), u32>,
    /// Slots each receiver asked each sender for, per block (`bpv1-47`).
    requested: HashMap<(usize, usize, block::Hash), u32>,
    want_accepted: HashMap<(usize, usize, block::Hash), u32>,
    /// Parts in flight to their receiver's proof job, keyed by `(to, from, block, index)`.
    arrived: HashMap<(usize, usize, block::Hash, u32), VerifiedPart>,
    /// Indices each sender announced on each link, keyed by `(from, to, block, index)`.
    announced: HashSet<(usize, usize, block::Hash, u32)>,
    /// Indices each sender flagged for push on each link (decision-24).
    flagged: HashSet<(usize, usize, block::Hash, u32)>,
    /// Parts accepted under a holder's push slots, keyed by
    /// `(receiver, pusher, block)`.
    push_accepted: HashMap<(usize, usize, block::Hash), u32>,
    /// Parts accepted under the proposer's push slots, keyed by
    /// `(receiver, proposer, block)`.
    seed_accepted: HashMap<(usize, usize, block::Hash), u32>,
}

/// Deterministic N-node mesh of production machines with per-link rates and
/// delays, `tool-9` scripted path estimates, and `tool-6` adversaries. It
/// routes every output through the production router and checks the wire
/// rules after every frame and the machine invariants after every step.
pub(super) struct MachineMesh {
    pub(super) now: Instant,
    pub(super) direct_copies: bool,
    seq: u64,
    pub(super) nodes: Vec<MeshNode>,
    links: HashMap<(usize, usize), LinkProfile>,
    queue: BinaryHeap<Reverse<ScheduledDelivery>>,
    envelopes: HashMap<block::Hash, Box<MetaEnvelope>>,
    wire: WireLedger,
}

impl MachineMesh {
    pub(super) fn new(node_count: u8, block_cfg: BlockConfig) -> Self {
        let nodes = (1..=node_count)
            .map(|b| MeshNode {
                id: node_id(b),
                table: BlockTable::new(block_cfg),
                peers: BTreeMap::new(),
                adversary: None,
                verified: HashMap::new(),
                block_events: Vec::new(),
                grants: Vec::new(),
                copy_batches: Vec::new(),
                ctrl_ready_at: HashMap::new(),
                data_ready_at: HashMap::new(),
            })
            .collect();
        Self {
            now: Instant::now(),
            direct_copies: false,
            seq: 0,
            nodes,
            links: HashMap::new(),
            queue: BinaryHeap::new(),
            envelopes: HashMap::new(),
            wire: WireLedger::default(),
        }
    }

    /// Links `a` and `b`. `a_sees_b` is the path estimate `a` holds for `b`.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn connect(
        &mut self,
        a: usize,
        b: usize,
        a_to_b: LinkProfile,
        b_to_a: LinkProfile,
        peer_cfg: PeerConfig,
        a_sees_b: Option<PathEstimate>,
        b_sees_a: Option<PathEstimate>,
    ) {
        self.links.insert((a, b), a_to_b);
        self.links.insert((b, a), b_to_a);
        for (from, to, estimate) in [(a, b, a_sees_b), (b, a, b_sees_a)] {
            let remote = self.nodes[to].id;
            if self.nodes[from].adversary.is_none() {
                self.nodes[from]
                    .peers
                    .insert(to, PeerState::new(peer_cfg, remote, 1));
            }
            if let Some(estimate) = estimate {
                self.nodes[from].table.step(
                    self.now,
                    BlockInput::PathObserved {
                        peer: remote,
                        estimate,
                    },
                );
            }
        }
    }

    /// Makes node `idx` a scripted adversary. Call before `connect`.
    pub(super) fn make_adversary(&mut self, idx: usize, adversary: Adversary) {
        self.nodes[idx].adversary = Some(adversary);
    }

    /// Admits a solved block at its proposer `node`, builds the production
    /// seed plan over the node's sessions and path table (decision-19), and
    /// hands each neighbour's session its seeds and direct copies. The
    /// session releases the copies whenever its data link idles.
    pub(super) fn propose(
        &mut self,
        node: usize,
        envelope: Box<MetaEnvelope>,
        parts: Vec<VerifiedPart>,
    ) {
        let block_id = envelope.block_id;
        let peers: Vec<NodeId> = self.nodes[node]
            .peers
            .keys()
            .map(|&p| self.nodes[p].id)
            .collect();
        let plan = seed_plan(
            &envelope.coding,
            self.nodes[node].id,
            &peers,
            self.nodes[node].table.paths(),
            self.direct_copies,
        );
        self.envelopes.insert(block_id, envelope.clone());
        self.nodes[node].verified.insert(block_id, self.now);
        self.step_table(
            node,
            BlockInput::BlockAdmittedLocal {
                height: MESH_HEIGHT,
                envelope,
                parts: parts.clone(),
            },
        );
        for output in queue_seeds(block_id, &plan, &parts) {
            let ProposerOutput::QueueSeeds {
                peer,
                seeds,
                copies,
                ..
            } = output
            else {
                unreachable!("queue_seeds emits QueueSeeds only");
            };
            let Some(to) = self.index_of(node, &peer) else {
                continue;
            };
            let seeds = seeds
                .into_iter()
                .map(|seed| Seed {
                    part: seed.part,
                    relay: seed.relay,
                })
                .collect();
            self.step_peer(
                node,
                to,
                PeerInput::Seeds(Seeds {
                    block_id,
                    seeds,
                    copies,
                }),
            );
        }
    }

    /// Releases direct copies on every idle data link that has them, as the
    /// session's writer does once its queue drains (decision-19).
    fn release_copies(&mut self) {
        for node in 0..self.nodes.len() {
            let peers: Vec<usize> = self.nodes[node].peers.keys().copied().collect();
            for peer in peers {
                while self.nodes[node]
                    .peers
                    .get(&peer)
                    .is_some_and(PeerState::has_copies)
                    && self.data_idle_at(node, peer) <= self.now
                {
                    self.step_peer(node, peer, PeerInput::DataIdle);
                }
            }
        }
    }

    /// When `node`'s data link to `peer` finishes its queued writes.
    pub(super) fn data_idle_at(&self, node: usize, peer: usize) -> Instant {
        self.nodes[node]
            .data_ready_at
            .get(&peer)
            .copied()
            .unwrap_or(self.now)
    }

    /// The next instant a data link with waiting copies idles.
    fn next_copy_release(&self) -> Option<Instant> {
        (0..self.nodes.len())
            .flat_map(|node| {
                self.nodes[node]
                    .peers
                    .iter()
                    .filter(|(_, state)| state.has_copies())
                    .map(move |(&peer, _)| self.data_idle_at(node, peer))
            })
            .min()
    }

    /// Steps `node`'s session with `peer` and runs its outputs.
    pub(super) fn step_peer(&mut self, node: usize, peer: usize, input: PeerInput) {
        let Some(state) = self.nodes[node].peers.get_mut(&peer) else {
            return;
        };
        let step = state.step(self.now, input);
        state.assert_invariants();
        let session_gen = state.session_gen();
        self.apply_peer_step(node, (peer, session_gen), step);
    }

    /// Steps `node`'s table and runs its outputs.
    pub(super) fn step_table(&mut self, node: usize, input: BlockInput) {
        let step = self.nodes[node].table.step(self.now, input);
        self.nodes[node].table.assert_invariants();
        self.apply_block_step(node, step);
    }

    /// Parts receivers accepted under push slots, over every link
    /// (decision-24).
    pub(super) fn pushed_parts_accepted(&self) -> u32 {
        self.wire.push_accepted.values().sum()
    }

    /// Data-stream frames written over every link.
    pub(super) fn data_frames(&self) -> u32 {
        self.wire.data_frames.values().sum()
    }

    /// Indices `from` flagged for push to `to` (decision-24).
    pub(super) fn flagged(&self, from: usize, to: usize, block_id: &block::Hash) -> Vec<u32> {
        let mut flagged: Vec<u32> = self
            .wire
            .flagged
            .iter()
            .filter(|&&(f, t, b, _)| f == from && t == to && b == *block_id)
            .map(|&(.., index)| index)
            .collect();
        flagged.sort_unstable();
        flagged
    }

    /// Indices `from` announced to `to`, flagged or not.
    pub(super) fn announced(&self, from: usize, to: usize, block_id: &block::Hash) -> Vec<u32> {
        let mut announced: Vec<u32> = self
            .wire
            .announced
            .iter()
            .filter(|&&(f, t, b, _)| f == from && t == to && b == *block_id)
            .map(|&(.., index)| index)
            .collect();
        announced.sort_unstable();
        announced
    }

    /// Indices whose parts `from` sent to `to`.
    pub(super) fn sent_parts(&self, from: usize, to: usize, block_id: &block::Hash) -> Vec<u32> {
        let mut sent: Vec<u32> = self
            .wire
            .parts
            .iter()
            .filter(|&&(f, t, b, _)| f == from && t == to && b == *block_id)
            .map(|&(.., index)| index)
            .collect();
        sent.sort_unstable();
        sent
    }

    pub(super) fn verified(&self, node: usize, block_id: &block::Hash) -> bool {
        self.nodes[node].verified.contains_key(block_id)
    }

    /// Delivers frames and ticks every machine until `end`.
    pub(super) fn run_until(&mut self, end: Instant) {
        let mut next_tick = self.now + MESH_TICK;
        loop {
            self.release_copies();
            let next_frame = self.queue.peek().map(|r| r.0.deliver_at);
            let next_event = next_frame.into_iter().chain(self.next_copy_release()).min();
            match next_event {
                Some(at) if at <= next_tick && at <= end => {
                    self.now = self.now.max(at);
                    if next_frame == Some(at) {
                        let item = self.queue.pop().expect("peeked a frame").0;
                        self.deliver(item);
                    }
                }
                _ if next_tick > end => break,
                _ => {
                    self.now = next_tick;
                    next_tick += MESH_TICK;
                    for node in 0..self.nodes.len() {
                        let peers: Vec<usize> = self.nodes[node].peers.keys().copied().collect();
                        for peer in peers {
                            self.step_peer(node, peer, PeerInput::Tick);
                        }
                        if self.nodes[node].adversary.is_none() {
                            let peer_states = self.peer_states_for(node);
                            self.step_table(node, BlockInput::Tick { peer_states });
                        }
                    }
                }
            }
        }
    }

    /// Asserts that no session between honest nodes closed.
    pub(super) fn assert_honest_links_open(&self) {
        for node in self.nodes.iter().filter(|n| n.adversary.is_none()) {
            for (&peer, state) in &node.peers {
                if self.nodes[peer].adversary.is_none() {
                    assert!(
                        !state.is_closed(),
                        "honest link closed: {:?}",
                        state.close_cause()
                    );
                }
            }
        }
    }

    fn index_of(&self, node: usize, id: &NodeId) -> Option<usize> {
        self.nodes[node]
            .peers
            .keys()
            .copied()
            .find(|&p| self.nodes[p].id == *id)
    }

    fn peer_states_for(&self, node: usize) -> Vec<PullPeerState> {
        self.nodes[node]
            .peers
            .values()
            .map(|state| pull_state(state, self.now))
            .collect()
    }

    fn apply_peer_step(&mut self, node: usize, (peer, session_gen): (usize, u64), step: PeerStep) {
        for event in &step.events {
            match event {
                PeerEvent::PartPrefix { block_id, auth, .. } => {
                    self.check_accepted(node, peer, *block_id, *auth);
                }
                PeerEvent::CopiesQueued { count, .. } => {
                    let now = self.now;
                    self.nodes[node].copy_batches.push((now, peer, *count));
                }
                _ => {}
            }
        }
        let peer_id = self.nodes[peer].id;
        for output in step.outputs {
            let states = || self.peer_states_for(node);
            match route_peer_output(peer_id, session_gen, output, states) {
                PeerRoute::Table(input) => self.step_table(node, input),
                PeerRoute::Session(output) => self.session_output(node, peer, output),
            }
        }
    }

    fn apply_block_step(&mut self, node: usize, step: BlockStep) {
        let now = self.now;
        self.nodes[node]
            .block_events
            .extend(step.events.into_iter().map(|event| (now, event)));
        for output in step.outputs {
            match route_block_output(output) {
                BlockRoute::Peer {
                    peer,
                    session_gen,
                    input,
                } => {
                    if let PeerInput::PullGranted(grant) = &input {
                        self.nodes[node].grants.push((
                            now,
                            peer,
                            grant.block_id,
                            grant.stripe,
                            grant.requested,
                        ));
                    }
                    match self.index_of(node, &peer) {
                        Some(idx) => self.step_peer(node, idx, input),
                        None => {
                            let states = || self.peer_states_for(node);
                            if let Some(release) = refused_grant(peer, session_gen, input, states) {
                                self.step_table(node, release);
                            }
                        }
                    }
                }
                BlockRoute::Broadcast(broadcast) => {
                    let peers: Vec<usize> = self.nodes[node].peers.keys().copied().collect();
                    for idx in peers {
                        let input = broadcast.input_for(&self.nodes[idx].id);
                        self.step_peer(node, idx, input);
                    }
                }
                BlockRoute::Shell(output) => self.shell(node, output),
            }
        }
    }

    /// The node's shell work, completed at once: header admission, the
    /// unknown-parent checks, and commitment checks pass, stripe jobs run inline, and the verifier
    /// accepts.
    fn shell(&mut self, node: usize, output: BlockOutput) {
        let input = match output {
            BlockOutput::AdmitHeader { work_id, .. } => BlockInput::HeaderAdmitted {
                work_id,
                result: HeaderAdmissionResult::Admitted {
                    height: MESH_HEIGHT,
                    parent_hash: TEST_TIP_HASH,
                },
            },
            BlockOutput::CheckUnknownParent { work_id, .. } => BlockInput::UnknownParentChecked {
                work_id,
                verdict: UnknownParentVerdict::Park,
            },
            BlockOutput::VerifyCommitment { work_id, .. } => {
                let peers = self.nodes[node]
                    .peers
                    .keys()
                    .map(|&p| self.nodes[p].id)
                    .collect();
                self.seq += 1;
                BlockInput::CommitmentChecked {
                    work_id,
                    ok: true,
                    peers,
                    rng_seed: self.seq,
                }
            }
            BlockOutput::StartStripeJob {
                work_id,
                block_id,
                stripe,
                coding,
                enc_id,
                held,
                ..
            } => BlockInput::StripeJobDone {
                work_id,
                block_id,
                stripe,
                result: decode_and_check_stripe(&coding, &enc_id, stripe, &held).map_err(|_| ()),
            },
            BlockOutput::SubmitVerifier {
                work_id, block_id, ..
            } => {
                self.nodes[node].verified.insert(block_id, self.now);
                self.step_table(node, BlockInput::BlockAssembled { work_id, block_id });
                BlockInput::VerifierVerdict {
                    work_id,
                    block_id,
                    verdict: VerifierVerdict::Valid,
                }
            }
            BlockOutput::ClosePeer { peer, row, .. } => {
                panic!("mesh metadata is valid, but the table closed {peer:?} with {row:?}")
            }
            BlockOutput::ClaimBody { .. }
            | BlockOutput::ExtendBodyClaim { .. }
            | BlockOutput::ReleaseBodyClaim { .. } => return,
            routed => unreachable!("route_block_output keeps {routed:?} off the shell"),
        };
        self.step_table(node, input);
    }

    /// Runs one session output: writes go to the wire, serving reads the
    /// table, and proof work completes at once with the part that arrived.
    fn session_output(&mut self, node: usize, peer: usize, output: PeerOutput) {
        match output {
            PeerOutput::SendControl(msg) => {
                self.send(node, peer, MeshFrame::Control(*msg));
            }
            PeerOutput::SendHeaderMeta(msg) => {
                self.send(node, peer, MeshFrame::HeaderMeta(*msg));
            }
            PeerOutput::SendHave {
                block_id,
                flags,
                form,
                indices,
            } => {
                self.send(
                    node,
                    peer,
                    MeshFrame::Have {
                        block_id,
                        flags,
                        form,
                        indices,
                    },
                );
            }
            PeerOutput::PushPart { block_id, index } => {
                // bpv1-72: every pushed part follows its flag on the link.
                assert!(
                    self.wire.flagged.contains(&(node, peer, block_id, index)),
                    "bpv1-72 pushed part {index} without its flag"
                );
                assert_ne!(
                    self.envelopes[&block_id].proposer_node, self.nodes[peer].id,
                    "bpv1-71 pushed a part to the proposer"
                );
                let part = self.nodes[node]
                    .table
                    .get_part(&block_id, index)
                    .cloned()
                    .expect("bpv1-71 a holder pushes only a part it holds");
                self.send(node, peer, MeshFrame::Part { block_id, part });
            }
            PeerOutput::SendSeedPart { block_id, part, .. } => {
                // bpv1-72: every seed follows its flag on the link.
                assert!(
                    self.wire
                        .flagged
                        .contains(&(node, peer, block_id, part.index())),
                    "bpv1-72 seed {} without its flag",
                    part.index()
                );
                self.send(node, peer, MeshFrame::Part { block_id, part });
            }
            PeerOutput::SendWantEnd { want_id, sent, .. } => {
                self.send(node, peer, MeshFrame::WantEnd { want_id, sent });
                // The mesh hands each frame to its link at once, so it
                // reports the end written, as the data writer does.
                self.step_peer(node, peer, PeerInput::WantEndWritten);
            }
            PeerOutput::ServeWant {
                want_id,
                block_id,
                stripe,
                eligible_slots,
            } => {
                let parts = held_parts(&self.nodes[node].table, &block_id, stripe, &eligible_slots);
                // A `Want` names at most `n` slots of one stripe.
                let sent = parts.len() as u16;
                for part in parts {
                    let index = part.index();
                    if let Some(state) = self.nodes[node].peers.get_mut(&peer) {
                        let _ = state.step(
                            self.now,
                            PeerInput::ServePartQueued(ServePartQueued {
                                want_id,
                                block_id,
                                index,
                            }),
                        );
                    }
                    self.send(node, peer, MeshFrame::Part { block_id, part });
                }
                self.step_peer(
                    node,
                    peer,
                    PeerInput::ServeDone(ServeDone {
                        want_id,
                        sent,
                        reason: WantEndReason::Served,
                    }),
                );
            }
            PeerOutput::SubmitHeaderMeta { block_id, .. } => {
                if let Some(envelope) = self.envelopes.get(&block_id).cloned() {
                    let from_peer = self.nodes[peer].id;
                    self.step_table(
                        node,
                        BlockInput::MetaReceived {
                            envelope,
                            from_peer,
                            session_gen: 1,
                        },
                    );
                }
            }
            PeerOutput::VerifyPart { token, payload } => {
                let key = (node, peer, token.block_id, token.index);
                let Some(part) = self.wire.arrived.remove(&key) else {
                    return;
                };
                let from = self.nodes[peer].id;
                // usize -> u64 widens on every supported target.
                let bytes = payload.len() as u64;
                let input =
                    match charge_part(&mut self.nodes[node].table, self.now, from, token, bytes) {
                        Ok(_) => PeerInput::PartVerified { token, part },
                        Err(refused) => refused,
                    };
                self.step_peer(node, peer, input);
            }
            PeerOutput::CancelQueuedPush { .. }
            | PeerOutput::CancelQueuedStripe { .. }
            | PeerOutput::PurgeQueuedBlock { .. }
            | PeerOutput::PauseDataReads
            | PeerOutput::ResumeDataReads
            | PeerOutput::CloseProtocol(_)
            | PeerOutput::CloseNeutralStall => {}
            routed => unreachable!("route_peer_output sends {routed:?} to the table"),
        }
    }

    /// Checks the wire rules for an honest frame from `from` to `to`, then
    /// schedules it.
    fn send(&mut self, from: usize, to: usize, frame: MeshFrame) {
        let table = &self.nodes[from].table;
        match &frame {
            MeshFrame::HeaderMeta(Bpv1Message::HeaderMeta { part_root, .. }) => {
                let block_id = self.block_of_root(part_root);
                assert!(
                    self.wire.meta.insert((from, to, block_id)),
                    "bpv1-4 HeaderMeta sent twice on one link"
                );
            }
            MeshFrame::Control(Bpv1Message::Want {
                block_id,
                requested,
                ..
            }) => {
                *self
                    .wire
                    .requested
                    .entry((from, to, *block_id))
                    .or_default() += u32::from(requested.count());
            }
            MeshFrame::Control(Bpv1Message::StripeDone { block_id, stripe }) => {
                let (coding, _) = table
                    .admitted_descriptor(block_id)
                    .expect("bpv1-37 StripeDone for an admitted block");
                // A padding part is known zeros, so it counts as held.
                let held = (0..coding.n)
                    .filter_map(|slot| coding.wire_index(*stripe, slot))
                    .filter(|&index| {
                        coding.is_padding(index) || table.get_part(block_id, index).is_some()
                    })
                    .count();
                assert!(
                    held >= coding.k as usize,
                    "bpv1-37 StripeDone with {held} of k={} parts",
                    coding.k
                );
            }
            MeshFrame::Control(Bpv1Message::BlockDone { block_id }) => {
                // `Verifying` holds a canonically assembled block.
                assert!(
                    matches!(
                        table.block_phase(block_id),
                        Some(BlockPhase::Verifying | BlockPhase::Done)
                    ),
                    "bpv1-37 BlockDone before assembly"
                );
            }
            MeshFrame::Control(_) | MeshFrame::HeaderMeta(_) => {}
            MeshFrame::Part { block_id, part } => {
                assert!(
                    table
                        .get_part(block_id, part.index())
                        .is_some_and(|held| held.payload() == part.payload()),
                    "bpv1-19 forwarded a part the sender does not hold verified"
                );
                assert!(
                    self.wire.parts.insert((from, to, *block_id, part.index())),
                    "bpv1-17 part sent twice on one link"
                );
            }
            MeshFrame::Have {
                block_id,
                flags,
                indices,
                ..
            } => {
                assert!(
                    indices
                        .iter()
                        .all(|&index| table.get_part(block_id, index).is_some()),
                    "bpv1-24 Have for a part the sender does not hold"
                );
                for &index in indices {
                    assert!(
                        self.wire.announced.insert((from, to, *block_id, index)),
                        "bpv1-25 index {index} announced twice on one link"
                    );
                    if flags.push() {
                        self.wire.flagged.insert((from, to, *block_id, index));
                    }
                }
            }
            MeshFrame::WantEnd { want_id, .. } => {
                assert!(
                    self.wire.ends.insert((from, to, *want_id)),
                    "bpv1-29 WantEnd sent twice"
                );
            }
        }
        self.enqueue(from, to, frame);
    }

    /// bpv1-47: a peer's accepted parts stay within the slots this node asked
    /// it for, plus the parts it flagged for push.
    fn check_accepted(&mut self, node: usize, peer: usize, block_id: block::Hash, auth: PartAuth) {
        let key = (node, peer, block_id);
        match auth {
            PartAuth::Want(_) => {
                let accepted = self.wire.want_accepted.entry(key).or_default();
                *accepted += 1;
                let requested = self.wire.requested.get(&key).copied().unwrap_or(0);
                assert!(
                    *accepted <= requested,
                    "bpv1-47 accepted {accepted} Want parts of {requested} requested"
                );
            }
            PartAuth::Push | PartAuth::Seed => {
                if auth == PartAuth::Seed {
                    assert_eq!(
                        self.envelopes[&block_id].proposer_node, self.nodes[peer].id,
                        "bpv1-47 seed part from a non-proposer"
                    );
                }
                let flagged = self
                    .wire
                    .flagged
                    .iter()
                    .filter(|&&(from, to, b, _)| from == peer && to == node && b == block_id)
                    .count();
                let counts = if auth == PartAuth::Seed {
                    &mut self.wire.seed_accepted
                } else {
                    &mut self.wire.push_accepted
                };
                let accepted = counts.entry(key).or_default();
                *accepted += 1;
                assert!(
                    *accepted as usize <= flagged,
                    "bpv1-69 accepted {accepted} pushed parts beyond the {flagged} flagged"
                );
            }
        }
    }

    fn block_of_root(&self, part_root: &[u8; 32]) -> block::Hash {
        self.envelopes
            .iter()
            .find(|(_, e)| e.part_root == *part_root)
            .map(|(id, _)| *id)
            .expect("mesh HeaderMeta belongs to a proposed block")
    }

    fn enqueue(&mut self, from: usize, to: usize, frame: MeshFrame) {
        let Some(link) = self.links.get(&(from, to)).copied() else {
            return;
        };
        let is_ctrl = matches!(frame, MeshFrame::Control(_));
        if !is_ctrl {
            // The data-frame limit counts block traffic only. A `HeaderMeta`
            // still goes silent with its stream.
            let carried = self.wire.data_frames.entry((from, to)).or_default();
            if !matches!(frame, MeshFrame::HeaderMeta(_)) {
                *carried += 1;
            }
            if link.data_frames.is_some_and(|limit| *carried > limit) {
                return;
            }
        }
        if link.silent {
            return;
        }
        let bytes: u64 = match &frame {
            MeshFrame::Control(_) | MeshFrame::HeaderMeta(_) => 64,
            MeshFrame::Part { part, .. } => part.payload().len() as u64,
            // Widening usize to u64 is lossless on supported targets.
            MeshFrame::Have { form, indices, .. } => 33 + form.indices_len(indices.len()) as u64,
            MeshFrame::WantEnd { .. } => 7,
        };
        let tx_ns = u128::from(bytes)
            .saturating_mul(1_000_000_000)
            .div_ceil(u128::from(link.rate_bps.max(1)));
        let tx = Duration::from_nanos(u64::try_from(tx_ns).unwrap_or(u64::MAX));
        let (ready, delay) = if is_ctrl {
            (&mut self.nodes[from].ctrl_ready_at, link.ctrl_delay)
        } else {
            (&mut self.nodes[from].data_ready_at, link.data_delay)
        };
        let start = ready.get(&to).copied().unwrap_or(self.now).max(self.now);
        ready.insert(to, start + tx);
        self.seq += 1;
        self.queue.push(Reverse(ScheduledDelivery {
            deliver_at: start + tx + delay,
            seq: self.seq,
            from,
            to,
            frame,
        }));
    }

    fn deliver(&mut self, item: ScheduledDelivery) {
        let ScheduledDelivery {
            from, to, frame, ..
        } = item;
        if let Some(adversary) = self.nodes[to].adversary {
            self.adversary_receives(to, from, adversary, frame);
            return;
        }
        let input = match frame {
            MeshFrame::HeaderMeta(msg @ Bpv1Message::HeaderMeta { part_root, .. }) => {
                PeerInput::RecvHeaderMeta {
                    block_id: self.block_of_root(&part_root),
                    message: Box::new(msg),
                }
            }
            MeshFrame::Control(Bpv1Message::Want {
                want_id,
                block_id,
                stripe,
                requested,
            }) => PeerInput::RecvWant(RecvWant {
                want_id,
                block_id,
                stripe,
                requested,
            }),
            MeshFrame::Control(Bpv1Message::StripeDone { block_id, stripe }) => {
                PeerInput::RecvStripeDone { block_id, stripe }
            }
            MeshFrame::Control(Bpv1Message::BlockDone { block_id }) => {
                PeerInput::RecvBlockDone { block_id }
            }
            MeshFrame::Control(other) | MeshFrame::HeaderMeta(other) => {
                unreachable!("{other:?} travels on the other stream")
            }
            MeshFrame::Part { block_id, part } => {
                let index = part.index();
                let payload = encode_part_frame(block_id, &part).payload;
                self.wire.arrived.insert((to, from, block_id, index), part);
                PeerInput::RecvPartPrefix(RecvPartPrefix {
                    block_id,
                    index,
                    payload,
                })
            }
            MeshFrame::Have {
                block_id,
                flags,
                form,
                indices,
            } => PeerInput::RecvHave(RecvHave {
                block_id,
                flags,
                form,
                frame_bytes: 33 + form.indices_len(indices.len()),
                indices,
            }),
            MeshFrame::WantEnd { want_id, sent, .. } => {
                PeerInput::RecvWantEnd(RecvWantEnd { want_id, sent })
            }
        };
        self.step_peer(to, from, input);
    }

    /// A scripted adversary answers `from`'s frame. Its frames skip the wire
    /// checks, which hold only for honest senders.
    fn adversary_receives(
        &mut self,
        adversary: usize,
        from: usize,
        script: Adversary,
        frame: MeshFrame,
    ) {
        match frame {
            MeshFrame::HeaderMeta(Bpv1Message::HeaderMeta { part_root, .. }) => {
                let block_id = self.block_of_root(&part_root);
                let total = self.envelopes[&block_id].coding.total_parts().unwrap_or(0);
                let indices: Vec<u32> = (0..total).collect();
                self.enqueue(
                    adversary,
                    from,
                    MeshFrame::Have {
                        block_id,
                        flags: HaveFlags::Plain,
                        form: HaveForm::smaller(total, indices.len()),
                        indices,
                    },
                );
            }
            MeshFrame::Control(Bpv1Message::Want { want_id, .. })
                if script == Adversary::LiesThenEmpty =>
            {
                self.enqueue(adversary, from, MeshFrame::WantEnd { want_id, sent: 0 });
            }
            _ => {}
        }
    }
}
