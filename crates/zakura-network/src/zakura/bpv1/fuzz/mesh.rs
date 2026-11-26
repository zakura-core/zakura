//! The `bpv1_mesh` fuzz target: four nodes of production BPv1 machines,
//! with fuzz-chosen delivery order and an adversary (`tool-2`, `tool-4`).
//!
//! Nodes 0–2 are honest. Each runs a production [`BlockTable`] (with its
//! [`PeerPaths`](crate::zakura::bpv1::paths::PeerPaths)) and one
//! [`PeerState`] per connection; node 0 also runs the [`ProposerState`] and
//! proposes up to three post-NU7 regtest blocks. Node 3 is the adversary: it
//! runs no machine, records every frame it receives, and sends crafted,
//! replayed, mutated or arbitrary frames.
//!
//! The driver is a model of the IO shell that keeps no protocol state of its
//! own. Machine outputs go through the production router
//! (`route_peer_output`, `route_block_output`, `refused_grant`,
//! `charge_part`, `held_parts`). Control messages cross the links as frames
//! from `encode_frame`, data as frames from the production
//! [`Bpv1FifoQueue`] writer, and receivers read them with the production
//! stream decoders. Shell work waits in a per-node job list until the input
//! completes it, in any order:
//! - header admission answers from the proposer's chain: `Admitted` at the
//!   block's height once the node holds its parent, `MissingParent` before,
//!   `PeerInvalid` for a header no one solved, or a local error;
//! - unknown-parent checks park an envelope whose commitment holds and close
//!   any other with row 4 or row 3;
//! - commitment checks run the production check on regtest with NU7 active;
//! - stripe jobs run the production decoder, or fail as a canceled job;
//! - verification runs the production assembler; the verdict is `Valid`, a
//!   root mismatch for a body that does not assemble, or local capacity;
//! - part proofs run the production frame proof; a result whose session
//!   ended releases its charge, as the shell's worker does;
//! - the proposer's encode runs the production encoder.
//!
//! A node commits a block when its verifier accepts it, when block sync
//! ends its fallback, or (the proposer) when it solves it. Committing steps
//! `TipHeight` and `ParentAdmitted`. Block sync may also carry every honest
//! node's tip far past the mesh's blocks, as a node catching up does. The
//! tables then retire the mesh's blocks and, past twice the propagation
//! window, evict their tombstones (`EvictRetired`) while frames for them
//! are still in flight.
//!
//! Checks after every step: every stepped session's and table's
//! `assert_invariants`; no honest connection closes with a protocol row,
//! from either side; a peer's `HeaderMeta` never readmits a retired block;
//! every body claim release follows its claim; an assembled block is the
//! proposer's block. Every session that ends drains (`assert_drained`).
//!
//! The driver never sends `SessionEnd` other than when a connection ends.
//! It sends every other [`PeerInput`] and [`BlockInput`] as the shell does,
//! so `PartFailed`, `PartRefused`, `MetaRejected` and the rest arise from
//! the adversary and the delivery order rather than from the driver.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use zakura_chain::{
    block::{self, Header, Height},
    parameters::Network,
    serialization::{ZcashDeserialize, ZcashSerialize},
    transaction::Transaction,
};
use zakura_quic::NodeId;
use zakura_rt::time::Instant;

use super::{
    chain::{block_of, build_v6_tx, committed_coinbase, regtest_nu7},
    fuzz_node,
    quality::{hit_block_step, hit_peer_step, proposer_input_name, Reached},
    FuzzBytes,
};
use crate::zakura::{
    bpv1::{
        assemble::assemble_block,
        blocks::{
            BlockConfig, BlockInput, BlockOutput, BlockPhase, BlockTable, HeaderAdmissionResult,
            MetaEnvelope, PullPeerState, VerifierVerdict, WorkId,
        },
        codec::{decode_and_check_stripe, encode_body, EncodedBody},
        commitment::coinbase_paths,
        paths::PathEstimate,
        peer::{
            PartToken, PeerConfig, PeerInput, PeerOutput, PeerState, Seed, Seeds, ServeDone,
            ServePartQueued,
        },
        profile::{Coding, Profile},
        proposer::{
            compute_body_identity, PreparedCommitment, ProposerConfig, ProposerInput,
            ProposerOutput, ProposerState, TemplateId,
        },
        route::{
            admitted_input, charge_part, held_parts, pull_state, refused_grant, route_block_output,
            route_peer_output, BlockRoute, PeerRoute,
        },
        service::{
            control_input, data_input, parse_and_verify_part_frame, verify_envelope_commitment,
        },
        unknown_parent::UnknownParentVerdict,
        wire::{
            encode_part_frame, message_type, Bpv1FifoQueue, Bpv1Message, DataQueueItem, HaveFlags,
            WantBitmap, WantEndReason, PROPAGATION_DEPTH,
        },
    },
    wire_codec::encode_frame,
    Frame,
};

/// Summary returned by [`replay_bpv1_mesh_bytes`].
#[derive(Clone, Debug, Default)]
pub struct MeshReplaySummary {
    /// Machine steps taken.
    pub steps: usize,
    /// Blocks the proposer solved.
    pub solved: usize,
    /// Honest nodes that committed the last solved block.
    pub committed: usize,
    /// Every input, output, close row and phase the replay reached.
    pub reached: Reached,
}

/// The fuzz property the mesh checks name.
const MESH: &str = "bpv1/mesh";

/// Nodes in the mesh: three honest nodes, then the adversary.
const NODES: usize = 4;
/// The proposer.
const PROPOSER: usize = 0;
/// The adversary.
const ADVERSARY: usize = 3;
/// Blocks the proposer may solve.
const BLOCKS: usize = 3;
/// Frames the adversary remembers for replay.
const INBOX: usize = 64;
/// Inputs a replay reads at most, which bounds the cost of one input.
const MAX_OPS: usize = 400;

/// Operations the mesh reads; `tag % OPS` picks one. Delivery and writing
/// take several slots each, so an arbitrary input mostly moves frames.
const OPS: u8 = 25;

/// A block the proposer may build: its transactions after the coinbase.
struct Planned {
    height: u32,
    light_client_root: [u8; 32],
    txs: Vec<Arc<Transaction>>,
    body: Vec<u8>,
    tx_count: u32,
    commitment: Option<PreparedCommitment>,
    solved: bool,
}

/// A solved block, as the chain and the mesh's honest shells know it.
struct Solved {
    id: block::Hash,
    height: u32,
    raw: Vec<u8>,
    encoded: EncodedBody,
}

/// One session and its data writer.
struct Session {
    state: PeerState,
    writer: Bpv1FifoQueue,
}

/// Shell work a node's machines started and the input completes.
enum Job {
    Header {
        work_id: WorkId,
        block_id: block::Hash,
    },
    UnknownParent {
        work_id: WorkId,
        envelope: Box<MetaEnvelope>,
    },
    Commitment {
        work_id: WorkId,
        height: u32,
        envelope: Box<MetaEnvelope>,
    },
    Stripe {
        work_id: WorkId,
        block_id: block::Hash,
        stripe: u32,
        coding: Coding,
        enc_id: [u8; 32],
        held: Vec<Option<crate::zakura::bpv1::tree::VerifiedPart>>,
    },
    Verify {
        work_id: WorkId,
        block_id: block::Hash,
        header_bytes: Vec<u8>,
        coinbase_bytes: Vec<u8>,
        coding: Coding,
        stripe_sources: Vec<Vec<Arc<[u8]>>>,
    },
    Proof {
        peer: usize,
        session_gen: u64,
        token: PartToken,
        payload: Vec<u8>,
        coding: Coding,
        part_root: [u8; 32],
    },
    Encode {
        work_id: WorkId,
        block: usize,
    },
}

struct Node {
    id: NodeId,
    table: BlockTable,
    sessions: BTreeMap<usize, Session>,
    jobs: Vec<Job>,
    /// Blocks holding a body claim.
    claimed: Vec<block::Hash>,
    /// Blocks the table retired.
    retired: Vec<block::Hash>,
    /// The verified tip height.
    tip: u32,
}

/// The frames in flight from one node to another, per stream.
#[derive(Default)]
struct Link {
    control: VecDeque<Frame>,
    data: VecDeque<Frame>,
}

struct Mesh {
    profile: Profile,
    network: Network,
    chain_id: [u8; 32],
    now: Instant,
    nodes: Vec<Node>,
    proposer: ProposerState,
    links: BTreeMap<(usize, usize), Link>,
    planned: Vec<Planned>,
    solved: Vec<Solved>,
    inbox: VecDeque<Frame>,
    /// The planned block whose template the proposer is building.
    templating: usize,
    next_gen: u64,
    summary: MeshReplaySummary,
}

/// Replays `bytes` as a four-node history and asserts the checks above.
///
/// The first byte picks the profile (bit 0 set: `W2`, else `TINY`) and how
/// many transactions follow each block's coinbase (bits 1–2, 3–4 and 5–6,
/// zero to three). Bit 7 stresses the mesh: the proposer queues direct
/// copies (deal-3), and node 2's part buffer holds one part, so it refuses
/// parts before proof. Every following three bytes are one operation.
pub fn replay_bpv1_mesh_bytes(bytes: &[u8]) -> MeshReplaySummary {
    let mut input = FuzzBytes::new(bytes);
    let head = input.byte();
    let mut mesh = Mesh::new(head);
    let mut ops = 0;
    while !input.is_empty() && ops < MAX_OPS {
        let (tag, a, b) = (input.byte(), input.byte(), input.byte());
        mesh.op(tag % OPS, a, b, &mut input);
        ops += 1;
    }
    mesh.finish()
}

impl Mesh {
    fn new(head: u8) -> Self {
        let profile = if head & 1 != 0 {
            Profile::W2
        } else {
            Profile::TINY
        };
        let network = regtest_nu7();
        let chain_id = network.genesis_hash().0;
        let stress = head & 0x80 != 0;
        let nodes = (0..NODES)
            .map(|i| {
                let mut config = BlockConfig::for_profile(profile);
                if stress && i == 2 {
                    config.part_buffer_bytes = u64::from(profile.s_max);
                }
                (i, config)
            })
            .map(|(i, config)| Node {
                // `i` is below `NODES`, so the cast keeps it.
                id: fuzz_node(0x10 + i as u8),
                table: BlockTable::new(config),
                sessions: BTreeMap::new(),
                jobs: Vec::new(),
                claimed: Vec::new(),
                retired: Vec::new(),
                tip: 0,
            })
            .collect::<Vec<_>>();
        let proposer = ProposerState::new(ProposerConfig {
            direct_copies: stress,
            ..ProposerConfig::new(chain_id, nodes[PROPOSER].id)
        });
        let planned = (0..BLOCKS)
            .map(|i| {
                // `i` is below `BLOCKS`, so the casts keep it.
                let height = i as u32 + 1;
                let count = usize::from(head >> (1 + 2 * i) & 3);
                let txs: Vec<Arc<Transaction>> = (0..count)
                    .map(|t| Arc::new(build_v6_tx(Height(height), (16 * i + t) as u8 + 1)))
                    .collect();
                let mut body = Vec::new();
                for tx in &txs {
                    tx.zcash_serialize(&mut body)
                        .expect("a v6 transaction serializes");
                }
                Planned {
                    height,
                    light_client_root: [0x90 + i as u8; 32],
                    tx_count: count as u32 + 1,
                    txs,
                    body,
                    commitment: None,
                    solved: false,
                }
            })
            .collect();
        let mut mesh = Self {
            profile,
            network,
            chain_id,
            now: Instant::now(),
            nodes,
            proposer,
            links: BTreeMap::new(),
            planned,
            solved: Vec::new(),
            inbox: VecDeque::new(),
            templating: 0,
            next_gen: 1,
            summary: MeshReplaySummary::default(),
        };
        // The shell's first frontier sets each table's verified tip: the
        // regtest genesis block.
        let genesis = block::Hash(chain_id);
        for node in (0..NODES).filter(|&n| Self::honest(n)) {
            mesh.step_table(
                node,
                BlockInput::TipHeight {
                    height: 0,
                    hash: genesis,
                },
            );
        }
        for i in 0..NODES {
            for j in i + 1..NODES {
                mesh.connect(i, j);
            }
        }
        mesh
    }

    /// The hash of the block at `height - 1` in the mesh's chain: the
    /// regtest genesis block below height 1, else the solved block there.
    /// A parent the proposer has not solved has a hash no block carries.
    fn parent_of(&self, height: u32) -> block::Hash {
        if height <= 1 {
            return block::Hash(self.chain_id);
        }
        self.solved
            .iter()
            .find(|p| p.height + 1 == height)
            .map_or(block::Hash([0; 32]), |p| p.id)
    }

    fn honest(node: usize) -> bool {
        node != ADVERSARY
    }

    fn index_of(&self, id: &NodeId) -> Option<usize> {
        self.nodes.iter().position(|n| n.id == *id)
    }

    fn peer_states(&self, node: usize) -> Vec<PullPeerState> {
        self.nodes[node]
            .sessions
            .values()
            .map(|s| pull_state(&s.state, self.now))
            .collect()
    }

    // ---------------------------------------------------------------------
    // Operations
    // ---------------------------------------------------------------------

    fn op(&mut self, tag: u8, a: u8, b: u8, input: &mut FuzzBytes<'_>) {
        match tag {
            0..=3 => self.deliver(a, false),
            4..=7 => self.deliver(a, true),
            8..=10 => self.write(a),
            11 | 12 => self.complete(a, b, false),
            13 => self.complete(a, b, true),
            14 => self.template(a),
            15 => self.solve(a),
            16 => self.tick(Duration::from_millis(u64::from(b) * 50)),
            17 => self.tick(Duration::from_secs(u64::from(b))),
            18 => self.observe_path(a, b),
            19 => self.disconnect(a),
            20 => self.reconnect(a),
            21 => match b % 3 {
                0 => self.control_stalled(a),
                1 => self.fallback_done(a),
                _ => self.sync_past(b),
            },
            22 => self.adversary_crafted(a, b),
            23 => self.adversary_raw(a, b, input),
            _ => self.commit_failed(a),
        }
    }

    /// Delivers the head frame of one link's control or data stream.
    fn deliver(&mut self, a: u8, data: bool) {
        let busy: Vec<(usize, usize)> = self
            .links
            .iter()
            .filter(|(_, link)| {
                if data {
                    !link.data.is_empty()
                } else {
                    !link.control.is_empty()
                }
            })
            .map(|(&key, _)| key)
            .collect();
        let Some(&(from, to)) = busy.get(usize::from(a) % busy.len().max(1)) else {
            return;
        };
        if data
            && self.nodes[to]
                .sessions
                .get(&from)
                .is_some_and(|s| s.state.is_data_paused())
        {
            return;
        }
        let link = self.links.get_mut(&(from, to)).expect("busy links exist");
        let frame = if data {
            link.data.pop_front()
        } else {
            link.control.pop_front()
        }
        .expect("busy links hold a frame");
        if to == ADVERSARY {
            if self.inbox.len() >= INBOX {
                self.inbox.pop_front();
            }
            self.inbox.push_back(frame);
            return;
        }
        let decoded = if data {
            data_input(frame)
        } else {
            control_input(&frame)
        };
        match decoded {
            Ok(input) => self.step_peer(to, from, input),
            Err(row) => {
                assert_eq!(
                    from, ADVERSARY,
                    "{MESH}: bpv1-61 node {to} read a frame from honest node {from} that breaks {row:?}"
                );
                self.summary.reached.hit(row.label());
                self.end_connection(to, from);
            }
        }
    }

    /// Writes the head of one session's data queue to its link, as the data
    /// writer does, or tells an empty writer's session its link is idle.
    fn write(&mut self, a: u8) {
        let candidates: Vec<(usize, usize)> = (0..NODES)
            .filter(|&n| Self::honest(n))
            .flat_map(|n| {
                self.nodes[n]
                    .sessions
                    .iter()
                    .filter(|(_, s)| !s.writer.is_empty() || s.state.has_copies())
                    .map(move |(&p, _)| (n, p))
                    .collect::<Vec<_>>()
            })
            .collect();
        let Some(&(node, peer)) = candidates.get(usize::from(a) % candidates.len().max(1)) else {
            return;
        };
        let session = self.nodes[node]
            .sessions
            .get_mut(&peer)
            .expect("candidates have sessions");
        let Some(item) = session.writer.pop_front() else {
            // The proposer's direct copies keep an idle upload busy.
            self.step_peer(node, peer, PeerInput::DataIdle);
            return;
        };
        let Some(frame) = item.encode() else {
            return;
        };
        // usize -> u64 widens on every supported target.
        let bytes = frame.payload.len() as u64;
        self.links
            .entry((node, peer))
            .or_default()
            .data
            .push_back(frame);
        match item {
            DataQueueItem::Part { block_id, .. } => {
                self.step_table(node, BlockInput::PartUploaded { block_id, bytes });
            }
            DataQueueItem::WantEnd { .. } => self.step_peer(node, peer, PeerInput::WantEndWritten),
            DataQueueItem::Have { .. } => {}
        }
    }

    /// Completes one node's shell job. With `local_failure`, the job fails
    /// as a busy or restarting node's does.
    fn complete(&mut self, a: u8, b: u8, local_failure: bool) {
        let node = usize::from(a) % ADVERSARY;
        let jobs = &mut self.nodes[node].jobs;
        if jobs.is_empty() {
            return;
        }
        let job = jobs.remove(usize::from(b) % jobs.len());
        match job {
            Job::Header { work_id, block_id } => {
                let result = match self.solved.iter().find(|s| s.id == block_id) {
                    _ if local_failure => HeaderAdmissionResult::LocalError,
                    None => HeaderAdmissionResult::PeerInvalid,
                    Some(s) if self.nodes[node].tip + 1 >= s.height => {
                        HeaderAdmissionResult::Admitted {
                            height: s.height,
                            parent_hash: self.parent_of(s.height),
                        }
                    }
                    Some(s) => HeaderAdmissionResult::MissingParent {
                        parent_hash: self.parent_of(s.height),
                    },
                };
                self.step_table(node, BlockInput::HeaderAdmitted { work_id, result });
            }
            Job::UnknownParent { work_id, envelope } => {
                let verdict = match self.solved.iter().find(|s| s.id == envelope.block_id) {
                    None => UnknownParentVerdict::Invalid(
                        crate::zakura::bpv1::close::Bpv1CloseRow::Row3InvalidHeader,
                    ),
                    Some(s) if self.commitment_holds(s.height, &envelope) => {
                        UnknownParentVerdict::Park
                    }
                    Some(_) => UnknownParentVerdict::Invalid(
                        crate::zakura::bpv1::close::Bpv1CloseRow::Row4BadMetaOrCommitment,
                    ),
                };
                self.step_table(node, BlockInput::UnknownParentChecked { work_id, verdict });
            }
            Job::Commitment {
                work_id,
                height,
                envelope,
            } => {
                let ok = self.commitment_holds(height, &envelope);
                let peers = self.nodes[node]
                    .sessions
                    .values()
                    .map(|s| s.state.remote_node())
                    .collect();
                let rng_seed = u64::from(b);
                self.step_table(
                    node,
                    BlockInput::CommitmentChecked {
                        work_id,
                        ok,
                        peers,
                        rng_seed,
                    },
                );
            }
            Job::Stripe {
                work_id,
                block_id,
                stripe,
                coding,
                enc_id,
                held,
            } => {
                let result = if local_failure {
                    Err(())
                } else {
                    decode_and_check_stripe(&coding, &enc_id, stripe, &held).map_err(|_| ())
                };
                self.step_table(
                    node,
                    BlockInput::StripeJobDone {
                        work_id,
                        block_id,
                        stripe,
                        result,
                    },
                );
            }
            Job::Verify {
                work_id,
                block_id,
                header_bytes,
                coinbase_bytes,
                coding,
                stripe_sources,
            } => self.verify(
                node,
                work_id,
                block_id,
                &header_bytes,
                &coinbase_bytes,
                &coding,
                &stripe_sources,
                local_failure,
            ),
            Job::Proof {
                peer,
                session_gen,
                token,
                payload,
                coding,
                part_root,
            } => {
                let verified =
                    parse_and_verify_part_frame(&payload, &coding, part_root, token.index);
                let live = self.nodes[node]
                    .sessions
                    .get(&peer)
                    .is_some_and(|s| s.state.session_gen() == session_gen);
                let release = !live || verified.is_none();
                if live {
                    let input = match verified {
                        Some(part) => PeerInput::PartVerified { token, part },
                        None => PeerInput::PartFailed { token },
                    };
                    self.step_peer(node, peer, input);
                }
                if release {
                    let from_peer = self.nodes[peer].id;
                    self.step_table(
                        node,
                        BlockInput::ReleasePartCharge {
                            block_id: token.block_id,
                            index: token.index,
                            from_peer,
                            session_gen,
                        },
                    );
                }
            }
            Job::Encode { work_id, block } => {
                let planned = &self.planned[block];
                let encoded = encode_body(&self.profile, &planned.body, planned.tx_count)
                    .expect("a planned body fits both profiles");
                self.step_proposer(ProposerInput::EncodeDone { work_id, encoded });
            }
        }
    }

    /// Runs the production assembler on a submitted body and answers the
    /// verifier's verdict.
    #[allow(clippy::too_many_arguments)]
    fn verify(
        &mut self,
        node: usize,
        work_id: WorkId,
        block_id: block::Hash,
        header_bytes: &[u8],
        coinbase_bytes: &[u8],
        coding: &Coding,
        stripe_sources: &[Vec<Arc<[u8]>>],
        local_failure: bool,
    ) {
        let assembled = Header::zcash_deserialize(header_bytes)
            .ok()
            .and_then(|header| {
                assemble_block(
                    &self.profile,
                    &header,
                    coding,
                    coinbase_bytes,
                    stripe_sources,
                )
                .ok()
            });
        let verdict = match assembled {
            Some((raw, _)) => {
                let solved = self
                    .solved
                    .iter()
                    .find(|s| s.id == block_id)
                    .unwrap_or_else(|| {
                        panic!("{MESH}: node {node} assembled unsolved block {block_id:?}")
                    });
                assert!(
                    raw == solved.raw,
                    "{MESH}: bpv1-42 node {node} assembled another body for {block_id:?}"
                );
                self.step_table(node, BlockInput::BlockAssembled { work_id, block_id });
                if local_failure {
                    VerifierVerdict::LocalCapacity
                } else {
                    VerifierVerdict::Valid
                }
            }
            None => VerifierVerdict::RootMismatch,
        };
        self.step_table(
            node,
            BlockInput::VerifierVerdict {
                work_id,
                block_id,
                verdict,
            },
        );
        if verdict == VerifierVerdict::Valid {
            self.commit(node, block_id);
        }
    }

    fn commitment_holds(&self, height: u32, envelope: &MetaEnvelope) -> bool {
        verify_envelope_commitment(&self.network, Height(height), self.chain_id, envelope)
    }

    /// Builds the template of the planned block `a` picks.
    fn template(&mut self, a: u8) {
        let block = usize::from(a) % BLOCKS;
        let planned = &self.planned[block];
        if planned.solved {
            return;
        }
        let input = ProposerInput::TemplateBuilt {
            // `block` is below `BLOCKS`, so the cast keeps it.
            template_id: TemplateId(block as u64 + 1),
            body_identity: compute_body_identity(planned.tx_count, &planned.body),
            light_client_root: planned.light_client_root,
        };
        self.templating = block;
        self.step_proposer(input);
    }

    /// Solves the planned block `a` picks, once its template has a
    /// commitment, with a coinbase that carries it.
    /// The proposer's own commit of solved block `a` fails.
    fn commit_failed(&mut self, a: u8) {
        let Some(solved) = self.solved.get(usize::from(a) % self.solved.len().max(1)) else {
            return;
        };
        let block_id = solved.id;
        self.step_table(PROPOSER, BlockInput::ProposedCommitFailed { block_id });
    }

    fn solve(&mut self, a: u8) {
        let at = usize::from(a) % BLOCKS;
        let planned = &self.planned[at];
        let Some(commitment) = planned.commitment.clone() else {
            return;
        };
        if planned.solved {
            return;
        }
        let coinbase = Arc::new(committed_coinbase(
            Height(planned.height),
            &commitment.script,
        ));
        let mut txs = vec![coinbase];
        txs.extend(planned.txs.iter().cloned());
        let block = block_of(planned.light_client_root, txs);
        let paths = coinbase_paths(&block).expect("a solved block has a coinbase");
        let id = block.hash();
        let height = planned.height;
        let body_identity = compute_body_identity(planned.tx_count, &planned.body);
        let encoded = encode_body(&self.profile, &planned.body, planned.tx_count)
            .expect("a planned body fits both profiles");
        self.planned[at].solved = true;
        self.solved.push(Solved {
            id,
            height,
            raw: block
                .zcash_serialize_to_vec()
                .expect("a solved block serializes"),
            encoded,
        });
        self.summary.solved += 1;
        let peers = self.nodes[PROPOSER]
            .sessions
            .values()
            .map(|s| s.state.remote_node())
            .collect();
        let input = ProposerInput::Solved {
            block_id: id,
            height,
            header_bytes: block
                .header
                .zcash_serialize_to_vec()
                .expect("a header serializes"),
            coinbase_bytes: paths.coinbase,
            txid_siblings: paths.txid_siblings,
            auth_siblings: paths.auth_siblings,
            body_identity,
            peers,
            paths: Box::new(self.nodes[PROPOSER].table.paths().clone()),
        };
        self.step_proposer(input);
        self.commit(PROPOSER, id);
    }

    fn tick(&mut self, by: Duration) {
        self.now += by;
        for node in (0..NODES).filter(|&n| Self::honest(n)) {
            let peer_states = self.peer_states(node);
            self.step_table(node, BlockInput::Tick { peer_states });
            let peers: Vec<usize> = self.nodes[node].sessions.keys().copied().collect();
            for peer in peers {
                self.step_peer(node, peer, PeerInput::Tick);
            }
        }
    }

    /// Feeds one path estimate of a node's peer into its table's paths.
    fn observe_path(&mut self, a: u8, b: u8) {
        let node = usize::from(a) % ADVERSARY;
        let peer = usize::from(a >> 2) % NODES;
        if peer == node {
            return;
        }
        let estimate = PathEstimate {
            min_rtt: (b & 1 == 0).then(|| Duration::from_millis(u64::from(b) + 1)),
            rate_in: (b & 2 == 0).then(|| (u64::from(a) + 1) * 40_000),
            rate_out: (b & 4 == 0).then(|| (u64::from(b) + 1) * 40_000),
        };
        let peer = self.nodes[peer].id;
        self.step_table(node, BlockInput::PathObserved { peer, estimate });
    }

    /// Ends the connection `a` picks.
    fn disconnect(&mut self, a: u8) {
        let live: Vec<(usize, usize)> = self.connections(true);
        if let Some(&(i, j)) = live.get(usize::from(a) % live.len().max(1)) {
            self.end_connection(i, j);
        }
    }

    /// Reconnects the ended connection `a` picks.
    fn reconnect(&mut self, a: u8) {
        let ended: Vec<(usize, usize)> = self.connections(false);
        if let Some(&(i, j)) = ended.get(usize::from(a) % ended.len().max(1)) {
            self.connect(i, j);
        }
    }

    /// The node pairs whose connection is live, or ended.
    fn connections(&self, live: bool) -> Vec<(usize, usize)> {
        (0..NODES)
            .flat_map(|i| (i + 1..NODES).map(move |j| (i, j)))
            .filter(|&(i, j)| {
                let up = self.nodes[i].sessions.contains_key(&j)
                    || self.nodes[j].sessions.contains_key(&i);
                up == live
            })
            .collect()
    }

    /// A session whose control frames wait unread reports its stall.
    fn control_stalled(&mut self, a: u8) {
        let stalled: Vec<(usize, usize)> = self
            .links
            .iter()
            .filter(|(&(from, _), link)| Self::honest(from) && !link.control.is_empty())
            .map(|(&key, _)| key)
            .collect();
        if let Some(&(from, to)) = stalled.get(usize::from(a) % stalled.len().max(1)) {
            self.step_peer(from, to, PeerInput::ControlStalled);
        }
    }

    /// The adversary sends a well-formed message built from what it knows:
    /// a `Want`, a `Have`, an unasked part, a `WantEnd`, a replay of a frame
    /// it received, such a replay with one byte changed, a part whose last
    /// payload byte is forged, or a `BlockDone`.
    fn adversary_crafted(&mut self, a: u8, b: u8) {
        let to = usize::from(a) % ADVERSARY;
        // `b >> 6` picks the block; `b & 0x3F` the indices or the part.
        let solved = self
            .solved
            .get(usize::from(b >> 6) % self.solved.len().max(1));
        let mask = b & 0x3F;
        let block_id = solved.map_or(block::Hash([0xEE; 32]), |s| s.id);
        let (control, frame) = match a >> 2 & 7 {
            0 => {
                let mut requested = WantBitmap::EMPTY;
                for slot in (0..8u32).filter(|i| mask & (1 << i) != 0) {
                    requested.set(slot);
                }
                let message = Bpv1Message::Want {
                    want_id: u32::from(a >> 5) + 1,
                    block_id,
                    stripe: u32::from(a >> 7),
                    requested,
                };
                (true, encode_frame(&message).ok())
            }
            1 => {
                let flags =
                    [HaveFlags::Plain, HaveFlags::Push, HaveFlags::Relay][usize::from(a >> 5) % 3];
                let indices = (0..8u32).filter(|i| mask & (1 << i) != 0).collect();
                let message = Bpv1Message::Have {
                    block_id,
                    flags,
                    indices,
                };
                (false, encode_frame(&message).ok())
            }
            2 => {
                let part = solved.and_then(|s| {
                    s.encoded
                        .parts
                        .get(usize::from(mask) % s.encoded.parts.len().max(1))
                });
                (false, part.map(|p| encode_part_frame(block_id, p)))
            }
            3 => {
                let message = Bpv1Message::WantEnd {
                    want_id: u32::from(b & 7) + 1,
                    sent: u16::from(b >> 3 & 7),
                    reason: [
                        WantEndReason::Served,
                        WantEndReason::UnknownBlock,
                        WantEndReason::Retired,
                    ][usize::from(a >> 5) % 3],
                };
                (false, encode_frame(&message).ok())
            }
            4 | 5 => {
                let frame = self
                    .inbox
                    .get(usize::from(b) % self.inbox.len().max(1))
                    .cloned();
                let mut frame = frame;
                if a >> 2 & 7 == 5 {
                    if let Some(f) = frame.as_mut() {
                        let len = f.payload.len();
                        if len > 0 {
                            f.payload[usize::from(b) % len] ^= a | 1;
                        }
                    }
                }
                let control = frame.as_ref().is_some_and(|f| {
                    !matches!(
                        f.message_type,
                        message_type::PART | message_type::HAVE | message_type::WANT_END
                    )
                });
                (control, frame)
            }
            6 => {
                let part = solved.and_then(|s| {
                    s.encoded
                        .parts
                        .get(usize::from(mask) % s.encoded.parts.len().max(1))
                });
                let mut frame = part.map(|p| encode_part_frame(block_id, p));
                if let Some(last) = frame.as_mut().and_then(|f| f.payload.last_mut()) {
                    *last ^= 1;
                }
                (false, frame)
            }
            _ => {
                let message = Bpv1Message::BlockDone { block_id };
                (true, encode_frame(&message).ok())
            }
        };
        if let Some(frame) = frame {
            self.adversary_send(to, control, frame);
        }
    }

    /// The adversary sends an arbitrary frame: a type from `a`, then up to
    /// `b % 96` bytes of the input as its payload.
    fn adversary_raw(&mut self, a: u8, b: u8, input: &mut FuzzBytes<'_>) {
        let to = usize::from(a) % ADVERSARY;
        let types = [
            message_type::HEADER_META,
            message_type::WANT,
            message_type::STRIPE_DONE,
            message_type::BLOCK_DONE,
            message_type::PART,
            message_type::HAVE,
            message_type::WANT_END,
        ];
        let message_type = types[usize::from(a >> 2) % types.len()];
        let payload = input.take(usize::from(b % 96)).to_vec();
        let frame = Frame {
            message_type,
            flags: 0,
            payload,
        };
        // `a` bit 7 sends the frame on the other stream.
        let control = matches!(
            message_type,
            message_type::HEADER_META
                | message_type::WANT
                | message_type::STRIPE_DONE
                | message_type::BLOCK_DONE
        ) != (a & 0x80 != 0);
        self.adversary_send(to, control, frame);
    }

    fn adversary_send(&mut self, to: usize, control: bool, frame: Frame) {
        if !self.nodes[to].sessions.contains_key(&ADVERSARY) {
            return;
        }
        let link = self.links.entry((ADVERSARY, to)).or_default();
        if control {
            link.control.push_back(frame);
        } else {
            link.data.push_back(frame);
        }
    }

    // ---------------------------------------------------------------------
    // Sessions
    // ---------------------------------------------------------------------

    /// Opens a session on each honest side of `(i, j)` and replays the
    /// node's active blocks to it, as the shell does for a new session.
    fn connect(&mut self, i: usize, j: usize) {
        let session_gen = self.next_gen;
        self.next_gen += 1;
        for (local, remote) in [(i, j), (j, i)] {
            if !Self::honest(local) {
                continue;
            }
            let state = PeerState::new(
                PeerConfig::for_profile(self.profile),
                self.nodes[remote].id,
                session_gen,
            );
            self.nodes[local].sessions.insert(
                remote,
                Session {
                    state,
                    writer: Bpv1FifoQueue::default(),
                },
            );
        }
        for (local, remote) in [(i, j), (j, i)] {
            if !Self::honest(local) {
                continue;
            }
            for (admitted, ready) in self.nodes[local].table.active_admitted_blocks() {
                let block_id = admitted.block_id;
                self.step_peer(
                    local,
                    remote,
                    PeerInput::BlockAdmitted(admitted_input(&admitted)),
                );
                if !ready.is_empty() {
                    self.step_peer(
                        local,
                        remote,
                        PeerInput::Ready {
                            block_id,
                            indices: ready,
                            push: false,
                        },
                    );
                }
            }
        }
    }

    /// Ends the connection between `i` and `j`: each honest side's session
    /// ends and drains, and the frames in flight are lost.
    fn end_connection(&mut self, i: usize, j: usize) {
        self.links.remove(&(i, j));
        self.links.remove(&(j, i));
        for (local, remote) in [(i, j), (j, i)] {
            let Some(mut session) = self.nodes[local].sessions.remove(&remote) else {
                continue;
            };
            let end = session.state.step(self.now, PeerInput::SessionEnd);
            session.state.assert_drained();
            self.summary.steps += 1;
            hit_peer_step(
                &mut self.summary.reached,
                &PeerInput::SessionEnd,
                &end.outputs,
            );
            // The shell forgets the session's metadata senders, then ends the
            // session in the table. A mesh link never has a newer session
            // that replaced this one, so `SessionEnded` also drops the
            // peer's node facts.
            let peer = self.nodes[remote].id;
            let session_gen = session.state.session_gen();
            self.step_table(local, BlockInput::MetaSessionEnded { peer, session_gen });
            let peer_states = self.peer_states(local);
            self.step_table(
                local,
                BlockInput::SessionEnded {
                    peer,
                    session_gen,
                    peer_states,
                },
            );
        }
    }

    // ---------------------------------------------------------------------
    // Machines and routing
    // ---------------------------------------------------------------------

    fn step_proposer(&mut self, input: ProposerInput) {
        self.summary.reached.hit(proposer_input_name(&input));
        let step = self.proposer.step(input);
        self.summary.steps += 1;
        for output in step.outputs {
            match output {
                ProposerOutput::StartEncode { work_id } => {
                    let block = self.templating;
                    self.nodes[PROPOSER]
                        .jobs
                        .push(Job::Encode { work_id, block });
                }
                ProposerOutput::SetCommitment {
                    template_id,
                    commitment,
                } => {
                    // Template ids count blocks from one.
                    let at = usize::try_from(template_id.0 - 1).expect("template ids are small");
                    self.planned[at].commitment = Some(commitment);
                }
                ProposerOutput::AdmitSolvedLocal {
                    height,
                    envelope,
                    parts,
                } => self.step_table(
                    PROPOSER,
                    BlockInput::BlockAdmittedLocal {
                        height,
                        envelope,
                        parts,
                    },
                ),
                ProposerOutput::QueueSeeds {
                    peer,
                    block_id,
                    seeds,
                    copies,
                } => {
                    let Some(to) = self.index_of(&peer) else {
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
                        PROPOSER,
                        to,
                        PeerInput::Seeds(Seeds {
                            block_id,
                            seeds,
                            copies,
                        }),
                    );
                }
            }
        }
    }

    /// Steps `node`'s session with `peer`, if it is live, and runs its
    /// outputs as the shell does.
    fn step_peer(&mut self, node: usize, peer: usize, input: PeerInput) {
        let Some(session) = self.nodes[node].sessions.get_mut(&peer) else {
            return;
        };
        let step = session.state.step(self.now, input.clone());
        session.state.assert_invariants();
        self.summary.steps += 1;
        hit_peer_step(&mut self.summary.reached, &input, &step.outputs);
        let session_gen = session.state.session_gen();
        let mut work: VecDeque<PeerOutput> = step.outputs.into();
        while let Some(output) = work.pop_front() {
            if let PeerOutput::ServeWant {
                want_id,
                block_id,
                stripe,
                eligible_slots,
            } = output
            {
                let served =
                    self.serve_want(node, peer, want_id, block_id, stripe, &eligible_slots);
                let Some(session) = self.nodes[node].sessions.get_mut(&peer) else {
                    return;
                };
                let done = PeerInput::ServeDone(ServeDone {
                    want_id,
                    sent: served,
                    reason: WantEndReason::Served,
                });
                let step = session.state.step(self.now, done.clone());
                session.state.assert_invariants();
                self.summary.steps += 1;
                hit_peer_step(&mut self.summary.reached, &done, &step.outputs);
                work.extend(step.outputs);
                continue;
            }
            let remote = self.nodes[peer].id;
            let states = || self.peer_states(node);
            match route_peer_output(remote, session_gen, output, states) {
                PeerRoute::Table(input) => self.step_table(node, input),
                PeerRoute::Session(output) => self.session_output(node, peer, output),
            }
        }
        if self.nodes[node]
            .sessions
            .get(&peer)
            .is_some_and(|s| s.state.is_closed())
        {
            self.end_connection(node, peer);
        }
    }

    /// Queues the held parts a `Want` asks for and returns how many.
    fn serve_want(
        &mut self,
        node: usize,
        peer: usize,
        want_id: u32,
        block_id: block::Hash,
        stripe: u32,
        eligible_slots: &[u32],
    ) -> u16 {
        let parts = held_parts(&self.nodes[node].table, &block_id, stripe, eligible_slots);
        // A `Want` names at most `n` slots of one stripe, so the count fits.
        let sent = parts.len() as u16;
        let now = self.now;
        let Some(session) = self.nodes[node].sessions.get_mut(&peer) else {
            return 0;
        };
        for part in parts {
            let index = part.index();
            session.writer.push_back(DataQueueItem::Part {
                block_id,
                stripe,
                part,
                push: false,
            });
            let queued = PeerInput::ServePartQueued(ServePartQueued {
                want_id,
                block_id,
                index,
            });
            let step = session.state.step(now, queued.clone());
            session.state.assert_invariants();
            self.summary.steps += 1;
            hit_peer_step(&mut self.summary.reached, &queued, &step.outputs);
        }
        sent
    }

    /// Runs one session output that stays with the session.
    fn session_output(&mut self, node: usize, peer: usize, output: PeerOutput) {
        let session_gen = match self.nodes[node].sessions.get(&peer) {
            Some(s) => s.state.session_gen(),
            None => return,
        };
        match output {
            PeerOutput::SendControl(message) => {
                let frame = encode_frame(message.as_ref()).unwrap_or_else(|e| {
                    panic!("{MESH}: node {node} queued an unencodable {message:?}: {e:?}")
                });
                self.links
                    .entry((node, peer))
                    .or_default()
                    .control
                    .push_back(frame);
            }
            PeerOutput::SendHave {
                block_id,
                flags,
                indices,
            } => self.push_data(
                node,
                peer,
                DataQueueItem::Have {
                    block_id,
                    flags,
                    indices,
                },
            ),
            PeerOutput::PushPart { block_id, index } => {
                let table = &self.nodes[node].table;
                let held = table
                    .admitted_descriptor(&block_id)
                    .and_then(|(c, _)| c.split_index(index))
                    .zip(table.get_part(&block_id, index).cloned());
                if let Some(((stripe, _), part)) = held {
                    self.push_data(
                        node,
                        peer,
                        DataQueueItem::Part {
                            block_id,
                            stripe,
                            part,
                            push: true,
                        },
                    );
                }
            }
            PeerOutput::SendSeedPart { block_id, part } => {
                let stripe = self.nodes[node]
                    .table
                    .admitted_descriptor(&block_id)
                    .and_then(|(c, _)| c.split_index(part.index()))
                    .map_or(0, |(s, _)| s);
                self.push_data(
                    node,
                    peer,
                    DataQueueItem::Part {
                        block_id,
                        stripe,
                        part,
                        push: true,
                    },
                );
            }
            PeerOutput::SendWantEnd {
                want_id,
                sent,
                reason,
            } => self.push_data(
                node,
                peer,
                DataQueueItem::WantEnd {
                    want_id,
                    sent,
                    reason,
                },
            ),
            PeerOutput::SubmitHeaderMeta { block_id, message } => {
                if let Some(envelope) =
                    crate::zakura::bpv1::service::decode_meta_envelope(block_id, *message)
                {
                    let from_peer = self.nodes[peer].id;
                    self.step_table(
                        node,
                        BlockInput::MetaReceived {
                            envelope,
                            from_peer,
                            session_gen,
                        },
                    );
                }
            }
            PeerOutput::VerifyPart { token, payload } => {
                let from = self.nodes[peer].id;
                // usize -> u64 widens on every supported target.
                let bytes = payload.len() as u64;
                match charge_part(&mut self.nodes[node].table, self.now, from, token, bytes) {
                    Ok((coding, part_root)) => self.nodes[node].jobs.push(Job::Proof {
                        peer,
                        session_gen,
                        token,
                        payload,
                        coding,
                        part_root,
                    }),
                    Err(refused) => self.step_peer(node, peer, refused),
                }
            }
            PeerOutput::CancelQueuedPush { block_id, index } => {
                if let Some(s) = self.nodes[node].sessions.get_mut(&peer) {
                    s.writer.cancel_push(block_id, index);
                }
            }
            PeerOutput::CancelQueuedStripe { block_id, stripe } => {
                if let Some(s) = self.nodes[node].sessions.get_mut(&peer) {
                    s.writer.cancel_stripe(block_id, stripe);
                }
            }
            PeerOutput::PurgeQueuedBlock {
                block_id,
                include_haves,
            } => {
                if let Some(s) = self.nodes[node].sessions.get_mut(&peer) {
                    s.writer.purge_block(block_id, include_haves);
                }
            }
            PeerOutput::CloseProtocol(cause) => {
                assert_eq!(
                    peer, ADVERSARY,
                    "{MESH}: node {node} closed honest node {peer} with {cause:?}"
                );
            }
            PeerOutput::PauseDataReads
            | PeerOutput::ResumeDataReads
            | PeerOutput::CloseNeutralStall => {}
            routed => unreachable!("route_peer_output sends {routed:?} to the table"),
        }
    }

    fn push_data(&mut self, node: usize, peer: usize, item: DataQueueItem) {
        if let Some(s) = self.nodes[node].sessions.get_mut(&peer) {
            s.writer.push_back(item);
        }
    }

    /// Steps `node`'s table and routes its outputs.
    fn step_table(&mut self, node: usize, input: BlockInput) {
        let step = self.nodes[node].table.step(self.now, input.clone());
        self.nodes[node].table.assert_invariants();
        self.summary.steps += 1;
        hit_block_step(&mut self.summary.reached, &input, &step.outputs);
        for output in step.outputs {
            self.check_table_output(node, &output);
            match route_block_output(output) {
                BlockRoute::Peer {
                    peer,
                    session_gen,
                    input,
                } => {
                    let to = self.index_of(&peer).expect("tables name mesh nodes");
                    let live = self.nodes[node]
                        .sessions
                        .get(&to)
                        .is_some_and(|s| session_gen.is_none_or(|g| g == s.state.session_gen()));
                    if live {
                        self.step_peer(node, to, input);
                    } else {
                        let states = || self.peer_states(node);
                        if let Some(release) = refused_grant(peer, session_gen, input, states) {
                            self.step_table(node, release);
                        }
                    }
                }
                BlockRoute::Broadcast(broadcast) => {
                    let peers: Vec<usize> = self.nodes[node].sessions.keys().copied().collect();
                    for to in peers {
                        let input = broadcast.input_for(&self.nodes[to].id);
                        self.step_peer(node, to, input);
                    }
                }
                BlockRoute::Shell(output) => self.shell(node, output),
            }
        }
        for blk in &self.solved {
            if let Some(phase) = self.nodes[node].table.block_phase(&blk.id) {
                self.summary.reached.hit(super::quality::phase_name(phase));
            }
        }
    }

    /// The table-level checks on one output.
    fn check_table_output(&mut self, node: usize, output: &BlockOutput) {
        let n = &mut self.nodes[node];
        match output {
            BlockOutput::PublishAdmitted { admitted } if !admitted.as_proposer => assert!(
                !n.retired.contains(&admitted.block_id),
                "{MESH}: bpv1-46 node {node} readmitted retired block {:?}",
                admitted.block_id
            ),
            BlockOutput::PublishRetire { block_id, .. } => n.retired.push(*block_id),
            BlockOutput::ClaimBody { block_id, .. } => {
                if !n.claimed.contains(block_id) {
                    n.claimed.push(*block_id);
                }
            }
            BlockOutput::ExtendBodyClaim { block_id, .. } => assert!(
                n.claimed.contains(block_id),
                "{MESH}: bpv1-44 node {node} extended a body claim it does not hold"
            ),
            BlockOutput::ReleaseBodyClaim { block_id, .. } => {
                assert!(
                    n.claimed.contains(block_id),
                    "{MESH}: bpv1-44 node {node} released a body claim it never took"
                );
                n.claimed.retain(|id| id != block_id);
            }
            _ => {}
        }
    }

    /// Queues the table's shell work, or closes the peer it names.
    fn shell(&mut self, node: usize, output: BlockOutput) {
        let job = match output {
            BlockOutput::AdmitHeader {
                work_id, block_id, ..
            } => Job::Header { work_id, block_id },
            BlockOutput::CheckUnknownParent {
                work_id, envelope, ..
            } => Job::UnknownParent { work_id, envelope },
            BlockOutput::VerifyCommitment {
                work_id,
                height,
                envelope,
                ..
            } => Job::Commitment {
                work_id,
                height,
                envelope,
            },
            BlockOutput::StartStripeJob {
                work_id,
                block_id,
                stripe,
                coding,
                enc_id,
                held,
                ..
            } => Job::Stripe {
                work_id,
                block_id,
                stripe,
                coding,
                enc_id,
                held,
            },
            BlockOutput::SubmitVerifier {
                work_id,
                block_id,
                header_bytes,
                coinbase_bytes,
                coding,
                stripe_sources,
            } => Job::Verify {
                work_id,
                block_id,
                header_bytes,
                coinbase_bytes,
                coding,
                stripe_sources,
            },
            BlockOutput::ClosePeer {
                peer,
                session_gen,
                row,
            } => {
                let to = self.index_of(&peer).expect("tables name mesh nodes");
                assert_eq!(
                    to, ADVERSARY,
                    "{MESH}: node {node}'s table closed honest node {to} with {row:?}"
                );
                self.summary.reached.hit(row.label());
                if self.nodes[node]
                    .sessions
                    .get(&to)
                    .is_some_and(|s| s.state.session_gen() == session_gen)
                {
                    self.end_connection(node, to);
                }
                return;
            }
            // `check_table_output` keeps the claim ledger.
            BlockOutput::ClaimBody { .. }
            | BlockOutput::ExtendBodyClaim { .. }
            | BlockOutput::ReleaseBodyClaim { .. } => return,
            routed => unreachable!("route_block_output keeps {routed:?} off the shell"),
        };
        self.nodes[node].jobs.push(job);
    }

    /// Commits `block_id` on `node`, with the inputs the shell derives from
    /// a new verified tip (`frontier_inputs`): the tip height, the end of
    /// the block's fallback, if any, and its children's wake-up.
    fn commit(&mut self, node: usize, block_id: block::Hash) {
        let Some(height) = self
            .solved
            .iter()
            .find(|s| s.id == block_id)
            .map(|s| s.height)
        else {
            return;
        };
        if height <= self.nodes[node].tip {
            return;
        }
        self.nodes[node].tip = height;
        self.step_table(
            node,
            BlockInput::TipHeight {
                height,
                hash: block_id,
            },
        );
        self.step_table(node, BlockInput::FallbackDone { block_id });
        self.step_table(
            node,
            BlockInput::ParentAdmitted {
                hash: block_id,
                height,
            },
        );
    }

    /// Block sync fetches and commits one block that fell back on the node
    /// `a` picks.
    fn fallback_done(&mut self, a: u8) {
        let node = usize::from(a) % ADVERSARY;
        let fallen: Vec<block::Hash> = self
            .solved
            .iter()
            .map(|s| s.id)
            .filter(|id| self.nodes[node].table.block_phase(id) == Some(BlockPhase::Fallback))
            .collect();
        if let Some(&block_id) = fallen.get(usize::from(a >> 2) % fallen.len().max(1)) {
            self.commit(node, block_id);
        }
    }

    /// Block sync commits blocks past the mesh's chain on every honest
    /// node: the verified tip jumps more than twice the propagation window
    /// past the highest mesh block, so every table retires the mesh's
    /// blocks and evicts their tombstones. The committed blocks are not
    /// mesh blocks, so their hashes are synthetic.
    fn sync_past(&mut self, b: u8) {
        let top = self.solved.iter().map(|s| s.height).max().unwrap_or(0);
        for node in (0..NODES).filter(|&n| Self::honest(n)) {
            let height = self.nodes[node]
                .tip
                .max(top)
                .saturating_add(2 * PROPAGATION_DEPTH + 1)
                .saturating_add(u32::from(b >> 2));
            let mut hash = [0xEE; 32];
            hash[..4].copy_from_slice(&height.to_le_bytes());
            let hash = block::Hash(hash);
            self.nodes[node].tip = height;
            self.step_table(node, BlockInput::TipHeight { height, hash });
            self.step_table(node, BlockInput::FallbackDone { block_id: hash });
            self.step_table(node, BlockInput::ParentAdmitted { hash, height });
        }
    }

    /// Lets block sync finish every fallback, ends every connection, and
    /// checks that each session drained.
    fn finish(mut self) -> MeshReplaySummary {
        for node in (0..NODES).filter(|&n| Self::honest(n)) {
            // `node` is below `NODES`, so the cast keeps it; each call
            // finishes one fallback, and a node holds at most `BLOCKS`.
            for _ in 0..BLOCKS {
                self.fallback_done(node as u8);
            }
        }
        for (i, j) in self.connections(true) {
            self.end_connection(i, j);
        }
        if let Some(last) = self.solved.last() {
            self.summary.committed = (0..NODES)
                .filter(|&n| Self::honest(n) && self.nodes[n].tip >= last.height)
                .count();
        }
        self.summary
    }
}
