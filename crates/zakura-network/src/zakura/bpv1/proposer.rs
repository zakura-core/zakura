//! Pure proposer state machine (`ProposerState`, `decision-1`).
//!
//! Moves Reed–Solomon encoding from solve time to template-build time so the
//! coinbase can commit to `C = T("commit", ...)` before the miner starts
//! hashing. `C` names no peer (decision-19). Header-only template updates
//! reuse the prepared encoding (`opt-8`); body changes trigger a fresh encode.
//! A solved block verifies its coinbase commitment, then the proposer builds
//! the seed plan from the neighbours live at that moment, queues `HeaderMeta`
//! and `BlockDone` to every peer, and pushes each neighbour its seeds behind
//! flagged `Have`s (`bpv1-9`, `bpv1-17`, `bpv1-67`).

use std::{cmp::Reverse, time::Duration};

use zakura_chain::{
    block::{self, Header},
    serialization::ZcashDeserialize,
    transaction::Transaction,
};
use zakura_quic::NodeId;
use zakura_rt::collections::{HashMap, HashSet};

use super::{
    blocks::{MetaEnvelope, WorkId},
    codec::EncodedBody,
    commitment::{
        check_block_commitments, commit_hash, commitment_script, extract_single_commitment,
        CoinbaseBinding, CommitmentFields, COMMITMENT_SCRIPT_LEN,
    },
    paths::PeerPaths,
    profile::{Coding, W2_MAX_COINBASE_BYTES, W2_PROFILE_ID},
    tree::VerifiedPart,
};

/// Maximum prepared encodings, and maximum in-flight encodes, keyed by `body_identity`.
pub const MAX_PREPARED_ENCODINGS: usize = 4;

/// Opaque mining template identifier.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct TemplateId(pub u64);

/// Configuration for [`ProposerState`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ProposerConfig {
    /// Chain identifier committed in `C`.
    pub chain_id: [u8; 32],
    /// Local node's transport identity (`proposer_node`).
    pub local_node: NodeId,
    /// Permit the experimental deal-3 scheduler to queue direct copies.
    pub direct_copies: bool,
}

impl ProposerConfig {
    /// Creates the configuration for `chain_id` and `local_node`.
    pub const fn new(chain_id: [u8; 32], local_node: NodeId) -> Self {
        Self {
            chain_id,
            local_node,
            direct_copies: false,
        }
    }
}

/// Prepared commitment output ready to embed in the v6 coinbase transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedCommitment {
    /// 32-byte commitment hash `C`.
    pub commitment_hash: [u8; 32],
    /// Exact 39-byte `OP_RETURN` script (`6a 25 BPV1 01 C`).
    pub script: [u8; COMMITMENT_SCRIPT_LEN],
    /// Coding tuple committed by `C`.
    pub coding: Coding,
    /// `part_root` committed by `C`.
    pub part_root: [u8; 32],
    /// The template's light-client root, which `HeaderMeta` carries once the
    /// block is solved. `C` does not commit it, so templates for one body on
    /// different parents share `C` and differ only here.
    pub light_client_root: [u8; 32],
}

/// Synchronous input to [`ProposerState::step`].
#[derive(Clone, Debug)]
pub enum ProposerInput {
    /// A new or updated mining template was built.
    TemplateBuilt {
        /// Template identifier.
        template_id: TemplateId,
        /// BLAKE3 identity of `tx_count || B'` (same across header-only changes).
        body_identity: [u8; 32],
        /// The template's light-client root (`hashLightClientRoot`).
        light_client_root: [u8; 32],
    },
    /// Background encode worker completed for `work_id`.
    ///
    /// Each template waiting on the encode keeps the `light_client_root` from
    /// its own `TemplateBuilt`.
    EncodeDone {
        /// Work identity from `StartEncode`.
        work_id: WorkId,
        /// Encoded body (`coding`, `part_root`, `parts`).
        encoded: EncodedBody,
    },
    /// A block was solved (locally mined or externally submitted).
    Solved {
        /// Consensus hash of the solved block.
        block_id: block::Hash,
        /// Height of the solved block.
        height: u32,
        /// Serialized full header bytes.
        header_bytes: Vec<u8>,
        /// Serialized coinbase transaction bytes.
        coinbase_bytes: Vec<u8>,
        /// Txid Merkle siblings for index 0.
        txid_siblings: Vec<[u8; 32]>,
        /// ZIP 244 auth-data Merkle siblings for index 0.
        auth_siblings: Vec<[u8; 32]>,
        /// BLAKE3 identity of `tx_count || B'` of the solved block.
        body_identity: [u8; 32],
        /// The neighbours live now, the seed plan's candidates.
        peers: Vec<NodeId>,
        /// Current [`PeerPaths`] snapshot, for `rate_out` and RTT.
        paths: Box<PeerPaths>,
    },
}

/// Effect emitted by [`ProposerState::step`].
#[derive(Clone, Debug)]
pub enum ProposerOutput {
    /// Start encoding `B'` on the encode worker pool.
    StartEncode {
        /// Work identity for staleness filtering (`bpv1-63`).
        work_id: WorkId,
    },
    /// Install the BPv1 commitment output on `template_id`.
    SetCommitment {
        /// Target template.
        template_id: TemplateId,
        /// Computed commitment fields & script.
        commitment: PreparedCommitment,
    },
    /// Admit the solved block locally into `BlockTable` (`as_proposer: true`),
    /// which broadcasts `HeaderMeta` + `BlockDone` to every peer BEFORE seed parts (`bpv1-8`).
    AdmitSolvedLocal {
        /// Solved block height.
        height: u32,
        /// Full metadata envelope.
        envelope: Box<MetaEnvelope>,
        /// All `stripes * n` verified parts.
        parts: Vec<VerifiedPart>,
    },
    /// Queue `peer`'s seeds (`bpv1-9`, `bpv1-17`). The session writes the
    /// deal-1 and deal-2 seeds at once and releases the direct copies to
    /// its idle data writer.
    QueueSeeds {
        /// Seeded neighbour.
        peer: NodeId,
        /// Block hash.
        block_id: block::Hash,
        /// deal-1 and deal-2 seeds, in queue order.
        seeds: Vec<QueuedSeed>,
        /// deal-3 direct copies, in release order.
        copies: Vec<VerifiedPart>,
    },
}

/// One seed a [`ProposerOutput::QueueSeeds`] queues.
#[derive(Clone, Debug)]
pub struct QueuedSeed {
    /// Verified part to push.
    pub part: VerifiedPart,
    /// Whether the neighbour pushes it onward: a relay flag (decision-24).
    pub relay: bool,
    /// The deal that gave it, for `seed.bytes{deal}`.
    pub deal: SeedDeal,
}

/// Typed event emitted by [`ProposerState::step`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ProposerEvent {
    /// Existing prepared encoding reused across a header-only template update (`opt-8`).
    EncodeReused {
        /// Template id.
        template_id: TemplateId,
        /// Reused body identity.
        body_identity: [u8; 32],
    },
    /// New encoding job started for a changed body.
    EncodeStarted {
        /// Template id.
        template_id: TemplateId,
        /// Work identity.
        work_id: WorkId,
    },
    /// Solved block matched prepared commitment and started BPv1 publication.
    Solved {
        /// Block hash.
        block_id: block::Hash,
    },
    /// One seeded neighbour's share of a solved block's seed plan
    /// (decision-19).
    SeedPlanned {
        /// Block hash.
        block_id: block::Hash,
        /// Seeded neighbour.
        peer: NodeId,
        /// Pushed seeds.
        pushed: usize,
        /// Other seeds, direct copies included.
        other: usize,
        /// The `push_rate` the plan assumed, in bytes per second.
        push_rate: u64,
        /// Predicted `finish(H)` after the plan starts.
        finish: Duration,
    },
}

/// Result of one [`ProposerState::step`] transition.
#[derive(Clone, Debug, Default)]
pub struct ProposerStep {
    /// Effects for IO to execute.
    pub outputs: Vec<ProposerOutput>,
    /// Typed events for traces, metrics, and tests.
    pub events: Vec<ProposerEvent>,
}

/// A template waiting on an in-flight encode of its body.
#[derive(Clone, Debug)]
struct Waiter {
    template_id: TemplateId,
    light_client_root: [u8; 32],
}

/// One in-flight encode, shared by every template for its body.
#[derive(Clone, Debug)]
struct PendingEncode {
    work_id: WorkId,
    body_identity: [u8; 32],
    /// Newest at the back, at most [`MAX_COMMITMENTS_PER_BODY`].
    waiters: Vec<Waiter>,
}

/// Maximum issued commitments retained per body identity so solves on earlier
/// templates still match.
const MAX_COMMITMENTS_PER_BODY: usize = 4;

#[derive(Clone, Debug)]
struct PreparedEntry {
    body_identity: [u8; 32],
    encoded: EncodedBody,
    /// Commitments issued for this body, newest at the back, at most
    /// [`MAX_COMMITMENTS_PER_BODY`]. They share one `C` and differ only by
    /// light-client root.
    commitments: Vec<PreparedCommitment>,
}

impl PreparedEntry {
    /// Records `commitment` unless an equal one is already held, dropping the
    /// oldest past the bound.
    fn remember(&mut self, commitment: &PreparedCommitment) {
        if self.commitments.contains(commitment) {
            return;
        }
        push_bounded(
            &mut self.commitments,
            commitment.clone(),
            MAX_COMMITMENTS_PER_BODY,
        );
    }
}

/// Appends `item` and drops the oldest entries until `items.len() <= max`.
fn push_bounded<T>(items: &mut Vec<T>, item: T, max: usize) {
    items.push(item);
    let excess = items.len().saturating_sub(max);
    items.drain(..excess);
}

/// Pure BPv1 proposer state machine.
#[derive(Clone, Debug)]
pub struct ProposerState {
    config: ProposerConfig,
    next_work_id: u64,
    /// In-flight encodes keyed by body, oldest first, at most [`MAX_PREPARED_ENCODINGS`].
    pending: Vec<PendingEncode>,
    /// Prepared encodings keyed by body, oldest first, at most [`MAX_PREPARED_ENCODINGS`].
    prepared: Vec<PreparedEntry>,
}

impl ProposerState {
    /// Creates a fresh proposer machine.
    pub fn new(config: ProposerConfig) -> Self {
        Self {
            config,
            next_work_id: 1,
            pending: Vec::new(),
            prepared: Vec::new(),
        }
    }

    /// The bytes the prepared encodings hold: each encoding's parts at
    /// [`Coding::encoded_bytes`]. An encode in flight holds its bytes outside
    /// the machine until [`ProposerInput::EncodeDone`].
    pub fn prepared_bytes(&self) -> u64 {
        self.prepared
            .iter()
            .map(|entry| entry.encoded.coding.encoded_bytes())
            .fold(0, u64::saturating_add)
    }

    /// Executes one synchronous proposer transition.
    pub fn step(&mut self, input: ProposerInput) -> ProposerStep {
        let mut step = ProposerStep::default();
        match input {
            ProposerInput::TemplateBuilt {
                template_id,
                body_identity,
                light_client_root,
            } => {
                let waiter = Waiter {
                    template_id,
                    light_client_root,
                };
                if let Some(at) = self
                    .prepared
                    .iter()
                    .position(|e| e.body_identity == body_identity)
                {
                    // opt-8: a header-only update reuses the encoding; `C` is a cheap hash.
                    step.events.push(ProposerEvent::EncodeReused {
                        template_id,
                        body_identity,
                    });
                    self.commit(at, &waiter, &mut step);
                } else if let Some(pending) = self
                    .pending
                    .iter_mut()
                    .find(|p| p.body_identity == body_identity)
                {
                    // Join the body's in-flight encode instead of restarting it.
                    push_bounded(&mut pending.waiters, waiter, MAX_COMMITMENTS_PER_BODY);
                } else {
                    let work_id = WorkId(self.next_work_id);
                    self.next_work_id = self.next_work_id.wrapping_add(1).max(1);
                    push_bounded(
                        &mut self.pending,
                        PendingEncode {
                            work_id,
                            body_identity,
                            waiters: vec![waiter],
                        },
                        MAX_PREPARED_ENCODINGS,
                    );
                    step.events.push(ProposerEvent::EncodeStarted {
                        template_id,
                        work_id,
                    });
                    step.outputs.push(ProposerOutput::StartEncode { work_id });
                }
            }
            ProposerInput::EncodeDone { work_id, encoded } => {
                // bpv1-63: a completion for an unknown or evicted encode is stale.
                let Some(at) = self.pending.iter().position(|p| p.work_id == work_id) else {
                    return step;
                };
                let pending = self.pending.remove(at);
                push_bounded(
                    &mut self.prepared,
                    PreparedEntry {
                        body_identity: pending.body_identity,
                        encoded,
                        commitments: Vec::new(),
                    },
                    MAX_PREPARED_ENCODINGS,
                );
                let at = self.prepared.len() - 1;
                for waiter in &pending.waiters {
                    self.commit(at, waiter, &mut step);
                }
            }
            ProposerInput::Solved {
                block_id,
                height,
                header_bytes,
                coinbase_bytes,
                txid_siblings,
                auth_siblings,
                body_identity,
                peers,
                paths,
            } => {
                let Some(entry) = self
                    .prepared
                    .iter()
                    .find(|e| e.body_identity == body_identity)
                else {
                    // No prepared encoding (e.g., external submission miss) -> existing relay.
                    return step;
                };
                // bpv1-9 & 06 §1: verify coinbase holds a committed hash C from this prepared entry.
                if coinbase_bytes.len() > W2_MAX_COINBASE_BYTES {
                    return step;
                }
                let mut cb_slice = coinbase_bytes.as_slice();
                let Ok(coinbase_tx) = Transaction::zcash_deserialize_bounded(&mut cb_slice) else {
                    return step;
                };
                if !cb_slice.is_empty() {
                    return step;
                }
                let Ok(found_commit) = extract_single_commitment(&coinbase_tx) else {
                    return step;
                };
                let mut hdr_slice = header_bytes.as_slice();
                let Ok(header) = Header::zcash_deserialize(&mut hdr_slice) else {
                    return step;
                };
                // Templates for one body on different parents share `C`; the
                // header's `hashBlockCommitments` picks the light-client root
                // the block was built on, which receivers check (check-3).
                let Some(commitment) = entry.commitments.iter().rev().find(|c| {
                    c.commitment_hash == found_commit
                        && check_block_commitments(
                            &header,
                            &coinbase_tx,
                            &auth_siblings,
                            c.light_client_root,
                        )
                        .is_ok()
                }) else {
                    return step;
                };

                let coding = entry.encoded.coding;
                let part_root = entry.encoded.part_root;
                let parts = entry.encoded.parts.clone();

                let binding = CoinbaseBinding {
                    coinbase: coinbase_bytes,
                    txid_siblings,
                    auth_siblings,
                    light_client_root: commitment.light_client_root,
                };
                let envelope = Box::new(MetaEnvelope {
                    block_id,
                    header_bytes,
                    binding,
                    coding,
                    part_root,
                    proposer_node: self.config.local_node,
                });

                let plan = seed_plan(
                    &coding,
                    self.config.local_node,
                    &peers,
                    &paths,
                    self.config.direct_copies,
                );
                step.events.push(ProposerEvent::Solved { block_id });
                for holder in &plan.holders {
                    step.events.push(ProposerEvent::SeedPlanned {
                        block_id,
                        peer: holder.peer,
                        pushed: holder.pushed,
                        other: holder.other,
                        push_rate: holder.push_rate,
                        finish: holder.finish,
                    });
                }
                // 1. AdmitSolvedLocal emits PublishAdmitted -> HeaderMeta, then
                //    BlockDone, to every connected peer BEFORE any seed
                //    (`bpv1-8`, `bpv1-9`).
                step.outputs.push(ProposerOutput::AdmitSolvedLocal {
                    height,
                    envelope,
                    parts: parts.clone(),
                });
                // 2. Each neighbour's seeds and direct copies; its session
                //    releases the copies once its data writer is idle.
                step.outputs.extend(queue_seeds(block_id, &plan, &parts));
            }
        }
        step
    }

    /// Builds `C` for `waiter` over prepared entry `at`, records it, and installs it.
    fn commit(&mut self, at: usize, waiter: &Waiter, step: &mut ProposerStep) {
        let coding = self.prepared[at].encoded.coding;
        let part_root = self.prepared[at].encoded.part_root;
        let commitment = self.build_commitment(coding, part_root, waiter);
        self.prepared[at].remember(&commitment);
        step.outputs.push(ProposerOutput::SetCommitment {
            template_id: waiter.template_id,
            commitment,
        });
    }

    fn build_commitment(
        &self,
        coding: Coding,
        part_root: [u8; 32],
        waiter: &Waiter,
    ) -> PreparedCommitment {
        let Waiter {
            light_client_root, ..
        } = *waiter;
        let commitment_hash = commit_hash(&CommitmentFields {
            chain_id: self.config.chain_id,
            profile_id: W2_PROFILE_ID,
            coding,
            part_root,
            proposer_node: self.config.local_node,
        });
        PreparedCommitment {
            commitment_hash,
            script: commitment_script(commitment_hash),
            coding,
            part_root,
            light_client_root,
        }
    }
}

/// Which deal of the seed plan queued a seed.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum SeedDeal {
    /// deal-1: every part once, pushed unless every neighbour is at its cap.
    One,
    /// deal-2: cover copies so the loss of one neighbour costs parity only.
    Two,
    /// deal-3: a direct copy of a part the neighbour lacks, released while
    /// its data writer would otherwise idle.
    Direct,
}

/// One seed of the plan.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PlannedSeed {
    /// Seeded neighbour.
    pub peer: NodeId,
    /// Wire part index.
    pub index: u32,
    /// Whether the neighbour pushes it on (decision-24).
    pub pushed: bool,
    /// The deal that gave it.
    pub deal: SeedDeal,
}

/// One seeded neighbour's share of the plan, for traces.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HolderPlan {
    /// Seeded neighbour.
    pub peer: NodeId,
    /// Pushed seeds.
    pub pushed: usize,
    /// Other seeds, direct copies included.
    pub other: usize,
    /// The `push_rate` the plan assumed.
    pub push_rate: u64,
    /// Predicted `finish(H)` after the plan starts: when the holder's push
    /// reaches its slowest peer. Without pushed seeds, when its seeds arrive.
    pub finish: Duration,
}

/// The proposer's seeds for one solved block (decision-19).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SeedPlan {
    /// deal-1 and deal-2 seeds, in queue order.
    pub covering: Vec<PlannedSeed>,
    /// deal-3 direct copies, closest neighbour first, each neighbour's in
    /// release order.
    pub direct: Vec<PlannedSeed>,
    /// Each eligible neighbour's share, in neighbour order.
    pub holders: Vec<HolderPlan>,
}

/// Groups `seeds` by neighbour, each in queue order, the neighbours in
/// order of their first seed.
pub fn by_peer(seeds: &[PlannedSeed]) -> Vec<(NodeId, Vec<PlannedSeed>)> {
    let mut groups: Vec<(NodeId, Vec<PlannedSeed>)> = Vec::new();
    for seed in seeds {
        match groups.iter_mut().find(|(peer, _)| *peer == seed.peer) {
            Some((_, group)) => group.push(*seed),
            None => groups.push((seed.peer, vec![*seed])),
        }
    }
    groups
}

/// One [`ProposerOutput::QueueSeeds`] per neighbour in `plan`: its deal-1
/// and deal-2 seeds, then its direct copies, the neighbours in order of
/// their first seed.
pub fn queue_seeds(
    block_id: block::Hash,
    plan: &SeedPlan,
    parts: &[VerifiedPart],
) -> Vec<ProposerOutput> {
    // Widening u32 to usize is lossless on supported targets.
    let part = |seed: &PlannedSeed| parts[seed.index as usize].clone();
    let mut copies = by_peer(&plan.direct);
    let mut outputs = Vec::new();
    for (peer, seeds) in by_peer(&plan.covering) {
        let copies = copies
            .iter()
            .position(|(p, _)| *p == peer)
            .map_or_else(Vec::new, |at| copies.remove(at).1);
        outputs.push(ProposerOutput::QueueSeeds {
            peer,
            block_id,
            seeds: seeds
                .iter()
                .map(|seed| QueuedSeed {
                    part: part(seed),
                    relay: seed.pushed,
                    deal: seed.deal,
                })
                .collect(),
            copies: copies.iter().map(part).collect(),
        });
    }
    for (peer, copies) in copies {
        outputs.push(ProposerOutput::QueueSeeds {
            peer,
            block_id,
            seeds: Vec::new(),
            copies: copies.iter().map(part).collect(),
        });
    }
    outputs
}

/// One neighbour's share of the plan as the deals build it.
struct Share {
    peer: NodeId,
    /// Seed bytes given so far; the next seed queues behind them.
    bytes: u64,
    /// Pushed seed bytes given so far, in push order.
    pushed_bytes: u64,
    held: HashSet<u32>,
    /// Seeds per stripe.
    held_in: HashMap<u32, u32>,
    /// Pushed seeds per stripe.
    pushed_in: HashMap<u32, u32>,
}

impl Share {
    fn give(&mut self, coding: &Coding, index: u32, pushed: bool) {
        let stripe = coding.split_index(index).map_or(0, |(s, _)| s);
        let bytes = u64::from(coding.wire_len(index).unwrap_or(0));
        self.bytes = self.bytes.saturating_add(bytes);
        self.held.insert(index);
        *self.held_in.entry(stripe).or_insert(0) += 1;
        if pushed {
            self.pushed_bytes = self.pushed_bytes.saturating_add(bytes);
            *self.pushed_in.entry(stripe).or_insert(0) += 1;
        }
    }

    fn count(per_stripe: &HashMap<u32, u32>, stripe: u32) -> u32 {
        per_stripe.get(&stripe).copied().unwrap_or(0)
    }
}

/// Builds the seed plan over `peers` (decision-19).
///
/// A neighbour outside `SLOW_X` × the median seed arrival of an even share
/// is not eligible. Seed arrival uses the proposer's `rate_out`.
/// - deal-1 gives every non-padding part once, as a pushed seed, to the
///   neighbour with the earliest push finish
///   [`PeerPaths::push_arrival`]. A neighbour takes at most
///   `cap = min(n − k, ceil(real / eligible))` pushed seeds of a stripe,
///   where `real` counts the stripe's non-padding parts. The even share
///   bounds what any `floor((n − k) / cap)` withholding holders can take
///   from a stripe to `n − k` parts, so the others still push `k` (counting
///   padding). A part left once every neighbour is at its cap goes to the
///   earliest seed arrival as an other seed; that happens only with fewer
///   than `ceil(real / (n − k))` eligible neighbours.
/// - deal-2 copies a neighbour's parts to the next-earliest neighbour as
///   other seeds while losing it would leave a stripe below `k` distinct
///   parts at the others (bpv1-67). Padding counts as held.
/// - deal-3 lists direct copies: each neighbour may get any part it lacks
///   while it holds fewer than `k` parts of the stripe. A neighbour's
///   copies go latest holder push first (a part nobody pushes counts as
///   latest), so the copies most likely to beat the push go out first, and
///   the neighbours go closest first: earliest arrival of their seeds. The
///   session releases copies only while its data writer would idle, and
///   drops each one the neighbour turns out not to need.
pub fn seed_plan(
    coding: &Coding,
    local: NodeId,
    peers: &[NodeId],
    paths: &PeerPaths,
    direct_copies: bool,
) -> SeedPlan {
    let mut unique: Vec<NodeId> = Vec::with_capacity(peers.len());
    for &peer in peers {
        if peer != local && !unique.contains(&peer) {
            unique.push(peer);
        }
    }
    unique.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    let parts: Vec<u32> = (0..coding.total_parts().unwrap_or(0))
        .filter(|&index| !coding.is_padding(index))
        .collect();
    if unique.is_empty() || parts.is_empty() {
        return SeedPlan::default();
    }
    let body_bytes: u64 = parts
        .iter()
        .map(|&index| u64::from(coding.wire_len(index).unwrap_or(0)))
        .sum();
    // Widening usize to u64 is lossless on supported targets.
    let share_bytes = body_bytes.div_ceil(unique.len() as u64);
    let eligible = paths.eligible_seed_peers(&unique, share_bytes);
    let mut shares: Vec<Share> = eligible
        .iter()
        .map(|&peer| Share {
            peer,
            bytes: 0,
            pushed_bytes: 0,
            held: HashSet::new(),
            held_in: HashMap::new(),
            pushed_in: HashMap::new(),
        })
        .collect();
    let mut plan = SeedPlan::default();
    let n = coding.n;
    let k = coding.k;
    let stripe_of = |index: u32| coding.split_index(index).map_or(0, |(s, _)| s);
    let bytes_of = |index: u32| u64::from(coding.wire_len(index).unwrap_or(0));
    let seed_arrival =
        |share: &Share, index: u32| paths.seed_time(&share.peer, share.bytes + bytes_of(index));
    // The earliest seed arrival of `index` among the shares `allowed` admits.
    let earliest = |shares: &[Share], index: u32, allowed: &dyn Fn(&Share) -> bool| {
        shares
            .iter()
            .enumerate()
            .filter(|(_, share)| allowed(share))
            .min_by_key(|(_, share)| (seed_arrival(share, index), *share.peer.as_bytes()))
            .map(|(at, _)| at)
    };

    // deal-1: every part once, pushed by the holder whose push finishes
    // first, each holder within its even share of a stripe.
    let parity = n - k;
    let mut real_in: HashMap<u32, u32> = HashMap::new();
    for &index in &parts {
        *real_in.entry(stripe_of(index)).or_insert(0) += 1;
    }
    // Past u32::MAX neighbours the share is one part either way.
    let eligible_count = u32::try_from(shares.len()).unwrap_or(u32::MAX);
    let cap = |stripe: u32| {
        parity.min(
            real_in
                .get(&stripe)
                .copied()
                .unwrap_or(0)
                .div_ceil(eligible_count),
        )
    };
    // Each pushed part's holder, and the holder's pushed bytes through it.
    let mut pushed_by: HashMap<u32, (usize, u64)> = HashMap::new();
    for &index in &parts {
        let stripe = stripe_of(index);
        let bytes = bytes_of(index);
        let holder = shares
            .iter()
            .enumerate()
            .filter(|(_, share)| Share::count(&share.pushed_in, stripe) < cap(stripe))
            .min_by_key(|(_, share)| {
                (
                    paths.push_arrival(
                        &share.peer,
                        share.bytes.saturating_add(bytes),
                        share.pushed_bytes.saturating_add(bytes),
                    ),
                    *share.peer.as_bytes(),
                )
            })
            .map(|(at, _)| at);
        let (at, pushed) = match holder {
            Some(at) => (at, true),
            None => (
                earliest(&shares, index, &|_| true).expect("at least one neighbour is eligible"),
                false,
            ),
        };
        shares[at].give(coding, index, pushed);
        if direct_copies && pushed {
            pushed_by.insert(index, (at, shares[at].pushed_bytes));
        }
        plan.covering.push(PlannedSeed {
            peer: shares[at].peer,
            index,
            pushed,
            deal: SeedDeal::One,
        });
    }

    // deal-2: survive the loss of any one neighbour (bpv1-67).
    if shares.len() >= 2 {
        for stripe in 0..coding.stripes {
            let real: Vec<u32> = parts
                .iter()
                .copied()
                .filter(|&index| stripe_of(index) == stripe)
                .collect();
            // Widening usize to u32 is lossless: a stripe has at most `MAX_N` parts.
            let padding = n - real.len() as u32;
            for lost in 0..shares.len() {
                loop {
                    let others = real
                        .iter()
                        .filter(|index| {
                            shares
                                .iter()
                                .enumerate()
                                .any(|(at, share)| at != lost && share.held.contains(index))
                        })
                        .count() as u32;
                    if others + padding >= k {
                        break;
                    }
                    let index = *real
                        .iter()
                        .find(|index| {
                            shares[lost].held.contains(index)
                                && shares
                                    .iter()
                                    .enumerate()
                                    .all(|(at, share)| at == lost || !share.held.contains(index))
                        })
                        .expect("the lost neighbour holds the parts the others lack");
                    let lost_peer = shares[lost].peer;
                    let at = earliest(&shares, index, &|share| share.peer != lost_peer)
                        .expect("deal-2 runs with at least two neighbours");
                    shares[at].give(coding, index, false);
                    plan.covering.push(PlannedSeed {
                        peer: shares[at].peer,
                        index,
                        pushed: false,
                        deal: SeedDeal::Two,
                    });
                }
            }
        }
    }

    for share in &shares {
        let finish = if share.pushed_bytes == 0 {
            paths.seed_time(&share.peer, share.bytes)
        } else {
            paths.push_arrival(&share.peer, share.bytes, share.pushed_bytes)
        };
        let pushed: u32 = share.pushed_in.values().sum();
        let held: u32 = share.held_in.values().sum();
        plan.holders.push(HolderPlan {
            peer: share.peer,
            // Widening u32 to usize is lossless on supported targets.
            pushed: pushed as usize,
            // Widening u32 to usize is lossless on supported targets.
            other: (held - pushed) as usize,
            push_rate: paths.push_rate(&share.peer),
            finish,
        });
    }

    if !direct_copies {
        return plan;
    }

    // Each holder's push starts behind all its deal-1 and deal-2 seeds.
    let push_at: HashMap<u32, Duration> = pushed_by
        .iter()
        .map(|(&index, &(at, through))| {
            let share = &shares[at];
            (index, paths.push_arrival(&share.peer, share.bytes, through))
        })
        .collect();
    // deal-3: direct copies of the parts each neighbour lacks, latest
    // holder push first. A part nobody pushes never arrives by push.
    let mut order = parts.clone();
    order.sort_by_key(|index| {
        (
            Reverse(push_at.get(index).copied().unwrap_or(Duration::MAX)),
            *index,
        )
    });
    let mut closest: Vec<usize> = (0..shares.len()).collect();
    closest.sort_by_key(|&at| {
        let share = &shares[at];
        (
            paths.seed_time(&share.peer, share.bytes),
            *share.peer.as_bytes(),
        )
    });
    for at in closest {
        for &index in &order {
            let share = &shares[at];
            if share.held.contains(&index) || Share::count(&share.held_in, stripe_of(index)) >= k {
                continue;
            }
            shares[at].give(coding, index, false);
            plan.holders[at].other += 1;
            plan.direct.push(PlannedSeed {
                peer: shares[at].peer,
                index,
                pushed: false,
                deal: SeedDeal::Direct,
            });
        }
    }
    plan
}

/// Computes the canonical `body_identity = BLAKE3(u32_le(tx_count) || body_bytes)`
/// used to detect body changes versus header-only template updates (`opt-8`).
pub fn compute_body_identity(tx_count: u32, body_bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&tx_count.to_le_bytes());
    hasher.update(body_bytes);
    *hasher.finalize().as_bytes()
}
