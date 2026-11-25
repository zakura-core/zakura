//! Decoder 3 of the `bpv1_block` fuzz target (`tool-2`).
//!
//! The first byte picks the profile (bit 0 set: `W2`, else `TINY`), the
//! layout of each of the three blocks (bits 1–2, 3–4 and 5–6, indexing
//! [`geometries`]), and with bit 7 a table of two slots and a part budget of
//! `s_max` bytes, so eviction and budget refusals fire. Every following three
//! bytes are one operation `(tag, a, b)`; `tag % OPS` picks it.
//!
//! Operations drive every [`BlockInput`] and answer the table's own work
//! from a ledger of the work ids it issued: header admission, unknown-parent
//! checks, commitment checks, stripe jobs, and verifier submissions. A
//! completion may answer current work, repeat a finished one, or name work
//! the table never issued. A stripe job's answer comes from the production
//! decoder on the parts the job carried, or fails as a canceled job does.
//!
//! Block `i` sits at height `i + 1` and its parent is block `i - 1`, so
//! `MissingParent` and `ParentAdmitted` name real chain neighbours.

use std::time::Duration;

use zakura_chain::block;
use zakura_quic::NodeId;
use zakura_rt::time::Instant;

use super::{
    BlockConfig, BlockInput, BlockOutput, BlockTable, HeaderAdmissionResult, MetaEnvelope,
    PullPeerState, ReleaseReason, VerifierVerdict, WorkId,
};
use crate::zakura::bpv1::{
    close::Bpv1CloseRow,
    codec::decode_and_check_stripe,
    commitment::CoinbaseBinding,
    fuzz::{
        fuzz_node, geometries,
        quality::{hit_block_step, phase_name, Reached},
        Geometry,
    },
    paths::{PathEstimate, RateSample},
    peer::PartAuth,
    profile::Profile,
    tree::VerifiedPart,
    unknown_parent::UnknownParentVerdict,
};

/// The hash of the replay chain's block at `height`: block `i` sits at
/// height `i + 1`, height 0 is its root `[0; 32]`, and every other height
/// carries a hash no replay block has.
fn chain_hash(blocks: &[FuzzBlock], height: u32) -> block::Hash {
    if height == 0 {
        return block::Hash([0; 32]);
    }
    blocks.iter().find(|b| b.height == height).map_or_else(
        || {
            let mut hash = [0xE0; 32];
            hash[..4].copy_from_slice(&height.to_le_bytes());
            block::Hash(hash)
        },
        |b| b.id,
    )
}

/// Summary returned by [`replay_bpv1_block_bytes`] (`tool-2`).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BlockReplaySummary {
    /// Number of decoded inputs applied.
    pub steps: usize,
    /// Final pending metadata count.
    pub pending_meta: usize,
    /// Final retained part count across active blocks.
    pub retained_parts: usize,
    /// Every input, output, close row, phase and completion kind reached.
    pub reached: Reached,
}

/// Operations decoder 3 reads.
const OPS: u8 = 25;

/// The fuzz property the replay's own ledger checks name.
const BLOCK_REPLAY: &str = "bpv1/block-replay";

/// One block of the replay.
struct FuzzBlock {
    id: block::Hash,
    height: u32,
    parent: block::Hash,
    geometry: Geometry,
}

impl FuzzBlock {
    /// A `HeaderMeta` envelope. `bad_layout` adds a parity part, which breaks
    /// the canonical layout (bpv1-10).
    fn envelope(&self, proposer: NodeId, bad_layout: bool) -> Box<MetaEnvelope> {
        let mut coding = self.geometry.encoded.coding;
        if bad_layout {
            coding.n = coding.n.saturating_add(1);
        }
        Box::new(MetaEnvelope {
            block_id: self.id,
            header_bytes: self.id.0.to_vec(),
            binding: CoinbaseBinding {
                coinbase: vec![0x01],
                txid_siblings: Vec::new(),
                auth_siblings: Vec::new(),
                light_client_root: [0; 32],
            },
            coding,
            part_root: self.geometry.encoded.part_root,
            proposer_node: proposer,
        })
    }

    /// Wire indices `0..8` whose bit is set in `mask`. Indices past the
    /// layout stay in when `a` bit 7 is set, so range checks see them.
    fn indices(&self, mask: u8, out_of_range: bool) -> Vec<u32> {
        let total = self.geometry.encoded.coding.total_parts().unwrap_or(0);
        (0..8u32)
            .filter(|i| mask & (1 << i) != 0 && (out_of_range || *i < total))
            .collect()
    }

    fn stripe(&self, byte: u8) -> u32 {
        u32::from(byte)
            .checked_rem(self.geometry.encoded.coding.stripes.saturating_add(1))
            .unwrap_or(0)
    }
}

/// What a work id asked the shell to do.
#[derive(Clone, Debug)]
enum Work {
    Header {
        block: usize,
    },
    UnknownParent,
    Commitment,
    Stripe {
        block_id: block::Hash,
        stripe: u32,
        result: Result<Vec<VerifiedPart>, ()>,
    },
    Verify {
        block_id: block::Hash,
    },
}

impl Work {
    fn kind(&self) -> u8 {
        match self {
            Work::Header { .. } => 0,
            Work::UnknownParent => 1,
            Work::Commitment => 2,
            Work::Stripe { .. } => 3,
            Work::Verify { .. } => 4,
        }
    }
}

/// The table's work, recorded from its outputs.
#[derive(Default)]
struct Ledger {
    /// Every issued work id with its work, in issue order.
    issued: Vec<(WorkId, Work)>,
    /// Work ids the replay already answered.
    answered: Vec<WorkId>,
    /// Blocks the table retired. None may be admitted again.
    retired: Vec<block::Hash>,
    /// Blocks holding a body claim.
    claimed: Vec<block::Hash>,
}

impl Ledger {
    /// Issued work of `kind`, picked by `pick`, recording whether the answer
    /// is current or stale. `None` when no such work was issued.
    fn answer(&mut self, kind: u8, pick: u8, reached: &mut Reached) -> Option<(WorkId, Work)> {
        let of_kind: Vec<&(WorkId, Work)> = self
            .issued
            .iter()
            .filter(|(_, w)| w.kind() == kind)
            .collect();
        let (id, work) = (*of_kind.get(usize::from(pick).checked_rem(of_kind.len())?)?).clone();
        if self.answered.contains(&id) {
            reached.hit("work:stale");
        } else {
            reached.hit("work:current");
            self.answered.push(id);
        }
        Some((id, work))
    }
}

/// Deterministically replays arbitrary fuzz bytes through a fresh
/// [`BlockTable`], asserting its invariants and the replay's ledger checks
/// after every step (`tool-2`).
pub fn replay_bpv1_block_bytes(bytes: &[u8]) -> BlockReplaySummary {
    let (head, ops) = bytes.split_first().unwrap_or((&0, &[]));
    let profile = if head & 1 != 0 {
        Profile::W2
    } else {
        Profile::TINY
    };
    let mut config = BlockConfig::for_profile(profile);
    if head & 0x80 != 0 {
        config.max_concurrent_blocks = 2;
        config.part_buffer_bytes = u64::from(profile.s_max);
    }
    let peers: [NodeId; 3] = [fuzz_node(1), fuzz_node(2), fuzz_node(3)];
    let blocks: Vec<FuzzBlock> = (0..3u8)
        .map(|i| {
            let mut layouts = geometries(&profile, i);
            let pick = usize::from(head >> (1 + 2 * i)) % layouts.len();
            FuzzBlock {
                id: block::Hash([0x10 * (i + 1); 32]),
                height: u32::from(i) + 1,
                parent: block::Hash([0x10 * i; 32]),
                geometry: layouts.swap_remove(pick),
            }
        })
        .collect();
    let mut table = BlockTable::new(config);
    let mut now = Instant::now();
    let mut ledger = Ledger::default();
    let mut summary = BlockReplaySummary::default();
    for block in &blocks {
        summary.reached.hit(block.geometry.name);
    }

    for op in ops.chunks(3) {
        let tag = op[0] % OPS;
        let a = op.get(1).copied().unwrap_or(0);
        let b = op.get(2).copied().unwrap_or(0);
        let block_at = usize::from(a >> 2) % blocks.len();
        let block = &blocks[block_at];
        let peer = peers[usize::from(a) % peers.len()];
        // `a` bit 7 marks every peer unable to pull, so plans find no peer.
        let peer_states: Vec<PullPeerState> = peers
            .iter()
            .map(|&p| PullPeerState {
                peer: p,
                ahead_bytes: u64::from(b & 3) * 1_000,
                can_pull: a & 0x80 == 0,
                window_remaining: u32::from(b >> 6) + 1,
            })
            .collect();
        let reached = &mut summary.reached;
        let input = match tag {
            // The tip's hash is the chain's block at that height, or with
            // `a` bit 7 a fork's, so no block extends it.
            0 => {
                let height = u32::from(b % 40);
                BlockInput::TipHeight {
                    height,
                    hash: if a & 0x80 != 0 {
                        block::Hash([0xF0; 32])
                    } else {
                        chain_hash(&blocks, height)
                    },
                }
            }
            1 => BlockInput::ParentAdmitted {
                hash: block.id,
                height: block.height,
            },
            // `b` bit 0 sends a second session's copy, bit 1 a bad layout.
            2 => BlockInput::MetaReceived {
                envelope: block.envelope(peers[usize::from(b >> 2) % 3], b & 2 != 0),
                from_peer: peer,
                session_gen: 1 + u64::from(b & 1),
            },
            3 => match ledger.answer(0, b, reached) {
                Some((work_id, Work::Header { block })) => BlockInput::HeaderAdmitted {
                    work_id,
                    result: match a % 4 {
                        // `a` bit 7 admits the header on a fork: its parent
                        // is not the chain's block below it.
                        0 => HeaderAdmissionResult::Admitted {
                            height: blocks[block].height,
                            parent_hash: if a & 0x80 != 0 {
                                block::Hash([0xF1; 32])
                            } else {
                                blocks[block].parent
                            },
                        },
                        1 => HeaderAdmissionResult::MissingParent {
                            parent_hash: blocks[block].parent,
                        },
                        2 => HeaderAdmissionResult::PeerInvalid,
                        _ => HeaderAdmissionResult::LocalError,
                    },
                },
                _ => BlockInput::HeaderAdmitted {
                    work_id: unknown_work(reached, b),
                    result: HeaderAdmissionResult::LocalError,
                },
            },
            4 => BlockInput::UnknownParentChecked {
                work_id: ledger
                    .answer(1, b, reached)
                    .map_or_else(|| unknown_work(reached, b), |(id, _)| id),
                verdict: [
                    UnknownParentVerdict::Park,
                    UnknownParentVerdict::Drop,
                    UnknownParentVerdict::Invalid(Bpv1CloseRow::Row3InvalidHeader),
                    UnknownParentVerdict::Invalid(Bpv1CloseRow::Row4BadMetaOrCommitment),
                ][usize::from(a % 4)],
            },
            5 => BlockInput::CommitmentChecked {
                work_id: ledger
                    .answer(2, b, reached)
                    .map_or_else(|| unknown_work(reached, b), |(id, _)| id),
                ok: a & 1 == 0,
                peers: peers.to_vec(),
                rng_seed: u64::from(b),
            },
            // This node proposes the block with every part it encoded.
            6 => BlockInput::BlockAdmittedLocal {
                height: block.height,
                envelope: block.envelope(fuzz_node(0), false),
                parts: block.geometry.encoded.parts.clone(),
            },
            // `b` bit 0 sets the relay flag; the proposer sends relay flags.
            7 => BlockInput::PushFlagged {
                peer: if b & 1 != 0 { peers[0] } else { peer },
                block_id: block.id,
                indices: block.indices(b, a & 0x80 != 0),
                relay: b & 1 != 0,
            },
            8 => BlockInput::PathObserved {
                peer,
                estimate: PathEstimate {
                    min_rtt: (b & 1 == 0).then(|| Duration::from_millis(u64::from(b) + 1)),
                    rate_in: (b & 2 == 0).then(|| (u64::from(a) + 1) * 50_000),
                    rate_out: (b & 4 == 0).then(|| (u64::from(a) + 1) * 50_000),
                },
            },
            9 => BlockInput::RateSample {
                peer,
                sample: RateSample {
                    bytes: u64::from(b) * 512,
                    sent_at: now,
                    first_at: now + Duration::from_millis(u64::from(a & 0x0F)),
                    last_at: now + Duration::from_millis(u64::from(a & 0x0F) + u64::from(b)),
                },
            },
            // `b` bit 0 ends one session's unanswered metadata (`b` bit 1
            // picks its generation, as `MetaReceived` sends it); otherwise
            // the peer's last session ends.
            10 if b & 1 != 0 => BlockInput::MetaSessionEnded {
                peer,
                session_gen: 1 + u64::from(b >> 1 & 1),
            },
            10 => BlockInput::SessionEnded { peer, peer_states },
            11 => BlockInput::PeerBlockDone {
                peer,
                block_id: block.id,
                peer_states,
            },
            12 => BlockInput::PeerAnnounced {
                peer,
                block_id: block.id,
                indices: block.indices(b, false),
                peer_states,
            },
            13 => BlockInput::PlanPulls {
                block_id: block.id,
                stripe: block.stripe(b),
                peer_states,
            },
            14 => BlockInput::ReleaseClaims {
                peer,
                block_id: block.id,
                stripe: block.stripe(b >> 4),
                slots: (0..4u32).filter(|i| b & (1 << i) != 0).collect(),
                reason: [
                    ReleaseReason::Late,
                    ReleaseReason::Failed,
                    ReleaseReason::Refused,
                ][usize::from(a >> 5 & 3) % 3],
                peer_states,
            },
            // The IO shell charges a part before proof, then inserts it once
            // the proof passes. A refused charge leaves the insert a no-op.
            15 => {
                let parts = &block.geometry.encoded.parts;
                let Some(part) = parts.get(usize::from(b) % parts.len().max(1)).cloned() else {
                    continue;
                };
                // usize -> u64 widens on every supported target.
                let len = part.payload().len() as u64;
                let charge = table.charge_part(now, &block.id, part.index(), peer, len);
                reached.hit(if charge.is_ok() {
                    "charge:accepted"
                } else {
                    "charge:refused"
                });
                BlockInput::InsertPart {
                    block_id: block.id,
                    part,
                    from_peer: peer,
                    auth: [PartAuth::Seed, PartAuth::Push, PartAuth::Want(1)]
                        [usize::from(a >> 5 & 3) % 3],
                    peer_states,
                }
            }
            // A failed proof releases the charge the shell took before it.
            16 => {
                let parts = &block.geometry.encoded.parts;
                let index = parts
                    .get(usize::from(b) % parts.len().max(1))
                    .map_or(u32::from(b), |p| p.index());
                // `s_max` bounds a part's charge.
                let len = u64::from(profile.s_max);
                let _ = table.charge_part(now, &block.id, index, peer, len);
                BlockInput::ReleasePartCharge {
                    block_id: block.id,
                    index,
                    from_peer: peer,
                }
            }
            17 => BlockInput::PartUploaded {
                block_id: block.id,
                bytes: u64::from(b) * 64,
            },
            // The decoder's answer, or a failure when `a` bit 6 is set, as a
            // canceled or panicked job reports.
            18 => match ledger.answer(3, b, reached) {
                Some((
                    work_id,
                    Work::Stripe {
                        block_id,
                        stripe,
                        result,
                    },
                )) => BlockInput::StripeJobDone {
                    work_id,
                    block_id,
                    stripe,
                    result: if a & 0x40 != 0 { Err(()) } else { result },
                },
                _ => BlockInput::StripeJobDone {
                    work_id: unknown_work(reached, b),
                    block_id: block.id,
                    stripe: 0,
                    result: Err(()),
                },
            },
            19 => match ledger.answer(4, b, reached) {
                Some((work_id, Work::Verify { block_id })) => {
                    BlockInput::BlockAssembled { work_id, block_id }
                }
                _ => BlockInput::BlockAssembled {
                    work_id: unknown_work(reached, b),
                    block_id: block.id,
                },
            },
            20 => {
                let verdict = [
                    VerifierVerdict::Valid,
                    VerifierVerdict::RootMismatch,
                    VerifierVerdict::LocalCapacity,
                    VerifierVerdict::Invalid,
                    VerifierVerdict::Duplicate,
                ][usize::from(a >> 5 & 7) % 5];
                match ledger.answer(4, b, reached) {
                    Some((work_id, Work::Verify { block_id })) => BlockInput::VerifierVerdict {
                        work_id,
                        block_id,
                        verdict,
                    },
                    _ => BlockInput::VerifierVerdict {
                        work_id: unknown_work(reached, b),
                        block_id: block.id,
                        verdict,
                    },
                }
            }
            // Block sync ends a claim it was given, or any block when no
            // claim is held.
            21 => BlockInput::FallbackDone {
                block_id: ledger
                    .claimed
                    .get(usize::from(b) % ledger.claimed.len().max(1))
                    .copied()
                    .unwrap_or(block.id),
            },
            // Up to 25.5 s in 100 ms steps: stalls, repair rounds, ageing.
            22 => {
                now += Duration::from_millis(u64::from(b) * 100);
                BlockInput::Tick { peer_states }
            }
            // Up to 255 s: the verification deadline.
            23 => {
                now += Duration::from_secs(u64::from(b));
                BlockInput::Tick { peer_states }
            }
            // This node's commit of a block fails; only a proposed block retires.
            _ => BlockInput::ProposedCommitFailed { block_id: block.id },
        };
        step(&mut table, now, input, &blocks, &mut ledger, &mut summary);
    }

    summary.pending_meta = table.pending_meta_len();
    summary.retained_parts = table.retained_part_count();
    summary
}

/// A work id the table never issued: its ids count up from one.
fn unknown_work(reached: &mut Reached, b: u8) -> WorkId {
    reached.hit("work:unknown");
    WorkId(u64::MAX - u64::from(b))
}

/// Steps the table, checks its invariants and the ledger rules, and records
/// its new work.
fn step(
    table: &mut BlockTable,
    now: Instant,
    input: BlockInput,
    blocks: &[FuzzBlock],
    ledger: &mut Ledger,
    summary: &mut BlockReplaySummary,
) {
    let stepped = table.step(now, input.clone());
    table.assert_invariants();
    summary.steps += 1;
    hit_block_step(&mut summary.reached, &input, &stepped.outputs);
    for output in stepped.outputs {
        let issued = match output {
            BlockOutput::AdmitHeader {
                work_id, block_id, ..
            } => blocks
                .iter()
                .position(|blk| blk.id == block_id)
                .map(|block| (work_id, Work::Header { block })),
            BlockOutput::CheckUnknownParent { work_id, .. } => Some((work_id, Work::UnknownParent)),
            BlockOutput::VerifyCommitment { work_id, .. } => Some((work_id, Work::Commitment)),
            BlockOutput::StartStripeJob {
                work_id,
                block_id,
                stripe,
                coding,
                enc_id,
                held,
                ..
            } => Some((
                work_id,
                Work::Stripe {
                    block_id,
                    stripe,
                    result: decode_and_check_stripe(&coding, &enc_id, stripe, &held)
                        .map_err(|_| ()),
                },
            )),
            BlockOutput::SubmitVerifier {
                work_id, block_id, ..
            } => Some((work_id, Work::Verify { block_id })),
            // A peer's `HeaderMeta` never brings a retired block back, even
            // after its tombstone is purged: `top` only rises, so a purged
            // height stays below the window. This node's own solved block
            // skips the window check (`BlockAdmittedLocal`), so it may.
            BlockOutput::PublishAdmitted {
                admitted,
                from_peer: Some(_),
            } => {
                assert!(
                    !ledger.retired.contains(&admitted.block_id),
                    "{BLOCK_REPLAY}: bpv1-46 retired block {:?} was admitted again",
                    admitted.block_id
                );
                None
            }
            BlockOutput::PublishRetire { block_id, .. } => {
                ledger.retired.push(block_id);
                None
            }
            BlockOutput::ClaimBody { block_id, .. } => {
                if !ledger.claimed.contains(&block_id) {
                    ledger.claimed.push(block_id);
                }
                None
            }
            BlockOutput::ExtendBodyClaim { block_id, .. } => {
                assert!(
                    ledger.claimed.contains(&block_id),
                    "{BLOCK_REPLAY}: bpv1-44 extended body claim of {block_id:?} it does not hold"
                );
                None
            }
            BlockOutput::ReleaseBodyClaim { block_id, .. } => {
                let held = ledger.claimed.iter().position(|id| *id == block_id);
                assert!(
                    held.is_some(),
                    "{BLOCK_REPLAY}: bpv1-44 released body claim of {block_id:?} it never took"
                );
                ledger.claimed.retain(|id| *id != block_id);
                None
            }
            _ => None,
        };
        if let Some((work_id, work)) = issued {
            // bpv1-63: every job carries an id no earlier job had.
            assert!(
                ledger.issued.iter().all(|(id, _)| *id != work_id),
                "{BLOCK_REPLAY}: bpv1-63 work id {work_id:?} issued twice"
            );
            ledger.issued.push((work_id, work));
        }
    }
    for blk in blocks {
        if let Some(phase) = table.block_phase(&blk.id) {
            summary.reached.hit(phase_name(phase));
        }
        // A retired block holds no body claim (bpv1-44, bpv1-46).
        if table.block_phase(&blk.id) == Some(super::BlockPhase::Retired) {
            assert!(
                !ledger.claimed.contains(&blk.id),
                "{BLOCK_REPLAY}: retired block {:?} still holds its body claim",
                blk.id
            );
        }
    }
}
