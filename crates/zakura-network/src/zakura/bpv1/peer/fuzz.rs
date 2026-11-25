//! Decoder 2 of the `bpv1_peer` fuzz target (`tool-2`).
//!
//! The first byte picks the profile (bit 0 set: `W2`, else `TINY`) and the
//! layout of each of the three blocks (bits 1–2, 3–4 and 5–6, indexing
//! [`geometries`]). Every following three bytes are one operation
//! `(tag, a, b)`; `tag % OPS` picks it. Operations drive every
//! [`PeerInput`] and answer the session's own work from ledgers the replay
//! keeps: the proof tokens of `VerifyPart`, the `Want`s it serves and sends,
//! and the `WantEnd`s its data writer holds. A completion may name current,
//! finished, or never-issued work.
//!
//! `SessionEnd` ends every replay and is never an operation: the IO shell
//! sends nothing to a session after it.

use std::time::Duration;

use zakura_chain::block;
use zakura_rt::time::Instant;

use super::{
    BlockAdmitted, DropReason, PartToken, PeerConfig, PeerInput, PeerOutput, PeerState,
    PullGranted, RecvHave, RecvPartPrefix, RecvWant, RecvWantEnd, Retire, Seed, Seeds, ServeDone,
    ServePartQueued,
};
use crate::zakura::bpv1::{
    commitment::CoinbaseBinding,
    fuzz::{
        fuzz_node, geometries,
        quality::{hit_peer_step, Reached},
        Geometry,
    },
    paths::Arrival,
    profile::Profile,
    tree::VerifiedPart,
    wire::{encode_part_frame, Bpv1Message, HaveFlags, WantBitmap, WantEndReason},
};

/// Summary returned by [`replay_bpv1_peer_bytes`] (`tool-2`, `p2p-fuzz`).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PeerReplaySummary {
    /// Total inputs replayed, `SessionEnd` included.
    pub steps: usize,
    /// Protocol close row triggered, if any.
    pub closed_row: Option<u8>,
    /// Every input, output, close row and completion kind the replay reached.
    pub reached: Reached,
}

/// Operations decoder 2 reads.
const OPS: u8 = 28;

/// The fuzz property the replay's own ledger checks name.
const PEER_REPLAY: &str = "bpv1/peer-replay";

/// One block of the replay: its id and layout.
struct FuzzBlock {
    id: block::Hash,
    geometry: Geometry,
}

impl FuzzBlock {
    fn part(&self, index: u32) -> Option<&VerifiedPart> {
        usize::try_from(index)
            .ok()
            .and_then(|i| self.geometry.encoded.parts.get(i))
    }

    /// A `HeaderMeta` for this block from `proposer`. `bad_layout` adds a
    /// parity part, which breaks the canonical layout (bpv1-10).
    fn meta(&self, proposer: zakura_quic::NodeId, bad_layout: bool) -> Bpv1Message {
        let mut coding = self.geometry.encoded.coding;
        if bad_layout {
            coding.n = coding.n.saturating_add(1);
        }
        Bpv1Message::HeaderMeta {
            header: self.id.0.to_vec(),
            binding: CoinbaseBinding {
                coinbase: vec![0x01],
                txid_siblings: Vec::new(),
                auth_siblings: Vec::new(),
                light_client_root: [0; 32],
            },
            coding,
            part_root: self.geometry.encoded.part_root,
            proposer_node: proposer,
        }
    }

    /// Wire indices `0..8` whose bit is set in `mask`, within the layout.
    fn indices(&self, mask: u8) -> Vec<u32> {
        let total = self.geometry.encoded.coding.total_parts().unwrap_or(0);
        (0..8u32)
            .filter(|i| mask & (1 << i) != 0 && *i < total)
            .collect()
    }

    /// The stripe-relative slots whose bit is set in `mask`, as a bitmap.
    fn bitmap(&self, mask: u8) -> WantBitmap {
        let mut bitmap = WantBitmap::EMPTY;
        for slot in (0..8u32).filter(|i| mask & (1 << i) != 0) {
            if slot < self.geometry.encoded.coding.n.max(1) {
                bitmap.set(slot);
            }
        }
        bitmap
    }

    fn stripe(&self, byte: u8) -> u32 {
        let stripes = self.geometry.encoded.coding.stripes;
        // A set high bit names a stripe outside the layout.
        if byte & 0x80 != 0 {
            stripes.saturating_add(u32::from(byte & 0x7F))
        } else {
            u32::from(byte).checked_rem(stripes).unwrap_or(0)
        }
    }
}

/// A `Want` the session answers as server, as `ServeWant` handed it.
struct Serve {
    want_id: u32,
    block_id: block::Hash,
    indices: Vec<u32>,
    queued: u16,
}

/// Replays arbitrary fuzz bytes as a sequence of [`PeerInput`]s against a
/// fresh [`PeerState`], asserting its invariants after every step, the
/// replay's ledger checks, and a full drain on `SessionEnd` (`tool-2`).
pub fn replay_bpv1_peer_bytes(bytes: &[u8]) -> PeerReplaySummary {
    let (head, ops) = bytes.split_first().unwrap_or((&0, &[]));
    let profile = if head & 1 != 0 {
        Profile::W2
    } else {
        Profile::TINY
    };
    let local = fuzz_node(1);
    let remote = fuzz_node(2);
    let third = fuzz_node(3);
    let blocks: Vec<FuzzBlock> = (0..3u8)
        .map(|i| {
            let mut layouts = geometries(&profile, i);
            let pick = usize::from(head >> (1 + 2 * i)) % layouts.len();
            FuzzBlock {
                id: block::Hash([0x11 * (i + 1); 32]),
                geometry: layouts.swap_remove(pick),
            }
        })
        .collect();
    let mut peer = PeerState::new(PeerConfig::for_profile(profile), local, remote, 1);
    let mut now = Instant::now();
    let mut ledger = Ledger::default();
    let mut summary = PeerReplaySummary::default();
    for block in &blocks {
        summary.reached.hit(block.geometry.name);
    }

    for op in ops.chunks(3) {
        let tag = op[0] % OPS;
        let a = op.get(1).copied().unwrap_or(0);
        let b = op.get(2).copied().unwrap_or(0);
        let block = &blocks[usize::from(a) % blocks.len()];
        let nodes = [local, remote, third];
        let reached = &mut summary.reached;
        let Some(input) = decode(
            tag,
            a,
            b,
            block,
            &blocks,
            &mut ledger,
            reached,
            &mut now,
            nodes,
        ) else {
            continue;
        };
        step(&mut peer, now, input, &blocks, &mut ledger, &mut summary);
    }

    summary.closed_row = peer.close_cause().map(|c| {
        // A fieldless `repr(u8)` enum's discriminant is its declared row number.
        c.row as u8
    });
    step(
        &mut peer,
        now,
        PeerInput::SessionEnd,
        &blocks,
        &mut ledger,
        &mut summary,
    );
    peer.assert_drained();
    summary
}

/// The session's work the replay answers, recorded from its outputs.
#[derive(Default)]
struct Ledger {
    /// Every `VerifyPart` token, in issue order.
    tokens: Vec<PartToken>,
    /// Tokens the replay already completed.
    finished: Vec<PartToken>,
    /// `Want`s the session serves.
    serves: Vec<Serve>,
    /// `Want`s the session sent: `(want_id, requested wire indices)`.
    wants_out: Vec<(u32, Vec<u32>)>,
    /// `WantEnd`s queued that the data writer has not reported written.
    unwritten_ends: u32,
}

impl Ledger {
    /// Token `pick` of the issued ones, recording whether it is current or
    /// finished, or a never-issued token when none was issued or `fresh`.
    fn token(
        &mut self,
        pick: u8,
        fresh: bool,
        block: &FuzzBlock,
        reached: &mut Reached,
    ) -> PartToken {
        if fresh || self.tokens.is_empty() {
            reached.hit("work:unknown");
            return PartToken {
                session_gen: 1,
                block_id: block.id,
                index: u32::from(pick & 7),
                attempt: u64::MAX - u64::from(pick),
            };
        }
        let token = self.tokens[usize::from(pick) % self.tokens.len()];
        if self.finished.contains(&token) {
            reached.hit("work:stale");
        } else {
            reached.hit("work:current");
            self.finished.push(token);
        }
        token
    }
}

/// Decodes one operation into its input, or `None` for a completion with no
/// work to answer.
#[allow(clippy::too_many_arguments)]
fn decode(
    tag: u8,
    a: u8,
    b: u8,
    block: &FuzzBlock,
    blocks: &[FuzzBlock],
    ledger: &mut Ledger,
    reached: &mut Reached,
    now: &mut Instant,
    [local, remote, third]: [zakura_quic::NodeId; 3],
) -> Option<PeerInput> {
    let input = match tag {
        // `b` picks the proposer: the remote, a third node, or this node.
        0 => {
            let proposer = [remote, third, local][usize::from(b % 3)];
            PeerInput::BlockAdmitted(BlockAdmitted {
                block_id: block.id,
                coding: block.geometry.encoded.coding,
                proposer_node: proposer,
                header_meta: (b & 4 == 0).then(|| Box::new(block.meta(proposer, false))),
                as_proposer: proposer == local,
                have_held_until: (b & 8 != 0).then(|| *now + Duration::from_millis(500)),
            })
        }
        1 => PeerInput::RecvHeaderMeta {
            block_id: block.id,
            message: Box::new(block.meta([remote, third][usize::from(b & 1)], b & 2 != 0)),
        },
        // A part as an honest sender writes it, or an index past the layout.
        2 => {
            let index = u32::from(b);
            let payload = block
                .part(index)
                .map(|part| encode_part_frame(block.id, part).payload)
                .unwrap_or_else(|| vec![0; 36]);
            PeerInput::RecvPartPrefix(RecvPartPrefix {
                block_id: block.id,
                index,
                payload,
            })
        }
        // A real part whose frame is cut short or runs long by `b & 7 + 1` bytes.
        3 => {
            let index = u32::from(b >> 3);
            let mut payload = block
                .part(index)
                .map(|part| encode_part_frame(block.id, part).payload)
                .unwrap_or_else(|| vec![0; 36]);
            let delta = usize::from(b & 7) + 1;
            if a & 0x80 != 0 {
                payload.resize(payload.len() + delta, 0);
            } else {
                payload.truncate(payload.len().saturating_sub(delta).max(36));
            }
            PeerInput::RecvPartPrefix(RecvPartPrefix {
                block_id: block.id,
                index,
                payload,
            })
        }
        // A proof worker verifies the token's part. A token past the
        // layout has no part to verify, so the worker fails it.
        4 => {
            let token = ledger.token(b, a & 0x80 != 0, block, reached);
            let owner = blocks.iter().find(|blk| blk.id == token.block_id);
            match owner.and_then(|blk| blk.part(token.index)) {
                Some(part) => PeerInput::PartVerified {
                    token,
                    part: part.clone(),
                },
                None => PeerInput::PartFailed { token },
            }
        }
        5 => PeerInput::PartFailed {
            token: ledger.token(b, a & 0x80 != 0, block, reached),
        },
        6 => PeerInput::PartRefused {
            token: ledger.token(b, a & 0x80 != 0, block, reached),
            reason: if a & 0x40 != 0 {
                DropReason::NotAdmitted
            } else {
                DropReason::LocalCapacity
            },
        },
        7 => PeerInput::Ready {
            block_id: block.id,
            indices: block.indices(b),
            push: a & 0x40 != 0,
        },
        // `a` bits 3–4 pick the flags. Bit 6 lists the indices out of order,
        // which closes (bpv1-25).
        8 => {
            let mut indices = block.indices(b);
            if a & 0x40 != 0 {
                indices.reverse();
            }
            let flags =
                [HaveFlags::Plain, HaveFlags::Push, HaveFlags::Relay][usize::from(a >> 3 & 3) % 3];
            PeerInput::RecvHave(RecvHave {
                block_id: block.id,
                flags,
                frame_bytes: 39 + 4 * indices.len(),
                indices,
            })
        }
        9 => PeerInput::PullGranted(PullGranted {
            block_id: block.id,
            stripe: block.stripe(a >> 2),
            requested: block.bitmap(b),
            arrival: Arrival {
                at: *now + Duration::from_millis(50),
                rtt: Duration::from_millis(20),
                rate_bps: 1_000_000,
            },
        }),
        // Seeds for the parts `b` selects, relayed when `a` bit 6 is set;
        // the parts `b` leaves out become direct copies when bit 5 is.
        10 => {
            let seeds = block
                .indices(b)
                .into_iter()
                .filter_map(|i| block.part(i).cloned())
                .map(|part| Seed {
                    part,
                    relay: a & 0x40 != 0,
                })
                .collect();
            let copies = if a & 0x20 != 0 {
                block
                    .indices(!b)
                    .into_iter()
                    .filter_map(|i| block.part(i).cloned())
                    .collect()
            } else {
                Vec::new()
            };
            PeerInput::Seeds(Seeds {
                block_id: block.id,
                seeds,
                copies,
            })
        }
        // A zero id when `a` bit 7 is set (row 2 at the decoder; the
        // machine sees it only from a broken decoder).
        11 => PeerInput::RecvWant(RecvWant {
            want_id: if a & 0x80 != 0 {
                0
            } else {
                u32::from(a >> 2 & 3) + 1
            },
            block_id: block.id,
            stripe: block.stripe(a >> 4 & 0x83),
            requested: block.bitmap(b),
        }),
        12 => {
            let serve = pick_mut(&mut ledger.serves, b)?;
            let index = serve
                .indices
                .get(usize::from(a) % serve.indices.len().max(1))
                .copied()
                .unwrap_or(u32::from(a));
            serve.queued = serve.queued.saturating_add(1);
            PeerInput::ServePartQueued(ServePartQueued {
                want_id: serve.want_id,
                block_id: serve.block_id,
                index,
            })
        }
        // The count the server queued, or `a`'s low bits when bit 7 is set.
        13 => {
            let at = usize::from(b) % ledger.serves.len().max(1);
            if ledger.serves.is_empty() {
                return None;
            }
            let serve = ledger.serves.remove(at);
            PeerInput::ServeDone(ServeDone {
                want_id: serve.want_id,
                sent: if a & 0x80 != 0 {
                    u16::from(a & 3)
                } else {
                    serve.queued
                },
                reason: [
                    WantEndReason::Served,
                    WantEndReason::UnknownBlock,
                    WantEndReason::Retired,
                ][usize::from(a >> 5 & 3) % 3],
            })
        }
        14 => {
            ledger.unwritten_ends = ledger.unwritten_ends.saturating_sub(1);
            PeerInput::WantEndWritten
        }
        // The count of requested indices, or `b`'s low bits when `a` bit 7
        // is set; a made-up id when bit 6 is.
        15 => {
            let (want_id, requested) = match pick(&ledger.wants_out, b) {
                Some((id, requested)) if a & 0x40 == 0 => (*id, requested.len()),
                _ => (u32::from(b) + 100, 0),
            };
            let sent = if a & 0x80 != 0 {
                u16::from(b & 3)
            } else {
                u16::try_from(requested).unwrap_or(u16::MAX)
            };
            PeerInput::RecvWantEnd(RecvWantEnd { want_id, sent })
        }
        16 => PeerInput::LocalStripeDone {
            block_id: block.id,
            stripe: block.stripe(b),
        },
        17 => PeerInput::LocalBlockDone { block_id: block.id },
        18 => PeerInput::RecvStripeDone {
            block_id: block.id,
            stripe: block.stripe(b),
        },
        19 => PeerInput::RecvBlockDone { block_id: block.id },
        20 => PeerInput::Retire(Retire {
            block_id: block.id,
            stripes: block.geometry.encoded.coding.stripes,
            n: block.geometry.encoded.coding.n,
        }),
        21 => PeerInput::MetaRejected { block_id: block.id },
        22 => PeerInput::EvictRetired { block_id: block.id },
        // Up to 25 s in 100 ms steps: late pulls, deadlines, early ageing.
        23 => {
            *now += Duration::from_millis(u64::from(b) * 100);
            PeerInput::Tick
        }
        // Up to 17 min in 4 s steps: the liveness cooldown and its repeat.
        24 => {
            *now += Duration::from_secs(u64::from(b) * 4);
            PeerInput::Tick
        }
        25 => PeerInput::DataIdle,
        26 => PeerInput::ControlStalled,
        _ => {
            *now += crate::zakura::bpv1::wire::HAVE_FLUSH_INTERVAL;
            PeerInput::Tick
        }
    };
    Some(input)
}

fn pick<T>(items: &[T], byte: u8) -> Option<&T> {
    items.get(usize::from(byte).checked_rem(items.len())?)
}

fn pick_mut<T>(items: &mut [T], byte: u8) -> Option<&mut T> {
    let at = usize::from(byte).checked_rem(items.len())?;
    items.get_mut(at)
}

/// Steps `peer`, checks its invariants and the ledger rules, and records the
/// session's new work.
fn step(
    peer: &mut PeerState,
    now: Instant,
    input: PeerInput,
    blocks: &[FuzzBlock],
    ledger: &mut Ledger,
    summary: &mut PeerReplaySummary,
) {
    let verified = match &input {
        PeerInput::PartVerified { token, part } => Some((*token, part.clone())),
        _ => None,
    };
    let stepped = peer.step(now, input.clone());
    peer.assert_invariants();
    summary.steps += 1;
    hit_peer_step(&mut summary.reached, &input, &stepped.outputs);
    for output in stepped.outputs {
        match output {
            // bpv1-63: every proof job carries a token no earlier job had.
            PeerOutput::VerifyPart { token, .. } => {
                assert!(
                    !ledger.tokens.contains(&token),
                    "{PEER_REPLAY}: bpv1-63 a proof job reused token {token:?}"
                );
                ledger.tokens.push(token);
            }
            // A verified part reaches the table only from the proof that
            // verified it.
            PeerOutput::InsertPart { block_id, part, .. } => {
                assert!(
                    verified.as_ref().is_some_and(|(token, proved)| {
                        token.block_id == block_id && *proved == part
                    }),
                    "{PEER_REPLAY}: inserted part {} of {block_id:?} without its proof",
                    part.index()
                );
            }
            PeerOutput::ServeWant {
                want_id,
                block_id,
                stripe,
                eligible_slots,
            } => {
                let coding = blocks
                    .iter()
                    .find(|blk| blk.id == block_id)
                    .map(|blk| blk.geometry.encoded.coding);
                let indices = eligible_slots
                    .iter()
                    .filter_map(|&slot| coding?.wire_index(stripe, slot))
                    .collect();
                ledger.serves.push(Serve {
                    want_id,
                    block_id,
                    indices,
                    queued: 0,
                });
            }
            PeerOutput::SendControl(message) => {
                if let Bpv1Message::Want {
                    want_id,
                    block_id,
                    stripe,
                    requested,
                } = *message
                {
                    let coding = blocks
                        .iter()
                        .find(|blk| blk.id == block_id)
                        .map(|blk| blk.geometry.encoded.coding);
                    let indices = requested
                        .iter()
                        .filter_map(|slot| coding?.wire_index(stripe, slot))
                        .collect();
                    ledger.wants_out.push((want_id, indices));
                }
            }
            PeerOutput::SendWantEnd { .. } => ledger.unwritten_ends += 1,
            _ => {}
        }
    }
}
