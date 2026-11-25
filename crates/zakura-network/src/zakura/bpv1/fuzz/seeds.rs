//! In-code seeds for the BPv1 fuzz targets.
//!
//! Each seed is a short history in its target's decoder format that walks
//! one protocol path end to end: a pull, a serve, a proposal, a repair, an
//! adversary. Together they reach every name [`super::quality`] expects, so
//! a short run starts from every alternative. A campaign writes them to its
//! corpus directory; the repository commits no corpus files.

use zakura_chain::{
    block::{self, genesis::regtest_genesis_block},
    serialization::ZcashSerialize,
};

use super::fuzz_node;
use crate::zakura::{
    bpv1::{
        commitment::CoinbaseBinding,
        profile::Profile,
        wire::{Bpv1Message, HaveFlags, HaveForm, WantBitmap, WantEndReason},
    },
    wire_codec::encode_frame,
};

/// One seed input of a BPv1 fuzz target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Bpv1Seed {
    /// The fuzz target that reads it.
    pub target: &'static str,
    /// The seed's file name in the target's corpus.
    pub name: &'static str,
    /// The input bytes.
    pub bytes: Vec<u8>,
}

/// Every BPv1 seed, for every target.
pub fn seed_inputs() -> Vec<Bpv1Seed> {
    let mut seeds = Vec::new();
    seeds.extend(peer_seeds());
    seeds.extend(block_seeds());
    seeds.extend(mesh_seeds());
    seeds.extend(wire_seeds());
    seeds.extend(codec_seeds());
    seeds
}

/// A replay input: the head byte, then each `(tag, a, b)` operation.
fn ops(head: u8, ops: &[(u8, u8, u8)]) -> Vec<u8> {
    let mut bytes = vec![head];
    for &(tag, a, b) in ops {
        bytes.extend([tag, a, b]);
    }
    bytes
}

fn seed(target: &'static str, name: &'static str, bytes: Vec<u8>) -> Bpv1Seed {
    Bpv1Seed {
        target,
        name,
        bytes,
    }
}

// Decoder 2 of `bpv1_peer`. Block 0 is `a % 3 == 0`.
const PEER_ADMIT: u8 = 0;
const PEER_META: u8 = 1;
const PEER_PART: u8 = 2;
const PEER_PART_LEN: u8 = 3;
const PEER_VERIFIED: u8 = 4;
const PEER_FAILED: u8 = 5;
const PEER_REFUSED: u8 = 6;
const PEER_READY: u8 = 7;
const PEER_HAVE: u8 = 8;
const PEER_GRANT: u8 = 9;
const PEER_SEEDS: u8 = 10;
const PEER_WANT: u8 = 11;
const PEER_QUEUED: u8 = 12;
const PEER_SERVED: u8 = 13;
const PEER_END_WRITTEN: u8 = 14;
const PEER_WANT_END: u8 = 15;
const PEER_LOCAL_STRIPE: u8 = 16;
const PEER_LOCAL_BLOCK: u8 = 17;
const PEER_STRIPE_DONE: u8 = 18;
const PEER_BLOCK_DONE: u8 = 19;
const PEER_RETIRE: u8 = 20;
const PEER_REJECTED: u8 = 21;
const PEER_EVICT: u8 = 22;
const PEER_TICK: u8 = 23;
const PEER_TICK_LONG: u8 = 24;
const PEER_IDLE: u8 = 25;
const PEER_STALLED: u8 = 26;

/// The `TINY` exact layout (index 2) for block 0, empty for the others.
const PEER_EXACT: u8 = 2 << 1;

fn peer_seeds() -> Vec<Bpv1Seed> {
    let target = "bpv1_peer";
    vec![
        // A pull: the remote announces, the table grants, parts prove.
        seed(
            target,
            "pull",
            ops(
                PEER_EXACT,
                &[
                    (PEER_ADMIT, 0, 1),
                    (PEER_HAVE, 0, 0x0F),
                    (PEER_GRANT, 0, 0x0F),
                    (PEER_PART, 0, 0),
                    (PEER_PART, 0, 1),
                    (PEER_PART, 0, 2),
                    (PEER_PART, 0, 3),
                    (PEER_VERIFIED, 0, 0),
                    (PEER_VERIFIED, 0, 1),
                    (PEER_FAILED, 0, 2),
                    (PEER_REFUSED, 0x40, 3),
                    (PEER_VERIFIED, 0, 0),
                    (PEER_VERIFIED, 0x80, 0),
                    (PEER_WANT_END, 0, 0),
                    (PEER_TICK, 0, 20),
                    (PEER_LOCAL_STRIPE, 0, 0),
                    (PEER_LOCAL_BLOCK, 0, 0),
                    (PEER_RETIRE, 0, 0),
                    (PEER_EVICT, 0, 0),
                ],
            ),
        ),
        // A serve: this node holds parts and answers the remote's `Want`.
        seed(
            target,
            "serve",
            ops(
                PEER_EXACT,
                &[
                    (PEER_ADMIT, 0, 0),
                    (PEER_READY, 0, 0x3F),
                    (PEER_WANT, 0x0C, 0x03),
                    (PEER_QUEUED, 0, 0),
                    (PEER_QUEUED, 1, 0),
                    (PEER_SERVED, 0, 0),
                    (PEER_END_WRITTEN, 0, 0),
                    (PEER_STRIPE_DONE, 0, 0),
                    (PEER_BLOCK_DONE, 0, 0),
                    (PEER_TICK_LONG, 0, 200),
                    (PEER_IDLE, 0, 0),
                    (PEER_META, 1, 1),
                    (PEER_REJECTED, 1, 0),
                    (PEER_STALLED, 0, 0),
                ],
            ),
        ),
        // The proposer seeds the remote, relays included, and releases its
        // direct copies to an idle writer.
        seed(
            target,
            "propose",
            ops(
                PEER_EXACT,
                &[
                    (PEER_ADMIT, 0, 2),
                    (PEER_SEEDS, 0x60, 0x03),
                    (PEER_IDLE, 0, 0),
                    (PEER_IDLE, 0, 0),
                    (PEER_TICK, 0, 5),
                ],
            ),
        ),
        // A third node proposes; the remote pushes its flagged parts, then
        // relays, then breaks the order rule.
        seed(
            target,
            "push",
            ops(
                PEER_EXACT,
                &[
                    (PEER_ADMIT, 0, 9),
                    (PEER_HAVE, 0x09, 0x03),
                    (PEER_PART, 0, 0),
                    (PEER_PART, 0, 1),
                    (PEER_PART_LEN, 0x80, 0x10),
                ],
            ),
        ),
        // Early data before admission, then a duplicate `HeaderMeta`.
        seed(
            target,
            "early",
            ops(
                PEER_EXACT,
                &[
                    (PEER_HAVE, 0x12, 0x01),
                    (PEER_PART, 0, 0),
                    (PEER_META, 0, 0),
                    (PEER_META, 0, 0),
                ],
            ),
        ),
        // A `Want` beyond the layout and a bad `WantEnd`.
        seed(
            target,
            "bad-want",
            ops(PEER_EXACT, &[(PEER_ADMIT, 0, 0), (PEER_WANT, 0x80, 0x01)]),
        ),
        seed(
            target,
            "bad-want-end",
            ops(PEER_EXACT, &[(PEER_ADMIT, 0, 1), (PEER_WANT_END, 0x40, 3)]),
        ),
    ]
}

// Decoder 3 of `bpv1_block`. Block `(a >> 2) % 3`, peer `a % 3`.
const BLOCK_TIP: u8 = 0;
const BLOCK_PARENT: u8 = 1;
const BLOCK_META: u8 = 2;
const BLOCK_HEADER: u8 = 3;
const BLOCK_UNKNOWN_PARENT: u8 = 4;
const BLOCK_COMMITMENT: u8 = 5;
const BLOCK_LOCAL: u8 = 6;
const BLOCK_FLAGGED: u8 = 7;
const BLOCK_PATH: u8 = 8;
const BLOCK_RATE: u8 = 9;
const BLOCK_SESSION_END: u8 = 10;
const BLOCK_PEER_DONE: u8 = 11;
const BLOCK_ANNOUNCED: u8 = 12;
const BLOCK_PLAN: u8 = 13;
const BLOCK_RELEASE: u8 = 14;
const BLOCK_INSERT: u8 = 15;
const BLOCK_RELEASE_CHARGE: u8 = 16;
const BLOCK_UPLOADED: u8 = 17;
const BLOCK_STRIPE_DONE: u8 = 18;
const BLOCK_ASSEMBLED: u8 = 19;
const BLOCK_VERDICT: u8 = 20;
const BLOCK_FALLBACK_DONE: u8 = 21;
const BLOCK_TICK: u8 = 22;
const BLOCK_TICK_LONG: u8 = 23;
const BLOCK_COMMIT_FAILED: u8 = 24;

/// The `TINY` exact layout for block 0, empty for the others.
const BLOCK_EXACT: u8 = 2 << 1;

/// Admits block `(a >> 2) % 3` from peer `a % 3`.
fn admit_block(a: u8) -> [(u8, u8, u8); 3] {
    [
        (BLOCK_META, a, 0),
        (BLOCK_HEADER, 0, 0xFF),
        (BLOCK_COMMITMENT, 0, 0xFF),
    ]
}

fn block_seeds() -> Vec<Bpv1Seed> {
    let target = "bpv1_block";
    let mut collect = admit_block(1).to_vec();
    collect.extend([
        (BLOCK_PATH, 1, 0),
        (BLOCK_ANNOUNCED, 1, 0x3F),
        (BLOCK_PLAN, 0, 0),
        (BLOCK_INSERT, 0x41, 0),
        (BLOCK_INSERT, 0x41, 1),
        (BLOCK_INSERT, 0x01, 4),
        (BLOCK_INSERT, 0x21, 5),
        (BLOCK_RELEASE_CHARGE, 1, 2),
        (BLOCK_STRIPE_DONE, 0, 0),
        (BLOCK_ASSEMBLED, 0, 0),
        (BLOCK_VERDICT, 0, 0),
        (BLOCK_UPLOADED, 0, 4),
        (BLOCK_RATE, 1, 8),
        (BLOCK_PEER_DONE, 1, 0),
        (BLOCK_ASSEMBLED, 0, 0),
        (BLOCK_STRIPE_DONE, 0, 9),
        (BLOCK_TIP, 0, 30),
    ]);
    let mut fallback = admit_block(2).to_vec();
    fallback.extend([
        (BLOCK_ANNOUNCED, 2, 0x03),
        (BLOCK_RELEASE, 0x22, 0x01),
        (BLOCK_RELEASE, 0x42, 0x02),
        (BLOCK_FLAGGED, 0, 0x0C),
        (BLOCK_FLAGGED, 2, 0x30),
        (BLOCK_SESSION_END, 2, 0),
        (BLOCK_TICK, 0, 60),
        (BLOCK_TICK, 0, 120),
        (BLOCK_FALLBACK_DONE, 0, 0),
    ]);
    let mut verify_late = admit_block(1).to_vec();
    verify_late.extend([
        (BLOCK_INSERT, 0x41, 0),
        (BLOCK_INSERT, 0x41, 1),
        (BLOCK_INSERT, 0x41, 2),
        (BLOCK_INSERT, 0x41, 3),
        (BLOCK_VERDICT, 0x40, 0),
        (BLOCK_TICK_LONG, 0, 200),
        (BLOCK_VERDICT, 0x20, 0),
    ]);
    vec![
        seed(target, "collect", ops(BLOCK_EXACT, &collect)),
        seed(target, "fallback", ops(BLOCK_EXACT, &fallback)),
        seed(target, "verify-late", ops(BLOCK_EXACT, &verify_late)),
        // A child before its parent: it waits, then its parent commits.
        seed(
            target,
            "unknown-parent",
            ops(
                BLOCK_EXACT,
                &[
                    (BLOCK_META, 0x05, 0),
                    (BLOCK_HEADER, 1, 0),
                    (BLOCK_UNKNOWN_PARENT, 0, 0),
                    (BLOCK_PARENT, 0, 0),
                    (BLOCK_META, 0x09, 0),
                    (BLOCK_HEADER, 1, 1),
                    (BLOCK_UNKNOWN_PARENT, 2, 1),
                    (BLOCK_META, 0x02, 0x02),
                    // The second session's copy waits; that session ends.
                    (BLOCK_META, 0x06, 0x01),
                    (BLOCK_SESSION_END, 0x02, 0x03),
                    (BLOCK_META, 0x0A, 0),
                    (BLOCK_HEADER, 2, 2),
                    (BLOCK_META, 0x01, 0),
                    (BLOCK_HEADER, 3, 3),
                    (BLOCK_COMMITMENT, 1, 0),
                    (BLOCK_TICK, 0, 200),
                ],
            ),
        ),
        // This node proposes, then the window moves past every block.
        seed(
            target,
            "propose",
            ops(
                BLOCK_EXACT | 0x80,
                &[
                    (BLOCK_LOCAL, 0, 0),
                    (BLOCK_LOCAL, 4, 0),
                    (BLOCK_LOCAL, 8, 0),
                    (BLOCK_COMMIT_FAILED, 4, 0),
                    (BLOCK_TIP, 0, 25),
                    (BLOCK_TIP, 0, 39),
                ],
            ),
        ),
    ]
}

// The `bpv1_mesh` operations.
const MESH_CONTROL: u8 = 0;
const MESH_DATA: u8 = 4;
const MESH_WRITE: u8 = 8;
const MESH_COMPLETE: u8 = 11;
const MESH_FAIL: u8 = 13;
const MESH_TEMPLATE: u8 = 14;
const MESH_SOLVE: u8 = 15;
const MESH_TICK: u8 = 16;
const MESH_TICK_LONG: u8 = 17;
const MESH_PATH: u8 = 18;
const MESH_DISCONNECT: u8 = 19;
const MESH_RECONNECT: u8 = 20;
/// `b % 3` picks a control stall, a fallback's end, or block sync past
/// the window.
const MESH_STALL_OR_FALLBACK: u8 = 21;
const MESH_ADVERSARY: u8 = 22;
const MESH_ADVERSARY_RAW: u8 = 23;
const MESH_COMMIT_FAILED: u8 = 24;

/// One transaction after each block's coinbase.
const MESH_ONE_TX: u8 = 0b0010_1010;

/// Moves every queued frame and completes every job, `rounds` times.
fn pump(rounds: u8) -> Vec<(u8, u8, u8)> {
    let mut out = Vec::new();
    for round in 0..rounds {
        for i in 0..6u8 {
            let a = round.wrapping_mul(7).wrapping_add(i);
            out.extend([
                (MESH_WRITE, a, 0),
                (MESH_DATA, a, 0),
                (MESH_CONTROL, a, 0),
                (MESH_COMPLETE, a, i),
            ]);
        }
    }
    out
}

/// Proposes planned block `block`: its template, its encode, its solve.
fn propose(block: u8) -> Vec<(u8, u8, u8)> {
    vec![
        (MESH_TEMPLATE, block, 0),
        (MESH_COMPLETE, 0, 0),
        (MESH_SOLVE, block, 0),
    ]
}

/// The `a` byte of a crafted adversary message of `kind` to node `to`:
/// `a % 3` picks the node and `(a >> 2) & 7` the kind.
fn crafted(kind: u8, to: u8) -> u8 {
    (0..=u8::MAX)
        .find(|a| a % 3 == to && (a >> 2) & 7 == kind)
        .expect("every kind and node has an `a` byte")
}

/// The stress bit: direct copies, and a one-part buffer on node 2.
const MESH_STRESS: u8 = 0x80;

fn mesh_seeds() -> Vec<Bpv1Seed> {
    let target = "bpv1_mesh";
    // Three blocks, each spread before the next.
    let mut honest = Vec::new();
    for block in 0..3 {
        honest.extend(propose(block));
        honest.extend(pump(4));
        honest.push((MESH_TICK, 0, 4));
    }
    // Block 1 reaches its receivers before its parent commits there.
    let mut orphan = propose(0);
    orphan.extend(propose(1));
    orphan.extend(pump(8));
    // Node 2 has no proposer link, so it pulls from node 1.
    let mut pull = vec![(MESH_DISCONNECT, 1, 0), (MESH_PATH, 0x02, 0x10)];
    pull.extend(propose(0));
    pull.extend(pump(8));
    pull.extend([(MESH_TICK, 0, 10), (MESH_TICK, 0, 40)]);
    pull.extend(pump(4));
    pull.extend([(MESH_RECONNECT, 0, 0), (MESH_TICK_LONG, 0, 30)]);
    pull.extend(pump(2));
    // Node 2 hears of the block only from the adversary, so it falls back
    // and block sync fetches it.
    let mut fallback = vec![(MESH_DISCONNECT, 1, 0), (MESH_DISCONNECT, 2, 0)];
    fallback.extend(propose(0));
    fallback.extend(pump(2));
    for b in 0..6 {
        fallback.push((MESH_ADVERSARY, 0x11, b));
    }
    fallback.extend(pump(3));
    fallback.extend([
        (MESH_TICK_LONG, 0, 20),
        (MESH_RECONNECT, 0, 0),
        (MESH_RECONNECT, 0, 0),
        (MESH_STALL_OR_FALLBACK, 2, 1),
        (MESH_TICK_LONG, 0, 200),
    ]);
    // The receivers' header admission fails locally, then a stalled link.
    let mut local_failures = propose(0);
    local_failures.extend([
        (MESH_CONTROL, 0, 0),
        (MESH_FAIL, 1, 0),
        (MESH_CONTROL, 0, 0),
        (MESH_FAIL, 2, 0),
    ]);
    local_failures.extend(pump(3));
    local_failures.extend([(MESH_STALL_OR_FALLBACK, 0, 0), (MESH_DISCONNECT, 0, 0)]);
    local_failures.extend(pump(2));
    // The adversary tries each crafted message on every honest node.
    let mut adversary = propose(0);
    adversary.extend(pump(3));
    for kind in 0..8u8 {
        for to in 0..3u8 {
            adversary.push((MESH_ADVERSARY, crafted(kind, to), 0x01));
        }
        adversary.extend(pump(1));
    }
    adversary.push((MESH_ADVERSARY, 0x25, 0x01));
    adversary.push((MESH_ADVERSARY, 0x19, 0x00));
    adversary.extend(pump(2));
    adversary.extend(propose(1));
    adversary.extend(pump(4));
    let mut stress = propose(0);
    stress.extend(pump(6));
    stress.extend(propose(1));
    stress.push((MESH_STALL_OR_FALLBACK, 0, 0));
    stress.extend(pump(6));
    // Block sync carries every tip past the window while block 1's frames
    // are in flight; the tables evict the tombstones.
    let mut sync_past = propose(0);
    sync_past.extend(pump(4));
    sync_past.extend(propose(1));
    sync_past.extend([
        (MESH_CONTROL, 0, 0),
        (MESH_STALL_OR_FALLBACK, 0, 2),
        (MESH_TICK, 0, 1),
    ]);
    sync_past.extend(pump(3));
    // The adversary flags a push to node 1, then sends the part forged.
    let mut forged = propose(0);
    forged.extend([
        (MESH_CONTROL, 0, 0),
        (MESH_COMPLETE, 1, 0),
        (MESH_COMPLETE, 1, 0),
        (MESH_ADVERSARY, 0x25, 0x01),
        (MESH_ADVERSARY, 0x19, 0x00),
        (MESH_DATA, 0, 0),
        (MESH_DATA, 0, 0),
        (MESH_COMPLETE, 1, 0),
    ]);
    forged.extend(pump(2));
    // The proposer's commit fails mid-propagation; it retires the block.
    let mut commit_failed = propose(0);
    commit_failed.extend(pump(1));
    commit_failed.push((MESH_COMMIT_FAILED, 0, 0));
    commit_failed.extend(pump(3));
    vec![
        seed(target, "honest", ops(MESH_ONE_TX, &honest)),
        seed(target, "orphan", ops(MESH_ONE_TX, &orphan)),
        seed(target, "pull", ops(MESH_ONE_TX, &pull)),
        seed(target, "fallback", ops(MESH_ONE_TX, &fallback)),
        seed(target, "local-failures", ops(MESH_ONE_TX, &local_failures)),
        seed(target, "adversary", ops(MESH_ONE_TX, &adversary)),
        seed(target, "stress", ops(MESH_STRESS | MESH_ONE_TX, &stress)),
        seed(target, "forged-push", ops(MESH_ONE_TX, &forged)),
        seed(target, "sync-past", ops(MESH_ONE_TX, &sync_past)),
        seed(target, "commit-failed", ops(MESH_ONE_TX, &commit_failed)),
        seed(
            target,
            "adversary-raw",
            ops(
                MESH_ONE_TX,
                &[
                    (MESH_ADVERSARY_RAW, 0x01, 8),
                    (0, 0, 0),
                    (0, 0, 0),
                    (MESH_ADVERSARY_RAW, 0x92, 8),
                    (MESH_DATA, 0, 0),
                ],
            ),
        ),
        seed(target, "w2", ops(1 | MESH_ONE_TX, &honest)),
    ]
}

/// A `HeaderMeta` whose header parses: the regtest genesis header.
fn genesis_meta() -> Bpv1Message {
    let genesis = regtest_genesis_block();
    let coding = Profile::TINY
        .derive_coding(0, 1)
        .expect("an empty body has a layout");
    Bpv1Message::HeaderMeta {
        header: genesis
            .header
            .zcash_serialize_to_vec()
            .expect("the genesis header serializes"),
        binding: CoinbaseBinding {
            coinbase: genesis.transactions[0]
                .zcash_serialize_to_vec()
                .expect("the genesis coinbase serializes"),
            txid_siblings: Vec::new(),
            auth_siblings: Vec::new(),
            light_client_root: [0; 32],
        },
        coding,
        part_root: [0; 32],
        proposer_node: fuzz_node(1),
    }
}

fn wire_seeds() -> Vec<Bpv1Seed> {
    let target = "bpv1_wire";
    let block_id = block::Hash([0x5A; 32]);
    let mut requested = WantBitmap::EMPTY;
    requested.set(1);
    let messages = [
        (0u8, "header-meta", genesis_meta()),
        (
            1,
            "want",
            Bpv1Message::Want {
                want_id: 1,
                block_id,
                stripe: 0,
                requested,
            },
        ),
        (
            2,
            "stripe-done",
            Bpv1Message::StripeDone {
                block_id,
                stripe: 0,
            },
        ),
        (3, "block-done", Bpv1Message::BlockDone { block_id }),
        (
            5,
            "have",
            Bpv1Message::Have {
                block_id,
                flags: HaveFlags::Push,
                form: HaveForm::List,
                indices: vec![0, 2],
            },
        ),
        (
            6,
            "want-end",
            Bpv1Message::WantEnd {
                want_id: 1,
                sent: 1,
                reason: WantEndReason::Served,
            },
        ),
    ];
    let mut seeds: Vec<Bpv1Seed> = messages
        .into_iter()
        .map(|(selector, name, message)| {
            let frame = encode_frame(&message).expect("a seed message encodes");
            let mut bytes = vec![selector];
            bytes.extend(frame.payload);
            seed(target, name, bytes)
        })
        .collect();
    // An honest part frame, then one with a flipped proof byte, then one
    // cut short. Selector 4 is `Part`; bit 4 builds a real part frame.
    seeds.push(seed(target, "part", vec![0x14, 1, 0]));
    seeds.push(seed(target, "part-forged", vec![0x14, 1, 40, 0xFF, 0]));
    seeds.push(seed(target, "part-short", vec![0x34, 2, 3, 0]));
    // A `Have` with reserved frame flags, an unknown type, and garbage.
    seeds.push(seed(target, "flags", vec![0x0D, 0x02, 1, 2, 3]));
    let [low, high] = 0x77u16.to_le_bytes();
    seeds.push(seed(target, "unknown", vec![7, low, high, 0]));
    seeds.push(seed(target, "garbage", vec![0, 1, 2, 3]));
    seeds
}

fn codec_seeds() -> Vec<Bpv1Seed> {
    let target = "bpv1_codec";
    // `[mode, tx_count, choice × 8, tamper index, tamper byte, body…]`.
    let codec = |mode: u8, tx_count: u8, body_len: usize| {
        let mut bytes = vec![mode, tx_count, 3, 1, 4, 1, 5, 9, 2, 6, 5, 0x5A];
        // `i % 251` is below 256, so the cast keeps every value.
        bytes.extend((0..body_len).map(|i| (i % 251) as u8));
        bytes
    };
    vec![
        seed(target, "empty", codec(0x08, 1, 0)),
        seed(target, "trimmed", codec(0x0E, 2, 21)),
        seed(target, "exact", codec(0x06, 2, 32)),
        seed(target, "multi-stripe", codec(0x06, 3, 200)),
        seed(target, "w2", codec(0x07, 2, 100)),
        seed(target, "refused", codec(0, 0, 8)),
    ]
}
