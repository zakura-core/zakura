//! The `bpv1_codec` fuzz target: encoding, any-`k` decoding, the re-encode
//! check, part proofs and canonical assembly (decision-10, bpv1-41, bpv1-42).
//!
//! Input layout: `[mode, tx_count, choice × 8, tamper index, tamper byte,
//! body…]`. `mode` bit 0 picks `W2` over `TINY`; bit 1 encodes as a lying
//! proposer, who replaces one part's payload and builds an honest tree over
//! it; bit 2 tampers with one part's wire payload before proof; bit 3
//! assembles the regtest genesis header and coinbase over the body. The
//! eight choice bytes pick the `k`-subset each stripe decodes from.
//!
//! Checks, for every input the profile encodes:
//! - every part proves against `part_root` from its wire payload, and the
//!   proof rebuilds the same part;
//! - every `k`-subset of a stripe decodes and re-encodes to the stripe's
//!   `n` parts, and `k - 1` parts never decode;
//! - a lying proposer's stripe never passes the re-encode check from any
//!   `k`-subset;
//! - a tampered wire payload never proves;
//! - assembly either refuses or returns `header || compactsize(tx_count)
//!   || coinbase || body`.
//!
//! The target excludes nothing the codec exposes. Inputs whose body exceeds
//! the profile's limits stop at the refused encoding.

use std::sync::Arc;

use zakura_chain::{block::genesis::regtest_genesis_block, serialization::ZcashSerialize};

use super::{quality::Reached, FuzzBytes};
use crate::zakura::{
    bpv1::{
        assemble::assemble_block,
        codec::{decode_and_check_stripe, encode_body, EncodedBody},
        profile::{Coding, Profile},
        tree::{build_part_tree, verify_part_proof, VerifiedPart},
    },
    wire_codec::write_compact_size,
};

/// Summary returned by [`replay_bpv1_codec_bytes`].
#[derive(Clone, Debug, Default)]
pub struct CodecReplaySummary {
    /// Stripes decoded from a `k`-subset.
    pub decoded_stripes: usize,
    /// Every outcome the replay reached.
    pub reached: Reached,
}

/// The names this target must be able to reach.
pub(super) const EXPECTED: &[&str] = &[
    "profile:TINY",
    "profile:W2",
    "codec:encoded",
    "codec:refused",
    "layout:empty",
    "layout:trimmed",
    "layout:exact",
    "layout:multi-stripe",
    "proof:verified",
    "proof:tampered-refused",
    "decode:any-k",
    "decode:short-refused",
    "decode:lying-caught",
    "assemble:ok",
    "assemble:refused",
];

/// Fixed bytes before the body.
const HEAD: usize = 12;

/// The property the codec checks name.
const CODEC: &str = "bpv1/codec";

/// Replays `bytes` through the BPv1 codec and asserts the checks above.
pub fn replay_bpv1_codec_bytes(bytes: &[u8]) -> CodecReplaySummary {
    let mut input = FuzzBytes::new(bytes);
    let mode = input.byte();
    let tx_count = u32::from(input.byte());
    let choice = input.take(8).to_vec();
    let tamper_at = input.byte();
    let tamper = input.byte();
    let body = &bytes[HEAD.min(bytes.len())..];
    let mut summary = CodecReplaySummary::default();
    let reached = &mut summary.reached;

    let profile = if mode & 1 != 0 {
        reached.hit("profile:W2");
        Profile::W2
    } else {
        reached.hit("profile:TINY");
        Profile::TINY
    };
    let Ok(encoded) = encode_body(&profile, body, tx_count) else {
        reached.hit("codec:refused");
        return summary;
    };
    reached.hit("codec:encoded");
    let coding = encoded.coding;
    reached.hit(layout_name(&coding));

    check_proofs(&encoded, tamper_at, tamper, mode & 4 != 0, reached);
    summary.decoded_stripes = check_decodes(&encoded, &choice, reached);
    if mode & 2 != 0 {
        check_lying_proposer(&encoded, &choice, tamper_at, tamper, reached);
    }
    if mode & 8 != 0 {
        check_assembly(&profile, &encoded, body, reached);
    }
    summary
}

/// The layout name the shared geometries use.
fn layout_name(coding: &Coding) -> &'static str {
    if coding.stripes == 0 {
        return "layout:empty";
    }
    if coding.stripes > 1 {
        return "layout:multi-stripe";
    }
    let last_source = coding.k.saturating_sub(1);
    if coding.wire_len(last_source) == Some(coding.part_bytes) {
        "layout:exact"
    } else {
        "layout:trimmed"
    }
}

/// Every part proves from its wire payload; a tampered payload never does.
fn check_proofs(
    encoded: &EncodedBody,
    tamper_at: u8,
    tamper: u8,
    tampering: bool,
    reached: &mut Reached,
) {
    let prove = |part: &VerifiedPart, wire: &[u8]| {
        verify_part_proof(
            &encoded.coding,
            &encoded.encoding_id,
            &encoded.part_root,
            part.index(),
            part.siblings().to_vec(),
            wire,
        )
    };
    for part in &encoded.parts {
        let proved = prove(part, part.wire_payload()).unwrap_or_else(|e| {
            panic!(
                "{CODEC}: bpv1-12 part {} fails its proof: {e}",
                part.index()
            )
        });
        assert_eq!(&proved, part, "{CODEC}: the proof rebuilt another part");
        reached.hit("proof:verified");
    }
    let Some(part) = pick(&encoded.parts, tamper_at) else {
        return;
    };
    if !tampering || part.wire_payload().is_empty() {
        return;
    }
    let mut wire = part.wire_payload().to_vec();
    let at = usize::from(tamper_at) % wire.len();
    // XOR with a nonzero byte always changes the payload.
    wire[at] ^= tamper.max(1);
    assert!(
        prove(part, &wire).is_err(),
        "{CODEC}: bpv1-12 a tampered payload of part {} proved",
        part.index()
    );
    reached.hit("proof:tampered-refused");
}

/// Every stripe decodes from the chosen `k`-subset to its `n` parts, and
/// never from `k - 1` parts. Returns how many stripes decoded.
fn check_decodes(encoded: &EncodedBody, choice: &[u8], reached: &mut Reached) -> usize {
    let coding = &encoded.coding;
    let mut decoded = 0;
    for stripe in 0..coding.stripes {
        let parts = stripe_parts(&encoded.parts, coding, stripe);
        let held = subset(coding, stripe, parts, choice, coding.k);
        let rebuilt = decode_and_check_stripe(coding, &encoded.encoding_id, stripe, &held)
            .unwrap_or_else(|e| panic!("{CODEC}: bpv1-41 stripe {stripe} fails any-k decode: {e}"));
        assert_eq!(
            rebuilt, parts,
            "{CODEC}: stripe {stripe} decoded other parts"
        );
        decoded += 1;
        reached.hit("decode:any-k");

        // Padding slots count as held, so a stripe with padding decodes from
        // fewer stored parts. `k - 1` counted parts never decode.
        let short = subset(coding, stripe, parts, choice, coding.k.saturating_sub(1));
        assert!(
            decode_and_check_stripe(coding, &encoded.encoding_id, stripe, &short).is_err(),
            "{CODEC}: stripe {stripe} decoded from k - 1 parts"
        );
        reached.hit("decode:short-refused");
    }
    decoded
}

/// A proposer who swaps one part's payload and builds an honest tree over
/// the result: every part proves, but no `k`-subset passes the re-encode
/// check (bpv1-41).
fn check_lying_proposer(
    encoded: &EncodedBody,
    choice: &[u8],
    tamper_at: u8,
    tamper: u8,
    reached: &mut Reached,
) {
    let coding = &encoded.coding;
    let Some(victim) = pick(&encoded.parts, tamper_at) else {
        return;
    };
    // A padding part is fixed by the layout, so changing it is not a codeword
    // the decoder can be fooled with: every honest decoder assumes zeros.
    if coding.is_padding(victim.index()) {
        return;
    }
    let payloads: Vec<Arc<[u8]>> = encoded
        .parts
        .iter()
        .map(|part| {
            let mut payload = part.payload().to_vec();
            if part.index() == victim.index() {
                // Past `wire_len` the payload must stay zero (decision-18),
                // so the lie changes a byte that crosses the wire.
                let wire = coding.wire_len(part.index()).unwrap_or(0) as usize;
                let at = usize::from(tamper) % wire.max(1);
                payload[at] ^= tamper.max(1);
            }
            Arc::from(payload)
        })
        .collect();
    let (_, lying) = build_part_tree(coding, &encoded.encoding_id, payloads)
        .expect("the lying tree has the honest part count");
    let (stripe, _) = coding
        .split_index(victim.index())
        .expect("the victim is a part of this layout");
    let parts = stripe_parts(&lying, coding, stripe);
    for rotate in 0..choice.len() {
        let mut rotated = choice.to_vec();
        rotated.rotate_left(rotate);
        let held = subset(coding, stripe, parts, &rotated, coding.k);
        assert!(
            decode_and_check_stripe(coding, &encoded.encoding_id, stripe, &held).is_err(),
            "{CODEC}: bpv1-41 a lying stripe passed the re-encode check"
        );
    }
    reached.hit("decode:lying-caught");
}

/// Assembles the regtest genesis header and coinbase over `body`. A
/// returned block's bytes are exactly the canonical concatenation.
fn check_assembly(profile: &Profile, encoded: &EncodedBody, body: &[u8], reached: &mut Reached) {
    let genesis = regtest_genesis_block();
    let coinbase = genesis.transactions[0]
        .zcash_serialize_to_vec()
        .expect("the genesis coinbase serializes");
    let coding = &encoded.coding;
    let sources: Vec<Vec<Arc<[u8]>>> = (0..coding.stripes)
        .map(|stripe| {
            stripe_parts(&encoded.parts, coding, stripe)
                .iter()
                .take(coding.k as usize)
                .map(VerifiedPart::payload_arc)
                .collect()
        })
        .collect();
    match assemble_block(profile, &genesis.header, coding, &coinbase, &sources) {
        Ok((raw, block)) => {
            let mut expected = genesis
                .header
                .zcash_serialize_to_vec()
                .expect("the genesis header serializes");
            write_compact_size(u64::from(coding.tx_count), &mut expected);
            expected.extend_from_slice(&coinbase);
            expected.extend_from_slice(body);
            assert_eq!(raw, expected, "{CODEC}: bpv1-42 assembly is not canonical");
            assert_eq!(
                block.transactions.len(),
                coding.tx_count as usize,
                "{CODEC}: bpv1-42 assembly lost a transaction"
            );
            reached.hit("assemble:ok");
        }
        Err(_) => reached.hit("assemble:refused"),
    }
}

/// The `n` parts of `stripe`, in slot order.
fn stripe_parts<'a>(parts: &'a [VerifiedPart], coding: &Coding, stripe: u32) -> &'a [VerifiedPart] {
    let n = coding.n as usize;
    let start = stripe as usize * n;
    &parts[start..start + n]
}

/// `count` of the stripe's counted slots, chosen by `choice`, as the held
/// vector the decoder takes. Padding slots count as held without a part, as
/// the table holds them.
fn subset(
    coding: &Coding,
    stripe: u32,
    parts: &[VerifiedPart],
    choice: &[u8],
    count: u32,
) -> Vec<Option<VerifiedPart>> {
    let n = parts.len();
    let mut order: Vec<usize> = (0..n).collect();
    // A Fisher-Yates shuffle driven by the choice bytes, salted per stripe.
    for i in (1..n).rev() {
        let byte = choice.get(i % choice.len().max(1)).copied().unwrap_or(0);
        let j = (usize::from(byte) + stripe as usize) % (i + 1);
        order.swap(i, j);
    }
    let padding = |slot: usize| {
        coding
            .wire_index(stripe, slot as u32)
            .is_some_and(|index| coding.is_padding(index))
    };
    let padded = (0..n).filter(|&slot| padding(slot)).count();
    let wanted = (count as usize).saturating_sub(padded);
    let mut held = vec![None; n];
    for slot in order
        .into_iter()
        .filter(|&slot| !padding(slot))
        .take(wanted)
    {
        held[slot] = Some(parts[slot].clone());
    }
    held
}

fn pick<T>(items: &[T], byte: u8) -> Option<&T> {
    items.get(usize::from(byte).checked_rem(items.len())?)
}
