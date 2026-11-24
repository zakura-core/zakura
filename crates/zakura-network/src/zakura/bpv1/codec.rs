//! Systematic Reed–Solomon GF(2^16) stripe encoding, decoding, and re-encode
//! verification using `reed-solomon-simd` 3.1.
//!
//! Implements decision-10 (`bpv1-41`):
//! - When a stripe holds all `k` source parts, it assembles immediately without
//!   decoding; the re-encode check gates only serving regenerated parity.
//! - When decoding uses any parity part, all `n` parts are re-encoded and
//!   verified against the stripe's committed `stripe_root` before assembly or
//!   serving regenerated parts.

use std::{collections::BTreeMap, sync::Arc};

use thiserror::Error;

use super::{
    profile::{Coding, LayoutError, Profile},
    tree::{build_part_tree, build_stripe_verified_parts, encoding_id, ProofError, VerifiedPart},
};

/// A fully encoded block body ready for seeding and serving by the proposer.
#[derive(Clone, Debug)]
pub struct EncodedBody {
    /// Derived coding parameters.
    pub coding: Coding,
    /// `encoding_id = T("encoding", coding)`.
    pub encoding_id: [u8; 32],
    /// Root of the part tree over all `stripes * n` parts.
    pub part_root: [u8; 32],
    /// All `stripes * n` verified parts in wire-index order (`0..stripes * n`).
    pub parts: Vec<VerifiedPart>,
}

/// Encodes `body_bytes` (the post-coinbase transaction bytes `B'`) into
/// systematic RS stripes and builds their part tree.
pub fn encode_body(
    profile: &Profile,
    body_bytes: &[u8],
    tx_count: u32,
) -> Result<EncodedBody, CodecError> {
    // Widening usize to u64 is lossless on supported targets.
    let coding = profile.derive_coding(body_bytes.len() as u64, tx_count)?;
    let part_len = coding.part_bytes as usize;
    let k = coding.k as usize;
    let n = coding.n as usize;

    let mut payloads: Vec<Arc<[u8]>> = Vec::with_capacity((coding.stripes as usize) * n);
    for stripe in 0..coding.stripes as usize {
        let originals: Vec<Vec<u8>> = (0..k)
            .map(|slot| {
                let start = ((stripe * k + slot) * part_len).min(body_bytes.len());
                let end = (start + part_len).min(body_bytes.len());
                let mut shard = vec![0u8; part_len];
                shard[..end - start].copy_from_slice(&body_bytes[start..end]);
                shard
            })
            .collect();
        let recovery = reed_solomon_simd::encode(k, n - k, &originals)?;
        payloads.extend(originals.into_iter().chain(recovery).map(Arc::from));
    }

    let encoding_id = encoding_id(&coding);
    let (part_root, parts) = build_part_tree(&coding, &encoding_id, payloads)?;
    Ok(EncodedBody {
        coding,
        encoding_id,
        part_root,
        parts,
    })
}

/// Decodes a stripe from any `k` distinct [`VerifiedPart`]s (indexed by slot
/// `0..n`), re-encodes all `n` parts, and returns them once their recomputed
/// `stripe_root(stripe)` matches the held parts' `stripe_root` (bpv1-41).
///
/// Works for both parity-assisted decode and the low-priority re-encode check
/// after an all-systematic assembly (decision-10). A padding source part
/// (decision-18) needs no entry in `held`: the layout fixes it as zeros.
pub fn decode_and_check_stripe(
    coding: &Coding,
    enc_id: &[u8; 32],
    stripe: u32,
    held: &[Option<VerifiedPart>],
) -> Result<Vec<VerifiedPart>, CodecError> {
    let k = coding.k as usize;
    let n = coding.n as usize;
    let part_len = coding.part_bytes as usize;
    if stripe >= coding.stripes || held.len() != n {
        return Err(CodecError::InvalidInput);
    }

    // Every held part must sit at its slot and share one stripe root. The first
    // one anchors the rebuilt parts' proofs above the stripe.
    // A padding part (decision-18) is all zeros by the layout, so it counts
    // as held without being stored. The root check below covers it.
    let zeros = vec![0u8; part_len];
    let is_padding = |slot: u32| {
        coding
            .wire_index(stripe, slot)
            .is_some_and(|index| coding.is_padding(index))
    };
    let mut anchor: Option<&VerifiedPart> = None;
    let mut originals: Vec<(usize, &[u8])> = Vec::with_capacity(k);
    let mut recoveries: Vec<(usize, &[u8])> = Vec::with_capacity(n - k);
    for (slot, part) in (0..).zip(held) {
        let Some(part) = part else {
            if is_padding(slot) {
                // Widening u32 to usize is lossless on supported targets.
                originals.push((slot as usize, &zeros));
            }
            continue;
        };
        if Some(part.index()) != coding.wire_index(stripe, slot) || part.payload().len() != part_len
        {
            return Err(CodecError::InvalidInput);
        }
        match anchor {
            Some(anchor) if anchor.stripe_root() != part.stripe_root() => {
                return Err(CodecError::CodewordCheckFailed);
            }
            Some(_) => {}
            None => anchor = Some(part),
        }
        // Widening u32 to usize is lossless on supported targets.
        let slot = slot as usize;
        if slot < k {
            originals.push((slot, part.payload()));
        } else {
            recoveries.push((slot - k, part.payload()));
        }
    }
    let have = originals.len() + recoveries.len();
    let Some(anchor) = anchor.filter(|_| have >= k) else {
        return Err(CodecError::NotEnoughParts { needed: k, have });
    };

    // Restore the missing source parts; a stripe holding all `k` needs none.
    let mut restored = if originals.len() == k {
        BTreeMap::new()
    } else {
        // `decode` takes exactly `k` shards in total.
        let needed = k - originals.len();
        reed_solomon_simd::decode(
            k,
            n - k,
            originals.iter().copied(),
            recoveries.iter().take(needed).copied(),
        )?
    };
    let mut payloads = held[..k]
        .iter()
        .enumerate()
        .map(|(slot, part)| match part {
            Some(part) => Some(part.payload_arc()),
            // Widening usize to u32 is lossless: `slot < k <= u32::MAX`.
            None if is_padding(slot as u32) => Some(Arc::from(zeros.as_slice())),
            None => restored.remove(&slot).map(Arc::from),
        })
        .collect::<Option<Vec<Arc<[u8]>>>>()
        .ok_or(CodecError::CodewordCheckFailed)?;

    // Re-encode the parity. A held part that differs from its re-encoded
    // payload changes the stripe root, so the root check below catches it.
    let parity = reed_solomon_simd::encode(k, n - k, &payloads)?;
    payloads.extend(parity.into_iter().map(Arc::from));
    build_stripe_verified_parts(coding, enc_id, stripe, anchor, payloads)
        .map_err(|_| CodecError::CodewordCheckFailed)
}

/// Errors produced by BPv1 stripe encoding or decoding.
#[derive(Clone, Debug, PartialEq, Error)]
pub enum CodecError {
    /// Coding layout derivation failed.
    #[error(transparent)]
    Layout(#[from] LayoutError),
    /// Part tree or proof validation failed.
    #[error(transparent)]
    Proof(#[from] ProofError),
    /// Underlying Reed–Solomon error.
    #[error("Reed-Solomon codec error: {0}")]
    ReedSolomon(#[from] reed_solomon_simd::Error),
    /// Caller supplied fewer than `k` distinct verified parts.
    #[error("not enough parts to decode stripe: need {needed}, have {have}")]
    NotEnoughParts {
        /// Required source part count `k`.
        needed: usize,
        /// Available distinct verified parts.
        have: usize,
    },
    /// Invalid arguments passed to the codec.
    #[error("invalid codec input")]
    InvalidInput,
    /// Re-encoding all `n` parts did not reproduce the committed `stripe_root` (bpv1-41).
    #[error("re-encoded stripe does not match committed stripe root")]
    CodewordCheckFailed,
}
