//! BPv1 part Merkle tree, BLAKE3 key-derivation hashing, and [`VerifiedPart`].
//!
//! Tree geometry follows `docs/specs/bpv1.md`:
//! - `encoding_id = T("encoding", coding)`
//! - `leaf[i]     = T("leaf", encoding_id || u32(i) || payload[i])`
//! - `pad[p]      = T("pad",  encoding_id || u32(p))`
//! - `parent      = T("node", left || right)`
//!
//! where `T(name, bytes)` is BLAKE3 key-derivation mode with context
//! `"Zakura BPv1 " + name`.
//!
//! [`VerifiedPart`] can only be constructed inside this module: after
//! verifying a Merkle proof against an admitted `part_root` (bpv1-12), from
//! the tree a proposer builds over its own payloads, or from a re-encoded
//! stripe whose `stripe_root` matches a verified part (bpv1-41).

use std::sync::{Arc, OnceLock};

use thiserror::Error;

use super::profile::Coding;

/// Context prefix for all W2 BLAKE3 KDF hashes.
const HASH_CONTEXT_PREFIX: &str = "Zakura BPv1 ";

/// `T(name, bytes)`: BLAKE3 in key-derivation mode with context
/// `"Zakura BPv1 " + name`.
///
/// The keyed hasher is derived on first use and cloned for each hash, so a
/// hash costs no key derivation.
#[derive(Debug)]
pub struct Bpv1Hash {
    name: &'static str,
    keyed: OnceLock<blake3::Hasher>,
}

impl Bpv1Hash {
    /// The hash `T(name, ·)`.
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            keyed: OnceLock::new(),
        }
    }

    /// `T(name, chunks[0] || chunks[1] || ...)`, without an intermediate buffer.
    pub fn hash(&self, chunks: &[&[u8]]) -> [u8; 32] {
        let mut hasher = self
            .keyed
            .get_or_init(|| {
                blake3::Hasher::new_derive_key(&format!("{HASH_CONTEXT_PREFIX}{}", self.name))
            })
            .clone();
        for chunk in chunks {
            hasher.update(chunk);
        }
        *hasher.finalize().as_bytes()
    }
}

/// `T("encoding", ·)`.
static ENCODING: Bpv1Hash = Bpv1Hash::new("encoding");
/// `T("leaf", ·)`.
static LEAF: Bpv1Hash = Bpv1Hash::new("leaf");
/// `T("pad", ·)`.
static PAD: Bpv1Hash = Bpv1Hash::new("pad");
/// `T("node", ·)`.
static NODE: Bpv1Hash = Bpv1Hash::new("node");
/// `T("commit", ·)`, the coinbase commitment `C`.
pub(crate) static COMMIT: Bpv1Hash = Bpv1Hash::new("commit");

/// Computes `encoding_id = T("encoding", coding)`.
pub fn encoding_id(coding: &Coding) -> [u8; 32] {
    let encoded = coding.to_bytes();
    ENCODING.hash(&[&encoded])
}

/// Computes `leaf[index] = T("leaf", encoding_id || u32(index) || payload)`.
pub fn leaf_hash(enc_id: &[u8; 32], index: u32, payload: &[u8]) -> [u8; 32] {
    let idx = index.to_le_bytes();
    LEAF.hash(&[enc_id, &idx, payload])
}

/// Computes `pad[position] = T("pad", encoding_id || u32(position))`.
pub fn pad_hash(enc_id: &[u8; 32], position: u32) -> [u8; 32] {
    let pos = position.to_le_bytes();
    PAD.hash(&[enc_id, &pos])
}

/// Computes `parent = T("node", left || right)`.
pub fn node_hash(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    NODE.hash(&[left, right])
}

/// Builds the part tree over all `stripes * n` payloads in wire-index order
/// (01 §4), and returns `part_root` with every part and its proof.
///
/// Each proof is read off the tree its payload was hashed into, so the parts
/// are verified by construction.
///
/// An empty body has no parts. Its tree has `next_pow2(0) * next_pow2(0) = 1`
/// leaf position, which holds a pad leaf, so its `part_root` is
/// `pad[0] = T("pad", encoding_id || u32(0))`.
pub fn build_part_tree(
    coding: &Coding,
    enc_id: &[u8; 32],
    payloads: Vec<Arc<[u8]>>,
) -> Result<([u8; 32], Vec<VerifiedPart>), ProofError> {
    let n = coding.n as usize;
    let total = coding.total_parts().ok_or(ProofError::IndexOutOfRange)? as usize;
    if payloads.len() != total {
        return Err(ProofError::WrongPartCount {
            expected: total,
            actual: payloads.len(),
        });
    }
    let mut stripes = Vec::with_capacity(coding.stripes as usize);
    for stripe in 0..coding.stripes {
        // Widening u32 to usize is lossless on supported targets.
        let start = stripe as usize * n;
        stripes.push(levels(stripe_leaves(
            coding,
            enc_id,
            stripe,
            &payloads[start..start + n],
        )?));
    }
    // Stripes from `stripes` up to the next power of two hold only pad leaves.
    let mut stripe_roots: Vec<[u8; 32]> = stripes.iter().map(|levels| root(levels)).collect();
    for stripe in coding.stripes..coding.padded_stripes() {
        stripe_roots.push(root(&levels(stripe_leaves(coding, enc_id, stripe, &[])?)));
    }
    let upper = levels(stripe_roots);
    let part_root = root(&upper);

    let mut parts = Vec::with_capacity(total);
    let mut payloads = payloads.into_iter();
    for (stripe, levels) in (0..).zip(&stripes) {
        // Widening u32 to usize is lossless on supported targets.
        let upper_siblings: Vec<[u8; 32]> = path(&upper, stripe as usize).collect();
        parts.extend(stripe_parts(
            coding,
            stripe,
            levels,
            &upper_siblings,
            part_root,
            payloads.by_ref().take(n),
        ));
    }
    Ok((part_root, parts))
}

/// Builds all `n` parts of `stripe` from re-encoded payloads, after checking
/// that they hash to the stripe root of `anchor`, an already verified part of
/// the stripe (bpv1-41). The parts take `anchor`'s path above the stripe and
/// its `part_root`.
pub fn build_stripe_verified_parts(
    coding: &Coding,
    enc_id: &[u8; 32],
    stripe: u32,
    anchor: &VerifiedPart,
    payloads: Vec<Arc<[u8]>>,
) -> Result<Vec<VerifiedPart>, ProofError> {
    if coding.split_index(anchor.index).map(|(s, _)| s) != Some(stripe) {
        return Err(ProofError::IndexOutOfRange);
    }
    let n = coding.n as usize;
    if payloads.len() != n {
        return Err(ProofError::WrongPartCount {
            expected: n,
            actual: payloads.len(),
        });
    }
    let levels = levels(stripe_leaves(coding, enc_id, stripe, &payloads)?);
    if root(&levels) != anchor.stripe_root {
        return Err(ProofError::RootMismatch);
    }
    Ok(stripe_parts(
        coding,
        stripe,
        &levels,
        anchor.upper_siblings(coding),
        anchor.part_root,
        payloads,
    ))
}

/// The leaves of `stripe`'s subtree: one per payload in slot order, then pad
/// leaves up to `next_pow2(n)`.
fn stripe_leaves(
    coding: &Coding,
    enc_id: &[u8; 32],
    stripe: u32,
    payloads: &[Arc<[u8]>],
) -> Result<Vec<[u8; 32]>, ProofError> {
    let part_len = coding.part_bytes as usize;
    let width = coding.stripe_width();
    // A validated coding's tree positions fit in u32, and widening u32 to
    // usize is lossless on supported targets.
    (0..width)
        .map(|slot| match payloads.get(slot as usize) {
            Some(payload) if payload.len() != part_len => Err(ProofError::WrongPayloadLength {
                expected: part_len,
                actual: payload.len(),
            }),
            Some(payload) => Ok(leaf_hash(enc_id, stripe * coding.n + slot, payload)),
            None => Ok(pad_hash(enc_id, stripe * width + slot)),
        })
        .collect()
}

/// Every level of the subtree over a power-of-two run of `leaves`, from the
/// leaves up to the root.
fn levels(leaves: Vec<[u8; 32]>) -> Vec<Vec<[u8; 32]>> {
    let mut levels = vec![leaves];
    while let Some(level) = levels.last().filter(|level| level.len() > 1) {
        let next = level
            .as_chunks::<2>()
            .0
            .iter()
            .map(|[left, right]| node_hash(left, right))
            .collect();
        levels.push(next);
    }
    levels
}

/// The root of `levels`.
fn root(levels: &[Vec<[u8; 32]>]) -> [u8; 32] {
    levels
        .last()
        .and_then(|level| level.first())
        .copied()
        .expect("levels start from at least one leaf")
}

/// The siblings from position `pos` on the bottom level of `levels` up to,
/// but not including, the root.
fn path(levels: &[Vec<[u8; 32]>], mut pos: usize) -> impl Iterator<Item = [u8; 32]> + '_ {
    levels[..levels.len() - 1].iter().map(move |level| {
        let sibling = level[pos ^ 1];
        pos >>= 1;
        sibling
    })
}

/// Folds `node` at position `pos` up `siblings` in leaf-to-root order.
fn fold(mut node: [u8; 32], pos: u32, siblings: &[[u8; 32]]) -> [u8; 32] {
    for (level, sibling) in siblings.iter().enumerate() {
        node = if (pos >> level) & 1 == 0 {
            node_hash(&node, sibling)
        } else {
            node_hash(sibling, &node)
        };
    }
    node
}

/// Wraps a stripe's payloads with their proofs: the path within the stripe's
/// subtree `levels`, then `upper_siblings` up to `part_root`.
fn stripe_parts(
    coding: &Coding,
    stripe: u32,
    levels: &[Vec<[u8; 32]>],
    upper_siblings: &[[u8; 32]],
    part_root: [u8; 32],
    payloads: impl IntoIterator<Item = Arc<[u8]>>,
) -> Vec<VerifiedPart> {
    let stripe_root = root(levels);
    (0..)
        .zip(payloads)
        // Widening u32 to usize is lossless on supported targets.
        .map(|(slot, payload)| VerifiedPart {
            index: stripe * coding.n + slot,
            wire_len: coding
                .wire_len(stripe * coding.n + slot)
                .expect("slot is inside the stripe's layout"),
            payload,
            siblings: path(levels, slot as usize)
                .chain(upper_siblings.iter().copied())
                .collect(),
            stripe_root,
            part_root,
        })
        .collect()
}

/// A part whose Merkle proof has been verified against an admitted `part_root`,
/// or which was locally encoded/regenerated and checked against `stripe_root`.
///
/// Its fields are private so untrusted bytes can never be wrapped as a
/// [`VerifiedPart`] without passing through verification (bpv1-12, level-1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedPart {
    index: u32,
    /// `wire_len(index)`: the payload bytes that cross the wire (decision-18).
    wire_len: u32,
    payload: Arc<[u8]>,
    siblings: Arc<[[u8; 32]]>,
    stripe_root: [u8; 32],
    part_root: [u8; 32],
}

impl VerifiedPart {
    /// Wire index of this part (`0..stripes * n`).
    pub fn index(&self) -> u32 {
        self.index
    }

    /// The `S`-byte part payload, padded with zeros past the body's end.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// The payload as it crosses the wire: its first `wire_len(index)` bytes
    /// (01 §4, decision-18).
    pub fn wire_payload(&self) -> &[u8] {
        // Widening u32 to usize is lossless on supported targets.
        &self.payload[..self.wire_len as usize]
    }

    /// Cheap clone of the underlying `Arc<[u8]>` payload buffer (opt-1).
    pub fn payload_arc(&self) -> Arc<[u8]> {
        self.payload.clone()
    }

    /// Verified Merkle proof siblings in leaf-to-root order (forwarded as-is, opt-1).
    pub fn siblings(&self) -> &[[u8; 32]] {
        &self.siblings
    }

    /// Verified `stripe_root(s)` yielded by the first `log2(next_pow2(n))` levels.
    pub fn stripe_root(&self) -> [u8; 32] {
        self.stripe_root
    }

    /// Authenticated full-tree `part_root` this part was verified against (`bpv1-12`).
    pub fn part_root(&self) -> [u8; 32] {
        self.part_root
    }

    /// Upper-tree siblings above `stripe_root(s)`, used when regenerating parts
    /// of this stripe so regenerated parts carry valid full proofs to `part_root`.
    pub fn upper_siblings(&self, coding: &Coding) -> &[[u8; 32]] {
        &self.siblings[coding.stripe_tree_depth()..]
    }
}

/// Verifies a received part's Merkle inclusion proof against `part_root`
/// (bpv1-12). `wire_payload` is the payload as it crossed the wire, exactly
/// `wire_len(index)` bytes (bpv1-66); the part is padded with zeros to `S`
/// before hashing. On success returns a [`VerifiedPart`] whose `stripe_root`
/// is derived from the first `log2(next_pow2(n))` levels.
pub fn verify_part_proof(
    coding: &Coding,
    enc_id: &[u8; 32],
    part_root: &[u8; 32],
    index: u32,
    siblings: Vec<[u8; 32]>,
    wire_payload: &[u8],
) -> Result<VerifiedPart, ProofError> {
    let pos = coding
        .tree_position(index)
        .ok_or(ProofError::IndexOutOfRange)?;
    let wire_len = coding.wire_len(index).ok_or(ProofError::IndexOutOfRange)?;
    // Widening u32 to usize is lossless on supported targets.
    if wire_payload.len() != wire_len as usize {
        return Err(ProofError::WrongPayloadLength {
            expected: wire_len as usize,
            actual: wire_payload.len(),
        });
    }
    let mut padded = vec![0u8; coding.part_bytes as usize];
    padded[..wire_payload.len()].copy_from_slice(wire_payload);
    let payload: Arc<[u8]> = padded.into();
    let expected_depth = coding.tree_depth();
    if siblings.len() != expected_depth {
        return Err(ProofError::WrongProofDepth {
            expected: expected_depth,
            actual: siblings.len(),
        });
    }

    let stripe_depth = coding.stripe_tree_depth();
    let leaf = leaf_hash(enc_id, index, &payload);
    let stripe_root = fold(leaf, pos, &siblings[..stripe_depth]);
    if fold(stripe_root, pos >> stripe_depth, &siblings[stripe_depth..]) != *part_root {
        return Err(ProofError::RootMismatch);
    }

    Ok(VerifiedPart {
        index,
        wire_len,
        payload,
        siblings: siblings.into(),
        stripe_root,
        part_root: *part_root,
    })
}

/// Errors returned by part-tree construction or Merkle proof verification.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Error)]
pub enum ProofError {
    /// Part index or stripe is outside the admitted layout.
    #[error("part index out of range")]
    IndexOutOfRange,
    /// Part count does not match the layout.
    #[error("expected {expected} parts, got {actual}")]
    WrongPartCount {
        /// Expected number of parts.
        expected: usize,
        /// Supplied number of parts.
        actual: usize,
    },
    /// Part payload length does not match `S`, or `wire_len(i)` on the wire.
    #[error("expected {expected}-byte part payload, got {actual}")]
    WrongPayloadLength {
        /// Expected length.
        expected: usize,
        /// Actual payload length.
        actual: usize,
    },
    /// Proof sibling count does not equal the layout's exact tree depth.
    #[error("expected {expected} Merkle siblings, got {actual}")]
    WrongProofDepth {
        /// Expected proof depth.
        expected: usize,
        /// Supplied sibling count.
        actual: usize,
    },
    /// Recomputed root did not match the admitted root.
    #[error("Merkle root mismatch")]
    RootMismatch,
}
