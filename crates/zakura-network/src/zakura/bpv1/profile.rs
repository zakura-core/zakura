//! BPv1 coding profiles, layout arithmetic, and wire index geometry.
//!
//! All layout functions, the part Merkle tree, and the state machines take a
//! [`Profile`] as a parameter (decision-4). [`Profile::W2`] is the only
//! negotiated wire profile; [`Profile::TINY`] lets tests and fuzzers reach
//! multi-stripe blocks and deep proofs cheaply.

use thiserror::Error;
use zakura_chain::block::MAX_BLOCK_BYTES;

use crate::zakura::wire_codec::compact_size_len;

/// Wire profile identifier for W2.
pub const W2_PROFILE_ID: u16 = 2;

/// The profile a node runs BPv1 under, which the memory plan sizes
/// (decision-4).
pub const NODE_PROFILE: Profile = Profile::W2;

/// W2 codec identifier (`reed-solomon-simd` 3.1 systematic RS over GF(2^16)).
pub const W2_CODEC_ID: u16 = 2;

/// W2 smallest part payload size in bytes (`S_MIN = 4,096`, decision-18).
pub const W2_S_MIN: u32 = 4_096;

/// W2 largest part payload size in bytes (`S_MAX = 65,536`, decision-18).
pub const W2_S_MAX: u32 = 65_536;

/// W2 part size granule: `S` is a multiple of 64 bytes for the SIMD codec (decision-18).
pub const W2_PART_ALIGN: u32 = 64;

/// W2 maximum source parts per stripe (`STRIPE_K = 64`).
pub const W2_STRIPE_K: u32 = 64;

/// W2 maximum coded parts per stripe (`64 + ceil(64 / 3) = 86`).
pub const W2_MAX_N: u32 = 86;

/// W2 maximum stripes per block (`MAX_STRIPES = 512`).
pub const W2_MAX_STRIPES: u32 = 512;

/// W2 maximum Merkle proof depth (`log2(next_pow2(512)) + log2(next_pow2(86)) = 9 + 7 = 16`).
pub const W2_MAX_PROOF_DEPTH: usize = 16;

/// W2 maximum indices per `Have` frame (`MAX_HAVE_INDICES = 4,096`).
pub const W2_MAX_HAVE_INDICES: usize = 4_096;

/// W2 maximum serialized header bytes (`MAX_HEADER_BYTES = 4,096`).
pub const W2_MAX_HEADER_BYTES: usize = 4_096;

/// W2 maximum serialized coinbase transaction bytes: the consensus bound,
/// `MAX_BLOCK_BYTES` (decision-23). Consensus has no separate coinbase limit,
/// so a tighter bound would send valid blocks to slower relay (bpv1-68).
// `MAX_BLOCK_BYTES` (2,000,000) fits in `usize` on every supported target.
pub const W2_MAX_COINBASE_BYTES: usize = MAX_BLOCK_BYTES as usize;

/// W2 maximum Merkle path depth for coinbase txid and auth-data proofs (`32`).
pub const W2_MAX_COMMITMENT_PATH_DEPTH: usize = 32;

/// Size in bytes of a serialized [`Coding`] tuple:
/// `u16(codec_id) || u64(body_bytes) || u32(tx_count) || u32(part_bytes) || u32(stripes) || u32(k) || u32(n)`.
pub const CODING_BYTES: usize = 30;

/// Bytes of the deepest W2 part proof (`W2_MAX_PROOF_DEPTH` 32-byte siblings).
// Widening usize to u64 is lossless on supported targets.
pub const PART_PROOF_BYTES: u64 = (W2_MAX_PROOF_DEPTH * 32) as u64;

/// The coding geometry of a BPv1 profile (01 §4, decision-4).
///
/// W2 is the only negotiated profile, so the profile and codec ids and the
/// syntax bounds are the `W2_*` constants. The parity rule is
/// [`Profile::parity_parts`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Profile {
    /// Smallest part payload size `S_MIN` (decision-18).
    pub s_min: u32,
    /// Largest part payload size `S_MAX` (decision-18).
    pub s_max: u32,
    /// Granule `S` is rounded up to. It is even, as the codec requires, and
    /// divides both `s_min` and `s_max`.
    pub part_align: u32,
    /// Maximum source parts per stripe (`STRIPE_K`).
    pub stripe_k: u32,
    /// Maximum stripes per block (`MAX_STRIPES`).
    pub max_stripes: u32,
    /// Maximum total block bytes (`len(header) + compactsize(tx_count) + len(coinbase) + body_bytes`).
    pub max_block_bytes: u64,
}

impl Profile {
    /// The negotiated production profile W2, enforcing the consensus 2,000,000-byte block limit.
    pub const W2: Self = Self {
        s_min: W2_S_MIN,
        s_max: W2_S_MAX,
        part_align: W2_PART_ALIGN,
        stripe_k: W2_STRIPE_K,
        max_stripes: W2_MAX_STRIPES,
        max_block_bytes: MAX_BLOCK_BYTES,
    };

    /// Tiny profile for property tests and fuzzers (`S_MIN = 8`, `S_MAX = 32`,
    /// `STRIPE_K = 4`, decision-4). Its 8-byte granule lets `S` vary with the body.
    pub const TINY: Self = Self {
        s_min: 8,
        s_max: 32,
        part_align: 8,
        stripe_k: 4,
        max_block_bytes: 65_536,
        ..Self::W2
    };

    /// Number of parity parts for a stripe of `k` source parts (`ceil(k / 3)`).
    pub const fn parity_parts(k: u32) -> u32 {
        k.div_ceil(3)
    }

    /// Maximum coded parts `n` per stripe under this profile.
    pub const fn max_n(&self) -> u32 {
        self.stripe_k + Self::parity_parts(self.stripe_k)
    }

    /// Maximum Merkle proof depth under this profile.
    pub const fn max_proof_depth(&self) -> usize {
        let stripe_bits = self.max_stripes.next_power_of_two().trailing_zeros() as usize;
        let slot_bits = self.max_n().next_power_of_two().trailing_zeros() as usize;
        stripe_bits + slot_bits
    }

    /// Whether the part-size parameters are well formed: an even non-zero
    /// granule that divides `0 < s_min <= s_max`, and a non-zero `STRIPE_K`.
    const fn is_valid(&self) -> bool {
        self.part_align > 0
            && self.part_align.is_multiple_of(2)
            && self.s_min > 0
            && self.s_min <= self.s_max
            && self.s_min.is_multiple_of(self.part_align)
            && self.s_max.is_multiple_of(self.part_align)
            && self.stripe_k > 0
    }

    /// The part size `S` for a body of `body_bytes` bytes (decision-18): the
    /// smallest granule multiple that fits the body in one stripe of
    /// `STRIPE_K` source parts, clamped to `S_MIN..=S_MAX`.
    pub fn part_bytes_for(&self, body_bytes: u64) -> u32 {
        let fit = body_bytes
            .div_ceil(u64::from(self.stripe_k))
            .div_ceil(u64::from(self.part_align))
            .saturating_mul(u64::from(self.part_align));
        // The clamp bounds `fit` by `s_max`, which is a u32.
        fit.clamp(u64::from(self.s_min), u64::from(self.s_max)) as u32
    }

    /// Derives the unique canonical [`Coding`] for a block body `B'` of
    /// `body_bytes` bytes and `tx_count` total transactions (including coinbase).
    ///
    /// An empty `B'` has no parts: `K = stripes = k = n = 0`. Its block is
    /// the header and the coinbase, which `HeaderMeta` already carries.
    pub fn derive_coding(&self, body_bytes: u64, tx_count: u32) -> Result<Coding, LayoutError> {
        if tx_count == 0 {
            return Err(LayoutError::ZeroTransactions);
        }
        let part_bytes = self.part_bytes_for(body_bytes);
        let s = u64::from(part_bytes);
        let total_source =
            u32::try_from(body_bytes.div_ceil(s)).map_err(|_| LayoutError::TooManyStripes)?;
        let stripes = total_source.div_ceil(self.stripe_k);
        if stripes > self.max_stripes {
            return Err(LayoutError::TooManyStripes);
        }
        // An empty body has no stripes, so `k` and `n` are zero.
        let k = if stripes == 0 {
            0
        } else {
            total_source.div_ceil(stripes)
        };
        let parity = Self::parity_parts(k);
        // For a non-empty body `total_source >= 1` and `stripes <= total_source`,
        // so `1 <= k <= stripe_k` and `n = k + ceil(k / 3) > k`.
        let n = k
            .checked_add(parity)
            .ok_or(LayoutError::ArithmeticOverflow)?;
        // Verify padded body size fits in u64 without overflow.
        u64::from(stripes)
            .checked_mul(u64::from(k))
            .and_then(|parts| parts.checked_mul(s))
            .ok_or(LayoutError::ArithmeticOverflow)?;

        Ok(Coding {
            codec_id: W2_CODEC_ID,
            body_bytes,
            tx_count,
            part_bytes,
            stripes,
            k,
            n,
        })
    }

    /// The coding of the largest body this profile accepts, with every stripe
    /// at the largest `n`. Every block's parts fit in its
    /// [`Coding::encoded_bytes`], because `S` and the stripe count never
    /// shrink as the body grows.
    pub fn max_coding(&self) -> Coding {
        let part_bytes = self.part_bytes_for(self.max_block_bytes);
        let stripes = self
            .max_block_bytes
            .div_ceil(u64::from(part_bytes))
            .div_ceil(u64::from(self.stripe_k))
            .min(u64::from(self.max_stripes));
        Coding {
            codec_id: W2_CODEC_ID,
            body_bytes: self.max_block_bytes,
            tx_count: 2,
            part_bytes,
            // `max_stripes` is a u32, so the clamped count fits.
            stripes: stripes as u32,
            k: self.stripe_k,
            n: self.max_n(),
        }
    }

    /// Validates an untrusted [`Coding`] tuple against this profile, the
    /// header length, and the coinbase length using checked arithmetic (bpv1-10).
    pub fn validate_coding(
        &self,
        coding: &Coding,
        header_len: usize,
        coinbase_len: usize,
    ) -> Result<(), LayoutError> {
        if coding.codec_id != W2_CODEC_ID {
            return Err(LayoutError::UnsupportedCodec(coding.codec_id));
        }
        if header_len == 0 || header_len > W2_MAX_HEADER_BYTES {
            return Err(LayoutError::HeaderLengthOutOfRange(header_len));
        }
        if coinbase_len == 0 || coinbase_len > W2_MAX_COINBASE_BYTES {
            return Err(LayoutError::CoinbaseLengthOutOfRange(coinbase_len));
        }
        // The derivation gives zero stripes exactly when `body_bytes` is zero,
        // so this rejects a zero-part layout with body bytes and a layout
        // with parts but no body bytes.
        let expected = self.derive_coding(coding.body_bytes, coding.tx_count)?;
        if *coding != expected {
            return Err(LayoutError::CodingMismatch);
        }
        // A single-transaction block has no post-coinbase transactions (`B'` is empty).
        if coding.tx_count == 1 && coding.body_bytes != 0 {
            return Err(LayoutError::CodingMismatch);
        }
        if coding.tx_count > 1 && coding.body_bytes == 0 {
            return Err(LayoutError::CodingMismatch);
        }
        // Widening u32 to usize is lossless on supported targets.
        let count_prefix_len = compact_size_len(coding.tx_count as usize);
        // Widening usize to u64 is lossless on supported targets.
        let total_block_bytes = (header_len as u64)
            .checked_add(count_prefix_len as u64)
            .and_then(|v| v.checked_add(coinbase_len as u64))
            .and_then(|v| v.checked_add(coding.body_bytes))
            .ok_or(LayoutError::ArithmeticOverflow)?;
        if total_block_bytes > self.max_block_bytes {
            return Err(LayoutError::BlockTooLarge {
                total: total_block_bytes,
                max: self.max_block_bytes,
            });
        }
        Ok(())
    }
}

/// Compile-time checks tying W2 constants directly to [`Profile::W2`] (decision-4).
const _: () = {
    assert!(Profile::W2.s_max == W2_S_MAX);
    assert!(Profile::W2.stripe_k == W2_STRIPE_K);
    assert!(Profile::W2.max_n() == W2_MAX_N);
    assert!(Profile::W2.max_proof_depth() == W2_MAX_PROOF_DEPTH);
    assert!(Profile::W2.is_valid());
    assert!(Profile::TINY.is_valid());
};

/// Canonical coding parameters carried in `HeaderMeta` and committed by the coinbase.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Coding {
    /// Codec identifier (W2: `2`).
    pub codec_id: u16,
    /// Exact byte length of `B'` (all transactions after the coinbase, without count prefix).
    pub body_bytes: u64,
    /// Total transaction count in the block, coinbase included.
    pub tx_count: u32,
    /// Byte length `S` of every padded part payload, chosen per block (decision-18).
    pub part_bytes: u32,
    /// Number of independent Reed–Solomon stripes.
    pub stripes: u32,
    /// Source parts per stripe (`0 < k < n`, or `0` for an empty body).
    pub k: u32,
    /// Total coded parts per stripe (`k + parity`, or `0` for an empty body).
    pub n: u32,
}

impl Coding {
    /// Canonical 30-byte little-endian serialization used in wire frames and
    /// commitment transcripts.
    pub fn to_bytes(&self) -> [u8; CODING_BYTES] {
        let mut out = [0u8; CODING_BYTES];
        out[0..2].copy_from_slice(&self.codec_id.to_le_bytes());
        out[2..10].copy_from_slice(&self.body_bytes.to_le_bytes());
        out[10..14].copy_from_slice(&self.tx_count.to_le_bytes());
        out[14..18].copy_from_slice(&self.part_bytes.to_le_bytes());
        out[18..22].copy_from_slice(&self.stripes.to_le_bytes());
        out[22..26].copy_from_slice(&self.k.to_le_bytes());
        out[26..30].copy_from_slice(&self.n.to_le_bytes());
        out
    }

    /// Parses a 30-byte little-endian coding tuple.
    pub fn from_bytes(bytes: &[u8; CODING_BYTES]) -> Self {
        Self {
            codec_id: u16::from_le_bytes([bytes[0], bytes[1]]),
            body_bytes: u64::from_le_bytes([
                bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9],
            ]),
            tx_count: u32::from_le_bytes([bytes[10], bytes[11], bytes[12], bytes[13]]),
            part_bytes: u32::from_le_bytes([bytes[14], bytes[15], bytes[16], bytes[17]]),
            stripes: u32::from_le_bytes([bytes[18], bytes[19], bytes[20], bytes[21]]),
            k: u32::from_le_bytes([bytes[22], bytes[23], bytes[24], bytes[25]]),
            n: u32::from_le_bytes([bytes[26], bytes[27], bytes[28], bytes[29]]),
        }
    }

    /// Total coded parts (`stripes * n`).
    pub fn total_parts(&self) -> Option<u32> {
        self.stripes.checked_mul(self.n)
    }

    /// The most bytes this coding's parts hold: every coded part padded to
    /// `S`, each with a proof of the deepest W2 depth.
    pub fn encoded_bytes(&self) -> u64 {
        u64::from(self.stripes)
            .saturating_mul(u64::from(self.n))
            .saturating_mul(u64::from(self.part_bytes).saturating_add(PART_PROOF_BYTES))
    }

    /// Total padded source bytes (`stripes * k * S`).
    pub fn padded_source_bytes(&self) -> Option<usize> {
        let total = u64::from(self.stripes)
            .checked_mul(u64::from(self.k))?
            .checked_mul(u64::from(self.part_bytes))?;
        usize::try_from(total).ok()
    }

    /// Power-of-two leaf width of one stripe subtree (`next_pow2(n)`, which
    /// is `1` for an empty body).
    pub fn stripe_width(&self) -> u32 {
        self.n.next_power_of_two()
    }

    /// Power-of-two stripe count (`next_pow2(stripes)`, which is `1` for an
    /// empty body).
    pub fn padded_stripes(&self) -> u32 {
        self.stripes.next_power_of_two()
    }

    /// Depth of each stripe subtree (`log2(next_pow2(n))`).
    pub fn stripe_tree_depth(&self) -> usize {
        self.stripe_width().trailing_zeros() as usize
    }

    /// Full Merkle tree depth (`log2(next_pow2(stripes)) + log2(next_pow2(n))`).
    pub fn tree_depth(&self) -> usize {
        self.padded_stripes().trailing_zeros() as usize + self.stripe_tree_depth()
    }

    /// Wire index for `(stripe, slot)` (`stripe * n + slot`).
    pub fn wire_index(&self, stripe: u32, slot: u32) -> Option<u32> {
        if stripe >= self.stripes || slot >= self.n {
            return None;
        }
        stripe.checked_mul(self.n)?.checked_add(slot)
    }

    /// `(stripe, slot)` for a wire index `i < stripes * n`.
    pub fn split_index(&self, index: u32) -> Option<(u32, u32)> {
        if self.n == 0 || index >= self.total_parts()? {
            return None;
        }
        Some((index / self.n, index % self.n))
    }

    /// Payload bytes of part `index` on the wire, `wire_len(i)` (01 §4,
    /// decision-18): a source part ends where the body ends, and a parity
    /// part carries all `S` bytes.
    pub fn wire_len(&self, index: u32) -> Option<u32> {
        let (stripe, slot) = self.split_index(index)?;
        if slot >= self.k {
            return Some(self.part_bytes);
        }
        let start = (u64::from(stripe) * u64::from(self.k) + u64::from(slot))
            .saturating_mul(u64::from(self.part_bytes));
        // The clamp bounds the length by `part_bytes`, which is a u32.
        Some(
            self.body_bytes
                .saturating_sub(start)
                .min(u64::from(self.part_bytes)) as u32,
        )
    }

    /// Whether part `index` holds only padding (`wire_len = 0`). Such a part
    /// exists only when `stripes * k > K >= 1`; it is held from admission and
    /// is never announced, requested, or sent (decision-18). Fewer than
    /// `stripes` parts are padding, and every stripe keeps at least one source
    /// part with body bytes. An empty body has no parts at all.
    pub fn is_padding(&self, index: u32) -> bool {
        self.wire_len(index) == Some(0)
    }

    /// Merkle leaf position for wire index `i`: `(i / n) * next_pow2(n) + (i mod n)`.
    pub fn tree_position(&self, index: u32) -> Option<u32> {
        let (stripe, slot) = self.split_index(index)?;
        stripe.checked_mul(self.stripe_width())?.checked_add(slot)
    }
}

/// Errors returned when validating a coding tuple or layout.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Error)]
pub enum LayoutError {
    /// Block declared zero transactions (a block must carry at least a coinbase).
    #[error("tx_count must be at least 1")]
    ZeroTransactions,
    /// Codec identifier does not match the negotiated profile.
    #[error("unsupported codec identifier {0}")]
    UnsupportedCodec(u16),
    /// Coding parameters do not match those derived from `body_bytes` and the profile.
    #[error("coding parameters do not match profile derivation")]
    CodingMismatch,
    /// Block requires more stripes than `MAX_STRIPES`.
    #[error("stripe count exceeds profile limit")]
    TooManyStripes,
    /// Checked size arithmetic overflowed.
    #[error("layout size arithmetic overflow")]
    ArithmeticOverflow,
    /// Header length is zero or exceeds `max_header_bytes`.
    #[error("header length {0} out of profile bounds")]
    HeaderLengthOutOfRange(usize),
    /// Coinbase length is zero or exceeds `max_coinbase_bytes`.
    #[error("coinbase length {0} out of profile bounds")]
    CoinbaseLengthOutOfRange(usize),
    /// Reconstructed block would exceed the profile's maximum block size.
    #[error("block size {total} exceeds maximum {max}")]
    BlockTooLarge {
        /// Total assembled block size in bytes.
        total: u64,
        /// Profile limit in bytes.
        max: u64,
    },
}
