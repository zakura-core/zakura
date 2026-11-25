//! Canonical block assembly from admitted header, committed coinbase, and
//! decoded source stripes (bpv1-42).

use std::sync::Arc;

use thiserror::Error;
use zakura_chain::{
    block::{Block, Header},
    serialization::ZcashSerialize,
};

use super::profile::{Coding, Profile};
use crate::zakura::wire_codec::{compact_size_len, write_compact_size};

/// Assembles a canonical block from the admitted `header`, committed
/// `coinbase_bytes`, and all `stripes` of `k` source parts (`stripe_sources`).
/// An empty body has no stripes, so its block is
/// `header || compactsize(1) || coinbase`.
///
/// Enforces bpv1-42:
/// - exact stripe and source-part counts;
/// - every padding byte after `coding.body_bytes` is `0x00`;
/// - `header || compactsize(tx_count) || coinbase || B'[..body_bytes]` fits
///   within `profile.max_block_bytes` under checked arithmetic;
/// - canonical parse via `Block::zcash_deserialize_bounded` with zero trailing
///   bytes and `block.transactions.len() == coding.tx_count`.
pub fn assemble_block(
    profile: &Profile,
    header: &Header,
    coding: &Coding,
    coinbase_bytes: &[u8],
    stripe_sources: &[Vec<Arc<[u8]>>],
) -> Result<(Vec<u8>, Block), AssembleError> {
    let stripes = coding.stripes as usize;
    let k = coding.k as usize;
    let part_len = coding.part_bytes as usize;
    if stripe_sources.len() != stripes {
        return Err(AssembleError::IncompleteStripes {
            expected: stripes,
            actual: stripe_sources.len(),
        });
    }

    let padded_len = coding
        .padded_source_bytes()
        .ok_or(AssembleError::SizeOverflow)?;
    let body_bytes = usize::try_from(coding.body_bytes).map_err(|_| AssembleError::SizeOverflow)?;
    if body_bytes > padded_len {
        return Err(AssembleError::SizeOverflow);
    }

    // Serialize header once to verify total block size before allocating the body.
    let header_bytes = header
        .zcash_serialize_to_vec()
        .map_err(|_| AssembleError::InvalidHeader)?;
    // Widening u32 to usize is lossless on supported targets.
    let count_len = compact_size_len(coding.tx_count as usize);
    let total_len = header_bytes
        .len()
        .checked_add(count_len)
        .and_then(|v| v.checked_add(coinbase_bytes.len()))
        .and_then(|v| v.checked_add(body_bytes))
        .ok_or(AssembleError::SizeOverflow)?;
    // Widening usize to u64 is lossless on supported targets.
    if (total_len as u64) > profile.max_block_bytes {
        return Err(AssembleError::BlockTooLarge {
            total: total_len as u64,
            max: profile.max_block_bytes,
        });
    }

    let mut raw = Vec::with_capacity(total_len);
    raw.extend_from_slice(&header_bytes);
    write_compact_size(u64::from(coding.tx_count), &mut raw);
    raw.extend_from_slice(coinbase_bytes);

    let mut written_body = 0usize;
    for stripe in stripe_sources {
        if stripe.len() != k {
            return Err(AssembleError::IncompleteStripeParts {
                expected: k,
                actual: stripe.len(),
            });
        }
        for part in stripe {
            if part.len() != part_len {
                return Err(AssembleError::WrongPartLength {
                    expected: part_len,
                    actual: part.len(),
                });
            }
            if written_body < body_bytes {
                let take = (body_bytes - written_body).min(part_len);
                raw.extend_from_slice(&part[..take]);
                written_body += take;
                if take < part_len && part[take..].iter().any(|&b| b != 0) {
                    return Err(AssembleError::NonZeroPadding);
                }
            } else if part.iter().any(|&b| b != 0) {
                return Err(AssembleError::NonZeroPadding);
            }
        }
    }
    debug_assert_eq!(raw.len(), total_len);

    // Canonical bounded parse (enforced by `cargo xtask check-bounded-decode`).
    let mut cursor = raw.as_slice();
    let block = Block::zcash_deserialize_bounded(&mut cursor)
        .map_err(|_| AssembleError::CanonicalParseFailed)?;
    if !cursor.is_empty() {
        return Err(AssembleError::TrailingBytes(cursor.len()));
    }
    if block.transactions.len() != coding.tx_count as usize {
        return Err(AssembleError::TransactionCountMismatch {
            expected: coding.tx_count as usize,
            actual: block.transactions.len(),
        });
    }
    Ok((raw, block))
}

/// Errors returned while assembling a block from decoded source parts.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Error)]
pub enum AssembleError {
    /// Not every stripe is present.
    #[error("expected {expected} stripes, got {actual}")]
    IncompleteStripes {
        /// Expected stripe count.
        expected: usize,
        /// Supplied stripe count.
        actual: usize,
    },
    /// A stripe did not supply `k` source parts.
    #[error("expected {expected} source parts in stripe, got {actual}")]
    IncompleteStripeParts {
        /// Expected `k`.
        expected: usize,
        /// Supplied count.
        actual: usize,
    },
    /// A source part's length did not equal `S`.
    #[error("expected {expected}-byte part, got {actual}")]
    WrongPartLength {
        /// Profile `S`.
        expected: usize,
        /// Actual length.
        actual: usize,
    },
    /// Padding bytes past `coding.body_bytes` contained a non-zero byte.
    #[error("non-zero padding byte past body_bytes")]
    NonZeroPadding,
    /// Size arithmetic overflowed.
    #[error("block assembly size overflow")]
    SizeOverflow,
    /// Header serialization failed.
    #[error("failed to serialize admitted header")]
    InvalidHeader,
    /// Assembled block exceeded the profile's block-size cap.
    #[error("assembled block size {total} exceeds max {max}")]
    BlockTooLarge {
        /// Assembled size.
        total: u64,
        /// Profile cap.
        max: u64,
    },
    /// Assembled bytes failed `Block::zcash_deserialize_bounded`.
    #[error("assembled block failed canonical bounded parse")]
    CanonicalParseFailed,
    /// Trailing bytes remained after parsing the assembled block.
    #[error("assembled block had {0} trailing bytes")]
    TrailingBytes(usize),
    /// Parsed transaction count differed from `coding.tx_count`.
    #[error("expected {expected} transactions, parsed {actual}")]
    TransactionCountMismatch {
        /// `coding.tx_count`.
        expected: usize,
        /// Parsed transaction count.
        actual: usize,
    },
}
