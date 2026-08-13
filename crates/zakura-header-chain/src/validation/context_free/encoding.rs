use thiserror::Error;
use zakura_chain::block;

/// Invalid canonical encoding or signed-version semantics for an in-memory header.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum HeaderEncodingError {
    /// The signed version semantics are invalid.
    #[error("invalid block-header version {version}: {reason}")]
    Version {
        /// Rejected unsigned storage representation.
        version: u32,
        /// Stable reason shared with canonical serialization.
        reason: &'static str,
    },
    /// The timestamp cannot be represented by the canonical wire encoding.
    #[error(
        "invalid block-header timestamp {timestamp}: timestamp must fit in unsigned 32-bit seconds"
    )]
    Timestamp {
        /// Rejected seconds since the Unix epoch.
        timestamp: i64,
    },
}

/// Validate the signed version rule, then compute the canonical full-header hash.
pub fn validate_encoding_version_hash(
    header: &block::Header,
) -> Result<block::Hash, HeaderEncodingError> {
    block::validate_header_version(header.version).map_err(|reason| {
        HeaderEncodingError::Version {
            version: header.version,
            reason,
        }
    })?;
    let timestamp = header.time.timestamp();
    u32::try_from(timestamp).map_err(|_| HeaderEncodingError::Timestamp { timestamp })?;
    Ok(header.hash())
}
