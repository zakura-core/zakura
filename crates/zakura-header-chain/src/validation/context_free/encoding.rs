use thiserror::Error;
use zakura_chain::block;

/// Invalid canonical encoding or signed-version semantics for an in-memory header.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid block-header version {version}: {reason}")]
pub struct HeaderEncodingError {
    /// Rejected unsigned storage representation.
    pub version: u32,
    /// Stable reason shared with canonical serialization.
    pub reason: &'static str,
}

/// Validate the signed version rule, then compute the canonical full-header hash.
pub fn validate_encoding_version_hash(
    header: &block::Header,
) -> Result<block::Hash, HeaderEncodingError> {
    block::validate_header_version(header.version).map_err(|reason| HeaderEncodingError {
        version: header.version,
        reason,
    })?;
    Ok(header.hash())
}
