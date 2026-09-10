use thiserror::Error;
use zakura_chain::block;

/// Parent-link failure at one exact zero-based header offset.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
#[error("header at offset {offset} names parent {actual:?}, expected {expected:?}")]
pub struct HeaderLinkError {
    /// Zero-based offset of the failing header.
    pub offset: usize,
    /// Expected parent hash.
    pub expected: block::Hash,
    /// Actual parent hash.
    pub actual: block::Hash,
}

/// Validate the first parent link and every internal link in a header run.
pub fn validate_link(
    parent_hash: block::Hash,
    headers: &[block::Header],
) -> Result<(), HeaderLinkError> {
    let mut expected = parent_hash;
    for (offset, header) in headers.iter().enumerate() {
        if header.previous_block_hash != expected {
            return Err(HeaderLinkError {
                offset,
                expected,
                actual: header.previous_block_hash,
            });
        }
        expected = header.hash();
    }
    Ok(())
}
