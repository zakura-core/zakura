//! Zakura's reviewed Mainnet historical frontier grid, published as a crate.
//!
//! The grid lets a fast-synced archive node answer historical treestate reads across the band
//! it skipped, by anchoring a short replay at the nearest published frontier instead of
//! replaying from genesis. Zakura embeds these bytes at compile time and needs no network
//! access to use them.
//!
//! # This artifact carries no trust weight
//!
//! A note-commitment root is a binding commitment to its frontier, and a consuming node already
//! holds an authenticated root for every height in the band. Every entry taken from this grid is
//! checked against that root before it anchors anything, and an entry that fails is skipped
//! rather than fatal. Corrupt or hostile bytes therefore produce a node that declines to answer,
//! never one that answers wrongly. The digest constants below detect accidental damage and pin
//! the payload's identity; they are not what makes the grid safe to consume.
//!
//! # Payload provenance
//!
//! The bytes come from one immutable release-state bundle produced by an archive node and are
//! packaged by `scripts/pack-assets-crate.py` in the Zakura repository. Entry placement is a
//! deterministic function of the chain rather than of timing, so anyone with a Mainnet archive
//! node can regenerate a byte-identical grid and compare.

include!("generated.rs");

/// Canonical bytes of the Mainnet historical frontier grid.
pub static MAINNET_FRONTIER_GRID: &[u8; MAINNET_FRONTIER_GRID_LEN] =
    include_bytes!("mainnet-frontier-grid.bin");

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::*;

    /// Byte offset of the framing digest: magic, version, network, spacing, checkpoint, count.
    const DIGEST_OFFSET: usize = 8 + 2 + 1 + 4 + 4 + 4;

    /// Fixed header length, including the framing digest.
    const HEADER_LEN: usize = DIGEST_OFFSET + 32;

    #[test]
    fn payload_matches_its_declared_identity() {
        assert_eq!(
            MAINNET_FRONTIER_GRID.len(),
            MAINNET_FRONTIER_GRID_LEN,
            "declared length must describe the embedded payload"
        );
        assert_eq!(
            <[u8; 32]>::from(Sha256::digest(MAINNET_FRONTIER_GRID)),
            MAINNET_FRONTIER_GRID_SHA256,
            "declared digest must describe the embedded payload"
        );
        assert_eq!(
            hex_lower(&MAINNET_FRONTIER_GRID_SHA256),
            MAINNET_FRONTIER_GRID_SHA256_HEX,
            "the hex and byte forms of the digest must agree"
        );
    }

    /// The declared checkpoint and entry count are what a consumer pins against, so they must be
    /// read out of the payload rather than asserted alongside it.
    #[test]
    fn payload_header_matches_its_declared_framing() {
        let bytes = &MAINNET_FRONTIER_GRID[..];
        assert!(bytes.len() >= HEADER_LEN, "payload must contain a header");
        assert_eq!(&bytes[..8], b"ZKVCTFR1", "payload must be a frontier grid");
        assert_eq!(
            u16::from_le_bytes([bytes[8], bytes[9]]),
            1,
            "format version"
        );
        assert_eq!(bytes[10], 1, "network tag must be Mainnet");

        assert_eq!(
            u32::from_le_bytes(bytes[15..19].try_into().expect("four bytes")),
            MAINNET_FRONTIER_GRID_CHECKPOINT,
            "declared checkpoint must match the header"
        );
        assert_eq!(
            u32::from_le_bytes(bytes[19..23].try_into().expect("four bytes")),
            MAINNET_FRONTIER_GRID_ENTRIES,
            "declared entry count must match the header"
        );

        let framing = Sha256::new()
            .chain_update(&bytes[..DIGEST_OFFSET])
            .chain_update(&bytes[HEADER_LEN..])
            .finalize();
        assert_eq!(
            &framing[..],
            &bytes[DIGEST_OFFSET..HEADER_LEN],
            "the payload's own framing digest must cover its header fields and entries"
        );
    }

    fn hex_lower(bytes: &[u8; 32]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}
