//! Malformed note commitment trees must fail to deserialize, rather than panic.
//!
//! Treestate artifacts are untrusted input that store these trees in the
//! [`bincode_format`](crate::serialization::bincode_format).

use crate::{orchard, sapling, serialization::bincode_format, sprout};

/// A zero field element, which is canonical in every pool.
const ZERO: [u8; 32] = [0; 32];

/// Bytes that aren't a canonical Jubjub or Pallas field element.
const NON_CANONICAL: [u8; 32] = [0xff; 32];

/// Encodes a non-empty legacy frontier with a left leaf, followed by an absent cached root.
fn frontier_bytes(position: u8, leaf: [u8; 32], ommers: &[[u8; 32]]) -> Vec<u8> {
    let ommer_count = u8::try_from(ommers.len()).expect("tests use fewer than 251 ommers");

    let mut bytes = vec![1, position, 0];
    bytes.extend_from_slice(&leaf);
    bytes.push(ommer_count);
    for ommer in ommers {
        bytes.extend_from_slice(ommer);
    }
    bytes.push(0);

    bytes
}

/// Encodes an empty frontier with the cached root `root`.
fn cached_root_bytes(root: [u8; 32]) -> Vec<u8> {
    let mut bytes = vec![0, 1];
    bytes.extend_from_slice(&root);

    bytes
}

#[test]
fn consistent_frontiers_deserialize() {
    let bytes = frontier_bytes(0, ZERO, &[]);

    bincode_format::deserialize::<sprout::tree::NoteCommitmentTree>(&bytes)
        .expect("a single-leaf Sprout frontier is valid");
    bincode_format::deserialize::<sapling::tree::NoteCommitmentTree>(&bytes)
        .expect("a single-leaf Sapling frontier is valid");
    bincode_format::deserialize::<orchard::tree::NoteCommitmentTree>(&bytes)
        .expect("a single-leaf Orchard frontier is valid");
}

#[test]
fn inconsistent_frontiers_are_errors() {
    // Position 0 has no ommers, so this frontier has one too many.
    let bytes = frontier_bytes(0, ZERO, &[ZERO]);
    assert_eq!(bytes.len(), 69);

    assert!(bincode_format::deserialize::<sprout::tree::NoteCommitmentTree>(&bytes).is_err());
    assert!(bincode_format::deserialize::<sapling::tree::NoteCommitmentTree>(&bytes).is_err());
    assert!(bincode_format::deserialize::<orchard::tree::NoteCommitmentTree>(&bytes).is_err());
}

#[test]
fn non_canonical_cached_roots_are_errors() {
    let bytes = cached_root_bytes(NON_CANONICAL);

    assert!(bincode_format::deserialize::<sapling::tree::NoteCommitmentTree>(&bytes).is_err());
    assert!(bincode_format::deserialize::<orchard::tree::NoteCommitmentTree>(&bytes).is_err());

    // Sprout roots are hashes, so any 32 bytes are a valid root.
    bincode_format::deserialize::<sprout::tree::NoteCommitmentTree>(&bytes)
        .expect("Sprout roots accept any bytes");
}

#[test]
fn non_canonical_nodes_are_errors() {
    let bytes = frontier_bytes(0, NON_CANONICAL, &[]);

    assert!(bincode_format::deserialize::<sapling::tree::NoteCommitmentTree>(&bytes).is_err());
    assert!(bincode_format::deserialize::<orchard::tree::NoteCommitmentTree>(&bytes).is_err());
}
