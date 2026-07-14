//! Shielded-format serialization tests.

use super::*;

fn sample_roots() -> CommitmentRootsByHeight {
    CommitmentRootsByHeight {
        sapling: sapling::tree::NoteCommitmentTree::default().root(),
        orchard: orchard::tree::NoteCommitmentTree::default().root(),
        auth_data_root: AuthDataRoot::from([7u8; 32]),
        ironwood: ironwood::tree::NoteCommitmentTree::default().root(),
        sapling_tx: 11,
        orchard_tx: 13,
        ironwood_tx: 17,
    }
}

#[test]
fn commitment_roots_reads_complete_legacy_layouts() {
    let roots = sample_roots();
    let bytes = roots.as_bytes();

    let pre_auth = CommitmentRootsByHeight::from_bytes(&bytes[..PRE_AUTH_DATA_ROOTS_DISK_BYTES]);
    assert_eq!(pre_auth.sapling, roots.sapling);
    assert_eq!(pre_auth.orchard, roots.orchard);
    assert_eq!(pre_auth.auth_data_root, AuthDataRoot::from([0u8; 32]));
    assert_eq!(
        pre_auth.ironwood,
        ironwood::tree::NoteCommitmentTree::default().root()
    );
    assert_eq!(pre_auth.sapling_tx, 0);
    assert_eq!(pre_auth.orchard_tx, 0);
    assert_eq!(pre_auth.ironwood_tx, 0);

    let pre_ironwood = CommitmentRootsByHeight::from_bytes(&bytes[..PRE_IRONWOOD_ROOTS_DISK_BYTES]);
    assert_eq!(pre_ironwood.sapling, roots.sapling);
    assert_eq!(pre_ironwood.orchard, roots.orchard);
    assert_eq!(pre_ironwood.auth_data_root, roots.auth_data_root);
    assert_eq!(
        pre_ironwood.ironwood,
        ironwood::tree::NoteCommitmentTree::default().root()
    );
    assert_eq!(pre_ironwood.sapling_tx, 0);
    assert_eq!(pre_ironwood.orchard_tx, 0);
    assert_eq!(pre_ironwood.ironwood_tx, 0);

    assert_eq!(CommitmentRootsByHeight::from_bytes(bytes), roots);
}

#[test]
#[should_panic(expected = "invalid commitment roots format length")]
fn commitment_roots_rejects_partial_current_layout() {
    let bytes = sample_roots().as_bytes();

    CommitmentRootsByHeight::from_bytes(&bytes[..128]);
}
