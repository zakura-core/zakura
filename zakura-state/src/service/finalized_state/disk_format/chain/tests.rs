//! Chain-format serialization tests.

use zakura_chain::{
    block::Block,
    history_tree::{HistoryTreeBlockParts, NonEmptyHistoryTree},
    ironwood, orchard,
    parameters::NetworkUpgrade,
    sapling,
    serialization::ZcashDeserializeInto,
};

use crate::{
    constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
    service::finalized_state::{disk_format::RawBytes, ZakuraDb, STATE_COLUMN_FAMILIES_IN_CODE},
    Config,
};

use super::*;

fn valid_history_tree() -> NonEmptyHistoryTree {
    let network = Network::Mainnet;
    let block: Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .expect("test block must deserialize");
    let sapling_root = sapling::tree::Root::default();
    let orchard_root = orchard::tree::Root::default();
    let ironwood_root = ironwood::tree::Root::default();
    let height = NetworkUpgrade::Heartwood
        .activation_height(&network)
        .expect("Heartwood has a Mainnet activation height");

    NonEmptyHistoryTree::from_cache(
        &network,
        1,
        NonEmptyHistoryTree::from_parts(
            &network,
            HistoryTreeBlockParts {
                header: &block.header,
                height,
                sapling_root: &sapling_root,
                orchard_root: &orchard_root,
                ironwood_root: &ironwood_root,
                sapling_tx: 0,
                orchard_tx: 0,
                ironwood_tx: 0,
            },
        )
        .expect("test history tree must be valid")
        .peaks()
        .clone(),
        height,
    )
    .expect("cached test history tree must be valid")
}

fn legacy_parts_from(tree: &NonEmptyHistoryTree) -> LegacyHistoryTreeParts {
    LegacyHistoryTreeParts {
        network_kind: tree.network().kind(),
        size: tree.size(),
        peaks: tree
            .peaks()
            .iter()
            .map(|(index, entry)| {
                let serialized = bincode::DefaultOptions::new()
                    .serialize(entry)
                    .expect("history tree entry serialization succeeds");
                let mut inner = [0; LEGACY_MAX_ENTRY_SIZE];
                inner.copy_from_slice(&serialized[..LEGACY_MAX_ENTRY_SIZE]);
                (*index, LegacyEntry { inner })
            })
            .collect(),
        current_height: tree.current_height(),
    }
}

fn ephemeral_db(network: &Network) -> ZakuraDb {
    ZakuraDb::new(
        &Config::ephemeral(),
        STATE_DATABASE_KIND,
        &state_database_format_version_in_code(),
        network,
        true,
        STATE_COLUMN_FAMILIES_IN_CODE
            .iter()
            .map(ToString::to_string),
        false,
    )
    .expect("opening an ephemeral finalized state database succeeds")
}

fn write_raw_history_tree(db: &ZakuraDb, key: RawBytes, bytes: Vec<u8>) {
    db.raw_history_tree_cf()
        .new_batch_for_writing()
        .zs_insert(&key, &RawBytes::new_raw_bytes(bytes))
        .write_batch()
        .expect("writing test history tree bytes succeeds");
}

/// A history tree written by a pre-NU6.3 database format must be read in place,
/// zero-padding each entry up to the current width.
#[test]
fn history_tree_parts_reads_legacy_entry_width() {
    let legacy = LegacyHistoryTreeParts {
        network_kind: NetworkKind::Mainnet,
        size: 42,
        peaks: BTreeMap::from([
            (
                0,
                LegacyEntry {
                    inner: [0xAB; LEGACY_MAX_ENTRY_SIZE],
                },
            ),
            (
                5,
                LegacyEntry {
                    inner: [0xCD; LEGACY_MAX_ENTRY_SIZE],
                },
            ),
        ]),
        current_height: Height(1_000),
    };

    let legacy_bytes = bincode::DefaultOptions::new()
        .serialize(&legacy)
        .expect("legacy serialization succeeds");

    let parts =
        HistoryTreeParts::try_from_bytes(&legacy_bytes).expect("legacy snapshot must decode");

    assert_eq!(parts.network_kind, NetworkKind::Mainnet);
    assert_eq!(parts.size, 42);
    assert_eq!(parts.current_height, Height(1_000));
    assert_eq!(parts.peaks.len(), 2);
    assert_eq!(parts.as_bytes(), HistoryTreeParts::from(legacy).as_bytes());
    assert!(parts.as_bytes().len() > legacy_bytes.len());
}

/// A legacy-width row can fail current-width decoding with a non-EOF error if the wider entry
/// consumes into the next legacy map entry and interprets entry bytes as bincode control bytes.
#[test]
fn history_tree_parts_reads_legacy_entry_width_after_non_eof_current_error() {
    let legacy = LegacyHistoryTreeParts {
        network_kind: NetworkKind::Mainnet,
        size: 42,
        peaks: BTreeMap::from([
            (
                0,
                LegacyEntry {
                    inner: [0xAB; LEGACY_MAX_ENTRY_SIZE],
                },
            ),
            (
                5,
                LegacyEntry {
                    inner: {
                        let mut inner = [0xCD; LEGACY_MAX_ENTRY_SIZE];
                        inner[72] = 0xFE;
                        inner
                    },
                },
            ),
        ]),
        current_height: Height(1_000),
    };

    let legacy_bytes = bincode::DefaultOptions::new()
        .serialize(&legacy)
        .expect("legacy serialization succeeds");

    let current_error =
        match bincode::DefaultOptions::new().deserialize::<HistoryTreeParts>(&legacy_bytes) {
            Ok(_) => panic!("legacy bytes must not parse as current-width history tree parts"),
            Err(error) => error,
        };

    assert!(
        !matches!(
            current_error.as_ref(),
            bincode::ErrorKind::Io(io_error)
                if io_error.kind() == std::io::ErrorKind::UnexpectedEof
        ),
        "test fixture must exercise a legacy row with a non-EOF current-width error"
    );

    let parts =
        HistoryTreeParts::try_from_bytes(&legacy_bytes).expect("legacy snapshot must decode");

    assert_eq!(parts.network_kind, NetworkKind::Mainnet);
    assert_eq!(parts.size, 42);
    assert_eq!(parts.current_height, Height(1_000));
    assert_eq!(parts.peaks.len(), 2);
    assert_eq!(parts.as_bytes(), HistoryTreeParts::from(legacy).as_bytes());
    assert!(parts.as_bytes().len() > legacy_bytes.len());
}

/// Data written at the current entry width round-trips without hitting the legacy fallback.
#[test]
fn history_tree_parts_round_trips_current_width() {
    let parts = HistoryTreeParts {
        network_kind: NetworkKind::Testnet,
        size: 3,
        peaks: BTreeMap::from([(
            0,
            zcash_history::Entry::from_raw_bytes_padded(&[7; LEGACY_MAX_ENTRY_SIZE]),
        )]),
        current_height: Height(9),
    };

    let bytes = parts.as_bytes();
    let parsed = HistoryTreeParts::try_from_bytes(&bytes).expect("current snapshot must decode");

    assert_eq!(parsed.network_kind, NetworkKind::Testnet);
    assert_eq!(parsed.size, 3);
    assert_eq!(parsed.current_height, Height(9));
    assert_eq!(parsed.as_bytes(), bytes);
}

#[test]
fn malformed_history_tree_parts_return_both_decode_errors() {
    let error = HistoryTreeParts::try_from_bytes([0xFF]).expect_err("malformed bytes must fail");

    assert!(matches!(
        error,
        HistoryTreeDecodeError::InvalidEncoding {
            current: _,
            legacy: _
        }
    ));
}

#[test]
fn history_tree_parts_reject_wrong_network() {
    let parts = HistoryTreeParts::from(&valid_history_tree());

    let error = parts
        .with_network(&Network::new_default_testnet())
        .expect_err("Mainnet snapshot must not load on Testnet");

    assert!(matches!(
        error,
        HistoryTreeDecodeError::NetworkMismatch {
            stored: NetworkKind::Mainnet,
            configured: NetworkKind::Testnet,
        }
    ));
}

#[test]
fn history_tree_parts_reject_invalid_tree_structure() {
    let height = NetworkUpgrade::Heartwood
        .activation_height(&Network::Mainnet)
        .expect("Heartwood has a Mainnet activation height");
    let parts = HistoryTreeParts {
        network_kind: NetworkKind::Mainnet,
        size: 1,
        peaks: BTreeMap::from([(0, zcash_history::Entry::from_raw_bytes_padded(&[]))]),
        current_height: height,
    };

    let error = parts
        .with_network(&Network::Mainnet)
        .expect_err("invalid cached entry must fail reconstruction");

    assert!(matches!(error, HistoryTreeDecodeError::HistoryTree(_)));
}

#[test]
fn try_history_tree_reads_current_snapshot() {
    let network = Network::Mainnet;
    let db = ephemeral_db(&network);
    let tree = valid_history_tree();
    let parts = HistoryTreeParts::from(&tree);
    write_raw_history_tree(&db, RawBytes::new_raw_bytes(Vec::new()), parts.as_bytes());

    let decoded = db
        .try_history_tree()
        .expect("valid current snapshot must load");

    assert_eq!(decoded.hash(), Some(tree.hash()));
}

#[test]
fn try_history_tree_reads_legacy_snapshot() {
    let network = Network::Mainnet;
    let db = ephemeral_db(&network);
    let tree = valid_history_tree();
    let legacy = legacy_parts_from(&tree);
    let legacy_bytes = bincode::DefaultOptions::new()
        .serialize(&legacy)
        .expect("legacy serialization succeeds");
    write_raw_history_tree(
        &db,
        RawBytes::new_raw_bytes(Height(1).as_bytes().as_ref().to_vec()),
        legacy_bytes,
    );

    let decoded = db
        .try_history_tree()
        .expect("valid legacy snapshot must load");

    assert_eq!(decoded.hash(), Some(tree.hash()));
}

#[test]
fn try_history_tree_propagates_malformed_snapshot() {
    let network = Network::Mainnet;
    let db = ephemeral_db(&network);
    write_raw_history_tree(&db, RawBytes::new_raw_bytes(Vec::new()), vec![0xFF]);

    let error = db
        .try_history_tree()
        .expect_err("malformed snapshot must fail");

    assert!(matches!(
        error,
        HistoryTreeDecodeError::InvalidEncoding {
            current: _,
            legacy: _
        }
    ));
}
