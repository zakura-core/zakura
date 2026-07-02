//! Chain-format serialization tests.

use super::*;

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

    let parts = HistoryTreeParts::from_bytes(&legacy_bytes);

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

    let parts = HistoryTreeParts::from_bytes(&legacy_bytes);

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
    let parsed = HistoryTreeParts::from_bytes(&bytes);

    assert_eq!(parsed.network_kind, NetworkKind::Testnet);
    assert_eq!(parsed.size, 3);
    assert_eq!(parsed.current_height, Height(9));
    assert_eq!(parsed.as_bytes(), bytes);
}
