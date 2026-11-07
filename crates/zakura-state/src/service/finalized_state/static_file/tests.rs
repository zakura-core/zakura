//! Tests for transaction static files.

use std::fs::{self, OpenOptions};

use zakura_chain::block::Height;

use super::*;

/// Returns `count` distinct fake transactions for block `height`.
fn transactions(height: u32, count: u8) -> Vec<RawBytes> {
    (0..count)
        .map(|index| {
            let len = 3 + usize::from(index) + (height as usize % 7);
            RawBytes::new_raw_bytes(vec![index ^ height as u8; len])
        })
        .collect()
}

/// Returns blocks `heights` with a varying number of transactions.
fn blocks(heights: impl IntoIterator<Item = u32>) -> Vec<(Height, Vec<RawBytes>)> {
    heights
        .into_iter()
        .map(|height| (Height(height), transactions(height, 1 + (height % 3) as u8)))
        .collect()
}

/// Opens writable static files in `dir` with `blocks_per_file`.
fn open(dir: &Path, blocks_per_file: u32) -> StaticFiles {
    StaticFiles::open(dir.join(STATIC_FILE_DIR), true, blocks_per_file).expect("static files open")
}

/// Checks every block in `expected` reads back, by block and by transaction.
fn assert_blocks(static_files: &StaticFiles, expected: &[(Height, Vec<RawBytes>)]) {
    for (height, transactions) in expected {
        assert_eq!(
            static_files.block_transactions(*height).unwrap().as_ref(),
            Some(transactions),
            "block {height:?}",
        );
        for (index, transaction) in transactions.iter().enumerate() {
            assert_eq!(
                static_files.transaction(*height, index).unwrap().as_ref(),
                Some(transaction),
            );
        }
        assert_eq!(
            static_files
                .transaction(*height, transactions.len())
                .unwrap(),
            None
        );
    }
}

#[test]
fn blocks_per_file_follows_reths_pruned_rule() {
    assert_eq!(blocks_per_file(None), 500_000);
    assert_eq!(blocks_per_file(Some(10_000)), 2_500);
    assert_eq!(blocks_per_file(Some(101)), 1_000);
    assert_eq!(blocks_per_file(Some(u32::MAX)), 500_000);
}

#[test]
fn appended_blocks_read_back_across_file_boundaries_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let expected = blocks(0..2_500);

    let static_files = open(dir.path(), 1_000);
    static_files.append_blocks(&expected[..700]).unwrap();
    static_files.append_blocks(&expected[700..]).unwrap();
    assert_eq!(static_files.tip(), Some(Height(2_499)));
    assert_blocks(&static_files, &expected);
    assert_eq!(
        static_files.block_transactions(Height(2_500)).unwrap(),
        None
    );
    drop(static_files);

    let names: Vec<_> = fs::read_dir(dir.path().join(STATIC_FILE_DIR))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.ends_with(".conf"))
        .collect();
    assert_eq!(names.len(), 3, "one jar per 1,000 block range: {names:?}");

    let reopened = open(dir.path(), 1_000);
    assert_eq!(reopened.tip(), Some(Height(2_499)));
    assert_blocks(&reopened, &expected);
}

#[test]
fn appends_must_be_above_the_tip() {
    let dir = tempfile::tempdir().unwrap();
    let static_files = open(dir.path(), 1_000);
    static_files.append_blocks(&blocks(10..20)).unwrap();

    assert!(matches!(
        static_files.append_blocks(&blocks([19])),
        Err(StaticFileError::OutOfOrder { .. })
    ));
}

#[test]
fn gaps_start_new_jars() {
    let dir = tempfile::tempdir().unwrap();
    let static_files = open(dir.path(), 1_000);
    let first = blocks(0..10);
    let second = blocks(500..510);
    static_files.append_blocks(&first).unwrap();
    static_files.append_blocks(&second).unwrap();

    assert_blocks(&static_files, &first);
    assert_blocks(&static_files, &second);
    assert_eq!(static_files.block_transactions(Height(10)).unwrap(), None);
    assert_eq!(
        static_files.ranges_within(Height(5)..=Height(505)),
        vec![Height(5)..=Height(9), Height(500)..=Height(505)],
    );
}

#[test]
fn interrupted_appends_are_cut_back_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let expected = blocks(0..50);
    let static_files = open(dir.path(), 1_000);
    static_files.append_blocks(&expected).unwrap();
    drop(static_files);

    // Simulate a crash after the data and offsets were written, before the config.
    let base = jar_base(&dir.path().join(STATIC_FILE_DIR), 0, 999);
    for path in [base.clone(), suffixed(&base, "off"), suffixed(&base, "blk")] {
        let file = OpenOptions::new().append(true).open(&path).unwrap();
        std::io::Write::write_all(&mut &file, &[0xAB; 24]).unwrap();
    }

    let reopened = open(dir.path(), 1_000);
    assert_eq!(reopened.tip(), Some(Height(49)));
    assert_blocks(&reopened, &expected);

    // The next append lands where the committed data ended.
    let more = blocks(50..60);
    reopened.append_blocks(&more).unwrap();
    assert_blocks(&reopened, &expected);
    assert_blocks(&reopened, &more);
}

#[test]
fn files_shorter_than_their_config_are_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let static_files = open(dir.path(), 1_000);
    static_files.append_blocks(&blocks(0..50)).unwrap();
    drop(static_files);

    let base = jar_base(&dir.path().join(STATIC_FILE_DIR), 0, 999);
    let data = OpenOptions::new().write(true).open(&base).unwrap();
    let len = data.metadata().unwrap().len();
    data.set_len(len - 1).unwrap();

    assert!(matches!(
        StaticFiles::open(dir.path().join(STATIC_FILE_DIR), true, 1_000),
        Err(StaticFileError::Corrupt { .. })
    ));
}

#[test]
fn uncommitted_jars_are_removed_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let static_dir = dir.path().join(STATIC_FILE_DIR);
    let static_files = open(dir.path(), 1_000);
    static_files.append_blocks(&blocks(0..10)).unwrap();
    drop(static_files);

    // A crash after creating a jar's files, before its first config.
    Jar::create(&static_dir, 1_000, 1_999).unwrap();

    let reopened = open(dir.path(), 1_000);
    assert_eq!(reopened.tip(), Some(Height(9)));
    assert!(!jar_base(&static_dir, 1_000, 1_999).exists());
}

#[test]
fn hostile_offsets_are_rejected_without_large_allocations() {
    let dir = tempfile::tempdir().unwrap();
    let static_files = open(dir.path(), 1_000);
    static_files.append_blocks(&blocks(0..5)).unwrap();
    drop(static_files);

    // Point block 2's second offset far past the data file.
    let base = jar_base(&dir.path().join(STATIC_FILE_DIR), 0, 999);
    let off = OpenOptions::new()
        .write(true)
        .open(suffixed(&base, "off"))
        .unwrap();
    write_all_at(&off, &u64::MAX.to_le_bytes(), INDEX_HEADER_LEN + 8 * 4).unwrap();

    let reopened = open(dir.path(), 1_000);
    assert!(matches!(
        reopened.block_transactions(Height(2)),
        Err(StaticFileError::Corrupt { .. })
    ));
}

#[test]
fn truncate_above_removes_blocks_and_whole_jars() {
    let dir = tempfile::tempdir().unwrap();
    let expected = blocks(0..2_500);
    let static_files = open(dir.path(), 1_000);
    static_files.append_blocks(&expected).unwrap();

    static_files.truncate_above(Some(Height(1_200))).unwrap();
    assert_eq!(static_files.tip(), Some(Height(1_200)));
    assert_blocks(&static_files, &expected[..=1_200]);
    assert_eq!(
        static_files.block_transactions(Height(1_201)).unwrap(),
        None
    );

    // Appending after a truncation reuses the shortened jar.
    static_files.append_blocks(&expected[1_201..1_300]).unwrap();
    assert_blocks(&static_files, &expected[..1_300]);
    drop(static_files);

    let reopened = open(dir.path(), 1_000);
    assert_eq!(reopened.tip(), Some(Height(1_299)));
    reopened.truncate_above(None).unwrap();
    assert_eq!(reopened.tip(), None);
    assert_eq!(reopened.block_transactions(Height(0)).unwrap(), None);
}

#[test]
fn delete_below_removes_only_whole_jars() {
    let dir = tempfile::tempdir().unwrap();
    let expected = blocks(0..2_500);
    let static_files = open(dir.path(), 1_000);
    static_files.append_blocks(&expected).unwrap();

    static_files.delete_below(Height(1_500)).unwrap();
    assert_eq!(static_files.block_transactions(Height(999)).unwrap(), None);
    assert_blocks(&static_files, &expected[1_000..]);
}

#[test]
fn read_only_opens_see_later_commits() {
    let dir = tempfile::tempdir().unwrap();
    let static_dir = dir.path().join(STATIC_FILE_DIR);
    let reader = StaticFiles::open(static_dir.clone(), false, 1_000).unwrap();
    assert_eq!(reader.tip(), None);

    let writer = open(dir.path(), 1_000);
    let expected = blocks(0..1_200);
    writer.append_blocks(&expected).unwrap();

    assert_blocks(&reader, &expected);
}
