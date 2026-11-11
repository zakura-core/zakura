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

/// Opens writable static files in `dir` with `blocks_per_file`, and repairs them.
fn open(dir: &Path, blocks_per_file: u32) -> StaticFiles {
    let static_files = StaticFiles::open(dir.join(STATIC_FILE_DIR), true, blocks_per_file)
        .expect("static files open");
    static_files.repair().expect("static files repair");
    static_files
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

    // A rejected append changed no files, so the store keeps accepting writes.
    assert!(!static_files.has_failed());
    static_files.append_blocks(&blocks(20..25)).unwrap();
    assert_eq!(static_files.tip(), Some(Height(24)));
}

#[cfg(unix)]
#[test]
fn a_failed_write_stops_writes_until_reopen() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let static_dir = dir.path().join(STATIC_FILE_DIR);
    let static_files = open(dir.path(), 1_000);
    static_files.append_blocks(&blocks(0..10)).unwrap();

    // The next block starts a new jar, which can't be created in a read-only directory.
    let set_mode =
        |mode| fs::set_permissions(&static_dir, fs::Permissions::from_mode(mode)).unwrap();
    set_mode(0o500);
    let failed = static_files.append_blocks(&blocks(1_000..1_010));
    set_mode(0o700);
    assert!(matches!(failed, Err(StaticFileError::Io { .. })));

    assert!(static_files.has_failed());
    assert!(matches!(
        static_files.append_blocks(&blocks(1_000..1_010)),
        Err(StaticFileError::Failed)
    ));
    assert!(matches!(
        static_files.delete_below(Height(5)),
        Err(StaticFileError::Failed)
    ));
    assert_blocks(&static_files, &blocks(0..10));
    drop(static_files);

    let reopened = open(dir.path(), 1_000);
    reopened.append_blocks(&blocks(1_000..1_010)).unwrap();
    assert_eq!(reopened.tip(), Some(Height(1_009)));
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
fn oversized_configs_are_corrupt_without_reading_them() {
    let dir = tempfile::tempdir().unwrap();
    let static_files = open(dir.path(), 1_000);
    static_files.append_blocks(&blocks(0..5)).unwrap();
    drop(static_files);

    // A sparse 1 GiB config costs no disk, but reading all of it would.
    let base = jar_base(&dir.path().join(STATIC_FILE_DIR), 0, 999);
    let conf = OpenOptions::new()
        .write(true)
        .open(suffixed(&base, "conf"))
        .unwrap();
    conf.set_len(1 << 30).unwrap();

    assert!(matches!(
        StaticFiles::open(dir.path().join(STATIC_FILE_DIR), false, 1_000),
        Err(StaticFileError::Corrupt { .. })
    ));
}

#[test]
fn writable_opens_change_nothing_until_repair() {
    let dir = tempfile::tempdir().unwrap();
    let static_dir = dir.path().join(STATIC_FILE_DIR);
    let static_files = open(dir.path(), 1_000);
    static_files.append_blocks(&blocks(0..10)).unwrap();
    drop(static_files);

    // A jar that lost its config, and an interrupted write on the committed one.
    let lost = jar_base(&static_dir, 1_000, 1_999);
    open(dir.path(), 1_000)
        .append_blocks(&blocks(1_000..1_010))
        .unwrap();
    fs::remove_file(suffixed(&lost, "conf")).unwrap();
    let base = jar_base(&static_dir, 0, 999);
    let data = OpenOptions::new().append(true).open(&base).unwrap();
    std::io::Write::write_all(&mut &data, &[0xAB; 24]).unwrap();
    let long_len = data.metadata().unwrap().len();

    let reopened = StaticFiles::open(static_dir.clone(), true, 1_000).unwrap();
    assert_eq!(reopened.tip(), Some(Height(9)));
    assert!(lost.exists(), "a lost config's data survives the open");
    assert_eq!(fs::metadata(&base).unwrap().len(), long_len);

    reopened.repair().unwrap();
    assert!(!lost.exists());
    assert_eq!(fs::metadata(&base).unwrap().len(), long_len - 24);
    assert_blocks(&reopened, &blocks(0..10));
}

#[test]
fn repair_keeps_a_leftover_jar_an_append_recommitted() {
    let dir = tempfile::tempdir().unwrap();
    let static_dir = dir.path().join(STATIC_FILE_DIR);
    let static_files = open(dir.path(), 1_000);
    static_files.append_blocks(&blocks(0..10)).unwrap();
    drop(static_files);

    // A crash after creating a jar's files, before its first config.
    Jar::create(&static_dir, 1_000, 1_999).unwrap();

    // Writes don't need a repair first, so one recreates and commits that jar.
    let reopened = StaticFiles::open(static_dir.clone(), true, 1_000).unwrap();
    reopened.append_blocks(&blocks(1_000..1_010)).unwrap();
    reopened.repair().unwrap();
    drop(reopened);

    let reopened = open(dir.path(), 1_000);
    assert_blocks(&reopened, &blocks(0..10));
    assert_blocks(&reopened, &blocks(1_000..1_010));
}

#[test]
fn jars_sharing_a_first_block_are_not_hidden() {
    let dir = tempfile::tempdir().unwrap();
    let static_dir = dir.path().join(STATIC_FILE_DIR);
    let static_files = open(dir.path(), 1_000);
    static_files.append_blocks(&blocks(0..10)).unwrap();
    drop(static_files);

    // A leftover from a crash under a different `blocks_per_file` is removed.
    Jar::create(&static_dir, 0, 1_999).unwrap();
    let reopened = open(dir.path(), 1_000);
    assert_eq!(reopened.tip(), Some(Height(9)));
    assert!(!jar_base(&static_dir, 0, 1_999).exists());
    drop(reopened);

    // A second committed jar with the same first block overlaps the first.
    let other = tempfile::tempdir().unwrap();
    open(other.path(), 2_000)
        .append_blocks(&blocks(0..10))
        .unwrap();
    for entry in fs::read_dir(other.path().join(STATIC_FILE_DIR)).unwrap() {
        let entry = entry.unwrap();
        fs::copy(entry.path(), static_dir.join(entry.file_name())).unwrap();
    }
    assert!(matches!(
        StaticFiles::open(static_dir, true, 1_000),
        Err(StaticFileError::Corrupt { .. })
    ));
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
fn repeated_offsets_are_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let static_files = open(dir.path(), 1_000);
    static_files.append_blocks(&blocks(0..5)).unwrap();
    drop(static_files);

    // Give block 2's first transaction an empty span, below the count cap.
    let base = jar_base(&dir.path().join(STATIC_FILE_DIR), 0, 999);
    let off = OpenOptions::new()
        .read(true)
        .write(true)
        .open(suffixed(&base, "off"))
        .unwrap();
    let mut first = [0; 8];
    read_exact_at(&off, &mut first, INDEX_HEADER_LEN + 8 * 3).unwrap();
    write_all_at(&off, &first, INDEX_HEADER_LEN + 8 * 4).unwrap();

    let reopened = open(dir.path(), 1_000);
    assert!(matches!(
        reopened.block_transactions(Height(2)),
        Err(StaticFileError::Corrupt { .. })
    ));
}

#[test]
fn blocks_over_the_transaction_cap_are_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let static_files = open(dir.path(), 1_000);
    let cap = usize::try_from(Transaction::max_allocation()).unwrap();
    let one_byte = |count| {
        (0..count)
            .map(|_| RawBytes::new_raw_bytes(vec![1]))
            .collect()
    };
    static_files
        .append_blocks(&[(Height(0), one_byte(cap)), (Height(1), one_byte(cap + 1))])
        .unwrap();

    assert_eq!(
        static_files
            .block_transactions(Height(0))
            .unwrap()
            .map(|block| block.len()),
        Some(cap)
    );
    assert!(matches!(
        static_files.block_transactions(Height(1)),
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

    assert!(!static_files.has_files_below(Height(999)));
    assert!(static_files.has_files_below(Height(1_000)));

    static_files.delete_below(Height(1_500)).unwrap();
    assert!(!static_files.has_files_below(Height(1_500)));
    assert_eq!(static_files.block_transactions(Height(999)).unwrap(), None);
    assert_blocks(&static_files, &expected[1_000..]);
}

#[test]
fn read_only_refresh_lists_later_commits() {
    let dir = tempfile::tempdir().unwrap();
    let static_dir = dir.path().join(STATIC_FILE_DIR);
    let writer = open(dir.path(), 1_000);
    writer.append_blocks(&blocks(0..100)).unwrap();
    let reader = StaticFiles::open(static_dir, false, 1_000).unwrap();

    writer.append_blocks(&blocks(100..1_200)).unwrap();
    let all = Height(0)..=Height(1_199);
    assert_eq!(
        reader.ranges_within(all.clone()),
        vec![Height(0)..=Height(99)]
    );
    // Lookups never rescan on their own.
    assert_eq!(reader.block_transactions(Height(150)).unwrap(), None);

    reader.refresh_to(Height(1_199), Height(0)).unwrap();
    assert_eq!(
        reader.ranges_within(all.clone()),
        vec![Height(0)..=Height(999), Height(1_000)..=Height(1_199)]
    );

    // A refresh the known jars already cover doesn't rescan.
    writer.append_blocks(&blocks(1_200..1_300)).unwrap();
    reader.refresh_to(Height(1_100), Height(0)).unwrap();
    assert_eq!(reader.tip(), Some(Height(1_199)));
}

#[test]
fn read_only_refresh_keeps_open_jars_the_writer_pruned() {
    let dir = tempfile::tempdir().unwrap();
    let static_dir = dir.path().join(STATIC_FILE_DIR);
    let writer = open(dir.path(), 1_000);
    let expected = blocks(0..2_100);
    writer.append_blocks(&expected[..2_000]).unwrap();
    let reader = StaticFiles::open(static_dir, false, 1_000).unwrap();

    // The writer prunes the first jar, then appends more.
    writer.delete_below(Height(1_000)).unwrap();
    writer.append_blocks(&expected[2_000..]).unwrap();

    // The reader still retains block 500, so it keeps that jar's open files.
    reader.refresh_to(Height(2_099), Height(500)).unwrap();
    assert_blocks(&reader, &expected);

    // Once its floor passes the jar, the next refresh drops it.
    writer.append_blocks(&blocks(2_100..2_200)).unwrap();
    reader.refresh_to(Height(2_199), Height(1_000)).unwrap();
    assert_eq!(reader.block_transactions(Height(500)).unwrap(), None);
    assert_blocks(&reader, &expected[1_000..]);
}

#[test]
fn read_only_refresh_confirms_only_recorded_heights() {
    let dir = tempfile::tempdir().unwrap();
    let static_dir = dir.path().join(STATIC_FILE_DIR);
    let writer = open(dir.path(), 1_000);
    writer.append_blocks(&blocks(0..1_500)).unwrap();
    let reader = StaticFiles::open(static_dir, false, 1_000).unwrap();

    // The reader's records only confirm block 1,000, so a later height its
    // jars already cover still rescans.
    reader.refresh_to(Height(1_000), Height(0)).unwrap();
    assert_eq!(reader.refreshed_to(), Some(Height(1_000)));
    writer.truncate_above(Some(Height(1_200))).unwrap();
    writer.append_blocks(&blocks(1_201..1_300)).unwrap();
    reader.refresh_to(Height(1_250), Height(0)).unwrap();
    assert_eq!(reader.tip(), Some(Height(1_299)));
    assert_blocks(&reader, &blocks(1_201..1_300));
}

#[test]
fn a_jar_sized_under_another_setting_closes() {
    let dir = tempfile::tempdir().unwrap();
    let static_dir = dir.path().join(STATIC_FILE_DIR);
    let archive = open(dir.path(), 500_000);
    archive.append_blocks(&blocks(0..1_200)).unwrap();
    drop(archive);

    // Reopened with pruned sizing, the next block starts a jar ending on the
    // new boundary instead of filling the archive jar.
    let pruned = open(dir.path(), 1_000);
    pruned.append_blocks(&blocks(1_200..2_100)).unwrap();
    assert!(jar_base(&static_dir, 1_200, 1_999).exists());
    assert!(jar_base(&static_dir, 2_000, 2_999).exists());
    assert_blocks(&pruned, &blocks(0..2_100));
    drop(pruned);

    let reopened = open(dir.path(), 1_000);
    assert_blocks(&reopened, &blocks(0..2_100));
    reopened.delete_below(Height(1_200)).unwrap();
    assert!(!jar_base(&static_dir, 0, 499_999).exists());
    assert_blocks(&reopened, &blocks(1_200..2_100));
}

#[test]
fn read_only_rescans_keep_open_jars_a_prune_hid() {
    // A prune can land mid-scan and hide a jar between two the scan found,
    // jars after the first one it found, or every jar.
    for (hidden, keep_from) in [
        (&[(1_000, 1_999)][..], 0),
        (&[(1_000, 1_999), (2_000, 2_999)][..], 1_500),
        (&[(0, 999), (1_000, 1_999), (2_000, 2_999)][..], 500),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let static_dir = dir.path().join(STATIC_FILE_DIR);
        let expected = blocks(0..3_000);
        open(dir.path(), 1_000).append_blocks(&expected).unwrap();
        let reader = StaticFiles::open(static_dir.clone(), false, 1_000).unwrap();

        for (first, end) in hidden {
            let base = jar_base(&static_dir, *first, *end);
            for suffix in ["conf", "off", "blk"] {
                fs::remove_file(suffixed(&base, suffix)).unwrap();
            }
            fs::remove_file(base).unwrap();
        }

        reader.rescan(Height(keep_from)).unwrap();
        assert_blocks(&reader, &expected[keep_from as usize..]);
    }
}

/// Makes `link` a symbolic link to the file `target`.
#[cfg(any(unix, windows))]
fn symlink_file(target: &Path, link: &Path) {
    #[cfg(unix)]
    std::os::unix::fs::symlink(target, link).unwrap();
    #[cfg(windows)]
    std::os::windows::fs::symlink_file(target, link).unwrap();
}

/// Makes `link` a symbolic link to the directory `target`.
#[cfg(any(unix, windows))]
fn symlink_dir(target: &Path, link: &Path) {
    #[cfg(unix)]
    std::os::unix::fs::symlink(target, link).unwrap();
    #[cfg(windows)]
    std::os::windows::fs::symlink_dir(target, link).unwrap();
}

#[cfg(any(unix, windows))]
#[test]
fn linked_jar_files_are_rejected_without_touching_the_target() {
    for suffix in [None, Some("off"), Some("blk")] {
        let dir = tempfile::tempdir().unwrap();
        let static_dir = dir.path().join(STATIC_FILE_DIR);
        open(dir.path(), 1_000)
            .append_blocks(&blocks(0..10))
            .unwrap();

        // Repair would cut a linked file's target to the committed length.
        let base = jar_base(&static_dir, 0, 999);
        let linked = suffix.map_or(base.clone(), |suffix| suffixed(&base, suffix));
        let outside = dir.path().join("outside");
        fs::write(&outside, vec![7; 100_000]).unwrap();
        fs::remove_file(&linked).unwrap();
        symlink_file(&outside, &linked);

        assert!(StaticFiles::open(static_dir.clone(), true, 1_000).is_err());
        assert!(StaticFiles::open(static_dir, false, 1_000).is_err());
        assert_eq!(fs::read(&outside).unwrap(), vec![7; 100_000], "{suffix:?}");
    }
}

#[cfg(unix)]
#[test]
fn a_fifo_config_is_rejected_without_blocking() {
    let dir = tempfile::tempdir().unwrap();
    let static_dir = dir.path().join(STATIC_FILE_DIR);
    open(dir.path(), 1_000)
        .append_blocks(&blocks(0..10))
        .unwrap();

    let conf = suffixed(&jar_base(&static_dir, 0, 999), "conf");
    fs::remove_file(&conf).unwrap();
    assert!(std::process::Command::new("mkfifo")
        .arg(&conf)
        .status()
        .unwrap()
        .success());

    let (opened, result) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = opened.send(StaticFiles::open(static_dir, true, 1_000).is_err());
    });
    assert!(result
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the open returns instead of waiting for a FIFO writer"));
}

#[cfg(any(unix, windows))]
#[test]
fn commits_replace_a_linked_temporary_config_instead_of_writing_through_it() {
    let dir = tempfile::tempdir().unwrap();
    let static_dir = dir.path().join(STATIC_FILE_DIR);
    let static_files = open(dir.path(), 1_000);
    static_files.append_blocks(&blocks(0..10)).unwrap();

    let outside = dir.path().join("outside");
    fs::write(&outside, b"keep").unwrap();
    symlink_file(
        &outside,
        &suffixed(&jar_base(&static_dir, 0, 999), "conf.tmp"),
    );

    static_files.append_blocks(&blocks(10..20)).unwrap();
    assert_eq!(fs::read(&outside).unwrap(), b"keep");
    drop(static_files);
    assert_blocks(&open(dir.path(), 1_000), &blocks(0..20));
}

#[cfg(any(unix, windows))]
#[test]
fn a_linked_static_file_directory_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let elsewhere = dir.path().join("elsewhere");
    fs::create_dir(&elsewhere).unwrap();
    let static_dir = dir.path().join(STATIC_FILE_DIR);
    symlink_dir(&elsewhere, &static_dir);

    for writable in [true, false] {
        let error = StaticFiles::open(static_dir.clone(), writable, 1_000)
            .expect_err("a linked directory is rejected");
        assert!(
            error.to_string().contains("state.static_files_dir"),
            "the error points at the config field: {error}"
        );
    }
}

#[cfg(windows)]
#[test]
fn a_junction_static_file_directory_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let elsewhere = dir.path().join("elsewhere");
    fs::create_dir(&elsewhere).unwrap();
    let static_dir = dir.path().join(STATIC_FILE_DIR);
    assert!(std::process::Command::new("cmd")
        .arg("/C")
        .arg("mklink")
        .arg("/J")
        .arg(&static_dir)
        .arg(&elsewhere)
        .status()
        .unwrap()
        .success());

    assert!(StaticFiles::open(static_dir.clone(), true, 1_000).is_err());
    assert!(StaticFiles::open(static_dir, false, 1_000).is_err());
}
