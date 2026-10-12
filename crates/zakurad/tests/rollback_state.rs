//! Runs the standalone `zakura-rollback-state` binary against small on-disk state databases.
//!
//! The fixtures commit the real Mainnet test vector blocks from genesis to
//! [`FIXTURE_TIP`] through the state service's checkpoint path, so they match the
//! `--network mainnet` the binary parses, and need no mining. The checkpoint path trusts block
//! validity rather than re-checking it, which is the same trust a checkpoint-synced node gives
//! these blocks. The fixtures exercise database kind selection, the dry run, and rollback's
//! reversal of the tip and canonical UTXO set. They keep every block body and stay below
//! Overwinter, so they do not cover missing pruned bodies or history tree replay, and they do not
//! check transparent address index contents; the `zakura-state` rollback tests cover those.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::Arc,
};

use tempfile::TempDir;
use tower::{Service, ServiceExt};

use zakura_chain::{
    block::{self, Block, Height},
    parameters::Network,
    serialization::ZcashDeserializeInto,
    transparent::OutPoint,
};
use zakura_state::{
    CheckpointVerifiedBlock, Config, PruningConfig, Request, StateDatabaseKind, StorageMode,
    ZakuraDb,
};

/// The finalized tip height of every fixture database.
const FIXTURE_TIP: u32 = 10;

/// The height the tests roll back to.
const TARGET: u32 = 5;

/// How a fixture database is created, which decides its [`StateDatabaseKind`].
#[derive(Clone, Copy, Debug)]
enum Fixture {
    /// Pruned checkpoint sync into a fresh cache, which skips transparent indexes.
    WithoutTransparentIndexes,
    /// Archive storage, which builds transparent indexes.
    Indexed,
}

impl Fixture {
    fn config(self, cache_dir: &Path) -> Config {
        let storage_mode = match self {
            Fixture::WithoutTransparentIndexes => StorageMode::Pruned(PruningConfig::default()),
            Fixture::Indexed => StorageMode::Archive,
        };

        Config {
            cache_dir: cache_dir.to_path_buf(),
            ephemeral: false,
            storage_mode,
            checkpoint_sync: true,
            ..Config::default()
        }
    }

    fn kind(self) -> StateDatabaseKind {
        match self {
            Fixture::WithoutTransparentIndexes => StateDatabaseKind::WithoutTransparentIndexes,
            Fixture::Indexed => StateDatabaseKind::Indexed,
        }
    }
}

fn mainnet_blocks() -> Vec<Arc<Block>> {
    (0..=FIXTURE_TIP)
        .map(|height| {
            zakura_test::vectors::CONTINUOUS_MAINNET_BLOCKS
                .get(&height)
                .expect("the continuous Mainnet test vectors cover the fixture heights")
                .zcash_deserialize_into()
                .expect("Mainnet test vector blocks deserialize")
        })
        .collect()
}

/// Returns the coinbase output of `block`, which is spent by no block in the fixture.
fn coinbase_outpoint(block: &Block) -> OutPoint {
    OutPoint {
        hash: block.transactions[0].hash(),
        index: 0,
    }
}

/// Creates a `fixture` database in `cache_dir` with the Mainnet blocks up to [`FIXTURE_TIP`].
fn create_fixture(fixture: Fixture, cache_dir: &Path) {
    let config = fixture.config(cache_dir);
    let blocks = mainnet_blocks();

    // The state service's background tasks hold the database open until their runtime shuts
    // down, so a private runtime releases the database lock before the binary opens it.
    let runtime = tokio::runtime::Runtime::new().expect("test runtime starts");
    runtime.block_on(async move {
        // The last checkpoint is the fixture tip, so a pruned fixture keeps every block body
        // inside its retention window, like the recent blocks of a checkpoint-synced node.
        let (mut state, _, _, _) =
            zakura_state::init(config, &Network::Mainnet, Height(FIXTURE_TIP), 0)
                .await
                .expect("fixture state opens");

        for block in blocks {
            state
                .ready()
                .await
                .expect("fixture state is ready")
                .call(Request::CommitCheckpointVerifiedBlock(
                    CheckpointVerifiedBlock::from(block),
                ))
                .await
                .expect("fixture block commits");
        }
    });
    runtime.shutdown_timeout(std::time::Duration::from_secs(30));

    assert_eq!(
        open(fixture, cache_dir).tip().map(|(height, _)| height),
        Some(Height(FIXTURE_TIP)),
        "the fixture database is fully committed"
    );
    assert!(
        db_path(fixture.kind(), cache_dir).exists(),
        "the fixture created a {:?} database",
        fixture.kind()
    );
}

fn db_path(kind: StateDatabaseKind, cache_dir: &Path) -> PathBuf {
    Fixture::Indexed.config(cache_dir).db_path(
        kind,
        zakura_state::state_database_format_version_in_code().major,
        &Network::Mainnet,
    )
}

/// Opens the fixture database read-only.
fn open(fixture: Fixture, cache_dir: &Path) -> ZakuraDb {
    let (_, db, _) = zakura_state::init_read_only(fixture.config(cache_dir), &Network::Mainnet)
        .expect("fixture database opens read-only");
    db
}

/// Returns every file under `dir` and its contents.
fn snapshot(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut files = BTreeMap::new();
    let mut pending = vec![dir.to_path_buf()];

    while let Some(dir) = pending.pop() {
        for entry in fs::read_dir(&dir).expect("fixture directory is readable") {
            let path = entry.expect("fixture directory entry is readable").path();
            if path.is_dir() {
                pending.push(path);
            } else {
                let contents = fs::read(&path).expect("fixture file is readable");
                files.insert(path, contents);
            }
        }
    }

    files
}

fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).expect("copy destination is writable");

    for entry in fs::read_dir(from).expect("copy source is readable") {
        let path = entry.expect("copy source entry is readable").path();
        let target = to.join(path.file_name().expect("directory entries have names"));
        if path.is_dir() {
            copy_dir(&path, &target);
        } else {
            fs::copy(&path, &target).expect("copying a fixture file succeeds");
        }
    }
}

/// Runs the standalone binary on `cache_dir` with `args` after the common arguments.
fn rollback_state(cache_dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_zakura-rollback-state"))
        .arg("--network")
        .arg("mainnet")
        .arg("--cache-dir")
        .arg(cache_dir)
        .arg("--height")
        .arg(TARGET.to_string())
        .args(args)
        .output()
        .expect("zakura-rollback-state runs")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[track_caller]
fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "zakura-rollback-state failed\nstdout:\n{}\nstderr:\n{}",
        stdout(output),
        stderr(output),
    );
}

/// Asserts that the binary failed with `expected` in its error output, without changing any file
/// in `cache_dir`.
#[track_caller]
fn assert_refused(cache_dir: &Path, args: &[&str], expected: &str) {
    let before = snapshot(cache_dir);
    let output = rollback_state(cache_dir, args);

    assert!(
        !output.status.success(),
        "zakura-rollback-state {args:?} must fail\nstdout:\n{}",
        stdout(&output),
    );
    assert!(
        stderr(&output).contains(expected),
        "zakura-rollback-state {args:?} must report {expected:?}\nstderr:\n{}",
        stderr(&output),
    );
    assert!(
        snapshot(cache_dir) == before,
        "zakura-rollback-state {args:?} must not change the cache directory",
    );
}

/// Asserts that the dry run reports the plan without changing the database.
#[track_caller]
fn assert_dry_run(fixture: Fixture, cache_dir: &Path, args: &[&str]) {
    let blocks = mainnet_blocks();
    let before = snapshot(&db_path(fixture.kind(), cache_dir));

    let output = rollback_state(cache_dir, &[args, &["--dry-run"]].concat());
    assert_success(&output);
    let stdout = stdout(&output);
    assert!(stdout.contains("rollback plan:"), "{stdout}");
    assert!(
        stdout.contains(&format!(
            "new finalized tip: height {TARGET}, hash {}",
            blocks[TARGET as usize].hash()
        )),
        "{stdout}"
    );

    assert!(
        snapshot(&db_path(fixture.kind(), cache_dir)) == before,
        "the dry run must not change the database files"
    );
    let db = open(fixture, cache_dir);
    assert_eq!(
        db.tip(),
        Some((Height(FIXTURE_TIP), blocks[FIXTURE_TIP as usize].hash()))
    );
    for block in &blocks[1..] {
        assert!(db.utxo(&coinbase_outpoint(block)).is_some());
    }
}

/// Asserts that the rollback moved the tip to [`TARGET`] and removed exactly the UTXOs created
/// above it.
#[track_caller]
fn assert_rolled_back(fixture: Fixture, cache_dir: &Path, args: &[&str]) {
    let blocks = mainnet_blocks();

    let output = rollback_state(cache_dir, args);
    assert_success(&output);
    let stdout = stdout(&output);
    assert!(stdout.contains("rollback complete:"), "{stdout}");
    assert!(
        stdout.contains(&format!("rolled-back blocks: {}", FIXTURE_TIP - TARGET)),
        "{stdout}"
    );

    let db = open(fixture, cache_dir);
    let target_hash: block::Hash = blocks[TARGET as usize].hash();
    assert_eq!(db.tip(), Some((Height(TARGET), target_hash)));
    for (height, block) in blocks.iter().enumerate().skip(1) {
        let outpoint = coinbase_outpoint(block);
        if height <= TARGET as usize {
            assert!(
                db.utxo(&outpoint).is_some(),
                "the height {height} coinbase output stays unspent"
            );
        } else {
            assert!(
                db.utxo(&outpoint).is_none(),
                "the height {height} coinbase output was rolled back"
            );
        }
    }
    assert_eq!(
        db.skips_transparent_indexes(),
        !fixture.kind().builds_transparent_indexes(),
        "rollback keeps the database kind"
    );
}

/// The standalone binary defaults to archive storage, so it refuses an index-free pruned
/// database unless `--storage-mode pruned` is passed, which then dry-runs and rolls it back.
#[test]
fn standalone_rollback_of_database_without_transparent_indexes() {
    let _init_guard = zakura_test::init();
    let fixture = Fixture::WithoutTransparentIndexes;
    let cache_dir = TempDir::new().expect("temp dir");
    create_fixture(fixture, cache_dir.path());

    let archive_error = "cannot open state in archive storage mode";
    assert_refused(cache_dir.path(), &["--dry-run"], archive_error);
    assert_refused(cache_dir.path(), &[], archive_error);
    assert_refused(
        cache_dir.path(),
        &["--storage-mode", "archive", "--dry-run"],
        archive_error,
    );
    assert_refused(
        cache_dir.path(),
        &["--storage-mode", "archive"],
        archive_error,
    );
    assert_refused(
        cache_dir.path(),
        &["--storage-mode", "full"],
        "invalid value 'full' for '--storage-mode",
    );

    assert_dry_run(fixture, cache_dir.path(), &["--storage-mode", "pruned"]);
    assert_rolled_back(fixture, cache_dir.path(), &["--storage-mode", "pruned"]);
}

/// The standalone binary still rolls back an indexed archive database by default.
#[test]
fn standalone_rollback_of_indexed_database() {
    let _init_guard = zakura_test::init();
    let fixture = Fixture::Indexed;
    let cache_dir = TempDir::new().expect("temp dir");
    create_fixture(fixture, cache_dir.path());

    assert_dry_run(fixture, cache_dir.path(), &[]);
    assert_dry_run(fixture, cache_dir.path(), &["--storage-mode", "archive"]);
    assert_rolled_back(fixture, cache_dir.path(), &[]);
}

/// A cache directory holding both database kinds is refused in either storage mode, rather than
/// rolling back one of them.
#[test]
fn standalone_rollback_refuses_ambiguous_databases() {
    let _init_guard = zakura_test::init();
    let cache_dir = TempDir::new().expect("temp dir");
    let index_free_dir = TempDir::new().expect("temp dir");
    create_fixture(Fixture::Indexed, cache_dir.path());
    create_fixture(Fixture::WithoutTransparentIndexes, index_free_dir.path());
    copy_dir(
        &db_path(
            StateDatabaseKind::WithoutTransparentIndexes,
            index_free_dir.path(),
        ),
        &db_path(
            StateDatabaseKind::WithoutTransparentIndexes,
            cache_dir.path(),
        ),
    );

    let ambiguous_error = "found both an indexed state database";
    for args in [
        &["--storage-mode", "pruned", "--dry-run"][..],
        &["--storage-mode", "pruned"],
        &["--dry-run"],
        &[],
    ] {
        assert_refused(cache_dir.path(), args, ambiguous_error);
    }
}
