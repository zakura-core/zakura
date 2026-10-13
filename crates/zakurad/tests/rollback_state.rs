//! Exercise the standalone rollback utility against disposable on-disk databases.

use std::{path::Path, process::Command, sync::Arc};

use clap::Parser;
use zakura_chain::{
    block::{Block, Height},
    parameters::Network,
    serialization::ZcashDeserializeInto,
    transparent::OutPoint,
};
use zakura_state::{CheckpointVerifiedBlock, Config, FinalizedState, PruningConfig, StorageMode};
use zakurad::commands::rollback_state::RollbackStateCmd;

fn config(cache_dir: &Path, pruned: bool) -> Config {
    Config {
        cache_dir: cache_dir.to_owned(),
        storage_mode: if pruned {
            StorageMode::Pruned(PruningConfig::default())
        } else {
            StorageMode::Archive
        },
        ..Config::default()
    }
}

fn fixture(cache_dir: &Path, pruned: bool) -> Vec<Arc<Block>> {
    let blocks: Vec<Arc<Block>> = [
        zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES.as_ref(),
        zakura_test::vectors::BLOCK_MAINNET_1_BYTES.as_ref(),
        zakura_test::vectors::BLOCK_MAINNET_2_BYTES.as_ref(),
    ]
    .into_iter()
    .map(|bytes: &[u8]| {
        Arc::new(
            bytes
                .zcash_deserialize_into::<Block>()
                .expect("valid mainnet block"),
        )
    })
    .collect();
    let mut state = FinalizedState::new(&config(cache_dir, pruned), &Network::Mainnet)
        .expect("disposable state opens");
    for block in &blocks {
        state
            .commit_finalized_direct(
                CheckpointVerifiedBlock::from(block.clone()).into(),
                None,
                None,
                "standalone rollback test",
            )
            .expect("mainnet fixture commits");
    }
    blocks
}

fn rollback(cache_dir: &Path, extra: &[&str]) -> std::process::Output {
    // Nextest supplies the relocated binary path when running tests from an archive.
    let executable = std::env::var("CARGO_BIN_EXE_zakura-rollback-state")
        .unwrap_or_else(|_| env!("CARGO_BIN_EXE_zakura-rollback-state").to_owned());
    let mut child = Command::new(executable)
        .args(["--network", "Mainnet", "--height", "1", "--cache-dir"])
        .arg(cache_dir)
        .args(extra)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("standalone utility starts");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while child
        .try_wait()
        .expect("utility status is readable")
        .is_none()
    {
        if std::time::Instant::now() >= deadline {
            child.kill().expect("timed-out utility can be stopped");
            child.wait().expect("timed-out utility is reaped");
            panic!("standalone rollback exceeded 60 seconds");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    child
        .wait_with_output()
        .expect("utility output is readable")
}

fn assert_tip_and_utxos(cache_dir: &Path, pruned: bool, blocks: &[Arc<Block>], height: u32) {
    let tip_index = usize::try_from(height).expect("fixture height fits usize");
    let state = FinalizedState::new(&config(cache_dir, pruned), &Network::Mainnet)
        .expect("fixture reopens");
    assert_eq!(state.db.skips_transparent_indexes(), pruned);
    assert_eq!(
        state.db.tip(),
        Some((Height(height), blocks[tip_index].hash()))
    );
    for (block_height, block) in blocks.iter().enumerate().skip(1) {
        for transaction in &block.transactions {
            for index in 0..transaction.outputs().len() {
                let outpoint = OutPoint {
                    hash: transaction.hash(),
                    index: index.try_into().expect("fixture output index fits u32"),
                };
                // These early mainnet blocks only create coinbase outputs, with no spends.
                assert_eq!(
                    state.db.utxo(&outpoint).is_some(),
                    block_height <= tip_index
                );
            }
        }
    }
}

#[test]
fn standalone_pruned_rollback_previews_then_removes_tip_and_outputs() {
    let dir = tempfile::tempdir().expect("disposable directory exists");
    let blocks = fixture(dir.path(), true);
    let preview = rollback(dir.path(), &["--storage-mode", "pruned", "--dry-run"]);
    assert!(
        preview.status.success(),
        "{}",
        String::from_utf8_lossy(&preview.stderr)
    );
    assert!(String::from_utf8_lossy(&preview.stdout).contains("rollback plan"));
    assert_tip_and_utxos(dir.path(), true, &blocks, 2);

    let result = rollback(dir.path(), &["--storage-mode", "pruned"]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(String::from_utf8_lossy(&result.stdout).contains("rollback complete"));
    assert_tip_and_utxos(dir.path(), true, &blocks, 1);
}

#[test]
fn standalone_archive_default_still_rolls_back_indexed_state() {
    let dir = tempfile::tempdir().expect("disposable directory exists");
    let blocks = fixture(dir.path(), false);
    assert!(rollback(dir.path(), &["--dry-run"]).status.success());
    assert_tip_and_utxos(dir.path(), false, &blocks, 2);
    assert!(rollback(dir.path(), &[]).status.success());
    assert_tip_and_utxos(dir.path(), false, &blocks, 1);
}

#[test]
fn standalone_archive_rejects_index_free_state_without_mutation() {
    let dir = tempfile::tempdir().expect("disposable directory exists");
    let blocks = fixture(dir.path(), true);
    for args in [
        vec![],
        vec!["--dry-run"],
        vec!["--storage-mode", "archive"],
        vec!["--storage-mode", "archive", "--dry-run"],
    ] {
        let result = rollback(dir.path(), &args);
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr)
            .contains("cannot open state in archive storage mode"));
        assert_tip_and_utxos(dir.path(), true, &blocks, 2);
    }
}

#[test]
fn rollback_without_override_preserves_configured_pruned_storage() {
    let dir = tempfile::tempdir().expect("disposable directory exists");
    let blocks = fixture(dir.path(), true);
    let command =
        RollbackStateCmd::parse_from(["rollback-state", "--network", "Mainnet", "--height", "1"]);
    command
        .run_with_config(
            config(dir.path(), true),
            zakura_consensus::Config::default(),
        )
        .expect("loaded pruned configuration permits rollback without a CLI override");
    assert_tip_and_utxos(dir.path(), true, &blocks, 1);
}
