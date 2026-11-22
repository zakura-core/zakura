//! Tests for rewriting retired seed node IDs in config files.

use std::fs;

use zakura_network::zakura::RETIRED_ZAKURA_SEED_NODE_IDS;

use super::*;

const DEFAULT_ENTRY: &str = zakura_network::zakura::DEFAULT_MAINNET_ZAKURA_BOOTSTRAP_PEERS[0];

fn retired_entry(index: usize, addr: &str) -> (String, String) {
    let (retired, current) = RETIRED_ZAKURA_SEED_NODE_IDS[index];
    (format!("{retired}@{addr}"), format!("{current}@{addr}"))
}

/// Writes `text` to `zakura.toml` in a new temp dir.
fn config_file(text: &str) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir is creatable");
    let path = dir.path().join("zakura.toml");
    fs::write(&path, text).expect("temp config is writable");
    (dir, path)
}

/// In each layout, only the retired IDs change, the rest of the file stays byte for
/// byte, and the backup holds the original.
#[test]
fn rewrite_replaces_only_retired_ids_in_every_layout() {
    let (old_a, new_a) = retired_entry(0, "192.0.2.1:8234");
    let (old_b, new_b) = retired_entry(9, "192.0.2.2:8234");
    let other = format!("{}@192.0.2.3:8234", "ef".repeat(32));

    let layouts = [
        format!(
            "# my node\n[network]\nnetwork = \"Mainnet\"\n\n[network.zakura]\n\
             bootstrap_peers = [\n    \"{old_a}\", # seed\n    \"{other}\",\n    \"{old_b}\",\n]\n"
        ),
        format!("network.zakura.bootstrap_peers = [\"{old_a}\", \"{other}\", \"{old_b}\"] # x\n"),
        format!("[network]\nzakura = {{ bootstrap_peers = ['{old_a}', '{other}', '{old_b}'] }}\n"),
    ];

    for original in layouts {
        let (_dir, path) = config_file(&original);

        let outcome = rewrite_config_file(&path).expect("rewrite succeeds");

        let path = fs::canonicalize(&path).expect("config exists");
        let backup = with_suffix(&path, BACKUP_SUFFIX);
        assert_eq!(
            outcome,
            RewriteOutcome::Rewrote {
                path: path.clone(),
                replaced: vec![
                    (old_a.clone(), new_a.clone()),
                    (old_b.clone(), new_b.clone())
                ],
                backup: backup.clone(),
            }
        );
        let expected = original.replace(&old_a, &new_a).replace(&old_b, &new_b);
        assert_eq!(fs::read_to_string(&path).unwrap(), expected);
        assert_eq!(fs::read_to_string(&backup).unwrap(), original);

        assert_eq!(
            rewrite_config_file(&path).expect("second rewrite succeeds"),
            RewriteOutcome::Unchanged,
        );
    }
}

/// A file without retired IDs, or without the setting, is not written and gets no backup.
#[test]
fn rewrite_leaves_other_files_alone() {
    let current = DEFAULT_ENTRY.to_string();
    for original in [
        format!("[network.zakura]\nbootstrap_peers = [\"{current}\"]\n"),
        "[network]\nnetwork = \"Testnet\"\n".to_string(),
        String::new(),
    ] {
        let (dir, path) = config_file(&original);

        assert_eq!(
            rewrite_config_file(&path).unwrap(),
            RewriteOutcome::Unchanged
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), original);
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1, "no backup");
    }
}

/// An earlier backup is the first original, so a later rewrite keeps it.
#[test]
fn rewrite_keeps_an_existing_backup() {
    let (old, new) = retired_entry(1, "192.0.2.1:8234");
    let original = format!("network.zakura.bootstrap_peers = [\"{old}\"]\n");
    let (_dir, path) = config_file(&original);
    let backup = with_suffix(&fs::canonicalize(&path).unwrap(), BACKUP_SUFFIX);
    fs::write(&backup, "first original").unwrap();

    rewrite_config_file(&path).expect("rewrite succeeds");

    assert_eq!(fs::read_to_string(&backup).unwrap(), "first original");
    assert_eq!(
        fs::read_to_string(&path).unwrap(),
        original.replace(&old, &new)
    );
}

/// A symlinked config is rewritten at its target, and the link stays a link.
#[cfg(unix)]
#[test]
fn rewrite_follows_a_symlink() {
    let (old, new) = retired_entry(2, "192.0.2.1:8234");
    let original = format!("network.zakura.bootstrap_peers = [\"{old}\"]\n");
    let (dir, target) = config_file(&original);
    let link = dir.path().join("link.toml");
    std::os::unix::fs::symlink(&target, &link).unwrap();

    rewrite_config_file(&link).expect("rewrite succeeds");

    assert!(fs::symlink_metadata(&link)
        .unwrap()
        .file_type()
        .is_symlink());
    assert_eq!(
        fs::read_to_string(&target).unwrap(),
        original.replace(&old, &new)
    );
}

/// The rewrite keeps a UTF-8 byte order mark, which the loader accepts.
#[test]
fn rewrite_keeps_a_byte_order_mark() {
    let (old, new) = retired_entry(3, "192.0.2.1:8234");
    let original = format!("\u{feff}network.zakura.bootstrap_peers = [\"{old}\"]\n");
    let (_dir, path) = config_file(&original);

    rewrite_config_file(&path).expect("rewrite succeeds");

    assert_eq!(
        fs::read_to_string(&path).unwrap(),
        original.replace(&old, &new)
    );
}

/// A failed write reports an error and leaves the config as it was.
#[test]
fn failed_write_leaves_the_file_alone() {
    let (old, _new) = retired_entry(4, "192.0.2.1:8234");
    let original = format!("network.zakura.bootstrap_peers = [\"{old}\"]\n");
    let (_dir, path) = config_file(&original);
    // A directory where the temp file goes makes the write fail, even for root.
    fs::create_dir(with_suffix(&fs::canonicalize(&path).unwrap(), ".tmp")).unwrap();

    let error = rewrite_config_file(&path).expect_err("the temp file cannot be written");

    assert!(matches!(error, RewriteError::Io { .. }), "{error}");
    assert_eq!(fs::read_to_string(&path).unwrap(), original);
}

/// A file that does not parse is reported, not rewritten.
#[test]
fn unparseable_file_is_reported() {
    let (_dir, path) = config_file("network.zakura.bootstrap_peers = [");

    let error = rewrite_config_file(&path).expect_err("the file is not TOML");

    assert!(matches!(error, RewriteError::Parse { .. }), "{error}");
}
