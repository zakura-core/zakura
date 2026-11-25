//! Tests for replacing retired seed node IDs in the loaded config and the config file.

use std::fs;

use super::*;

const DEFAULT_ENTRY: &str = zakura_network::zakura::DEFAULT_MAINNET_ZAKURA_BOOTSTRAP_PEERS[0];

/// Some default seeds' `(retired, current)` node IDs, Mainnet and Testnet.
const SEED_IDS: [(&str, &str); 3] = [
    // asia-0
    (
        "1398f62c6d1a457c51ba6a4b5f3dbd2f69fca93216218dc8997e416bd17d93ca",
        "078a60720f0d474743a51674b152f49f575704a77905c16475683cff9d18f246",
    ),
    // us-east-0
    (
        "9ec67ad6834bc2ca0d659c240e042d3446c37cabcc092b527d459c87d938b4a4",
        "a98d73e0114f9f8ae3d5a0b4ec0b8cef20b51d715de6898a39fc3f23948196b7",
    ),
    // zakura-testnet-1
    (
        "57ad39fad4f0bca46cf1ea831772a99d5027b372fef2be5a0ea68e1b5bb4da49",
        "884eb8c4e0d6005bd3a6e086df064efc12b866632dababa4249b2ceb42d43982",
    ),
];

fn retired_entry(seed: usize, addr: &str) -> (String, String) {
    let (retired, current) = SEED_IDS[seed];
    (format!("{retired}@{addr}"), format!("{current}@{addr}"))
}

fn one_entry_file(entry: &str) -> String {
    format!("network.zakura.bootstrap_peers = [\"{entry}\"]\n")
}

/// Writes `text` to `zakura.toml` in a new temp dir.
fn config_file(text: &str) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir is creatable");
    let path = dir.path().join("zakura.toml");
    fs::write(&path, text).expect("temp config is writable");
    (dir, path)
}

/// In each layout, only the retired IDs change and the rest of the file stays byte for
/// byte, with no other file created.
#[test]
fn rewrite_replaces_only_retired_ids_in_every_layout() {
    let (old_a, new_a) = retired_entry(0, "192.0.2.1:8234");
    let (old_b, new_b) = retired_entry(2, "192.0.2.2:8234");
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
        let (dir, path) = config_file(&original);

        let outcome = rewrite_config_file(&path).expect("rewrite succeeds");

        assert_eq!(
            outcome,
            RewriteOutcome::Rewrote {
                path: path.clone(),
                replaced: vec![
                    (old_a.clone(), new_a.clone()),
                    (old_b.clone(), new_b.clone())
                ],
            }
        );
        let expected = original.replace(&old_a, &new_a).replace(&old_b, &new_b);
        assert_eq!(fs::read_to_string(&path).unwrap(), expected);
        assert_eq!(
            fs::read_dir(dir.path()).unwrap().count(),
            1,
            "no other file"
        );

        assert_eq!(
            rewrite_config_file(&path).expect("second rewrite succeeds"),
            RewriteOutcome::Unchanged,
        );
    }
}

/// A file without retired IDs, or without the setting, is not written.
#[test]
fn rewrite_leaves_other_files_alone() {
    let current = DEFAULT_ENTRY.to_string();
    for original in [
        format!("[network.zakura]\nbootstrap_peers = [\"{current}\"]\n"),
        "[network]\nnetwork = \"Testnet\"\n".to_string(),
        String::new(),
    ] {
        let (_dir, path) = config_file(&original);

        assert_eq!(
            rewrite_config_file(&path).unwrap(),
            RewriteOutcome::Unchanged
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), original);
    }
}

/// A file that already names a seed by its current ID keeps the seed's retired entry, so
/// the two never join into one wider bootstrap dial.
#[test]
fn rewrite_keeps_a_retired_entry_when_the_current_id_is_listed() {
    let (old, _) = retired_entry(0, "192.0.2.1:8234");
    let (_, new) = retired_entry(0, "192.0.2.2:8234");
    let original = format!("network.zakura.bootstrap_peers = [\"{old}\", \"{new}\"]\n");
    let (_dir, path) = config_file(&original);

    assert_eq!(
        rewrite_config_file(&path).unwrap(),
        RewriteOutcome::Unchanged
    );
    assert_eq!(fs::read_to_string(&path).unwrap(), original);
}

/// The file is written in place, so it keeps its inode and with it its owner, permissions
/// and access control lists.
#[cfg(unix)]
#[test]
fn rewrite_keeps_the_file() {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let (old, _new) = retired_entry(0, "192.0.2.1:8234");
    let (_dir, path) = config_file(&one_entry_file(&old));
    fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
    let before = fs::metadata(&path).unwrap();

    rewrite_config_file(&path).expect("rewrite succeeds");

    let after = fs::metadata(&path).unwrap();
    assert_eq!(
        (
            after.dev(),
            after.ino(),
            after.uid(),
            after.gid(),
            after.mode()
        ),
        (
            before.dev(),
            before.ino(),
            before.uid(),
            before.gid(),
            before.mode()
        ),
    );
}

/// A symlinked config is rewritten at its target, and the link stays a link.
#[cfg(unix)]
#[test]
fn rewrite_follows_a_symlink() {
    let (old, new) = retired_entry(1, "192.0.2.1:8234");
    let original = one_entry_file(&old);
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
    let (old, new) = retired_entry(1, "192.0.2.1:8234");
    let original = format!("\u{feff}{}", one_entry_file(&old));
    let (_dir, path) = config_file(&original);

    rewrite_config_file(&path).expect("rewrite succeeds");

    assert_eq!(
        fs::read_to_string(&path).unwrap(),
        original.replace(&old, &new)
    );
}

/// A file that changed after it was read, for example because a new config was installed,
/// is not written.
#[test]
fn changed_file_is_not_written() {
    let (old, _new) = retired_entry(1, "192.0.2.1:8234");
    let original = one_entry_file(&old);
    let (_dir, path) = config_file(&original);
    let (edits, _) = id_edits(&original).unwrap();
    let installed = one_entry_file(&format!("{}@192.0.2.5:8234", "ab".repeat(32)));
    fs::write(&path, &installed).unwrap();

    let error = write_if_unchanged(&path, &original, &edits).expect_err("the file changed");

    assert!(matches!(error, RewriteError::Changed { .. }), "{error}");
    assert_eq!(fs::read_to_string(&path).unwrap(), installed);
}

/// A read-only config is the operator's to change, so it is left alone.
#[test]
fn rewrite_leaves_a_read_only_file_alone() {
    let (old, _new) = retired_entry(0, "192.0.2.1:8234");
    let original = one_entry_file(&old);
    let (_dir, path) = config_file(&original);
    let mut permissions = fs::metadata(&path).unwrap().permissions();
    permissions.set_readonly(true);
    fs::set_permissions(&path, permissions).unwrap();

    // Root can open a read-only file for writing, so the explicit check refuses it;
    // anyone else is refused by the open.
    let error = rewrite_config_file(&path).expect_err("the file is read-only");

    assert!(
        matches!(
            error,
            RewriteError::ReadOnly { .. } | RewriteError::Io { .. }
        ),
        "{error}"
    );
    assert_eq!(fs::read_to_string(&path).unwrap(), original);
}

/// A file that does not parse is reported, not rewritten.
#[test]
fn unparseable_file_is_reported() {
    let (_dir, path) = config_file("network.zakura.bootstrap_peers = [");

    let error = rewrite_config_file(&path).expect_err("the file is not TOML");

    assert!(matches!(error, RewriteError::Parse { .. }), "{error}");
}

/// Only an exact node ID match inside `network.zakura.bootstrap_peers` changes. The
/// same ID in a comment, in another setting or in a `bootstrap_peers` outside
/// `network.zakura` stays, and so do entries with other IDs, the order and the addresses.
#[test]
fn rewrite_touches_only_matched_bootstrap_ids() {
    let (retired, current) = SEED_IDS[1];
    let upper = retired.to_uppercase();
    let near_miss = format!("{}0", &retired[..63]);
    let original = format!(
        "# seed {retired} retired\n\
         bootstrap_peers = [\"{retired}@192.0.2.9:1\"]\n\
         [network]\n\
         unconditional_peers = [\"{retired}@peer.example.com:8233\"]\n\
         [network.zakura]\n\
         bootstrap_peers = [\n\
         \x20   \"{near_miss}@192.0.2.1:8234\", # {retired}\n\
         \x20   \"{upper}@[2001:db8::1]:9999\",\n\
         \x20   \"{retired}\",\n\
         \x20   \"{retired}@192.0.2.2:8234\",\n\
         ]\n"
    );
    let (_dir, path) = config_file(&original);

    let outcome = rewrite_config_file(&path).expect("rewrite succeeds");

    let RewriteOutcome::Rewrote { replaced, .. } = outcome else {
        panic!("two entries match");
    };
    assert_eq!(
        replaced,
        vec![
            (
                format!("{upper}@[2001:db8::1]:9999"),
                format!("{current}@[2001:db8::1]:9999")
            ),
            (
                format!("{retired}@192.0.2.2:8234"),
                format!("{current}@192.0.2.2:8234")
            ),
        ]
    );
    let expected = original
        .replace(
            &format!("\"{upper}@[2001:db8::1]:9999\""),
            &format!("\"{current}@[2001:db8::1]:9999\""),
        )
        .replace(
            &format!("\"{retired}@192.0.2.2:8234\""),
            &format!("\"{current}@192.0.2.2:8234\""),
        );
    assert_eq!(fs::read_to_string(&path).unwrap(), expected);
}

/// The check before writing accepts only a file that loads like the original with the
/// given entries replaced.
#[test]
fn rewrite_is_checked_against_the_loader() {
    let (old, new) = retired_entry(1, "192.0.2.1:8234");
    let original = format!("{}other = 1\n", one_entry_file(&old));
    let replaced = [(old.clone(), new.clone())];

    assert!(loads_as_original_with_replacements(
        &original,
        &original.replace(&old, &new),
        &replaced,
    ));
    for wrong in [
        original.clone(),
        original
            .replace(&old, &new)
            .replace("other = 1", "other = 2"),
        original.replace(&old, &new).replace("other = 1", ""),
        original.replace(&old, &format!("{new}x")),
        "not toml [".to_string(),
    ] {
        assert!(
            !loads_as_original_with_replacements(&original, &wrong, &replaced),
            "{wrong}"
        );
    }
}

/// The loaded config gets the same replacement as the file, and an entry the file does not
/// hold, such as one from an environment override, is replaced too.
#[test]
fn loaded_config_follows_the_file() {
    let (old_file, new_file) = retired_entry(0, "192.0.2.1:8234");
    let (old_env, new_env) = retired_entry(2, "192.0.2.2:8234");
    let (_dir, path) = config_file(&one_entry_file(&old_file));
    let mut zakura = ZakuraConfig {
        bootstrap_peers: vec![old_file, old_env.clone()],
        ..ZakuraConfig::default()
    };

    replace_retired_seed_ids(Some(&path), &mut zakura);

    assert_eq!(
        zakura.bootstrap_peers,
        vec![new_file.clone(), new_env.clone()]
    );
    assert_eq!(
        fs::read_to_string(&path).unwrap(),
        one_entry_file(&new_file)
    );

    let mut without_file = ZakuraConfig {
        bootstrap_peers: vec![old_env],
        ..ZakuraConfig::default()
    };
    replace_retired_seed_ids(None, &mut without_file);
    assert_eq!(without_file.bootstrap_peers, vec![new_env]);
}

/// An ID spelled with escapes cannot be overwritten in place, so the file keeps it for the
/// operator to fix, and the loaded config is still switched to the current ID.
#[test]
fn escaped_id_is_left_in_the_file() {
    let (old, new) = retired_entry(1, "192.0.2.1:8234");
    let escaped = format!("\\u{:04x}{}", u32::from(old.as_bytes()[0]), &old[1..]);
    let original = one_entry_file(&escaped);
    let (_dir, path) = config_file(&original);
    let mut zakura = ZakuraConfig {
        bootstrap_peers: vec![old],
        ..ZakuraConfig::default()
    };

    replace_retired_seed_ids(Some(&path), &mut zakura);

    assert_eq!(fs::read_to_string(&path).unwrap(), original);
    assert_eq!(zakura.bootstrap_peers, vec![new]);
}
