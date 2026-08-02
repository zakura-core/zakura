//! Tests for cookie file creation security.

use std::fs;

use super::super::cookie;
use crate::server::cookie::Cookie;

#[test]
fn cookie_authenticate_matches_only_exact_cookie() {
    let _init_guard = zakura_test::init();

    let cookie = Cookie::default();
    let dir = tempfile::tempdir().unwrap();

    cookie::write_to_disk(&cookie, dir.path(), None).unwrap();

    let cookie_value = fs::read_to_string(dir.path().join(".cookie"))
        .unwrap()
        .trim_start_matches("__cookie__:")
        .to_string();

    assert!(cookie.authenticate(cookie_value.clone()));
    assert!(!cookie.authenticate(format!("{cookie_value}extra")));

    let mut wrong_same_len = cookie_value.clone();
    wrong_same_len.replace_range(0..1, "A");
    if wrong_same_len == cookie_value {
        wrong_same_len.replace_range(0..1, "B");
    }

    assert!(!cookie.authenticate(wrong_same_len));
}

#[test]
fn cookie_file_has_restrictive_permissions() {
    let _init_guard = zakura_test::init();

    let dir = tempfile::tempdir().unwrap();
    let cookie = Cookie::default();

    cookie::write_to_disk(&cookie, dir.path(), None).unwrap();

    let cookie_path = dir.path().join(".cookie");
    let metadata = fs::metadata(&cookie_path).unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o600,
            "cookie file should have mode 0600, got {mode:o}"
        );
    }

    assert!(metadata.len() > 0, "cookie file should not be empty");
}

#[cfg(unix)]
#[test]
fn cookie_write_replaces_existing_permissive_file() {
    let _init_guard = zakura_test::init();

    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let cookie_path = dir.path().join(".cookie");
    fs::write(&cookie_path, b"old cookie").unwrap();
    fs::set_permissions(&cookie_path, fs::Permissions::from_mode(0o644)).unwrap();

    let cookie = Cookie::default();
    cookie::write_to_disk(&cookie, dir.path(), None).unwrap();

    let metadata = fs::metadata(&cookie_path).unwrap();
    let mode = metadata.permissions().mode() & 0o777;
    assert_eq!(
        mode, 0o600,
        "cookie file should have mode 0600 after replacement, got {mode:o}"
    );

    assert!(
        fs::read_to_string(&cookie_path)
            .unwrap()
            .starts_with("__cookie__:"),
        "cookie file should contain a fresh cookie"
    );
}

#[cfg(unix)]
#[test]
fn cookie_write_rejects_symlink() {
    let _init_guard = zakura_test::init();

    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("decoy");
    fs::write(&target, b"").unwrap();

    std::os::unix::fs::symlink(&target, dir.path().join(".cookie")).unwrap();

    let cookie = Cookie::default();
    let result = cookie::write_to_disk(&cookie, dir.path(), None);
    assert!(result.is_err(), "should reject symlink at cookie path");
}
