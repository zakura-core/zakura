//! Directory compatibility tests run environment cases in isolated processes.

#[cfg(all(unix, not(target_arch = "wasm32")))]
fn run_probe(command: &mut std::process::Command) -> std::process::Output {
    use std::{
        process::Stdio,
        time::{Duration, Instant},
    };

    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the directory probe can run");
    let started = Instant::now();
    loop {
        if child
            .try_wait()
            .expect("the probe's status can be checked")
            .is_some()
        {
            break;
        }
        if started.elapsed() >= Duration::from_secs(30) {
            child.kill().expect("the timed out probe can be stopped");
            child.wait().expect("the stopped probe can be reaped");
            panic!("directory probe exceeded its 30 second timeout");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let output = child
        .wait_with_output()
        .expect("the completed probe's output is available");
    assert!(
        output.status.success(),
        "directory probe failed: {output:?}"
    );
    output
}

#[cfg(all(unix, not(target_arch = "wasm32")))]
#[test]
fn empty_and_missing_home_use_the_same_account_fallback() {
    use std::process::Command;

    let mut results = Vec::new();
    for empty in [false, true] {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "common::directories::tests::directory_probe",
                "--nocapture",
            ])
            .env("ZAKURA_DIRECTORY_PROBE", "fallback")
            .env_remove("HOME");
        if empty {
            command.env("HOME", "");
        }
        let output = run_probe(&mut command);
        let output = String::from_utf8(output.stdout).unwrap();
        results.push(
            output
                .lines()
                .find(|line| line.starts_with("HOME-RESULT:"))
                .unwrap()
                .to_owned(),
        );
    }
    assert_eq!(results[0], results[1]);
}

#[cfg(all(unix, not(target_arch = "wasm32")))]
#[test]
fn home_and_xdg_paths() {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt, process::Command};

    for home in [
        OsString::from("relative-home"),
        OsString::from_vec(b"/home/non-utf8-\xff".to_vec()),
    ] {
        for xdg in ["", "relative-xdg", "/absolute-xdg"] {
            run_probe(
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "common::directories::tests::directory_probe",
                        "--nocapture",
                    ])
                    .env("ZAKURA_DIRECTORY_PROBE", "1")
                    .env("HOME", &home)
                    .env("XDG_CACHE_HOME", xdg)
                    .env("XDG_CONFIG_HOME", xdg),
            );
        }
    }
}

#[cfg(all(unix, not(target_arch = "wasm32")))]
#[test]
// The child probe returns its home lookup result to its parent through stdout.
#[allow(clippy::print_stdout)]
fn directory_probe() {
    use std::path::PathBuf;

    let Some(probe) = std::env::var_os("ZAKURA_DIRECTORY_PROBE") else {
        return;
    };
    if probe == "fallback" {
        println!("HOME-RESULT:{:?}", super::home_dir());
        return;
    }
    let home = PathBuf::from(std::env::var_os("HOME").unwrap());
    assert_eq!(super::home_dir(), Some(home.clone()));

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        assert_eq!(super::cache_dir(), Some(home.join("Library/Caches")));
        assert_eq!(
            super::preference_dir(),
            Some(home.join("Library/Preferences"))
        );
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    {
        let xdg = std::env::var("XDG_CACHE_HOME").unwrap();
        let expected_cache = if xdg == "/absolute-xdg" {
            PathBuf::from("/absolute-xdg")
        } else {
            home.join(".cache")
        };
        let expected_config = if xdg == "/absolute-xdg" {
            PathBuf::from("/absolute-xdg")
        } else {
            home.join(".config")
        };
        assert_eq!(super::cache_dir(), Some(expected_cache));
        assert_eq!(super::preference_dir(), Some(expected_config));
    }
}

#[cfg(windows)]
#[test]
fn windows_known_folders() {
    assert!(super::home_dir().is_some_and(|path| path.is_absolute()));
    assert!(super::cache_dir().is_some_and(|path| path.is_absolute()));
    assert_eq!(super::cache_dir(), super::preference_dir());
}
