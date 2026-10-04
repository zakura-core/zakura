//! Exercise process-wide resource limits in an isolated child process.

use super::*;

#[cfg(unix)]
#[test]
fn open_file_limits_in_child() {
    const CHILD_ENV: &str = "ZAKURA_TEST_OPEN_FILE_LIMIT_CHILD";
    if std::env::var_os(CHILD_ENV).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                concat!(module_path!(), "::open_file_limits_in_child")
                    .strip_prefix("zakura_state::")
                    .unwrap(),
                "--nocapture",
            ])
            .env(CHILD_ENV, "1")
            .output()
            .expect("the test executable can run a child process");
        assert!(
            output.status.success(),
            "child failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("1 passed"),
            "the child must actually run the resource-limit test",
        );
        return;
    }

    let mut limits = open_file_limits().unwrap();
    assert!(limits.rlim_max >= 256, "the child needs 256 open files");
    #[cfg(any(
        target_vendor = "apple",
        target_os = "freebsd",
        target_os = "dragonfly"
    ))]
    {
        let kernel_limit = kernel_open_file_limit().unwrap();
        // Unix resource-limit types are unsigned integers of at most 64 bits.
        let expected_limit = kernel_limit.min(limits.rlim_max as u64);
        limits.rlim_cur = 64;
        set_open_file_limits(&limits).unwrap();
        assert_eq!(increase_nofile_limit(u64::MAX).unwrap(), expected_limit);
        assert_eq!(open_file_limits().unwrap().rlim_max, limits.rlim_max);
    }
    limits.rlim_cur = 64;
    limits.rlim_max = 256;
    set_open_file_limits(&limits).unwrap();

    // A small request must leave the existing soft and hard limits unchanged.
    assert_eq!(increase_nofile_limit(0).unwrap(), 64);
    assert_eq!(increase_nofile_limit(32).unwrap(), 64);
    assert_eq!(open_file_limits().unwrap().rlim_cur, 64);
    assert_eq!(open_file_limits().unwrap().rlim_max, 256);

    // A kernel cap below or equal to the soft limit must never lower it.
    for kernel_limit in [32, 64] {
        assert_eq!(
            increase_nofile_limit_with(128, || Ok(kernel_limit)).unwrap(),
            64
        );
        assert_eq!(current_nofile_limit().unwrap(), 64);
        assert_eq!(open_file_limits().unwrap().rlim_max, 256);
    }

    // Raising the soft limit must preserve the hard limit.
    assert_eq!(increase_nofile_limit(128).unwrap(), 128);
    assert_eq!(open_file_limits().unwrap().rlim_cur, 128);
    assert_eq!(open_file_limits().unwrap().rlim_max, 256);

    // Even an overflowing platform-sized request is clamped to the hard limit.
    assert_eq!(increase_nofile_limit(u64::MAX).unwrap(), 256);
    assert_eq!(open_file_limits().unwrap().rlim_cur, 256);
    assert_eq!(open_file_limits().unwrap().rlim_max, 256);
    assert_eq!(increase_nofile_limit(32).unwrap(), 256);

    // Kernel errors from the setter must reach the caller.
    limits.rlim_cur = 257;
    assert!(set_open_file_limits(&limits).is_err());
}

#[cfg(any(
    target_vendor = "apple",
    target_os = "freebsd",
    target_os = "dragonfly"
))]
#[test]
fn kernel_file_limit_is_readable() {
    assert!(kernel_open_file_limit().unwrap() > 0);
}

#[cfg(not(unix))]
#[test]
fn non_unix_limits_are_unchanged() {
    for requested in [0, 1, 256, u64::MAX] {
        assert_eq!(increase_nofile_limit(requested).unwrap(), requested);
    }
}
