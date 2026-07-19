//! Process-lifecycle test bodies for the zcashd-compat integration test suite.

use std::time::Duration;
#[cfg(target_os = "linux")]
use std::{
    ffi::OsString,
    os::unix::ffi::OsStringExt,
    process::{Command, Stdio},
};

use color_eyre::eyre::{eyre, Result};
use tokio::time::sleep;
#[cfg(target_os = "linux")]
use zakura_test::command::CommandExt;

use super::setup_zcashd_compat;

#[cfg(target_os = "linux")]
use super::launch::{send_signal, spawn_zakurad_with_zcashd_compat_config};
#[cfg(target_os = "linux")]
use super::{config::read_test_network_kind, zakura_skip_zcashd_compat_tests};
#[cfg(target_os = "linux")]
use zakura_chain::parameters::NetworkKind;

#[cfg(target_os = "linux")]
struct PidKillGuard(Option<u32>);

#[cfg(target_os = "linux")]
impl PidKillGuard {
    fn new(pid: u32) -> Self {
        Self(Some(pid))
    }

    fn disarm(&mut self) {
        self.0 = None;
    }
}

#[cfg(target_os = "linux")]
impl Drop for PidKillGuard {
    fn drop(&mut self) {
        if let Some(pid) = self.0 {
            let _ = send_signal(pid, "-KILL");
        }
    }
}

#[cfg(target_os = "linux")]
fn process_exists(pid: u32) -> bool {
    std::path::Path::new(&format!("/proc/{pid}")).exists()
}

#[cfg(target_os = "linux")]
async fn wait_for_process_to_stop(pid: u32) -> Result<()> {
    for _ in 0..100 {
        let status = std::fs::read_to_string(format!("/proc/{pid}/status"))?;
        let is_stopped = status
            .lines()
            .find_map(|line| line.strip_prefix("State:"))
            .is_some_and(|state| state.trim_start().starts_with('T'));
        if is_stopped {
            return Ok(());
        }
        sleep(Duration::from_millis(10)).await;
    }

    Err(eyre!(
        "zcashd process {pid} did not enter the stopped state"
    ))
}

/// Verifies that zakurad shuts down cleanly while supervising a running zcashd.
///
/// Only runs in managed (regtest) mode; skipped on external networks where we
/// do not own the zakurad process.
#[cfg(target_os = "linux")]
pub async fn zakurad_clean_shutdown() -> Result<()> {
    if zakura_skip_zcashd_compat_tests() || read_test_network_kind()? != NetworkKind::Regtest {
        return Ok(());
    }

    let mut setup = spawn_zakurad_with_zcashd_compat_config(|config| {
        config.zcashd_compat.shutdown_grace_period = Duration::from_secs(5);
    })
    .await?;

    let zakurad = setup
        .managed
        .take()
        .expect("managed process is present in regtest mode");
    let zakurad_pid = zakurad
        .child
        .as_ref()
        .expect("managed zakurad child is still running")
        .id();
    let zcashd_pid = setup.zcashd_pid()?;
    let mut zcashd_guard = PidKillGuard::new(zcashd_pid);
    let zcashd_executable = std::fs::read_link(format!("/proc/{zcashd_pid}/exe"))?;
    let zcashd_args = std::fs::read(format!("/proc/{zcashd_pid}/cmdline"))?
        .split(|byte| *byte == 0)
        .filter(|arg| !arg.is_empty())
        .skip(1)
        .map(|arg| OsString::from_vec(arg.to_vec()))
        .collect::<Vec<_>>();

    // A stopped child cannot handle SIGTERM, so Zakura must use its bounded
    // supervisor fallback and reap the child before exiting.
    send_signal(zcashd_pid, "-STOP")?;
    wait_for_process_to_stop(zcashd_pid).await?;
    send_signal(zakurad_pid, "-TERM")?;
    let output = zakurad
        .wait_with_output_or_timeout(Duration::from_secs(40))?
        .assert_success()?;

    if !process_exists(zcashd_pid) {
        zcashd_guard.disarm();
    }

    let mut restart_command = Command::new(&zcashd_executable);
    restart_command
        .args(zcashd_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut restarted_zcashd = restart_command.spawn2((), zcashd_executable.to_string_lossy())?;

    let restart_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let restart_status = restarted_zcashd
            .child
            .as_mut()
            .expect("restarted zcashd child is present")
            .try_wait()?;
        if let Some(status) = restart_status {
            let restart_output = restarted_zcashd.wait_with_output()?;
            drop(output);
            return Err(eyre!(
                "zcashd restart exited with {status}\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&restart_output.output.stdout),
                String::from_utf8_lossy(&restart_output.output.stderr),
            ));
        }

        let original_zcashd_exited = !process_exists(zcashd_pid);
        if original_zcashd_exited {
            zcashd_guard.disarm();
        }
        let rpc_ready = if original_zcashd_exited {
            tokio::time::timeout(
                Duration::from_secs(1),
                setup
                    .zcashd_client
                    .json_result_from_call::<serde_json::Value>("getblockchaininfo", "[]"),
            )
            .await
            .is_ok_and(|result| result.is_ok())
        } else {
            false
        };
        if rpc_ready {
            zcashd_guard.disarm();
            restarted_zcashd.kill(false)?;
            restarted_zcashd.wait_with_output()?;
            drop(output);
            return Ok(());
        }

        if tokio::time::Instant::now() >= restart_deadline {
            drop(output);
            return Err(eyre!("restarted zcashd did not become RPC-ready"));
        }

        sleep(Duration::from_millis(250)).await;
    }
}

/// Verifies that zcashd restarts automatically after an unexpected exit while
/// zakurad's supervisor is running.
///
/// Triggers a clean zcashd shutdown via its own `stop` RPC, waits for the
/// supervisor to restart it, then verifies zcashd is responsive again.
///
/// Only runs in managed (regtest) mode.
pub async fn zcashd_restarts_after_exit() -> Result<()> {
    let Some(setup) = setup_zcashd_compat().await? else {
        return Ok(());
    };

    if !setup.can_mutate() {
        return setup.teardown();
    }

    // Ask zcashd to stop gracefully; the zakurad supervisor should restart it.
    let _: serde_json::Value = setup
        .zcashd_client
        .json_result_from_call("stop", "[]")
        .await
        .map_err(|e| eyre!("zcashd stop: {e}"))?;

    // Wait for zcashd to exit and the supervisor to restart it (up to 30 s).
    let mut recovered = false;
    for attempt in 1..=30u32 {
        sleep(Duration::from_secs(1)).await;
        let result = setup
            .zcashd_client
            .json_result_from_call::<serde_json::Value>("getblockchaininfo", "[]")
            .await;
        if result.is_ok() {
            recovered = true;
            break;
        }
        if attempt == 30 {
            break;
        }
    }

    assert!(
        recovered,
        "zcashd did not come back up within 30 s after stop"
    );

    setup.teardown()
}
