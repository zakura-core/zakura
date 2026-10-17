//! Process-level tests for how `zakurad` selects its configuration, starts, reports
//! errors, and shuts down.
//!
//! Each test runs the real `zakurad` binary in a scratch directory with a cleared
//! environment, and checks its exit status and output streams. Commands that exit on
//! their own without starting a node run through [`TestChild`]. Commands that start a
//! node run through [`Node`], which reads both output streams while the node runs, so a
//! test can wait for a log line or send a signal.

#![allow(clippy::unwrap_in_result)]

use std::{
    env,
    ffi::OsStr,
    fs,
    io::{BufRead, BufReader, Read},
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::{Child, ChildStdout, Command, ExitStatus, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::sync::mpsc;

use tempfile::TempDir;
use zakura_chain::parameters::Network;
use zakura_state::{PruningConfig, StorageMode};
use zakura_test::command::{check_failure_regexes, CommandExt, TestChild};
use zakurad::config::ZakuradConfig;

#[allow(dead_code)]
#[path = "common/failure_messages.rs"]
mod failure_messages;

/// How long a node may take to log a startup milestone or exit.
const PROCESS_TIMEOUT: Duration = Duration::from_secs(120);

/// The log line written once every startup task has been spawned.
#[cfg(unix)]
const STARTED: &str = "spawned initial Zakura tasks";

/// The last log line of a node that stopped without an error.
const STOPPED: &str = "stopping zakurad";

/// A synthetic credential that must never be printed.
const SYNTHETIC_CREDENTIAL: &str = "synthetic-credential-4f1c9a";

/// A scratch directory for one `zakurad` process, with its own home and config directories.
struct Scratch {
    dir: TempDir,
}

impl Scratch {
    fn new() -> Self {
        let dir = tempfile::Builder::new()
            .prefix("zakurad_lifecycle")
            .tempdir()
            .expect("temporary directory is created");
        let scratch = Self { dir };
        fs::create_dir_all(scratch.preference_dir()).expect("config directory is created");
        scratch
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    /// The directory `zakurad` searches for an implicit config file.
    fn preference_dir(&self) -> PathBuf {
        if cfg!(target_os = "macos") {
            self.path().join("home/Library/Preferences")
        } else {
            self.path().join("xdg")
        }
    }

    /// A regtest config that keeps all state and listeners local to this directory.
    fn regtest_config(&self) -> ZakuradConfig {
        let mut config = ZakuradConfig::default();
        config.network.network = Network::new_regtest(Default::default());
        config.network.listen_addr = "127.0.0.1:0".parse().expect("valid address");
        config.network.p2p_stack = zakura_network::P2pStack::Legacy;
        config.network.cache_dir = zakura_network::CacheDir::disabled();
        config.network.identity_dir = self.path().join("identity");
        config.state.ephemeral = true;
        config.state.cache_dir = self.path().join("state");
        config
    }

    fn write_config(&self, path: impl AsRef<Path>, config: &ZakuradConfig) -> PathBuf {
        let config = toml::Value::try_from(config).expect("config serializes to TOML");
        let config = toml::to_string_pretty(&config).expect("TOML value is printable");
        self.write_file(path, &config)
    }

    fn write_file(&self, path: impl AsRef<Path>, contents: &str) -> PathBuf {
        let path = self.path().join(path);
        fs::write(&path, contents).expect("scratch file is written");
        path
    }

    /// Returns a `zakurad` command with only this directory's environment.
    fn command<I, S>(&self, args: I) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        // Archived test runs set the binary's location at runtime.
        let zakurad = env::var_os("CARGO_BIN_EXE_zakurad")
            .unwrap_or_else(|| env!("CARGO_BIN_EXE_zakurad").into());
        let mut command = Command::new(zakurad);
        command
            .args(args)
            .current_dir(self.path())
            .env_clear()
            .env("HOME", self.path().join("home"))
            .env("XDG_CONFIG_HOME", self.preference_dir())
            .env("XDG_CACHE_HOME", self.path().join("cache"))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(path) = env::var_os("PATH") {
            command.env("PATH", path);
        }
        command
    }

    /// Runs a command that exits on its own without starting a node.
    ///
    /// [`TestChild`] reads the output after the process exits, so the command must
    /// write less than a pipe buffer of output.
    fn run(&self, command: &mut Command) -> Run {
        let child: TestChild<()> = command.spawn2((), "zakurad").expect("zakurad starts");
        let output = child
            .wait_with_output_or_timeout(PROCESS_TIMEOUT)
            .expect("zakurad exits in time")
            .output;
        Run {
            status: output.status,
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
    }

    /// Runs a command that starts a node and is expected to exit on its own.
    fn run_node(&self, command: &mut Command) -> Run {
        self.spawn(command).wait()
    }

    /// Starts a long-running command.
    fn spawn(&self, command: &mut Command) -> Node {
        Node::spawn(command)
    }

    /// A regtest config that the node rejects as soon as it opens its state, after it
    /// has logged its configuration.
    fn rejected_state_config(&self) -> ZakuradConfig {
        let mut config = self.regtest_config();
        config.state.storage_mode = StorageMode::Pruned(PruningConfig { tx_retention: 1 });
        config
    }
}

/// A running `zakurad` node whose output is collected in the background.
///
/// Dropping it kills a process that is still running, for example when a test fails.
struct Node {
    child: Child,
    /// Each stdout line as it is read, including a final line without a newline.
    #[cfg(unix)]
    stdout_lines: mpsc::Receiver<String>,
    /// All stdout bytes read so far.
    stdout: Arc<Mutex<Vec<u8>>>,
    stdout_reader: Option<thread::JoinHandle<()>>,
    stderr_reader: Option<thread::JoinHandle<String>>,
}

impl Node {
    fn spawn(command: &mut Command) -> Self {
        let mut child = command.spawn().expect("zakurad starts");
        let stdout = child.stdout.take().expect("stdout is piped");
        let mut stderr = child.stderr.take().expect("stderr is piped");
        #[cfg(unix)]
        let (lines_tx, stdout_lines) = mpsc::channel();
        let collected = Arc::new(Mutex::new(Vec::new()));
        let stdout_reader = Self::read_lines(
            stdout,
            #[cfg(unix)]
            lines_tx,
            collected.clone(),
        );
        let stderr_reader = thread::spawn(move || {
            let mut output = Vec::new();
            let _ = stderr.read_to_end(&mut output);
            String::from_utf8_lossy(&output).into_owned()
        });

        Self {
            child,
            #[cfg(unix)]
            stdout_lines,
            stdout: collected,
            stdout_reader: Some(stdout_reader),
            stderr_reader: Some(stderr_reader),
        }
    }

    fn read_lines(
        stdout: ChildStdout,
        #[cfg(unix)] lines_tx: mpsc::Sender<String>,
        collected: Arc<Mutex<Vec<u8>>>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut stdout = BufReader::new(stdout);
            let mut line = Vec::new();
            while matches!(stdout.read_until(b'\n', &mut line), Ok(1..)) {
                collected
                    .lock()
                    .expect("no reader panics while holding the lock")
                    .extend_from_slice(&line);
                #[cfg(unix)]
                let _ = lines_tx.send(String::from_utf8_lossy(&line).into_owned());
                line.clear();
            }
        })
    }

    fn stdout(&self) -> String {
        String::from_utf8_lossy(&self.stdout.lock().expect("lock is not poisoned")).into_owned()
    }

    /// Waits until a stdout line contains `text`, or panics with the output so far.
    ///
    /// The deadline applies even if the node stops writing output.
    #[cfg(unix)]
    fn wait_for_stdout(&mut self, text: &str) {
        let deadline = Instant::now() + PROCESS_TIMEOUT;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.stdout_lines.recv_timeout(remaining) {
                Ok(line) if line.contains(text) => return,
                Ok(_) => {}
                Err(error) => panic!(
                    "zakurad did not log {text:?} ({error}); stdout so far:\n{}",
                    self.stdout()
                ),
            }
        }
    }

    /// Sends a Unix signal to the process.
    #[cfg(unix)]
    fn signal(&self, signal: &str) {
        let status = Command::new("kill")
            .arg(format!("-{signal}"))
            .arg(self.child.id().to_string())
            .status()
            .expect("kill command runs");
        assert!(status.success(), "signal {signal} is delivered");
    }

    /// Waits for the process to exit and returns all of its output.
    fn wait(mut self) -> Run {
        let deadline = Instant::now() + PROCESS_TIMEOUT;
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("process status is readable") {
                break status;
            }
            if Instant::now() >= deadline {
                panic!(
                    "zakurad did not exit within {PROCESS_TIMEOUT:?}; stdout so far:\n{}",
                    self.stdout()
                );
            }
            thread::sleep(Duration::from_millis(50));
        };
        self.stdout_reader
            .take()
            .expect("output is collected once")
            .join()
            .expect("stdout reader finishes");
        let stderr = self
            .stderr_reader
            .take()
            .expect("output is collected once")
            .join()
            .expect("stderr reader finishes");
        let stdout = self.stdout();

        Run {
            status,
            stdout,
            stderr,
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// The result of one `zakurad` process.
struct Run {
    status: ExitStatus,
    stdout: String,
    stderr: String,
}

impl Run {
    fn assert_status(&self, code: i32) -> &Self {
        assert_eq!(
            self.status.code(),
            Some(code),
            "unexpected exit status {}\nstdout:\n{}\nstderr:\n{}",
            self.status,
            self.stdout,
            self.stderr,
        );
        self
    }

    fn assert_stdout_contains(&self, text: &str) -> &Self {
        assert!(
            self.stdout.contains(text),
            "stdout does not contain {text:?}\nstdout:\n{}\nstderr:\n{}",
            self.stdout,
            self.stderr,
        );
        self
    }

    fn assert_stdout_lacks(&self, text: &str) -> &Self {
        assert!(
            !self.stdout.contains(text),
            "stdout unexpectedly contains {text:?}\nstdout:\n{}",
            self.stdout,
        );
        self
    }

    fn assert_stderr_contains(&self, text: &str) -> &Self {
        assert!(
            self.stderr.contains(text),
            "stderr does not contain {text:?}\nstdout:\n{}\nstderr:\n{}",
            self.stdout,
            self.stderr,
        );
        self
    }

    fn assert_no_output_contains(&self, text: &str) -> &Self {
        assert!(
            !self.stdout.contains(text) && !self.stderr.contains(text),
            "output unexpectedly contains {text:?}\nstdout:\n{}\nstderr:\n{}",
            self.stdout,
            self.stderr,
        );
        self
    }

    /// Asserts that the last stdout line is the clean shutdown line.
    #[cfg(unix)]
    fn assert_stopped_cleanly(&self) -> &Self {
        self.assert_status(0);
        let last_line = self.stdout.lines().last().unwrap_or_default();
        assert!(
            last_line.contains(STOPPED),
            "the last log line is not {STOPPED:?}\nstdout:\n{}\nstderr:\n{}",
            self.stdout,
            self.stderr,
        );
        self
    }
}

/// Returns a local TCP address that stays occupied while the listener is alive.
fn occupied_port() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a local port is available");
    let addr = listener.local_addr().expect("listener has an address");
    (listener, addr)
}

// Configuration selection

/// With no arguments, `zakurad` starts a node using `zakura.toml` from the preference
/// directory, and SIGTERM stops it cleanly after flushing its final log line.
#[cfg(unix)]
#[test]
fn implicit_start_uses_preference_dir_config_and_stops_on_sigterm() {
    let scratch = Scratch::new();
    let config_path = scratch.write_config(scratch.preference_dir().join("zakura.toml"), &{
        let mut config = scratch.regtest_config();
        config.network.peerset_initial_target_size = 11;
        config
    });

    let mut node = scratch.spawn(&mut scratch.command::<_, &str>([]));
    node.wait_for_stdout(STARTED);
    node.signal("TERM");
    let run = node.wait();

    run.assert_stopped_cleanly()
        .assert_stdout_contains(&format!("Using config file at: {config_path:?}"))
        .assert_stdout_contains("peerset_initial_target_size=11")
        .assert_stdout_contains("received SIGTERM, starting shutdown")
        .assert_stdout_contains("shutting down Zakura");
}

/// A legacy `zebrad.toml` in the preference directory is used when `zakura.toml` is absent,
/// with a deprecation warning, and SIGINT stops the node cleanly.
#[cfg(unix)]
#[test]
fn legacy_config_fallback_is_used_and_stops_on_sigint() {
    let scratch = Scratch::new();
    let config_path = scratch.write_config(scratch.preference_dir().join("zebrad.toml"), &{
        let mut config = scratch.regtest_config();
        config.network.peerset_initial_target_size = 13;
        config
    });

    let mut node = scratch.spawn(&mut scratch.command(["start"]));
    node.wait_for_stdout(STARTED);
    node.signal("INT");
    let run = node.wait();

    run.assert_stopped_cleanly()
        .assert_stdout_contains("zebrad.toml is deprecated; rename this config file to zakura.toml")
        .assert_stdout_contains(&format!("Using config file at: {config_path:?}"))
        .assert_stdout_contains("peerset_initial_target_size=13")
        .assert_stdout_contains("received SIGINT, starting shutdown");
}

/// Without any config file, `zakurad` uses defaults with environment overrides applied.
#[cfg(unix)]
#[test]
fn start_without_config_file_uses_defaults_and_environment() {
    let scratch = Scratch::new();

    let mut command = scratch.command(["start"]);
    command
        .env("ZAKURA_NETWORK__NETWORK", "Regtest")
        .env("ZAKURA_NETWORK__LISTEN_ADDR", "127.0.0.1:0")
        .env("ZAKURA_NETWORK__P2P_STACK", "legacy")
        .env("ZAKURA_NETWORK__PEERSET_INITIAL_TARGET_SIZE", "17")
        .env("ZAKURA_STATE__EPHEMERAL", "true")
        .env("ZAKURA_STATE__CACHE_DIR", scratch.path().join("state"));
    let mut node = scratch.spawn(&mut command);
    node.wait_for_stdout(STARTED);
    node.signal("TERM");
    let run = node.wait();

    run.assert_stopped_cleanly()
        .assert_stdout_contains("No config file provided, using default configuration")
        .assert_stdout_contains("network=Regtest")
        .assert_stdout_contains("peerset_initial_target_size=17");
}

/// An explicit `--config` wins over the preference directory, environment variables win
/// over the file, and command-line tracing filters win over the configured filter.
#[cfg(unix)]
#[test]
fn explicit_config_environment_and_cli_overrides_take_precedence() {
    let scratch = Scratch::new();
    scratch.write_config(scratch.preference_dir().join("zakura.toml"), &{
        let mut config = scratch.regtest_config();
        config.network.peerset_initial_target_size = 19;
        config
    });
    let explicit = scratch.write_config("explicit.toml", &{
        let mut config = scratch.regtest_config();
        config.network.peerset_initial_target_size = 23;
        config.network.max_connections_per_ip = 3;
        config.tracing.filter = Some("warn".to_string());
        config
    });

    let mut command = scratch.command([
        OsStr::new("--config"),
        explicit.as_os_str(),
        OsStr::new("start"),
        OsStr::new("info,zakurad=debug"),
    ]);
    command.env("ZAKURA_NETWORK__PEERSET_INITIAL_TARGET_SIZE", "29");
    let mut node = scratch.spawn(&mut command);
    node.wait_for_stdout(STARTED);
    node.signal("TERM");
    let run = node.wait();

    run.assert_stopped_cleanly()
        .assert_stdout_contains(&format!("Using config file at: {explicit:?}"))
        .assert_stdout_contains("peerset_initial_target_size=29")
        .assert_stdout_contains("filter=\"info,zakurad=debug\"");
}

/// When both config files are in the preference directory, `zakura.toml` is used, and the
/// legacy file is neither read nor mentioned.
#[test]
fn zakura_toml_wins_over_legacy_config() {
    let scratch = Scratch::new();
    let config_path = scratch.write_config(scratch.preference_dir().join("zakura.toml"), &{
        let mut config = scratch.rejected_state_config();
        config.network.peerset_initial_target_size = 31;
        config
    });
    // Reading this file would stop startup with a config error.
    scratch.write_file(
        scratch.preference_dir().join("zebrad.toml"),
        "[network]\nnot_a_real_field = 1\n",
    );

    let run = scratch.run_node(&mut scratch.command(["start"]));

    run.assert_status(1)
        .assert_stdout_contains(&format!("Using config file at: {config_path:?}"))
        .assert_stdout_contains("peerset_initial_target_size=31")
        .assert_no_output_contains("zebrad.toml");
}

/// Config sources apply in order: defaults, then the file, then `ZEBRA_` environment
/// variables, then `ZAKURA_` environment variables, then command-line tracing filters.
#[test]
fn config_sources_apply_in_precedence_order() {
    let scratch = Scratch::new();
    let config = scratch.write_config("zakura.toml", &{
        let mut config = scratch.rejected_state_config();
        config.network.peerset_initial_target_size = 47;
        config.tracing.filter = Some("error".to_string());
        config
    });
    let command = |filters: &[&str]| {
        let mut args = vec![OsStr::new("-c"), config.as_os_str(), OsStr::new("start")];
        args.extend(filters.iter().map(OsStr::new));
        let mut command = scratch.command(args);
        command
            .env("ZEBRA_NETWORK__PEERSET_INITIAL_TARGET_SIZE", "53")
            .env("ZAKURA_NETWORK__PEERSET_INITIAL_TARGET_SIZE", "59")
            .env("ZEBRA_TRACING__FILTER", "info");
        command
    };

    // The file overrides the default ephemeral state setting, `ZEBRA_` overrides the
    // file's filter, and `ZAKURA_` overrides `ZEBRA_`.
    let run = scratch.run_node(&mut command(&[]));
    run.assert_status(1)
        .assert_stdout_contains("filter=\"info\"")
        .assert_stdout_contains("peerset_initial_target_size=59")
        .assert_stdout_contains("ephemeral_state=true");

    // Command-line filters override every configured filter.
    let mut with_cli_filter = command(&["info,zakurad=info"]);
    with_cli_filter.env("ZAKURA_TRACING__FILTER", "warn");
    let run = scratch.run_node(&mut with_cli_filter);
    run.assert_status(1)
        .assert_stdout_contains("filter=\"info,zakurad=info\"")
        .assert_stdout_contains("peerset_initial_target_size=59");
}

/// Tracing filters given before any subcommand apply to the implicit `start`.
#[test]
fn implicit_start_applies_top_level_filters() {
    let scratch = Scratch::new();
    let config_path = scratch.write_config(
        scratch.preference_dir().join("zakura.toml"),
        &scratch.rejected_state_config(),
    );

    let run = scratch.run_node(&mut scratch.command(["--filters", "info,zakurad=info"]));

    run.assert_status(1)
        .assert_stdout_contains(&format!("Using config file at: {config_path:?}"))
        .assert_stdout_contains("filter=\"info,zakurad=info\"")
        .assert_stdout_contains("Starting zakurad");
}

/// `--verbose` makes `start` log at debug level when no filter is configured.
#[test]
fn verbose_start_uses_debug_filter() {
    let scratch = Scratch::new();
    let config = scratch.write_config("zakura.toml", &scratch.rejected_state_config());

    let run = scratch.run_node(&mut scratch.command([
        OsStr::new("--verbose"),
        OsStr::new("--config"),
        config.as_os_str(),
        OsStr::new("start"),
    ]));

    run.assert_status(1)
        .assert_stdout_contains(&format!("Using config file at: {config:?}"))
        .assert_stdout_contains("filter=\"debug\"");
}

// Configuration errors

/// A config file that is not valid TOML stops startup with the loader error.
#[test]
fn malformed_config_file_is_rejected() {
    let scratch = Scratch::new();
    let config = scratch.write_file("malformed.toml", "[network\nnetwork = \"Regtest\"\n");

    let run = scratch.run(&mut scratch.command([
        OsStr::new("-c"),
        config.as_os_str(),
        OsStr::new("start"),
    ]));

    run.assert_status(1)
        .assert_stderr_contains("error: Zakura could not load the provided configuration file")
        .assert_stderr_contains(
            "error: zakura fatal error: I/O operation failed: Configuration error: \
             could not parse configuration file",
        )
        .assert_stdout_lacks("Starting zakurad");
}

/// An unknown config field stops startup, including for utility commands.
#[test]
fn unknown_config_field_is_rejected() {
    let scratch = Scratch::new();
    let config = scratch.write_file(
        "unknown.toml",
        "[network]\nnetwork = \"Regtest\"\nnot_a_real_field = 1\n",
    );

    for command in [&["start"][..], &["tip-height", "--network", "Testnet"][..]] {
        let mut args = vec![OsStr::new("-c"), config.as_os_str()];
        args.extend(command.iter().map(OsStr::new));
        let run = scratch.run(&mut scratch.command(args));

        run.assert_status(1)
            .assert_stderr_contains("error: Zakura could not load the provided configuration file")
            .assert_stderr_contains(
                "zakura fatal error: I/O operation failed: Configuration error:",
            )
            .assert_stderr_contains("not_a_real_field");
    }
}

/// A missing explicit config file stops startup.
#[test]
fn missing_explicit_config_file_is_rejected() {
    let scratch = Scratch::new();
    let missing = scratch.path().join("missing.toml");

    let run = scratch.run(&mut scratch.command([
        OsStr::new("-c"),
        missing.as_os_str(),
        OsStr::new("start"),
    ]));

    run.assert_status(1).assert_stderr_contains(
        "zakura fatal error: I/O operation failed: Configuration error: \
         could not read configuration file",
    );
}

/// `generate` ignores config errors, so it can produce a replacement config.
#[test]
fn generate_ignores_config_errors() {
    let scratch = Scratch::new();
    let config = scratch.write_file("malformed.toml", "[network\n");

    let run = scratch.run(&mut scratch.command([
        OsStr::new("-c"),
        config.as_os_str(),
        OsStr::new("generate"),
    ]));

    run.assert_status(0)
        .assert_stdout_contains("# Default configuration for zakurad.")
        .assert_stdout_contains("[network]");
    assert_eq!(run.stderr, "", "generate writes nothing to stderr");
}

/// Sensitive values can't be set through the environment, and the rejected value is
/// never printed.
#[test]
fn sensitive_environment_override_is_rejected_without_printing_it() {
    let scratch = Scratch::new();
    let config = scratch.write_config("zakura.toml", &scratch.regtest_config());

    for variable in ["ZAKURA_RPC__PASSWORD", "ZEBRA_RPC__PASSWORD"] {
        let mut command =
            scratch.command([OsStr::new("-c"), config.as_os_str(), OsStr::new("start")]);
        command.env(variable, SYNTHETIC_CREDENTIAL);
        let run = scratch.run(&mut command);

        run.assert_status(1)
            .assert_stderr_contains(
                "zakura fatal error: I/O operation failed: Configuration error:",
            )
            .assert_stderr_contains(variable)
            .assert_no_output_contains(SYNTHETIC_CREDENTIAL);
    }
}

/// A command-line flag that conflicts with the config stops startup before the node starts.
#[test]
fn conflicting_command_line_override_is_rejected() {
    let scratch = Scratch::new();
    let config = scratch.write_config("zakura.toml", &{
        let mut config = scratch.regtest_config();
        config.network.p2p_stack = zakura_network::P2pStack::Zakura;
        config
    });

    let run = scratch.run(&mut scratch.command([
        OsStr::new("-c"),
        config.as_os_str(),
        OsStr::new("start"),
        OsStr::new("--zcashd-compat"),
    ]));

    run.assert_status(1)
        .assert_stderr_contains(
            "error: zakura fatal error: I/O operation failed: \
             zcashd-compat P2P sidecar mode requires the legacy Zcash P2P stack",
        )
        .assert_stdout_lacks("Starting zakurad");
}

/// An invalid config combination stops startup before the node starts.
#[test]
fn invalid_config_combination_is_rejected() {
    let scratch = Scratch::new();
    let config = scratch.write_config("zakura.toml", &{
        let mut config = scratch.regtest_config();
        config.zcashd_compat.block_gossip_peer_ips = vec!["127.0.0.1".parse().expect("valid IP")];
        config
    });

    let run = scratch.run(&mut scratch.command([
        OsStr::new("-c"),
        config.as_os_str(),
        OsStr::new("start"),
    ]));

    run.assert_status(1).assert_stderr_contains(
        "error: zakura fatal error: I/O operation failed: \
         zcashd_compat.block_gossip_peer_ips requires zcashd_compat.enabled = true",
    );
}

/// A log file that can't be opened stops startup before the node starts.
#[test]
fn tracing_setup_failure_is_reported() {
    let scratch = Scratch::new();
    let config = scratch.write_config("zakura.toml", &{
        let mut config = scratch.regtest_config();
        // A directory can't be opened as a log file.
        config.tracing.log_file = Some(scratch.path().to_path_buf());
        config
    });

    let run = scratch.run(&mut scratch.command([
        OsStr::new("-c"),
        config.as_os_str(),
        OsStr::new("start"),
    ]));

    run.assert_status(1)
        .assert_stdout_contains("Sending logs to")
        .assert_stderr_contains("error: zakura fatal error: I/O operation failed:")
        .assert_stdout_lacks("Starting zakurad");
}

// Startup and shutdown

/// An error returned by the running node stops it with a failure status, after logging why.
#[test]
fn runtime_error_exits_with_failure() {
    let scratch = Scratch::new();
    let config = scratch.write_config("zakura.toml", &scratch.rejected_state_config());

    let run = scratch.run_node(&mut scratch.command([
        OsStr::new("-c"),
        config.as_os_str(),
        OsStr::new("start"),
    ]));

    run.assert_status(1)
        .assert_stdout_contains("Starting zakurad")
        .assert_stdout_contains("invalid state storage configuration")
        .assert_stdout_contains("shutting down Zakura due to an error")
        .assert_stdout_lacks(STOPPED);
}

/// An occupied P2P listener port stops the node with a panic.
#[test]
fn occupied_p2p_listener_exits_with_failure() {
    let scratch = Scratch::new();
    let (_listener, addr) = occupied_port();
    let config = scratch.write_config("zakura.toml", &{
        let mut config = scratch.regtest_config();
        config.network.listen_addr = addr;
        config
    });

    let run = scratch.run_node(&mut scratch.command([
        OsStr::new("-c"),
        config.as_os_str(),
        OsStr::new("start"),
    ]));

    run.assert_status(101)
        .assert_stdout_contains("Starting zakurad")
        .assert_stdout_lacks(STOPPED);

    let panic_header = run
        .stderr
        .lines()
        .find(|line| line.contains("panicked at"))
        .expect("the default panic hook prints a panic header");
    let assert_panic_detected = |patterns: &[&str]| {
        let failures = regex::RegexSet::new(patterns).expect("failure patterns are valid regexes");
        let ignores = regex::RegexSet::empty();
        assert!(
            check_failure_regexes(
                Ok(panic_header.to_owned()),
                &failures,
                &ignores,
                "zakurad",
                false,
            )
            .is_err(),
            "acceptance tests must detect the node's panic output: {}",
            run.stderr,
        );
        assert!(
            check_failure_regexes(
                Ok("Starting zakurad".to_owned()),
                &failures,
                &ignores,
                "zakurad",
                false,
            )
            .is_ok(),
            "normal startup output must not be treated as a panic",
        );
    };
    assert_panic_detected(failure_messages::ZAKURA_FAILURE_MESSAGES);
    #[cfg(feature = "zakura-checkpoints")]
    assert_panic_detected(failure_messages::ZAKURA_CHECKPOINTS_FAILURE_MESSAGES);
}

/// A panic during startup, after logging has started, reports the panic and still writes
/// the queued log lines.
#[cfg(feature = "prometheus")]
#[test]
fn occupied_metrics_listener_panics_after_partial_startup() {
    let scratch = Scratch::new();
    let (_listener, addr) = occupied_port();
    let config = scratch.write_config("zakura.toml", &{
        let mut config = scratch.regtest_config();
        config.metrics.endpoint_addr = Some(addr);
        config
    });

    let run = scratch.run_node(&mut scratch.command([
        OsStr::new("-c"),
        config.as_os_str(),
        OsStr::new("start"),
    ]));

    run.assert_status(101)
        .assert_stderr_contains("Opening metrics endpoint listener")
        .assert_stderr_contains("Diagnostic metadata:")
        .assert_stdout_contains("initialized rayon thread pool for CPU-bound tasks")
        .assert_stdout_contains("Trying to open metrics endpoint")
        .assert_stdout_lacks("Starting zakurad");
}

/// A shutdown signal that arrives while the node is still starting stops it cleanly.
#[cfg(unix)]
#[test]
fn shutdown_during_startup_exits_cleanly() {
    let scratch = Scratch::new();
    let config = scratch.write_config("zakura.toml", &scratch.regtest_config());

    let mut node = scratch.spawn(&mut scratch.command([
        OsStr::new("-c"),
        config.as_os_str(),
        OsStr::new("start"),
    ]));
    node.wait_for_stdout("initializing node state");
    node.signal("TERM");
    let run = node.wait();

    run.assert_stopped_cleanly()
        .assert_stdout_contains("received SIGTERM, starting shutdown");
}

/// Logs written to a file are flushed through the final shutdown line.
#[cfg(unix)]
#[test]
fn log_file_is_flushed_on_shutdown() {
    let scratch = Scratch::new();
    let log_file = scratch.path().join("logs").join("zakurad.log");
    let config = scratch.write_config("zakura.toml", &{
        let mut config = scratch.regtest_config();
        config.tracing.log_file = Some(log_file.clone());
        config
    });

    let node = scratch.spawn(&mut scratch.command([
        OsStr::new("-c"),
        config.as_os_str(),
        OsStr::new("start"),
    ]));
    // Logs go to the file, so wait for the startup milestone there.
    let deadline = Instant::now() + PROCESS_TIMEOUT;
    while !fs::read_to_string(&log_file).is_ok_and(|log| log.contains(STARTED)) {
        assert!(Instant::now() < deadline, "zakurad did not log {STARTED:?}");
        thread::sleep(Duration::from_millis(50));
    }
    node.signal("TERM");
    let run = node.wait();

    run.assert_status(0)
        .assert_stdout_contains("Sending logs to");
    let log = fs::read_to_string(&log_file).expect("log file is readable");
    let last_line = log.lines().last().unwrap_or_default();
    assert!(
        last_line.contains(STOPPED),
        "the last logged line is {last_line:?}, not {STOPPED:?}"
    );
}

/// A configured OpenTelemetry exporter with no collector doesn't stop the node from
/// shutting down cleanly, and its endpoint is not logged.
#[cfg(all(unix, feature = "opentelemetry"))]
#[test]
fn opentelemetry_exporter_shuts_down_with_the_node() {
    let scratch = Scratch::new();
    let (listener, addr) = occupied_port();
    // Leave nothing listening on the collector port.
    drop(listener);
    let endpoint = format!("http://user:{SYNTHETIC_CREDENTIAL}@{addr}");
    let config = scratch.write_config("zakura.toml", &{
        let mut config = scratch.regtest_config();
        config.tracing.opentelemetry_endpoint = Some(endpoint);
        config
    });

    let mut node = scratch.spawn(&mut scratch.command([
        OsStr::new("-c"),
        config.as_os_str(),
        OsStr::new("start"),
    ]));
    node.wait_for_stdout(STARTED);
    node.signal("TERM");
    let run = node.wait();

    run.assert_stopped_cleanly()
        .assert_stdout_contains("installed OpenTelemetry tracing layer")
        .assert_no_output_contains(SYNTHETIC_CREDENTIAL);
}

// Utility commands

/// Utility commands don't start the node or the metrics endpoint, even when verbose.
#[test]
fn utility_commands_do_not_start_server_components() {
    let scratch = Scratch::new();
    let (_listener, metrics_addr) = occupied_port();
    let config = scratch.write_config("zakura.toml", &{
        let mut config = scratch.regtest_config();
        // Starting the metrics endpoint on this occupied port would panic.
        config.metrics.endpoint_addr = Some(metrics_addr);
        config
    });
    let empty_state = scratch.path().join("empty-state");

    let run = scratch.run(&mut scratch.command([
        OsStr::new("-v"),
        OsStr::new("-c"),
        config.as_os_str(),
        OsStr::new("tip-height"),
        OsStr::new("--network"),
        OsStr::new("Testnet"),
        OsStr::new("--cache-dir"),
        empty_state.as_os_str(),
    ]));

    run.assert_status(0)
        .assert_stdout_lacks("Starting zakurad")
        .assert_stdout_lacks("Trying to open metrics endpoint")
        .assert_stdout_contains("started tracing component")
        .assert_stdout_lacks("loaded node configuration")
        .assert_stdout_lacks("Diagnostic metadata:");
    assert!(
        !run.stderr.contains("Thank you for running"),
        "utility commands don't print the node intro"
    );
}

/// `tip-height` is quiet unless `-v` or `--verbose` is given.
#[test]
fn utility_command_verbosity_flags() {
    let scratch = Scratch::new();
    let config = scratch.write_config("zakura.toml", &scratch.regtest_config());
    let empty_state = scratch.path().join("empty-state");

    for verbosity in [None, Some("-v"), Some("--verbose")] {
        let mut args: Vec<&OsStr> = verbosity.into_iter().map(OsStr::new).collect();
        args.extend([
            OsStr::new("-c"),
            config.as_os_str(),
            OsStr::new("tip-height"),
            OsStr::new("--network"),
            OsStr::new("Testnet"),
            OsStr::new("--cache-dir"),
            empty_state.as_os_str(),
        ]);
        let run = scratch.run(&mut scratch.command(args));

        run.assert_status(0)
            .assert_stdout_contains("Failed to read chain tip height from state");
        if verbosity.is_some() {
            run.assert_stdout_contains("started tracing component");
        } else {
            run.assert_stdout_lacks("started tracing component");
        }
    }
}

/// `generate --output-file` writes the same config `generate` prints, and that config
/// loads.
#[test]
fn generated_config_file_round_trips() {
    let scratch = Scratch::new();
    let generated = scratch.path().join("generated.toml");

    let printed = scratch.run(&mut scratch.command(["generate"]));
    printed.assert_status(0);
    assert_eq!(printed.stderr, "", "generate writes nothing to stderr");

    let written = scratch.run(&mut scratch.command([
        OsStr::new("generate"),
        OsStr::new("-o"),
        generated.as_os_str(),
    ]));
    written.assert_status(0);
    assert_eq!(
        written.stdout, "",
        "generate writes the config only to the file"
    );
    assert_eq!(written.stderr, "", "generate writes nothing to stderr");

    let file = fs::read_to_string(&generated).expect("generated config is readable");
    assert!(file.starts_with("# Default configuration for zakurad."));
    assert_eq!(printed.stdout, format!("{file}\n"));

    let empty_state = scratch.path().join("empty-state");
    let run = scratch.run(&mut scratch.command([
        OsStr::new("-c"),
        generated.as_os_str(),
        OsStr::new("tip-height"),
        OsStr::new("--network"),
        OsStr::new("Testnet"),
        OsStr::new("--cache-dir"),
        empty_state.as_os_str(),
    ]));
    run.assert_status(0);
    assert_eq!(run.stderr, "", "the generated config loads without errors");
}

/// The stored config fixtures load in the real binary.
#[test]
fn stored_config_fixtures_load() {
    let scratch = Scratch::new();
    let empty_state = scratch.path().join("empty-state");
    let configs = env::var_os("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| env!("CARGO_MANIFEST_DIR").into())
        .join("tests/common/configs");

    let mut fixtures: Vec<PathBuf> = fs::read_dir(&configs)
        .expect("config fixture directory is readable")
        .map(|entry| entry.expect("config fixture entry is readable").path())
        .filter(|path| path.extension() == Some(OsStr::new("toml")))
        .collect();
    fixtures.sort();
    assert!(!fixtures.is_empty(), "{configs:?} has config fixtures");

    for fixture in fixtures {
        let run = scratch.run(&mut scratch.command([
            OsStr::new("-c"),
            fixture.as_os_str(),
            OsStr::new("tip-height"),
            OsStr::new("--network"),
            OsStr::new("Testnet"),
            OsStr::new("--cache-dir"),
            empty_state.as_os_str(),
        ]));

        run.assert_status(0);
        assert_eq!(run.stderr, "", "{fixture:?} loads without errors");
    }
}

/// `generate` panics with its usual report when it can't create the output file.
#[test]
fn generate_reports_file_creation_failure() {
    let scratch = Scratch::new();
    let output = scratch.path().join("missing-dir").join("zakura.toml");

    let run = scratch.run(&mut scratch.command([
        OsStr::new("generate"),
        OsStr::new("-o"),
        output.as_os_str(),
    ]));

    run.assert_status(101)
        .assert_stderr_contains("must be able to open output file")
        .assert_stdout_lacks("# Default configuration for zakurad.");
    assert!(!output.exists(), "no config file is written");
}

/// A failing utility command logs why, and exits with a failure status after the log is
/// flushed.
#[test]
fn failing_utility_command_exits_with_failure() {
    let scratch = Scratch::new();
    let config = scratch.write_config("zakura.toml", &scratch.regtest_config());
    let missing = scratch.path().join("missing-subtrees.bin");

    let run = scratch.run(&mut scratch.command([
        OsStr::new("-c"),
        config.as_os_str(),
        OsStr::new("verify-historical-treestates"),
        OsStr::new("--network"),
        OsStr::new("Mainnet"),
        OsStr::new("--subtree-input"),
        missing.as_os_str(),
    ]));

    run.assert_status(1)
        .assert_stdout_contains("Subtree-root artifact verification failed")
        .assert_stdout_lacks("Starting zakurad");
}

/// Command-line parse errors are reported by the argument parser with its exit status.
#[test]
fn command_line_parse_errors_use_parser_exit_status() {
    let scratch = Scratch::new();

    let run = scratch.run(&mut scratch.command(["start", "--not-a-real-flag"]));

    run.assert_status(2)
        .assert_stderr_contains("unexpected argument '--not-a-real-flag'");
}

/// Help and version output exits successfully.
#[test]
fn help_and_version_are_printed() {
    let scratch = Scratch::new();

    let mut help_args = vec![vec!["--help"], vec!["help"]];
    for subcommand in [
        "audit-historical-treestates",
        "generate",
        "prune-state",
        "rollback-state",
        "start",
        "tip-height",
        "verify-historical-treestates",
    ] {
        help_args.push(vec![subcommand, "--help"]);
    }

    for args in help_args {
        let run = scratch.run(&mut scratch.command(args));
        run.assert_status(0).assert_stdout_contains("Usage:");
        assert_eq!(run.stderr, "", "help writes nothing to stderr");
    }

    for flag in ["--version", "-V"] {
        let run = scratch.run(&mut scratch.command([flag]));
        run.assert_status(0);
        assert!(
            run.stdout.starts_with("zakurad "),
            "unexpected version output {:?}",
            run.stdout
        );
        assert_eq!(run.stderr, "", "version writes nothing to stderr");
    }
}

/// Missing option values and required arguments are reported by the argument parser.
#[test]
fn missing_arguments_use_parser_exit_status() {
    let scratch = Scratch::new();

    for (args, expected) in [
        (&["-c"][..], "a value is required for '--config <CONFIG>'"),
        (&["rollback-state"][..], "--height <HEIGHT>"),
        (&["prune-state"][..], "--tx-retention <TX_RETENTION>"),
        (
            &["verify-historical-treestates"][..],
            "--subtree-input <SUBTREE_INPUT>",
        ),
    ] {
        let run = scratch.run(&mut scratch.command(args));

        run.assert_status(2).assert_stderr_contains(expected);
        assert_eq!(run.stdout, "", "parse errors write nothing to stdout");
    }
}
