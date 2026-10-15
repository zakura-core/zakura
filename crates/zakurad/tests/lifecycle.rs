//! Process-level tests for how `zakurad` selects its configuration, starts, reports
//! errors, and shuts down.
//!
//! Each test runs the real `zakurad` binary in a scratch directory with a cleared
//! environment, and checks its exit status and output streams.
//!
//! Set `ZAKURAD_LIFECYCLE_RECORD_DIR` to also write each run's exit status and
//! output to a JSON file in that directory, so two builds can be compared. Recorded
//! output has temporary paths, timestamps, versions, commits and ports replaced with
//! placeholders.

#![allow(clippy::unwrap_in_result)]

use std::{
    env,
    ffi::OsStr,
    fs,
    io::{BufRead, BufReader, Read},
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::{Child, ChildStdout, Command, ExitStatus, Stdio},
    sync::{mpsc, Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use regex::Regex;
use tempfile::TempDir;
use zakura_chain::parameters::Network;
use zakura_state::{PruningConfig, StorageMode};
use zakurad::config::ZakuradConfig;

/// How long a node may take to log a startup milestone or exit.
const PROCESS_TIMEOUT: Duration = Duration::from_secs(120);

/// The log line written once every startup task has been spawned.
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
        fs::create_dir_all(dir.path().join("home")).expect("home directory is created");
        fs::create_dir_all(dir.path().join("xdg")).expect("config directory is created");
        Self { dir }
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    /// The directory `zakurad` searches for an implicit config file.
    fn preference_dir(&self) -> PathBuf {
        self.path().join("xdg")
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
        let mut command = Command::new(env!("CARGO_BIN_EXE_zakurad"));
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

    /// Runs a command that is expected to exit on its own.
    fn run(&self, name: &str, command: &mut Command) -> Run {
        let node = Node::spawn(command);
        node.wait(name, self)
    }

    /// Starts a long-running command.
    fn spawn(&self, command: &mut Command) -> Node {
        Node::spawn(command)
    }
}

/// A running `zakurad` process whose output is collected in the background.
struct Node {
    child: Child,
    stdout_lines: mpsc::Receiver<String>,
    stdout: Arc<Mutex<Vec<String>>>,
    stdout_reader: thread::JoinHandle<()>,
    stderr_reader: thread::JoinHandle<String>,
}

impl Node {
    fn spawn(command: &mut Command) -> Self {
        let mut child = command.spawn().expect("zakurad starts");
        let stdout = child.stdout.take().expect("stdout is piped");
        let mut stderr = child.stderr.take().expect("stderr is piped");
        let (lines_tx, stdout_lines) = mpsc::channel();
        let collected = Arc::new(Mutex::new(Vec::new()));
        let stdout_reader = Self::read_lines(stdout, lines_tx, collected.clone());
        let stderr_reader = thread::spawn(move || {
            let mut output = Vec::new();
            let _ = stderr.read_to_end(&mut output);
            String::from_utf8_lossy(&output).into_owned()
        });

        Self {
            child,
            stdout_lines,
            stdout: collected,
            stdout_reader,
            stderr_reader,
        }
    }

    fn read_lines(
        stdout: ChildStdout,
        lines_tx: mpsc::Sender<String>,
        collected: Arc<Mutex<Vec<String>>>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                collected
                    .lock()
                    .expect("no reader panics while holding the lock")
                    .push(line.clone());
                let _ = lines_tx.send(line);
            }
        })
    }

    /// Waits until a stdout line contains `text`, or panics with the output so far.
    fn wait_for_stdout(&mut self, text: &str) {
        let deadline = Instant::now() + PROCESS_TIMEOUT;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.stdout_lines.recv_timeout(remaining) {
                Ok(line) if line.contains(text) => return,
                Ok(_) => {}
                Err(error) => {
                    let _ = self.child.kill();
                    let stdout = self.stdout.lock().expect("lock is not poisoned").join("\n");
                    panic!("zakurad did not log {text:?} ({error}); stdout so far:\n{stdout}");
                }
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

    /// Waits for the process to exit and returns its output.
    fn wait(mut self, name: &str, scratch: &Scratch) -> Run {
        let deadline = Instant::now() + PROCESS_TIMEOUT;
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("process status is readable") {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                panic!("zakurad did not exit within {PROCESS_TIMEOUT:?}");
            }
            thread::sleep(Duration::from_millis(50));
        };
        self.stdout_reader.join().expect("stdout reader finishes");
        let stderr = self.stderr_reader.join().expect("stderr reader finishes");
        let stdout = self.stdout.lock().expect("lock is not poisoned").join("\n");

        let run = Run {
            status,
            stdout,
            stderr,
        };
        run.record(name, scratch.path());
        run
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

    /// Writes this run to `ZAKURAD_LIFECYCLE_RECORD_DIR`, if it is set.
    fn record(&self, name: &str, scratch: &Path) {
        let Some(dir) = env::var_os("ZAKURAD_LIFECYCLE_RECORD_DIR") else {
            return;
        };
        let record = serde_json::json!({
            "name": name,
            "status": self.status.code(),
            "signal": signal_of(&self.status),
            "stdout": normalize(&self.stdout, scratch),
            "stderr": normalize(&self.stderr, scratch),
        });
        let path = PathBuf::from(dir).join(format!("{name}.json"));
        fs::write(
            path,
            serde_json::to_string_pretty(&record).expect("record serializes"),
        )
        .expect("record is written");
    }
}

#[cfg(unix)]
fn signal_of(status: &ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    status.signal()
}

#[cfg(not(unix))]
fn signal_of(_status: &ExitStatus) -> Option<i32> {
    None
}

/// Replaces values that differ between runs and builds with placeholders.
fn normalize(output: &str, scratch: &Path) -> Vec<String> {
    let replacements = [
        (r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z", "<time>"),
        (
            r"\b\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?\b",
            "<version>",
        ),
        (r"\b[0-9a-f]{7,40}\b", "<commit>"),
        (r"(127\.0\.0\.1|0\.0\.0\.0|\[::\]|\[::1\]):\d+", "$1:<port>"),
        (
            r"(commit timestamp|branch|rust release date): .*",
            "$1: <build>",
        ),
        (r"ThreadId\(\d+\)", "ThreadId(<id>)"),
        (r"\d+(\.\d+)?(ns|µs|ms|s)\b", "<duration>"),
    ]
    .map(|(pattern, replacement)| {
        (
            Regex::new(pattern).expect("normalization patterns are valid"),
            replacement,
        )
    });

    let scratch = scratch.to_string_lossy();
    output
        .lines()
        .map(|line| {
            let mut line = line.replace(scratch.as_ref(), "<scratch>");
            for (pattern, replacement) in &replacements {
                line = pattern.replace_all(&line, *replacement).into_owned();
            }
            line
        })
        .collect()
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
    let run = node.wait("implicit_start_sigterm", &scratch);

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
    let run = node.wait("legacy_config_sigint", &scratch);

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
    let run = node.wait("no_config_defaults", &scratch);

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
    let run = node.wait("explicit_config_overrides", &scratch);

    run.assert_stopped_cleanly()
        .assert_stdout_contains(&format!("Using config file at: {explicit:?}"))
        .assert_stdout_contains("peerset_initial_target_size=29")
        .assert_stdout_contains("filter=\"info,zakurad=debug\"");
}

// Configuration errors

/// A config file that is not valid TOML stops startup with the loader error.
#[test]
fn malformed_config_file_is_rejected() {
    let scratch = Scratch::new();
    let config = scratch.write_file("malformed.toml", "[network\nnetwork = \"Regtest\"\n");

    let run = scratch.run(
        "malformed_config",
        &mut scratch.command([OsStr::new("-c"), config.as_os_str(), OsStr::new("start")]),
    );

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

    for (name, command) in [
        ("unknown_field_start", &["start"][..]),
        (
            "unknown_field_tip_height",
            &["tip-height", "--network", "Testnet"][..],
        ),
    ] {
        let mut args = vec![OsStr::new("-c"), config.as_os_str()];
        args.extend(command.iter().map(OsStr::new));
        let run = scratch.run(name, &mut scratch.command(args));

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

    let run = scratch.run(
        "missing_config",
        &mut scratch.command([OsStr::new("-c"), missing.as_os_str(), OsStr::new("start")]),
    );

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

    let run = scratch.run(
        "generate_with_malformed_config",
        &mut scratch.command([OsStr::new("-c"), config.as_os_str(), OsStr::new("generate")]),
    );

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

    let mut command = scratch.command([OsStr::new("-c"), config.as_os_str(), OsStr::new("start")]);
    command.env("ZAKURA_RPC__PASSWORD", SYNTHETIC_CREDENTIAL);
    let run = scratch.run("sensitive_env_override", &mut command);

    run.assert_status(1)
        .assert_stderr_contains("zakura fatal error: I/O operation failed: Configuration error:")
        .assert_stderr_contains("ZAKURA_RPC__PASSWORD")
        .assert_no_output_contains(SYNTHETIC_CREDENTIAL);
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

    let run = scratch.run(
        "conflicting_cli_override",
        &mut scratch.command([
            OsStr::new("-c"),
            config.as_os_str(),
            OsStr::new("start"),
            OsStr::new("--zcashd-compat"),
        ]),
    );

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

    let run = scratch.run(
        "invalid_config_combination",
        &mut scratch.command([OsStr::new("-c"), config.as_os_str(), OsStr::new("start")]),
    );

    run.assert_status(1).assert_stderr_contains(
        "error: zakura fatal error: I/O operation failed: \
         zcashd_compat.block_gossip_peer_ips requires zcashd_compat.enabled = true",
    );
}

// Startup and shutdown

/// An error returned by the running node stops it with a failure status, after logging why.
#[test]
fn runtime_error_exits_with_failure() {
    let scratch = Scratch::new();
    let config = scratch.write_config("zakura.toml", &{
        let mut config = scratch.regtest_config();
        config.state.storage_mode = StorageMode::Pruned(PruningConfig { tx_retention: 1 });
        config
    });

    let run = scratch.run(
        "runtime_error",
        &mut scratch.command([OsStr::new("-c"), config.as_os_str(), OsStr::new("start")]),
    );

    run.assert_status(1)
        .assert_stdout_contains("Starting zakurad")
        .assert_stdout_contains("invalid state storage configuration")
        .assert_stdout_contains("shutting down Zakura due to an error")
        .assert_stdout_lacks(STOPPED);
}

/// An occupied P2P listener port stops the node with a failure status.
#[test]
fn occupied_p2p_listener_exits_with_failure() {
    let scratch = Scratch::new();
    let (_listener, addr) = occupied_port();
    let config = scratch.write_config("zakura.toml", &{
        let mut config = scratch.regtest_config();
        config.network.listen_addr = addr;
        config
    });

    let run = scratch.run(
        "occupied_p2p_listener",
        &mut scratch.command([OsStr::new("-c"), config.as_os_str(), OsStr::new("start")]),
    );

    assert!(!run.status.success(), "an occupied listener stops the node");
    run.assert_stdout_contains("Starting zakurad")
        .assert_stdout_lacks(STOPPED);
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

    let run = scratch.run(
        "occupied_metrics_listener",
        &mut scratch.command([OsStr::new("-c"), config.as_os_str(), OsStr::new("start")]),
    );

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
    let run = node.wait("shutdown_during_startup", &scratch);

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
    let run = node.wait("log_file_flush", &scratch);

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
    let run = node.wait("opentelemetry_shutdown", &scratch);

    run.assert_stopped_cleanly()
        .assert_stdout_contains("installed OpenTelemetry tracing layer")
        .assert_no_output_contains(SYNTHETIC_CREDENTIAL);
}

// Utility commands

/// Utility commands don't start the node, the async runtime, or the metrics endpoint,
/// and only log warnings by default.
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

    let run = scratch.run(
        "utility_tip_height",
        &mut scratch.command([
            OsStr::new("-c"),
            config.as_os_str(),
            OsStr::new("tip-height"),
            OsStr::new("--network"),
            OsStr::new("Testnet"),
            OsStr::new("--cache-dir"),
            empty_state.as_os_str(),
        ]),
    );

    run.assert_status(0)
        .assert_stdout_lacks("Starting zakurad")
        .assert_stdout_lacks("Trying to open metrics endpoint")
        .assert_stdout_lacks("loaded node configuration")
        .assert_stdout_lacks("initialized rayon thread pool");
    assert!(
        !run.stderr.contains("Thank you for running"),
        "utility commands don't print the node intro"
    );
}

/// A failing utility command exits with a failure status.
#[test]
fn failing_utility_command_exits_with_failure() {
    let scratch = Scratch::new();
    let config = scratch.write_config("zakura.toml", &scratch.regtest_config());
    let missing = scratch.path().join("missing-subtrees.bin");

    let run = scratch.run(
        "utility_failure",
        &mut scratch.command([
            OsStr::new("-c"),
            config.as_os_str(),
            OsStr::new("verify-historical-treestates"),
            OsStr::new("--network"),
            OsStr::new("Mainnet"),
            OsStr::new("--subtree-input"),
            missing.as_os_str(),
        ]),
    );

    run.assert_status(1).assert_stdout_lacks("Starting zakurad");
}

/// Command-line parse errors are reported by the argument parser with its exit status.
#[test]
fn command_line_parse_errors_use_parser_exit_status() {
    let scratch = Scratch::new();

    let run = scratch.run(
        "unknown_argument",
        &mut scratch.command(["start", "--not-a-real-flag"]),
    );

    run.assert_status(2)
        .assert_stderr_contains("unexpected argument '--not-a-real-flag'");
}
