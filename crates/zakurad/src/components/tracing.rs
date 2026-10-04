//! Tracing and logging infrastructure for Zebra.

use std::{
    io::IsTerminal,
    ops::{Deref, DerefMut},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};

mod component;

#[cfg(feature = "opentelemetry")]
mod otel;

pub use component::Tracing;

/// Tracing configuration section: outer config after cross-field defaults are applied.
///
/// This is a wrapper type that dereferences to the inner config type.
///
//
// TODO: replace with serde's finalizer attribute when that feature is implemented.
//       we currently use the recommended workaround of a wrapper struct with from/into attributes.
//       https://github.com/serde-rs/serde/issues/642#issuecomment-525432907
#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(
    deny_unknown_fields,
    default,
    from = "InnerConfig",
    into = "InnerConfig"
)]
pub struct Config {
    inner: InnerConfig,
}

impl Deref for Config {
    type Target = InnerConfig;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Config {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl From<InnerConfig> for Config {
    fn from(inner: InnerConfig) -> Self {
        Self { inner }
    }
}

impl From<Config> for InnerConfig {
    fn from(config: Config) -> Self {
        config.inner
    }
}

/// Tracing configuration section: inner config used to deserialize and apply cross-field defaults.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct InnerConfig {
    /// Whether to use colored terminal output, if available.
    ///
    /// Colored terminal output is automatically disabled if an output stream
    /// is connected to a file. (Or another non-terminal device.)
    ///
    /// Defaults to `true`, which automatically enables colored output to
    /// terminals.
    pub use_color: bool,

    /// Whether to force the use of colored terminal output, even if it's not available.
    ///
    /// Will force Zebra to use colored terminal output even if it does not detect that the output
    /// is a terminal that supports colors.
    ///
    /// Defaults to `false`, which keeps the behavior of `use_color`.
    pub force_use_color: bool,

    /// The filter used for tracing events.
    ///
    /// The filter is used to create a `tracing-subscriber`
    /// [`EnvFilter`](https://docs.rs/tracing-subscriber/0.2.10/tracing_subscriber/filter/struct.EnvFilter.html#directives),
    /// and more details on the syntax can be found there or in the examples
    /// below.
    ///
    /// If no filter is specified (`None`), the filter is set to `info` if the
    /// `-v` flag is given and `warn` if it is not given.
    ///
    /// # Examples
    ///
    /// `warn,zakurad=info,zakura_network=debug` sets a global `warn` level, an
    /// `info` level for the `zakurad` crate, and a `debug` level for the
    /// `zakura_network` crate.
    ///
    /// ```ascii,no_run
    /// [block_verify{height=Some\(block::Height\(.*000\)\)}]=trace
    /// ```
    /// sets `trace` level for all events occurring in the context of a
    /// `block_verify` span whose `height` field ends in `000`, i.e., traces the
    /// verification of every 1000th block.
    pub filter: Option<String>,

    /// The buffer_limit size sets the number of log lines that can be queued by the tracing subscriber
    /// to be written to stdout before logs are dropped.
    ///
    /// Defaults to 128,000 with a minimum of 100.
    pub buffer_limit: usize,

    /// Legacy flamegraph output path, accepted for configuration compatibility.
    ///
    /// Ignored because the built-in collector has been removed.
    #[serde(skip_serializing)]
    pub flamegraph: Option<PathBuf>,

    /// Legacy progress display setting, accepted for configuration compatibility.
    ///
    /// Ignored because the terminal progress display has been removed.
    #[serde(skip_serializing)]
    pub progress_bar: Option<ProgressConfig>,

    /// If set to a path, write the tracing logs to that path.
    ///
    /// By default, logs are sent to the terminal standard output.
    /// - Windows: `%LOCALAPPDATA%\zakura.log` or `C:\Users\%USERNAME%\AppData\Local\zakura.log`
    ///
    /// # Security
    ///
    /// If you are running Zebra with elevated permissions ("root"), create the
    /// directory for this file before running Zebra, and make sure the Zebra user
    /// account has exclusive access to that directory, and other users can't modify
    /// its parent directories.
    pub log_file: Option<PathBuf>,

    /// The use_journald flag sends tracing events to systemd-journald, on Linux
    /// distributions that use systemd.
    ///
    /// Install Zebra using `cargo install --features=journald` to enable this config.
    pub use_journald: bool,

    /// OpenTelemetry OTLP endpoint URL for distributed tracing.
    ///
    /// Install Zebra using `cargo install --features=opentelemetry` to enable this config.
    ///
    /// When `None` (default), OpenTelemetry is completely disabled with zero runtime overhead.
    /// When set, traces are exported via OTLP HTTP protocol.
    /// The URL must use HTTP or HTTPS. Its path is extended with `/v1/traces`
    /// unless it already ends with that suffix, ignoring trailing slashes.
    /// Query parameters are preserved, and fragments are discarded.
    ///
    /// Example: `"http://localhost:4318"`
    ///
    /// Can also be set via `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable (lower precedence).
    pub opentelemetry_endpoint: Option<String>,

    /// Service name reported to OpenTelemetry collector.
    ///
    /// Defaults to `"zakura"` if not specified.
    ///
    /// Can also be set via `OTEL_SERVICE_NAME` environment variable.
    pub opentelemetry_service_name: Option<String>,

    /// Trace sampling percentage between 0 and 100.
    ///
    /// Controls what percentage of traces are exported:
    /// - `100` = 100% (all traces, default)
    /// - `10` = 10% (recommended for high-traffic production)
    /// - `0` = 0% (effectively disabled)
    ///
    /// Lower values reduce network/collector overhead for busy nodes.
    ///
    /// Note: This differs from the standard `OTEL_TRACES_SAMPLER_ARG` which uses
    /// a ratio (0.0-1.0). Zebra uses percentage (0-100) for consistency with
    /// other integer-based configuration options.
    pub opentelemetry_sample_percent: Option<u8>,
}

/// Legacy progress display modes accepted when reading existing configuration.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ProgressConfig {
    /// Legacy detailed display mode, ignored.
    Detailed,

    /// Legacy summary display mode, ignored.
    #[default]
    #[serde(other)]
    Summary,
}

impl Config {
    /// Returns `true` if standard output should use color escapes.
    /// Automatically checks if Zebra is running in a terminal.
    pub fn use_color_stdout(&self) -> bool {
        self.force_use_color || (self.use_color && std::io::stdout().is_terminal())
    }

    /// Returns `true` if standard error should use color escapes.
    /// Automatically checks if Zebra is running in a terminal.
    pub fn use_color_stderr(&self) -> bool {
        self.force_use_color || (self.use_color && std::io::stderr().is_terminal())
    }

    /// Returns `true` if output that could go to standard output or standard error
    /// should use color escapes. Automatically checks if Zebra is running in a terminal.
    pub fn use_color_stdout_and_stderr(&self) -> bool {
        self.force_use_color
            || (self.use_color
                && std::io::stdout().is_terminal()
                && std::io::stderr().is_terminal())
    }
}

impl Default for InnerConfig {
    fn default() -> Self {
        Self {
            use_color: true,
            force_use_color: false,
            filter: None,
            buffer_limit: 128_000,
            flamegraph: None,
            progress_bar: None,
            log_file: None,
            use_journald: false,
            opentelemetry_endpoint: None,
            opentelemetry_service_name: None,
            opentelemetry_sample_percent: None,
        }
    }
}
