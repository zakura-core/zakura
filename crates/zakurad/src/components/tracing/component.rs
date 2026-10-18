//! Zakura's `tracing` setup.

use std::{
    fs::{self, File},
    io::Write,
};

use eyre::{eyre, Report};
use tokio::sync::watch;
use tracing::{field::Visit, Level};
use tracing_error::ErrorLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};
use zakura_chain::parameters::Network;

use crate::{application::build_version, components::tracing::Config};

use super::non_blocking::{non_blocking, WorkerGuard};

// Art generated with these two images.
// Zakura logo: project branding
// License: MIT or Apache 2.0
//
// Heart image: https://commons.wikimedia.org/wiki/File:Love_Heart_SVG.svg
// Author: Bubinator
// License: Public Domain or Unconditional Use
//
// How to render
//
// Convert heart image to PNG (2000px):
// curl -o heart.svg https://upload.wikimedia.org/wikipedia/commons/4/42/Love_Heart_SVG.svg
// cargo install resvg
// resvg --width 2000 --height 2000 heart.svg heart.png
//
// Then to text (40x20):
// img2txt -W 40 -H 20 -f utf8 -d none heart.png > heart.utf8
// img2txt -W 40 -H 20 -f utf8 -d none favicon.png > logo.utf8
// paste -d "\0" logo.utf8 heart.utf8 > zakura.utf8
static ZAKURA_ART: [u8; include_bytes!("zakura.utf8").len()] = *include_bytes!("zakura.utf8");

/// Returns a tracing setup error, with the prefix `zakurad` has always printed for it.
fn io_error(error: std::io::Error) -> Report {
    eyre!("I/O operation failed: {error}")
}

/// A type-erased boxed writer that can be sent between threads safely.
pub type BoxWrite = Box<dyn Write + Send + Sync + 'static>;

/// The global `tracing` subscriber's log writer.
pub struct Tracing {
    /// Drop guard for worker thread of non-blocking logger,
    /// responsible for flushing any remaining logs when the program terminates.
    //
    // Correctness: must be listed last in the struct, so it drops after other drops have logged.
    _guard: Option<WorkerGuard>,
}

impl Tracing {
    /// Try to create a new [`Tracing`] component with the given `config`.
    ///
    /// If `uses_intro` is true, show a welcome message, the `network`,
    /// and the Zakura logo on startup. (If the terminal supports it.)
    //
    // This method should only print to stderr, because stdout is for tracing logs.
    #[allow(clippy::print_stdout, clippy::print_stderr, clippy::unwrap_in_result)]
    pub fn new(network: &Network, config: Config, uses_intro: bool) -> Result<Self, Report> {
        // Only use color if tracing output is being sent to a terminal or if it was explicitly
        // forced to.
        let use_color = config.use_color_stdout();
        let use_color_stderr = config.use_color_stderr();

        let filter = config.filter.clone().unwrap_or_default();

        // Only show the intro for user-focused node server commands like `start`
        // Also skip the intro for regtest, since it pollutes the QA test logs
        if uses_intro && !network.is_regtest() {
            // If it's a terminal and color escaping is enabled: clear screen and
            // print Zakura logo (here `use_color` is being interpreted as
            // "use escape codes")
            if use_color_stderr {
                // Clear screen
                eprint!("\x1B[2J");
                eprintln!(
                    "{}",
                    std::str::from_utf8(&ZAKURA_ART)
                        .expect("should always work on a UTF-8 encoded constant")
                );
            }

            eprintln!(
                "Thank you for running a {} zakurad {} node!",
                network.lowercase_name(),
                build_version()
            );
            eprintln!(
                "You're helping to strengthen the network and contributing to a social good :)"
            );
        }

        let writer = if let Some(log_file) = config.log_file.as_ref() {
            // Make sure the directory for the log file exists.
            // If the log is configured in the current directory, it won't have a parent directory.
            //
            // # Security
            //
            // If the user is running Zebra with elevated permissions ("root"), they should
            // create the log file directory before running Zebra, and make sure the Zebra user
            // account has exclusive access to that directory, and other users can't modify
            // its parent directories.
            //
            // This avoids a TOCTOU security issue in the Rust filesystem API.
            let log_file_dir = log_file.parent();
            if let Some(log_file_dir) = log_file_dir {
                if !log_file_dir.exists() {
                    eprintln!("Directory for log file {log_file:?} does not exist, trying to create it...");

                    if let Err(create_dir_error) = fs::create_dir_all(log_file_dir) {
                        eprintln!("Failed to create directory for log file: {create_dir_error}");
                        eprintln!("Trying log file anyway...");
                    }
                }
            }

            if uses_intro {
                // We want this to appear on stdout instead of the usual log messages.
                println!("Sending logs to {log_file:?}...");
            }
            let log_file = File::options()
                .append(true)
                .create(true)
                .open(log_file)
                .map_err(io_error)?;
            Box::new(log_file) as BoxWrite
        } else {
            let stdout = std::io::stdout();
            Box::new(stdout) as BoxWrite
        };

        // Queue logs for the background writer, dropping new logs when full.
        let (non_blocking, worker_guard) =
            non_blocking(writer, config.buffer_limit.max(100)).map_err(io_error)?;

        // Construct a format subscriber with the supplied global logging filter.
        let subscriber = {
            use tracing_subscriber::FmtSubscriber;

            let logger = FmtSubscriber::builder()
                .with_ansi(use_color)
                .with_writer(non_blocking)
                .with_env_filter(&filter);

            let warn_error_layer = LastWarnErrorLayer {
                last_warn_error_sender: crate::application::LAST_WARN_ERROR_LOG_SENDER.clone(),
            };
            logger
                .finish()
                .with(warn_error_layer)
                .with(ErrorLayer::default())
        };

        // Add optional layers based on dynamic and compile-time configs

        #[cfg(feature = "journald")]
        let journaldlayer = if config.use_journald {
            let layer = tracing_journald::layer().map_err(|e| eyre!("component error: {e}"))?;

            Some(layer)
        } else {
            None
        };
        #[cfg(feature = "journald")]
        let subscriber = subscriber.with(journaldlayer);

        // Initialise the global tracing subscriber
        subscriber.init();

        // Log the tracing stack we just created
        tracing::info!(
            ?filter,
            TRACING_STATIC_MAX_LEVEL = ?tracing::level_filters::STATIC_MAX_LEVEL,
            LOG_STATIC_MAX_LEVEL = ?log::STATIC_MAX_LEVEL,
            "started tracing component",
        );

        if config.use_journald {
            if cfg!(feature = "journald") {
                info!("installed journald tracing layer");
            } else {
                warn!(
                    "unable to activate configured journald tracing: \
                     enable the 'journald' feature when compiling zakurad",
                );
            }
        }

        Ok(Self {
            _guard: Some(worker_guard),
        })
    }

    /// Drops guard for worker thread of non-blocking logger,
    /// to flush any remaining logs when the program terminates.
    ///
    /// Calling this more than once, or dropping `Tracing` afterwards, does nothing more.
    pub fn shutdown(&mut self) {
        self._guard.take();
    }
}

impl Drop for Tracing {
    /// Shuts down tracing, then flushes the standard output streams.
    fn drop(&mut self) {
        self.shutdown();

        // Some OSes require a flush to send all output to the terminal.
        let _ = std::io::stdout().lock().flush();
        let _ = std::io::stderr().lock().flush();
    }
}

impl std::fmt::Debug for Tracing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tracing").finish()
    }
}

// Visitor to extract only the "message" field from a log event.
struct MessageVisitor {
    message: Option<String>,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{value:?}"));
        }
    }
}

// Layer to store the last WARN or ERROR log event.
#[derive(Debug, Clone)]
struct LastWarnErrorLayer {
    last_warn_error_sender: watch::Sender<Option<(String, Level, chrono::DateTime<chrono::Utc>)>>,
}

impl<S> Layer<S> for LastWarnErrorLayer
where
    S: tracing::Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let level = *event.metadata().level();
        let timestamp = chrono::Utc::now();

        if level == Level::WARN || level == Level::ERROR {
            let mut visitor = MessageVisitor { message: None };
            event.record(&mut visitor);

            if let Some(message) = visitor.message {
                let _ = self
                    .last_warn_error_sender
                    .send(Some((message, level, timestamp)));
            }
        }
    }
}
