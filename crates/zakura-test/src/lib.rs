//! Miscellaneous test code for the Zakura node.
#![doc(html_favicon_url = "https://zakura.com/assets/rustdoc/zakura-favicon-128.png")]
#![doc(html_logo_url = "https://zakura.com/assets/rustdoc/zakura-icon.png")]
#![doc(html_root_url = "https://docs.rs/zakura_test")]

use std::sync::{LazyLock, Once};

use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[allow(missing_docs)]
pub mod command;

pub mod allocations;
pub mod execution;
pub mod log_capture;
pub mod mock_service;
pub mod net;
pub mod network_addr;
pub mod prelude;
pub mod process_stats;
pub mod report;
pub mod service_extensions;
pub mod transcript;
pub mod vectors;
pub mod zip0143;
pub mod zip0243;
pub mod zip0244;

/// A single-threaded Tokio runtime that can be shared between tests.
/// This runtime should be used for tests that need a single thread for consistent timings.
///
/// This shared runtime should be used in tests that use shared background tasks. An example is
/// with shared global `LazyLock<BatchVerifier>` types, because they spawn a background task when they
/// are first initialized. This background task is stopped when the runtime is shut down, so having
/// a runtime per test means that only the first test actually manages to successfully use the
/// background task. Using the shared runtime allows the background task to keep running for the
/// other tests that also use it.
///
/// A shared runtime should not be used in tests that need to pause and resume the Tokio timer.
/// This is because multiple tests might be sharing the runtime at the same time, so there could be
/// conflicts with pausing and resuming the timer at incorrect points. Even if only one test runs
/// at a time, there's a risk of a test finishing while the timer is paused (due to a test failure,
/// for example) and that means that the next test will already start with an incorrect timer
/// state.
pub static SINGLE_THREADED_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
});

/// A multi-threaded Tokio runtime that can be shared between tests.
/// This runtime should be used for tests that spawn blocking threads.
///
/// See [`SINGLE_THREADED_RUNTIME`] for details.
pub static MULTI_THREADED_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
});

static INIT: Once = Once::new();

/// Initialize global and thread-specific settings for tests,
/// such as tracing configs, panic hooks, and `cargo insta` settings.
///
/// This function should be called at the start of every test.
///
/// It returns a drop guard that must be stored in a variable, so that it
/// gets dropped when the test finishes.
#[must_use]
pub fn init() -> impl Drop {
    // Per-test

    // Settings for threads that snapshots data using `insta`

    let mut settings = insta::Settings::clone_current();
    settings.set_prepend_module_to_snapshot(false);
    let drop_guard = settings.bind_to_scope();

    // Globals

    INIT.call_once_force(|_| {
        let fmt_layer = fmt::layer().with_target(false);
        // Use the RUST_LOG env var, or by default:
        //  - warn for most tests, and
        //  - for some modules, hide expected warn logs
        let filter_layer = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| {
                // These filters apply when RUST_LOG isn't set
                EnvFilter::try_new("warn")
                    .unwrap()
                    .add_directive("zakura_consensus=error".parse().unwrap())
                    .add_directive("zakura_network=error".parse().unwrap())
                    .add_directive("zakura_state=error".parse().unwrap())
                    .add_directive("zakurad=error".parse().unwrap())
                    .add_directive("tor_circmgr=error".parse().unwrap())
            })
            // These filters apply on top of RUST_LOG.
            // Avoid adding filters to this list, because users can't override them.
            //
            // (There are currently no always-on directives.)
            ;

        tracing_subscriber::registry()
            .with(filter_layer)
            .with(fmt_layer)
            .with(log_capture::LogCaptureLayer)
            .try_init()
            .ok();

        report::SectionHandler::install();
        install_test_panic_hook();
    });

    drop_guard
}

/// Initialize globals for tests that need a separate Tokio runtime instance.
///
/// This is generally used in proptests, which don't support the `#[tokio::test]` attribute.
///
/// If a runtime needs to be shared between tests, use the [`SINGLE_THREADED_RUNTIME`] or
/// [`MULTI_THREADED_RUNTIME`] instances instead.
///
/// See also the [`init`] function, which is called by this function.
pub fn init_async() -> (tokio::runtime::Runtime, impl Drop) {
    let drop_guard = init();

    (
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime"),
        drop_guard,
    )
}

/// Installs a panic hook that skips the panic output of tests that returned an `Err`.
///
/// The test harness has already printed the `Err` value, so the panic message
/// adds nothing. Other panics use the default hook.
fn install_test_panic_hook() {
    let default_hook = std::panic::take_hook();

    #[allow(clippy::print_stderr)]
    std::panic::set_hook(Box::new(move |info| {
        let payload = info
            .payload()
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| info.payload().downcast_ref::<&str>().copied())
            .unwrap_or_default();

        if payload.contains("the test returned a termination value with a non-zero status code") {
            eprintln!("---- end of test output ----");
        } else {
            default_hook(info);
        }
    }));
}
