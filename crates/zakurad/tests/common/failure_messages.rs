//! Failure messages logged by test child processes.
//!
//! # Warning
//!
//! Test functions in this file will not be run.
//! This file is only for test library code.

/// Failure log messages for any process, from the OS or shell.
///
/// These messages show that the child process has failed.
/// So when we see them in the logs, we make the test fail.
pub const PROCESS_FAILURE_MESSAGES: &[&str] = &[
    // Linux
    "Aborted",
    // macOS / BSDs
    "Abort trap",
    // TODO: add other OS or C library errors?
];

/// Failure log messages from Zakura.
///
/// These `zakurad` messages show that an acceptance test has failed.
/// So when we see them in the logs, we make the test fail.
pub const ZAKURA_FAILURE_MESSAGES: &[&str] = &[
    // Rust-specific panics
    "The application panicked",
    // RPC port errors
    "Address already in use",
    // TODO: disable if this actually happens during test zakurad shutdown
    "Stopping RPC endpoint",
    // Missing RPCs in zakurad logs (this log is from PR #3860)
    //
    // TODO: temporarily disable until enough RPCs are implemented, if needed
    "Received unrecognized RPC request",
    // RPC argument errors: parsing and data
    //
    // These logs are produced by jsonrpc_core inside Zebra,
    // but it doesn't log them yet.
    //
    // TODO: log these errors in Zebra, and check for them in the Zebra logs?
    "Invalid params",
    "Method not found",
    // Logs related to end of support halting feature.
    zakurad::components::sync::end_of_support::EOS_PANIC_MESSAGE_HEADER,
];

/// Failure log messages from `zakura-checkpoints`.
///
/// These `zakura-checkpoints` messages show that checkpoint generation has failed.
/// So when we see them in the logs, we make the test fail.
#[cfg(feature = "zakura-checkpoints")]
pub const ZAKURA_CHECKPOINTS_FAILURE_MESSAGES: &[&str] = &[
    // Rust-specific panics
    "The application panicked",
    // RPC port errors
    "Address already in use",
    // RPC argument errors: parsing and data
    //
    // These logs are produced by jsonrpc_core inside Zebra,
    // but it doesn't log them yet.
    //
    // TODO: log these errors in Zebra, and check for them in the Zebra logs?
    "Invalid params",
    "Method not found",
    // Incorrect command-line arguments
    "USAGE",
    "Invalid value",
];
