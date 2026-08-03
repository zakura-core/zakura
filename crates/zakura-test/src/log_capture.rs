//! Capturing log messages from the process-wide tracing subscriber in tests.
//!
//! `tracing_test::traced_test` and test-scoped subscribers can't be used in test binaries
//! that call [`crate::init`]:
//! - a second process-wide subscriber races with the one set by [`crate::init`], and
//! - a thread-scoped subscriber creates a second span registry, which corrupts span
//!   bookkeeping when tasks migrate between threads with different default subscribers.
//!
//! Instead, [`crate::init`] installs a `LogCaptureLayer` in the single process-wide
//! subscriber, and tests read the messages it captures through a [`LogCapture`] handle.

use std::sync::{Arc, Mutex, Weak};

use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, Layer};

/// The message buffers of the [`LogCapture`]s that are currently alive.
///
/// This is process-wide state, because it is read by the [`LogCaptureLayer`] in the
/// process-wide subscriber set by [`crate::init`].
static ACTIVE_CAPTURES: Mutex<Vec<Weak<Mutex<Vec<String>>>>> = Mutex::new(Vec::new());

/// A handle to log messages captured from the process-wide tracing subscriber.
///
/// Capturing starts when the handle is created, and stops when it is dropped.
///
/// Captured messages come from every thread and task in the process, including other
/// tests running concurrently in the same test binary. So tests should only assert on
/// messages that no other test in the binary can produce.
///
/// Messages are only captured if they pass the subscriber's filter, which is `RUST_LOG`
/// or the [`crate::init`] defaults. Warnings and errors always pass the default filter.
pub struct LogCapture {
    messages: Arc<Mutex<Vec<String>>>,
}

impl LogCapture {
    /// Starts capturing log messages, and returns a handle for reading them.
    pub fn new() -> Self {
        let messages = Arc::new(Mutex::new(Vec::new()));

        let mut captures = ACTIVE_CAPTURES
            .lock()
            .expect("no code panics while holding the capture list lock");
        // Remove buffers whose `LogCapture` handles have been dropped.
        captures.retain(|capture| capture.strong_count() > 0);
        captures.push(Arc::downgrade(&messages));

        Self { messages }
    }

    /// Returns true if any captured log message contains `needle`.
    pub fn contains(&self, needle: &str) -> bool {
        self.messages
            .lock()
            .expect("no code panics while holding the message buffer lock")
            .iter()
            .any(|message| message.contains(needle))
    }
}

impl Default for LogCapture {
    fn default() -> Self {
        Self::new()
    }
}

/// A tracing layer that copies log messages into the active [`LogCapture`] buffers.
///
/// Does nothing unless a [`LogCapture`] is alive, so it is always installed by
/// [`crate::init`].
pub(crate) struct LogCaptureLayer;

impl<S: tracing::Subscriber> Layer<S> for LogCaptureLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let captures = ACTIVE_CAPTURES
            .lock()
            .expect("no code panics while holding the capture list lock");
        if captures.is_empty() {
            return;
        }

        let mut message = String::new();
        event.record(&mut MessageVisitor(&mut message));

        for capture in captures.iter().filter_map(Weak::upgrade) {
            capture
                .lock()
                .expect("no code panics while holding the message buffer lock")
                .push(message.clone());
        }
    }
}

/// A field visitor that extracts an event's `message` field.
struct MessageVisitor<'a>(&'a mut String);

impl Visit for MessageVisitor<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            use std::fmt::Write as _;

            let _ = write!(self.0, "{value:?}");
        }
    }
}
