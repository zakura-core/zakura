use std::sync::{Arc, Mutex};

use super::*;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Default)]
struct Recorded {
    bytes: Vec<u8>,
    flushes: usize,
    dropped: bool,
    thread: Option<thread::ThreadId>,
}

#[derive(Default)]
struct RecordingWriter {
    recorded: Arc<Mutex<Recorded>>,
    fail_first_write: bool,
    fail_flush: bool,
    flushed: Option<Sender<()>>,
}

// Test coordination failures must panic: the logger discards output errors.
#[allow(clippy::unwrap_in_result)]
impl Write for RecordingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if std::mem::take(&mut self.fail_first_write) {
            return Err(io::Error::other("injected write failure"));
        }
        let mut recorded = self
            .recorded
            .lock()
            .expect("the recording writer only locks state in non-panicking code");
        recorded.bytes.extend_from_slice(bytes);
        recorded.thread = Some(thread::current().id());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.recorded
            .lock()
            .expect("the recording writer only locks state in non-panicking code")
            .flushes += 1;
        if let Some(flushed) = &self.flushed {
            let _ = flushed.try_send(());
        }
        if self.fail_flush {
            Err(io::Error::other("injected flush failure"))
        } else {
            Ok(())
        }
    }
}

impl Drop for RecordingWriter {
    fn drop(&mut self) {
        self.recorded.lock().unwrap().dropped = true;
    }
}

struct BlockedWriter {
    inner: RecordingWriter,
    entered: Sender<()>,
    release: Receiver<()>,
}

#[allow(clippy::unwrap_in_result)]
impl Write for BlockedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.entered
            .try_send(())
            .expect("each test makes at most two writes into a two-slot notification queue");
        self.release
            .recv_timeout(TEST_TIMEOUT)
            .expect("the test releases each write before its watchdog timeout");
        self.inner.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn blocked_writer() -> (
    BlockedWriter,
    Receiver<()>,
    Sender<()>,
    Arc<Mutex<Recorded>>,
) {
    let inner = RecordingWriter::default();
    let recorded = inner.recorded.clone();
    let (entered, entered_receiver) = bounded(2);
    let (release_sender, release) = bounded(2);
    (
        BlockedWriter {
            inner,
            entered,
            release,
        },
        entered_receiver,
        release_sender,
        recorded,
    )
}

#[test]
fn queued_logs_are_written_in_order_and_flushed_on_shutdown() {
    let output = RecordingWriter::default();
    let recorded = output.recorded.clone();
    let (mut writer, guard) = non_blocking(output, 10).unwrap();
    writer.write_all(b"first\n").unwrap();
    writer.make_writer().write_all(b"second\n").unwrap();
    writer.flush().unwrap();
    drop(guard);

    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.bytes, b"first\nsecond\n");
    assert!(recorded.flushes > 0);
    assert!(recorded.dropped);
    assert_ne!(recorded.thread, Some(thread::current().id()));
}

#[test]
fn an_idle_worker_flushes_before_shutdown() {
    let (flushed, completion) = bounded(1);
    let output = RecordingWriter {
        recorded: Default::default(),
        fail_first_write: false,
        fail_flush: false,
        flushed: Some(flushed),
    };
    let recorded = output.recorded.clone();
    let (mut writer, _guard) = non_blocking(output, 1).unwrap();
    writer.write_all(b"idle batch").unwrap();
    completion.recv_timeout(TEST_TIMEOUT).unwrap();
    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.bytes, b"idle batch");
    assert!(!recorded.dropped);
}

#[test]
fn a_full_queue_drops_new_logs_without_blocking_callers() {
    let (output, entered, release, recorded) = blocked_writer();
    let (mut writer, guard) = non_blocking(output, 1).unwrap();
    writer.write_all(b"first").unwrap();
    entered.recv_timeout(TEST_TIMEOUT).unwrap();
    writer.write_all(b"second").unwrap();

    let (returned, completion) = bounded(1);
    let caller = thread::spawn(move || {
        let result = writer.write_all(b"dropped");
        returned.send(result).unwrap();
    });
    completion.recv_timeout(TEST_TIMEOUT).unwrap().unwrap();
    caller.join().unwrap();
    release.send(()).unwrap();
    release.send(()).unwrap();
    drop(guard);
    assert_eq!(recorded.lock().unwrap().bytes, b"firstsecond");
}

#[test]
fn writes_after_worker_shutdown_are_lossy_and_successful() {
    let output = RecordingWriter::default();
    let recorded = output.recorded.clone();
    let (mut writer, guard) = non_blocking(output, 1).unwrap();
    drop(guard);
    assert_eq!(writer.write(b"late").unwrap(), 4);
    writer.write_all(b"also late").unwrap();
    writer.flush().unwrap();
    assert!(recorded.lock().unwrap().bytes.is_empty());
}

#[test]
fn output_errors_do_not_prevent_later_writes_or_shutdown() {
    let output = RecordingWriter {
        recorded: Default::default(),
        fail_first_write: true,
        fail_flush: true,
        flushed: None,
    };
    let recorded = output.recorded.clone();
    let (mut writer, guard) = non_blocking(output, 10).unwrap();
    writer.write_all(b"failed").unwrap();
    writer.write_all(b"kept").unwrap();
    drop(guard);

    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.bytes, b"kept");
    assert!(recorded.flushes > 0);
    assert!(recorded.dropped);
}

#[test]
fn concurrent_tracing_events_reach_the_background_writer() {
    let output = RecordingWriter::default();
    let recorded = output.recorded.clone();
    let (writer, guard) = non_blocking(output, 100).unwrap();
    let (finished, completion) = bounded(8);
    let callers: Vec<_> = (0..8)
        .map(|id| {
            let writer = writer.clone();
            let finished = finished.clone();
            thread::spawn(move || {
                let subscriber = tracing_subscriber::fmt()
                    .with_ansi(false)
                    .without_time()
                    .with_writer(writer)
                    .finish();
                tracing::subscriber::with_default(subscriber, || {
                    tracing::info!(id, "background event");
                });
                finished.send(()).unwrap();
            })
        })
        .collect();
    for _ in &callers {
        completion.recv_timeout(TEST_TIMEOUT).unwrap();
    }
    for caller in callers {
        caller.join().unwrap();
    }
    drop(guard);

    let recorded = recorded.lock().unwrap();
    let text = std::str::from_utf8(&recorded.bytes).unwrap();
    assert_eq!(text.lines().count(), 8);
    for id in 0..8 {
        assert!(text.contains(&format!("id={id}")));
    }
}

#[test]
fn blocked_output_does_not_hold_up_shutdown_indefinitely() {
    // Exercise both a full queue (shutdown cannot be enqueued) and a queue
    // with room (shutdown is enqueued but the output cannot finish yet).
    for full_queue in [true, false] {
        let (output, entered, release, recorded) = blocked_writer();
        let (mut writer, guard) = non_blocking(output, 1).unwrap();
        writer.write_all(b"blocked").unwrap();
        entered.recv_timeout(TEST_TIMEOUT).unwrap();
        if full_queue {
            writer.write_all(b"queued").unwrap();
        }
        // Keep a completion receiver after the guard is dropped so the test
        // can wait for the detached worker after unblocking its output.
        let finished = guard.finished.clone();
        let (returned, completion) = bounded(1);
        let shutdown = thread::spawn(move || {
            drop(guard);
            returned.send(()).unwrap();
        });
        completion.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!recorded.lock().unwrap().dropped);
        shutdown.join().unwrap();

        drop(writer);
        release.send(()).unwrap();
        if full_queue {
            release.send(()).unwrap();
        }
        finished.recv_timeout(TEST_TIMEOUT).unwrap();
        let recorded = recorded.lock().unwrap();
        assert!(recorded.dropped);
        assert_eq!(
            recorded.bytes,
            if full_queue {
                b"blockedqueued".as_slice()
            } else {
                b"blocked".as_slice()
            }
        );
    }
}
