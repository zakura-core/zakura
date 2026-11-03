//! A bounded, lossy background writer for tracing output.

use std::{
    io::{self, Write},
    sync::{
        mpsc::{sync_channel, Receiver, SyncSender, TrySendError},
        Arc, Condvar, Mutex, PoisonError,
    },
    thread,
    time::{Duration, Instant},
};

use tracing_subscriber::fmt::MakeWriter;

#[cfg(test)]
mod tests;

/// Preserve the previous logger's bounded shutdown waits.
const SHUTDOWN_ENQUEUE_TIMEOUT: Duration = Duration::from_millis(100);
const SHUTDOWN_FLUSH_TIMEOUT: Duration = Duration::from_secs(1);
/// How often shutdown retries enqueueing into a full queue.
const SHUTDOWN_ENQUEUE_RETRY_INTERVAL: Duration = Duration::from_millis(1);

#[derive(Debug)]
enum Message {
    Line(Vec<u8>),
    Shutdown,
}

/// Cloned tracing writers enqueue bytes without waiting for output I/O.
#[derive(Clone, Debug)]
pub(super) struct NonBlocking {
    sender: SyncSender<Message>,
}

/// Keeps the worker alive and requests a flush when tracing shuts down.
#[must_use]
#[derive(Debug)]
pub(super) struct WorkerGuard {
    sender: SyncSender<Message>,
    finished: Finished,
}

/// Set by the worker thread after it has flushed and dropped its output.
#[derive(Clone, Debug, Default)]
struct Finished(Arc<(Mutex<bool>, Condvar)>);

impl Finished {
    fn set(&self) {
        let (finished, changed) = &*self.0;
        // The flag is a plain `bool`, so a panic while it was locked can't leave it invalid.
        *finished.lock().unwrap_or_else(PoisonError::into_inner) = true;
        changed.notify_all();
    }

    /// Waits up to `timeout` for the worker to finish, returning whether it did.
    fn wait_timeout(&self, timeout: Duration) -> bool {
        let (finished, changed) = &*self.0;
        let finished = finished.lock().unwrap_or_else(PoisonError::into_inner);
        let (finished, _) = changed
            .wait_timeout_while(finished, timeout, |finished| !*finished)
            .unwrap_or_else(PoisonError::into_inner);
        *finished
    }
}

/// Starts the background writer with a bounded queue of write buffers.
pub(super) fn non_blocking<W: Write + Send + 'static>(
    writer: W,
    buffer_limit: usize,
) -> io::Result<(NonBlocking, WorkerGuard)> {
    let (sender, receiver) = sync_channel(buffer_limit);
    let finished = Finished::default();
    let worker_finished = finished.clone();
    thread::Builder::new()
        .name("zakura-log".to_owned())
        .spawn(move || {
            run_worker(writer, receiver);
            worker_finished.set();
        })?;

    Ok((
        NonBlocking {
            sender: sender.clone(),
        },
        WorkerGuard { sender, finished },
    ))
}

impl Write for NonBlocking {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        // A full or disconnected queue drops logs, matching the previous lossy
        // writer. Reporting success keeps output I/O off the caller's path.
        let _ = self.sender.try_send(Message::Line(bytes.to_vec()));
        Ok(bytes.len())
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.write(bytes).map(|_| ())
    }

    fn flush(&mut self) -> io::Result<()> {
        // The worker flushes each batch. Only the guard waits for shutdown.
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for NonBlocking {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        if send_with_timeout(&self.sender, Message::Shutdown, SHUTDOWN_ENQUEUE_TIMEOUT) {
            // A stuck output must not hang shutdown. The worker can finish
            // later even if this guard has already timed out.
            self.finished.wait_timeout(SHUTDOWN_FLUSH_TIMEOUT);
        }
    }
}

/// Sends `message`, waiting up to `timeout` for room in a full queue.
///
/// Returns whether the message was sent. The standard library's
/// `SyncSender::send_timeout` is not stable yet.
fn send_with_timeout<T>(sender: &SyncSender<T>, mut message: T, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        match sender.try_send(message) {
            Ok(()) => return true,
            Err(TrySendError::Disconnected(_)) => return false,
            Err(TrySendError::Full(unsent)) => {
                if Instant::now() >= deadline {
                    return false;
                }
                message = unsent;
                thread::sleep(SHUTDOWN_ENQUEUE_RETRY_INTERVAL);
            }
        }
    }
}

fn run_worker<W: Write>(mut writer: W, receiver: Receiver<Message>) {
    'batches: while let Ok(mut message) = receiver.recv() {
        // Write everything already queued, then flush the batch once.
        loop {
            match message {
                Message::Line(bytes) => {
                    // Output failures must not stop queue draining or shutdown.
                    let _ = writer.write_all(&bytes);
                }
                Message::Shutdown => break 'batches,
            }
            match receiver.try_recv() {
                Ok(next) => message = next,
                Err(_) => break,
            }
        }
        let _ = writer.flush();
    }
    let _ = writer.flush();
}
