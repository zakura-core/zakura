//! A bounded, lossy background writer for tracing output.

use std::{
    io::{self, Write},
    thread,
    time::Duration,
};

use crossbeam_channel::{bounded, Receiver, Sender};
use tracing_subscriber::fmt::MakeWriter;

#[cfg(test)]
mod tests;

/// Preserve the previous logger's bounded shutdown waits.
const SHUTDOWN_ENQUEUE_TIMEOUT: Duration = Duration::from_millis(100);
const SHUTDOWN_FLUSH_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug)]
enum Message {
    Line(Vec<u8>),
    Shutdown,
}

/// Cloned tracing writers enqueue bytes without waiting for output I/O.
#[derive(Clone, Debug)]
pub(super) struct NonBlocking {
    sender: Sender<Message>,
}

/// Keeps the worker alive and requests a flush when tracing shuts down.
#[must_use]
#[derive(Debug)]
pub(super) struct WorkerGuard {
    sender: Sender<Message>,
    finished: Receiver<()>,
}

/// Starts the background writer with a bounded queue of write buffers.
pub(super) fn non_blocking<W: Write + Send + 'static>(
    writer: W,
    buffer_limit: usize,
) -> io::Result<(NonBlocking, WorkerGuard)> {
    let (sender, receiver) = bounded(buffer_limit);
    let (finished_sender, finished) = bounded(1);
    thread::Builder::new()
        .name("zakura-log".to_owned())
        .spawn(move || {
            run_worker(writer, receiver);
            let _ = finished_sender.try_send(());
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
        if self
            .sender
            .send_timeout(Message::Shutdown, SHUTDOWN_ENQUEUE_TIMEOUT)
            .is_ok()
        {
            // A stuck output must not hang shutdown. The worker can finish
            // later even if this guard has already timed out.
            let _ = self.finished.recv_timeout(SHUTDOWN_FLUSH_TIMEOUT);
        }
    }
}

fn run_worker<W: Write>(mut writer: W, receiver: Receiver<Message>) {
    for message in &receiver {
        match message {
            Message::Line(bytes) => {
                // Output failures must not stop queue draining or shutdown.
                let _ = writer.write_all(&bytes);
            }
            Message::Shutdown => break,
        }
        if receiver.is_empty() {
            let _ = writer.flush();
        }
    }
    let _ = writer.flush();
}
