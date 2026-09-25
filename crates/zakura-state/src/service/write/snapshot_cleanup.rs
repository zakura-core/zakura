//! Dispose of retired reader snapshots without delaying the state writer when idle.

use std::{
    sync::mpsc::{sync_channel, SyncSender, TrySendError},
    thread::{self, JoinHandle},
    time::Instant,
};

use tokio::sync::watch;

/// Owns one cleanup thread with no backlog. A busy worker leaves disposal to the caller.
pub(super) struct SnapshotCleanup<T> {
    sender: Option<SyncSender<T>>,
    worker: Option<JoinHandle<()>>,
}

impl<T: Send + 'static> SnapshotCleanup<T> {
    pub(super) fn new() -> Self {
        // A rendezvous accepts a snapshot only while the worker is waiting for it.
        // Even during catch-up, we retain at most one extra snapshot, not a queue.
        let (sender, receiver) = sync_channel(0);
        let worker = thread::Builder::new()
            .name("state-cleanup".into())
            .spawn(move || {
                while let Ok(snapshot) = receiver.recv() {
                    let start = Instant::now();
                    drop(snapshot);
                    metrics::histogram!("state.snapshot_cleanup.background.duration_seconds")
                        .record(start.elapsed().as_secs_f64());
                }
            });

        match worker {
            Ok(worker) => Self {
                sender: Some(sender),
                worker: Some(worker),
            },
            Err(error) => {
                tracing::warn!(%error, "snapshot cleanup thread unavailable; disposing inline");
                Self {
                    sender: None,
                    worker: None,
                }
            }
        }
    }

    /// Publishes before retiring the old view, preserving `watch::Sender::send` semantics.
    pub(super) fn publish(&self, sender: &watch::Sender<T>, snapshot: T) {
        // Like `send`, do not replace the stored value if there are no receivers.
        // Receivers may still close after this check, which `send` also permits.
        if sender.is_closed() {
            return;
        }

        // Readers keep their own shared references. Releasing this owner cannot
        // invalidate an old snapshot that a reader is still using.
        self.retire(sender.send_replace(snapshot));
    }

    fn retire(&self, snapshot: T) {
        let (snapshot, reason) = match &self.sender {
            Some(sender) => match sender.try_send(snapshot) {
                Ok(()) => {
                    metrics::counter!("state.snapshot_cleanup.offloaded").increment(1);
                    return;
                }
                Err(TrySendError::Full(snapshot)) => (snapshot, "busy"),
                Err(TrySendError::Disconnected(snapshot)) => (snapshot, "disconnected"),
            },
            None => (snapshot, "unavailable"),
        };

        // Never wait for cleanup capacity or spawn an unbounded blocking task.
        let start = Instant::now();
        drop(snapshot);
        metrics::counter!("state.snapshot_cleanup.inline", "reason" => reason).increment(1);
        metrics::histogram!("state.snapshot_cleanup.inline.duration_seconds")
            .record(start.elapsed().as_secs_f64());
    }
}

impl<T> Drop for SnapshotCleanup<T> {
    fn drop(&mut self) {
        // Close before joining, including on early writer exits. The worker only
        // drops owned memory and never waits for the writer or its async runtime.
        drop(self.sender.take());
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                tracing::warn!("snapshot cleanup thread panicked");
            }
        }
    }
}

#[cfg(test)]
mod tests;
