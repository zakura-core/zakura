//! Transport-owned framed stream handles.
//!
//! `FramedRecv` and `FramedSend` are the service-facing handles for application
//! stream frames. The transport applies each stream's declared `Stream::frame_cap`,
//! per-kind message-rate buckets, and idle freshness updates in its stream workers
//! before frames reach these handles.

use tokio::sync::mpsc;
#[cfg(test)]
use tokio::sync::oneshot;
#[cfg(test)]
use tokio::time::{timeout, Duration};

use super::Frame;

#[cfg(test)]
const TEST_BARRIER_TIMEOUT: Duration = Duration::from_secs(1);

/// Receive half for bounded, rate-admitted Zakura frames.
#[derive(Debug)]
pub struct FramedRecv {
    receiver: mpsc::Receiver<Frame>,
    #[cfg(test)]
    barrier_receiver: Option<mpsc::UnboundedReceiver<oneshot::Sender<()>>>,
}

impl FramedRecv {
    /// Wrap a bounded frame receiver.
    pub fn new(receiver: mpsc::Receiver<Frame>) -> Self {
        Self {
            receiver,
            #[cfg(test)]
            barrier_receiver: None,
        }
    }

    /// Receive the next admitted frame, or `None` after the transport closes the stream.
    pub async fn recv(&mut self) -> Option<Frame> {
        #[cfg(test)]
        if let Some(barriers) = self.barrier_receiver.as_mut() {
            loop {
                tokio::select! {
                    biased;
                    frame = self.receiver.recv() => return frame,
                    barrier = barriers.recv() => {
                        let barrier = barrier?;
                        let _ = barrier.send(());
                    }
                }
            }
        }

        self.receiver.recv().await
    }
}

/// Send half for bounded Zakura frames.
#[derive(Clone, Debug)]
pub struct FramedSend {
    sender: mpsc::Sender<Frame>,
    #[cfg(test)]
    barrier_sender: Option<mpsc::UnboundedSender<oneshot::Sender<()>>>,
}

impl FramedSend {
    /// Wrap a bounded frame sender.
    pub fn new(sender: mpsc::Sender<Frame>) -> Self {
        Self {
            sender,
            #[cfg(test)]
            barrier_sender: None,
        }
    }

    /// Queue a frame for transport-owned encoding and writing.
    pub async fn send(&self, frame: Frame) -> Result<(), mpsc::error::SendError<Frame>> {
        self.sender.send(frame).await
    }

    /// Try to queue a frame without waiting for capacity.
    pub fn try_send(&self, frame: Frame) -> Result<(), mpsc::error::TrySendError<Frame>> {
        self.sender.try_send(frame)
    }

    /// Current free slots in the bounded transport queue.
    pub fn capacity(&self) -> usize {
        self.sender.capacity()
    }

    /// Total slots in the bounded transport queue.
    pub fn max_capacity(&self) -> usize {
        self.sender.max_capacity()
    }

    /// Wait until the receiving task has consumed every frame queued before
    /// this call and returned to its receive loop.
    #[cfg(test)]
    pub(crate) async fn barrier_for_test(&self) -> Result<(), &'static str> {
        let barriers = self
            .barrier_sender
            .as_ref()
            .ok_or("this framed sender has no test barrier")?;
        let (acknowledge, acknowledged) = oneshot::channel();
        barriers
            .send(acknowledge)
            .map_err(|_| "the framed receiver closed before the test barrier")?;
        timeout(TEST_BARRIER_TIMEOUT, acknowledged)
            .await
            .map_err(|_| "the framed receiver timed out before the test barrier")?
            .map_err(|_| "the framed receiver dropped the test barrier")
    }
}

/// Build a bounded in-memory framed channel for scaffolding and tests.
pub fn framed_channel(depth: usize) -> (FramedSend, FramedRecv) {
    let (sender, receiver) = mpsc::channel(depth);
    #[cfg(test)]
    {
        let (barrier_sender, barrier_receiver) = mpsc::unbounded_channel();
        (
            FramedSend {
                sender,
                barrier_sender: Some(barrier_sender),
            },
            FramedRecv {
                receiver,
                barrier_receiver: Some(barrier_receiver),
            },
        )
    }

    #[cfg(not(test))]
    {
        (FramedSend::new(sender), FramedRecv::new(receiver))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn framed_barrier_times_out_when_receiver_stops_progressing() {
        let (sender, _receiver) = framed_channel(1);

        assert_eq!(
            sender.barrier_for_test().await,
            Err("the framed receiver timed out before the test barrier")
        );
    }
}
