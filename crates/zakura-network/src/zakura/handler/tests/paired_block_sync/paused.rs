//! A negotiated sibling service whose bounded consumer deliberately pauses.

use super::*;

const FRAME_BYTES: u32 = 16 * 1024;
const FIRST_KIND: u16 = 64;

#[derive(Debug)]
pub(super) struct PausedService {
    streams: Vec<Stream>,
    sessions: mpsc::Sender<PausedSession>,
}

impl PausedService {
    pub(super) fn new(count: u16) -> (Arc<Self>, mpsc::Receiver<PausedSession>) {
        assert!(count <= 2);
        let (sessions, receiver) = mpsc::channel(2);
        let streams = (0..count)
            .map(|index| Stream {
                kind: FIRST_KIND + index,
                version: 1,
                frame_cap: FRAME_BYTES,
                capability: 1 << 17,
                mode: StreamMode::Persistent,
            })
            .collect();
        (Arc::new(Self { streams, sessions }), receiver)
    }
}

impl Service for PausedService {
    fn name(&self) -> &'static str {
        "paused-test-service"
    }

    fn streams(&self) -> &[Stream] {
        &self.streams
    }

    fn stream_queue_depths(&self, _: Stream) -> Option<(usize, usize)> {
        Some((1, 1))
    }

    fn stream_write_policy(&self, _: Stream) -> StreamWritePolicy {
        // Two paused streams must retain connection credit past block sync's
        // 32-second deadline. A sibling timeout would release it prematurely.
        StreamWritePolicy::Timeout(if self.streams.len() == 2 {
            LOSS_DEADLINE
        } else {
            Duration::from_secs(10)
        })
    }

    fn add_peer(&self, mut peer: Peer) {
        let cancel = peer.service_cancel_token();
        for stream in &self.streams {
            if let Some((recv, send)) = peer.take_stream(stream.kind) {
                if self
                    .sessions
                    .try_send(PausedSession {
                        recv,
                        send,
                        cancel: cancel.clone(),
                    })
                    .is_err()
                {
                    cancel.cancel();
                }
            }
        }
    }

    fn remove_peer(&self, _: &ZakuraPeerId, _: ZakuraConnId) {}
}

#[derive(Debug)]
pub(super) struct PausedSession {
    recv: FramedRecv,
    send: FramedSend,
    cancel: CancellationToken,
}

impl PausedSession {
    pub(super) fn assert_active(&self) {
        assert!(
            !self.cancel.is_cancelled(),
            "the paused sibling must retain its receive window"
        );
    }

    pub(super) async fn receive(receiver: &mut mpsc::Receiver<Self>) -> Result<Self, BoxError> {
        let session = timeout(DEADLINE, receiver.recv())
            .await?
            .ok_or("sibling service closed")?;
        Ok(session)
    }

    pub(super) async fn fill_window(&self) -> Result<(), BoxError> {
        self.fill_window_bytes(DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW)
            .await
    }

    pub(super) async fn fill_window_bytes(&self, window_bytes: u32) -> Result<(), BoxError> {
        let payload = vec![42; usize::try_from(FRAME_BYTES)? - FRAME_HEADER_BYTES];
        timeout(DEADLINE, async {
            for _ in 0..window_bytes / FRAME_BYTES {
                self.send
                    .send(Frame {
                        message_type: 1,
                        flags: 0,
                        payload: payload.clone(),
                    })
                    .await?;
            }
            Ok::<_, BoxError>(())
        })
        .await??;
        Ok(())
    }

    pub(super) fn resume(mut self) -> AbortOnDropHandle<()> {
        AbortOnDropHandle::new(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = self.cancel.cancelled() => break,
                    frame = self.recv.recv() => {
                        let Some(frame) = frame else { break; };
                        assert!(frame.payload.iter().all(|byte| *byte == 42));
                    }
                }
            }
        }))
    }
}

impl Drop for PausedSession {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
