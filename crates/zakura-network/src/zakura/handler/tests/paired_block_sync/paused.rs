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
        assert!(
            count <= 3,
            "two paused streams and one independent progress probe"
        );
        let (sessions, receiver) = mpsc::channel(3);
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
    pub(super) async fn send_probe(&self) -> Result<(), BoxError> {
        self.send
            .send(Frame {
                message_type: 1,
                flags: 0,
                payload: vec![42],
            })
            .await?;
        Ok(())
    }

    pub(super) async fn receive_probe(&mut self) -> Result<(), BoxError> {
        let frame = timeout(Duration::from_secs(3), self.recv.recv())
            .await?
            .ok_or("independent service closed before receiving its probe")?;
        assert_eq!(frame.payload, [42]);
        Ok(())
    }

    pub(super) async fn receive(receiver: &mut mpsc::Receiver<Self>) -> Result<Self, BoxError> {
        let session = timeout(DEADLINE, receiver.recv())
            .await?
            .ok_or("sibling service closed")?;
        Ok(session)
    }

    pub(super) async fn fill_window(&self) -> Result<(), BoxError> {
        let payload = vec![42; usize::try_from(FRAME_BYTES)? - FRAME_HEADER_BYTES];
        timeout(DEADLINE, async {
            for _ in 0..DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW / FRAME_BYTES {
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
