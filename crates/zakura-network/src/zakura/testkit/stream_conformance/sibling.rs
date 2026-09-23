//! Sibling services that share a connection with the layout.
//!
//! A sibling echoes probes, so a test can show the connection still carries
//! other services. It can also stop reading: the peer's frames then fill the
//! sibling's stream window and hold connection credit, as a stalled service
//! would.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, PoisonError,
    },
};

use tokio::sync::watch;
use tokio_util::task::AbortOnDropHandle;

use super::CONFORMANCE_DEADLINE;
use crate::{
    zakura::{
        Frame, FramedSend, Peer, Service, SessionOpening, SessionPolicy, Stream, StreamQueueDepths,
        ZakuraConnId, ZakuraPeerId,
    },
    BoxError,
};

/// Probe, reply, and fill message types.
pub(crate) const PROBE: u16 = 1;
pub(crate) const REPLY: u16 = 2;
const FILL: u16 = 3;

/// Bytes of one fill frame.
pub(crate) const FILL_BYTES: usize = 1024 * 1024;

const SIBLING: Stream = Stream {
    kind: 950,
    version: 1,
    // A frame header and one fill frame.
    frame_cap: 2 * 1024 * 1024,
    capability: 1 << 49,
    queue_depths: Some(StreamQueueDepths {
        inbound: 1,
        outbound: 1,
    }),
    ..Stream::PERSISTENT
};

/// Two one-stream siblings, each with its own capability.
pub(crate) const SIBLINGS: [Stream; 2] = [
    SIBLING,
    Stream {
        kind: 951,
        capability: 1 << 50,
        ..SIBLING
    },
];

#[derive(Debug, Default)]
struct Shared {
    sends: Mutex<HashMap<ZakuraPeerId, FramedSend>>,
    /// Probe nonces answered so far.
    replies: watch::Sender<HashSet<u64>>,
    /// Sessions admitted, per peer.
    admitted: watch::Sender<HashMap<ZakuraPeerId, u64>>,
    next_nonce: AtomicU64,
    /// Fill bytes queued for writing.
    filled: Arc<AtomicU64>,
    /// While true, this sibling stops reading.
    paused: watch::Sender<bool>,
}

/// A one-stream service that echoes probes.
#[derive(Debug)]
pub(crate) struct SiblingService {
    stream: [Stream; 1],
    shared: Arc<Shared>,
}

impl SiblingService {
    pub(crate) fn new(stream: Stream) -> Self {
        Self {
            stream: [stream],
            shared: Arc::default(),
        }
    }

    fn send(&self, peer: &ZakuraPeerId) -> Result<FramedSend, BoxError> {
        self.shared
            .sends
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(peer)
            .cloned()
            .ok_or_else(|| "the sibling has no session with the peer".into())
    }

    /// Wait until this sibling has a session with `peer`.
    pub(crate) async fn wait_session(&self, peer: &ZakuraPeerId) -> Result<(), BoxError> {
        let mut admitted = self.shared.admitted.subscribe();
        tokio::time::timeout(
            CONFORMANCE_DEADLINE,
            admitted.wait_for(|admitted| admitted.contains_key(peer)),
        )
        .await
        .map_err(|_| "timed out waiting for the sibling session")??;
        Ok(())
    }

    /// Send a probe to `peer`'s sibling and wait for its echo.
    pub(crate) async fn probe(&self, peer: &ZakuraPeerId) -> Result<(), BoxError> {
        let nonce = self.shared.next_nonce.fetch_add(1, Ordering::Relaxed);
        let mut replies = self.shared.replies.subscribe();
        self.send(peer)?
            .send(Frame {
                message_type: PROBE,
                flags: 0,
                payload: nonce.to_le_bytes().to_vec(),
            })
            .await
            .map_err(|_| "the sibling stream closed")?;
        tokio::time::timeout(
            CONFORMANCE_DEADLINE,
            replies.wait_for(|replies| replies.contains(&nonce)),
        )
        .await
        .map_err(|_| "timed out waiting for the sibling probe's echo")??;
        Ok(())
    }

    /// Send fill frames to `peer` until the returned task drops.
    pub(crate) fn flood(&self, peer: &ZakuraPeerId) -> Result<AbortOnDropHandle<()>, BoxError> {
        let send = self.send(peer)?;
        let filled = self.shared.filled.clone();
        Ok(AbortOnDropHandle::new(tokio::spawn(async move {
            loop {
                let fill = Frame {
                    message_type: FILL,
                    flags: 0,
                    payload: vec![0; FILL_BYTES],
                };
                if send.send(fill).await.is_err() {
                    return;
                }
                // Widening usize to u64 is lossless on supported targets.
                filled.fetch_add(FILL_BYTES as u64, Ordering::Relaxed);
            }
        })))
    }

    /// Fill bytes this sibling queued for writing.
    pub(crate) fn filled(&self) -> u64 {
        self.shared.filled.load(Ordering::Relaxed)
    }

    /// Stop or resume reading.
    pub(crate) fn pause(&self, paused: bool) {
        self.shared.paused.send_replace(paused);
    }
}

impl Service for SiblingService {
    fn name(&self) -> &'static str {
        "stream_conformance_sibling"
    }

    fn streams(&self) -> &[Stream] {
        &self.stream
    }

    fn session_policy(&self) -> SessionPolicy {
        SessionPolicy {
            opening: SessionOpening::InitiatorOnly,
            reopen: false,
        }
    }

    fn add_peer(&self, mut peer: Peer) {
        let Some((mut recv, send)) = peer.take_stream(self.stream[0].kind) else {
            peer.service_cancel_token().cancel();
            return;
        };
        let cancel = peer.service_cancel_token();
        let shared = self.shared.clone();
        shared
            .sends
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(peer.id.clone(), send.clone());
        shared.admitted.send_modify(|admitted| {
            *admitted.entry(peer.id.clone()).or_default() += 1;
        });
        tokio::spawn(async move {
            let mut paused = shared.paused.subscribe();
            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return,
                    _ = paused.wait_for(|paused| !*paused) => {}
                }
                let frame = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return,
                    frame = recv.recv() => frame,
                };
                let Some(frame) = frame else { return };
                match frame.message_type {
                    PROBE => {
                        let reply = Frame {
                            message_type: REPLY,
                            ..frame
                        };
                        if send.send(reply).await.is_err() {
                            return;
                        }
                    }
                    REPLY => {
                        if let Ok(nonce) = <[u8; 8]>::try_from(frame.payload) {
                            shared.replies.send_modify(|replies| {
                                replies.insert(u64::from_le_bytes(nonce));
                            });
                        }
                    }
                    _ => {}
                }
            }
        });
    }

    fn remove_peer(&self, peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {
        self.shared
            .sends
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(peer);
    }
}
