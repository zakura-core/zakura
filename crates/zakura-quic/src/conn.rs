//! Authenticated connections (API-4), their statistics (OBS-4, OBS-12) and
//! the per-connection sample (OBS-3).

use std::{
    fmt,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use bytes::Bytes;
use quinn_proto::{ConnectionError, ConnectionStats, StreamId, VarInt};
use tokio::sync::oneshot;

use crate::{
    driver::{ConnCmd, ConnRef},
    key::NodeId,
    stream::{RecvStream, SendStream},
};

/// How often the connection task samples stats and re-checks the remote
/// address (PATH-3, OBS-3).
pub(crate) const SAMPLE_INTERVAL: Duration = Duration::from_secs(10);

/// Decides whether a migrated address must close the connection (PATH-3).
pub type BanCheck = Arc<dyn Fn(IpAddr) -> bool + Send + Sync>;

/// One per-connection sample, for the `quic_conn` trace table (OBS-3).
#[derive(Clone, Debug)]
pub struct ConnSample {
    /// The TLS-proven remote node ID.
    pub remote_id: NodeId,
    /// The admitted IP (PATH-4).
    pub admitted_ip: IpAddr,
    /// Smoothed RTT.
    pub rtt: Duration,
    /// Congestion window in bytes.
    pub cwnd: u64,
    /// Packets lost.
    pub lost_packets: u64,
    /// Congestion events.
    pub congestion_events: u64,
    /// UDP payload bytes sent.
    pub bytes_sent: u64,
    /// UDP payload bytes received.
    pub bytes_received: u64,
    /// Current path MTU.
    pub current_mtu: u16,
    /// Bytes in flight after the last ACK batch (OBS-12).
    pub bytes_in_flight: u64,
    /// Time streams waited on flow control or the send window (OBS-12).
    pub send_blocked: Duration,
    /// The close reason, set only on the final sample.
    pub close_reason: Option<String>,
}

/// Receives per-connection samples every 10 s and on close (OBS-3).
pub type ConnObserver = Arc<dyn Fn(&ConnSample) + Send + Sync>;

/// Connection statistics (OBS-4, OBS-12).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ConnStats {
    /// quinn-proto's counters: UDP, frames (including `DATA_BLOCKED` and
    /// `STREAM_DATA_BLOCKED`) and the path (RTT, congestion window, losses,
    /// MTU and black holes).
    pub connection: ConnectionStats,
    /// What the instrumented congestion controller saw.
    pub congestion: CongestionStats,
    /// Counters the driver keeps.
    pub driver: DriverStats,
}

/// Congestion controller state (OBS-12).
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct CongestionStats {
    /// Congestion window in bytes.
    pub window: u64,
    /// Slow-start threshold in bytes, if the controller has one.
    pub ssthresh: Option<u64>,
    /// Pacing rate in bits per second, if the controller sets one.
    pub pacing_rate_bps: Option<u64>,
    /// Bytes in flight after the last ACK batch.
    pub bytes_in_flight: u64,
    /// Whether the connection was application-limited before the last ACKs.
    pub app_limited: bool,
    /// Bytes acknowledged.
    pub acked_bytes: u64,
}

/// Counters the driver keeps beside quinn-proto's (OBS-12).
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct DriverStats {
    /// Bytes Zakura handed over that quinn-proto hasn't accepted yet.
    pub queued_send_bytes: u64,
    /// Time writes waited on stream or connection flow control or the send
    /// window, summed over streams.
    pub send_blocked: Duration,
    /// Send system calls.
    pub transmits: u64,
    /// Datagrams sent; above `transmits` when GSO batches them.
    pub datagrams_sent: u64,
    /// Datagrams the endpoint routed to this connection.
    pub datagrams_received: u64,
}

/// The endpoint's observer and ban check, read by connection tasks.
#[derive(Default)]
pub(crate) struct Hooks {
    observer: Mutex<Option<ConnObserver>>,
    ban: Mutex<Option<BanCheck>>,
}

impl Hooks {
    pub(crate) fn observer(&self) -> Option<ConnObserver> {
        self.observer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn set_observer(&self, observer: ConnObserver) {
        *self.observer.lock().unwrap_or_else(PoisonError::into_inner) = Some(observer);
    }

    pub(crate) fn set_ban(&self, ban: BanCheck) {
        *self.ban.lock().unwrap_or_else(PoisonError::into_inner) = Some(ban);
    }

    pub(crate) fn is_banned(&self, ip: IpAddr) -> bool {
        let ban = self
            .ban
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        ban.is_some_and(|banned| banned(ip))
    }
}

/// An authenticated QUIC connection.
///
/// Cloning shares the connection. When the last clone and the last stream
/// drop, the connection closes with application code 0 (API-4).
#[derive(Clone)]
pub struct Conn {
    inner: ConnRef,
    remote_id: NodeId,
    admitted_addr: SocketAddr,
    alpn: Arc<[u8]>,
}

impl fmt::Debug for Conn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Conn")
            .field("remote_id", &self.remote_id)
            .field("admitted_addr", &self.admitted_addr)
            .field("alpn", &String::from_utf8_lossy(&self.alpn))
            .finish()
    }
}

impl Conn {
    pub(crate) fn new(
        inner: ConnRef,
        remote_id: NodeId,
        admitted_addr: SocketAddr,
        alpn: Vec<u8>,
    ) -> Self {
        Self {
            inner,
            remote_id,
            admitted_addr,
            alpn: alpn.into(),
        }
    }

    /// The peer's node ID, proven by the TLS handshake (TLS-8).
    pub fn remote_id(&self) -> NodeId {
        self.remote_id
    }

    /// The IP admitted at accept or dial time. It never changes (PATH-4).
    pub fn admitted_ip(&self) -> IpAddr {
        self.admitted_addr.ip()
    }

    /// The socket address admitted at accept or dial time.
    pub fn admitted_addr(&self) -> SocketAddr {
        self.admitted_addr
    }

    /// The negotiated ALPN.
    pub fn alpn(&self) -> &[u8] {
        &self.alpn
    }

    /// Opens a bidirectional stream.
    ///
    /// Waits while the peer's stream limit is reached. The peer learns of the
    /// stream when the first byte arrives.
    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        let id = self.request(|reply| ConnCmd::OpenBi { reply }).await?;
        Ok(self.streams(id))
    }

    /// Accepts the peer's next bidirectional stream.
    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        let id = self.request(|reply| ConnCmd::AcceptBi { reply }).await?;
        Ok(self.streams(id))
    }

    /// Closes the connection with an application error code and reason.
    pub fn close(&self, code: VarInt, reason: &[u8]) {
        self.inner.shared.set_closed(ConnectionError::LocallyClosed);
        let _ = self.inner.send(ConnCmd::Close {
            code,
            reason: Bytes::copy_from_slice(reason),
        });
    }

    /// Resolves when the connection closes, with the reason.
    pub async fn closed(&self) -> ConnectionError {
        let mut closed = self.inner.shared.closed.subscribe();
        let reason = closed
            .wait_for(Option::is_some)
            .await
            .map(|reason| reason.clone());
        match reason {
            Ok(reason) => reason.unwrap_or(ConnectionError::LocallyClosed),
            Err(_) => self.inner.shared.close_reason(),
        }
    }

    /// The close reason, if the connection has closed.
    pub fn close_reason(&self) -> Option<ConnectionError> {
        self.inner.shared.closed.borrow().clone()
    }

    /// Connection statistics (OBS-4), or `None` once the connection task has
    /// ended.
    pub async fn stats(&self) -> Option<ConnStats> {
        let (reply, stats) = oneshot::channel();
        self.inner.cmds.send(ConnCmd::Stats { reply }).ok()?;
        stats.await.ok()
    }

    /// A process-unique ID for this connection.
    pub fn stable_id(&self) -> usize {
        self.inner.shared.stable_id
    }

    async fn request(
        &self,
        cmd: impl FnOnce(oneshot::Sender<Result<StreamId, ConnectionError>>) -> ConnCmd,
    ) -> Result<StreamId, ConnectionError> {
        let (reply, result) = oneshot::channel();
        self.inner.send(cmd(reply))?;
        result
            .await
            .unwrap_or_else(|_| Err(self.inner.shared.close_reason()))
    }

    fn streams(&self, id: StreamId) -> (SendStream, RecvStream) {
        (
            SendStream::new(self.inner.clone(), id),
            RecvStream::new(self.inner.clone(), id),
        )
    }
}
