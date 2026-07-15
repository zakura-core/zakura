//! Zakura protocol service trait surface.

use std::{collections::HashMap, fmt, future::Future, net::IpAddr, pin::Pin};

use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{CloseCause, FramedRecv, FramedSend};
use crate::{
    zakura::{ServicePeerDirection, ZakuraConnId, ZakuraPeerId},
    BoxError,
};

use super::Frame;

/// Boxed future returned by object-safe stream handlers.
pub type BoxRunFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Transport mode for a service-declared stream.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum StreamMode {
    /// A long-lived ordered stream between connected peers.
    Ordered {
        /// Which side of a connection may proactively open this stream.
        opening: OrderedStreamOpening,
    },
    /// A short-lived request/response stream opened per request.
    RequestResponse,
}

/// Opening policy for a long-lived ordered stream.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum OrderedStreamOpening {
    /// Only the peer that initiated the underlying connection opens the stream.
    Initiator,
    /// Either peer may open the stream when its local service has demand.
    ///
    /// If both peers open it concurrently, the transport's deterministic
    /// collision tiebreak selects one physical stream for both services.
    EitherPeer,
}

/// A service-declared Zakura stream.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Stream {
    /// Unique stream kind carried in `StreamPrelude.stream_kind`.
    pub kind: u16,
    /// Version of this stream kind.
    pub version: u16,
    /// Maximum application frame bytes for this stream.
    pub frame_cap: u32,
    /// Capability bit both peers must negotiate before this stream is wired.
    pub capability: u64,
    /// Stream lifetime and opening semantics.
    pub mode: StreamMode,
}

impl Stream {
    /// Return whether this is a long-lived ordered stream.
    pub(crate) fn is_ordered(self) -> bool {
        matches!(self.mode, StreamMode::Ordered { .. })
    }

    /// Return whether this is a short-lived request/response stream.
    pub(crate) fn is_request_response(self) -> bool {
        self.mode == StreamMode::RequestResponse
    }

    /// Return whether this side may proactively open this ordered stream.
    pub(crate) fn may_open_ordered(self, is_connection_initiator: bool) -> bool {
        match self.mode {
            StreamMode::Ordered {
                opening: OrderedStreamOpening::Initiator,
            } => is_connection_initiator,
            StreamMode::Ordered {
                opening: OrderedStreamOpening::EitherPeer,
            } => true,
            StreamMode::RequestResponse => false,
        }
    }
}

/// Transport state for one ordered service stream.
#[derive(Debug)]
pub(crate) struct ServiceStream {
    pub(crate) session_id: u64,
    pub(crate) recv: FramedRecv,
    pub(crate) send: FramedSend,
    pub(crate) cancel_token: CancellationToken,
}

impl ServiceStream {
    pub(crate) fn new(
        session_id: u64,
        recv: FramedRecv,
        send: FramedSend,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            session_id,
            recv,
            send,
            cancel_token,
        }
    }
}

/// Per-peer transport state handed to a service when a peer connects.
#[derive(Debug)]
pub struct Peer {
    /// Authenticated Zakura peer identity.
    pub id: ZakuraPeerId,
    /// Supervisor registration generation that owns this service session.
    pub conn_id: ZakuraConnId,
    /// Remote IP address when the transport knows it.
    pub remote_ip: Option<IpAddr>,
    /// Capabilities accepted by both peers.
    pub negotiated: u64,
    /// Direction of the underlying authenticated connection.
    pub direction: ServicePeerDirection,
    streams: HashMap<u16, ServiceStream>,
    cancel_token: CancellationToken,
    service_cancel_token: CancellationToken,
    close_cause: CloseCause,
}

impl Peer {
    /// Build a peer from already-opened transport streams.
    pub fn new(
        id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        negotiated: u64,
        streams: HashMap<u16, (FramedRecv, FramedSend)>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self::new_with_conn_id_and_direction(
            0,
            id,
            remote_ip,
            negotiated,
            ServicePeerDirection::Inbound,
            streams,
            cancel_token,
        )
    }

    /// Build a peer from already-opened transport streams and a known direction.
    pub fn new_with_direction(
        id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        negotiated: u64,
        direction: ServicePeerDirection,
        streams: HashMap<u16, (FramedRecv, FramedSend)>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self::new_with_conn_id_and_direction(
            0,
            id,
            remote_ip,
            negotiated,
            direction,
            streams,
            cancel_token,
        )
    }

    /// Build a peer from transport streams, connection id, and direction.
    pub(crate) fn new_with_conn_id_and_direction(
        conn_id: ZakuraConnId,
        id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        negotiated: u64,
        direction: ServicePeerDirection,
        streams: HashMap<u16, (FramedRecv, FramedSend)>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self::new_with_conn_id_and_direction_and_close_cause(
            conn_id,
            id,
            remote_ip,
            negotiated,
            direction,
            streams,
            cancel_token,
            CloseCause::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_conn_id_and_direction_and_close_cause(
        conn_id: ZakuraConnId,
        id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        negotiated: u64,
        direction: ServicePeerDirection,
        streams: HashMap<u16, (FramedRecv, FramedSend)>,
        cancel_token: CancellationToken,
        close_cause: CloseCause,
    ) -> Self {
        let streams = streams
            .into_iter()
            .map(|(kind, (recv, send))| {
                (
                    kind,
                    ServiceStream::new(0, recv, send, cancel_token.child_token()),
                )
            })
            .collect::<HashMap<_, _>>();
        Self::new_with_service_streams(
            conn_id,
            id,
            remote_ip,
            negotiated,
            direction,
            streams,
            cancel_token,
            close_cause,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_service_streams(
        conn_id: ZakuraConnId,
        id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        negotiated: u64,
        direction: ServicePeerDirection,
        streams: HashMap<u16, ServiceStream>,
        cancel_token: CancellationToken,
        close_cause: CloseCause,
    ) -> Self {
        let service_cancel_token = streams
            .values()
            .next()
            .map(|stream| stream.cancel_token.clone())
            .unwrap_or_else(|| cancel_token.child_token());
        Self::new_with_service_cancel_token(
            conn_id,
            id,
            remote_ip,
            negotiated,
            direction,
            streams,
            cancel_token,
            service_cancel_token,
            close_cause,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_service_cancel_token(
        conn_id: ZakuraConnId,
        id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        negotiated: u64,
        direction: ServicePeerDirection,
        streams: HashMap<u16, ServiceStream>,
        cancel_token: CancellationToken,
        service_cancel_token: CancellationToken,
        close_cause: CloseCause,
    ) -> Self {
        Self {
            id,
            conn_id,
            remote_ip,
            negotiated,
            direction,
            streams,
            cancel_token,
            service_cancel_token,
            close_cause,
        }
    }

    /// Take ownership of a stream pair for `kind`.
    pub fn take_stream(&mut self, kind: u16) -> Option<(FramedRecv, FramedSend)> {
        self.streams
            .remove(&kind)
            .map(|stream| (stream.recv, stream.send))
    }

    /// Take ownership of a stream pair and its owning ordered-stream generation.
    pub fn take_stream_with_session_id(
        &mut self,
        kind: u16,
    ) -> Option<(u64, FramedRecv, FramedSend)> {
        self.streams
            .remove(&kind)
            .map(|stream| (stream.session_id, stream.recv, stream.send))
    }

    /// Return the cancellation token for this peer's service tasks.
    ///
    /// The token is the transport supervisor's per-peer disconnect token, so it
    /// fires when this peer disconnects or the local node shuts down.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// Return the cancellation token for this service's local session work.
    ///
    /// Cancelling this token parks only the local service session. The parent
    /// peer token still fires on connection shutdown.
    pub fn service_cancel_token(&self) -> CancellationToken {
        self.service_cancel_token.clone()
    }

    /// Return the shared first-cause connection close recorder.
    pub(crate) fn close_cause(&self) -> CloseCause {
        self.close_cause.clone()
    }

    /// Split this peer into fields so the registry can fan streams out by owner.
    pub(crate) fn into_parts(
        self,
    ) -> (
        ZakuraPeerId,
        ZakuraConnId,
        Option<IpAddr>,
        u64,
        ServicePeerDirection,
        HashMap<u16, ServiceStream>,
        CancellationToken,
        CloseCause,
    ) {
        (
            self.id,
            self.conn_id,
            self.remote_ip,
            self.negotiated,
            self.direction,
            self.streams,
            self.cancel_token,
            self.close_cause,
        )
    }
}

/// A Zakura protocol service.
pub trait Service: fmt::Debug + Send + Sync + 'static {
    /// Stable service name for logs and diagnostics.
    fn name(&self) -> &'static str;

    /// Streams this service owns.
    fn streams(&self) -> &[Stream];

    /// Return whether this service currently wants a new session for `peer`.
    ///
    /// This is a cheap, advisory demand check used by the transport before
    /// opening an ordered stream. Implementations intentionally keep this to
    /// local room/interest state; remote first-party summary preference is
    /// applied upstream before dialing or escalation selection.
    ///
    /// A `false` result is temporary: local capacity, usefulness, or retry
    /// backoff can change while the underlying connection remains healthy. For
    /// ordered streams this check is repeated with bounded backoff until the
    /// service accepts a session or the connection closes.
    ///
    /// The service remains authoritative at [`Service::add_peer`], where it can
    /// still reject or locally park a session if its state changed concurrently.
    fn wants_peer(
        &self,
        _peer: &ZakuraPeerId,
        _negotiated: u64,
        _direction: ServicePeerDirection,
    ) -> bool {
        true
    }

    /// Add a connected peer and spawn any per-stream work owned by this service.
    fn add_peer(&self, peer: Peer);

    /// Remove a disconnected peer.
    fn remove_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId);

    /// Deliver one request-response frame to this service.
    fn deliver_frame(
        &self,
        _peer_id: ZakuraPeerId,
        _stream_kind: u16,
        _frame: Frame,
    ) -> Result<(), SinkReject> {
        Err(SinkReject::protocol(
            "service does not accept inbound frames",
        ))
    }

    /// Return this service's request/response handler, if it has one.
    fn as_request_response(&self) -> Option<&dyn RequestResponseService> {
        None
    }
}

/// A Zakura service that accepts one-shot request/response streams.
pub trait RequestResponseService: Service {
    /// Deliver one request-response request frame to this service.
    ///
    /// `max_frame_bytes` bounds each encoded outbound frame; `max_message_bytes`
    /// is the peer's negotiated full-message cap. Responders must size outbound
    /// frame payloads against the smaller of the two so the peer never receives
    /// a frame larger than its accepted message cap.
    fn request_frame<'a>(
        &'a self,
        peer_id: ZakuraPeerId,
        stream_kind: u16,
        request_id: u64,
        max_frame_bytes: u32,
        max_message_bytes: u32,
        frame: Frame,
    ) -> BoxRunFuture<'a, Result<Vec<Frame>, SinkReject>>;
}

/// A per-stream reader owned by a service.
///
/// This trait deliberately uses an explicit boxed future instead of native
/// `async fn` or the `async-trait` crate: stream handlers are intended to be
/// object-dispatched, and the explicit signature keeps that object safety without
/// adding another dependency.
pub trait Sink: Send + 'static {
    /// Run the reader until the stream closes or is rejected.
    fn run(self: Box<Self>, recv: FramedRecv) -> BoxRunFuture<'static, Result<(), SinkReject>>;
}

/// A per-stream writer owned by a service.
///
/// P2 services can either run this task shape directly or keep a concrete
/// typed send handle built from the [`FramedSend`] handed to [`Service::add_peer`].
///
/// See [`Sink`] for why this uses an explicit boxed future.
pub trait Source: Send + 'static {
    /// Run the writer until the stream closes.
    fn run(self: Box<Self>, send: FramedSend) -> BoxRunFuture<'static, ()>;
}

/// Reason a service sink rejected a decoded frame stream.
#[derive(Debug, Error)]
pub enum SinkReject {
    /// The peer sent protocol-invalid data, so the connection should close.
    #[error("inbound sink rejected protocol-invalid frame: {0}")]
    Protocol(#[source] BoxError),

    /// Local sink state prevented delivery; the peer is not at fault.
    #[error("inbound sink could not accept frame locally: {0}")]
    Local(#[source] BoxError),
}

impl SinkReject {
    /// Build a fatal peer-protocol rejection.
    pub fn protocol(error: impl Into<BoxError>) -> Self {
        Self::Protocol(error.into())
    }

    /// Build a non-fatal local-delivery rejection.
    pub fn local(error: impl Into<BoxError>) -> Self {
        Self::Local(error.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_reject_constructors_preserve_protocol_and_local_contract() {
        let protocol = SinkReject::protocol("bad frame");
        let local = SinkReject::local("closed queue");

        assert!(matches!(protocol, SinkReject::Protocol(_)));
        assert!(matches!(local, SinkReject::Local(_)));
        assert!(protocol.to_string().contains("protocol-invalid"));
        assert!(local.to_string().contains("locally"));
    }
}
