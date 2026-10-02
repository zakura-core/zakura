//! UDP sockets: bind, buffer sizing (SOCK-1 to SOCK-5) and the one-shot
//! rebind on a fatal receive error (SOCK-9).

use std::{
    fmt, io,
    io::IoSliceMut,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use noq::{
    udp::{RecvMeta, UdpSocketState},
    AsyncUdpSocket, Runtime, UdpSender,
};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::sync::mpsc;

/// The effective socket buffer sizes read back after bind (SOCK-3).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SocketBuffers {
    /// Requested `SO_RCVBUF`.
    pub recv_requested: usize,
    /// Effective `SO_RCVBUF` as the kernel reports it.
    pub recv_effective: usize,
    /// Requested `SO_SNDBUF`.
    pub send_requested: usize,
    /// Effective `SO_SNDBUF` as the kernel reports it.
    pub send_effective: usize,
}

/// A freshly bound socket and its metadata.
pub(crate) struct BoundSocket {
    pub(crate) socket: std::net::UdpSocket,
    pub(crate) local_addr: SocketAddr,
    pub(crate) buffers: SocketBuffers,
    pub(crate) inode: Option<u64>,
}

/// Binds one UDP socket and sizes its buffers.
pub(crate) fn bind_udp(
    addr: SocketAddr,
    recv_buffer_bytes: usize,
    send_buffer_bytes: usize,
) -> io::Result<BoundSocket> {
    let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
    if addr.is_ipv6() {
        // SOCK-2.
        socket.set_only_v6(true)?;
    }
    socket.bind(&addr.into())?;
    socket.set_nonblocking(true)?;
    let socket: std::net::UdpSocket = socket.into();

    // SOCK-3: request through noq-udp and read both sizes back.
    let state = UdpSocketState::new((&socket).into())?;
    if let Err(error) = state.set_recv_buffer_size((&socket).into(), recv_buffer_bytes) {
        tracing::warn!(target: "zakura_quic", %addr, %error, "failed to set SO_RCVBUF");
    }
    if let Err(error) = state.set_send_buffer_size((&socket).into(), send_buffer_bytes) {
        tracing::warn!(target: "zakura_quic", %addr, %error, "failed to set SO_SNDBUF");
    }
    let buffers = SocketBuffers {
        recv_requested: recv_buffer_bytes,
        recv_effective: state.recv_buffer_size((&socket).into()).unwrap_or(0),
        send_requested: send_buffer_bytes,
        send_effective: state.send_buffer_size((&socket).into()).unwrap_or(0),
    };
    warn_if_clamped(addr, &buffers);

    let local_addr = socket.local_addr()?;
    #[cfg(target_os = "linux")]
    let inode = crate::sys::socket_inode(&socket);
    #[cfg(not(target_os = "linux"))]
    let inode = None;
    Ok(BoundSocket {
        socket,
        local_addr,
        buffers,
        inode,
    })
}

/// Returns the granted size. Linux reports double the size it grants, to cover
/// bookkeeping overhead (SOCK-4).
fn granted(effective: usize) -> usize {
    if cfg!(target_os = "linux") {
        effective / 2
    } else {
        effective
    }
}

/// Logs one warning per clamped buffer (SOCK-4).
fn warn_if_clamped(addr: SocketAddr, buffers: &SocketBuffers) {
    if granted(buffers.recv_effective) < buffers.recv_requested {
        tracing::warn!(
            target: "zakura_quic",
            %addr,
            requested = buffers.recv_requested,
            effective = buffers.recv_effective,
            "kernel clamped SO_RCVBUF below the request; raise net.core.rmem_max",
        );
    }
    if granted(buffers.send_effective) < buffers.send_requested {
        tracing::warn!(
            target: "zakura_quic",
            %addr,
            requested = buffers.send_requested,
            effective = buffers.send_effective,
            "kernel clamped SO_SNDBUF below the request; raise net.core.wmem_max",
        );
    }
}

/// Wraps noq's socket so a fatal receive error asks for a rebind instead of
/// stopping the endpoint driver (SOCK-9).
///
/// noq's driver ends on any receive error other than `ConnectionReset`, which
/// would stop the endpoint. This wrapper reports the error on `failures` and
/// parks the driver. The endpoint's socket supervisor then calls
/// `noq::Endpoint::rebind_abstract`, which swaps the socket, hands every
/// connection a new sender and wakes the driver. When the supervisor gives up,
/// it closes the channel and the wrapper returns the error, which stops the
/// driver.
pub(crate) struct RebindOnError {
    inner: Box<dyn AsyncUdpSocket>,
    failures: mpsc::UnboundedSender<io::Error>,
    failed: bool,
}

impl RebindOnError {
    pub(crate) fn wrap(
        runtime: &Arc<dyn Runtime>,
        socket: std::net::UdpSocket,
        failures: mpsc::UnboundedSender<io::Error>,
    ) -> io::Result<Box<dyn AsyncUdpSocket>> {
        Ok(Box::new(Self {
            inner: runtime.wrap_udp_socket(socket)?,
            failures,
            failed: false,
        }))
    }
}

impl fmt::Debug for RebindOnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RebindOnError")
            .field("inner", &self.inner)
            .field("failed", &self.failed)
            .finish()
    }
}

impl AsyncUdpSocket for RebindOnError {
    fn create_sender(&self) -> Pin<Box<dyn UdpSender>> {
        self.inner.create_sender()
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        if self.failed {
            // Waiting for the supervisor to swap this socket out.
            return Poll::Pending;
        }
        match self.inner.poll_recv(cx, bufs, meta) {
            Poll::Ready(Err(error))
                if !matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock
                        | io::ErrorKind::Interrupted
                        | io::ErrorKind::ConnectionReset
                ) =>
            {
                let kind = error.kind();
                match self.failures.send(error) {
                    Ok(()) => {
                        self.failed = true;
                        Poll::Pending
                    }
                    Err(mpsc::error::SendError(error)) => {
                        tracing::error!(target: "zakura_quic", ?kind, "UDP socket failed again; stopping the endpoint");
                        Poll::Ready(Err(error))
                    }
                }
            }
            other => other,
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn max_receive_segments(&self) -> std::num::NonZeroUsize {
        self.inner.max_receive_segments()
    }

    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}
