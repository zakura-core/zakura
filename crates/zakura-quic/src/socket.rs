//! UDP sockets: bind and buffer sizing (SOCK-1 to SOCK-5), datagram I/O with
//! the Linux offload fast path (SOCK-13), and the socket cell that survives a
//! one-shot rebind (SOCK-9).
//!
//! On Linux, sends and receives go through `sendmsg`/`recvmsg` with
//! generic segmentation offload (`UDP_SEGMENT`), generic receive offload
//! (`UDP_GRO`), packet info for wildcard binds and the don't-fragment option
//! that path MTU discovery needs. Other platforms use tokio's plain
//! `send_to`/`recv_from`: correct, but one datagram per system call and no
//! MTU discovery.

use std::{
    io,
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, PoisonError, RwLock,
    },
    time::Duration,
};

use quinn_proto::Transmit;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::sync::watch;

/// Receive buffer size: the largest UDP payload, and the largest batch GRO
/// coalesces (SOCK-13).
pub(crate) const RECV_BUFFER_BYTES: usize = 1 << 16;
/// Datagrams per segmented send (SOCK-13). Matches the `quinn` crate.
const MAX_GSO_SEGMENTS: usize = 10;

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

    // SOCK-3: request both sizes and read them back.
    if let Err(error) = socket.set_recv_buffer_size(recv_buffer_bytes) {
        tracing::warn!(target: "zakura_quic", %addr, %error, "failed to set SO_RCVBUF");
    }
    if let Err(error) = socket.set_send_buffer_size(send_buffer_bytes) {
        tracing::warn!(target: "zakura_quic", %addr, %error, "failed to set SO_SNDBUF");
    }
    let buffers = SocketBuffers {
        recv_requested: recv_buffer_bytes,
        recv_effective: socket.recv_buffer_size().unwrap_or(0),
        send_requested: send_buffer_bytes,
        send_effective: socket.send_buffer_size().unwrap_or(0),
    };
    warn_if_clamped(addr, &buffers);

    let socket: std::net::UdpSocket = socket.into();
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
            granted = granted(buffers.recv_effective),
            reported = buffers.recv_effective,
            "kernel clamped SO_RCVBUF below the request; raise net.core.rmem_max",
        );
    }
    if granted(buffers.send_effective) < buffers.send_requested {
        tracing::warn!(
            target: "zakura_quic",
            %addr,
            requested = buffers.send_requested,
            granted = granted(buffers.send_effective),
            reported = buffers.send_effective,
            "kernel clamped SO_SNDBUF below the request; raise net.core.wmem_max",
        );
    }
}

/// One received datagram, or one GRO batch of equal-size datagrams.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecvMeta {
    /// The sender.
    pub(crate) remote: SocketAddr,
    /// Bytes received.
    pub(crate) len: usize,
    /// The size of each datagram in the batch; equals `len` without GRO.
    pub(crate) stride: usize,
    /// The local address the datagram arrived on, for wildcard binds.
    pub(crate) dst_ip: Option<IpAddr>,
}

/// A bound socket and the offloads it supports (SOCK-13).
#[derive(Debug)]
pub(crate) struct UdpIo {
    socket: tokio::net::UdpSocket,
    /// GSO works until the first `EIO`, which means the NIC can't checksum
    /// segmented sends.
    gso: AtomicBool,
    gro: bool,
    pktinfo: bool,
    mtud: bool,
}

impl UdpIo {
    /// Wraps a bound socket. `offload` enables GSO and GRO where the kernel
    /// supports them (CTRL-12).
    pub(crate) fn new(socket: std::net::UdpSocket, offload: bool) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        let caps = linux::configure(&socket, offload);
        #[cfg(not(target_os = "linux"))]
        let caps = {
            let _ = offload;
            Caps::default()
        };
        Ok(Self {
            socket: tokio::net::UdpSocket::from_std(socket)?,
            gso: AtomicBool::new(caps.gso),
            gro: caps.gro,
            pktinfo: caps.pktinfo,
            mtud: caps.mtud,
        })
    }

    /// Whether the socket refuses to fragment, which MTU discovery needs.
    pub(crate) fn mtud_allowed(&self) -> bool {
        self.mtud
    }

    /// Whether the kernel accepted segmented sends at bind time.
    pub(crate) fn gso(&self) -> bool {
        self.gso.load(Ordering::Relaxed)
    }

    /// Datagrams one transmit may carry.
    pub(crate) fn max_transmit_segments(&self) -> usize {
        if self.gso() {
            MAX_GSO_SEGMENTS
        } else {
            1
        }
    }

    pub(crate) async fn readable(&self) -> io::Result<()> {
        self.socket.readable().await
    }

    pub(crate) async fn writable(&self) -> io::Result<()> {
        self.socket.writable().await
    }

    /// Receives one datagram or GRO batch into `buf`.
    ///
    /// Returns `Ok(None)` for a datagram the socket dropped as malformed, and
    /// `WouldBlock` once the queue is empty.
    pub(crate) fn try_recv(&self, buf: &mut [u8]) -> io::Result<Option<RecvMeta>> {
        #[cfg(target_os = "linux")]
        {
            self.socket.try_io(tokio::io::Interest::READABLE, || {
                linux::recv(&self.socket, buf, self.gro)
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let (len, remote) = self.socket.try_recv_from(buf)?;
            Ok(Some(RecvMeta {
                remote,
                len,
                stride: len,
                dst_ip: None,
            }))
        }
    }

    /// Sends one transmit. A datagram the network can't take is dropped
    /// rather than reported, because QUIC recovers from loss.
    pub(crate) fn try_send(&self, transmit: &Transmit, contents: &[u8]) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            let src_ip = transmit.src_ip.filter(|_| self.pktinfo);
            let result = self.socket.try_io(tokio::io::Interest::WRITABLE, || {
                linux::send(&self.socket, transmit, src_ip, contents)
            });
            match result {
                Err(error)
                    if error.raw_os_error() == Some(libc::EIO)
                        && transmit.segment_size.is_some() =>
                {
                    // The NIC can't checksum segmented sends; fall back for
                    // good and let QUIC retransmit this batch.
                    tracing::info!(target: "zakura_quic", "disabling GSO after EIO");
                    self.gso.store(false, Ordering::Relaxed);
                    metrics::counter!("zakura.quic.socket.send_dropped").increment(1);
                    Ok(())
                }
                Err(error) if is_droppable(&error) => {
                    metrics::counter!("zakura.quic.socket.send_dropped").increment(1);
                    Ok(())
                }
                other => other,
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let segment = transmit.segment_size.unwrap_or(contents.len()).max(1);
            for datagram in contents.chunks(segment) {
                match self.socket.try_send_to(datagram, transmit.destination) {
                    Ok(_) => {}
                    Err(error) if is_droppable(&error) => {
                        metrics::counter!("zakura.quic.socket.send_dropped").increment(1);
                    }
                    Err(error) => return Err(error),
                }
            }
            Ok(())
        }
    }
}

/// Errors that lose one datagram but leave the socket usable.
fn is_droppable(error: &io::Error) -> bool {
    // EMSGSIZE: an MTU probe larger than the path. EADDRNOTAVAIL and EINVAL:
    // a packet-info source address that left the host (SOCK-12).
    #[cfg(unix)]
    {
        matches!(
            error.raw_os_error(),
            Some(
                libc::EMSGSIZE
                    | libc::EADDRNOTAVAIL
                    | libc::EINVAL
                    | libc::ENETUNREACH
                    | libc::EHOSTUNREACH
            )
        )
    }
    #[cfg(not(unix))]
    {
        let _ = error;
        false
    }
}

/// The offloads a socket supports.
#[derive(Clone, Copy, Debug, Default)]
struct Caps {
    gso: bool,
    gro: bool,
    pktinfo: bool,
    mtud: bool,
}

/// Holds a socket's current [`UdpIo`] and lets the endpoint swap it after a
/// fatal error (SOCK-9).
///
/// Connection tasks send through [`SocketCell::get`] and never keep the
/// socket across a generation change, so a rebind can close the old socket
/// before it binds the same address again.
#[derive(Debug)]
pub(crate) struct SocketCell {
    current: RwLock<Option<Arc<UdpIo>>>,
    generation: watch::Sender<u64>,
}

impl SocketCell {
    pub(crate) fn new(io: UdpIo) -> Self {
        Self {
            current: RwLock::new(Some(Arc::new(io))),
            generation: watch::Sender::new(0),
        }
    }

    /// The current socket, or `None` while a rebind runs.
    pub(crate) fn get(&self) -> Option<Arc<UdpIo>> {
        self.current
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Closes the socket for good once its last in-flight user lets go.
    pub(crate) fn close(&self) {
        self.current
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        self.generation.send_modify(|generation| *generation += 1);
    }

    /// Changes whenever the socket is taken or replaced.
    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.generation.subscribe()
    }

    /// Closes the current socket, waits up to 1 s for its last users to let
    /// go, and installs the socket `rebind` returns.
    pub(crate) async fn replace(
        &self,
        rebind: impl FnOnce() -> io::Result<UdpIo>,
    ) -> io::Result<()> {
        let old = self
            .current
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        self.generation.send_modify(|generation| *generation += 1);
        if let Some(old) = old {
            let weak = Arc::downgrade(&old);
            drop(old);
            for _ in 0..1000 {
                if weak.strong_count() == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        let io = rebind()?;
        *self.current.write().unwrap_or_else(PoisonError::into_inner) = Some(Arc::new(io));
        self.generation.send_modify(|generation| *generation += 1);
        Ok(())
    }
}

// SOCK-13: the fast path needs `sendmsg`/`recvmsg` with control messages,
// which no safe std or socket2 API exposes.
#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
mod linux {
    //! The Linux fast path (SOCK-13). Every `unsafe` block carries a
    //! `SAFETY` comment; control messages are parsed with explicit bounds
    //! checks and never trust a length the kernel or a peer could influence.

    use std::{
        io, mem,
        net::{IpAddr, Ipv4Addr, Ipv6Addr},
        os::fd::AsRawFd,
        sync::OnceLock,
    };

    use quinn_proto::Transmit;
    use socket2::{SockAddr, SockAddrStorage};

    use super::{Caps, RecvMeta};

    /// Control buffer size; GRO plus IPv6 packet info needs 64 bytes.
    const CONTROL_BYTES: usize = 128;

    /// A control buffer aligned for `cmsghdr`.
    #[repr(C, align(8))]
    struct Control([u8; CONTROL_BYTES]);

    /// Types every bit pattern of which is a valid value, so they can be read
    /// from and written to byte buffers.
    ///
    /// # Safety
    ///
    /// Implementers must be plain C structs or integers without invalid bit
    /// patterns. Raw pointers count: any address is a valid raw pointer.
    unsafe trait Pod: Copy {}
    // SAFETY: integers and C structs of integers, arrays and raw pointers.
    unsafe impl Pod for libc::c_int {}
    unsafe impl Pod for u16 {}
    unsafe impl Pod for libc::cmsghdr {}
    unsafe impl Pod for libc::msghdr {}
    unsafe impl Pod for libc::in_pktinfo {}
    unsafe impl Pod for libc::in6_pktinfo {}

    /// Reads a `T` at `at`, or `None` if it doesn't fit in `bytes`.
    fn read_pod<T: Pod>(bytes: &[u8], at: usize) -> Option<T> {
        let end = at.checked_add(mem::size_of::<T>())?;
        let source = bytes.get(at..end)?;
        // SAFETY: `source` holds exactly `size_of::<T>()` initialized bytes,
        // `read_unaligned` needs no alignment, and `T: Pod` accepts any bits.
        Some(unsafe { source.as_ptr().cast::<T>().read_unaligned() })
    }

    /// Writes `value` at `at`; returns `false` if it doesn't fit.
    fn write_pod<T: Pod>(bytes: &mut [u8], at: usize, value: T) -> bool {
        let Some(end) = at.checked_add(mem::size_of::<T>()) else {
            return false;
        };
        let Some(target) = bytes.get_mut(at..end) else {
            return false;
        };
        // SAFETY: `target` has room for exactly one `T` and
        // `write_unaligned` needs no alignment.
        unsafe { target.as_mut_ptr().cast::<T>().write_unaligned(value) };
        true
    }

    /// An all-zero `T`.
    fn zeroed<T: Pod>() -> T {
        read_pod(&[0u8; 128], 0).expect("every Pod type this module uses is under 128 bytes")
    }

    /// `CMSG_ALIGN`: Linux aligns control messages to the pointer size.
    const fn align(len: usize) -> usize {
        (len + mem::size_of::<usize>() - 1) & !(mem::size_of::<usize>() - 1)
    }

    /// Bytes from a control message's start to its data (`CMSG_LEN(0)`).
    const HEADER: usize = align(mem::size_of::<libc::cmsghdr>());

    fn setsockopt(
        socket: &impl AsRawFd,
        level: libc::c_int,
        name: libc::c_int,
        value: libc::c_int,
    ) -> io::Result<()> {
        // SAFETY: `value` outlives the call and the length is its exact size,
        // which fits in `socklen_t`.
        let result = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                level,
                name,
                (&value as *const libc::c_int).cast(),
                mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Whether the kernel accepts `UDP_SEGMENT`, probed once on a scratch
    /// socket so the option's default never applies to a real one.
    fn gso_supported() -> bool {
        static SUPPORTED: OnceLock<bool> = OnceLock::new();
        *SUPPORTED.get_or_init(|| {
            std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
                .and_then(|probe| setsockopt(&probe, libc::SOL_UDP, libc::UDP_SEGMENT, 1500))
                .is_ok()
        })
    }

    /// Sets the socket options the fast path uses and reports which took.
    pub(super) fn configure(socket: &std::net::UdpSocket, offload: bool) -> Caps {
        let local = socket.local_addr().ok();
        let ipv6 = local.is_some_and(|addr| addr.is_ipv6());
        let wildcard = local.is_some_and(|addr| addr.ip().is_unspecified());
        // quinn-proto's MTU discovery needs packets that never fragment.
        let mtud = if ipv6 {
            setsockopt(
                socket,
                libc::IPPROTO_IPV6,
                libc::IPV6_MTU_DISCOVER,
                libc::IPV6_PMTUDISC_PROBE,
            )
        } else {
            setsockopt(
                socket,
                libc::IPPROTO_IP,
                libc::IP_MTU_DISCOVER,
                libc::IP_PMTUDISC_PROBE,
            )
        }
        .is_ok();
        // A wildcard socket must reply from the address each peer used.
        let pktinfo = wildcard
            && if ipv6 {
                setsockopt(socket, libc::IPPROTO_IPV6, libc::IPV6_RECVPKTINFO, 1)
            } else {
                setsockopt(socket, libc::IPPROTO_IP, libc::IP_PKTINFO, 1)
            }
            .is_ok();
        let gro = offload && setsockopt(socket, libc::SOL_UDP, libc::UDP_GRO, 1).is_ok();
        let gso = offload && gso_supported();
        Caps {
            gso,
            gro,
            pktinfo,
            mtud,
        }
    }

    pub(super) fn recv(
        socket: &tokio::net::UdpSocket,
        buf: &mut [u8],
        gro: bool,
    ) -> io::Result<Option<RecvMeta>> {
        let mut name = SockAddrStorage::zeroed();
        let mut iov = libc::iovec {
            iov_base: buf.as_mut_ptr().cast(),
            iov_len: buf.len(),
        };
        let mut control = Control([0; CONTROL_BYTES]);
        let mut hdr: libc::msghdr = zeroed();
        hdr.msg_name = (&raw mut name).cast();
        hdr.msg_namelen = name.size_of();
        hdr.msg_iov = &mut iov;
        hdr.msg_iovlen = 1;
        hdr.msg_control = control.0.as_mut_ptr().cast();
        hdr.msg_controllen = CONTROL_BYTES as _;
        // SAFETY: every pointer in `hdr` refers to a live buffer of the length
        // `hdr` gives, and nothing else borrows them during the call.
        let received = unsafe { libc::recvmsg(socket.as_raw_fd(), &mut hdr, 0) };
        // A negative return is the only failure; others are byte counts.
        let Ok(len) = usize::try_from(received) else {
            return Err(io::Error::last_os_error());
        };
        if hdr.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0 {
            // A truncated datagram is corrupt, and without the GRO control
            // message the batch can't be split.
            metrics::counter!("zakura.quic.socket.recv_truncated").increment(1);
            return Ok(None);
        }
        // SAFETY: the kernel initialized `msg_namelen` bytes of `name`, which
        // can't exceed the storage size passed in.
        let remote = unsafe { SockAddr::new(name, hdr.msg_namelen) }.as_socket();
        let Some(remote) = remote else {
            return Ok(None);
        };
        // glibc declares `msg_controllen` as `usize` and musl as `u32`; both fit.
        #[allow(clippy::unnecessary_cast)]
        let controllen = (hdr.msg_controllen as usize).min(CONTROL_BYTES);
        let (stride, dst_ip) = parse_control(&control.0[..controllen]);
        Ok(Some(RecvMeta {
            remote,
            len,
            stride: stride.filter(|_| gro).unwrap_or(len).clamp(1, len.max(1)),
            dst_ip,
        }))
    }

    /// Reads the GRO segment size and the packet-info destination address.
    fn parse_control(control: &[u8]) -> (Option<usize>, Option<IpAddr>) {
        let mut stride = None;
        let mut dst_ip = None;
        let mut at = 0;
        while let Some(header) = read_pod::<libc::cmsghdr>(control, at) {
            // glibc declares `cmsg_len` as `usize` and musl as `u32`; both fit.
            #[allow(clippy::unnecessary_cast)]
            let len = header.cmsg_len as usize;
            let Some(end) = at
                .checked_add(len)
                .filter(|end| len >= HEADER && *end <= control.len())
            else {
                break;
            };
            let data = &control[at + HEADER..end];
            match (header.cmsg_level, header.cmsg_type) {
                (libc::SOL_UDP, libc::UDP_GRO) => {
                    stride = read_pod::<libc::c_int>(data, 0)
                        .and_then(|size| usize::try_from(size).ok())
                        .filter(|size| *size > 0);
                }
                (libc::IPPROTO_IP, libc::IP_PKTINFO) => {
                    dst_ip = read_pod::<libc::in_pktinfo>(data, 0)
                        .map(|info| IpAddr::V4(Ipv4Addr::from(u32::from_be(info.ipi_addr.s_addr))));
                }
                (libc::IPPROTO_IPV6, libc::IPV6_PKTINFO) => {
                    dst_ip = read_pod::<libc::in6_pktinfo>(data, 0)
                        .map(|info| IpAddr::V6(Ipv6Addr::from(info.ipi6_addr.s6_addr)));
                }
                _ => {}
            }
            at += align(len);
        }
        (stride, dst_ip)
    }

    /// Appends one control message; returns the new length, or `None` if the
    /// buffer is full.
    fn push_control<T: Pod>(
        control: &mut [u8],
        at: usize,
        level: libc::c_int,
        kind: libc::c_int,
        value: T,
    ) -> Option<usize> {
        let mut header: libc::cmsghdr = zeroed();
        header.cmsg_len = (HEADER + mem::size_of::<T>()) as _;
        header.cmsg_level = level;
        header.cmsg_type = kind;
        let next = at + align(HEADER + mem::size_of::<T>());
        (next <= control.len()
            && write_pod(control, at, header)
            && write_pod(control, at + HEADER, value))
        .then_some(next)
    }

    pub(super) fn send(
        socket: &tokio::net::UdpSocket,
        transmit: &Transmit,
        src_ip: Option<IpAddr>,
        contents: &[u8],
    ) -> io::Result<()> {
        let destination = SockAddr::from(transmit.destination);
        let mut iov = libc::iovec {
            iov_base: contents.as_ptr().cast_mut().cast(),
            iov_len: contents.len(),
        };
        let mut control = Control([0; CONTROL_BYTES]);
        let mut used = 0;
        if let Some(segment) = transmit
            .segment_size
            .filter(|segment| *segment < contents.len())
        {
            // quinn-proto caps segments at the MTU, so the size fits in u16.
            let segment = u16::try_from(segment).unwrap_or(u16::MAX);
            used = push_control(
                &mut control.0,
                used,
                libc::SOL_UDP,
                libc::UDP_SEGMENT,
                segment,
            )
            .expect("the control buffer has room for every message");
        }
        match src_ip {
            Some(IpAddr::V4(ip)) => {
                let mut info: libc::in_pktinfo = zeroed();
                info.ipi_spec_dst.s_addr = u32::from(ip).to_be();
                used = push_control(
                    &mut control.0,
                    used,
                    libc::IPPROTO_IP,
                    libc::IP_PKTINFO,
                    info,
                )
                .expect("the control buffer has room for every message");
            }
            Some(IpAddr::V6(ip)) => {
                let mut info: libc::in6_pktinfo = zeroed();
                info.ipi6_addr.s6_addr = ip.octets();
                used = push_control(
                    &mut control.0,
                    used,
                    libc::IPPROTO_IPV6,
                    libc::IPV6_PKTINFO,
                    info,
                )
                .expect("the control buffer has room for every message");
            }
            None => {}
        }
        let mut hdr: libc::msghdr = zeroed();
        hdr.msg_name = destination.as_ptr().cast_mut().cast();
        hdr.msg_namelen = destination.len();
        hdr.msg_iov = &mut iov;
        hdr.msg_iovlen = 1;
        if used > 0 {
            hdr.msg_control = control.0.as_mut_ptr().cast();
            hdr.msg_controllen = used as _;
        }
        // SAFETY: every pointer in `hdr` refers to a live buffer of the length
        // `hdr` gives; `sendmsg` only reads them.
        let sent = unsafe { libc::sendmsg(socket.as_raw_fd(), &hdr, 0) };
        if sent < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn message<T: Pod>(level: libc::c_int, kind: libc::c_int, value: T) -> Vec<u8> {
            let mut control = vec![0u8; CONTROL_BYTES];
            let len = push_control(&mut control, 0, level, kind, value).unwrap();
            control.truncate(len);
            control
        }

        #[test]
        fn control_messages_round_trip() {
            let mut control = message(libc::SOL_UDP, libc::UDP_GRO, 1200 as libc::c_int);
            let mut info: libc::in_pktinfo = zeroed();
            info.ipi_addr.s_addr = u32::from(Ipv4Addr::new(192, 0, 2, 7)).to_be();
            control.extend(message(libc::IPPROTO_IP, libc::IP_PKTINFO, info));
            assert_eq!(
                parse_control(&control),
                (Some(1200), Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 7))))
            );
        }

        #[test]
        fn malformed_control_messages_are_ignored() {
            let valid = message(libc::SOL_UDP, libc::UDP_GRO, 1200 as libc::c_int);
            // Every truncation shorter than `cmsg_len` parses to nothing.
            for len in 0..HEADER + mem::size_of::<libc::c_int>() {
                assert_eq!(parse_control(&valid[..len]), (None, None), "length {len}");
            }
            // A header that claims more bytes than the buffer holds.
            let mut lying = valid.clone();
            let mut header: libc::cmsghdr = read_pod(&lying, 0).unwrap();
            header.cmsg_len = 4096;
            write_pod(&mut lying, 0, header);
            assert_eq!(parse_control(&lying), (None, None));
            // A header shorter than itself must not loop forever.
            let mut short = valid.clone();
            header.cmsg_len = 0;
            write_pod(&mut short, 0, header);
            assert_eq!(parse_control(&short), (None, None));
            // A zero or negative GRO size is ignored.
            assert_eq!(
                parse_control(&message(libc::SOL_UDP, libc::UDP_GRO, -5 as libc::c_int)),
                (None, None)
            );
        }
    }
}
