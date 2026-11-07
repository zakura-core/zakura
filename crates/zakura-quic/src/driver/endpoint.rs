//! The endpoint task: the reactor that owns one socket's
//! `quinn_proto::Endpoint`.

use std::{
    io,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::BytesMut;
use quinn_proto::{
    ConnectionEvent, ConnectionHandle, DatagramEvent, EndpointEvent, Incoming, Transmit,
};
use zakura_rt::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, oneshot, watch,
    },
    task::{JoinSet, JoinSetExt},
};

use super::{
    connection::CurrentLimits,
    connection::{self, now, ConnTaskParams},
    ConnRef, ConnShared, Connecting, CLOSING_QUEUE_BYTES, CONNECTION_QUEUE_BYTES,
    SOCKET_QUEUE_BYTES,
};
use crate::{
    congestion::CongestionProbe,
    endpoint::{
        decide, inbound_handshake, Acceptor, Admit, ConnectionSlot, IncomingInfo, Shared,
        SocketSlot,
    },
    socket::{SocketCell, SocketFactory, UdpIo, RECV_BUFFER_BYTES},
    sys::canonical_addr,
};

/// How long closing waits for connections to drain (API-7).
pub(crate) const SHUTDOWN_DRAIN: Duration = Duration::from_secs(3);
/// A second socket failure within this window stops the endpoint (SOCK-9).
const REBIND_WINDOW: Duration = Duration::from_secs(60);
/// Receive calls per wakeup before the task serves its other inputs.
const MAX_RECVS_PER_WAKEUP: usize = 32;
/// Endpoint events handled per wakeup.
const MAX_EVENTS_PER_WAKEUP: usize = 64;
/// How often the task exports its receive counters.
const FLUSH_INTERVAL: Duration = Duration::from_secs(10);

/// A request from the [`crate::QuicEndpoint`] handle.
pub(crate) enum EndpointCmd {
    Connect {
        config: quinn_proto::ClientConfig,
        remote: SocketAddr,
        probe: Arc<CongestionProbe>,
        slot: Arc<ConnectionSlot>,
        reply: oneshot::Sender<Result<Connecting, quinn_proto::ConnectError>>,
    },
    Serve {
        server: Arc<quinn_proto::ServerConfig>,
        acceptor: Arc<dyn Acceptor>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

struct ConnEntry {
    /// Bounded by [`CONNECTION_QUEUE_BYTES`] through `shared`.
    datagrams: mpsc::UnboundedSender<(ConnectionEvent, usize)>,
    control: mpsc::UnboundedSender<ConnectionEvent>,
    shared: Arc<ConnShared>,
    /// Released when the entry leaves the table (ADM-12, ADM-13).
    slot: Arc<ConnectionSlot>,
}

struct Serving {
    server: Arc<quinn_proto::ServerConfig>,
    acceptor: Arc<dyn Acceptor>,
}

#[derive(Default)]
struct RecvCounters {
    calls: u64,
    datagrams: u64,
    queue_drops: u64,
    oversize_drops: u64,
}

pub(crate) struct EndpointTask<F: SocketFactory> {
    endpoint: quinn_proto::Endpoint,
    /// Builds the SOCK-9 replacement socket.
    factory: Arc<F>,
    socket: Arc<SocketCell<F::Socket>>,
    slot: Arc<SocketSlot>,
    shared: Arc<Shared>,
    cmds: mpsc::UnboundedReceiver<EndpointCmd>,
    cmds_open: bool,
    events_tx: mpsc::UnboundedSender<(ConnectionHandle, EndpointEvent)>,
    events_rx: mpsc::UnboundedReceiver<(ConnectionHandle, EndpointEvent)>,
    conns: HashMap<ConnectionHandle, ConnEntry>,
    serve: Option<Serving>,
    closing: watch::Sender<bool>,
    close_deadline: Option<Instant>,
    shutdown_replies: Vec<oneshot::Sender<()>>,
    tasks: JoinSet<()>,
    last_rebind: Option<Instant>,
    socket_failed: bool,
    more_to_recv: bool,
    recv_buf: Vec<u8>,
    response_buf: Vec<u8>,
    counters: RecvCounters,
    /// The queued datagram bytes across this socket's connections.
    queued_total: Arc<AtomicUsize>,
}

impl<F: SocketFactory> Drop for EndpointTask<F> {
    /// Releases the socket even when `shutdown` aborts the task (API-7).
    fn drop(&mut self) {
        self.socket.close();
    }
}

impl<F: SocketFactory> EndpointTask<F> {
    /// A task for the socket `io`, which `factory` bound at `slot.local_addr`.
    pub(crate) fn new(
        shared: Arc<Shared>,
        slot: Arc<SocketSlot>,
        factory: Arc<F>,
        io: F::Socket,
        cmds: mpsc::UnboundedReceiver<EndpointCmd>,
    ) -> Self {
        let allow_mtud = io.mtud_allowed();
        let endpoint =
            quinn_proto::Endpoint::new(Arc::new(shared.endpoint_config()), None, allow_mtud, None);
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        Self {
            endpoint,
            factory,
            socket: Arc::new(SocketCell::new(io)),
            slot,
            shared,
            cmds,
            cmds_open: true,
            events_tx,
            events_rx,
            conns: HashMap::new(),
            serve: None,
            closing: watch::Sender::new(false),
            close_deadline: None,
            shutdown_replies: Vec::new(),
            tasks: JoinSet::new(),
            last_rebind: None,
            socket_failed: false,
            more_to_recv: false,
            recv_buf: vec![0; RECV_BUFFER_BYTES],
            response_buf: Vec::new(),
            counters: RecvCounters::default(),
            queued_total: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(crate) async fn run(mut self) {
        let mut flush = zakura_rt::time::interval(FLUSH_INTERVAL);
        flush.set_missed_tick_behavior(zakura_rt::time::MissedTickBehavior::Delay);
        let mut deadline = Box::pin(zakura_rt::time::sleep(Duration::ZERO));
        loop {
            if self.close_deadline.is_some() && self.conns.is_empty() {
                break;
            }
            let io = if self.socket_failed {
                None
            } else {
                self.socket.get()
            };
            let more_to_recv = self.more_to_recv;
            if let Some(at) = self.close_deadline {
                deadline
                    .as_mut()
                    .reset(zakura_rt::time::Instant::from_std(at));
            }
            zakura_rt::select! {
                biased;
                Some((handle, event)) = self.events_rx.recv() => {
                    self.on_endpoint_event(handle, event);
                    for _ in 1..MAX_EVENTS_PER_WAKEUP {
                        let Ok((handle, event)) = self.events_rx.try_recv() else { break };
                        self.on_endpoint_event(handle, event);
                    }
                }
                cmd = self.cmds.recv(), if self.cmds_open => match cmd {
                    Some(cmd) => self.on_cmd(cmd),
                    // API-7: the last handle dropped.
                    None => {
                        self.cmds_open = false;
                        self.begin_close();
                    }
                },
                // Housekeeping goes before receive, so peer traffic can't
                // starve task reaping, the close deadline or the counters.
                Some(_) = self.tasks.join_next(), if !self.tasks.is_empty() => {}
                () = &mut deadline, if self.close_deadline.is_some() => {
                    tracing::debug!(target: "zakura_quic", "connections didn't drain within 3 s");
                    break;
                }
                _ = flush.tick() => self.flush_counters(),
                ready = async { io.as_ref()?.readable().await.ok() }, if io.is_some() && !more_to_recv => {
                    if ready.is_some() {
                        self.receive(io).await;
                    }
                }
                // Yield to the runtime between receive batches, so a busy
                // socket can't hold this worker thread.
                () = zakura_rt::task::yield_now(), if io.is_some() && more_to_recv => {
                    self.receive(io).await;
                }
            }
        }
        self.flush_counters();
        // API-7 step 4: release the socket even if a connection task still
        // holds the cell.
        self.socket.close();
        for reply in self.shutdown_replies.drain(..) {
            let _ = reply.send(());
        }
        // Dropping the set aborts every handshake and handler still running.
        self.tasks.shutdown().await;
    }

    fn on_cmd(&mut self, cmd: EndpointCmd) {
        match cmd {
            EndpointCmd::Connect {
                config,
                remote,
                probe,
                slot,
                reply,
            } => {
                if self.close_deadline.is_some() {
                    let _ = reply.send(Err(quinn_proto::ConnectError::EndpointStopping));
                    return;
                }
                let result = self
                    .endpoint
                    .connect(now(), config, remote, crate::tls::UNSENT_SERVER_NAME)
                    .map(|(handle, conn)| self.spawn_connection(handle, conn, probe, remote, slot));
                let _ = reply.send(result);
            }
            EndpointCmd::Serve { server, acceptor } => {
                if self.close_deadline.is_none() {
                    self.endpoint.set_server_config(Some(server.clone()));
                    self.serve = Some(Serving { server, acceptor });
                }
            }
            EndpointCmd::Shutdown { reply } => {
                self.shutdown_replies.push(reply);
                self.begin_close();
            }
        }
    }

    /// Stops accepting and closes every connection (API-7).
    fn begin_close(&mut self) {
        if self.close_deadline.is_some() {
            return;
        }
        self.endpoint.set_server_config(None);
        self.serve = None;
        self.closing.send_replace(true);
        self.close_deadline = Some(now() + SHUTDOWN_DRAIN);
    }

    fn spawn_connection(
        &mut self,
        handle: ConnectionHandle,
        conn: quinn_proto::Connection,
        probe: Arc<CongestionProbe>,
        admitted: SocketAddr,
        slot: Arc<ConnectionSlot>,
    ) -> Connecting {
        let (cmds_tx, cmds) = mpsc::unbounded_channel();
        let (datagrams_tx, datagrams) = mpsc::unbounded_channel();
        let (control_tx, control) = mpsc::unbounded_channel();
        let (handshake_tx, handshake) = oneshot::channel();
        let shared = ConnShared::new(canonical_addr(admitted), self.queued_total.clone());
        self.conns.insert(
            handle,
            ConnEntry {
                datagrams: datagrams_tx,
                control: control_tx,
                shared: shared.clone(),
                slot: slot.clone(),
            },
        );
        let config = &self.shared.config;
        let limits = CurrentLimits {
            receive_window: u64::from(config.receive_window_bytes),
            send_window: config.send_window_bytes,
            max_bidi_streams: u64::from(self.shared.max_bidi_streams),
        };
        zakura_rt::task::spawn_named(
            "quic.connection",
            connection::run(ConnTaskParams {
                handle,
                conn,
                shared: shared.clone(),
                cmds,
                datagrams,
                control,
                to_endpoint: self.events_tx.clone(),
                socket: self.socket.clone(),
                closing: self.closing.subscribe(),
                network: self.shared.network.subscribe(),
                probe,
                hooks: self.shared.hooks.clone(),
                admitted: canonical_addr(admitted),
                handshake: handshake_tx,
                limits,
                slot,
            }),
        );
        Connecting {
            conn: ConnRef {
                cmds: cmds_tx,
                shared,
            },
            handshake,
            remote: canonical_addr(admitted),
        }
    }

    fn on_endpoint_event(&mut self, handle: ConnectionHandle, event: EndpointEvent) {
        // quinn-proto indexes its table by handle; an event for a connection it
        // already freed must not reach it.
        if !self.conns.contains_key(&handle) {
            return;
        }
        let drained = event.is_drained();
        if let Some(reply) = self.endpoint.handle_event(handle, event) {
            if let Some(entry) = self.conns.get(&handle) {
                let _ = entry.control.send(reply);
            }
        }
        if drained {
            // The connection's state is freed: its slot is free too (ADM-11).
            self.conns.remove(&handle);
        }
    }

    /// Reads until the socket is empty or the per-wakeup budget runs out.
    async fn receive(&mut self, io: Option<Arc<F::Socket>>) {
        let Some(io) = io else {
            return;
        };
        self.more_to_recv = false;
        let mut failure = None;
        for _ in 0..MAX_RECVS_PER_WAKEUP {
            let mut buf = std::mem::take(&mut self.recv_buf);
            let result = io.try_recv(&mut buf);
            match result {
                Ok(Some(meta)) => {
                    self.counters.calls += 1;
                    self.on_datagrams(meta.remote, meta.dst_ip, meta.stride, &buf[..meta.len], &io);
                    self.recv_buf = buf;
                    continue;
                }
                Ok(None) => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.recv_buf = buf;
                    return;
                }
                // 6pp4: each call consumes one ICMP-reported error, so retrying
                // can't spin forever.
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::ConnectionReset
                    ) => {}
                Err(error) => failure = Some(error),
            }
            self.recv_buf = buf;
            if failure.is_some() {
                break;
            }
        }
        match failure {
            Some(error) => {
                drop(io);
                self.on_socket_error(error).await;
            }
            None => self.more_to_recv = true,
        }
    }

    fn on_datagrams(
        &mut self,
        remote: SocketAddr,
        dst_ip: Option<std::net::IpAddr>,
        stride: usize,
        data: &[u8],
        io: &F::Socket,
    ) {
        let now = now();
        // The advertised limit fits in a u16, so the cast can't truncate.
        let max_len = self.endpoint.config().get_max_udp_payload_size() as usize;
        for datagram in split_segments(data, stride, max_len) {
            self.counters.datagrams += 1;
            let Some(datagram) = datagram else {
                self.counters.oversize_drops += 1;
                continue;
            };
            let bytes = datagram.len();
            self.response_buf.clear();
            match self
                .endpoint
                .handle(now, remote, dst_ip, None, datagram, &mut self.response_buf)
            {
                Some(DatagramEvent::NewConnection(incoming)) => self.on_incoming(incoming, io),
                Some(DatagramEvent::ConnectionEvent(handle, event)) => {
                    if let Some(entry) = self.conns.get(&handle) {
                        let queued = &entry.shared.queued_datagram_bytes;
                        let limit = if entry.slot.is_closing() {
                            CLOSING_QUEUE_BYTES
                        } else {
                            CONNECTION_QUEUE_BYTES
                        };
                        if queued.load(Ordering::Relaxed) + bytes > limit
                            || self.queued_total.load(Ordering::Relaxed) + bytes
                                > SOCKET_QUEUE_BYTES
                        {
                            // SPEC §6a: a queue is full; the peer retransmits.
                            self.counters.queue_drops += 1;
                            entry.shared.queue_drops.fetch_add(1, Ordering::Relaxed);
                        } else {
                            queued.fetch_add(bytes, Ordering::Relaxed);
                            self.queued_total.fetch_add(bytes, Ordering::Relaxed);
                            if entry.datagrams.send((event, bytes)).is_err() {
                                // The connection task has ended.
                                queued.fetch_sub(bytes, Ordering::Relaxed);
                                self.queued_total.fetch_sub(bytes, Ordering::Relaxed);
                            }
                        }
                    }
                }
                Some(DatagramEvent::Response(transmit)) => self.send_response(&transmit, io),
                None => {}
            }
        }
    }

    fn send_response(&self, transmit: &Transmit, io: &F::Socket) {
        // A stateless response that can't go out now is dropped; the peer
        // retries.
        if let Err(error) = io.try_send(transmit, &self.response_buf[..transmit.size]) {
            if error.kind() != io::ErrorKind::WouldBlock {
                metrics::counter!("zakura.quic.socket.response_send_errors").increment(1);
                tracing::debug!(target: "zakura_quic", %error, "stateless response send failed");
            }
        }
    }

    /// Admission (SPEC §7): decide before any handshake work (ADM-2).
    fn on_incoming(&mut self, incoming: Incoming, io: &F::Socket) {
        let Some((server, acceptor)) = self
            .serve
            .as_ref()
            .map(|serving| (serving.server.clone(), serving.acceptor.clone()))
        else {
            self.endpoint.ignore(incoming);
            return;
        };
        let raw_remote = incoming.remote_address();
        let remote = canonical_addr(raw_remote);
        let info = IncomingInfo {
            remote,
            validated: incoming.remote_address_validated(),
            pending_total: self.shared.pending.total(),
            pending_from_ip: self.shared.pending.for_ip(remote.ip()),
        };
        let decision = decide(&self.shared.config, acceptor.as_ref(), &info);
        tracing::trace!(target: "zakura_quic", remote = %info.remote, ?decision, "incoming");
        self.response_buf.clear();
        match decision {
            Admit::Accept => {
                // ADM-12: a live slot. Closed connections draining hold their
                // own allowance, so they don't count here (ADM-13).
                let Some(slot) = self.shared.connections.claim_inbound() else {
                    metrics::counter!("zakura.quic.incoming.refused").increment(1);
                    tracing::trace!(target: "zakura_quic", remote = %info.remote, "connection cap full");
                    let transmit = self.endpoint.refuse(incoming, &mut self.response_buf);
                    self.send_response(&transmit, io);
                    return;
                };
                // ADM-7: count the handshake from Accept until it finishes.
                let pending = self.shared.pending.enter(remote.ip());
                let probe = Arc::new(CongestionProbe::default());
                let mut server = (*server).clone();
                server.transport_config(self.shared.connection_transport(probe.clone(), io.gso()));
                match self.endpoint.accept(
                    incoming,
                    now(),
                    &mut self.response_buf,
                    Some(Arc::new(server)),
                ) {
                    Ok((handle, conn)) => {
                        metrics::counter!("zakura.quic.incoming.accepted").increment(1);
                        let connecting =
                            self.spawn_connection(handle, conn, probe, raw_remote, slot);
                        self.tasks.spawn_named(
                            "quic.inbound_handshake",
                            inbound_handshake(
                                connecting,
                                self.shared.config.handshake_timeout(),
                                pending,
                                acceptor,
                            ),
                        );
                    }
                    Err(error) => {
                        metrics::counter!("zakura.quic.handshake.failed").increment(1);
                        tracing::trace!(target: "zakura_quic", cause = %error.cause, "accept failed");
                        if let Some(transmit) = error.response {
                            self.send_response(&transmit, io);
                        }
                    }
                }
            }
            Admit::Refuse => {
                metrics::counter!("zakura.quic.incoming.refused").increment(1);
                let transmit = self.endpoint.refuse(incoming, &mut self.response_buf);
                self.send_response(&transmit, io);
            }
            Admit::Retry => match self.endpoint.retry(incoming, &mut self.response_buf) {
                Ok(transmit) => {
                    metrics::counter!("zakura.quic.incoming.retried").increment(1);
                    self.send_response(&transmit, io);
                }
                Err(error) => {
                    metrics::counter!("zakura.quic.incoming.refused").increment(1);
                    self.response_buf.clear();
                    let transmit = self
                        .endpoint
                        .refuse(error.into_incoming(), &mut self.response_buf);
                    self.send_response(&transmit, io);
                }
            },
            Admit::Ignore => {
                metrics::counter!("zakura.quic.incoming.ignored").increment(1);
                self.endpoint.ignore(incoming);
            }
        }
    }

    /// Rebinds once after a fatal socket error, or stops the endpoint (SOCK-9).
    async fn on_socket_error(&mut self, error: io::Error) {
        let local_addr = self.slot.local_addr;
        tracing::warn!(target: "zakura_quic", %local_addr, %error, "UDP socket failed; rebinding");
        // The driver's clock, not `Instant::now`, so a simulation's virtual
        // time decides the window too.
        if self
            .last_rebind
            .is_some_and(|at| now().saturating_duration_since(at) < REBIND_WINDOW)
        {
            tracing::error!(target: "zakura_quic", %local_addr, %error, "UDP socket failed twice within 60 s; stopping the endpoint");
            self.stop_socket();
            return;
        }
        let factory = &self.factory;
        let slot = &self.slot;
        let rebound = self
            .socket
            .replace(|| {
                let bound = factory.bind(local_addr)?;
                slot.inode
                    .store(bound.inode.unwrap_or(0), Ordering::Relaxed);
                Ok(bound.io)
            })
            .await;
        match rebound {
            Ok(()) => {
                // The window starts once the replacement is in place.
                self.last_rebind = Some(now());
                self.slot.rebinds.fetch_add(1, Ordering::Relaxed);
                metrics::counter!("zakura.quic.socket.rebinds").increment(1);
            }
            Err(error) => {
                tracing::error!(target: "zakura_quic", %local_addr, %error, "rebind failed; stopping the endpoint");
                self.stop_socket();
            }
        }
    }

    fn stop_socket(&mut self) {
        self.socket_failed = true;
        self.slot.stopped.store(true, Ordering::Relaxed);
        metrics::counter!("zakura.quic.socket.stopped").increment(1);
        self.begin_close();
    }

    fn flush_counters(&mut self) {
        let counters = std::mem::take(&mut self.counters);
        metrics::counter!("zakura.quic.socket.recv_calls").increment(counters.calls);
        metrics::counter!("zakura.quic.socket.datagrams_received").increment(counters.datagrams);
        metrics::counter!("zakura.quic.endpoint.queue_drops").increment(counters.queue_drops);
        metrics::counter!("zakura.quic.endpoint.oversize_drops").increment(counters.oversize_drops);
        self.slot
            .oversize_drops
            .fetch_add(counters.oversize_drops, Ordering::Relaxed);
        self.slot
            .queue_drops
            .fetch_add(counters.queue_drops, Ordering::Relaxed);
        self.slot
            .recv_calls
            .fetch_add(counters.calls, Ordering::Relaxed);
        self.slot
            .datagrams_received
            .fetch_add(counters.datagrams, Ordering::Relaxed);
    }
}

/// Splits a received batch into datagrams, each in its own allocation.
///
/// quinn-proto keeps received stream data as slices of the datagram it arrived
/// in. If the datagrams of a GRO batch shared one allocation, one buffered
/// chunk would pin the whole batch (up to 64 KiB), and the receive window would
/// no longer bound a peer's memory.
///
/// A datagram longer than `max_len`, the `max_udp_payload_size` this endpoint
/// advertises, yields `None` without being copied (SPEC §6a). quinn-proto
/// routes a known connection ID before it authenticates the packet, so
/// without this check a peer could queue 64 KiB per datagram.
pub(crate) fn split_segments(
    data: &[u8],
    stride: usize,
    max_len: usize,
) -> impl Iterator<Item = Option<BytesMut>> + '_ {
    data.chunks(stride.max(1))
        .map(move |segment| (segment.len() <= max_len).then(|| BytesMut::from(segment)))
}

#[cfg(test)]
mod tests {
    use zakura_rt::sync::{
        atomic::{AtomicBool, AtomicU64},
        counter,
    };

    use super::*;
    use crate::{
        endpoint::{ConnectionSlots, PendingTable},
        sim::{LinkModel, SimNet},
        socket::SocketBuffers,
        tls::TlsConfig,
        NodeSecretKey, QuicConfig,
    };

    /// SOCK-9: the window starts once the replacement is installed, so time
    /// spent waiting for the old socket's last user doesn't shorten it.
    #[tokio::test(start_paused = true)]
    async fn the_rebind_window_starts_when_the_replacement_is_installed() {
        let secret = NodeSecretKey::generate();
        let shared = Arc::new(Shared {
            id: secret.public(),
            config: QuicConfig::default(),
            max_bidi_streams: 16,
            tls: TlsConfig::new(&secret),
            pending: PendingTable::default(),
            connections: ConnectionSlots::new(16, 0, 16),
            hooks: Arc::default(),
            shutdown: watch::Sender::new(false),
            network: watch::Sender::new(0),
            network_changes: counter::AtomicU64::new(0),
            inputs: crate::EndpointInputs::default(),
        });
        let addr: SocketAddr = "10.0.0.1:4000".parse().unwrap();
        let slot = Arc::new(SocketSlot {
            local_addr: addr,
            buffers: SocketBuffers::default(),
            gso: false,
            rebinds: counter::AtomicU64::new(0),
            inode: AtomicU64::new(0),
            recv_calls: counter::AtomicU64::new(0),
            datagrams_received: counter::AtomicU64::new(0),
            queue_drops: counter::AtomicU64::new(0),
            oversize_drops: counter::AtomicU64::new(0),
            stopped: AtomicBool::new(false),
        });
        let net = SimNet::new(LinkModel::default());
        let io = net.bind(addr).unwrap();
        let (_cmds, cmds_rx) = mpsc::unbounded_channel();
        let mut task = EndpointTask::new(shared, slot, Arc::new(net), io, cmds_rx);

        // An in-flight user holds the old socket for 500 ms.
        let held = task.socket.get().unwrap();
        let started = now();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            drop(held);
        });
        task.on_socket_error(io::Error::other("injected")).await;

        assert_eq!(task.slot.rebinds.load(Ordering::Relaxed), 1);
        let rebound_at = task.last_rebind.expect("the rebind succeeded");
        assert!(rebound_at >= started + Duration::from_millis(500));
    }
}
