//! The connection task: the peer routine that owns one QUIC connection.

use std::{
    collections::VecDeque,
    mem,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use quinn_proto::{
    ConnectionError, ConnectionEvent, ConnectionHandle, Dir, EndpointEvent, Event, StreamEvent,
    StreamId, Transmit, TransportError, TransportErrorCode, VarInt,
};
use zakura_rt::{
    collections::{HashMap, HashSet},
    sync::{atomic::Ordering, mpsc, oneshot, watch},
};

use super::{ConnCmd, ConnShared, Handshake, ReadReply, READ_BATCH_BYTES};
use crate::{
    congestion::CongestionProbe,
    conn::{
        CongestionStats, ConnLimits, ConnLoad, ConnSample, ConnStats, DriverStats, Hooks,
        LOAD_INTERVAL, SAMPLE_INTERVAL,
    },
    endpoint::ConnectionSlot,
    error::{ReadError, WriteError},
    key::NodeId,
    socket::{SocketCell, UdpIo},
    sys::canonical_ip,
    tls,
};

/// Transmits per wakeup before the task yields to its inputs. Matches the
/// `quinn` crate's budget.
const MAX_TRANSMITS_PER_WAKEUP: usize = 20;
/// Inputs of one kind handled per wakeup before the task drives output.
const MAX_INPUTS_PER_WAKEUP: usize = 64;
/// The shortest gap between two warnings about failed sends.
const SEND_WARNING_INTERVAL: Duration = Duration::from_secs(60);

/// The driver's clock: `zakura-rt`'s monotonic clock, which is tokio's in
/// production, so paused-time tests drive quinn-proto, and virtual inside a
/// simulated run.
pub(crate) fn now() -> Instant {
    zakura_rt::time::Instant::now().into_std()
}

/// Everything a connection task starts with.
pub(crate) struct ConnTaskParams<S> {
    pub(crate) handle: ConnectionHandle,
    pub(crate) conn: quinn_proto::Connection,
    pub(crate) shared: Arc<ConnShared>,
    pub(crate) cmds: mpsc::UnboundedReceiver<ConnCmd>,
    /// Routed datagrams with their sizes, which count against the endpoint's
    /// per-connection byte budget until handled.
    pub(crate) datagrams: mpsc::UnboundedReceiver<(ConnectionEvent, usize)>,
    pub(crate) control: mpsc::UnboundedReceiver<ConnectionEvent>,
    pub(crate) to_endpoint: mpsc::UnboundedSender<(ConnectionHandle, EndpointEvent)>,
    pub(crate) socket: Arc<SocketCell<S>>,
    pub(crate) closing: watch::Receiver<bool>,
    pub(crate) network: watch::Receiver<u64>,
    pub(crate) probe: Arc<CongestionProbe>,
    pub(crate) hooks: Arc<Hooks>,
    pub(crate) admitted: SocketAddr,
    pub(crate) handshake: oneshot::Sender<Result<Handshake, ConnectionError>>,
    /// The limits the transport config starts the connection with.
    pub(crate) limits: CurrentLimits,
    pub(crate) slot: Arc<ConnectionSlot>,
}

/// The connection's limits as last set (API-9).
#[derive(Clone, Copy, Debug)]
pub(crate) struct CurrentLimits {
    /// The receive window before held credit (API-10).
    pub(crate) receive_window: u64,
    pub(crate) send_window: u64,
    pub(crate) max_bidi_streams: u64,
}

/// How a receive stream ended, kept until its reader asks.
#[derive(Clone, Copy, Debug)]
enum RecvEnd {
    Finished,
    Reset(VarInt),
}

/// A write the connection hasn't fully handed to quinn-proto yet.
struct PendingWrite {
    data: Bytes,
    reply: Option<oneshot::Sender<Result<(), WriteError>>>,
}

#[derive(Default)]
struct SendState {
    writes: VecDeque<PendingWrite>,
    finish_after: bool,
    blocked_since: Option<Instant>,
}

/// Counters the driver keeps beside quinn-proto's (OBS-12).
#[derive(Clone, Copy, Debug, Default)]
struct Counters {
    transmits: u64,
    datagrams_sent: u64,
    datagrams_received: u64,
    send_blocked: Duration,
    delivered_bytes: u64,
    send_errors: u64,
}

/// Counters already exported as metric increments.
#[derive(Clone, Copy, Debug, Default)]
struct Exported {
    lost_packets: u64,
    congestion_events: u64,
    bytes_sent: u64,
    bytes_received: u64,
    transmits: u64,
    datagrams_sent: u64,
}

struct ConnTask<S: UdpIo> {
    handle: ConnectionHandle,
    conn: quinn_proto::Connection,
    shared: Arc<ConnShared>,
    to_endpoint: mpsc::UnboundedSender<(ConnectionHandle, EndpointEvent)>,
    socket: Arc<SocketCell<S>>,
    probe: Arc<CongestionProbe>,
    hooks: Arc<Hooks>,
    admitted: SocketAddr,
    handshake: Option<oneshot::Sender<Result<Handshake, ConnectionError>>>,

    remote_id: Option<NodeId>,
    error: Option<ConnectionError>,
    /// Why this node closed the connection, for the final sample (OBS-3).
    local_close: Option<String>,
    drained_sent: bool,
    /// The peer's IP as of the last path check (PATH-3).
    path_ip: IpAddr,
    limits: CurrentLimits,
    /// Every credit hold this task applied, and every release it took
    /// (API-10). The window leaves out their difference. Separate totals keep
    /// a release that arrives before its hold from being lost.
    credit_held: u64,
    credit_released: u64,
    /// When the load snapshot was last published, and whether it is stale.
    load_published: Option<Instant>,
    load_stale: bool,
    last_send_warning: Option<Instant>,

    openers: VecDeque<oneshot::Sender<Result<StreamId, ConnectionError>>>,
    opened_spare: VecDeque<StreamId>,
    acceptors: VecDeque<oneshot::Sender<Result<StreamId, ConnectionError>>>,
    accepted_spare: VecDeque<StreamId>,
    sends: HashMap<StreamId, SendState>,
    /// The peer's STOP_SENDING code for each stream this node reset in
    /// answer, or will reset once it is accepted, until the handle finishes,
    /// resets or drops it. A later write gets `Stopped(code)` instead of the
    /// reset stream's `ClosedStream`.
    stopped: HashMap<StreamId, VarInt>,
    /// Streams whose send handles can still issue writes.
    writable_handles: HashSet<StreamId>,
    /// How many remote bidirectional streams quinn-proto has handed out. A
    /// remote stream with a higher index is still waiting to be accepted.
    accepted_bidi: u64,
    /// Each stream's waiting read and the most bytes it asked for.
    reads: HashMap<StreamId, (oneshot::Sender<ReadReply>, usize)>,
    recv_ends: HashMap<StreamId, RecvEnd>,
    /// Streams whose receive half may still buffer unread data in
    /// quinn-proto, and whose send half may still hold unacknowledged data.
    /// Closing releases exactly these (ADM-13), so the work is bounded by the
    /// open streams, not by every stream the connection ever had.
    recv_payload: HashSet<StreamId>,
    send_payload: HashSet<StreamId>,
    /// This connection's ADM-12 slot, which moves to the closing allowance
    /// at close (ADM-13).
    slot: Arc<ConnectionSlot>,

    send_buf: Vec<u8>,
    blocked: Option<(Transmit, Vec<u8>)>,
    more_to_send: bool,
    counters: Counters,
    exported: Exported,
}

/// Runs one connection until quinn-proto drains it.
pub(crate) async fn run<S: UdpIo>(params: ConnTaskParams<S>) {
    let ConnTaskParams {
        handle,
        conn,
        shared,
        mut cmds,
        mut datagrams,
        mut control,
        to_endpoint,
        socket,
        mut closing,
        mut network,
        probe,
        hooks,
        admitted,
        handshake,
        limits,
        slot,
    } = params;
    let mut socket_generation = socket.subscribe();
    let credit = shared.clone();
    let mut task = ConnTask {
        handle,
        conn,
        shared,
        to_endpoint,
        socket,
        probe,
        hooks,
        admitted,
        handshake: Some(handshake),
        remote_id: None,
        error: None,
        local_close: None,
        drained_sent: false,
        path_ip: canonical_ip(admitted.ip()),
        limits,
        credit_held: 0,
        credit_released: 0,
        load_published: None,
        load_stale: false,
        last_send_warning: None,
        openers: VecDeque::new(),
        opened_spare: VecDeque::new(),
        acceptors: VecDeque::new(),
        accepted_spare: VecDeque::new(),
        sends: HashMap::new(),
        stopped: HashMap::new(),
        writable_handles: HashSet::new(),
        accepted_bidi: 0,
        reads: HashMap::new(),
        recv_ends: HashMap::new(),
        recv_payload: HashSet::new(),
        send_payload: HashSet::new(),
        slot,
        send_buf: Vec::new(),
        blocked: None,
        more_to_send: false,
        counters: Counters::default(),
        exported: Exported::default(),
    };

    let mut cmds_open = true;
    let mut timer = Box::pin(zakura_rt::time::sleep(Duration::ZERO));
    let mut timer_deadline: Option<Instant> = None;
    let mut sampler = zakura_rt::time::interval(SAMPLE_INTERVAL);
    sampler.set_missed_tick_behavior(zakura_rt::time::MissedTickBehavior::Delay);
    sampler.tick().await;
    let mut publish = Box::pin(zakura_rt::time::sleep(Duration::ZERO));
    let mut publish_deadline: Option<Instant> = None;
    let mut closing_seen = false;
    if *closing.borrow() {
        task.close(
            VarInt::from_u32(0),
            Bytes::from_static(b"endpoint shutdown"),
        );
    }

    loop {
        task.drive();
        if task.conn.is_drained() {
            break;
        }
        if !closing_seen && task.conn.is_closed() {
            closing_seen = true;
            if !task.enter_closing(&mut datagrams) {
                // ADM-13: the closing allowance is full, so drop the state
                // now. Dropping the task reports the connection drained.
                metrics::counter!("zakura.quic.connections.closing_full").increment(1);
                break;
            }
        }
        match task.publish_load() {
            Some(deadline) if publish_deadline != Some(deadline) => {
                publish
                    .as_mut()
                    .reset(zakura_rt::time::Instant::from_std(deadline));
                publish_deadline = Some(deadline);
            }
            Some(_) => {}
            None => publish_deadline = None,
        }
        match task.conn.poll_timeout() {
            Some(deadline) if timer_deadline != Some(deadline) => {
                timer
                    .as_mut()
                    .reset(zakura_rt::time::Instant::from_std(deadline));
                timer_deadline = Some(deadline);
            }
            Some(_) => {}
            None => timer_deadline = None,
        }
        let blocked_io = task.blocked.as_ref().and_then(|_| task.socket.get());
        let more_to_send = task.more_to_send;

        // Local inputs come before the datagram queue: they are bounded (one
        // read in flight and 64 KiB of write-ahead per stream) and wait on
        // network progress themselves, while a peer can keep the queue full.
        // Datagrams first would let a flood starve close, limits, shutdown
        // and the PATH-3 check.
        zakura_rt::select! {
            biased;
            event = control.recv() => match event {
                Some(event) => task.conn.handle_event(event),
                None => {
                    task.endpoint_gone();
                    break;
                }
            },
            cmd = cmds.recv(), if cmds_open => match cmd {
                Some(cmd) => {
                    task.handle_cmd(cmd);
                    for _ in 1..MAX_INPUTS_PER_WAKEUP {
                        let Ok(cmd) = cmds.try_recv() else { break };
                        task.handle_cmd(cmd);
                    }
                }
                // API-4: the last handle dropped.
                None => {
                    cmds_open = false;
                    let reason: &'static [u8] = if task.remote_id.is_some() {
                        b"all handles dropped"
                    } else {
                        b"handshake abandoned"
                    };
                    task.close(VarInt::from_u32(0), Bytes::from_static(reason));
                }
            },
            Ok(()) = closing.changed() => {
                if *closing.borrow() {
                    task.close(VarInt::from_u32(0), Bytes::from_static(b"endpoint shutdown"));
                }
            }
            () = &mut timer, if timer_deadline.is_some() => {
                timer_deadline = None;
                task.conn.handle_timeout(now());
            }
            Ok(()) = network.changed() => {
                // SOCK-12: forget the local address so the kernel picks one.
                task.conn.local_address_changed();
            }
            Ok(()) = socket_generation.changed() => {}
            // The next drive adds the released credit back (API-10).
            () = credit.credit_released() => {}
            _ = sampler.tick() => task.sample(),
            () = &mut publish, if publish_deadline.is_some() => publish_deadline = None,
            event = datagrams.recv() => match event {
                Some(event) => {
                    task.on_datagram(event);
                    for _ in 1..MAX_INPUTS_PER_WAKEUP {
                        let Ok(event) = datagrams.try_recv() else { break };
                        task.on_datagram(event);
                    }
                }
                None => {
                    task.endpoint_gone();
                    break;
                }
            },
            _ = async { blocked_io.as_ref()?.writable().await.ok() }, if blocked_io.is_some() => {}
            // Yield between transmit batches, so a busy connection can't hold
            // the worker thread.
            () = zakura_rt::task::yield_now(), if more_to_send => {}
        }
    }
    task.drain_datagrams(&mut datagrams).await;
}

impl<S: UdpIo> ConnTask<S> {
    async fn drain_datagrams(
        &self,
        datagrams: &mut mpsc::UnboundedReceiver<(ConnectionEvent, usize)>,
    ) {
        // Close first so concurrent endpoint sends fail and refund their own charge.
        datagrams.close();
        while let Some((_, bytes)) = datagrams.recv().await {
            self.refund_datagram(bytes);
        }
    }

    fn refund_datagram(&self, bytes: usize) {
        self.shared
            .queued_datagram_bytes
            .fetch_sub(bytes, Ordering::Relaxed);
        self.shared
            .socket_queued_bytes
            .fetch_sub(bytes, Ordering::Relaxed);
    }

    fn on_datagram(&mut self, (event, bytes): (ConnectionEvent, usize)) {
        self.refund_datagram(bytes);
        self.conn.handle_event(event);
        self.counters.datagrams_received += 1;
    }

    /// Publishes the load snapshot if it is stale and the last one is at
    /// least [`LOAD_INTERVAL`] old. Returns when to publish otherwise.
    fn publish_load(&mut self) -> Option<Instant> {
        if !self.load_stale {
            return None;
        }
        let now = now();
        if let Some(due) = self
            .load_published
            .map(|at| at + LOAD_INTERVAL)
            .filter(|due| *due > now)
        {
            return Some(due);
        }
        self.load_published = Some(now);
        self.load_stale = false;
        let load = self.load();
        self.shared.load.send_replace(load);
        None
    }

    fn load(&mut self) -> ConnLoad {
        let stats = self.conn.stats();
        let mut load = ConnLoad::new(self.conn.remote_address());
        load.delivered_bytes = self.counters.delivered_bytes;
        // usize always fits in u64 on supported targets.
        load.queued_send_bytes = self.queued_send_bytes() as u64;
        load.send_blocked_streams = self
            .sends
            .values()
            .filter(|state| state.blocked_since.is_some())
            .count() as u64;
        load.queued_datagram_bytes =
            self.shared.queued_datagram_bytes.load(Ordering::Relaxed) as u64;
        load.queue_drops = self.shared.queue_drops.load(Ordering::Relaxed);
        load.remote_open_streams = self.conn.streams().remote_open_streams(Dir::Bi);
        load.receive_window = self.limits.receive_window;
        load.send_window = self.limits.send_window;
        load.max_bidi_streams = self.limits.max_bidi_streams;
        load.udp_rx = stats.udp_rx;
        load.frame_rx = stats.frame_rx;
        load.send_errors = self.counters.send_errors;
        load
    }

    fn queued_send_bytes(&self) -> usize {
        self.sends
            .values()
            .flat_map(|state| state.writes.iter())
            .map(|write| write.data.len())
            .sum()
    }

    /// PATH-3: closes the connection if the peer's current IP is banned.
    /// Runs when the IP changes and, while it differs from the admitted IP,
    /// on every sample, so a ban that lands after the move still applies.
    fn check_path(&mut self, recheck: bool) {
        if self.remote_id.is_none() || self.error.is_some() {
            return;
        }
        let ip = canonical_ip(self.conn.remote_address().ip());
        let changed = ip != self.path_ip;
        self.path_ip = ip;
        let moved = ip != canonical_ip(self.admitted.ip());
        if !(changed || (recheck && moved)) {
            return;
        }
        if changed {
            tracing::debug!(target: "zakura_quic", %ip, admitted = %self.admitted, "peer migrated");
        }
        if self.hooks.is_banned(ip) {
            metrics::counter!("zakura.quic.paths.closed_banned").increment(1);
            tracing::debug!(target: "zakura_quic", %ip, "closing a connection that migrated to a banned IP");
            self.close(VarInt::from_u32(0), Bytes::from_static(b"banned path"));
        }
    }

    /// Moves every pending output: app events, datagrams and endpoint events.
    fn drive(&mut self) {
        self.update_credit(0);
        if self
            .conn
            .poll_timeout()
            .is_some_and(|deadline| deadline <= now())
        {
            // A timer can fire late under load; handle it on the clock.
            self.conn.handle_timeout(now());
        }
        // quinn-proto emits no event when the peer migrates: compare. This
        // runs before transmit, so a close it causes goes out now.
        self.check_path(false);
        self.process_events();
        self.transmit();
        self.process_events();
        while let Some(event) = self.conn.poll_endpoint_events() {
            self.drained_sent |= event.is_drained();
            let _ = self.to_endpoint.send((self.handle, event));
        }
        self.load_stale = true;
    }

    fn transmit(&mut self) {
        self.more_to_send = false;
        let Some(io) = self.socket.get() else {
            // A rebind is in progress (SOCK-9): drop what quinn-proto
            // produces; it counts as loss and gets retransmitted.
            self.blocked = None;
            while self
                .conn
                .poll_transmit(now(), 1, &mut self.send_buf)
                .is_some()
            {
                self.send_buf.clear();
            }
            return;
        };
        if let Some((transmit, contents)) = self.blocked.take() {
            match io.try_send(&transmit, &contents[..transmit.size]) {
                Ok(()) => self.count_sent(&transmit),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    self.blocked = Some((transmit, contents));
                    return;
                }
                Err(error) => self.send_failed(&error),
            }
        }
        let max_segments = io.max_transmit_segments();
        for _ in 0..MAX_TRANSMITS_PER_WAKEUP {
            self.send_buf.clear();
            let Some(transmit) = self
                .conn
                .poll_transmit(now(), max_segments, &mut self.send_buf)
            else {
                return;
            };
            match io.try_send(&transmit, &self.send_buf[..transmit.size]) {
                Ok(()) => self.count_sent(&transmit),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    self.blocked = Some((transmit, mem::take(&mut self.send_buf)));
                    return;
                }
                Err(error) => self.send_failed(&error),
            }
        }
        self.more_to_send = true;
    }

    fn count_sent(&mut self, transmit: &Transmit) {
        self.counters.transmits += 1;
        let datagrams = match transmit.segment_size {
            Some(segment) if segment > 0 => transmit.size.div_ceil(segment),
            _ => 1,
        };
        // A transmit holds at most 10 datagrams, so this can't truncate.
        self.counters.datagrams_sent += datagrams as u64;
    }

    fn send_failed(&mut self, error: &std::io::Error) {
        self.counters.send_errors += 1;
        metrics::counter!("zakura.quic.socket.send_errors").increment(1);
        // The packet is lost, so a persistent error (a firewall's EPERM, say)
        // silently stalls the connection: warn, but at most once a minute.
        let now = now();
        if self
            .last_send_warning
            .is_none_or(|at| now.saturating_duration_since(at) >= SEND_WARNING_INTERVAL)
        {
            self.last_send_warning = Some(now);
            tracing::warn!(
                target: "zakura_quic",
                %error,
                remote = %self.conn.remote_address(),
                errors = self.counters.send_errors,
                "UDP send failed",
            );
        } else {
            tracing::debug!(target: "zakura_quic", %error, "UDP send failed");
        }
    }

    fn process_events(&mut self) {
        while let Some(event) = self.conn.poll() {
            match event {
                Event::HandshakeDataReady | Event::DatagramReceived | Event::DatagramsUnblocked => {
                }
                Event::Connected => self.on_connected(),
                Event::ConnectionLost { reason } => self.terminate(reason),
                Event::Stream(StreamEvent::Opened { dir: Dir::Bi }) => self.serve_acceptors(),
                Event::Stream(StreamEvent::Opened { dir: Dir::Uni }) => {}
                Event::Stream(StreamEvent::Available { dir: Dir::Bi }) => self.serve_openers(),
                Event::Stream(StreamEvent::Available { dir: Dir::Uni }) => {}
                Event::Stream(StreamEvent::Readable { id }) => self.serve_reader(id),
                Event::Stream(StreamEvent::Writable { id }) => self.serve_writer(id),
                // All sent data is acknowledged, so quinn-proto holds none.
                Event::Stream(StreamEvent::Finished { id }) => {
                    self.send_payload.remove(&id);
                }
                Event::Stream(StreamEvent::Stopped { id, error_code }) => {
                    self.on_stopped(id, error_code);
                }
            }
        }
    }

    fn on_connected(&mut self) {
        let session = self.conn.crypto_session();
        let handshake = Handshake {
            remote_id: tls::remote_node_id(session),
            alpn: tls::negotiated_alpn(session),
        };
        self.remote_id = handshake.remote_id;
        metrics::gauge!("zakura.quic.connections").increment(1.0);
        if let Some(reply) = self.handshake.take() {
            let _ = reply.send(Ok(handshake));
        }
    }

    /// Fails every waiter with `reason`. Runs once.
    fn terminate(&mut self, reason: ConnectionError) {
        if self.error.is_some() {
            return;
        }
        self.error = Some(reason.clone());
        self.shared.set_closed(reason.clone());
        if let Some(reply) = self.handshake.take() {
            let _ = reply.send(Err(reason.clone()));
        }
        for reply in self.openers.drain(..).chain(self.acceptors.drain(..)) {
            let _ = reply.send(Err(reason.clone()));
        }
        for (_, (reply, _)) in self.reads.drain() {
            let _ = reply.send(Err(ReadError::ConnectionLost(reason.clone())));
        }
        for (_, state) in self.sends.drain() {
            for write in state.writes {
                if let Some(reply) = write.reply {
                    let _ = reply.send(Err(WriteError::ConnectionLost(reason.clone())));
                }
            }
        }
        if self.remote_id.is_some() {
            metrics::gauge!("zakura.quic.connections").decrement(1.0);
            let reason = match (&reason, self.local_close.take()) {
                (ConnectionError::LocallyClosed, Some(local)) => local,
                _ => reason.to_string(),
            };
            let sample = self.build_sample(Some(reason));
            if let Some(observer) = self.hooks.observer() {
                observer(&sample);
            }
        }
    }

    /// Releases a closed connection's payload and moves its slot to the
    /// closing allowance (ADM-13). Returns `false` when the allowance is full,
    /// and the caller then drops the connection's state at once.
    fn enter_closing(
        &mut self,
        datagrams: &mut mpsc::UnboundedReceiver<(ConnectionEvent, usize)>,
    ) -> bool {
        self.release_streams();
        let slot = self.slot.clone();
        // The backlog was queued under the live limit. A closed connection
        // only answers with CONNECTION_CLOSE, so dropping it loses nothing.
        // Drop only what was queued at the switch: refunds reopen room, so a
        // loop until empty could run as long as the peer keeps sending.
        // Later datagrams sit under the closing limit, and the run loop
        // handles them as usual.
        slot.begin_closing(|| {
            for _ in 0..datagrams.len() {
                let Ok((_, bytes)) = datagrams.try_recv() else {
                    break;
                };
                self.refund_datagram(bytes);
            }
        })
    }

    /// Frees the stream payload quinn-proto still holds for a closed
    /// connection (ADM-13). quinn-proto keeps every stream's buffers until the
    /// connection drains, though a closed connection neither sends nor
    /// receives stream data. Stopping a receive half drops its buffered data
    /// and resetting a send half drops its unacknowledged data.
    fn release_streams(&mut self) {
        // Remote streams the application never accepted can hold data too.
        while let Some(id) = self.conn.streams().accept(Dir::Bi) {
            self.accepted_bidi = id.index() + 1;
            self.recv_payload.insert(id);
        }
        for id in self.recv_payload.drain() {
            let _ = self.conn.recv_stream(id).stop(VarInt::from_u32(0));
        }
        for id in self.send_payload.drain() {
            let _ = self.conn.send_stream(id).reset(VarInt::from_u32(0));
        }
    }

    fn close(&mut self, code: VarInt, reason: Bytes) {
        if self.error.is_none() {
            self.local_close = Some(format!(
                "closed locally (code {code}): {}",
                String::from_utf8_lossy(&reason)
            ));
            self.conn.close(now(), code, reason);
        }
        self.terminate(ConnectionError::LocallyClosed);
    }

    /// The endpoint task ended without draining this connection.
    fn endpoint_gone(&mut self) {
        self.terminate(ConnectionError::TransportError(TransportError {
            code: TransportErrorCode::INTERNAL_ERROR,
            frame: None,
            reason: "the endpoint stopped".into(),
        }));
    }

    fn handle_cmd(&mut self, cmd: ConnCmd) {
        if let Some(error) = &self.error {
            reply_closed(cmd, error);
            return;
        }
        match cmd {
            ConnCmd::OpenBi { reply } => {
                self.openers.retain(|waiter| !waiter.is_closed());
                self.openers.push_back(reply);
                self.serve_openers();
            }
            ConnCmd::AcceptBi { reply } => {
                self.acceptors.retain(|waiter| !waiter.is_closed());
                self.acceptors.push_back(reply);
                self.serve_acceptors();
            }
            ConnCmd::Write { id, data, reply } => {
                if let Some(&code) = self.stopped.get(&id) {
                    let _ = reply.send(Err(WriteError::Stopped(code)));
                    return;
                }
                self.sends
                    .entry(id)
                    .or_default()
                    .writes
                    .push_back(PendingWrite {
                        data,
                        reply: Some(reply),
                    });
                self.serve_writer(id);
            }
            ConnCmd::Finish { id } | ConnCmd::DropSend { id } => {
                self.writable_handles.remove(&id);
                if self.stopped.remove(&id).is_none() {
                    match self.sends.get_mut(&id) {
                        Some(state) if !state.writes.is_empty() => state.finish_after = true,
                        _ => {
                            let _ = self.conn.send_stream(id).finish();
                        }
                    }
                }
            }
            ConnCmd::Reset { id, code } => {
                self.writable_handles.remove(&id);
                self.stopped.remove(&id);
                self.fail_writes(id, WriteError::ClosedStream);
                self.send_payload.remove(&id);
                let _ = self.conn.send_stream(id).reset(code);
            }
            ConnCmd::Unclaimed { id, accepted } => {
                // The caller gave up after the reply was sent. Queue the
                // stream for the next caller so no stream ID is lost.
                if accepted {
                    self.accepted_spare.push_front(id);
                    self.serve_acceptors();
                } else {
                    self.opened_spare.push_front(id);
                    self.serve_openers();
                }
            }
            ConnCmd::Read {
                id,
                max,
                hold,
                reply,
            } => {
                // Before the read, so the bytes it takes pay the hold first.
                self.update_credit(hold);
                self.reads.insert(id, (reply, max));
                self.serve_reader(id);
            }
            ConnCmd::Stop { id, code, hold } => {
                self.update_credit(hold);
                self.stop(id, code);
            }
            ConnCmd::DropRecv { id, hold } => {
                self.update_credit(hold);
                self.stop(id, VarInt::from_u32(0));
            }
            ConnCmd::Close { code, reason } => self.close(code, reason),
            ConnCmd::Stats { reply } => {
                let _ = reply.send(self.stats());
            }
            ConnCmd::SetLimits { limits } => self.set_limits(limits),
            ConnCmd::SetPriority { id, priority } => {
                let _ = self.conn.send_stream(id).set_priority(priority);
            }
        }
    }

    fn set_limits(&mut self, limits: ConnLimits) {
        if let Some(bytes) = limits.receive_window {
            self.limits.receive_window = bytes;
            self.apply_receive_window();
        }
        if let Some(bytes) = limits.send_window {
            self.conn.set_send_window(bytes);
            self.limits.send_window = bytes;
        }
        if let Some(streams) = limits.max_bidi_streams {
            self.conn
                .set_max_concurrent_streams(Dir::Bi, VarInt::from_u32(streams));
            self.limits.max_bidi_streams = u64::from(streams);
        }
    }

    /// Takes a reader's new `hold` and every release since the last update
    /// into the receive window (API-10).
    ///
    /// quinn-proto counts a shrink as debt that later reads pay before they
    /// grant the peer more credit, so the bytes the reader takes next return
    /// no credit until the hold drops. A grow grants the released credit at
    /// once.
    fn update_credit(&mut self, hold: u64) {
        let released = self.shared.take_released_credit();
        if hold == 0 && released == 0 {
            return;
        }
        self.credit_held = self.credit_held.saturating_add(hold);
        self.credit_released = self.credit_released.saturating_add(released);
        self.apply_receive_window();
    }

    /// The credit that live holds keep from the peer.
    fn outstanding_credit(&self) -> u64 {
        self.credit_held.saturating_sub(self.credit_released)
    }

    fn apply_receive_window(&mut self) {
        let window = self
            .limits
            .receive_window
            .saturating_sub(self.outstanding_credit());
        self.conn
            .set_receive_window(VarInt::from_u64(window).unwrap_or(VarInt::MAX));
    }

    fn stop(&mut self, id: StreamId, code: VarInt) {
        self.reads.remove(&id);
        // Stopping drops whatever quinn-proto buffered for the stream.
        self.recv_payload.remove(&id);
        if self.recv_ends.remove(&id).is_none() {
            let _ = self.conn.recv_stream(id).stop(code);
        }
    }

    fn serve_openers(&mut self) {
        while let Some(reply) = self.openers.pop_front() {
            if reply.is_closed() {
                continue;
            }
            let Some(id) = self.opened_spare.pop_front().or_else(|| {
                let id = self.conn.streams().open(Dir::Bi)?;
                self.recv_payload.insert(id);
                self.send_payload.insert(id);
                Some(id)
            }) else {
                self.openers.push_front(reply);
                return;
            };
            self.writable_handles.insert(id);
            if let Err(Ok(id)) = reply.send(Ok(id)) {
                // The opener gave up; the peer hasn't seen the stream yet, so
                // the next opener can use it.
                self.opened_spare.push_front(id);
            }
        }
    }

    fn serve_acceptors(&mut self) {
        while let Some(reply) = self.acceptors.pop_front() {
            if reply.is_closed() {
                continue;
            }
            let id = match self.accepted_spare.pop_front() {
                Some(id) => id,
                None => {
                    let Some(id) = self.conn.streams().accept(Dir::Bi) else {
                        self.acceptors.push_front(reply);
                        return;
                    };
                    self.accepted_bidi = id.index() + 1;
                    self.recv_payload.insert(id);
                    // The reset `on_stopped` deferred until this accept.
                    if let Some(&code) = self.stopped.get(&id) {
                        let _ = self.conn.send_stream(id).reset(code);
                    } else {
                        self.send_payload.insert(id);
                    }
                    id
                }
            };
            self.writable_handles.insert(id);
            if let Err(Ok(id)) = reply.send(Ok(id)) {
                self.accepted_spare.push_front(id);
            }
        }
    }

    fn serve_writer(&mut self, id: StreamId) {
        let Some(state) = self.sends.get_mut(&id) else {
            return;
        };
        while let Some(front) = state.writes.front_mut() {
            while !front.data.is_empty() {
                let mut chunks = [mem::take(&mut front.data)];
                let result = self.conn.send_stream(id).write_chunks(&mut chunks);
                let [rest] = chunks;
                front.data = rest;
                match result {
                    Ok(_) => {}
                    Err(quinn_proto::WriteError::Blocked) => {
                        // Flow control or the send window: wait for Writable.
                        state.blocked_since.get_or_insert_with(now);
                        return;
                    }
                    Err(quinn_proto::WriteError::Stopped(code)) => {
                        self.fail_writes(id, WriteError::Stopped(code));
                        return;
                    }
                    Err(quinn_proto::WriteError::ClosedStream) => {
                        self.fail_writes(id, WriteError::ClosedStream);
                        return;
                    }
                }
            }
            if let Some(since) = state.blocked_since.take() {
                self.counters.send_blocked += now().saturating_duration_since(since);
            }
            let write = state
                .writes
                .pop_front()
                .expect("front_mut returned an element");
            if let Some(reply) = write.reply {
                let _ = reply.send(Ok(()));
            }
        }
        if state.finish_after {
            let _ = self.conn.send_stream(id).finish();
        }
        self.sends.remove(&id);
    }

    /// Handles the peer's STOP_SENDING: fails pending writes, keeps the code
    /// while a handle can still write or the stream awaits acceptance, and
    /// answers with RESET_STREAM.
    fn on_stopped(&mut self, id: StreamId, error_code: VarInt) {
        self.fail_writes(id, WriteError::Stopped(error_code));
        let awaiting_accept = id.initiator() != self.conn.side()
            && id.dir() == Dir::Bi
            && id.index() >= self.accepted_bidi;
        if awaiting_accept {
            // quinn-proto counts a remote send stream only once it is
            // accepted, so a reset acked before then underflows that count.
            // `serve_acceptors` sends the reset on accept.
            self.stopped.insert(id, error_code);
            return;
        }
        if self.writable_handles.contains(&id) {
            self.stopped.insert(id, error_code);
        }
        // RFC 9000 §3.5: answer STOP_SENDING with RESET_STREAM. quinn-proto
        // leaves this to the application, and without it the peer never
        // learns the final size, so it never frees the stream or returns its
        // stream credit.
        self.send_payload.remove(&id);
        let _ = self.conn.send_stream(id).reset(error_code);
    }

    fn fail_writes(&mut self, id: StreamId, error: WriteError) {
        let Some(state) = self.sends.remove(&id) else {
            return;
        };
        if let Some(since) = state.blocked_since {
            self.counters.send_blocked += now().saturating_duration_since(since);
        }
        for write in state.writes {
            if let Some(reply) = write.reply {
                let _ = reply.send(Err(error.clone()));
            }
        }
    }

    fn serve_reader(&mut self, id: StreamId) {
        let Some((reply, max)) = self.reads.remove(&id) else {
            return;
        };
        if reply.is_closed() {
            return;
        }
        if let Some(end) = self.recv_ends.remove(&id) {
            let _ = reply.send(end_reply(end));
            return;
        }
        let mut stream = self.conn.recv_stream(id);
        let mut chunks = match stream.read(true) {
            Ok(chunks) => chunks,
            Err(_) => {
                self.recv_payload.remove(&id);
                let _ = reply.send(Err(ReadError::ClosedStream));
                return;
            }
        };
        let mut data = Vec::new();
        let mut budget = max.clamp(1, READ_BATCH_BYTES);
        let mut end = None;
        while budget > 0 {
            match chunks.next(budget) {
                Ok(Some(chunk)) => {
                    budget = budget.saturating_sub(chunk.bytes.len());
                    // usize always fits in u64 on supported targets.
                    self.counters.delivered_bytes += chunk.bytes.len() as u64;
                    data.push(chunk.bytes);
                }
                Ok(None) => {
                    end = Some(RecvEnd::Finished);
                    break;
                }
                Err(quinn_proto::ReadError::Blocked) => break,
                Err(quinn_proto::ReadError::Reset(code)) => {
                    end = Some(RecvEnd::Reset(code));
                    break;
                }
            }
        }
        // Reading pays any held credit first, then frees receive window; the
        // next drive() sends the credit.
        let _ = chunks.finalize();
        if end.is_some() {
            // Read to the end or reset: quinn-proto buffers nothing more.
            self.recv_payload.remove(&id);
        }
        match (data.is_empty(), end) {
            (false, end) => {
                if let Some(end) = end {
                    self.recv_ends.insert(id, end);
                }
                let _ = reply.send(Ok(Some(data)));
            }
            (true, Some(end)) => {
                let _ = reply.send(end_reply(end));
            }
            // Nothing to read yet: park until Readable (backpressure: no
            // read, no new credit for the peer).
            (true, None) => {
                self.reads.insert(id, (reply, max));
            }
        }
    }

    fn stats(&self) -> ConnStats {
        let metrics = self.conn.congestion_state().metrics();
        let queued = self.queued_send_bytes();
        let ongoing: Duration = self
            .sends
            .values()
            .filter_map(|state| state.blocked_since)
            .map(|since| now().saturating_duration_since(since))
            .sum();
        ConnStats {
            connection: self.conn.stats(),
            congestion: CongestionStats {
                window: metrics.congestion_window,
                ssthresh: metrics.ssthresh,
                pacing_rate_bps: metrics.pacing_rate,
                bytes_in_flight: self.probe.in_flight(),
                app_limited: self.probe.app_limited(),
                acked_bytes: self.probe.acked_bytes(),
            },
            driver: DriverStats {
                // usize always fits in u64 on supported targets.
                queued_send_bytes: queued as u64,
                send_blocked: self.counters.send_blocked + ongoing,
                transmits: self.counters.transmits,
                datagrams_sent: self.counters.datagrams_sent,
                datagrams_received: self.counters.datagrams_received,
                queue_drops: self.shared.queue_drops.load(Ordering::Relaxed),
                send_errors: self.counters.send_errors,
            },
        }
    }

    fn build_sample(&mut self, close_reason: Option<String>) -> ConnSample {
        let stats = self.stats();
        let path = stats.connection.path;
        let exported = Exported {
            lost_packets: path.lost_packets,
            congestion_events: path.congestion_events,
            bytes_sent: stats.connection.udp_tx.bytes,
            bytes_received: stats.connection.udp_rx.bytes,
            transmits: stats.driver.transmits,
            datagrams_sent: stats.driver.datagrams_sent,
        };
        export_delta(&self.exported, &exported);
        self.exported = exported;
        metrics::histogram!("zakura.quic.path.rtt_seconds").record(path.rtt.as_secs_f64());
        // Precision loss above 2^53 bytes doesn't matter for a histogram.
        metrics::histogram!("zakura.quic.path.cwnd_bytes").record(path.cwnd as f64);
        ConnSample {
            stable_id: self.shared.stable_id,
            outbound: self.conn.side().is_client(),
            remote_id: self.remote_id.unwrap_or_else(|| {
                unreachable!("samples run only after the handshake set remote_id")
            }),
            admitted_ip: canonical_ip(self.admitted.ip()),
            rtt: path.rtt,
            cwnd: path.cwnd,
            lost_packets: path.lost_packets,
            congestion_events: path.congestion_events,
            bytes_sent: stats.connection.udp_tx.bytes,
            bytes_received: stats.connection.udp_rx.bytes,
            current_mtu: path.current_mtu,
            bytes_in_flight: stats.congestion.bytes_in_flight,
            send_blocked: stats.driver.send_blocked,
            close_reason,
        }
    }

    /// The 10 s sample: PATH-3's re-check, then OBS-3.
    fn sample(&mut self) {
        if self.remote_id.is_none() || self.error.is_some() {
            return;
        }
        self.check_path(true);
        if self.error.is_some() {
            return;
        }
        let sample = self.build_sample(None);
        if let Some(observer) = self.hooks.observer() {
            observer(&sample);
        }
    }
}

impl<S: UdpIo> Drop for ConnTask<S> {
    fn drop(&mut self) {
        self.terminate(ConnectionError::LocallyClosed);
        if !self.drained_sent {
            // Let the endpoint free this connection's state (and its slot).
            let _ = self
                .to_endpoint
                .send((self.handle, EndpointEvent::drained()));
        }
    }
}

fn end_reply(end: RecvEnd) -> ReadReply {
    match end {
        RecvEnd::Finished => Ok(None),
        RecvEnd::Reset(code) => Err(ReadError::Reset(code)),
    }
}

/// Answers a command after the connection closed.
fn reply_closed(cmd: ConnCmd, error: &ConnectionError) {
    match cmd {
        ConnCmd::OpenBi { reply } | ConnCmd::AcceptBi { reply } => {
            let _ = reply.send(Err(error.clone()));
        }
        ConnCmd::Write { reply, .. } => {
            let _ = reply.send(Err(WriteError::ConnectionLost(error.clone())));
        }
        ConnCmd::Read { reply, .. } => {
            let _ = reply.send(Err(ReadError::ConnectionLost(error.clone())));
        }
        ConnCmd::Finish { .. }
        | ConnCmd::Unclaimed { .. }
        | ConnCmd::Reset { .. }
        | ConnCmd::Stop { .. }
        | ConnCmd::DropSend { .. }
        | ConnCmd::DropRecv { .. }
        | ConnCmd::Close { .. }
        | ConnCmd::Stats { .. }
        | ConnCmd::SetLimits { .. }
        | ConnCmd::SetPriority { .. } => {}
    }
}

/// Exports the growth since `previous` as counter increments (OBS-1, OBS-12).
fn export_delta(previous: &Exported, next: &Exported) {
    metrics::counter!("zakura.quic.packets.lost")
        .increment(next.lost_packets.saturating_sub(previous.lost_packets));
    metrics::counter!("zakura.quic.congestion_events").increment(
        next.congestion_events
            .saturating_sub(previous.congestion_events),
    );
    metrics::counter!("zakura.quic.bytes.sent")
        .increment(next.bytes_sent.saturating_sub(previous.bytes_sent));
    metrics::counter!("zakura.quic.bytes.received")
        .increment(next.bytes_received.saturating_sub(previous.bytes_received));
    metrics::counter!("zakura.quic.socket.transmits")
        .increment(next.transmits.saturating_sub(previous.transmits));
    metrics::counter!("zakura.quic.socket.datagrams_sent")
        .increment(next.datagrams_sent.saturating_sub(previous.datagrams_sent));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{socket::TokioUdpIo, NodeSecretKey};
    use zakura_rt::sync::atomic::AtomicUsize;

    fn task() -> ConnTask<TokioUdpIo> {
        client_task().1
    }

    /// A task for a client connection that hasn't sent anything yet, and the
    /// client endpoint that routes its datagrams.
    fn client_task() -> (quinn_proto::Endpoint, ConnTask<TokioUdpIo>) {
        client_task_with(quinn_proto::TransportConfig::default())
    }

    /// [`client_task`] with `transport` on the client connection.
    fn client_task_with(
        transport: quinn_proto::TransportConfig,
    ) -> (quinn_proto::Endpoint, ConnTask<TokioUdpIo>) {
        let key = NodeSecretKey::from_bytes(&[1; 32]);
        let crypto = tls::TlsConfig::new(&key)
            .client_config(key.public(), &[b"test"])
            .unwrap();
        let mut endpoint = quinn_proto::Endpoint::new(Arc::default(), None, true, None);
        let admitted = "127.0.0.1:12345".parse().unwrap();
        let mut config = quinn_proto::ClientConfig::new(Arc::new(crypto));
        config.transport_config(Arc::new(transport));
        let (handle, conn) = endpoint
            .connect(now(), config, admitted, tls::UNSENT_SERVER_NAME)
            .unwrap();
        let shared = ConnShared::new(admitted, Arc::new(AtomicUsize::new(0)));
        let to_endpoint = mpsc::unbounded_channel().0;
        let udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        udp.set_nonblocking(true).unwrap();
        let socket = Arc::new(SocketCell::new(TokioUdpIo::new(udp, false).unwrap()));
        let probe = Arc::default();
        let hooks = Arc::default();
        let handshake = oneshot::channel().0;
        let limits = CurrentLimits {
            receive_window: 1024,
            send_window: 1024,
            max_bidi_streams: 16,
        };
        let task = ConnTask {
            handle,
            conn,
            shared,
            to_endpoint,
            socket,
            probe,
            hooks,
            admitted,
            handshake: Some(handshake),
            remote_id: None,
            error: None,
            local_close: None,
            drained_sent: false,
            path_ip: canonical_ip(admitted.ip()),
            limits,
            credit_held: 0,
            credit_released: 0,
            load_published: None,
            load_stale: false,
            last_send_warning: None,
            openers: VecDeque::new(),
            opened_spare: VecDeque::new(),
            acceptors: VecDeque::new(),
            accepted_spare: VecDeque::new(),
            sends: HashMap::new(),
            stopped: HashMap::new(),
            writable_handles: HashSet::new(),
            accepted_bidi: 0,
            reads: HashMap::new(),
            recv_ends: HashMap::new(),
            recv_payload: HashSet::new(),
            send_payload: HashSet::new(),
            slot: crate::endpoint::ConnectionSlots::new(1, 0, 1)
                .claim_dial()
                .unwrap(),
            send_buf: Vec::new(),
            blocked: None,
            more_to_send: false,
            counters: Counters::default(),
            exported: Exported::default(),
        };
        (endpoint, task)
    }

    /// A server endpoint that accepts the test client.
    fn server_endpoint() -> quinn_proto::Endpoint {
        server_endpoint_with(quinn_proto::TransportConfig::default())
    }

    /// [`server_endpoint`] with `transport` on accepted connections.
    fn server_endpoint_with(transport: quinn_proto::TransportConfig) -> quinn_proto::Endpoint {
        let key = NodeSecretKey::from_bytes(&[1; 32]);
        let crypto = tls::TlsConfig::new(&key)
            .server_config(vec![b"test".to_vec()])
            .unwrap();
        let mut config = quinn_proto::ServerConfig::with_crypto(Arc::new(crypto));
        config.transport_config(Arc::new(transport));
        quinn_proto::Endpoint::new(Arc::default(), Some(Arc::new(config)), true, None)
    }

    /// Moves datagrams between the task's connection and the server's in
    /// memory until neither has more to send, accepting the server side on
    /// the client's first Initial.
    fn pump(
        client: &mut quinn_proto::Endpoint,
        task: &mut ConnTask<TokioUdpIo>,
        server: &mut quinn_proto::Endpoint,
        accepted: &mut Option<(ConnectionHandle, quinn_proto::Connection)>,
    ) {
        let client_addr: SocketAddr = "127.0.0.1:54321".parse().unwrap();
        for _ in 0..64 {
            let mut moved = false;
            let mut buf = Vec::new();
            while let Some(transmit) = task.conn.poll_transmit(now(), 1, &mut buf) {
                moved = true;
                let data = bytes::BytesMut::from(&buf[..transmit.size]);
                let mut response = Vec::new();
                match server.handle(now(), client_addr, None, None, data, &mut response) {
                    Some(quinn_proto::DatagramEvent::NewConnection(incoming)) => {
                        *accepted =
                            Some(server.accept(incoming, now(), &mut response, None).unwrap());
                    }
                    Some(quinn_proto::DatagramEvent::ConnectionEvent(_, event)) => {
                        accepted.as_mut().unwrap().1.handle_event(event);
                    }
                    _ => {}
                }
                buf.clear();
            }
            if let Some((handle, conn)) = accepted.as_mut() {
                while let Some(event) = conn.poll_endpoint_events() {
                    if let Some(event) = server.handle_event(*handle, event) {
                        conn.handle_event(event);
                    }
                }
                while let Some(transmit) = conn.poll_transmit(now(), 1, &mut buf) {
                    moved = true;
                    let data = bytes::BytesMut::from(&buf[..transmit.size]);
                    let mut response = Vec::new();
                    if let Some(quinn_proto::DatagramEvent::ConnectionEvent(_, event)) =
                        client.handle(now(), task.admitted, None, None, data, &mut response)
                    {
                        task.conn.handle_event(event);
                    }
                    buf.clear();
                }
            }
            while let Some(event) = task.conn.poll_endpoint_events() {
                if let Some(event) = client.handle_event(task.handle, event) {
                    task.conn.handle_event(event);
                }
            }
            if !moved {
                return;
            }
        }
    }

    /// The connection and stream receive windows of a [`CreditPair`] client.
    const WINDOW: u64 = 64 * 1024;

    /// A connected client task with [`WINDOW`] as both receive windows, and a
    /// server peer that sends on one bidirectional stream as fast as the
    /// client's credit allows.
    struct CreditPair {
        client: quinn_proto::Endpoint,
        task: ConnTask<TokioUdpIo>,
        server: quinn_proto::Endpoint,
        accepted: Option<(ConnectionHandle, quinn_proto::Connection)>,
        stream: StreamId,
        /// Bytes the peer's send stream took, at least what reached the client.
        sent: u64,
    }

    impl CreditPair {
        fn connected() -> Self {
            let mut client_transport = quinn_proto::TransportConfig::default();
            client_transport
                .receive_window(VarInt::from_u64(WINDOW).unwrap())
                .stream_receive_window(VarInt::from_u64(WINDOW).unwrap());
            // A congestion window far above WINDOW, so flow control alone
            // limits the peer and every pump delivers what it sent.
            let mut server_transport = quinn_proto::TransportConfig::default();
            let mut cubic = quinn_proto::congestion::CubicConfig::default();
            cubic.initial_window(64 * WINDOW);
            server_transport.congestion_controller_factory(Arc::new(cubic));
            let (client, mut task) = client_task_with(client_transport);
            task.limits.receive_window = WINDOW;
            let mut pair = Self {
                client,
                task,
                server: server_endpoint_with(server_transport),
                accepted: None,
                stream: StreamId::new(quinn_proto::Side::Server, Dir::Bi, 0),
                sent: 0,
            };
            pair.pump();
            assert!(!pair.task.conn.is_handshaking());
            let opened = pair.peer().streams().open(Dir::Bi).unwrap();
            assert_eq!(opened, pair.stream);
            pair
        }

        fn peer(&mut self) -> &mut quinn_proto::Connection {
            &mut self.accepted.as_mut().unwrap().1
        }

        fn pump(&mut self) {
            pump(
                &mut self.client,
                &mut self.task,
                &mut self.server,
                &mut self.accepted,
            );
        }

        /// Writes until the peer's flow control blocks, delivers the bytes and
        /// returns how many the peer took.
        fn fill(&mut self) -> u64 {
            let stream = self.stream;
            let mut took = 0;
            while let Ok(written) = self.peer().send_stream(stream).write(&[7; 4096]) {
                // usize fits in u64 on supported targets.
                took += written as u64;
            }
            self.sent += took;
            self.pump();
            took
        }

        /// Reads up to `max` bytes straight from quinn-proto, bypassing the
        /// task, and delivers the credit the read frees.
        fn read_direct(&mut self, max: u64) -> u64 {
            let mut stream = self.task.conn.recv_stream(self.stream);
            let mut chunks = stream.read(true).unwrap();
            let mut read = 0;
            // usize holds WINDOW on supported targets.
            while let Ok(Some(chunk)) = chunks.next((max - read) as usize) {
                read += chunk.bytes.len() as u64;
                if read == max {
                    break;
                }
            }
            let _ = chunks.finalize();
            self.pump();
            read
        }
    }

    /// API-10 relies on quinn-proto 0.11 counting a shrink below granted
    /// credit as debt that reads repay before they grant the peer more, and
    /// granting a grow at once.
    #[tokio::test]
    async fn a_receive_window_shrink_is_debt_that_reads_repay() {
        let mut pair = CreditPair::connected();
        assert_eq!(pair.fill(), WINDOW);
        let _ = pair.task.conn.streams().accept(Dir::Bi).unwrap();

        // Hold a quarter: reading it repays the debt and grants nothing.
        let quarter = WINDOW / 4;
        pair.task
            .conn
            .set_receive_window(VarInt::from_u64(WINDOW - quarter).unwrap());
        assert_eq!(pair.read_direct(quarter), quarter);
        assert_eq!(pair.fill(), 0);

        // Releasing it grants the quarter back without a read.
        pair.task
            .conn
            .set_receive_window(VarInt::from_u64(WINDOW).unwrap());
        pair.pump();
        assert_eq!(pair.fill(), quarter);

        // Without a hold, a read grants its bytes.
        assert_eq!(pair.read_direct(quarter), quarter);
        assert_eq!(pair.fill(), quarter);
    }

    /// API-10: with a greedy peer and random holds, reads and releases, the
    /// bytes the peer sent minus the bytes whose credit came back never
    /// exceed the receive window, and every release returns its credit.
    #[tokio::test]
    async fn held_credit_bounds_a_greedy_peer() {
        for seed in 1..=8u64 {
            let mut pair = CreditPair::connected();
            pair.fill();
            let (reply, accepted) = oneshot::channel();
            pair.task.handle_cmd(ConnCmd::AcceptBi { reply });
            assert_eq!(accepted.await.unwrap().unwrap(), pair.stream);

            let mut rng = seed;
            let mut next = |bound: u64| {
                // xorshift64: a fixed sequence per seed.
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng % bound
            };
            let mut holds = Vec::new();
            let mut released = 0;
            // The unread bytes of the frame the latest hold covers.
            let mut frame_left = 0;
            let mut read = None;
            for _ in 0..400 {
                if read.is_none() && next(3) != 0 {
                    let (max, hold) = match (frame_left, next(2)) {
                        (0, 0) => (next(WINDOW / 4) + 1, 0),
                        (0, _) => {
                            let frame = next(WINDOW / 4) + 1;
                            holds.push(frame);
                            frame_left = frame;
                            (frame, frame)
                        }
                        (left, _) => (left, 0),
                    };
                    let (reply, result) = oneshot::channel();
                    pair.task.handle_cmd(ConnCmd::Read {
                        id: pair.stream,
                        // usize holds WINDOW on supported targets.
                        max: max as usize,
                        hold,
                        reply,
                    });
                    read = Some((result, frame_left > 0));
                } else if !holds.is_empty() {
                    // Any hold, the unfinished frame's included.
                    // usize holds the hold count on supported targets.
                    let hold = holds.swap_remove(next(holds.len() as u64) as usize);
                    pair.task.shared.release_credit(hold);
                    pair.task.update_credit(0);
                    released += hold;
                }
                pair.pump();
                pair.task.process_events();
                if let Some((result, in_frame)) = read.as_mut() {
                    if let Ok(reply) = result.try_recv() {
                        let chunks = reply.unwrap().unwrap();
                        // usize fits in u64 on supported targets.
                        let n: u64 = chunks.iter().map(|chunk| chunk.len() as u64).sum();
                        if *in_frame {
                            frame_left -= n;
                        } else {
                            released += n;
                        }
                        read = None;
                    }
                }
                pair.fill();
                assert!(
                    pair.sent.saturating_sub(released) <= WINDOW,
                    "seed {seed}: sent {} released {released}",
                    pair.sent
                );
            }

            // Every release returns its credit: with no hold left, reads
            // alone keep the peer sending.
            for hold in holds.drain(..) {
                pair.task.shared.release_credit(hold);
            }
            pair.task.update_credit(0);
            assert_eq!(pair.task.outstanding_credit(), 0);
            let before = pair.sent;
            pair.task.reads.clear();
            pair.read_direct(WINDOW);
            pair.fill();
            assert!(pair.sent > before, "seed {seed}");
        }
    }

    /// API-10: read-ahead can deliver a frame before its hold reaches the
    /// task, so a release can arrive first. The window still ends where it
    /// started, instead of losing the hold for good.
    #[tokio::test]
    async fn a_release_before_its_hold_leaves_no_credit_held() {
        let mut task = task();
        task.update_credit(100);
        task.shared.release_credit(40);
        task.update_credit(0);
        assert_eq!(task.outstanding_credit(), 60);
        // The second hold's release overtakes it.
        task.shared.release_credit(60 + 25);
        task.update_credit(0);
        assert_eq!(task.outstanding_credit(), 0);
        task.update_credit(25);
        assert_eq!(task.outstanding_credit(), 0);
    }

    /// ADM-13: closing stops every receive half that may buffer data, the
    /// streams the application never accepted included, and resets every
    /// send half that may hold unacknowledged data.
    #[tokio::test]
    async fn closing_releases_every_stream_that_may_hold_payload() {
        let (mut client, mut task) = client_task();
        let mut server = server_endpoint();
        let mut accepted = None;
        pump(&mut client, &mut task, &mut server, &mut accepted);
        assert!(!task.conn.is_handshaking());

        // The peer sends on a stream this node never accepts.
        let (_, peer) = accepted.as_mut().unwrap();
        let unaccepted = peer.streams().open(Dir::Bi).unwrap();
        peer.send_stream(unaccepted).write(&[1; 4096]).unwrap();
        pump(&mut client, &mut task, &mut server, &mut accepted);

        // This node writes data the peer never acknowledges.
        let (reply, opened) = oneshot::channel();
        task.handle_cmd(ConnCmd::OpenBi { reply });
        let unacked = opened.await.unwrap().unwrap();
        let (reply, _written) = oneshot::channel();
        task.handle_cmd(ConnCmd::Write {
            id: unacked,
            data: Bytes::from_static(&[2; 4096]),
            reply,
        });

        task.close(VarInt::from_u32(0), Bytes::from_static(b"test"));
        task.release_streams();
        assert!(task.recv_payload.is_empty());
        assert!(task.send_payload.is_empty());
        // Both were released already, so releasing them again fails.
        let code = VarInt::from_u32(0);
        assert!(task.conn.recv_stream(unaccepted).stop(code).is_err());
        assert!(task.conn.send_stream(unacked).reset(code).is_err());
    }

    #[tokio::test]
    async fn draining_datagrams_refunds_only_this_connections_socket_bytes() {
        let mut task = task();
        let key = NodeSecretKey::from_bytes(&[1; 32]);
        let crypto = tls::TlsConfig::new(&key)
            .server_config(vec![b"test".to_vec()])
            .unwrap();
        let config = quinn_proto::ServerConfig::with_crypto(Arc::new(crypto));
        let mut server =
            quinn_proto::Endpoint::new(Arc::default(), Some(Arc::new(config)), true, None);
        let remote = "127.0.0.1:54321".parse().unwrap();
        let mut packet = Vec::new();
        let transmit = task.conn.poll_transmit(now(), 1, &mut packet).unwrap();
        let data = bytes::BytesMut::from(&packet[..transmit.size]);
        let mut response = Vec::new();
        let Some(quinn_proto::DatagramEvent::NewConnection(incoming)) =
            server.handle(now(), remote, None, None, data.clone(), &mut response)
        else {
            panic!("client Initial must create an incoming connection")
        };
        let _connection = server.accept(incoming, now(), &mut response, None).unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let other_connection_bytes = 73;
        task.shared
            .socket_queued_bytes
            .store(other_connection_bytes, Ordering::Relaxed);
        for _ in 0..3 {
            let Some(quinn_proto::DatagramEvent::ConnectionEvent(_, event)) =
                server.handle(now(), remote, None, None, data.clone(), &mut response)
            else {
                panic!("duplicate Initial must route to the accepted connection")
            };
            task.shared
                .queued_datagram_bytes
                .fetch_add(data.len(), Ordering::Relaxed);
            task.shared
                .socket_queued_bytes
                .fetch_add(data.len(), Ordering::Relaxed);
            tx.send((event, data.len())).unwrap();
        }
        task.drain_datagrams(&mut rx).await;
        assert!(tx.is_closed());
        assert_eq!(task.shared.queued_datagram_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(
            task.shared.socket_queued_bytes.load(Ordering::Relaxed),
            other_connection_bytes
        );
    }

    /// ADM-13: entering the closing allowance drops and refunds the datagrams
    /// queued under the live limit, so the closed connection holds no more
    /// than its closing charge.
    #[tokio::test]
    async fn entering_closing_drops_the_live_datagram_backlog() {
        let (_client, mut task) = client_task();
        let mut server = server_endpoint();
        let remote = "127.0.0.1:54321".parse().unwrap();
        let mut packet = Vec::new();
        let transmit = task.conn.poll_transmit(now(), 1, &mut packet).unwrap();
        let data = bytes::BytesMut::from(&packet[..transmit.size]);
        let mut response = Vec::new();
        let Some(quinn_proto::DatagramEvent::NewConnection(incoming)) =
            server.handle(now(), remote, None, None, data.clone(), &mut response)
        else {
            panic!("client Initial must create an incoming connection")
        };
        let _connection = server.accept(incoming, now(), &mut response, None).unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let other_connection_bytes = 73;
        task.shared
            .socket_queued_bytes
            .store(other_connection_bytes, Ordering::Relaxed);
        // More than the closing limit, queued while the connection was live.
        let charged = super::super::CLOSING_QUEUE_BYTES;
        for _ in 0..3 {
            let Some(quinn_proto::DatagramEvent::ConnectionEvent(_, event)) =
                server.handle(now(), remote, None, None, data.clone(), &mut response)
            else {
                panic!("duplicate Initial must route to the accepted connection")
            };
            task.shared
                .queued_datagram_bytes
                .fetch_add(charged, Ordering::Relaxed);
            task.shared
                .socket_queued_bytes
                .fetch_add(charged, Ordering::Relaxed);
            tx.send((event, charged)).unwrap();
        }

        task.close(VarInt::from_u32(0), Bytes::from_static(b"test"));
        assert!(task.enter_closing(&mut rx));
        assert!(task.slot.is_closing());
        assert!(rx.try_recv().is_err());
        assert_eq!(task.shared.queued_datagram_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(
            task.shared.socket_queued_bytes.load(Ordering::Relaxed),
            other_connection_bytes
        );
    }

    #[tokio::test]
    async fn stopped_codes_live_only_until_send_handle_closes() {
        for close_kind in 0..3 {
            let mut task = task();
            let id = StreamId::new(quinn_proto::Side::Client, Dir::Bi, 0);
            let code = VarInt::from_u32(9);
            task.writable_handles.insert(id);
            task.on_stopped(id, code);
            let (reply, result) = oneshot::channel();
            task.handle_cmd(ConnCmd::Write {
                id,
                data: Bytes::from_static(b"x"),
                reply,
            });
            assert_eq!(result.await.unwrap(), Err(WriteError::Stopped(code)));
            let close = match close_kind {
                0 => ConnCmd::Finish { id },
                1 => ConnCmd::DropSend { id },
                _ => ConnCmd::Reset { id, code },
            };
            task.handle_cmd(close);
            assert!(!task.writable_handles.contains(&id));
            assert!(task.stopped.is_empty());
            task.on_stopped(id, code);
            assert!(
                task.stopped.is_empty(),
                "late STOP_SENDING must not retain a code"
            );
        }
    }
}
