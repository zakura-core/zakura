//! The connection task: the peer routine that owns one QUIC connection.

use std::{
    collections::{HashMap, VecDeque},
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
use tokio::sync::{mpsc, oneshot, watch};

use super::{ConnCmd, ConnShared, Handshake, ReadReply, READ_BATCH_BYTES};
use crate::{
    congestion::CongestionProbe,
    conn::{CongestionStats, ConnSample, ConnStats, DriverStats, Hooks, SAMPLE_INTERVAL},
    error::{ReadError, WriteError},
    key::NodeId,
    socket::SocketCell,
    sys::canonical_ip,
    tls,
};

/// Transmits per wakeup before the task yields to its inputs. Matches the
/// `quinn` crate's budget.
const MAX_TRANSMITS_PER_WAKEUP: usize = 20;
/// Inputs of one kind handled per wakeup before the task drives output.
const MAX_INPUTS_PER_WAKEUP: usize = 64;

/// The current time on tokio's clock, so paused-time tests drive quinn-proto.
pub(crate) fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}

/// Everything a connection task starts with.
pub(crate) struct ConnTaskParams {
    pub(crate) handle: ConnectionHandle,
    pub(crate) conn: quinn_proto::Connection,
    pub(crate) shared: Arc<ConnShared>,
    pub(crate) cmds: mpsc::UnboundedReceiver<ConnCmd>,
    pub(crate) datagrams: mpsc::Receiver<ConnectionEvent>,
    pub(crate) control: mpsc::UnboundedReceiver<ConnectionEvent>,
    pub(crate) to_endpoint: mpsc::UnboundedSender<(ConnectionHandle, EndpointEvent)>,
    pub(crate) socket: Arc<SocketCell>,
    pub(crate) closing: watch::Receiver<bool>,
    pub(crate) network: watch::Receiver<u64>,
    pub(crate) probe: Arc<CongestionProbe>,
    pub(crate) hooks: Arc<Hooks>,
    pub(crate) admitted: SocketAddr,
    pub(crate) handshake: oneshot::Sender<Result<Handshake, ConnectionError>>,
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

struct ConnTask {
    handle: ConnectionHandle,
    conn: quinn_proto::Connection,
    shared: Arc<ConnShared>,
    to_endpoint: mpsc::UnboundedSender<(ConnectionHandle, EndpointEvent)>,
    socket: Arc<SocketCell>,
    probe: Arc<CongestionProbe>,
    hooks: Arc<Hooks>,
    admitted: SocketAddr,
    handshake: Option<oneshot::Sender<Result<Handshake, ConnectionError>>>,

    remote_id: Option<NodeId>,
    error: Option<ConnectionError>,
    drained_sent: bool,
    sampled_ip: IpAddr,

    openers: VecDeque<oneshot::Sender<Result<StreamId, ConnectionError>>>,
    opened_spare: VecDeque<StreamId>,
    acceptors: VecDeque<oneshot::Sender<Result<StreamId, ConnectionError>>>,
    accepted_spare: VecDeque<StreamId>,
    sends: HashMap<StreamId, SendState>,
    reads: HashMap<StreamId, oneshot::Sender<ReadReply>>,
    recv_ends: HashMap<StreamId, RecvEnd>,

    send_buf: Vec<u8>,
    blocked: Option<(Transmit, Vec<u8>)>,
    more_to_send: bool,
    counters: Counters,
    exported: Exported,
}

/// Runs one connection until quinn-proto drains it.
pub(crate) async fn run(params: ConnTaskParams) {
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
    } = params;
    let mut socket_generation = socket.subscribe();
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
        drained_sent: false,
        sampled_ip: canonical_ip(admitted.ip()),
        openers: VecDeque::new(),
        opened_spare: VecDeque::new(),
        acceptors: VecDeque::new(),
        accepted_spare: VecDeque::new(),
        sends: HashMap::new(),
        reads: HashMap::new(),
        recv_ends: HashMap::new(),
        send_buf: Vec::new(),
        blocked: None,
        more_to_send: false,
        counters: Counters::default(),
        exported: Exported::default(),
    };

    let mut cmds_open = true;
    let mut timer = Box::pin(tokio::time::sleep(Duration::ZERO));
    let mut timer_deadline: Option<Instant> = None;
    let mut sampler = tokio::time::interval(SAMPLE_INTERVAL);
    sampler.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    sampler.tick().await;
    if *closing.borrow() {
        task.close(VarInt::from_u32(0), Bytes::new());
    }

    loop {
        task.drive();
        if task.conn.is_drained() {
            break;
        }
        match task.conn.poll_timeout() {
            Some(deadline) if timer_deadline != Some(deadline) => {
                timer
                    .as_mut()
                    .reset(tokio::time::Instant::from_std(deadline));
                timer_deadline = Some(deadline);
            }
            Some(_) => {}
            None => timer_deadline = None,
        }
        let blocked_io = task.blocked.as_ref().and_then(|_| task.socket.get());
        let more_to_send = task.more_to_send;

        tokio::select! {
            biased;
            event = control.recv() => match event {
                Some(event) => task.conn.handle_event(event),
                None => {
                    task.endpoint_gone();
                    break;
                }
            },
            event = datagrams.recv() => match event {
                Some(event) => {
                    task.conn.handle_event(event);
                    task.counters.datagrams_received += 1;
                    for _ in 1..MAX_INPUTS_PER_WAKEUP {
                        let Ok(event) = datagrams.try_recv() else { break };
                        task.conn.handle_event(event);
                        task.counters.datagrams_received += 1;
                    }
                }
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
                    task.close(VarInt::from_u32(0), Bytes::new());
                }
            },
            () = &mut timer, if timer_deadline.is_some() => {
                timer_deadline = None;
                task.conn.handle_timeout(now());
            }
            Ok(()) = closing.changed() => {
                if *closing.borrow() {
                    task.close(VarInt::from_u32(0), Bytes::new());
                }
            }
            Ok(()) = network.changed() => {
                // SOCK-12: forget the local address so the kernel picks one.
                task.conn.local_address_changed();
            }
            Ok(()) = socket_generation.changed() => {}
            _ = async { blocked_io.as_ref()?.writable().await.ok() }, if blocked_io.is_some() => {}
            _ = sampler.tick() => task.sample(),
            () = std::future::ready(()), if more_to_send => {}
        }
    }
}

impl ConnTask {
    /// Moves every pending output: app events, datagrams and endpoint events.
    fn drive(&mut self) {
        if self
            .conn
            .poll_timeout()
            .is_some_and(|deadline| deadline <= now())
        {
            // A timer can fire late under load; handle it on the clock.
            self.conn.handle_timeout(now());
        }
        self.process_events();
        self.transmit();
        self.process_events();
        while let Some(event) = self.conn.poll_endpoint_events() {
            self.drained_sent |= event.is_drained();
            let _ = self.to_endpoint.send((self.handle, event));
        }
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
        metrics::counter!("zakura.quic.socket.send_errors").increment(1);
        tracing::debug!(target: "zakura_quic", %error, "UDP send failed");
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
                Event::Stream(StreamEvent::Finished { .. }) => {}
                Event::Stream(StreamEvent::Stopped { id, error_code }) => {
                    self.fail_writes(id, WriteError::Stopped(error_code));
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
        for (_, reply) in self.reads.drain() {
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
            let sample = self.build_sample(Some(reason.to_string()));
            if let Some(observer) = self.hooks.observer() {
                observer(&sample);
            }
        }
    }

    fn close(&mut self, code: VarInt, reason: Bytes) {
        if self.error.is_none() {
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
            ConnCmd::Finish { id } | ConnCmd::DropSend { id } => match self.sends.get_mut(&id) {
                Some(state) if !state.writes.is_empty() => state.finish_after = true,
                _ => {
                    let _ = self.conn.send_stream(id).finish();
                }
            },
            ConnCmd::Reset { id, code } => {
                self.fail_writes(id, WriteError::ClosedStream);
                let _ = self.conn.send_stream(id).reset(code);
            }
            ConnCmd::Read { id, reply } => {
                self.reads.insert(id, reply);
                self.serve_reader(id);
            }
            ConnCmd::Stop { id, code } => self.stop(id, code),
            ConnCmd::DropRecv { id } => self.stop(id, VarInt::from_u32(0)),
            ConnCmd::Close { code, reason } => self.close(code, reason),
            ConnCmd::Stats { reply } => {
                let _ = reply.send(self.stats());
            }
        }
    }

    fn stop(&mut self, id: StreamId, code: VarInt) {
        self.reads.remove(&id);
        if self.recv_ends.remove(&id).is_none() {
            let _ = self.conn.recv_stream(id).stop(code);
        }
    }

    fn serve_openers(&mut self) {
        while let Some(reply) = self.openers.pop_front() {
            if reply.is_closed() {
                continue;
            }
            let Some(id) = self
                .opened_spare
                .pop_front()
                .or_else(|| self.conn.streams().open(Dir::Bi))
            else {
                self.openers.push_front(reply);
                return;
            };
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
            let Some(id) = self
                .accepted_spare
                .pop_front()
                .or_else(|| self.conn.streams().accept(Dir::Bi))
            else {
                self.acceptors.push_front(reply);
                return;
            };
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
        let Some(reply) = self.reads.remove(&id) else {
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
                let _ = reply.send(Err(ReadError::ClosedStream));
                return;
            }
        };
        let mut data = Vec::new();
        let mut budget = READ_BATCH_BYTES;
        let mut end = None;
        while budget > 0 {
            match chunks.next(budget) {
                Ok(Some(chunk)) => {
                    budget = budget.saturating_sub(chunk.bytes.len());
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
        // Reading frees receive window; the next drive() sends the credit.
        let _ = chunks.finalize();
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
                self.reads.insert(id, reply);
            }
        }
    }

    fn stats(&self) -> ConnStats {
        let metrics = self.conn.congestion_state().metrics();
        let queued: usize = self
            .sends
            .values()
            .flat_map(|state| state.writes.iter())
            .map(|write| write.data.len())
            .sum();
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

    /// The 10 s sample: PATH-3's ban check, then OBS-3.
    fn sample(&mut self) {
        if self.remote_id.is_none() || self.error.is_some() {
            return;
        }
        // PATH-3: quinn-proto emits no event when the peer migrates, so
        // compare the remote address with the last sample's.
        let ip = canonical_ip(self.conn.remote_address().ip());
        if ip != self.sampled_ip {
            self.sampled_ip = ip;
            if self.hooks.is_banned(ip) {
                metrics::counter!("zakura.quic.paths.closed_banned").increment(1);
                tracing::debug!(target: "zakura_quic", %ip, "closing a connection that migrated to a banned IP");
                self.close(VarInt::from_u32(0), Bytes::from_static(b"banned path"));
                return;
            }
        }
        let sample = self.build_sample(None);
        if let Some(observer) = self.hooks.observer() {
            observer(&sample);
        }
    }
}

impl Drop for ConnTask {
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
        | ConnCmd::Reset { .. }
        | ConnCmd::Stop { .. }
        | ConnCmd::DropSend { .. }
        | ConnCmd::DropRecv { .. }
        | ConnCmd::Close { .. }
        | ConnCmd::Stats { .. } => {}
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
