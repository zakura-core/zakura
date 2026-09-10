//! Set up and retire two persistent QUIC streams as one service session.
//!
//! A pair has a request stream and a data/control stream. In block sync's paired
//! layout, `GetBlocks` uses the request stream; `Status`, blocks, and ending
//! messages use the data/control stream. Each stream has its own `SendStream`
//! and `RecvStream`: those are the two directions of one stream.
//!
//! The service registry defines the roles and their allowed messages. Each role
//! has its own queues and worker. A blocked request reader or writer does not stop
//! the data/control worker from being polled. Both streams share cancellation and
//! are retired together.
//!
//! Setup protects each stream with `SetupIo`, retains its metadata in
//! `PreparedOrderedStream`, and starts both workers together once the pair is
//! complete. Incoming roles can arrive in either order; `PendingOrderedPairs`
//! matches them by the opener's pair ID under a bounded setup deadline.

use super::*;
use crate::zakura::OrderedStreamPair;

/// Own both directions of one QUIC stream until setup hands them to a new owner.
///
/// While the handles are present, dropping this guard resets sending and stops
/// receiving. This also cleans up failed or cancelled setup, including a partly
/// written prelude (the header identifying the stream's role and version).
/// `take` transfers ownership and leaves `None`, disabling this guard's cleanup.
pub(super) struct SetupIo(Option<(SendStream, RecvStream)>);

impl SetupIo {
    pub(super) fn new(send: SendStream, recv: RecvStream) -> Self {
        Self(Some((send, recv)))
    }

    /// Borrow the handles for setup I/O while retaining responsibility for cleanup.
    pub(super) fn streams(&mut self) -> (&mut SendStream, &mut RecvStream) {
        let (send, recv) = self
            .0
            .as_mut()
            .expect("setup owns both stream halves until handoff");
        (send, recv)
    }

    /// Hand both handles to the caller, which becomes responsible for their lifetime.
    pub(super) fn take(mut self) -> (SendStream, RecvStream) {
        self.0
            .take()
            .expect("setup owns both stream halves until handoff")
    }
}

impl Drop for SetupIo {
    fn drop(&mut self) {
        if let Some((send, recv)) = &mut self.0 {
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE));
            let _ = recv.stop(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE));
        }
    }
}

/// One stream's I/O, setup metadata, and resources, before its worker starts.
///
/// Retaining this value keeps its transport stream permit and any service
/// reservation charged, including while it waits for the other role. Dropping
/// it stops the stream through `SetupIo` and releases its resource ownership.
pub(super) struct PreparedOrderedStream {
    io: SetupIo,
    stream: Stream,
    prelude: StreamPrelude,
    context: StreamWorkerContext,
}

impl PreparedOrderedStream {
    /// Attach the pair's shared service reservation before starting its workers.
    pub(super) fn set_session_resources(
        &mut self,
        resources: Option<Arc<dyn crate::zakura::OrderedSessionResources>>,
    ) {
        self.context.session_resources = resources;
    }

    pub(super) fn new(
        send: SendStream,
        recv: RecvStream,
        stream: Stream,
        prelude: StreamPrelude,
        context: StreamWorkerContext,
    ) -> Self {
        Self {
            io: SetupIo(Some((send, recv))),
            stream,
            prelude,
            context,
        }
    }

    /// Start a standalone ordered service that does not declare a companion role.
    pub(super) fn spawn_single(
        self,
        workers: &mut JoinSet<()>,
        queue_depth: usize,
        opened_locally: bool,
        exits: mpsc::UnboundedSender<OrderedSessionExit>,
    ) -> AdmittedOrderedSession {
        let (send, recv) = self.io.take();
        spawn_persistent_stream_worker(
            workers,
            send,
            recv,
            self.stream,
            self.prelude,
            self.context,
            queue_depth,
            opened_locally,
            exits,
        )
    }
}

/// The first incoming role, held until its companion arrives or setup expires.
struct PendingPair {
    // Shared wire ID chosen by the opener, scoped to this connection and opener.
    id: u64,
    first: PreparedOrderedStream,
    // Starts with the first role; subsequent arrivals cannot extend it.
    deadline: Instant,
}

/// Match incoming request and data/control streams on this connection.
///
/// Retain at most one incomplete offer per negotiated pair. Keying by the data
/// role's kind, rather than the peer's chosen pair ID, prevents changing IDs from
/// creating extra pending offers. Each waiting stream owns its transport permit
/// and shares the service reservation that will move into the running pair.
#[derive(Default)]
pub(super) struct PendingOrderedPairs {
    pairs: HashMap<u16, PendingPair>,
    // One entry per negotiated pair, so repeated failures cannot grow this map.
    retry_after: HashMap<u16, Instant>,
}

impl PendingOrderedPairs {
    /// Reserve service capacity for the first role, or share the waiting role's
    /// reservation. A service that limits session slots charges the pair once;
    /// each role also holds its own transport stream permit. Recently expired
    /// offers must wait out their cooldown before reserving again. `insert`
    /// checks the pair ID and role.
    pub(super) fn reserve_or_share(
        &self,
        pair: OrderedStreamPair,
        registry: &ServiceRegistry,
        direction: ServicePeerDirection,
    ) -> Result<
        Option<Arc<dyn crate::zakura::OrderedSessionResources>>,
        crate::zakura::OrderedSessionFull,
    > {
        if self
            .retry_after
            .get(&pair.data.kind)
            .is_some_and(|retry| *retry > Instant::now())
        {
            return Err(crate::zakura::OrderedSessionFull);
        }
        match self.pairs.get(&pair.data.kind) {
            Some(pending) => Ok(pending.first.context.session_resources.clone()),
            None => registry
                .service_for_kind(pair.data.kind)
                .expect("a selected pair has an owning service")
                .reserve_ordered_session(direction),
        }
    }

    /// Earliest setup deadline, used to wake the connection loop even if no more
    /// stream bytes arrive from the peer.
    pub(super) fn deadline(&self) -> Option<Instant> {
        self.pairs.values().map(|pair| pair.deadline).min()
    }

    /// Drop expired offers, stopping their streams and releasing their ownership.
    /// A short retry delay gives other connections a chance to use that capacity.
    pub(super) fn expire(&mut self, now: Instant) {
        self.retry_after.retain(|_, retry| *retry > now);
        for (kind, pair) in self.pairs.extract_if(|_, pair| pair.deadline <= now) {
            // Give waiting connections a setup interval to claim the released
            // capacity before this peer can reserve another incomplete pair.
            self.retry_after
                .insert(kind, now + pair.first.context.limits.prelude_timeout);
        }
    }

    /// Retain the first role with `Ok(None)`, or return a complete pair as
    /// `Ok(Some((data, requests)))`, regardless of which role arrived first.
    ///
    /// Both roles must match the negotiated pair, carry the same nonzero wire ID,
    /// and arrive before the first role's deadline. Two copies of one role cannot
    /// form a pair. The connection handler validates negotiation before calling.
    pub(super) fn insert(
        &mut self,
        pair: OrderedStreamPair,
        id: u64,
        incoming: PreparedOrderedStream,
    ) -> Result<Option<(PreparedOrderedStream, PreparedOrderedStream)>, ZakuraHandlerError> {
        if id == 0 || (incoming.stream != pair.data && incoming.stream != pair.requests) {
            return Err(ZakuraHandlerError::InvalidOrderedPair);
        }
        // Taking the first role out also makes it drop if matching the second
        // role fails below, so a rejected offer cannot keep its old stream alive.
        let Some(pending) = self.pairs.remove(&pair.data.kind) else {
            let deadline = Instant::now() + incoming.context.limits.prelude_timeout;
            self.pairs.insert(
                pair.data.kind,
                PendingPair {
                    id,
                    first: incoming,
                    deadline,
                },
            );
            return Ok(None);
        };
        if pending.id != id
            || pending.first.stream == incoming.stream
            || Instant::now() >= pending.deadline
        {
            return Err(ZakuraHandlerError::InvalidOrderedPair);
        }
        if incoming.stream == pair.data {
            Ok(Some((incoming, pending.first)))
        } else {
            Ok(Some((pending.first, incoming)))
        }
    }
}

/// Start a complete pair with separate queues and independently polled workers.
///
/// Request writes may wait for serving capacity while data writes retain a
/// deadline. Both workers share cancellation: when either ends, the other must
/// stop too. Publish one session exit only after both workers have finished, so
/// the connection handler observes the pair's lifetime as a single unit.
pub(super) fn spawn_ordered_pair(
    workers: &mut JoinSet<()>,
    mut data: PreparedOrderedStream,
    mut requests: PreparedOrderedStream,
    queue_depth: usize,
    opened_locally: bool,
    exits: mpsc::UnboundedSender<OrderedSessionExit>,
) -> AdmittedOrderedSession {
    // Tell the service setup is complete. The workers and application senders
    // keep the shared service slot owned through teardown.
    if let Some(resources) = &data.context.session_resources {
        resources.admitted();
    }
    let cancel = data.context.connection_token.child_token();
    // Let the service distinguish peer closure from locally initiated cancellation.
    let remote_close = CancellationToken::new();
    data.context.stream_token = cancel.clone();
    requests.context.stream_token = cancel.clone();
    let (inbound_depth, outbound_depth) =
        bounded_stream_queue_depths(queue_depth, data.context.queue_depths);
    let (data_tx, data_rx) = mpsc::channel(inbound_depth);
    let (data_send, data_out) = worker_framed_channel(outbound_depth);
    // One raw queued request plus one reader-held frame. The service owns its
    // decoded waiting request separately from these transport bounds.
    let (request_tx, request_rx) = mpsc::channel(1);
    let (request_send, request_out) = worker_framed_channel(1);
    // Expose data/control as the primary stream and requests as its companion.
    // The local data-stream ID identifies this session in lifecycle events;
    // the wire pair ID was only needed to match the two roles during setup.
    let admitted = AdmittedOrderedSession {
        kind: data.stream.kind,
        version: data.stream.version,
        session_id: data.context.stream_id,
        recv: FramedRecv::new(data_rx).with_remote_close(remote_close.clone()),
        send: data_send.with_session_resources(data.context.session_resources.clone()),
        cancel_token: cancel.clone(),
        companion: Some(ServiceStreamRole {
            kind: requests.stream.kind,
            version: requests.stream.version,
            recv: FramedRecv::new(request_rx).with_remote_close(remote_close.clone()),
            send: request_send.with_session_resources(requests.context.session_resources.clone()),
        }),
    };
    let exit = OrderedSessionExit {
        stream: data.stream,
        session_id: admitted.session_id,
        opened_locally,
    };
    workers.spawn(async move {
        // Aborting this supervising task must cancel the pair as well.
        let _cancel_on_exit = cancel.clone().drop_guard();
        let (data_send, data_recv) = data.io.take();
        let (request_send, request_recv) = requests.io.take();
        // Poll both workers concurrently. Awaiting one before starting the other
        // would make request backpressure obstruct data/control traffic again.
        tokio::join!(
            async {
                persistent_stream_worker_with_policy(
                    data_send,
                    data_recv,
                    data.prelude,
                    data.context,
                    data_tx,
                    data_out,
                    inbound_depth,
                    OrderedWritePolicy::PairData,
                    Some(remote_close.clone()),
                )
                .await;
                cancel.cancel();
            },
            async {
                persistent_stream_worker_with_policy(
                    request_send,
                    request_recv,
                    requests.prelude,
                    requests.context,
                    request_tx,
                    request_out,
                    1,
                    OrderedWritePolicy::PairRequests,
                    Some(remote_close.clone()),
                )
                .await;
                cancel.cancel();
            },
        );
        let _ = exits.send(exit);
    });
    admitted
}

impl ZakuraProtocolHandler {
    /// Complete local setup of one outbound stream without starting its worker.
    ///
    /// Reserve a transport stream slot, open a bidirectional stream, and write its
    /// prelude under bounded waits. For a pair, the caller supplies the same
    /// nonzero `pair_id` for both roles and attaches their shared service resources
    /// afterward. The returned value retains the stream slot until handoff or drop.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn prepare_ordered_stream(
        &self,
        connection: &Connection,
        stream: Stream,
        pair_id: Option<u64>,
        stream_sem: &Arc<Semaphore>,
        message_buckets: &mut MessageRateBuckets,
        limits: ZakuraConnectionLimits,
        connection_token: CancellationToken,
        close_cause: CloseCause,
        freshness_tx: watch::Sender<Instant>,
        conn: ZakuraConnTrace,
        peer_id: ZakuraPeerId,
    ) -> Result<PreparedOrderedStream, ZakuraHandlerError> {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        let permit = stream_sem
            .clone()
            .try_acquire_owned()
            .map_err(|_| ZakuraHandlerError::ResourceLimit("ordered stream permit"))?;
        let io = timeout(OUTBOUND_STREAM_WRITE_TIMEOUT, connection.open_bi())
            .await
            .map_err(|_| ZakuraHandlerError::Timeout("open ordered service stream"))??;
        let mut io = SetupIo(Some(io));
        let prelude = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind: stream.kind,
            stream_version: stream.version,
            request_id: None,
            max_frame_bytes: inbound_frame_cap_for_stream(&limits, stream),
        };
        let mut bytes = prelude.encode()?;
        // The pair ID follows the ordinary prelude. It matches persistent roles;
        // it is separate from the prelude's per-request `request_id` field.
        if let Some(id) = pair_id {
            bytes.extend_from_slice(&id.to_le_bytes());
        }
        timeout(
            OUTBOUND_STREAM_WRITE_TIMEOUT,
            io.0.as_mut()
                .expect("setup retains its stream halves")
                .0
                .write_all(&bytes),
        )
        .await
        .map_err(|_| ZakuraHandlerError::Timeout("ordered stream prelude write"))??;
        // Both roles spend the same message-rate budget, so splitting a service
        // across two streams does not double its allowance.
        let bucket_kind = self
            .registry
            .ordered_stream_pair(stream)
            .map_or(stream.kind, |pair| pair.data.kind);
        let message_bucket = message_bucket_for(
            message_buckets,
            bucket_kind,
            limits.message_rate_per_second,
            RealClock,
        );
        let context = StreamWorkerContext {
            conn: conn.clone(),
            peer_id,
            stream_id,
            _permit: permit,
            limits,
            inbound_frame_cap: prelude.max_frame_bytes,
            message_payload_limits: self.registry.message_payload_limits(stream),
            message_types: self.registry.message_types(stream),
            queue_depths: self.registry.stream_queue_depths(stream),
            session_resources: None,
            outbound_frame_cap: application_frame_cap(&limits, stream),
            message_bucket,
            stream_token: connection_token.child_token(),
            connection_token,
            close_cause,
            freshness_tx,
        };
        metrics::counter!("zakura.p2p.stream.accepted", "stream_kind" => stream_kind_label(stream.kind)).increment(1);
        conn.trace_stream("accepted", stream_id, Some(stream_kind_label(stream.kind)));
        Ok(PreparedOrderedStream {
            io,
            stream,
            prelude,
            context,
        })
    }
}

#[cfg(test)]
mod tests;
