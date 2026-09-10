//! Set up every required persistent stream before admitting a service session.
//!
//! Each member has independent queues and workers. The session shares a wire
//! identifier, admission reservation, message budget, and cancellation scope.
//! Single-stream sessions retain their existing prelude without a wire identifier.

use super::*;
use crate::zakura::transport::SessionLayout;

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
/// reservation charged, including while it waits for the remaining members. Dropping
/// it stops the stream through `SetupIo` and releases its resource ownership.
pub(super) struct PreparedStream {
    io: SetupIo,
    stream: Stream,
    prelude: StreamPrelude,
    context: StreamWorkerContext,
}

impl PreparedStream {
    /// Attach the session's shared service reservation before starting its workers.
    pub(super) fn set_session_resources(
        &mut self,
        resources: Option<Arc<dyn crate::zakura::SessionResources>>,
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
}

/// Incomplete setup retains every arrived stream under the first arrival's deadline.
struct PendingSession {
    id: u64,
    streams: Vec<PreparedStream>,
    deadline: Instant,
}

/// At most one incomplete offer per service session on this connection.
#[derive(Default)]
pub(super) struct PendingSessions {
    sessions: HashMap<u16, PendingSession>,
    retry_after: HashMap<u16, Instant>,
}

impl PendingSessions {
    /// Charge the service once, then share its reservation across all members.
    pub(super) fn reserve_or_share(
        &self,
        layout: &SessionLayout,
        registry: &ServiceRegistry,
        direction: ServicePeerDirection,
    ) -> Result<Option<Arc<dyn crate::zakura::SessionResources>>, crate::zakura::SessionFull> {
        let kind = layout.primary().kind;
        if self
            .retry_after
            .get(&kind)
            .is_some_and(|retry| *retry > Instant::now())
        {
            return Err(crate::zakura::SessionFull);
        }
        match self.sessions.get(&kind) {
            Some(pending) => Ok(pending.streams[0].context.session_resources.clone()),
            None => registry
                .service_for_kind(kind)
                .expect("a selected session has an owning service")
                .reserve_session(direction),
        }
    }

    pub(super) fn deadline(&self) -> Option<Instant> {
        self.sessions.values().map(|session| session.deadline).min()
    }

    /// Expiry releases every arrived member and briefly defers another offer.
    pub(super) fn expire(&mut self, now: Instant) {
        self.retry_after.retain(|_, retry| *retry > now);
        for (kind, session) in self
            .sessions
            .extract_if(|_, session| session.deadline <= now)
        {
            self.retry_after.insert(
                kind,
                now + session.streams[0].context.limits.prelude_timeout,
            );
        }
    }

    /// Admit the complete layout in kind order, regardless of arrival order.
    pub(super) fn insert(
        &mut self,
        layout: &SessionLayout,
        id: u64,
        incoming: PreparedStream,
    ) -> Result<Option<Vec<PreparedStream>>, ZakuraHandlerError> {
        let kind = layout.primary().kind;
        // Remove first so every invalid continuation releases the existing offer.
        let pending = self.sessions.remove(&kind);
        if (layout.is_multi_stream() && id == 0) || !layout.streams.contains(&incoming.stream) {
            return Err(ZakuraHandlerError::InvalidServiceSession);
        }
        let mut pending = pending.unwrap_or_else(|| PendingSession {
            id,
            streams: Vec::with_capacity(layout.streams.len()),
            deadline: Instant::now() + incoming.context.limits.prelude_timeout,
        });
        if pending.id != id
            || Instant::now() >= pending.deadline
            || pending.streams.iter().any(|s| {
                s.stream.kind == incoming.stream.kind || !layout.streams.contains(&s.stream)
            })
        {
            return Err(ZakuraHandlerError::InvalidServiceSession);
        }
        pending.streams.push(incoming);
        if pending.streams.len() == layout.streams.len() {
            pending.streams.sort_unstable_by_key(|s| s.stream.kind);
            Ok(Some(pending.streams))
        } else {
            self.sessions.insert(kind, pending);
            Ok(None)
        }
    }
}

/// Run all members independently and report exit only after every worker finishes.
pub(super) fn spawn_service_session(
    workers: &mut JoinSet<()>,
    streams: Vec<PreparedStream>,
    queue_depth: usize,
    opened_locally: bool,
    exits: mpsc::UnboundedSender<SessionExit>,
) -> AdmittedSession {
    let primary = streams
        .first()
        .expect("a validated session has at least one stream");
    if let Some(resources) = &primary.context.session_resources {
        resources.admitted();
    }
    let cancel = primary.context.connection_token.child_token();
    let remote_close = CancellationToken::new();
    let mut admitted = AdmittedSession {
        kind: primary.stream.kind,
        session_id: primary.context.stream_id,
        cancel_token: cancel.clone(),
        streams: Vec::with_capacity(streams.len()),
    };
    let exit = SessionExit {
        stream: primary.stream,
        session_id: admitted.session_id,
        opened_locally,
    };
    let mut running = futures::stream::FuturesUnordered::new();
    for mut prepared in streams {
        prepared.context.stream_token = cancel.clone();
        let (inbound_depth, outbound_depth) =
            bounded_stream_queue_depths(queue_depth, prepared.context.queue_depths);
        let (inbound_tx, inbound_rx) = mpsc::channel(inbound_depth);
        let (sender, outbound_rx) = worker_framed_channel(outbound_depth);
        admitted.streams.push(ServiceStreamRole {
            kind: prepared.stream.kind,
            version: prepared.stream.version,
            recv: FramedRecv::new(inbound_rx).with_remote_close(remote_close.clone()),
            send: sender.with_session_resources(prepared.context.session_resources.clone()),
        });
        let remote_close = remote_close.clone();
        running.push(async move {
            let (send, recv) = prepared.io.take();
            persistent_stream_worker_with_policy(
                send,
                recv,
                prepared.prelude,
                prepared.context,
                inbound_tx,
                outbound_rx,
                inbound_depth,
                Some(remote_close),
            )
            .await;
        });
    }
    workers.spawn(async move {
        let _cancel_on_exit = cancel.clone().drop_guard();
        while running.next().await.is_some() {
            cancel.cancel();
        }
        let _ = exits.send(exit);
    });
    admitted
}

impl ZakuraProtocolHandler {
    /// Complete local setup of one outbound stream without starting its worker.
    ///
    /// Reserve a transport stream slot, open a bidirectional stream, and write its
    /// prelude under bounded waits. For a multi-stream session, the caller supplies the same
    /// nonzero `session_id` for every member and attaches their shared service resources
    /// afterward. The returned value retains the stream slot until handoff or drop.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn prepare_ordered_stream(
        &self,
        connection: &Connection,
        stream: Stream,
        session_id: Option<u64>,
        stream_sem: &Arc<Semaphore>,
        message_buckets: &mut MessageRateBuckets,
        limits: ZakuraConnectionLimits,
        connection_token: CancellationToken,
        close_cause: CloseCause,
        freshness_tx: watch::Sender<Instant>,
        conn: ZakuraConnTrace,
        peer_id: ZakuraPeerId,
    ) -> Result<PreparedStream, ZakuraHandlerError> {
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
        // The session ID follows the ordinary prelude. It matches persistent roles;
        // it is separate from the prelude's per-request `request_id` field.
        if let Some(id) = session_id {
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
        // All members spend the same message-rate budget, so splitting a service
        // across several streams does not double its allowance.
        let bucket_kind = self
            .registry
            .session_layout(stream)
            .map_or(stream.kind, |layout| layout.primary().kind);
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
            write_policy: self.registry.stream_write_policy(stream),
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
        Ok(PreparedStream {
            io,
            stream,
            prelude,
            context,
        })
    }
}

#[cfg(test)]
mod tests;
