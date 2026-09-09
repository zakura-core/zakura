//! Connection-scoped setup and teardown for persistent stream pairs.

use super::*;
use crate::zakura::OrderedStreamPair;

/// Own raw stream halves until setup succeeds. Failed or cancelled setup must
/// stop both directions rather than leave a partially written prelude alive.
struct SetupIo(Option<(SendStream, RecvStream)>);

impl SetupIo {
    fn take(mut self) -> (SendStream, RecvStream) {
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

pub(super) struct PreparedOrderedStream {
    io: SetupIo,
    stream: Stream,
    prelude: StreamPrelude,
    context: StreamWorkerContext,
}

impl PreparedOrderedStream {
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

struct PendingPair {
    id: u64,
    first: PreparedOrderedStream,
    deadline: Instant,
}

/// At most one incomplete offer per negotiated pair on this connection.
/// Each retained half also owns a transport stream permit until it is stopped.
#[derive(Default)]
pub(super) struct PendingOrderedPairs {
    pairs: HashMap<u16, PendingPair>,
}

impl PendingOrderedPairs {
    pub(super) fn deadline(&self) -> Option<Instant> {
        self.pairs.values().map(|pair| pair.deadline).min()
    }

    pub(super) fn expire(&mut self, now: Instant) {
        self.pairs.retain(|_, pair| pair.deadline > now);
    }

    pub(super) fn insert(
        &mut self,
        pair: OrderedStreamPair,
        id: u64,
        incoming: PreparedOrderedStream,
    ) -> Result<Option<(PreparedOrderedStream, PreparedOrderedStream)>, ZakuraHandlerError> {
        if id == 0 || (incoming.stream != pair.data && incoming.stream != pair.requests) {
            return Err(ZakuraHandlerError::InvalidOrderedPair);
        }
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

/// Workers share cancellation and publish one exit only after both have ended.
pub(super) fn spawn_ordered_pair(
    workers: &mut JoinSet<()>,
    mut data: PreparedOrderedStream,
    mut requests: PreparedOrderedStream,
    queue_depth: usize,
    opened_locally: bool,
    exits: mpsc::UnboundedSender<OrderedSessionExit>,
) -> AdmittedOrderedSession {
    let cancel = data.context.connection_token.child_token();
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
    let admitted = AdmittedOrderedSession {
        kind: data.stream.kind,
        version: data.stream.version,
        session_id: data.context.stream_id,
        recv: FramedRecv::new(data_rx),
        send: data_send,
        cancel_token: cancel.clone(),
        companion: Some(ServiceStreamRole {
            kind: requests.stream.kind,
            version: requests.stream.version,
            recv: FramedRecv::new(request_rx),
            send: request_send,
        }),
    };
    let exit = OrderedSessionExit {
        stream: data.stream,
        session_id: admitted.session_id,
        opened_locally,
    };
    workers.spawn(async move {
        let _cancel_on_exit = cancel.clone().drop_guard();
        let (data_send, data_recv) = data.io.take();
        let (request_send, request_recv) = requests.io.take();
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
            queue_depths: self.registry.stream_queue_depths(stream),
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
