//! A generic service for one layout that uses only the toolkit.
//!
//! It reserves sessions through `SessionCapacity` and keeps them in a
//! `SessionTable`. It serves each request row through `Serve`, answering with
//! the adapter's messages. It downloads through fenced `Reservations`, with
//! the reader precheck attached to every member that carries responses. It
//! publishes each subscription row through `Publications`, pushing pages
//! with the capacity `ServeCapacity::push` grants.

use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, PoisonError,
    },
    time::Duration,
};

use futures::{stream::SelectAll, StreamExt};
use tokio::sync::{watch, Notify};
use tokio_util::sync::CancellationToken;

use super::{
    LayoutPlan, RequestPlan, StreamConformance, SubscriptionPlan, SubscriptionUpdate, UpdateOp,
};
use crate::{
    zakura::{
        regulation::{
            sizing, Applied, Current, Produce, Publications, Push, Replacement, ReservationPool,
            Reservations, Responded, ResponseCap, ResponseSink, Serve, ServeCapacity, ServeEnd,
            SessionCapacity, SessionKey, SessionTable, SharedReservations, SinkError, SinkProgress,
            SubscriptionLimits, WorkLease, WriterFence,
        },
        wire_codec::{decode_frame, encode_frame, WireMessage},
        CloseCause, Frame, FramedRecv, FramedSend, MessageRole, Peer, Service,
        ServicePeerDirection, ServicePeerLimits, SessionDemand, SessionFull, SessionOpening,
        SessionPolicy, SessionResources, Stream, ZakuraConnId, ZakuraPeerId, FRAME_HEADER_BYTES,
    },
    BoxError,
};

/// A reservation's key: the request row and the exchange.
pub(crate) type ExchangeKey = (u16, u32);

/// Serves one request row with the adapter's messages: one part, if the row
/// has a part row, then one ending.
struct EchoProduce<A> {
    plan: RequestPlan,
    _adapter: PhantomData<fn() -> A>,
}

impl<A: StreamConformance> Produce for EchoProduce<A> {
    type Request = u32;
    type Message = A::Message;

    fn response_cap(&self, _exchange: &u32) -> ResponseCap {
        self.plan.cap()
    }

    async fn produce(
        &self,
        exchange: &u32,
        _lease: WorkLease,
        mut sink: ResponseSink<A::Message>,
    ) -> Result<Responded, ServeEnd> {
        let fault = |error: SinkError| ServeEnd::LocalFault(error.to_string());
        if let Some(part) = self.plan.part {
            sink.send(&A::message(part, *exchange)).map_err(fault)?;
        }
        sink.finish(&A::message(self.plan.end, *exchange))
            .map_err(fault)
    }

    fn local_failure(&self, exchange: &u32, _sent: SinkProgress) -> A::Message {
        A::message(self.plan.end, *exchange)
    }
}

/// One admitted session of the layout.
#[derive(Debug)]
pub(crate) struct LayoutSession<A: StreamConformance> {
    pub(crate) key: SessionKey,
    pub(crate) peer: ZakuraPeerId,
    pub(crate) sends: Vec<FramedSend>,
    pub(crate) reservations: SharedReservations<ExchangeKey>,
    serve: Vec<Serve<EchoProduce<A>>>,
    /// The peer's subscriptions, per subscription row in the plan's order.
    publishers: Vec<Publisher>,
    pub(crate) fence: WriterFence,
    /// The session's token: cancelling it retires every member.
    pub(crate) cancel: CancellationToken,
    /// The connection's token.
    pub(crate) connection: CancellationToken,
    pub(crate) close_cause: CloseCause,
    /// Every exchange this session ended, in order.
    pub(crate) ended: watch::Sender<Vec<ExchangeKey>>,
}

/// One subscription row's publications on a session, shared with the task
/// that pushes its pages.
#[derive(Clone, Debug)]
struct Publisher {
    publications: Arc<Mutex<Publications<u32, u32>>>,
    wake: Arc<Notify>,
}

impl Publisher {
    fn lock(&self) -> std::sync::MutexGuard<'_, Publications<u32, u32>> {
        self.publications
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn apply(&self, update: SubscriptionUpdate) -> Result<(), String> {
        let SubscriptionUpdate {
            op,
            key,
            sequence,
            acknowledged,
            added,
        } = update;
        let mut publications = self.lock();
        let applied = match op {
            UpdateOp::Open => publications
                .open(key, sequence, added, acknowledged)
                .map(|()| Applied::Live),
            UpdateOp::Grant => publications.grant(&key, sequence, &acknowledged, added),
            UpdateOp::Close => publications.close(&key, sequence, &acknowledged),
        }
        .map_err(|fault| fault.to_string())?;
        if applied == Applied::Live {
            self.wake.notify_one();
        }
        Ok(())
    }
}

impl<A: StreamConformance> std::fmt::Debug for EchoProduce<A> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EchoProduce")
            .field("plan", &self.plan)
            .finish()
    }
}

impl<A: StreamConformance> LayoutSession<A> {
    /// Open requests on the serving session of `request_type`.
    pub(crate) fn serving_open(&self, shared: &LayoutShared<A>, request_type: u16) -> u32 {
        let index = shared.request_index(request_type);
        self.serve[index].open()
    }

    /// Handle one inbound frame. An error is a violation that disconnects
    /// the peer.
    fn handle(&self, shared: &LayoutShared<A>, frame: &Frame) -> Result<(), String> {
        let (row, _) = shared
            .plan
            .row(frame.message_type)
            .ok_or_else(|| format!("no row for message type {}", frame.message_type))?;
        shared.handled.fetch_add(1, Ordering::Relaxed);
        let len = frame.payload.len();
        match row.role {
            MessageRole::Request { .. } => {
                let message = decode::<A>(frame)?;
                let index = shared.request_index(row.message_type);
                self.serve[index]
                    .admit(A::exchange(&message))
                    .map_err(|violation| violation.to_string())
            }
            MessageRole::Response {
                request,
                ends_exchange,
            } => {
                // The reader ran the same check before it read the payload.
                self.reservations
                    .lock()
                    .precheck(frame.message_type, len)
                    .map_err(|refused| format!("{refused:?}"))?;
                let message = decode::<A>(frame)?;
                let key = (request, A::exchange(&message));
                let mut reservations = self.reservations.lock();
                if ends_exchange {
                    reservations
                        .claim_end(&key, frame.message_type, len)
                        .map_err(|refused| refused.to_string())?;
                    drop(reservations);
                    self.ended.send_modify(|ended| ended.push(key));
                } else {
                    reservations
                        .claim_frame(&key, frame.message_type, len)
                        .map_err(|refused| refused.to_string())?;
                }
                Ok(())
            }
            MessageRole::Subscription { .. } => {
                let message = decode::<A>(frame)?;
                let update = A::read_update(&message)
                    .ok_or_else(|| format!("row {} carried no update", row.message_type))?;
                let index = shared.subscription_index(row.message_type);
                self.publishers[index].apply(update)
            }
            MessageRole::Announcement { .. } => decode::<A>(frame).map(drop),
        }
    }
}

fn decode<A: StreamConformance>(frame: &Frame) -> Result<A::Message, String> {
    decode_frame::<A::Message>(frame).map_err(|error| error.to_string())
}

/// State the service shares with its session tasks.
#[derive(Debug)]
pub(crate) struct LayoutShared<A: StreamConformance> {
    pub(crate) plan: LayoutPlan,
    capacity: SessionCapacity,
    pub(crate) table: SessionTable<Arc<LayoutSession<A>>>,
    /// Serving capacity per request row, in the plan's order.
    pub(crate) serving: Vec<ServeCapacity>,
    /// Page capacity per subscription row, in the plan's order.
    pub(crate) pushing: Vec<ServeCapacity>,
    /// Pages this node pushed.
    pub(crate) pushed: AtomicU64,
    /// Pages that waited for capacity.
    pub(crate) push_waits: AtomicU64,
    /// Subscriptions this node ended, their outcomes queued.
    pub(crate) published_ended: AtomicU64,
    pub(crate) pool: ReservationPool,
    /// Frames that reached this node's handler.
    pub(crate) handled: AtomicU64,
    /// Violations this node disconnected a peer for.
    pub(crate) violations: Mutex<Vec<String>>,
    /// While true, session tasks stop reading.
    pub(crate) paused: watch::Sender<bool>,
}

impl<A: StreamConformance> LayoutShared<A> {
    fn request_index(&self, request_type: u16) -> usize {
        self.plan
            .requests
            .iter()
            .position(|plan| plan.row.message_type == request_type)
            .expect("the plan lists every request row of the layout")
    }

    fn subscription_index(&self, subscription_type: u16) -> usize {
        self.plan
            .subscriptions
            .iter()
            .position(|plan| plan.row.message_type == subscription_type)
            .expect("the plan lists every subscription row of the layout")
    }

    /// The violations this node disconnected a peer for.
    pub(crate) fn violations(&self) -> Vec<String> {
        self.violations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// A service that declares one layout and uses the toolkit for it.
#[derive(Debug)]
pub(crate) struct LayoutService<A: StreamConformance> {
    pub(crate) shared: Arc<LayoutShared<A>>,
}

impl<A: StreamConformance> LayoutService<A> {
    /// A service for `layout`, with capacity derived from the throughput
    /// target.
    pub(crate) fn new(layout: &'static [Stream]) -> Self {
        let plan = LayoutPlan::new(layout);
        let serving = plan
            .requests
            .iter()
            .map(|request| {
                let largest = request.cap().output_bytes();
                let limits = sizing::serve_limits(largest, Duration::from_millis(1));
                ServeCapacity::new("stream_conformance", request.row, limits)
                    .expect("derived limits are valid")
            })
            .collect();
        let pushing = plan
            .subscriptions
            .iter()
            .map(|subscription| {
                // Widening usize to u64 is lossless on supported targets.
                let largest = (subscription.page.payload.max() + FRAME_HEADER_BYTES) as u64;
                let limits = sizing::serve_limits(largest, Duration::from_millis(1));
                ServeCapacity::new("stream_conformance", subscription.row, limits)
                    .expect("derived limits are valid")
            })
            .collect();
        let smallest = plan
            .requests
            .iter()
            .map(|request| request.cap().bytes.max(1))
            .min()
            .unwrap_or(1);
        let pool = ReservationPool::new(sizing::reservation_entries(smallest))
            .expect("derived entries are valid");
        let limits = ServicePeerLimits {
            max_inbound_peers: 4,
            max_outbound_peers: 4,
            max_pending_escalations: 4,
            ..ServicePeerLimits::default()
        };
        Self {
            shared: Arc::new(LayoutShared {
                plan,
                capacity: SessionCapacity::new("stream_conformance", &limits),
                table: SessionTable::default(),
                serving,
                pushing,
                pushed: AtomicU64::new(0),
                push_waits: AtomicU64::new(0),
                published_ended: AtomicU64::new(0),
                pool,
                handled: AtomicU64::new(0),
                violations: Mutex::new(Vec::new()),
                paused: watch::channel(false).0,
            }),
        }
    }

    /// Reserve exchange `exchange` of `request_type` with `peer`, then write
    /// its request under the session's fence.
    pub(crate) async fn request(
        &self,
        peer: &ZakuraPeerId,
        request_type: u16,
        exchange: u32,
    ) -> Result<(), BoxError> {
        let shared = &self.shared;
        let (_, session) = shared.table.get(peer).ok_or("the peer has no session")?;
        let plan = shared.plan.requests[shared.request_index(request_type)];
        let entry = shared.pool.entry().await;
        let opened = session.fence.open().ok_or("the session's fence retired")?;
        let writer = opened.writer();
        let key = (request_type, exchange);
        session
            .reservations
            .lock()
            .reserve_fenced(key, request_type, plan.cap(), entry, opened)?;
        let frame =
            encode_frame(&A::message(plan.row, exchange)).map_err(|error| error.to_string())?;
        if let Err(error) = session.sends[plan.stream].send_fenced(frame, &writer).await {
            session.reservations.lock().retract(&key);
            return Err(error.into());
        }
        Ok(())
    }
}

impl<A: StreamConformance> Service for LayoutService<A> {
    fn name(&self) -> &'static str {
        "stream_conformance"
    }

    fn streams(&self) -> &[Stream] {
        self.shared.plan.layout
    }

    fn reserve_session(
        &self,
        direction: ServicePeerDirection,
    ) -> Result<Option<Arc<dyn SessionResources>>, SessionFull> {
        self.shared.capacity.reserve(direction).map(Some)
    }

    fn session_policy(&self) -> SessionPolicy {
        SessionPolicy {
            opening: SessionOpening::EitherSide,
            reopen: true,
        }
    }

    fn session_demand(
        &self,
        _conn_id: ZakuraConnId,
        _peer: &ZakuraPeerId,
        _negotiated: u64,
        direction: ServicePeerDirection,
    ) -> SessionDemand {
        self.shared.capacity.demand(direction)
    }

    fn reserved_session_demand(
        &self,
        _conn_id: ZakuraConnId,
        _peer: &ZakuraPeerId,
        _negotiated: u64,
        _direction: ServicePeerDirection,
    ) -> SessionDemand {
        SessionDemand::OpenNow
    }

    fn add_peer(&self, mut peer: Peer) {
        let shared = self.shared.clone();
        let cancel = peer.service_cancel_token();
        let mut session_id = 0;
        let mut recvs = Vec::new();
        let mut sends = Vec::new();
        for member in shared.plan.layout {
            let Some((id, recv, send)) = peer.take_stream_with_session_id(member.kind) else {
                cancel.cancel();
                return;
            };
            session_id = id;
            recvs.push(recv);
            sends.push(send);
        }
        let window = shared
            .plan
            .requests
            .iter()
            .map(|plan| usize::try_from(plan.max_in_flight).expect("a small limit fits usize"))
            .sum();
        let reservations = SharedReservations::new(Reservations::new(A::Message::RULES, window));
        for &(_, stream) in &shared.plan.responses {
            // Several response rows may share a member; the first attach wins.
            recvs[stream].attach_precheck(Arc::new(reservations.clone()));
        }
        let serve = shared
            .plan
            .requests
            .iter()
            .zip(&shared.serving)
            .map(|(plan, capacity)| {
                capacity.session(
                    Arc::new(EchoProduce {
                        plan: *plan,
                        _adapter: PhantomData,
                    }),
                    &peer.id,
                    plan.max_in_flight,
                    sends[plan.response_stream].clone(),
                    cancel.clone(),
                )
            })
            .collect();
        let publishers = shared
            .plan
            .subscriptions
            .iter()
            .zip(&shared.pushing)
            .map(|(plan, capacity)| {
                let limits = SubscriptionLimits::from_rule(plan.row)
                    .expect("the plan lists subscription rows");
                let publisher = Publisher {
                    publications: Arc::new(Mutex::new(Publications::new(limits))),
                    wake: Arc::new(Notify::new()),
                };
                tokio::spawn(publish(
                    shared.clone(),
                    *plan,
                    publisher.clone(),
                    capacity.push(&peer.id),
                    sends[plan.response_stream].clone(),
                    cancel.clone(),
                ));
                publisher
            })
            .collect();
        let key = SessionKey {
            conn_id: peer.conn_id,
            session_id,
        };
        let fence = WriterFence::new(peer.cancel_token(), peer.close_cause());
        let session = Arc::new(LayoutSession {
            key,
            peer: peer.id.clone(),
            sends,
            reservations,
            serve,
            publishers,
            fence: fence.clone(),
            cancel: cancel.clone(),
            connection: peer.cancel_token(),
            close_cause: peer.close_cause(),
            ended: watch::channel(Vec::new()).0,
        });
        let current = Current {
            key,
            cancel,
            fence,
            session: session.clone(),
        };
        if matches!(
            shared.table.replace(peer.id.clone(), current),
            Replacement::Refused
        ) {
            return;
        }
        tokio::spawn(run_session(shared, session, recvs));
    }

    fn remove_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
        if let Some((key, _)) = self.shared.table.get(peer) {
            if key.conn_id == conn_id {
                self.shared.table.remove(peer, key);
            }
        }
    }
}

/// Push pages of every live subscription while credit lasts, and end each
/// subscription after its `Close`.
///
/// One task writes a row's pages and endings, so each ending follows its
/// subscription's pages. A page waits for the capacity a served response
/// takes; an update wakes the task, so `Close` never waits behind it.
async fn publish<A: StreamConformance>(
    shared: Arc<LayoutShared<A>>,
    plan: SubscriptionPlan,
    publisher: Publisher,
    push: Push,
    send: FramedSend,
    cancel: CancellationToken,
) {
    loop {
        let (ended, page) = {
            let mut publications = publisher.lock();
            let mut closing: Vec<u32> = publications
                .keys()
                .copied()
                .filter(|key| publications.is_closing(key))
                .collect();
            closing.sort_unstable();
            let ended: Vec<_> = closing
                .into_iter()
                .filter_map(|key| Some((key, publications.end(&key)?)))
                .collect();
            let mut keys: Vec<u32> = publications.keys().copied().collect();
            keys.sort_unstable();
            let page = keys.into_iter().find_map(|key| {
                let cursor = publications.last_sent(&key)?.checked_add(1)?;
                let frame = encode_frame(&A::page(plan.page, key, cursor)).ok()?;
                let unspent = publications.unspent(&key)?;
                // Widening usize to u64 is lossless on supported targets.
                (unspent.objects >= 1 && unspent.bytes >= frame.payload.len() as u64)
                    .then_some((key, cursor, frame))
            });
            (ended, page)
        };
        for (key, terminal) in ended {
            let Ok(frame) = encode_frame(&A::message(plan.end, key)) else {
                cancel.cancel();
                return;
            };
            if !terminal.send(&send, frame).await {
                return;
            }
            shared.published_ended.fetch_add(1, Ordering::Relaxed);
        }
        let Some((key, cursor, frame)) = page else {
            tokio::select! {
                () = cancel.cancelled() => return,
                () = publisher.wake.notified() => continue,
            }
        };
        let len = frame.payload.len();
        let acquire = push.acquire(len);
        tokio::pin!(acquire);
        let permit = match futures::FutureExt::now_or_never(&mut acquire) {
            Some(permit) => permit,
            None => {
                shared.push_waits.fetch_add(1, Ordering::Relaxed);
                tokio::select! {
                    () = cancel.cancelled() => return,
                    () = publisher.wake.notified() => continue,
                    permit = acquire => permit,
                }
            }
        };
        // A stall means the subscription changed while the page waited.
        if publisher.lock().reserve_page(&key, 1, len, cursor).is_ok() {
            if !permit.send(&send, frame).await {
                return;
            }
            shared.pushed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Read every member until the session ends, then remove the session.
async fn run_session<A: StreamConformance>(
    shared: Arc<LayoutShared<A>>,
    session: Arc<LayoutSession<A>>,
    recvs: Vec<FramedRecv>,
) {
    let mut inbound: SelectAll<_> = futures::stream::select_all(recvs.into_iter().map(|recv| {
        futures::stream::unfold(recv, |mut recv| async move {
            recv.recv().await.map(|frame| (frame, recv))
        })
        .boxed()
    }));
    let mut paused = shared.paused.subscribe();
    loop {
        tokio::select! {
            biased;
            _ = session.cancel.cancelled() => break,
            _ = paused.wait_for(|paused| !*paused) => {}
        }
        let frame = tokio::select! {
            biased;
            _ = session.cancel.cancelled() => break,
            frame = inbound.next() => frame,
        };
        let Some(frame) = frame else { break };
        if let Err(violation) = session.handle(&shared, &frame) {
            shared
                .violations
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(violation);
            session.close_cause.record("service_protocol_reject");
            session.connection.cancel();
            break;
        }
    }
    shared.table.remove(&session.peer, session.key);
}
