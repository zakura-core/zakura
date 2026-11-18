//! Test nodes for `tx-relay`'s cluster tests: a real v2 node with the real
//! `tx-relay` service over a stand-in for the pool core, and the real legacy
//! adapter over an inbound stub the test reads.
//!
//! The stand-in core runs on the real pool port. It grants reservations,
//! admits or rejects every body it is handed as the test scripts, and
//! publishes what it holds.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Weak,
    },
    task::{Context, Poll},
};

use tokio::sync::{mpsc, Notify};
use tower::Service;
use zakura_chain::{
    amount::Amount,
    block,
    transaction::{Transaction, UnminedTxId, VerifiedUnminedTx},
};
use zakura_node_services::mempool::{
    port::{
        pool_port, ChainTip, Change, ChangeSeq, PoolCommand, PoolHandle, PoolInbox, PoolSnapshot,
        ReservationId, Unreserved, Verdict,
    },
    TransactionDependencies,
};
use zakura_rt::sync::{oneshot, CancellationToken, Mutex};

use super::{tx_relay_service, MayCool, TxRelaySessions};
use crate::{
    zakura::{
        testkit::{ZakuraTestNode, ZakuraTestNodeBuilder},
        LegacyGossipSink, TransportObserver,
    },
    BoxError, Request, Response,
};

/// The stand-in core's state: what it holds, how it answers, and what it was
/// handed. Tests change it between steps.
pub(super) struct Held {
    pub(super) active: bool,
    pub(super) transactions: Vec<VerifiedUnminedTx>,
    /// The bodies submitted to it, in order.
    pub(super) received: Vec<UnminedTxId>,
    /// A weak handle on each decoded body submitted, in order, so a test
    /// can count who else still holds it.
    pub(super) bodies: Vec<Weak<Transaction>>,
    /// The answer to every reserve. `None` grants it.
    pub(super) refuse: Option<Unreserved>,
    /// The verdict for every body. Admitted ones join the pool.
    pub(super) verdict: Verdict,
    /// Changes waiting for the next publication.
    pub(super) pending: Vec<Change>,
    /// Holds every verdict, and the body's slot, until a change clears it:
    /// a pool slow to verify. The body is still taken and counted in
    /// `received` at once.
    pub(super) slow: bool,
}

impl Default for Held {
    fn default() -> Self {
        Self {
            active: true,
            transactions: Vec::new(),
            received: Vec::new(),
            bodies: Vec::new(),
            refuse: None,
            verdict: Verdict::Admitted,
            pending: Vec::new(),
            slow: false,
        }
    }
}

/// A pool port with a stand-in core on it, the core's state, and the signal
/// that makes it publish its state again.
pub(super) struct TestPool {
    pub(super) handle: PoolHandle,
    pub(super) held: Arc<Mutex<Held>>,
    pub(super) republish: Arc<Notify>,
    /// Attaches a core started by [`Self::detached`].
    attach: Arc<Notify>,
}

impl TestPool {
    /// A port whose stand-in core starts with `held`.
    pub(super) fn new(held: Held) -> Self {
        let pool = Self::detached(held);
        pool.attach();
        pool
    }

    /// A port whose stand-in core starts with `held` only once
    /// [`Self::attach`] is called. Until then the port has no snapshot and
    /// its commands wait, as while the pool is still attaching.
    pub(super) fn detached(held: Held) -> Self {
        let (handle, inbox) = pool_port();
        let held = Arc::new(Mutex::new(held));
        let republish = Arc::new(Notify::new());
        let attach = Arc::new(Notify::new());
        tokio::spawn(fake_core(
            inbox,
            held.clone(),
            republish.clone(),
            attach.clone(),
        ));
        Self {
            handle,
            held,
            republish,
            attach,
        }
    }

    /// Lets a detached core start. A second call does nothing.
    pub(super) fn attach(&self) {
        self.attach.notify_one();
    }

    /// Changes the core's state with `change`, and publishes it after
    /// `changes`.
    pub(super) fn change(&self, change: impl FnOnce(&mut Held), changes: Vec<Change>) {
        change(&mut self.held.lock().expect("the test never poisons the lock"));
        self.held
            .lock()
            .expect("the test never poisons the lock")
            .pending
            .extend(changes);
        self.republish.notify_one();
    }

    /// The bodies submitted so far.
    pub(super) fn received(&self) -> Vec<UnminedTxId> {
        self.held
            .lock()
            .expect("the test never poisons the lock")
            .received
            .clone()
    }
}

/// A verdict a slow core holds: the reply, the verdict, and the body's
/// reservation and transaction.
type Parked = (
    oneshot::Sender<Verdict>,
    Verdict,
    zakura_node_services::mempool::port::Reservation,
    zakura_chain::transaction::UnminedTx,
);

/// Runs a stand-in core on `inbox` until every handle is gone, once
/// `attach` fires. It publishes again whenever `republish` fires.
async fn fake_core(
    mut inbox: PoolInbox,
    held: Arc<Mutex<Held>>,
    republish: Arc<Notify>,
    attach: Arc<Notify>,
) {
    attach.notified().await;
    let mut next_id = 0;
    let mut parked: Vec<Parked> = Vec::new();
    publish(&inbox, &held, Vec::new());
    loop {
        let command = tokio::select! {
            command = inbox.next() => command,
            () = republish.notified() => {
                let (changes, slow) = {
                    let mut held = held.lock().expect("the test never poisons the lock");
                    (std::mem::take(&mut held.pending), held.slow)
                };
                publish(&inbox, &held, changes);
                if !slow {
                    for (reply, verdict, reservation, transaction) in parked.drain(..) {
                        decide(&inbox, &held, reply, verdict, reservation, transaction);
                    }
                }
                continue;
            }
        };
        let Some(command) = command else {
            return;
        };
        match command {
            PoolCommand::Reserve { tx_id, reply, .. } => {
                let refuse = held
                    .lock()
                    .expect("the test never poisons the lock")
                    .refuse
                    .clone();
                if let Some(refused) = refuse {
                    let _ = reply.send(Err(refused));
                    continue;
                }
                next_id += 1;
                let reservation =
                    inbox.grant(ReservationId(next_id), tx_id, CancellationToken::new());
                let _ = reply.send(Ok(reservation));
            }
            PoolCommand::Submit {
                reservation,
                transaction,
                reply,
            } => {
                let (verdict, slow) = {
                    let mut held = held.lock().expect("the test never poisons the lock");
                    held.received.push(transaction.id());
                    held.bodies.push(Arc::downgrade(transaction.transaction()));
                    (held.verdict.clone(), held.slow)
                };
                if slow {
                    parked.push((reply, verdict, reservation, transaction));
                } else {
                    decide(&inbox, &held, reply, verdict, reservation, transaction);
                }
            }
            _ => {}
        }
    }
}

/// Answers one submission with `verdict`, and publishes the body if it is
/// admitted.
fn decide(
    inbox: &PoolInbox,
    held: &Mutex<Held>,
    reply: oneshot::Sender<Verdict>,
    verdict: Verdict,
    reservation: zakura_node_services::mempool::port::Reservation,
    transaction: zakura_chain::transaction::UnminedTx,
) {
    let id = transaction.id();
    let admitted = matches!(verdict, Verdict::Admitted);
    if admitted {
        // The stand-in does not verify: it pays the body a fee that passes
        // the mempool's fee check for any body up to the protocol's size.
        let verified = VerifiedUnminedTx::new(
            transaction,
            Amount::try_from(1_000_000_000).expect("a valid test fee"),
            0,
            0,
            Arc::new(Vec::new()),
        )
        .expect("the test fee pays for any body up to the protocol's size");
        held.lock()
            .expect("the test never poisons the lock")
            .transactions
            .push(verified);
    }
    drop(reservation);
    let _ = reply.send(verdict);
    if admitted {
        publish(inbox, held, vec![Change::Added(id)]);
    }
}

/// Publishes what the stand-in holds, after `changes`.
fn publish(inbox: &PoolInbox, held: &Mutex<Held>, changes: Vec<Change>) {
    let (active, transactions) = {
        let held = held.lock().expect("the test never poisons the lock");
        (held.active, held.transactions.clone())
    };
    inbox.publish(changes, |last: ChangeSeq| {
        let mut snapshot = PoolSnapshot::new(
            ChainTip {
                hash: block::Hash([0; 32]),
                height: block::Height(0),
            },
            last,
            transactions,
            TransactionDependencies::default(),
        );
        snapshot.active = active;
        snapshot
    });
}

/// The legacy adapter's inbound stub: it records every request and answers
/// it with nothing.
#[derive(Clone)]
pub(super) struct InboundStub {
    requests: mpsc::UnboundedSender<Request>,
}

impl Service<Request> for InboundStub {
    type Response = Response;
    type Error = BoxError;
    type Future = std::future::Ready<Result<Response, BoxError>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let _ = self.requests.send(request);
        std::future::ready(Ok(Response::Nil))
    }
}

/// A test node with the legacy adapter and, if `pool` is given, `tx-relay`.
pub(super) struct RelayNode {
    pub(super) node: ZakuraTestNode,
    pub(super) pool: Option<TestPool>,
    pub(super) sessions: Option<TxRelaySessions>,
    /// Every request the legacy adapter handed its inbound service.
    pub(super) inbound: mpsc::UnboundedReceiver<Request>,
}

impl RelayNode {
    /// Spawns node `seed`. With `pool`, it runs `tx-relay` over it and
    /// supports its capability.
    pub(super) async fn spawn(seed: u64, pool: Option<TestPool>) -> Result<Self, BoxError> {
        Self::spawn_cooling(seed, pool, Arc::new(|_| true)).await
    }

    /// Spawns node `seed` as [`Self::spawn`] does, with `may_cool` as its
    /// check for whether a verdict may start a cooldown.
    pub(super) async fn spawn_cooling(
        seed: u64,
        pool: Option<TestPool>,
        may_cool: MayCool,
    ) -> Result<Self, BoxError> {
        Self::spawn_with(seed, pool, may_cool, |builder| builder).await
    }

    /// Spawns node `seed` as [`Self::spawn_cooling`] does, with `configure`
    /// applied to its builder first, for limits, capabilities or an
    /// observer.
    pub(super) async fn spawn_with(
        seed: u64,
        pool: Option<TestPool>,
        may_cool: MayCool,
        configure: impl FnOnce(ZakuraTestNodeBuilder) -> ZakuraTestNodeBuilder,
    ) -> Result<Self, BoxError> {
        let (requests, inbound) = mpsc::unbounded_channel();
        let mut builder =
            configure(ZakuraTestNode::builder(seed)).service_from_supervisor(move |supervisor| {
                Arc::new(LegacyGossipSink::spawn(
                    InboundStub { requests },
                    supervisor,
                ))
            });
        let mut sessions = None;
        if let Some(pool) = &pool {
            let relay = tx_relay_service(
                pool.handle.clone(),
                may_cool,
                &Default::default(),
                Default::default(),
            );
            sessions = relay.tx_relay_sessions.clone();
            builder = builder.custom_service(relay);
        }
        Ok(Self {
            node: builder.spawn().await?,
            pool,
            sessions,
            inbound,
        })
    }

    /// Spawns node `seed` with `node` as its legacy adapter's inbound
    /// service, `tx-relay` over `pool` if one is given, `observer` seeing
    /// its transport if one is given, and `configure` applied to its builder
    /// first, for limits. Its `pool` and `inbound` are empty: the test holds
    /// the pool and reads `node` itself.
    pub(super) async fn spawn_loaded<Node>(
        seed: u64,
        pool: Option<PoolHandle>,
        node: Node,
        observer: Option<Arc<dyn TransportObserver>>,
        configure: impl FnOnce(ZakuraTestNodeBuilder) -> ZakuraTestNodeBuilder,
    ) -> Result<Self, BoxError>
    where
        Node: Service<Request, Response = Response, Error = BoxError> + Send + Clone + 'static,
        Node::Future: Send + 'static,
    {
        let mut builder =
            configure(ZakuraTestNode::builder(seed)).service_from_supervisor(move |supervisor| {
                Arc::new(LegacyGossipSink::spawn(node, supervisor))
            });
        if let Some(observer) = observer {
            builder = builder.transport_observer(observer);
        }
        let mut sessions = None;
        if let Some(pool) = pool {
            let relay = tx_relay_service(
                pool,
                Arc::new(|_| true),
                &Default::default(),
                Default::default(),
            );
            sessions = relay.tx_relay_sessions.clone();
            builder = builder.custom_service(relay);
        }
        Ok(Self {
            node: builder.spawn().await?,
            pool: None,
            sessions,
            inbound: mpsc::unbounded_channel().1,
        })
    }
}

/// A metrics recorder that keeps every counter it is handed, by name and
/// labels, and drops gauges and histograms.
///
/// `metrics` reads the recorder of the thread a metric is recorded on, so a
/// test installs this on the threads of the node it measures and no other,
/// with `metrics::set_default_local_recorder` on a single-threaded test.
#[derive(Clone, Debug, Default)]
pub(super) struct Counters(Arc<Mutex<HashMap<metrics::Key, Arc<AtomicU64>>>>);

impl Counters {
    /// The total of every `name` counter whose labels include each of
    /// `labels`.
    pub(super) fn get(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        self.0
            .lock()
            .expect("the test never poisons the lock")
            .iter()
            .filter(|(key, _)| {
                key.name() == name
                    && labels.iter().all(|(label, value)| {
                        key.labels()
                            .any(|have| have.key() == *label && have.value() == *value)
                    })
            })
            .map(|(_, count)| count.load(Ordering::Relaxed))
            .sum()
    }
}

impl metrics::Recorder for Counters {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        let count = self
            .0
            .lock()
            .expect("the test never poisons the lock")
            .entry(key.clone())
            .or_default()
            .clone();
        metrics::Counter::from_arc(count)
    }

    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }

    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}
