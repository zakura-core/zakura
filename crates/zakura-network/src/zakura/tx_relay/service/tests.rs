//! Serving: a peer's requests are bounded by our `Hello`, and a body is
//! built only when the writer reaches it, from the pool as it is then.

use std::time::Duration;

use zakura_chain::{
    block,
    parameters::Network,
    serialization::ZcashSerialize,
    transaction::{UnminedTxId, VerifiedUnminedTx},
};
use zakura_node_services::mempool::{
    port::{
        pool_port, ChainTip, Change, Charge, PoolCommand, PoolInbox, PoolSnapshot, Rejection,
        RejectionScope, RelayedFetch, RemovalReason,
    },
    TransactionDependencies,
};

use super::*;
use crate::zakura::tx_relay::ZakuraTxRelayConfig;
use crate::zakura::{framed_channel, transport::CloseCause};

/// Distinct Mainnet transactions, each at most the protocol's size.
fn transactions(count: usize) -> Vec<VerifiedUnminedTx> {
    let mut seen = std::collections::HashSet::new();
    Network::Mainnet
        .unmined_transactions_in_blocks(..)
        .filter(|tx| seen.insert(tx.transaction.id()))
        .take(count)
        .collect()
}

/// Publishes an active pool of `transactions` after `changes`.
fn publish(inbox: &PoolInbox, changes: Vec<Change>, transactions: Vec<VerifiedUnminedTx>) {
    inbox.publish(changes, |last| {
        PoolSnapshot::new(
            ChainTip {
                hash: block::Hash([0; 32]),
                height: block::Height(0),
            },
            last,
            transactions,
            TransactionDependencies::default(),
        )
    });
}

/// A closer whose cause the test reads.
fn closer() -> (Closer, CloseCause, CancellationToken) {
    let cause = CloseCause::new();
    let cancel = CancellationToken::new();
    (
        Closer {
            conn_cancel: cancel.clone(),
            close_cause: cause.clone(),
        },
        cause,
        cancel,
    )
}

/// The message in a frame a writer was given.
fn message(outgoing: Outgoing) -> TxRelayMessage {
    match outgoing {
        Outgoing::Frame(frame, _) => {
            decode_frame(&frame).expect("the routine encodes valid frames")
        }
        Outgoing::Serve { .. } => panic!("a serve job, not a frame"),
    }
}

/// A job that waits behind a peer that stopped reading, for an id the pool
/// evicts meanwhile, holds no body. When the writer reaches it, it answers
/// `NotFound` on the state stream. The writer reports each answer it
/// starts, which frees the request's place.
#[tokio::test]
async fn a_body_evicted_while_its_job_waits_is_answered_not_found() {
    let transaction = transactions(1).remove(0);
    let id = transaction.transaction.id();
    let (handle, inbox) = pool_port();
    publish(&inbox, vec![Change::Added(id)], vec![transaction]);

    let (send, mut recv) = framed_channel(1);
    let (state, mut answers) = mpsc::channel(STATE_QUEUE);
    let (reports, mut reports_rx) = mpsc::unbounded_channel();
    let bodies = Bodies {
        pool: handle.reader(),
        state,
        reports,
    };
    let writer = spawn_writer(send, DATA_QUEUE, CancellationToken::new(), Some(bodies));

    // While the pool holds it, the job gets the body.
    writer
        .send(Outgoing::Serve(id))
        .await
        .expect("the writer runs");
    let frame = recv.recv().await.expect("the writer sends the body");
    assert!(matches!(
        decode_frame::<TxRelayMessage>(&frame),
        Ok(TxRelayMessage::Tx { id: sent, .. }) if sent == id
    ));

    // The peer stops reading: one frame fills the transport queue and the
    // next holds the writer, so the job waits behind them.
    let filler = || {
        Outgoing::Frame(
            encode_frame(&TxRelayMessage::Have(vec![(id, 1)])).expect("a valid frame"),
            None,
        )
    };
    writer.send(filler()).await.expect("the writer runs");
    writer.send(filler()).await.expect("the writer runs");
    writer
        .send(Outgoing::Serve(id))
        .await
        .expect("the writer runs");

    // The pool evicts the id, then the peer reads again.
    publish(
        &inbox,
        vec![Change::Removed {
            id,
            reason: RemovalReason::Evicted,
        }],
        Vec::new(),
    );
    for _ in 0..2 {
        recv.recv().await.expect("the fillers go out");
    }
    let answer = tokio::time::timeout(Duration::from_secs(5), answers.recv())
        .await
        .expect("the writer answers")
        .expect("the state queue is open");
    assert_eq!(message(answer), TxRelayMessage::NotFound(id));
    assert!(
        tokio::time::timeout(Duration::from_millis(200), recv.recv())
            .await
            .is_err(),
        "no body goes out"
    );
    for _ in 0..2 {
        let report = reports_rx.try_recv().expect("each answer is reported");
        assert_eq!(report, WriterReport::Started(id));
    }
}

/// The connection every test session runs on.
fn connection() -> (ZakuraPeerId, ZakuraConnId) {
    (ZakuraPeerId::new(vec![1; 32]).expect("a valid id"), 1)
}

/// A routine's wiring on `pool`, with no reactor task: the test holds what
/// ended sessions hand off.
fn wiring(pool: PoolHandle) -> (Wiring, mpsc::UnboundedReceiver<Handoff>) {
    wiring_with(pool, super::super::test_memory_pool())
}

/// [`wiring`], whose fetches lease from `memory_pool`.
fn wiring_with(
    pool: PoolHandle,
    memory_pool: MemoryPool,
) -> (Wiring, mpsc::UnboundedReceiver<Handoff>) {
    let (handoff, handed) = mpsc::unbounded_channel();
    let node = Node {
        shared: Shared::new(RelayConfig {
            fan_out: ZakuraTxRelayConfig::default().fan_out(),
            short_id_key: 7,
        }),
        sessions: HashMap::default(),
    };
    let wiring = Wiring {
        node: Arc::new(Mutex::new(node)),
        pool,
        memory_pool,
        may_cool: Arc::new(|_| true),
        handoff,
        owned: Owned::default(),
        record: TxRelaySessions::default(),
        have_buckets: HaveBuckets::default(),
        base: Instant::now(),
        unconditional: UnconditionalPeers::default(),
        #[cfg(zakura_sim)]
        observer: None,
    };
    (wiring, handed)
}

/// The address of every test session's peer. No test lists it as
/// unconditional unless it says so.
fn peer_ip() -> IpAddr {
    IpAddr::from([192, 0, 2, 1])
}

/// One session's routine, driven step by step, whose writers' queues the
/// test holds. No writer runs, so no answer starts.
struct Session {
    routine: Routine,
    cause: CloseCause,
    cancel: CancellationToken,
    data: mpsc::Receiver<Outgoing>,
    /// Held unread, so the state writer's queue stays open.
    _state: mpsc::Receiver<Outgoing>,
    _reports: mpsc::UnboundedSender<WriterReport>,
}

impl Session {
    /// Opens a session of peer 1 on [`connection`] from [`peer_ip`], as
    /// `add_peer` does, and lets it read the pool.
    fn open(wiring: &Wiring) -> Self {
        Self::open_with_credit(wiring, None)
    }

    /// [`Self::open`] on a connection whose receive credit is `credit`.
    fn open_with_credit(wiring: &Wiring, credit: Option<ConnectionCredit>) -> Self {
        let (data, data_recv) = mpsc::channel(DATA_QUEUE);
        let (state, state_recv) = mpsc::channel(STATE_QUEUE);
        let (reports, reports_rx) = mpsc::unbounded_channel();
        let (close, cause, cancel) = closer();
        let have = MeteredSender::new(open_have_bucket(&wiring.have_buckets, &connection()));
        let io = SessionIo::new(data, state, close, reports_rx, have, connection(), credit);
        let mut routine = Routine::open(
            wiring.clone(),
            PeerKey(1),
            Some(peer_ip()),
            io,
            CancellationToken::new(),
            None,
        );
        routine.read_pool();
        routine.after_step();
        Self {
            routine,
            cause,
            cancel,
            data: data_recv,
            _state: state_recv,
            _reports: reports,
        }
    }

    /// Hands the peer's `message` to the routine, and runs what follows a
    /// step.
    async fn deliver(&mut self, message: TxRelayMessage) {
        self.routine.message(message).await;
        self.routine.after_step();
    }
}

/// A routine with a pool of `count` transactions, and one session on
/// connection 1 whose writers the test holds and never reads, as a peer
/// that stopped reading. The session's peer has sent its `Hello`, and every
/// id has gone out to it in one `Have`.
struct Serving {
    session: Session,
    wiring: Wiring,
    ids: Vec<UnminedTxId>,
    /// Held unread, so the core's side of the port stays open.
    _inbox: PoolInbox,
    _handed: mpsc::UnboundedReceiver<Handoff>,
}

impl Serving {
    async fn new(count: usize) -> Self {
        let pool = transactions(count);
        let ids: Vec<UnminedTxId> = pool.iter().map(|tx| tx.transaction.id()).collect();
        let (handle, inbox) = pool_port();
        publish(
            &inbox,
            ids.iter().map(|id| Change::Added(*id)).collect(),
            pool,
        );
        let (wiring, handed) = wiring(handle);
        let mut session = Session::open(&wiring);
        session
            .deliver(TxRelayMessage::Hello {
                serves: 16,
                max_tx_bytes: MAX_TX_BYTES,
            })
            .await;
        match message(
            session
                .data
                .try_recv()
                .expect("the session announces the pool"),
        ) {
            TxRelayMessage::Have(entries) => assert_eq!(entries.len(), ids.len()),
            other => panic!("expected a Have, got {other:?}"),
        }
        Self {
            session,
            wiring,
            ids,
            _inbox: inbox,
            _handed: handed,
        }
    }
}

/// A session that replaces another on the same connection, even before the
/// old one has ended, pays the same `Have` bucket.
#[tokio::test]
async fn sessions_on_one_connection_share_one_have_bucket() {
    let serving = Serving::new(1).await;
    let sessions = |wiring: &Wiring| {
        let buckets = wiring.have_buckets.lock().expect("never poisoned");
        assert_eq!(buckets.len(), 1);
        buckets[&connection()].sessions
    };
    let _second = Session::open(&serving.wiring);
    assert_eq!(sessions(&serving.wiring), 2);
    serving.session.routine.finish();
    assert_eq!(sessions(&serving.wiring), 1);
}

/// A peer may not have one id outstanding twice, including while its first
/// request's serve job waits on the writer.
#[tokio::test]
async fn an_id_requested_again_while_its_job_waits_closes_the_peer() {
    let mut serving = Serving::new(1).await;
    let id = serving.ids[0];
    let session = &mut serving.session;
    session.deliver(TxRelayMessage::GetTxs(id)).await;
    assert!(!session.cancel.is_cancelled());
    session.deliver(TxRelayMessage::GetTxs(id)).await;
    assert_eq!(
        session.cause.get().map(CloseOutcome::label),
        Some("tx-relay: id requested twice")
    );
}

/// Applies `session`'s pool calls as its routine does, up to and including
/// the first result that `last` picks.
async fn apply_calls(session: &mut Session, last: fn(&PoolResult) -> bool) {
    while let Some(result) = session.routine.calls.next().await {
        let done = last(&result);
        session.routine.pool_result(result);
        session.routine.after_step();
        if done {
            return;
        }
    }
    panic!("the routine ran out of calls");
}

/// A peer's address can join or leave `network.unconditional_peers` while
/// its session runs, as a pinned name's joins once a connection proves the
/// name's node ID. A body keeps the exemption its supplier had when it
/// arrived. One that arrived while the address was listed is never charged,
/// even if the address leaves while the body decodes, so the rejection that
/// bans an ordinary supplier leaves the peer connected. One that arrived
/// while the address was not listed is charged, even if the address joins.
#[test]
fn a_body_keeps_the_exemption_its_supplier_had_when_it_arrived() {
    // The runtime has one blocking thread, so holding it holds a decode.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .expect("a test runtime builds");
    runtime.block_on(async {
        for listed in [false, true] {
            let mut pool = transactions(2);
            let supplied = pool.pop().expect("two transactions").transaction;
            let (id, size) = (
                supplied.id(),
                u32::try_from(supplied.size()).expect("a test body is small"),
            );
            let (handle, mut inbox) = pool_port();
            publish(&inbox, vec![Change::Added(pool[0].transaction.id())], pool);
            let (mut wiring, _handed) = wiring(handle);
            let unconditional = toml::from_str::<crate::config::Config>(
                "unconditional_peers = ['a.example.com:8233']",
            )
            .expect("a valid config")
            .unconditional_peers;
            wiring.unconditional = unconditional.clone();
            let mut session = Session::open(&wiring);
            session
                .deliver(TxRelayMessage::Hello {
                    serves: 16,
                    max_tx_bytes: MAX_TX_BYTES,
                })
                .await;
            session
                .deliver(TxRelayMessage::Have(vec![(id, size)]))
                .await;

            // The core grants the fetch.
            let grant = async {
                let Some(PoolCommand::Reserve { tx_id, reply, .. }) = inbox.next().await else {
                    panic!("the session reserves the id it was offered");
                };
                let reservation = inbox.grant(ReservationId(1), tx_id, CancellationToken::new());
                let _ = reply.send(Ok(reservation));
            };
            let reserved = apply_calls(&mut session, |result| {
                matches!(result, PoolResult::Reserved { .. })
            });
            tokio::time::timeout(
                Duration::from_secs(5),
                futures::future::join(reserved, grant),
            )
            .await
            .expect("the reservation is granted");

            // The address joins the list after the session opened.
            if listed {
                unconditional.set_resolved_ip(0, Some(peer_ip()));
            }

            // The body arrives while the blocking thread is held, so the routine
            // takes it and waits on its decode.
            let (release, held) = std::sync::mpsc::channel::<()>();
            let hold = tokio::task::spawn_blocking(move || held.recv());
            let body = supplied
                .transaction()
                .zcash_serialize_to_vec()
                .expect("a valid transaction serializes");
            let mut delivery = Box::pin(session.deliver(TxRelayMessage::Tx { id, body }));
            assert!(
                futures::poll!(&mut delivery).is_pending(),
                "the body waits on its decode"
            );

            // While the body decodes, the address leaves the list if it was on
            // it and joins if it was not. It stays that way through the verdict.
            unconditional.set_resolved_ip(0, (!listed).then(peer_ip));
            drop(release);
            delivery.await;
            let _ = hold.await;

            // The core rejects the body with the charge that bans its supplier.
            let reject = async {
                let Some(PoolCommand::Submit { reply, .. }) = inbox.next().await else {
                    panic!("the session submits the body");
                };
                let _ = reply.send(PortVerdict::Rejected(Rejection {
                    scope: RejectionScope::Uncached,
                    charge: Charge::Ban,
                    tip: block::Height(0),
                    detail: Arc::new(std::io::Error::other("an invalid transaction")),
                }));
            };
            let decided = apply_calls(&mut session, |result| {
                matches!(result, PoolResult::Verdict { .. })
            });
            tokio::time::timeout(
                Duration::from_secs(5),
                futures::future::join(decided, reject),
            )
            .await
            .expect("the verdict arrives");

            let cause = session.cause.get().map(CloseOutcome::label);
            if listed {
                assert_eq!(cause, None, "an unconditional supplier is never charged");
            } else {
                assert_eq!(cause, Some("tx-relay: banned transaction"));
            }
        }
    });
}

/// A connection whose last session ended stays owned through the
/// transport's reopen backoff, so discovery cannot retire it before the
/// session reopens, and stops being owned after it.
#[test]
fn a_connection_stays_owned_through_the_reopen_gap() {
    let owned = Owned::default();
    let connection = (ZakuraPeerId::new(vec![1; 32]).expect("a valid id"), 1);
    owned
        .lock()
        .expect("never poisoned")
        .entry(connection.clone())
        .or_default()
        .sessions = 1;
    let ended = Instant::now();
    disown(&owned, &connection, ended);
    let owned = owned.lock().expect("never poisoned");
    let ownership = &owned[&connection];
    assert!(ownership.claims(ended + Duration::from_secs(8)));
    assert!(!ownership.claims(ended + REOPEN_GAP));
}

/// `Have` frames queued on the data stream reach the machine as one `Have`,
/// so a peer that sends one-entry frames costs one lock step per queued
/// run. The frame after them is still handled, after them: here a body
/// nobody asked for, which closes the peer only once all three ids are
/// taken.
#[tokio::test]
async fn queued_have_frames_reach_the_machine_as_one() {
    let (handle, inbox) = pool_port();
    publish(&inbox, Vec::new(), Vec::new());
    let (wiring, _handed) = wiring(handle);
    let mut session = Session::open(&wiring);
    session
        .deliver(TxRelayMessage::Hello {
            serves: 16,
            max_tx_bytes: MAX_TX_BYTES,
        })
        .await;

    let announced = transactions(4);
    let frame = |message: &TxRelayMessage| encode_frame(message).expect("a valid frame");
    let have = |tx: &VerifiedUnminedTx| {
        let size = u32::try_from(tx.transaction.size()).expect("a test body is small");
        frame(&TxRelayMessage::Have(vec![(tx.transaction.id(), size)]))
    };
    let (send, mut data) = framed_channel(8);
    for tx in &announced[1..3] {
        send.try_send(have(tx)).expect("the queue has room");
    }
    let unrequested = TxRelayMessage::Tx {
        id: announced[3].transaction.id(),
        body: vec![0],
    };
    send.try_send(frame(&unrequested))
        .expect("the queue has room");

    let open = session
        .routine
        .data_frame(&have(&announced[0]), &mut data)
        .await;
    assert!(open, "every frame decodes");
    assert!(data.try_recv().is_err(), "every queued frame is taken");
    assert_eq!(
        session.routine.calls.len(),
        3,
        "each announced id is reserved"
    );
    assert_eq!(
        session.cause.get(),
        Some(fault(CloseRow::RELAY_CLOSE_9)),
        "the unrequested body closes the peer"
    );
}

/// Answers the core's next reservation with `answer` while `session` applies
/// its pool calls up to the reservation's result, after checking that the
/// reservation asks for `tx_id` and says `relayed`.
async fn answer_reserve(
    session: &mut Session,
    inbox: &mut PoolInbox,
    tx_id: UnminedTxId,
    relayed: RelayedFetch,
    answer: Result<Reservation, PortUnreserved>,
) {
    let core = async {
        let Some(PoolCommand::Reserve {
            tx_id: asked,
            relayed: said,
            reply,
        }) = inbox.next().await
        else {
            panic!("the fetch asks the core for a reservation");
        };
        assert_eq!((asked, said), (tx_id, Some(relayed)));
        let _ = reply.send(answer);
    };
    let reserved = apply_calls(session, |r| matches!(r, PoolResult::Reserved { .. }));
    tokio::time::timeout(
        Duration::from_secs(5),
        futures::future::join(reserved, core),
    )
    .await
    .expect("the reservation's answer comes back promptly");
}

/// `02 §7` item 7, `mem-7`:
/// - A fetch leases its announced size from `Consumer::Tx` before it asks
///   the core for a reservation, and holds the lease until the core ends it,
///   after the reservation is granted.
/// - A fetch that finds `Consumer::Tx` full still asks, and says it leased
///   nothing. The core decides whether the empty-pool exception takes it.
#[tokio::test]
async fn a_fetch_holds_its_tx_lease_until_the_core_ends_it() {
    use crate::zakura::memory::{ConsumerSpec, PoolConfig};

    let mut txs = transactions(3);
    let pooled = txs.remove(0);
    let tx_a = txs.remove(0).transaction;
    let tx_b = txs.remove(0).transaction;
    let (id_a, size_a) = (tx_a.id(), u32::try_from(tx_a.size()).expect("fits u32"));
    let (id_b, size_b) = (tx_b.id(), u32::try_from(tx_b.size()).expect("fits u32"));

    // `Consumer::Tx` holds `tx_a` and no more while block sync borrows the
    // shared bytes.
    let (handle, mut inbox) = pool_port();
    publish(
        &inbox,
        vec![Change::Added(pooled.transaction.id())],
        vec![pooled],
    );
    let shared = u64::from(size_b) * 2;
    let memory_pool = MemoryPool::new(
        PoolConfig::new(u64::from(size_a) + shared, 0)
            .with_consumer(
                Consumer::Tx,
                ConsumerSpec::bulk(u64::from(size_a), u64::from(size_a) + shared, 1),
            )
            .with_consumer(Consumer::BlockSync, ConsumerSpec::bulk(0, shared, 1)),
    );
    let tx_leased = || memory_pool.snapshot().consumers[Consumer::Tx.index()].leased;
    let borrowed = memory_pool
        .try_acquire(Consumer::BlockSync, None, shared)
        .expect("block sync borrows the shared bytes");
    let (wiring, _handed) = wiring_with(handle, memory_pool.clone());
    let mut session = Session::open(&wiring);
    session
        .deliver(TxRelayMessage::Hello {
            serves: 16,
            max_tx_bytes: MAX_TX_BYTES,
        })
        .await;

    // The core grants `tx_a`'s fetch and counts its lease.
    session
        .deliver(TxRelayMessage::Have(vec![(id_a, size_a)]))
        .await;
    assert_eq!(tx_leased(), u64::from(size_a));
    let (reservation, hold) = inbox.grant_relayed(ReservationId(1), id_a, CancellationToken::new());
    let leased = RelayedFetch {
        announced_size: u64::from(size_a),
        leased_bytes: u64::from(size_a),
    };
    answer_reserve(&mut session, &mut inbox, id_a, leased, Ok(reservation)).await;
    assert_eq!(
        tx_leased(),
        u64::from(size_a),
        "a granted fetch keeps its lease"
    );

    // `Consumer::Tx` is full, so `tx_b`'s fetch leases nothing and says so.
    session
        .deliver(TxRelayMessage::Have(vec![(id_b, size_b)]))
        .await;
    let unleased = RelayedFetch {
        announced_size: u64::from(size_b),
        leased_bytes: 0,
    };
    let refusal = Err(PortUnreserved::NoRoom);
    answer_reserve(&mut session, &mut inbox, id_b, unleased, refusal).await;
    assert_eq!(tx_leased(), u64::from(size_a));

    // The core ends `tx_a`'s lease, and the lease goes back.
    drop(hold);
    tokio::time::timeout(
        Duration::from_secs(5),
        apply_calls(&mut session, |r| {
            matches!(r, PoolResult::TxLeaseEnded { .. })
        }),
    )
    .await
    .expect("the lease's end reaches the routine promptly");
    assert_eq!(tx_leased(), 0);
    drop(borrowed);
    assert_eq!(memory_pool.snapshot().total_leased, 0);
}

/// Receive credit on a real connection. The simulator has no local QUIC
/// endpoint.
#[cfg(not(zakura_sim))]
mod receive_credit {
    use super::*;

    /// Accepts every connection on [`CREDIT_ALPN`] and holds it open.
    struct HoldConnections;

    const CREDIT_ALPN: &[u8] = b"/zakura/test/tx-relay-credit/1";

    impl zakura_quic::Acceptor for HoldConnections {
        fn admit(&self, _incoming: &zakura_quic::IncomingInfo) -> zakura_quic::Admit {
            zakura_quic::Admit::Accept
        }

        fn alpns(&self) -> Vec<Vec<u8>> {
            vec![CREDIT_ALPN.to_vec()]
        }

        fn handle(&self, connection: zakura_quic::Conn) -> futures::future::BoxFuture<'static, ()> {
            Box::pin(async move {
                connection.closed().await;
            })
        }
    }

    /// `02 §6`, `02 §7` item 7: receive credit beyond the base is always backed
    /// by a lease. The empty-pool exception's fetch has no lease, so its
    /// `GetTxs` adds no credit: its body rides on the maximal `Tx` frame in the
    /// data stream's base. Otherwise a `NotFound` would leave the peer credit
    /// that no drain covers, once per connection.
    #[tokio::test]
    async fn an_unleased_fetch_adds_no_receive_credit() -> Result<(), crate::BoxError> {
        use crate::zakura::{
            memory::{ConsumerSpec, PoolConfig},
            testkit::LocalEndpointFactory,
        };

        let tx = transactions(1).remove(0).transaction;
        let (id, size) = (tx.id(), u32::try_from(tx.size()).expect("fits u32"));

        // Block sync borrows all of `S`, so `Consumer::Tx`, with no floor,
        // leases nothing.
        let shared = 4 * u64::from(MAX_TX_BYTES);
        let memory_pool = MemoryPool::new(
            PoolConfig::new(shared, 0)
                .with_consumer(Consumer::Tx, ConsumerSpec::bulk(0, shared, 1))
                .with_consumer(Consumer::BlockSync, ConsumerSpec::bulk(0, shared, 1)),
        );
        let _borrowed = memory_pool
            .try_acquire(Consumer::BlockSync, None, shared)
            .expect("block sync borrows the shared bytes");

        let server = LocalEndpointFactory::new().endpoint(93401).await?;
        let client = LocalEndpointFactory::new().endpoint(93402).await?;
        server.serve(HoldConnections)?;
        let conn = tokio::time::timeout(
            Duration::from_secs(5),
            client.connect(
                LocalEndpointFactory::node_addr(&server).await,
                &[CREDIT_ALPN],
            ),
        )
        .await??;
        let base = 1024 * 1024;
        let credit = ConnectionCredit::new_quic(connection().1, conn, base, base);

        let (handle, mut inbox) = pool_port();
        publish(&inbox, Vec::new(), Vec::new());
        let (wiring, _handed) = wiring_with(handle, memory_pool.clone());
        let mut session = Session::open_with_credit(&wiring, Some(credit.clone()));
        session
            .deliver(TxRelayMessage::Hello {
                serves: 16,
                max_tx_bytes: MAX_TX_BYTES,
            })
            .await;

        // The empty-pool exception grants the unleased fetch, which asks.
        session
            .deliver(TxRelayMessage::Have(vec![(id, size)]))
            .await;
        let (reservation, hold) =
            inbox.grant_relayed(ReservationId(1), id, CancellationToken::new());
        let unleased = RelayedFetch {
            announced_size: u64::from(size),
            leased_bytes: 0,
        };
        answer_reserve(&mut session, &mut inbox, id, unleased, Ok(reservation)).await;
        assert_eq!(
            memory_pool.snapshot().consumers[Consumer::Tx.index()].leased,
            0
        );
        assert!(
            session.routine.machine.state.requests.contains_key(&id),
            "the fetch asked for its body"
        );
        assert_eq!(
            credit.receive_window(),
            base,
            "an unleased fetch adds no credit"
        );

        // The peer has no body. The fetch ends, and the window never grew.
        session.deliver(TxRelayMessage::NotFound(id)).await;
        drop(hold);
        tokio::time::timeout(
            Duration::from_secs(5),
            apply_calls(&mut session, |r| {
                matches!(r, PoolResult::TxLeaseEnded { .. })
            }),
        )
        .await
        .expect("the lease's end reaches the routine promptly");
        assert_eq!(credit.receive_window(), base);
        Ok(())
    }
}
