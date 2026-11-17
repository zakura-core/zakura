//! Serving: a peer's requests are bounded by our `Hello`, and a body is
//! built only when the writer reaches it, from the pool as it is then.

use std::time::Duration;

use zakura_chain::{
    block,
    parameters::Network,
    transaction::{UnminedTxId, VerifiedUnminedTx},
};
use zakura_node_services::mempool::{
    port::{pool_port, ChainTip, Change, PoolInbox, PoolSnapshot, RemovalReason},
    TransactionDependencies,
};

use super::*;
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
/// `NotFound` on the state stream, and the request's place comes back.
#[tokio::test]
async fn a_body_evicted_while_its_job_waits_is_answered_not_found() {
    let transaction = transactions(1).remove(0);
    let id = transaction.transaction.id();
    let (handle, inbox) = pool_port();
    publish(&inbox, vec![Change::Added(id)], vec![transaction]);

    let (send, mut recv) = framed_channel(1);
    let (state, mut answers) = mpsc::channel(STATE_QUEUE);
    let (close, _cause, _cancel) = closer();
    let bodies = Bodies {
        pool: handle.reader(),
        state,
        close,
    };
    let writer = spawn_writer(send, DATA_QUEUE, CancellationToken::new(), Some(bodies));
    let requests = Arc::new(Semaphore::new(MAX_SERVES));
    let serve = || Outgoing::Serve {
        id,
        request: Request {
            _place: requests
                .clone()
                .try_acquire_owned()
                .expect("the test stays within the limit"),
            _outstanding: Arc::new(()),
        },
    };

    // While the pool holds it, the job gets the body.
    writer.send(serve()).await.expect("the writer runs");
    let frame = recv.recv().await.expect("the writer sends the body");
    assert!(matches!(
        decode_frame::<TxRelayMessage>(&frame),
        Ok(TxRelayMessage::Tx { id: sent, .. }) if sent == id
    ));

    // The peer stops reading: one frame fills the transport queue and the
    // next holds the writer, so the job waits behind them.
    let filler = || {
        Outgoing::Frame(
            encode_frame(&TxRelayMessage::Inv(vec![(id, 1)])).expect("a valid frame"),
            None,
        )
    };
    writer.send(filler()).await.expect("the writer runs");
    writer.send(filler()).await.expect("the writer runs");
    writer.send(serve()).await.expect("the writer runs");

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
    // The writer lets the request go once its answer is queued.
    let every_place = u32::try_from(MAX_SERVES).expect("MAX_SERVES is 16, which fits a u32");
    let _places = tokio::time::timeout(Duration::from_secs(5), requests.acquire_many(every_place))
        .await
        .expect("the request's place comes back")
        .expect("the semaphore stays open");
}

/// The connection every test session runs on.
fn connection() -> (ZakuraPeerId, ZakuraConnId) {
    (ZakuraPeerId::new(vec![1; 32]).expect("a valid id"), 1)
}

/// A routine's wiring on `pool`, with no reactor task: the test holds what
/// ended sessions hand off.
fn wiring(pool: PoolHandle) -> (Wiring, mpsc::UnboundedReceiver<Calls>) {
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
        may_cool: Arc::new(|_| true),
        handoff,
        owned: Owned::default(),
        record: TxRelaySessions::default(),
        inv_buckets: InvBuckets::default(),
        base: Instant::now(),
    };
    (wiring, handed)
}

/// One session's routine, driven step by step, whose writers' queues the
/// test holds.
struct Session {
    routine: Routine,
    cause: CloseCause,
    cancel: CancellationToken,
    data: mpsc::Receiver<Outgoing>,
    /// Held unread, so the state writer's queue stays open.
    _state: mpsc::Receiver<Outgoing>,
}

impl Session {
    /// Opens a session of peer 1 on [`connection`], as `add_peer` does, and
    /// lets it read the pool.
    fn open(wiring: &Wiring) -> Self {
        let (data, data_recv) = mpsc::channel(DATA_QUEUE);
        let (state, state_recv) = mpsc::channel(STATE_QUEUE);
        let (close, cause, cancel) = closer();
        open_inv_bucket(&wiring.inv_buckets, &connection());
        let io = SessionIo::new(data, state, close, connection());
        let mut routine = Routine::open(
            wiring.clone(),
            PeerKey(1),
            false,
            io,
            CancellationToken::new(),
        );
        routine.read_pool();
        routine.after_step();
        Self {
            routine,
            cause,
            cancel,
            data: data_recv,
            _state: state_recv,
        }
    }

    /// Hands the peer's `message` to the routine, and runs what follows a
    /// step.
    fn deliver(&mut self, message: TxRelayMessage) {
        self.routine.message(message);
        self.routine.after_step();
    }
}

/// A routine with a pool of `count` transactions, and one session on
/// connection 1 whose writers the test holds and never reads, as a peer
/// that stopped reading. The session's peer has sent its `Hello`, and every
/// id has gone out to it in one `Inv`.
struct Serving {
    session: Session,
    wiring: Wiring,
    ids: Vec<UnminedTxId>,
    /// Held unread, so the core's side of the port stays open.
    _inbox: PoolInbox,
    _handed: mpsc::UnboundedReceiver<Calls>,
}

impl Serving {
    fn new(count: usize) -> Self {
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
        session.deliver(TxRelayMessage::Hello {
            serves: 16,
            max_tx_bytes: MAX_TX_BYTES,
        });
        match message(
            session
                .data
                .try_recv()
                .expect("the session announces the pool"),
        ) {
            TxRelayMessage::Inv(entries) => assert_eq!(entries.len(), ids.len()),
            other => panic!("expected an Inv, got {other:?}"),
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

/// A peer may have as many requests unanswered as our `Hello` serves. One
/// more breaks its limit, however slowly it reads, and closes it.
#[tokio::test]
async fn a_request_past_our_hello_closes_the_peer() {
    let mut serving = Serving::new(MAX_SERVES + 1);
    let ids = serving.ids.clone();
    let session = &mut serving.session;
    for id in &ids[..MAX_SERVES] {
        session.deliver(TxRelayMessage::GetTxs(*id));
    }
    assert!(
        !session.cancel.is_cancelled(),
        "requests within the limit wait"
    );
    session.deliver(TxRelayMessage::GetTxs(ids[MAX_SERVES]));
    assert!(session.cancel.is_cancelled());
    assert_eq!(session.cause.get(), Some("tx-relay: too many requests"));
}

/// A request we would answer `NotFound` still needs a place, so with every
/// place taken it closes the peer instead of slipping past our `Hello`.
#[tokio::test]
async fn a_request_answered_not_found_counts_toward_the_limit() {
    let mut serving = Serving::new(MAX_SERVES);
    let ids = serving.ids.clone();
    let session = &mut serving.session;
    for id in &ids {
        session.deliver(TxRelayMessage::GetTxs(*id));
    }
    let unknown = transactions(MAX_SERVES + 1)
        .remove(MAX_SERVES)
        .transaction
        .id();
    session.deliver(TxRelayMessage::GetTxs(unknown));
    assert_eq!(session.cause.get(), Some("tx-relay: too many requests"));
}

/// A session that replaces another on the same connection, even before the
/// old one has ended, pays the same `Inv` bucket.
#[tokio::test]
async fn sessions_on_one_connection_share_one_inv_bucket() {
    let serving = Serving::new(1);
    let sessions = |wiring: &Wiring| {
        let buckets = wiring.inv_buckets.lock().expect("never poisoned");
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
    let mut serving = Serving::new(1);
    let id = serving.ids[0];
    let session = &mut serving.session;
    session.deliver(TxRelayMessage::GetTxs(id));
    assert!(!session.cancel.is_cancelled());
    session.deliver(TxRelayMessage::GetTxs(id));
    assert_eq!(session.cause.get(), Some("tx-relay: id requested twice"));
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
