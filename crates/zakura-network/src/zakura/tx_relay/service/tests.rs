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
        Outgoing::Frame(frame, _) => decode_frame(&frame).expect("the actor encodes valid frames"),
        Outgoing::Serve { .. } => panic!("a serve job, not a frame"),
    }
}

/// A reader takes no next frame while the actor holds its last message, so
/// that frame's receive grant stays with the transport. In a stream one
/// frame deep, a third frame finds no room until the actor drops the first.
#[tokio::test]
async fn a_reader_waits_for_the_actor_before_it_takes_the_next_frame() {
    let (send, recv) = framed_channel(1);
    let (inputs, mut received) = mpsc::unbounded_channel();
    let (close, _cause, cancel) = closer();
    spawn_reader(7, recv, inputs, cancel.clone(), close);
    let frame = |n: u8| {
        encode_frame(&TxRelayMessage::NotFound(UnminedTxId::from_legacy_id(
            zakura_chain::transaction::Hash([n; 32]),
        )))
        .expect("a NotFound encodes")
    };

    send.try_send(frame(1)).expect("the stream has room");
    let first = tokio::time::timeout(Duration::from_secs(5), received.recv())
        .await
        .expect("the reader hands over the first frame")
        .expect("the reader is running");
    send.try_send(frame(2))
        .expect("taking the first frame made room");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        send.try_send(frame(3)).is_err(),
        "the reader left the second frame in the stream"
    );
    assert!(
        received.try_recv().is_err(),
        "only one message waits for the actor"
    );

    drop(first);
    let second = tokio::time::timeout(Duration::from_secs(5), received.recv())
        .await
        .expect("the reader takes the second frame once the first is handled")
        .expect("the reader is running");
    assert!(matches!(
        second,
        Input::Message {
            message: TxRelayMessage::NotFound(_),
            ..
        }
    ));
    send.try_send(frame(3))
        .expect("taking the second frame made room");
    cancel.cancel();
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

/// An actor with a pool of `count` transactions, and one session on
/// connection 1 whose writers the test holds and never reads, as a peer
/// that stopped reading. The session's peer has sent its `Hello`, and every
/// id has gone out to it in one `Inv`.
struct Serving {
    actor: Actor,
    ids: Vec<UnminedTxId>,
    cause: CloseCause,
    cancel: CancellationToken,
    budget: Arc<Semaphore>,
    /// Held unread, so the core's side of the port and the state writer's
    /// queue stay open.
    _inbox: PoolInbox,
    data: mpsc::Receiver<Outgoing>,
    _state: mpsc::Receiver<Outgoing>,
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
        let mut actor = Actor::new(
            handle,
            Arc::new(|_| true),
            &ZakuraTxRelayConfig::default(),
            TxRelaySessions::default(),
            Owned::default(),
        );
        actor.read_pool();
        let (cause, cancel, data, state) = open(&mut actor, 1);
        let mut serving = Self {
            actor,
            ids,
            cause,
            cancel,
            budget: Arc::new(Semaphore::new(1 << 20)),
            _inbox: inbox,
            data,
            _state: state,
        };
        serving.deliver(TxRelayMessage::Hello {
            serves: 16,
            max_tx_bytes: MAX_TX_BYTES,
        });
        match message(
            serving
                .data
                .try_recv()
                .expect("the session announces the pool"),
        ) {
            TxRelayMessage::Inv(entries) => assert_eq!(entries.len(), serving.ids.len()),
            other => panic!("expected an Inv, got {other:?}"),
        }
        serving
    }

    /// Hands the session's peer's `message` to the actor, and runs a step.
    fn deliver(&mut self, message: TxRelayMessage) {
        self.actor.input(Input::Message {
            key: 1,
            message,
            _permit: self
                .budget
                .clone()
                .try_acquire_owned()
                .expect("the budget is large"),
        });
        self.actor.apply_effects();
        self.actor.flush_adverts();
    }
}

/// Opens session `key` on connection 1 and returns its closer's cause and
/// token, and its writers' queues.
fn open(
    actor: &mut Actor,
    key: u64,
) -> (
    CloseCause,
    CancellationToken,
    mpsc::Receiver<Outgoing>,
    mpsc::Receiver<Outgoing>,
) {
    let (data, data_recv) = mpsc::channel(DATA_QUEUE);
    let (state, state_recv) = mpsc::channel(STATE_QUEUE);
    let (close, cause, cancel) = closer();
    actor.input(Input::Open {
        key,
        peer: PeerKey(1),
        exempt: false,
        data,
        state,
        close,
        connection: (ZakuraPeerId::new(vec![1; 32]).expect("a valid id"), 1),
    });
    (cause, cancel, data_recv, state_recv)
}

/// A peer may have as many requests unanswered as our `Hello` serves. One
/// more breaks its limit, however slowly it reads, and closes it.
#[tokio::test]
async fn a_request_past_our_hello_closes_the_peer() {
    let mut serving = Serving::new(MAX_SERVES + 1);
    let ids = serving.ids.clone();
    for id in &ids[..MAX_SERVES] {
        serving.deliver(TxRelayMessage::GetTxs(*id));
    }
    assert!(
        !serving.cancel.is_cancelled(),
        "requests within the limit wait"
    );
    serving.deliver(TxRelayMessage::GetTxs(ids[MAX_SERVES]));
    assert!(serving.cancel.is_cancelled());
    assert_eq!(serving.cause.get(), Some("tx-relay: too many requests"));
}

/// A request we would answer `NotFound` still needs a place, so with every
/// place taken it closes the peer instead of slipping past our `Hello`.
#[tokio::test]
async fn a_request_answered_not_found_counts_toward_the_limit() {
    let mut serving = Serving::new(MAX_SERVES);
    let ids = serving.ids.clone();
    for id in &ids {
        serving.deliver(TxRelayMessage::GetTxs(*id));
    }
    let unknown = transactions(MAX_SERVES + 1)
        .remove(MAX_SERVES)
        .transaction
        .id();
    serving.deliver(TxRelayMessage::GetTxs(unknown));
    assert_eq!(serving.cause.get(), Some("tx-relay: too many requests"));
}

/// A session that replaces another on the same connection, even before the
/// old one has ended, pays the same `Inv` bucket.
#[tokio::test]
async fn sessions_on_one_connection_share_one_inv_bucket() {
    let mut serving = Serving::new(1);
    let connection = (ZakuraPeerId::new(vec![1; 32]).expect("a valid id"), 1);
    let _second = open(&mut serving.actor, 2);
    assert_eq!(serving.actor.inv_buckets.len(), 1);
    assert_eq!(serving.actor.inv_buckets[&connection].sessions, 2);
    serving.actor.input(Input::Ended { key: 1 });
    assert_eq!(serving.actor.inv_buckets[&connection].sessions, 1);
}

/// A peer may not have one id outstanding twice, including while its first
/// request's serve job waits on the writer.
#[tokio::test]
async fn an_id_requested_again_while_its_job_waits_closes_the_peer() {
    let mut serving = Serving::new(1);
    let id = serving.ids[0];
    serving.deliver(TxRelayMessage::GetTxs(id));
    assert!(!serving.cancel.is_cancelled());
    serving.deliver(TxRelayMessage::GetTxs(id));
    assert_eq!(serving.cause.get(), Some("tx-relay: id requested twice"));
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
