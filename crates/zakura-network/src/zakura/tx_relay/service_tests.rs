//! End to end: two v2 nodes with `tx-relay` on move a transaction between
//! their pools.
//!
//! Each node runs a stand-in for the pool core over the real port. It grants
//! every reservation, admits every body it is handed, and publishes what it
//! admits. That is enough to show a transaction travel from one pool, over
//! the wire, into the other.

use std::{sync::Arc, time::Duration};

use tokio::sync::Notify;
use zakura_chain::{
    amount::Amount,
    block,
    parameters::Network,
    transaction::{UnminedTxId, VerifiedUnminedTx},
};
use zakura_node_services::mempool::{
    port::{
        pool_port, ChainTip, Change, ChangeSeq, PoolCommand, PoolHandle, PoolInbox, PoolSnapshot,
        ReservationId, Verdict,
    },
    TransactionDependencies,
};
use zakura_rt::sync::{CancellationToken, Mutex};

use super::{tx_relay_service, wire::ZAKURA_CAP_TX_RELAY};
use crate::{
    zakura::{
        testkit::{await_until, ZakuraTestNode},
        ZakuraPeerId,
    },
    BoxError,
};

/// The stand-in core's state: what it holds, and what it was handed.
#[derive(Default)]
struct Held {
    active: bool,
    transactions: Vec<VerifiedUnminedTx>,
    received: Vec<UnminedTxId>,
}

/// Runs a stand-in core on `inbox` until every handle is gone. It turns on
/// when `turn_on` fires, publishing no changes.
async fn fake_core(mut inbox: PoolInbox, held: Arc<Mutex<Held>>, turn_on: Arc<Notify>) {
    let mut next_id = 0;
    publish(&inbox, &held, Vec::new());
    loop {
        let command = tokio::select! {
            command = inbox.next() => command,
            () = turn_on.notified() => {
                held.lock().expect("the test never poisons the lock").active = true;
                publish(&inbox, &held, Vec::new());
                continue;
            }
        };
        let Some(command) = command else {
            return;
        };
        match command {
            PoolCommand::Reserve { tx_id, reply, .. } => {
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
                let id = transaction.id();
                // The stand-in does not verify: it pays the body a fee that
                // passes the mempool's fee check.
                let verified = VerifiedUnminedTx::new(
                    transaction,
                    Amount::try_from(1_000_000).expect("a valid test fee"),
                    0,
                    0,
                    Arc::new(Vec::new()),
                )
                .expect("the test fee pays for any vector transaction");
                {
                    let mut held = held.lock().expect("the test never poisons the lock");
                    held.received.push(id);
                    held.transactions.push(verified);
                }
                drop(reservation);
                let _ = reply.send(Verdict::Admitted);
                publish(&inbox, &held, vec![Change::Added(id)]);
            }
            _ => {}
        }
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

/// A pool port with a stand-in core running on it, and the signal that
/// turns the core on.
fn pool(held: Arc<Mutex<Held>>) -> (PoolHandle, Arc<Notify>) {
    let (handle, inbox) = pool_port();
    let turn_on = Arc::new(Notify::new());
    tokio::spawn(fake_core(inbox, held, turn_on.clone()));
    (handle, turn_on)
}

#[tokio::test]
async fn a_transaction_moves_between_two_nodes() -> Result<(), BoxError> {
    moves_between_two_nodes(true).await
}

/// A pool usually starts off and turns on later, and turning on an empty pool
/// publishes no changes. The relay still has to learn that it may fetch.
#[tokio::test]
async fn a_transaction_moves_once_an_empty_pool_turns_on() -> Result<(), BoxError> {
    moves_between_two_nodes(false).await
}

/// Node A holds a transaction and node B's pool is empty. B's pool starts on
/// when `b_starts_on`, and otherwise turns on once the nodes are connected.
/// B's pool admits the transaction.
async fn moves_between_two_nodes(b_starts_on: bool) -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let transaction = Network::Mainnet
        .unmined_transactions_in_blocks(..)
        .find(|tx| tx.transaction.id().auth_digest().is_some())
        .expect("the Mainnet vectors hold a v5 transaction");
    let id = transaction.transaction.id();

    let held_a = Arc::new(Mutex::new(Held {
        active: true,
        transactions: vec![transaction],
        received: Vec::new(),
    }));
    let held_b = Arc::new(Mutex::new(Held {
        active: b_starts_on,
        ..Held::default()
    }));
    let (pool_a, _) = pool(held_a.clone());
    let (pool_b, turn_on_b) = pool(held_b.clone());
    let relay_a = tx_relay_service(
        pool_a,
        Arc::new(|_| true),
        &Default::default(),
        Default::default(),
    );
    let relay_b = tx_relay_service(
        pool_b,
        Arc::new(|_| true),
        &Default::default(),
        Default::default(),
    );
    let sessions_b = relay_b
        .tx_relay_sessions
        .clone()
        .expect("tx-relay registers with its live-session record");

    let node_a = ZakuraTestNode::builder(71)
        .service(relay_a.service)
        .supported_capabilities(ZAKURA_CAP_TX_RELAY)
        .spawn()
        .await?;
    let node_b = ZakuraTestNode::builder(72)
        .service(relay_b.service)
        .supported_capabilities(ZAKURA_CAP_TX_RELAY)
        .spawn()
        .await?;
    node_a
        .connect_native(&node_b, Duration::from_secs(5))
        .await?;
    if !b_starts_on {
        // Let A's announcement reach B while B's pool is still off.
        tokio::time::sleep(Duration::from_secs(1)).await;
        turn_on_b.notify_one();
    }

    await_until(
        "node B's pool admits node A's transaction",
        Duration::from_secs(20),
        || {
            held_b
                .lock()
                .expect("the test never poisons the lock")
                .received
                .contains(&id)
        },
    )
    .await?;
    let peer_a = ZakuraPeerId::new(node_a.node_addr().await.id.as_bytes().to_vec())?;
    assert!(
        sessions_b.is_live(&peer_a),
        "node B publishes its live session with node A"
    );
    assert!(
        held_a
            .lock()
            .expect("the test never poisons the lock")
            .received
            .is_empty(),
        "node A already held the transaction, so it fetches nothing"
    );

    node_a.shutdown().await;
    await_until(
        "node B removes the ended session",
        Duration::from_secs(20),
        || !sessions_b.is_live(&peer_a),
    )
    .await?;
    node_b.shutdown().await;
    Ok(())
}
