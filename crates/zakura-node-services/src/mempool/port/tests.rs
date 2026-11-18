//! Tests of the port: its plain data, its handle and its inbox.

use std::time::Duration;

use zakura_chain::{
    block::Height,
    transaction::{self, UnminedTxId},
};

use super::{
    pool_port, ChainTip, Change, ChangeLog, ChangeSeq, ChangesSince, LocalFault, PoolCommand,
    PoolSnapshot, ReservationId, Unreserved, Verdict, CHANGE_LOG_CAPACITY, COMMAND_QUEUE,
};
use crate::mempool::TransactionDependencies;

fn hash(n: u16) -> transaction::Hash {
    let mut bytes = [0; 32];
    bytes[..2].copy_from_slice(&n.to_le_bytes());
    transaction::Hash(bytes)
}

fn id(n: u16) -> UnminedTxId {
    UnminedTxId::Legacy(hash(n))
}

fn secs(seconds: u64) -> Duration {
    Duration::from_secs(seconds)
}

/// A deadline no test reaches, for calls whose test is not about deadlines.
fn far() -> zakura_rt::time::Instant {
    zakura_rt::time::Instant::now() + secs(3600)
}

/// A unit transaction that the test only needs for its id.
fn body(n: u8) -> zakura_chain::transaction::UnminedTx {
    use zakura_chain::{
        block::Height as BlockHeight,
        transaction::{LockTime, Transaction},
    };
    let mut expiry = BlockHeight(1_000_000);
    expiry.0 += u32::from(n);
    Transaction::V4 {
        inputs: Vec::new(),
        outputs: Vec::new(),
        lock_time: LockTime::unlocked(),
        expiry_height: expiry,
        joinsplit_data: None,
        sapling_shielded_data: None,
    }
    .into()
}

fn snapshot_at(last_change: ChangeSeq) -> PoolSnapshot {
    PoolSnapshot::new(
        ChainTip {
            hash: zakura_chain::block::Hash([0; 32]),
            height: Height(100),
        },
        last_change,
        Vec::new(),
        TransactionDependencies::default(),
    )
}

#[test]
fn change_log_numbers_entries_in_order_across_batches() {
    let mut log = ChangeLog::default();
    assert_eq!(log.since(ChangeSeq(0)), ChangesSince::Entries(Vec::new()));

    assert_eq!(log.append([Change::Refused(id(1))]), ChangeSeq(1));
    // An empty batch moves no cursor.
    assert_eq!(log.append([]), ChangeSeq(1));
    assert_eq!(
        log.append([Change::Added(id(1)), Change::Added(id(2))]),
        ChangeSeq(3)
    );

    let ChangesSince::Entries(entries) = log.since(ChangeSeq(1)) else {
        panic!("a reader one batch behind is still on the log");
    };
    assert_eq!(
        entries
            .iter()
            .map(|entry| (entry.seq, entry.batch, entry.change))
            .collect::<Vec<_>>(),
        vec![
            (ChangeSeq(2), 3, Change::Added(id(1))),
            (ChangeSeq(3), 3, Change::Added(id(2))),
        ]
    );
}

#[test]
fn change_log_resyncs_a_reader_that_fell_off() {
    let mut log = ChangeLog::default();
    let total = u16::try_from(CHANGE_LOG_CAPACITY + 2).expect("the capacity fits the test's ids");
    for n in 0..total {
        log.append([Change::Added(id(n))]);
    }
    // Entries 1 and 2 are gone. A reader at 2 needs entry 3, which is kept.
    assert_eq!(log.since(ChangeSeq(1)), ChangesSince::Resync);
    assert!(
        matches!(log.since(ChangeSeq(2)), ChangesSince::Entries(entries) if entries.len() == CHANGE_LOG_CAPACITY)
    );
}

#[tokio::test]
async fn a_dropped_reservation_reaches_the_core() {
    let (handle, mut inbox) = pool_port();
    let core = tokio::spawn(async move {
        let Some(PoolCommand::Reserve { tx_id, reply, .. }) = inbox.command().await else {
            panic!("the first command is the reservation");
        };
        let _ = reply.send(Ok(inbox.grant(ReservationId(7), tx_id, Default::default())));
        inbox
    });

    let reservation = handle
        .reserve(id(1), far())
        .await
        .expect("the test's deadline is far off")
        .expect("the test core grants it");
    let mut inbox = core.await.expect("the test core does not panic");
    assert_eq!(inbox.released(), None);

    drop(reservation);
    assert_eq!(inbox.released(), Some(ReservationId(7)));
}

#[tokio::test]
async fn a_stopped_core_answers_every_call() {
    let (handle, inbox) = pool_port();
    drop(inbox);
    assert!(matches!(
        handle
            .reserve(id(1), far())
            .await
            .expect("the test's deadline is far off"),
        Err(Unreserved::Stopped)
    ));
    assert!(handle.snapshot().is_none());
}

#[tokio::test]
async fn cancelling_a_blocked_submission_frees_its_slot() {
    let (handle, mut inbox) = pool_port();
    let transaction = body(1);
    let tx_id = transaction.id();
    let reservation = inbox.grant(ReservationId(7), tx_id, Default::default());

    // The command queue is full, so the submission waits to be sent.
    for _ in 0..COMMAND_QUEUE {
        handle
            .fetch_failed(
                inbox.grant(ReservationId(9), tx_id, Default::default()),
                far(),
            )
            .await;
    }
    let caller = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .submit(reservation, transaction, far())
                .await
                .expect("the test's deadline is far off")
        })
    };
    tokio::task::yield_now().await;
    assert_eq!(inbox.released(), None);

    // The caller goes away while it waits. The command never reaches the
    // core, so the reservation inside it is dropped still armed.
    caller.abort();
    assert!(caller
        .await
        .expect_err("the caller was aborted")
        .is_cancelled());
    assert_eq!(inbox.released(), Some(ReservationId(7)));
}

#[tokio::test]
async fn an_accepted_reservation_is_the_cores_and_a_dropped_command_is_not() {
    let (handle, mut inbox) = pool_port();
    let transaction = body(1);
    let reservation = inbox.grant(ReservationId(7), transaction.id(), Default::default());
    let caller = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .submit(reservation, transaction, far())
                .await
                .expect("the test's deadline is far off")
        })
    };

    let Some(PoolCommand::Submit {
        reservation, reply, ..
    }) = inbox.command().await
    else {
        panic!("a reservation that wants the body sends it");
    };
    assert_eq!(reservation.accept(), ReservationId(7));
    let _ = reply.send(Verdict::Admitted);
    assert!(matches!(
        caller.await.expect("the caller is not cancelled"),
        Verdict::Admitted
    ));
    // The core owns the outcome now. Nothing is released.
    assert_eq!(inbox.released(), None);
}

#[tokio::test]
async fn a_body_for_another_id_is_refused_and_frees_the_slot() {
    let (handle, mut inbox) = pool_port();
    let reservation = inbox.grant(ReservationId(9), body(1).id(), Default::default());
    assert!(matches!(
        handle
            .submit(reservation, body(2), far())
            .await
            .expect("the test's deadline is far off"),
        Verdict::LocalFault(LocalFault::WrongTransaction)
    ));
    assert_eq!(inbox.released(), Some(ReservationId(9)));
}

#[tokio::test]
async fn a_view_pairs_each_snapshot_with_exactly_its_changes() {
    let (handle, inbox) = pool_port();
    assert!(handle.view(ChangeSeq(0)).is_none());

    inbox.publish([Change::Added(id(1))], snapshot_at);
    inbox.publish([Change::Added(id(2)), Change::Refused(id(3))], snapshot_at);

    let view = handle.view(ChangeSeq(1)).expect("published");
    assert_eq!(view.snapshot.last_change, ChangeSeq(3));
    let ChangesSince::Entries(entries) = view.changes else {
        panic!("the reader is on the log");
    };
    assert_eq!(
        entries.iter().map(|entry| entry.change).collect::<Vec<_>>(),
        vec![Change::Added(id(2)), Change::Refused(id(3))]
    );
}

#[test]
fn a_reader_is_never_handed_changes_newer_than_its_snapshot() {
    let mut log = ChangeLog::default();
    log.append([Change::Added(id(1))]);
    log.append([Change::Added(id(2))]);
    // A snapshot taken after the first batch.
    let ChangesSince::Entries(entries) = log.between(ChangeSeq(0), ChangeSeq(1)) else {
        panic!("the reader is on the log");
    };
    assert_eq!(
        entries.iter().map(|entry| entry.change).collect::<Vec<_>>(),
        vec![Change::Added(id(1))]
    );
}

#[tokio::test]
async fn a_dropped_reservation_wakes_an_idle_core() {
    let (handle, mut inbox) = pool_port();
    let reservation = inbox.grant(ReservationId(7), id(1), Default::default());
    // The core has nothing to do and waits.
    let core = tokio::spawn(async move { inbox.next().await });
    tokio::task::yield_now().await;

    // No command follows. The drop alone has to wake it.
    drop(reservation);
    let event = tokio::time::timeout(secs(5), core)
        .await
        .expect("the core wakes")
        .expect("the core does not panic");
    assert!(matches!(
        event,
        Some(PoolCommand::Released(ReservationId(7)))
    ));
    drop(handle);
}

#[test]
fn a_snapshot_says_whether_the_pool_is_on() {
    let tip = ChainTip {
        hash: zakura_chain::block::Hash([0; 32]),
        height: Height(100),
    };
    assert!(snapshot_at(ChangeSeq(0)).active);
    let off = PoolSnapshot::inactive(tip, ChangeSeq(0));
    assert!(!off.active);
    assert!(off.transactions.is_empty());
}
