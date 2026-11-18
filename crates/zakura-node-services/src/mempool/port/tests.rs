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

/// After the log has dropped its oldest entries, a read from any cursor it
/// still covers returns exactly the entries after that cursor, in order:
/// the offset it starts at matches the entries' own positions.
#[test]
fn change_log_reads_start_at_the_cursor_after_eviction() {
    let mut log = ChangeLog::default();
    let total = u16::try_from(CHANGE_LOG_CAPACITY + 100).expect("the capacity fits the test's ids");
    for n in 0..total {
        log.append([Change::Added(id(n))]);
    }
    let last = log.last().0;
    let oldest = last - u64::try_from(CHANGE_LOG_CAPACITY).expect("the capacity fits a u64") + 1;
    for cursor in [oldest - 1, oldest, oldest + 1, last - 2, last - 1, last] {
        let ChangesSince::Entries(entries) = log.since(ChangeSeq(cursor)) else {
            panic!("a cursor at {cursor} is still on the log");
        };
        let seqs: Vec<u64> = entries.iter().map(|entry| entry.seq.0).collect();
        assert_eq!(
            seqs,
            ((cursor + 1)..=last).collect::<Vec<_>>(),
            "cursor {cursor}"
        );
    }
    assert_eq!(log.since(ChangeSeq(oldest - 2)), ChangesSince::Resync);
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
async fn submitting_now_to_a_full_queue_drops_the_body_and_answers_no_room() {
    let (handle, mut inbox) = pool_port();
    let transaction = body(1);
    let tx_id = transaction.id();
    for _ in 0..COMMAND_QUEUE {
        handle
            .fetch_failed(
                inbox.grant(ReservationId(9), tx_id, Default::default()),
                far(),
            )
            .await;
    }
    let reservation = inbox.grant(ReservationId(7), tx_id, Default::default());
    let verdict = handle.submit_now(reservation, transaction, far());
    // The reservation went with the body before the verdict was awaited.
    assert_eq!(inbox.released(), Some(ReservationId(7)));
    assert!(matches!(verdict.await, Ok(Verdict::NoRoom)));

    // With room, the body reaches the core and its verdict comes back.
    while inbox.released().is_some() {}
    for _ in 0..COMMAND_QUEUE {
        assert!(matches!(
            inbox.command().await,
            Some(PoolCommand::FetchFailed(_))
        ));
    }
    let transaction = body(2);
    let reservation = inbox.grant(ReservationId(8), transaction.id(), Default::default());
    let verdict = handle.submit_now(reservation, transaction, far());
    let Some(PoolCommand::Submit { reply, .. }) = inbox.command().await else {
        panic!("with room, the body reaches the core");
    };
    let _ = reply.send(Verdict::Admitted);
    assert!(matches!(verdict.await, Ok(Verdict::Admitted)));
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
    assert!(off.is_empty());
}

/// The outpoint `n` names.
fn outpoint(n: u16) -> zakura_chain::transparent::OutPoint {
    zakura_chain::transparent::OutPoint {
        hash: hash(n),
        index: 0,
    }
}

/// A transaction that spends `outpoint(n)`, as a verifier passes it.
fn spending(n: u16) -> zakura_chain::transaction::VerifiedUnminedTx {
    use zakura_chain::{
        amount::Amount,
        transaction::{LockTime, Transaction, VerifiedUnminedTx},
        transparent::{Input, Script},
    };
    let transaction = Transaction::V4 {
        inputs: vec![Input::PrevOut {
            outpoint: outpoint(n),
            unlock_script: Script::new(&[]),
            sequence: 0,
        }],
        outputs: Vec::new(),
        lock_time: LockTime::unlocked(),
        expiry_height: Height(1_000_000),
        joinsplit_data: None,
        sapling_shielded_data: None,
    };
    VerifiedUnminedTx::new(
        transaction.into(),
        Amount::try_from(10_000).expect("a valid fee"),
        0,
        0,
        std::sync::Arc::new(Vec::new()),
    )
    .expect("the fee pays for the transaction")
}

#[test]
fn a_snapshot_after_changes_matches_one_built_from_scratch() {
    let tip = ChainTip {
        hash: zakura_chain::block::Hash([0; 32]),
        height: Height(100),
    };
    let [a, b, c, d] = [1, 2, 3, 4].map(spending);
    let mined = |tx: &zakura_chain::transaction::VerifiedUnminedTx| tx.transaction.id().mined_id();
    let before = PoolSnapshot::new(
        tip,
        ChangeSeq(1),
        vec![a.clone(), b.clone()],
        TransactionDependencies::default(),
    );

    // The step removes `a`, adds `c`, and adds `d` and evicts it again.
    let pool = [b.clone(), c.clone()];
    let changes = [
        Change::Removed {
            id: a.transaction.id(),
            reason: super::RemovalReason::Mined,
        },
        Change::Added(c.transaction.id()),
        Change::Added(d.transaction.id()),
        Change::Removed {
            id: d.transaction.id(),
            reason: super::RemovalReason::Evicted,
        },
    ];
    let after = before.after(
        tip,
        ChangeSeq(5),
        &changes,
        |id| pool.iter().find(|tx| mined(tx) == *id),
        vec![mined(&c), mined(&b)],
        TransactionDependencies::default(),
    );

    let fresh = PoolSnapshot::new(
        tip,
        ChangeSeq(5),
        vec![c.clone(), b.clone()],
        TransactionDependencies::default(),
    );
    assert!(after.transactions().eq(fresh.transactions()));
    for (n, tx) in [(1, &a), (2, &b), (3, &c), (4, &d)] {
        assert_eq!(after.spends(&outpoint(n)), fresh.spends(&outpoint(n)));
        assert_eq!(after.transaction(&mined(tx)), fresh.transaction(&mined(tx)));
    }
    // The earlier snapshot still reads as it was published.
    assert!(before.transactions().eq([&a, &b]));
    assert!(before.spends(&outpoint(1)) && !before.spends(&outpoint(3)));
}
