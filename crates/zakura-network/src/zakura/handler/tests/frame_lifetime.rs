//! Real QUIC frames reach their stream workers' frame gates and worker slots.

use super::read_close_cause::Link;
use super::*;
use crate::zakura::regulation::{OutputByteBudget, SlotBudget};

const TEST_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a test watches for something that must not happen.
const HELD: Duration = Duration::from_millis(150);

fn frame_bytes(message_type: u16, payload_len: usize) -> Vec<u8> {
    let frame = Frame {
        message_type,
        flags: 0,
        payload: vec![1; payload_len],
    };
    frame.encode(1024).expect("the test frame fits the cap")
}

fn prelude(request_id: Option<u64>) -> StreamPrelude {
    StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind: 900,
        stream_version: 1,
        request_id,
        max_frame_bytes: 1024,
    }
}

/// A raw gate of `capacity` bytes, with the budget that observes it.
fn raw_gate(capacity: u64) -> (FrameGate, OutputByteBudget) {
    let budget = OutputByteBudget::new(capacity).unwrap();
    let wide = OutputByteBudget::new(1 << 30).unwrap();
    (FrameGate::new(budget.clone(), wide), budget)
}

/// The reader stops after a checked header until capacity returns. A worker's
/// frame stays charged until its consumer receives and releases it.
#[tokio::test]
async fn a_saturated_gate_holds_the_payload_until_capacity_returns() -> Result<(), BoxError> {
    let mut link = Link::connect().await?;
    let mut context = tabled_context(None, 128);
    let (gate, budget) = raw_gate(64);
    let shared = gate.try_admit(64).unwrap();
    let _floor = gate.try_admit(64).unwrap();
    context.resources.raw = gate.clone();

    let bytes = frame_bytes(1, 16);
    let (_peer_send, _peer_recv, send, recv) = link.open(&bytes).await?;
    let (inbound_tx, mut inbound_rx) = mpsc::channel(4);
    let (_outbound_tx, outbound_rx) = worker_framed_channel(1);
    let cancel = context.connection_token.clone();
    context.precheck.start();
    let worker = AbortOnDropHandle::new(tokio::spawn(persistent_stream_worker(
        send,
        recv,
        prelude(None),
        context,
        inbound_tx,
        outbound_rx,
        4,
    )));
    assert!(
        timeout(HELD, inbound_rx.recv()).await.is_err(),
        "no frame passes the saturated gate"
    );

    drop(shared);
    let admitted = timeout(TEST_TIMEOUT, inbound_rx.recv())
        .await?
        .ok_or("the freed capacity admits the frame")?;
    assert_eq!(admitted.frame.payload, vec![1; 16]);
    assert!(admitted.grant.waited() > Duration::ZERO);
    assert_eq!(budget.granted(), 24, "the queued frame keeps its charge");
    drop(admitted);
    assert_eq!(budget.granted(), 0);

    cancel.cancel();
    timeout(TEST_TIMEOUT, worker).await??;
    link.close().await;
    Ok(())
}

/// One stream's unfunded frame stays in QUIC's buffers, where the peer memory budget
/// funds it, while a second stream on the same saturated connection delivers its floor
/// frame. Neither frame adds to the node's raw charge, so nothing counts twice.
#[tokio::test]
async fn a_saturated_node_delivers_another_streams_floor_frame() -> Result<(), BoxError> {
    let mut link = Link::connect().await?;
    let node = NodeResources::new(ResourceEnvelope::new(1 << 20, 1 << 20, 1));
    let connection = node.connection();
    let (raw_hold, output_hold) = node.hold_node_budgets();
    let held_raw = node.usage().raw_bytes;

    let mut paused = tabled_context(None, 128);
    paused.resources = connection.stream();
    let paused_floor = paused.resources.raw.try_admit(1).unwrap();
    let mut flowing = tabled_context(None, 128);
    flowing.resources = connection.stream();
    assert_eq!(node.usage().owners.setup, 2);

    let bytes = frame_bytes(1, 16);
    let (_paused_peer_send, _paused_peer_recv, send, recv) = link.open(&bytes).await?;
    let (paused_tx, mut paused_rx) = mpsc::channel(4);
    let (_paused_outbound, outbound_rx) = worker_framed_channel(1);
    let paused_cancel = paused.connection_token.clone();
    paused.precheck.start();
    let paused_worker = AbortOnDropHandle::new(tokio::spawn(persistent_stream_worker(
        send,
        recv,
        prelude(None),
        paused,
        paused_tx,
        outbound_rx,
        4,
    )));

    let (_flowing_peer_send, _flowing_peer_recv, send, recv) = link.open(&bytes).await?;
    let (flowing_tx, mut flowing_rx) = mpsc::channel(4);
    let (_flowing_outbound, outbound_rx) = worker_framed_channel(1);
    let flowing_cancel = flowing.connection_token.clone();
    flowing.precheck.start();
    let flowing_worker = AbortOnDropHandle::new(tokio::spawn(persistent_stream_worker(
        send,
        recv,
        prelude(None),
        flowing,
        flowing_tx,
        outbound_rx,
        4,
    )));

    let delivered = timeout(TEST_TIMEOUT, flowing_rx.recv())
        .await?
        .ok_or("the second stream's floor funds its frame")?;
    assert_eq!(delivered.frame.payload, vec![1; 16]);
    assert!(
        timeout(HELD, paused_rx.recv()).await.is_err(),
        "the first stream's floor is taken, so its frame stays in QUIC"
    );
    assert_eq!(
        node.usage().raw_bytes,
        held_raw,
        "unread and floor frames add no raw charge"
    );

    drop(paused_floor);
    let released = timeout(TEST_TIMEOUT, paused_rx.recv())
        .await?
        .ok_or("the returned floor slot admits the held frame")?;
    assert_eq!(released.frame.payload, vec![1; 16]);

    drop((delivered, released));
    paused_cancel.cancel();
    flowing_cancel.cancel();
    timeout(TEST_TIMEOUT, paused_worker).await??;
    timeout(TEST_TIMEOUT, flowing_worker).await??;
    assert_eq!(node.usage().owners.total(), 0);
    drop((raw_hold, output_hold));
    assert_eq!(node.usage().raw_bytes, 0);
    link.close().await;
    Ok(())
}

/// A peer that declares a large frame and stops holds its admitted bytes only
/// until cancellation ends the reader. Each stream's owner leaves the count.
#[tokio::test]
async fn cancelling_a_partial_frame_returns_its_admission() -> Result<(), BoxError> {
    let mut link = Link::connect().await?;
    let mut context = tabled_context(None, 128);
    let node = NodeResources::new(ResourceEnvelope::new(1 << 20, 1 << 20, 1));
    let (gate, budget) = raw_gate(1 << 20);
    context.resources = node.connection().stream();
    context.resources.raw = gate;
    context.resources.owner.advance(OwnerPhase::Installed);
    assert_eq!(node.owners().counts().installed, 1);

    let mut bytes = frame_bytes(1, 512);
    bytes.truncate(100);
    let (_peer_send, _peer_recv, send, recv) = link.open(&bytes).await?;
    let (inbound_tx, _inbound_rx) = mpsc::channel(4);
    let (_outbound_tx, outbound_rx) = worker_framed_channel(1);
    let cancel = context.connection_token.clone();
    context.precheck.start();
    let worker = AbortOnDropHandle::new(tokio::spawn(persistent_stream_worker(
        send,
        recv,
        prelude(None),
        context,
        inbound_tx,
        outbound_rx,
        4,
    )));
    timeout(TEST_TIMEOUT, async {
        while budget.granted() != 520 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;

    cancel.cancel();
    timeout(TEST_TIMEOUT, worker).await??;
    assert_eq!(budget.granted(), 0);
    assert_eq!(node.owners().counts().total(), 0);
    link.close().await;
    Ok(())
}

/// A request waits for an execution slot before its service runs. Cancelling
/// that wait ends the worker, and the raw frame's charge ends with it.
#[tokio::test]
async fn a_request_waits_for_an_execution_slot_and_cancels_cleanly() -> Result<(), BoxError> {
    let mut link = Link::connect().await?;
    let mut context = tabled_context(None, 128);
    let workers = SlotBudget::new(1).unwrap();
    let held = workers.try_reserve().unwrap();
    let (gate, budget) = raw_gate(1 << 20);
    context.resources.request_workers = workers.clone();
    context.resources.raw = gate;
    let cancel = context.connection_token.clone();

    let bytes = frame_bytes(1, 16);
    let (_peer_send, _peer_recv, send, recv) = link.open(&bytes).await?;
    let worker = AbortOnDropHandle::new(tokio::spawn(request_stream_worker(
        send,
        recv,
        prelude(Some(1)),
        context,
        Arc::new(ServiceRegistry::new(vec![])?),
    )));
    timeout(TEST_TIMEOUT, async {
        while budget.granted() != 24 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    tokio::time::sleep(HELD).await;
    assert!(!worker.is_finished(), "the request waits for its slot");
    assert_eq!(budget.granted(), 24, "the waiting request keeps its frame");

    cancel.cancel();
    timeout(TEST_TIMEOUT, worker).await??;
    assert_eq!(budget.granted(), 0);
    drop(held);
    assert!(workers.try_reserve().is_some());
    link.close().await;
    Ok(())
}

/// When a slot is free the request runs and the slot returns with its call.
#[tokio::test]
async fn a_request_returns_its_slot_with_its_call() -> Result<(), BoxError> {
    let mut link = Link::connect().await?;
    let mut context = tabled_context(None, 128);
    let workers = SlotBudget::new(1).unwrap();
    context.resources.request_workers = workers.clone();

    let bytes = frame_bytes(1, 16);
    let (_peer_send, _peer_recv, send, recv) = link.open(&bytes).await?;
    timeout(
        TEST_TIMEOUT,
        request_stream_worker(
            send,
            recv,
            prelude(Some(1)),
            context,
            Arc::new(ServiceRegistry::new(vec![])?),
        ),
    )
    .await?;
    assert_eq!(workers.reserved(), 0);
    link.close().await;
    Ok(())
}

/// An unpolled stream-setup future's resources are its own. Dropping the value
/// that carries them leaves nothing, as the connection loop's teardown does.
#[test]
fn dropped_stream_resources_leave_no_owner() {
    let node = NodeResources::new(ResourceEnvelope::new(1 << 20, 1 << 20, 1));
    let resources = node.connection().stream();
    let setup = async move {
        let _resources = resources;
        future::pending::<()>().await
    };
    assert_eq!(node.owners().counts().setup, 1);
    drop(setup);
    assert_eq!(node.owners().counts().total(), 0);
}
