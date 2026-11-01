//! Tests for lifecycle trace rows.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use futures::{channel::mpsc as futures_mpsc, StreamExt};
use tokio::sync::{mpsc, Semaphore};
use zakura_chain::{block, transaction};
use zakura_jsonl_trace::{
    tables, JsonlTraceTables, JsonlTracer, JsonlWriteEvent, ENVELOPE_COLUMNS, EXTRA_COLUMN,
};

use super::*;
use crate::{
    peer::{block_gossip::BlockGossipPeer, ClientRequest},
    protocol::external::{InventoryHash, Message},
    Request, Response,
};

pub(crate) type Row = BTreeMap<&'static str, String>;

pub(crate) fn capture(
    tables: JsonlTraceTables,
) -> (LifecycleTrace, mpsc::Receiver<JsonlWriteEvent>) {
    let (tx, rx) = mpsc::channel(1024);
    (
        LifecycleTrace::new(JsonlTracer::new(tx).with_tables(tables)),
        rx,
    )
}

/// Takes every queued row.
pub(crate) fn drain(rx: &mut mpsc::Receiver<JsonlWriteEvent>) -> Vec<JsonlWriteEvent> {
    std::iter::from_fn(|| rx.try_recv().ok()).collect()
}

/// Returns the rows of `table`, as column names mapped to raw CSV fields.
pub(crate) fn rows(
    written: &[JsonlWriteEvent],
    table: zakura_jsonl_trace::JsonlTraceTable,
) -> Vec<Row> {
    let mut rows = Vec::new();
    for written in written
        .iter()
        .filter(|written| written.table == table.table())
    {
        let record = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(written.line.as_slice())
            .records()
            .next()
            .expect("each row is one CSV record")
            .expect("each row is valid CSV");
        let row: Row = ENVELOPE_COLUMNS
            .iter()
            .chain(table.header())
            .copied()
            .chain([EXTRA_COLUMN])
            .zip(record.iter().map(str::to_owned))
            .collect();
        assert_eq!(row[EXTRA_COLUMN], "", "every field has a column: {row:?}");
        rows.push(row);
    }
    rows
}

fn event<'a>(rows: &'a [Row], event: &str, conn: u64) -> &'a Row {
    rows.iter()
        .find(|row| row["event"] == event && row["conn"] == conn.to_string())
        .unwrap_or_else(|| panic!("{event} row for connection {conn}: {rows:#?}"))
}

fn hash(byte: u8) -> block::Hash {
    block::Hash([byte; 32])
}

fn gossip_peer(trace: ConnectionTrace) -> (BlockGossipPeer, futures_mpsc::Receiver<ClientRequest>) {
    let (sender, receiver) = futures_mpsc::channel(0);
    (
        BlockGossipPeer {
            sender,
            uploads: Default::default(),
            registry: None,
            version: crate::constants::CURRENT_NETWORK_PROTOCOL_VERSION,
            sidecar: false,
            trace,
        },
        receiver,
    )
}

/// Every upload slot is held by an older advertisement, and then the next peer's connection is
/// busy for 500 ms. The rows attribute 3 s to the slot and 500 ms to the connection.
#[tokio::test(start_paused = true)]
async fn gossip_rows_separate_slot_wait_from_connection_queue_wait() {
    const BUSY_CONNECTION: Duration = Duration::from_millis(500);

    let (trace, mut rx) = capture(JsonlTraceTables::all());
    let (holder_trace, waiter_trace) = (
        trace.connection(false, "holder"),
        trace.connection(true, "waiter"),
    );
    let (holder_conn, waiter_conn) = (holder_trace.id(), waiter_trace.id());
    let (holder, mut holder_rx) = gossip_peer(holder_trace.clone());
    let (waiter, mut waiter_rx) = gossip_peer(waiter_trace.clone());

    let block = hash(1);
    let slots = Arc::new(Semaphore::new(1));
    let broadcast = trace.block_broadcast(block, true, 2, 1, slots.available_permits());

    // The holder takes the only slot and writes its inventory, but its peer never requests the
    // block, so it keeps the slot until the no-request deadline.
    let holder_task =
        tokio::spawn(holder.advertise(block, slots.clone(), broadcast.advert(0, &holder_trace)));
    let inv = holder_rx.next().await.expect("holder receives inventory");
    inv.tx
        .send(Ok(Response::Nil))
        .expect("advertisement awaits the response");

    let waiter_task =
        tokio::spawn(waiter.advertise(block, slots.clone(), broadcast.advert(1, &waiter_trace)));
    // The waiter's inventory reaches its connection once the holder releases the slot.
    let inv = waiter_rx.next().await.expect("waiter receives inventory");
    assert_eq!(inv.request, Request::AdvertiseBlock(block, None));
    tokio::time::sleep(BUSY_CONNECTION).await;
    inv.tx
        .send(Ok(Response::Nil))
        .expect("advertisement awaits the response");

    assert!(holder_task.await.expect("holder finishes"));
    assert!(waiter_task.await.expect("waiter finishes"));
    broadcast.finished(2);

    let rows = rows(&drain(&mut rx), tables::BLOCK_GOSSIP);
    let start = &rows[0];
    assert_eq!(start["event"], "broadcast_start");
    assert_eq!(start["reason"], "mined");
    assert_eq!(start["peers"], "2");
    assert_eq!(start["concurrency"], "1");
    assert_eq!(start["available_slots"], "1");
    let broadcast_id = &start["broadcast_id"];
    assert!(rows.iter().all(|row| &row["broadcast_id"] == broadcast_id));
    assert!(rows.iter().all(|row| row["hash"] == block.to_string()));

    let released = event(&rows, "slot_released", holder_conn);
    assert_eq!(released["release_reason"], "no_request");
    assert_eq!(released["held_us"], "3000000");

    // Broadcast-slot wait.
    let acquired = event(&rows, "slot_acquired", waiter_conn);
    assert_eq!(acquired["wait_us"], "3000000");
    assert_eq!(acquired["position"], "1");
    assert_eq!(acquired["peer"], "waiter");
    // Connection-queue wait.
    assert_eq!(event(&rows, "inv_enqueued", waiter_conn)["wait_us"], "0");
    assert_eq!(
        event(&rows, "inv_written", waiter_conn)["wait_us"],
        "500000"
    );
    assert_eq!(
        event(&rows, "slot_released", waiter_conn)["held_us"],
        "3500000"
    );

    let done = event(&rows, "advert_done", waiter_conn);
    assert_eq!(done["succeeded"], "true");
    assert_eq!(done["elapsed_us"], "6500000");
    let finished = rows.last().expect("broadcast rows");
    assert_eq!(finished["event"], "broadcast_done");
    assert_eq!(finished["succeeded_peers"], "2");
    assert_eq!(finished["elapsed_us"], "6500000");
}

#[tokio::test(start_paused = true)]
async fn written_block_releases_its_slot_after_the_write_grace() {
    let (trace, mut rx) = capture(JsonlTraceTables::from_names(["block_gossip"]).unwrap());
    let conn = trace.connection(false, "uploader");
    let (peer, mut peer_rx) = gossip_peer(conn.clone());
    let uploads = peer.uploads.clone();
    let block = hash(2);
    let slots = Arc::new(Semaphore::new(1));
    let broadcast = trace.block_broadcast(block, false, 1, 1, 1);

    let task = tokio::spawn(peer.advertise(block, slots, broadcast.advert(0, &conn)));
    let inv = peer_rx.next().await.expect("peer receives inventory");
    inv.tx
        .send(Ok(Response::Nil))
        .expect("advertisement awaits the response");
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    uploads.requested(block);
    tokio::time::sleep(Duration::from_millis(200)).await;
    uploads.written(block);
    assert!(task.await.expect("advertisement finishes"));

    let rows = rows(&drain(&mut rx), tables::BLOCK_GOSSIP);
    assert_eq!(rows[0]["reason"], "relay");
    let released = event(&rows, "slot_released", conn.id());
    assert_eq!(released["release_reason"], "write_grace");
    assert_eq!(released["requested_us"], "100000");
    assert_eq!(released["written_us"], "300000");
    assert_eq!(released["held_us"], "1050000");
}

#[tokio::test(start_paused = true)]
async fn cancelled_advertisement_records_its_slot_release() {
    let (trace, mut rx) = capture(JsonlTraceTables::all());
    let conn = trace.connection(false, "cancelled");
    let (peer, mut peer_rx) = gossip_peer(conn.clone());
    let broadcast = trace.block_broadcast(hash(3), false, 1, 1, 1);

    let task = tokio::spawn(peer.advertise(
        hash(3),
        Arc::new(Semaphore::new(1)),
        broadcast.advert(0, &conn),
    ));
    let _inv = peer_rx.next().await.expect("peer receives inventory");
    task.abort();
    assert!(task.await.expect_err("task was aborted").is_cancelled());

    let rows = rows(&drain(&mut rx), tables::BLOCK_GOSSIP);
    assert_eq!(
        event(&rows, "slot_released", conn.id())["release_reason"],
        "cancelled"
    );
    assert_eq!(event(&rows, "advert_done", conn.id())["succeeded"], "false");
}

#[test]
fn request_rows_show_the_request_a_selected_connection_is_already_serving() {
    let (trace, mut rx) = capture(JsonlTraceTables::all());
    let conn = trace.connection(true, "busy");
    let tx_id = transaction::UnminedTxId::Legacy(transaction::Hash([9; 32]));
    let tx_request = Request::TransactionsById([tx_id].into_iter().collect());
    let block_request = Request::BlocksByHash([hash(4)].into_iter().collect());

    // The connection is serving a transaction request.
    let tx_trace = {
        conn.dispatched(1, &tx_request, "maybe", 1);
        conn.stamp_dispatched(&tx_request)
    };
    conn.enqueued(&tx_trace);
    conn.dequeued(&tx_trace, &tx_request, false);
    conn.request_written(None);

    // The peer set routes a block request to the same connection.
    conn.dispatched(2, &block_request, "advertised", 3);
    let block_trace = conn.stamp(Some(2), &block_request);
    conn.enqueued(&block_trace);
    conn.request_finished("ok");
    conn.dequeued(&block_trace, &block_request, false);
    conn.request_written(None);
    conn.request_finished("ok");

    let rows = rows(&drain(&mut rx), tables::P2P_REQUEST);
    let events: Vec<_> = rows.iter().map(|row| row["event"].as_str()).collect();
    assert_eq!(
        events,
        [
            "dispatch", "enqueue", "dequeue", "write", "dispatch", "enqueue", "finish", "dequeue",
            "write", "finish"
        ]
    );
    assert!(rows.iter().all(|row| row["conn"] == conn.id().to_string()
        && row["direction"] == "inbound"
        && row["peer"] == "busy"));

    let dispatch = &rows[4];
    assert_eq!(dispatch["request_id"], "2");
    assert_eq!(dispatch["request"], "BlocksByHash");
    assert_eq!(dispatch["hash"], hash(4).to_string());
    assert_eq!(dispatch["item_count"], "1");
    assert_eq!(dispatch["route"], "advertised");
    assert_eq!(dispatch["candidates"], "3");
    assert_eq!(dispatch["active_request"], "TransactionsById");
    assert_eq!(dispatch["active_request_id"], "1");
    assert!(!dispatch["active_age_us"].is_empty());
    assert_eq!(dispatch["queued"], "0");

    assert_eq!(rows[5]["queued"], "1");
    assert_eq!(rows[6]["request_id"], "1");
    assert_eq!(rows[6]["result"], "ok");
    let dequeue = &rows[7];
    assert_eq!(dequeue["request_id"], "2");
    assert_eq!(dequeue["queued"], "0");
    assert!(!dequeue["queue_us"].is_empty());
    assert_eq!(rows[8]["result"], "ok");
    assert!(!rows[8]["write_us"].is_empty());
    assert!(!rows[9]["elapsed_us"].is_empty());
}

#[test]
fn unstamped_requests_are_active_but_not_queued() {
    let (trace, mut rx) = capture(JsonlTraceTables::all());
    let conn = trace.connection(false, "pinged");
    let ping = Request::Ping(Default::default());

    conn.dequeued(&RequestTrace::default(), &ping, false);
    conn.dispatched(1, &Request::Peers, "p2c", 1);

    let rows = rows(&drain(&mut rx), tables::P2P_REQUEST);
    assert_eq!(rows[0]["event"], "dequeue");
    assert_eq!(rows[0]["request"], "Ping");
    assert!(rows[0]["queue_us"].is_empty());
    assert_eq!(rows[1]["active_request"], "Ping");
    assert!(rows[1]["active_request_id"].is_empty());
    assert_eq!(rows[1]["queued"], "0");
}

#[test]
fn p2p_rows_record_every_inventory_block_hash_and_the_write_completion() {
    let (trace, mut rx) = capture(JsonlTraceTables::from_names(["p2p"]).unwrap());
    let conn = trace.connection(false, "relay");
    let mut items: Vec<InventoryHash> = (1..=8).map(|byte| hash(byte).into()).collect();
    items.push(InventoryHash::Tx(transaction::Hash([9; 32])));
    let inv = Message::Inv(items);

    let send = conn.p2p_send_start(&inv);
    conn.p2p_send_done(send, &Ok(()), None);
    let send = conn.p2p_send_start(&Message::GetAddr);
    conn.p2p_send_done(send, &Err(crate::PeerError::ConnectionSendTimeout), None);
    conn.p2p_recv(&inv);

    let rows = rows(&drain(&mut rx), tables::P2P);
    let events: Vec<_> = rows.iter().map(|row| row["event"].as_str()).collect();
    assert_eq!(
        events,
        ["send_start", "send_done", "send_start", "send_done", "recv"]
    );
    let hashes: Vec<String> =
        serde_json::from_str(&rows[0]["block_hashes"]).expect("block hashes are a JSON list");
    assert_eq!(
        hashes,
        (1..=8)
            .map(|byte| hash(byte).to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(rows[0]["msg"], "inv");
    assert_eq!(rows[0]["item_count"], "9");
    assert_eq!(rows[0]["tx_count"], "1");
    assert_eq!(rows[0]["seq"], "0");
    assert_eq!(rows[0]["direction"], "outbound");
    assert!(rows[0]["write_us"].is_empty());

    assert_eq!(rows[1]["seq"], "0");
    assert_eq!(rows[1]["msg"], "inv");
    assert_eq!(rows[1]["result"], "ok");
    assert!(!rows[1]["write_us"].is_empty());
    assert_eq!(rows[3]["seq"], "1");
    assert_eq!(rows[3]["result"], "error");
    assert!(!rows[3]["error"].is_empty());

    assert_eq!(rows[4]["block_hashes"], rows[0]["block_hashes"]);
    assert!(rows[4]["seq"].is_empty());
}

/// A peer can send up to 50,000 inventory entries, so one row lists a bounded number of hashes
/// and still counts the whole message.
#[test]
fn p2p_rows_limit_the_block_hashes_of_oversized_inventory() {
    let (trace, mut rx) = capture(JsonlTraceTables::from_names(["p2p"]).unwrap());
    let conn = trace.connection(true, "flooder");
    let items: Vec<InventoryHash> = (0..=u16::try_from(p2p::MAX_ROW_BLOCK_HASHES).unwrap())
        .map(|index| {
            let mut bytes = [0; 32];
            bytes[..2].copy_from_slice(&index.to_le_bytes());
            block::Hash(bytes).into()
        })
        .collect();
    let total = items.len();

    conn.p2p_recv(&Message::Inv(items));

    let rows = rows(&drain(&mut rx), tables::P2P);
    let hashes: Vec<String> =
        serde_json::from_str(&rows[0]["block_hashes"]).expect("block hashes are a JSON list");
    assert_eq!(hashes.len(), p2p::MAX_ROW_BLOCK_HASHES);
    assert_eq!(rows[0]["item_count"], total.to_string());
    assert_eq!(rows[0]["tx_count"], "0");
}

#[test]
fn p2p_block_rows_record_hash_height_and_size() {
    use zakura_chain::serialization::{ZcashDeserializeInto, ZcashSerialize};

    let (trace, mut rx) = capture(JsonlTraceTables::all());
    let conn = trace.connection(false, "uploader");
    let block: Arc<block::Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .expect("test vector is a valid block");

    conn.p2p_recv(&Message::Block(block.clone()));

    let row = &rows(&drain(&mut rx), tables::P2P)[0];
    assert_eq!(row["msg"], "block");
    assert_eq!(row["hash"], block.hash().to_string());
    assert_eq!(row["height"], "1");
    assert_eq!(row["body_bytes"], block.zcash_serialized_size().to_string());
}

#[test]
fn unselected_tables_build_no_rows() {
    let (trace, mut rx) = capture(JsonlTraceTables::from_names(["mining"]).unwrap());
    let conn = trace.connection(false, "quiet");

    assert!(conn.p2p_send_start(&Message::GetAddr).is_none());
    conn.p2p_recv(&Message::GetAddr);
    let request = Request::Peers;
    conn.dispatched(1, &request, "p2c", 1);
    conn.enqueued(&conn.stamp(Some(1), &request));
    let mut advert = trace
        .block_broadcast(hash(5), true, 1, 1, 1)
        .advert(0, &conn);
    advert.started();
    advert.finished(true);

    assert!(rx.try_recv().is_err(), "no row is queued");
    assert!(trace.emitter.is_none());

    // Selecting only `block_gossip` leaves the other lifecycle tables empty.
    let (trace, mut rx) = capture(JsonlTraceTables::from_names(["block_gossip"]).unwrap());
    let conn = trace.connection(false, "gossip-only");
    assert!(conn.p2p_send_start(&Message::GetAddr).is_none());
    conn.dispatched(1, &request, "p2c", 1);
    trace.block_broadcast(hash(5), true, 1, 1, 1);
    let written = rx.try_recv().expect("the broadcast row is queued");
    assert_eq!(written.table, tables::BLOCK_GOSSIP.table());
    assert!(rx.try_recv().is_err());
}

#[test]
fn connection_ids_are_unique_without_tracing() {
    let first = LifecycleTrace::noop().connection(false, "a");
    let second = ConnectionTrace::untraced();
    assert_ne!(first.id(), second.id());
    assert!(first.p2p_send_start(&Message::GetAddr).is_none());
}

#[test]
fn p2p_body_upload_rows_record_acknowledgement_or_its_absence() {
    use std::time::Instant;

    use crate::peer::tcp_progress::{TcpInfo, UploadOutcome, UploadReport, UploadSample};

    let (trace, mut rx) = capture(JsonlTraceTables::from_names(["p2p"]).unwrap());
    let conn = trace.connection(false, "uploader");
    let start = TcpInfo {
        total_retrans: 2,
        ..TcpInfo::default()
    };
    let last = TcpInfo {
        bytes_acked: 1_000,
        notsent_bytes: Some(0),
        unacked: 0,
        total_retrans: 5,
        snd_cwnd: 10,
        rtt_us: 1_500,
        rttvar_us: 100,
        delivery_rate: Some(2_000_000),
        snd_mss: 1_448,
    };
    let report = |outcome, acked_message_bytes| UploadReport {
        outcome,
        started_at: Instant::now(),
        elapsed: Duration::from_millis(250),
        message_len: 1_000,
        start,
        sample_count: 5,
        last_sample: Some(UploadSample {
            elapsed: Duration::from_millis(250),
            info: last,
            acked_message_bytes,
        }),
    };

    conn.p2p_body_upload(3, &report(UploadOutcome::Acked, 1_000));
    conn.p2p_body_upload(4, &report(UploadOutcome::Closed, 400));

    let rows = rows(&drain(&mut rx), tables::P2P);
    assert_eq!(rows[0]["event"], "body_acked");
    assert_eq!(rows[0]["seq"], "3");
    assert_eq!(rows[0]["msg"], "block");
    assert_eq!(rows[0]["ack_us"], "250000");
    assert_eq!(rows[0]["acked_bytes"], "1000");
    assert_eq!(rows[0]["tcp_retrans"], "3");
    assert_eq!(rows[0]["tcp_total_retrans"], "5");
    assert_eq!(rows[0]["tcp_delivery_rate"], "2000000");
    assert_eq!(rows[0]["samples"], "5");
    assert_eq!(rows[0]["result"], "acked");

    assert_eq!(rows[1]["event"], "body_ack_timeout");
    assert_eq!(rows[1]["result"], "closed");
    assert!(rows[1]["ack_us"].is_empty());
    assert_eq!(rows[1]["acked_bytes"], "400");
}
