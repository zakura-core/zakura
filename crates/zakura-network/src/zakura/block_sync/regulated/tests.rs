use std::{sync::Arc, time::Duration};

use futures::future::BoxFuture;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use zakura_chain::{block, serialization::ZcashDeserializeInto};
use zakura_test::vectors::{BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_2_BYTES};

use super::{
    serving::{Read, ReadResult, Server, Source},
    wire::{Message, Range, GET_BLOCKS},
};
use crate::zakura::{
    block_sync::{BlockSyncMessage, BlockSyncStatus},
    framed_channel,
    regulation::{ServeCapacity, ServeLimits},
    wire_codec::{decode_frame, encode_frame, WireMessage},
    Frame, ZakuraPeerId,
};

#[derive(Debug)]
struct Store {
    blocks: Vec<(block::Height, Arc<block::Block>, usize)>,
    started: Arc<Semaphore>,
    release: Arc<Semaphore>,
    fail: bool,
}

impl Source for Store {
    fn read(&self, request: Read) -> BoxFuture<'static, Result<ReadResult, crate::BoxError>> {
        let blocks = self.blocks.clone();
        let fail = self.fail;
        let started = self.started.clone();
        let release = self.release.clone();
        Box::pin(async move {
            if fail {
                return Err("storage failure".into());
            }
            Ok(tokio::task::spawn_blocking(move || {
                if !request.lease.try_start() {
                    return ReadResult {
                        blocks: vec![],
                        lease: request.lease,
                    };
                }
                started.add_permits(1);
                futures::executor::block_on(release.acquire())
                    .expect("test gate stays open")
                    .forget();
                ReadResult {
                    blocks,
                    lease: request.lease,
                }
            })
            .await?)
        })
    }
}

fn store(blocks: &[&[u8]], open: bool) -> Arc<Store> {
    Arc::new(Store {
        blocks: blocks
            .iter()
            .enumerate()
            .map(|(index, bytes)| {
                (
                    block::Height(u32::try_from(index + 1).unwrap()),
                    Arc::new(bytes.zcash_deserialize_into().unwrap()),
                    bytes.len(),
                )
            })
            .collect(),
        started: Arc::new(Semaphore::new(0)),
        release: Arc::new(Semaphore::new(if open { 128 } else { 0 })),
        fail: false,
    })
}

fn capacity(body_bytes: u32) -> ServeCapacity {
    let bytes = Range::new(block::Height(1), 128)
        .unwrap()
        .response_cap(body_bytes)
        .output_bytes();
    ServeCapacity::new(
        "block-sync",
        &GET_BLOCKS,
        ServeLimits {
            node_execution: 1,
            peer_execution: 1,
            peer_output_bytes: bytes * 2,
            node_output_bytes: bytes * 4,
        },
    )
    .unwrap()
}

async fn receive(recv: &mut crate::zakura::FramedRecv) -> Message {
    let frame = tokio::time::timeout(Duration::from_secs(5), recv.recv())
        .await
        .unwrap()
        .unwrap();
    decode_frame(&frame).unwrap()
}

#[test]
fn envelopes_preserve_version_two_bytes() {
    let range = Range::new(block::Height(1), 2).unwrap();
    let block = Arc::new(
        BLOCK_MAINNET_1_BYTES
            .as_slice()
            .zcash_deserialize_into()
            .unwrap(),
    );
    for (new, old) in [
        (
            Message::Status(BlockSyncStatus::default()),
            BlockSyncMessage::Status(BlockSyncStatus::default()),
        ),
        (
            Message::GetBlocks(range),
            BlockSyncMessage::GetBlocks {
                start_height: range.start,
                count: range.count,
            },
        ),
        (
            Message::Block(BLOCK_MAINNET_1_BYTES.to_vec()),
            BlockSyncMessage::Block(block),
        ),
        (
            Message::BlocksDone {
                start: range.start,
                returned: 1,
            },
            BlockSyncMessage::BlocksDone {
                start_height: range.start,
                returned: 1,
            },
        ),
        (
            Message::RangeUnavailable(range),
            BlockSyncMessage::RangeUnavailable {
                start_height: range.start,
                count: range.count,
            },
        ),
    ] {
        let frame = encode_frame(&new).unwrap();
        assert_eq!(frame, old.encode_frame().unwrap());
        assert_eq!(decode_frame::<Message>(&frame).unwrap(), new);
    }
}

#[test]
fn reject_ranges_and_envelopes_before_block_decoding() {
    assert!(Range::new(block::Height::MAX, 2).is_err());
    assert!(Range::new(block::Height(1), 0).is_err());
    assert!(Range::new(block::Height(1), 129).is_err());
    let frame = Frame {
        message_type: 3,
        flags: 0,
        payload: vec![2, 0],
    };
    assert!(decode_frame::<Message>(&frame).is_err());
    for row in Message::RULES {
        let frame = Frame {
            message_type: row.message_type,
            flags: 0,
            payload: vec![0; row.payload.max() + 1],
        };
        assert!(decode_frame::<Message>(&frame).is_err());
    }
}

#[tokio::test]
async fn body_limit_excludes_tags_and_ending_and_stops_at_a_prefix() {
    let source = store(&[&BLOCK_MAINNET_1_BYTES, &BLOCK_MAINNET_2_BYTES], true);
    let limit = u32::try_from(BLOCK_MAINNET_1_BYTES.len()).unwrap();
    let capacity = capacity(limit);
    let (send, mut recv) = framed_channel(1);
    let cancel = CancellationToken::new();
    let server = capacity.session(
        Arc::new(Server::new(source, 128, limit).unwrap()),
        &ZakuraPeerId::new(vec![1; 32]).unwrap(),
        2,
        send,
        cancel.clone(),
    );
    server
        .admit(Range::new(block::Height(1), 2).unwrap())
        .unwrap();
    assert_eq!(
        receive(&mut recv).await,
        Message::Block(BLOCK_MAINNET_1_BYTES.to_vec())
    );
    assert_eq!(
        receive(&mut recv).await,
        Message::BlocksDone {
            start: block::Height(1),
            returned: 1
        }
    );
    cancel.cancel();
}

#[tokio::test]
async fn too_small_body_budget_returns_original_unavailable_range() {
    let source = store(&[&BLOCK_MAINNET_1_BYTES], true);
    let capacity = capacity(1);
    let (send, mut recv) = framed_channel(1);
    let cancel = CancellationToken::new();
    let server = capacity.session(
        Arc::new(Server::new(source, 1, 1).unwrap()),
        &ZakuraPeerId::new(vec![2; 32]).unwrap(),
        2,
        send,
        cancel.clone(),
    );
    let range = Range::new(block::Height(1), 128).unwrap();
    server.admit(range).unwrap();
    assert_eq!(receive(&mut recv).await, Message::RangeUnavailable(range));
    cancel.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_keeps_storage_execution_until_the_actual_job_finishes() {
    let source = store(&[], false);
    let _release_on_drop = ReleaseOnDrop(source.release.clone());
    let capacity = capacity(1);
    let (send, _recv) = framed_channel(1);
    let cancel = CancellationToken::new();
    let producer = Arc::new(Server::new(source.clone(), 1, 1).unwrap());
    let first = capacity.session(
        producer.clone(),
        &ZakuraPeerId::new(vec![3; 32]).unwrap(),
        1,
        send,
        cancel.clone(),
    );
    let range = Range::new(block::Height(1), 1).unwrap();
    first.admit(range).unwrap();
    tokio::time::timeout(Duration::from_secs(5), source.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    cancel.cancel();
    assert_eq!(capacity.node_execution_held(), 1);
    let (send, mut recv) = framed_channel(1);
    let second_cancel = CancellationToken::new();
    let second = capacity.session(
        producer,
        &ZakuraPeerId::new(vec![4; 32]).unwrap(),
        1,
        send,
        second_cancel.clone(),
    );
    second.admit(range).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), source.started.acquire())
            .await
            .is_err()
    );
    source.release.add_permits(2);
    assert_eq!(receive(&mut recv).await, Message::RangeUnavailable(range));
    second_cancel.cancel();
}

struct ReleaseOnDrop(Arc<Semaphore>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.add_permits(128);
    }
}

#[test]
fn status_values_are_rejected_instead_of_silently_clamped() {
    for status in [
        BlockSyncStatus {
            max_blocks_per_response: 0,
            ..Default::default()
        },
        BlockSyncStatus {
            max_inflight_requests: 32_769,
            ..Default::default()
        },
        BlockSyncStatus {
            max_response_bytes: 33_554_433,
            ..Default::default()
        },
        BlockSyncStatus {
            servable_low: block::Height(2),
            servable_high: block::Height(1),
            ..Default::default()
        },
    ] {
        assert!(encode_frame(&Message::Status(status)).is_err());
    }
    let mut frame = BlockSyncMessage::Status(BlockSyncStatus::default())
        .encode_frame()
        .unwrap();
    frame.payload[41..45].copy_from_slice(&0_u32.to_le_bytes());
    assert!(decode_frame::<Message>(&frame).is_err());
}

#[tokio::test]
async fn a_storage_failure_ends_without_faulting_or_cancelling_the_session() {
    let mut source = store(&[], true);
    Arc::get_mut(&mut source).unwrap().fail = true;
    let capacity = capacity(1);
    let (send, mut recv) = framed_channel(1);
    let cancel = CancellationToken::new();
    let server = capacity.session(
        Arc::new(Server::new(source, 1, 1).unwrap()),
        &ZakuraPeerId::new(vec![5; 32]).unwrap(),
        1,
        send,
        cancel.clone(),
    );
    let range = Range::new(block::Height(1), 1).unwrap();
    server.admit(range).unwrap();
    assert_eq!(receive(&mut recv).await, Message::RangeUnavailable(range));
    assert!(!cancel.is_cancelled());
    cancel.cancel();
}

#[test]
fn envelope_allocations_are_bounded_by_the_payload() {
    for frame in [
        encode_frame(&Message::GetBlocks(
            Range::new(block::Height(1), 128).unwrap(),
        ))
        .unwrap(),
        encode_frame(&Message::Block(BLOCK_MAINNET_1_BYTES.to_vec())).unwrap(),
        Frame {
            message_type: 3,
            flags: 0,
            payload: vec![2; 2_000_001],
        },
    ] {
        let (_, allocated) = zakura_test::allocations::measure(|| decode_frame::<Message>(&frame));
        assert!(
            allocated.peak_live_bytes
                <= Message::max_heap_bytes(frame.message_type, frame.payload.len())
        );
    }
}

struct LiveServing {
    service: crate::zakura::BlockSyncService,
    input: crate::zakura::FramedSend,
    output: crate::zakura::FramedRecv,
    cancel: CancellationToken,
}

impl Drop for LiveServing {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

fn live_serving(source: Arc<Store>, max_bytes: u32) -> LiveServing {
    use crate::zakura::{
        BlockSyncService, Peer, Service, ServicePeerDirection, ZakuraBlockSyncConfig,
        ZAKURA_CAP_BLOCK_SYNC, ZAKURA_STREAM_BLOCK_SYNC,
    };
    let config = ZakuraBlockSyncConfig {
        max_response_bytes: max_bytes,
        peer_limits: crate::zakura::ServicePeerLimits {
            max_outbound_peers: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let cancel = CancellationToken::new();
    let mut startup =
        super::super::BlockSyncStartup::inert(config.clone()).with_range_source(source);
    startup.shutdown = cancel.clone();
    let (handle, _actions, _task) = super::super::spawn_block_sync_reactor(startup);
    let service = BlockSyncService::new_with_handle(
        config,
        handle,
        zakura_chain::serialization::ZcashDecoder::for_network(
            &zakura_chain::parameters::Network::Mainnet,
        ),
    );
    let resources = service
        .reserve_session(ServicePeerDirection::Outbound)
        .unwrap()
        .unwrap();
    resources.admitted();
    let (input, recv) = framed_channel(4);
    let (send, output) = framed_channel(4);
    let send = send.with_session_resources(Some(resources));
    service.add_peer(Peer::new_with_direction(
        ZakuraPeerId::new(vec![11; 32]).unwrap(),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        std::collections::HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (recv, send))]),
        cancel.clone(),
    ));
    LiveServing {
        service,
        input,
        output,
        cancel,
    }
}

async fn next_response(output: &mut crate::zakura::FramedRecv) -> Message {
    loop {
        let message = receive(output).await;
        if !matches!(message, Message::Status(_)) {
            return message;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_routine_serves_bounded_prefixes_and_allows_reuse_after_each_ending() {
    let source = store(&[&BLOCK_MAINNET_1_BYTES, &BLOCK_MAINNET_2_BYTES], true);
    let _release_on_drop = ReleaseOnDrop(source.release.clone());
    let mut live = live_serving(
        source.clone(),
        u32::try_from(BLOCK_MAINNET_1_BYTES.len()).unwrap(),
    );
    for _ in 0..32 {
        source.release.add_permits(1);
        live.input
            .send(
                BlockSyncMessage::GetBlocks {
                    start_height: block::Height(1),
                    count: 2,
                }
                .encode_frame()
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            next_response(&mut live.output).await,
            Message::Block(BLOCK_MAINNET_1_BYTES.to_vec())
        );
        assert_eq!(
            next_response(&mut live.output).await,
            Message::BlocksDone {
                start: block::Height(1),
                returned: 1
            }
        );
        assert!(!live.cancel.is_cancelled());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_routine_rejects_overlaps_and_retains_the_session_until_storage_stops() {
    use crate::zakura::{Service, ServicePeerDirection};
    let source = store(&[], false);
    let _release_on_drop = ReleaseOnDrop(source.release.clone());
    let live = live_serving(source.clone(), 1);
    live.input
        .send(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(1),
                count: 2,
            }
            .encode_frame()
            .unwrap(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), source.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    live.input
        .send(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(2),
                count: 1,
            }
            .encode_frame()
            .unwrap(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), live.cancel.cancelled())
        .await
        .unwrap();
    assert!(live
        .service
        .reserve_session(ServicePeerDirection::Outbound)
        .is_err());
    assert_eq!(
        source.started.available_permits(),
        0,
        "the overlap starts no second read"
    );
    source.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if live
                .service
                .reserve_session(ServicePeerDirection::Outbound)
                .is_ok()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[derive(Debug)]
struct TimedOutRead {
    started: Arc<Semaphore>,
    release: Arc<Semaphore>,
}

impl Source for TimedOutRead {
    fn read(&self, request: Read) -> BoxFuture<'static, Result<ReadResult, crate::BoxError>> {
        let started = self.started.clone();
        let release = self.release.clone();
        Box::pin(async move {
            let entered = started.clone();
            tokio::task::spawn_blocking(move || {
                assert!(request.lease.try_start());
                entered.add_permits(1);
                futures::executor::block_on(release.acquire())
                    .unwrap()
                    .forget();
                drop(request.lease);
            });
            started.acquire().await.unwrap().forget();
            Err("state read timed out while its blocking job is still running".into())
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timed_out_read_keeps_the_same_peer_budget_after_its_output_drains() {
    let source = Arc::new(TimedOutRead {
        started: Arc::new(Semaphore::new(0)),
        release: Arc::new(Semaphore::new(0)),
    });
    let _release_on_drop = ReleaseOnDrop(source.release.clone());
    let capacity = capacity(1);
    let peer = ZakuraPeerId::new(vec![12; 32]).unwrap();
    let (send, mut recv) = framed_channel(1);
    let cancel = CancellationToken::new();
    let serving = capacity.session(
        Arc::new(Server::new(source.clone(), 1, 1).unwrap()),
        &peer,
        1,
        send,
        cancel.clone(),
    );
    let range = Range::new(block::Height(1), 1).unwrap();
    serving.admit(range).unwrap();
    assert_eq!(receive(&mut recv).await, Message::RangeUnavailable(range));
    cancel.cancel();
    drop(serving);
    tokio::time::timeout(Duration::from_secs(5), async {
        while capacity.node_output_held() != 0 {
            tokio::task::yield_now().await;
        }
        assert!(
            recv.recv().await.is_none(),
            "the retired session drops its writer"
        );
    })
    .await
    .unwrap();
    assert_eq!(capacity.node_execution_held(), 1);
    assert_eq!(
        capacity.peer_held(&peer),
        (1, 0),
        "a reconnect must find the still-running peer execution budget"
    );
}
