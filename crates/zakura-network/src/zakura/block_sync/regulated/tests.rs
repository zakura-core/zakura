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

struct Store {
    blocks: Vec<(block::Height, Arc<block::Block>, usize)>,
    started: Arc<Semaphore>,
    release: Arc<Semaphore>,
}

impl Source for Store {
    fn read(&self, request: Read) -> BoxFuture<'static, Result<ReadResult, crate::BoxError>> {
        let blocks = self.blocks.clone();
        let started = self.started.clone();
        let release = self.release.clone();
        Box::pin(async move {
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
