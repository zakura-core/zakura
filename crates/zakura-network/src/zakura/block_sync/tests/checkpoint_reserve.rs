//! A worst-case checkpoint range commits on the `Consensus` reserve alone.
//!
//! The node's pool comes from a default Mainnet `MemoryPlan`, and the test
//! leases every bulk byte block sync could use, so `Consumer::BlockSync` has
//! nothing to give. The first range ends at a checkpoint at [`CHECKPOINT`] and
//! holds the most bytes the checkpoint rule allows: bodies just under
//! `MAX_CHECKPOINT_BYTE_COUNT` in all, then a near-maximum body. Headers
//! advertise no size, so every request reserves a maximum block, and one
//! range's requests could ask for more than the reserve holds. A silent peer
//! takes requests and never answers, so its leases stay held until they
//! expire. The test plays the checkpoint verifier: it holds every submission
//! up to the next checkpoint until the whole range is submitted, then commits
//! it in order. Above the last checkpoint each block commits on its own.

use std::{collections::BTreeMap, sync::Mutex as StdMutex};

use zakura_chain::{
    amount::Amount,
    parameters::checkpoint::{constants::MAX_CHECKPOINT_BYTE_COUNT, list::CheckpointList},
};

use super::*;
use crate::zakura::memory::{Consumer, PoolEvent, PoolObserver};

/// The checkpoint that ends the worst-case range, and the last checkpoint.
const CHECKPOINT: u32 = 400;

/// [`CHECKPOINT`]'s block in the chain, which starts at height 1.
// 400 fits `usize`.
const CHECKPOINT_INDEX: usize = CHECKPOINT as usize - 1;

/// Blocks above genesis: the range, then a few semantic heights above it.
const BLOCKS: u32 = CHECKPOINT + 10;

/// The peak `Consensus` lease total over the pool's events, and how many `BlockSync`
/// leases the pool granted.
#[derive(Debug, Default)]
struct LeaseRecord {
    consensus_peak: u64,
    block_sync_grants: u64,
}

/// Folds the pool's events into a [`LeaseRecord`].
#[derive(Debug, Default)]
struct RecordingObserver(StdMutex<LeaseRecord>);

impl PoolObserver for RecordingObserver {
    fn observe(&self, event: PoolEvent) {
        let mut record = self.0.lock().expect("unpoisoned");
        match event {
            PoolEvent::Granted {
                consumer,
                bytes,
                consumer_leased,
                ..
            } => {
                if consumer == Consumer::Consensus {
                    record.consensus_peak = record.consensus_peak.max(consumer_leased);
                } else if consumer == Consumer::BlockSync && bytes > 0 {
                    record.block_sync_grants += 1;
                }
            }
            PoolEvent::Resized {
                consumer: Consumer::Consensus,
                consumer_leased,
                ..
            } => record.consensus_peak = record.consensus_peak.max(consumer_leased),
            _ => {}
        }
    }
}

/// `template` at `height`, padded with an output script so the block
/// serializes to about `bytes`.
fn padded_block_at_height(
    template: &Arc<block::Block>,
    height: u32,
    bytes: u32,
) -> Arc<block::Block> {
    let mut padded = template.as_ref().clone();
    let pad = bytes.saturating_sub(block_size(template) + 16);
    let coinbase = Arc::make_mut(&mut padded.transactions[0]);
    coinbase.outputs_mut().push(transparent::Output {
        value: Amount::zero(),
        // Test sizes stay below `MAX_BLOCK_BYTES`, which fits `usize`.
        lock_script: transparent::Script::new(&vec![0; pad as usize]),
    });
    fake_block_at_height(&Arc::new(padded), block::Height(height))
}

/// The worst-case range below [`CHECKPOINT`], then small blocks above it.
fn worst_case_chain() -> Vec<Arc<block::Block>> {
    let template = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    // Every body but the last keeps the range under the byte rule.
    let body = u32::try_from(MAX_CHECKPOINT_BYTE_COUNT / u64::from(CHECKPOINT)).unwrap() - 1_000;
    let last = u32::try_from(block::MAX_BLOCK_BYTES).unwrap() - 10_000;
    (1..=BLOCKS)
        .map(|height| match height {
            CHECKPOINT => padded_block_at_height(&template, height, last),
            height if height < CHECKPOINT => padded_block_at_height(&template, height, body),
            height => fake_block_at_height(&template, block::Height(height)),
        })
        .collect()
}

/// Serves every requested body and ends each request, or never answers.
async fn play(
    serve: bool,
    blocks: Arc<Vec<Arc<block::Block>>>,
    inbound: FramedSend,
    mut outbound: FramedRecv,
) {
    while let Some(frame) = outbound.recv().await {
        let Ok(BlockSyncMessage::GetBlocks {
            start_height,
            count,
        }) = BlockSyncMessage::decode_frame(frame)
        else {
            continue;
        };
        if !serve {
            continue;
        }
        let mut replies: Vec<_> = (start_height.0..start_height.0.saturating_add(count))
            // Heights start at 1 and stay within the test chain, and a `u32`
            // fits `usize`.
            .map(|height| BlockSyncMessage::Block(blocks[height as usize - 1].clone()))
            .collect();
        replies.push(BlockSyncMessage::BlocksDone {
            start_height,
            returned: count,
        });
        for reply in replies {
            let frame = reply.encode_frame().expect("a test reply encodes");
            if inbound.send(frame).await.is_err() {
                return;
            }
        }
    }
}

/// With `Consumer::BlockSync` holding all shared space, the node fetches,
/// decodes and commits a worst-case checkpoint range and the heights above
/// it, leasing only from the `Consensus` reserve.
#[tokio::test(start_paused = true)]
async fn a_worst_case_checkpoint_range_commits_with_bulk_memory_full() {
    let blocks = Arc::new(worst_case_chain());
    let range_bytes: u64 = blocks[..=CHECKPOINT_INDEX]
        .iter()
        .map(|block| u64::from(block_size(block)))
        .sum();
    let last_body = u64::from(block_size(&blocks[CHECKPOINT_INDEX]));
    assert!(
        range_bytes - last_body < MAX_CHECKPOINT_BYTE_COUNT
            && range_bytes >= MAX_CHECKPOINT_BYTE_COUNT,
        "the generator would put a checkpoint at {CHECKPOINT}: {range_bytes} bytes"
    );
    let tip = blocks.last().expect("the chain has bodies").hash();
    let metas: Vec<_> = blocks
        .iter()
        .map(|block| BlockSyncBlockMeta {
            size: BlockSizeEstimate::Unknown,
            ..block_meta(block)
        })
        .collect();

    // A default Mainnet node with 256 MiB of shared space, all of it leased
    // by block sync's bulk consumer.
    let config = crate::Config::default();
    let minimum = crate::zakura::memory::MemoryPlan::size(256 << 30, &config, &config.network)
        .expect("a large budget fits")
        .required_minimum_bytes();
    let plan =
        crate::zakura::memory::MemoryPlan::size(minimum + (256 << 20), &config, &config.network)
            .expect("the minimum plus shared space fits");
    let observer = Arc::new(RecordingObserver::default());
    let pool = plan.build_pool().with_observer(Some(observer.clone()));
    let bulk = pool
        .try_acquire(
            Consumer::BlockSync,
            None,
            pool.available_for(Consumer::BlockSync),
        )
        .expect("block sync can lease its floor and the shared space");
    let snapshot = pool.snapshot();
    assert_eq!(snapshot.shared_used, snapshot.shared_capacity);
    assert!(snapshot.shared_capacity > 0);
    assert_eq!(pool.available_for(Consumer::BlockSync), 0);
    let reserve = pool.available_for(Consumer::Consensus);
    assert_eq!(reserve, BS_MAX_DECODED_SUBMISSION_WINDOW_BYTES);
    assert!(
        u64::from(CHECKPOINT) * block::MAX_BLOCK_BYTES > reserve,
        "unhinted requests for one range could spend the reserve"
    );

    let checkpoints = Arc::new(
        CheckpointList::from_list([
            (block::Height(0), block::Hash([1; 32])),
            (block::Height(CHECKPOINT), blocks[CHECKPOINT_INDEX].hash()),
        ])
        .expect("the test list is valid"),
    );
    let block_sync_config = immediate_body_download_config();
    let (_tip_tx, tip_rx) = watch::channel((block::Height(BLOCKS), tip));
    let mut startup = header_tip_startup(
        genesis_frontiers(),
        (block::Height(BLOCKS), tip),
        tip_rx,
        block_sync_config.clone(),
    );
    startup.memory = crate::zakura::BlockSyncMemory::new(pool.clone(), None);
    startup.max_checkpoint_height = Some(block::Height(CHECKPOINT));
    startup.commit_window = crate::zakura::CommitWindow::new(checkpoints.clone());
    let (handle, mut actions, reactor_task) =
        spawn_block_sync_reactor(startup).expect("valid block-sync config");
    let service = BlockSyncService::new_with_handle_for_test(block_sync_config, handle.clone());

    // The silent peer sorts first, so it takes requests before the honest one.
    for (byte, serve) in [(0x10, false), (0xF0, true)] {
        let (inbound_tx, inbound_rx) = framed_channel(16);
        let (outbound_tx, outbound_rx) = framed_channel(16);
        let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
        service.add_peer(Peer::new_with_direction(
            peer(byte),
            None,
            ZAKURA_CAP_BLOCK_SYNC_REGULATED,
            ServicePeerDirection::Outbound,
            streams,
            CancellationToken::new(),
        ));
        let status = BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(BLOCKS),
            tip_hash: tip,
            max_blocks_per_response: 1,
            max_inflight_requests: 64,
            max_response_bytes: MAX_BS_RESPONSE_BYTES,
        };
        inbound_tx
            .send(
                BlockSyncMessage::Status(status)
                    .encode_frame()
                    .expect("status encodes"),
            )
            .await
            .expect("the peer's stream is open");
        tokio::spawn(play(serve, blocks.clone(), inbound_tx, outbound_rx));
    }
    handle
        .send(BlockSyncEvent::NeededBlocks(metas.clone()))
        .await
        .expect("the reactor runs");

    // Play the verifiers: hold submissions until every height up to the next
    // checkpoint, or the next height above the last one, is submitted.
    let state = tokio::spawn(async move {
        let mut held = BTreeMap::new();
        let mut verified = 0;
        while verified < BLOCKS {
            match actions.recv().await.expect("the reactor runs") {
                BlockSyncAction::QueryNeededBlocks { .. } => {
                    // Heights start at 1, so height `h` is at index `h - 1`,
                    // and a `u32` fits `usize`.
                    let needed = metas[verified as usize..].to_vec();
                    handle
                        .send(BlockSyncEvent::NeededBlocks(needed))
                        .await
                        .expect("the reactor runs");
                }
                BlockSyncAction::SubmitBlock {
                    owner,
                    source,
                    token,
                    block,
                } => {
                    let height = block.coinbase_height().expect("a test body has a height");
                    held.insert(height.0, (owner, source, token, block.hash()));
                }
                _ => {}
            }
            let target = checkpoints
                .checkpoint_after(block::Height(verified))
                .map_or(verified + 1, |(height, _)| height.0);
            if !(verified + 1..=target).all(|height| held.contains_key(&height)) {
                continue;
            }
            for height in verified + 1..=target {
                let (owner, source, token, hash) =
                    held.remove(&height).expect("every height was submitted");
                handle
                    .send(BlockSyncEvent::BlockApplyFinished {
                        owner,
                        source,
                        token,
                        height: block::Height(height),
                        hash,
                        outcome: test_block_apply_outcome(BlockApplyResult::Committed),
                    })
                    .await
                    .expect("the reactor runs");
                handle
                    .send(BlockSyncEvent::ChainTipGrow(BlockSyncFrontiers {
                        finalized_height: block::Height(0),
                        verified_block_tip: block::Height(height),
                        verified_block_hash: hash,
                    }))
                    .await
                    .expect("the reactor runs");
            }
            verified = target;
        }
    });
    time::timeout(Duration::from_secs(600), state)
        .await
        .expect("every block commits")
        .expect("the state player runs");

    // Read the record before any later lease change reports to the observer.
    let record = std::mem::take(&mut *observer.0.lock().expect("unpoisoned"));
    assert!(
        record.consensus_peak >= range_bytes,
        "the whole range was retained on Consensus at once: {record:?}"
    );
    assert!(record.consensus_peak <= reserve, "{record:?}");
    assert_eq!(
        record.block_sync_grants, 1,
        "only the test's own lease came from block sync: {record:?}"
    );
    drop(bulk);
    reactor_task.abort();
}
