use std::{
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc,
    },
    time::Duration,
};

use futures::FutureExt;
use tokio::{sync::oneshot, time::timeout};
use zakura_chain::{
    block,
    parameters::Network,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
};

use super::spawn_owned_block_range;

const DEADLINE: Duration = Duration::from_secs(10);

#[derive(Debug)]
struct DropSignal {
    drops: Arc<AtomicUsize>,
    finished: Option<oneshot::Sender<()>>,
}

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if let Some(finished) = self.finished.take() {
            let _ = finished.send(());
        }
    }
}

fn resources() -> (DropSignal, Arc<AtomicUsize>, oneshot::Receiver<()>) {
    let drops = Arc::new(AtomicUsize::new(0));
    let (finished, receiver) = oneshot::channel();
    (
        DropSignal {
            drops: drops.clone(),
            finished: Some(finished),
        },
        drops,
        receiver,
    )
}

fn genesis() -> Arc<block::Block> {
    Arc::new(
        zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .expect("the genesis fixture is a serialized block"),
    )
}

#[tokio::test]
async fn returned_blocks_retain_resources_and_respect_the_byte_cap() {
    let _guard = zakura_test::init();
    let (resources, drops, _finished) = resources();
    let block = genesis();
    let reads = Arc::new(AtomicUsize::new(0));
    let worker_reads = reads.clone();
    let result = timeout(
        DEADLINE,
        spawn_owned_block_range(
            block::Height(1),
            3,
            5,
            resources,
            |_| false,
            move |_| {
                worker_reads.fetch_add(1, Ordering::SeqCst);
                // Synthetic sizes isolate the response cap from fixture size.
                Some((block.clone(), 2))
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 3);
    assert_eq!(
        result
            .blocks()
            .iter()
            .map(|(height, _, _)| *height)
            .collect::<Vec<_>>(),
        [block::Height(1), block::Height(2)]
    );
    assert_eq!(result.resources().drops.load(Ordering::SeqCst), 0);
    let (blocks, resources) = result.into_parts();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(blocks);
    drop(resources);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancellation_before_the_first_lookup_skips_the_range() {
    let _guard = zakura_test::init();
    let (resources, drops, _finished) = resources();
    let result = timeout(
        DEADLINE,
        spawn_owned_block_range(
            block::Height(1),
            2,
            10,
            resources,
            |_| true,
            |_| panic!("cancelled work must not reach its first lookup"),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(result.blocks().is_empty());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(result);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancellation_between_lookups_retains_the_completed_prefix() {
    let _guard = zakura_test::init();
    let (resources, drops, _finished) = resources();
    let cancelled = Arc::new(AtomicBool::new(false));
    let worker_cancelled = cancelled.clone();
    let block = genesis();
    let result = timeout(
        DEADLINE,
        spawn_owned_block_range(
            block::Height(1),
            2,
            10,
            resources,
            move |_| cancelled.load(Ordering::SeqCst),
            move |_| {
                assert!(!worker_cancelled.swap(true, Ordering::SeqCst));
                Some((block.clone(), 2))
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.blocks().len(), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(result);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn dropping_the_waiter_keeps_a_running_read_charged() {
    let _guard = zakura_test::init();
    let (resources, drops, finished) = resources();
    let (started_tx, started) = oneshot::channel();
    let mut started_tx = Some(started_tx);
    let (resume, blocked) = mpsc::sync_channel(1);
    let block = genesis();
    let job = spawn_owned_block_range(
        block::Height(1),
        1,
        10,
        resources,
        |_| false,
        move |_| {
            started_tx.take().unwrap().send(()).unwrap();
            blocked.recv_timeout(DEADLINE).unwrap();
            Some((block.clone(), 2))
        },
    );
    timeout(DEADLINE, started).await.unwrap().unwrap();
    drop(job);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    resume.send(()).unwrap();
    timeout(DEADLINE, finished).await.unwrap().unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn aborting_the_caller_keeps_a_running_read_charged() {
    let _guard = zakura_test::init();
    let (resources, drops, finished) = resources();
    let (started_tx, started) = oneshot::channel();
    let mut started_tx = Some(started_tx);
    let (resume, blocked) = mpsc::sync_channel(1);
    let block = genesis();
    let caller = tokio::spawn(async move {
        spawn_owned_block_range(
            block::Height(1),
            1,
            10,
            resources,
            |_| false,
            move |_| {
                started_tx.take().unwrap().send(()).unwrap();
                blocked.recv_timeout(DEADLINE).unwrap();
                Some((block.clone(), 2))
            },
        )
        .await
    });
    timeout(DEADLINE, started).await.unwrap().unwrap();
    caller.abort();
    assert!(timeout(DEADLINE, caller)
        .await
        .unwrap()
        .unwrap_err()
        .is_cancelled());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    resume.send(()).unwrap();
    timeout(DEADLINE, finished).await.unwrap().unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_panicking_read_releases_resources_once() {
    let _guard = zakura_test::init();
    let (resources, drops, finished) = resources();
    let job = spawn_owned_block_range(
        block::Height(1),
        1,
        10,
        resources,
        |_| false,
        |_| panic!("injected database panic"),
    );
    let outcome = timeout(DEADLINE, AssertUnwindSafe(job).catch_unwind())
        .await
        .unwrap();
    assert!(outcome.is_err());
    timeout(DEADLINE, finished).await.unwrap().unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn the_state_api_returns_an_owned_empty_range() {
    let _guard = zakura_test::init();
    let (resources, drops, _finished) = resources();
    let (_state, mut read_state, _tip, _change) =
        timeout(DEADLINE, crate::init_test_services(&Network::Mainnet))
            .await
            .unwrap();
    let result = timeout(
        DEADLINE,
        read_state.read_owned_block_range(block::Height(1), 1, 10, resources, |_| false),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(result.blocks().is_empty());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(result);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn the_state_api_returns_committed_blocks_with_their_resources() {
    let _guard = zakura_test::init();
    let (resources, drops, _finished) = resources();
    let block = genesis();
    let size = block.zcash_serialized_size();
    let (_state, mut read_state, _tip, _change) = timeout(
        DEADLINE,
        crate::populated_state([block.clone()], &Network::Mainnet),
    )
    .await
    .unwrap();
    let result = timeout(
        DEADLINE,
        read_state.read_owned_block_range(
            block::Height(0),
            2,
            u32::try_from(size).unwrap(),
            resources,
            |_| false,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.blocks(), [(block::Height(0), block, size)]);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(result);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn readiness_failure_releases_resources_without_dispatching_a_read() {
    use crate::service::write::{BlockWriteTaskFailure, HeaderChainAttachmentError};

    let _guard = zakura_test::init();
    let (resources, drops, finished) = resources();
    let (_state, mut read_state, _tip, _change) =
        timeout(DEADLINE, crate::init_test_services(&Network::Mainnet))
            .await
            .unwrap();
    let failure = BlockWriteTaskFailure::from(&HeaderChainAttachmentError::MissingGenesis);
    read_state.block_write_failure.set(failure.clone()).unwrap();
    let error = timeout(
        DEADLINE,
        read_state.read_owned_block_range(block::Height(1), 1, 10, resources, |_| {
            panic!("a readiness failure must prevent dispatch of the blocking job")
        }),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.to_string(), failure.to_string());
    timeout(DEADLINE, finished).await.unwrap().unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
