use super::*;
use crate::zakura::{
    block_sync::regulated::serving::{Read, ReadResult},
    framed_channel,
};
use futures::future::BoxFuture;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    time::Duration,
};
use tokio::sync::Semaphore;
use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};

/// Gates real serving reads while leaving response production and cleanup unchanged.
#[derive(Debug, Default)]
struct ControlledSource {
    gates: Mutex<BTreeMap<Height, Arc<Semaphore>>>,
    default_gate: Option<Arc<Semaphore>>,
    started: AtomicUsize,
    /// Serialized blocks served by height. Reads return an empty prefix without them.
    blocks: BTreeMap<Height, Vec<u8>>,
}

impl Source for ControlledSource {
    fn read(&self, request: Read) -> BoxFuture<'static, Result<ReadResult, crate::BoxError>> {
        self.started.fetch_add(1, Ordering::SeqCst);
        let gate = self
            .gates
            .lock()
            .unwrap()
            .get(&request.start_height)
            .cloned()
            .or_else(|| self.default_gate.clone());
        let blocks: Vec<_> = self
            .blocks
            .range(request.start_height..)
            .take(usize::try_from(request.count).unwrap())
            .map(|(height, block)| (*height, block.clone()))
            .collect();
        Box::pin(async move {
            if request.lease.try_start() {
                if let Some(gate) = gate {
                    gate.acquire().await?.forget();
                }
            }
            Ok(ReadResult {
                blocks,
                lease: request.lease,
            })
        })
    }
}

/// Serialized Mainnet blocks at `heights`, keyed by height.
fn mainnet_blocks(heights: std::ops::RangeInclusive<u32>) -> BTreeMap<Height, Vec<u8>> {
    heights
        .map(|height| {
            let bytes = zakura_test::vectors::CONTINUOUS_MAINNET_BLOCKS[&height];
            (Height(height), bytes.to_vec())
        })
        .collect()
}

/// A served message, with each block reduced to its coinbase height.
#[derive(Debug, Eq, PartialEq)]
enum Served {
    Block(Height),
    End(super::super::wire::RangeEnding),
}

/// The next served message.
async fn served(output: &mut crate::zakura::FramedRecv) -> Served {
    match super::super::tests::receive(output).await {
        super::super::wire::Message::Block(bytes) => {
            let block: Block = bytes.zcash_deserialize_into().unwrap();
            Served::Block(block.coinbase_height().unwrap())
        }
        super::super::wire::Message::End(ending) => Served::End(ending),
        other => panic!("served an unexpected {other:?}"),
    }
}

/// The `BlocksDone` ending of a range from `start` that returned `returned` blocks.
fn done(start: u32, returned: u32) -> Served {
    Served::End(super::super::wire::RangeEnding::done(
        Height(start),
        returned,
    ))
}

/// Wait for the production completion drain to release a range.
async fn wait_completed(session: &mut ServingSession, start: Height) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            session.remove_completed();
            if !session.ranges.contains_key(&start) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

/// A later range can finish while an earlier read is blocked, without its old
/// completion releasing a replacement.
#[tokio::test]
async fn completed_ranges_are_reusable_before_earlier_output_drains() {
    let source = Arc::new(ControlledSource::default());
    let first_gate = Arc::new(Semaphore::new(0));
    source
        .gates
        .lock()
        .unwrap()
        .insert(Height(1), first_gate.clone());
    let serving = Serving::new(source.clone(), &ZakuraBlockSyncConfig::default());
    let cancel = CancellationToken::new();
    let (send, mut output) = framed_channel(4);
    let mut session = serving.session(
        &ZakuraPeerId::new(vec![61; 32]).unwrap(),
        send,
        cancel.clone(),
        CancellationToken::new(),
        Default::default(),
    );
    session.admit(Range::new(Height(1), 1).unwrap()).unwrap();
    session.admit(Range::new(Height(2), 1).unwrap()).unwrap();
    wait_completed(&mut session, Height(2)).await;
    assert!(session.ranges.contains_key(&Height(1)));
    assert!(session.admit(Range::new(Height(1), 1).unwrap()).is_err());
    let next_gate = Arc::new(Semaphore::new(0));
    source
        .gates
        .lock()
        .unwrap()
        .insert(Height(2), next_gate.clone());
    session.admit(Range::new(Height(2), 1).unwrap()).unwrap();
    first_gate.add_permits(1);
    // Ready order wrote the old range 2 before range 1's read finished.
    for height in [2, 1] {
        assert!(matches!(super::super::tests::receive(&mut output).await,
            super::super::wire::Message::End(ending)
                if ending == super::super::wire::RangeEnding::unavailable(Height(height), 1)));
    }
    session.remove_completed();
    assert!(
        session.ranges.contains_key(&Height(2)),
        "old completion cannot release the replacement"
    );
    assert!(session.admit(Range::new(Height(2), 1).unwrap()).is_err());
    next_gate.add_permits(1);
    super::super::tests::receive(&mut output).await;
    wait_completed(&mut session, Height(2)).await;
    assert!(session.ranges.is_empty());
    cancel.cancel();
}

/// Finishing a retired session's read cannot remove a range from the replacement session.
#[tokio::test]
async fn cancelled_sessions_do_not_complete_replacement_ranges() {
    let source = Arc::new(ControlledSource::default());
    let old_gate = Arc::new(Semaphore::new(0));
    source
        .gates
        .lock()
        .unwrap()
        .insert(Height(1), old_gate.clone());
    let serving = Serving::new(source.clone(), &ZakuraBlockSyncConfig::default());
    let peer = ZakuraPeerId::new(vec![62; 32]).unwrap();
    let old_cancel = CancellationToken::new();
    let (send, old_output) = framed_channel(4);
    let mut old = serving.session(
        &peer,
        send,
        old_cancel.clone(),
        CancellationToken::new(),
        Default::default(),
    );
    old.admit(Range::new(Height(1), 1).unwrap()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while source.started.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    old_cancel.cancel();
    drop((old, old_output));
    let new_gate = Arc::new(Semaphore::new(0));
    source
        .gates
        .lock()
        .unwrap()
        .insert(Height(1), new_gate.clone());
    let cancel = CancellationToken::new();
    let (send, mut output) = framed_channel(4);
    let mut new = serving.session(
        &peer,
        send,
        cancel.clone(),
        CancellationToken::new(),
        Default::default(),
    );
    new.admit(Range::new(Height(1), 1).unwrap()).unwrap();
    old_gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while source.started.load(Ordering::SeqCst) < 2
            || serving.capacity.node_execution_held() != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    new.remove_completed();
    assert!(new.ranges.contains_key(&Height(1)));
    assert!(new.admit(Range::new(Height(1), 1).unwrap()).is_err());
    new_gate.add_permits(1);
    super::super::tests::receive(&mut output).await;
    wait_completed(&mut new, Height(1)).await;
    cancel.cancel();
}

/// A session for `source` with the production serving limits, whose output
/// holds `queue` frames.
fn ready_session(
    source: &Arc<ControlledSource>,
    peer: u8,
    queue: usize,
) -> (
    Serving,
    ServingSession,
    crate::zakura::FramedRecv,
    CancellationToken,
) {
    let serving = Serving::new(source.clone(), &ZakuraBlockSyncConfig::default());
    let cancel = CancellationToken::new();
    let (send, output) = framed_channel(queue);
    let session = serving.session(
        &ZakuraPeerId::new(vec![peer; 32]).unwrap(),
        send,
        cancel.clone(),
        CancellationToken::new(),
        Default::default(),
    );
    (serving, session, output, cancel)
}

/// Height 1's block and ending are written while the earlier height 2 read is blocked.
#[tokio::test]
async fn a_ready_range_is_sent_while_an_earlier_read_is_blocked() {
    let source = Arc::new(ControlledSource {
        blocks: mainnet_blocks(1..=2),
        ..Default::default()
    });
    let blocked = Arc::new(Semaphore::new(0));
    source
        .gates
        .lock()
        .unwrap()
        .insert(Height(2), blocked.clone());
    let (_serving, mut session, mut output, cancel) = ready_session(&source, 64, 4);
    session.admit(Range::new(Height(2), 1).unwrap()).unwrap();
    session.admit(Range::new(Height(1), 1).unwrap()).unwrap();
    assert_eq!(served(&mut output).await, Served::Block(Height(1)));
    assert_eq!(served(&mut output).await, done(1, 1));
    wait_completed(&mut session, Height(1)).await;
    assert!(
        session.ranges.contains_key(&Height(2)),
        "the height 2 read is still blocked"
    );
    assert!(output.try_recv().is_err());
    blocked.add_permits(1);
    assert_eq!(served(&mut output).await, Served::Block(Height(2)));
    assert_eq!(served(&mut output).await, done(2, 1));
    wait_completed(&mut session, Height(2)).await;
    cancel.cancel();
}

/// Concurrent multi-block requests, reused every round, are each answered with
/// their first block and `BlocksDone(1)`, since we advertise one block per
/// response. Ranges may interleave with each other but each ends exactly once,
/// after its block.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_multi_block_requests_get_one_block_prefixes() {
    const RANGES: [(u32, u32); 4] = [(1, 2), (3, 3), (6, 1), (7, 4)];
    let gate = Arc::new(Semaphore::new(0));
    let source = Arc::new(ControlledSource {
        default_gate: Some(gate.clone()),
        blocks: mainnet_blocks(1..=10),
        ..Default::default()
    });
    let (_serving, mut session, mut output, cancel) = ready_session(&source, 65, 4);
    for _round in 0..16 {
        // Every range from the last round has ended, so each admission reuses its heights.
        for (start, count) in RANGES {
            session
                .admit(Range::new(Height(start), count).unwrap())
                .unwrap();
        }
        gate.add_permits(RANGES.len());
        let mut received = [0; RANGES.len()];
        let mut ended = [false; RANGES.len()];
        while ended.contains(&false) {
            let served = served(&mut output).await;
            let height = match &served {
                Served::Block(height) => *height,
                Served::End(ending) if ending.outcome == super::super::wire::Outcome::Done => {
                    ending.start
                }
                Served::End(other) => panic!("unexpected ending {other:?}"),
            };
            let index = RANGES
                .iter()
                .position(|(start, count)| (*start..start + count).contains(&height.0))
                .unwrap();
            let (start, _count) = RANGES[index];
            assert!(!ended[index], "range {start} sent after its ending");
            if served == Served::Block(height) {
                assert_eq!(height, Height(start), "only the first block is served");
                received[index] += 1;
            } else {
                assert_eq!(served, done(start, 1));
                assert_eq!(received[index], 1);
                ended[index] = true;
            }
        }
    }
    assert!(
        output.try_recv().is_err(),
        "each range's ending is its last frame"
    );
    session.remove_completed();
    assert!(session.ranges.is_empty());
    cancel.cancel();
}

/// A completed range's output is already queued ahead of anything a
/// replacement for the same heights can send, even before it is written.
#[tokio::test]
async fn a_same_height_replacement_never_precedes_the_old_ranges_output() {
    let source = Arc::new(ControlledSource {
        blocks: mainnet_blocks(1..=3),
        ..Default::default()
    });
    // One transport slot that nobody reads holds the old output back.
    let (serving, mut session, mut output, cancel) = ready_session(&source, 66, 1);
    session.admit(Range::new(Height(1), 3).unwrap()).unwrap();
    wait_completed(&mut session, Height(1)).await;
    assert!(
        serving.capacity.node_output_held() > 0,
        "the old range's output is not all written"
    );
    session.admit(Range::new(Height(1), 3).unwrap()).unwrap();
    for _ in 0..2 {
        assert_eq!(served(&mut output).await, Served::Block(Height(1)));
        assert_eq!(served(&mut output).await, done(1, 1));
    }
    wait_completed(&mut session, Height(1)).await;
    assert!(output.try_recv().is_err());
    cancel.cancel();
}

mod lifecycle;
mod load;

/// Default peer limits budget 2816 requests, so 512 sessions fill exactly 512 MiB.
#[test]
fn the_default_serving_limit_fits_the_bookkeeping_budget() {
    let config = ZakuraBlockSyncConfig::default();
    let limit = serving_max_inflight_requests(&config);
    assert_eq!(serving_sessions(&config), 512);
    assert_eq!(limit, 2816);
    let sessions = u64::try_from(serving_sessions(&config)).unwrap();
    let envelope = sessions * (SESSION_ALLOWANCE_BYTES + 2 * u64::from(limit) * BYTES_PER_REQUEST);
    assert!(envelope <= BOOKKEEPING_BUDGET_BYTES);
    let one_more =
        sessions * (SESSION_ALLOWANCE_BYTES + 2 * u64::from(limit + 1) * BYTES_PER_REQUEST);
    assert!(one_more > BOOKKEEPING_BUDGET_BYTES);
    // Local download sizing keeps the configured advertisement.
    assert_eq!(config.advertised_max_inflight_requests(), 32_000);
}

/// Fewer sessions keep the configured limit; more sessions shrink it. Past 3,633
/// slots nothing fits, and the reactor refuses that config before serving.
#[test]
fn the_serving_limit_follows_session_slots_and_configuration() {
    let with = |inbound, outbound, configured| {
        let mut config = ZakuraBlockSyncConfig {
            max_inflight_requests: configured,
            ..Default::default()
        };
        config.peer_limits.max_inbound_peers = inbound;
        config.peer_limits.max_outbound_peers = outbound;
        serving_max_inflight_requests(&config)
    };
    assert_eq!(with(8, 8, 32_000), 32_000);
    assert_eq!(with(256, 256, 100), 100);
    assert_eq!(with(0, 0, 32_000), 32_000);
    assert_eq!(budgeted_inflight_requests(0), None);
    assert_eq!(budgeted_inflight_requests(3633), Some(1));
    assert_eq!(budgeted_inflight_requests(3634), Some(0));
    assert_eq!(with(3633, 0, 32_000), 1);
    assert_eq!(with(3634, 0, 32_000), 0);
    assert_eq!(with(usize::MAX, usize::MAX, 32_000), 0);
}

/// Serving enforces the advertised limit: `2 × limit` open requests are served, one more faults.
#[tokio::test]
async fn serving_enforces_the_budgeted_limit() {
    let source = Arc::new(ControlledSource {
        default_gate: Some(Arc::new(Semaphore::new(0))),
        ..Default::default()
    });
    let config = ZakuraBlockSyncConfig::default();
    let limit = serving_max_inflight_requests(&config);
    let serving = Serving::new(source, &config);
    let cancel = CancellationToken::new();
    let (send, _output) = framed_channel(4);
    let mut session = serving.session(
        &ZakuraPeerId::new(vec![62; 32]).unwrap(),
        send,
        cancel.clone(),
        CancellationToken::new(),
        Default::default(),
    );
    for height in 1..=2 * limit {
        session
            .admit(Range::new(Height(height), 1).unwrap())
            .unwrap();
    }
    assert!(session
        .admit(Range::new(Height(2 * limit + 1), 1).unwrap())
        .is_err());
    cancel.cancel();
}

/// Live ranges may touch but never share a height. Each overlap is a peer
/// fault, and a refused range leaves the admitted ranges unchanged.
#[tokio::test]
async fn adjacent_ranges_are_admitted_and_any_overlap_disconnects() {
    let source = Arc::new(ControlledSource {
        default_gate: Some(Arc::new(Semaphore::new(0))),
        ..Default::default()
    });
    let serving = Serving::new(source, &ZakuraBlockSyncConfig::default());
    let cancel = CancellationToken::new();
    let (send, _output) = framed_channel(4);
    let mut session = serving.session(
        &ZakuraPeerId::new(vec![63; 32]).unwrap(),
        send,
        cancel.clone(),
        CancellationToken::new(),
        Default::default(),
    );
    // Heights 10..=14, then the ranges that end just below and start just above it.
    for (start, count) in [(10, 5), (5, 5), (15, 3)] {
        session
            .admit(Range::new(Height(start), count).unwrap())
            .unwrap();
    }
    for (start, count) in [
        (10, 5),
        (12, 1),
        (4, 2),
        (9, 2),
        (14, 2),
        (17, 1),
        (16, 4),
        (1, 128),
    ] {
        assert!(
            matches!(
                session.admit(Range::new(Height(start), count).unwrap()),
                Err(SinkReject::Protocol(_))
            ),
            "{count} heights from {start} overlap a live range"
        );
    }
    assert_eq!(
        session.ranges.keys().copied().collect::<Vec<_>>(),
        [Height(5), Height(10), Height(15)]
    );
    session.admit(Range::new(Height(18), 1).unwrap()).unwrap();
    session.admit(Range::new(Height(1), 4).unwrap()).unwrap();
    cancel.cancel();
}
