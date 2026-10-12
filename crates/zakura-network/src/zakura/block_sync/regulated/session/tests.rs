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

/// Gates real serving reads while leaving response production and cleanup unchanged.
#[derive(Debug, Default)]
struct ControlledSource {
    gates: Mutex<BTreeMap<Height, Arc<Semaphore>>>,
    default_gate: Option<Arc<Semaphore>>,
    started: AtomicUsize,
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
        Box::pin(async move {
            if request.lease.try_start() {
                if let Some(gate) = gate {
                    gate.acquire().await?.forget();
                }
            }
            Ok(ReadResult {
                blocks: Vec::new(),
                lease: request.lease,
            })
        })
    }
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

/// Later responses can finish behind a blocked writer without releasing a replacement twice.
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
    session.admit(Height(1), 1).unwrap();
    session.admit(Height(2), 1).unwrap();
    wait_completed(&mut session, Height(2)).await;
    assert!(session.ranges.contains_key(&Height(1)));
    assert!(session.admit(Height(1), 1).is_err());
    let next_gate = Arc::new(Semaphore::new(0));
    source
        .gates
        .lock()
        .unwrap()
        .insert(Height(2), next_gate.clone());
    session.admit(Height(2), 1).unwrap();
    first_gate.add_permits(1);
    for height in [1, 2] {
        assert!(matches!(super::super::tests::receive(&mut output).await,
            super::super::wire::Message::RangeUnavailable(range) if range.start == Height(height)));
    }
    session.remove_completed();
    assert!(
        session.ranges.contains_key(&Height(2)),
        "old completion cannot release the replacement"
    );
    assert!(session.admit(Height(2), 1).is_err());
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
    old.admit(Height(1), 1).unwrap();
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
    new.admit(Height(1), 1).unwrap();
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
    assert!(new.admit(Height(1), 1).is_err());
    new_gate.add_permits(1);
    super::super::tests::receive(&mut output).await;
    wait_completed(&mut new, Height(1)).await;
    cancel.cancel();
}

mod lifecycle;

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

/// Fewer sessions keep the configured limit; more sessions shrink it to at least one.
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
    assert_eq!(with(3634, 0, 32_000), 1);
    assert_eq!(with(usize::MAX, usize::MAX, 32_000), 1);
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
        session.admit(Height(height), 1).unwrap();
    }
    assert!(session.admit(Height(2 * limit + 1), 1).is_err());
    cancel.cancel();
}
