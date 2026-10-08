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
