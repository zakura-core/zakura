//! Compares the simulator's `EnginePort` with this production adapter
//! (storage.md ST2.5).
//!
//! Each test runs one request sequence through `HeaderChainServicePort`, over
//! an ephemeral zakura-state on Regtest, and through `zakura_storage`'s
//! `EnginePort`, and requires the same replies. It covers what the DoS
//! properties read: locator permits, capacity release, retained-path eviction
//! and timeouts. Committed snapshots are compared whole. Replies are compared
//! by shape, since lease IDs and error sources are each side's own. Every deliberate difference is an
//! `expected_difference_*` test below, which pins what the simulator does
//! instead.

use std::{sync::Arc, time::Duration};

use tower::service_fn;
use zakura_chain::block;
use zakura_header_chain::{Frontier, HeaderWorkAuthority, SourceId};
use zakura_node_services::{
    header_chain::{
        AcquirePath, AcquirePathReply, ApplyHeaderTargetOutcome, LocatorReadPermit, Port, ReadPath,
        ReadPathReply, RetainedHeaderPath, SelectedChainMembership, ServingCapacitySignal,
        VctRepairContextReply,
    },
    sync_lifecycle::HeaderRuntimeStatus,
};
use zakura_storage::header::{
    adapter::EnginePort,
    fixture::HeaderFixture,
    lease::{MAX_HEADER_SYNC_HEIGHT_RANGE, MAX_RETAINED_PATH_LEASES, RETAINED_PATH_LEASE_IDLE},
    HeaderOp, Outcome, ADAPTER_DEADLINE,
};

use super::*;

/// The production adapter over an ephemeral Regtest state whose header-chain
/// runtime has attached at genesis, as `zakurad` starts it.
async fn production() -> Arc<dyn Port> {
    let network = HeaderFixture::regtest().network().clone();
    let config = zakura_state::Config {
        ephemeral: true,
        debug_skip_non_finalized_state_backup_task: true,
        enable_zakura_header_seed_from_committed_blocks: true,
        ..Default::default()
    };
    let (mut state, read_state, _, _, authority) =
        zakura_state::init_with_header_chain_body_evidence(config, &network, block::Height(0), 2)
            .await
            .expect("the ephemeral state starts");
    state
        .ready()
        .await
        .expect("the state is ready for genesis")
        .call(zakura_state::Request::CommitCheckpointVerifiedBlock(
            block::genesis::regtest_genesis_block().into(),
        ))
        .await
        .expect("genesis commits");
    state
        .ready()
        .await
        .expect("the state performs semantic handoff");
    read_state
        .subscribe_header_runtime_status()
        .wait_for(|status| matches!(status, HeaderRuntimeStatus::Ready { .. }))
        .await
        .expect("the header runtime attaches at genesis");
    let state = {
        let state = Arc::new(tokio::sync::Mutex::new(state));
        service_fn(move |request| {
            let state = state.clone();
            async move { state.lock().await.ready().await?.call(request).await }
        })
    };
    let snapshots = read_state.subscribe_header_chain_snapshots();
    Arc::new(HeaderChainServicePort::new(
        state, read_state, authority, network, snapshots,
    ))
}

/// How long `both` waits, in wall-clock time, for `production` to start.
const SETUP_TIMEOUT: Duration = Duration::from_secs(60);

/// Both ports, each holding only the genesis anchor. Fails the test if
/// `production` doesn't start within `SETUP_TIMEOUT`. A tokio timeout can't
/// bound it, because tests with a paused clock auto-advance past the timeout
/// while zakura-state's writer thread commits genesis.
async fn both() -> [Arc<dyn Port>; 2] {
    let (expire, expired) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        std::thread::sleep(SETUP_TIMEOUT);
        let _ = expire.send(());
    });
    [
        tokio::select! {
            biased;
            port = production() => port,
            _ = expired => panic!("the production state didn't start within {SETUP_TIMEOUT:?}"),
        },
        Arc::new(EnginePort::new(&HeaderFixture::regtest())),
    ]
}

/// `port`'s latest committed snapshot.
fn snapshot(port: &dyn Port) -> zakura_header_chain::EngineSnapshot {
    port.selected_chain()
        .borrow()
        .clone()
        .expect("an attached port has a snapshot")
}

/// Prepares and applies `headers` on `parent` through `port`. Returns their
/// frontiers.
async fn admit(
    port: &dyn Port,
    parent: Frontier,
    headers: &[Arc<block::Header>],
    request_id: u64,
) -> Vec<Frontier> {
    let request =
        HeaderFixture::regtest().target_request(&snapshot(port), parent, headers, request_id);
    let prepared = port
        .prepare_header_target(request)
        .await
        .expect("fixture headers prepare");
    assert_eq!(
        port.apply_header_target(prepared)
            .await
            .expect("fixture headers apply"),
        ApplyHeaderTargetOutcome::Applied
    );
    (1..)
        .zip(headers)
        .map(|(offset, header)| {
            Frontier::new(block::Height(parent.height.0 + offset), header.hash())
        })
        .collect()
}

/// A five-header main chain and a four-header fork from height 2, on
/// `port`. The fork reaches height 6, so selection switches to it. Every
/// port builds the same chain from the same fixture.
async fn chain(port: &dyn Port) -> (Vec<Frontier>, Vec<Frontier>) {
    let fixture = HeaderFixture::regtest();
    let anchor = fixture.anchor();
    let main = admit(port, anchor, &fixture.extend(anchor, 5, 0), 1).await;
    let fork = admit(port, main[1], &fixture.extend(main[1], 4, 1), 2).await;
    (main, fork)
}

/// The peer numbered `n`.
fn peer(n: u8) -> SourceId {
    SourceId::from_digest([n; 32])
}

/// `peer`'s acquire toward `target`, scoped as the v9 publisher scopes it.
fn acquire_request(
    port: &dyn Port,
    peer: SourceId,
    target: block::Hash,
    locators: Vec<block::Hash>,
) -> AcquirePath {
    AcquirePath {
        source: peer,
        session_id: 1,
        scope: HeaderWorkAuthority::for_target(&snapshot(port), target),
        target_tip_hash: target,
        locator_hashes: locators,
    }
}

/// An acquire reply's shape: its variant and, for a lease, everything but
/// the adapter's own handle and seal.
fn acquire_shape(reply: &Result<AcquirePathReply, PortError>) -> String {
    match reply {
        Ok(AcquirePathReply::Acquired(path)) => format!(
            "acquired {:?}..{:?} {:?}",
            path.common_ancestor, path.target, path.scope
        ),
        Ok(AcquirePathReply::CapacityBusy(_)) => "capacity-busy".to_owned(),
        Ok(other) => format!("{other:?}"),
        Err(error) => error_shape(error),
    }
}

/// A read reply's shape: the whole page, which holds nothing specific to
/// one adapter, or its variant.
fn read_shape(reply: &Result<ReadPathReply, PortError>) -> String {
    match reply {
        Ok(ReadPathReply::Page(page)) => format!("page {page:?}"),
        Ok(ReadPathReply::Unavailable) => "unavailable".to_owned(),
        Err(error) => error_shape(error),
    }
}

/// A port error's shape: its variant and whether it carries a source.
fn error_shape(error: &PortError) -> String {
    match error {
        PortError::Timeout => "timeout".to_owned(),
        PortError::Unavailable { source } => format!("unavailable(source={})", source.is_some()),
    }
}

/// The leased path in `reply`.
fn leased(reply: Result<AcquirePathReply, PortError>) -> RetainedHeaderPath {
    match reply {
        Ok(AcquirePathReply::Acquired(path)) => *path,
        other => panic!("expected a lease, got {other:?}"),
    }
}

/// The signal in `port`'s refusal of `request`, which must be
/// `CapacityBusy`.
async fn refused(port: &dyn Port, request: AcquirePath) -> ServingCapacitySignal {
    match port.acquire_header_path(request).await {
        Ok(AcquirePathReply::CapacityBusy(signal)) => signal,
        other => panic!("expected a capacity refusal, got {other:?}"),
    }
}

/// The page request after `after` for `count` headers, with tree roots.
fn page_request(after: block::Hash, count: u32) -> ReadPath {
    ReadPath {
        after_hash: after,
        max_header_count: count,
        want_tree_aux: true,
    }
}

/// What a port shows of its selected chain: the committed snapshot, the
/// locator, and the membership of `hashes`.
async fn selection(
    port: &dyn Port,
    hashes: &[block::Hash],
) -> (
    zakura_header_chain::EngineSnapshot,
    Vec<Frontier>,
    Vec<SelectedChainMembership>,
) {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = LocatorReadPermit::new(
        semaphore
            .clone()
            .try_acquire_owned()
            .expect("a fresh semaphore has a permit"),
    );
    let locator = port
        .continuation_locator(permit)
        .await
        .expect("the locator reads")
        .expect("an attached port has a locator");
    // The read held the only clone, so its capacity is free again.
    assert_eq!(semaphore.available_permits(), 1);
    let mut memberships = Vec::new();
    for hash in hashes {
        memberships.push(
            port.selected_chain_contains(*hash)
                .await
                .expect("membership reads"),
        );
    }
    (snapshot(port), locator.entries().to_vec(), memberships)
}

/// Committed snapshots, locators and membership agree on both branches:
/// after the main chain alone, and after a heavier fork takes over. A
/// locator read keeps its permit only until it ends.
#[tokio::test]
async fn selection_locator_and_membership_match() {
    let fixture = HeaderFixture::regtest();
    let anchor = fixture.anchor();
    let main_headers = fixture.extend(anchor, 5, 0);
    let main_frontiers: Vec<_> = (1..)
        .zip(&main_headers)
        .map(|(height, header)| Frontier::new(block::Height(height), header.hash()))
        .collect();
    let fork_headers = fixture.extend(main_frontiers[1], 4, 1);
    let mut hashes: Vec<_> = main_headers
        .iter()
        .chain(&fork_headers)
        .map(|header| header.hash())
        .collect();
    hashes.push(block::Hash([0xee; 32]));
    let mut seen = Vec::new();
    for port in both().await {
        let port = &*port;
        admit(port, anchor, &main_headers, 1).await;
        let on_main = selection(port, &hashes).await;
        let fork = admit(port, main_frontiers[1], &fork_headers, 2).await;
        let on_fork = selection(port, &hashes).await;
        seen.push((on_main, on_fork, fork));
    }
    assert_eq!(seen[0], seen[1]);
    let (
        (main_snapshot, main_locator, main_members),
        (fork_snapshot, fork_locator, fork_members),
        fork,
    ) = &seen[0];
    assert_eq!(main_snapshot.frontiers.header_best, main_frontiers[4]);
    assert_eq!(main_locator.first(), Some(&main_frontiers[4]));
    assert_eq!(fork_snapshot.frontiers.header_best, fork[3]);
    assert_eq!(fork_locator.first(), Some(&fork[3]));
    for (index, frontier) in main_frontiers.iter().enumerate() {
        assert_eq!(
            main_members[index],
            SelectedChainMembership::Selected(*frontier)
        );
        // After the switch only the shared prefix stays selected.
        let expected = if index < 2 {
            SelectedChainMembership::Selected(*frontier)
        } else {
            SelectedChainMembership::NotSelected
        };
        assert_eq!(fork_members[index], expected);
    }
    for (index, frontier) in fork.iter().enumerate() {
        assert_eq!(
            main_members[5 + index],
            SelectedChainMembership::NotRetained
        );
        assert_eq!(
            fork_members[5 + index],
            SelectedChainMembership::Selected(*frontier)
        );
    }
    assert_eq!(
        main_members.last(),
        Some(&SelectedChainMembership::NotRetained)
    );
    assert_eq!(
        fork_members.last(),
        Some(&SelectedChainMembership::NotRetained)
    );
}

/// Paging, continuation, a fork path, an empty path and every acquire
/// refusal give the same replies.
#[tokio::test]
async fn path_serving_matches() {
    let mut seen: Vec<Vec<String>> = Vec::new();
    for port in both().await {
        let port = &*port;
        let (main, fork) = chain(port).await;
        let anchor = HeaderFixture::regtest().anchor();
        let mut shapes = Vec::new();
        let path = leased(
            port.acquire_header_path(acquire_request(
                port,
                peer(1),
                main[4].hash,
                vec![anchor.hash],
            ))
            .await,
        );
        let first = port
            .read_header_path(path.clone(), page_request(anchor.hash, 2))
            .await;
        // Each header carries the delivery its admission recorded, so the
        // whole-page comparison covers auxiliary deliveries too.
        let Ok(ReadPathReply::Page(first_page)) = &first else {
            panic!("the first page is served: {first:?}");
        };
        assert_eq!(first_page.headers.len(), 2);
        assert!(first_page
            .aux_deliveries
            .iter()
            .all(|deliveries| deliveries.len() == 1));
        shapes.push(read_shape(&first));
        // A served page consumed the lease.
        shapes.push(read_shape(
            &port
                .read_header_path(path, page_request(main[1].hash, 2))
                .await,
        ));
        let next = port
            .acquire_header_path(acquire_request(
                port,
                peer(1),
                main[4].hash,
                vec![main[1].hash],
            ))
            .await;
        shapes.push(acquire_shape(&next));
        shapes.push(read_shape(
            &port
                .read_header_path(leased(next), page_request(main[1].hash, 10))
                .await,
        ));
        let fork_path = port
            .acquire_header_path(acquire_request(
                port,
                peer(2),
                fork[1].hash,
                vec![main[1].hash],
            ))
            .await;
        shapes.push(acquire_shape(&fork_path));
        shapes.push(read_shape(
            &port
                .read_header_path(leased(fork_path), page_request(main[1].hash, 10))
                .await,
        ));
        let empty = port
            .acquire_header_path(acquire_request(
                port,
                peer(3),
                main[2].hash,
                vec![main[2].hash],
            ))
            .await;
        shapes.push(acquire_shape(&empty));
        for (target, locators) in [
            (block::Hash([0xee; 32]), vec![main[0].hash]),
            (main[4].hash, vec![block::Hash([0xee; 32])]),
            (main[4].hash, Vec::new()),
            (main[4].hash, vec![main[0].hash; 14]),
        ] {
            shapes.push(acquire_shape(
                &port
                    .acquire_header_path(acquire_request(port, peer(4), target, locators))
                    .await,
            ));
        }
        let mut stale_scope = acquire_request(port, peer(4), main[4].hash, vec![main[0].hash]);
        stale_scope.scope = HeaderWorkAuthority::for_target(&snapshot(port), main[3].hash);
        shapes.push(acquire_shape(&port.acquire_header_path(stale_scope).await));
        let held = leased(
            port.acquire_header_path(acquire_request(
                port,
                peer(5),
                main[4].hash,
                vec![main[0].hash],
            ))
            .await,
        );
        for count in [0, MAX_HEADER_SYNC_HEIGHT_RANGE + 1] {
            shapes.push(read_shape(
                &port
                    .read_header_path(held.clone(), page_request(main[0].hash, count))
                    .await,
            ));
        }
        shapes.push(read_shape(
            &port
                .read_header_path(held, page_request(main[1].hash, 1))
                .await,
        ));
        seen.push(shapes);
    }
    assert_eq!(seen[0], seen[1]);
}

/// A held lease refuses its own peer, and full capacity refuses every new
/// peer with one shared signal. A served read and a release each free a slot
/// that the next peer then takes, and wake the waiters at that moment.
#[tokio::test]
async fn capacity_and_release_match() {
    let mut seen = Vec::new();
    for port in both().await {
        let port = &*port;
        let (main, _) = chain(port).await;
        let wide = |n: u8| acquire_request(port, peer(n), main[4].hash, vec![main[0].hash]);
        let mut events = Vec::new();
        let mut paths = vec![leased(port.acquire_header_path(wide(0)).await)];
        let own = refused(
            port,
            acquire_request(port, peer(0), main[3].hash, vec![main[0].hash]),
        )
        .await;
        for n in 1..MAX_RETAINED_PATH_LEASES {
            let n = u8::try_from(n).expect("16 peers fit u8");
            paths.push(leased(port.acquire_header_path(wide(n)).await));
        }
        let full = [
            refused(port, wide(100)).await,
            refused(port, wide(101)).await,
        ];
        let waiting = |full: &[ServingCapacitySignal; 2]| {
            format!(
                "{} {:?}",
                own.is_released(),
                full.each_ref().map(ServingCapacitySignal::is_released)
            )
        };
        events.push(waiting(&full));
        // A read that serves another peer's whole path consumes its lease.
        let served = port
            .read_header_path(paths[3].clone(), page_request(main[0].hash, 4))
            .await;
        events.push(match &served {
            Ok(ReadPathReply::Page(page)) => format!("served complete={}", page.complete),
            other => read_shape(other),
        });
        events.push(waiting(&full));
        // The freed slot takes one peer, and the next waits on a new signal.
        events.push(acquire_shape(&port.acquire_header_path(wide(102)).await));
        let again = refused(port, wide(103)).await;
        events.push(format!("{} {}", own.is_released(), again.is_released()));
        // Releasing peer 0's lease wakes peer 0 and the new waiter. Repeats
        // are safe.
        port.release_header_path(paths[0].clone())
            .await
            .expect("a release succeeds");
        port.release_header_path(paths[0].clone())
            .await
            .expect("a repeated release succeeds");
        events.push(format!("{} {}", own.is_released(), again.is_released()));
        events.push(acquire_shape(&port.acquire_header_path(wide(104)).await));
        events.push(acquire_shape(&port.acquire_header_path(wide(105)).await));
        seen.push(events);
    }
    assert_eq!(seen[0], seen[1]);
    assert_eq!(
        seen[0][..3],
        [
            "false [false, false]",
            "served complete=true",
            "false [true, true]"
        ]
    );
    assert!(seen[0][3].starts_with("acquired "), "{}", seen[0][3]);
    assert_eq!(seen[0][4..6], ["false false", "true true"]);
    assert!(seen[0][6].starts_with("acquired "), "{}", seen[0][6]);
    assert_eq!(seen[0][7], "capacity-busy");
}

/// The ordinary slots and the reserve slot. With every ordinary slot taken,
/// a path longer than one page is refused onto the ordinary signal
/// (`beyond_reserve`), while a short path still takes the reserve slot. Each
/// signal fires only when its own class has room.
#[tokio::test]
async fn reserve_slot_and_beyond_reserve_match() {
    let fixture = HeaderFixture::regtest();
    let mut seen = Vec::new();
    for port in both().await {
        let port = &*port;
        let anchor = fixture.anchor();
        let low = admit(
            port,
            anchor,
            &fixture.extend(anchor, MAX_HEADER_SYNC_HEIGHT_RANGE, 0),
            1,
        )
        .await;
        let near = *low.last().expect("the batch has headers");
        let tip = admit(port, near, &fixture.extend(near, 2, 0), 2).await[1].hash;
        // A short path covers the tip's two headers above `near`. A long one
        // starts at the anchor, one page plus two below the tip.
        let short = |n: u8| acquire_request(port, peer(n), tip, vec![near.hash]);
        let long = |n: u8| acquire_request(port, peer(n), tip, vec![anchor.hash]);
        let mut events = Vec::new();
        let mut ordinary = Vec::new();
        for n in 1..MAX_RETAINED_PATH_LEASES {
            let n = u8::try_from(n).expect("16 peers fit u8");
            ordinary.push(leased(port.acquire_header_path(short(n)).await));
        }
        let general = refused(port, long(100)).await;
        let reserve = leased(port.acquire_header_path(short(101)).await);
        let fallback = refused(port, short(102)).await;
        let signals = || format!("{} {}", general.is_released(), fallback.is_released());
        events.push(signals());
        // Freeing the reserve slot wakes only the fallback waiter, and the
        // long path still can't use it.
        port.release_header_path(reserve)
            .await
            .expect("a release succeeds");
        events.push(signals());
        events.push(acquire_shape(&port.acquire_header_path(long(100)).await));
        // Freeing an ordinary slot wakes the ordinary waiter, and the long
        // path takes that slot.
        port.release_header_path(ordinary[0].clone())
            .await
            .expect("a release succeeds");
        events.push(signals());
        events.push(acquire_shape(&port.acquire_header_path(long(100)).await));
        seen.push(events);
    }
    assert_eq!(seen[0], seen[1]);
    assert_eq!(
        seen[0][..4],
        ["false false", "false true", "capacity-busy", "true true"]
    );
    assert!(seen[0][4].starts_with("acquired "), "{}", seen[0][4]);
}

/// Eviction on the idle deadline: A's lease survives a sweep one tick before
/// it, then is swept by peer C's acquire, not by B's empty-locator acquire,
/// and A's waiting signal fires then.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn idle_eviction_matches() {
    let mut seen = Vec::new();
    for port in both().await {
        let port = &*port;
        let (main, _) = chain(port).await;
        let mut events = Vec::new();
        let a = leased(
            port.acquire_header_path(acquire_request(
                port,
                peer(1),
                main[4].hash,
                vec![main[0].hash],
            ))
            .await,
        );
        let Ok(AcquirePathReply::CapacityBusy(waiting)) = port
            .acquire_header_path(acquire_request(
                port,
                peer(1),
                main[3].hash,
                vec![main[0].hash],
            ))
            .await
        else {
            panic!("a held lease refuses its peer");
        };
        // A's own acquire sweeps without taking a slot, so one tick before
        // the deadline it must still find A's lease.
        tokio::time::advance(RETAINED_PATH_LEASE_IDLE - Duration::from_nanos(1)).await;
        let early = port
            .acquire_header_path(acquire_request(
                port,
                peer(1),
                main[3].hash,
                vec![main[0].hash],
            ))
            .await;
        events.push(format!(
            "{} {}",
            acquire_shape(&early),
            waiting.is_released()
        ));
        tokio::time::advance(Duration::from_nanos(1)).await;
        let empty = port
            .acquire_header_path(acquire_request(port, peer(2), main[3].hash, Vec::new()))
            .await;
        events.push(format!(
            "{} {}",
            acquire_shape(&empty),
            waiting.is_released()
        ));
        let c = port
            .acquire_header_path(acquire_request(
                port,
                peer(3),
                main[3].hash,
                vec![main[0].hash],
            ))
            .await;
        events.push(format!("{} {}", acquire_shape(&c), waiting.is_released()));
        events.push(read_shape(
            &port
                .read_header_path(a, page_request(main[0].hash, 1))
                .await,
        ));
        seen.push(events);
    }
    assert_eq!(seen[0], seen[1]);
    // One tick early, A's lease is still live. At the deadline, the empty
    // locator sweeps nothing, C's acquire sweeps A's lease and wakes A's
    // waiter, and A's handle then reads nothing.
    assert_eq!(seen[0][0], "capacity-busy false");
    assert_eq!(seen[0][1], "unavailable(source=true) false");
    assert!(
        seen[0][2].starts_with("acquired ") && seen[0][2].ends_with(" true"),
        "{}",
        seen[0][2]
    );
    assert_eq!(seen[0][3], "unavailable");
}

/// Timeouts: production fails a wedged state read at exactly the contract's
/// adapter deadline, and the contract offers `timeout` from that instant on.
/// Preparation's lease read fails with `local-resource` at the same instant.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn adapter_deadlines_match_the_contract() {
    assert_eq!(ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT, ADAPTER_DEADLINE);
    let key = port::AdapterKey::new();
    let wedged = HeaderChainServicePort {
        state: tower::service_fn(|_: zakura_state::Request| async {
            std::future::pending::<Result<zakura_state::Response, zakura_state::BoxError>>().await
        }),
        read_state: tower::service_fn(|_: zakura_state::ReadRequest| async {
            std::future::pending::<Result<zakura_state::ReadResponse, zakura_state::BoxError>>()
                .await
        }),
        authority: zakura_state::HeaderChainBodyEvidenceAuthority::new_test(),
        network: HeaderFixture::regtest().network().clone(),
        adapter_key: key.clone(),
        selected_chain: tokio::sync::watch::channel(None).1,
    };
    let anchor = HeaderFixture::regtest().anchor();
    let scope = HeaderWorkAuthority {
        header_generation: zakura_header_chain::HeaderGeneration::new(1),
        branch: zakura_header_chain::BranchId::new(anchor.hash, anchor.hash),
    };
    let path = RetainedHeaderPath::from_adapter(&key, 1, peer(1), 1, anchor, anchor, scope);
    let permit = LocatorReadPermit::new(
        Arc::new(tokio::sync::Semaphore::new(1))
            .try_acquire_owned()
            .expect("a fresh semaphore has a permit"),
    );
    let started = tokio::time::Instant::now();
    let results = [
        (
            HeaderOp::ContinuationLocator,
            wedged.continuation_locator(permit).await.err(),
        ),
        (
            HeaderOp::AcquireHeaderPath,
            wedged
                .acquire_header_path(AcquirePath {
                    source: peer(1),
                    session_id: 1,
                    scope,
                    target_tip_hash: anchor.hash,
                    locator_hashes: vec![anchor.hash],
                })
                .await
                .err(),
        ),
        (
            HeaderOp::ReadHeaderPath,
            wedged
                .read_header_path(path.clone(), page_request(anchor.hash, 1))
                .await
                .err(),
        ),
        (
            HeaderOp::ReleaseHeaderPath,
            wedged.release_header_path(path).await.err(),
        ),
        (
            HeaderOp::SelectedChainContains,
            wedged.selected_chain_contains(anchor.hash).await.err(),
        ),
    ];
    for (op, error) in results {
        assert!(matches!(error, Some(PortError::Timeout)), "{}", op.name());
        assert!(!op
            .enabled(ADAPTER_DEADLINE - Duration::from_nanos(1))
            .any(|outcome| outcome == Outcome::Fault(zakura_storage::header::Fault::Timeout)));
        assert!(op
            .enabled(ADAPTER_DEADLINE)
            .any(|outcome| outcome == Outcome::Fault(zakura_storage::header::Fault::Timeout)));
    }
    // Each wedged read waited out exactly one deadline.
    assert_eq!(
        tokio::time::Instant::now().duration_since(started),
        ADAPTER_DEADLINE * 5
    );

    let fixture = HeaderFixture::regtest();
    let headers = fixture.extend(anchor, 1, 0);
    let engine = EnginePort::new(&fixture);
    let request = fixture.target_request(&engine.snapshot(), anchor, &headers, 1);
    let before = tokio::time::Instant::now();
    let error = wedged
        .prepare_header_target(request)
        .await
        .expect_err("a wedged lease read fails");
    assert_eq!(
        tokio::time::Instant::now().duration_since(before),
        ADAPTER_DEADLINE
    );
    assert_eq!(
        error.category,
        zakura_header_chain::ErrorCategory::LocalResourceOrStorage
    );
    assert!(HeaderOp::PrepareHeaderTarget
        .enabled(ADAPTER_DEADLINE)
        .any(|outcome| outcome == Outcome::Fault(zakura_storage::header::Fault::LocalResource)));
}

/// `vct_repair_context` agrees: both answer `Stale`. The simulator always
/// does. Production refuses any repair above the checkpoint ceiling, which
/// `zakurad` sets to height 0 on Regtest, so on Regtest it always does too.
/// Expected difference: on a network with checkpoints, production can
/// resolve a repair that the simulator still answers `Stale`. Fixtures are
/// Regtest only.
#[tokio::test]
async fn vct_repair_context_matches_on_regtest() {
    let network = HeaderFixture::regtest().network().clone();
    for checkpoint_sync in [true, false] {
        let config = zakura_consensus::Config {
            checkpoint_sync,
            ..Default::default()
        };
        let (_, ceiling) = zakura_consensus::router::init_checkpoint_list(config, &network);
        // `production()` starts its state with this same ceiling.
        assert_eq!(ceiling, block::Height(0));
    }
    let mut replies = Vec::new();
    for port in both().await {
        chain(&*port).await;
        let owner = zakura_header_chain::BodyWorkAuthority::for_snapshot(&snapshot(&*port))
            .bind(1, std::num::NonZeroU64::new(1).expect("one is nonzero"));
        for height in [1, 3, 5] {
            replies.push(
                port.vct_repair_context(owner, block::Height(height))
                    .await
                    .expect("the repair context reads"),
            );
        }
    }
    assert!(replies
        .iter()
        .all(|reply| *reply == VctRepairContextReply::Stale));
}

/// Expected difference: no reservations. Production reserves a slot before
/// it resolves a path, so a concurrent acquire from the same peer can be
/// refused while the first one runs. The simulator runs each acquire
/// atomically, the ST1 contract's excluded-histories rule, so only
/// sequential histories exist, and in those both sides agree, as this test
/// and `capacity_and_release_match` show.
#[tokio::test]
async fn expected_difference_no_reservations_sequentially_agree() {
    let mut seen = Vec::new();
    for port in both().await {
        let port = &*port;
        let (main, _) = chain(port).await;
        // A valid request whose scope names another target passes the
        // locator checks, so production reserves a slot before refusing it.
        let mut scoped = acquire_request(port, peer(1), main[4].hash, vec![main[0].hash]);
        scoped.scope = HeaderWorkAuthority::for_target(&snapshot(port), main[3].hash);
        let refused = port.acquire_header_path(scoped).await;
        // The refusal leaves no reservation behind, so the peer's next
        // acquire succeeds.
        let next = port
            .acquire_header_path(acquire_request(
                port,
                peer(1),
                main[4].hash,
                vec![main[0].hash],
            ))
            .await;
        assert_eq!(acquire_shape(&refused), "Busy");
        assert!(
            matches!(next, Ok(AcquirePathReply::Acquired(_))),
            "{next:?}"
        );
        seen.push((acquire_shape(&refused), acquire_shape(&next)));
    }
    assert_eq!(seen[0], seen[1]);
}

/// Expected difference: sweep order. Production keeps leases in a
/// `HashMap`, so when one sweep ends several leases it fires their signals
/// in hash order. The simulator uses a `BTreeMap`, so it fires them in peer
/// order. No caller can see the order within one sweep, so both sides
/// release the same set.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn expected_difference_sweep_order_releases_the_same_set() {
    let mut seen = Vec::new();
    for port in both().await {
        let port = &*port;
        let (main, _) = chain(port).await;
        let mut waiting = Vec::new();
        for n in 1..=3 {
            leased(
                port.acquire_header_path(acquire_request(
                    port,
                    peer(n),
                    main[4].hash,
                    vec![main[0].hash],
                ))
                .await,
            );
            let Ok(AcquirePathReply::CapacityBusy(signal)) = port
                .acquire_header_path(acquire_request(
                    port,
                    peer(n),
                    main[3].hash,
                    vec![main[0].hash],
                ))
                .await
            else {
                panic!("a held lease refuses its peer");
            };
            waiting.push(signal);
        }
        tokio::time::advance(RETAINED_PATH_LEASE_IDLE).await;
        leased(
            port.acquire_header_path(acquire_request(
                port,
                peer(9),
                main[4].hash,
                vec![main[0].hash],
            ))
            .await,
        );
        seen.push(
            waiting
                .iter()
                .map(|signal| signal.is_released())
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(seen[0], seen[1]);
    assert_eq!(seen[0], [true, true, true]);
}
