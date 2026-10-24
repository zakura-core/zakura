//! Retained-path serving on the engine-backed port: leases, pages,
//! capacity, the idle sweep and the contract's input checks (storage.md
//! ST2.1 and ST2.2, and the ST1 contract's release events).

use std::{
    future::Future,
    pin::pin,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};

use zakura_chain::block;
use zakura_header_chain::{Frontier, HeaderWorkAuthority, SourceId};
use zakura_node_services::header_chain::{
    AcquirePath, AcquirePathReply, ApplyHeaderTargetOutcome, Port, PortError, ReadPath,
    ReadPathReply, RetainedHeaderPath, RetainedHeaderPathPage, ServingCapacitySignal,
};
use zakura_storage::header::{
    adapter::EnginePort,
    fixture::HeaderFixture,
    lease::{MAX_RETAINED_PATH_LEASES, RETAINED_PATH_LEASE_IDLE},
    HeaderReply, HeaderRequest,
};

/// Polls `future` once. Every adapter operation runs when first polled.
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("the adapter completes each operation on its first poll"),
    }
}

/// A port holding a five-header main chain and a two-header fork from
/// height 2. Returns the port and the main and fork frontiers.
fn chain() -> (EnginePort, Vec<Frontier>, Vec<Frontier>) {
    let fixture = HeaderFixture::regtest();
    let port = EnginePort::new(&fixture);
    let anchor = fixture.anchor();
    let main = admit(&fixture, &port, anchor, &fixture.extend(anchor, 5, 0), 1);
    let fork = admit(&fixture, &port, main[1], &fixture.extend(main[1], 2, 1), 2);
    (port, main, fork)
}

/// Prepares and applies `headers` on `parent`. Returns their frontiers.
fn admit(
    fixture: &HeaderFixture,
    port: &EnginePort,
    parent: Frontier,
    headers: &[Arc<block::Header>],
    request_id: u64,
) -> Vec<Frontier> {
    let request = fixture.target_request(&port.snapshot(), parent, headers, request_id);
    let prepared = ready(port.prepare_header_target(request)).expect("fixture headers prepare");
    assert_eq!(
        ready(port.apply_header_target(prepared)).expect("fixture headers apply"),
        ApplyHeaderTargetOutcome::Applied
    );
    (1..)
        .zip(headers)
        .map(|(offset, header)| {
            Frontier::new(block::Height(parent.height.0 + offset), header.hash())
        })
        .collect()
}

/// The peer numbered `n`.
fn peer(n: u8) -> SourceId {
    SourceId::from_digest([n; 32])
}

/// `peer`'s acquire toward `target` with `locators`, scoped as the v9
/// publisher scopes it.
fn acquire(
    port: &EnginePort,
    peer: SourceId,
    target: block::Hash,
    locators: Vec<block::Hash>,
) -> Result<AcquirePathReply, PortError> {
    ready(port.acquire_header_path(AcquirePath {
        source: peer,
        session_id: 1,
        scope: HeaderWorkAuthority::for_target(&port.snapshot(), target),
        target_tip_hash: target,
        locator_hashes: locators,
    }))
}

/// The leased path in `reply`.
fn acquired(reply: Result<AcquirePathReply, PortError>) -> RetainedHeaderPath {
    match reply {
        Ok(AcquirePathReply::Acquired(path)) => *path,
        other => panic!("expected a lease, got {other:?}"),
    }
}

/// The wait signal in a `CapacityBusy` `reply`.
fn busy(reply: Result<AcquirePathReply, PortError>) -> ServingCapacitySignal {
    match reply {
        Ok(AcquirePathReply::CapacityBusy(signal)) => signal,
        other => panic!("expected CapacityBusy, got {other:?}"),
    }
}

/// Reads up to `count` headers of `path` after `after`.
fn read(
    port: &EnginePort,
    path: &RetainedHeaderPath,
    after: block::Hash,
    count: u32,
) -> Result<ReadPathReply, PortError> {
    ready(port.read_header_path(
        path.clone(),
        ReadPath {
            after_hash: after,
            max_header_count: count,
            want_tree_aux: true,
        },
    ))
}

/// The page in `reply`.
fn page(reply: Result<ReadPathReply, PortError>) -> RetainedHeaderPathPage {
    match reply {
        Ok(ReadPathReply::Page(page)) => *page,
        other => panic!("expected a page, got {other:?}"),
    }
}

/// ST2.1: a path is served page by page. A served page consumes its lease,
/// and the next acquire continues from the last header sent.
#[test]
fn a_path_is_served_page_by_page() {
    let (port, main, _) = chain();
    let anchor = HeaderFixture::regtest().anchor();
    let tip = main[4];
    let path = acquired(acquire(&port, peer(1), tip.hash, vec![anchor.hash]));
    assert_eq!((path.common_ancestor, path.target), (anchor, tip));
    assert_eq!(port.live_leases(), 1);

    let first = page(read(&port, &path, anchor.hash, 2));
    assert_eq!(
        first.headers.iter().map(|h| h.hash()).collect::<Vec<_>>(),
        [main[0].hash, main[1].hash]
    );
    assert!(!first.complete);
    // No bodies are committed, so every size and root is unknown.
    assert_eq!(first.finalized_body_sizes, [None, None]);
    assert_eq!(first.finalized_tree_aux, [None, None]);
    assert_eq!(port.live_leases(), 0, "a served page consumes the lease");
    assert_eq!(
        read(&port, &path, main[1].hash, 2).expect("the read runs"),
        ReadPathReply::Unavailable,
        "a consumed lease can't be read again"
    );

    let next = acquired(acquire(&port, peer(1), tip.hash, vec![main[1].hash]));
    assert_eq!(next.common_ancestor, main[1]);
    let rest = page(read(&port, &next, main[1].hash, 10));
    assert_eq!(
        rest.headers.iter().map(|h| h.hash()).collect::<Vec<_>>(),
        [main[2].hash, main[3].hash, main[4].hash]
    );
    assert!(rest.complete);
}

/// ST2.1: a fork's path is served by its exact hash, and a target at the
/// common ancestor gives a complete, empty path.
#[test]
fn a_fork_and_an_empty_path_are_served() {
    let (port, main, fork) = chain();
    let path = acquired(acquire(&port, peer(1), fork[1].hash, vec![main[1].hash]));
    assert_eq!(path.common_ancestor, main[1]);
    let fork_page = page(read(&port, &path, main[1].hash, 10));
    assert_eq!(
        fork_page
            .headers
            .iter()
            .map(|h| h.hash())
            .collect::<Vec<_>>(),
        [fork[0].hash, fork[1].hash]
    );
    assert!(fork_page.complete);

    let empty = acquired(acquire(&port, peer(2), main[2].hash, vec![main[2].hash]));
    assert_eq!(empty.common_ancestor, empty.target);
}

/// Refusals that need no capacity check: an unknown target, a locator off
/// the path, a scope for another target, and out-of-bounds counts.
#[test]
fn bad_requests_are_refused() {
    let (port, main, _) = chain();
    assert_eq!(
        acquire(&port, peer(1), block::Hash([0xee; 32]), vec![main[0].hash]).expect("runs"),
        AcquirePathReply::TargetNotRetained
    );
    assert_eq!(
        acquire(&port, peer(1), main[4].hash, vec![block::Hash([0xee; 32])]).expect("runs"),
        AcquirePathReply::NoLocatorIntersection
    );
    let mut scoped = AcquirePath {
        source: peer(1),
        session_id: 1,
        scope: HeaderWorkAuthority::for_target(&port.snapshot(), main[3].hash),
        target_tip_hash: main[4].hash,
        locator_hashes: vec![main[0].hash],
    };
    assert_eq!(
        ready(port.acquire_header_path(scoped.clone())).expect("runs"),
        AcquirePathReply::Busy
    );
    for locators in [Vec::new(), vec![main[0].hash; 14]] {
        scoped.locator_hashes = locators;
        assert!(matches!(
            ready(port.acquire_header_path(scoped.clone())),
            Err(PortError::Unavailable { source: Some(_) })
        ));
    }
    let path = acquired(acquire(&port, peer(1), main[4].hash, vec![main[0].hash]));
    for count in [0, 4_001] {
        assert!(matches!(
            read(&port, &path, main[0].hash, count),
            Err(PortError::Unavailable { source: Some(_) })
        ));
    }
    assert_eq!(
        read(&port, &path, main[1].hash, 1).expect("runs"),
        ReadPathReply::Unavailable,
        "a read must start after the last header served"
    );
    assert_eq!(port.live_leases(), 1, "refused reads keep the lease");
}

/// A peer holds one lease. Its second acquire gets `CapacityBusy`, whose
/// signal fires when the first lease ends. Releases repeat safely, and a
/// released handle reads nothing.
#[test]
fn a_held_lease_refuses_its_peer_until_released() {
    let (port, main, _) = chain();
    let path = acquired(acquire(&port, peer(1), main[4].hash, vec![main[0].hash]));
    let signal = busy(acquire(&port, peer(1), main[3].hash, vec![main[0].hash]));
    assert!(!signal.is_released());
    for _ in 0..2 {
        ready(port.release_header_path(path.clone())).expect("a release succeeds");
    }
    assert!(signal.is_released());
    assert_eq!(port.live_leases(), 0);
    assert_eq!(
        read(&port, &path, main[0].hash, 1).expect("runs"),
        ReadPathReply::Unavailable
    );
    acquired(acquire(&port, peer(1), main[3].hash, vec![main[0].hash]));
}

/// Every slot taken refuses new peers with a shared signal, which fires
/// when any lease ends.
#[test]
fn full_capacity_refuses_until_a_slot_frees() {
    let (port, main, _) = chain();
    let paths: Vec<_> = (0..MAX_RETAINED_PATH_LEASES)
        .map(|n| {
            let n = u8::try_from(n).expect("16 peers fit u8");
            acquired(acquire(&port, peer(n), main[4].hash, vec![main[0].hash]))
        })
        .collect();
    assert_eq!(port.live_leases(), MAX_RETAINED_PATH_LEASES);
    let first = busy(acquire(&port, peer(100), main[4].hash, vec![main[0].hash]));
    let second = busy(acquire(&port, peer(101), main[4].hash, vec![main[0].hash]));
    assert_eq!(first, second, "waiters share the class's signal");
    ready(port.release_header_path(paths[3].clone())).expect("a release succeeds");
    assert!(first.is_released());
    acquired(acquire(&port, peer(100), main[4].hash, vec![main[0].hash]));
}

/// The idle sweep isn't per peer: peer A's lease, idle for 30 s, ends when
/// peer B acquires, and fires the signal A's second acquire waits on.
#[tokio::test(start_paused = true)]
async fn another_peers_acquire_sweeps_an_idle_lease() {
    let (port, main, _) = chain();
    let a = acquired(acquire(&port, peer(1), main[4].hash, vec![main[0].hash]));
    let waiting = busy(acquire(&port, peer(1), main[3].hash, vec![main[0].hash]));
    tokio::time::advance(RETAINED_PATH_LEASE_IDLE - Duration::from_millis(1)).await;
    acquired(acquire(&port, peer(2), main[3].hash, vec![main[0].hash]));
    assert_eq!(
        port.live_leases(),
        2,
        "A's lease is still inside its deadline"
    );
    assert!(!waiting.is_released());

    tokio::time::advance(Duration::from_millis(1)).await;
    // B's empty locator fails its input check before the registry, so it
    // sweeps nothing.
    assert!(acquire(&port, peer(2), main[3].hash, Vec::new()).is_err());
    assert_eq!(port.live_leases(), 2);
    assert!(!waiting.is_released());

    // C's acquire passes its checks, so it sweeps A's expired lease. B's,
    // taken 1 ms before A's deadline, lives on.
    acquired(acquire(&port, peer(3), main[3].hash, vec![main[0].hash]));
    assert!(waiting.is_released());
    assert_eq!(port.live_leases(), 2);
    assert_eq!(
        read(&port, &a, main[0].hash, 1).expect("runs"),
        ReadPathReply::Unavailable
    );
}

/// Rule A: a read or release with another adapter's handle gets the
/// contract's rejection and touches no lease.
#[test]
fn a_foreign_handle_gets_the_contract_rejection() {
    let (ours, main, _) = chain();
    let (theirs, _, _) = chain();
    let ours_path = acquired(acquire(&ours, peer(1), main[4].hash, vec![main[0].hash]));
    let foreign = acquired(acquire(&theirs, peer(1), main[4].hash, vec![main[0].hash]));
    let request = HeaderRequest::ReadHeaderPath {
        path: foreign.clone(),
        request: ReadPath {
            after_hash: main[0].hash,
            max_header_count: 1,
            want_tree_aux: false,
        },
    };
    assert!(matches!(
        request.rejection_reply(ours.adapter_key()),
        Some(HeaderReply::ReadHeaderPath(Err(PortError::Unavailable {
            source: None
        })))
    ));
    assert!(matches!(
        read(&ours, &foreign, main[0].hash, 1),
        Err(PortError::Unavailable { source: None })
    ));
    assert!(matches!(
        ready(ours.release_header_path(foreign)),
        Err(PortError::Unavailable { source: None })
    ));
    assert_eq!(ours.live_leases(), 1);
    page(read(&ours, &ours_path, main[0].hash, 1));
}
