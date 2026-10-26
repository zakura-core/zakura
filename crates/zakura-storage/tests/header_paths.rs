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

/// A port with a chain of `length` headers on the anchor, admitted in
/// batches no longer than the transition cap. Returns the port and the
/// headers' frontiers.
fn long_chain(length: u32) -> (EnginePort, Vec<Frontier>) {
    let fixture = HeaderFixture::regtest();
    let port = EnginePort::new(&fixture);
    let mut frontiers = Vec::new();
    let mut parent = fixture.anchor();
    let cap = u32::try_from(zakura_header_chain::MAX_HEADERS_PER_TRANSITION_V1)
        .expect("the cap fits u32");
    let mut request_id = 1;
    while frontiers.len() < length as usize {
        // Widening a header count to usize is lossless on supported targets.
        let remaining = length - frontiers.len() as u32;
        let batch = admit(
            &fixture,
            &port,
            parent,
            &fixture.extend(parent, remaining.min(cap), 0),
            request_id,
        );
        parent = *batch.last().expect("a batch has headers");
        frontiers.extend(batch);
        request_id += 1;
    }
    (port, frontiers)
}

/// The reserve slot serves a path of exactly 4,000 headers and refuses
/// 4,001 as beyond reserve. The two refusals wait on different signals:
/// beyond reserve waits for an ordinary slot, and a full registry waits for
/// any slot.
#[test]
fn the_reserve_slot_boundary_is_4000_headers() {
    let (port, chain) = long_chain(4_001);
    let anchor = HeaderFixture::regtest().anchor();
    // Fill the 15 ordinary slots with short paths.
    let ordinary: Vec<_> = (1..MAX_RETAINED_PATH_LEASES)
        .map(|n| {
            let n = u8::try_from(n).expect("16 peers fit u8");
            acquired(acquire(&port, peer(n), chain[1].hash, vec![chain[0].hash]))
        })
        .collect();
    let beyond = busy(acquire(
        &port,
        peer(100),
        chain[4_000].hash,
        vec![anchor.hash],
    ));
    let reserve = acquired(acquire(
        &port,
        peer(101),
        chain[3_999].hash,
        vec![anchor.hash],
    ));
    assert_eq!(
        (reserve.common_ancestor, reserve.target),
        (anchor, chain[3_999])
    );
    let full = busy(acquire(
        &port,
        peer(102),
        chain[1].hash,
        vec![chain[0].hash],
    ));
    assert_ne!(beyond, full, "the two refusals wait on different signals");

    // Freeing the reserve slot wakes the full-registry waiter only.
    ready(port.release_header_path(reserve)).expect("a release succeeds");
    assert!(full.is_released());
    assert!(!beyond.is_released());
    // Freeing an ordinary slot wakes the beyond-reserve waiter.
    ready(port.release_header_path(ordinary[0].clone())).expect("a release succeeds");
    assert!(beyond.is_released());
    acquired(acquire(
        &port,
        peer(100),
        chain[4_000].hash,
        vec![anchor.hash],
    ));
}

/// The headers of `reply`'s page, by hash.
fn page_hashes(reply: Result<ReadPathReply, PortError>) -> (Vec<block::Hash>, bool) {
    let page = page(reply);
    (
        page.headers.iter().map(|h| h.hash()).collect(),
        page.complete,
    )
}

/// The continuation cache. A matching acquire continues from the cached
/// cursor. A session, target or locator mismatch drops the cursor and walks
/// the graph instead, which shows in the lease each one gets. A full cache
/// evicts its lowest lease ID, and a late release of a consumed handle can't
/// end its replacement.
#[test]
fn the_continuation_cache_matches_production() {
    let (port, main, _) = chain();
    let anchor = HeaderFixture::regtest().anchor();
    // Peer `n` leases toward the main tip from the anchor and reads one
    // header, so its cached cursor stops at main[0] toward main[4].
    let consume = |n: u8| {
        let path = acquired(acquire(&port, peer(n), main[4].hash, vec![anchor.hash]));
        page(read(&port, &path, anchor.hash, 1));
        path
    };

    // A session mismatch walks the graph. A walk from main[0] builds the
    // same lease a continuation would, so the hit count is the witness.
    consume(1);
    let hits = port.continuation_hits();
    let wrong_session = acquired(ready(port.acquire_header_path(AcquirePath {
        source: peer(1),
        session_id: 2,
        scope: HeaderWorkAuthority::for_target(&port.snapshot(), main[4].hash),
        target_tip_hash: main[4].hash,
        locator_hashes: vec![main[0].hash],
    })));
    assert_eq!(
        port.continuation_hits(),
        hits,
        "the session mismatch walked"
    );
    assert_eq!(
        (wrong_session.common_ancestor, wrong_session.target),
        (main[0], main[4])
    );
    assert!(!port.continued_peers().contains(&peer(1)));
    ready(port.release_header_path(wrong_session)).expect("a release succeeds");

    // A target mismatch gets the requested target. Reusing the cursor
    // would give main[4].
    consume(1);
    let wrong_target = acquired(acquire(&port, peer(1), main[3].hash, vec![main[0].hash]));
    assert_eq!(port.continuation_hits(), hits);
    assert_eq!(
        (wrong_target.common_ancestor, wrong_target.target),
        (main[0], main[3])
    );
    assert_eq!(
        page_hashes(read(&port, &wrong_target, main[0].hash, 10)),
        (vec![main[1].hash, main[2].hash, main[3].hash], true)
    );
    assert!(!port.continued_peers().contains(&peer(1)));

    // A locator that isn't the cursor's last header starts from that
    // locator. Reusing the cursor would start after main[0].
    consume(1);
    let wrong_locator = acquired(acquire(&port, peer(1), main[4].hash, vec![anchor.hash]));
    assert_eq!(port.continuation_hits(), hits);
    assert_eq!(
        (wrong_locator.common_ancestor, wrong_locator.target),
        (anchor, main[4])
    );
    assert_eq!(
        page_hashes(read(&port, &wrong_locator, anchor.hash, 1)),
        (vec![main[0].hash], false)
    );
    assert!(
        port.continued_peers().contains(&peer(1)),
        "the read cached a new cursor"
    );

    // A matching acquire continues: it's a hit, and it serves the rest of
    // the cached path. The late release of the consumed handle doesn't end
    // the new lease.
    let old = acquired(acquire(&port, peer(1), main[4].hash, vec![main[0].hash]));
    assert_eq!(port.continuation_hits(), hits + 1, "the match continued");
    assert_eq!((old.common_ancestor, old.target), (main[0], main[4]));
    page(read(&port, &old, main[0].hash, 1));
    let next = acquired(acquire(&port, peer(1), main[4].hash, vec![main[1].hash]));
    assert_eq!(port.continuation_hits(), hits + 2);
    ready(port.release_header_path(old)).expect("a stale release succeeds");
    assert_eq!(
        port.live_leases(),
        1,
        "the stale release left the new lease"
    );
    assert_eq!(
        page_hashes(read(&port, &next, main[1].hash, 10)),
        (vec![main[2].hash, main[3].hash, main[4].hash], true)
    );

    // Seventeen continuations, made in descending peer order, so lease IDs
    // rise as peer IDs fall. The cache keeps 16 and evicts the lowest lease
    // ID, which is peer 17's, not the lowest peer.
    for n in (1..=17).rev() {
        consume(n);
    }
    let cached = port.continued_peers();
    assert_eq!(cached.len(), MAX_RETAINED_PATH_LEASES);
    assert!(
        !cached.contains(&peer(17)),
        "peer 17 held the lowest lease ID"
    );
    assert!(cached.contains(&peer(1)), "the lowest peer stays");
}

/// A read sweeps every expired lease and fires their signals, and a page
/// that's served consumes its lease and fires the signal its peer waits on.
#[tokio::test(start_paused = true)]
async fn a_read_sweeps_and_a_served_page_frees_its_slot() {
    let (port, main, _) = chain();
    let a = acquired(acquire(&port, peer(1), main[4].hash, vec![main[0].hash]));
    let b = acquired(acquire(&port, peer(2), main[4].hash, vec![main[0].hash]));
    let b_waiting = busy(acquire(&port, peer(2), main[3].hash, vec![main[0].hash]));
    tokio::time::advance(RETAINED_PATH_LEASE_IDLE - Duration::from_millis(1)).await;
    let c = acquired(acquire(&port, peer(3), main[4].hash, vec![main[0].hash]));
    let c_waiting = busy(acquire(&port, peer(3), main[3].hash, vec![main[0].hash]));
    tokio::time::advance(Duration::from_millis(1)).await;

    // A's read sweeps A's and B's expired leases, but not C's.
    assert_eq!(
        read(&port, &a, main[0].hash, 1).expect("runs"),
        ReadPathReply::Unavailable
    );
    assert!(b_waiting.is_released());
    assert_eq!(port.live_leases(), 1);
    assert_eq!(
        read(&port, &b, main[0].hash, 1).expect("runs"),
        ReadPathReply::Unavailable
    );

    // C's page consumes its lease and wakes C's waiting acquire.
    assert!(!c_waiting.is_released());
    page(read(&port, &c, main[0].hash, 1));
    assert!(c_waiting.is_released());
    assert_eq!(port.live_leases(), 0);
}
