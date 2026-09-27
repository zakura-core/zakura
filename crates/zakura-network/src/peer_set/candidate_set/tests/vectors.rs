//! Fixed test vectors for CandidateSet.

use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::Arc,
    time::Duration as StdDuration,
};

use chrono::{DateTime, Duration, Utc};
use tokio::time::{self, Instant};
use tracing::Span;

use zakura_chain::{parameters::Network::*, serialization::DateTime32};
use zakura_test::mock_service::{MockService, PanicAssertion};

use crate::{
    constants::{DEFAULT_MAX_CONNS_PER_IP, GET_ADDR_FANOUT, MIN_PEER_GET_ADDR_INTERVAL},
    types::{MetaAddr, PeerServices},
    AddressBook, Request, Response,
};

use super::super::{validate_addrs, CandidateSet};

/// Test that offset is applied when all addresses have `last_seen` times in the future.
#[test]
fn offsets_last_seen_times_in_the_future() {
    let last_seen_limit = DateTime32::now();
    let last_seen_limit_chrono = last_seen_limit.to_chrono();

    let input_peers = mock_gossiped_peers(vec![
        last_seen_limit_chrono + Duration::minutes(30),
        last_seen_limit_chrono + Duration::minutes(15),
        last_seen_limit_chrono + Duration::minutes(45),
    ]);

    let validated_peers: Vec<_> = validate_addrs(input_peers, last_seen_limit).collect();

    let expected_offset = Duration::minutes(45);
    let expected_peers = mock_gossiped_peers(vec![
        last_seen_limit_chrono + Duration::minutes(30) - expected_offset,
        last_seen_limit_chrono + Duration::minutes(15) - expected_offset,
        last_seen_limit_chrono + Duration::minutes(45) - expected_offset,
    ]);

    assert_eq!(validated_peers, expected_peers);
}

/// Test that offset is not applied if all addresses have `last_seen` times that are in the past.
#[test]
fn doesnt_offset_last_seen_times_in_the_past() {
    let last_seen_limit = DateTime32::now();
    let last_seen_limit_chrono = last_seen_limit.to_chrono();

    let input_peers = mock_gossiped_peers(vec![
        last_seen_limit_chrono - Duration::minutes(30),
        last_seen_limit_chrono - Duration::minutes(45),
        last_seen_limit_chrono - Duration::days(1),
    ]);

    let validated_peers: Vec<_> = validate_addrs(input_peers.clone(), last_seen_limit).collect();

    let expected_peers = input_peers;

    assert_eq!(validated_peers, expected_peers);
}

/// Test that offset is applied to all the addresses if at least one has a `last_seen` time in the
/// future.
///
/// Times that are in the past should be changed as well.
#[test]
fn offsets_all_last_seen_times_if_one_is_in_the_future() {
    let last_seen_limit = DateTime32::now();
    let last_seen_limit_chrono = last_seen_limit.to_chrono();

    let input_peers = mock_gossiped_peers(vec![
        last_seen_limit_chrono + Duration::minutes(55),
        last_seen_limit_chrono - Duration::days(3),
        last_seen_limit_chrono - Duration::hours(2),
    ]);

    let validated_peers: Vec<_> = validate_addrs(input_peers, last_seen_limit).collect();

    let expected_offset = Duration::minutes(55);
    let expected_peers = mock_gossiped_peers(vec![
        last_seen_limit_chrono + Duration::minutes(55) - expected_offset,
        last_seen_limit_chrono - Duration::days(3) - expected_offset,
        last_seen_limit_chrono - Duration::hours(2) - expected_offset,
    ]);

    assert_eq!(validated_peers, expected_peers);
}

/// Test that offset is not applied if the most recent `last_seen` time is equal to the limit.
#[test]
fn doesnt_offsets_if_most_recent_last_seen_times_is_exactly_the_limit() {
    let last_seen_limit = DateTime32::now();
    let last_seen_limit_chrono = last_seen_limit.to_chrono();

    let input_peers = mock_gossiped_peers(vec![
        last_seen_limit_chrono,
        last_seen_limit_chrono - Duration::minutes(3),
        last_seen_limit_chrono - Duration::hours(1),
    ]);

    let validated_peers: Vec<_> = validate_addrs(input_peers.clone(), last_seen_limit).collect();

    let expected_peers = input_peers;

    assert_eq!(validated_peers, expected_peers);
}

/// Rejects all addresses if underflow occurs when applying the offset.
#[test]
fn rejects_all_addresses_if_applying_offset_causes_an_underflow() {
    let last_seen_limit = DateTime32::now();

    let input_peers = mock_gossiped_peers(vec![
        DateTime32::from(u32::MIN).to_chrono(),
        last_seen_limit.to_chrono(),
        DateTime32::from(u32::MAX).to_chrono(),
    ]);

    let mut validated_peers = validate_addrs(input_peers, last_seen_limit);

    assert!(validated_peers.next().is_none());
}

/// Pruned peers are connection candidates only while pruned peer dialing is enabled.
#[tokio::test]
async fn candidate_set_dials_pruned_peers_only_when_enabled() {
    let _init_guard = zakura_test::init();

    let pruned_addr: SocketAddr = "127.0.0.1:8233".parse().unwrap();
    let mut address_book = AddressBook::new(
        SocketAddr::from_str("0.0.0.0:0").unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        Span::none(),
    );
    address_book.update(
        MetaAddr::new_gossiped_meta_addr(
            pruned_addr.into(),
            PeerServices::empty(),
            DateTime32::now(),
        )
        .new_gossiped_change()
        .expect("gossiped MetaAddr produces a NewGossiped change"),
    );
    let address_book = Arc::new(std::sync::Mutex::new(address_book));
    let peer_service = MockService::build().for_unit_tests::<Request, Response, _>();

    let mut disabled = CandidateSet::new(address_book.clone(), peer_service.clone());
    assert_eq!(disabled.next().await, None);

    let (dial_pruned_tx, dial_pruned_rx) = tokio::sync::watch::channel(false);
    let mut candidates =
        CandidateSet::new(address_book, peer_service).with_pruned_peer_dialing(dial_pruned_rx, 1);
    assert_eq!(candidates.next().await, None);

    dial_pruned_tx.send_replace(true);
    let candidate = candidates
        .next()
        .await
        .expect("pruned peers are candidates when dialing is enabled");
    assert_eq!(candidate.addr(), pruned_addr.into());
}

/// Pruned peers stop being candidates once the pruned outbound cap is reached,
/// but full nodes are still returned.
#[tokio::test]
async fn candidate_set_caps_pruned_outbound_peers() {
    let _init_guard = zakura_test::init();

    let pruned_addrs: [SocketAddr; 2] = [
        "127.0.0.1:8233".parse().unwrap(),
        "127.0.0.2:8233".parse().unwrap(),
    ];
    let full_addr: SocketAddr = "127.0.0.3:8233".parse().unwrap();
    let mut address_book = AddressBook::new(
        SocketAddr::from_str("0.0.0.0:0").unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        Span::none(),
    );
    for addr in pruned_addrs {
        address_book.update(
            MetaAddr::new_gossiped_meta_addr(addr.into(), PeerServices::empty(), DateTime32::now())
                .new_gossiped_change()
                .expect("gossiped MetaAddr produces a NewGossiped change"),
        );
    }
    let address_book = Arc::new(std::sync::Mutex::new(address_book));
    let peer_service = MockService::build().for_unit_tests::<Request, Response, _>();

    let (_dial_pruned_tx, dial_pruned_rx) = tokio::sync::watch::channel(true);
    let mut candidates = CandidateSet::new(address_book.clone(), peer_service)
        .with_pruned_peer_dialing(dial_pruned_rx, 1);

    let first = candidates
        .next()
        .await
        .expect("the first pruned peer is under the cap");
    assert!(pruned_addrs
        .iter()
        .any(|addr| first.addr() == (*addr).into()));
    assert_eq!(
        candidates.next().await,
        None,
        "the pending attempt to the first pruned peer fills the cap",
    );

    address_book.lock().unwrap().update(
        MetaAddr::new_gossiped_meta_addr(
            full_addr.into(),
            PeerServices::NODE_NETWORK,
            DateTime32::now(),
        )
        .new_gossiped_change()
        .expect("gossiped MetaAddr produces a NewGossiped change"),
    );
    let full = candidates
        .next()
        .await
        .expect("full nodes are candidates regardless of the pruned cap");
    assert_eq!(full.addr(), full_addr.into());
}

/// Test that calls to [`CandidateSet::update`] are rate limited.
#[test]
fn candidate_set_updates_are_rate_limited() {
    // Run the test for enough time for `update` to actually run three times
    const INTERVALS_TO_RUN: u32 = 3;
    // How many times should `update` be called in each rate limit interval
    const POLL_FREQUENCY_FACTOR: u32 = 3;

    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();

    let address_book = AddressBook::new(
        SocketAddr::from_str("0.0.0.0:0").unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        Span::none(),
    );
    let mut peer_service = MockService::build().for_unit_tests();
    let mut candidate_set = CandidateSet::new(
        Arc::new(std::sync::Mutex::new(address_book)),
        peer_service.clone(),
    );

    runtime.block_on(async move {
        time::pause();

        let time_limit = Instant::now()
            + INTERVALS_TO_RUN * MIN_PEER_GET_ADDR_INTERVAL
            + StdDuration::from_secs(1);
        let mut next_allowed_request_time = Instant::now();

        while Instant::now() <= time_limit {
            candidate_set
                .update()
                .await
                .expect("Call to CandidateSet::update should not fail");

            if Instant::now() >= next_allowed_request_time {
                verify_fanned_out_requests(&mut peer_service).await;

                next_allowed_request_time = Instant::now() + MIN_PEER_GET_ADDR_INTERVAL;
            } else {
                peer_service.expect_no_requests().await;
            }

            time::advance(MIN_PEER_GET_ADDR_INTERVAL / POLL_FREQUENCY_FACTOR).await;
        }
    });
}

/// Test that a call to [`CandidateSet::update`] after a call to [`CandidateSet::update_initial`] is
/// rate limited.
#[test]
fn candidate_set_update_after_update_initial_is_rate_limited() {
    let (runtime, _init_guard) = zakura_test::init_async();
    let _guard = runtime.enter();

    let address_book = AddressBook::new(
        SocketAddr::from_str("0.0.0.0:0").unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        Span::none(),
    );
    let mut peer_service = MockService::build().for_unit_tests();
    let mut candidate_set = CandidateSet::new(
        Arc::new(std::sync::Mutex::new(address_book)),
        peer_service.clone(),
    );

    runtime.block_on(async move {
        time::pause();

        // Call `update_initial` first
        candidate_set
            .update_initial(GET_ADDR_FANOUT)
            .await
            .expect("Call to CandidateSet::update should not fail");

        verify_fanned_out_requests(&mut peer_service).await;

        // The following two calls to `update` should be skipped
        candidate_set
            .update()
            .await
            .expect("Call to CandidateSet::update should not fail");
        time::advance(MIN_PEER_GET_ADDR_INTERVAL / 2).await;
        candidate_set
            .update()
            .await
            .expect("Call to CandidateSet::update should not fail");

        peer_service.expect_no_requests().await;

        // After waiting for at least the minimum interval the call to `update` should succeed
        time::advance(MIN_PEER_GET_ADDR_INTERVAL).await;
        candidate_set
            .update()
            .await
            .expect("Call to CandidateSet::update should not fail");

        verify_fanned_out_requests(&mut peer_service).await;
    });
}

// Utility functions

/// Create a mock list of gossiped [`MetaAddr`]s with the specified `last_seen_times`.
///
/// The IP address and port of the generated ports should not matter for the test.
fn mock_gossiped_peers(last_seen_times: impl IntoIterator<Item = DateTime<Utc>>) -> Vec<MetaAddr> {
    last_seen_times
        .into_iter()
        .enumerate()
        .map(|(index, last_seen_chrono)| {
            let last_seen = last_seen_chrono
                .try_into()
                .expect("`last_seen` time doesn't fit in a `DateTime32`");

            MetaAddr::new_gossiped_meta_addr(
                SocketAddr::new(IpAddr::from([192, 168, 1, index as u8]), 20_000).into(),
                PeerServices::NODE_NETWORK,
                last_seen,
            )
        })
        .collect()
}

/// Verify that a batch of fanned out requests are sent by the candidate set.
///
/// # Panics
///
/// This will panic (causing the test to fail) if more or less requests are received than the
/// expected [`GET_ADDR_FANOUT`] amount.
async fn verify_fanned_out_requests(
    peer_service: &mut MockService<Request, Response, PanicAssertion>,
) {
    for _ in 0..GET_ADDR_FANOUT {
        peer_service
            .expect_request_that(|request| matches!(request, Request::Peers))
            .await
            .respond(Response::Peers(vec![]));
    }

    peer_service.expect_no_requests().await;
}
