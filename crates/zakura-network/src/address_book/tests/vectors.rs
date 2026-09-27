//! Fixed test vectors for the address book.

use std::time::Instant;

use chrono::Utc;
use tracing::Span;

use zakura_chain::{
    parameters::Network::*,
    serialization::{DateTime32, Duration32},
};

use crate::{
    constants::{
        DEFAULT_MAX_CONNS_PER_IP, MAX_ADDRS_IN_ADDRESS_BOOK, MAX_PEER_MISBEHAVIOR_SCORE,
        PRUNED_ADDR_RESPONSE_SHARE_DENOMINATOR,
    },
    meta_addr::{MetaAddr, MetaAddrChange},
    protocol::external::types::PeerServices,
    AddressBook,
};

/// Make sure an empty address book is actually empty.
#[test]
fn address_book_empty() {
    let address_book = AddressBook::new(
        "0.0.0.0:0".parse().unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        Span::current(),
    );

    assert_eq!(
        address_book
            .reconnection_peers(Instant::now(), Utc::now())
            .next(),
        None
    );
    assert_eq!(address_book.len(), 0);
}

/// Peer addresses stay redacted unless the address book is explicitly configured to expose them.
#[test]
fn peer_address_exposure_requires_explicit_opt_in() {
    let address_book = AddressBook::new(
        "0.0.0.0:0".parse().unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        Span::current(),
    );

    assert!(!address_book.expose_peer_addresses);
    let address_book = address_book.with_expose_peer_addresses(true);

    assert!(address_book.expose_peer_addresses);
    assert!(address_book.clone().expose_peer_addresses);
}

/// Helper: build a `MetaAddrChange::NewGossiped` for a given address and
/// last-seen time. Used to seed the address book before triggering a ban so
/// the test exercises the by-IP cleanup loop on real entries.
fn gossiped_change(
    addr: crate::PeerSocketAddr,
    services: PeerServices,
    untrusted_last_seen: DateTime32,
) -> MetaAddrChange {
    MetaAddr::new_gossiped_meta_addr(addr, services, untrusted_last_seen)
        .new_gossiped_change()
        .expect("gossiped MetaAddr should produce a NewGossiped change")
}

/// Regression test for https://github.com/ZcashFoundation/zebra/issues/10580.
///
/// Applying a ban-threshold misbehavior update with
/// `max_connections_per_ip > 1` must remove every address for the banned IP,
/// even when peer priority separates those addresses in `by_addr`.
#[test]
fn misbehavior_ban_removes_all_addresses_for_ip() {
    let banned_addr: crate::PeerSocketAddr = "127.0.0.1:8233".parse().unwrap();
    let other_port_same_ip: crate::PeerSocketAddr = "127.0.0.1:8234".parse().unwrap();
    let unrelated_addr: crate::PeerSocketAddr = "127.0.0.2:8233".parse().unwrap();

    let mut address_book =
        AddressBook::new("0.0.0.0:0".parse().unwrap(), &Mainnet, 2, Span::current());
    let mut address_metrics = address_book.address_metrics_watcher();

    // Seed two entries on the soon-to-be-banned IP plus an unrelated entry,
    // so the ban path's per-IP cleanup has visible work to do.
    address_book.update(gossiped_change(
        banned_addr,
        PeerServices::NODE_NETWORK,
        DateTime32::MIN,
    ));
    address_book.update(gossiped_change(
        other_port_same_ip,
        PeerServices::NODE_NETWORK,
        DateTime32::MIN.saturating_add(Duration32::from_seconds(1)),
    ));
    address_book.update(gossiped_change(
        unrelated_addr,
        PeerServices::NODE_NETWORK,
        DateTime32::MIN.saturating_add(Duration32::from_seconds(2)),
    ));
    address_book.peers.assert_consistent();

    // Put the banned and unrelated addresses in the Responded state, leaving
    // the other same-IP address in the lower-priority gossiped state.
    address_book.update(MetaAddr::new_reconnect(banned_addr));
    address_book.update(MetaAddr::new_responded(banned_addr, None));
    address_book.update(MetaAddr::new_reconnect(unrelated_addr));
    address_book.update(MetaAddr::new_responded(unrelated_addr, None));

    assert!(address_book.get(banned_addr).is_some());
    assert!(address_book.get(other_port_same_ip).is_some());

    let ordered_addrs: Vec<_> = address_book.peers().map(|peer| peer.addr()).collect();
    let same_ip_positions: Vec<_> = ordered_addrs
        .iter()
        .enumerate()
        .filter_map(|(index, addr)| (addr.ip() == banned_addr.ip()).then_some(index))
        .collect();
    assert_eq!(same_ip_positions.len(), 2);
    assert!(same_ip_positions[1] > same_ip_positions[0] + 1);

    let bans = address_book.bans();
    assert_eq!(address_metrics.borrow_and_update().num_addresses, 3);

    address_book.update(MetaAddrChange::UpdateMisbehavior {
        addr: banned_addr,
        score_increment: MAX_PEER_MISBEHAVIOR_SCORE,
    });
    address_book.peers.assert_consistent();

    assert!(
        bans.contains(banned_addr.ip()),
        "ban-threshold misbehavior should ban the peer IP"
    );
    assert!(
        address_book.get(banned_addr).is_none(),
        "primary banned address should be removed from the address book"
    );
    assert!(
        address_book.get(other_port_same_ip).is_none(),
        "all addresses for the banned IP should be removed from the address book"
    );
    assert!(
        address_book.get(unrelated_addr).is_some(),
        "unrelated IP entries should remain after banning a different IP"
    );
    assert!(
        address_metrics.has_changed().unwrap(),
        "the ban should publish updated address metrics"
    );
    assert_eq!(
        address_metrics.borrow_and_update().num_addresses,
        1,
        "published metrics should exclude all addresses on the banned IP"
    );
}

/// Gossiped peers without `NODE_NETWORK` are kept and gossiped, but are not
/// attempted or cached, because they may not serve historical blocks.
#[test]
fn pruned_peers_are_gossiped_but_not_attempted_or_cached() {
    let full_addr: crate::PeerSocketAddr = "127.0.0.1:8233".parse().unwrap();
    let pruned_addr: crate::PeerSocketAddr = "127.0.0.2:8233".parse().unwrap();

    let mut address_book = AddressBook::new(
        "0.0.0.0:0".parse().unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        Span::current(),
    );
    address_book.update(gossiped_change(
        full_addr,
        PeerServices::NODE_NETWORK,
        DateTime32::now(),
    ));
    address_book.update(gossiped_change(
        pruned_addr,
        PeerServices::empty(),
        DateTime32::now(),
    ));

    let pruned = address_book
        .get(pruned_addr)
        .expect("gossiped pruned peers are kept in the address book");
    assert!(!pruned.is_full_node());

    let reconnection_peers: Vec<_> = address_book
        .reconnection_peers(Instant::now(), Utc::now())
        .map(|peer| peer.addr())
        .collect();
    assert_eq!(reconnection_peers, vec![full_addr]);

    let cacheable: Vec<_> = address_book
        .cacheable(Utc::now())
        .into_iter()
        .map(|peer| peer.addr())
        .collect();
    assert_eq!(cacheable, vec![full_addr]);

    let sanitized = address_book.sanitized(Utc::now());
    let sanitized_pruned = sanitized
        .iter()
        .find(|peer| peer.addr() == pruned_addr)
        .expect("pruned peers are gossiped");
    assert_eq!(sanitized_pruned.services, Some(PeerServices::empty()));
}

/// Only live or pending outbound connections to pruned peers count toward
/// the pruned outbound connection cap.
#[test]
fn pruned_outbound_peer_count_counts_live_and_pending_outbound_pruned_peers() {
    let gossiped_only: crate::PeerSocketAddr = "127.0.0.1:8233".parse().unwrap();
    let pending: crate::PeerSocketAddr = "127.0.0.2:8233".parse().unwrap();
    let live: crate::PeerSocketAddr = "127.0.0.3:8233".parse().unwrap();
    let inbound: crate::PeerSocketAddr = "127.0.0.4:8233".parse().unwrap();
    let full: crate::PeerSocketAddr = "127.0.0.5:8233".parse().unwrap();

    let mut address_book = AddressBook::new(
        "0.0.0.0:0".parse().unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        Span::current(),
    );

    // Gossiped, but never attempted: not counted.
    address_book.update(gossiped_change(
        gossiped_only,
        PeerServices::empty(),
        DateTime32::now(),
    ));
    // Outbound attempt in progress: counted.
    address_book.update(gossiped_change(
        pending,
        PeerServices::empty(),
        DateTime32::now(),
    ));
    address_book.update(MetaAddr::new_reconnect(pending));
    // Live outbound connection: counted.
    address_book.update(MetaAddr::new_connected(live, &PeerServices::empty(), false));
    address_book.update(MetaAddr::new_responded(live, None));
    // Live inbound connection: not counted.
    address_book.update(MetaAddr::new_connected(
        inbound,
        &PeerServices::empty(),
        true,
    ));
    address_book.update(MetaAddr::new_responded(inbound, None));
    // Live outbound full node: not counted.
    address_book.update(MetaAddr::new_connected(
        full,
        &PeerServices::NODE_NETWORK,
        false,
    ));
    address_book.update(MetaAddr::new_responded(full, None));

    assert_eq!(address_book.pruned_outbound_peer_count(Utc::now()), 2);
}

/// A pruned node gossips its own listener address with its real services.
#[test]
fn pruned_local_listener_is_gossiped() {
    let local_listener: std::net::SocketAddr = "127.0.0.1:8233".parse().unwrap();
    let address_book = AddressBook::new(
        local_listener,
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        Span::current(),
    )
    .with_local_listener_services(PeerServices::empty());

    let gossiped = address_book.fresh_get_addr_response();

    assert_eq!(gossiped.len(), 1);
    assert_eq!(gossiped[0].addr(), local_listener.into());
    assert_eq!(gossiped[0].services, Some(PeerServices::empty()));
}

/// Pruned peers fill at most a bounded share of each `GetAddr` response.
#[test]
fn get_addr_response_bounds_pruned_share() {
    const PEERS_PER_KIND: u8 = 16;

    let mut address_book = AddressBook::new(
        "0.0.0.0:0".parse().unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        Span::current(),
    );
    for index in 0..PEERS_PER_KIND {
        address_book.update(gossiped_change(
            format!("127.0.1.{index}:8233").parse().unwrap(),
            PeerServices::NODE_NETWORK,
            DateTime32::now(),
        ));
        address_book.update(gossiped_change(
            format!("127.0.2.{index}:8233").parse().unwrap(),
            PeerServices::empty(),
            DateTime32::now(),
        ));
    }

    let gossiped = address_book.fresh_get_addr_response();
    let gossiped_pruned = gossiped.iter().filter(|peer| !peer.is_full_node()).count();

    // Half of the 32 active addresses are gossiped, and at most a quarter of those are pruned.
    assert_eq!(gossiped.len(), usize::from(PEERS_PER_KIND));
    assert!(
        gossiped_pruned
            <= gossiped
                .len()
                .div_ceil(PRUNED_ADDR_RESPONSE_SHARE_DENOMINATOR)
    );
}

/// Make sure peers are attempted in priority order.
#[test]
fn address_book_peer_order() {
    let addr1 = "127.0.0.1:1".parse().unwrap();
    let addr2 = "127.0.0.2:2".parse().unwrap();

    let mut meta_addr1 =
        MetaAddr::new_gossiped_meta_addr(addr1, PeerServices::NODE_NETWORK, DateTime32::MIN);
    let mut meta_addr2 = MetaAddr::new_gossiped_meta_addr(
        addr2,
        PeerServices::NODE_NETWORK,
        DateTime32::MIN.saturating_add(Duration32::from_seconds(1)),
    );

    // Regardless of the order of insertion, the most recent address should be chosen first
    let addrs = vec![meta_addr1, meta_addr2];
    let address_book = AddressBook::new_with_addrs(
        "0.0.0.0:0".parse().unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        MAX_ADDRS_IN_ADDRESS_BOOK,
        Span::current(),
        addrs,
    );
    assert_eq!(
        address_book
            .reconnection_peers(Instant::now(), Utc::now())
            .next(),
        Some(meta_addr2),
    );

    // Reverse the order, check that we get the same result
    let addrs = vec![meta_addr2, meta_addr1];
    let address_book = AddressBook::new_with_addrs(
        "0.0.0.0:0".parse().unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        MAX_ADDRS_IN_ADDRESS_BOOK,
        Span::current(),
        addrs,
    );
    assert_eq!(
        address_book
            .reconnection_peers(Instant::now(), Utc::now())
            .next(),
        Some(meta_addr2),
    );

    // Now check that the order depends on the time, not the address
    meta_addr1.addr = addr2;
    meta_addr2.addr = addr1;

    let addrs = vec![meta_addr1, meta_addr2];
    let address_book = AddressBook::new_with_addrs(
        "0.0.0.0:0".parse().unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        MAX_ADDRS_IN_ADDRESS_BOOK,
        Span::current(),
        addrs,
    );
    assert_eq!(
        address_book
            .reconnection_peers(Instant::now(), Utc::now())
            .next(),
        Some(meta_addr2),
    );

    // Reverse the order, check that we get the same result
    let addrs = vec![meta_addr2, meta_addr1];
    let address_book = AddressBook::new_with_addrs(
        "0.0.0.0:0".parse().unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        MAX_ADDRS_IN_ADDRESS_BOOK,
        Span::current(),
        addrs,
    );
    assert_eq!(
        address_book
            .reconnection_peers(Instant::now(), Utc::now())
            .next(),
        Some(meta_addr2),
    );
}

/// Check that `reconnection_peers` skips addresses with IPs for which
/// Zebra already has recently updated outbound peers.
#[test]
fn reconnection_peers_skips_recently_updated_ip() {
    // tests that reconnection_peers() skips addresses where there's a connection at that IP with a recent:
    // - `last_response`
    test_reconnection_peers_skips_recently_updated_ip(true, |addr| {
        MetaAddr::new_responded(addr, None)
    });

    // tests that reconnection_peers() *does not* skip addresses where there's a connection at that IP with a recent:
    // - `last_attempt`
    test_reconnection_peers_skips_recently_updated_ip(false, MetaAddr::new_reconnect);
    // - `last_failure`
    test_reconnection_peers_skips_recently_updated_ip(false, |addr| {
        MetaAddr::new_errored(addr, PeerServices::NODE_NETWORK)
    });
}

fn test_reconnection_peers_skips_recently_updated_ip<
    M: Fn(crate::PeerSocketAddr) -> crate::meta_addr::MetaAddrChange,
>(
    should_skip_ip: bool,
    make_meta_addr_change: M,
) {
    let addr1 = "127.0.0.1:1".parse().unwrap();
    let addr2 = "127.0.0.1:2".parse().unwrap();

    let meta_addr1 = make_meta_addr_change(addr1).into_new_meta_addr(
        Instant::now(),
        Utc::now().try_into().expect("will succeed until 2038"),
    );
    let meta_addr2 = MetaAddr::new_gossiped_meta_addr(
        addr2,
        PeerServices::NODE_NETWORK,
        DateTime32::MIN.saturating_add(Duration32::from_seconds(1)),
    );

    // The second address should be skipped because the first address has a
    // recent `last_response` time and the two addresses have the same IP.
    let addrs = vec![meta_addr1, meta_addr2];
    let address_book = AddressBook::new_with_addrs(
        "0.0.0.0:0".parse().unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        MAX_ADDRS_IN_ADDRESS_BOOK,
        Span::current(),
        addrs,
    );

    let next_reconnection_peer = address_book
        .reconnection_peers(Instant::now(), Utc::now())
        .next();

    if should_skip_ip {
        assert_eq!(next_reconnection_peer, None,);
    } else {
        assert_ne!(next_reconnection_peer, None,);
    }
}
