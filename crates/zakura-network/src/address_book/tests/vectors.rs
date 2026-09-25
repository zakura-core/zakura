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
        MIN_PEER_RECONNECTION_DELAY,
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

/// Peers learned from inbound connections are neither dialed nor cached.
///
/// Their port is the peer's ephemeral source port rather than a listener, so
/// dialing them wastes the crawler's connection budget. They are also connected
/// right now, which makes them rank as maximally active — without the filter
/// they crowd dialable peers out of the disk cache and a restarted node comes
/// back with mostly undialable candidates.
#[test]
fn inbound_peers_are_not_reconnection_or_cache_candidates() {
    let inbound_addr = "127.0.0.1:54321".parse().unwrap();
    let outbound_addr = "127.0.0.2:8233".parse().unwrap();

    let instant_now = Instant::now();
    let chrono_now = Utc::now();
    let local_now: DateTime32 = chrono_now.try_into().expect("will succeed until 2038");

    let inbound_peer = MetaAddr::new_connected(inbound_addr, &PeerServices::NODE_NETWORK, true)
        .into_new_meta_addr(instant_now, local_now);
    let outbound_peer = MetaAddr::new_connected(outbound_addr, &PeerServices::NODE_NETWORK, false)
        .into_new_meta_addr(instant_now, local_now);

    let address_book = AddressBook::new_with_addrs(
        "0.0.0.0:0".parse().unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        MAX_ADDRS_IN_ADDRESS_BOOK,
        Span::current(),
        vec![inbound_peer, outbound_peer],
    );

    // Both peers are active for gossip, so activity is not what separates them.
    assert!(inbound_peer.is_active_for_gossip(chrono_now));
    assert!(outbound_peer.is_active_for_gossip(chrono_now));

    // Look at the book after the reconnection delay, so neither peer is held
    // back by `was_recently_updated`.
    let later_instant = instant_now + MIN_PEER_RECONNECTION_DELAY * 2;
    let later_chrono = chrono_now
        + chrono::Duration::from_std(MIN_PEER_RECONNECTION_DELAY * 2)
            .expect("test reconnection delay fits in chrono");

    let reconnection_addrs: Vec<_> = address_book
        .reconnection_peers(later_instant, later_chrono)
        .map(|peer| peer.addr())
        .collect();
    assert_eq!(reconnection_addrs, vec![outbound_addr]);

    let cacheable_addrs: Vec<_> = address_book
        .cacheable(chrono_now)
        .into_iter()
        .map(|peer| peer.addr())
        .collect();
    assert_eq!(cacheable_addrs, vec![outbound_addr]);
}

/// An inbound connection does not stop us dialing the same peer's listener.
///
/// `most_recent_by_ip` spaces out our own outbound connections to one IP, but it does
/// not record which direction the connection came from. An inbound peer refreshes its
/// entry with every message it sends, so it holds its IP's slot for as long as it stays
/// connected — and the address that slot bars us from dialing is a different address on
/// that IP, the peer's listener. Peers that connect to us are the ones most active on
/// the network, so this silently removes the best dial candidates, in proportion to how
/// many inbound connections we accept.
#[test]
fn inbound_connections_do_not_block_dialing_the_same_ip() {
    // The peer's ephemeral source port, and the listener it is really reachable on.
    let inbound_addr = "127.0.0.1:54321".parse().unwrap();
    let listener_addr = "127.0.0.1:8233".parse().unwrap();
    // A peer we dialed ourselves, and another address at that same IP.
    let outbound_addr = "127.0.0.2:8233".parse().unwrap();
    let same_ip_as_outbound = "127.0.0.2:8234".parse().unwrap();

    let instant_now = Instant::now();
    let chrono_now = Utc::now();
    let local_now: DateTime32 = chrono_now.try_into().expect("will succeed until 2038");

    let inbound_peer = MetaAddr::new_connected(inbound_addr, &PeerServices::NODE_NETWORK, true)
        .into_new_meta_addr(instant_now, local_now);
    let outbound_peer = MetaAddr::new_connected(outbound_addr, &PeerServices::NODE_NETWORK, false)
        .into_new_meta_addr(instant_now, local_now);
    let gossiped = |addr| {
        MetaAddr::new_gossiped_meta_addr(addr, PeerServices::NODE_NETWORK, local_now)
            .new_gossiped_change()
            .expect("recently gossiped peer creates an address book change")
            .into_new_meta_addr(instant_now, local_now)
    };

    let address_book = AddressBook::new_with_addrs(
        "0.0.0.0:0".parse().unwrap(),
        &Mainnet,
        // Only a limit of one enables the per-IP cache under test.
        DEFAULT_MAX_CONNS_PER_IP,
        MAX_ADDRS_IN_ADDRESS_BOOK,
        Span::current(),
        vec![
            inbound_peer,
            gossiped(listener_addr),
            outbound_peer,
            gossiped(same_ip_as_outbound),
        ],
    );

    let reconnection_addrs: Vec<_> = address_book
        .reconnection_peers(instant_now, chrono_now)
        .map(|peer| peer.addr())
        .collect();

    // The listener is dialable even though its IP holds a live inbound connection,
    // while the address sharing an IP with a peer we dialed ourselves is still spaced
    // out, which is what this cache is for.
    assert_eq!(reconnection_addrs, vec![listener_addr]);
}

/// Addresses we have never connected to ourselves are not written to the disk cache.
///
/// The cache file stores bare socket addresses, so an entry read back from disk has
/// no record of where it came from: it is re-added as an ordinary initial peer, with
/// no inbound flag. Caching an address on another peer's word therefore launders it,
/// and the ephemeral source ports that inbound peers gossip around the network
/// survive every restart. Requiring a response of our own is the only property that
/// outlives the round trip through the cache file.
#[test]
fn peers_we_have_never_connected_to_are_not_cached() {
    let gossiped_addr = "127.0.0.1:54321".parse().unwrap();
    let connected_addr = "127.0.0.2:8233".parse().unwrap();

    let instant_now = Instant::now();
    let chrono_now = Utc::now();
    let local_now: DateTime32 = chrono_now.try_into().expect("will succeed until 2038");

    let gossiped_peer =
        MetaAddr::new_gossiped_meta_addr(gossiped_addr, PeerServices::NODE_NETWORK, local_now)
            .new_gossiped_change()
            .expect("recently gossiped peer creates an address book change")
            .into_new_meta_addr(instant_now, local_now);
    let connected_peer =
        MetaAddr::new_connected(connected_addr, &PeerServices::NODE_NETWORK, false)
            .into_new_meta_addr(instant_now, local_now);

    let address_book = AddressBook::new_with_addrs(
        "0.0.0.0:0".parse().unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        MAX_ADDRS_IN_ADDRESS_BOOK,
        Span::current(),
        vec![gossiped_peer, connected_peer],
    );

    // The gossiped peer passes every other cache filter: it is not inbound, and its
    // gossiped last seen time makes it active for gossip.
    assert!(!gossiped_peer.is_inbound());
    assert!(gossiped_peer.is_active_for_gossip(chrono_now));
    assert!(!gossiped_peer.has_ever_responded());

    let cacheable_addrs: Vec<_> = address_book
        .cacheable(chrono_now)
        .into_iter()
        .map(|peer| peer.addr())
        .collect();
    assert_eq!(cacheable_addrs, vec![connected_addr]);
}
