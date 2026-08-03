//! Tests for the address book.

#![allow(clippy::unwrap_in_result)]

use std::net::{IpAddr, Ipv4Addr};

use chrono::Utc;
use tracing::Span;
use zakura_chain::parameters::Network::Mainnet;

use crate::{
    constants::{DEFAULT_MAX_CONNS_PER_IP, MAX_BANNED_IPS},
    types::MetaAddr,
};

use super::{AddressBook, BanList, BannedIps};

mod prop;
mod vectors;

#[test]
fn ban_list_evicts_the_oldest_ip_at_capacity() {
    let mut bans = BanList::default();
    let oldest = IpAddr::V4(Ipv4Addr::from(1));

    for ip in 1..=MAX_BANNED_IPS {
        bans.insert(IpAddr::V4(Ipv4Addr::from(u32::try_from(ip).unwrap())));
    }

    let newest = IpAddr::V4(Ipv4Addr::from(u32::try_from(MAX_BANNED_IPS + 1).unwrap()));
    bans.insert(newest);

    assert!(!bans.ips.contains(&oldest));
    assert!(bans.ips.contains(&newest));
    assert_eq!(bans.ips.len(), MAX_BANNED_IPS);
    assert_eq!(bans.insertion_order.len(), MAX_BANNED_IPS);
}

#[test]
fn banned_ips_match_ipv4_and_ipv4_mapped_ipv6() {
    let ipv4 = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let ipv4_mapped = IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped());

    assert!(BannedIps::with_banned_ip(ipv4).contains(ipv4_mapped));
    assert!(BannedIps::with_banned_ip(ipv4_mapped).contains(ipv4));
}

#[test]
fn address_metrics_are_updated_once_per_batch() {
    let mut address_book = AddressBook::new(
        "0.0.0.0:0".parse().unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        Span::none(),
    );
    let mut address_metrics = address_book.address_metrics_watcher();
    let initial_update_count = address_book.address_metrics_update_count;
    let peers = [
        "11.1.1.1:8233".parse().unwrap(),
        "11.1.1.2:8233".parse().unwrap(),
        "11.1.1.3:8233".parse().unwrap(),
        "11.1.1.4:8233".parse().unwrap(),
    ]
    .map(MetaAddr::new_initial_peer);

    address_book.extend(peers);

    assert_eq!(
        address_book.address_metrics_update_count,
        initial_update_count + 1
    );
    assert!(address_metrics.has_changed().unwrap());
    assert_eq!(
        address_metrics.borrow_and_update().num_addresses,
        peers.len()
    );
    assert_eq!(
        address_book.address_metrics(Utc::now()),
        *address_metrics.borrow()
    );
}
