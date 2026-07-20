//! Tests for the address book.

#![allow(clippy::unwrap_in_result)]

use std::net::{IpAddr, Ipv4Addr};

use crate::constants::MAX_BANNED_IPS;

use super::BanList;

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
