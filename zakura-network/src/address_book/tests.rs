//! Tests for the address book.

#![allow(clippy::unwrap_in_result)]

use std::{
    net::{IpAddr, Ipv4Addr},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Wake, Waker},
};

use crate::constants::MAX_BANNED_IPS;

use super::{BanList, BannedIps};

mod prop;
mod vectors;

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

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
fn new_bans_wake_after_becoming_visible() {
    let bans = BannedIps::default();
    let before_registration = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
    let ipv4 = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2));
    let ipv4_mapped = IpAddr::V6(Ipv4Addr::new(192, 0, 2, 2).to_ipv6_mapped());

    assert!(bans.insert(before_registration));
    assert!(bans.contains(before_registration));

    let wake_counter = Arc::new(WakeCounter::default());
    bans.register_waker(&Waker::from(wake_counter.clone()));

    assert!(bans.insert(ipv4));
    assert!(bans.contains(ipv4));
    assert_eq!(wake_counter.0.load(Ordering::SeqCst), 1);

    assert!(!bans.insert(ipv4_mapped));
    assert_eq!(
        wake_counter.0.load(Ordering::SeqCst),
        1,
        "a duplicate canonical IP must not produce another wake"
    );
}
