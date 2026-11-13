//! Tests for the shared cooldown wrapper.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    time::Instant,
};

use super::PeerCooldowns;

const PEER: IpAddr = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7));

#[test]
fn ipv4_mapped_addresses_share_a_cooldown() {
    let start = Instant::now();
    let cooldowns = PeerCooldowns::default();
    let mapped = IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xcb00, 0x7107));

    cooldowns.record_invalid_transaction(mapped, start);

    assert!(cooldowns.is_cooling_down(PEER, start));
}

#[test]
fn clones_share_cooldowns() {
    let start = Instant::now();
    let cooldowns = PeerCooldowns::default();
    let download_task_cooldowns = cooldowns.clone();

    cooldowns.record_invalid_transaction(PEER, start);

    assert!(download_task_cooldowns.is_cooling_down(PEER, start));
}
