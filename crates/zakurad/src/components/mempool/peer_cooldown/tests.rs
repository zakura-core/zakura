//! Tests for per-IP transaction cooldowns.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    time::{Duration, Instant},
};

use super::{PeerCooldowns, BASE_COOLDOWN, MAX_COOLDOWN, MAX_COOLDOWN_PEERS};

const PEER: IpAddr = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7));

#[test]
fn first_invalid_transaction_starts_base_cooldown() {
    let start = Instant::now();
    let cooldowns = PeerCooldowns::default();

    assert!(!cooldowns.is_cooling_down(PEER, start));
    assert_eq!(
        cooldowns.record_invalid_transaction(PEER, start),
        Some(BASE_COOLDOWN)
    );

    assert!(cooldowns.is_cooling_down(PEER, start));
    assert!(cooldowns.is_cooling_down(PEER, start + BASE_COOLDOWN - Duration::from_secs(1)));
    assert!(!cooldowns.is_cooling_down(PEER, start + BASE_COOLDOWN));
}

#[test]
fn failures_during_a_cooldown_do_not_add_strikes() {
    let start = Instant::now();
    let cooldowns = PeerCooldowns::default();

    cooldowns.record_invalid_transaction(PEER, start);

    // Transactions queued before the cooldown started finish verifying later.
    assert_eq!(
        cooldowns.record_invalid_transaction(PEER, start + Duration::from_secs(1)),
        None
    );

    // The next cooldown is only the second strike.
    assert_eq!(
        cooldowns.record_invalid_transaction(PEER, start + BASE_COOLDOWN),
        Some(BASE_COOLDOWN * 2)
    );
}

#[test]
fn repeated_cooldowns_double_up_to_the_maximum() {
    let mut now = Instant::now();
    let cooldowns = PeerCooldowns::default();
    let mut lengths = Vec::new();

    for _ in 0..8 {
        let length = cooldowns
            .record_invalid_transaction(PEER, now)
            .expect("each failure arrives after the previous cooldown ended");
        lengths.push(length);
        now += length;
    }

    let minutes: Vec<u64> = lengths.iter().map(|length| length.as_secs() / 60).collect();
    assert_eq!(minutes, [10, 20, 40, 80, 120, 120, 120, 120]);
    assert_eq!(lengths.last(), Some(&MAX_COOLDOWN));
}

#[test]
fn history_is_forgotten_after_a_quiet_period() {
    let start = Instant::now();
    let cooldowns = PeerCooldowns::default();

    cooldowns.record_invalid_transaction(PEER, start);
    let second = start + BASE_COOLDOWN;
    assert_eq!(
        cooldowns.record_invalid_transaction(PEER, second),
        Some(BASE_COOLDOWN * 2)
    );

    // The second cooldown ends at `second + 2 * BASE_COOLDOWN`. After a further
    // `MAX_COOLDOWN` without failures, the peer starts over.
    let forgotten = second + BASE_COOLDOWN * 2 + MAX_COOLDOWN;
    assert_eq!(
        cooldowns.record_invalid_transaction(PEER, forgotten),
        Some(BASE_COOLDOWN)
    );
}

#[test]
fn history_is_kept_until_the_quiet_period_ends() {
    let start = Instant::now();
    let cooldowns = PeerCooldowns::default();

    cooldowns.record_invalid_transaction(PEER, start);

    let almost_forgotten = start + BASE_COOLDOWN + MAX_COOLDOWN - Duration::from_secs(1);
    assert_eq!(
        cooldowns.record_invalid_transaction(PEER, almost_forgotten),
        Some(BASE_COOLDOWN * 2)
    );
}

#[test]
fn ipv4_mapped_addresses_share_a_cooldown() {
    let start = Instant::now();
    let cooldowns = PeerCooldowns::default();
    let mapped = IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xcb00, 0x7107));

    cooldowns.record_invalid_transaction(mapped, start);

    assert!(cooldowns.is_cooling_down(PEER, start));
}

#[test]
fn a_full_history_drops_forgotten_peers_first() {
    let start = Instant::now();
    let cooldowns = PeerCooldowns::default();

    for index in 0..MAX_COOLDOWN_PEERS {
        let index = u32::try_from(index).expect("the peer bound fits in u32");
        cooldowns
            .record_invalid_transaction(IpAddr::V4(Ipv4Addr::from(0x0a00_0000 + index)), start);
    }
    assert_eq!(cooldowns.len(), MAX_COOLDOWN_PEERS);

    // Evict one forgotten history without scanning or clearing the map.
    let forgotten = start + BASE_COOLDOWN + MAX_COOLDOWN;
    cooldowns.record_invalid_transaction(PEER, forgotten);

    assert_eq!(cooldowns.len(), MAX_COOLDOWN_PEERS);
    assert!(cooldowns.is_cooling_down(PEER, forgotten));
}

#[test]
fn clones_share_cooldowns() {
    let start = Instant::now();
    let cooldowns = PeerCooldowns::default();
    let download_task_cooldowns = cooldowns.clone();

    cooldowns.record_invalid_transaction(PEER, start);

    assert!(download_task_cooldowns.is_cooling_down(PEER, start));
}

#[test]
fn history_stays_bounded() {
    let start = Instant::now();
    let cooldowns = PeerCooldowns::default();

    for index in 0..=MAX_COOLDOWN_PEERS {
        let index = u32::try_from(index).expect("the peer bound fits in u32");
        let ip = IpAddr::V4(Ipv4Addr::from(0x0a00_0000 + index));
        // Later peers start later, so the first peer's cooldown ends earliest.
        cooldowns.record_invalid_transaction(ip, start + Duration::from_millis(index.into()));
    }

    assert_eq!(cooldowns.len(), MAX_COOLDOWN_PEERS);

    let first = IpAddr::V4(Ipv4Addr::from(0x0a00_0000));
    let last = IpAddr::V4(Ipv4Addr::from(
        0x0a00_0000 + u32::try_from(MAX_COOLDOWN_PEERS).expect("the peer bound fits in u32"),
    ));
    assert!(cooldowns.is_cooling_down(first, start));
    assert!(cooldowns.is_cooling_down(last, start + Duration::from_secs(1)));
    assert!(!cooldowns.peers().peers.contains_key(&last));

    // The untracked peer can acquire a slot once the earliest cooldown ends.
    assert!(!cooldowns.is_cooling_down(last, start + BASE_COOLDOWN));
    assert_eq!(
        cooldowns.record_invalid_transaction(last, start + BASE_COOLDOWN),
        Some(BASE_COOLDOWN)
    );
    assert!(!cooldowns.peers().peers.contains_key(&first));
    assert!(cooldowns.peers().peers.contains_key(&last));
    assert_eq!(cooldowns.peers().expirations.len(), MAX_COOLDOWN_PEERS);
}

#[test]
fn extending_a_cooldown_replaces_its_expiration() {
    let start = Instant::now();
    let cooldowns = PeerCooldowns::default();
    cooldowns.record_invalid_transaction(PEER, start);
    cooldowns.record_invalid_transaction(PEER, start + BASE_COOLDOWN);
    let state = cooldowns.peers();
    assert_eq!(state.expirations.len(), 1);
    assert_eq!(
        state.expirations.first(),
        Some(&(start + BASE_COOLDOWN * 3, PEER))
    );
}
