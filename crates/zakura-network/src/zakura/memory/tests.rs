//! Unit tests and proptests for the memory pool machine (`mem-3`, `mem-4`,
//! `mem-13`, `mem-14`) and serving's second-level [`PeerSubPool`].
//!
//! Every proptest drives the pure machines directly and checks invariants on
//! their own state (decision-15; no twin model restating the pool's rules).

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use proptest::prelude::*;
use zakura_rt::time::Instant;

use super::peer_pool::PeerSubPool;
use super::{
    Consumer, ConsumerSpec, LeaseDenied, MemoryPool, MemoryPoolMachine, OverrunReport, PoolConfig,
    PoolEvent, PoolObserver, Priority, Refusal, WakeClass, OVERRUN_WARN_INTERVAL,
};
use crate::zakura::ZakuraPeerId;

const KIB: u64 = 1024;
const MIB: u64 = 1024 * 1024;

#[derive(Clone, Debug)]
enum Op {
    Lease {
        consumer_idx: usize,
        bytes: u64,
    },
    Release {
        slot: usize,
        fraction: u8,
    },
    Resize {
        slot: usize,
        new_bytes: u64,
    },
    SetCap {
        consumer_idx: usize,
        cap: u64,
    },
    AddWaiter {
        consumer_idx: usize,
        below: bool,
        contended: bool,
    },
    RemoveWaiter {
        slot: usize,
    },
    Wake {
        released_by: Option<usize>,
        freed_shared: bool,
    },
    TakeTurn {
        consumer_idx: usize,
    },
}

fn arb_consumer_idx() -> impl Strategy<Value = usize> {
    0..Consumer::COUNT
}

fn arb_bulk_idx() -> impl Strategy<Value = usize> {
    Consumer::BULK[0].index()..=Consumer::BULK[3].index()
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (arb_consumer_idx(), 0u64..=(256 * KIB)).prop_map(|(consumer_idx, bytes)| Op::Lease { consumer_idx, bytes }),
        4 => (0usize..32, 0u8..=4).prop_map(|(slot, fraction)| Op::Release { slot, fraction }),
        2 => (0usize..32, 0u64..=(256 * KIB)).prop_map(|(slot, new_bytes)| Op::Resize { slot, new_bytes }),
        1 => (arb_consumer_idx(), 0u64..=(512 * KIB)).prop_map(|(consumer_idx, cap)| Op::SetCap { consumer_idx, cap }),
        2 => (arb_bulk_idx(), any::<bool>(), any::<bool>()).prop_map(|(consumer_idx, below, contended)| Op::AddWaiter { consumer_idx, below, contended }),
        2 => (0usize..32).prop_map(|slot| Op::RemoveWaiter { slot }),
        1 => (prop::option::of(arb_consumer_idx()), any::<bool>()).prop_map(|(released_by, freed_shared)| Op::Wake { released_by, freed_shared }),
        1 => arb_consumer_idx().prop_map(|consumer_idx| Op::TakeTurn { consumer_idx }),
    ]
}

fn arb_fitting_pool_config() -> impl Strategy<Value = PoolConfig> {
    (
        0u64..=(64 * KIB),                         // fixed
        prop::array::uniform3(0u64..=(32 * KIB)),  // reserved floors
        prop::array::uniform4(0u64..=(32 * KIB)),  // bulk floors
        prop::array::uniform4(0u64..=(256 * KIB)), // bulk cap extras
        prop::array::uniform4(1u64..=8),           // bulk weights
        0u64..=(512 * KIB),                        // shared S
    )
        .prop_map(
            |(fixed, reserved_floors, bulk_floors, bulk_cap_extras, bulk_weights, shared_s)| {
                let mut config = PoolConfig::new(0, fixed);
                for (i, &c) in Consumer::RESERVED.iter().enumerate() {
                    config = config.with_consumer(c, ConsumerSpec::reserved(reserved_floors[i]));
                }
                for (i, &c) in Consumer::BULK.iter().enumerate() {
                    let floor = bulk_floors[i];
                    let cap = floor + bulk_cap_extras[i];
                    config =
                        config.with_consumer(c, ConsumerSpec::bulk(floor, cap, bulk_weights[i]));
                }
                config.budget_b = fixed + config.total_floors() + shared_s;
                config
            },
        )
}

/// One waiter the proptest registered on the machine.
type Waiter = (Consumer, WakeClass, bool);

/// Check core structural invariants on `machine` after any step.
fn assert_machine_invariants(
    machine: &MemoryPoolMachine,
    active: &[(Consumer, u64)],
    waiters: &[Waiter],
) {
    // mem-3: sum of live leases <= S + floors, and shared_used <= S.
    assert!(
        machine.shared_used() <= machine.shared_capacity(),
        "shared_used {} > shared_capacity {}",
        machine.shared_used(),
        machine.shared_capacity()
    );
    assert!(
        machine.total_leased() <= machine.lease_limit(),
        "total_leased {} > lease_limit {}",
        machine.total_leased(),
        machine.lease_limit()
    );
    assert!(
        machine.fixed_bytes() + machine.total_leased() <= machine.budget_b(),
        "fitting config overran budget_b"
    );

    // Per-consumer sums and shared accounting match active leases exactly.
    let mut sum_by_consumer = [0u64; Consumer::COUNT];
    for &(consumer, bytes) in active {
        sum_by_consumer[consumer.index()] += bytes;
    }
    let mut expected_total = 0u64;
    let mut expected_shared = 0u64;
    for consumer in Consumer::ALL {
        let leased = machine.leased(consumer);
        assert_eq!(leased, sum_by_consumer[consumer.index()]);
        expected_total += leased;
        expected_shared += leased.saturating_sub(machine.floor(consumer));
        if consumer.priority() == Priority::Reserved {
            assert_eq!(machine.floor(consumer), machine.cap(consumer));
            assert!(leased <= machine.cap(consumer));
        }
        // fix-8: a cap stays between the floor and the plan's cap.
        assert!(machine.floor(consumer) <= machine.cap(consumer));
        assert!(machine.cap(consumer) <= machine.planned_cap(consumer));

        // Waiter counts and the waiting flag follow registered waiters only.
        for class in WakeClass::ALL {
            let registered = waiters
                .iter()
                .filter(|&&(c, k, _)| c == consumer && k == class)
                .count();
            assert_eq!(machine.waiters(consumer, class) as usize, registered);
        }
        let contended = waiters
            .iter()
            .any(|&(c, _, contended)| c == consumer && contended);
        assert_eq!(machine.is_waiting(consumer), contended);
    }
    assert_eq!(machine.total_leased(), expected_total);
    assert_eq!(machine.shared_used(), expected_shared);

    // mem-13: Sessions never exceeds its protected share, even with no waiters.
    assert!(
        machine.leased(Consumer::Sessions) <= machine.protected_share(Consumer::Sessions),
        "sessions leased {} > protected share {}",
        machine.leased(Consumer::Sessions),
        machine.protected_share(Consumer::Sessions)
    );
}

/// A wake only names queues that hold waiters.
fn assert_wake_targets_waiters(machine: &MemoryPoolMachine, wake: super::Wake) {
    for consumer in Consumer::ALL {
        for class in WakeClass::ALL {
            if wake.wakes_waiter(consumer, class) {
                assert!(
                    machine.waiters(consumer, class) > 0,
                    "woke an empty {class:?} queue of {consumer:?}"
                );
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// mem-3 (`pool-bounded`), mem-4 (`pool-shares`) and mem-13
    /// (`sessions-within-share`), driven through leases, caps, waiters and
    /// wakeups. At every step: total leases <= S + floors, Sessions stays
    /// within its protected share, only registered waiters mark waiting, and
    /// dropping all leases returns every counter to zero.
    #[test]
    fn mem3_and_mem13_pool_bounded_and_sessions_within_share(
        config in arb_fitting_pool_config(),
        ops in prop::collection::vec(arb_op(), 1..=80),
    ) {
        let mut machine = MemoryPoolMachine::new(config);
        let mut active: Vec<(Consumer, u64)> = Vec::new();
        let mut waiters: Vec<Waiter> = Vec::new();
        let now = Instant::now();

        for op in ops {
            match op {
                Op::Lease { consumer_idx, bytes } => {
                    let consumer = Consumer::ALL[consumer_idx];
                    let before_leased = machine.leased(consumer);
                    let other_below_waiting = machine.any_below_share_waiting_except(Some(consumer));
                    let was_below_share = machine.is_below_share(consumer);
                    let was_waiting = machine.is_waiting(consumer);
                    let cap = machine.cap(consumer);
                    let floor = machine.floor(consumer);

                    match machine.try_lease(consumer, bytes) {
                        Ok(()) => {
                            if bytes > 0 {
                                active.push((consumer, bytes));
                                // Admitted lease stayed within cap.
                                prop_assert!(before_leased + bytes <= cap);
                                // mem-4: while another consumer below its share
                                // waits, only a consumer below its share gets a
                                // lease.
                                if other_below_waiting {
                                    prop_assert!(was_below_share || before_leased + bytes <= floor);
                                }
                            }
                        }
                        Err(refusal) => {
                            // mem-4: a consumer staying within its floor is never refused.
                            prop_assert!(
                                before_leased.saturating_add(bytes) > floor,
                                "consumer {consumer:?} refused within floor ({before_leased} + {bytes} <= {floor})"
                            );
                            // fix-3: only pool contention is contention.
                            if let Refusal::Contended(reason) = refusal {
                                prop_assert!(matches!(
                                    reason,
                                    LeaseDenied::PoolFull | LeaseDenied::BelowShareWaiting
                                ));
                                // mem-5: reserved consumers never wait behind bulk.
                                prop_assert_eq!(consumer.priority(), Priority::Bulk);
                            }
                            // Sessions never waits for its own long-lived leases.
                            if consumer == Consumer::Sessions {
                                prop_assert!(!matches!(refusal, Refusal::OwnLimit(_)));
                            }
                        }
                    }
                    // fix-2: deciding a lease never marks waiting.
                    prop_assert_eq!(machine.is_waiting(consumer), was_waiting);
                }
                Op::Release { slot, fraction } => {
                    if !active.is_empty() {
                        let idx = slot % active.len();
                        let (consumer, held) = active[idx];
                        let release_bytes = if fraction == 0 {
                            held
                        } else {
                            (held * u64::from(fraction) / 4).max(1).min(held)
                        };
                        let released = machine.release(consumer, release_bytes);
                        prop_assert_eq!(released.bytes, release_bytes);
                        if release_bytes == held {
                            active.swap_remove(idx);
                        } else {
                            active[idx].1 -= release_bytes;
                        }
                        let wake = machine.wake(Some(consumer), released.freed_shared > 0, false);
                        assert_wake_targets_waiters(&machine, wake);
                    }
                }
                Op::Resize { slot, new_bytes } => {
                    if !active.is_empty() {
                        let idx = slot % active.len();
                        let (consumer, held) = active[idx];
                        let resized = if new_bytes < held {
                            machine.release(consumer, held - new_bytes);
                            true
                        } else {
                            machine.try_lease(consumer, new_bytes - held).is_ok()
                        };
                        if resized {
                            if new_bytes == 0 {
                                active.swap_remove(idx);
                            } else {
                                active[idx].1 = new_bytes;
                            }
                        }
                    }
                }
                Op::SetCap { consumer_idx, cap } => {
                    let consumer = Consumer::ALL[consumer_idx];
                    machine.set_cap(consumer, cap);
                }
                Op::AddWaiter { consumer_idx, below, contended } => {
                    let consumer = Consumer::ALL[consumer_idx];
                    let class = WakeClass::of(below);
                    machine.add_waiter(consumer, class, contended);
                    waiters.push((consumer, class, contended));
                }
                Op::RemoveWaiter { slot } => {
                    if !waiters.is_empty() {
                        let (consumer, class, contended) = waiters.swap_remove(slot % waiters.len());
                        let rule3_before = machine.any_below_share_waiting_except(None);
                        machine.remove_waiter(consumer, class, contended);
                        let wake = machine.wake(None, false, rule3_before);
                        assert_wake_targets_waiters(&machine, wake);
                    }
                }
                Op::Wake { released_by, freed_shared } => {
                    let released_by = released_by.map(|idx| Consumer::ALL[idx]);
                    let rule3 = machine.any_below_share_waiting_except(None);
                    let wake = machine.wake(released_by, freed_shared, rule3);
                    assert_wake_targets_waiters(&machine, wake);
                    // A consumer above its share never wakes while one below
                    // its share waits, unless its own release woke it.
                    if rule3 {
                        for consumer in Consumer::BULK {
                            if Some(consumer) != released_by && !machine.is_below_share(consumer) {
                                prop_assert!(!wake.wakes_waiter(consumer, WakeClass::Below));
                                prop_assert!(!wake.wakes_waiter(consumer, WakeClass::Above));
                            }
                        }
                    }
                }
                Op::TakeTurn { consumer_idx } => {
                    let wake = machine.take_turn(Consumer::ALL[consumer_idx]);
                    assert_wake_targets_waiters(&machine, wake);
                }
            }

            assert_machine_invariants(&machine, &active, &waiters);
            // mem-14: a fitting default pool never triggers an overrun warning.
            prop_assert_eq!(machine.check_overrun(now), None);
        }

        // Release every live lease and waiter: every counter returns to zero.
        for (consumer, bytes) in active.drain(..) {
            machine.release(consumer, bytes);
        }
        for (consumer, class, contended) in waiters.drain(..) {
            machine.remove_waiter(consumer, class, contended);
        }
        assert_machine_invariants(&machine, &active, &waiters);
        prop_assert_eq!(machine.total_leased(), 0);
        prop_assert_eq!(machine.shared_used(), 0);
    }

    /// mem-4 (`pool-shares`):
    /// - While a consumer below its share waits, no consumer above its share
    ///   gets a new lease.
    /// - With no waiter, any borrowable consumer may borrow up to its cap.
    /// - A consumer at its floor always gets a lease.
    #[test]
    fn mem4_pool_shares_waiting_borrowing_and_floors(
        floor_a in 1u64..=(32 * KIB),
        floor_b in 1u64..=(32 * KIB),
        weight_a in 1u64..=4,
        weight_b in 1u64..=4,
        shared_s in (64 * KIB)..=(512 * KIB),
    ) {
        let cap_all = floor_a + floor_b + shared_s;
        let config = PoolConfig::new(floor_a + floor_b + shared_s + 32 * KIB, 0)
            .with_consumer(Consumer::Consensus, ConsumerSpec::reserved(32 * KIB))
            .with_consumer(Consumer::BlockSync, ConsumerSpec::bulk(floor_a, cap_all, weight_a))
            .with_consumer(Consumer::Serving, ConsumerSpec::bulk(floor_b, cap_all, weight_b));

        let mut machine = MemoryPoolMachine::new(config);
        let share_a = machine.share_of_s(Consumer::BlockSync);
        let share_b = machine.share_of_s(Consumer::Serving);

        // 1. With no waiter, BlockSync borrows all of S (plus its floor) up to cap.
        prop_assert_eq!(
            machine.try_lease(Consumer::BlockSync, floor_a + shared_s),
            Ok(())
        );
        prop_assert_eq!(machine.shared_free(), 0);

        // 2. Even with S completely full, Consensus (reserved) and Serving (floor)
        //    always get their full reserved/floor bytes.
        prop_assert_eq!(machine.try_lease(Consumer::Consensus, 32 * KIB), Ok(()));
        prop_assert_eq!(machine.try_lease(Consumer::Serving, floor_b), Ok(()));

        // 3. Serving (below its share, since share_b > 0) requests 1 byte above
        //    its floor: pool contention, which marks nothing until Serving
        //    waits.
        if share_b > 0 {
            prop_assert_eq!(
                machine.try_lease(Consumer::Serving, 1),
                Err(Refusal::Contended(LeaseDenied::PoolFull))
            );
            prop_assert!(!machine.is_waiting(Consumer::Serving));
            machine.add_waiter(Consumer::Serving, WakeClass::Below, true);
            prop_assert!(machine.is_waiting(Consumer::Serving));
            prop_assert!(machine.is_below_share(Consumer::Serving));

            // BlockSync releases 1 byte above its share, leaving S free, and
            // immediately tries to re-lease 1 byte while still above its share.
            // Rule 3 must refuse it because Serving (below share) is waiting.
            if shared_s > share_a {
                machine.release(Consumer::BlockSync, 1);
                prop_assert_eq!(machine.shared_free(), 1);
                prop_assert_eq!(
                    machine.try_lease(Consumer::BlockSync, 1),
                    Err(Refusal::Contended(LeaseDenied::BelowShareWaiting))
                );
                // Serving wakes and takes that 1 byte, then stops waiting.
                prop_assert_eq!(machine.try_lease(Consumer::Serving, 1), Ok(()));
                machine.remove_waiter(Consumer::Serving, WakeClass::Below, true);
                prop_assert!(!machine.is_waiting(Consumer::Serving));
            }
        }
    }

    /// Serving's second-level `PeerSubPool`: per-peer sums stay incremental,
    /// idle floors are lent (fix-6), a waiting peer below its part holds back
    /// peers at or above theirs (`#1888`), and teardown leaves nothing.
    #[test]
    fn peer_sub_pool_fairness_and_teardown(
        floor in 1u64..=(16 * KIB),
        ceiling_extra in (32 * KIB)..=(256 * KIB),
        ops in prop::collection::vec((0usize..4, 0u64..=(64 * KIB), 0u8..4), 1..=60),
    ) {
        let peers: Vec<ZakuraPeerId> = (1u8..=4).map(test_peer).collect();
        let ceiling = floor * 4 + ceiling_extra;
        let mut sub = PeerSubPool::new();
        for peer in &peers {
            sub.register(peer, floor, ceiling);
        }
        let mut waiting = [0u32; 4];

        for (peer_idx, bytes, action) in ops {
            let peer = &peers[peer_idx];
            match action {
                0 => {
                    let leased = sub.peer_leased(peer);
                    let was_below = sub.is_below_share(peer, ceiling);
                    let other_waiting_below = peers.iter().enumerate().any(|(i, other)| {
                        i != peer_idx && waiting[i] > 0 && sub.is_below_share(other, ceiling)
                    });
                    match sub.check(peer, bytes, ceiling) {
                        Ok(()) => {
                            if bytes > 0 {
                                sub.charge(peer, bytes);
                            }
                            prop_assert!(leased + bytes <= ceiling);
                            if bytes > 0 && other_waiting_below {
                                prop_assert!(was_below);
                            }
                        }
                        Err(refusal) => {
                            prop_assert!(leased + bytes > floor);
                            prop_assert_ne!(refusal.reason(), LeaseDenied::PoolFull);
                        }
                    }
                }
                1 => {
                    let held = sub.peer_leased(peer);
                    sub.release(peer, bytes.min(held));
                }
                2 => {
                    sub.add_waiter(peer);
                    waiting[peer_idx] += 1;
                }
                _ => {
                    if waiting[peer_idx] > 0 {
                        sub.remove_waiter(peer);
                        waiting[peer_idx] -= 1;
                    }
                }
            }

            let leased: Vec<u64> = peers.iter().map(|peer| sub.peer_leased(peer)).collect();
            // fix-6: only the floor bytes peers use leave the shared part.
            let floor_usage: u64 = leased.iter().map(|&bytes| bytes.min(floor)).sum();
            prop_assert_eq!(sub.share_of_s(ceiling), (ceiling - floor_usage) / 4);
        }

        // Releasing, unwaiting and unregistering every peer empties the sub-pool.
        for (idx, peer) in peers.iter().enumerate() {
            sub.release(peer, sub.peer_leased(peer));
            for _ in 0..waiting[idx] {
                sub.remove_waiter(peer);
            }
            sub.unregister(peer);
        }
        prop_assert_eq!(sub, PeerSubPool::new());
    }
}

fn test_peer(byte: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![byte; 32]).expect("32 bytes is a valid peer id")
}

/// mem-13: Sessions never borrows above its share even when S is wide open and
/// no other consumer is waiting, yet always gets its floor even when S is full.
#[test]
fn mem13_sessions_never_borrows_and_keeps_floor() {
    let config = PoolConfig::new(100 * MIB, 0)
        .with_consumer(
            Consumer::BlockSync,
            ConsumerSpec::bulk(4 * MIB, 100 * MIB, 1),
        )
        .with_consumer(
            Consumer::Sessions,
            ConsumerSpec::bulk(16 * MIB, 100 * MIB, 1),
        );
    // S = 100 - 4 - 16 = 80 MiB; equal weights -> 40 MiB share each.
    // Sessions protected share = 16 + 40 = 56 MiB.
    let mut machine = MemoryPoolMachine::new(config);
    assert_eq!(machine.shared_capacity(), 80 * MIB);
    assert_eq!(machine.protected_share(Consumer::Sessions), 56 * MIB);

    // Even with zero other consumers active, Sessions cannot take 56 MiB + 1,
    // and waiting can't help.
    assert_eq!(
        machine.try_lease(Consumer::Sessions, 56 * MIB + 1),
        Err(Refusal::Final(LeaseDenied::ShareExceeded))
    );
    assert!(!machine.is_waiting(Consumer::Sessions));

    // Sessions can take up to its full protected share (56 MiB), and then
    // never waits for its own long-lived leases.
    assert_eq!(machine.try_lease(Consumer::Sessions, 56 * MIB), Ok(()));
    assert_eq!(
        machine.try_lease(Consumer::Sessions, 1),
        Err(Refusal::Final(LeaseDenied::ShareExceeded))
    );
    machine.release(Consumer::Sessions, 56 * MIB);

    // BlockSync borrows the entire 80 MiB shared pool.
    assert_eq!(machine.try_lease(Consumer::BlockSync, 84 * MIB), Ok(()));
    assert_eq!(machine.shared_free(), 0);

    // Sessions still gets its full 16 MiB floor immediately.
    assert_eq!(machine.try_lease(Consumer::Sessions, 16 * MIB), Ok(()));

    // Growing Sessions into its share while BlockSync holds S is contention.
    // Once Sessions waits, BlockSync can't borrow freed bytes.
    assert_eq!(
        machine.try_lease(Consumer::Sessions, MIB),
        Err(Refusal::Contended(LeaseDenied::PoolFull))
    );
    machine.add_waiter(Consumer::Sessions, WakeClass::Below, true);
    machine.release(Consumer::BlockSync, MIB);
    assert_eq!(
        machine.try_lease(Consumer::BlockSync, MIB),
        Err(Refusal::Contended(LeaseDenied::BelowShareWaiting))
    );
    assert_eq!(machine.try_lease(Consumer::Sessions, MIB), Ok(()));
}

/// fix-1: two consumers below their shares never refuse each other, so both
/// get free bytes while both wait.
#[test]
fn below_share_waiters_never_block_each_other() {
    let config = PoolConfig::new(30 * MIB, 0)
        .with_consumer(Consumer::BlockSync, ConsumerSpec::bulk(MIB, 30 * MIB, 1))
        .with_consumer(Consumer::Serving, ConsumerSpec::bulk(MIB, 30 * MIB, 1))
        .with_consumer(Consumer::Tx, ConsumerSpec::bulk(MIB, 30 * MIB, 1));
    // S = 27 MiB -> share 9 MiB each -> protected 10 MiB each.
    let mut machine = MemoryPoolMachine::new(config);
    machine.add_waiter(Consumer::BlockSync, WakeClass::Below, true);
    machine.add_waiter(Consumer::Serving, WakeClass::Below, true);

    assert_eq!(machine.try_lease(Consumer::BlockSync, 4 * MIB), Ok(()));
    assert_eq!(machine.try_lease(Consumer::Serving, 4 * MIB), Ok(()));
    // Tx fills its share, and then is held back for them.
    assert_eq!(machine.try_lease(Consumer::Tx, 10 * MIB), Ok(()));
    assert_eq!(
        machine.try_lease(Consumer::Tx, MIB),
        Err(Refusal::Contended(LeaseDenied::BelowShareWaiting))
    );
}

/// fix-3: a consumer at its own cap waits for its own releases, not on
/// contention, and fix-8: `set_cap` never raises a cap past the plan's.
#[test]
fn own_cap_is_not_contention_and_caps_stay_within_the_plan() {
    let config = PoolConfig::new(30 * MIB, 0)
        .with_consumer(Consumer::BlockSync, ConsumerSpec::bulk(MIB, 8 * MIB, 1))
        .with_consumer(Consumer::Serving, ConsumerSpec::bulk(MIB, 30 * MIB, 1));
    let mut machine = MemoryPoolMachine::new(config);

    assert_eq!(machine.try_lease(Consumer::BlockSync, 8 * MIB), Ok(()));
    assert_eq!(
        machine.try_lease(Consumer::BlockSync, MIB),
        Err(Refusal::OwnLimit(LeaseDenied::CapExceeded))
    );
    assert_eq!(
        machine.try_lease(Consumer::BlockSync, 9 * MIB),
        Err(Refusal::Final(LeaseDenied::CapExceeded))
    );

    machine.set_cap(Consumer::BlockSync, 64 * MIB);
    assert_eq!(machine.cap(Consumer::BlockSync), 8 * MIB);
    machine.set_cap(Consumer::BlockSync, 0);
    assert_eq!(machine.cap(Consumer::BlockSync), MIB);
    machine.set_cap(Consumer::BlockSync, 4 * MIB);
    assert_eq!(machine.cap(Consumer::BlockSync), 4 * MIB);
}

/// fix-7: a release that returns no shared bytes wakes only its own consumer,
/// one waiter at a time, and each woken waiter passes the next turn on until
/// every waiter tried once.
#[test]
fn wake_rounds_visit_each_waiter_once() {
    let config = PoolConfig::new(30 * MIB, 0)
        .with_consumer(
            Consumer::BlockSync,
            ConsumerSpec::bulk(4 * MIB, 30 * MIB, 1),
        )
        .with_consumer(Consumer::Serving, ConsumerSpec::bulk(MIB, 30 * MIB, 1));
    let mut machine = MemoryPoolMachine::new(config);
    machine.add_waiter(Consumer::BlockSync, WakeClass::Below, false);
    machine.add_waiter(Consumer::BlockSync, WakeClass::Above, false);
    machine.add_waiter(Consumer::Serving, WakeClass::Below, true);

    assert_eq!(machine.try_lease(Consumer::BlockSync, 2 * MIB), Ok(()));
    let released = machine.release(Consumer::BlockSync, MIB);
    assert_eq!(released.freed_shared, 0);

    let wake = machine.wake(Some(Consumer::BlockSync), false, false);
    assert!(wake.wakes_waiter(Consumer::BlockSync, WakeClass::Below));
    assert!(!wake.wakes_waiter(Consumer::BlockSync, WakeClass::Above));
    assert!(!wake.wakes_waiter(Consumer::Serving, WakeClass::Below));

    let next = machine.take_turn(Consumer::BlockSync);
    assert!(next.wakes_waiter(Consumer::BlockSync, WakeClass::Above));
    assert!(machine.take_turn(Consumer::BlockSync).is_empty());
}

/// Stalled serving reader is blocked from exceeding its fair part once another
/// peer below its part waits (`#1888`), while two peers below their parts never
/// block each other (fix-1).
#[test]
fn peer_sub_pool_caps_stalled_reader_when_peer_waits() {
    let mut sub = PeerSubPool::new();
    let (p1, p2, p3) = (test_peer(1), test_peer(2), test_peer(3));
    sub.register(&p1, 2 * MIB, 30 * MIB);
    sub.register(&p2, 2 * MIB, 30 * MIB);
    sub.register(&p3, 2 * MIB, 30 * MIB);

    // fix-6: idle peers' floors are lent, so each part is 30 / 3 = 10 MiB.
    assert_eq!(sub.share_of_s(30 * MIB), 10 * MIB);

    // Peer 1 (soon stalled) borrows 18 MiB while the others are idle.
    assert_eq!(sub.check(&p1, 18 * MIB, 30 * MIB), Ok(()));
    sub.charge(&p1, 18 * MIB);
    // Parts above used floors: (30 - 2) / 3.
    assert_eq!(sub.share_of_s(30 * MIB), 28 * MIB / 3);

    // Peers 2 and 3 wait below their parts; neither blocks the other.
    sub.add_waiter(&p2);
    sub.add_waiter(&p3);
    assert_eq!(sub.check(&p2, MIB, 30 * MIB), Ok(()));
    assert_eq!(sub.check(&p3, MIB, 30 * MIB), Ok(()));

    // Peer 1, above its part, is held back for them.
    assert_eq!(
        sub.check(&p1, MIB, 30 * MIB),
        Err(Refusal::Contended(LeaseDenied::BelowShareWaiting))
    );
    sub.remove_waiter(&p2);
    sub.remove_waiter(&p3);
    assert_eq!(sub.check(&p1, MIB, 30 * MIB), Ok(()));
}

/// mem-14 (`overrides-warn`): mocked-clock check for the 10-minute rate limit.
#[test]
fn mem14_overrun_warning_rate_limited_with_mocked_clock() {
    // Default fitting config never overruns.
    let fitting = PoolConfig::new(16 * MIB, 4 * MIB)
        .with_consumer(Consumer::Sessions, ConsumerSpec::bulk(4 * MIB, 8 * MIB, 1));
    let mut ok_machine = MemoryPoolMachine::new(fitting);
    let t0 = Instant::now();
    assert_eq!(ok_machine.try_lease(Consumer::Sessions, 8 * MIB), Ok(()));
    assert_eq!(ok_machine.check_overrun(t0), None);

    // Operator override pushes fixed + sessions floor past budget_b (10 MiB).
    let overridden = PoolConfig::new(10 * MIB, 4 * MIB)
        .with_consumer(Consumer::Sessions, ConsumerSpec::bulk(8 * MIB, 8 * MIB, 1))
        .with_consumer(Consumer::BlockSync, ConsumerSpec::bulk(MIB, 10 * MIB, 1));
    let mut machine = MemoryPoolMachine::new(overridden);
    // Shared pool saturates at 0 so short-lived leases cannot borrow past B.
    assert_eq!(machine.shared_capacity(), 0);
    // Below B initially (4 + 6 = 10 MiB): no warning.
    assert_eq!(machine.try_lease(Consumer::Sessions, 6 * MIB), Ok(()));
    assert_eq!(machine.check_overrun(t0), None);

    // Taking the remaining 2 MiB of overridden floor crosses B -> immediate report.
    assert_eq!(machine.try_lease(Consumer::Sessions, 2 * MIB), Ok(()));
    assert_eq!(
        machine.check_overrun(t0),
        Some(OverrunReport {
            live_bytes: 12 * MIB,
            budget_bytes: 10 * MIB,
            excess_bytes: 2 * MIB,
        })
    );

    // Suppressed throughout the 10-minute window.
    assert_eq!(machine.check_overrun(t0 + Duration::from_secs(1)), None);
    assert_eq!(
        machine.check_overrun(t0 + OVERRUN_WARN_INTERVAL - Duration::from_secs(1)),
        None
    );

    // Fires again at exactly +10 minutes.
    assert_eq!(
        machine.check_overrun(t0 + OVERRUN_WARN_INTERVAL),
        Some(OverrunReport {
            live_bytes: 12 * MIB,
            budget_bytes: 10 * MIB,
            excess_bytes: 2 * MIB,
        })
    );

    // Drops below B: no warning.
    machine.release(Consumer::Sessions, 2 * MIB);
    assert_eq!(machine.check_overrun(t0 + OVERRUN_WARN_INTERVAL * 2), None);
}

#[derive(Debug, Default)]
struct RecordingObserver {
    events: Mutex<Vec<PoolEvent>>,
}

impl RecordingObserver {
    fn events(&self) -> Vec<PoolEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl PoolObserver for RecordingObserver {
    fn observe(&self, event: PoolEvent) {
        self.events.lock().unwrap().push(event);
    }
}

/// Replay `events` as a gauge would: every active lease ID's bytes match pool
/// totals and end at zero after teardown.
fn replay_gauge(events: &[PoolEvent]) -> HashMap<u64, (Consumer, u64)> {
    let mut live: HashMap<u64, (Consumer, u64)> = HashMap::new();
    for &event in events {
        match event {
            PoolEvent::Granted {
                lease_id,
                consumer,
                bytes,
                consumer_leased,
                consumer_cap,
                total_leased,
                pool_limit,
                ..
            } => {
                assert!(consumer_leased <= consumer_cap);
                assert!(total_leased <= pool_limit);
                assert!(live.insert(lease_id, (consumer, bytes)).is_none());
            }
            PoolEvent::Resized {
                lease_id,
                consumer,
                old_bytes,
                new_bytes,
                total_leased,
                pool_limit,
                ..
            } => {
                assert!(total_leased <= pool_limit);
                let entry = live.get_mut(&lease_id).expect("resized lease exists");
                assert_eq!(entry.0, consumer);
                assert_eq!(entry.1, old_bytes);
                entry.1 = new_bytes;
            }
            PoolEvent::Split {
                from_lease_id,
                to_lease_id,
                consumer,
                bytes,
                ..
            } => {
                let entry = live.get_mut(&from_lease_id).expect("split source exists");
                assert_eq!(entry.0, consumer);
                entry.1 -= bytes;
                if entry.1 == 0 {
                    live.remove(&from_lease_id);
                }
                assert!(live.insert(to_lease_id, (consumer, bytes)).is_none());
            }
            PoolEvent::Merged {
                into_lease_id,
                from_lease_id,
                consumer,
                bytes,
                ..
            } => {
                let (from_c, from_b) = live.remove(&from_lease_id).expect("merge source exists");
                assert_eq!(from_c, consumer);
                assert_eq!(from_b, bytes);
                let into = live.get_mut(&into_lease_id).expect("merge target exists");
                assert_eq!(into.0, consumer);
                into.1 += bytes;
            }
            PoolEvent::Moved {
                from_lease_id,
                to_lease_id,
                from,
                to,
                bytes,
                total_leased,
                pool_limit,
                ..
            } => {
                assert!(total_leased <= pool_limit);
                let entry = live.get_mut(&from_lease_id).expect("move source exists");
                assert_eq!(entry.0, from);
                entry.1 -= bytes;
                if entry.1 == 0 {
                    live.remove(&from_lease_id);
                }
                assert!(live.insert(to_lease_id, (to, bytes)).is_none());
            }
            PoolEvent::Released {
                lease_id,
                consumer,
                bytes,
                total_leased,
                pool_limit,
                ..
            } => {
                assert!(total_leased <= pool_limit);
                let (c, b) = live.remove(&lease_id).expect("released lease exists");
                assert_eq!(c, consumer);
                assert_eq!(b, bytes);
            }
            PoolEvent::Labeled { .. } | PoolEvent::Waited { .. } => {}
        }
    }
    live
}

#[test]
fn pool_lease_raii_shrink_grow_split_merge_and_observer_replay() {
    let config = PoolConfig::new(16 * MIB, 0).with_consumer(
        Consumer::BlockSync,
        ConsumerSpec::bulk(2 * MIB, 16 * MIB, 1),
    );
    let observer = Arc::new(RecordingObserver::default());
    let pool = MemoryPool::new(config).with_observer(Some(observer.clone()));

    let mut lease = pool
        .try_acquire(Consumer::BlockSync, Some(7), 8 * MIB)
        .expect("acquire");
    assert_eq!(lease.bytes(), 8 * MIB);
    assert_eq!(lease.peer(), Some(7));
    assert_eq!(pool.snapshot().total_leased, 8 * MIB);

    // Shrink.
    lease.shrink_to(5 * MIB);
    assert_eq!(lease.bytes(), 5 * MIB);
    assert_eq!(pool.snapshot().total_leased, 5 * MIB);

    // Grow.
    lease.try_grow_to(7 * MIB).expect("grow");
    assert_eq!(lease.bytes(), 7 * MIB);
    assert_eq!(pool.snapshot().total_leased, 7 * MIB);

    // Split + merge preserve exact accounting.
    let child = lease.split(3 * MIB).expect("split");
    assert_eq!(lease.bytes(), 4 * MIB);
    assert_eq!(child.bytes(), 3 * MIB);
    assert_eq!(pool.snapshot().total_leased, 7 * MIB);

    lease.merge(child);
    assert_eq!(lease.bytes(), 7 * MIB);
    assert_eq!(pool.snapshot().total_leased, 7 * MIB);

    // Split everything off leaves parent empty (no-op drop).
    let moved = lease.split(7 * MIB).expect("full split");
    assert_eq!(lease.bytes(), 0);
    drop(lease);
    assert_eq!(pool.snapshot().total_leased, 7 * MIB);

    drop(moved);
    assert_eq!(pool.snapshot().total_leased, 0);

    let live = replay_gauge(&observer.events());
    assert!(live.is_empty(), "every lease returned to zero: {live:?}");
}

#[tokio::test]
async fn async_acquire_wakes_below_share_first_and_unblocks_borrowers_on_cancel() {
    let config = PoolConfig::new(10 * MIB, 0)
        .with_consumer(Consumer::BlockSync, ConsumerSpec::bulk(MIB, 10 * MIB, 1))
        .with_consumer(Consumer::Serving, ConsumerSpec::bulk(MIB, 10 * MIB, 1));
    // S = 8 MiB -> share = 4 MiB each -> protected = 5 MiB each.
    let pool = MemoryPool::new(config);

    // BlockSync borrows the full 9 MiB (1 floor + 8 shared).
    let mut holder = pool
        .try_acquire(Consumer::BlockSync, None, 9 * MIB)
        .expect("borrow all of S");

    // Serving (below share: 0 < 5 MiB) waits for 2 MiB.
    let pool_s = pool.clone();
    let serving_waiter = tokio::spawn(async move {
        pool_s
            .acquire(Consumer::Serving, Some(2), 2 * MIB)
            .await
            .expect("serving acquires")
    });

    // BlockSync (above share: 9 >= 5 MiB) also waits for 1 MiB.
    let pool_b = pool.clone();
    let blocksync_waiter = tokio::spawn(async move {
        pool_b
            .acquire(Consumer::BlockSync, Some(1), MIB)
            .await
            .expect("blocksync acquires after serving")
    });

    // Give both spawned tasks a chance to register their WaitGuards.
    for _ in 0..20 {
        let snap = pool.snapshot();
        if snap.consumers[Consumer::Serving.index()].waiting
            && snap.consumers[Consumer::BlockSync.index()].waiting
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(pool.snapshot().consumers[Consumer::Serving.index()].waiting);
    assert!(pool.snapshot().consumers[Consumer::BlockSync.index()].waiting);

    // Release 2 MiB from BlockSync -> Serving (below share) wakes and takes it;
    // BlockSync waiter (above share) must remain pending until more frees up.
    holder.shrink_by(2 * MIB);
    let serving_lease = tokio::time::timeout(Duration::from_secs(2), serving_waiter)
        .await
        .expect("serving woke promptly")
        .expect("serving task succeeded");
    assert_eq!(serving_lease.bytes(), 2 * MIB);

    // Now Serving is no longer waiting, so releasing 1 more MiB unblocks BlockSync!
    holder.shrink_by(MIB);
    let blocksync_lease = tokio::time::timeout(Duration::from_secs(2), blocksync_waiter)
        .await
        .expect("blocksync woke once below-share waiter finished")
        .expect("blocksync task succeeded");
    assert_eq!(blocksync_lease.bytes(), MIB);

    drop(serving_lease);
    drop(blocksync_lease);
    drop(holder);
    assert_eq!(pool.snapshot().total_leased, 0);

    // Also verify cancelling a below-share waiter unblocks an above-share waiter.
    let mut holder2 = pool
        .try_acquire(Consumer::BlockSync, None, 8 * MIB)
        .expect("borrow 8 MiB");
    let pool_s2 = pool.clone();
    let cancelled_serving = tokio::spawn(async move {
        let _ = pool_s2.acquire(Consumer::Serving, None, 4 * MIB).await;
    });
    for _ in 0..20 {
        if pool.snapshot().consumers[Consumer::Serving.index()].waiting {
            break;
        }
        tokio::task::yield_now().await;
    }
    let pool_b2 = pool.clone();
    let borrower = tokio::spawn(async move {
        pool_b2
            .acquire(Consumer::BlockSync, None, MIB)
            .await
            .expect("borrower wakes after cancellation")
    });
    for _ in 0..20 {
        if pool.snapshot().consumers[Consumer::BlockSync.index()].waiting {
            break;
        }
        tokio::task::yield_now().await;
    }

    // Cancel the below-share waiter -> WaitGuard drop wakes the borrower!
    cancelled_serving.abort();
    let _ = cancelled_serving.await;
    let borrowed = tokio::time::timeout(Duration::from_secs(2), borrower)
        .await
        .expect("borrower unblocked when below-share waiter dropped")
        .expect("borrower task succeeded");
    assert_eq!(borrowed.bytes(), MIB);
    holder2.shrink_to(0);
    drop(holder2);
    drop(borrowed);
    assert_eq!(pool.snapshot().total_leased, 0);
}

#[tokio::test]
async fn session_admission_refuses_atomically_past_share_without_borrowing() {
    // mem-13: Sessions never borrows idle S past its protected share, and
    // refuses atomically rather than admitting a partial reactor set. Each
    // connection costs 2 MiB: its transport base plus its reactor bases.
    let cost = 2 * MIB;

    // Budget = 10 MiB, fixed = 0, Sessions floor = 2 MiB, BlockSync floor = 2 MiB -> S = 6 MiB.
    // Equal weights (1, 1) -> each share = 3 MiB -> Sessions protected_share = 5 MiB.
    // Two full connections (4 MiB) fit; a third (6 MiB > 5 MiB) MUST be refused
    // immediately with ShareExceeded even though 6 MiB of S is completely idle!
    let config = PoolConfig::new(10 * MIB, 0)
        .with_consumer(Consumer::Sessions, ConsumerSpec::bulk(2 * MIB, 10 * MIB, 1))
        .with_consumer(
            Consumer::BlockSync,
            ConsumerSpec::bulk(2 * MIB, 10 * MIB, 1),
        );
    let observer = Arc::new(RecordingObserver::default());
    let pool = MemoryPool::new(config).with_observer(Some(observer.clone()));
    let shutdown = zakura_rt::sync::CancellationToken::new();

    let lease_1 = pool
        .acquire_session_lease(1, cost, &shutdown, Duration::from_secs(1))
        .await
        .expect("first connection fits under Sessions share");
    let lease_2 = pool
        .acquire_session_lease(2, cost, &shutdown, Duration::from_secs(1))
        .await
        .expect("second connection fits under Sessions share");
    assert_eq!(lease_1.bytes(), 2 * MIB);
    assert_eq!(lease_2.bytes(), 2 * MIB);
    assert_eq!(
        pool.snapshot().consumers[Consumer::Sessions.index()].leased,
        4 * MIB
    );

    // Third connection needs 2 MiB (total 6 MiB > protected_share 5 MiB): must
    // fail immediately with ShareExceeded, leaving zero partial leases and no
    // lingering waiter state.
    let err = pool
        .acquire_session_lease(3, cost, &shutdown, Duration::from_secs(5))
        .await
        .expect_err("third connection must not borrow past Sessions protected share");
    assert_eq!(err, LeaseDenied::ShareExceeded);
    let snap = pool.snapshot();
    assert_eq!(snap.consumers[Consumer::Sessions.index()].leased, 4 * MIB);
    assert!(!snap.consumers[Consumer::Sessions.index()].waiting);

    // Disconnecting both peers returns every lease to zero.
    drop(lease_1);
    drop(lease_2);
    assert_eq!(pool.snapshot().total_leased, 0);
    let live = replay_gauge(&observer.events());
    assert!(
        live.is_empty(),
        "dropping session leases returns every lease to zero: {live:?}"
    );
}

#[tokio::test]
async fn session_admission_reclaims_borrowed_shared_memory_and_cleans_up_on_timeout() {
    // mem-13: when bulk temporarily borrowed S, Sessions below its protected
    // share registers a below-share waiter, reclaims S as soon as bulk shrinks,
    // and takes the connection's whole cost at once.
    let total = 2 * MIB + 256 * KIB;

    let config = PoolConfig::new(10 * MIB, 0)
        .with_consumer(Consumer::Sessions, ConsumerSpec::bulk(2 * MIB, 10 * MIB, 1))
        .with_consumer(
            Consumer::BlockSync,
            ConsumerSpec::bulk(2 * MIB, 10 * MIB, 1),
        );
    let pool = MemoryPool::new(config);
    let shutdown = zakura_rt::sync::CancellationToken::new();

    // BlockSync borrows the entire 6 MiB shared pool (total 8 MiB).
    let mut bulk = pool
        .try_acquire(Consumer::BlockSync, Some(99), 8 * MIB)
        .expect("BlockSync borrows full S");

    // Sessions still has its 2 MiB floor guaranteed, so a 2 MiB connection
    // could use floor, while `total` (2.25 MiB) needs 256 KiB from S and must
    // wait for BlockSync to yield borrowed shared memory.
    let pool_for_task = pool.clone();
    let shutdown_for_task = shutdown.clone();
    let admit = tokio::spawn(async move {
        pool_for_task
            .acquire_session_lease(7, total, &shutdown_for_task, Duration::from_secs(2))
            .await
    });

    for _ in 0..20 {
        if pool.snapshot().consumers[Consumer::Sessions.index()].waiting {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        pool.snapshot().consumers[Consumer::Sessions.index()].waiting,
        "Sessions registers a below-share waiter while bulk holds borrowed S"
    );

    // BlockSync shrinks by 1 MiB -> Sessions wakes and takes the grant.
    bulk.shrink_by(MIB);
    let lease = tokio::time::timeout(Duration::from_secs(2), admit)
        .await
        .expect("Sessions woke promptly")
        .expect("task succeeded")
        .expect("Sessions reclaimed its share from bulk");
    assert_eq!(lease.bytes(), total);
    assert_eq!(
        pool.snapshot().consumers[Consumer::Sessions.index()].leased,
        total
    );
    assert!(!pool.snapshot().consumers[Consumer::Sessions.index()].waiting);

    // Now test timeout cleanup when bulk refuses to yield S: must return
    // PoolFull without wedging `waiting` so future borrowers aren't starved.
    let mut bulk2 = pool
        .try_acquire(Consumer::BlockSync, Some(98), 768 * KIB)
        .expect("BlockSync fills remaining S");
    let err = pool
        .acquire_session_lease(8, total, &shutdown, Duration::from_millis(25))
        .await
        .expect_err("timed-out session admission refuses neutrally");
    assert_eq!(err, LeaseDenied::PoolFull);
    assert!(
        !pool.snapshot().consumers[Consumer::Sessions.index()].waiting,
        "waiter guard must clear Sessions waiting state on timeout"
    );

    bulk2.shrink_to(0);
    drop(bulk2);
    drop(bulk);
    drop(lease);
    assert_eq!(pool.snapshot().total_leased, 0);
}

/// fix-2: a refused plain try marks nothing waiting but arms one notifier
/// broadcast for pollers; a wait token marks waiting until it drops.
#[tokio::test]
async fn plain_try_marks_nothing_and_wait_token_marks_waiting_until_dropped() {
    let config = PoolConfig::new(10 * MIB, 0)
        .with_consumer(Consumer::BlockSync, ConsumerSpec::bulk(MIB, 10 * MIB, 1))
        .with_consumer(Consumer::Serving, ConsumerSpec::bulk(MIB, 10 * MIB, 1));
    // S = 8 MiB -> share = 4 MiB each -> protected = 5 MiB each.
    let pool = MemoryPool::new(config);
    let waiting = |consumer: Consumer| pool.snapshot().consumers[consumer.index()].waiting;
    let mut holder = pool
        .try_acquire(Consumer::Serving, None, 9 * MIB)
        .expect("Serving borrows all of S");

    assert_eq!(
        pool.try_acquire(Consumer::BlockSync, None, 2 * MIB)
            .unwrap_err(),
        LeaseDenied::PoolFull
    );
    assert!(!waiting(Consumer::BlockSync));
    // Serving, above its share, still borrows freed bytes.
    holder.shrink_by(MIB);
    let extra = pool
        .try_acquire(Consumer::Serving, None, MIB)
        .expect("nobody waits, so Serving borrows");
    drop(extra);

    // The refusal armed a poller wakeup for the release above.
    let token = pool.wait_token(Consumer::BlockSync);
    assert!(waiting(Consumer::BlockSync));
    assert_eq!(
        pool.try_acquire(Consumer::Serving, None, MIB).unwrap_err(),
        LeaseDenied::BelowShareWaiting
    );

    let notified = pool.notifier(Consumer::BlockSync).notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    holder.shrink_by(MIB);
    tokio::time::timeout(Duration::from_secs(2), notified)
        .await
        .expect("a release that frees S wakes a token holder's notifier");

    drop(token);
    assert!(!waiting(Consumer::BlockSync));
    let extra = pool
        .try_acquire(Consumer::Serving, None, MIB)
        .expect("the dropped token no longer holds Serving back");
    drop(extra);
    drop(holder);
}

/// fix-3: a caller blocked by its own consumer's cap waits without marking the
/// consumer waiting or reporting a wait, and its own release wakes it.
#[tokio::test]
async fn own_cap_wait_marks_nothing_and_wakes_on_own_release() {
    let config = PoolConfig::new(10 * MIB, 0)
        .with_consumer(Consumer::BlockSync, ConsumerSpec::bulk(MIB, 4 * MIB, 1))
        .with_consumer(Consumer::Serving, ConsumerSpec::bulk(MIB, 10 * MIB, 1));
    let observer = Arc::new(RecordingObserver::default());
    let pool = MemoryPool::new(config).with_observer(Some(observer.clone()));
    let mut holder = pool
        .try_acquire(Consumer::BlockSync, None, 4 * MIB)
        .expect("BlockSync fills its cap");

    let pool_w = pool.clone();
    let waiter = tokio::spawn(async move { pool_w.acquire(Consumer::BlockSync, None, MIB).await });
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(!waiter.is_finished());
    assert!(!pool.snapshot().consumers[Consumer::BlockSync.index()].waiting);

    // Serving, below its share, isn't held back by BlockSync's cap wait.
    let serving = pool
        .try_acquire(Consumer::Serving, None, 3 * MIB)
        .expect("Serving borrows");

    holder.shrink_by(MIB);
    let lease = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("BlockSync's own release wakes its waiter")
        .expect("the waiter task doesn't panic")
        .expect("the waiter gets its lease");
    assert_eq!(lease.bytes(), MIB);
    assert!(!observer
        .events()
        .iter()
        .any(|event| matches!(event, PoolEvent::Waited { .. })));
    drop((lease, serving, holder));
    assert!(replay_gauge(&observer.events()).is_empty());
}

/// fix-7: wakeups go one waiter at a time, and a woken waiter that still
/// doesn't fit passes its turn on, so a smaller request behind it gets bytes.
#[tokio::test]
async fn woken_waiter_that_does_not_fit_passes_its_turn_on() {
    let config = PoolConfig::new(10 * MIB, 0)
        .with_consumer(Consumer::BlockSync, ConsumerSpec::bulk(MIB, 10 * MIB, 1))
        .with_consumer(Consumer::Serving, ConsumerSpec::bulk(MIB, 10 * MIB, 1));
    let pool = MemoryPool::new(config);
    let mut holder = pool
        .try_acquire(Consumer::Serving, None, 9 * MIB)
        .expect("Serving borrows all of S");

    let pool_big = pool.clone();
    let big =
        tokio::spawn(async move { pool_big.acquire(Consumer::BlockSync, None, 5 * MIB).await });
    while !pool.snapshot().consumers[Consumer::BlockSync.index()].waiting {
        tokio::task::yield_now().await;
    }
    let pool_small = pool.clone();
    let small =
        tokio::spawn(async move { pool_small.acquire(Consumer::BlockSync, None, 2 * MIB).await });
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    // One shared byte block frees: only the 2 MiB request fits.
    holder.shrink_by(MIB);
    let lease = tokio::time::timeout(Duration::from_secs(2), small)
        .await
        .expect("the turn reaches the smaller waiter")
        .expect("the waiter task doesn't panic")
        .expect("the smaller waiter gets its lease");
    assert_eq!(lease.bytes(), 2 * MIB);
    assert!(!big.is_finished());
    big.abort();
    let _ = big.await;
    assert!(!pool.snapshot().consumers[Consumer::BlockSync.index()].waiting);
}

/// `mem-15`'s labels: a split or moved lease keeps its height, a regrant
/// from zero reports it again, and a move that leaves bytes behind clears
/// the source's height.
#[test]
fn lease_heights_follow_splits_moves_and_regrants() {
    // S is zero, so block sync can take only its 1 MiB floor.
    let config = PoolConfig::new(3 * MIB, 0)
        .with_consumer(Consumer::Consensus, ConsumerSpec::reserved(2 * MIB))
        .with_consumer(Consumer::BlockSync, ConsumerSpec::bulk(MIB, MIB, 1));
    let observer = Arc::new(RecordingObserver::default());
    let pool = MemoryPool::new(config).with_observer(Some(observer.clone()));
    let mut lease = pool
        .try_acquire(Consumer::Consensus, Some(1), 2 * MIB)
        .expect("the reserve fits");
    lease.label_height(7);
    let mut drain = lease.split(MIB + MIB / 2).expect("the lease holds it");
    assert_eq!(drain.height(), Some(7));
    let moved = drain.move_to(Consumer::BlockSync);
    assert_eq!((moved.bytes(), moved.height()), (MIB, Some(7)));
    assert_eq!((drain.bytes(), drain.height()), (MIB / 2, None));
    let labels: Vec<_> = observer
        .events()
        .into_iter()
        .filter_map(|event| match event {
            PoolEvent::Labeled { lease_id, height } => Some((lease_id, height)),
            _ => None,
        })
        .collect();
    assert_eq!(
        labels,
        [(lease.lease_id(), Some(7)), (drain.lease_id(), None)]
    );

    lease.shrink_to(0);
    lease.try_grow_to(MIB / 4).expect("the reserve has room");
    assert_eq!(
        observer.events().last(),
        Some(&PoolEvent::Labeled {
            lease_id: lease.lease_id(),
            height: Some(7),
        })
    );
}
