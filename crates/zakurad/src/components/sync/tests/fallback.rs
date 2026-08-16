//! Tests for the Zakura body-sync stall watchdog
//! ([`ChainSync::bootstrap_genesis_then_pause`]).
//!
//! These exercise the pure decision function [`zakura_block_sync_stalled`] directly,
//! so they are deterministic and need no clock, services, or live `ChainTip`.

use std::collections::HashMap;

use indexmap::IndexSet;
use zakura_chain::{
    block::{self, Height},
    chain_sync_status::ChainSyncStatus,
};

use super::super::{
    cap_checkpoint_bootstrap_hashes, checkpoint_bootstrap_hash_limit,
    engage_legacy_fallback_alongside_zakura, legacy_probe_supports_fallback,
    zakura_block_sync_stalled, zakura_sync_status_length, zakura_watchdog_action, SyncStatus,
    ZakuraLegacyProbe, ZakuraStallTracker, ZakuraWatchdogAction, ZAKURA_LEGACY_BEHIND_THRESHOLD,
};

#[test]
fn genesis_bootstrap_transfers_apply_ownership_once() {
    let handoff = crate::commands::start::zakura::SyncCoordinator::new_legacy_bootstrap();

    assert!(
        handoff.clone().begin_apply().is_none(),
        "native applies must stay disabled while the genesis fetch owns the verifier"
    );

    handoff
        .finish_legacy_bootstrap()
        .expect("genesis bootstrap transfers ownership exactly once");
    let permit = handoff
        .clone()
        .begin_apply()
        .expect("native applies start after the durable genesis handoff");
    drop(permit);

    handoff
        .finish_legacy_bootstrap()
        .expect_err("a duplicate genesis handoff is an explicit invalid transition");
    assert!(
        handoff.clone().begin_apply().is_some(),
        "repeating the one-way bootstrap signal must leave Zakura as owner"
    );
}

#[test]
fn completed_legacy_fallback_returns_apply_ownership_to_zakura() {
    let bootstrap_handoff = crate::commands::start::zakura::SyncCoordinator::new_legacy_bootstrap();
    futures::executor::block_on(
        bootstrap_handoff.acquire_legacy_fallback(std::time::Duration::from_secs(1)),
    )
    .expect_err("fallback recovery must not bypass initial genesis bootstrap ownership");
    assert!(
        bootstrap_handoff.begin_apply().is_none(),
        "fallback recovery must not bypass initial genesis bootstrap ownership"
    );

    let handoff = crate::commands::start::zakura::SyncCoordinator::new();

    let lease = futures::executor::block_on(engage_legacy_fallback_alongside_zakura(&handoff))
        .expect("a drained native pipeline yields one fallback lease");
    assert!(handoff.is_yielded_to_legacy());
    assert!(handoff.clone().begin_apply().is_none());

    drop(lease);
    assert!(!handoff.is_yielded_to_legacy());
    assert!(
        handoff.begin_apply().is_some(),
        "a drained legacy recovery round must return apply ownership to Zakura"
    );
}

fn height_hash(height: u32) -> block::Hash {
    let mut bytes = [0; 32];
    bytes[..4].copy_from_slice(&height.to_le_bytes());
    block::Hash(bytes)
}

fn height_hashes(start: u32, end: u32) -> IndexSet<block::Hash> {
    (start..=end).map(height_hash).collect()
}

fn pending_heights(start: u32, end: u32) -> HashMap<block::Hash, Option<Height>> {
    (start..=end)
        .map(|height| (height_hash(height), Some(Height(height))))
        .collect()
}

fn unknown_pending(count: u32) -> HashMap<block::Hash, Option<Height>> {
    (0..count)
        .map(|index| {
            let mut bytes = [0; 32];
            bytes[..4].copy_from_slice(&index.to_le_bytes());
            (block::Hash(bytes), None)
        })
        .collect()
}

#[test]
fn legacy_checkpoint_bootstrap_leaves_post_checkpoint_hashes_for_zakura() {
    let checkpoint = Height(200);

    assert_eq!(
        checkpoint_bootstrap_hash_limit(Some(Height(0)), checkpoint, 23),
        177,
        "fresh bootstrap must stop queued compatibility work at checkpoint 200"
    );
    assert_eq!(
        checkpoint_bootstrap_hash_limit(Some(Height(100)), checkpoint, 23),
        77,
        "a resumed checkpoint range must apply the same exact boundary"
    );
    assert_eq!(
        checkpoint_bootstrap_hash_limit(Some(checkpoint), checkpoint, 0),
        0,
        "native Zakura owns every block after the checkpoint"
    );
}

#[test]
fn checkpoint_cap_removes_a_repeated_pending_prefix_before_truncating() {
    let pending = pending_heights(3_201, 3_300);
    let mut advertised = height_hashes(3_201, 3_700);

    assert!(cap_checkpoint_bootstrap_hashes(
        &mut advertised,
        Some(Height(3_200)),
        Height(3_600),
        &pending,
    ));
    assert_eq!(advertised, height_hashes(3_301, 3_600));
}

#[test]
fn checkpoint_cap_stops_a_fresh_response_at_the_boundary() {
    let mut advertised = height_hashes(3_201, 3_700);

    assert!(cap_checkpoint_bootstrap_hashes(
        &mut advertised,
        Some(Height(3_200)),
        Height(3_600),
        &HashMap::new(),
    ));
    assert_eq!(advertised, height_hashes(3_201, 3_600));
}

#[test]
fn checkpoint_cap_accounts_for_a_non_overlapping_pending_prefix() {
    let pending = pending_heights(3_201, 3_300);
    let mut advertised = height_hashes(3_301, 3_800);

    assert!(cap_checkpoint_bootstrap_hashes(
        &mut advertised,
        Some(Height(3_200)),
        Height(3_600),
        &pending,
    ));
    assert_eq!(advertised, height_hashes(3_301, 3_600));
}

#[test]
fn checkpoint_cap_does_not_count_stale_or_above_boundary_tasks() {
    let stale = height_hash(3_200);
    let above_boundary = height_hash(3_700);
    let mut pending = HashMap::from([
        (stale, Some(Height(3_200))),
        (above_boundary, Some(Height(3_700))),
    ]);
    let mut advertised = height_hashes(3_201, 3_700);
    advertised.insert(stale);

    assert!(cap_checkpoint_bootstrap_hashes(
        &mut advertised,
        Some(Height(3_200)),
        Height(3_600),
        &pending,
    ));
    assert_eq!(advertised, height_hashes(3_201, 3_600));

    pending.insert(height_hash(3_201), None);
    let mut unknown_pending = height_hashes(3_201, 3_700);
    assert!(cap_checkpoint_bootstrap_hashes(
        &mut unknown_pending,
        Some(Height(3_200)),
        Height(3_600),
        &pending,
    ));
    assert_eq!(unknown_pending, height_hashes(3_202, 3_600));
}

#[test]
fn unknown_pending_work_cannot_claim_checkpoint_completion() {
    let pending = unknown_pending(400);
    let mut advertised = IndexSet::new();

    assert!(
        !cap_checkpoint_bootstrap_hashes(
            &mut advertised,
            Some(Height(3_200)),
            Height(3_600),
            &pending,
        ),
        "unknown-height budget exhaustion is not evidence that the checkpoint was reached"
    );
    assert!(advertised.is_empty());

    let partial_pending = unknown_pending(399);
    let mut one_new_hash = IndexSet::from([height_hash(9_999)]);
    assert!(!cap_checkpoint_bootstrap_hashes(
        &mut one_new_hash,
        Some(Height(3_200)),
        Height(3_600),
        &partial_pending,
    ));
    assert_eq!(one_new_hash, IndexSet::from([height_hash(9_999)]));

    let known_boundary = HashMap::from([(height_hash(3_600), Some(Height(3_600)))]);
    assert!(cap_checkpoint_bootstrap_hashes(
        &mut IndexSet::new(),
        Some(Height(3_200)),
        Height(3_600),
        &known_boundary,
    ));
}

/// The original height-only rule, reproduced here only to demonstrate the F-88602
/// hole: any increase in the verified tip — including a gossip-trickled block —
/// resets the idle counter, so the watchdog never falls back.
fn legacy_stalled(
    last_height: &mut Option<Height>,
    idle_polls: &mut u64,
    verified_height: Option<Height>,
    max_idle_polls: u64,
) -> bool {
    if verified_height > *last_height {
        *last_height = verified_height;
        *idle_polls = 0;
        false
    } else {
        *idle_polls += 1;
        *idle_polls >= max_idle_polls
    }
}

/// A peer trickling next-height blocks over gossip bumps the verified tip. The
/// watchdog treats any verified-tip advance as progress and does not use the
/// best-header frontier to decide whether Zakura is stalled.
#[test]
fn verified_tip_progress_prevents_fallback() {
    let max_idle_polls = 5;

    let mut verified = 0u32;

    let mut legacy_last = Some(Height(verified));
    let mut legacy_idle = 0u64;
    let mut tracker = ZakuraStallTracker::new(Some(Height(verified)));

    let mut legacy_fell_back = false;
    let mut new_fell_back = false;
    for _ in 0..(max_idle_polls * 4) {
        verified += 1;
        legacy_fell_back |= legacy_stalled(
            &mut legacy_last,
            &mut legacy_idle,
            Some(Height(verified)),
            max_idle_polls,
        );
        new_fell_back |=
            zakura_block_sync_stalled(&mut tracker, Some(Height(verified)), max_idle_polls);
    }

    assert!(
        !legacy_fell_back,
        "the legacy height-only rule never falls back while the verified tip advances"
    );
    assert!(
        !new_fell_back,
        "the watchdog must not fall back while the verified tip advances"
    );
}

/// A working bulk downloader closing a real gap must keep Zakura sync as the primary
/// path and never fall back.
#[test]
fn real_block_sync_progress_keeps_primary_path() {
    let max_idle_polls = 5;
    let mut tracker = ZakuraStallTracker::new(Some(Height(0)));

    let mut verified = 0u32;
    for _ in 0..60 {
        verified = verified.saturating_add(200);
        assert!(
            !zakura_block_sync_stalled(&mut tracker, Some(Height(verified)), max_idle_polls,),
            "healthy bulk sync closing 200 blocks/poll must never fall back"
        );
    }
}

/// A node advancing one verified block at a time is making progress and must
/// not fall back.
#[test]
fn one_block_progress_stays_primary() {
    let max_idle_polls = 3;
    let mut tracker = ZakuraStallTracker::new(Some(Height(100)));

    let mut height = 100u32;
    for _ in 0..20 {
        height += 1;
        assert!(
            !zakura_block_sync_stalled(&mut tracker, Some(Height(height)), max_idle_polls,),
            "a node advancing one verified block at a time must not fall back"
        );
    }
}

/// Steady moderate sync must be credited as progress. This guards against
/// reintroducing a best-header gap rule that can false-positive while verified
/// blocks are advancing.
#[test]
fn steady_moderate_sync_does_not_false_positive() {
    let max_idle_polls = 5;
    let mut tracker = ZakuraStallTracker::new(Some(Height(0)));

    let mut verified = 0u32;
    let mut fell_back = false;
    for _ in 0..400 {
        verified = verified.saturating_add(50);
        fell_back |=
            zakura_block_sync_stalled(&mut tracker, Some(Height(verified)), max_idle_polls);
    }
    assert!(
        !fell_back,
        "sync below the per-poll floor but accumulating closure across polls must not fall back"
    );
}

/// The watchdog uses the original "verified tip moved at all" rule.
#[test]
fn uses_legacy_tip_moved_rule() {
    let max_idle_polls = 3;
    let mut tracker = ZakuraStallTracker::new(Some(Height(0)));

    // Tip advancing: treated as progress.
    for v in 1..=10u32 {
        assert!(!zakura_block_sync_stalled(
            &mut tracker,
            Some(Height(v)),
            max_idle_polls,
        ));
    }

    // Tip frozen: idle accrues and it falls back after the window.
    let frozen = Some(Height(10));
    let mut fell_back = false;
    for _ in 0..max_idle_polls {
        fell_back = zakura_block_sync_stalled(&mut tracker, frozen, max_idle_polls);
    }
    assert!(
        fell_back,
        "with a frozen verified tip, the legacy rule still trips the fallback"
    );
}

/// The fleet-restart blind spot: every Zakura node restarts together and
/// freezes at a common height with `header_tip == verified_tip`, so the node
/// looks caught up. The legacy-informed probe must engage once the verified tip
/// stays frozen while the node looks caught up.
#[test]
fn frozen_with_zero_gap_arms_the_legacy_probe() {
    let min_frozen_polls = 3;
    let frozen = Some(Height(1_000));
    let mut probe = ZakuraLegacyProbe::new(frozen);

    // Frozen tip, looks caught up (zero gap): the probe arms after the window.
    let mut armed = false;
    for poll in 1..=min_frozen_polls {
        let now_armed = probe.should_probe(frozen, true, min_frozen_polls);
        if poll < min_frozen_polls {
            assert!(
                !now_armed,
                "must not probe before the freeze window elapses"
            );
        }
        armed |= now_armed;
    }
    assert!(
        armed,
        "a frozen tip that looks caught up must arm the legacy cross-check probe"
    );
}

/// A node still advancing its verified tip — however slowly — must never arm
/// the legacy probe.
#[test]
fn advancing_tip_never_arms_the_legacy_probe() {
    let min_frozen_polls = 3;
    let mut probe = ZakuraLegacyProbe::new(Some(Height(0)));

    let mut height = 0u32;
    for _ in 0..50 {
        height += 1;
        assert!(
            !probe.should_probe(Some(Height(height)), true, min_frozen_polls),
            "an advancing verified tip must never arm the legacy probe"
        );
    }
}

/// When the header gap is large, `looks_caught_up` is false and the legacy
/// probe must stay off even with a frozen tip.
#[test]
fn frozen_but_materially_behind_leaves_probe_to_gap_rule() {
    let min_frozen_polls = 3;
    let frozen = Some(Height(10));
    let mut probe = ZakuraLegacyProbe::new(frozen);

    for _ in 0..(min_frozen_polls * 4) {
        assert!(
            !probe.should_probe(frozen, false, min_frozen_polls),
            "a large header gap means the legacy probe must stay off"
        );
    }
}

/// The fallback decision fires on a frozen verified tip, and the hand-off keeps
/// the Zakura reactors alive: legacy ChainSync resumes as the body-sync driver
/// while Zakura quiesces into a serving/advertising bridge.
#[test]
fn stalled_zakura_with_legacy_fallback_keeps_zakura_reactors_alive() {
    let max_idle_polls = 3;
    let mut tracker = ZakuraStallTracker::new(Some(Height(0)));
    let mut legacy_probe = ZakuraLegacyProbe::new(Some(Height(0)));

    let mut action = ZakuraWatchdogAction::ContinueWaiting;
    let mut header = 1_000u32;
    for _ in 0..=max_idle_polls {
        header += 1;
        action = zakura_watchdog_action(
            &mut tracker,
            &mut legacy_probe,
            Some(Height(0)),
            Some(Height(header)),
            max_idle_polls,
            true,
        );
    }

    assert_eq!(
        action,
        ZakuraWatchdogAction::FallbackToLegacy,
        "a frozen verified tip must trigger legacy fallback when it is enabled"
    );
    let handoff = crate::commands::start::zakura::SyncCoordinator::new();
    let _lease = futures::executor::block_on(engage_legacy_fallback_alongside_zakura(&handoff))
        .expect("an idle native pipeline yields one fallback lease");
    assert!(
        handoff.is_yielded_to_legacy(),
        "fallback must yield Zakura block sync to legacy sync"
    );
    assert!(
        handoff.begin_apply().is_none(),
        "no new Zakura applies may start after the fallback engages"
    );
}

#[test]
fn stalled_zakura_without_legacy_fallback_keeps_waiting() {
    let max_idle_polls = 3;
    let mut tracker = ZakuraStallTracker::new(Some(Height(0)));
    let mut legacy_probe = ZakuraLegacyProbe::new(Some(Height(0)));

    let mut saw_warn_only = false;
    let mut header = 1_000u32;
    for _ in 0..(max_idle_polls * 2) {
        header += 1;
        let action = zakura_watchdog_action(
            &mut tracker,
            &mut legacy_probe,
            Some(Height(0)),
            Some(Height(header)),
            max_idle_polls,
            false,
        );

        assert_ne!(
            action,
            ZakuraWatchdogAction::FallbackToLegacy,
            "Zakura-only nodes must not fall back to absent legacy peers"
        );
        saw_warn_only |= action == ZakuraWatchdogAction::WarnOnly;
    }

    assert!(
        saw_warn_only,
        "Zakura-only stalls should still produce the warn-only watchdog action"
    );
}

/// A frozen tip that looks caught up cross-checks legacy peers, and a probe at
/// or above the behind threshold engages fallback without cancelling Zakura.
#[test]
fn frozen_zero_gap_with_legacy_peers_ahead_engages_fallback() {
    let max_idle_polls = 5;
    let frozen = Some(Height(1_000));
    let mut tracker = ZakuraStallTracker::new(frozen);
    let mut legacy_probe = ZakuraLegacyProbe::new(frozen);

    let mut action = ZakuraWatchdogAction::ContinueWaiting;
    for _ in 0..3 {
        action = zakura_watchdog_action(
            &mut tracker,
            &mut legacy_probe,
            frozen,
            frozen,
            max_idle_polls,
            true,
        );
    }

    assert_eq!(
        action,
        ZakuraWatchdogAction::ProbeLegacyPeers,
        "a frozen tip that looks caught up must cross-check legacy peers"
    );
    assert!(
        legacy_probe_supports_fallback(Some(ZAKURA_LEGACY_BEHIND_THRESHOLD)),
        "legacy peers at or above the behind threshold must trigger fallback"
    );

    let handoff = crate::commands::start::zakura::SyncCoordinator::new();
    let _lease = futures::executor::block_on(engage_legacy_fallback_alongside_zakura(&handoff))
        .expect("an idle native pipeline yields one fallback lease");
    assert!(
        handoff.is_yielded_to_legacy(),
        "fallback must yield Zakura block sync to legacy sync"
    );
}

#[test]
fn legacy_probe_below_threshold_keeps_zakura_running() {
    assert!(
        !legacy_probe_supports_fallback(None),
        "no legacy peer answer must not force a fallback"
    );
    assert!(
        !legacy_probe_supports_fallback(Some(ZAKURA_LEGACY_BEHIND_THRESHOLD - 1)),
        "legacy peers below the behind threshold must not force a fallback"
    );
}

#[test]
fn zakura_sync_status_length_reports_local_header_gap() {
    assert_eq!(
        zakura_sync_status_length(Some(Height(100)), Some(Height(100))),
        Some(0),
        "a caught-up Zakura body tip should report a close-to-tip sync length"
    );
    assert_eq!(
        zakura_sync_status_length(Some(Height(100)), Some(Height(110))),
        Some(10),
        "a small local header/body gap should preserve the existing close-to-tip heuristic"
    );
    assert_eq!(
        zakura_sync_status_length(Some(Height(110)), Some(Height(100))),
        Some(0),
        "a stale header-tip read should not make a synced body tip look behind"
    );
    assert_eq!(
        zakura_sync_status_length(None, Some(Height(100))),
        None,
        "without a verified body tip, Zakura should not publish a readiness signal"
    );
    assert_eq!(
        zakura_sync_status_length(Some(Height(100)), None),
        None,
        "without a header frontier, Zakura should not publish a readiness signal"
    );
}

#[test]
fn zakura_sync_status_lengths_drive_existing_mempool_gate() {
    let (sync_status, mut recent_syncs) = SyncStatus::new();

    assert!(
        !sync_status.is_close_to_tip(),
        "an empty sync-status history starts with mempool disabled"
    );

    recent_syncs.push_extend_tips_length(
        zakura_sync_status_length(Some(Height(100)), Some(Height(100)))
            .expect("caught-up Zakura tips produce a sync status length"),
    );
    assert!(
        sync_status.is_close_to_tip(),
        "a caught-up Zakura body/header frontier should activate the existing close-to-tip gate"
    );

    let (sync_status, mut recent_syncs) = SyncStatus::new();
    recent_syncs.push_extend_tips_length(
        zakura_sync_status_length(Some(Height(110)), Some(Height(100)))
            .expect("local verified tip ahead of headers produces a sync status length"),
    );
    assert!(
        sync_status.is_close_to_tip(),
        "a locally mined block ahead of the peer header tip should activate the mempool gate"
    );

    let (sync_status, mut recent_syncs) = SyncStatus::new();
    recent_syncs.push_extend_tips_length(
        zakura_sync_status_length(Some(Height(100)), Some(Height(201)))
            .expect("known Zakura tips produce a sync status length"),
    );
    assert!(
        !sync_status.is_close_to_tip(),
        "a Zakura body/header gap over 100 blocks should keep the existing close-to-tip gate disabled"
    );
}

/// Locks in the fallback behavior: engaging legacy fallback must be a commit
/// barrier for Zakura body applies, while leaving the reactors alive as a
/// serving bridge.
#[tokio::test(start_paused = true)]
async fn fallback_handoff_drains_applies_without_cancelling_zakura() {
    let handoff = crate::commands::start::zakura::SyncCoordinator::new();
    let permit = handoff.begin_apply().expect("applies run before fallback");

    let drain_handoff = handoff.clone();
    let drain =
        tokio::spawn(async move { engage_legacy_fallback_alongside_zakura(&drain_handoff).await });

    tokio::task::yield_now().await;
    assert!(
        !drain.is_finished(),
        "the drain waits for in-flight applies"
    );
    assert!(
        handoff.begin_apply().is_none(),
        "no new Zakura applies may start once fallback begins"
    );

    drop(permit);
    let lease = drain
        .await
        .expect("drain task completes")
        .expect("the fallback lease is acquired after the drain");
    assert!(handoff.is_yielded_to_legacy());
    drop(lease);
    assert!(
        handoff.begin_apply().is_some(),
        "dropping the fallback lease restores native ownership"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fallback_drain_does_not_lose_concurrent_last_apply_wakeup() {
    let handoff = crate::commands::start::zakura::SyncCoordinator::new();
    let permit = handoff.begin_apply().expect("applies run before fallback");

    let drain_handoff = handoff.clone();
    let drain = tokio::spawn(async move {
        drain_handoff
            .acquire_legacy_fallback(std::time::Duration::from_secs(30))
            .await
    });

    let dropper = tokio::task::spawn_blocking(move || drop(permit));

    let lease = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        dropper.await.expect("dropper task completes");
        drain
            .await
            .expect("drain task completes")
            .expect("the fallback lease is acquired after the concurrent release")
    })
    .await
    .expect("drain must observe the final apply release without waiting for its timeout");

    assert!(handoff.is_yielded_to_legacy());
    assert!(
        handoff.begin_apply().is_none(),
        "no new Zakura applies may start after the concurrent drain"
    );
    drop(lease);
    assert!(
        handoff.begin_apply().is_some(),
        "dropping the concurrently acquired lease restores native ownership"
    );
}

#[tokio::test(start_paused = true)]
async fn fallback_diagnostic_intervals_never_authorize_legacy_with_a_live_apply() {
    let handoff = crate::commands::start::zakura::SyncCoordinator::new();
    let permit = handoff.begin_apply().expect("native apply starts");
    let drain_handoff = handoff.clone();
    let fallback = tokio::spawn(async move {
        drain_handoff
            .acquire_legacy_fallback(std::time::Duration::from_secs(60))
            .await
    });

    tokio::task::yield_now().await;
    for _ in 0..3 {
        tokio::time::advance(std::time::Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        assert!(
            !fallback.is_finished(),
            "a diagnostic interval must not authorize legacy while a native permit is live"
        );
        assert!(
            handoff.begin_apply().is_none(),
            "fallback drain must continue rejecting new native applies"
        );
    }

    drop(permit);
    let lease = fallback
        .await
        .expect("fallback task stays alive")
        .expect("fallback acquires only after the native apply finishes");
    assert!(handoff.is_yielded_to_legacy());
    drop(lease);
    assert!(handoff.begin_apply().is_some());
}

#[tokio::test]
async fn cancelling_fallback_drain_restores_native_ownership() {
    let handoff = crate::commands::start::zakura::SyncCoordinator::new();
    let permit = handoff.begin_apply().expect("native apply starts");
    let drain_handoff = handoff.clone();
    let fallback = tokio::spawn(async move {
        drain_handoff
            .acquire_legacy_fallback(std::time::Duration::from_secs(60))
            .await
    });

    tokio::task::yield_now().await;
    assert!(handoff.is_yielded_to_legacy());
    fallback.abort();
    fallback
        .await
        .expect_err("the fallback acquisition is explicitly cancelled");

    drop(permit);
    assert!(
        handoff.begin_apply().is_some(),
        "dropping the acquisition future drops its lease and restores native ownership"
    );
}

#[tokio::test]
async fn panic_during_fallback_round_restores_native_ownership() {
    let handoff = crate::commands::start::zakura::SyncCoordinator::new();
    let panic_handoff = handoff.clone();
    let fallback = tokio::spawn(async move {
        let _lease = panic_handoff
            .acquire_legacy_fallback(std::time::Duration::from_secs(60))
            .await
            .expect("idle native pipeline drains immediately");
        panic!("simulated legacy fallback panic");
    });

    fallback
        .await
        .expect_err("the simulated fallback round panics");
    assert!(
        handoff.begin_apply().is_some(),
        "unwinding the fallback future drops its lease and restores native ownership"
    );
}

#[tokio::test]
async fn only_one_fallback_lease_can_own_an_apply_epoch() {
    let handoff = crate::commands::start::zakura::SyncCoordinator::new();
    let lease = handoff
        .acquire_legacy_fallback(std::time::Duration::from_secs(60))
        .await
        .expect("the first fallback acquires the idle pipeline");
    handoff
        .acquire_legacy_fallback(std::time::Duration::from_secs(60))
        .await
        .expect_err("a competing fallback cannot acquire the same apply epoch");

    drop(lease);
    let next_lease = handoff
        .acquire_legacy_fallback(std::time::Duration::from_secs(60))
        .await
        .expect("a later fallback can acquire the restored native pipeline");
    drop(next_lease);
    assert!(handoff.begin_apply().is_some());
}
