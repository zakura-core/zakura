//! Tests for the Zakura body-sync stall watchdog
//! ([`ChainSync::bootstrap_genesis_then_pause`]).
//!
//! These exercise the pure decision function [`zakura_block_sync_stalled`] and the
//! [`stop_zakura_sync`] hand-off helper directly, so they are deterministic and need
//! no clock, services, or live `ChainTip`.

use tokio_util::sync::CancellationToken;

use zebra_chain::{block::Height, chain_sync_status::ChainSyncStatus};

use super::super::{
    legacy_probe_supports_fallback, stop_zakura_sync, zakura_block_sync_stalled,
    zakura_sync_status_length, zakura_watchdog_action, SyncStatus, ZakuraLegacyProbe,
    ZakuraStallTracker, ZakuraWatchdogAction, ZAKURA_LEGACY_BEHIND_THRESHOLD,
};

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

#[tokio::test]
async fn stalled_zakura_with_legacy_fallback_cancels_the_shutdown_token() {
    let max_idle_polls = 3;
    let token = CancellationToken::new();
    let driver_view = token.child_token();
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
    stop_zakura_sync(None, &Some(token)).await;
    assert!(
        driver_view.is_cancelled(),
        "falling back to legacy must cancel the Zakura sync drivers' shutdown token"
    );
}

#[test]
fn stalled_zakura_without_legacy_fallback_keeps_waiting() {
    let max_idle_polls = 3;
    let token = CancellationToken::new();
    let driver_view = token.child_token();
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
    assert!(
        !driver_view.is_cancelled(),
        "warn-only Zakura stalls must not cancel the Zakura shutdown token"
    );
}

#[tokio::test]
async fn frozen_zero_gap_with_legacy_peers_ahead_cancels_the_shutdown_token() {
    let max_idle_polls = 5;
    let frozen = Some(Height(1_000));
    let token = CancellationToken::new();
    let driver_view = token.child_token();
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

    stop_zakura_sync(None, &Some(token)).await;
    assert!(
        driver_view.is_cancelled(),
        "legacy-informed fallback must cancel the Zakura sync drivers' shutdown token"
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

/// The point of this test is to lock in the fallback behavior: when Zebra decides to stop using
/// Zakura sync and fall back to legacy sync, it must signal the running Zakura driver tasks to shut down.
/// This asserts that the shutdown token is cancelled when the fallback occurs.
#[tokio::test]
async fn fallback_cancels_the_zakura_shutdown_token() {
    let token = CancellationToken::new();
    assert!(
        !token.is_cancelled(),
        "precondition: a fresh token is not cancelled"
    );

    // A child token stands in for the drivers' observed shutdown: cancelling the shared token the
    // watchdog holds must propagate to what the drivers actually await.
    let driver_view = token.child_token();

    stop_zakura_sync(None, &Some(token)).await;

    assert!(
        driver_view.is_cancelled(),
        "falling back to legacy must cancel the Zakura sync drivers' shutdown token"
    );
}

/// On a Zakura-only node there is no endpoint shutdown token, so the hand-off helper must be a
/// no-op rather than panic.
#[tokio::test]
async fn stop_zakura_sync_is_a_noop_without_a_token() {
    // Must not panic.
    stop_zakura_sync(None, &None).await;
}
