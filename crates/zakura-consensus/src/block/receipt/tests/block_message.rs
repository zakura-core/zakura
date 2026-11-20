//! How a legacy `block` message claims receipt order from the moment it started arriving.

use super::*;
use zakura_node_services::block_receipt::BlockReservation;

/// The source and order a body of `hash` at `height` takes when it arrives at `now`.
fn used(
    claims: &AnnouncementClaims,
    hash: block::Hash,
    height: block::Height,
    now: Instant,
) -> Option<(ClaimSource, u64)> {
    match claims.resolve(hash, Some(height), now) {
        ClaimOutcome::Used { source, order, .. } => Some((source, order)),
        _ => None,
    }
}

/// Records that a `block` message for `hash` at [`CLAIMED`], which reserved `reserved` at
/// `started_at` with the verified tip at [`TIP`], completed at `now`.
fn receive(
    claims: &AnnouncementClaims,
    hash: block::Hash,
    reserved: u64,
    started_at: Instant,
    now: Instant,
) {
    assert!(claims.note_block_message_at(
        hash,
        Some(CLAIMED),
        BlockReservation {
            order: reserved,
            verified_tip: TIP,
            floor: TIP,
            started_at
        },
        now
    ));
}

#[test]
fn a_block_message_started_before_a_rival_inv_outranks_it_though_it_completes_later() {
    let claims = AnnouncementClaims::default();
    let started_at = Instant::now();
    let reserved = order::next();

    // The rival is announced and its body arrives while the pushed block is still arriving.
    let rival_at = started_at + Duration::from_millis(300);
    announce(&claims, hash(2), rival_at);
    let rival = body_order(&claims, hash(2), rival_at);

    let completed_at = started_at + Duration::from_millis(800);
    receive(&claims, hash(1), reserved, started_at, completed_at);
    assert_eq!(
        used(&claims, hash(1), CLAIMED, completed_at),
        Some((ClaimSource::BlockMessage, reserved)),
    );
    assert!(reserved < rival);
}

#[test]
fn a_block_message_deadline_counts_from_its_start() {
    let claims = AnnouncementClaims::default();
    let started_at = Instant::now();
    let reserved = order::next();
    let completed_at = started_at + Duration::from_millis(1_500);
    receive(&claims, hash(1), reserved, started_at, completed_at);

    let at_the_bound = started_at + CLAIM_BODY_DEADLINE;
    assert!(matches!(
        claims.resolve(hash(1), Some(CLAIMED), at_the_bound),
        ClaimOutcome::Used { source: ClaimSource::BlockMessage, order, age }
            if order == reserved && age == CLAIM_BODY_DEADLINE
    ));

    let late = at_the_bound + Duration::from_millis(1);
    assert!(matches!(
        claims.resolve(hash(1), Some(CLAIMED), late),
        ClaimOutcome::Expired { source: ClaimSource::BlockMessage, age }
            if age == late - started_at
    ));
}

#[test]
fn a_block_keeps_the_earlier_of_its_inv_and_its_block_message() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    let later = now + Duration::from_millis(500);

    // The inv arrives before the message starts, so the inv keeps its order and time.
    announce(&claims, hash(1), now);
    let inv = claims
        .announced_order(hash(1))
        .expect("the inv is recorded");
    let reserved = order::next();
    receive(&claims, hash(1), reserved, later, later);
    assert_eq!(claims.announced_order(hash(1)), Some(inv));
    assert!(matches!(
        claims.resolve(hash(1), Some(CLAIMED), later),
        ClaimOutcome::Used { source: ClaimSource::Inv, order, age }
            if order == inv && age == later - now
    ));

    // The message starts before the inv, which arrives while the message is still arriving.
    let reserved = order::next();
    announce(&claims, hash(2), later);
    let inv = claims
        .announced_order(hash(2))
        .expect("the inv is recorded");
    let completed_at = later + Duration::from_millis(100);
    receive(&claims, hash(2), reserved, now, completed_at);
    assert!(reserved < inv);
    assert!(matches!(
        claims.resolve(hash(2), Some(CLAIMED), completed_at),
        ClaimOutcome::Used { source: ClaimSource::BlockMessage, order, age }
            if order == reserved && age == completed_at - now
    ));

    // A later delivery of the same block does not move it back.
    let again = order::next();
    receive(&claims, hash(2), again, completed_at, completed_at);
    announce(&claims, hash(2), completed_at);
    assert_eq!(claims.announced_order(hash(2)), Some(reserved));
}

#[test]
fn a_block_keeps_the_earlier_of_its_header_claim_and_its_block_message() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();

    // The header is claimed first, so it keeps the block's place.
    claim(&claims, hash(1), now);
    let header = claims
        .claimed_order(hash(1))
        .expect("the header is claimed");
    receive(&claims, hash(1), order::next(), now, now);
    assert_eq!(
        used(&claims, hash(1), CLAIMED, now),
        Some((ClaimSource::Header, header))
    );

    // The message started before the header was claimed.
    let reserved = order::next();
    claim(&claims, hash(2), now);
    receive(&claims, hash(2), reserved, now, now);
    assert_eq!(
        used(&claims, hash(2), CLAIMED, now),
        Some((ClaimSource::BlockMessage, reserved))
    );
}

#[test]
fn a_block_message_outlives_an_earlier_header_claim_that_is_not_applied() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    let pending = claims.begin_at([(hash(1), CLAIMED)], TIP, now);
    let reserved = order::next();
    receive(&claims, hash(1), reserved, now, now);
    assert!(claims
        .claimed_order(hash(1))
        .is_some_and(|header| header < reserved));

    drop(pending);
    assert_eq!(
        used(&claims, hash(1), CLAIMED, now),
        Some((ClaimSource::BlockMessage, reserved))
    );
}

#[test]
fn a_rival_committed_while_a_block_arrives_does_not_move_it_out_of_the_window() {
    let claims = AnnouncementClaims::default();
    let started_at = Instant::now();
    let reserved = order::next();

    // The rival at the same height commits and raises the tip before the block completes.
    let rival_at = started_at + Duration::from_millis(300);
    claims.note_announcement_at(hash(2), CLAIMED, rival_at);

    let completed_at = started_at + Duration::from_millis(800);
    receive(&claims, hash(1), reserved, started_at, completed_at);
    assert_eq!(
        used(&claims, hash(1), CLAIMED, completed_at),
        Some((ClaimSource::BlockMessage, reserved))
    );
}

#[test]
fn only_block_messages_just_above_the_tip_at_their_start_are_recorded() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    let note = |index, height: Option<block::Height>| {
        claims.note_block_message_at(hash(index), height, claims.reserve_block_message(TIP), now)
    };

    for (index, height) in [None, Some(99), Some(100), Some(103), Some(5_000)]
        .into_iter()
        .enumerate()
    {
        let height = height.map(block::Height);
        assert!(
            !note(index, height),
            "a body at {height:?} is not racing at the tip"
        );
        assert_eq!(claims.announced_order(hash(index)), None);
    }
    for (index, height) in [(10, 101), (11, 102)] {
        assert!(note(index, Some(block::Height(height))));
        assert!(used(&claims, hash(index), block::Height(height), now).is_some());
    }

    // Once the tip passes height 101, a tie there can no longer be decided.
    claims.note_announcement_at(hash(20), block::Height(102), now);
    assert_eq!(used(&claims, hash(10), CLAIMED, now), None);
}

#[test]
fn a_mined_block_outranks_an_earlier_block_message_at_its_height() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    let reserved = order::next();
    receive(&claims, hash(1), reserved, now, now);

    let mined = claims.mark_mined(hash(3), CLAIMED, order::next());
    assert!(matches!(
        claims.resolve(hash(1), Some(CLAIMED), now),
        ClaimOutcome::Voided {
            source: ClaimSource::BlockMessage,
            ..
        }
    ));

    drop(mined);
    assert_eq!(
        used(&claims, hash(1), CLAIMED, now),
        Some((ClaimSource::BlockMessage, reserved))
    );
}

#[test]
fn a_reservation_that_is_never_noted_has_no_effect() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    let unused = order::next();

    announce(&claims, hash(1), now);
    let inv = claims
        .announced_order(hash(1))
        .expect("the inv is recorded");
    assert!(unused < inv, "orders stay increasing across the gap");
    assert_eq!(
        used(&claims, hash(1), CLAIMED, now),
        Some((ClaimSource::Inv, inv))
    );
    assert_eq!(
        claims.resolve(hash(2), Some(CLAIMED), now),
        ClaimOutcome::Unclaimed
    );
}

#[test]
fn a_full_store_refuses_a_new_block_message() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    for index in 0..MAX_ANNOUNCED_BLOCKS {
        announce(&claims, hash(index), now);
    }
    receive(&claims, hash(MAX_ANNOUNCED_BLOCKS), order::next(), now, now);
    assert_eq!(claims.announced_order(hash(MAX_ANNOUNCED_BLOCKS)), None);
}

#[test]
fn a_block_message_taking_over_a_later_inv_keeps_the_tip_at_its_start() {
    let claims = AnnouncementClaims::default();
    let started_at = Instant::now();
    let reserved = order::next();

    // A rival at the same height commits, then another peer announces this block at the new tip.
    let inv_at = started_at + Duration::from_millis(500);
    claims.note_announcement_at(hash(1), CLAIMED, inv_at);

    let completed_at = started_at + Duration::from_millis(800);
    receive(&claims, hash(1), reserved, started_at, completed_at);
    assert_eq!(
        used(&claims, hash(1), CLAIMED, completed_at),
        Some((ClaimSource::BlockMessage, reserved))
    );
}

#[test]
fn an_expired_reservation_does_not_displace_a_usable_one() {
    let claims = AnnouncementClaims::default();
    let slow_start = Instant::now();
    let slow = order::next();
    let fast_start = slow_start + Duration::from_millis(300);
    let fast = order::next();
    receive(
        &claims,
        hash(1),
        fast,
        fast_start,
        slow_start + Duration::from_secs(1),
    );

    // The slow copy completes after its own deadline but inside the fast copy's.
    let slow_done = slow_start + CLAIM_BODY_DEADLINE + Duration::from_millis(100);
    receive(&claims, hash(1), slow, slow_start, slow_done);
    assert_eq!(
        used(&claims, hash(1), CLAIMED, slow_done),
        Some((ClaimSource::BlockMessage, fast))
    );
}

#[test]
fn a_usable_block_message_replaces_an_expired_inv() {
    let claims = AnnouncementClaims::default();
    let inv_at = Instant::now();
    announce(&claims, hash(1), inv_at);
    let inv = claims
        .announced_order(hash(1))
        .expect("the inv is recorded");

    let started_at = inv_at + Duration::from_millis(1_500);
    let reserved = order::next();
    let completed_at = inv_at + CLAIM_BODY_DEADLINE + Duration::from_millis(500);
    receive(&claims, hash(1), reserved, started_at, completed_at);
    assert!(inv < reserved);
    assert_eq!(
        used(&claims, hash(1), CLAIMED, completed_at),
        Some((ClaimSource::BlockMessage, reserved)),
        "the block ranks as if it had never been announced by inv",
    );
}

/// Exercise the same verifier decision with every pair of sources in both arrival orders.
#[tokio::test(start_paused = true)]
async fn verifier_uses_the_later_sighting_after_the_earlier_deadline() {
    use ClaimSource::{BlockMessage, Header, Inv};
    for (first, second) in [
        (BlockMessage, Inv),
        (Inv, BlockMessage),
        (Header, Inv),
        (Inv, Header),
        (Header, BlockMessage),
        (BlockMessage, Header),
        (BlockMessage, BlockMessage),
    ] {
        let claims = AnnouncementClaims::default();
        let hash = hash(1);
        let observe = |source| match source {
            Header => {
                claims.begin([(hash, CLAIMED)], TIP).commit();
                None
            }
            Inv => {
                claims.note_announcement(hash, TIP);
                None
            }
            BlockMessage => Some(claims.reserve_block_message(TIP)),
        };
        let first_transfer = observe(first);
        tokio::time::advance(Duration::from_millis(1500)).await;
        let second_transfer = observe(second);
        tokio::time::advance(Duration::from_millis(450)).await;
        for transfer in [first_transfer, second_transfer].into_iter().flatten() {
            assert!(claims.note_block_message(hash, Some(CLAIMED), transfer));
        }
        let (source, early) = used(&claims, hash, CLAIMED, Instant::now()).unwrap();
        assert_eq!(source, first);
        tokio::time::advance(Duration::from_millis(100)).await;
        let (source, later) = used(&claims, hash, CLAIMED, Instant::now()).unwrap();
        assert_eq!(source, second);
        assert!(
            later > early,
            "the expired order from {first:?} must yield to {second:?}"
        );
    }
}

#[test]
fn a_rollback_distinguishes_new_transfers_from_those_already_in_flight() {
    let claims = AnnouncementClaims::default();
    let in_flight = claims.reserve_block_message(TIP);
    claims.note_announcement(hash(5), CLAIMED);
    // Same current best tip for both transfers; only the second started after the rollback.
    let after_rollback = claims.reserve_block_message(TIP);
    assert!(claims.note_block_message(hash(1), Some(CLAIMED), in_flight));
    assert!(!claims.note_block_message(hash(2), Some(CLAIMED), after_rollback));
    assert_eq!(
        used(&claims, hash(1), CLAIMED, Instant::now()),
        Some((ClaimSource::BlockMessage, in_flight.order))
    );
    assert_eq!(used(&claims, hash(2), CLAIMED, Instant::now()), None);
    // A block above the remembered floor can qualify during the rollback.
    let higher = block::Height(TIP.0 + 2);
    assert!(claims.note_block_message(hash(3), Some(higher), claims.reserve_block_message(TIP)));
    assert!(used(&claims, hash(3), higher, Instant::now()).is_some());
    assert_eq!(used(&claims, hash(3), CLAIMED, Instant::now()), None);
}

#[test]
fn an_inventory_sighting_outside_its_window_allows_an_eligible_transfer() {
    let claims = AnnouncementClaims::default();
    let height = block::Height(103);
    claims.note_announcement(hash(1), block::Height(100));
    let transfer = claims.reserve_block_message(block::Height(101));
    assert!(claims.note_block_message(hash(1), Some(height), transfer));
    assert_eq!(
        used(&claims, hash(1), height, Instant::now()),
        Some((ClaimSource::BlockMessage, transfer.order))
    );
}

#[tokio::test(start_paused = true)]
async fn duplicate_transfers_share_a_bounded_budget_and_leave_room_for_inventory() {
    let claims = AnnouncementClaims::default();
    for _ in 0..MAX_ANNOUNCED_BLOCKS {
        assert!(claims.note_block_message(
            hash(1),
            Some(CLAIMED),
            claims.reserve_block_message(TIP)
        ));
    }
    let complete =
        |hash| claims.note_block_message(hash, Some(CLAIMED), claims.reserve_block_message(TIP));
    assert!(
        complete(hash(2)),
        "a refused near-tip delivery counts against the connection limit"
    );
    assert_eq!(used(&claims, hash(2), CLAIMED, Instant::now()), None);
    claims.note_announcement(hash(2), TIP);
    assert_eq!(
        used(&claims, hash(2), CLAIMED, Instant::now()).unwrap().0,
        ClaimSource::Inv
    );
    tokio::time::advance(CLAIM_BODY_DEADLINE * 2 + Duration::from_millis(1)).await;
    assert!(complete(hash(3)));
    assert_eq!(
        used(&claims, hash(3), CLAIMED, Instant::now()).unwrap().0,
        ClaimSource::BlockMessage
    );
}
