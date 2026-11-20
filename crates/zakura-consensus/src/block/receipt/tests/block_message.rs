//! How a legacy `block` message claims receipt order from the moment it started arriving.

use super::*;

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

/// Records that a `block` message for `hash`, which reserved `reserved` at `started_at`,
/// completed at `now`, with the verified tip at [`TIP`].
fn receive(
    claims: &AnnouncementClaims,
    hash: block::Hash,
    reserved: u64,
    started_at: Instant,
    now: Instant,
) {
    claims.note_block_message_at(hash, reserved, started_at, TIP, now);
}

#[test]
fn a_block_message_started_before_a_rival_inv_outranks_it_though_it_completes_later() {
    let claims = AnnouncementClaims::default();
    let started_at = Instant::now();
    let reserved = claims.reserve_order();

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
    let reserved = claims.reserve_order();
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
    let reserved = claims.reserve_order();
    receive(&claims, hash(1), reserved, later, later);
    assert_eq!(claims.announced_order(hash(1)), Some(inv));
    assert!(matches!(
        claims.resolve(hash(1), Some(CLAIMED), later),
        ClaimOutcome::Used { source: ClaimSource::Inv, order, age }
            if order == inv && age == later - now
    ));

    // The message starts before the inv, which arrives while the message is still arriving.
    let reserved = claims.reserve_order();
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
    let again = claims.reserve_order();
    receive(&claims, hash(2), again, completed_at, completed_at);
    announce(&claims, hash(2), completed_at);
    assert_eq!(claims.announced_order(hash(2)), Some(reserved));
}

#[test]
fn a_block_keeps_the_earlier_of_its_header_claim_and_its_block_message() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();

    // The header is claimed first, so the message records nothing.
    claim(&claims, hash(1), now);
    let header = claims
        .claimed_order(hash(1))
        .expect("the header is claimed");
    receive(&claims, hash(1), claims.reserve_order(), now, now);
    assert_eq!(claims.announced_order(hash(1)), None);
    assert_eq!(
        used(&claims, hash(1), CLAIMED, now),
        Some((ClaimSource::Header, header))
    );

    // The message started before the header was claimed.
    let reserved = claims.reserve_order();
    claim(&claims, hash(2), now);
    receive(&claims, hash(2), reserved, now, now);
    assert_eq!(
        used(&claims, hash(2), CLAIMED, now),
        Some((ClaimSource::BlockMessage, reserved))
    );
}

#[test]
fn a_block_message_claims_only_bodies_just_above_the_verified_tip() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    receive(&claims, hash(1), claims.reserve_order(), now, now);

    for height in [99, 100, 103, 5_000].map(block::Height) {
        assert_eq!(
            claims.resolve(hash(1), Some(height), now),
            ClaimOutcome::Unclaimed,
            "a body at {height:?} is not racing at the tip",
        );
    }
    for height in [101, 102].map(block::Height) {
        assert!(used(&claims, hash(1), height, now).is_some());
    }

    // Once the tip passes height 101, a tie there can no longer be decided.
    claims.note_block_message_at(
        hash(2),
        claims.reserve_order(),
        now,
        block::Height(102),
        now,
    );
    assert_eq!(used(&claims, hash(1), CLAIMED, now), None);
}

#[test]
fn a_mined_block_outranks_an_earlier_block_message_at_its_height() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    let reserved = claims.reserve_order();
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
    let unused = claims.reserve_order();

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
    receive(
        &claims,
        hash(MAX_ANNOUNCED_BLOCKS),
        claims.reserve_order(),
        now,
        now,
    );
    assert_eq!(claims.announced_order(hash(MAX_ANNOUNCED_BLOCKS)), None);
}
