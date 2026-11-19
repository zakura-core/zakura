//! How claims are made, kept, dropped and resolved.

use super::*;

/// What a body of `hash` with no size finds when it arrives at `now` inside its deadline.
fn used(claims: &AnnouncementClaims, hash: block::Hash, now: Instant) -> Used {
    match claims.resolve(hash, now, || 0) {
        ClaimOutcome::Used { order, age, .. } => Used { order, age },
        outcome => panic!("expected a used claim, found {outcome:?}"),
    }
}

struct Used {
    order: u64,
    age: Duration,
}

#[test]
fn a_body_inside_its_deadline_keeps_the_order_of_its_header() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    claim(&claims, hash(1), now);
    let competitor = order::next();

    let at_the_bound = now + body_deadline(0);
    let order = body_order(&claims, hash(1), 0, at_the_bound);
    assert!(order < competitor);
    assert_eq!(Some(order), claims.claimed_order(hash(1)));
    assert!(
        body_order(&claims, hash(2), 0, now) > competitor,
        "a claim only reserves priority for the hash it was made for",
    );
}

#[test]
fn the_first_claim_for_a_block_is_permanent() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    claim(&claims, hash(1), now);
    let first = used(&claims, hash(1), now);

    let later = now + Duration::from_millis(300);
    claim(&claims, hash(1), later);
    let again = used(&claims, hash(1), later);
    assert_eq!(again.order, first.order);
    assert_eq!(
        again.age,
        Duration::from_millis(300),
        "the claim still dates from the first header",
    );
}

#[test]
fn announcing_a_withheld_block_again_does_not_renew_its_claim() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    claim(&claims, hash(1), now);

    // The body is withheld past its deadline while the header is delivered again.
    let mut late = now;
    for _ in 0..3 {
        late += MAX_CLAIM_BODY_DEADLINE;
        claim(&claims, hash(1), late);
    }
    let competitor = order::next();
    assert!(
        body_order(&claims, hash(1), 2_000_000, late) > competitor,
        "a withheld body is ranked by its arrival",
    );
}

#[test]
fn a_body_arriving_during_its_header_apply_takes_the_reserved_order() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    let pending = claims.begin_at([(hash(1), CLAIMED)], TIP, now);
    let rival_during_apply = order::next();

    // The block's own body arrives before the header chain has accepted its header.
    let during = body_order(&claims, hash(1), 0, now);
    assert!(during < rival_during_apply);

    pending.commit();
    assert_eq!(body_order(&claims, hash(1), 0, now), during);
}

#[test]
fn a_header_that_is_not_applied_leaves_no_claim() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    drop(claims.begin_at([(hash(1), CLAIMED)], TIP, now));
    assert!(!claims.is_claimed(hash(1)));

    // A later apply of the same header claims from its own arrival.
    let later = now + MAX_CLAIM_BODY_DEADLINE;
    claim(&claims, hash(1), later);
    let found = used(&claims, hash(1), later);
    assert_eq!(found.age, Duration::ZERO);
}

#[test]
fn the_deadline_grows_with_body_size_up_to_the_window() {
    const MIB: usize = 1024 * 1024;
    assert_eq!(body_deadline(0), CLAIM_BASE_GRACE);
    assert_eq!(
        body_deadline(MIB),
        CLAIM_BASE_GRACE + Duration::from_secs(1)
    );
    assert_eq!(body_deadline(2 * MIB), MAX_CLAIM_BODY_DEADLINE);
    assert_eq!(body_deadline(usize::MAX), MAX_CLAIM_BODY_DEADLINE);

    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    claim(&claims, hash(1), now);
    let competitor = order::next();

    let past_the_grace = now + CLAIM_BASE_GRACE + Duration::from_millis(1);
    assert!(body_order(&claims, hash(1), 1_000, past_the_grace) > competitor);
    assert!(body_order(&claims, hash(1), MIB, past_the_grace) < competitor);

    let past_the_window = now + MAX_CLAIM_BODY_DEADLINE + Duration::from_millis(1);
    assert!(body_order(&claims, hash(1), 2 * MIB, past_the_window) > competitor);
}

#[test]
fn only_headers_just_above_the_verified_tip_are_claimed() {
    let claims = AnnouncementClaims::default();
    // A batch that reaches from below the tip to far ahead of it, as catch-up delivers.
    let heights = [99, 100, 101, 102, 103, 5_000].map(block::Height);
    let headers = heights
        .into_iter()
        .enumerate()
        .map(|(index, height)| (hash(index), height));
    claims.begin_at(headers, TIP, Instant::now()).commit();

    let claimed: Vec<_> = heights
        .into_iter()
        .enumerate()
        .filter(|(index, _)| claims.is_claimed(hash(*index)))
        .map(|(_, height)| height)
        .collect();
    assert_eq!(claimed, [block::Height(101), block::Height(102)]);
}

#[test]
fn a_full_table_refuses_new_claims_and_keeps_the_old_ones() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    for index in 0..MAX_ANNOUNCEMENT_CLAIMS {
        claim(&claims, hash(index), now);
    }
    let orders = || -> Vec<_> {
        (0..MAX_ANNOUNCEMENT_CLAIMS)
            .map(|index| {
                claims
                    .claimed_order(hash(index))
                    .expect("the claim is held")
            })
            .collect()
    };
    let before = orders();

    claim(&claims, hash(MAX_ANNOUNCEMENT_CLAIMS), now);
    assert!(!claims.is_claimed(hash(MAX_ANNOUNCEMENT_CLAIMS)));
    assert_eq!(orders(), before);
}

#[test]
fn claims_below_the_verified_tip_are_dropped_for_good() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    claim(&claims, hash(1), now);

    // The claimed height is the new tip, where a tie can still be decided.
    claims.begin_at([], CLAIMED, now).commit();
    assert!(claims.is_claimed(hash(1)));

    let above = block::Height(CLAIMED.0 + 1);
    claims.begin_at([], above, now).commit();
    assert!(!claims.is_claimed(hash(1)));

    // Neither a new delivery nor a lower tip brings the claim back.
    claims.begin_at([(hash(1), CLAIMED)], above, now).commit();
    claim(&claims, hash(1), now);
    assert!(!claims.is_claimed(hash(1)));
}

#[test]
fn a_reservation_overtaken_by_the_verified_tip_is_not_committed() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    let pending = claims.begin_at([(hash(1), CLAIMED)], TIP, now);
    claims
        .begin_at([], block::Height(CLAIMED.0 + 1), now)
        .commit();
    pending.commit();
    assert!(!claims.is_claimed(hash(1)));
}

#[test]
fn a_mined_block_outranks_an_earlier_claim_at_its_height() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    claim(&claims, hash(1), now);
    claims
        .begin_at([(hash(2), block::Height(CLAIMED.0 + 1))], TIP, now)
        .commit();
    let rival = claims
        .claimed_order(hash(1))
        .expect("the rival was claimed");
    let above = claims
        .claimed_order(hash(2))
        .expect("the next height was claimed");

    let mined_order = order::next();
    let mined = claims.mark_mined(hash(3), CLAIMED, mined_order);
    assert!(
        body_order(&claims, hash(1), 0, now) > mined_order,
        "a rival announced first does not displace the mined block",
    );
    assert_eq!(
        body_order(&claims, hash(2), 0, now),
        above,
        "a claim at another height is honoured",
    );

    // The mined block failed verification, so the rival's claim is back in force.
    drop(mined);
    assert_eq!(body_order(&claims, hash(1), 0, now), rival);
}

#[test]
fn a_mined_block_record_lasts_while_any_request_for_it_is_active_or_once_it_verifies() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    claim(&claims, hash(1), now);
    let voided = || {
        matches!(
            claims.resolve(hash(1), now, || 0),
            ClaimOutcome::Voided { .. }
        )
    };

    let mined_order = order::next();
    let first = claims.mark_mined(hash(3), CLAIMED, mined_order);
    let duplicate = claims.mark_mined(hash(3), CLAIMED, mined_order);
    // The duplicate submission is rejected while the first is still verifying.
    drop(duplicate);
    assert!(voided());

    // The first submission verified, so the record outlives its request.
    first.keep();
    assert!(voided());

    // A mined block at the next height that fails leaves this one's record in place.
    let next = claims.mark_mined(hash(4), block::Height(CLAIMED.0 + 1), mined_order + 1);
    drop(next);
    assert!(voided());
}

#[test]
fn size_labels_split_at_their_bounds() {
    assert_eq!(size_label(99_999), "under_100kB");
    assert_eq!(size_label(100_000), "under_500kB");
    assert_eq!(size_label(499_999), "under_500kB");
    assert_eq!(size_label(999_999), "under_1MB");
    assert_eq!(size_label(1_000_000), "1MB_or_more");
}

#[test]
fn a_claim_lasts_while_any_apply_covering_it_runs_or_succeeds() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    let first = claims.begin_at([(hash(1), CLAIMED)], TIP, now);
    let reserved = claims
        .claimed_order(hash(1))
        .expect("the first apply reserved it");
    let second = claims.begin_at([(hash(1), CLAIMED)], TIP, now + CLAIM_BASE_GRACE);

    // The apply that reserved the claim fails, while another apply of the header succeeds.
    drop(first);
    assert_eq!(claims.claimed_order(hash(1)), Some(reserved));
    second.commit();
    assert_eq!(used(&claims, hash(1), now).order, reserved);

    // Once accepted, a failed apply of the same header leaves the claim in place.
    drop(claims.begin_at([(hash(1), CLAIMED)], TIP, now));
    assert_eq!(claims.claimed_order(hash(1)), Some(reserved));

    // A later apply that finishes first keeps the earlier reservation too.
    let earlier = claims.begin_at([(hash(3), CLAIMED)], TIP, now);
    let reserved = claims
        .claimed_order(hash(3))
        .expect("the earlier apply reserved it");
    claims
        .begin_at([(hash(3), CLAIMED)], TIP, now + CLAIM_BASE_GRACE)
        .commit();
    earlier.commit();
    assert_eq!(used(&claims, hash(3), now).order, reserved);

    // A header no apply succeeds for loses its claim.
    let failing = [
        claims.begin_at([(hash(2), CLAIMED)], TIP, now),
        claims.begin_at([(hash(2), CLAIMED)], TIP, now),
    ];
    drop(failing);
    assert!(!claims.is_claimed(hash(2)));
}

#[test]
fn mined_block_records_end_once_the_node_mines_well_above_them() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    claim(&claims, hash(1), now);
    let voided = || {
        matches!(
            claims.resolve(hash(1), now, || 0),
            ClaimOutcome::Voided { .. }
        )
    };
    claims.mark_mined(hash(3), CLAIMED, order::next()).keep();
    assert!(voided());

    let two_above = block::Height(CLAIMED.0 + 2);
    claims.mark_mined(hash(4), two_above, order::next()).keep();
    assert!(voided(), "a record within the claim window is kept");

    let three_above = block::Height(CLAIMED.0 + 3);
    claims
        .mark_mined(hash(5), three_above, order::next())
        .keep();
    assert!(!voided());
}

#[test]
fn mined_block_records_end_once_the_verified_tip_passes_them() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    claims.mark_mined(hash(3), CLAIMED, order::next()).keep();

    // At the verified tip a rival at the mined block's height can still win a tie.
    claims.begin_at([], CLAIMED, now).commit();
    assert_eq!(claims.mined_blocks(), 1);

    claims
        .begin_at([], block::Height(CLAIMED.0 + 1), now)
        .commit();
    assert_eq!(claims.mined_blocks(), 0);
}

#[test]
fn a_guard_for_a_pruned_record_leaves_a_newer_record_alone() {
    let claims = AnnouncementClaims::default();
    let stale = claims.mark_mined(hash(3), CLAIMED, order::next());
    // Mining well above it prunes the record while its request is still verifying.
    claims
        .mark_mined(hash(4), block::Height(CLAIMED.0 + 3), order::next())
        .keep();
    let again = claims.mark_mined(hash(3), CLAIMED, order::next());
    assert_eq!(claims.mined_blocks(), 2);

    drop(stale);
    assert_eq!(
        claims.mined_blocks(),
        2,
        "the newer record is still verifying"
    );
    drop(again);
    assert_eq!(claims.mined_blocks(), 1);
}
