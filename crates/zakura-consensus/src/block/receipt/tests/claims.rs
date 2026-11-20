//! How claims are made, kept, dropped and resolved.

use super::*;

/// What a body of `hash` finds when it arrives at `now` inside its deadline.
fn used(claims: &AnnouncementClaims, hash: block::Hash, now: Instant) -> Used {
    match claims.resolve(hash, Some(CLAIMED), now) {
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

    let at_the_bound = now + CLAIM_BODY_DEADLINE;
    let order = body_order(&claims, hash(1), at_the_bound);
    assert!(order < competitor);
    assert_eq!(Some(order), claims.claimed_order(hash(1)));
    assert!(
        body_order(&claims, hash(2), now) > competitor,
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
        late += CLAIM_BODY_DEADLINE;
        claim(&claims, hash(1), late);
    }
    let competitor = order::next();
    assert!(
        body_order(&claims, hash(1), late) > competitor,
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
    let during = body_order(&claims, hash(1), now);
    assert!(during < rival_during_apply);

    pending.commit();
    assert_eq!(body_order(&claims, hash(1), now), during);
}

#[test]
fn a_header_that_is_not_applied_leaves_no_claim() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    drop(claims.begin_at([(hash(1), CLAIMED)], TIP, now));
    assert!(!claims.is_claimed(hash(1)));

    // A later apply of the same header claims from its own arrival.
    let later = now + CLAIM_BODY_DEADLINE;
    claim(&claims, hash(1), later);
    let found = used(&claims, hash(1), later);
    assert_eq!(found.age, Duration::ZERO);
}

#[test]
fn small_and_full_size_bodies_get_the_same_deadline() {
    use zakura_test::vectors::{BLOCK_MAINNET_1_BYTES, BLOCK_TESTNET_141042_BYTES};

    let now = Instant::now();
    for bytes in [&*BLOCK_MAINNET_1_BYTES, &*BLOCK_TESTNET_141042_BYTES] {
        let block: Block = bytes.zcash_deserialize_into().unwrap();
        let hash = block.hash();
        let height = block
            .coinbase_height()
            .expect("both fixtures have a coinbase height");
        let claims = AnnouncementClaims::default();
        claims
            .begin_at([(hash, height)], height.previous().unwrap(), now)
            .commit();
        let reserved = claims.claimed_order(hash).unwrap();
        let registry = ReceiptRegistry::new(claims);
        let body = BlockHandle::from(Arc::new(block));

        let deadline = now + CLAIM_BODY_DEADLINE;
        assert_eq!(registry.register_at(hash, &body, deadline).order, reserved);
        let late = deadline + Duration::from_millis(1);
        assert!(registry.register_at(hash, &body, late).order > reserved);
    }
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
        body_order(&claims, hash(1), now) > mined_order,
        "a rival announced first does not displace the mined block",
    );
    assert_eq!(
        body_order(&claims, hash(2), now),
        above,
        "a claim at another height is honoured",
    );

    // The mined block failed verification, so the rival's claim is back in force.
    drop(mined);
    assert_eq!(body_order(&claims, hash(1), now), rival);
}

#[test]
fn a_mined_block_record_lasts_while_any_request_for_it_is_active_or_once_it_verifies() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    claim(&claims, hash(1), now);
    let voided = || {
        matches!(
            claims.resolve(hash(1), Some(CLAIMED), now),
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

    // A higher submission cannot retire this record while pending or after rejection.
    let higher = claims.mark_mined(hash(4), block::Height(CLAIMED.0 + 3), mined_order + 1);
    assert!(voided());
    drop(higher);
    assert!(voided());

    // Nor can one that verifies. Only its age retires a record.
    claims
        .mark_mined(hash(5), block::Height(CLAIMED.0 + 3), mined_order + 2)
        .keep();
    assert!(voided());
}

#[test]
fn recording_metrics_sizes_only_bodies_with_a_claim() {
    use ClaimOutcome::{Expired, Unclaimed, Used, Voided};

    let source = ClaimSource::Inv;
    let age = Duration::from_millis(500);
    for outcome in [
        Unclaimed,
        Used {
            order: 1,
            source,
            age,
        },
        Expired { source, age },
        Voided { source, age },
    ] {
        let mut sizes = 0;
        outcome.record(hash(1), 1, || {
            sizes += 1;
            100_000
        });
        assert_eq!(sizes, usize::from(outcome != Unclaimed), "{outcome:?}");
    }
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
    let later = now + Duration::from_millis(500);
    let second = claims.begin_at([(hash(1), CLAIMED)], TIP, later);

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
    claims.begin_at([(hash(3), CLAIMED)], TIP, later).commit();
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

/// Submits and rejects another mined block at `now`, which retires the records old enough.
fn submit_another(claims: &AnnouncementClaims, now: Instant) {
    drop(claims.mark_mined_at(hash(9), block::Height(CLAIMED.0 + 1), order::next(), now));
}

#[test]
fn a_rival_claimed_before_a_mined_block_never_regains_its_claim() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    // The rival is claimed at the instant the mined block is recorded, the latest that a claim
    // with an earlier order can be made.
    claim(&claims, hash(1), now);
    let mined_order = order::next();
    claims
        .mark_mined_at(hash(3), CLAIMED, mined_order, now)
        .keep();

    // The record outlasts the claim's deadline, so it voids the claim to its last instant.
    let claim_ends = now + CLAIM_BODY_DEADLINE;
    for arrival in [now, claim_ends, now + MINED_RECORD_LIFETIME] {
        submit_another(&claims, arrival);
        assert!(matches!(
            claims.resolve(hash(1), Some(CLAIMED), arrival),
            ClaimOutcome::Voided { .. }
        ));
        assert!(body_order(&claims, hash(1), arrival) > mined_order);
    }

    // Once the record is retired, the claim has long expired on its own.
    let late = now + MINED_RECORD_LIFETIME + Duration::from_millis(1);
    submit_another(&claims, late);
    assert_eq!(claims.mined_blocks(), 0);
    assert!(matches!(
        claims.resolve(hash(1), Some(CLAIMED), late),
        ClaimOutcome::Expired { .. }
    ));
    assert!(body_order(&claims, hash(1), late) > mined_order);
}

#[test]
fn a_verified_mined_block_record_is_retired_only_by_its_age() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    claims
        .mark_mined_at(hash(3), CLAIMED, order::next(), now)
        .keep();

    // The verified tip passing the block does not retire its record.
    claims
        .begin_at([], block::Height(CLAIMED.0 + 1), now)
        .commit();
    assert_eq!(claims.mined_blocks(), 1);

    // It outlasts the deadline of every claim it can void.
    submit_another(
        &claims,
        now + CLAIM_BODY_DEADLINE + Duration::from_millis(1),
    );
    assert_eq!(claims.mined_blocks(), 1);

    submit_another(&claims, now + MINED_RECORD_LIFETIME);
    assert_eq!(claims.mined_blocks(), 1, "kept through its lifetime");

    submit_another(
        &claims,
        now + MINED_RECORD_LIFETIME + Duration::from_millis(1),
    );
    assert_eq!(claims.mined_blocks(), 0);
}

#[test]
fn a_mined_block_record_takes_the_height_of_the_body_that_verifies() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    claim(&claims, hash(1), now);
    let voided = || {
        matches!(
            claims.resolve(hash(1), Some(CLAIMED), now),
            ClaimOutcome::Voided { .. }
        )
    };

    // Two bodies share a header hash. The first names another height and is rejected.
    let other_height = block::Height(CLAIMED.0 + 1);
    let rejected = claims.mark_mined(hash(3), other_height, order::next());
    claims.mark_mined(hash(3), CLAIMED, order::next()).keep();
    assert!(voided());
    drop(rejected);
    assert!(voided());
}

#[test]
fn a_mined_block_still_verifying_keeps_its_record_whatever_its_age() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    let long_after = now + MINED_RECORD_LIFETIME * 10;

    let rejected = claims.mark_mined_at(hash(3), CLAIMED, order::next(), now);
    let verified = claims.mark_mined_at(hash(4), CLAIMED, order::next(), now);
    submit_another(&claims, long_after);
    assert_eq!(claims.mined_blocks(), 2);

    // Each guard still finds its own record.
    drop(rejected);
    assert_eq!(claims.mined_blocks(), 1);
    verified.keep();
    assert_eq!(claims.mined_blocks(), 1);

    // Once verified, the record is old enough to retire.
    submit_another(&claims, long_after);
    assert_eq!(claims.mined_blocks(), 0);
}

#[test]
fn a_block_mined_again_after_its_record_retired_is_recorded_anew() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    let first_order = order::next();
    claims
        .mark_mined_at(hash(3), CLAIMED, first_order, now)
        .keep();

    // A rival is claimed after the first record has aged out, then the block is mined again.
    // While the first record was still held, the block would have joined it and kept
    // `first_order`, and the rival's claim would have been honoured.
    let later = now + MINED_RECORD_LIFETIME + Duration::from_millis(1);
    claim(&claims, hash(1), later);
    let rival = claims
        .claimed_order(hash(1))
        .expect("the rival was claimed");
    let again_order = order::next();
    let again = claims.mark_mined_at(hash(3), CLAIMED, again_order, later);
    assert!(first_order < rival && rival < again_order);

    assert!(
        body_order(&claims, hash(1), later) > again_order,
        "the new record carries the new submission's order",
    );
    drop(again);
    assert_eq!(body_order(&claims, hash(1), later), rival);
}
