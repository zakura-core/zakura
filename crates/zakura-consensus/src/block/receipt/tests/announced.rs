//! How a legacy block `inv` claims receipt order, alone and next to a header claim.

use super::*;

/// The source and order a body of `hash` at `height` takes when it arrives at `now`.
fn used(
    claims: &AnnouncementClaims,
    hash: block::Hash,
    height: block::Height,
    now: Instant,
) -> Option<(ClaimSource, u64)> {
    match claims.resolve(hash, Some(height), arrival(now)) {
        ClaimOutcome::Used { source, order, .. } => Some((source, order)),
        _ => None,
    }
}

#[test]
fn a_body_inside_its_inv_deadline_outranks_a_rival_announced_later() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    announce(&claims, hash(1), now);
    let announced = claims
        .announced_order(hash(1))
        .expect("the first inv is recorded");

    // The rival is announced later, but its body arrives first.
    let rival_at = now + Duration::from_millis(100);
    announce(&claims, hash(2), rival_at);
    let rival = body_order(&claims, hash(2), rival_at);

    // After the rival commits, another peer announces it at the new tip.
    claims.note_announcement_at(hash(2), CLAIMED, rival_at);

    let at_the_bound = now + CLAIM_BODY_DEADLINE;
    assert_eq!(
        used(&claims, hash(1), CLAIMED, at_the_bound),
        Some((ClaimSource::Inv, announced)),
    );
    assert!(announced < rival);

    let late = at_the_bound + Duration::from_millis(1);
    assert!(
        matches!(
            claims.resolve(hash(1), Some(CLAIMED), arrival(late)),
            ClaimOutcome::Expired {
                source: ClaimSource::Inv,
                ..
            }
        ),
        "a body past its inv deadline loses the claim",
    );
    assert!(
        body_order(&claims, hash(1), late) > rival,
        "a body past its inv deadline is ranked by its arrival",
    );
}

#[test]
fn an_inv_claims_only_bodies_just_above_the_verified_tip() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    announce(&claims, hash(1), now);

    for height in [99, 100, 103, 5_000].map(block::Height) {
        assert_eq!(
            claims.resolve(hash(1), Some(height), arrival(now)),
            ClaimOutcome::Unclaimed,
            "a body at {height:?} is not racing at the tip",
        );
    }
    assert_eq!(
        claims.resolve(hash(1), None, arrival(now)),
        ClaimOutcome::Unclaimed,
        "a body without a coinbase height has no place to claim",
    );
    for height in [101, 102].map(block::Height) {
        assert!(used(&claims, hash(1), height, now).is_some());
    }
}

#[test]
fn advancing_the_tip_cannot_make_an_out_of_range_announcement_eligible() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    announce(&claims, hash(1), now);

    // A duplicate sees a newer tip. Height 103 was three ahead at the first announcement.
    claims.note_announcement_at(hash(1), block::Height(101), now);
    assert_eq!(used(&claims, hash(1), block::Height(103), now), None);

    // Once the tip passes height 101, a tie there can no longer be decided.
    claims.note_announcement_at(hash(2), block::Height(102), now);
    assert_eq!(used(&claims, hash(1), CLAIMED, now), None);
}

#[test]
fn the_earlier_of_a_header_claim_and_an_inv_wins() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    let later = now + Duration::from_millis(500);

    // The inv arrives before the header.
    announce(&claims, hash(1), now);
    claim(&claims, hash(1), later);
    let inv = claims
        .announced_order(hash(1))
        .expect("the inv is recorded");
    assert!(
        inv < claims
            .claimed_order(hash(1))
            .expect("the header is claimed")
    );
    assert_eq!(
        used(&claims, hash(1), CLAIMED, later),
        Some((ClaimSource::Inv, inv))
    );

    // The header arrives before the inv, which then records nothing.
    claim(&claims, hash(2), now);
    announce(&claims, hash(2), later);
    let header = claims
        .claimed_order(hash(2))
        .expect("the header is claimed");
    assert_eq!(claims.announced_order(hash(2)), None);
    assert_eq!(
        used(&claims, hash(2), CLAIMED, later),
        Some((ClaimSource::Header, header))
    );
}

#[test]
fn an_inv_during_a_failed_header_apply_keeps_its_place_and_deadline() {
    let claims = AnnouncementClaims::default();
    let start = Instant::now();
    let pending = || claims.begin_at([(hash(1), CLAIMED)], TIP, start);
    let (first, second) = (pending(), pending());
    let inv_at = start + Duration::from_secs(1);
    announce(&claims, hash(1), inv_at);
    let rival = body_order(&claims, hash(2), inv_at);
    let deadline = inv_at + CLAIM_BODY_DEADLINE;
    drop(first);
    announce(&claims, hash(1), deadline);
    assert_eq!(used(&claims, hash(1), CLAIMED, deadline), None);

    drop(second);
    assert!(body_order(&claims, hash(1), deadline) < rival);
    assert!(body_order(&claims, hash(1), deadline + Duration::from_nanos(1)) > rival);
}

#[test]
fn accepting_any_header_apply_discards_its_later_inv() {
    for accept_first in [true, false] {
        let claims = AnnouncementClaims::default();
        let start = Instant::now();
        let pending = || claims.begin_at([(hash(1), CLAIMED)], TIP, start);
        let (accepted, cancelled) = (pending(), pending());
        let inv_at = start + Duration::from_secs(1);
        announce(&claims, hash(1), inv_at);
        let header = claims.claimed_order(hash(1)).unwrap();
        assert!(claims.announced_order(hash(1)).unwrap() > header);
        assert_eq!(
            used(&claims, hash(1), CLAIMED, inv_at),
            Some((ClaimSource::Header, header))
        );
        if accept_first {
            accepted.commit();
            drop(cancelled);
        } else {
            drop(cancelled);
            accepted.commit();
        }
        assert_eq!(claims.announced_order(hash(1)), None);
        assert_eq!(
            used(&claims, hash(1), CLAIMED, inv_at),
            Some((ClaimSource::Header, header))
        );
        assert_eq!(
            used(&claims, hash(1), CLAIMED, inv_at + CLAIM_BODY_DEADLINE),
            None
        );
    }
}

#[test]
fn advancing_past_a_pending_header_discards_its_fallback() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    let pending = claims.begin_at([(hash(1), CLAIMED)], TIP, now);
    announce(&claims, hash(1), now);
    assert!(claims.announced_order(hash(1)).is_some());
    claims
        .begin_at([], block::Height(CLAIMED.0 + 1), now)
        .commit();
    drop(pending);
    assert_eq!(claims.announced_order(hash(1)), None);
}

#[test]
fn an_expired_inv_does_not_cost_a_later_header_claim_its_deadline() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    announce(&claims, hash(1), now);
    let header_at = now + Duration::from_millis(1_500);
    claim(&claims, hash(1), header_at);
    let header = claims
        .claimed_order(hash(1))
        .expect("the header is claimed");

    let inv_expired = now + CLAIM_BODY_DEADLINE + Duration::from_millis(1);
    assert_eq!(
        used(&claims, hash(1), CLAIMED, inv_expired),
        Some((ClaimSource::Header, header))
    );

    // Once both expire, the metrics report the first announcement.
    let both_expired = header_at + CLAIM_BODY_DEADLINE + Duration::from_millis(1);
    assert!(matches!(
        claims.resolve(hash(1), Some(CLAIMED), arrival(both_expired)),
        ClaimOutcome::Expired { source: ClaimSource::Inv, age, .. } if age == both_expired - now
    ));
}

#[test]
fn a_voided_inv_falls_back_to_a_header_received_after_local_mining() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    announce(&claims, hash(1), now);
    let mined_order = order::next();
    let _mined = claims.mark_mined(hash(2), CLAIMED, mined_order);
    claim(&claims, hash(1), now);
    let header_order = claims.claimed_order(hash(1)).unwrap();

    assert!(mined_order < header_order);
    assert_eq!(
        used(&claims, hash(1), CLAIMED, now),
        Some((ClaimSource::Header, header_order))
    );
}

#[test]
fn a_repeated_inv_does_not_renew_or_move_the_claim() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    announce(&claims, hash(1), now);
    let first = claims
        .announced_order(hash(1))
        .expect("the first inv is recorded");

    let again = now + CLAIM_BODY_DEADLINE;
    announce(&claims, hash(1), again);
    assert_eq!(claims.announced_order(hash(1)), Some(first));

    let late = again + Duration::from_millis(1);
    announce(&claims, hash(1), late);
    assert!(
        matches!(
            claims.resolve(hash(1), Some(CLAIMED), arrival(late)),
            ClaimOutcome::Expired { age, .. } if age == late - now
        ),
        "the claim still dates from the first inv",
    );
}

#[test]
fn a_reannounced_block_gets_a_fresh_place_behind_its_rival() {
    let claims = AnnouncementClaims::default();
    let (block, rival) = (hash(1), hash(2));
    let start = Instant::now();
    announce(&claims, block, start);
    assert!(claims.announced_order(block).is_some());

    // Recording the rival also prunes the first block's old announcement.
    let rival_at = start + CLAIM_BODY_DEADLINE * 2 + Duration::from_millis(1);
    announce(&claims, rival, rival_at);
    assert_eq!(claims.announced_order(block), None);
    let again = rival_at + Duration::from_millis(100);
    announce(&claims, block, again);

    // Both bodies qualify for their announcements, even though the rival's body arrives last.
    let Some((ClaimSource::Inv, block_order)) = used(&claims, block, CLAIMED, again) else {
        panic!("the re-announced block should qualify for its fresh announcement");
    };
    let Some((ClaimSource::Inv, rival_order)) =
        used(&claims, rival, CLAIMED, again + Duration::from_millis(100))
    else {
        panic!("the rival should still qualify for its announcement");
    };
    assert!(
        rival_order < block_order,
        "the rival keeps its earlier place"
    );
}

#[test]
fn a_full_inv_store_refuses_new_blocks_until_old_entries_expire() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    for index in 0..MAX_ANNOUNCED_BLOCKS {
        announce(&claims, hash(index), now);
    }
    let orders = || -> Vec<_> {
        (0..MAX_ANNOUNCED_BLOCKS)
            .map(|index| claims.announced_order(hash(index)))
            .collect()
    };
    let before = orders();
    assert!(before.iter().all(Option::is_some));

    // Still inside the store's lifetime, which is twice the body deadline.
    let inside = now + CLAIM_BODY_DEADLINE * 2;
    announce(&claims, hash(MAX_ANNOUNCED_BLOCKS), inside);
    assert_eq!(claims.announced_order(hash(MAX_ANNOUNCED_BLOCKS)), None);
    assert_eq!(orders(), before);

    let expired = inside + Duration::from_millis(1);
    announce(&claims, hash(MAX_ANNOUNCED_BLOCKS), expired);
    assert!(claims.announced_order(hash(MAX_ANNOUNCED_BLOCKS)).is_some());
    assert!(orders().iter().all(Option::is_none));
}

#[test]
fn a_mined_block_outranks_an_earlier_inv_at_its_height() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    announce(&claims, hash(1), now);
    let announced = claims
        .announced_order(hash(1))
        .expect("the inv is recorded");

    let mined_order = order::next();
    let mined = claims.mark_mined(hash(3), CLAIMED, mined_order);
    assert!(matches!(
        claims.resolve(hash(1), Some(CLAIMED), arrival(now)),
        ClaimOutcome::Voided {
            source: ClaimSource::Inv,
            ..
        }
    ));
    assert!(
        body_order(&claims, hash(1), now) > mined_order,
        "a rival announced first does not displace the mined block",
    );

    drop(mined);
    assert_eq!(
        used(&claims, hash(1), CLAIMED, now),
        Some((ClaimSource::Inv, announced))
    );
}

#[test]
fn an_invalid_body_under_an_announced_hash_leaves_the_real_block_its_order() {
    let claims = AnnouncementClaims::default();
    let registry = ReceiptRegistry::new(claims.clone());
    let now = Instant::now();
    let block: Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    claims.note_announcement_at(block.hash(), block::Height(0), now);
    let announced = claims
        .announced_order(block.hash())
        .expect("the inv is recorded");

    // A junk body under the announced hash arrives first.
    let mut junk = block.clone();
    junk.transactions.push(block.transactions[0].clone());
    assert_eq!(junk.hash(), block.hash());
    let junk = registry.register_at(
        block.hash(),
        &BlockHandle::from(Arc::new(junk)),
        arrival(now),
    );
    assert_eq!(junk.order, announced);
    drop(junk);

    let real = BlockHandle::from(Arc::new(block.clone()));
    assert_eq!(
        registry
            .register_at(block.hash(), &real, arrival(now))
            .order,
        announced,
        "the junk body did not use the announcement up",
    );
}
