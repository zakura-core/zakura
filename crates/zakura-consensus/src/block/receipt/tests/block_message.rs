//! How the block header at the start of a legacy `block` message claims receipt order.
//!
//! It makes the same sighting an `inv` makes, so the rules those share are tested once, in
//! `announced`. These tests cover what differs: when the sighting is made, and that the two
//! kinds of message share one sighting per block.

use super::*;

/// The source and order a body of `hash` at [`CLAIMED`] takes when it arrives at `now`.
fn used(
    claims: &AnnouncementClaims,
    hash: block::Hash,
    now: Instant,
) -> Option<(ClaimSource, u64)> {
    match claims.resolve(hash, Some(CLAIMED), arrival(now)) {
        ClaimOutcome::Used { source, order, .. } => Some((source, order)),
        _ => None,
    }
}

/// Records that the block header of a `block` message carrying `hash` arrived at `now`, with
/// the verified tip at [`TIP`].
fn sight_header(claims: &AnnouncementClaims, hash: block::Hash, now: Instant) {
    claims.note_block_header_at(hash, TIP, now);
}

#[test]
fn a_block_whose_header_arrived_first_outranks_a_rival_announced_during_its_transfer() {
    let claims = AnnouncementClaims::default();
    let header_at = Instant::now();
    sight_header(&claims, hash(1), header_at);
    let place = claims
        .announced_order(hash(1))
        .expect("the header sighting is recorded");
    assert_eq!(
        used(&claims, hash(1), header_at),
        Some((ClaimSource::BlockMessage, place)),
        "the place is in force before any copy of the block has finished arriving",
    );

    // The rival is announced and its body arrives while the first block is still arriving.
    let rival_at = header_at + Duration::from_millis(300);
    announce(&claims, hash(2), rival_at);
    let rival = body_order(&claims, hash(2), rival_at);

    let completed_at = header_at + Duration::from_millis(800);
    assert_eq!(
        used(&claims, hash(1), completed_at),
        Some((ClaimSource::BlockMessage, place)),
    );
    assert!(place < rival);
}

#[test]
fn a_block_header_sighting_deadline_counts_from_the_sighting() {
    let claims = AnnouncementClaims::default();
    let header_at = Instant::now();
    sight_header(&claims, hash(1), header_at);

    let at_the_bound = header_at + CLAIM_BODY_DEADLINE;
    assert!(matches!(
        claims.resolve(hash(1), Some(CLAIMED), arrival(at_the_bound)),
        ClaimOutcome::Used { source: ClaimSource::BlockMessage, age, .. }
            if age == CLAIM_BODY_DEADLINE
    ));

    let late = at_the_bound + Duration::from_millis(1);
    assert!(matches!(
        claims.resolve(hash(1), Some(CLAIMED), arrival(late)),
        ClaimOutcome::Expired { source: ClaimSource::BlockMessage, age }
            if age == late - header_at
    ));
}

/// A peer that withholds a block cannot keep its place fresh by sending the block's header, or
/// an `inv` for it, again and again.
#[test]
fn a_withheld_block_cannot_keep_its_place_by_being_shown_again() {
    let claims = AnnouncementClaims::default();
    let first_at = Instant::now();
    sight_header(&claims, hash(1), first_at);
    let place = claims
        .announced_order(hash(1))
        .expect("the first sighting is recorded");

    for step in 1..=5u32 {
        let again_at = first_at + Duration::from_millis(500) * step;
        sight_header(&claims, hash(1), again_at);
        announce(&claims, hash(1), again_at);
        assert_eq!(claims.announced_order(hash(1)), Some(place));
    }

    let late = first_at + CLAIM_BODY_DEADLINE + Duration::from_millis(500);
    assert!(
        matches!(
            claims.resolve(hash(1), Some(CLAIMED), arrival(late)),
            ClaimOutcome::Expired { source: ClaimSource::BlockMessage, age }
                if age == late - first_at
        ),
        "the body is two seconds past the first sighting, however recently it was shown again",
    );
}

#[test]
fn a_block_keeps_its_first_legacy_sighting_whichever_message_made_it() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    let later = now + Duration::from_millis(300);

    announce(&claims, hash(1), now);
    let inv = claims.announced_order(hash(1));
    sight_header(&claims, hash(1), later);
    assert_eq!(claims.announced_order(hash(1)), inv);
    assert_eq!(
        used(&claims, hash(1), later).map(|(source, _)| source),
        Some(ClaimSource::Inv),
    );

    sight_header(&claims, hash(2), now);
    let header = claims.announced_order(hash(2));
    announce(&claims, hash(2), later);
    assert_eq!(claims.announced_order(hash(2)), header);
    assert_eq!(
        used(&claims, hash(2), later).map(|(source, _)| source),
        Some(ClaimSource::BlockMessage),
    );
}

#[test]
fn a_rival_committed_while_a_block_arrives_does_not_cost_it_its_place() {
    let claims = AnnouncementClaims::default();
    let header_at = Instant::now();
    sight_header(&claims, hash(1), header_at);
    let place = claims.announced_order(hash(1));

    // The rival at the same height commits and raises the tip before the block completes.
    let rival_at = header_at + Duration::from_millis(300);
    claims.note_announcement_at(hash(3), CLAIMED, rival_at);

    let completed_at = header_at + Duration::from_millis(800);
    assert_eq!(
        used(&claims, hash(1), completed_at).map(|(_, order)| order),
        place,
    );
}

#[test]
fn the_earlier_of_a_header_claim_and_a_block_header_sighting_wins() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    let later = now + Duration::from_millis(300);

    // Native header sync admits the header first.
    claim(&claims, hash(1), now);
    let native = claims
        .claimed_order(hash(1))
        .expect("the header is claimed");
    sight_header(&claims, hash(1), later);
    assert_eq!(
        used(&claims, hash(1), later),
        Some((ClaimSource::Header, native))
    );

    // The legacy `block` message's header arrives first.
    sight_header(&claims, hash(2), now);
    let legacy = claims
        .announced_order(hash(2))
        .expect("the header sighting is recorded");
    claim(&claims, hash(2), later);
    assert_eq!(
        used(&claims, hash(2), later),
        Some((ClaimSource::BlockMessage, legacy))
    );
}

#[test]
fn a_mined_block_outranks_an_earlier_block_header_sighting_at_its_height() {
    let claims = AnnouncementClaims::default();
    let now = Instant::now();
    sight_header(&claims, hash(1), now);

    let _mined = claims.mark_mined(hash(3), CLAIMED, order::next());
    assert!(matches!(
        claims.resolve(hash(1), Some(CLAIMED), arrival(now)),
        ClaimOutcome::Voided {
            source: ClaimSource::BlockMessage,
            ..
        }
    ));
}
