//! How the block header at the start of a legacy `block` message claims receipt order.
//!
//! It makes the same sighting an `inv` makes, so the rules those share (the deadline, the height
//! window, header claims and mined blocks) are tested once, in `announced`. These tests cover
//! what differs: when the sighting is made, and that the two kinds of message share one
//! sighting per block.

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

/// The verifier's registry gives a real body the place its block header took.
#[test]
fn a_body_registered_after_its_block_header_takes_the_headers_place() {
    let claims = AnnouncementClaims::default();
    let registry = ReceiptRegistry::new(claims.clone());
    let header_at = Instant::now();
    let block: Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    claims.note_block_header_at(block.hash(), block::Height(0), header_at);
    let place = claims
        .announced_order(block.hash())
        .expect("the header sighting is recorded");

    // A rival's body is registered while the block's transactions are still arriving.
    let rival: Block = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let rival = BlockHandle::from(Arc::new(rival));
    let rival = registry.register_at(
        rival.hash(),
        &rival,
        arrival(header_at + Duration::from_millis(300)),
    );

    let body = BlockHandle::from(Arc::new(block));
    let receipt = registry.register_at(
        body.hash(),
        &body,
        arrival(header_at + Duration::from_millis(800)),
    );
    assert_eq!(receipt.order, place);
    assert!(receipt.order < rival.order);
}
