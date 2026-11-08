//! [`super::durable_row`] against the rows zakura-state writes, for every
//! auxiliary outcome, including the judged ones no simulated run reaches.

use std::num::NonZeroU64;

use zakura_chain::block;
use zakura_header_chain::{
    AuxDelivery, BodySizeHint, EvidenceId, Frontier, HeaderWorkAuthority, SourceId,
    VctRepairContext,
};

use super::durable_row;
use crate::header::{adapter::EnginePort, fixture::HeaderFixture};

/// The judged outcome codes: authenticated, rejected and disputed.
const JUDGED: [u8; 3] = [1, 2, 3];

/// A rooted delivery at height 1 with no outcome.
fn delivery() -> AuxDelivery {
    let fixture = HeaderFixture::regtest();
    let snapshot = EnginePort::new(&fixture).snapshot();
    let hash = block::Hash([2; 32]);
    let owner = HeaderWorkAuthority::for_target(&snapshot, hash)
        .bind(1, NonZeroU64::new(1).expect("one is nonzero"))
        .into();
    AuxDelivery::new(
        EvidenceId::from_digest([1; 32]),
        hash,
        SourceId::from_digest([3; 32]),
        owner,
        BodySizeHint::new(100).expect("a small size is a hint"),
        Some(fixture.tree_aux(block::Height(1))),
    )
}

/// `code`'s outcome fields: none for an unauthenticated delivery, else two
/// observations and a boundary.
fn outcome(code: u8) -> ([Option<[u8; 32]>; 2], Option<block::Hash>) {
    if code == 0 {
        ([None, None], None)
    } else {
        ([Some([4; 32]), Some([5; 32])], Some(block::Hash([6; 32])))
    }
}

/// Each outcome's row carries the base delivery and the raw outcome fields
/// zakura-state writes, and recovering the row gives the live delivery
/// back. A scheduling body-size correction stays off the row, since
/// zakura-state keeps it in its own column.
#[test]
fn each_outcome_round_trips_through_its_row() {
    let base = delivery();
    for code in 0..=3 {
        let (digests, boundary) = outcome(code);
        let live = base
            .test_only_with_outcome(code, digests, boundary)
            .expect("the outcome is well formed");
        let corrected = live.with_scheduling_body_size(BodySizeHint::new(500).expect("a hint"));
        for live in [live, corrected] {
            let row = durable_row(live);
            assert_eq!(row.delivery(), base, "{code}");
            assert_eq!(row.outcome_status_code(), code);
            assert_eq!(row.observation_digests(), digests, "{code}");
            assert_eq!(row.outcome_boundary_hash(), boundary, "{code}");
            assert!(row.has_valid_outcome(), "{code}");
            assert_eq!(
                row.delivery()
                    .test_only_with_outcome(code, digests, boundary)
                    .map(AuxDelivery::without_scheduling_body_size),
                Some(live.without_scheduling_body_size()),
                "{code}"
            );
        }
    }
}

/// A row constrains a repair when it carries a root or a judged outcome,
/// as the repair rules read zakura-state's rows: a rootless unauthenticated
/// row alone doesn't.
#[test]
fn rooted_or_judged_rows_constrain_a_repair() {
    let rooted = delivery();
    let rootless = AuxDelivery::new(
        rooted.delivery_id,
        rooted.header_hash,
        rooted.source,
        rooted.owner,
        rooted.body_size,
        None,
    );
    assert!(!VctRepairContext::rows_constrain_repair(&[durable_row(
        rootless
    )]));
    assert!(VctRepairContext::rows_constrain_repair(&[durable_row(
        rooted
    )]));
    for code in JUDGED {
        let (digests, boundary) = outcome(code);
        let judged = rootless
            .test_only_with_outcome(code, digests, boundary)
            .expect("the outcome is well formed");
        assert!(
            VctRepairContext::rows_constrain_repair(&[durable_row(judged)]),
            "{code}"
        );
    }
}

/// A rejected row excludes its own input at its boundary, so a repair that
/// brings that input again is refused, while an unauthenticated row
/// excludes nothing.
#[test]
fn a_rejected_row_excludes_its_input() {
    let base = delivery();
    let input = base.tree_aux.expect("the delivery is rooted");
    let target = Frontier::new(block::Height(1), base.header_hash);
    let (digests, boundary) = outcome(2);
    let rejected = base
        .test_only_with_outcome(2, digests, boundary)
        .expect("the outcome is well formed");
    assert_eq!(
        VctRepairContext::durable_rows_exclude(target, boundary, &[durable_row(rejected)], input),
        Ok(true)
    );
    assert_eq!(
        VctRepairContext::durable_rows_exclude(target, boundary, &[durable_row(base)], input),
        Ok(false)
    );
}
