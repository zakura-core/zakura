//! The authority check's rules, fed by hand on the Regtest fixture.

use std::num::NonZeroU64;

use zakura_chain::{block, work::difficulty::U256};
use zakura_header_chain::{
    BranchId, HeaderGeneration, HeaderSyncWorkOwner, HeaderWorkAuthority, StateVersion,
    VerifiedGeneration,
};
use zakura_network::zakura::{
    observe::RequestRef,
    v9::{Cursor, Operation, SubscribeHeaders},
    AuxSchema, Credit,
};

use super::*;

/// The mirror's peer ID.
fn mirror() -> ZakuraPeerId {
    ZakuraPeerId::new(vec![2; 32]).expect("32 bytes is a valid peer ID")
}

/// The main chain's first `count` fixture headers past genesis, as
/// frontiers.
fn main_chain(count: u32) -> Vec<Frontier> {
    let fixture = HeaderFixture::regtest();
    fixture
        .extend(fixture.anchor(), count, 0)
        .iter()
        .zip(1..)
        .map(|(header, height)| Frontier::new(block::Height(height), header.hash()))
        .collect()
}

/// The reactor's adoption of a snapshot anchored at genesis.
fn adopted() -> ReactorEvent {
    let anchor = HeaderFixture::regtest().anchor();
    ReactorEvent::SnapshotAdopted {
        state_version: StateVersion::new(1),
        header_generation: HeaderGeneration::new(1),
        finalized: anchor,
        header_best: anchor,
        suffix_work: U256::from(0_u32),
        oldest_retained: block::Height(0),
        verified_generation: VerifiedGeneration::new(1),
        verified_best: anchor,
    }
}

/// The subscriber's `Open` of subscription 1 on session 7 to `target`,
/// from `locators`.
fn open(target: Frontier, locators: &[block::Hash]) -> ReactorEvent {
    ReactorEvent::CreditIssued {
        kind: CreditKind::Open,
        peer: mirror(),
        session_id: 7,
        subscription: 1,
        objects: 10,
        bytes: 10_000,
        update: Box::new(SubscribeHeaders {
            operation: Operation::Open,
            subscription_id: 1,
            update_sequence: 0,
            initial_target_tip_hash: target.hash,
            acknowledged: Cursor {
                height: block::Height(0),
                hash: locators[0],
                headers: 0,
                bytes: 0,
            },
            locator_hashes: locators.to_vec(),
            added_credit: Credit {
                objects: 10,
                bytes: 10_000,
            },
            tree_aux_schema: AuxSchema::None,
        }),
        target_height: target.height,
    }
}

/// Round 1's request on subscription 1, with an owner for `request_id`
/// targeting `target` from `anchor`.
fn request(request_id: u64, target: block::Hash, anchor: block::Hash) -> RequestRef {
    let owner = HeaderSyncWorkOwner::Header(
        HeaderWorkAuthority {
            header_generation: HeaderGeneration::new(1),
            branch: BranchId::new(anchor, target),
        }
        .bind(7, NonZeroU64::new(request_id).expect("nonzero")),
    );
    RequestRef {
        peer: mirror(),
        session_id: 7,
        request_id: 1,
        incarnation: 1,
        owner,
        target_tip_hash: target,
    }
}

/// Round 1's start, handing on `locator`.
fn started(request: RequestRef, locator: &[block::Hash]) -> ReactorEvent {
    ReactorEvent::Started {
        request,
        purpose: HeaderTargetPurpose::Normal,
        locator: locator.to_vec(),
    }
}

/// A page on round 1 declaring `ancestor` and carrying `hashes`.
fn page(request: &RequestRef, ancestor: Frontier, hashes: &[block::Hash]) -> ReactorEvent {
    ReactorEvent::PageStaged {
        from_peer: mirror(),
        from_session: 7,
        request: request.clone(),
        reservation: 1,
        subscription: 1,
        frame: None,
        headers: hashes.len(),
        reserved_left: 0,
        ancestor,
        hashes: hashes.to_vec(),
        stored: hashes.to_vec(),
        staged_total: hashes.len(),
    }
}

/// The breaks `events` give, fed in order on the reactor's task.
fn breaks(events: &[ReactorEvent]) -> Vec<String> {
    let mut check = Authority::new(AuthorityEvidence::default(), HeaderFixture::regtest());
    for event in events {
        check.observe(event, true);
    }
    check.drain(1)
}

/// A round bound to its Open that stages the chain to its target in two
/// pages holds.
#[test]
fn a_bound_round_on_its_path_holds() {
    let chain = main_chain(4);
    let genesis = HeaderFixture::regtest().anchor();
    let round = request(1, chain[3].hash, genesis.hash);
    assert_eq!(
        breaks(&[
            adopted(),
            open(chain[3], &[genesis.hash]),
            started(round.clone(), &[genesis.hash]),
            page(&round, genesis, &[chain[0].hash, chain[1].hash]),
            page(&round, chain[1], &[chain[2].hash, chain[3].hash]),
        ]),
        Vec::<String>::new()
    );
}

/// An owner bound to another request, target or anchor, or a round handed
/// another locator than its Open's, breaks owner binding.
#[test]
fn a_round_bound_off_its_open_breaks() {
    let chain = main_chain(4);
    let genesis = HeaderFixture::regtest().anchor().hash;
    for (round, locator, why) in [
        (request(2, chain[3].hash, genesis), genesis, "for request 2"),
        (request(1, chain[2].hash, genesis), genesis, "targeting"),
        (
            request(1, chain[3].hash, chain[0].hash),
            genesis,
            "anchored at",
        ),
        (
            request(1, chain[3].hash, genesis),
            chain[0].hash,
            "handed on locator",
        ),
    ] {
        let found = breaks(&[
            adopted(),
            open(chain[3], &[genesis]),
            started(round, &[locator]),
        ]);
        assert!(
            found.iter().any(|found| found.contains(why)),
            "{why}: {found:?}"
        );
    }
}

/// A page that skips a header, declares an ancestor other than a locator
/// or the last staged header, passes its target, or carries a locator
/// breaks the path rule.
#[test]
fn a_page_off_its_path_breaks() {
    let chain = main_chain(4);
    let genesis = HeaderFixture::regtest().anchor();
    let round = request(1, chain[3].hash, genesis.hash);
    let base = || {
        vec![
            adopted(),
            open(chain[3], &[chain[1].hash, genesis.hash]),
            started(round.clone(), &[chain[1].hash, genesis.hash]),
        ]
    };
    let with = |pages: Vec<ReactorEvent>| {
        let mut events = base();
        events.extend(pages);
        breaks(&events)
    };
    let skipped = with(vec![page(&round, genesis, &[chain[1].hash])]);
    assert!(skipped[0].contains("isn't a fixture child"), "{skipped:?}");
    let undeclared = with(vec![page(&round, chain[0], &[chain[1].hash])]);
    assert!(
        undeclared[0].contains("not one of its Open's locators"),
        "{undeclared:?}"
    );
    let locator = with(vec![page(&round, genesis, &[chain[0].hash, chain[1].hash])]);
    assert!(
        locator[0].contains("carried its Open's locator"),
        "{locator:?}"
    );
    let later = with(vec![
        page(&round, chain[1], &[chain[2].hash]),
        page(&round, chain[1], &[chain[2].hash]),
    ]);
    assert!(later[0].contains("declared ancestor"), "{later:?}");
    let past = with(vec![page(
        &round,
        chain[1],
        &[chain[2].hash, chain[3].hash],
    )]);
    assert_eq!(past, Vec::<String>::new());
    let beyond = main_chain(5);
    let overrun = with(vec![page(
        &round,
        chain[1],
        &[chain[2].hash, chain[3].hash, beyond[4].hash],
    )]);
    assert!(
        overrun
            .iter()
            .any(|found| found.contains("without ending at the target")),
        "{overrun:?}"
    );
}
