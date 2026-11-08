//! The authority check's rules, fed by hand on the Regtest fixture.

use std::num::NonZeroU64;

use zakura_chain::{block, work::difficulty::U256};
use zakura_header_chain::{
    BranchId, HeaderGeneration, HeaderSyncWorkOwner, HeaderWorkAuthority, StateVersion,
    VerifiedGeneration,
};
use zakura_network::zakura::{
    observe::{RequestRef, ReservationEnd, ReservationPurpose},
    v9::{Cursor, Headers, Operation, RegtestMessage, SubscribeHeaders},
    wire_codec::encode_frame,
    AuxSchema, Credit, HeaderEntry,
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

/// The mirror's reservation `reservation` of `headers` headers.
fn reserved(reservation: u64, headers: usize) -> ReactorEvent {
    ReactorEvent::ReservationOpened {
        reservation,
        peer: mirror(),
        purpose: ReservationPurpose::Ordinary,
        headers,
        request: None,
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
        bytes: 2_630,
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
                bytes: 2_630,
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
    breaks_declaring(&[], events)
}

/// [`breaks`], with `declared` `(parent, header)` pairs beyond the fixture.
/// The `Open`'s reservation comes first.
fn breaks_declaring(
    declared: &[(block::Hash, block::Hash)],
    events: &[ReactorEvent],
) -> Vec<String> {
    let mut check = Authority::new(
        AuthorityEvidence::default(),
        HeaderFixture::regtest(),
        [0, 1, 2],
        declared.iter().copied(),
    );
    check.observe(&reserved(99, 10), true, false);
    for event in events {
        check.observe(event, true, false);
    }
    check.drain(1)
}

/// The breaks `events` give with no reservation fed first.
fn credit_breaks(events: &[ReactorEvent]) -> Vec<String> {
    let mut check = Authority::new(
        AuthorityEvidence::default(),
        HeaderFixture::regtest(),
        [0],
        [],
    );
    for event in events {
        check.observe(event, true, false);
    }
    check.drain(1)
}

/// A page on a fork branch the scenario uses holds, and on one it doesn't
/// use breaks the path rule.
#[test]
fn a_page_on_a_scenario_branch_holds() {
    let fixture = HeaderFixture::regtest();
    let genesis = fixture.anchor();
    let fork = fixture.extend(genesis, 2, 5);
    let tip = Frontier::new(block::Height(2), fork[1].hash());
    let round = request(1, tip.hash, genesis.hash);
    let events = [
        adopted(),
        open(tip, &[genesis.hash]),
        started(round.clone(), &[genesis.hash]),
        page(&round, genesis, &[fork[0].hash(), fork[1].hash()]),
    ];
    let with = |branches: &[u8]| {
        let mut check = Authority::new(
            AuthorityEvidence::default(),
            HeaderFixture::regtest(),
            branches.iter().copied(),
            [],
        );
        check.observe(&reserved(99, 10), true, false);
        for event in &events {
            check.observe(event, true, false);
        }
        check.drain(1)
    };
    assert_eq!(with(&[0, 5]), Vec::<String>::new());
    let found = with(&[0, 1, 2]);
    assert!(
        found
            .iter()
            .any(|found| found.contains("isn't a fixture or declared child")),
        "{found:?}"
    );
}

/// A page ending at a header the scenario declares beyond the fixture, as
/// an invalid tip a peer advertises, holds. The same page without the
/// declaration breaks the path rule.
#[test]
fn a_declared_header_is_on_its_path() {
    let chain = main_chain(3);
    let genesis = HeaderFixture::regtest().anchor();
    let invalid = Frontier::new(block::Height(3), block::Hash([9; 32]));
    let round = request(1, invalid.hash, genesis.hash);
    let events = [
        adopted(),
        open(invalid, &[genesis.hash]),
        started(round.clone(), &[genesis.hash]),
        page(
            &round,
            genesis,
            &[chain[0].hash, chain[1].hash, invalid.hash],
        ),
    ];
    assert_eq!(
        breaks_declaring(&[(chain[1].hash, invalid.hash)], &events),
        Vec::<String>::new()
    );
    let found = breaks(&events);
    assert!(
        found
            .iter()
            .any(|found| found.contains("isn't a fixture or declared child")),
        "{found:?}"
    );
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
    assert!(
        skipped[0].contains("isn't a fixture or declared child"),
        "{skipped:?}"
    );
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

/// A grant of subscription 1 adding `objects` headers and `bytes` bytes,
/// with `sequence`, acknowledging `acknowledged` and naming `target`.
fn grant(
    objects: u32,
    bytes: u32,
    sequence: u32,
    acknowledged: Frontier,
    target: block::Hash,
) -> ReactorEvent {
    let update = SubscribeHeaders {
        operation: Operation::Grant,
        subscription_id: 1,
        update_sequence: sequence,
        initial_target_tip_hash: target,
        acknowledged: cursor(acknowledged),
        locator_hashes: Vec::new(),
        added_credit: Credit { objects, bytes },
        tree_aux_schema: AuxSchema::None,
    };
    ReactorEvent::CreditIssued {
        kind: CreditKind::Grant,
        peer: mirror(),
        session_id: 7,
        subscription: 1,
        objects,
        bytes,
        update: Box::new(update),
        target_height: block::Height(4),
    }
}

/// The cursor at `at` of a subscription from genesis that received one
/// plain one-header page per header.
fn cursor(at: Frontier) -> Cursor {
    let headers = u64::from(at.height.0);
    Cursor {
        height: at.height,
        hash: at.hash,
        headers,
        bytes: headers * credit::smallest_page(AuxSchema::None),
    }
}

/// The subscriber's acceptance of subscription 1 through `through`.
fn accepted(through: Frontier) -> ReactorEvent {
    let cursor = cursor(through);
    ReactorEvent::CursorsAccepted {
        peer: mirror(),
        session_id: 7,
        subscription: 1,
        through,
        headers: cursor.headers,
        bytes: cursor.bytes,
    }
}

/// The subscriber's claim of a page of subscription 1 with `headers`
/// headers ending at `through`, one plain one-header page's bytes per
/// header, as [`cursor`] counts.
fn claimed(through: Frontier, headers: u32) -> ReactorEvent {
    let cursor = cursor(through);
    ReactorEvent::CursorClaimed {
        peer: mirror(),
        session_id: 7,
        subscription: 1,
        through,
        headers: cursor.headers,
        bytes: cursor.bytes,
        page_last: through,
        page_headers: u64::from(headers),
        page_bytes: u64::from(headers) * credit::smallest_page(AuxSchema::None),
        charged: (cursor.headers, cursor.bytes),
    }
}

/// `event`, a grant, acknowledging one byte more than its cursor consumed.
fn one_byte_more(mut event: ReactorEvent) -> ReactorEvent {
    if let ReactorEvent::CreditIssued { update, .. } = &mut event {
        update.acknowledged.bytes += 1;
    }
    event
}

/// An `Open` within its reservation holds. One past it, or one whose
/// reservation a cancellation returned first, breaks issuance. So does a
/// byte credit past one smallest page per header.
#[test]
fn credit_stays_within_its_reservation() {
    let chain = main_chain(4);
    let genesis = HeaderFixture::regtest().anchor().hash;
    assert_eq!(
        credit_breaks(&[reserved(1, 10), open(chain[3], &[genesis])]),
        Vec::<String>::new()
    );
    let over = credit_breaks(&[reserved(1, 9), open(chain[3], &[genesis])]);
    assert!(over[0].contains("granted 10 headers"), "{over:?}");
    let cancelled = credit_breaks(&[
        reserved(1, 10),
        ReactorEvent::ReservationEnded {
            reservation: 1,
            released: 10,
            cause: ReservationEnd::Cancelled,
        },
        open(chain[3], &[genesis]),
    ]);
    assert!(cancelled[0].contains("hold 0"), "{cancelled:?}");
    let page = u32::try_from(credit::smallest_page(AuxSchema::None)).expect("small");
    let start = Frontier::new(block::Height(0), genesis);
    let base = || vec![reserved(1, 10), open(chain[3], &[genesis]), reserved(2, 2)];
    let mut fits = base();
    fits.push(grant(2, 2 * page, 1, start, chain[3].hash));
    assert_eq!(credit_breaks(&fits), Vec::<String>::new());
    let mut bytes = base();
    bytes.push(grant(2, 2 * page + 1, 1, start, chain[3].hash));
    let found = credit_breaks(&bytes);
    assert!(found[0].contains("bytes for 2 headers"), "{found:?}");
}

/// A grant or close that names another subscription than the state's or
/// another target, skips a sequence or
/// acknowledges anything but the latest acceptance, down to its credit
/// counts, breaks the update rule.
#[test]
fn updates_carry_their_subscriptions_state() {
    let chain = main_chain(4);
    let genesis = HeaderFixture::regtest().anchor();
    let round = request(1, chain[3].hash, genesis.hash);
    let base = || {
        vec![
            reserved(1, 10),
            adopted(),
            open(chain[3], &[genesis.hash]),
            started(round.clone(), &[genesis.hash]),
            claimed(chain[1], 2),
            page(&round, genesis, &[chain[0].hash, chain[1].hash]),
            accepted(chain[1]),
            reserved(2, 2),
        ]
    };
    let with = |update: ReactorEvent| {
        let mut events = base();
        events.push(update);
        credit_breaks(&events)
    };
    assert_eq!(
        with(grant(2, 0, 1, chain[1], chain[3].hash)),
        Vec::<String>::new()
    );
    for (update, why) in [
        (grant(2, 0, 1, chain[1], chain[2].hash), "names target"),
        (
            grant(2, 0, 2, chain[1], chain[3].hash),
            "has sequence 2, not 1",
        ),
        (grant(2, 0, 1, genesis, chain[3].hash), "acknowledges"),
        (
            one_byte_more(grant(2, 0, 1, chain[1], chain[3].hash)),
            "acknowledges",
        ),
    ] {
        let found = with(update);
        assert!(
            found.iter().any(|found| found.contains(why)),
            "{why}: {found:?}"
        );
    }
    let close = |target: block::Hash| ReactorEvent::CloseBuilt {
        peer: mirror(),
        session_id: 7,
        subscription: 1,
        update: Box::new(SubscribeHeaders {
            operation: Operation::Close,
            subscription_id: 1,
            update_sequence: 1,
            initial_target_tip_hash: target,
            acknowledged: cursor(chain[1]),
            locator_hashes: Vec::new(),
            added_credit: Credit {
                objects: 0,
                bytes: 0,
            },
            tree_aux_schema: AuxSchema::None,
        }),
    };
    assert_eq!(with(close(chain[3].hash)), Vec::<String>::new());
    let mut renamed = close(chain[3].hash);
    if let ReactorEvent::CloseBuilt { update, .. } = &mut renamed {
        update.subscription_id = 2;
    }
    let found = with(renamed);
    assert!(
        found
            .iter()
            .any(|found| found.contains("Close names subscription 2")),
        "{found:?}"
    );
    let found = with(close(chain[2].hash));
    assert!(
        found
            .iter()
            .any(|found| found.contains("Close names target")),
        "{found:?}"
    );
}

/// Acceptance through a staged page's last header holds. Through a cursor
/// no page staged, as a dropped late page's, or below an earlier
/// acceptance, it breaks.
#[test]
fn acceptance_reaches_only_staged_pages() {
    let chain = main_chain(4);
    let genesis = HeaderFixture::regtest().anchor();
    let round = request(1, chain[3].hash, genesis.hash);
    let base = || {
        vec![
            reserved(1, 10),
            adopted(),
            open(chain[3], &[genesis.hash]),
            started(round.clone(), &[genesis.hash]),
            claimed(chain[1], 2),
            page(&round, genesis, &[chain[0].hash, chain[1].hash]),
            claimed(chain[2], 1),
            page(&round, chain[1], &[chain[2].hash]),
        ]
    };
    let with = |through: &[Frontier]| {
        let mut events = base();
        events.extend(through.iter().map(|through| accepted(*through)));
        credit_breaks(&events)
    };
    assert_eq!(with(&[chain[1], chain[2]]), Vec::<String>::new());
    // A late page the subscriber claimed and the reactor dropped unstaged.
    let mut events = base();
    events.extend([claimed(chain[3], 1), accepted(chain[3])]);
    let late = credit_breaks(&events);
    assert!(
        matches!(&late[..], [only] if only.contains("no staged page ends at")),
        "{late:?}"
    );
    let backwards = with(&[chain[2], chain[1]]);
    assert!(backwards[0].contains("below its earlier"), "{backwards:?}");
}

/// The model's smallest plain page is an encoded one-header page's payload.
#[test]
fn the_smallest_page_is_an_encoded_one_header_page() {
    let fixture = HeaderFixture::regtest();
    let header = fixture.extend(fixture.anchor(), 1, 0).remove(0);
    let page = RegtestMessage::Headers(Headers {
        subscription_id: 1,
        initial_target_tip_hash: header.hash(),
        common_ancestor_height: block::Height(0),
        common_ancestor_hash: fixture.anchor().hash,
        reaches_initial_target: true,
        tree_aux_schema: AuxSchema::None,
        entries: vec![HeaderEntry {
            header,
            body_size: 0,
            tree_aux: None,
        }],
    });
    let frame = encode_frame(&page).expect("a one-header page encodes");
    assert_eq!(
        credit::smallest_page(AuxSchema::None),
        u64::try_from(frame.payload.len()).expect("small")
    );
}

/// Credit and acceptance in a stale call's handler are
/// `stale-completion-inert`'s, so an unbacked grant there and an
/// acceptance through a cursor no page staged break nothing here, and a
/// later update acknowledges what the stale handler accepted.
#[test]
fn a_stale_handlers_credit_is_left_to_the_stale_check() {
    let chain = main_chain(4);
    let genesis = HeaderFixture::regtest().anchor();
    let mut check = Authority::new(
        AuthorityEvidence::default(),
        HeaderFixture::regtest(),
        [0],
        [],
    );
    for event in [reserved(1, 10), open(chain[3], &[genesis.hash])] {
        check.observe(&event, true, false);
    }
    for event in [grant(5, 0, 1, genesis, chain[3].hash), accepted(chain[2])] {
        check.observe(&event, true, true);
    }
    for event in [reserved(2, 2), grant(2, 0, 2, chain[2], chain[3].hash)] {
        check.observe(&event, true, false);
    }
    assert_eq!(check.drain(1), Vec::<String>::new());
}

/// A claim whose recorded cursor isn't what its pages consumed, or that
/// ends at another header than its page, breaks, and so does an
/// acceptance of a cursor no claim recorded, though its header was staged.
#[test]
fn claims_record_what_their_pages_consumed() {
    let chain = main_chain(4);
    let genesis = HeaderFixture::regtest().anchor();
    let round = request(1, chain[3].hash, genesis.hash);
    let base = || {
        vec![
            reserved(1, 10),
            adopted(),
            open(chain[3], &[genesis.hash]),
            started(round.clone(), &[genesis.hash]),
        ]
    };
    let mut miscounted = base();
    let mut claim = claimed(chain[1], 2);
    if let ReactorEvent::CursorClaimed { bytes, .. } = &mut claim {
        *bytes += 1;
    }
    miscounted.push(claim);
    let found = credit_breaks(&miscounted);
    assert!(found[0].contains("but its pages consumed"), "{found:?}");
    let mut undercharged = base();
    let mut claim = claimed(chain[1], 2);
    if let ReactorEvent::CursorClaimed { charged, .. } = &mut claim {
        charged.1 -= 1;
    }
    undercharged.push(claim);
    let found = credit_breaks(&undercharged);
    assert!(found[0].contains("counts"), "{found:?}");
    let mut elsewhere = base();
    let mut claim = claimed(chain[1], 2);
    if let ReactorEvent::CursorClaimed { through, .. } = &mut claim {
        *through = chain[0];
    }
    elsewhere.push(claim);
    let found = credit_breaks(&elsewhere);
    assert!(found[0].contains("for a page ending at"), "{found:?}");
    let mut unclaimed = base();
    unclaimed.extend([
        claimed(chain[1], 2),
        page(&round, genesis, &[chain[0].hash, chain[1].hash]),
        one_byte_more_accepted(accepted(chain[1])),
    ]);
    let found = credit_breaks(&unclaimed);
    assert!(found[0].contains("which no claim recorded"), "{found:?}");
}

/// `event`, an acceptance, one byte past its cursor.
fn one_byte_more_accepted(mut event: ReactorEvent) -> ReactorEvent {
    if let ReactorEvent::CursorsAccepted { bytes, .. } = &mut event {
        *bytes += 1;
    }
    event
}

/// A built Close that adds credit or isn't a Close breaks, and so does
/// credit that comes in an update of another operation.
#[test]
fn updates_are_their_operations() {
    let chain = main_chain(4);
    let genesis = HeaderFixture::regtest().anchor();
    let start = Frontier::new(block::Height(0), genesis.hash);
    let base = || vec![reserved(1, 10), open(chain[3], &[genesis.hash])];
    let close = |objects: u32, operation: Operation| ReactorEvent::CloseBuilt {
        peer: mirror(),
        session_id: 7,
        subscription: 1,
        update: Box::new(SubscribeHeaders {
            operation,
            subscription_id: 1,
            update_sequence: 1,
            initial_target_tip_hash: chain[3].hash,
            acknowledged: cursor(start),
            locator_hashes: Vec::new(),
            added_credit: Credit { objects, bytes: 0 },
            tree_aux_schema: AuxSchema::None,
        }),
    };
    for (event, why) in [
        (close(1, Operation::Close), "built Close is a Close adding"),
        (close(0, Operation::Grant), "built Close is a Grant"),
    ] {
        let mut events = base();
        events.push(event);
        let found = credit_breaks(&events);
        assert!(
            found.iter().any(|found| found.contains(why)),
            "{why}: {found:?}"
        );
    }
    let mut mislabeled = grant(2, 0, 1, start, chain[3].hash);
    if let ReactorEvent::CreditIssued { update, .. } = &mut mislabeled {
        update.operation = Operation::Close;
    }
    let mut events = base();
    events.extend([reserved(2, 2), mislabeled]);
    let found = credit_breaks(&events);
    assert!(
        found
            .iter()
            .any(|found| found.contains("Grant credit came in a Close update")),
        "{found:?}"
    );
}
