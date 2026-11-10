//! The authority check's rules, fed by hand on the Regtest fixture.

use std::num::NonZeroU64;

use zakura_chain::{block, work::difficulty::U256};
use zakura_header_chain::{
    BranchId, HeaderGeneration, HeaderSyncWorkOwner, HeaderWorkAuthority, StateVersion,
    VerifiedGeneration,
};
use zakura_network::zakura::{
    observe::{CreditTotals, RequestRef, ReservationEnd, ReservationPurpose},
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
        totals: Default::default(),
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

/// `events` with each credit fact's primitive totals filled in as an
/// honest primitive counts them: every update's credit and every claimed
/// page's, per subscription.
fn honest(events: &[ReactorEvent]) -> Vec<ReactorEvent> {
    let mut counted: std::collections::HashMap<(u64, u64), CreditTotals> = Default::default();
    events
        .iter()
        .cloned()
        .map(|mut event| {
            match &mut event {
                ReactorEvent::CreditIssued {
                    session_id,
                    subscription,
                    update,
                    totals,
                    ..
                } => {
                    let count = counted.entry((*session_id, *subscription)).or_default();
                    count.granted_headers += u64::from(update.added_credit.objects);
                    count.granted_bytes += u64::from(update.added_credit.bytes);
                    *totals = *count;
                }
                ReactorEvent::CursorClaimed {
                    session_id,
                    subscription,
                    page_headers,
                    page_bytes,
                    totals,
                    ..
                } => {
                    let count = counted.entry((*session_id, *subscription)).or_default();
                    count.consumed_headers += *page_headers;
                    count.consumed_bytes += *page_bytes;
                    *totals = *count;
                }
                ReactorEvent::CloseBuilt {
                    session_id,
                    subscription,
                    totals,
                    ..
                }
                | ReactorEvent::CursorsAccepted {
                    session_id,
                    subscription,
                    totals,
                    ..
                } => *totals = *counted.entry((*session_id, *subscription)).or_default(),
                _ => {}
            }
            event
        })
        .collect()
}

/// Notes, for each Open in `events`, that a locator read gave its first
/// locator where the Open says it starts, as a test's storage would.
fn read_starts(check: &mut Authority, events: &[ReactorEvent]) {
    for event in events {
        if let ReactorEvent::CreditIssued {
            kind: CreditKind::Open,
            update,
            ..
        } = event
        {
            if let Some(first) = update.locator_hashes.first() {
                check.note_read(*first, update.acknowledged.height);
            }
        }
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
    read_starts(&mut check, events);
    for event in &honest(events) {
        check.observe(event, true, false);
    }
    check.drain(1)
}

/// The breaks `events` give with no reservation fed first, their totals
/// filled in honestly.
fn credit_breaks(events: &[ReactorEvent]) -> Vec<String> {
    raw_credit_breaks(&honest(events))
}

/// [`credit_breaks`], with `events`' totals as given.
fn raw_credit_breaks(events: &[ReactorEvent]) -> Vec<String> {
    let mut check = Authority::new(
        AuthorityEvidence::default(),
        HeaderFixture::regtest(),
        [0],
        [],
    );
    read_starts(&mut check, events);
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
        read_starts(&mut check, &events);
        for event in &honest(&events) {
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
        totals: Default::default(),
    }
}

/// The cursor at `at` of a subscription from genesis whose pages were the
/// tests' split: headers 1 and 2 in one plain page, then one per header.
fn cursor(at: Frontier) -> Cursor {
    let headers = u64::from(at.height.0);
    let bytes = match headers {
        0 => 0,
        1 | 2 => credit::page_size(headers, AuxSchema::None),
        _ => {
            credit::page_size(2, AuxSchema::None)
                + (headers - 2) * credit::smallest_page(AuxSchema::None)
        }
    };
    Cursor {
        height: at.height,
        hash: at.hash,
        headers,
        bytes,
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
        stored: (through, cursor.headers, cursor.bytes),
        totals: Default::default(),
    }
}

/// `round`'s apply as call `call`, then `events` in that call's completion,
/// as the reactor handles an apply's result.
fn in_apply(round: &RequestRef, call: u64, events: Vec<ReactorEvent>) -> Vec<ReactorEvent> {
    use zakura_network::zakura::observe::{PortCall, ReactorCause};

    let mut handled = vec![
        ReactorEvent::Dispatched {
            call,
            op: PortCall::Apply {
                peer: mirror(),
                source: zakura_header_chain::SourceId::from_digest([2; 32]),
                owner: round.owner,
                target_tip_hash: round.target_tip_hash,
            },
        },
        ReactorEvent::CauseSet {
            cause: ReactorCause::Completion { call },
        },
    ];
    handled.extend(events);
    handled.push(ReactorEvent::CauseSet {
        cause: ReactorCause::Idle,
    });
    handled
}

/// Storage's apply operation `id` for `round`'s apply call `call`, as
/// [`in_apply`] dispatches it, completing with `result`.
fn completed(
    round: &RequestRef,
    id: u64,
    call: u64,
    result: crate::observe::StorageResult,
) -> [StorageFact; 2] {
    let source = zakura_header_chain::SourceId::from_digest([2; 32]);
    [
        StorageFact::Submitted {
            id,
            request: crate::observe::StorageRequest::ApplyHeaderTarget {
                owner: crate::storage::work_owner(round.owner),
                source: crate::storage::hex(&source.digest()),
                parent: None,
                target: crate::storage::hash(round.target_tip_hash),
                hashes: None,
            },
            digest: String::new(),
            call: Some(call),
        },
        StorageFact::Completed {
            id,
            outcome: "engine".to_owned(),
            result,
            delivered: true,
            generation: 0,
            state: String::new(),
        },
    ]
}

/// The subscriber's claim of a plain page of subscription 1 with `headers`
/// headers ending at `through`, as [`cursor`] counts it.
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
        page_bytes: credit::page_size(u64::from(headers), AuxSchema::None),
        totals: Default::default(),
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
        let mut events = vec![
            reserved(1, 10),
            adopted(),
            open(chain[3], &[genesis.hash]),
            started(round.clone(), &[genesis.hash]),
            claimed(chain[1], 2),
            page(&round, genesis, &[chain[0].hash, chain[1].hash]),
        ];
        events.extend(in_apply(&round, 9, vec![accepted(chain[1])]));
        events.push(reserved(2, 2));
        events
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
        totals: Default::default(),
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

/// Acceptance through the last page a round staged before its apply, in
/// that apply's completion, holds once the apply's storage operation
/// completed `Applied`. Through a cursor no page staged, as a dropped late
/// page's, through an earlier page of the round, outside an apply's
/// completion, in another subscription's apply, after a failed apply, or
/// below an earlier acceptance, it breaks.
#[test]
fn acceptance_reaches_only_the_applied_rounds_last_page() {
    use crate::observe::StorageResult;

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
    // The credit half's breaks for `events`, then storage's `result` for
    // apply call 9. Whether the apply reached storage faithfully is the
    // submission half's.
    let judged = |events: Vec<ReactorEvent>, result: Option<StorageResult>| {
        let mut credit = credit::Credit::default();
        let events = honest(&events);
        for event in &events {
            if let ReactorEvent::CreditIssued {
                kind: CreditKind::Open,
                update,
                ..
            } = event
            {
                if let Some(first) = update.locator_hashes.first() {
                    credit.note_read(*first, update.acknowledged.height);
                }
            }
            credit.observe(event, true, false);
        }
        if let Some(result) = result {
            for fact in &completed(&round, 4, 9, result) {
                credit.storage(fact);
            }
        }
        credit.take_breaks()
    };
    let applied = |through: Frontier| {
        let mut events = base();
        events.extend(in_apply(&round, 9, vec![accepted(through)]));
        events
    };
    assert_eq!(
        judged(applied(chain[2]), Some(StorageResult::Applied)),
        Vec::<String>::new()
    );
    // Storage's result may reach the check a drain late, but an apply
    // that reported none breaks at the drain after.
    let mut credit = credit::Credit::default();
    for event in &honest(&applied(chain[2])) {
        if let ReactorEvent::CreditIssued {
            kind: CreditKind::Open,
            update,
            ..
        } = event
        {
            if let Some(first) = update.locator_hashes.first() {
                credit.note_read(*first, update.acknowledged.height);
            }
        }
        credit.observe(event, true, false);
    }
    assert_eq!(credit.take_breaks(), Vec::<String>::new());
    let unreported = credit.take_breaks();
    assert!(
        matches!(&unreported[..], [only] if only.contains("reported no storage result")),
        "{unreported:?}"
    );
    let failed = judged(
        applied(chain[2]),
        Some(StorageResult::ApplyFailed {
            category: "invalid_header".to_owned(),
        }),
    );
    assert!(
        matches!(&failed[..], [only] if only.contains("whose apply didn't succeed")),
        "{failed:?}"
    );
    let earlier = judged(applied(chain[1]), Some(StorageResult::Applied));
    assert!(
        matches!(&earlier[..], [only] if only.contains("not its round's last page")),
        "{earlier:?}"
    );
    let mut outside = base();
    outside.push(accepted(chain[2]));
    let outside = judged(outside, None);
    assert!(
        matches!(&outside[..], [only] if only.contains("outside an apply call's completion")),
        "{outside:?}"
    );
    let other = RequestRef {
        session_id: 8,
        owner: HeaderSyncWorkOwner::Header(
            round
                .owner
                .header_authority()
                .bind(8, NonZeroU64::new(1).expect("nonzero")),
        ),
        ..round.clone()
    };
    let mut elsewhere = base();
    elsewhere.extend(in_apply(&other, 9, vec![accepted(chain[2])]));
    let elsewhere = judged(elsewhere, Some(StorageResult::Applied));
    assert!(
        matches!(&elsewhere[..], [only] if only.contains("an apply for")),
        "{elsewhere:?}"
    );
    // A late page the subscriber claimed after the apply's dispatch and
    // the reactor dropped unstaged.
    let mut late = base();
    let mut handled = in_apply(&round, 9, vec![accepted(chain[3])]);
    handled.insert(1, claimed(chain[3], 1));
    late.extend(handled);
    let late = judged(late, Some(StorageResult::Applied));
    assert!(
        late.iter()
            .any(|found| found.contains("no staged page ends at")),
        "{late:?}"
    );
    let mut backwards = base();
    backwards.extend(in_apply(&round, 9, vec![accepted(chain[2])]));
    backwards.extend(in_apply(&round, 10, vec![accepted(chain[1])]));
    let backwards = judged(backwards, Some(StorageResult::Applied));
    assert!(
        backwards
            .iter()
            .any(|found| found.contains("below its earlier")),
        "{backwards:?}"
    );
}

/// The model's plain page sizes are encoded pages' payloads, of one header
/// and of several.
#[test]
fn page_sizes_are_encoded_pages() {
    let fixture = HeaderFixture::regtest();
    for count in [1, 3] {
        let headers = fixture.extend(fixture.anchor(), count, 0);
        let page = RegtestMessage::Headers(Headers {
            subscription_id: 1,
            initial_target_tip_hash: headers.last().expect("one at least").hash(),
            common_ancestor_height: block::Height(0),
            common_ancestor_hash: fixture.anchor().hash,
            reaches_initial_target: true,
            tree_aux_schema: AuxSchema::None,
            entries: headers
                .into_iter()
                .map(|header| HeaderEntry {
                    header,
                    body_size: 0,
                    tree_aux: None,
                })
                .collect(),
        });
        let frame = encode_frame(&page).expect("a page encodes");
        assert_eq!(
            credit::page_size(u64::from(count), AuxSchema::None),
            u64::try_from(frame.payload.len()).expect("small"),
            "{count} headers"
        );
    }
    assert_eq!(
        credit::smallest_page(AuxSchema::None),
        credit::page_size(1, AuxSchema::None)
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
    let events = honest(&[
        reserved(1, 10),
        open(chain[3], &[genesis.hash]),
        grant(5, 0, 1, genesis, chain[3].hash),
        accepted(chain[2]),
        reserved(2, 2),
        grant(2, 0, 2, chain[2], chain[3].hash),
    ]);
    read_starts(&mut check, &events);
    for (at, event) in events.iter().enumerate() {
        // The middle grant and acceptance come in a stale call's handler.
        check.observe(event, true, (2..4).contains(&at));
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
    // The primitive charged a byte less than the page's layout takes, and
    // the cursor and totals agree with what it charged.
    let mut short = base();
    let mut claim = claimed(chain[1], 2);
    if let ReactorEvent::CursorClaimed {
        bytes, page_bytes, ..
    } = &mut claim
    {
        *bytes -= 1;
        *page_bytes -= 1;
    }
    short.push(claim);
    let found = credit_breaks(&short);
    assert!(found[0].contains("which takes"), "{found:?}");
    let mut undercharged = base();
    undercharged.push(claimed(chain[1], 2));
    let mut undercharged = honest(&undercharged);
    if let Some(ReactorEvent::CursorClaimed { totals, .. }) = undercharged.last_mut() {
        totals.consumed_bytes -= 1;
    }
    let found = raw_credit_breaks(&undercharged);
    assert!(found[0].contains("primitive counts"), "{found:?}");
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

/// Pages that consume more than the updates granted break, though every
/// cursor and total agrees with them. An acceptance whose stored cursor
/// isn't the one asked for breaks too.
#[test]
fn pages_stay_within_their_credit_and_acceptance_stores_its_cursor() {
    let chain = main_chain(4);
    let genesis = HeaderFixture::regtest().anchor();
    let round = request(1, chain[3].hash, genesis.hash);
    // An Open granting exactly one two-header page.
    let mut small = open(chain[3], &[genesis.hash]);
    let page_bytes = u32::try_from(credit::page_size(2, AuxSchema::None)).expect("small");
    if let ReactorEvent::CreditIssued {
        objects,
        bytes,
        update,
        ..
    } = &mut small
    {
        (*objects, *bytes) = (2, page_bytes);
        update.added_credit = Credit {
            objects: 2,
            bytes: page_bytes,
        };
    }
    let base = || {
        vec![
            reserved(1, 10),
            adopted(),
            small.clone(),
            started(round.clone(), &[genesis.hash]),
        ]
    };
    let mut within = base();
    within.extend([
        claimed(chain[1], 2),
        page(&round, genesis, &[chain[0].hash, chain[1].hash]),
    ]);
    assert_eq!(credit_breaks(&within), Vec::<String>::new());
    let mut over = base();
    over.extend([claimed(chain[1], 2), claimed(chain[2], 1)]);
    let found = credit_breaks(&over);
    assert!(
        found
            .iter()
            .any(|found| found.contains("its updates granted")),
        "{found:?}"
    );
    let mut stored_elsewhere = within;
    let mut accepted = accepted(chain[1]);
    if let ReactorEvent::CursorsAccepted { stored, .. } = &mut accepted {
        stored.0 = chain[0];
    }
    stored_elsewhere.push(accepted);
    let found = credit_breaks(&stored_elsewhere);
    assert!(
        found.iter().any(|found| found.contains("not the")),
        "{found:?}"
    );
}

/// `event`, an acceptance, one byte past its cursor, as asked for and as
/// stored.
fn one_byte_more_accepted(mut event: ReactorEvent) -> ReactorEvent {
    if let ReactorEvent::CursorsAccepted { bytes, stored, .. } = &mut event {
        *bytes += 1;
        stored.2 += 1;
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
        totals: Default::default(),
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

/// An Open that starts with something consumed or at a sequence other than
/// 0 breaks, and so do primitive totals that count more credit than the
/// built updates granted.
#[test]
fn the_primitive_counts_what_the_updates_and_pages_did() {
    let chain = main_chain(4);
    let genesis = HeaderFixture::regtest().anchor();
    let mut started = open(chain[3], &[genesis.hash]);
    if let ReactorEvent::CreditIssued { update, .. } = &mut started {
        update.acknowledged.headers = 1;
    }
    let found = credit_breaks(&[reserved(1, 10), started]);
    assert!(
        found.iter().any(|found| found.contains("Open starts at")),
        "{found:?}"
    );
    let mut sequenced = open(chain[3], &[genesis.hash]);
    if let ReactorEvent::CreditIssued { update, .. } = &mut sequenced {
        update.update_sequence = 1;
    }
    let found = credit_breaks(&[reserved(1, 10), sequenced]);
    assert!(
        found
            .iter()
            .any(|found| found.contains("sequence 1, not 0")),
        "{found:?}"
    );
    let mut stored = honest(&[reserved(1, 10), open(chain[3], &[genesis.hash])]);
    if let Some(ReactorEvent::CreditIssued { totals, .. }) = stored.last_mut() {
        totals.granted_headers += 1;
    }
    let found = raw_credit_breaks(&stored);
    assert!(
        found.iter().any(|found| found.contains("primitive counts")),
        "{found:?}"
    );
}

/// An Open whose start height isn't the one storage gave its first locator
/// breaks, and so does one whose first locator no read gave.
#[test]
fn an_open_starts_where_storage_put_its_first_locator() {
    let chain = main_chain(4);
    let genesis = HeaderFixture::regtest().anchor();
    let open = open(chain[3], &[genesis.hash]);
    let with = |read: Option<block::Height>| {
        let mut check = Authority::new(
            AuthorityEvidence::default(),
            HeaderFixture::regtest(),
            [0],
            [],
        );
        if let Some(height) = read {
            check.note_read(genesis.hash, height);
        }
        for event in &honest(&[reserved(1, 10), open.clone()]) {
            check.observe(event, true, false);
        }
        check.drain(1)
    };
    assert_eq!(with(Some(block::Height(0))), Vec::<String>::new());
    for read in [Some(block::Height(1)), None] {
        let found = with(read);
        assert!(
            found.iter().any(|found| found.contains("Open starts at")),
            "{read:?}: {found:?}"
        );
    }
}

/// A dispatched prepare whose submission is another kind of request breaks
/// the submission rule at once, with its reply still pending.
#[test]
fn a_submission_of_another_kind_breaks_at_once() {
    use zakura_network::zakura::observe::PortCall;

    let chain = main_chain(2);
    let genesis = HeaderFixture::regtest().anchor();
    let round = request(1, chain[1].hash, genesis.hash);
    let mut check = Authority::new(
        AuthorityEvidence::default(),
        HeaderFixture::regtest(),
        [0],
        [],
    );
    check.observe(
        &ReactorEvent::Dispatched {
            call: 1,
            op: PortCall::Prepare {
                peer: mirror(),
                source: zakura_header_chain::SourceId::from_digest([2; 32]),
                owner: round.owner,
                common_ancestor: genesis,
                target: chain[1],
                hashes: vec![chain[0].hash, chain[1].hash],
            },
        },
        true,
        false,
    );
    let locator = crate::observe::StorageFact::Submitted {
        id: 1,
        request: crate::observe::StorageRequest::ContinuationLocator,
        digest: String::new(),
        call: Some(1),
    };
    check.storage([&locator]);
    let found = check.drain(1);
    assert!(
        found
            .iter()
            .any(|found| found.contains("not its dispatch's prepare")),
        "{found:?}"
    );
}

/// A prepare call that reaches storage a second time breaks the submission
/// rule at once, though its first request matched its dispatch.
#[test]
fn a_second_submission_from_one_call_breaks_at_once() {
    use zakura_network::zakura::observe::PortCall;

    let chain = main_chain(2);
    let genesis = HeaderFixture::regtest().anchor();
    let round = request(1, chain[1].hash, genesis.hash);
    let mut check = Authority::new(
        AuthorityEvidence::default(),
        HeaderFixture::regtest(),
        [0],
        [],
    );
    let source = zakura_header_chain::SourceId::from_digest([2; 32]);
    check.observe(
        &ReactorEvent::Dispatched {
            call: 1,
            op: PortCall::Prepare {
                peer: mirror(),
                source,
                owner: round.owner,
                common_ancestor: genesis,
                target: chain[1],
                hashes: vec![chain[0].hash, chain[1].hash],
            },
        },
        true,
        false,
    );
    let prepare = |id| crate::observe::StorageFact::Submitted {
        id,
        request: crate::observe::StorageRequest::PrepareHeaderTarget {
            owner: crate::storage::work_owner(round.owner),
            source: crate::storage::hex(&source.digest()),
            common_ancestor: crate::storage::point(genesis),
            target: crate::storage::point(chain[1]),
            headers: 2,
            work: None,
            first: None,
            hashes: vec![
                crate::storage::hash(chain[0].hash),
                crate::storage::hash(chain[1].hash),
            ],
        },
        digest: String::new(),
        call: Some(1),
    };
    check.storage([&prepare(1)]);
    assert_eq!(check.drain(1), Vec::<String>::new());
    check.storage([&prepare(2)]);
    let found = check.drain(2);
    assert!(
        found
            .iter()
            .any(|found| found.contains("reached storage again")),
        "{found:?}"
    );
}

/// An apply's owner, source and target are judged against its dispatch
/// whether or not a prepare handed it headers. Without a handoff it breaks
/// the submission rule at once, with its fields also compared. With one,
/// a matching sealed insertion holds.
#[test]
fn an_apply_is_judged_with_or_without_a_handoff() {
    use zakura_network::zakura::observe::{HeaderWorkEnd, PortCall};

    let chain = main_chain(2);
    let genesis = HeaderFixture::regtest().anchor();
    let round = request(1, chain[1].hash, genesis.hash);
    let source = zakura_header_chain::SourceId::from_digest([2; 32]);
    let dispatch = |check: &mut Authority| {
        for (call, op) in [
            (
                1,
                PortCall::Prepare {
                    peer: mirror(),
                    source,
                    owner: round.owner,
                    common_ancestor: genesis,
                    target: chain[1],
                    hashes: vec![chain[0].hash, chain[1].hash],
                },
            ),
            (
                2,
                PortCall::Apply {
                    peer: mirror(),
                    source,
                    owner: round.owner,
                    target_tip_hash: chain[1].hash,
                },
            ),
        ] {
            check.observe(&ReactorEvent::Dispatched { call, op }, true, false);
        }
    };
    let apply = |source: zakura_header_chain::SourceId| crate::observe::StorageFact::Submitted {
        id: 2,
        request: crate::observe::StorageRequest::ApplyHeaderTarget {
            owner: crate::storage::work_owner(round.owner),
            source: crate::storage::hex(&source.digest()),
            parent: Some(crate::storage::hash(genesis.hash)),
            target: crate::storage::hash(chain[1].hash),
            hashes: Some(vec![
                crate::storage::hash(chain[0].hash),
                crate::storage::hash(chain[1].hash),
            ]),
        },
        digest: String::new(),
        call: Some(2),
    };
    let new_check = || {
        Authority::new(
            AuthorityEvidence::default(),
            HeaderFixture::regtest(),
            [0],
            [],
        )
    };

    let mut unhanded = new_check();
    dispatch(&mut unhanded);
    unhanded.storage([&apply(source)]);
    let found = unhanded.drain(1);
    assert!(
        matches!(&found[..], [only] if only.contains("no prepare handed the apply")),
        "{found:?}"
    );

    let mut other_source = new_check();
    dispatch(&mut other_source);
    other_source.storage([&apply(zakura_header_chain::SourceId::from_digest([3; 32]))]);
    let found = other_source.drain(1);
    assert!(
        found.iter().any(|found| found.contains("source"))
            && found
                .iter()
                .any(|found| found.contains("no prepare handed the apply")),
        "{found:?}"
    );

    let mut handed = new_check();
    dispatch(&mut handed);
    handed.observe(
        &ReactorEvent::HeadersEnded {
            call: 1,
            incarnation: 1,
            headers: 2,
            end: HeaderWorkEnd::HandedToApply { apply: 2 },
        },
        true,
        false,
    );
    handed.storage([&apply(source)]);
    assert_eq!(handed.drain(1), Vec::<String>::new());
}

/// The model's V1 page sizes are encoded V1 pages' payloads, each entry
/// carrying its auxiliary record, so a wrong auxiliary byte count fails
/// here.
#[test]
fn v1_page_sizes_are_encoded_pages() {
    use zakura_chain::{block::merkle::AuthDataRoot, ironwood, orchard, sapling};
    use zakura_header_chain::TreeAuxRecordV1;

    let fixture = HeaderFixture::regtest();
    for count in [1, 3] {
        let headers = fixture.extend(fixture.anchor(), count, 0);
        let page = RegtestMessage::Headers(Headers {
            subscription_id: 1,
            initial_target_tip_hash: headers.last().expect("one at least").hash(),
            common_ancestor_height: block::Height(0),
            common_ancestor_hash: fixture.anchor().hash,
            reaches_initial_target: true,
            tree_aux_schema: AuxSchema::V1,
            entries: (1..)
                .zip(headers)
                .map(|(height, header)| HeaderEntry {
                    header,
                    body_size: 0,
                    tree_aux: Some(TreeAuxRecordV1 {
                        height: block::Height(height),
                        sapling_root: sapling::tree::Root::default(),
                        orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
                        ironwood_root: ironwood::tree::NoteCommitmentTree::default().root(),
                        sapling_tx_count: 0,
                        orchard_tx_count: 0,
                        ironwood_tx_count: 0,
                        auth_data_root: AuthDataRoot::from([0; 32]),
                    }),
                })
                .collect(),
        });
        let frame = encode_frame(&page).expect("a V1 page encodes");
        assert_eq!(
            credit::page_size(u64::from(count), AuxSchema::V1),
            u64::try_from(frame.payload.len()).expect("small"),
            "{count} headers"
        );
    }
    assert!(credit::smallest_page(AuxSchema::V1) > credit::smallest_page(AuxSchema::None));
}

/// Under a V1 Open, a claimed page is charged its V1 size: one charged as
/// a plain page breaks, with its cursor and totals agreeing.
#[test]
fn a_v1_page_is_charged_its_auxiliary_records() {
    let chain = main_chain(4);
    let genesis = HeaderFixture::regtest().anchor();
    let mut v1 = open(chain[3], &[genesis.hash]);
    let granted = u32::try_from(credit::page_size(2, AuxSchema::V1)).expect("small");
    if let ReactorEvent::CreditIssued {
        objects,
        bytes,
        update,
        ..
    } = &mut v1
    {
        (*objects, *bytes) = (2, granted);
        update.tree_aux_schema = AuxSchema::V1;
        update.added_credit = Credit {
            objects: 2,
            bytes: granted,
        };
    }
    let claim = |charged: u64| ReactorEvent::CursorClaimed {
        peer: mirror(),
        session_id: 7,
        subscription: 1,
        through: chain[1],
        headers: 2,
        bytes: charged,
        page_last: chain[1],
        page_headers: 2,
        page_bytes: charged,
        totals: Default::default(),
    };
    let with = |charged| credit_breaks(&[reserved(1, 10), v1.clone(), claim(charged)]);
    assert_eq!(
        with(credit::page_size(2, AuxSchema::V1)),
        Vec::<String>::new()
    );
    let found = with(credit::page_size(2, AuxSchema::None));
    assert!(
        found.iter().any(|found| found.contains("which takes")),
        "{found:?}"
    );
}
