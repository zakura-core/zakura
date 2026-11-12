//! The authority check's rules, fed by hand on the Regtest fixture.

use std::num::NonZeroU64;

use zakura_chain::{block, work::difficulty::U256};
use zakura_header_chain::{
    BranchId, HeaderGeneration, HeaderSyncWorkOwner, HeaderWorkAuthority, SourceId, StateVersion,
    VerifiedGeneration,
};
use zakura_network::zakura::{
    observe::{
        ClaimBreak, ClaimRule, ClaimSubject, CreditTotals, HeaderWorkEnd, HeaderWorkLedger,
        PortCall, RequestRef, ReservationEnd, ReservationPurpose, StorageCallKind,
    },
    v9::{Cursor, Headers, Operation, RegtestMessage, SubscribeHeaders},
    wire_codec::encode_frame,
    AuxSchema, Credit, HeaderEntry,
};

use super::finding::ledger_attribution;
use super::*;
use crate::domain::header_sync::AUTHORITY_CLAIMS;
use crate::harness::header_node::{
    claim::{Claim, ClaimEvidence, ClaimRecord},
    tap::peer,
};

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
/// page's, per subscription, by peer, session and ID.
fn honest(events: &[ReactorEvent]) -> Vec<ReactorEvent> {
    let mut counted: std::collections::HashMap<Subscription, CreditTotals> = Default::default();
    events
        .iter()
        .cloned()
        .map(|mut event| {
            match &mut event {
                ReactorEvent::CreditIssued {
                    peer,
                    session_id,
                    subscription,
                    update,
                    totals,
                    ..
                } => {
                    let count = counted
                        .entry((peer.clone(), *session_id, *subscription))
                        .or_default();
                    count.granted_headers += u64::from(update.added_credit.objects);
                    count.granted_bytes += u64::from(update.added_credit.bytes);
                    *totals = *count;
                }
                ReactorEvent::CursorClaimed {
                    peer,
                    session_id,
                    subscription,
                    page_headers,
                    page_bytes,
                    totals,
                    ..
                } => {
                    let count = counted
                        .entry((peer.clone(), *session_id, *subscription))
                        .or_default();
                    count.consumed_headers += *page_headers;
                    count.consumed_bytes += *page_bytes;
                    *totals = *count;
                }
                ReactorEvent::CloseBuilt {
                    peer,
                    session_id,
                    subscription,
                    totals,
                    ..
                }
                | ReactorEvent::CursorsAccepted {
                    peer,
                    session_id,
                    subscription,
                    totals,
                    ..
                } => {
                    *totals = *counted
                        .entry((peer.clone(), *session_id, *subscription))
                        .or_default();
                }
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

/// `findings` as their claim records' details render them.
fn rendered(findings: Vec<AuthorityFinding>) -> Vec<String> {
    findings.iter().map(ToString::to_string).collect()
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
        AuthorityEvidence::new(&EvidenceStores::default()),
        HeaderFixture::regtest(),
        [0, 1, 2],
        declared.iter().copied(),
    );
    check.observe(&reserved(99, 10), true, false);
    read_starts(&mut check, events);
    for event in &honest(events) {
        check.observe(event, true, false);
    }
    rendered(check.drain(1))
}

/// The breaks `events` give with no reservation fed first, their totals
/// filled in honestly.
fn credit_breaks(events: &[ReactorEvent]) -> Vec<String> {
    raw_credit_breaks(&honest(events))
}

/// [`credit_breaks`], with `events`' totals as given. Each apply call
/// they dispatch completes `Applied` in storage, as the credit half reads
/// it.
fn raw_credit_breaks(events: &[ReactorEvent]) -> Vec<String> {
    let mut check = Authority::new(
        AuthorityEvidence::new(&EvidenceStores::default()),
        HeaderFixture::regtest(),
        [0],
        [],
    );
    read_starts(&mut check, events);
    for event in events {
        if let ReactorEvent::Dispatched {
            call,
            op: zakura_network::zakura::observe::PortCall::Apply { .. },
        } = event
        {
            for fact in &applied_in_storage(*call) {
                check.credit.storage(fact);
            }
        }
        check.observe(event, true, false);
    }
    rendered(check.drain(1))
}

/// Storage's operation for reactor call `call`, completing `Applied`, as
/// the credit half reads it.
fn applied_in_storage(call: u64) -> [StorageFact; 2] {
    [
        StorageFact::Submitted {
            id: 1_000 + call,
            request: crate::observe::StorageRequest::SelectedChainContains {
                hash: String::new(),
            },
            digest: String::new(),
            call: Some(call),
        },
        StorageFact::Completed {
            id: 1_000 + call,
            outcome: "engine".to_owned(),
            result: crate::observe::StorageResult::Applied,
            delivered: true,
            generation: 0,
            state: String::new(),
        },
    ]
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
            AuthorityEvidence::new(&EvidenceStores::default()),
            HeaderFixture::regtest(),
            branches.iter().copied(),
            [],
        );
        check.observe(&reserved(99, 10), true, false);
        read_starts(&mut check, &events);
        for event in &honest(&events) {
            check.observe(event, true, false);
        }
        rendered(check.drain(1))
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
/// completion, off the reactor's task, in another subscription's apply,
/// after a failed apply, or below an earlier acceptance, it breaks. A
/// subscription resumed in a later round accepts that round's last page.
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
    // The credit half's breaks for `events`, with acceptances reported on
    // the reactor's task if `on_reactor`, then storage's `result` for apply
    // calls 9 and 10. Whether the apply reached storage faithfully is the
    // submission half's.
    let judge = |events: Vec<ReactorEvent>, result: Option<StorageResult>, on_reactor: bool| {
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
            let reactor = on_reactor || !matches!(event, ReactorEvent::CursorsAccepted { .. });
            credit.observe(event, reactor, false);
        }
        if let Some(result) = result {
            for (id, call) in [(4, 9), (5, 10)] {
                for fact in &completed(&round, id, call, result.clone()) {
                    credit.storage(fact);
                }
            }
        }
        rendered(credit.take_breaks())
    };
    let judged =
        |events: Vec<ReactorEvent>, result: Option<StorageResult>| judge(events, result, true);
    let applied = |through: Frontier| {
        let mut events = base();
        events.extend(in_apply(&round, 9, vec![accepted(through)]));
        events
    };
    assert_eq!(
        judged(applied(chain[2]), Some(StorageResult::Applied)),
        Vec::<String>::new()
    );
    // Storage's facts reach the check before the drain that sees the
    // acceptance, so an apply that reported none by then breaks there.
    let unreported = judged(applied(chain[2]), None);
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
    // Another task's acceptance doesn't borrow the reactor's completion.
    let off = judge(applied(chain[2]), None, false);
    assert!(
        matches!(&off[..], [only] if only.contains("off the reactor's task")),
        "{off:?}"
    );
    // A subscription resumed in a later round accepts that round's last
    // page, not the earlier round's.
    let resumed = |through: Frontier| {
        let mut events = base();
        events.truncate(events.len() - 2);
        events.extend(in_apply(&round, 9, vec![accepted(chain[1])]));
        events.push(reserved(2, 2));
        events.push(grant(2, 0, 1, chain[1], chain[3].hash));
        events.push(claimed(chain[2], 1));
        events.push(page(&round, chain[1], &[chain[2].hash]));
        events.extend(in_apply(&round, 10, vec![accepted(through)]));
        events
    };
    assert_eq!(
        judged(resumed(chain[2]), Some(StorageResult::Applied)),
        Vec::<String>::new()
    );
    let stale = judged(resumed(chain[1]), Some(StorageResult::Applied));
    assert!(
        stale
            .iter()
            .any(|found| found.contains("not its round's last page")),
        "{stale:?}"
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
        AuthorityEvidence::new(&EvidenceStores::default()),
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
    assert_eq!(rendered(check.drain(1)), Vec::<String>::new());
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
            AuthorityEvidence::new(&EvidenceStores::default()),
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
        rendered(check.drain(1))
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
        AuthorityEvidence::new(&EvidenceStores::default()),
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
    let found = rendered(check.drain(1));
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
        AuthorityEvidence::new(&EvidenceStores::default()),
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
    assert_eq!(rendered(check.drain(1)), Vec::<String>::new());
    check.storage([&prepare(2)]);
    let found = rendered(check.drain(2));
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
            AuthorityEvidence::new(&EvidenceStores::default()),
            HeaderFixture::regtest(),
            [0],
            [],
        )
    };

    let mut unhanded = new_check();
    dispatch(&mut unhanded);
    unhanded.storage([&apply(source)]);
    let found = rendered(unhanded.drain(1));
    assert!(
        matches!(&found[..], [only] if only.contains("no prepare handed the apply")),
        "{found:?}"
    );

    let mut other_source = new_check();
    dispatch(&mut other_source);
    other_source.storage([&apply(zakura_header_chain::SourceId::from_digest([3; 32]))]);
    let found = rendered(other_source.drain(1));
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
    assert_eq!(rendered(handed.drain(1)), Vec::<String>::new());
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

/// A drain's applicability noted after the node's stores froze is dropped.
#[test]
fn a_frozen_authority_store_drops_its_applicability() {
    crate::stores::assert_freeze_drops_writes(
        AuthorityEvidence::new,
        |evidence| {
            evidence.change(|store| store.applicable = Some(1));
        },
        AuthorityEvidence::any,
    );
}

/// A second mirror's peer ID.
fn other_mirror() -> ZakuraPeerId {
    ZakuraPeerId::new(vec![3; 32]).expect("32 bytes is a valid peer ID")
}

/// `peer`'s reservation `reservation` of `headers` headers.
fn reserved_by(peer: &ZakuraPeerId, reservation: u64, headers: usize) -> ReactorEvent {
    let mut event = reserved(reservation, headers);
    if let ReactorEvent::ReservationOpened { peer: by, .. } = &mut event {
        *by = peer.clone();
    }
    event
}

/// [`open`], for subscription `subscription` on `peer`'s session `session`.
fn open_on(
    peer: &ZakuraPeerId,
    session: u64,
    subscription: u64,
    target: Frontier,
    locators: &[block::Hash],
) -> ReactorEvent {
    let mut event = open(target, locators);
    if let ReactorEvent::CreditIssued {
        peer: on,
        session_id,
        subscription: id,
        update,
        ..
    } = &mut event
    {
        *on = peer.clone();
        *session_id = session;
        *id = subscription;
        update.subscription_id = subscription;
    }
    event
}

/// Round `incarnation`'s request on subscription `subscription` of
/// `peer`'s session `session`, bound to its `Open`'s `target` from
/// `anchor`.
fn request_on(
    peer: &ZakuraPeerId,
    session: u64,
    subscription: u64,
    incarnation: u64,
    target: block::Hash,
    anchor: block::Hash,
) -> RequestRef {
    let owner = HeaderSyncWorkOwner::Header(
        HeaderWorkAuthority {
            header_generation: HeaderGeneration::new(1),
            branch: BranchId::new(anchor, target),
        }
        .bind(
            session,
            NonZeroU64::new(subscription).expect("a test subscription is nonzero"),
        ),
    );
    RequestRef {
        peer: peer.clone(),
        session_id: session,
        request_id: subscription,
        incarnation,
        owner,
        target_tip_hash: target,
    }
}

/// [`page`], staged on `request`'s subscription from its own session, in
/// `frame`.
fn page_on(
    request: &RequestRef,
    frame: Option<u64>,
    ancestor: Frontier,
    hashes: &[block::Hash],
) -> ReactorEvent {
    let mut event = page(request, ancestor, hashes);
    if let ReactorEvent::PageStaged {
        from_peer,
        from_session,
        subscription,
        frame: tagged,
        ..
    } = &mut event
    {
        *from_peer = request.peer.clone();
        *from_session = request.session_id;
        *subscription = request.request_id;
        *tagged = frame;
    }
    event
}

/// A staged page: its subscription, its headers and its frame.
type CandidatePage = (Subscription, Vec<block::Hash>, u64);

/// The findings one drain gives for three candidate pages, each on its own
/// subscription, staged in one step: the mirror's honest page on session
/// 7 subscription 1 in frame 3, its page skipping a header on session 8
/// subscription 2 in frame 4, and the other mirror's page carrying its
/// `Open`'s locator on session 9 subscription 3 in frame 5. Also the
/// pages, as `(subscription, hashes, frame)`, in that order.
pub(crate) fn two_page_breaks() -> (Vec<AuthorityFinding>, [CandidatePage; 3]) {
    let chain = main_chain(4);
    let genesis = HeaderFixture::regtest().anchor();
    let target = chain[3];
    let (mirror, other) = (mirror(), other_mirror());
    let honest_round = request_on(&mirror, 7, 1, 1, target.hash, genesis.hash);
    let skipping_round = request_on(&mirror, 8, 2, 2, target.hash, genesis.hash);
    let locator_round = request_on(&other, 9, 3, 3, target.hash, genesis.hash);
    let honest_page = vec![chain[0].hash, chain[1].hash];
    let skipping_page = vec![chain[1].hash];
    let locator_page = vec![chain[0].hash, chain[1].hash];
    let events = [
        adopted(),
        reserved_by(&mirror, 1, 10),
        open_on(&mirror, 7, 1, target, &[genesis.hash]),
        reserved_by(&mirror, 2, 10),
        open_on(&mirror, 8, 2, target, &[genesis.hash]),
        reserved_by(&other, 3, 10),
        open_on(&other, 9, 3, target, &[chain[1].hash, genesis.hash]),
        started(honest_round.clone(), &[genesis.hash]),
        started(skipping_round.clone(), &[genesis.hash]),
        started(locator_round.clone(), &[chain[1].hash, genesis.hash]),
        page_on(&honest_round, Some(3), genesis, &honest_page),
        page_on(&skipping_round, Some(4), genesis, &skipping_page),
        page_on(&locator_round, Some(5), genesis, &locator_page),
    ];
    let mut check = Authority::new(
        AuthorityEvidence::new(&EvidenceStores::default()),
        HeaderFixture::regtest(),
        [0],
        [],
    );
    read_starts(&mut check, &events);
    for event in &honest(&events) {
        check.observe(event, true, false);
    }
    (
        check.drain(1),
        [
            ((mirror.clone(), 7, 1), honest_page, 3),
            ((mirror, 8, 2), skipping_page, 4),
            ((other, 9, 3), locator_page, 5),
        ],
    )
}

/// C22 (a): of three candidate pages staged in one step, the two that
/// break the path rule each give a finding naming the path rule and its
/// own page, by peer, session, subscription, headers and frame, and not
/// the other's. The claim records the tap keeps for them carry that
/// attribution into the claims store's snapshot, which the full report
/// holds. `header_sync`'s domain tests check the property's violations.
#[test]
fn each_page_finding_names_its_own_page() {
    let (found, [_, skipping, locator]) = two_page_breaks();
    let expected = [&skipping, &locator].map(|(subscription, hashes, frame)| Attribution {
        rule: AuthorityRule::Path,
        subject: AuthoritySubject::page(subscription, hashes, Some(*frame)),
    });
    let attributions: Vec<&Attribution> = found.iter().map(|found| &found.attribution).collect();
    assert_eq!(attributions, [&expected[0], &expected[1]], "{found:?}");
    assert!(
        found[0]
            .detail
            .contains("isn't a fixture or declared child"),
        "{found:?}"
    );
    assert!(
        found[1].detail.contains("carried its Open's locator"),
        "{found:?}"
    );
    let rendered = rendered(found.clone());
    for (line, own, theirs) in [
        (&rendered[0], &expected[0], &expected[1]),
        (&rendered[1], &expected[1], &expected[0]),
    ] {
        let lead = format!("CommitOnlyAuthorized [path] {}: ", own.subject);
        assert!(line.starts_with(&lead), "{line}");
        assert!(!line.contains(&theirs.subject.to_string()), "{line}");
    }
    assert!(
        rendered[0].starts_with(&format!(
            "CommitOnlyAuthorized [path] page peer {} session 8 subscription 2 headers [{}] \
             frame 4: ",
            peer(&mirror()),
            crate::storage::hash(skipping.1[0]),
        )),
        "{}",
        rendered[0]
    );
    // A header both the subject and the detail print is spelled the same
    // in both, in its byte order, and never in its display order.
    for (line, shared) in [(&rendered[0], skipping.1[0]), (&rendered[1], locator.1[1])] {
        let (subject, detail) = line
            .split_once(": ")
            .expect("a rendered finding puts its detail after its subject");
        let spelled = crate::storage::hash(shared);
        assert!(subject.contains(&spelled), "{line}");
        assert!(detail.contains(&spelled), "{line}");
        assert!(!line.contains(&shared.to_string()), "{line}");
    }

    let stores = EvidenceStores::default();
    let claims = ClaimEvidence::new(&stores);
    for finding in found {
        claims.record(finding.record(1));
    }
    let snapshot = stores.snapshot();
    let rows = snapshot
        .as_array()
        .into_iter()
        .flatten()
        .find(|store| store["store"] == "claims")
        .map(|store| store["snapshot"].clone())
        .expect("the claims store is in the node's snapshot");
    let peer_hex = |peer_id: &ZakuraPeerId| peer(peer_id);
    let headers = |hashes: &[block::Hash]| -> Vec<String> {
        hashes.iter().copied().map(crate::storage::hash).collect()
    };
    assert_eq!(
        rows,
        serde_json::json!([
            {
                "step": 1,
                "claim": "CommitOnlyAuthorized",
                "detail": rendered[0],
                "reported": false,
                "attribution": {
                    "rule": "path",
                    "subject": {
                        "subject": "page",
                        "peer": peer_hex(&skipping.0 .0),
                        "session": 8,
                        "subscription": 2,
                        "headers": headers(&skipping.1),
                        "frame": 4,
                    },
                },
            },
            {
                "step": 1,
                "claim": "CommitOnlyAuthorized",
                "detail": rendered[1],
                "reported": false,
                "attribution": {
                    "rule": "path",
                    "subject": {
                        "subject": "page",
                        "peer": peer_hex(&locator.0 .0),
                        "session": 9,
                        "subscription": 3,
                        "headers": headers(&locator.1),
                        "frame": 5,
                    },
                },
            },
        ])
    );
}

/// C22 (b): an initial `Open` granting more than its peer reserved breaks
/// the credit rule before any page, as the `overgrant` planted case does.
/// Its finding names that `Open`, by subscription, kind and sequence, and
/// no page.
#[test]
fn an_open_credit_break_names_its_update_and_no_page() {
    let chain = main_chain(4);
    let genesis = HeaderFixture::regtest().anchor().hash;
    let mut check = Authority::new(
        AuthorityEvidence::new(&EvidenceStores::default()),
        HeaderFixture::regtest(),
        [0],
        [],
    );
    let events = honest(&[reserved(1, 9), open(chain[3], &[genesis])]);
    read_starts(&mut check, &events);
    for event in &events {
        check.observe(event, true, false);
    }
    let found = check.drain(1);
    let subject = AuthoritySubject::Update {
        peer: peer(&mirror()),
        session: 7,
        subscription: 1,
        kind: UpdateKind::Open,
        sequence: 0,
    };
    assert_eq!(
        found
            .iter()
            .map(|found| &found.attribution)
            .collect::<Vec<_>>(),
        [&Attribution {
            rule: AuthorityRule::Credit,
            subject: subject.clone(),
        }],
        "{found:?}"
    );
    let line = found[0].to_string();
    assert!(
        line.starts_with(&format!(
            "CommitOnlyAuthorized [credit] update Open sequence 0 peer {} session 7 \
             subscription 1: ",
            peer(&mirror())
        )),
        "{line}"
    );
    assert!(line.contains("granted 10 headers"), "{line}");
    assert!(!line.contains("page"), "{line}");
    let record = found[0].clone().record(1);
    assert!(
        !serde_json::to_string(&record.attribution)
            .expect("an attribution serializes")
            .contains("\"page\""),
        "{record:?}"
    );
}

/// The findings `events` give, their totals filled in honestly, fed in
/// order on the reactor's task to a check of the main branch.
fn findings(events: &[ReactorEvent]) -> Vec<AuthorityFinding> {
    let mut check = Authority::new(
        AuthorityEvidence::new(&EvidenceStores::default()),
        HeaderFixture::regtest(),
        [0],
        [],
    );
    read_starts(&mut check, events);
    for event in &honest(events) {
        check.observe(event, true, false);
    }
    check.drain(1)
}

/// Each of `found`'s attributions, in order.
fn attributions(found: &[AuthorityFinding]) -> Vec<Attribution> {
    found
        .iter()
        .map(|found| found.attribution.clone())
        .collect()
}

/// `rule` broken by `subject`.
fn blamed(rule: AuthorityRule, subject: AuthoritySubject) -> Attribution {
    Attribution { rule, subject }
}

/// The mirror's session 7 subscription 1, and copies of it one key off:
/// another peer and another session that share its subscription number,
/// and another subscription on its session.
fn one_key_off() -> (Subscription, [Subscription; 3]) {
    (
        (mirror(), 7, 1),
        [(other_mirror(), 7, 1), (mirror(), 8, 1), (mirror(), 7, 2)],
    )
}

/// C22 (c), the producer join: the checker judges each round and page
/// against its own subscription's `Open` and names it whole. Inputs pair
/// a subscription with one a key off, by peer, session or subscription,
/// two of which share its subscription number, and pages, rounds, updates
/// and calls a key off within one subscription. Each case breaks exactly
/// one of the pair, and only that one is named, with every key, so a join
/// on an incomplete identity misses it or blames the other.
#[test]
fn each_finding_joins_on_its_full_identity() {
    let chain = main_chain(4);
    let genesis = HeaderFixture::regtest().anchor();
    let (base, others) = one_key_off();
    let (peer_id, session, subscription) = &base;

    for other in &others {
        let (other_peer, other_session, other_subscription) = other;
        // Rounds: the other's Open targets another header, so its round,
        // bound to this one's target, is the one bound off its Open.
        let kept = request_on(
            peer_id,
            *session,
            *subscription,
            1,
            chain[3].hash,
            genesis.hash,
        );
        let off = request_on(
            other_peer,
            *other_session,
            *other_subscription,
            2,
            chain[3].hash,
            genesis.hash,
        );
        let found = findings(&[
            adopted(),
            reserved_by(peer_id, 1, 10),
            open_on(peer_id, *session, *subscription, chain[3], &[genesis.hash]),
            reserved_by(other_peer, 2, 10),
            open_on(
                other_peer,
                *other_session,
                *other_subscription,
                chain[2],
                &[genesis.hash],
            ),
            started(kept, &[genesis.hash]),
            started(off.clone(), &[genesis.hash]),
        ]);
        assert_eq!(
            attributions(&found),
            [blamed(
                AuthorityRule::OwnerBinding,
                AuthoritySubject::round(2, other)
            )],
            "{other:?}: {found:?}"
        );

        // Pages: the same headers in the same frame, which carry the
        // other's Open's locator and not this one's.
        let kept = request_on(
            peer_id,
            *session,
            *subscription,
            1,
            chain[3].hash,
            genesis.hash,
        );
        let off = request_on(
            other_peer,
            *other_session,
            *other_subscription,
            2,
            chain[3].hash,
            genesis.hash,
        );
        let headers = [chain[0].hash, chain[1].hash];
        let found = findings(&[
            adopted(),
            reserved_by(peer_id, 1, 10),
            open_on(peer_id, *session, *subscription, chain[3], &[genesis.hash]),
            reserved_by(other_peer, 2, 10),
            open_on(
                other_peer,
                *other_session,
                *other_subscription,
                chain[3],
                &[chain[1].hash, genesis.hash],
            ),
            started(kept.clone(), &[genesis.hash]),
            started(off.clone(), &[chain[1].hash, genesis.hash]),
            page_on(&kept, Some(3), genesis, &headers),
            page_on(&off, Some(3), genesis, &headers),
        ]);
        assert_eq!(
            attributions(&found),
            [blamed(
                AuthorityRule::Path,
                AuthoritySubject::page(other, &headers, Some(3))
            )],
            "{other:?}: {found:?}"
        );
    }

    // Headers: two pages of one round, untagged, the second skipping a
    // header.
    let round = request_on(
        peer_id,
        *session,
        *subscription,
        1,
        chain[3].hash,
        genesis.hash,
    );
    let found = findings(&[
        adopted(),
        reserved_by(peer_id, 1, 10),
        open_on(peer_id, *session, *subscription, chain[3], &[genesis.hash]),
        started(round.clone(), &[genesis.hash]),
        page_on(&round, None, genesis, &[chain[0].hash]),
        page_on(&round, None, chain[0], &[chain[2].hash]),
    ]);
    assert_eq!(
        attributions(&found),
        [blamed(
            AuthorityRule::Path,
            AuthoritySubject::page(&base, &[chain[2].hash], None)
        )],
        "{found:?}"
    );

    // Round incarnation: two rounds of one subscription, the second bound
    // to another target than its Open's.
    let found = findings(&[
        adopted(),
        reserved_by(peer_id, 1, 10),
        open_on(peer_id, *session, *subscription, chain[3], &[genesis.hash]),
        started(
            request_on(
                peer_id,
                *session,
                *subscription,
                1,
                chain[3].hash,
                genesis.hash,
            ),
            &[genesis.hash],
        ),
        started(
            request_on(
                peer_id,
                *session,
                *subscription,
                2,
                chain[2].hash,
                genesis.hash,
            ),
            &[genesis.hash],
        ),
    ]);
    assert_eq!(
        attributions(&found),
        [blamed(
            AuthorityRule::OwnerBinding,
            AuthoritySubject::round(2, &base)
        )],
        "{found:?}"
    );

    // Update sequence: two grants of one subscription, the second a byte
    // past its headers' pages.
    let page = u32::try_from(credit::smallest_page(AuxSchema::None)).expect("small");
    let start = Frontier::new(block::Height(0), genesis.hash);
    let found = findings(&[
        reserved(1, 10),
        open(chain[3], &[genesis.hash]),
        reserved(2, 2),
        grant(2, 2 * page, 1, start, chain[3].hash),
        reserved(3, 2),
        grant(2, 2 * page + 1, 2, start, chain[3].hash),
    ]);
    assert_eq!(
        attributions(&found),
        [blamed(
            AuthorityRule::Credit,
            AuthoritySubject::update(&base, UpdateKind::Grant, 2)
        )],
        "{found:?}"
    );

    // Call: two prepares dispatched alike, the second reaching storage as
    // a locator read, and then the first reaching it again.
    use zakura_network::zakura::observe::PortCall;
    let round = request(1, chain[1].hash, genesis.hash);
    let source = SourceId::from_digest([2; 32]);
    let mut check = Authority::new(
        AuthorityEvidence::new(&EvidenceStores::default()),
        HeaderFixture::regtest(),
        [0],
        [],
    );
    for call in [1, 2] {
        check.observe(
            &ReactorEvent::Dispatched {
                call,
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
    }
    let prepare = |id, call| crate::observe::StorageFact::Submitted {
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
        call: Some(call),
    };
    let locator = crate::observe::StorageFact::Submitted {
        id: 11,
        request: crate::observe::StorageRequest::ContinuationLocator,
        digest: String::new(),
        call: Some(2),
    };
    check.storage([&prepare(10, 1), &locator]);
    let found = check.drain(1);
    assert!(
        !found.is_empty()
            && attributions(&found).iter().all(|found| *found
                == blamed(
                    AuthorityRule::Submission,
                    AuthoritySubject::Call { call: 2, op: 11 }
                )),
        "{found:?}"
    );
    check.storage([&prepare(12, 1)]);
    let found = check.drain(2);
    assert!(
        !found.is_empty()
            && attributions(&found).iter().all(|found| *found
                == blamed(
                    AuthorityRule::Submission,
                    AuthoritySubject::Call { call: 1, op: 12 }
                )),
        "{found:?}"
    );
}

/// C22 (c): two findings whose subjects agree on every key but one stay
/// distinct, as findings, as rendered and as the full report serializes
/// their attribution, for each key of a page, a
/// round, an update, a cursor, a call, a session and an engine run. The
/// same page staged in another frame, the real case, is one of them.
#[test]
fn findings_one_key_apart_stay_distinct() {
    let page = AuthoritySubject::Page {
        peer: peer(&mirror()),
        session: 8,
        subscription: 2,
        headers: vec!["aa".to_owned(), "bb".to_owned()],
        frame: Some(4),
    };
    let round = AuthoritySubject::Round {
        incarnation: 2,
        peer: peer(&mirror()),
        session: 8,
        subscription: 2,
    };
    let update = AuthoritySubject::Update {
        peer: peer(&mirror()),
        session: 8,
        subscription: 2,
        kind: UpdateKind::Grant,
        sequence: 3,
    };
    let cursor = AuthoritySubject::Cursor {
        peer: peer(&mirror()),
        session: 8,
        subscription: 2,
        fact: CursorFact::Claim,
        height: 5,
        hash: "aa".to_owned(),
    };
    let call = AuthoritySubject::Call { call: 4, op: 9 };
    let session = AuthoritySubject::session(&mirror(), 8, Some(1));
    let other = peer(&other_mirror());
    let reset = |kind: &str, target: &str, generation: u64| {
        AuthoritySubject::Engine(EngineRunRef::Reset {
            fact: Some(ResetFact {
                kind: kind.to_owned(),
                target: target.to_owned(),
                generation,
            }),
        })
    };
    let engine = reset("invalidate", "aa", 2);
    // Each subject, then each copy of it one key off.
    let cases: Vec<(AuthoritySubject, Vec<AuthoritySubject>)> = vec![
        (
            page.clone(),
            vec![
                AuthoritySubject::Page {
                    peer: other.clone(),
                    session: 8,
                    subscription: 2,
                    headers: vec!["aa".to_owned(), "bb".to_owned()],
                    frame: Some(4),
                },
                AuthoritySubject::Page {
                    peer: peer(&mirror()),
                    session: 9,
                    subscription: 2,
                    headers: vec!["aa".to_owned(), "bb".to_owned()],
                    frame: Some(4),
                },
                AuthoritySubject::Page {
                    peer: peer(&mirror()),
                    session: 8,
                    subscription: 3,
                    headers: vec!["aa".to_owned(), "bb".to_owned()],
                    frame: Some(4),
                },
                AuthoritySubject::Page {
                    peer: peer(&mirror()),
                    session: 8,
                    subscription: 2,
                    headers: vec!["aa".to_owned(), "cc".to_owned()],
                    frame: Some(4),
                },
                AuthoritySubject::Page {
                    peer: peer(&mirror()),
                    session: 8,
                    subscription: 2,
                    headers: vec!["bb".to_owned(), "aa".to_owned()],
                    frame: Some(4),
                },
                AuthoritySubject::Page {
                    peer: peer(&mirror()),
                    session: 8,
                    subscription: 2,
                    headers: vec!["aa".to_owned(), "bb".to_owned()],
                    frame: Some(5),
                },
                AuthoritySubject::Page {
                    peer: peer(&mirror()),
                    session: 8,
                    subscription: 2,
                    headers: vec!["aa".to_owned(), "bb".to_owned()],
                    frame: None,
                },
            ],
        ),
        (
            round.clone(),
            vec![
                AuthoritySubject::round(3, &(mirror(), 8, 2)),
                AuthoritySubject::round(2, &(other_mirror(), 8, 2)),
                AuthoritySubject::round(2, &(mirror(), 9, 2)),
                AuthoritySubject::round(2, &(mirror(), 8, 3)),
            ],
        ),
        (
            update.clone(),
            vec![
                AuthoritySubject::update(&(other_mirror(), 8, 2), UpdateKind::Grant, 3),
                AuthoritySubject::update(&(mirror(), 9, 2), UpdateKind::Grant, 3),
                AuthoritySubject::update(&(mirror(), 8, 3), UpdateKind::Grant, 3),
                AuthoritySubject::update(&(mirror(), 8, 2), UpdateKind::Close, 3),
                AuthoritySubject::update(&(mirror(), 8, 2), UpdateKind::Grant, 4),
            ],
        ),
        (
            cursor.clone(),
            vec![
                AuthoritySubject::Cursor {
                    peer: other.clone(),
                    session: 8,
                    subscription: 2,
                    fact: CursorFact::Claim,
                    height: 5,
                    hash: "aa".to_owned(),
                },
                AuthoritySubject::Cursor {
                    peer: peer(&mirror()),
                    session: 9,
                    subscription: 2,
                    fact: CursorFact::Claim,
                    height: 5,
                    hash: "aa".to_owned(),
                },
                AuthoritySubject::Cursor {
                    peer: peer(&mirror()),
                    session: 8,
                    subscription: 3,
                    fact: CursorFact::Claim,
                    height: 5,
                    hash: "aa".to_owned(),
                },
                AuthoritySubject::Cursor {
                    peer: peer(&mirror()),
                    session: 8,
                    subscription: 2,
                    fact: CursorFact::Acceptance,
                    height: 5,
                    hash: "aa".to_owned(),
                },
                AuthoritySubject::Cursor {
                    peer: peer(&mirror()),
                    session: 8,
                    subscription: 2,
                    fact: CursorFact::Claim,
                    height: 6,
                    hash: "aa".to_owned(),
                },
                AuthoritySubject::Cursor {
                    peer: peer(&mirror()),
                    session: 8,
                    subscription: 2,
                    fact: CursorFact::Claim,
                    height: 5,
                    hash: "bb".to_owned(),
                },
            ],
        ),
        (
            call.clone(),
            vec![
                AuthoritySubject::Call { call: 5, op: 9 },
                AuthoritySubject::Call { call: 4, op: 10 },
            ],
        ),
        (
            session.clone(),
            vec![
                AuthoritySubject::session(&other_mirror(), 8, Some(1)),
                AuthoritySubject::session(&mirror(), 9, Some(1)),
                AuthoritySubject::session(&mirror(), 8, Some(2)),
                AuthoritySubject::session(&mirror(), 8, None),
            ],
        ),
        (
            engine.clone(),
            vec![
                reset("reconsider", "aa", 2),
                reset("invalidate", "bb", 2),
                reset("invalidate", "aa", 3),
                AuthoritySubject::Engine(EngineRunRef::Reset { fact: None }),
                AuthoritySubject::Engine(EngineRunRef::Apply { op: 2 }),
                AuthoritySubject::Engine(EngineRunRef::Locator { op: 2 }),
            ],
        ),
    ];
    let finding = |subject: &AuthoritySubject| {
        AuthorityFinding::new(AuthorityRule::Path, subject.clone(), "the same detail")
    };
    for (base, others) in &cases {
        for other in others {
            assert_ne!(finding(base), finding(other), "{base} and {other}");
            assert_ne!(
                finding(base).to_string(),
                finding(other).to_string(),
                "{base} and {other}"
            );
            let sidecar = |subject: &AuthoritySubject| {
                serde_json::to_value(finding(subject).record(1).attribution)
                    .expect("an attribution serializes")
            };
            assert_ne!(sidecar(base), sidecar(other), "{base} and {other}");
        }
    }
    // One key off on the rule, with the subject and detail the same.
    assert_ne!(
        finding(&page).to_string(),
        AuthorityFinding::new(AuthorityRule::Credit, page.clone(), "the same detail").to_string()
    );

    // The real case: the same skipping page staged in frame 4 and in frame
    // 6 gives findings that agree on everything but the frame.
    let in_frame = |frame: u64| {
        let chain = main_chain(4);
        let genesis = HeaderFixture::regtest().anchor();
        let round = request_on(&mirror(), 8, 2, 2, chain[3].hash, genesis.hash);
        let events = [
            adopted(),
            reserved(1, 10),
            open_on(&mirror(), 8, 2, chain[3], &[genesis.hash]),
            started(round.clone(), &[genesis.hash]),
            page_on(&round, Some(frame), genesis, &[chain[1].hash]),
        ];
        let mut check = Authority::new(
            AuthorityEvidence::new(&EvidenceStores::default()),
            HeaderFixture::regtest(),
            [0],
            [],
        );
        read_starts(&mut check, &events);
        for event in &honest(&events) {
            check.observe(event, true, false);
        }
        check.drain(1)
    };
    let (four, six) = (in_frame(4), in_frame(6));
    assert_eq!(four.len(), 1, "{four:?}");
    assert_eq!(six.len(), 1, "{six:?}");
    assert_eq!(four[0].detail, six[0].detail);
    let AuthoritySubject::Page { frame, .. } = &four[0].attribution.subject else {
        panic!("a path finding names its page: {four:?}");
    };
    assert_eq!(*frame, Some(4));
    let mut refrated = four[0].attribution.subject.clone();
    if let AuthoritySubject::Page { frame, .. } = &mut refrated {
        *frame = Some(6);
    }
    assert_eq!(six[0].attribution.subject, refrated);
    assert_ne!(four[0].to_string(), six[0].to_string());
}

/// Folds `events` into a fresh work ledger, which must accept them, and
/// returns its claim breaks with the attribution the tap keeps for each.
fn ledger_claims(events: &[ReactorEvent]) -> Vec<(ClaimRule, Option<Attribution>)> {
    let mut ledger = HeaderWorkLedger::default();
    for event in events {
        if let Err(rule) = ledger.observe(event) {
            panic!("the ledger refused {event:?}: {rule}");
        }
    }
    ledger
        .take_claims()
        .iter()
        .map(|claim| (claim.rule, ledger_attribution(claim)))
        .collect()
}

/// The attribution `claims` hold for `rule`'s first break, after checking
/// every break is attributed exactly when `commit-only-authorized`
/// reports its rule.
fn attributed(claims: &[(ClaimRule, Option<Attribution>)], rule: ClaimRule) -> Attribution {
    for (found, attribution) in claims {
        assert_eq!(
            attribution.is_some(),
            AUTHORITY_CLAIMS.contains(&Claim::of(*found)),
            "{claims:?}"
        );
    }
    claims
        .iter()
        .find(|(found, _)| *found == rule)
        .and_then(|(_, attribution)| attribution.clone())
        .unwrap_or_else(|| panic!("no attributed {rule:?} in {claims:?}"))
}

/// The mirror's round 1 on session 7 subscription 1, toward the main
/// chain's fourth header, bound to its reservation 1 of 10 headers and
/// started, with the events that do it.
fn ledger_round() -> (RequestRef, Vec<ReactorEvent>) {
    let chain = main_chain(4);
    let genesis = HeaderFixture::regtest().anchor();
    let round = request_on(&mirror(), 7, 1, 1, chain[3].hash, genesis.hash);
    let events = vec![
        reserved(1, 10),
        ReactorEvent::ReservationBound {
            reservation: 1,
            request: round.clone(),
            max_headers: 10,
        },
        started(round.clone(), &[genesis.hash]),
    ];
    (round, events)
}

/// `round`'s prepare of `hashes` toward the last of them.
fn ledger_prepare(round: &RequestRef, hashes: &[block::Hash]) -> PortCall {
    let genesis = HeaderFixture::regtest().anchor();
    PortCall::Prepare {
        peer: round.peer.clone(),
        source: SourceId::from_digest(round.peer.digest()),
        owner: round.owner,
        common_ancestor: genesis,
        target: Frontier::new(
            block::Height(u32::try_from(hashes.len()).expect("a test page is short")),
            *hashes.last().expect("a test prepare sends headers"),
        ),
        hashes: hashes.to_vec(),
    }
}

/// Every work-ledger rule `commit-only-authorized` reports maps to its
/// authority rule, and none of the ledger's other rules is attributed,
/// even with a subject.
#[test]
fn each_ledger_rule_the_property_reports_has_its_authority_rule() {
    use AuthorityRule::{Credit, OwnerBinding, Path, Provenance, Submission};
    let reported = [
        (ClaimRule::ForeignPage, Provenance),
        (ClaimRule::WrongSource, Provenance),
        (ClaimRule::UnstagedPage, Path),
        (ClaimRule::OverReservation, Credit),
        (ClaimRule::WrongOwner, OwnerBinding),
        (ClaimRule::WrongRound, OwnerBinding),
        (ClaimRule::ForeignLease, OwnerBinding),
        (ClaimRule::HeldUnowned, OwnerBinding),
        (ClaimRule::UnstagedHeaders, OwnerBinding),
        (ClaimRule::WrongPayload, Submission),
        (ClaimRule::WrongCall, Submission),
        (ClaimRule::BadHandoff, Submission),
    ];
    let others = [
        ClaimRule::OverBudget,
        ClaimRule::LeakedReservation,
        ClaimRule::LeakedLease,
        ClaimRule::OpenOwnership,
        ClaimRule::OpenReservation,
        ClaimRule::UnreleasedObsoleteRound,
        ClaimRule::UnreleasedRound,
        ClaimRule::UnreleasedDepartedRound,
    ];
    let claim = |rule| ClaimBreak {
        rule,
        detail: String::new(),
        subject: Some(ClaimSubject::Hold {
            call: 1,
            incarnation: 2,
        }),
    };
    for (rule, authority) in reported {
        assert!(AUTHORITY_CLAIMS.contains(&Claim::of(rule)), "{rule:?}");
        assert_eq!(
            ledger_attribution(&claim(rule)),
            Some(Attribution {
                rule: authority,
                subject: AuthoritySubject::Hold { call: 1, round: 2 },
            }),
            "{rule:?}"
        );
    }
    for rule in others {
        assert!(!AUTHORITY_CLAIMS.contains(&Claim::of(rule)), "{rule:?}");
        assert_eq!(ledger_attribution(&claim(rule)), None, "{rule:?}");
    }
    assert_eq!(
        reported.len() + 1,
        AUTHORITY_CLAIMS.len(),
        "every ledger rule the property reports, beside its own"
    );
}

/// A ledger claim about a page names the page as it arrived, by peer,
/// session, subscription, headers and frame: a page from another session
/// than its round's, and one that overspent its reservation.
#[test]
fn a_ledger_page_claim_names_its_page() {
    let chain = main_chain(11);
    let genesis = HeaderFixture::regtest().anchor();
    let (round, mut foreign) = ledger_round();
    let mut page = page_on(&round, Some(4), genesis, &[chain[0].hash]);
    if let ReactorEvent::PageStaged { from_session, .. } = &mut page {
        *from_session = 9;
    }
    foreign.push(page);
    assert_eq!(
        attributed(&ledger_claims(&foreign), ClaimRule::ForeignPage),
        Attribution {
            rule: AuthorityRule::Provenance,
            subject: AuthoritySubject::page(&(mirror(), 9, 1), &[chain[0].hash], Some(4)),
        }
    );
    let (round, mut over) = ledger_round();
    let hashes: Vec<block::Hash> = chain.iter().map(|header| header.hash).collect();
    over.push(page_on(&round, Some(5), genesis, &hashes));
    assert_eq!(
        attributed(&ledger_claims(&over), ClaimRule::OverReservation),
        Attribution {
            rule: AuthorityRule::Credit,
            subject: AuthoritySubject::page(&(mirror(), 7, 1), &hashes, Some(5)),
        }
    );
}

/// A ledger claim about a call's hold names the call and its round, and
/// no page: a prepare holding a round that staged nothing.
#[test]
fn a_ledger_hold_claim_names_its_call_and_round() {
    let chain = main_chain(1);
    let (round, mut events) = ledger_round();
    events.extend([
        ReactorEvent::Dispatched {
            call: 3,
            op: ledger_prepare(&round, &[chain[0].hash]),
        },
        ReactorEvent::HeadersHeld {
            call: 3,
            kind: StorageCallKind::Prepare,
            peer: mirror(),
            incarnation: 1,
            headers: 1,
            leases: Vec::new(),
        },
    ]);
    let attribution = attributed(&ledger_claims(&events), ClaimRule::UnstagedHeaders);
    assert_eq!(
        attribution,
        Attribution {
            rule: AuthorityRule::OwnerBinding,
            subject: AuthoritySubject::Hold { call: 3, round: 1 },
        }
    );
    assert!(
        !serde_json::to_string(&attribution)
            .expect("an attribution serializes")
            .contains("\"page\""),
        "{attribution:?}"
    );
}

/// A ledger claim about a handoff names the prepare, the apply and the
/// round: a prepare handing its staged page to another peer's apply.
#[test]
fn a_ledger_handoff_claim_names_both_calls() {
    let chain = main_chain(1);
    let genesis = HeaderFixture::regtest().anchor();
    let (round, mut events) = ledger_round();
    events.extend([
        page_on(&round, None, genesis, &[chain[0].hash]),
        ReactorEvent::Dispatched {
            call: 1,
            op: ledger_prepare(&round, &[chain[0].hash]),
        },
        ReactorEvent::HeadersHeld {
            call: 1,
            kind: StorageCallKind::Prepare,
            peer: mirror(),
            incarnation: 1,
            headers: 1,
            leases: Vec::new(),
        },
        ReactorEvent::Dispatched {
            call: 2,
            op: PortCall::Apply {
                peer: other_mirror(),
                source: SourceId::from_digest(other_mirror().digest()),
                owner: round.owner,
                target_tip_hash: chain[0].hash,
            },
        },
        ReactorEvent::HeadersEnded {
            call: 1,
            incarnation: 1,
            headers: 1,
            end: HeaderWorkEnd::HandedToApply { apply: 2 },
        },
    ]);
    assert_eq!(
        attributed(&ledger_claims(&events), ClaimRule::BadHandoff),
        Attribution {
            rule: AuthorityRule::Submission,
            subject: AuthoritySubject::Handoff {
                prepare: 1,
                apply: 2,
                round: 1,
            },
        }
    );
}

/// One page from the mirror's session 9 staged on its session 7 round, a
/// `ForeignPage` break, folded through a fresh ledger: by `peer`, in
/// `frame`.
fn foreign_page_claim(peer_id: &ZakuraPeerId, frame: u64) -> ClaimBreak {
    let chain = main_chain(1);
    let genesis = HeaderFixture::regtest().anchor();
    let (round, mut events) = ledger_round();
    let mut page = page_on(&round, Some(frame), genesis, &[chain[0].hash]);
    if let ReactorEvent::PageStaged {
        from_peer,
        from_session,
        reserved_left,
        ..
    } = &mut page
    {
        *from_peer = peer_id.clone();
        *from_session = 9;
        *reserved_left = 9;
    }
    events.push(page);
    let mut ledger = HeaderWorkLedger::default();
    for event in &events {
        if let Err(rule) = ledger.observe(event) {
            panic!("the ledger refused {event:?}: {rule}");
        }
    }
    let mut claims = ledger.take_claims();
    assert_eq!(claims.len(), 1, "{claims:?}");
    claims.remove(0)
}

/// The claim records the tap's drain of `step` keeps for three
/// `ForeignPage` breaks: the mirror's page in frame 4, the same page from
/// the other mirror, and the mirror's page in frame 5. Each is one key off
/// the first, and the ledger's own words for all three are the same.
///
/// The ledger's maps draw their hashing seed from a run, so the breaks are
/// folded on this thread's first call, which must come outside one.
pub(crate) fn foreign_page_records(step: u64) -> Vec<ClaimRecord> {
    thread_local! {
        static CLAIMS: std::cell::RefCell<Option<Vec<ClaimBreak>>> =
            const { std::cell::RefCell::new(None) };
    }
    CLAIMS.with(|claims| {
        claims
            .borrow_mut()
            .get_or_insert_with(|| {
                [(mirror(), 4), (other_mirror(), 4), (mirror(), 5)]
                    .iter()
                    .map(|(peer_id, frame)| foreign_page_claim(peer_id, *frame))
                    .collect()
            })
            .iter()
            .map(|claim| ledger_record(step, claim.clone()))
            .collect()
    })
}

/// A page that arrived on another peer's or another session's stream than
/// its round's, and skips a header, gives a path finding that names the
/// page as it arrived, with the round fixed, which is the subject the
/// ledger's `ForeignPage` gives the same page. Each arrival differs from
/// the round in one key.
#[test]
fn a_path_finding_names_its_pages_arrival() {
    let chain = main_chain(4);
    let genesis = HeaderFixture::regtest().anchor();
    let round = request_on(&mirror(), 7, 1, 1, chain[3].hash, genesis.hash);
    for (peer_id, session) in [(other_mirror(), 7), (mirror(), 8)] {
        let mut page = page_on(&round, Some(4), genesis, &[chain[1].hash]);
        if let ReactorEvent::PageStaged {
            from_peer,
            from_session,
            reserved_left,
            ..
        } = &mut page
        {
            *from_peer = peer_id.clone();
            *from_session = session;
            *reserved_left = 9;
        }
        let arrival =
            AuthoritySubject::page(&(peer_id.clone(), session, 1), &[chain[1].hash], Some(4));
        let found = findings(&[
            adopted(),
            reserved(1, 10),
            open_on(&mirror(), 7, 1, chain[3], &[genesis.hash]),
            started(round.clone(), &[genesis.hash]),
            page.clone(),
        ]);
        assert_eq!(
            attributions(&found),
            [blamed(AuthorityRule::Path, arrival.clone())],
            "{peer_id:?} session {session}: {found:?}"
        );
        let (ledger_round, mut events) = ledger_round();
        assert_eq!(ledger_round, round);
        events.push(page);
        assert_eq!(
            attributed(&ledger_claims(&events), ClaimRule::ForeignPage).subject,
            arrival
        );
    }
}

/// `event`, a credit fact of the mirror's session 7, for its subscription
/// `subscription` instead.
fn on_subscription(mut event: ReactorEvent, subscription: u64) -> ReactorEvent {
    match &mut event {
        ReactorEvent::CursorClaimed {
            subscription: id, ..
        }
        | ReactorEvent::CursorsAccepted {
            subscription: id, ..
        } => *id = subscription,
        _ => unreachable!("only claims and acceptances are moved"),
    }
    event
}

/// A foreign page carrying another subscription's ID, staged on a round,
/// is judged against the round's own `Open` and staged path, so the
/// unrelated `Open` its ID names, on the same peer and session with
/// another target, gets no false path finding and no staged cursor: its
/// later acceptance of the page's last header still stages nothing.
#[test]
fn a_foreign_pages_subscription_id_leaves_other_opens_alone() {
    let chain = main_chain(4);
    let genesis = HeaderFixture::regtest().anchor();
    let round = request_on(&mirror(), 7, 1, 1, chain[3].hash, genesis.hash);
    let hashes = [chain[0].hash, chain[1].hash, chain[2].hash];
    let mut page = page_on(&round, Some(4), genesis, &hashes);
    if let ReactorEvent::PageStaged {
        from_peer,
        from_session,
        subscription,
        ..
    } = &mut page
    {
        *from_peer = other_mirror();
        *from_session = 8;
        *subscription = 2;
    }
    let found = findings(&[
        adopted(),
        reserved(1, 10),
        open_on(&mirror(), 7, 1, chain[3], &[genesis.hash]),
        reserved(2, 10),
        open_on(&mirror(), 7, 2, chain[1], &[genesis.hash]),
        started(round, &[genesis.hash]),
        page,
        on_subscription(claimed(chain[2], 3), 2),
        on_subscription(accepted(chain[2]), 2),
    ]);
    let rules: Vec<AuthorityRule> = found.iter().map(|found| found.attribution.rule).collect();
    assert!(!rules.contains(&AuthorityRule::Path), "{found:?}");
    assert!(
        found.iter().any(|found| found
            .to_string()
            .contains("session 7 subscription 2 accepted through")
            && found.detail.contains("which no staged page ends at")),
        "{found:?}"
    );
}
