//! Tests for the header sync version 9 subscriber.
//!
//! Pages and outcomes go through the real codec. A scripted publisher keeps
//! its own [`Publications`] ledger, so every update the subscriber writes must
//! apply there without a fault.

use std::collections::{HashSet, VecDeque};

use proptest::prelude::*;

use super::*;
use crate::zakura::{
    framed_channel,
    header_sync::{
        config::REGTEST_HEADER_BYTES,
        v9::{
            entry_bytes,
            test_support::{record, HeaderTree, OUTCOME_CODES},
            Message, RegtestMessage, HEADERS_RESPONSE_FIXED_BYTES,
        },
        wire::HeaderEntry,
    },
    regulation::{PrecheckSlot, Publications},
    wire_codec::{decode_frame, encode_frame},
    CloseCause, Frame, FrameRejection,
};
use tokio_util::sync::CancellationToken;

/// Regtest header bytes, so pages stay small.
const HB: usize = REGTEST_HEADER_BYTES;

/// The subscriber under test.
type Sub = Subscriber;

/// Headers per page that the scripted publisher sends.
const PAGE_HEADERS: u32 = 3;

/// A subscriber on an in-process session whose fence closes `connection`.
fn fenced(connection: &CancellationToken) -> Sub {
    let fence = WriterFence::new(connection.clone(), CloseCause::default());
    Sub::new(HB, fence, &framed_channel(1).1)
}

/// A subscriber on a session whose connection the test ignores.
fn subscriber() -> Sub {
    fenced(&CancellationToken::new())
}

/// A peer `Status` that advertises `tip` and the scripted publisher's page
/// caps.
fn status(tip: Frontier) -> Status {
    Status {
        max_headers_per_response: PAGE_HEADERS,
        max_message_bytes: page_bytes(PAGE_HEADERS, AuxSchema::V1),
        ..super::super::test_support::status::<HB>(tip)
    }
}

/// Bytes of a page of `count` headers under `schema`.
fn page_bytes(count: u32, schema: AuxSchema) -> u32 {
    // Widening u32 to usize is lossless on supported targets.
    u32::try_from(HEADERS_RESPONSE_FIXED_BYTES + count as usize * entry_bytes(HB, schema))
        .expect("a small page fits u32")
}

impl HeaderTree {
    /// The headers after `from` through `to`, if `to` descends from `from`.
    fn path(&self, from: Frontier, to: Frontier) -> Option<Vec<Frontier>> {
        if self.ancestor(to, from.height) != Some(from) {
            return None;
        }
        let mut path = Vec::new();
        let mut at = to;
        while at != from {
            path.push(at);
            at = self.parent(at)?;
        }
        path.reverse();
        Some(path)
    }

    /// The page entry for `at` under `schema`.
    fn entry(&self, at: Frontier, schema: AuxSchema) -> HeaderEntry {
        HeaderEntry {
            header: self.nodes[&at.hash].1.clone(),
            body_size: 0,
            tree_aux: (schema == AuxSchema::V1).then(|| record(at.height)),
        }
    }

    /// Up to 13 locators for a chain ending at `tip`: the tip, then every
    /// `step` heights down, then genesis.
    fn locators(&self, tip: Frontier, step: u32) -> Vec<Frontier> {
        let mut locators = vec![tip];
        let mut height = tip.height.0;
        while locators.len() < 12 && height > step {
            height -= step;
            locators.push(
                self.ancestor(tip, block::Height(height))
                    .expect("on the chain"),
            );
        }
        if tip != self.genesis {
            locators.push(self.genesis);
        }
        locators
    }
}

/// The encoded frame of a page of `headers` above `parent`.
fn page_frame(
    tree: &HeaderTree,
    id: u64,
    target: block::Hash,
    parent: Frontier,
    headers: &[Frontier],
    marker: bool,
    schema: AuxSchema,
) -> Frame {
    encode_frame(&RegtestMessage::Headers(Headers {
        subscription_id: id,
        initial_target_tip_hash: target,
        common_ancestor_height: parent.height,
        common_ancestor_hash: parent.hash,
        reaches_initial_target: marker,
        tree_aux_schema: schema,
        entries: headers
            .iter()
            .map(|&header| tree.entry(header, schema))
            .collect(),
    }))
    .expect("a test page encodes")
}

/// Decode `frame` as a page, with its payload length.
fn decode_page(frame: &Frame) -> (Headers, usize) {
    match decode_frame::<RegtestMessage>(frame).expect("a test frame decodes") {
        Message::Headers(page) => (page, frame.payload.len()),
        other => panic!("expected a page, got {other:?}"),
    }
}

/// Encode `update` with the real codec, which checks its shape and the
/// `Grant` minimum, and decode it back.
fn wire(update: &SubscribeHeaders) -> SubscribeHeaders {
    let frame = encode_frame(&RegtestMessage::SubscribeHeaders(update.clone()))
        .expect("the subscriber writes only legal updates");
    match decode_frame::<RegtestMessage>(&frame).expect("a legal update decodes") {
        Message::SubscribeHeaders(decoded) => decoded,
        other => panic!("expected an update, got {other:?}"),
    }
}

/// An outcome for `id`.
fn outcome(id: u64, target: block::Hash, code: HeadersOutcomeCode) -> HeadersOutcome {
    HeadersOutcome {
        subscription_id: id,
        initial_target_tip_hash: target,
        outcome: code,
    }
}

/// A subscriber with one subscription, and the pieces that built it.
struct Fixture {
    tree: HeaderTree,
    sub: Sub,
    id: u64,
    target: Frontier,
    schema: AuxSchema,
}

impl Fixture {
    /// Open to `target` with `locators` and `staging` reserved headers.
    fn open(tree: HeaderTree, target: Frontier, locators: &[Frontier], staging: u32) -> Self {
        Self::open_with(tree, &status(target), locators, AuxSchema::None, staging)
    }

    /// Open to the tip of `status` under `schema`, with `locators` and
    /// `staging` reserved headers.
    fn open_with(
        tree: HeaderTree,
        status: &Status,
        locators: &[Frontier],
        schema: AuxSchema,
        staging: u32,
    ) -> Self {
        let target = Frontier::new(status.selected_tip_height, status.selected_tip_hash);
        let mut sub = subscriber();
        let (open, _) = sub
            .open(status, locators, schema, staging)
            .expect("the first Open is recorded");
        let open = wire(&open);
        Self {
            tree,
            sub,
            id: open.subscription_id,
            target,
            schema,
        }
    }

    /// Reopen to the same target after the terminal outcome, under `schema`.
    fn reopen(
        &mut self,
        locators: &[Frontier],
        schema: AuxSchema,
        staging: u32,
    ) -> SubscribeHeaders {
        let (open, _) = self
            .sub
            .open(&status(self.target), locators, schema, staging)
            .expect("the terminal outcome freed the slot");
        let open = wire(&open);
        self.id = open.subscription_id;
        self.schema = schema;
        open
    }

    /// Deliver a page of `headers` above `parent`.
    fn page(
        &mut self,
        parent: Frontier,
        headers: &[Frontier],
        marker: bool,
    ) -> Result<Admitted, SubscriberFault> {
        let frame = page_frame(
            &self.tree,
            self.id,
            self.target.hash,
            parent,
            headers,
            marker,
            self.schema,
        );
        self.sub.admit(&frame)?;
        let (page, len) = decode_page(&frame);
        self.sub.receive_page(&page, len)
    }

    /// Deliver an outcome.
    fn outcome(&mut self, code: HeadersOutcomeCode) -> Result<Ended, SubscriberFault> {
        self.sub
            .receive_outcome(&outcome(self.id, self.target.hash, code))
    }
}

/// A tree with a chain of `length` headers, returned with the chain.
fn chain(length: u32) -> (HeaderTree, Vec<Frontier>) {
    let mut tree = HeaderTree::new();
    let genesis = tree.genesis;
    let mut chain = vec![genesis];
    chain.extend(tree.extend(genesis, length));
    (tree, chain)
}

/// An `Open` records its reservation first: the start cursor is the first
/// locator with zero counts, its credit is the whole window and fits a page,
/// and a second `Open` waits for the terminal outcome.
#[test]
fn open_records_its_reservation_first() {
    let (_tree, chain) = chain(6);
    let mut sub = subscriber();
    let locators = [chain[3], chain[1], chain[0]];
    let open = wire(
        &sub.open(&status(chain[6]), &locators, AuxSchema::None, 10)
            .expect("the first Open is recorded")
            .0,
    );
    assert_eq!(open.operation, Operation::Open);
    assert_eq!(open.update_sequence, 0);
    assert_eq!(open.initial_target_tip_hash, chain[6].hash);
    assert_eq!(
        open.acknowledged,
        Cursor {
            height: chain[3].height,
            hash: chain[3].hash,
            headers: 0,
            bytes: 0
        }
    );
    assert_eq!(
        open.locator_hashes,
        locators
            .iter()
            .map(|locator| locator.hash)
            .collect::<Vec<_>>()
    );
    let page = u32::try_from(smallest_page(HB, AuxSchema::None)).expect("fits u32");
    assert_eq!(
        open.added_credit,
        Credit {
            objects: 10,
            bytes: 10 * page
        }
    );
    assert_eq!(
        sub.tools
            .lock()
            .credit(&open.subscription_id)
            .map(|credit| credit.granted().objects),
        Some(10),
        "recorded before it is written"
    );

    // A conformant subscriber holds one subscription: a target change waits
    // for the terminal outcome, and the live credit stays.
    assert_eq!(
        sub.open(&status(chain[5]), &locators, AuxSchema::None, 10)
            .err(),
        Some(SubscribeError::Tools(SubscribeRefused::AtCapacity))
    );
    assert_eq!(sub.live(), Some(open.subscription_id));
    sub.receive_outcome(&outcome(
        open.subscription_id,
        chain[6].hash,
        HeadersOutcomeCode::TargetNotSelected,
    ))
    .expect("legal before the first page");
    let (next, _) = sub
        .open(&status(chain[6]), &locators, AuxSchema::None, 10)
        .expect("the terminal outcome freed the slot");
    assert_ne!(next.subscription_id, open.subscription_id, "a new ID");
}

/// Local refusals: bad locators, an unadvertised schema, no staging budget,
/// and a locator that puts the target at another height than the `Status`.
/// None records anything.
#[test]
fn open_refuses_what_the_publisher_would_fault() {
    let (_tree, chain) = chain(20);
    let mut sub = subscriber();
    let target = status(chain[20]);
    assert_eq!(
        sub.open(&target, &[], AuxSchema::None, 10).err(),
        Some(SubscribeError::Locators)
    );
    assert_eq!(
        sub.open(
            &target,
            &[chain[2], chain[1], chain[2]],
            AuxSchema::None,
            10
        )
        .err(),
        Some(SubscribeError::Locators)
    );
    assert_eq!(
        sub.open(&target, &chain[..14], AuxSchema::None, 10).err(),
        Some(SubscribeError::Locators)
    );
    let mut plain = status(chain[20]);
    plain.tree_aux_schema_mask = 0;
    assert_eq!(
        sub.open(&plain, &[chain[1]], AuxSchema::V1, 10).err(),
        Some(SubscribeError::Schema)
    );
    assert_eq!(
        sub.open(&target, &[chain[1]], AuxSchema::None, 0).err(),
        Some(SubscribeError::NoStaging)
    );
    let misplaced = Status {
        selected_tip_height: block::Height(19),
        ..status(chain[20])
    };
    assert_eq!(
        sub.open(&misplaced, &[chain[20], chain[1]], AuxSchema::None, 10)
            .err(),
        Some(SubscribeError::TargetHeight),
        "our locator puts the target at 20"
    );
    assert_eq!(sub.live(), None);
    assert_eq!(sub.tools.lock().len(), 0);

    // A schema-1 Open's byte credit fits a schema-1 page.
    let open = wire(
        &sub.open(&target, &[chain[1]], AuxSchema::V1, 1)
            .expect("one header of staging is enough")
            .0,
    );
    assert_eq!(open.added_credit.objects, 1);
    assert_eq!(
        open.added_credit.bytes as usize,
        smallest_page(HB, AuxSchema::V1)
    );
}

/// The constructor attaches the frame header check to a transport reader,
/// which refuses an unsolicited page before its payload is read and queues
/// at most the unspent credit of pages ahead of the reactor. `admit` releases
/// each page's charge, and without a reader it runs the same check before
/// decode.
#[test]
fn the_frame_header_check_runs_before_decode() {
    let (tree, chain) = chain(2);
    let frame = page_frame(
        &tree,
        1,
        chain[2].hash,
        chain[0],
        &chain[1..2],
        false,
        AuxSchema::None,
    );
    let slot = PrecheckSlot::default();
    let (_, recv) = framed_channel(1);
    let recv = recv.with_precheck(slot.clone());
    let fence = WriterFence::new(CancellationToken::new(), CloseCause::default());
    let mut sub = Sub::new(HB, fence, &recv);
    let reader = slot.get().expect("the subscriber attaches its check");
    let len = frame.payload.len();
    assert_eq!(
        reader.check(frame.message_type, len),
        Err(FrameRejection::Unsolicited)
    );
    assert_eq!(
        subscriber().admit(&frame),
        Err(SubscriptionFault::Unknown.into())
    );

    // Two headers of credit: the reader queues two one-header pages and
    // refuses a third. A taken page keeps its charge until its claim spends
    // the credit, so the reader never sees that credit free in between, and
    // only the Grant after acceptance makes room.
    sub.open(&status(chain[2]), &[chain[0]], AuxSchema::None, 2)
        .expect("the first Open is recorded");
    assert_eq!(reader.check(frame.message_type, len), Ok(()));
    assert_eq!(reader.check(frame.message_type, len), Ok(()));
    assert!(reader.check(frame.message_type, len).is_err());
    assert_eq!(sub.admit(&frame), Ok(()));
    assert!(
        reader.check(frame.message_type, len).is_err(),
        "a taken page holds its charge until its claim"
    );
    let (page, _) = decode_page(&frame);
    let admitted = sub.receive_page(&page, len).expect("the first page");
    assert!(reader.check(frame.message_type, len).is_err());
    sub.accept_through(admitted.id, &admitted.cursor, 2)
        .expect("a received cursor")
        .expect("one header of room is a prefix of a two-header window");
    assert_eq!(reader.check(frame.message_type, len), Ok(()));
}

/// Path pages start at the highest sent locator on the target's chain, carry
/// the marker on exactly the page that ends at the target, and pushes extend
/// the receive cursor.
#[test]
fn path_pages_carry_one_marker_then_follow() {
    let (mut tree, chain) = chain(6);
    let pushed = tree.extend(chain[6], 2);
    // The subscriber's own chain forks at height 2, so only chain[2] and
    // genesis lie on the target's chain.
    let fork = tree.extend(chain[2], 3);
    let locators = [fork[2], fork[0], chain[2], chain[0]];
    let mut fixture = Fixture::open(tree, chain[5], &locators, 100);

    let first = fixture
        .page(chain[2], &chain[3..5], false)
        .expect("extends the highest locator on the path");
    assert_eq!(first.parent, chain[2]);
    assert_eq!(first.cursor.height, chain[4].height);
    assert_eq!(first.cursor.hash, chain[4].hash);
    assert_eq!(first.cursor.headers, 2);
    assert!(!first.reaches_target);
    let marked = fixture
        .page(chain[4], &chain[5..6], true)
        .expect("ends at the target");
    assert!(marked.reaches_target);
    assert_eq!(marked.cursor.headers, 3);
    assert_eq!(
        marked.cursor.bytes,
        first.cursor.bytes + u64::try_from(smallest_page(HB, AuxSchema::None)).expect("fits")
    );
    fixture
        .page(chain[5], &[chain[6], pushed[0]], false)
        .expect("a push extends the receive cursor");
    fixture
        .page(pushed[0], &pushed[1..2], false)
        .expect("each push extends the one before");
    fixture
        .outcome(HeadersOutcomeCode::SubscriptionSuperseded)
        .expect("superseded is legal at any time");
}

/// A first or later locator at the target reaches it at `Open`: no path page
/// and no marker, the first push extends the target, and `HistoryPruned` is
/// outside every window.
#[test]
fn a_locator_at_the_target_reaches_it_at_open() {
    for target_index in [0, 1] {
        let (mut tree, chain) = chain(6);
        let pushed = tree.extend(chain[6], 2);
        let locators = [chain[6], chain[4], chain[0]];
        let target = locators[target_index];
        let mut fixture = Fixture::open(tree, target, &locators, 100);

        // The first page extends the target, not the first locator.
        let other = locators[1 - target_index];
        let next = |at: Frontier| match at.height.0 {
            4 => vec![chain[5]],
            _ => vec![pushed[0]],
        };
        assert_eq!(
            fixture.page(other, &next(other), false),
            Err(SubscriberFault::FirstParent)
        );
        assert_eq!(
            fixture.page(target, &next(target), true),
            Err(SubscriberFault::SecondMarker),
            "the Open already reached the target"
        );
        assert_eq!(
            fixture.outcome(HeadersOutcomeCode::HistoryPruned),
            Err(SubscriberFault::OutcomeWindow(
                HeadersOutcomeCode::HistoryPruned
            ))
        );
        let push = fixture
            .page(target, &next(target), false)
            .expect("the first push extends the target");
        assert!(!push.reaches_target);
        let ended = fixture
            .outcome(HeadersOutcomeCode::SubscriptionSuperseded)
            .expect("superseded is legal at any time");
        assert!(ended.reached);
        assert_eq!(ended.pages, 1);
    }
}

/// Every misplaced page faults: another subscription or target, a schema
/// mismatch, a first page off every sent locator, a skipped higher locator,
/// broken linkage, a missing, early, or second marker, a page past the
/// target, and a page above its credit.
#[test]
fn misplaced_pages_fault() {
    let (mut tree, chain) = chain(8);
    let fork = tree.extend(chain[1], 2);
    let locators = [chain[5], chain[3], chain[0]];
    let mut fixture = Fixture::open(tree, chain[7], &locators, 100);

    // Another subscription, target, or schema.
    fixture.id += 1;
    assert_eq!(
        fixture.page(chain[5], &chain[6..7], false),
        Err(SubscriptionFault::Unknown.into())
    );
    fixture.id -= 1;
    fixture.target = chain[8];
    assert_eq!(
        fixture.page(chain[5], &chain[6..7], false),
        Err(SubscriberFault::Target)
    );
    fixture.target = chain[7];
    fixture.schema = AuxSchema::V1;
    assert_eq!(
        fixture.page(chain[5], &chain[6..7], false),
        Err(SubscriberFault::Schema)
    );
    fixture.schema = AuxSchema::None;

    let before_first_page = [
        // Off every sent locator, or at a sent hash with another height.
        (chain[4], &chain[5..6], false, SubscriberFault::FirstParent),
        (fork[1], &chain[6..7], false, SubscriberFault::FirstParent),
        (
            Frontier::new(block::Height(4), chain[5].hash),
            &chain[6..7],
            false,
            SubscriberFault::FirstParent,
        ),
        // The path from chain[3] passes chain[5], mid-page or at its end.
        (chain[3], &chain[4..7], false, SubscriberFault::LowerLocator),
        (chain[3], &chain[4..6], false, SubscriberFault::LowerLocator),
        (chain[5], &chain[6..9], false, SubscriberFault::PastTarget),
        // The target page without the marker, then an early marker.
        (chain[5], &chain[6..8], false, SubscriberFault::Marker),
        (chain[5], &chain[6..7], true, SubscriberFault::Marker),
    ];
    for (parent, headers, marker, fault) in before_first_page {
        assert_eq!(
            fixture.page(parent, headers, marker),
            Err(fault),
            "{parent:?}"
        );
    }

    // Broken linkage after the first page.
    fixture
        .page(chain[5], &chain[6..7], false)
        .expect("extends the highest locator");
    assert_eq!(
        fixture.page(chain[5], &chain[6..8], true),
        Err(SubscriberFault::Parent),
        "a later page repeats the first page's parent"
    );
    assert_eq!(
        fixture.page(fork[0], &fork[1..2], false),
        Err(SubscriberFault::Parent)
    );

    // A second marker.
    fixture
        .page(chain[6], &chain[7..8], true)
        .expect("ends at the target");
    assert_eq!(
        fixture.page(chain[7], &chain[8..9], true),
        Err(SubscriberFault::SecondMarker)
    );

    // A page above its credit. The same seed rebuilds the same chain.
    let (tree, _) = self::chain(8);
    let mut fixture = Fixture::open(tree, chain[7], &locators, 1);
    let frame = page_frame(
        &fixture.tree,
        fixture.id,
        chain[7].hash,
        chain[5],
        &chain[6..8],
        true,
        AuxSchema::None,
    );
    let (page, len) = decode_page(&frame);
    assert_eq!(
        fixture.sub.receive_page(&page, len),
        Err(SubscriptionFault::OverCredit.into())
    );
}

/// A path that reaches the target's height without the target never reaches
/// it: a first page from a sent locator above the target, or a page whose
/// last header sits at the target's height under another hash.
#[test]
fn a_path_to_the_target_height_without_it_faults() {
    let (mut tree, chain) = chain(8);
    let beside = tree.extend(chain[6], 1);
    let above = tree.extend(chain[8], 1);
    // chain[8] descends from the target, chain[7]. chain[5] is its ancestor.
    let locators = [chain[8], chain[5], chain[0]];
    let mut fixture = Fixture::open(tree, chain[7], &locators, 100);
    assert_eq!(
        fixture.page(chain[8], &above, false),
        Err(SubscriberFault::PastTarget)
    );
    assert_eq!(
        fixture.page(chain[5], &[chain[6], beside[0]], false),
        Err(SubscriberFault::PastTarget)
    );

    // A `Status` that put the target one height off: the page that reaches
    // the target at its real height faults.
    for advertised in [6, 8] {
        let (tree, chain) = self::chain(8);
        let misplaced = Status {
            selected_tip_height: block::Height(advertised),
            ..status(chain[7])
        };
        let mut fixture = Fixture::open_with(
            tree,
            &misplaced,
            &[chain[5], chain[0]],
            AuxSchema::None,
            100,
        );
        assert_eq!(
            fixture.page(chain[5], &chain[6..8], true),
            Err(SubscriberFault::TargetHeight),
            "the target at height 7 where {advertised} was advertised"
        );
    }
}

/// A page must fit the header count and bytes of the latest `Status` before
/// it. A later `Status` that lowers a cap binds the pages after it, even if
/// an earlier one allowed them, and one that raises both lets larger pages
/// through.
#[test]
fn pages_fit_the_latest_advertised_caps() {
    let (tree, chain) = chain(4);
    let advertise = |headers, max_bytes| Status {
        max_headers_per_response: headers,
        max_message_bytes: max_bytes,
        ..status(chain[4])
    };
    let caps = advertise(1, page_bytes(2, AuxSchema::None));
    let mut fixture = Fixture::open_with(tree, &caps, &[chain[0]], AuxSchema::None, 100);
    assert_eq!(
        fixture.page(chain[0], &chain[1..3], false),
        Err(SubscriberFault::AboveAdvertised),
        "two headers where one was advertised"
    );
    assert_eq!(
        fixture.page(chain[0], &chain[1..4], false),
        Err(SubscriberFault::AboveAdvertised),
        "three headers' bytes where two were advertised"
    );
    fixture.sub.observe(&status(chain[4]));
    fixture
        .sub
        .observe(&advertise(3, page_bytes(1, AuxSchema::None)));
    assert_eq!(
        fixture.page(chain[0], &chain[1..3], false),
        Err(SubscriberFault::AboveAdvertised),
        "the latest Status lowered the byte cap"
    );
    fixture.sub.observe(&status(chain[4]));
    fixture
        .page(chain[0], &chain[1..4], false)
        .expect("the latest Status raised both caps");
}

/// Each outcome is legal exactly in its window: before the first page, before
/// the target, at any time, or after `Close`. Outside it, or for another
/// subscription or target, it faults.
#[test]
fn outcomes_fault_outside_their_window() {
    use HeadersOutcomeCode::*;
    // (pages, reached, close sent)
    let states = [
        (0, false, false),
        (0, false, true),
        (1, false, false),
        (2, true, false),
        (2, true, true),
    ];
    for code in OUTCOME_CODES {
        for (pages, reached, close) in states {
            let (tree, chain) = chain(6);
            let mut fixture = Fixture::open(tree, chain[6], &[chain[3], chain[0]], 100);
            if pages >= 1 {
                fixture
                    .page(chain[3], &chain[4..5], false)
                    .expect("the first page");
            }
            if pages >= 2 {
                fixture
                    .page(chain[4], &chain[5..7], true)
                    .expect("the target page");
            }
            assert_eq!(
                fixture.sub.live.as_ref().map(|live| live.reached),
                Some(reached)
            );
            if close {
                wire(&fixture.sub.close().expect("Close is recorded"));
            }
            let legal = match code {
                TargetNotRetained | NoLocatorIntersection | TargetNotSelected => pages == 0,
                HistoryPruned => !reached,
                SubscriptionSuperseded => true,
                SubscriptionClosed => close,
            };
            let result = fixture.outcome(code);
            if legal {
                let ended = result.expect("legal in its window");
                assert_eq!((ended.outcome, ended.pages), (code, pages));
                assert_eq!(fixture.sub.live(), None);
                assert_eq!(
                    fixture.outcome(code),
                    Err(SubscriptionFault::Unknown.into()),
                    "an outcome ends its subscription once"
                );
            } else {
                assert_eq!(result, Err(SubscriberFault::OutcomeWindow(code)));
            }
        }
    }

    // Another target, or no subscription.
    let (tree, chain) = chain(6);
    let mut fixture = Fixture::open(tree, chain[6], &[chain[3]], 100);
    assert_eq!(
        fixture
            .sub
            .receive_outcome(&outcome(fixture.id, chain[5].hash, SubscriptionSuperseded)),
        Err(SubscriberFault::Target)
    );
    assert_eq!(
        subscriber().receive_outcome(&outcome(1, chain[6].hash, SubscriptionSuperseded)),
        Err(SubscriptionFault::Unknown.into())
    );
}

/// A pruned publisher sends the schema-1 prefix it has records for, then
/// ends with `HistoryPruned` before the target. The outcome frees the slot and
/// the credit and reports the accepted cursor, so a reopen without records
/// continues from the validated prefix. Each subscription keeps its own
/// schema, and `HistoryPruned` stays out of window once the target is reached.
#[test]
fn history_pruned_before_the_target_reopens_without_records() {
    let (tree, chain) = chain(6);
    let mut fixture = Fixture::open_with(tree, &status(chain[6]), &[chain[0]], AuxSchema::V1, 100);
    let accepted = fixture
        .page(chain[0], &chain[1..3], false)
        .expect("records exist this far");
    fixture.schema = AuxSchema::None;
    assert_eq!(
        fixture.page(chain[2], &chain[3..4], false),
        Err(SubscriberFault::Schema),
        "a schema-1 subscription takes no plain page"
    );
    fixture.schema = AuxSchema::V1;
    let received = fixture
        .page(chain[2], &chain[3..4], false)
        .expect("the last page with records");
    assert_eq!(
        fixture
            .sub
            .accept_through(fixture.id, &accepted.cursor, 100),
        Ok(None)
    );
    let ended = fixture
        .outcome(HeadersOutcomeCode::HistoryPruned)
        .expect("legal before the target");
    assert_eq!(
        (ended.pages, ended.reached, ended.accepted),
        (2, false, accepted.cursor)
    );
    assert_ne!(ended.accepted, received.cursor);
    assert_eq!(
        fixture.sub.tools.lock().len(),
        0,
        "the credit and slot are free"
    );

    let start = Frontier::new(ended.accepted.height, ended.accepted.hash);
    let open = fixture.reopen(&[start, chain[0]], AuxSchema::None, 100);
    assert_eq!(open.tree_aux_schema, AuxSchema::None);
    assert_eq!(
        (open.acknowledged.height, open.acknowledged.hash),
        (start.height, start.hash)
    );
    assert_eq!(open.added_credit.objects, 100, "a fresh window");
    fixture.schema = AuxSchema::V1;
    assert_eq!(
        fixture.page(start, &chain[3..5], false),
        Err(SubscriberFault::Schema),
        "a plain subscription takes no schema-1 page"
    );
    fixture.schema = AuxSchema::None;
    fixture
        .page(start, &chain[3..5], false)
        .expect("the reopen continues from the accepted cursor");
    fixture
        .page(chain[4], &chain[5..7], true)
        .expect("ends at the target");
    assert_eq!(
        fixture.outcome(HeadersOutcomeCode::HistoryPruned),
        Err(SubscriberFault::OutcomeWindow(
            HeadersOutcomeCode::HistoryPruned
        ))
    );
}

/// A terminal outcome can arrive while pages wait for validation, or while a
/// validation result is still pending. `Ended::accepted` covers only accepted
/// pages, never the received cursor, and a result that lands after the
/// outcome is refused, even once a new subscription is live.
#[test]
fn an_outcome_racing_validation_reports_only_accepted_pages() {
    // A page received and not yet validated.
    let (tree, chain) = chain(6);
    let mut fixture = Fixture::open(tree, chain[6], &[chain[0]], 100);
    let received = fixture
        .page(chain[0], &chain[1..3], false)
        .expect("first page");
    let ended = fixture
        .outcome(HeadersOutcomeCode::SubscriptionSuperseded)
        .expect("legal at any time");
    assert_eq!(ended.pages, 1);
    assert_eq!(
        ended.accepted,
        Cursor {
            height: chain[0].height,
            hash: chain[0].hash,
            headers: 0,
            bytes: 0
        },
        "the start, not the received cursor"
    );
    assert_eq!(
        fixture
            .sub
            .accept_through(fixture.id, &received.cursor, 100),
        Err(SubscribeError::Tools(SubscribeRefused::Unknown))
    );

    // One page validated, the next one's result still pending.
    let (tree, chain) = self::chain(6);
    let mut fixture = Fixture::open(tree, chain[6], &[chain[0]], 100);
    let validated = fixture
        .page(chain[0], &chain[1..3], false)
        .expect("first page");
    let pending = fixture
        .page(chain[2], &chain[3..5], false)
        .expect("second page");
    assert_eq!(
        fixture
            .sub
            .accept_through(fixture.id, &validated.cursor, 100),
        Ok(None)
    );
    let old = fixture.id;
    let ended = fixture
        .outcome(HeadersOutcomeCode::HistoryPruned)
        .expect("legal before the target");
    assert_eq!(ended.accepted, validated.cursor);

    // The reactor reopens before the pending result lands.
    let start = Frontier::new(validated.cursor.height, validated.cursor.hash);
    let open = fixture.reopen(&[start, chain[0]], AuxSchema::None, 100);
    assert_eq!(
        fixture.sub.accept_through(old, &pending.cursor, 100),
        Err(SubscribeError::Tools(SubscribeRefused::Unknown))
    );
    let close = wire(&fixture.sub.close().expect("Close is recorded"));
    assert_eq!(close.subscription_id, open.subscription_id);
    assert_eq!(
        close.acknowledged, open.acknowledged,
        "the late result moved nothing"
    );
}

/// Grants refill the window in prefixes of half of it, never pass the
/// staging budget, always meet the `Grant` minimum, and acknowledge the
/// accepted cursor with consecutive sequences. A starved subscription grants
/// as soon as there is room, so a small window never stalls.
#[test]
fn grants_refill_half_window_prefixes() {
    let (tree, chain) = chain(20);
    let mut fixture = Fixture::open(tree, chain[20], &[chain[0]], 8);
    assert_eq!(Sub::prefix(8), 4);
    assert_eq!(Sub::prefix(2), 1);
    let id = fixture.id;
    // One-header pages, and each page's cursor, by height.
    let receive = |fixture: &mut Fixture, heights: std::ops::RangeInclusive<usize>| {
        heights
            .map(|height| {
                fixture
                    .page(chain[height - 1], &chain[height..=height], false)
                    .expect("a one-header page")
                    .cursor
            })
            .collect::<Vec<_>>()
    };
    let mut cursors = receive(&mut fixture, 1..=6);

    // Three accepted headers are below the prefix, and two unspent headers
    // still fit a page.
    assert_eq!(fixture.sub.accept_through(id, &cursors[2], 8), Ok(None));
    let grant = fixture
        .sub
        .accept_through(id, &cursors[3], 8)
        .expect("a received cursor")
        .expect("four headers reach the prefix");
    let grant = wire(&grant);
    assert_eq!(grant.operation, Operation::Grant);
    assert_eq!(grant.update_sequence, 1);
    assert_eq!(grant.acknowledged, cursors[3]);
    assert_eq!(grant.added_credit.objects, 4);
    assert_eq!(
        u64::from(grant.added_credit.bytes),
        cursors[3].bytes,
        "bytes refill what the accepted pages spent"
    );

    // A smaller budget revokes nothing and waits for acceptance.
    assert_eq!(fixture.sub.accept_through(id, &cursors[5], 2), Ok(None));
    let granted = |fixture: &Fixture| {
        fixture
            .sub
            .tools
            .lock()
            .credit(&id)
            .expect("live")
            .granted()
            .objects
    };
    assert_eq!(granted(&fixture), 12, "granted credit never drops");
    cursors.extend(receive(&mut fixture, 7..=12));
    let grant = wire(
        &fixture
            .sub
            .accept_through(id, &cursors[11], 2)
            .expect("a received cursor")
            .expect("the window is empty and starved"),
    );
    assert_eq!(grant.update_sequence, 2);
    assert_eq!(grant.acknowledged, cursors[11]);
    assert_eq!(
        grant.added_credit.objects, 2,
        "below the prefix, but starved"
    );
    assert_eq!(granted(&fixture) - cursors[11].headers, 2, "the new budget");
    assert_eq!(fixture.sub.grant_due(2), None, "the window is full");
    let grant = wire(&fixture.sub.grant_due(8).expect("a larger budget refills"));
    assert_eq!(grant.update_sequence, 3);
    assert_eq!(grant.acknowledged, cursors[11]);
    assert_eq!(grant.added_credit.objects, 6);
}

/// Updates acknowledge only cursors the reactor accepted: a cursor with a
/// changed field, one already passed, or one for another subscription is
/// refused locally. A page that fails contextual validation is never
/// acknowledged: the subscription accepts no later cursor and sends no more
/// grants, and a `Close` acknowledges only what passed.
#[test]
fn a_failed_validation_is_never_acknowledged() {
    let (tree, chain) = chain(6);
    let mut fixture = Fixture::open(tree, chain[6], &[chain[0]], 4);
    let passed = fixture
        .page(chain[0], &chain[1..3], false)
        .expect("first page");
    let failed = fixture
        .page(chain[2], &chain[3..5], false)
        .expect("second page");
    let id = fixture.id;
    let changed = Cursor {
        bytes: passed.cursor.bytes - 1,
        ..passed.cursor
    };
    assert_eq!(
        fixture.sub.accept_through(id, &changed, 4),
        Err(SubscribeError::Tools(SubscribeRefused::UnknownCursor))
    );
    assert_eq!(
        fixture.sub.accept_through(id + 1, &passed.cursor, 4),
        Err(SubscribeError::Tools(SubscribeRefused::Unknown))
    );
    let grant = fixture
        .sub
        .accept_through(id, &passed.cursor, 4)
        .expect("a received cursor")
        .expect("the two pages starved the window");
    assert_eq!(wire(&grant).acknowledged, passed.cursor);
    assert_eq!(
        fixture.sub.accept_through(id, &passed.cursor, 4),
        Err(SubscribeError::Tools(SubscribeRefused::UnknownCursor)),
        "a passed cursor"
    );
    assert!(fixture.sub.validation_failed(id));
    assert_eq!(
        fixture.sub.accept_through(id, &failed.cursor, 4),
        Err(SubscribeError::Failed)
    );
    // Eight headers of staging leave room for a full prefix of four, so only
    // the failure holds this grant back.
    assert_eq!(fixture.sub.grant_due(8), None);
    let close = wire(&fixture.sub.close().expect("Close is recorded"));
    assert_eq!(close.update_sequence, 2);
    assert_eq!(close.acknowledged, passed.cursor);
    assert!(!fixture.sub.validation_failed(id + 1));
}

/// Local failover keeps the subscription's credit: after `Close`, pages
/// within it are admitted, no `Grant` follows, and `SubscriptionClosed` ends
/// it.
#[test]
fn close_keeps_credit_and_admits_pages() {
    let (tree, chain) = chain(6);
    let mut fixture = Fixture::open(tree, chain[6], &[chain[0]], 4);
    let page = fixture
        .page(chain[0], &chain[1..3], false)
        .expect("first page");
    let close = wire(&fixture.sub.close().expect("Close is recorded"));
    assert_eq!(close.operation, Operation::Close);
    let after = fixture
        .page(chain[2], &chain[3..5], false)
        .expect("credit stays after Close");
    assert!(after.after_close);
    assert_eq!(
        fixture.sub.accept_through(fixture.id, &page.cursor, 4),
        Ok(None)
    );
    assert_eq!(fixture.sub.grant_due(4), None, "no Grant after Close");
    let ended = fixture
        .outcome(HeadersOutcomeCode::SubscriptionClosed)
        .expect("legal after Close");
    assert_eq!(ended.accepted, page.cursor);
}

/// An `Open` holds its session's exchange until the terminal outcome. A
/// session that ends after the `Open`'s first byte closes the connection, an
/// outcome releases the exchange, and retracting an `Open` closes the
/// connection only if a byte was written. A retired fence opens nothing.
#[test]
fn an_open_holds_its_exchange_until_the_outcome() {
    let (_tree, chain) = chain(2);
    let open = |sub: &mut Sub, written: bool| {
        let (open, writer) = sub
            .open(&status(chain[2]), &[chain[0]], AuxSchema::None, 4)
            .expect("recorded");
        if written {
            assert!(writer.publish(|| {}) && writer.try_start(|| true));
        }
        open.subscription_id
    };

    let connection = CancellationToken::new();
    let mut sub = fenced(&connection);
    open(&mut sub, true);
    drop(sub);
    assert!(
        connection.is_cancelled(),
        "a written Open lost its receiver"
    );

    let connection = CancellationToken::new();
    let mut sub = fenced(&connection);
    let id = open(&mut sub, true);
    sub.receive_outcome(&outcome(
        id,
        chain[2].hash,
        HeadersOutcomeCode::TargetNotSelected,
    ))
    .expect("legal before the first page");
    let id = open(&mut sub, false);
    assert!(sub.retract(id));
    assert_eq!(sub.live(), None);
    drop(sub);
    assert!(!connection.is_cancelled());

    let connection = CancellationToken::new();
    let mut sub = fenced(&connection);
    let id = open(&mut sub, true);
    assert!(sub.retract(id));
    assert!(
        connection.is_cancelled(),
        "the peer may answer a written Open"
    );

    let fence = WriterFence::new(CancellationToken::new(), CloseCause::default());
    let mut sub = Sub::new(HB, fence.clone(), &framed_channel(1).1);
    assert!(fence.retire());
    assert_eq!(
        sub.open(&status(chain[2]), &[chain[0]], AuxSchema::None, 4)
            .err(),
        Some(SubscribeError::Fenced)
    );
}

/// A scripted publisher's view of its subscription.
#[derive(Debug)]
struct Served {
    id: u64,
    target: block::Hash,
    schema: AuxSchema,
    /// The header the next page extends: the chosen locator, then the last
    /// header sent, or the target when a locator reached it.
    last: Frontier,
    reached: bool,
    end: Option<HeadersOutcomeCode>,
}

/// A conformant publisher over the real codec and its own ledger.
struct Script {
    ledger: Publications<u64, Cursor>,
    served: Option<Served>,
    tip: Frontier,
}

impl Script {
    /// A publisher whose selected tip is `tip`.
    fn new(tip: Frontier) -> Self {
        let limits = SubscriptionLimits::from_rule(&SUBSCRIBE_HEADERS)
            .expect("SUBSCRIBE_HEADERS is a subscription row");
        Self {
            ledger: Publications::new(limits),
            served: None,
            tip,
        }
    }

    /// Apply one update, which must never fault.
    fn apply(&mut self, tree: &HeaderTree, update: &SubscribeHeaders) {
        let id = update.subscription_id;
        match update.operation {
            Operation::Open => {
                self.ledger
                    .open(
                        id,
                        update.update_sequence,
                        update.added_credit,
                        update.acknowledged,
                    )
                    .expect("a conformant Open never faults");
                let mut served = Served {
                    id,
                    target: update.initial_target_tip_hash,
                    schema: update.tree_aux_schema,
                    last: tree.genesis,
                    reached: false,
                    end: None,
                };
                match tree.nodes.get(&update.initial_target_tip_hash) {
                    None => served.end = Some(HeadersOutcomeCode::TargetNotRetained),
                    // The selected chain moved off the target since `Status`.
                    Some(&(target, _))
                        if tree.ancestor(self.tip, target.height) != Some(target) =>
                    {
                        served.end = Some(HeadersOutcomeCode::TargetNotSelected)
                    }
                    Some(&(target, _)) => {
                        let chosen = update.locator_hashes.iter().find_map(|hash| {
                            let &(locator, _) = tree.nodes.get(hash)?;
                            (tree.ancestor(target, locator.height) == Some(locator))
                                .then_some(locator)
                        });
                        match chosen {
                            None => served.end = Some(HeadersOutcomeCode::NoLocatorIntersection),
                            Some(locator) => {
                                served.last = locator;
                                served.reached = locator == target;
                            }
                        }
                    }
                }
                self.served = Some(served);
            }
            Operation::Grant => {
                self.ledger
                    .grant(
                        &id,
                        update.update_sequence,
                        &update.acknowledged,
                        update.added_credit,
                    )
                    .expect("a conformant Grant never faults");
            }
            Operation::Close => {
                self.ledger
                    .close(&id, update.update_sequence, &update.acknowledged)
                    .expect("a conformant Close never faults");
            }
        }
    }

    /// End the subscription with `code` if `code` is legal now.
    fn end(&mut self, code: HeadersOutcomeCode) {
        let Some(served) = self.served.as_mut() else {
            return;
        };
        let pages = self
            .ledger
            .credit(&served.id)
            .is_some_and(|credit| credit.consumed().objects > 0);
        if served.end.is_none() && is_legal(code, pages, served.reached, false) {
            served.end = Some(code);
        }
    }

    /// Write up to `pages` pages that the credit allows, and any terminal
    /// outcome that is due.
    fn pump(&mut self, tree: &HeaderTree, out: &mut VecDeque<Frame>, mut pages: usize) {
        loop {
            let Some(served) = self.served.as_mut() else {
                return;
            };
            if self.ledger.is_closing(&served.id) && served.end.is_none() {
                served.end = Some(HeadersOutcomeCode::SubscriptionClosed);
            }
            if let Some(code) = served.end {
                drop(self.ledger.end(&served.id).expect("live until its outcome"));
                out.push_back(
                    encode_frame(&RegtestMessage::HeadersOutcome(outcome(
                        served.id,
                        served.target,
                        code,
                    )))
                    .expect("an outcome encodes"),
                );
                self.served = None;
                return;
            }
            if pages == 0 {
                return;
            }
            let unspent = self.ledger.unspent(&served.id).expect("live and open");
            let entry = u64::try_from(entry_bytes(HB, served.schema)).expect("fits");
            let fixed = u64::try_from(HEADERS_RESPONSE_FIXED_BYTES).expect("fits");
            let room = u64::from(PAGE_HEADERS)
                .min(unspent.objects)
                .min(unspent.bytes.saturating_sub(fixed) / entry);
            if room == 0 {
                return;
            }
            let goal = if served.reached {
                self.tip
            } else {
                tree.nodes[&served.target].0
            };
            if goal == served.last {
                return;
            }
            let Some(path) = tree.path(served.last, goal) else {
                served.end = Some(HeadersOutcomeCode::SubscriptionSuperseded);
                continue;
            };
            let count = usize::try_from(room).expect("small").min(path.len());
            let headers = &path[..count];
            let last = headers[count - 1];
            let marker = !served.reached && last.hash == served.target;
            let frame = page_frame(
                tree,
                served.id,
                served.target,
                served.last,
                headers,
                marker,
                served.schema,
            );
            let consumed = self.ledger.credit(&served.id).expect("live").consumed();
            let cursor = Cursor {
                height: last.height,
                hash: last.hash,
                headers: consumed.objects + u64::try_from(count).expect("fits"),
                bytes: consumed.bytes + u64::try_from(frame.payload.len()).expect("fits"),
            };
            self.ledger
                .reserve_page(
                    &served.id,
                    u32::try_from(count).expect("small"),
                    frame.payload.len(),
                    cursor,
                )
                .expect("the page fits the unspent credit");
            served.last = last;
            served.reached |= marker;
            out.push_back(frame);
            pages -= 1;
        }
    }
}

/// One step of a generated session.
#[derive(Clone, Debug)]
enum Op {
    /// Extend the publisher's selected chain.
    Grow(u8),
    /// Select a branch forking `depth` below the tip, `len` headers long.
    Reorg {
        depth: u8,
        len: u8,
    },
    /// Open to the selected tip `stale` tips ago, from a local chain that
    /// leaves the target's chain `depth` below it and forks for `fork`
    /// headers, with locators every `step` heights. `aim` 0 names an unknown
    /// target, and 1 sends only locators off the target's chain.
    Open {
        stale: u8,
        depth: u8,
        fork: u8,
        step: u8,
        records: bool,
        aim: u8,
    },
    /// Let the publisher write up to this many pages.
    Publish(u8),
    /// Deliver queued frames to the subscriber.
    Deliver(u8),
    /// Deliver queued updates to the publisher.
    Updates(u8),
    /// Validate and accept the oldest received pages.
    Validate(u8),
    /// Fail validation of the oldest received page.
    Fail,
    /// Change the staging budget and offer a grant.
    Staging(u16),
    Close,
    /// End the subscription with the outcome at this index, when legal.
    End(u8),
}

/// A generated operation.
fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (1..5_u8).prop_map(Op::Grow),
        1 => (1..4_u8, 0..5_u8).prop_map(|(depth, len)| Op::Reorg { depth, len }),
        3 => (0..3_u8, 0..12_u8, 0..3_u8, 1..4_u8, any::<bool>(), 0..12_u8).prop_map(
            |(stale, depth, fork, step, records, aim)| Op::Open {
                stale, depth, fork, step, records, aim
            }
        ),
        4 => (1..6_u8).prop_map(Op::Publish),
        5 => (1..6_u8).prop_map(Op::Deliver),
        4 => (1..4_u8).prop_map(Op::Updates),
        4 => (1..6_u8).prop_map(Op::Validate),
        1 => Just(Op::Fail),
        2 => prop_oneof![1..12_u16, Just(4_000_u16)].prop_map(Op::Staging),
        1 => Just(Op::Close),
        1 => (0..6_u8).prop_map(Op::End),
    ]
}

/// The reactor's side of a generated session.
struct Session {
    tree: HeaderTree,
    sub: Sub,
    script: Script,
    /// Frames to the subscriber, in stream order.
    to_subscriber: VecDeque<Frame>,
    /// Updates to the publisher, in stream order.
    to_publisher: VecDeque<SubscribeHeaders>,
    /// Admitted pages the reactor has not validated.
    unvalidated: VecDeque<Admitted>,
    /// Cursors an update may acknowledge: starts and validated pages.
    passed: Vec<(u64, Cursor)>,
    /// IDs of every `Open`.
    opened: HashSet<u64>,
    /// Selected tips, newest last.
    tips: Vec<Frontier>,
    staging: u32,
    failed: bool,
}

impl Session {
    /// Record an update the subscriber returned, and check what holds for
    /// every update.
    fn send(&mut self, update: SubscribeHeaders) {
        let update = wire(&update);
        assert!(
            self.passed
                .contains(&(update.subscription_id, update.acknowledged)),
            "an update acknowledges only a start or a validated cursor"
        );
        if update.operation == Operation::Grant {
            let tools = self.sub.tools.lock();
            let credit = tools.credit(&update.subscription_id).expect("live");
            let window = self.sub.window(self.staging, update.tree_aux_schema);
            assert!(
                credit.granted().objects - update.acknowledged.headers <= u64::from(window.objects),
                "a Grant keeps the window within the staging budget"
            );
            let live = self.sub.live.as_ref().expect("live");
            if !live.reached {
                let unspent = credit.unspent();
                let page = u64::try_from(smallest_page(HB, live.schema)).expect("fits");
                assert!(
                    unspent.objects >= 1 && unspent.bytes >= page,
                    "before the target, a Grant leaves credit for a page"
                );
            }
        }
        self.to_publisher.push_back(update);
    }

    /// Deliver up to `count` frames to the subscriber. None may fault.
    fn deliver(&mut self, count: usize) {
        for _ in 0..count {
            let Some(frame) = self.to_subscriber.pop_front() else {
                return;
            };
            self.sub
                .admit(&frame)
                .expect("a conformant frame header passes");
            match decode_frame::<RegtestMessage>(&frame).expect("the publisher writes valid frames")
            {
                Message::Headers(page) => {
                    let admitted = self
                        .sub
                        .receive_page(&page, frame.payload.len())
                        .expect("a conformant page never faults");
                    self.unvalidated.push_back(admitted);
                }
                Message::HeadersOutcome(outcome) => {
                    self.sub
                        .receive_outcome(&outcome)
                        .expect("a conformant outcome never faults");
                    self.unvalidated
                        .retain(|admitted| admitted.id != outcome.subscription_id);
                    self.failed = false;
                }
                other => panic!("the publisher wrote {other:?}"),
            }
        }
    }

    /// Deliver up to `count` updates to the publisher, then let it write.
    fn updates(&mut self, count: usize) {
        for _ in 0..count {
            let Some(update) = self.to_publisher.pop_front() else {
                break;
            };
            self.script.apply(&self.tree, &update);
        }
        self.script.pump(&self.tree, &mut self.to_subscriber, 0);
    }

    /// Run one generated step.
    fn step(&mut self, op: Op) {
        match op {
            Op::Grow(count) => {
                let added = self.tree.extend(self.script.tip, u32::from(count));
                self.script.tip = *added.last().expect("at least one header");
                self.tips.push(self.script.tip);
            }
            Op::Reorg { depth, len } => {
                let tip = self.script.tip;
                let height = block::Height(tip.height.0.saturating_sub(u32::from(depth)));
                let base = self.tree.ancestor(tip, height).expect("on the chain");
                let added = self.tree.extend(base, u32::from(len));
                self.script.tip = added.last().copied().unwrap_or(base);
                self.tips.push(self.script.tip);
            }
            Op::Open {
                stale,
                depth,
                fork,
                step,
                records,
                aim,
            } => {
                let stale = usize::from(stale).min(self.tips.len() - 1);
                let mut target = self.tips[self.tips.len() - 1 - stale];
                let height = block::Height(target.height.0.saturating_sub(u32::from(depth)));
                let base = self.tree.ancestor(target, height).expect("on the chain");
                let forked = self
                    .tree
                    .extend(base, u32::from(fork).max(u32::from(aim == 1)));
                let local = forked.last().copied().unwrap_or(base);
                let mut locators = self.tree.locators(local, u32::from(step));
                match aim {
                    0 => target.hash = block::Hash([0xee; 32]),
                    1 => locators = forked.into_iter().rev().collect(),
                    _ => {}
                }
                let schema = if records {
                    AuxSchema::V1
                } else {
                    AuxSchema::None
                };
                let live = self.sub.live();
                match self
                    .sub
                    .open(&status(target), &locators, schema, self.staging)
                {
                    Ok((open, _)) => {
                        assert!(live.is_none());
                        assert!(
                            self.opened.insert(open.subscription_id),
                            "every Open has a new ID"
                        );
                        self.passed.push((open.subscription_id, open.acknowledged));
                        self.send(open);
                    }
                    Err(refused) => {
                        assert!(live.is_some(), "only a live subscription refuses an Open");
                        assert_eq!(refused, SubscribeError::Tools(SubscribeRefused::AtCapacity));
                    }
                }
            }
            Op::Publish(count) => {
                self.script
                    .pump(&self.tree, &mut self.to_subscriber, usize::from(count))
            }
            Op::Deliver(count) => self.deliver(usize::from(count)),
            Op::Updates(count) => self.updates(usize::from(count)),
            Op::Validate(count) => {
                if self.failed {
                    return;
                }
                let count = usize::from(count).min(self.unvalidated.len());
                let Some(last) = self.unvalidated.drain(..count).next_back() else {
                    return;
                };
                self.passed.push((last.id, last.cursor));
                match self.sub.accept_through(last.id, &last.cursor, self.staging) {
                    Ok(Some(grant)) => self.send(grant),
                    Ok(None) => {}
                    Err(error) => panic!("a received cursor is accepted: {error}"),
                }
            }
            Op::Fail => {
                if let Some(page) = self.unvalidated.front() {
                    assert!(self.sub.validation_failed(page.id));
                    let cursor = page.cursor;
                    assert_eq!(
                        self.sub.accept_through(page.id, &cursor, self.staging),
                        Err(SubscribeError::Failed)
                    );
                    self.failed = true;
                }
            }
            Op::Staging(staging) => {
                self.staging = u32::from(staging);
                if let Some(grant) = self.sub.grant_due(self.staging) {
                    self.send(grant);
                }
            }
            Op::Close => {
                if let Ok(close) = self.sub.close() {
                    self.send(close);
                }
            }
            Op::End(index) => self
                .script
                .end(OUTCOME_CODES[usize::from(index) % OUTCOME_CODES.len()]),
        }
        self.script.pump(&self.tree, &mut self.to_subscriber, 0);
    }
}

/// Run `ops`, then close and drain both directions.
fn run(ops: Vec<Op>) {
    let mut tree = HeaderTree::new();
    let genesis = tree.genesis;
    let tip = *tree.extend(genesis, 4).last().expect("four headers");
    let mut session = Session {
        tree,
        sub: subscriber(),
        script: Script::new(tip),
        to_subscriber: VecDeque::new(),
        to_publisher: VecDeque::new(),
        unvalidated: VecDeque::new(),
        passed: Vec::new(),
        opened: HashSet::new(),
        tips: vec![tip],
        staging: 6,
        failed: false,
    };
    for op in ops {
        session.step(op);
    }
    if let Ok(close) = session.sub.close() {
        session.send(close);
    }
    for _ in 0..10_000 {
        if session.to_publisher.is_empty() && session.to_subscriber.is_empty() {
            break;
        }
        session.updates(usize::MAX);
        session
            .script
            .pump(&session.tree, &mut session.to_subscriber, usize::MAX);
        session.deliver(usize::MAX);
    }
    assert_eq!(session.sub.live(), None, "Close ends every subscription");
    assert!(session.script.served.is_none());
    assert_eq!(session.script.ledger.keys().count(), 0);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(std::env::var("HEADER_SYNC_PROPTEST_CASES").ok().and_then(|cases| cases.parse().ok()).unwrap_or(512)))]

    /// A conformant publisher's pages and outcomes never fault the subscriber,
    /// and the subscriber's updates never fault the publisher's ledger, under
    /// any interleaving, reorganization, staging change, or failed
    /// validation. Updates acknowledge only validated cursors, grants stay
    /// within the staging budget and leave a page of credit before the
    /// target, and every subscription ends once.
    #[test]
    fn conformant_sessions_never_fault(ops in prop::collection::vec(op(), 1..60)) {
        run(ops);
    }
}
