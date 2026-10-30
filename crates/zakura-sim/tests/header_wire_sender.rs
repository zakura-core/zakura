//! M5-E2: the sender half of M5.3. The node's own frames on
//! `header-sync/wire` are labelled by the sender adapter under the sender's
//! strict limits, causally: each against the peer updates the node had
//! applied when it started writing the frame. Honest runs keep every
//! applicable sender template, and crafted histories fed to the adapter
//! show it labels each sender violation and fails its property.
#![cfg(zakura_sim)]

mod support;

use std::{sync::Arc, time::Duration};

use support::{
    inputs,
    script::{run_under, Node, OBSERVED},
};
use zakura_chain::block;
use zakura_network::zakura::{
    v9::{
        Cursor, Headers, HeadersOutcome, HeadersOutcomeCode, Message, Operation, RegtestMessage,
        Status, SubscribeHeaders,
    },
    wire_codec::encode_frame,
    AuxSchema, Credit, Frame, HeaderEntry, MessageRole,
};
use zakura_sim::{
    harness::{
        header_node::{Chain, Policy},
        header_sync::{HeaderWire, WireScenario},
        run_prepared, HarnessError, Subsystem,
    },
    model::{Label, SubscriptionModel},
    observe::{TransitionFact, TransportFact},
    peer::{
        adapter::SenderAdapter,
        honest::{CreditSpec, GrantPolicy, HonestSpec, SubscribeSpec},
        PeerAction, PeerSpec,
    },
    table::{sender_properties, GeneratedId, Template},
    MemorySink, Observation, Plan, PlanInput, PropertyOutcome, RunReport,
};
use zakura_storage::header::fixture::HeaderFixture;

/// `status/sender-cadence`.
const CADENCE: &str = "header-sync/s5v9/status/sender-cadence";
/// `subscribe-headers/sender-within-live`.
const LIVE: &str = "header-sync/s5v9/subscribe-headers/sender-within-live";
/// `subscribe-headers/sender-within-credit`.
const CREDIT: &str = "header-sync/s5v9/subscribe-headers/sender-within-credit";
/// `headers/sender-answers-open`.
const ANSWERS: &str = "header-sync/s5v9/headers/sender-answers-open";
/// `headers-outcome/sender-ends-once`.
const ENDS: &str = "header-sync/s5v9/headers-outcome/sender-ends-once";

/// The sender templates header sync has no row for, with the row they'd
/// be on, `None` for any: it has no request row, so no
/// `sender-within-limit`, and only `status` has a cadence, so
/// `subscribe-headers`, `headers` and `headers-outcome` have no
/// `sender-cadence`.
const NOT_APPLICABLE: [(Option<&str>, Template); 4] = [
    (None, Template::SenderWithinLimit),
    (Some("subscribe-headers"), Template::SenderCadence),
    (Some("headers"), Template::SenderCadence),
    (Some("headers-outcome"), Template::SenderCadence),
];

/// One plain one-header Regtest page: 82 fixed bytes and a 181-byte entry.
const ONE_HEADER_PAGE: usize = 263;

/// The node's chain at the start of the honest runs.
const PRELOAD: u32 = 50;

/// The plans the honest runs take: all defaults, then two byte-chosen.
fn plans() -> Vec<PlanInput> {
    let mut plans = vec![Plan::from_fuzz(&[])];
    plans.extend(inputs(2, 16_384));
    plans
}

/// Runs `scenario` under `plan`, and returns the report even of a run that
/// ended in a harness failure, so the test can say what failed.
fn run(scenario: &WireScenario, plan: PlanInput) -> RunReport {
    let ports = HeaderWire::prepare(scenario);
    let ran = run_prepared::<HeaderWire>(
        scenario,
        ports.clone(),
        plan,
        &mut MemorySink::default(),
        None,
    );
    let report = match ran {
        Ok(report) => report,
        Err(HarnessError::Run { report, .. }) => *report,
        Err(error) => panic!("{error}"),
    };
    assert_eq!(ports.unplaced_records(), 0, "every peer record was logged");
    report
}

/// An honest peer holding `chain` headers, as `spec`.
fn honest(spec: PeerSpec, chain: u32, subscribe: Option<SubscribeSpec>) -> PeerSpec {
    PeerSpec {
        honest: Some(HonestSpec { chain, subscribe }),
        ..spec
    }
}

/// The outcome `report` gives `id`.
fn outcome<'a>(report: &'a RunReport, id: &str) -> &'a PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|property| property.property == id)
        .unwrap_or_else(|| panic!("{id} is registered"))
        .outcome
}

/// True if `id` held and applied at least once.
fn held(report: &RunReport, id: &str) -> bool {
    matches!(outcome(report, id), PropertyOutcome::Held { applicable } if *applicable > 0)
}

/// The property that stopped the run, if any.
fn stopped_by(report: &RunReport) -> Option<&str> {
    report.end.violation.as_ref().map(|v| v.property.as_str())
}

/// Checks that `report`'s node frames are each conformant, unexhausted,
/// and logged right after the `PeerReceived` of the same frame, one each.
fn every_node_frame_is_conformant(report: &RunReport) {
    let entries = report.log.entries();
    let mut received = 0;
    for (index, entry) in entries.iter().enumerate() {
        let Observation::PeerReceived {
            connection,
            quic_stream,
            seq,
            ..
        } = entry.obs
        else {
            continue;
        };
        received += 1;
        let next = entries.get(index + 1).map(|entry| &entry.obs);
        let Some(Observation::NodeFrame {
            connection: c,
            quic_stream: q,
            seq: s,
            label,
            exhausted,
            ..
        }) = next
        else {
            panic!("frame {seq} on {connection}/{quic_stream} has no NodeFrame: {next:?}");
        };
        assert_eq!((*c, *q, *s), (connection, Some(quic_stream), seq));
        assert_eq!(*label, Label::Conformant, "{next:?}");
        assert!(!exhausted, "{next:?}");
    }
    let labelled = entries
        .iter()
        .filter(|entry| matches!(entry.obs, Observation::NodeFrame { .. }))
        .count();
    assert!(received > 0, "the peer read the node's frames");
    assert_eq!(labelled, received, "one NodeFrame per frame read");
}

/// Over honest runs, every frame the node sends is `Conformant` under the
/// sender's limits, and each of header sync's sender templates holds where
/// it applies. Templates for roles header sync doesn't have are
/// [`NOT_APPLICABLE`].
///
/// - `peer-0` serves the 120-header tree to a node at 50, so the node
///   subscribes and grants, and subscribes to the node from genesis, so the
///   node publishes pages and ends the subscription when `peer-0` closes
///   it. All five templates apply.
/// - `peer-0` subscribes to a node at 1,200 with a held grant, once with
///   one object and two pages of bytes, once with two objects and one page
///   of bytes, so the node publishes right up to each balance and then
///   waits for the grant.
///
/// This fails if the adapter labels a conformant node frame as a fault: an
/// adapter that never applies `grant-applied` labels the first run's pages
/// past its `Open`'s credit `OverCredit`.
#[test]
fn node_frames_obey_the_sender_limits() {
    let ids: Vec<String> = sender_properties::<Node>("header-sync", &RegtestMessage::LAYOUT)
        .into_iter()
        .map(|property| property.id.to_string())
        .collect();
    assert_eq!(ids, [CADENCE, LIVE, CREDIT, ANSWERS, ENDS]);
    for id in &ids {
        let parsed = GeneratedId::parse(id).expect("a generated ID parses back");
        for (row, template) in NOT_APPLICABLE {
            let on_row = row.is_none_or(|row| parsed.row == Some(row));
            assert!(
                !(parsed.template == template && on_row),
                "{id} is generated, but header sync has no {template:?} on {row:?}"
            );
        }
    }
    assert!(
        RegtestMessage::ROWS
            .iter()
            .all(|row| !matches!(row.role, MessageRole::Request { .. })),
        "header sync has no request row"
    );

    let both_ways = WireScenario {
        node: Chain::main(PRELOAD),
        peers: vec![honest(
            PeerSpec::listening(),
            120,
            Some(SubscribeSpec {
                start: 0,
                open: CreditSpec {
                    objects: 250,
                    bytes: 65_750,
                },
                grants: GrantPolicy::Immediate,
            }),
        )],
        policy: Policy::converging(),
        ..WireScenario::default()
    };
    for plan in plans() {
        let report = run(&both_ways, plan);
        assert_eq!(report.end.violation, None, "{:?}", report.end);
        every_node_frame_is_conformant(&report);
        for id in [CADENCE, LIVE, CREDIT, ANSWERS, ENDS] {
            assert!(held(&report, id), "{id}: {:?}", outcome(&report, id));
        }
    }

    for credit in [
        CreditSpec {
            objects: 1,
            bytes: 526,
        },
        CreditSpec {
            objects: 2,
            bytes: 263,
        },
    ] {
        let mut peer = honest(
            PeerSpec::dialing(),
            PRELOAD,
            Some(SubscribeSpec {
                start: PRELOAD,
                open: credit,
                grants: GrantPolicy::Held {
                    grant: credit,
                    quiet_ms: 5_000,
                },
            }),
        );
        peer.script = vec![PeerAction::Grant];
        let slow = WireScenario {
            node: Chain::main(1_200),
            peers: vec![peer],
            policy: Policy::converging(),
            ..WireScenario::default()
        };
        for plan in plans() {
            let report = run(&slow, plan);
            assert_eq!(report.end.violation, None, "{:?}", report.end);
            every_node_frame_is_conformant(&report);
            let pages = report
                .log
                .entries()
                .iter()
                .filter(|entry| {
                    matches!(&entry.obs, Observation::NodeFrame { row: Some(row), .. } if row == "headers")
                })
                .count();
            assert!(pages >= 2, "the node paged after the grant: {pages}");
            for id in [CADENCE, ANSWERS] {
                assert!(held(&report, id), "{id}: {:?}", outcome(&report, id));
            }
        }
    }
}

/// The connection crafted histories start on.
const CONNECTION: u64 = 1;

/// The QUIC stream crafted histories start on.
const QUIC_STREAM: u64 = 4;

/// The subscription crafted histories open.
const SUB: u64 = 1;

/// A crafted node-side history, fed to the sender adapter as the wire
/// evidence would feed it.
struct History {
    adapter: SenderAdapter,
    /// The `NodeFrame`s so far.
    frames: Vec<Observation>,
    /// The connection and QUIC stream the next events are on.
    connection: u64,
    quic_stream: u64,
    /// When the next read happens, since the run's start.
    at: Duration,
}

impl History {
    /// An empty history.
    fn new() -> Self {
        History {
            adapter: SenderAdapter::new(),
            frames: Vec::new(),
            connection: CONNECTION,
            quic_stream: QUIC_STREAM,
            at: Duration::ZERO,
        }
    }

    /// Moves the next events to `connection`'s `quic_stream`.
    fn on(&mut self, connection: u64, quic_stream: u64) -> &mut Self {
        self.connection = connection;
        self.quic_stream = quic_stream;
        self
    }

    /// The node applied `transition`.
    fn applied(&mut self, transition: TransitionFact) -> &mut Self {
        self.adapter.node(
            self.connection,
            &TransportFact::Regulated {
                quic_stream: self.quic_stream,
                transition,
            },
        );
        self
    }

    /// The node applied the peer's `Open` of [`SUB`] with this credit.
    fn open_applied(&mut self, objects: u32, bytes: u32) -> &mut Self {
        self.applied(TransitionFact::OpenApplied {
            subscription: SUB,
            objects,
            bytes,
            seq: Some(0),
        })
    }

    /// The node applied the peer's `Grant` on [`SUB`].
    fn grant_applied(&mut self, objects: u32, bytes: u32) -> &mut Self {
        self.applied(TransitionFact::GrantApplied {
            subscription: SUB,
            objects,
            bytes,
        })
    }

    /// The node started writing `frame` as its `seq`-th.
    fn started(&mut self, seq: u64, frame: &Frame) -> &mut Self {
        self.adapter.node(
            self.connection,
            &TransportFact::FrameWriteStarted {
                quic_stream: self.quic_stream,
                seq,
                message_type: frame.message_type,
            },
        );
        self
    }

    /// The node finished writing `frame` as its `seq`-th.
    fn written(&mut self, seq: u64, frame: &Frame) -> &mut Self {
        self.adapter.node(
            self.connection,
            &TransportFact::FrameWritten {
                quic_stream: self.quic_stream,
                seq,
                message_type: frame.message_type,
            },
        );
        self
    }

    /// The node wrote `frame` as its `seq`-th, start to finish.
    fn wrote(&mut self, seq: u64, frame: &Frame) -> &mut Self {
        self.started(seq, frame).written(seq, frame)
    }

    /// The peer read the node's `seq`-th frame, `frame`. Returns its label.
    fn read(&mut self, seq: u64, frame: &Frame) -> Label {
        let node_frame = self
            .adapter
            .received(self.connection, self.quic_stream, seq, frame, self.at)
            .unwrap_or_else(|why| panic!("{why}"));
        let Observation::NodeFrame { label, .. } = node_frame else {
            panic!("the adapter logs a NodeFrame: {node_frame:?}");
        };
        self.frames.push(node_frame);
        label
    }

    /// The node wrote `frame` as its `seq`-th and the peer read it.
    fn sent(&mut self, seq: u64, frame: &Frame) -> Label {
        self.wrote(seq, frame);
        self.read(seq, frame)
    }

    /// The node frames, checked by header sync's sender properties.
    fn check(&self) -> RunReport {
        run_under(
            sender_properties::<Node>("header-sync", &RegtestMessage::LAYOUT),
            OBSERVED,
            vec![self.frames.clone()],
        )
    }
}

/// Encodes `message` as the node writes it.
fn frame(message: RegtestMessage) -> Frame {
    encode_frame(&message).expect("a crafted node frame is valid")
}

/// The fixture's first `count` headers after its anchor.
fn headers(count: u32) -> (block::Hash, Vec<Arc<block::Header>>) {
    let fixture = HeaderFixture::regtest();
    let anchor = fixture.anchor();
    (anchor.hash, fixture.extend(anchor, count, 0))
}

/// The node's plain page of header `height` for `subscription`.
fn page(subscription: u64, height: u32) -> Frame {
    let (anchor, headers) = headers(height);
    let parent_hash = match height {
        1 => anchor,
        _ => headers[usize::try_from(height - 2).expect("small")].hash(),
    };
    let header = headers.last().expect("one header or more").clone();
    frame(Message::Headers(Headers {
        subscription_id: subscription,
        initial_target_tip_hash: header.hash(),
        common_ancestor_height: block::Height(height - 1),
        common_ancestor_hash: parent_hash,
        reaches_initial_target: false,
        tree_aux_schema: AuxSchema::None,
        entries: vec![HeaderEntry {
            header,
            body_size: 0,
            tree_aux: None,
        }],
    }))
}

/// The node's terminal outcome for `subscription`.
fn outcome_frame(subscription: u64) -> Frame {
    frame(Message::HeadersOutcome(HeadersOutcome {
        subscription_id: subscription,
        initial_target_tip_hash: block::Hash([1; 32]),
        outcome: HeadersOutcomeCode::SubscriptionClosed,
    }))
}

/// The node's `Open` of `subscription` from the anchor.
fn open(subscription: u64) -> Frame {
    let (anchor, _) = headers(1);
    frame(Message::SubscribeHeaders(SubscribeHeaders {
        operation: Operation::Open,
        subscription_id: subscription,
        update_sequence: 0,
        initial_target_tip_hash: block::Hash([1; 32]),
        acknowledged: Cursor {
            height: block::Height(0),
            hash: anchor,
            headers: 0,
            bytes: 0,
        },
        locator_hashes: vec![anchor],
        added_credit: Credit {
            objects: 250,
            bytes: 65_750,
        },
        tree_aux_schema: AuxSchema::None,
    }))
}

/// The node's `Status`.
fn status() -> Frame {
    frame(Message::Status(Status {
        work_anchor_height: block::Height(0),
        work_anchor_hash: block::Hash([0; 32]),
        selected_tip_height: block::Height(1),
        selected_tip_hash: block::Hash([1; 32]),
        suffix_cumulative_work: Default::default(),
        oldest_retained_height: block::Height(0),
        max_headers_per_response: 1_000,
        max_subscriptions: 1,
        max_message_bytes: 263,
        tree_aux_schema_mask: 0,
    }))
}

/// The row of generated ID `id`.
fn row_of(id: &str) -> String {
    GeneratedId::parse(id)
        .and_then(|id| id.row.map(str::to_owned))
        .expect("a sender ID names its row")
}

/// Checks `history`'s last frame is labelled `label` under `id`'s row, and
/// that it fails `id`, and no other property first.
fn fails(history: &History, label: Label, exhausted: bool, id: &str) {
    let Some(Observation::NodeFrame {
        label: got,
        exhausted: got_exhausted,
        row,
        ..
    }) = history.frames.last()
    else {
        panic!("a node frame");
    };
    assert_eq!((*got, *got_exhausted), (label, exhausted), "{id}");
    assert_eq!(row.as_deref(), Some(row_of(id).as_str()), "{id}");
    let report = history.check();
    assert_eq!(stopped_by(&report), Some(id), "{:?}", report.end);
}

/// Crafted node frames fed to the adapter. Each violation gets its exact
/// label and fails its generated property:
///
/// - a second `Open` while the first is live, over strict `max_live` 1
///   though within the receiver's tolerance of twice that, is `OverLive`
///   and fails `sender-within-live`
/// - a page past its subscription's object credit is `OverCredit` and
///   fails the subscription row's `sender-within-credit`
/// - a duplicate ending is `Unsolicited` and fails `sender-ends-once`
/// - a `Status` past the bucket's capacity at one instant is `exhausted`
///   and fails `sender-cadence`, on the same header stream or a reopened
///   one of the same connection
/// - a page for a subscription never opened, and one after its terminal
///   outcome, are each `Unsolicited` and fail `headers/sender-answers-open`
///
/// The controls: an `Open` after the first subscription's `end-claimed`, a
/// `Status` on a new connection or a refill interval later, and a page for
/// a live subscription, are `Conformant` and hold.
///
/// This fails if the adapter labels everything `Conformant`, or uses the
/// receiver's margins: an adapter that allows twice `max_live` labels the
/// second `Open` `Conformant`.
#[test]
fn the_sender_adapter_labels_violations() {
    // A second open: the sender's limit, not the receiver's tolerance.
    let mut second = History::new();
    assert_eq!(second.sent(0, &open(1)), Label::Conformant);
    assert_eq!(second.sent(1, &open(2)), Label::OverLive);
    fails(&second, Label::OverLive, false, LIVE);
    let subscription = RegtestMessage::ROWS
        .iter()
        .find(|row| matches!(row.role, MessageRole::Subscription { .. }))
        .expect("header sync has a subscription row");
    let mut receiver = SubscriptionModel::new(&RegtestMessage::ROWS, subscription.message_type)
        .expect("a subscription row");
    let credit = Credit {
        objects: 250,
        bytes: 65_750,
    };
    for key in [1, 2] {
        assert!(
            receiver.open_frame(0, key, 0, credit, 0).is_conformant(),
            "the receiver tolerates the second open"
        );
    }
    // The control: the node's open after the peer ended the first.
    let mut reopened = History::new();
    assert_eq!(reopened.sent(0, &open(1)), Label::Conformant);
    reopened.applied(TransitionFact::EndClaimed { subscription: 1 });
    assert_eq!(reopened.sent(1, &open(2)), Label::Conformant);
    let report = reopened.check();
    assert_eq!(report.end.violation, None);
    assert!(held(&report, LIVE));

    // A page past the credit.
    let mut over = History::new();
    over.open_applied(1, 526);
    assert_eq!(over.sent(0, &page(SUB, 1)), Label::Conformant);
    assert_eq!(over.sent(1, &page(SUB, 2)), Label::OverCredit);
    fails(&over, Label::OverCredit, false, CREDIT);

    // A duplicate ending.
    let mut twice = History::new();
    twice.open_applied(250, 65_750);
    assert_eq!(twice.sent(0, &outcome_frame(SUB)), Label::Conformant);
    assert_eq!(twice.sent(1, &outcome_frame(SUB)), Label::Unsolicited);
    fails(&twice, Label::Unsolicited, false, ENDS);

    // A status past the cadence: the bucket's capacity at one instant, then
    // one more.
    let status_row = RegtestMessage::ROWS
        .iter()
        .find(|row| row.name == "status")
        .expect("a status row");
    let MessageRole::Announcement { cadence } = status_row.role else {
        panic!("status is an announcement");
    };
    let mut burst = History::new();
    let status = status();
    for seq in 0..u64::from(cadence.capacity) {
        assert_eq!(burst.sent(seq, &status), Label::Conformant);
    }
    assert!(burst.frames.iter().all(|frame| matches!(
        frame,
        Observation::NodeFrame {
            exhausted: false,
            ..
        }
    )));
    burst.sent(u64::from(cadence.capacity), &status);
    fails(&burst, Label::Conformant, true, CADENCE);
    // The connection's bucket, which a reopened header stream shares.
    let mut reopened = History::new();
    for seq in 0..u64::from(cadence.capacity) {
        reopened.sent(seq, &status);
    }
    reopened.on(CONNECTION, QUIC_STREAM + 4).sent(0, &status);
    fails(&reopened, Label::Conformant, true, CADENCE);
    // The controls: a new connection gets its own bucket, and a refill
    // interval later the first bucket has a token again.
    for (connection, at) in [
        (CONNECTION + 1, Duration::ZERO),
        (CONNECTION, cadence.refill_interval),
    ] {
        let mut later = History::new();
        for seq in 0..u64::from(cadence.capacity) {
            later.sent(seq, &status);
        }
        later.at = at;
        let seq = if connection == CONNECTION {
            u64::from(cadence.capacity)
        } else {
            0
        };
        later.on(connection, QUIC_STREAM).sent(seq, &status);
        let report = later.check();
        assert_eq!(report.end.violation, None, "{connection} at {at:?}");
        assert!(held(&report, CADENCE));
    }

    // A page for a subscription never opened.
    let mut never = History::new();
    assert_eq!(never.sent(0, &page(7, 1)), Label::Unsolicited);
    fails(&never, Label::Unsolicited, false, ANSWERS);

    // A page after the terminal outcome.
    let mut after = History::new();
    after.open_applied(250, 65_750);
    assert_eq!(after.sent(0, &page(SUB, 1)), Label::Conformant);
    assert_eq!(after.sent(1, &outcome_frame(SUB)), Label::Conformant);
    assert_eq!(after.sent(2, &page(SUB, 2)), Label::Unsolicited);
    fails(&after, Label::Unsolicited, false, ANSWERS);

    // The control: a page for a live subscription.
    let mut live = History::new();
    live.open_applied(250, 65_750);
    assert_eq!(live.sent(0, &page(SUB, 1)), Label::Conformant);
    let report = live.check();
    assert_eq!(report.end.violation, None);
    assert!(held(&report, ANSWERS));
}

/// Two crafted histories where the peer's next grant reaches the node only
/// after an over-credit page started. `peer-0` opened with one header and
/// two pages of bytes, and the node writes two one-header pages.
///
/// - The grant is applied only after the second page's write finished.
/// - The second page's `frame-write-started` comes first, then
///   `grant-applied`, then its `frame-written`.
///
/// In both, the peer reads both pages after the grant, and the second is
/// `OverCredit`. In the first, a third page started after the grant is
/// `Conformant`, so the grant does count for later writes.
///
/// This fails if the adapter labels on arrival, which applies the grant
/// before both pages, or at write completion, which applies it before the
/// second history's second page.
#[test]
fn a_late_grant_never_excuses_an_earlier_page() {
    let (first, second, third) = (page(SUB, 1), page(SUB, 2), page(SUB, 3));

    let mut after_write = History::new();
    after_write.open_applied(1, 526);
    after_write.wrote(0, &first).wrote(1, &second);
    after_write.grant_applied(1, 263);
    after_write.wrote(2, &third);
    assert_eq!(after_write.read(0, &first), Label::Conformant);
    assert_eq!(after_write.read(1, &second), Label::OverCredit);
    fails(&after_write, Label::OverCredit, false, CREDIT);
    assert_eq!(after_write.read(2, &third), Label::Conformant);

    let mut mid_write = History::new();
    mid_write.open_applied(1, 526);
    mid_write.wrote(0, &first).started(1, &second);
    mid_write.grant_applied(1, 263);
    mid_write.written(1, &second);
    assert_eq!(mid_write.read(0, &first), Label::Conformant);
    assert_eq!(mid_write.read(1, &second), Label::OverCredit);
    fails(&mid_write, Label::OverCredit, false, CREDIT);
}

/// Paired crafted histories of one one-header page, 263 bytes on this tree,
/// with no `Grant` ever.
///
/// - Started after `open-applied` with one object and 263 bytes, it's
///   `Conformant`: the `Open`'s initial credit alone authorizes it.
/// - Started before the same `open-applied`, it's `Unsolicited` and fails
///   `headers/sender-answers-open`.
///
/// This fails if the adapter labels on arrival or at write completion,
/// which applies the `Open` before the second page.
#[test]
fn an_open_authorizes_only_after_it_is_applied() {
    let page = page(SUB, 1);
    assert_eq!(
        page.payload.len(),
        ONE_HEADER_PAGE,
        "a plain one-header page"
    );
    let bytes = u32::try_from(ONE_HEADER_PAGE).expect("small");

    let mut opened = History::new();
    opened.open_applied(1, bytes);
    assert_eq!(opened.sent(0, &page), Label::Conformant);
    let report = opened.check();
    assert_eq!(report.end.violation, None);
    assert!(held(&report, ANSWERS));

    let mut early = History::new();
    early.started(0, &page);
    early.open_applied(1, bytes);
    early.written(0, &page);
    assert_eq!(early.read(0, &page), Label::Unsolicited);
    fails(&early, Label::Unsolicited, false, ANSWERS);
}
