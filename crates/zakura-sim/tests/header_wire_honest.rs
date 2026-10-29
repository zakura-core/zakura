//! M5-D: the honest peer on `header-sync/wire`. A node behind its real
//! transport syncs from an honest peer and is never punished by one (M5.1),
//! runs at production limits, and stops granting or publishing when storage
//! or a subscriber holds back, without any limit shrunk (M5.4). The reopen
//! test extends M5-C's to a stream retired after both subscriptions ended.
//!
//! The Regtest fixtures are 120 and 1,200 headers, both within the
//! 4,000-header body window of the anchor, since M4's engine keeps
//! `verified_best` there.
#![cfg(zakura_sim)]

mod support;

use std::{collections::BTreeMap, rc::Rc, time::Duration};

use support::inputs;
use zakura_chain::parameters::Network;
use zakura_network::{
    zakura::{
        v9::RegtestMessage, ZakuraHeaderSyncConfig, ZakuraLocalLimits, LOCAL_MAX_MESSAGE_BYTES,
        ZAKURA_HEADER_SYNC_STREAM_VERSION, ZAKURA_STREAM_HEADER_SYNC,
    },
    Config,
};
use zakura_sim::{
    harness::{
        header_node::{
            policy::{Allow, Hold, OpKind, Select, Trigger},
            Chain, Policy,
        },
        header_sync::{HeaderWire, WirePorts, WireScenario},
        run_prepared, HarnessError, Subsystem,
    },
    model::{is_fault_cause, Label},
    observe::{
        PeerCredit, StorageFact, StorageRequest, TransitionFact, TransportFact, TransportLimits,
    },
    peer::{
        honest::{CreditSpec, GrantPolicy, HonestSpec, SubscribeSpec},
        PeerAction, PeerSpec,
    },
    property::{Assumptions, HasView, Property, Registry, RegistryChecker, Source, Sources},
    table::table_properties,
    Checker, MemorySink, Observation, Plan, PlanInput, PropertyOutcome, RunReport,
};

/// The `Headers` message type.
const HEADERS: u16 = 3;

/// The `HeadersOutcome` message type.
const HEADERS_OUTCOME: u16 = 4;

/// One plain one-header Regtest page: 82 fixed bytes and a 181-byte entry.
const ONE_HEADER_PAGE: u64 = 263;

/// The node's chain at the start of most runs.
const PRELOAD: u32 = 50;

/// The 64 plans the M5.1 tests run.
fn sync_plans() -> Vec<PlanInput> {
    inputs(64, 16_384)
}

/// The plans the other tests run: all defaults, then two byte-chosen.
fn plans() -> Vec<PlanInput> {
    let mut plans = vec![Plan::from_fuzz(&[])];
    plans.extend(inputs(2, 16_384));
    plans
}

/// One peer stream's offers in log order: each `PeerFrame`'s `seq`, and
/// whether its `PeerWritten` followed.
type Offers = Vec<(u64, bool)>;

/// Each peer stream's [`Offers`] in `report`, by connection and QUIC
/// stream.
///
/// # Panics
///
/// If a stream's offers don't count from 0, or a `PeerWritten` doesn't
/// finish its stream's earliest unwritten offer, with the same `seq`. So
/// writes finish in offer order, each once.
fn peer_offers(report: &RunReport) -> BTreeMap<(u64, u64), Offers> {
    let mut offers: BTreeMap<(u64, u64), Offers> = BTreeMap::new();
    for entry in report.log.entries() {
        match &entry.obs {
            Observation::PeerFrame {
                connection,
                quic_stream,
                seq,
                ..
            } => {
                let quic_stream =
                    quic_stream.expect("a wire run's PeerFrame names its QUIC stream");
                let offered = offers.entry((*connection, quic_stream)).or_default();
                assert_eq!(
                    usize::try_from(*seq).ok(),
                    Some(offered.len()),
                    "connection {connection}, stream {quic_stream}: offers count from 0"
                );
                offered.push((*seq, false));
            }
            Observation::PeerWritten {
                connection,
                quic_stream,
                seq,
            } => {
                let next = offers
                    .get_mut(&(*connection, *quic_stream))
                    .and_then(|offered| offered.iter_mut().find(|(_, written)| !*written));
                match next {
                    Some((offered, written)) if offered == seq => *written = true,
                    _ => panic!(
                        "connection {connection}, stream {quic_stream}: PeerWritten {seq} doesn't \
                         finish the stream's earliest unwritten offer, {next:?}"
                    ),
                }
            }
            _ => {}
        }
    }
    offers
}

/// Every frame a peer offered in `report` was written: each `PeerFrame`
/// pairs with one `PeerWritten` by connection, QUIC stream and `seq`, in
/// order, and no `PeerWritten` is left over ([`peer_offers`]).
fn assert_every_offer_written(report: &RunReport) {
    for ((connection, quic_stream), offered) in peer_offers(report) {
        assert!(
            offered.iter().all(|(_, written)| *written),
            "connection {connection}, stream {quic_stream}: an offered frame was never written, \
             {offered:?}"
        );
    }
}

/// Runs `scenario` under `plan`, with `extra` checking beside the harness,
/// and returns the report with the ports, which outlive the run. Every
/// frame a peer offered must have been written.
fn run_with(
    scenario: &WireScenario,
    plan: PlanInput,
    extra: Option<&mut dyn Checker>,
) -> (RunReport, WirePorts) {
    let ports = HeaderWire::prepare(scenario);
    let report = run_prepared::<HeaderWire>(
        scenario,
        ports.clone(),
        plan,
        &mut MemorySink::default(),
        extra,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    assert_eq!(ports.unplaced_records(), 0, "every peer record was logged");
    assert_every_offer_written(&report);
    (report, ports)
}

/// [`run_with`] with no extra checker.
fn run(scenario: &WireScenario, plan: PlanInput) -> (RunReport, WirePorts) {
    run_with(scenario, plan, None)
}

/// Runs `scenario` under `plan`, and returns the report even of a run that
/// ended in a harness failure, so a test can check what happened first.
fn run_to_end(scenario: &WireScenario, plan: PlanInput) -> (RunReport, WirePorts) {
    let ports = HeaderWire::prepare(scenario);
    let ran = run_prepared::<HeaderWire>(
        scenario,
        ports.clone(),
        plan,
        &mut MemorySink::default(),
        None,
    );
    match ran {
        Ok(report) => (report, ports),
        Err(HarnessError::Run { report, .. }) => (*report, ports),
        Err(error) => panic!("{error}"),
    }
}

/// An honest peer holding `chain` headers, as `spec`.
fn honest(spec: PeerSpec, chain: u32, subscribe: Option<SubscribeSpec>) -> PeerSpec {
    PeerSpec {
        honest: Some(HonestSpec { chain, subscribe }),
        ..spec
    }
}

/// The node at [`PRELOAD`], with `peers`, under `policy`.
fn scenario(peers: Vec<PeerSpec>, policy: Policy) -> WireScenario {
    WireScenario {
        node: Chain::main(PRELOAD),
        peers,
        policy,
        ..WireScenario::default()
    }
}

/// M5.1's run: `peer-0` holds the 120-header tree and dials the node, under
/// M4's 5 s latency bound with `SuccessOnly` storage and no loss.
fn sync_scenario() -> WireScenario {
    scenario(
        vec![honest(PeerSpec::dialing(), 120, None)],
        Policy::converging(),
    )
}

/// One log entry with the virtual time it was logged at.
#[derive(Clone, Debug)]
struct At {
    step: u64,
    /// Nanoseconds since the run's start.
    ns: u64,
    obs: Observation,
}

/// Every entry of `report`'s log, with its time.
fn timed(report: &RunReport) -> Vec<At> {
    let mut ns = 0;
    report
        .log
        .entries()
        .iter()
        .map(|entry| {
            if let Observation::TimeAdvanced { to_ns, .. } = entry.obs {
                ns = to_ns;
            }
            At {
                step: entry.step,
                ns,
                obs: entry.obs.clone(),
            }
        })
        .collect()
}

/// The transport facts in `entries` on `connection`.
fn facts(entries: &[At], connection: u64) -> impl Iterator<Item = (&At, &TransportFact)> {
    entries.iter().filter_map(move |at| match &at.obs {
        Observation::Transport {
            connection: Some(c),
            event,
            ..
        } if *c == connection => Some((at, event)),
        _ => None,
    })
}

/// The one `Session` row's connection and QUIC stream, and `report`'s
/// only connection.
fn header_session(entries: &[At]) -> (u64, u64) {
    let sessions: Vec<_> = entries
        .iter()
        .filter_map(|at| match at.obs {
            Observation::Session {
                connection,
                quic_stream,
                ..
            } => Some((connection, quic_stream)),
            _ => None,
        })
        .collect();
    sessions[0]
}

/// The node syncs to the 120-header tip from an honest peer under each of
/// 64 plans, read from its engine, not from any connection state.
///
/// This fails if sync stalls after the handshake: a planted return in the
/// peer task right after it leaves the tip at 50.
#[test]
fn node_syncs_the_fixture_tip_from_an_honest_peer() {
    let scenario = sync_scenario();
    for plan in sync_plans() {
        let (_, ports) = run(&scenario, plan);
        let tip = Chain::main(120).tip(&ports.fixture);
        assert_eq!(
            ports.selected_tip(),
            tip,
            "the node reaches the fixture's tip"
        );
    }
}

/// A view-less harness, for the generated properties, which read only the
/// log.
struct Logged;

impl HasView for Logged {
    type View = ();

    /// Nothing.
    fn view(&self) {}
}

/// The ID of header sync's generated `conformant-never-closes`.
fn conformant_never_closes() -> String {
    format!(
        "header-sync/s{ZAKURA_STREAM_HEADER_SYNC}v{ZAKURA_HEADER_SYNC_STREAM_VERSION}/conformant-never-closes"
    )
}

/// A checker of [`conformant_never_closes`] alone, assembled from the
/// generated table properties (M5-Q13).
fn never_closes_checker() -> RegistryChecker<Logged> {
    let id = conformant_never_closes();
    let properties: Vec<Property<Logged>> =
        table_properties("header-sync", &RegtestMessage::LAYOUT)
            .into_iter()
            .filter(|property| property.id.as_str() == id)
            .collect();
    assert_eq!(properties.len(), 1, "{id} is generated");
    RegistryChecker::new(
        Registry::new(properties).expect("one property"),
        Rc::new(Logged),
        Assumptions::none(),
        &Sources::observed(&[Source::Frames, Source::Closes, Source::Storage]),
    )
}

/// Over M5.1's 64 plans, every frame the honest peer sends is labelled
/// `Conformant` by its models, each one the node reads is judged,
/// `conformant-never-closes` holds where it applies, and the node decides no
/// close with a fault cause.
///
/// This fails if the node punishes a conformant peer: a node patched to
/// refuse the peer's `Status` closes with `service_protocol_reject`.
#[test]
fn honest_runs_never_close() {
    let scenario = sync_scenario();
    for plan in sync_plans() {
        let mut checker = never_closes_checker();
        let (report, _) = run_with(&scenario, plan, Some(&mut checker));
        let outcome = report
            .properties
            .iter()
            .find(|property| property.property == conformant_never_closes())
            .map(|property| &property.outcome);
        assert!(
            matches!(outcome, Some(PropertyOutcome::Held { applicable }) if *applicable > 0),
            "{outcome:?}"
        );
        let mut rows = std::collections::BTreeSet::new();
        let (mut read, mut judged) = (0, 0);
        for entry in report.log.entries() {
            match &entry.obs {
                Observation::PeerFrame { label, row, .. } => {
                    assert_eq!(*label, Label::Conformant, "{:?}", entry.obs);
                    rows.insert(row.clone().expect("every frame has a row"));
                }
                Observation::Transport {
                    event: TransportFact::FrameRead { .. },
                    ..
                } => read += 1,
                Observation::FrameJudged { .. } => judged += 1,
                Observation::Transport {
                    event: TransportFact::CloseDecided { cause: Some(cause) },
                    ..
                } => assert!(!is_fault_cause(cause), "a fault close: {cause}"),
                _ => {}
            }
        }
        assert_eq!(
            rows.into_iter().collect::<Vec<_>>(),
            ["headers", "headers-outcome", "status"],
            "the peer served pages and an outcome"
        );
        assert!(read > 0);
        assert_eq!(judged, read, "every frame the node read was judged");
    }
}

/// The header session runs at production limits in both dial directions:
/// the limits and queue depths the node installed, read from each
/// session's `admitted`, are what the default config gives after both
/// handshake offers, which are production's own. The inbound depth is 4,
/// the service's cap, and the outbound depth keeps the whole transport
/// allowance, 4,096, since the service declares no cap of its own. The
/// reactor's `status_refresh_interval` and `request_timeout` are 30 s, read
/// from the startup `WireNode` actually passed to it, with the default
/// header-sync config.
///
/// This fails if the harness passes any test config.
#[test]
fn production_limits_are_in_force() {
    let config = Config {
        network: Network::new_regtest(Default::default()),
        ..Config::default()
    };
    let local = ZakuraLocalLimits::from_config(&config);
    let negotiated = local.clamp(&local.initial_limits());
    let ms = |d: Duration| u64::try_from(d.as_millis()).expect("production timeouts are short");
    let expected = TransportLimits {
        max_frame_bytes: negotiated.max_frame_bytes,
        max_message_bytes: negotiated.max_message_bytes,
        max_open_streams: negotiated.max_open_streams,
        max_inbound_queue_depth: negotiated.max_inbound_queue_depth,
        idle_timeout_ms: ms(negotiated.idle_timeout),
        prelude_timeout_ms: ms(negotiated.prelude_timeout),
        control_timeout_ms: ms(negotiated.control_timeout),
        stream_open_rate_per_second: negotiated.stream_open_rate_per_second,
        message_rate_per_second: negotiated.message_rate_per_second,
    };
    let depths = RegtestMessage::LAYOUT[0]
        .queue_depths
        .expect("header sync declares its depths");
    let transport = u64::from(negotiated.max_inbound_queue_depth);
    let inbound = transport.min(depths.inbound as u64);
    let outbound = transport.min(u64::try_from(depths.outbound).unwrap_or(u64::MAX));
    assert_eq!((inbound, outbound), (4, 4_096));
    let scenario = scenario(
        vec![
            honest(PeerSpec::dialing(), 120, None),
            honest(PeerSpec::listening(), 120, None),
        ],
        Policy::converging(),
    );
    for plan in plans() {
        let (report, ports) = run(&scenario, plan);
        let admitted: Vec<_> = report
            .log
            .entries()
            .iter()
            .filter_map(|entry| match &entry.obs {
                Observation::Transport {
                    event:
                        TransportFact::Admitted {
                            limits,
                            inbound_depth,
                            outbound_depth,
                            ..
                        },
                    ..
                } => Some((*limits, *inbound_depth, *outbound_depth)),
                _ => None,
            })
            .collect();
        assert_eq!(admitted.len(), 2, "one header session per direction");
        for session in admitted {
            assert_eq!(session, (expected, inbound, outbound));
        }
        let startup = ports
            .startup
            .borrow()
            .clone()
            .expect("the run built the node");
        assert_eq!(startup.config, ZakuraHeaderSyncConfig::default());
        assert_eq!(startup.status_refresh_interval, Duration::from_secs(30));
        assert_eq!(startup.request_timeout, Duration::from_secs(30));
        assert_eq!(startup.max_frame_bytes, LOCAL_MAX_MESSAGE_BYTES);
    }
}

/// A peer's balance on one subscription: what it was granted less what it
/// sent, in objects and bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Balance {
    objects: i64,
    bytes: i64,
}

impl Balance {
    /// Whether a legal next page, one plain header at least, fits.
    fn fits_a_page(self) -> bool {
        self.objects >= 1 && self.bytes >= ONE_HEADER_PAGE as i64
    }
}

/// The node subscribes to `peer-0` on the 1,200-header tree, and its first
/// prepare is held from its submission, which comes once 401 headers are
/// staged, after two 250-header reservation rounds. The hold opens 20 s
/// after the submission, inside the 30 s request deadline.
///
/// Once every page the node asked for has been judged, a 5 s window follows
/// in which the node sends no new credit: no `PeerReceived` carries credit.
/// At its end, `peer-0`'s object and byte balances, cumulative grants less
/// the plain pages it sent, are computed separately, and no legal next page
/// fits them. After the release, grants resume and the node reaches the
/// tip.
///
/// This fails if the node keeps granting while its prepare is held, or if
/// the window never happens.
#[test]
fn held_storage_stops_the_credit_grants() {
    let hold = Hold {
        select: Select::nth(OpKind::Prepare, 1),
        until: Trigger::VirtualTime(20_000),
        then: vec![Allow::Engine],
        deadline_ms: None,
    };
    let scenario = scenario(
        vec![honest(PeerSpec::dialing(), 1_200, None)],
        Policy {
            holds: vec![hold],
            ..Policy::converging()
        },
    );
    for plan in plans() {
        let (report, ports) = run(&scenario, plan);
        let entries = timed(&report);
        let (submitted, id, prepared) = entries
            .iter()
            .enumerate()
            .find_map(|(index, at)| match &at.obs {
                Observation::Storage {
                    fact:
                        StorageFact::Submitted {
                            id,
                            request: StorageRequest::PrepareHeaderTarget { headers, .. },
                            ..
                        },
                    ..
                } => Some((index, *id, *headers)),
                _ => None,
            })
            .expect("the node prepared");
        assert!(
            prepared >= 401,
            "the first prepare holds {prepared} headers, at least 401"
        );
        let completed = entries
            .iter()
            .find_map(|at| match &at.obs {
                Observation::Storage {
                    fact: StorageFact::Completed { id: done, .. },
                    ..
                } if *done == id => Some(at.ns),
                _ => None,
            })
            .expect("the hold opened");
        assert_eq!(
            completed - entries[submitted].ns,
            20_000_000_000,
            "held for 20 s"
        );
        // Every page sent before the hold opened.
        let pages: Vec<u64> = entries
            .iter()
            .filter_map(|at| match &at.obs {
                Observation::PeerFrame {
                    row: Some(row),
                    seq,
                    ..
                } if row == "headers" && at.ns < completed => Some(*seq),
                _ => None,
            })
            .collect();
        // The window opens once the prepare is held and the node has judged
        // every page sent.
        let judged = |seq: u64| {
            entries
                .iter()
                .position(
                    |at| matches!(at.obs, Observation::FrameJudged { seq: s, .. } if s == seq),
                )
                .expect("every page the node asked for was judged")
        };
        let start = pages
            .iter()
            .map(|seq| judged(*seq))
            .max()
            .expect("pages were sent")
            .max(submitted);
        let end = entries[start].ns + 5_000_000_000;
        assert!(
            end <= completed,
            "the window happened while the prepare was held"
        );
        let credited = |at: &At| {
            matches!(
                at.obs,
                Observation::PeerReceived {
                    credit: Some(_),
                    ..
                }
            )
        };
        assert!(
            !entries[start..]
                .iter()
                .take_while(|at| at.ns <= end)
                .any(credited),
            "no credit in the window"
        );
        let mut balance = Balance::default();
        for at in entries.iter().filter(|at| at.ns <= end) {
            match at.obs {
                Observation::PeerReceived {
                    credit: Some(PeerCredit { objects, bytes }),
                    ..
                } => {
                    balance.objects += i64::from(objects);
                    balance.bytes += i64::from(bytes);
                }
                Observation::PeerFrame {
                    row: Some(ref row),
                    payload,
                    ..
                } if row == "headers" => {
                    balance.objects -= ((payload - 82) / 181) as i64;
                    balance.bytes -= payload as i64;
                }
                _ => {}
            }
        }
        assert!(!balance.fits_a_page(), "{balance:?}");
        assert!(
            entries.iter().any(|at| at.ns >= completed && credited(at)),
            "grants resumed after the release"
        );
        assert_eq!(ports.selected_tip(), Chain::main(1_200).tip(&ports.fixture));
    }
}

/// `peer-0` subscribes to the node, which holds the 1,200-header tree, from
/// height 50 toward the tip with plain pages, and grants nothing more until
/// it's been quiet for 5 s after a page. In one history its object credit
/// runs out first: one object and 526 bytes leave 0 objects and 263 bytes
/// after a page. In the other, its bytes do: two objects and 263 bytes
/// leave one object and 0 bytes. Either way no page comes for those 5 s,
/// though more than 1,000 headers remain. Then the peer grants the same
/// again and another page comes.
///
/// This fails if the publisher overruns either balance, or if the stall
/// never happens: a patch that removes every enforcement of the exhausted
/// unit makes the publisher send past it.
#[test]
fn a_slow_subscriber_stops_the_publisher() {
    for (credit, left, regranted) in [
        (
            CreditSpec {
                objects: 1,
                bytes: 526,
            },
            Balance {
                objects: 0,
                bytes: 263,
            },
            Balance {
                objects: 0,
                bytes: 526,
            },
        ),
        (
            CreditSpec {
                objects: 2,
                bytes: 263,
            },
            Balance {
                objects: 1,
                bytes: 0,
            },
            Balance {
                objects: 2,
                bytes: 0,
            },
        ),
    ] {
        let subscribe = SubscribeSpec {
            start: PRELOAD,
            open: credit,
            grants: GrantPolicy::Held {
                grant: credit,
                quiet_ms: 5_000,
            },
        };
        let mut peer = honest(PeerSpec::dialing(), PRELOAD, Some(subscribe));
        peer.script = vec![PeerAction::Grant];
        let scenario = WireScenario {
            node: Chain::main(1_200),
            ..scenario(vec![peer], Policy::converging())
        };
        for plan in plans() {
            let (report, ports) = run_to_end(&scenario, plan);
            let entries = timed(&report);
            let pages: Vec<&At> = entries
                .iter()
                .filter(|at| {
                    matches!(at.obs, Observation::PeerReceived { message_type, .. } if message_type == HEADERS)
                })
                .collect();
            let granted = report
                .picks
                .iter()
                .find(|pick| pick.identity == "peer:peer-0:grant")
                .expect("the peer granted again")
                .step;
            let granted_at = entries
                .iter()
                .rfind(|at| at.step <= granted)
                .expect("the pick's step logged")
                .ns;
            let first = pages.first().expect("a first page");
            let before: Vec<_> = pages.iter().filter(|at| at.step <= granted).collect();
            assert_eq!(before.len(), 1, "one page before the grant: {before:?}");
            assert!(
                granted_at >= first.ns + 5_000_000_000,
                "the stall lasted 5 s"
            );
            // The balance after the pages before the grant, each plain:
            // 82 fixed bytes and 181 per header.
            let mut balance = Balance {
                objects: i64::from(credit.objects),
                bytes: i64::from(credit.bytes),
            };
            let spend = |balance: &mut Balance, at: &At| {
                let Observation::PeerReceived { payload, .. } = at.obs else {
                    unreachable!("pages are PeerReceived");
                };
                balance.objects -= i64::try_from((payload - 82) / 181).expect("a page is small");
                balance.bytes -= i64::try_from(payload).expect("a page is small");
            };
            for at in &before {
                spend(&mut balance, at);
            }
            assert_eq!(balance, left, "the publisher stayed within both balances");
            assert!(!balance.fits_a_page());
            let sent = u32::try_from(before.len()).expect("one page");
            assert!(1_200 - (PRELOAD + sent) > 1_000, "headers remain");
            assert!(
                pages.iter().any(|at| at.step > granted),
                "a page came after the grant"
            );
            // The pages after the grant stay within the regranted balance
            // in both units, and stall the publisher again.
            balance.objects += i64::from(credit.objects);
            balance.bytes += i64::from(credit.bytes);
            for at in pages.iter().filter(|at| at.step > granted) {
                spend(&mut balance, at);
                assert!(
                    balance.objects >= 0 && balance.bytes >= 0,
                    "a page after the grant overran it: {balance:?}"
                );
            }
            assert_eq!(balance, regranted);
            assert!(!balance.fits_a_page());
            // Checked last, so a publisher that overran fails above, before
            // the peer's next update, which its models then refuse.
            assert_eq!(report.end.harness, None);
            assert_eq!(report.end.violation, None);
            assert_eq!(ports.unplaced_records(), 0);
            assert_every_offer_written(&report);
        }
    }
}

/// A held grant's quiet time readies the peer for its `grant` only. With
/// `finish-header` next instead, the peer waits for every subscription to
/// end, which a held subscription never does, so the stream is never
/// retired with the exchange open.
///
/// This fails if the quiet time readies any next action.
#[test]
fn a_held_grant_readies_only_the_grant() {
    let credit = CreditSpec {
        objects: 1,
        bytes: 526,
    };
    let subscribe = SubscribeSpec {
        start: PRELOAD,
        open: credit,
        grants: GrantPolicy::Held {
            grant: credit,
            quiet_ms: 5_000,
        },
    };
    let mut peer = honest(PeerSpec::dialing(), PRELOAD, Some(subscribe));
    peer.script = vec![PeerAction::FinishHeader];
    let scenario = WireScenario {
        node: Chain::main(1_200),
        ..scenario(vec![peer], Policy::converging())
    };
    let (report, _) = run(&scenario, Plan::from_fuzz(&[]));
    assert!(
        !report
            .picks
            .iter()
            .any(|pick| pick.identity.starts_with("peer:")),
        "no action was offered"
    );
    assert!(report.log.entries().iter().any(|entry| matches!(
        entry.obs,
        Observation::PeerReceived { message_type, .. } if message_type == HEADERS
    )));
}

/// Once the page that reaches its target arrives, the peer sends `Close`,
/// and its held grant is never offered again, though the terminal outcome
/// is still on its way. The node holds 51 headers and the peer subscribes
/// from 50 with one header of credit and no quiet time.
///
/// This fails if the grant stays offered after `Close`: picking it fails
/// the run, since the subscription is closing.
#[test]
fn a_closed_subscription_offers_no_grant() {
    let credit = CreditSpec {
        objects: 1,
        bytes: 263,
    };
    let subscribe = SubscribeSpec {
        start: PRELOAD,
        open: credit,
        grants: GrantPolicy::Held {
            grant: credit,
            quiet_ms: 0,
        },
    };
    let mut peer = honest(PeerSpec::dialing(), PRELOAD, Some(subscribe));
    peer.script = vec![PeerAction::Grant];
    let scenario = WireScenario {
        node: Chain::main(PRELOAD + 1),
        ..scenario(vec![peer], Policy::converging())
    };
    for plan in plans() {
        let (report, _) = run(&scenario, plan);
        assert!(
            !report
                .picks
                .iter()
                .any(|pick| pick.identity == "peer:peer-0:grant"),
            "no grant after Close"
        );
        let outcome = report.log.entries().iter().any(|entry| {
            matches!(
                entry.obs,
                Observation::PeerReceived { message_type, .. } if message_type == HEADERS_OUTCOME
            )
        });
        assert!(outcome, "the subscription ended");
    }
}

/// M5-C's reopen, after real subscriptions. The node dials `peer-0`, each
/// subscribes to the other, and both subscriptions end before the peer
/// finishes its side of the header stream: the peer's on the node ended
/// (`publication-ended`, the node's terminal `frame-written` for it, and the
/// peer's `PeerReceived` of that frame), and the node's on the peer ended
/// (`end-claimed`), with every frame the node wrote read. The node's next
/// subscription is held back by holding its next locator.
///
/// The node then reopens on the live connection: a second `Session` row on
/// it, on a new QUIC stream with a new session ID, whose frames count from
/// 0 again, and no close is decided. A missing observation is a harness
/// failure, never evidence that the state is unreachable.
///
/// This fails if the peer finishes before both subscriptions end. With the
/// node's subscription still open, the retirement closes the connection
/// with `unfinished_exchange`.
#[test]
fn a_reopen_after_both_subscriptions_end_gets_a_new_identity() {
    let subscribe = SubscribeSpec {
        start: 0,
        open: CreditSpec {
            objects: 250,
            bytes: 65_750,
        },
        grants: GrantPolicy::Immediate,
    };
    let mut peer = honest(PeerSpec::listening(), 120, Some(subscribe));
    peer.script = vec![PeerAction::FinishHeader];
    let next_locator = Hold {
        select: Select::nth(OpKind::Locator, 2),
        until: Trigger::VirtualTime(3_600_000),
        then: vec![Allow::Engine],
        deadline_ms: None,
    };
    let scenario = scenario(
        vec![peer],
        Policy {
            holds: vec![next_locator],
            ..Policy::converging()
        },
    );
    for plan in plans() {
        let (report, _) = run(&scenario, plan);
        let entries = timed(&report);
        let finished = report
            .picks
            .iter()
            .find(|pick| pick.identity == "peer:peer-0:finish-header")
            .expect("the peer finished its header stream")
            .step;
        let (connection, first) = header_session(&entries);
        let before: Vec<At> = entries
            .iter()
            .filter(|at| at.step < finished)
            .cloned()
            .collect();
        let regulated = |want: fn(&TransitionFact) -> bool| {
            facts(&before, connection).find_map(|(at, fact)| match fact {
                TransportFact::Regulated {
                    quic_stream,
                    transition,
                } if *quic_stream == first && want(transition) => Some(at.step),
                _ => None,
            })
        };
        let ended = regulated(|t| matches!(t, TransitionFact::PublicationEnded { .. }))
            .expect("the peer's subscription on the node ended");
        regulated(|t| matches!(t, TransitionFact::EndClaimed { .. }))
            .expect("the node's subscription on the peer ended");
        let written: Vec<(u64, u64, u16)> = facts(&before, connection)
            .filter_map(|(at, fact)| match fact {
                TransportFact::FrameWritten {
                    quic_stream,
                    seq,
                    message_type,
                } if *quic_stream == first => Some((at.step, *seq, *message_type)),
                _ => None,
            })
            .collect();
        let received: Vec<(u64, u16)> = before
            .iter()
            .filter_map(|at| match at.obs {
                Observation::PeerReceived {
                    connection: c,
                    quic_stream,
                    seq,
                    message_type,
                    ..
                } if (c, quic_stream) == (connection, first) => Some((seq, message_type)),
                _ => None,
            })
            .collect();
        let terminal = written
            .iter()
            .find(|(step, _, message_type)| *step >= ended && *message_type == HEADERS_OUTCOME)
            .expect("the node wrote its terminal outcome");
        assert!(
            received.contains(&(terminal.1, HEADERS_OUTCOME)),
            "the peer read it"
        );
        assert_eq!(
            written
                .iter()
                .map(|(_, seq, message_type)| (*seq, *message_type))
                .collect::<Vec<_>>(),
            received,
            "the peer read every frame the node wrote"
        );

        let reopened = entries
            .iter()
            .find_map(|at| match at.obs {
                Observation::Session {
                    connection: c,
                    quic_stream,
                    kind,
                    version,
                    session_id,
                    ..
                } if at.step > finished => Some((c, quic_stream, kind, version, session_id)),
                _ => None,
            })
            .expect("the node reopened the header stream");
        let first_session = entries
            .iter()
            .find_map(|at| match at.obs {
                Observation::Session { session_id, .. } => Some(session_id),
                _ => None,
            })
            .expect("a first session");
        assert_eq!(
            (reopened.0, reopened.2, reopened.3),
            (
                connection,
                ZAKURA_STREAM_HEADER_SYNC,
                ZAKURA_HEADER_SYNC_STREAM_VERSION
            ),
            "on the same connection"
        );
        assert_ne!(reopened.1, first, "a new QUIC stream");
        assert_ne!(reopened.4, first_session, "a new session");
        let seq = facts(&entries, connection)
            .find_map(|(_, fact)| match fact {
                TransportFact::FrameWritten {
                    quic_stream, seq, ..
                } if *quic_stream == reopened.1 => Some(*seq),
                _ => None,
            })
            .expect("the node wrote on the reopened stream");
        assert_eq!(seq, 0, "the reopened stream's frames count from 0");
        // Every frame the node wrote on the retired stream, in the whole
        // run, was read and logged, any after the finish included.
        let written: Vec<u64> = facts(&entries, connection)
            .filter_map(|(_, fact)| match fact {
                TransportFact::FrameWritten {
                    quic_stream, seq, ..
                } if *quic_stream == first => Some(*seq),
                _ => None,
            })
            .collect();
        let read: Vec<u64> = entries
            .iter()
            .filter_map(|at| match at.obs {
                Observation::PeerReceived {
                    connection: c,
                    quic_stream,
                    seq,
                    ..
                } if (c, quic_stream) == (connection, first) => Some(seq),
                _ => None,
            })
            .collect();
        assert_eq!(written, read, "the retired stream's writes were all read");
        assert!(
            !facts(&entries, connection)
                .any(|(_, fact)| matches!(fact, TransportFact::CloseDecided { .. })),
            "no close was decided"
        );
    }
}

/// Honest runs replay: one plan run twice in one process gives the same
/// picks and the same log, so nothing process-wide leaks into the honest
/// script, its models or its records. The scenario has both directions of
/// subscription, a held grant, a reopen and a refresh: `peer-0` dials and
/// subscribes with a held grant, and the node dials `peer-1`, which serves
/// and subscribes, then finishes its header stream. The test first checks
/// that the history happened, so two equally short runs can't pass.
#[test]
fn an_honest_wire_run_replays_identically() {
    let credit = CreditSpec {
        objects: 1,
        bytes: 526,
    };
    let mut held = honest(
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
    held.script = vec![PeerAction::Grant];
    let mut serving = honest(
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
    );
    serving.script = vec![PeerAction::FinishHeader];
    let scenario = scenario(vec![held, serving], Policy::converging());
    for plan in plans() {
        let runs: Vec<_> = (0..2).map(|_| run(&scenario, plan.clone()).0).collect();
        for action in ["peer:peer-0:grant", "peer:peer-1:finish-header"] {
            assert!(
                runs[0].picks.iter().any(|pick| pick.identity == action),
                "{action}"
            );
        }
        let rows: std::collections::BTreeSet<_> = runs[0]
            .log
            .entries()
            .iter()
            .filter_map(|entry| match &entry.obs {
                Observation::PeerFrame { row: Some(row), .. } => Some(row.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            rows.into_iter().collect::<Vec<_>>(),
            ["headers", "headers-outcome", "status", "subscribe-headers"]
        );
        let sessions = runs[0]
            .log
            .entries()
            .iter()
            .filter(|entry| matches!(entry.obs, Observation::Session { .. }))
            .count();
        assert_eq!(sessions, 3, "two connections and one reopen");
        let statuses = runs[0]
            .log
            .entries()
            .iter()
            .filter(|entry| {
                matches!(&entry.obs, Observation::PeerFrame { row: Some(row), .. } if row == "status")
            })
            .count();
        assert!(statuses > sessions, "a Status was refreshed: {statuses}");
        assert_eq!(runs[0].picks, runs[1].picks);
        assert_eq!(runs[0].log.entries(), runs[1].log.entries());
    }
}
