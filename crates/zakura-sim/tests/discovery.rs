//! S4.7: discovery's `wait_complete` wakes once the initial exchange
//! completes. A scripted peer answers the node's Hello, GetPeers and
//! GetServices over an in-memory stream, on 2 workers.
#![cfg(zakura_sim)]

mod support;

use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    ops::RangeInclusive,
    time::{SystemTime, UNIX_EPOCH},
};

use support::{config, inputs, recorded};
use zakura_chain::parameters::Network;
use zakura_network::zakura::{
    framed_channel,
    wire_codec::{decode_frame, encode_frame},
    DiscoveryMessage, DiscoveryService, DiscoveryServiceSummary, Peer, Service, ServicePeerLimits,
    ServiceSummaryEnvelope, Services, ZakuraDiscoveryConfig, ZakuraDiscoveryHandle,
    ZakuraDiscoveryLocalConfig, ZakuraHandshakeConfig, ZakuraNodeRecord, ZakuraNodeRecordBody,
    ZakuraPeerId, ZakuraServiceId, ZAKURA_CAP_DISCOVERY, ZAKURA_STREAM_DISCOVERY,
};
use zakura_quic::NodeSecretKey;
use zakura_rt::{
    collections::HashMap,
    sync::{watch, CancellationToken},
};
use zakura_scheduler::Status;
use zakura_sim::{
    check::StepCtx, record::DrawRecord, record::Record, Checker, NoChecks, Observation,
    ObservationLog, Plan, PlanInput, PropertyOutcome, PropertyReport, RunEnd, RunReport, Sim,
    Violation,
};

/// The property this file checks (properties.md, Discovery).
const PROPERTY: &str = "discovery/wait-complete-wakes";

/// A signed record for `secret`, valid for ten minutes.
fn record(secret: &NodeSecretKey, handshake: &ZakuraHandshakeConfig) -> ZakuraNodeRecord {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after the epoch")
        .as_secs();
    let body = ZakuraNodeRecordBody {
        node_id: secret.public(),
        direct_addrs: vec![SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(45, 33, 30, 45)),
            8233,
        )],
        services: vec![ZakuraServiceId::discovery()],
        zakura_protocol_min: handshake.zakura_protocol_min,
        zakura_protocol_max: handshake.zakura_protocol_max,
        network_id: handshake.network_id,
        chain_id: handshake.chain_id,
        sequence: 1,
        expires_at_unix_secs: now + 600,
        extensions: Vec::new(),
    };
    ZakuraNodeRecord::sign(body, secret).expect("a fixture record signs")
}

/// Starts the discovery service with one inbound peer, and a task that plays
/// the peer: it waits for the node's three messages, then answers with its
/// Hello, Peers and Services.
fn scenario(sim: &mut Sim) {
    let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
    let local = NodeSecretKey::from_bytes(&[21; 32]);
    let peer_secret = NodeSecretKey::from_bytes(&[22; 32]);
    let (_connected, connected_rx) = watch::channel(Vec::new());
    let handle = ZakuraDiscoveryHandle::new(
        ZakuraDiscoveryLocalConfig {
            secret_key: local,
            direct_addrs: Vec::new(),
            services: vec![ZakuraServiceId::discovery()],
            zakura_protocol_min: handshake.zakura_protocol_min,
            zakura_protocol_max: handshake.zakura_protocol_max,
            network_id: handshake.network_id,
            chain_id: handshake.chain_id,
            last_authored_sequence: None,
        },
        ZakuraDiscoveryConfig {
            peer_limits: ServicePeerLimits {
                max_inbound_peers: 4,
                ..ServicePeerLimits::default()
            },
            ..ZakuraDiscoveryConfig::default()
        },
        connected_rx,
    )
    .expect("the fixture config is valid");
    let service = DiscoveryService::new(handle);
    let peer_id = ZakuraPeerId::new(peer_secret.public().as_bytes().to_vec())
        .expect("a node id is a valid peer id");
    let (peer_send, service_recv) = framed_channel(8);
    let (service_send, mut peer_recv) = framed_channel(8);
    let streams = HashMap::from_iter([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);
    service.add_peer(Peer::new(
        peer_id,
        None,
        ZAKURA_CAP_DISCOVERY,
        streams,
        CancellationToken::new(),
    ));

    sim.scheduler().spawn_root("peer", async move {
        // Keeps the service alive for the whole run.
        let _service = service;
        let _connected = _connected;
        let (mut hello, mut peers, mut services) = (false, false, false);
        while !(hello && peers && services) {
            let Some(frame) = peer_recv.recv().await else {
                return;
            };
            match decode_frame::<DiscoveryMessage>(&frame).expect("the node sends valid frames") {
                DiscoveryMessage::Hello { .. } => hello = true,
                DiscoveryMessage::GetPeers(_) => peers = true,
                DiscoveryMessage::GetServices(_) => services = true,
                DiscoveryMessage::Peers { .. } | DiscoveryMessage::Services(_) => {}
            }
        }
        let summary = DiscoveryServiceSummary {
            peer_exchange_slots_free: 1,
            max_records_per_response: 1,
            expected_disconnect_after_exchange: true,
        };
        let answers = [
            DiscoveryMessage::Hello {
                record: record(&peer_secret, &handshake),
            },
            DiscoveryMessage::Peers {
                records: Vec::new(),
            },
            DiscoveryMessage::Services(Services {
                node_id: peer_secret.public(),
                expires_at_unix_secs: u64::MAX,
                summaries: vec![ServiceSummaryEnvelope::discovery(&summary)
                    .expect("the fixture summary encodes")],
            }),
        ];
        for answer in answers {
            let frame = encode_frame(&answer).expect("the fixture answers encode");
            if peer_send.send(frame).await.is_err() {
                return;
            }
        }
        // Holds the stream open, as a live peer would.
        std::future::pending::<()>().await;
    });
}

/// Where `DiscoveryExchangeProgress` lives, and the lines of its
/// `complete` and `wait_complete` bodies and of the three `mark_*` notifies.
struct Sites {
    file: &'static str,
    /// `complete` through the end of `wait_complete`, the code a waiter runs
    /// between checking for completion and waiting.
    waiting: RangeInclusive<u32>,
    marks: Vec<u32>,
}

impl Sites {
    /// Finds the sites in the discovery service's source, so the check
    /// follows the code as it moves.
    fn find() -> Sites {
        let file = "crates/zakura-network/src/zakura/discovery/service.rs";
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../zakura-network/src/zakura/discovery/service.rs"
        );
        let source =
            std::fs::read_to_string(path).expect("the discovery source is in the workspace");
        let lines: Vec<&str> = source.lines().collect();
        let find = |from: usize, needle: &str| {
            from + lines[from..]
                .iter()
                .position(|l| l.contains(needle))
                .unwrap_or_else(|| panic!("service.rs has `{needle}`"))
        };
        // Lines are 1-based, and the file has far fewer than 2^32 of them.
        let line = |i: usize| (i + 1) as u32;
        let progress = find(0, "impl DiscoveryExchangeProgress");
        let complete = find(progress, "fn complete(&self)");
        let wait_complete = find(complete, "async fn wait_complete(&self)");
        let end = wait_complete
            + lines[wait_complete..]
                .iter()
                .position(|l| *l == "    }")
                .expect("wait_complete's body closes");
        let marks = ["fn mark_hello", "fn mark_peers", "fn mark_services"]
            .iter()
            .map(|mark| line(find(find(progress, mark), "notify_waiters()")))
            .collect();
        Sites {
            file,
            waiting: line(complete)..=line(end),
            marks,
        }
    }

    /// The line of `site` if it's in the discovery service's file.
    fn line(&self, site: &Option<String>) -> Option<u32> {
        let (file, line) = site.as_deref()?.rsplit_once(':')?;
        if file.ends_with(self.file) {
            line.parse().ok()
        } else {
            None
        }
    }
}

/// The source task's name, which `spawn_supervised_peer_task` labels.
fn is_source(task: &str) -> bool {
    task.ends_with("/discovery_source.1")
}

/// The task an identity runs, if it runs one.
fn task_of(identity: &str) -> Option<&str> {
    let rest = identity.strip_prefix("task:")?;
    Some(&rest[..rest.rfind(':')?])
}

/// Checks `discovery/wait-complete-wakes`, which properties.md's P6 will
/// formalize: at the first step with no ready task after the sink has run
/// all three `mark_*` notifies, the source must not be waiting after a point
/// in `complete` or `wait_complete`.
struct WaitCompleteWakes {
    sites: Sites,
    /// Each task's next point, as (kind, line), from its last stop.
    next: BTreeMap<String, (String, Option<u32>)>,
    /// The step that ran each mark's notify.
    marked: Vec<Option<u64>>,
    /// The source's last load and `notified` steps in the waiting code.
    last_load: Option<u64>,
    last_notified: Option<u64>,
    /// True once the source has created its `Notified` in the waiting code
    /// and hasn't checked for completion since, so it can only go on when
    /// woken.
    in_wait: bool,
    /// For building inputs: each step's enabled set, the index it picked,
    /// and the index that holds the source back at its `notified`.
    trace: Vec<(Vec<String>, usize, Option<usize>)>,
    /// Steps where the check applied: no ready task, every mark run.
    applicable: u32,
}

impl WaitCompleteWakes {
    fn new() -> Self {
        let sites = Sites::find();
        let marks = sites.marks.len();
        WaitCompleteWakes {
            sites,
            next: Default::default(),
            marked: vec![None; marks],
            last_load: None,
            last_notified: None,
            in_wait: false,
            trace: Vec::new(),
            applicable: 0,
        }
    }

    /// True if every mark's notify has run.
    fn all_marked(&self) -> bool {
        self.marked.iter().all(Option::is_some)
    }

    /// An enabled action other than the source, when the source's next
    /// point is its `notified` in the waiting code and a mark is still to
    /// come.
    fn hold_source(&self, enabled: &[String]) -> Option<usize> {
        let source_at_notified = enabled.iter().filter_map(|e| task_of(e)).any(|task| {
            is_source(task)
                && matches!(self.next.get(task), Some((at, Some(line)))
                    if at.starts_with("notified") && self.sites.waiting.contains(line))
        });
        if !source_at_notified || self.all_marked() {
            return None;
        }
        enabled
            .iter()
            .position(|e| !task_of(e).is_some_and(is_source))
    }
}

impl Checker for WaitCompleteWakes {
    fn after_step(&mut self, cx: &StepCtx<'_>) -> Option<Violation> {
        let enabled: Vec<String> = cx.enabled.iter().map(|e| e.identity.to_string()).collect();
        let pick = cx.pick.to_string();
        // The loop picks from the enabled set, so the pick is always in it.
        let index = enabled.iter().position(|e| *e == pick).unwrap_or_default();
        let hold = self.hold_source(&enabled);
        self.trace.push((enabled, index, hold));

        // What the pick just ran.
        if let Some(task) = task_of(&pick) {
            let ran = self.next.get(task).cloned();
            if let Some((at, Some(line))) = &ran {
                if let Some(i) = self.sites.marks.iter().position(|m| m == line) {
                    self.marked[i] = Some(cx.step);
                }
                if is_source(task) && self.sites.waiting.contains(line) {
                    if at.starts_with("load") {
                        self.last_load = Some(cx.step);
                        self.in_wait = false;
                    } else if at.starts_with("notified") {
                        self.last_notified = Some(cx.step);
                        self.in_wait = true;
                    }
                }
            }
        }
        for obs in cx.observations() {
            if let Observation::TaskReached { task, at, site } = obs {
                self.next
                    .insert(task.clone(), (at.clone(), self.sites.line(site)));
            }
        }

        if !self.all_marked() || cx.status == Status::Running {
            return None;
        }
        self.applicable += 1;
        if !self.in_wait {
            return None;
        }
        let final_notify = self.marked.iter().flatten().copied().max()?;
        Some(Violation {
            property: PROPERTY.into(),
            step: cx.step,
            witness: [self.last_load, Some(final_notify), self.last_notified]
                .into_iter()
                .flatten()
                .collect(),
            expected: "the source wakes once Hello, Peers and Services are marked".into(),
            actual: "the source still waits in wait_complete".into(),
        })
    }

    fn at_end(&mut self, end: &RunEnd, _: &ObservationLog) -> Vec<PropertyReport> {
        let outcome = match &end.violation {
            Some(violation) => PropertyOutcome::Violated {
                witness: violation.witness.clone(),
            },
            None if self.applicable > 0 => PropertyOutcome::Held {
                applicable: self.applicable,
            },
            None => PropertyOutcome::NotApplicable,
        };
        vec![PropertyReport {
            property: PROPERTY.into(),
            outcome,
        }]
    }
}

/// The run config: 2 workers, a budget well past the exchange, and tokio's
/// worker stack, since record verification runs deep in a debug build.
fn run_config() -> zakura_sim::RunConfig {
    let mut config = config(2, 3_000);
    config.scheduler.stack_size = zakura_scheduler::SchedulerConfig::default().stack_size;
    config
}

/// Runs the scenario under `plan` with the check. Returns the report, the
/// records, and the checker's trace.
fn run(plan: PlanInput) -> (RunReport, Vec<String>, WaitCompleteWakes) {
    let mut checker = WaitCompleteWakes::new();
    let (report, lines) = recorded(run_config(), plan, scenario, &mut checker);
    (report, lines, checker)
}

/// How many bytes choose among `range` values, as the `Plan` decodes them.
fn width(range: u128) -> usize {
    match range {
        0..=0x100 => 1,
        0x101..=0x1_0000 => 2,
        0x1_0001..=0x1_0000_0000 => 4,
        _ => 8,
    }
}

/// Appends `value` as `width(range)` little-endian bytes.
fn push(bytes: &mut Vec<u8>, value: u64, range: u128) {
    bytes.extend_from_slice(&value.to_le_bytes()[..width(range)]);
}

/// Appends a draw's answer, unless its bound makes it forced.
fn push_draw(bytes: &mut Vec<u8>, draw: &DrawRecord) {
    match draw.bound.0 {
        1 => {}
        0 => push(bytes, draw.value.0, 1 << 64),
        bound => push(bytes, draw.value.0, u128::from(bound)),
    }
}

/// Builds a fuzz input that holds the source back at its `notified` until
/// the sink has run all three marks. Each round reruns the input, finds the
/// first step where the round-robin default didn't hold the source back,
/// and rewrites the input from there.
///
/// Returns `Ok` with the input once it violates the property, or `Err` with
/// the finished input if holding the source back everywhere it can doesn't.
fn hold_source_back() -> Result<Vec<u8>, Vec<u8>> {
    let mut bytes = Vec::new();
    for _ in 0..1_000 {
        let (report, lines, checker) = run(Plan::from_fuzz(&bytes));
        if report.end.violation.is_some() {
            return Ok(bytes);
        }
        let step_of = |line: &String| match serde_json::from_str::<Record>(line).unwrap() {
            Record::Step(step) => Some(step),
            Record::Init(_) | Record::End(_) => None,
        };
        let steps: Vec<_> = lines.iter().filter_map(step_of).collect();
        let Some(deviation) = checker
            .trace
            .iter()
            .position(|(_, picked, hold)| hold.is_some_and(|h| h != *picked))
        else {
            return Err(bytes);
        };
        let mut next = Vec::new();
        if let Ok(Record::Init(init)) = serde_json::from_str::<Record>(&lines[0]) {
            init.draws.iter().for_each(|d| push_draw(&mut next, d));
        }
        for (step, (_, picked, hold)) in steps.iter().zip(&checker.trace).take(deviation + 1) {
            let index = if step.step == steps[deviation].step {
                hold.expect("the deviation holds the source")
            } else {
                *picked
            };
            if step.options > 1 {
                // A step's index and option count fit in u64 and u128.
                push(&mut next, index as u64, step.options as u128);
            }
            if step.step < steps[deviation].step {
                step.draws.iter().for_each(|d| push_draw(&mut next, d));
            }
        }
        bytes = next;
    }
    panic!("no input found in 1,000 rounds");
}

/// The committed input that holds the source back, from
/// [`hold_source_back`] on the code before the fix.
const INPUT: &[u8] = include_bytes!("fixtures/discovery-wait-complete.bin");

/// The regression, checked against the bug. CI's `sim-build` job applies
/// `fixtures/discovery-wait-complete-revert.patch`, which puts the old
/// `wait_complete` back, then runs this test by name. Unpatched, it would
/// fail, so it's ignored.
#[test]
#[ignore = "needs fixtures/discovery-wait-complete-revert.patch applied"]
fn reverted_fix_misses_the_wakeup() {
    let (report, _, _) = run(Plan::from_fuzz(INPUT));
    let violation = report.end.violation.expect("the committed input violates");
    assert_eq!(violation.property, PROPERTY);
    let [load, notify, notified] = violation.witness[..] else {
        panic!("the witness is load, final notify, notified: {violation:?}");
    };
    assert!(
        load < notify && notify < notified,
        "the source checked, the sink notified, then the source created its future: {violation:?}"
    );

    // Unchecked, the run shows what that costs: the source sits out its
    // 2 s initial-exchange timeout.
    let (report, _) = recorded(
        run_config(),
        Plan::from_fuzz(INPUT),
        scenario,
        &mut NoChecks,
    );
    assert!(
        report
            .picks
            .iter()
            .any(|p| p.identity == "advance-time:2000000000"),
        "the exchange timed out"
    );
}

#[test]
#[ignore = "rewrites the committed input; needs fixtures/discovery-wait-complete-revert.patch applied"]
fn regenerate_wait_complete_input() {
    let bytes = hold_source_back().expect("the reverted code misses the wakeup");
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/discovery-wait-complete.bin"
    );
    std::fs::write(path, &bytes).expect("the fixture directory exists");
}

/// Asserts `report` held the property at least once and never timed out
/// the exchange.
fn assert_wakes(report: &RunReport) {
    assert_eq!(report.end.violation, None);
    assert!(
        matches!(report.properties[0].outcome, PropertyOutcome::Held { applicable } if applicable > 0),
        "the run reached the point the property checks: {:?}",
        report.properties
    );
    assert!(
        !report
            .picks
            .iter()
            .any(|p| p.identity == "advance-time:2000000000"),
        "the exchange completed before its timeout"
    );
}

#[test]
fn committed_input_wakes() {
    let (report, _, _) = run(Plan::from_fuzz(INPUT));
    assert_wakes(&report);
}

#[test]
fn holding_the_source_back_finds_no_violation() {
    let bytes = hold_source_back().expect_err("the fix wakes the source");
    let (report, _, _) = run(Plan::from_fuzz(&bytes));
    assert_wakes(&report);
}

#[test]
fn seed_sweep_wakes() {
    for plan in inputs(200, 64) {
        let (report, _, _) = run(plan);
        assert_wakes(&report);
    }
}
