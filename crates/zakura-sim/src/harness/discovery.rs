//! S4.7: discovery's `wait_complete` wakes once the initial exchange
//! completes. A scripted peer answers the node's Hello, GetPeers and
//! GetServices over an in-memory stream, on 2 workers.

use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    ops::RangeInclusive,
    rc::Rc,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
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
use zakura_scheduler::{SchedulerConfig, Status};

use super::Subsystem;
use crate::{
    check::StepCtx,
    property::{HasView, Status as PropertyStatus},
    trace::meta::PropertyRef,
    Checker, Observation, ObservationLog, PropertyOutcome, PropertyReport, RunConfig, RunEnd, Sim,
    Violation,
};

/// The `wait_complete` scenario. It has no knobs, so its fuzz input is all
/// Plan bytes.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {}

/// Discovery with one inbound peer that answers the initial exchange,
/// checked by [`WaitCompleteWakes`].
pub struct WaitComplete;

impl HasView for WaitComplete {
    type View = ();

    /// Nothing. The checker reads the observations.
    fn view(&self) {}
}

impl Subsystem for WaitComplete {
    const NAME: &'static str = "discovery/wait-complete";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Scenario;

    /// [`run_config`].
    fn config(_: &Scenario) -> RunConfig {
        run_config()
    }

    /// [`spawn`].
    fn build(sim: &mut Sim, _: &Scenario) -> Self {
        spawn(sim);
        WaitComplete
    }

    /// [`WaitCompleteWakes`].
    fn checker(_: &Rc<Self>, _: &Scenario) -> Box<dyn Checker> {
        Box::new(WaitCompleteWakes::new())
    }

    /// [`PROPERTY`], active.
    fn properties(_: &Scenario) -> Vec<PropertyRef> {
        vec![PropertyRef {
            id: PROPERTY.to_owned(),
            status: PropertyStatus::Active,
        }]
    }

    /// The only scenario.
    fn draw(_: &[u8]) -> Scenario {
        Scenario {}
    }
}

/// The property this harness checks (properties.md, Discovery).
pub const PROPERTY: &str = "discovery/wait-complete-wakes";

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
pub fn spawn(sim: &mut Sim) {
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
#[derive(Debug, PartialEq, Eq)]
struct Sites {
    file: &'static str,
    /// `complete` through the end of `wait_complete`, the code a waiter runs
    /// between checking for completion and waiting.
    waiting: RangeInclusive<u32>,
    marks: Vec<u32>,
}

/// The discovery service's source, as the binary was built from it. The
/// observations carry the line numbers compiled into the binary, so the
/// sites must come from the same text, never from the checkout at run time.
const SERVICE_RS: &str = include_str!("../../../zakura-network/src/zakura/discovery/service.rs");

impl Sites {
    /// The sites in [`SERVICE_RS`], so the check follows the code as it
    /// moves.
    fn find() -> Sites {
        Sites::parse(SERVICE_RS)
    }

    /// The sites in `source`, the text of the discovery service's file.
    fn parse(source: &str) -> Sites {
        let file = "crates/zakura-network/src/zakura/discovery/service.rs";
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
pub struct WaitCompleteWakes {
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
    /// A checker that hasn't seen a step.
    pub fn new() -> Self {
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

    /// Each step's enabled set, the index it picked, and the index that
    /// would hold the source back at its `notified`, for building inputs by
    /// hand.
    pub fn choices(&self) -> &[(Vec<String>, usize, Option<usize>)] {
        &self.trace
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

impl Default for WaitCompleteWakes {
    /// [`WaitCompleteWakes::new`].
    fn default() -> Self {
        Self::new()
    }
}

impl Checker for WaitCompleteWakes {
    /// Tracks each task's next point and the marks, and checks the source at
    /// each step with no ready task once every mark has run.
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

    /// Reports the property: violated, held at every applicable step, or
    /// never applicable.
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
            holds_since: None,
        }]
    }
}

/// The run config: 2 workers, a budget well past the exchange, and tokio's
/// worker stack, since record verification runs deep in a debug build.
pub fn run_config() -> RunConfig {
    RunConfig {
        scheduler: SchedulerConfig {
            workers: 2,
            ..SchedulerConfig::default()
        },
        budget: 3_000,
        drain_bound: 1_000,
        ..RunConfig::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The checker's sites come from the embedded source, and only from the
    /// text it's given: a copy shifted down by 100 lines moves every site by
    /// 100, while the checker's own sites stay put.
    #[test]
    fn sites_come_from_the_embedded_source() {
        let built = Sites::find();
        assert_eq!(built, Sites::parse(SERVICE_RS));
        assert!(built.waiting.start() < built.waiting.end());
        assert_eq!(built.marks.len(), 3);

        let shifted = Sites::parse(&format!("{}{SERVICE_RS}", "\n".repeat(100)));
        assert_eq!(
            shifted.waiting,
            built.waiting.start() + 100..=built.waiting.end() + 100
        );
        assert_eq!(
            shifted.marks,
            built
                .marks
                .iter()
                .map(|line| line + 100)
                .collect::<Vec<_>>()
        );
        assert_eq!(Sites::find(), built);
    }
}
