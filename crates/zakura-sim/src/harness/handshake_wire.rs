//! The production wire node against the shared scripted handshake peer.

use std::sync::Arc;
use zakura_network::zakura::{Peer, Stream, ZakuraConnId, ZakuraPeerId};

use serde::{Deserialize, Serialize};
use zakura_network::zakura::{
    testkit::discovery::build_discovery_handle, DiscoveryService, Service, ZakuraHandshakeConfig,
    ZakuraServiceId,
};
use zakura_quic::{sim::SeededInputs, NodeAddr};
use zakura_storage::header::{controlled::ControlledPort, fixture::HeaderFixture};

use super::{
    header_node::{Chain, DispatchFlag, Effects, HeaderNode, Policy, PolicySource},
    header_sync::wire_config,
    wire_node::{TransportTap, WireNode},
    NetScenario, Subsystem,
};
use crate::{
    peer::{
        handshake::options::{ControlFault, HandshakeSpec, PreludeFault},
        Dialer, PeerCloses, PeerControl, PeerFailure, PeerRecords, PeerSpec, PeerTask, NODE_ADDR,
    },
    property::{HasView, Property, Source, Sources},
    RunConfig, Sim,
};

/// Each peer carries one connection's control, prelude, address and timing choices.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    /// The deterministic network and identity seed.
    #[serde(default)]
    pub net: NetScenario,
    /// Concurrent endpoints, which may share addresses or identities.
    pub peers: Vec<PeerSpec>,
    /// A runtime capability change ordered against the control handshake.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability_flip: Option<CapabilityFlip>,
}

/// The reconnect snapshot's order relative to an in-flight handshake.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CapabilityFlip {
    /// Enable header sync after the handshake snapshot but before registration.
    BeforeRegistration,
    /// Enable header sync after registration enters the reconnect snapshot.
    AfterRegistration,
    /// Enable, disable, and enable again before registration.
    TwiceBeforeRegistration,
}

impl Default for Scenario {
    fn default() -> Self {
        Self {
            net: NetScenario::default(),
            peers: vec![PeerSpec::dialing()],
            capability_flip: None,
        }
    }
}

/// The node's genesis state, prepared outside simulated time.
pub struct Prepared {
    fixture: HeaderFixture,
    port: Arc<ControlledPort>,
}

/// Checks only the handshake while the installed services remain idle.
pub struct Wire {
    scenario: Arc<Scenario>,
}

impl HasView for Wire {
    type View = Arc<Scenario>;
    fn view(&self) -> Self::View {
        self.scenario.clone()
    }
}

impl Subsystem for Wire {
    const NAME: &'static str = "handshake/wire";
    const GENERATOR: u32 = 2;
    const SCENARIO_BYTES: usize = 32;
    type Scenario = Scenario;
    type Prepared = Prepared;

    fn config(scenario: &Scenario) -> RunConfig {
        let mut config = wire_config();
        config.budget = config.budget.max(
            u64::try_from(scenario.peers.len())
                .expect("peer count fits u64")
                .saturating_mul(2_000),
        );
        config
    }

    fn prepare(_: &Scenario) -> Prepared {
        let fixture = HeaderFixture::regtest();
        let port = Arc::new(ControlledPort::new(&fixture));
        Prepared { fixture, port }
    }

    fn build(sim: &mut Sim, scenario: &Scenario, prepared: Prepared) -> Self {
        let seed = scenario.net.seed;
        let network = prepared.fixture.network().clone();
        let net = scenario.net.net();
        sim.add_source(Box::new(net.clone()));
        let policy = PolicySource::new(
            sim,
            prepared.port.clone(),
            Policy::converging(),
            Effects::default(),
        );
        sim.add_source(Box::new(policy));
        let header = HeaderNode::spawn(
            sim,
            "node",
            1,
            prepared.port,
            &prepared.fixture,
            Chain::default().tip(&prepared.fixture),
            DispatchFlag::default(),
            None,
        );
        let names: Vec<_> = (0..scenario.peers.len())
            .map(|i| format!("peer-{i}"))
            .collect();
        let (observer, tap) = TransportTap::sampling(sim, "node", net.clone(), names.clone());
        let (observer, capability_control) = if let Some(order) = scenario.capability_flip {
            use zakura_network::zakura::{HandshakePause, HandshakePhase};
            let (reached, observed) = zakura_rt::sync::watch::channel(HandshakePhase::Waiting);
            let (resume, released) = zakura_rt::sync::watch::channel(HandshakePhase::Waiting);
            (
                Arc::new(CapabilityObserver {
                    inner: observer,
                    pause: HandshakePause {
                        reached,
                        resume: released,
                    },
                }) as Arc<dyn zakura_network::zakura::TransportObserver>,
                Some((order, observed, resume)),
            )
        } else {
            (observer, None)
        };
        let dials = tap.dials();
        let shared_ids = tap.shared_ids();
        sim.add_source(Box::new(tap.with_handshake_counts()));
        let handshake = ZakuraHandshakeConfig::for_network(&network);
        let node_v6 = "[2001:db8::1]:4000".parse().unwrap();
        let listen_addrs = if scenario
            .peers
            .iter()
            .any(|peer| peer.address(0, 0).is_ipv6())
        {
            vec![NODE_ADDR, node_v6]
        } else {
            vec![NODE_ADDR]
        };
        let node = WireNode::spawn_with_services(
            sim,
            header,
            &network,
            net.sim(),
            seed,
            observer,
            dials,
            listen_addrs,
            move |supervisor, limits| {
                let handle = build_discovery_handle(
                    SeededInputs::node_key(seed, 0),
                    vec![NODE_ADDR],
                    vec![ZakuraServiceId::discovery()],
                    &handshake,
                    limits.max_connections,
                    0,
                    supervisor.subscribe(),
                )
                .expect("the Regtest discovery configuration is valid")
                .with_verification_slots(1);
                vec![
                    Arc::new(DiscoveryService::with_max_connections(
                        handle,
                        limits.max_connections,
                    )) as Arc<dyn Service>,
                    Arc::new(PreludeLayouts),
                ]
            },
            move |endpoint| async move {
                use zakura_network::zakura::HandshakePhase;
                let Some((order, mut reached, resume)) = capability_control else {
                    return;
                };
                endpoint.disable_header_sync_for_sim();
                if reached
                    .wait_for(|phase| *phase == HandshakePhase::Snapshot)
                    .await
                    .is_err()
                {
                    return;
                }
                if order == CapabilityFlip::AfterRegistration {
                    resume.send_replace(HandshakePhase::Snapshot);
                    if reached
                        .wait_for(|phase| *phase == HandshakePhase::Registered)
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                endpoint.enable_header_sync_for_sim(1).await;
                if order == CapabilityFlip::TwiceBeforeRegistration {
                    endpoint.disable_header_sync_for_sim();
                    endpoint.enable_header_sync_for_sim(2).await;
                }
                resume.send_replace(HandshakePhase::Registered);
            },
        );
        let failure = PeerFailure::default();
        let records = PeerRecords::default();
        let mut control = PeerControl::new(failure.clone(), records.clone(), shared_ids);
        for ((index, spec), name) in scenario.peers.iter().enumerate().zip(names) {
            let (readiness, commands) = control.add(&name, spec.script.clone(), false);
            let (listening, listened) = zakura_rt::sync::watch::channel(false);
            let task = PeerTask {
                name,
                index,
                spec: spec.clone(),
                seed,
                net: net.sim(),
                node: NodeAddr::with_addrs(
                    SeededInputs::node_key(seed, 0).public(),
                    [if spec.address(index, 0).is_ipv6() {
                        node_v6
                    } else {
                        NODE_ADDR
                    }],
                ),
                commands,
                readiness,
                failure: failure.clone(),
                listening,
                chain: None,
                records: records.clone(),
                closed: PeerCloses::default(),
                taken: 0,
            };
            sim.scheduler().spawn_root("peer", task.run());
            if spec.dialer == Dialer::Node {
                node.dial_handshake(sim, index, spec, listened);
            }
        }
        sim.add_source(Box::new(control));
        Wire {
            scenario: Arc::new(scenario.clone()),
        }
    }

    fn properties() -> Vec<Property<Self>> {
        let mut properties = crate::table::handshake::properties();
        properties.push(crate::table::handshake::installed_services_authorized());
        properties
    }

    fn sources() -> Sources {
        Sources::observed(&[
            Source::View,
            Source::Schedule,
            Source::Datagrams,
            Source::Frames,
            Source::Closes,
        ])
    }

    fn draw(prefix: &[u8]) -> Scenario {
        let byte = |index| prefix.get(index).copied().unwrap_or(0);
        let mut scenario = Scenario::default();
        let mut script = HandshakeSpec::default();
        match byte(0) % 8 {
            0 => script.control = HELLO_FAULTS[usize::from(byte(1)) % HELLO_FAULTS.len()],
            1 => {
                scenario.peers[0] = PeerSpec::listening();
                script.control = ACK_FAULTS[usize::from(byte(1)) % ACK_FAULTS.len()];
            }
            2 => {
                script.prelude = PRELUDE_FAULTS[usize::from(byte(1)) % PRELUDE_FAULTS.len()];
                if script.prelude == PreludeFault::IneligibleOpener {
                    scenario.peers[0] = PeerSpec::listening();
                }
                if script.prelude == PreludeFault::Version {
                    scenario.peers[0].capabilities |= (1 << 60) | (1 << 61);
                }
                if script.prelude == PreludeFault::MissingRequestId {
                    scenario.peers[0].capabilities |= 1 << 62;
                }
            }
            3 => {
                script.open_delay_ms = u64::from(byte(1)) * 40;
                script.length_delay_ms = u64::from(byte(2)) * 40;
                script.payload_delay_ms = u64::from(byte(3)) * 40;
                script.byte_delay_ms = u64::from(byte(4));
                script.abandon_after = (byte(5) != 0).then_some(usize::from(byte(5)));
            }
            4 => {
                script.bind_ip = Some(
                    match byte(1) % 3 {
                        0 => "192.0.2.1",
                        1 => "::ffff:192.0.2.1",
                        _ => "2001:db8:1::1",
                    }
                    .parse()
                    .unwrap(),
                );
                script.open_delay_ms = u64::from(byte(2)) * 40;
                scenario.peers = vec![PeerSpec::dialing(); usize::from(byte(3) % 32) + 1];
            }
            5 => {
                script.control = if byte(3) == 0 {
                    ControlFault::None
                } else {
                    ControlFault::Network
                };
                script.bind_ip = Some("192.0.2.1".parse().unwrap());
                script.retries = byte(1) % 32;
                script.retry_delay_ms = u64::from(byte(2));
            }
            6 => {
                scenario.capability_flip = match byte(1) % 4 {
                    0 => None,
                    1 => Some(CapabilityFlip::BeforeRegistration),
                    2 => Some(CapabilityFlip::AfterRegistration),
                    _ => Some(CapabilityFlip::TwiceBeforeRegistration),
                };
                script.dial_delay_ms = 1;
            }
            7 => {
                let incoming = usize::from(byte(1) % 96);
                let outgoing = usize::from(byte(2) % 8) + 1;
                scenario.peers = (0..incoming)
                    .map(|index| {
                        let mut peer = PeerSpec::dialing();
                        peer.handshake = Some(HandshakeSpec {
                            dial_delay_ms: u64::try_from(index).expect("peer index fits u64")
                                * u64::from(byte(5)),
                            open_delay_ms: u64::from(byte(3)) * 40,
                            ..HandshakeSpec::default()
                        });
                        peer
                    })
                    .chain((0..outgoing).map(|_| {
                        let mut peer = PeerSpec::listening();
                        peer.handshake = Some(HandshakeSpec {
                            dial_delay_ms: u64::from(byte(4)) * 10,
                            ..HandshakeSpec::default()
                        });
                        peer
                    }))
                    .collect();
                return scenario;
            }
            _ => unreachable!("eight families"),
        }
        for peer in &mut scenario.peers {
            peer.handshake = Some(script.clone());
        }
        scenario
    }
}

/// Every independently corruptible hello guard, with the honest case first.
pub const HELLO_FAULTS: &[ControlFault] = &[
    ControlFault::None,
    ControlFault::Magic,
    ControlFault::Version,
    ControlFault::Protocol,
    ControlFault::Path,
    ControlFault::Role,
    ControlFault::Network,
    ControlFault::Chain,
    ControlFault::Identity,
    ControlFault::RequiredChannel,
    ControlFault::FrameLimit,
    ControlFault::MessageLimit,
    ControlFault::StreamLimit,
    ControlFault::QueueLimit,
    ControlFault::IdleLimit,
    ControlFault::InitiatorNonce,
    ControlFault::ResponderNonce,
    ControlFault::Transcript,
    ControlFault::Oversize,
];
/// Ack fields that production validates, with the honest case first.
pub const ACK_FAULTS: &[ControlFault] = &[
    ControlFault::None,
    ControlFault::Magic,
    ControlFault::Version,
    ControlFault::Protocol,
    ControlFault::NonceEcho,
    ControlFault::UnofferedCapability,
    ControlFault::FrameLimit,
    ControlFault::MessageLimit,
    ControlFault::StreamLimit,
    ControlFault::QueueLimit,
    ControlFault::IdleLimit,
    ControlFault::LimitsAboveRequest,
    ControlFault::Oversize,
];
/// Prelude fields that the common stream setup path judges.
pub const PRELUDE_FAULTS: &[PreludeFault] = &[
    PreludeFault::None,
    PreludeFault::Magic,
    PreludeFault::Kind,
    PreludeFault::Capability,
    PreludeFault::Version,
    PreludeFault::FrameCap,
    PreludeFault::UnexpectedRequestId,
    PreludeFault::MissingRequestId,
    PreludeFault::IneligibleOpener,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn draws_round_trip_millisecond_timing_and_address_families() {
        for family in 0..8 {
            let mut bytes = [179; Wire::SCENARIO_BYTES];
            bytes[0] = family;
            let scenario = Wire::draw(&bytes);
            let json = serde_json::to_string(&scenario).unwrap();
            assert_eq!(scenario, serde_json::from_str::<Scenario>(&json).unwrap());
        }
    }
}

/// Custom service declarations expose the transport's version and request-ID guards.
/// Production's built-in services currently declare only their newest persistent layout.
#[derive(Debug)]
struct PreludeLayouts;

#[derive(Debug)]
struct CapabilityObserver {
    inner: Arc<dyn zakura_network::zakura::TransportObserver>,
    pause: zakura_network::zakura::HandshakePause,
}

impl zakura_network::zakura::TransportObserver for CapabilityObserver {
    fn observe(&self, event: zakura_network::zakura::TransportEvent) {
        self.inner.observe(event);
    }
    fn handshakes(&self, probe: zakura_network::zakura::HandshakeProbe) {
        self.inner.handshakes(probe);
    }
    fn handshake_slot(
        &self,
        remote: std::net::SocketAddr,
        shared_id: [u8; 16],
        probe: zakura_quic::SlotProbe,
    ) {
        self.inner.handshake_slot(remote, shared_id, probe);
    }
    fn quic_admission(&self, probe: zakura_quic::AdmissionProbe) {
        self.inner.quic_admission(probe);
    }
    fn connection(&self, id: ZakuraConnId, probe: zakura_network::zakura::ConnectionProbe) {
        self.inner.connection(id, probe);
    }
    fn handshake_pause(&self) -> Option<zakura_network::zakura::HandshakePause> {
        Some(self.pause.clone())
    }
}

impl Service for PreludeLayouts {
    fn name(&self) -> &'static str {
        "handshake-layouts"
    }
    fn streams(&self) -> &[Stream] {
        &[
            Stream {
                kind: 900,
                version: 1,
                frame_cap: 1024,
                capability: 1 << 60,
                ..Stream::PERSISTENT
            },
            Stream {
                kind: 900,
                version: 2,
                frame_cap: 1024,
                capability: 1 << 61,
                ..Stream::PERSISTENT
            },
            Stream {
                kind: 901,
                version: 1,
                frame_cap: 1024,
                capability: 1 << 62,
                ..Stream::REQUEST_RESPONSE
            },
        ]
    }
    fn add_peer(&self, _peer: Peer) {}
    fn remove_peer(&self, _peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {}
}
