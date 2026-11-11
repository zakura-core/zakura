//! The production wire node against the shared scripted handshake peer.

use std::sync::Arc;

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
}

impl Default for Scenario {
    fn default() -> Self {
        Self {
            net: NetScenario::default(),
            peers: vec![PeerSpec::dialing()],
        }
    }
}

/// The node's genesis state, prepared outside simulated time.
pub struct Prepared {
    fixture: HeaderFixture,
    port: Arc<ControlledPort>,
}

/// Checks only the handshake while the installed services remain idle.
pub struct Wire;

impl HasView for Wire {
    type View = ();
    fn view(&self) {}
}

impl Subsystem for Wire {
    const NAME: &'static str = "handshake/wire";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 32;
    type Scenario = Scenario;
    type Prepared = Prepared;

    fn config(_: &Scenario) -> RunConfig {
        wire_config()
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
        let (observer, tap) = TransportTap::new(sim, "node", net.clone(), names.clone());
        let dials = tap.dials();
        let shared_ids = tap.shared_ids();
        sim.add_source(Box::new(tap));
        let handshake = ZakuraHandshakeConfig::for_network(&network);
        let node = WireNode::spawn_with_services(
            sim,
            header,
            &network,
            net.sim(),
            seed,
            observer,
            dials,
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
                vec![Arc::new(DiscoveryService::with_max_connections(
                    handle,
                    limits.max_connections,
                )) as Arc<dyn Service>]
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
                node: NodeAddr::with_addrs(SeededInputs::node_key(seed, 0).public(), [NODE_ADDR]),
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
                node.dial(sim, index, listened);
            }
        }
        sim.add_source(Box::new(control));
        Wire
    }

    fn properties() -> Vec<Property<Self>> {
        crate::table::handshake::properties()
    }

    fn sources() -> Sources {
        Sources::observed(&[
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
        match byte(0) % 6 {
            0 => script.control = HELLO_FAULTS[usize::from(byte(1)) % HELLO_FAULTS.len()],
            1 => {
                scenario.peers[0] = PeerSpec::listening();
                script.control = ACK_FAULTS[usize::from(byte(1)) % ACK_FAULTS.len()];
            }
            2 => script.prelude = PRELUDE_FAULTS[usize::from(byte(1)) % PRELUDE_FAULTS.len()],
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
                script.control = ControlFault::Network;
                script.bind_ip = Some("192.0.2.1".parse().unwrap());
                script.retries = byte(1) % 32;
                script.retry_delay_ms = u64::from(byte(2));
            }
            _ => unreachable!("six families"),
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
        for family in 0..6 {
            let mut bytes = [179; Wire::SCENARIO_BYTES];
            bytes[0] = family;
            let scenario = Wire::draw(&bytes);
            let json = serde_json::to_string(&scenario).unwrap();
            assert_eq!(scenario, serde_json::from_str::<Scenario>(&json).unwrap());
        }
    }
}
