//! S4.7: discovery's `wait_complete` wakes once the initial exchange
//! completes. A scripted peer answers the node's Hello, GetPeers and
//! GetServices over an in-memory stream, on 2 workers.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::UNIX_EPOCH,
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
use zakura_quic::{NodeId, NodeSecretKey};
use zakura_rt::{
    collections::HashMap,
    sync::{watch, CancellationToken},
    time::utc_now,
};
use zakura_scheduler::SchedulerConfig;

use super::Subsystem;
use crate::{
    domain::discovery::wait_complete_wakes,
    property::{HasView, Property},
    RunConfig, Sim,
};

/// The `wait_complete` scenario. It has no knobs, so its fuzz input is all
/// Plan bytes.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {}

/// Discovery with one inbound peer that answers the initial exchange,
/// checked by [`wait_complete_wakes`].
pub struct WaitComplete;

impl HasView for WaitComplete {
    type View = ();

    /// Nothing. The property reads the observations.
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

    /// [`wait_complete_wakes`].
    fn properties() -> Vec<Property<Self>> {
        vec![wait_complete_wakes()]
    }

    /// The only scenario.
    fn draw(_: &[u8]) -> Scenario {
        Scenario {}
    }
}

/// A signed record for `secret`, valid for ten minutes of the run's UTC, which
/// is the clock discovery validates it against.
fn record(secret: &NodeSecretKey, handshake: &ZakuraHandshakeConfig) -> ZakuraNodeRecord {
    let now = utc_now()
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

/// What a test can read after a run of [`spawn`]'s scenario.
pub struct Spawned {
    /// A clone of the handle the node's discovery service runs on.
    pub handle: ZakuraDiscoveryHandle,
    /// The scripted peer's node id, which its Hello record carries.
    pub peer: NodeId,
}

/// Starts the discovery service with one inbound peer, and a task that plays
/// the peer: it waits for the node's three messages, then answers with its
/// Hello, Peers and Services.
pub fn spawn(sim: &mut Sim) -> Spawned {
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
    let spawned = Spawned {
        handle: handle.clone(),
        peer: peer_secret.public(),
    };
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
    spawned
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
