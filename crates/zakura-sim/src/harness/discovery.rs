//! S4.7: discovery's `wait_complete` wakes once the initial exchange
//! completes. A scripted peer answers the node's Hello, GetPeers and
//! GetServices over an in-memory stream, on 2 workers.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, UNIX_EPOCH},
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
    domain::discovery::{verification_capacity, wait_complete_wakes, VerificationView},
    property::{HasView, Property, Source, Sources},
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
    type Prepared = ();

    /// [`run_config`].
    fn config(_: &Scenario) -> RunConfig {
        run_config()
    }

    /// Nothing to set up.
    fn prepare(_: &Scenario) {}

    /// [`spawn`].
    fn build(sim: &mut Sim, _: &Scenario, (): ()) -> Self {
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
fn record(secret: &NodeSecretKey, handshake: &ZakuraHandshakeConfig, now: u64) -> ZakuraNodeRecord {
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
        expires_at_unix_secs: now.saturating_add(600),
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
        let now = utc_now()
            .duration_since(UNIX_EPOCH)
            .expect("the run clock is after the epoch")
            .as_secs();
        let answers = [
            DiscoveryMessage::Hello {
                record: record(&peer_secret, &handshake, now),
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

/// The readers [`VerificationCapacity`] runs, each with one relayed record.
pub const VERIFICATION_READERS: u64 = 3;
/// The verification slots [`VerificationCapacity`] fixes, in place of the host's count.
pub const VERIFICATION_SLOTS: u64 = 1;

/// The `verification-capacity` scenario. It has no knobs, so its fuzz input is
/// all Plan bytes.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityScenario {}

/// What the harness's tasks publish for the nonblocking view.
#[derive(Debug, Default)]
pub struct CapacityEvidence {
    /// One more than the records the book held when a slow holder took the state
    /// lock, or 0 while no holder has it.
    pub held_records: AtomicU64,
    /// The readers whose import returned.
    pub done: AtomicU64,
    /// The largest count of unsettled verification results that a check saw.
    pub peak_unsettled: AtomicU64,
}

/// The real discovery handle, with readers importing relayed records while a
/// slow holder keeps the state lock, checked by [`verification_capacity`].
pub struct VerificationCapacity {
    handle: ZakuraDiscoveryHandle,
    evidence: Arc<CapacityEvidence>,
}

impl HasView for VerificationCapacity {
    type View = VerificationView;

    /// Reads the slots and the tasks' published facts without blocking.
    fn view(&self) -> VerificationView {
        VerificationView {
            free_slots: u64::try_from(self.handle.free_verification_slots())
                .expect("the fixture's slot count is small"),
            evidence: Arc::clone(&self.evidence),
        }
    }
}

impl Subsystem for VerificationCapacity {
    const NAME: &'static str = "discovery/verification-capacity";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = CapacityScenario;
    type Prepared = Arc<CapacityEvidence>;

    /// [`run_config`].
    fn config(_: &CapacityScenario) -> RunConfig {
        run_config()
    }

    /// The evidence a caller may keep, to read after the run.
    fn prepare(_: &CapacityScenario) -> Arc<CapacityEvidence> {
        Arc::default()
    }

    /// [`spawn_capacity`].
    fn build(sim: &mut Sim, _: &CapacityScenario, evidence: Arc<CapacityEvidence>) -> Self {
        let handle = spawn_capacity(sim, &evidence);
        VerificationCapacity { handle, evidence }
    }

    /// [`verification_capacity`].
    fn properties() -> Vec<Property<Self>> {
        vec![verification_capacity()]
    }

    /// The schedule shows actual workers, and the view shows the handle's slots.
    fn sources() -> Sources {
        Sources::observed(&[Source::Schedule, Source::View])
    }

    /// The only scenario.
    fn draw(_: &[u8]) -> CapacityScenario {
        CapacityScenario {}
    }
}

/// Starts the readers and the slow state holder on one real handle.
fn spawn_capacity(sim: &mut Sim, evidence: &Arc<CapacityEvidence>) -> ZakuraDiscoveryHandle {
    let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
    let (connected, connected_rx) = watch::channel(Vec::new());
    let handle = ZakuraDiscoveryHandle::new(
        ZakuraDiscoveryLocalConfig {
            secret_key: NodeSecretKey::from_bytes(&[31; 32]),
            direct_addrs: Vec::new(),
            services: vec![ZakuraServiceId::discovery()],
            zakura_protocol_min: handshake.zakura_protocol_min,
            zakura_protocol_max: handshake.zakura_protocol_max,
            network_id: handshake.network_id,
            chain_id: handshake.chain_id,
            last_authored_sequence: None,
        },
        ZakuraDiscoveryConfig::default(),
        connected_rx,
    )
    .expect("the fixture config is valid")
    .with_verification_slots(VERIFICATION_SLOTS as usize);
    let relay = NodeSecretKey::from_bytes(&[32; 32]).public();
    for (reader, name) in ["reader-0", "reader-1", "reader-2"].into_iter().enumerate() {
        let handle = handle.clone();
        let evidence = Arc::clone(evidence);
        let handshake = handshake.clone();
        let author = 33 + u8::try_from(reader).expect("the fixture has few readers");
        sim.scheduler().spawn_root(name, async move {
            let now = utc_now()
                .duration_since(UNIX_EPOCH)
                .expect("the run clock is after the epoch")
                .as_secs();
            let secret = NodeSecretKey::from_bytes(&[author; 32]);
            let outcome = handle
                .import_peer_records([record(&secret, &handshake, now)], Some(relay))
                .await
                .expect("the fixture record is authentic");
            assert_eq!(outcome.added, 1, "the fixture record is new");
            evidence.done.fetch_add(1, Ordering::SeqCst);
        });
    }
    let holder = handle.clone();
    let evidence = Arc::clone(evidence);
    sim.scheduler().spawn_root("holder", async move {
        // Keeps the handle's connected watch open for the whole run.
        let _connected = connected;
        while evidence.done.load(Ordering::SeqCst) < VERIFICATION_READERS {
            if holder.free_verification_slots() == 0 {
                let evidence = &evidence;
                holder
                    .hold_state(|records| async move {
                        let records = u64::try_from(records).expect("the fixture book is small");
                        evidence.held_records.store(records + 1, Ordering::SeqCst);
                        zakura_rt::time::sleep(Duration::from_secs(1)).await;
                        evidence.held_records.store(0, Ordering::SeqCst);
                    })
                    .await;
            }
            zakura_rt::task::yield_now().await;
        }
    });
    handle
}
