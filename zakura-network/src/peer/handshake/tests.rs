//! Implements methods for testing [`Handshake`]

#![allow(clippy::unwrap_in_result)]

use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use super::*;
use crate::{
    peer_set::ActiveConnectionCounter,
    zakura::{
        Peer as ZakuraServicePeer, Service as ZakuraService, Stream, ZakuraPeerId,
        ZakuraUpgradeOutcome,
    },
    P2pStack,
};
use tokio::io::duplex;
use tower::ServiceExt;
use zakura_chain::{block, chain_tip::NoChainTip};
use zakura_test::mock_service::{MockService, PanicAssertion};

static TEST_ZAKURA_SECRET_KEY_COUNTER: AtomicUsize = AtomicUsize::new(1);

impl<S, C> Handshake<S, C>
where
    S: Service<Request, Response = Response, Error = BoxError> + Clone + Send + 'static,
    S::Future: Send,
    C: ChainTip + Clone + Send + 'static,
{
    /// Returns a count of how many connection nonces are stored in this [`Handshake`]
    pub async fn nonce_count(&self) -> usize {
        self.nonces.lock().await.len()
    }
}

fn peer_addr(port: u16) -> PeerSocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port)).into()
}

#[test]
fn connected_addr_labels_require_explicit_opt_in() {
    let peer = ConnectedAddr::new_outbound_direct(
        "192.0.2.1:8233".parse().expect("valid test peer address"),
    );

    assert_eq!(peer.addr_label(false), "v4redacted:8233");
    assert_eq!(peer.addr_label(true), "192.0.2.1:8233");
    assert_eq!(ConnectedAddr::Isolated.addr_label(true), "isolated");

    let debug = format!("{peer:?}");
    assert!(debug.contains("v4redacted:8233"));
    assert!(!debug.contains("192.0.2.1"));
}

fn test_config(p2p_stack: P2pStack) -> Config {
    Config::for_test(p2p_stack)
}

fn upgraded_outcome() -> ZakuraUpgradeOutcome {
    ZakuraUpgradeOutcome::Upgraded {
        peer_id: ZakuraPeerId::new(vec![7; 32]).expect("test peer id is within bounds"),
    }
}

async fn negotiate_test_pair(
    local_config: Config,
    remote_config: Config,
) -> (Arc<ConnectionInfo>, Arc<ConnectionInfo>) {
    let (local_stream, remote_stream) = duplex(16 * 1024);
    let mut local_conn = Framed::new(
        local_stream,
        Codec::builder().for_network(&local_config.network).finish(),
    );
    let mut remote_conn = Framed::new(
        remote_stream,
        Codec::builder()
            .for_network(&remote_config.network)
            .finish(),
    );

    let local_addr = ConnectedAddr::new_outbound_direct(peer_addr(18233));
    let remote_addr = ConnectedAddr::new_inbound_direct(peer_addr(28233));

    let local_nonces = Arc::new(futures::lock::Mutex::new(IndexSet::new()));
    let remote_nonces = Arc::new(futures::lock::Mutex::new(IndexSet::new()));

    let local_min_version = MinimumPeerVersion::new(NoChainTip, &local_config.network);
    let remote_min_version = MinimumPeerVersion::new(NoChainTip, &remote_config.network);

    let local_services = configured_advertised_services(&local_config, PeerServices::NODE_NETWORK);
    let remote_services =
        configured_advertised_services(&remote_config, PeerServices::NODE_NETWORK);
    let local_user_agent = configured_user_agent(&local_config, "/Zebra:local-test/".to_string());
    let remote_user_agent =
        configured_user_agent(&remote_config, "/Zebra:remote-test/".to_string());

    let local_task = tokio::spawn(async move {
        negotiate_version(
            &mut local_conn,
            &local_addr,
            local_config,
            local_nonces,
            local_user_agent,
            local_services,
            true,
            local_min_version,
            false,
        )
        .await
    });

    let remote_task = tokio::spawn(async move {
        negotiate_version(
            &mut remote_conn,
            &remote_addr,
            remote_config,
            remote_nonces,
            remote_user_agent,
            remote_services,
            true,
            remote_min_version,
            false,
        )
        .await
    });

    let local_info = local_task.await.unwrap().unwrap();
    let remote_info = remote_task.await.unwrap().unwrap();

    (local_info, remote_info)
}

fn test_handshake(
    config: Config,
    address_book_updater: tokio::sync::mpsc::Sender<MetaAddrChange>,
    calls: Arc<AtomicUsize>,
    outcome: ZakuraUpgradeOutcome,
) -> Handshake<MockService<Request, Response, PanicAssertion>> {
    let inbound_service: MockService<Request, Response, PanicAssertion> =
        MockService::build().for_unit_tests();

    Handshake::builder()
        .with_config(config)
        .with_inbound_service(inbound_service)
        .with_address_book_updater(address_book_updater)
        .with_advertised_services(PeerServices::NODE_NETWORK)
        .with_user_agent("/Zebra:handshake-test/".to_string())
        .with_zakura_handshake_connector(crate::zakura::ZakuraHandshakeConnector::for_test(
            calls, outcome,
        ))
        .want_transactions(true)
        .finish()
        .unwrap()
}

fn test_handshake_without_zakura_connector(
    config: Config,
    address_book_updater: tokio::sync::mpsc::Sender<MetaAddrChange>,
) -> Handshake<MockService<Request, Response, PanicAssertion>> {
    let inbound_service: MockService<Request, Response, PanicAssertion> =
        MockService::build().for_unit_tests();

    Handshake::builder()
        .with_config(config)
        .with_inbound_service(inbound_service)
        .with_address_book_updater(address_book_updater)
        .with_advertised_services(PeerServices::NODE_NETWORK)
        .with_user_agent("/Zebra:handshake-test/".to_string())
        .want_transactions(true)
        .finish()
        .unwrap()
}

fn test_handshake_with_connector(
    config: Config,
    address_book_updater: tokio::sync::mpsc::Sender<MetaAddrChange>,
    connector: crate::zakura::ZakuraHandshakeConnector,
) -> Handshake<MockService<Request, Response, PanicAssertion>> {
    let inbound_service: MockService<Request, Response, PanicAssertion> =
        MockService::build().for_unit_tests();

    Handshake::builder()
        .with_config(config)
        .with_inbound_service(inbound_service)
        .with_address_book_updater(address_book_updater)
        .with_advertised_services(PeerServices::NODE_NETWORK)
        .with_user_agent("/Zebra:handshake-test/".to_string())
        .with_zakura_handshake_connector(connector)
        .want_transactions(true)
        .finish()
        .unwrap()
}

/// A no-op service used to start a real Zakura
/// endpoint in tests without wiring an application service.
#[derive(Debug)]
struct DropSink;

impl ZakuraService for DropSink {
    fn name(&self) -> &'static str {
        "drop"
    }

    fn streams(&self) -> &[Stream] {
        &[]
    }

    fn add_peer(&self, _peer: ZakuraServicePeer) {}

    fn remove_peer(&self, _peer: &ZakuraPeerId, _conn_id: crate::zakura::ZakuraConnId) {}
}

/// Starts a real Zakura endpoint over loopback QUIC for an upgrade test.
///
/// Each endpoint gets an explicit test-only iroh identity so parallel tests do
/// not share the same persisted `~/.zakura` key or write to the user's real
/// identity file.
async fn start_test_zakura_endpoint() -> crate::zakura::ZakuraEndpoint {
    let key_index = TEST_ZAKURA_SECRET_KEY_COUNTER.fetch_add(1, Ordering::Relaxed) % 255 + 1;
    let key_byte = u8::try_from(key_index).expect("key index is in 1..=255 due to modulo");
    let secret = format!("{key_byte:02x}").repeat(32);
    let config: Config = toml::from_str(&format!(
        "p2p_stack = 'dual'\nzakura_node_secret_key = '{secret}'"
    ))
    .expect("test Zakura config with explicit identity key must parse");

    crate::zakura::spawn_zakura_endpoint(&config, |_supervisor, _trace| {
        Arc::new(DropSink) as Arc<dyn ZakuraService>
    })
    .await
    .expect("Zakura endpoint starts")
    .expect("v2_p2p is enabled in the test config")
}

/// Two mutually P2P-v2-capable nodes with live Zakura endpoints should exchange
/// the legacy upgrade prelude over the TCP stream, drop the legacy connection,
/// and establish a real Zakura QUIC connection that registers on both ends.
#[tokio::test]
async fn mutual_p2p_v2_legacy_upgrade_forms_zakura_connection() {
    let _init_guard = zakura_test::init();

    let local_endpoint = start_test_zakura_endpoint().await;
    let remote_endpoint = start_test_zakura_endpoint().await;

    let (local_stream, remote_stream) = duplex(16 * 1024);
    let (address_book_tx, _address_book_rx) = tokio::sync::mpsc::channel(8);

    let mut local_counter = ActiveConnectionCounter::new_counter();
    let mut remote_counter = ActiveConnectionCounter::new_counter();

    let local_handshake = test_handshake_with_connector(
        test_config(P2pStack::Dual),
        address_book_tx.clone(),
        local_endpoint.connector(),
    );
    let remote_handshake = test_handshake_with_connector(
        test_config(P2pStack::Dual),
        address_book_tx,
        remote_endpoint.connector(),
    );

    let local_task = tokio::spawn(local_handshake.oneshot(HandshakeRequest {
        data_stream: local_stream,
        connected_addr: ConnectedAddr::new_outbound_direct(peer_addr(18233)),
        connection_tracker: local_counter.track_connection(),
    }));
    let remote_task = tokio::spawn(remote_handshake.oneshot(HandshakeRequest {
        data_stream: remote_stream,
        connected_addr: ConnectedAddr::new_inbound_direct(peer_addr(28233)),
        connection_tracker: remote_counter.track_connection(),
    }));

    let local_error = local_task.await.unwrap().unwrap_err();
    let remote_error = remote_task.await.unwrap().unwrap_err();

    // A completed upgrade drops the legacy connection on both sides.
    assert!(local_error
        .downcast_ref::<HandshakeError>()
        .is_some_and(|error| matches!(error, HandshakeError::ZakuraUpgradeSelected)));
    assert!(remote_error
        .downcast_ref::<HandshakeError>()
        .is_some_and(|error| matches!(error, HandshakeError::ZakuraUpgradeSelected)));

    // The initiator dialed the responder's advertised Zakura address over QUIC;
    // both supervisors should register the other peer once it completes.
    let local_supervisor = local_endpoint.supervisor();
    let remote_supervisor = remote_endpoint.supervisor();
    let registered = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if !local_supervisor.registered_ids().await.is_empty()
                && !remote_supervisor.registered_ids().await.is_empty()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        registered.is_ok(),
        "the legacy upgrade should establish a Zakura connection registered on both endpoints",
    );

    local_endpoint.shutdown().await;
    remote_endpoint.shutdown().await;
}

/// An inbound legacy peer that sends a valid upgrade `Init`, receives our
/// `Accept`, and then never completes the native QUIC dial must not make the
/// responder report `Upgraded` and drop the working legacy TCP connection.
///
/// Regression test for `claude-legacy-upgrade-premature-upgraded-no-native`:
/// the responder previously returned `Upgraded` immediately after sending
/// `Accept`, so the outer handshake dropped legacy TCP with no registered
/// Zakura replacement. The responder must instead wait for the inbound native
/// registration (mirroring the initiator's hand-off wait) and otherwise fall
/// back to a neutral `Rejected`, keeping the legacy connection.
#[tokio::test]
async fn responder_upgrade_keeps_legacy_when_native_dial_never_registers() {
    let _init_guard = zakura_test::init();

    // A real responder endpoint with a live Zakura supervisor.
    let responder_endpoint = start_test_zakura_endpoint().await;
    let connector = responder_endpoint.connector();

    let network = test_config(P2pStack::Dual).network;
    let config = ZakuraHandshakeConfig::for_network(&network);

    // The legacy `version` nonces the responder observed for this peer.
    let nonces = ZakuraLegacyNonces {
        local_zebra_nonce: Nonce(0x1111_1111_1111_1111),
        remote_zebra_nonce: Nonce(0x2222_2222_2222_2222),
    };

    // The responder advertises its own live Zakura hints in the `Accept`.
    let (local_node_id, local_direct_addresses) = connector
        .local_iroh_hints()
        .await
        .expect("a live Zakura endpoint exposes local upgrade hints");

    let (responder_stream, peer_stream) = duplex(16 * 1024);
    let mut responder_conn = Framed::new(
        responder_stream,
        Codec::builder().for_network(&network).finish(),
    );
    let mut peer_conn = Framed::new(peer_stream, Codec::builder().for_network(&network).finish());

    // A valid `Init` that passes the responder's static, nonce, and protocol
    // validation, claiming a real 32-byte iroh identity the attacker never
    // brings online over native QUIC.
    let init = P2pV2UpgradeInit {
        magic: PRELUDE_MAGIC,
        prelude_version: config.prelude_version,
        zakura_protocol_min: config.zakura_protocol_min,
        zakura_protocol_max: config.zakura_protocol_max,
        network_id: config.network_id,
        chain_id: config.chain_id,
        capabilities: config.supported_capabilities,
        // The peer's nonce labels are the mirror image of the responder's.
        local_zebra_nonce: nonces.remote_zebra_nonce,
        remote_zebra_nonce: nonces.local_zebra_nonce,
        upgrade_nonce: [9u8; 32],
        iroh_node_id: vec![7u8; 32],
        iroh_direct_addresses: vec![b"192.0.2.1:1".to_vec()],
        iroh_relay_hint: None,
        max_control_frame_bytes: config.max_control_frame_bytes,
        max_open_streams: config.max_open_streams,
    };

    // The malicious initiator: send the `Init`, read the `Accept`, then go
    // silent (never dial the responder's native Zakura endpoint), holding the
    // legacy stream open for the rest of the test.
    let attacker = async move {
        let init_bytes = P2pV2Upgrade::Init(init)
            .encode()
            .expect("a valid init encodes");
        peer_conn
            .send(Message::P2pV2Upgrade(init_bytes))
            .await
            .expect("the initiator sends its upgrade init");

        let reply = peer_conn
            .next()
            .await
            .expect("the responder replies before closing the legacy stream")
            .expect("the reply frame decodes");
        let Message::P2pV2Upgrade(payload) = reply else {
            panic!("the responder must reply with a p2pv2 upgrade message");
        };
        assert!(
            matches!(P2pV2Upgrade::decode(&payload), Ok(P2pV2Upgrade::Accept(_))),
            "the responder must accept a valid init before waiting for native registration",
        );

        peer_conn
    };

    // The responder must not finalize the upgrade until a native registration
    // appears; with no dial it falls back after the appear timeout. The bound
    // makes a regression (immediate `Upgraded`) or a hang fail loudly.
    let responder = run_responder_upgrade(
        &mut responder_conn,
        &connector,
        &config,
        nonces,
        local_node_id,
        local_direct_addresses,
    );

    let (outcome, _held_peer_conn) = tokio::join!(
        tokio::time::timeout(std::time::Duration::from_secs(45), responder),
        attacker,
    );

    let outcome = outcome
        .expect("responder upgrade resolves within the time bound")
        .expect("responder upgrade returns an outcome, not a handshake error");

    assert!(
        matches!(outcome, ZakuraUpgradeOutcome::Rejected { .. }),
        "the responder reported {outcome:?}; it must keep the legacy connection \
         (a neutral Rejected fallback) when the inbound peer sends a valid Init but \
         never registers a native Zakura connection",
    );

    responder_endpoint.shutdown().await;
}

/// An inbound legacy peer that advertised `NODE_P2P_V2` and frames a `p2pv2up`
/// message whose payload fails to decode must be disconnected on the first
/// malformed upgrade message, not silently kept on the legacy connection.
///
/// Regression test for `claude-legacy-upgrade-malformed-fallback-fail-open`
/// (responder facet): `read_upgrade_prelude` previously erased
/// `P2pV2Upgrade::decode` errors to `None` via `.ok()`, so the responder mapped
/// malformed bytes to a neutral reject plus legacy fallback. That let a peer
/// force a downgrade to legacy by sending garbage upgrade bytes (SR-7
/// fail-open). The malformed prelude must instead surface as a non-neutral
/// `ZakuraUpgradePreludeMalformed` disconnect.
#[tokio::test]
async fn responder_upgrade_disconnects_on_malformed_prelude() {
    let _init_guard = zakura_test::init();

    let network = test_config(P2pStack::Dual).network;
    let config = ZakuraHandshakeConfig::for_network(&network);
    let nonces = ZakuraLegacyNonces {
        local_zebra_nonce: Nonce(0x1111_1111_1111_1111),
        remote_zebra_nonce: Nonce(0x2222_2222_2222_2222),
    };
    // The malformed-prelude branch returns before any endpoint use, so a
    // connector without a live endpoint is enough to exercise the responder
    // path.
    let connector = crate::zakura::ZakuraHandshakeConnector::unavailable();

    let (responder_stream, peer_stream) = duplex(16 * 1024);
    let mut responder_conn = Framed::new(
        responder_stream,
        Codec::builder().for_network(&network).finish(),
    );
    let mut peer_conn = Framed::new(peer_stream, Codec::builder().for_network(&network).finish());

    // A framed `p2pv2up` message whose payload has an unknown discriminator, so
    // `P2pV2Upgrade::decode` fails. Oversized/trailing/truncated payloads share
    // this same decode-error path.
    let attacker = async move {
        peer_conn
            .send(Message::P2pV2Upgrade(vec![0xFF; 16]))
            .await
            .expect("the malformed initiator frames its bogus upgrade prelude");
        peer_conn
    };

    let responder = run_responder_upgrade(
        &mut responder_conn,
        &connector,
        &config,
        nonces,
        vec![1u8; 32],
        vec![b"127.0.0.1:1".to_vec()],
    );

    let (outcome, _held_peer_conn) = tokio::join!(
        tokio::time::timeout(std::time::Duration::from_secs(10), responder),
        attacker,
    );

    let error = outcome
        .expect("the responder upgrade resolves within the time bound")
        .expect_err(
            "a malformed upgrade prelude must disconnect the peer, not fall back to legacy",
        );
    assert!(
        matches!(error, HandshakeError::ZakuraUpgradePreludeMalformed(_)),
        "the responder returned {error:?}; a malformed p2pv2up prelude must be a \
         first-offense disconnect",
    );
    assert!(
        !error.is_neutral_disconnect(),
        "a malformed upgrade prelude must be a penalized peer failure, not a neutral \
         legacy fallback",
    );
}

/// A panic in structured prelude decoding is a terminal serialization error
/// for that legacy peer, not a handshake-task panic.
#[test]
fn upgrade_prelude_parser_panic_becomes_serialization_error() {
    let _init_guard = zakura_test::init();

    let error = decode_upgrade_prelude_with(|| panic!("test prelude parser panic"))
        .expect_err("the parser panic must become a handshake error");

    assert!(matches!(
        error,
        HandshakeError::Serialization(SerializationError::Parse(
            "Zakura P2P v2 upgrade prelude parser panicked"
        ))
    ));
}

/// The TCP initiator side of the same regression: a peer that advertised
/// `NODE_P2P_V2`, receives our `Init`, and replies with a `p2pv2up` message
/// whose payload fails to decode must be disconnected, not kept on legacy.
///
/// Regression test for `claude-legacy-upgrade-malformed-fallback-fail-open`
/// (initiator facet): the initiator previously mapped a malformed `Accept` to a
/// neutral legacy fallback. It must instead surface a non-neutral
/// `ZakuraUpgradePreludeMalformed` disconnect.
#[tokio::test]
async fn initiator_upgrade_disconnects_on_malformed_prelude() {
    let _init_guard = zakura_test::init();

    let network = test_config(P2pStack::Dual).network;
    let config = ZakuraHandshakeConfig::for_network(&network);
    let nonces = ZakuraLegacyNonces {
        local_zebra_nonce: Nonce(0x3333_3333_3333_3333),
        remote_zebra_nonce: Nonce(0x4444_4444_4444_4444),
    };
    let connector = crate::zakura::ZakuraHandshakeConnector::unavailable();

    let (initiator_stream, peer_stream) = duplex(16 * 1024);
    let mut initiator_conn = Framed::new(
        initiator_stream,
        Codec::builder().for_network(&network).finish(),
    );
    let mut peer_conn = Framed::new(peer_stream, Codec::builder().for_network(&network).finish());

    // The malicious responder reads our `Init`, then replies with a framed
    // `p2pv2up` message whose payload fails to decode (an unknown discriminator)
    // instead of a well-formed `Accept`.
    let attacker = async move {
        let init = peer_conn
            .next()
            .await
            .expect("the initiator frames its upgrade init")
            .expect("the init frame decodes at the codec layer");
        assert!(
            matches!(init, Message::P2pV2Upgrade(_)),
            "the initiator must send an upgrade init first",
        );
        peer_conn
            .send(Message::P2pV2Upgrade(vec![0xFF; 16]))
            .await
            .expect("the malformed responder frames its bogus accept");
        peer_conn
    };

    let initiator = run_initiator_upgrade(
        &mut initiator_conn,
        &connector,
        &config,
        nonces,
        vec![1u8; 32],
        vec![b"127.0.0.1:1".to_vec()],
    );

    let (outcome, _held_peer_conn) = tokio::join!(
        tokio::time::timeout(std::time::Duration::from_secs(10), initiator),
        attacker,
    );

    let error = outcome
        .expect("the initiator upgrade resolves within the time bound")
        .expect_err("a malformed upgrade accept must disconnect the peer, not fall back to legacy");
    assert!(
        matches!(error, HandshakeError::ZakuraUpgradePreludeMalformed(_)),
        "the initiator returned {error:?}; a malformed p2pv2up accept must be a \
         first-offense disconnect",
    );
    assert!(!error.is_neutral_disconnect());
}

#[tokio::test]
async fn p2p_v2_service_bit_advertisement_follows_config() {
    let _init_guard = zakura_test::init();

    assert!(!configured_advertised_services(
        &test_config(P2pStack::Legacy),
        PeerServices::NODE_NETWORK | PeerServices::NODE_P2P_V2,
    )
    .contains(PeerServices::NODE_P2P_V2));

    let (disabled_peer_seen_by_enabled, enabled_peer_seen_by_disabled) =
        negotiate_test_pair(test_config(P2pStack::Dual), test_config(P2pStack::Legacy)).await;

    assert!(!disabled_peer_seen_by_enabled
        .remote
        .services
        .contains(PeerServices::NODE_P2P_V2));
    assert!(!disabled_peer_seen_by_enabled
        .remote
        .address_from
        .untrusted_services()
        .contains(PeerServices::NODE_P2P_V2));

    assert!(enabled_peer_seen_by_disabled
        .remote
        .services
        .contains(PeerServices::NODE_P2P_V2));
    assert!(enabled_peer_seen_by_disabled
        .remote
        .address_from
        .untrusted_services()
        .contains(PeerServices::NODE_P2P_V2));
    assert!(enabled_peer_seen_by_disabled
        .remote
        .user_agent
        .starts_with("/Zakura:"));
    assert!(enabled_peer_seen_by_disabled
        .remote
        .user_agent
        .contains("/Zebra:local-test/"));
}

/// The Zakura token is prepended for the v2 stack, but never doubled when the
/// user agent (like zakurad's default) already carries it.
#[test]
fn configured_user_agent_deduplicates_zakura_token() {
    let _init_guard = zakura_test::init();

    let zakura_token = format!("Zakura:{}", env!("CARGO_PKG_VERSION"));
    let default_user_agent = format!("/{zakura_token}/");

    // The legacy stack passes the configured user agent through unchanged.
    assert_eq!(
        configured_user_agent(&test_config(P2pStack::Legacy), default_user_agent.clone()),
        default_user_agent,
    );

    // The v2 stack advertises the token on its own or before a foreign agent.
    assert_eq!(
        configured_user_agent(&test_config(P2pStack::Dual), String::new()),
        default_user_agent,
    );
    assert_eq!(
        configured_user_agent(
            &test_config(P2pStack::Dual),
            "/MagicBean:6.3.0/".to_string()
        ),
        format!("/{zakura_token}/MagicBean:6.3.0/"),
    );

    // A user agent that already carries the token is not doubled.
    assert_eq!(
        configured_user_agent(&test_config(P2pStack::Dual), default_user_agent.clone()),
        default_user_agent,
    );
    assert_eq!(
        configured_user_agent(
            &test_config(P2pStack::Dual),
            format!("/{zakura_token}/MagicBean:6.3.0/"),
        ),
        format!("/{zakura_token}/MagicBean:6.3.0/"),
    );
}

#[test]
fn zakura_upgrade_errors_are_neutral_disconnects() {
    let _init_guard = zakura_test::init();

    assert!(HandshakeError::ZakuraUpgradeSelected.is_neutral_disconnect());
    assert!(
        HandshakeError::ZakuraUpgrade(crate::zakura::ZakuraUpgradeError::Unavailable)
            .is_neutral_disconnect()
    );
    assert!(!HandshakeError::Timeout.is_neutral_disconnect());
    // A malformed upgrade prelude is a real peer failure: it must be demoted
    // (reported failed), not treated as a neutral legacy fallback.
    assert!(!HandshakeError::ZakuraUpgradePreludeMalformed(
        crate::zakura::ZakuraProtocolError::InvalidDiscriminator(0xFF)
    )
    .is_neutral_disconnect());
}

#[test]
fn p2p_v2_upgrade_uses_main_version_services_only() {
    let _init_guard = zakura_test::init();

    let config = test_config(P2pStack::Dual);

    let addr = peer_addr(18233);
    let inconsistent_remote = VersionMessage {
        version: constants::CURRENT_NETWORK_PROTOCOL_VERSION,
        services: PeerServices::NODE_NETWORK,
        timestamp: Utc::now(),
        address_recv: AddrInVersion::new(addr, PeerServices::NODE_NETWORK),
        address_from: AddrInVersion::new(
            addr,
            PeerServices::NODE_NETWORK | PeerServices::NODE_P2P_V2,
        ),
        nonce: Nonce(1),
        user_agent: "/Zebra:test/".to_string(),
        start_height: block::Height(0),
        relay: true,
    };

    let connection_info = ConnectionInfo {
        connected_addr: ConnectedAddr::new_outbound_direct(addr),
        local: inconsistent_remote.clone(),
        remote: inconsistent_remote,
        negotiated_version: constants::CURRENT_NETWORK_PROTOCOL_VERSION,
        is_protected_peer: false,
    };

    assert!(!should_attempt_zakura_upgrade(&config, &connection_info));
}

#[test]
fn p2p_v2_upgrade_requires_local_enable_and_remote_service_bit() {
    let _init_guard = zakura_test::init();

    let addr = peer_addr(18233);
    let version = |services| VersionMessage {
        version: constants::CURRENT_NETWORK_PROTOCOL_VERSION,
        services,
        timestamp: Utc::now(),
        address_recv: AddrInVersion::new(addr, PeerServices::NODE_NETWORK),
        address_from: AddrInVersion::new(addr, services),
        nonce: Nonce(1),
        user_agent: "/Zebra:test/".to_string(),
        start_height: block::Height(0),
        relay: true,
    };
    let connection_info = |remote_services| ConnectionInfo {
        connected_addr: ConnectedAddr::new_outbound_direct(addr),
        local: version(PeerServices::NODE_NETWORK | PeerServices::NODE_P2P_V2),
        remote: version(remote_services),
        negotiated_version: constants::CURRENT_NETWORK_PROTOCOL_VERSION,
        is_protected_peer: false,
    };

    assert!(!should_attempt_zakura_upgrade(
        &test_config(P2pStack::Legacy),
        &connection_info(PeerServices::NODE_NETWORK | PeerServices::NODE_P2P_V2),
    ));
    assert!(!should_attempt_zakura_upgrade(
        &test_config(P2pStack::Dual),
        &connection_info(PeerServices::NODE_NETWORK),
    ));
    assert!(should_attempt_zakura_upgrade(
        &test_config(P2pStack::Dual),
        &connection_info(PeerServices::NODE_NETWORK | PeerServices::NODE_P2P_V2),
    ));
}

/// A configured inbound peer is recognized as protected, including when it
/// arrives as an IPv4-mapped IPv6 address; outbound, unconfigured, empty-set,
/// and isolated cases are not protected.
#[test]
fn connected_addr_is_protected_peer_matches_configured_inbound_ips() {
    let _init_guard = zakura_test::init();

    let configured = Ipv4Addr::new(203, 0, 113, 7);
    let protected: HashSet<IpAddr> = [IpAddr::V4(configured)].into_iter().collect();

    let inbound = |ip: IpAddr| ConnectedAddr::new_inbound_direct(SocketAddr::new(ip, 8233).into());

    // A configured inbound peer (plain IPv4) is protected.
    assert!(inbound(IpAddr::V4(configured)).is_protected_peer(&protected));

    // The same peer arriving as an IPv4-mapped IPv6 address is still protected:
    // its IP is canonicalized before the lookup.
    let mapped = IpAddr::V6(configured.to_ipv6_mapped());
    assert!(inbound(mapped).is_protected_peer(&protected));

    // An inbound peer that is not in the configured set is not protected.
    assert!(!inbound(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 9))).is_protected_peer(&protected));

    // Outbound connections are never protected, even to a configured IP.
    assert!(!ConnectedAddr::new_outbound_direct(
        SocketAddr::new(IpAddr::V4(configured), 8233).into()
    )
    .is_protected_peer(&protected));

    // An empty configured set protects no one.
    assert!(!inbound(IpAddr::V4(configured)).is_protected_peer(&HashSet::new()));

    // Isolated connections have no peer address and are never protected.
    assert!(!ConnectedAddr::Isolated.is_protected_peer(&protected));
}

#[tokio::test]
async fn remote_p2p_v2_with_local_disabled_continues_legacy() {
    let _init_guard = zakura_test::init();

    let (local_stream, remote_stream) = duplex(16 * 1024);
    let (address_book_tx, _address_book_rx) = tokio::sync::mpsc::channel(8);
    let local_calls = Arc::new(AtomicUsize::new(0));
    let remote_calls = Arc::new(AtomicUsize::new(0));

    let mut local_counter = ActiveConnectionCounter::new_counter();
    let mut remote_counter = ActiveConnectionCounter::new_counter();

    let local_handshake = test_handshake(
        test_config(P2pStack::Legacy),
        address_book_tx.clone(),
        local_calls.clone(),
        upgraded_outcome(),
    );
    let remote_handshake = test_handshake(
        test_config(P2pStack::Dual),
        address_book_tx,
        remote_calls.clone(),
        upgraded_outcome(),
    );

    let local_task = tokio::spawn(local_handshake.oneshot(HandshakeRequest {
        data_stream: local_stream,
        connected_addr: ConnectedAddr::new_outbound_direct(peer_addr(18233)),
        connection_tracker: local_counter.track_connection(),
    }));
    let remote_task = tokio::spawn(remote_handshake.oneshot(HandshakeRequest {
        data_stream: remote_stream,
        connected_addr: ConnectedAddr::new_inbound_direct(peer_addr(28233)),
        connection_tracker: remote_counter.track_connection(),
    }));

    let local_client = local_task.await.unwrap().unwrap();
    let remote_client = remote_task.await.unwrap().unwrap();

    assert_eq!(local_calls.load(Ordering::SeqCst), 0);
    assert_eq!(remote_calls.load(Ordering::SeqCst), 0);

    drop(local_client);
    drop(remote_client);
}

#[tokio::test]
async fn mutual_p2p_v2_without_connector_fails_closed() {
    let _init_guard = zakura_test::init();

    let (local_stream, remote_stream) = duplex(16 * 1024);
    let (address_book_tx, mut address_book_rx) = tokio::sync::mpsc::channel(8);

    let mut local_counter = ActiveConnectionCounter::new_counter();
    let mut remote_counter = ActiveConnectionCounter::new_counter();

    let local_handshake = test_handshake_without_zakura_connector(
        test_config(P2pStack::Dual),
        address_book_tx.clone(),
    );
    let remote_handshake =
        test_handshake_without_zakura_connector(test_config(P2pStack::Dual), address_book_tx);

    let local_task = tokio::spawn(local_handshake.oneshot(HandshakeRequest {
        data_stream: local_stream,
        connected_addr: ConnectedAddr::new_outbound_direct(peer_addr(18233)),
        connection_tracker: local_counter.track_connection(),
    }));
    let remote_task = tokio::spawn(remote_handshake.oneshot(HandshakeRequest {
        data_stream: remote_stream,
        connected_addr: ConnectedAddr::new_inbound_direct(peer_addr(28233)),
        connection_tracker: remote_counter.track_connection(),
    }));

    let local_error = local_task.await.unwrap().unwrap_err();
    let remote_error = remote_task.await.unwrap().unwrap_err();

    let mut saw_unavailable = false;
    for error in [&local_error, &remote_error] {
        let error = error
            .downcast_ref::<HandshakeError>()
            .expect("handshake task errors should be HandshakeError");
        saw_unavailable |= matches!(
            error,
            HandshakeError::ZakuraUpgrade(crate::zakura::ZakuraUpgradeError::Unavailable)
        );
        assert!(matches!(
            error,
            HandshakeError::ZakuraUpgrade(crate::zakura::ZakuraUpgradeError::Unavailable)
                | HandshakeError::ConnectionClosed
        ));
    }
    assert!(saw_unavailable);

    assert!(address_book_rx.try_recv().is_err());
    assert_eq!(local_counter.update_count(), 0);
    assert_eq!(remote_counter.update_count(), 0);
}

#[tokio::test]
async fn mutual_p2p_v2_selected_upgrade_skips_legacy_connection() {
    let _init_guard = zakura_test::init();

    let (local_stream, remote_stream) = duplex(16 * 1024);
    let (address_book_tx, mut address_book_rx) = tokio::sync::mpsc::channel(8);
    let local_calls = Arc::new(AtomicUsize::new(0));
    let remote_calls = Arc::new(AtomicUsize::new(0));

    let mut local_counter = ActiveConnectionCounter::new_counter();
    let mut remote_counter = ActiveConnectionCounter::new_counter();

    let local_handshake = test_handshake(
        test_config(P2pStack::Dual),
        address_book_tx.clone(),
        local_calls.clone(),
        upgraded_outcome(),
    );
    let remote_handshake = test_handshake(
        test_config(P2pStack::Dual),
        address_book_tx,
        remote_calls.clone(),
        upgraded_outcome(),
    );

    let local_task = tokio::spawn(local_handshake.oneshot(HandshakeRequest {
        data_stream: local_stream,
        connected_addr: ConnectedAddr::new_outbound_direct(peer_addr(18233)),
        connection_tracker: local_counter.track_connection(),
    }));
    let remote_task = tokio::spawn(remote_handshake.oneshot(HandshakeRequest {
        data_stream: remote_stream,
        connected_addr: ConnectedAddr::new_inbound_direct(peer_addr(28233)),
        connection_tracker: remote_counter.track_connection(),
    }));

    let local_error = local_task.await.unwrap().unwrap_err();
    let remote_error = remote_task.await.unwrap().unwrap_err();

    assert!(local_error
        .downcast_ref::<HandshakeError>()
        .is_some_and(|error| matches!(error, HandshakeError::ZakuraUpgradeSelected)));
    assert!(remote_error
        .downcast_ref::<HandshakeError>()
        .is_some_and(|error| matches!(error, HandshakeError::ZakuraUpgradeSelected)));

    assert_eq!(local_calls.load(Ordering::SeqCst), 1);
    assert_eq!(remote_calls.load(Ordering::SeqCst), 1);
    assert!(address_book_rx.try_recv().is_err());
    assert_eq!(local_counter.update_count(), 0);
    assert_eq!(remote_counter.update_count(), 0);
}
