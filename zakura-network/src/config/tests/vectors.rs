//! Fixed test vectors for zakura-network configuration.

use std::{net::SocketAddr, time::Duration};

use static_assertions::const_assert;
use zakura_chain::{
    block::Height,
    parameters::{
        testnet::{self, ConfiguredFundingStreams},
        Network,
    },
};

use crate::{
    config::zakura_listens_on_loopback_with_non_loopback_bootstrap_peers,
    config::zakura_secret_key_file_path,
    constants::{INBOUND_PEER_LIMIT_MULTIPLIER, OUTBOUND_PEER_LIMIT_MULTIPLIER},
    zakura::{
        DEFAULT_HS_MAX_INFLIGHT, DEFAULT_HS_RANGE, DEFAULT_TESTNET_ZAKURA_BOOTSTRAP_PEERS,
        DEFAULT_ZAKURA_BOOTSTRAP_PEERS, DEFAULT_ZAKURA_LISTEN_ADDR,
        DEFAULT_ZAKURA_MAX_CONNS_PER_IP,
    },
    CacheDir, Config, P2pStack,
};

use super::super::load_or_generate_zakura_secret_key;

fn default_zakura_bootstrap_peers() -> Vec<String> {
    DEFAULT_ZAKURA_BOOTSTRAP_PEERS
        .iter()
        .map(ToString::to_string)
        .collect()
}

fn default_testnet_zakura_bootstrap_peers() -> Vec<String> {
    DEFAULT_TESTNET_ZAKURA_BOOTSTRAP_PEERS
        .iter()
        .map(ToString::to_string)
        .collect()
}

#[test]
fn parse_config_listen_addr() {
    let _init_guard = zakura_test::init();

    let fixtures = vec![
        ("listen_addr = '0.0.0.0'", "0.0.0.0:8233"),
        ("listen_addr = '0.0.0.0:9999'", "0.0.0.0:9999"),
        (
            "listen_addr = '0.0.0.0'\nnetwork = 'Testnet'",
            "0.0.0.0:18233",
        ),
        (
            "listen_addr = '0.0.0.0:8233'\nnetwork = 'Testnet'",
            "0.0.0.0:8233",
        ),
        ("listen_addr = '[::]'", "[::]:8233"),
        ("listen_addr = '[::]:9999'", "[::]:9999"),
        ("listen_addr = '[::]'\nnetwork = 'Testnet'", "[::]:18233"),
        (
            "listen_addr = '[::]:8233'\nnetwork = 'Testnet'",
            "[::]:8233",
        ),
        ("listen_addr = '[::1]:8233'", "[::1]:8233"),
        ("listen_addr = '[2001:db8::1]:8233'", "[2001:db8::1]:8233"),
    ];

    for (config, value) in fixtures {
        let config: Config = toml::from_str(config).unwrap();
        assert_eq!(config.listen_addr.to_string(), value);
    }
}

/// Make sure the peer connection limits are consistent with each other.
#[test]
fn ensure_peer_connection_limits_consistent() {
    let _init_guard = zakura_test::init();

    // This fork prioritizes fast outbound sync over inbound-serving capacity.
    const_assert!(INBOUND_PEER_LIMIT_MULTIPLIER <= OUTBOUND_PEER_LIMIT_MULTIPLIER);

    let config = Config::default();

    assert!(
        config.peerset_inbound_connection_limit() <= config.peerset_outbound_connection_limit(),
        "this fork caps inbound connections at or below the outbound limit, to prioritize sync",
    );
}

#[test]
fn testnet_params_serialization_roundtrip() {
    let _init_guard = zakura_test::init();

    let mut config = Config {
        network: testnet::Parameters::build()
            .with_disable_pow(true)
            .to_network()
            .expect("failed to build configured network"),
        initial_testnet_peers: [].into(),
        ..Config::for_test(P2pStack::Dual)
    };
    config.zakura.apply_network_defaults(&config.network);

    let serialized = toml::to_string(&config).unwrap();
    let deserialized: Config = toml::from_str(&serialized).unwrap();

    assert_eq!(config, deserialized);
}

#[test]
fn zakura_node_secret_key_is_redacted_from_debug_and_serialization() {
    let _init_guard = zakura_test::init();

    let secret = "not-a-real-iroh-secret-but-sensitive";
    let config: Config = toml::from_str(&format!("zakura_node_secret_key = '{secret}'")).unwrap();

    assert_eq!(
        config
            .zakura_node_secret_key
            .as_ref()
            .expect("test config should parse the Zakura secret key")
            .expose_secret(),
        secret
    );

    let debug = format!("{config:?}");
    assert!(debug.contains("zakura_node_secret_key"));
    assert!(debug.contains("[redacted]"));
    assert!(!debug.contains(secret));

    let serialized = toml::to_string(&config).unwrap();
    assert!(!serialized.contains("zakura_node_secret_key"));
    assert!(!serialized.contains(secret));
}

#[test]
fn identity_dir_defaults_and_roundtrips() {
    let _init_guard = zakura_test::init();

    let default_config = Config::default();
    assert_eq!(
        default_config
            .identity_dir
            .file_name()
            .and_then(|name| name.to_str()),
        Some(".zakura"),
    );

    let config: Config = toml::from_str("identity_dir = '/tmp/zakura-identities'")
        .expect("identity_dir should parse as a path");
    assert_eq!(
        config.identity_dir,
        std::path::PathBuf::from("/tmp/zakura-identities"),
    );

    let serialized = toml::to_string(&config).expect("config should serialize");
    assert!(
        serialized.contains("identity_dir = \"/tmp/zakura-identities\""),
        "identity_dir should be included in generated network config",
    );
    let deserialized: Config = toml::from_str(&serialized).expect("serialized config should parse");
    assert_eq!(config, deserialized);
}

#[test]
fn p2p_stack_defaults_by_network_and_roundtrips() {
    let _init_guard = zakura_test::init();

    // An unset `p2p_stack` follows the network's binary default: Mainnet is legacy-only,
    // every other network runs both stacks.
    assert_eq!(Config::default().p2p_stack, P2pStack::Default);
    assert!(Config::default().legacy_p2p());
    assert!(!Config::default().v2_p2p());
    assert_eq!(
        Config::default().zakura.bootstrap_peers,
        default_zakura_bootstrap_peers()
    );

    for (network, stack) in [
        ("Mainnet", P2pStack::Legacy),
        ("Testnet", P2pStack::Dual),
        ("Regtest", P2pStack::Dual),
    ] {
        let config: Config = toml::from_str(&format!("network = '{network}'")).unwrap();

        assert_eq!(config.p2p_stack, P2pStack::Default);
        assert_eq!(config.p2p_stack.resolve(&config.network), stack);
        assert_eq!(config.legacy_p2p(), stack != P2pStack::Zakura);
        assert_eq!(config.v2_p2p(), stack != P2pStack::Legacy);
    }

    // Overriding `network` with struct-update syntax resolves against the new network, so a
    // struct-built config and the equivalent `zakurad.toml` always agree.
    let testnet = Config {
        network: Network::new_default_testnet(),
        ..Config::default()
    };
    assert!(testnet.legacy_p2p());
    assert!(testnet.v2_p2p());

    // An explicit stack overrides the network default in both directions.
    let mainnet_dual: Config = toml::from_str("network = 'Mainnet'\np2p_stack = 'dual'").unwrap();
    assert!(mainnet_dual.legacy_p2p());
    assert!(mainnet_dual.v2_p2p());

    let testnet_legacy: Config =
        toml::from_str("network = 'Testnet'\np2p_stack = 'legacy'").unwrap();
    assert!(testnet_legacy.legacy_p2p());
    assert!(!testnet_legacy.v2_p2p());

    // Every stack round-trips through its canonical name, and the deprecated flags are
    // never written back out.
    for stack in [
        P2pStack::Default,
        P2pStack::Legacy,
        P2pStack::Zakura,
        P2pStack::Dual,
    ] {
        let config = Config {
            p2p_stack: stack,
            ..Config::default()
        };

        let serialized = toml::to_string(&config).unwrap();
        assert!(!serialized.contains("legacy_p2p"));
        assert!(!serialized.contains("v2_p2p"));

        let deserialized: Config = toml::from_str(&serialized).unwrap();
        assert_eq!(config, deserialized);
        assert_eq!(deserialized.p2p_stack, stack);
    }

    let serialized = toml::to_string(&Config::default()).unwrap();
    assert!(serialized.contains("p2p_stack = \"default\""));
}

#[test]
fn zakura_bootstrap_peers_default_to_selected_network() {
    let _init_guard = zakura_test::init();

    let mainnet_config: Config = toml::from_str("network = 'Mainnet'").unwrap();
    assert_eq!(
        mainnet_config.zakura.bootstrap_peers,
        default_zakura_bootstrap_peers()
    );

    let testnet_config: Config = toml::from_str("network = 'Testnet'").unwrap();
    assert_eq!(
        testnet_config.zakura.bootstrap_peers,
        default_testnet_zakura_bootstrap_peers()
    );
}

#[test]
fn explicit_zakura_bootstrap_peers_override_network_defaults() {
    let _init_guard = zakura_test::init();

    let empty_config: Config = toml::from_str(
        r#"
        network = 'Testnet'

        [zakura]
        bootstrap_peers = []
        "#,
    )
    .unwrap();
    assert!(empty_config.zakura.bootstrap_peers.is_empty());

    let custom_config: Config = toml::from_str(
        r#"
        network = 'Testnet'

        [zakura]
        bootstrap_peers = ["ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6@127.0.0.1:8233"]
        "#,
    )
    .unwrap();
    assert_eq!(
        custom_config.zakura.bootstrap_peers,
        vec!["ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6@127.0.0.1:8233"]
    );
}

#[test]
fn zakura_warns_on_loopback_listener_with_public_bootstrap_peers() {
    let _init_guard = zakura_test::init();

    let config: Config = toml::from_str(
        r#"
        network = 'Testnet'

        [zakura]
        listen_addr = "127.0.0.1:18234"
        "#,
    )
    .unwrap();

    assert!(zakura_listens_on_loopback_with_non_loopback_bootstrap_peers(&config.zakura));
}

#[test]
fn zakura_loopback_listener_allows_loopback_bootstrap_peers() {
    let _init_guard = zakura_test::init();

    let config: Config = toml::from_str(
        r#"
        network = 'Testnet'

        [zakura]
        listen_addr = "127.0.0.1:18234"
        bootstrap_peers = ["ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6@127.0.0.1:8234"]
        "#,
    )
    .unwrap();

    assert!(!zakura_listens_on_loopback_with_non_loopback_bootstrap_peers(&config.zakura));
}

#[test]
fn zakura_public_listener_allows_public_bootstrap_peers() {
    let _init_guard = zakura_test::init();

    let config: Config = toml::from_str(
        r#"
        network = 'Testnet'

        [zakura]
        listen_addr = "0.0.0.0:18234"
        "#,
    )
    .unwrap();

    assert!(!zakura_listens_on_loopback_with_non_loopback_bootstrap_peers(&config.zakura));
    assert_eq!(
        config.zakura.listen_addr,
        Some("0.0.0.0:18234".parse::<SocketAddr>().unwrap())
    );
}

#[test]
fn zakura_dev_network_defaults_off_and_roundtrips() {
    let _init_guard = zakura_test::init();

    // Absent by default, so nodes join the public Zakura overlay.
    assert_eq!(Config::default().zakura.dev_network, None);

    let config: Config = toml::from_str(
        r#"
        [zakura]
        dev_network = "evan-breaking-change"
        "#,
    )
    .unwrap();
    assert_eq!(
        config.zakura.dev_network.as_deref(),
        Some("evan-breaking-change")
    );

    let serialized = toml::to_string(&config).unwrap();
    let deserialized: Config = toml::from_str(&serialized).unwrap();
    assert_eq!(config, deserialized);
}

#[test]
fn canonical_p2p_stack_names_parse() {
    let _init_guard = zakura_test::init();

    for (value, stack) in [
        ("default", P2pStack::Default),
        ("legacy", P2pStack::Legacy),
        ("zakura", P2pStack::Zakura),
        ("dual", P2pStack::Dual),
    ] {
        let config: Config = toml::from_str(&format!("p2p_stack = '{value}'"))
            .unwrap_or_else(|error| panic!("p2p_stack = '{value}' should parse: {error}"));

        assert_eq!(config.p2p_stack, stack, "p2p_stack = '{value}'");
    }
}

#[test]
fn p2p_stack_rejects_unknown_values() {
    let _init_guard = zakura_test::init();

    for value in ["enabled", "zebra", "v1", "v2", "combined"] {
        let error = toml::from_str::<Config>(&format!("p2p_stack = '{value}'")).unwrap_err();
        assert!(
            error.to_string().contains("unknown variant"),
            "unexpected p2p_stack error for {value:?}: {error}",
        );
    }
}

#[test]
fn deprecated_p2p_flags_map_onto_p2p_stack() {
    let _init_guard = zakura_test::init();

    // Both flags absent means the config predates them, so it follows the network default.
    let config: Config = toml::from_str("network = 'Mainnet'").unwrap();
    assert_eq!(config.p2p_stack, P2pStack::Default);

    // A flag that is absent falls back to the `true` default it had in the releases that
    // understood these flags.
    for (toml, stack) in [
        ("legacy_p2p = true\nv2_p2p = true", P2pStack::Dual),
        ("legacy_p2p = true\nv2_p2p = false", P2pStack::Legacy),
        ("legacy_p2p = false\nv2_p2p = true", P2pStack::Zakura),
        ("enable_p2p_v2 = false", P2pStack::Legacy),
        ("legacy_p2p = false", P2pStack::Zakura),
        ("v2_p2p = true", P2pStack::Dual),
    ] {
        let config: Config =
            toml::from_str(toml).unwrap_or_else(|error| panic!("{toml:?} should parse: {error}"));

        assert_eq!(config.p2p_stack, stack, "{toml:?}");
    }
}

#[test]
fn deprecated_p2p_flags_reject_disabling_all_networking() {
    let _init_guard = zakura_test::init();

    let error = toml::from_str::<Config>("legacy_p2p = false\nv2_p2p = false")
        .expect_err("a node with no P2P stack can't sync");

    assert!(
        error.to_string().contains("disables all"),
        "unexpected both-flags-false error: {error}",
    );
}

#[test]
fn deprecated_p2p_flags_conflict_with_p2p_stack() {
    let _init_guard = zakura_test::init();

    for toml in [
        "p2p_stack = 'dual'\nlegacy_p2p = true",
        "p2p_stack = 'dual'\nv2_p2p = true",
        "p2p_stack = 'legacy'\nenable_p2p_v2 = false",
    ] {
        let error = toml::from_str::<Config>(toml)
            .expect_err("p2p_stack and the deprecated flags can disagree, so both is an error");

        assert!(
            error.to_string().contains("can't be combined"),
            "unexpected conflict error for {toml:?}: {error}",
        );
    }
}

#[test]
fn p2p_v2_old_config_without_zakura_fields_uses_safe_defaults() {
    let _init_guard = zakura_test::init();

    let config: Config = toml::from_str(
        r#"
        network = "Testnet"
        listen_addr = "127.0.0.1:8233"
        peerset_initial_target_size = 25
        "#,
    )
    .unwrap();

    assert_eq!(config.listen_addr.to_string(), "127.0.0.1:8233");
    assert!(config.v2_p2p());
    assert!(config.legacy_p2p());
    assert_eq!(
        config.zakura.bootstrap_peers,
        default_testnet_zakura_bootstrap_peers()
    );
    assert!(config.zakura.max_connections > 0);
    assert_eq!(
        config.zakura.max_connections_per_ip,
        DEFAULT_ZAKURA_MAX_CONNS_PER_IP
    );
    assert!(config.zakura.max_pending_handshakes > 0);
    assert_eq!(
        config.zakura.header_sync.max_headers_per_response,
        DEFAULT_HS_RANGE
    );
    assert_eq!(
        config.zakura.header_sync.max_inflight_requests,
        DEFAULT_HS_MAX_INFLIGHT
    );
    assert_eq!(
        config.zakura.header_sync.status_refresh_interval,
        Duration::from_secs(30)
    );
    assert_eq!(config.zakura.header_sync.anchor_height, None);
    assert_eq!(config.zakura.header_sync.anchor_hash, None);
}

#[test]
fn p2p_v2_unknown_future_config_fields_are_rejected() {
    let _init_guard = zakura_test::init();

    let top_level = toml::from_str::<Config>("future_zakura_field = true")
        .expect_err("deny_unknown_fields rejects unknown top-level fields");
    assert!(
        top_level.to_string().contains("unknown field"),
        "unexpected error for unknown top-level field: {top_level}",
    );

    let nested = toml::from_str::<Config>(
        r#"
        [zakura]
        future_field = true
        "#,
    )
    .expect_err("deny_unknown_fields rejects unknown Zakura fields");
    assert!(
        nested.to_string().contains("unknown field"),
        "unexpected error for unknown nested field: {nested}",
    );

    let header_sync = toml::from_str::<Config>(
        r#"
        [zakura.header_sync]
        future_field = true
        "#,
    )
    .expect_err("deny_unknown_fields rejects unknown header-sync fields");
    assert!(
        header_sync.to_string().contains("unknown field"),
        "unexpected error for unknown header-sync field: {header_sync}",
    );

    let block_sync = toml::from_str::<Config>(
        r#"
        [zakura.block_sync]
        future_field = true
        "#,
    )
    .expect_err("deny_unknown_fields rejects unknown block-sync fields");
    assert!(
        block_sync.to_string().contains("unknown field"),
        "unexpected error for unknown block-sync field: {block_sync}",
    );
}

#[test]
fn p2p_v2_config_roundtrip_keeps_dconfig_zakura_fields() {
    let _init_guard = zakura_test::init();

    let config: Config = toml::from_str(
        r#"
        p2p_stack = "dual"

        [zakura]
        bootstrap_peers = ["ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6@127.0.0.1:8233"]
        max_connections = 7
        max_connections_per_ip = 5
        max_pending_handshakes = 3
        stream_open_rate_per_second = 11
        message_rate_per_second = 13
        trace_dir = "target/zakura-test-traces"

        [zakura.header_sync]
        max_headers_per_response = 333
        max_inflight_requests = 9
        status_refresh_interval = "45s"

        [zakura.block_sync]
        replace_legacy_syncer = true
        max_blocks_per_response = 5
        status_refresh_interval = "12s"
        "#,
    )
    .unwrap();

    let serialized = toml::to_string(&config).unwrap();
    assert!(serialized.contains("p2p_stack = \"dual\""));
    assert!(serialized.contains("[zakura]"));
    assert!(serialized.contains("bootstrap_peers"));
    assert!(serialized.contains("max_connections = 7"));
    assert!(serialized.contains("max_connections_per_ip = 5"));
    assert!(serialized.contains("trace_dir = \"target/zakura-test-traces\""));
    assert!(serialized.contains("[zakura.header_sync]"));
    assert!(serialized.contains("max_headers_per_response = 333"));
    assert!(serialized.contains("max_inflight_requests = 9"));
    assert!(serialized.contains("status_refresh_interval = \"45s\""));
    assert!(serialized.contains("[zakura.block_sync]"));
    assert!(!serialized.contains("replace_legacy_syncer"));
    assert!(serialized.contains("max_blocks_per_response = 5"));
    assert!(serialized.contains("status_refresh_interval = \"12s\""));
    assert_eq!(toml::from_str::<Config>(&serialized).unwrap(), config);
    assert!(
        !config.zakura.block_sync.replace_legacy_syncer,
        "deprecated replace_legacy_syncer config is accepted but ignored"
    );
}

#[test]
fn configured_regtest_checkpoints_preserve_regtest_identity() {
    let _init_guard = zakura_test::init();

    // Mirrors the per-node config the zakura-regtest-e2e harness writes for the from-scratch
    // catch-up node: a Regtest node that overrides only the checkpoint list (derived at
    // runtime from the miner's chain). Regtest identity — genesis hash and network magic —
    // must be preserved so the node still peers with a plain-Regtest miner; only checkpoint
    // verification is added.
    let genesis = Network::new_regtest(Default::default()).genesis_hash();
    let checkpoint = zakura_chain::block::Hash([7; 32]);

    // The exact minimal `[network.params]` table the harness writes: it overrides only the
    // checkpoint list and lets every other Regtest parameter default. `block::Hash`
    // serializes as a 32-byte array in internal (display-reversed) order, so the harness must
    // emit byte arrays, not hex — this asserts that exact form parses.
    let bytes_csv = |hash: zakura_chain::block::Hash| {
        hash.0
            .iter()
            .map(|byte| byte.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    };
    // The harness rewrites node2's `network = "Regtest"` line in place with this inline table
    // (a single-line `sed` replacement), so verify exactly that form.
    let inline = format!(
        "network = {{ params = {{ checkpoints = [[0, [{}]], [10, [{}]]] }} }}\n",
        bytes_csv(genesis),
        bytes_csv(checkpoint),
    );

    let config: Config = toml::from_str(&inline)
        .expect("the harness's inline ConfiguredRegtest checkpoint TOML deserializes");

    assert!(
        config.network.is_regtest(),
        "a checkpoint-only override must stay Regtest",
    );
    assert_eq!(
        config.network.genesis_hash(),
        genesis,
        "Regtest genesis hash is preserved, so the node still peers with a plain-Regtest miner",
    );

    let checkpoints = config.network.checkpoint_list();
    assert_eq!(
        checkpoints.max_height(),
        Height(10),
        "the derived checkpoint list replaces the genesis-only Regtest default",
    );
    assert_eq!(checkpoints.hash(Height(0)), Some(genesis));
    assert_eq!(checkpoints.hash(Height(10)), Some(checkpoint));
}

#[test]
fn configured_regtest_rejects_testnet_checkpoints() {
    let _init_guard = zakura_test::init();

    let error = toml::from_str::<Config>("network = { params = { checkpoints = true } }")
        .expect_err("Regtest must reject the default Testnet checkpoint list");

    assert!(
        error
            .to_string()
            .contains("first checkpoint hash must match genesis hash"),
        "unexpected Regtest checkpoint error: {error}",
    );
}

#[test]
fn zakura_bootstrap_peers_parse_in_nested_config() {
    let _init_guard = zakura_test::init();

    let config: Config = toml::from_str(
        r#"
        p2p_stack = "dual"

        [zakura]
        bootstrap_peers = ["ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6@127.0.0.1:8233"]
        max_connections = 4
        max_pending_handshakes = 2
        stream_open_rate_per_second = 3
        message_rate_per_second = 5
        "#,
    )
    .unwrap();

    assert!(config.v2_p2p());
    assert!(config.legacy_p2p());
    assert_eq!(config.zakura.bootstrap_peers.len(), 1);
    assert_eq!(config.zakura.max_connections, 4);
    assert_eq!(
        config.zakura.max_connections_per_ip,
        DEFAULT_ZAKURA_MAX_CONNS_PER_IP
    );
    assert_eq!(config.zakura.max_pending_handshakes, 2);
    assert_eq!(config.zakura.stream_open_rate_per_second, 3);
    assert_eq!(config.zakura.message_rate_per_second, 5);
}

#[test]
fn default_config_uses_ipv6() {
    let _init_guard = zakura_test::init();
    let config = Config::default();

    assert_eq!(config.listen_addr.to_string(), "[::]:8233");
    assert!(config.listen_addr.is_ipv6());
    assert_eq!(config.zakura.listen_addr, Some(DEFAULT_ZAKURA_LISTEN_ADDR));
    assert_eq!(
        config.zakura.max_connections_per_ip,
        DEFAULT_ZAKURA_MAX_CONNS_PER_IP
    );
}

#[test]
fn funding_streams_serialization_roundtrip() {
    let _init_guard = zakura_test::init();

    let fs = testnet::Parameters::default()
        .funding_streams()
        .iter()
        .map(ConfiguredFundingStreams::from)
        .collect();

    let mut config = Config {
        network: testnet::Parameters::build()
            .with_funding_streams(fs)
            .to_network()
            .expect("failed to build configured network"),
        initial_testnet_peers: [].into(),
        ..Config::for_test(P2pStack::Dual)
    };
    config.zakura.apply_network_defaults(&config.network);

    let serialized = toml::to_string(&config).unwrap();
    let deserialized: Config = toml::from_str(&serialized).unwrap();

    assert_eq!(config, deserialized);
}

/// Checks that a configured Testnet's temporary Orchard-disabling soft fork height
/// survives a serialization round-trip.
#[test]
fn temporary_orchard_disabling_soft_fork_height_serialization_roundtrip() {
    let _init_guard = zakura_test::init();

    let soft_fork_height = Height(2_000_000);

    let mut config = Config {
        network: testnet::Parameters::build()
            .with_temporary_orchard_disabling_soft_fork_height(soft_fork_height)
            .to_network()
            .expect("failed to build configured network"),
        initial_testnet_peers: [].into(),
        ..Config::for_test(P2pStack::Dual)
    };
    config.zakura.apply_network_defaults(&config.network);

    let serialized = toml::to_string(&config).unwrap();
    let deserialized: Config = toml::from_str(&serialized).unwrap();

    assert_eq!(config, deserialized);

    // The configured height must be preserved through the round-trip.
    let Network::Testnet(params) = &deserialized.network else {
        panic!("deserialized network must be a Testnet");
    };
    assert_eq!(
        params.temporary_orchard_disabling_soft_fork_height(),
        Some(soft_fork_height),
    );
}

/// With no `zakura_node_secret_key` and a writable identity directory, the
/// generated Zakura iroh identity must be persisted on first use and reused on
/// every later startup, so the node's `NodeId` is stable across restarts.
///
/// This is the regression test for `claude-ephemeral-node-secret-on-restart`:
/// before the fix, `Config::zakura_secret_key` generated a fresh ephemeral key on
/// every call and never wrote the reserved identity key file, so two startups
/// produced different `NodeId`s and no key file existed.
#[test]
fn zakura_secret_key_is_persisted_and_stable_across_restarts() {
    let _init_guard = zakura_test::init();

    let key_dir = tempfile::tempdir().expect("failed to create temp key dir");
    let key_file = key_dir.path().join("mainnet.zakura-iroh-secret-key");

    // The key file must not exist before first use.
    assert!(
        !key_file.exists(),
        "key file should not exist before the first startup",
    );

    // First startup: generate and persist a fresh key.
    let first = load_or_generate_zakura_secret_key(&key_file);

    // The reserved identity key file must now exist (atomic create+persist).
    assert!(
        key_file.exists(),
        "first startup must persist the generated identity key file",
    );

    // On Unix, the long-term private identity file must be owner-only (0o600).
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&key_file)
            .expect("key file metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "persisted secret key file must be owner-only");
    }

    // Second startup reading the same key file (simulating a process restart)
    // must reuse the persisted key, yielding the same `NodeId`.
    let after_restart = load_or_generate_zakura_secret_key(&key_file);

    assert_eq!(
        first.public(),
        after_restart.public(),
        "node identity must be stable across restarts when persisted to the identity file",
    );

    // Calling again must also be stable.
    let again = load_or_generate_zakura_secret_key(&key_file);
    assert_eq!(
        first.public(),
        again.public(),
        "repeat resolution must reuse the persisted key",
    );
}

/// A configured `zakura_node_secret_key` must always win and is never overwritten
/// by the automatic persistence path; a disabled cache dir still yields a
/// persistent Zakura identity path outside the cache directory.
#[test]
fn zakura_secret_key_honors_configured_key_and_disabled_cache() {
    let _init_guard = zakura_test::init();

    let key_dir = tempfile::tempdir().expect("failed to create temp key dir");
    let key_file = key_dir.path().join("mainnet.zakura-iroh-secret-key");

    // Persist a key first so an automatic key exists.
    let persisted = load_or_generate_zakura_secret_key(&key_file);

    // A configured key (64-char lowercase hex of the all-ones secret) must override
    // the persisted automatic key.
    let configured = "01".repeat(32);
    let with_key: Config = toml::from_str(&format!("zakura_node_secret_key = '{configured}'"))
        .expect("valid configured key parses");
    let from_config = with_key
        .zakura_secret_key()
        .expect("configured key should resolve");
    assert_ne!(
        from_config.public(),
        persisted.public(),
        "configured key must override the persisted automatic key",
    );

    // A disabled cache dir still has a persistent Zakura identity path, which is
    // outside the cache directory.
    let disabled = Config {
        cache_dir: CacheDir::disabled(),
        zakura_node_secret_key: None,
        p2p_stack: P2pStack::Dual,
        ..Config::default()
    };
    let disabled_key_file = disabled.identity_dir.join("mainnet.zakura-iroh-secret-key");
    assert_eq!(
        disabled_key_file,
        zakura_secret_key_file_path(&disabled.identity_dir, &disabled.network),
    );
    assert_eq!(
        disabled.cache_dir.peer_cache_file_path(&disabled.network),
        None,
        "disabled cache dir must still disable the peer cache",
    );
    assert_eq!(
        disabled_key_file
            .parent()
            .and_then(|path| path.file_name())
            .and_then(|name| name.to_str()),
        Some(".zakura"),
        "disabled cache dir must still store the Zakura identity outside the peer cache",
    );
    assert_eq!(
        disabled_key_file
            .file_name()
            .and_then(|name| name.to_str()),
        Some("mainnet.zakura-iroh-secret-key"),
        "disabled cache dir must still yield a persistent Zakura identity path outside the peer cache",
    );
}
