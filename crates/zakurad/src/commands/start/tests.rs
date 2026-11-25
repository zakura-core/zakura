use eyre::eyre;
use tokio_util::sync::CancellationToken;
use zakura_chain::{block, miner_mesh::GatewayId};

use super::{error_exit_status, supervise_block_sync_driver, NodeTasks, StartCmd};
use crate::components::{miner_mesh::LocalGateway, zcashd_compat};
use crate::config::ZakuradConfig;
use zakura_network::types::PeerServices;
use zakura_network::P2pStack;
use zakura_state::{PruningConfig, StorageMode};

#[tokio::test]
async fn shutdown_cannot_mask_a_ready_writer_failure() {
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let failure: zakura_state::BoxError = "terminal writer failure".into();
    // Force the interleaving where shutdown wins even though writer failure is ready.
    let result = tokio::select! {
        biased;
        _ = shutdown.cancelled() => Ok(()),
        _ = std::future::ready(()) => panic!("shutdown wins this interleaving"),
    };
    let error = StartCmd::finish_shutdown(result, Some(failure)).unwrap_err();
    assert!(error.to_string().contains("terminal writer failure"));
    assert!(StartCmd::finish_shutdown(Ok(()), None).is_ok());
    assert_eq!(
        StartCmd::finish_shutdown(Err(eyre!("task failed")), None)
            .unwrap_err()
            .to_string(),
        "task failed"
    );
}

/// The node keeps the first sighting of each block legacy peers show it, to rank the block's
/// body. If peers could fill that store, they could deny an honest block its place, so at the
/// default connection limits it must hold everything they can show it while an entry lasts.
///
/// The store's size and lifetime are set in `zakura-consensus`. Each connection's allowance
/// and the connection limits are set in `zakura-network`, which does not depend on it. This
/// test fails if either side changes without the other.
#[test]
fn legacy_peers_cannot_fill_the_block_sighting_store_at_the_default_limits() {
    use zakura_consensus::AnnouncementClaims;

    let most_shown = zakura_network::BlockAnnouncementHook::most_blocks_passed_within(
        &ZakuradConfig::default().network,
        AnnouncementClaims::LEGACY_SIGHTING_LIFETIME,
    );
    assert!(
        most_shown <= AnnouncementClaims::MAX_LEGACY_SIGHTINGS,
        "legacy peers can show the node {most_shown} blocks while a sighting lasts, but the \
         store holds {}: raise the store's size or lower what one connection may pass on",
        AnnouncementClaims::MAX_LEGACY_SIGHTINGS,
    );
}

#[test]
fn advertised_services_describe_storage_mode_with_or_without_zcashd_compat() {
    for compat in [false, true] {
        for (storage_mode, expected) in [
            (
                StorageMode::Archive,
                PeerServices::NODE_NETWORK | PeerServices::NODE_BLOCK_PUSH,
            ),
            (
                StorageMode::Pruned(PruningConfig::default()),
                PeerServices::NODE_NETWORK
                    | PeerServices::NODE_BLOCK_PUSH
                    | PeerServices::NODE_PRUNED,
            ),
        ] {
            let mut config = ZakuradConfig::default();
            config.state.storage_mode = storage_mode;
            config.zcashd_compat.enabled = compat;
            assert_eq!(StartCmd::advertised_services(&config), expected);
        }
    }
}

#[test]
fn zcashd_compat_flag_enables_mode() {
    let cmd = StartCmd {
        filters: Vec::new(),
        zcashd_compat: true,
        unsafe_low_specs: false,
    };
    let mut config = ZakuradConfig::default();
    config.zcashd_compat.manage_zcashd = false;

    let config = cmd
        .override_config(config)
        .expect("zcashd-compat override config should succeed");

    assert!(config.zcashd_compat.enabled);
}

#[test]
fn zcashd_compat_config_enables_mode() {
    let cmd = StartCmd {
        filters: Vec::new(),
        zcashd_compat: false,
        unsafe_low_specs: false,
    };
    let mut config = ZakuradConfig::default();
    config.zcashd_compat.enabled = true;
    config.zcashd_compat.manage_zcashd = false;

    let config = cmd
        .override_config(config)
        .expect("zcashd-compat override config should succeed");

    assert!(config.zcashd_compat.enabled);
}

#[test]
fn blocksync_throughput_probe_requires_v2_p2p() {
    let cmd = StartCmd {
        filters: Vec::new(),
        zcashd_compat: false,
        unsafe_low_specs: false,
    };
    let mut config = ZakuradConfig::default();
    config.network.p2p_stack = P2pStack::Legacy;
    config.sync.debug_blocksync_throughput_target_height = Some(100);

    let error = cmd
        .override_config(config)
        .expect_err("throughput probe should require v2 P2P");

    assert!(
        error.to_string().contains(
            "sync.debug_blocksync_throughput_target_height requires the Zakura P2P v2 stack"
        ),
        "unexpected error: {error}"
    );
}

#[test]
fn announcing_a_gateway_requires_v2_p2p() {
    let cmd = StartCmd {
        filters: Vec::new(),
        zcashd_compat: false,
        unsafe_low_specs: false,
    };
    let mut config = ZakuradConfig::default();
    config.network.p2p_stack = P2pStack::Legacy;
    config.mining.miner_mesh = true;

    let error = cmd
        .override_config(config.clone())
        .expect_err("a gateway needs the v2 stack");
    assert!(
        error
            .to_string()
            .contains("mining.miner_mesh requires the Zakura P2P v2 stack"),
        "unexpected error: {error}"
    );

    // The same node starts without the flag, and with the flag once it runs v2.
    config.mining.miner_mesh = false;
    cmd.override_config(config.clone())
        .expect("a legacy node that does not announce is valid");
    config.mining.miner_mesh = true;
    config.network.p2p_stack = P2pStack::Dual;
    cmd.override_config(config.clone())
        .expect("a v2 node can announce");

    // A gateway others cannot dial, or a node that builds no coinbase, is warned about but
    // not refused. The warnings themselves are not captured here.
    config.network.zakura.listen_addr = None;
    config.mining.miner_address = None;
    cmd.override_config(config)
        .expect("a gateway without a listen address or miner address still starts");
}

/// A gateway that cannot keep a key file would come back with a new ID, so it refuses to
/// start. One whose key is saved, or that announces nothing, starts.
#[test]
fn announcing_a_gateway_requires_a_key_that_survives_restarts() {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    // The identity directory's path is taken by a regular file, so it can hold no key file.
    let blocked = dir.path().join("blocked");
    std::fs::write(&blocked, "").expect("write the blocking file");

    let mut config = ZakuradConfig::default();
    config.network.p2p_stack = P2pStack::Dual;
    config.network.identity_dir = blocked;

    StartCmd::network_config_for_start(&config)
        .expect("a node that does not announce may have no usable key file");

    config.mining.miner_mesh = true;
    let error = StartCmd::network_config_for_start(&config)
        .expect_err("a gateway must be able to keep its key");
    assert!(
        error
            .to_string()
            .contains("mining.miner_mesh needs a node key that stays the same across restarts"),
        "unexpected error: {error}"
    );

    config.network.identity_dir = dir.path().join("identity");
    let network = StartCmd::network_config_for_start(&config)
        .expect("a gateway with a writable key directory saves its key");
    let persisted = config
        .network
        .zakura_secret_key()
        .expect("read the saved identity")
        .public();
    std::fs::rename(&config.network.identity_dir, dir.path().join("moved"))
        .expect("move the identity directory before endpoint startup");
    assert_eq!(
        network
            .zakura_secret_key()
            .expect("load the endpoint identity")
            .public(),
        persisted
    );
    assert!(
        !config.network.identity_dir.exists(),
        "startup must not generate a replacement key"
    );
}

/// A start command with no flags, for checking a config the way `zakurad start` does.
fn plain_start() -> StartCmd {
    StartCmd {
        filters: Vec::new(),
        zcashd_compat: false,
        unsafe_low_specs: false,
    }
}

/// The gateway ID of a pool's relay node.
const RELAY: GatewayId = GatewayId::from_bytes([0x42; 20]);

#[test]
fn naming_more_than_one_gateway_is_refused_as_not_supported_yet() {
    let mut config = ZakuradConfig::default();
    config.mining.miner_mesh = true;
    config.mining.miner_mesh_gateways = vec![RELAY, GatewayId::from_bytes([0x43; 20])];

    let error = plain_start()
        .override_config(config)
        .expect_err("a node names one gateway for now");
    assert!(
        error
            .to_string()
            .contains("naming more than one gateway is not supported yet"),
        "unexpected error: {error}"
    );
}

#[test]
fn a_named_gateway_without_miner_mesh_is_refused() {
    let mut config = ZakuradConfig::default();
    config.mining.miner_mesh_gateways = vec![RELAY];

    let error = plain_start()
        .override_config(config)
        .expect_err("the gateway would never be named");
    assert!(
        error
            .to_string()
            .contains("mining.miner_mesh_gateways has no effect unless mining.miner_mesh is set"),
        "unexpected error: {error}"
    );
}

/// A template node that names its relay is not a gateway itself, so it may run only the
/// legacy stack and keeps no node key.
#[test]
fn a_node_naming_its_relay_needs_neither_v2_nor_a_lasting_key() {
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    // The identity directory's path is taken by a regular file, so it can hold no key file.
    let blocked = dir.path().join("blocked");
    std::fs::write(&blocked, "").expect("write the blocking file");

    let mut config = ZakuradConfig::default();
    config.network.p2p_stack = P2pStack::Legacy;
    config.network.identity_dir = blocked;
    config.mining.miner_mesh = true;
    config.mining.miner_mesh_gateways = vec![RELAY];

    plain_start()
        .override_config(config.clone())
        .expect("a legacy node may name its relay");
    StartCmd::network_config_for_start(&config).expect("a node naming its relay pins no key");
}

#[test]
fn a_relay_without_a_miner_address_needs_v2() {
    let mut config = ZakuradConfig::default();
    config.network.p2p_stack = P2pStack::Legacy;
    config.mining.miner_mesh = true;
    config.mining.miner_address = None;

    let error = plain_start()
        .override_config(config)
        .expect_err("a relay is a gateway, so it needs v2");
    assert!(
        error
            .to_string()
            .contains("mining.miner_mesh requires the Zakura P2P v2 stack"),
        "unexpected error: {error}"
    );
}

/// A node that runs the miner mesh and builds coinbases.
fn announcing_config() -> ZakuradConfig {
    let mut config = ZakuradConfig::default();
    config.mining.miner_mesh = true;
    config.mining.miner_address = Some(
        "t1bmMa1wJDFdbc2TiURQP5BbBz6jHjUBuHq"
            .parse()
            .expect("a valid transparent address"),
    );
    config
}

const OWN_NODE_ID: [u8; 32] = [0x07; 32];

#[test]
fn a_node_without_the_miner_mesh_announces_and_stands_for_nothing() {
    let mut config = announcing_config();
    config.mining.miner_mesh = false;

    assert_eq!(
        StartCmd::local_mesh_gateway(&config, Some(&OWN_NODE_ID)).unwrap(),
        LocalGateway::default(),
    );
}

#[test]
fn a_miner_names_itself_unless_it_names_a_relay() {
    let mut config = announcing_config();
    let own = Some(GatewayId::from_node_id(&OWN_NODE_ID));
    assert_eq!(
        StartCmd::local_mesh_gateway(&config, Some(&OWN_NODE_ID)).unwrap(),
        LocalGateway {
            announced: own,
            stands_for: own,
        },
    );
    assert!(
        StartCmd::local_mesh_gateway(&config, None).is_err(),
        "a node that names itself needs its v2 node ID"
    );

    config.mining.miner_mesh_gateways = vec![RELAY];
    let relay = LocalGateway {
        announced: Some(RELAY),
        stands_for: Some(RELAY),
    };
    assert_eq!(
        StartCmd::local_mesh_gateway(&config, Some(&OWN_NODE_ID)).unwrap(),
        relay,
        "the configured relay comes before the node itself"
    );
    assert_eq!(
        StartCmd::local_mesh_gateway(&config, None).unwrap(),
        relay,
        "a node naming its relay needs no v2 endpoint"
    );
}

/// A node with no miner address builds no coinbase, so it announces nothing. A relay that
/// names itself still stands for itself, so it counts the blocks that name it.
#[test]
fn a_node_without_a_miner_address_announces_nothing() {
    let mut config = announcing_config();
    config.mining.miner_address = None;
    assert_eq!(
        StartCmd::local_mesh_gateway(&config, Some(&OWN_NODE_ID)).unwrap(),
        LocalGateway {
            announced: None,
            stands_for: Some(GatewayId::from_node_id(&OWN_NODE_ID)),
        },
    );

    config.mining.miner_mesh_gateways = vec![RELAY];
    assert_eq!(
        StartCmd::local_mesh_gateway(&config, None).unwrap(),
        LocalGateway::default(),
        "a template node with no miner address names its relay in no coinbase"
    );
}

#[test]
fn a_miner_or_a_node_running_the_miner_mesh_prefers_gateways() {
    let mut config = ZakuradConfig::default();
    config.mining.miner_address = None;
    assert!(!StartCmd::prefers_gateways(&config));

    config.mining.miner_address = announcing_config().mining.miner_address;
    assert!(StartCmd::prefers_gateways(&config));

    config.mining.miner_address = None;
    config.mining.miner_mesh = true;
    assert!(StartCmd::prefers_gateways(&config));
}

#[test]
fn vct_fast_sync_requires_checkpoint_sync_at_startup() {
    let cmd = StartCmd {
        filters: Vec::new(),
        zcashd_compat: false,
        unsafe_low_specs: false,
    };
    let mut config = ZakuradConfig::default();
    config.consensus.checkpoint_sync = false;
    config.consensus.vct_fast_sync = Some(true);

    let error = cmd
        .override_config(config)
        .expect_err("startup should reject explicit VCT fast sync without checkpoint sync");

    assert!(
        error.to_string().contains(
            "invalid consensus configuration: consensus.vct_fast_sync = true requires consensus.checkpoint_sync = true"
        ),
        "unexpected error: {error}"
    );

    // A pre-VCT config that only disables checkpoint sync must keep working:
    // the unset knob is not a contradiction, it just has no effect.
    let cmd = StartCmd {
        filters: Vec::new(),
        zcashd_compat: false,
        unsafe_low_specs: false,
    };
    let mut config = ZakuradConfig::default();
    config.consensus.checkpoint_sync = false;
    config.consensus.vct_fast_sync = None;

    cmd.override_config(config)
        .expect("checkpoint_sync = false with vct_fast_sync unset is a valid config");
}

#[test]
fn block_gossip_peer_ips_require_zcashd_compat() {
    let cmd = StartCmd {
        filters: Vec::new(),
        zcashd_compat: false,
        unsafe_low_specs: false,
    };
    let mut config = ZakuradConfig::default();
    config.zcashd_compat.block_gossip_peer_ips =
        vec![std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)];

    let error = cmd
        .override_config(config)
        .expect_err("block gossip peers should require zcashd-compat");

    assert!(
        error
            .to_string()
            .contains("zcashd_compat.block_gossip_peer_ips requires"),
        "error should explain the zcashd-compat requirement: {error}"
    );
}

#[test]
fn zcashd_compat_config_rejects_disabled_legacy_p2p() {
    let cmd = StartCmd {
        filters: Vec::new(),
        zcashd_compat: false,
        unsafe_low_specs: false,
    };
    let mut config = ZakuradConfig::default();
    config.zcashd_compat.enabled = true;
    config.zcashd_compat.manage_zcashd = false;
    config.network.p2p_stack = P2pStack::Zakura;

    let error = cmd
        .override_config(config)
        .expect_err("the P2P sidecar should require the legacy P2P listener");
    assert!(
        error
            .to_string()
            .contains("requires the legacy Zcash P2P stack"),
        "unexpected error: {error}"
    );
}

#[test]
fn zcashd_compat_config_rejects_peer_selection_extra_args() {
    let cmd = StartCmd {
        filters: Vec::new(),
        zcashd_compat: false,
        unsafe_low_specs: false,
    };
    let mut config = ZakuradConfig::default();
    config.zcashd_compat.enabled = true;
    config.zcashd_compat.manage_zcashd = true;
    config.zcashd_compat.zcashd_source = zcashd_compat::ConfigZcashdBinarySource::Embedded;
    config.zcashd_compat.zcashd_extra_args = vec!["-addnode=1.2.3.4".to_string()];

    let error = cmd
        .override_config(config)
        .expect_err("peer-selection extra args should be rejected");
    assert!(
        error.to_string().contains("peer-selection"),
        "unexpected error: {error}"
    );
}

#[test]
fn zcashd_compat_manage_zcashd_requires_resolvable_path() {
    let cmd = StartCmd {
        filters: Vec::new(),
        zcashd_compat: true,
        unsafe_low_specs: false,
    };
    let mut config = ZakuradConfig::default();
    config.zcashd_compat.manage_zcashd = true;
    config.zcashd_compat.zcashd_path = Some("/definitely/missing/zcashd-compat".into());

    let error = cmd
        .override_config(config)
        .expect_err("zcashd-compat override should fail for an unresolvable zcashd path");

    assert!(
        error
            .to_string()
            .contains("zcashd-compat mode could not resolve zcashd_path"),
        "unexpected error: {error}"
    );
}

#[test]
fn zcashd_compat_path_source_requires_explicit_path() {
    let cmd = StartCmd {
        filters: Vec::new(),
        zcashd_compat: true,
        unsafe_low_specs: false,
    };
    let mut config = ZakuradConfig::default();
    config.zcashd_compat.manage_zcashd = true;
    config.zcashd_compat.zcashd_source = zcashd_compat::ConfigZcashdBinarySource::Path;
    config.zcashd_compat.zcashd_path = None;

    let error = cmd
        .override_config(config)
        .expect_err("path source should require explicit zcashd_path");
    assert!(
        error.to_string().contains("zcashd_source=path"),
        "unexpected error: {error}"
    );
}

#[test]
fn zcashd_compat_embedded_source_allows_missing_local_path() {
    let cmd = StartCmd {
        filters: Vec::new(),
        zcashd_compat: true,
        unsafe_low_specs: false,
    };
    let mut config = ZakuradConfig::default();
    config.zcashd_compat.manage_zcashd = true;
    config.zcashd_compat.zcashd_source = zcashd_compat::ConfigZcashdBinarySource::Embedded;
    config.zcashd_compat.zcashd_path = None;

    cmd.override_config(config)
        .expect("embedded source should be validated at runtime, not override-time");
}

#[test]
fn zcashd_compat_config_manage_zcashd_requires_resolvable_path() {
    let cmd = StartCmd {
        filters: Vec::new(),
        zcashd_compat: false,
        unsafe_low_specs: false,
    };
    let mut config = ZakuradConfig::default();
    config.zcashd_compat.enabled = true;
    config.zcashd_compat.manage_zcashd = true;
    config.zcashd_compat.zcashd_path = Some("/definitely/missing/zcashd-compat".into());

    let error = cmd
        .override_config(config)
        .expect_err("zcashd-compat config should fail for an unresolvable zcashd path");

    assert!(
        error
            .to_string()
            .contains("zcashd-compat mode could not resolve zcashd_path"),
        "unexpected error: {error}"
    );
}

#[test]
fn zcashd_compat_supervisor_shutdown_timeout_matches_config() {
    let mut config = ZakuradConfig::default();

    config.zcashd_compat.enabled = true;
    config.zcashd_compat.manage_zcashd = true;
    config.zcashd_compat.shutdown_grace_period = std::time::Duration::from_secs(42);
    assert_eq!(
        StartCmd::zcashd_compat_supervisor_shutdown_timeout(&config),
        Some(std::time::Duration::from_secs(42) + StartCmd::ZCASHD_COMPAT_SHUTDOWN_TIMEOUT_MARGIN),
        "outer supervisor wait must exceed the child grace period so task \
         abort cannot preempt graceful termination",
    );

    config.zcashd_compat.manage_zcashd = false;
    assert_eq!(
        StartCmd::zcashd_compat_supervisor_shutdown_timeout(&config),
        None
    );

    config.zcashd_compat.enabled = false;
    config.zcashd_compat.manage_zcashd = true;
    assert_eq!(
        StartCmd::zcashd_compat_supervisor_shutdown_timeout(&config),
        None
    );
}

#[test]
fn zcashd_compat_supervisor_ok_exit_does_not_exit_zakura() {
    assert!(!StartCmd::zcashd_compat_supervisor_should_exit(Ok(Ok(()))));
}

#[test]
fn zcashd_compat_supervisor_error_does_not_exit_zakura() {
    assert!(!StartCmd::zcashd_compat_supervisor_should_exit(Ok(Err(
        eyre!("simulated zcashd supervisor runtime failure"),
    ))));
}

#[tokio::test]
async fn zcashd_compat_supervisor_panic_does_not_exit_zakura() {
    let join_err = tokio::spawn(async {
        panic!("simulated zcashd supervisor panic");
    })
    .await
    .expect_err("task should panic");

    assert!(!StartCmd::zcashd_compat_supervisor_should_exit(Err(
        join_err
    )));
}

#[test]
fn header_sync_fatal_event_returns_an_error_and_starts_shutdown() {
    let authority = zakura_header_chain::BodyWorkAuthority {
        header: zakura_header_chain::HeaderWorkAuthority {
            header_generation: zakura_header_chain::HeaderGeneration::new(3),
            branch: zakura_header_chain::BranchId::new(block::Hash([1; 32]), block::Hash([2; 32])),
        },
        verified_generation: zakura_header_chain::VerifiedGeneration::new(4),
        body_work_epoch: zakura_header_chain::BodyWorkEpoch::default(),
    };
    let owner = authority
        .bind(
            5,
            std::num::NonZeroU64::new(6).expect("the test request ID is nonzero"),
        )
        .into();
    let event = zakura_network::zakura::HeaderSyncFatalEvent {
        phase: "apply",
        owner,
        repair_generation: 7,
        target: zakura_header_chain::Frontier::new(block::Height(8), block::Hash([2; 32])),
        elapsed: std::time::Duration::from_secs(30 * 60),
    };
    let shutdown = CancellationToken::new();

    let error = StartCmd::handle_header_sync_fatal_event(event, &shutdown)
        .expect_err("a fatal VCT operation must stop the node");

    assert!(shutdown.is_cancelled(), "the node starts normal shutdown");
    assert!(error.to_string().contains("VCT local apply operation"));
}

#[tokio::test]
async fn block_sync_driver_exit_returns_an_error_and_starts_shutdown() {
    let shutdown = CancellationToken::new();
    let (fatal_tx, mut fatal_rx) = tokio::sync::mpsc::unbounded_channel();

    supervise_block_sync_driver(async {}, fatal_tx, shutdown.clone()).await;
    fatal_rx
        .recv()
        .await
        .expect("an unexpected driver exit must notify the node root");

    let error = StartCmd::handle_block_sync_driver_exit(&shutdown)
        .expect_err("a critical block-sync driver exit must stop the node");

    assert!(shutdown.is_cancelled(), "the node starts normal shutdown");
    assert!(error.to_string().contains("block-sync driver exited"));
}

#[tokio::test]
async fn block_sync_driver_panic_notifies_the_node_root() {
    let shutdown = CancellationToken::new();
    let (fatal_tx, mut fatal_rx) = tokio::sync::mpsc::unbounded_channel();
    let driver = tokio::spawn(supervise_block_sync_driver(
        async { panic!("simulated block-sync driver panic") },
        fatal_tx,
        shutdown,
    ));

    fatal_rx
        .recv()
        .await
        .expect("a driver panic must notify the node root");
    let error = driver
        .await
        .expect_err("the simulated driver panic propagates");
    assert!(error.is_panic(), "the driver task reports a panic");
}

#[tokio::test]
async fn unpolled_block_sync_driver_cancellation_notifies_the_node_root() {
    let shutdown = CancellationToken::new();
    let (fatal_tx, mut fatal_rx) = tokio::sync::mpsc::unbounded_channel();
    let driver = supervise_block_sync_driver(std::future::pending(), fatal_tx, shutdown);

    drop(driver);

    fatal_rx
        .recv()
        .await
        .expect("canceling an unpolled driver must notify the node root");
}

#[tokio::test]
async fn requested_shutdown_suppresses_block_sync_driver_fatal_event() {
    let shutdown = CancellationToken::new();
    let (fatal_tx, mut fatal_rx) = tokio::sync::mpsc::unbounded_channel();
    shutdown.cancel();

    supervise_block_sync_driver(async {}, fatal_tx, shutdown).await;

    assert_eq!(
        fatal_rx.recv().await,
        None,
        "requested shutdown closes the channel without a fatal event",
    );
}

/// Spawns a task that never finishes, and a receiver that errors once the task is aborted.
fn spawn_until_aborted() -> (
    tokio::task::JoinHandle<Result<(), eyre::Report>>,
    tokio::sync::oneshot::Receiver<()>,
) {
    let (held_by_task, aborted) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _held_by_task = held_by_task;
        std::future::pending().await
    });
    (task, aborted)
}

async fn assert_aborted(aborted: tokio::sync::oneshot::Receiver<()>) {
    tokio::time::timeout(std::time::Duration::from_secs(10), aborted)
        .await
        .expect("an aborted task drops its sender")
        .expect_err("the task never sends");
}

#[tokio::test]
async fn supervised_task_exit_is_reported_with_its_exit_status() {
    let mut node_tasks = NodeTasks::default();
    assert!(
        node_tasks.next_supervised_exit().await.is_none(),
        "no task is supervised yet",
    );

    let (still_running, _aborted) = spawn_until_aborted();
    node_tasks.supervise("still running", still_running, |result| result);
    node_tasks.supervise("clean exit", tokio::spawn(async {}), Ok);
    node_tasks
        .next_supervised_exit()
        .await
        .expect("the clean exit task is supervised")
        .expect("a clean exit is not a node failure");

    node_tasks.supervise(
        "failing",
        tokio::spawn(async { Err(std::io::Error::other("task failed")) }),
        error_exit_status,
    );
    let error = node_tasks
        .next_supervised_exit()
        .await
        .expect("the failing task is supervised")
        .expect_err("a task error is a node failure");
    assert_eq!(error.to_string(), "task failed");
}

#[tokio::test]
async fn server_error_does_not_fail_the_node() {
    let mut node_tasks = NodeTasks::default();
    node_tasks.supervise_server(
        "test",
        tokio::spawn(async { Err(tower::BoxError::from("server failed")) }),
    );

    node_tasks
        .next_supervised_exit()
        .await
        .expect("the server task is supervised")
        .expect("a server error stops the node without an error");
}

#[tokio::test]
#[should_panic(expected = "unexpected panic in the panicking task")]
async fn supervised_task_panic_is_propagated() {
    let mut node_tasks = NodeTasks::default();
    node_tasks.supervise(
        "panicking",
        tokio::spawn(async { panic!("task panicked") }),
        Ok,
    );

    let _ = node_tasks.next_supervised_exit().await;
}

#[tokio::test]
async fn abort_supervised_leaves_tracked_tasks_running_until_drop() {
    let mut node_tasks = NodeTasks::default();
    let (supervised, supervised_aborted) = spawn_until_aborted();
    let (tracked, mut tracked_aborted) = spawn_until_aborted();
    node_tasks.supervise("supervised", supervised, |result| result);
    node_tasks.track(&tracked);

    node_tasks.abort_supervised();
    assert_aborted(supervised_aborted).await;
    assert!(
        matches!(
            tracked_aborted.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ),
        "a tracked task keeps running after the supervised tasks are aborted",
    );

    drop(node_tasks);
    assert_aborted(tracked_aborted).await;
}

#[tokio::test]
async fn dropping_node_tasks_aborts_supervised_tasks() {
    let mut node_tasks = NodeTasks::default();
    let (supervised, supervised_aborted) = spawn_until_aborted();
    node_tasks.supervise("supervised", supervised, |result| result);

    drop(node_tasks);
    assert_aborted(supervised_aborted).await;
}
