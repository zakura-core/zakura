//! Exercise the startup coordinator and genesis task across a durable height-zero restart.

use std::{sync::Arc, time::Duration};

use tower::{service_fn, Service, ServiceExt};
use zakura_chain::{
    block::Height, chain_tip::ChainTip, parameters::Network, serialization::ZcashDeserializeInto,
};

use super::new_sync_coordinator;

#[tokio::test(flavor = "multi_thread")]
async fn genesis_bootstrap_reopens_with_readiness_before_and_after_coordinator() {
    const PHASE_ENV: &str = "ZAKURA_GENESIS_HANDOFF_TEST_PHASE";
    const DIR_ENV: &str = "ZAKURA_GENESIS_HANDOFF_TEST_DIR";
    let phase = std::env::var(PHASE_ENV).ok();
    let Some(phase) = phase else {
        let dir = tempfile::tempdir().expect("test state directory is created");
        for phase in ["persist", "ready-before", "ready-after"] {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("commands::start::genesis_bootstrap_tests::genesis_bootstrap_reopens_with_readiness_before_and_after_coordinator")
                .arg("--exact")
                .arg("--nocapture")
                .env(PHASE_ENV, phase)
                .env(DIR_ENV, dir.path())
                .status()
                .expect("restart test process starts");
            assert!(status.success(), "height-zero {phase} succeeds");
        }
        return;
    };

    let _guard = zakura_test::init();
    let mut config = crate::config::ZakuradConfig::default();
    config.network.network = Network::Mainnet;
    config.network.p2p_stack = zakura_network::config::P2pStack::Zakura;
    config.state = zakura_state::Config {
        cache_dir: std::env::var_os(DIR_ENV).unwrap().into(),
        ephemeral: false,
        debug_skip_non_finalized_state_backup_task: true,
        enable_zakura_header_seed_from_committed_blocks: true,
        ..Default::default()
    };
    // In the early case, runtime readiness cannot have been published yet: state
    // has not even started. The late case waits explicitly for Ready below.
    let early = (phase != "ready-before").then(|| new_sync_coordinator(&config.network));
    let (mut state, read_state, tip, _tip_change, _authority) =
        zakura_state::init_with_header_chain_body_evidence(
            config.state.clone(),
            &config.network.network,
            Height(0),
            2,
        )
        .await
        .expect("persistent state opens");
    if phase == "persist" {
        let genesis: Arc<zakura_chain::block::Block> =
            zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
                .zcash_deserialize_into()
                .unwrap();
        state
            .ready()
            .await
            .unwrap()
            .call(zakura_state::Request::CommitCheckpointVerifiedBlock(
                genesis.into(),
            ))
            .await
            .expect("genesis persists");
    }
    state
        .ready()
        .await
        .expect("semantic state handoff completes");
    let mut runtime = read_state.subscribe_header_runtime_status();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !runtime.borrow().is_ready() {
            runtime
                .changed()
                .await
                .expect("runtime publisher stays live");
        }
    })
    .await
    .expect("persisted genesis attaches the runtime");
    assert_eq!(tip.best_tip_height(), Some(Height(0)));
    let coordinator = early.unwrap_or_else(|| new_sync_coordinator(&config.network));

    let state = Arc::new(tokio::sync::Mutex::new(state));
    let state = service_fn(move |request| {
        let state = state.clone();
        async move { state.lock().await.ready().await?.call(request).await }
    });
    let peers = service_fn(|_: zakura_network::Request| async {
        panic!("persisted genesis must not be downloaded");
        #[allow(unreachable_code)]
        Ok::<_, crate::BoxError>(zakura_network::Response::Nil)
    });
    let verifier = service_fn(|_: zakura_consensus::Request| async {
        panic!("persisted genesis must not be recommitted");
        #[allow(unreachable_code)]
        Ok::<_, crate::BoxError>(zakura_chain::block::Hash([0; 32]))
    });
    let (misbehavior, _receiver) = tokio::sync::mpsc::channel(1);
    let (syncer, _status) = crate::components::ChainSync::new(
        &config,
        Height(0),
        peers,
        verifier,
        state,
        tip,
        misbehavior,
    );
    let snapshots = read_state.subscribe_header_chain_snapshots();
    let mut phases = coordinator.subscribe_apply_phase();
    let bootstrap = syncer.bootstrap_genesis_then_pause(
        read_state,
        snapshots,
        runtime,
        false,
        coordinator.clone(),
    );
    tokio::pin!(bootstrap);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                result = &mut bootstrap => panic!("genesis task exited instead of monitoring native sync: {result:?}"),
                changed = phases.changed() => {
                    changed.expect("coordinator stays live");
                    if coordinator.zakura_owns_applies() { break; }
                }
            }
        }
    }).await.expect("genesis task hands off without a stall");
    assert!(
        futures::poll!(&mut bootstrap).is_pending(),
        "genesis task remains in its watchdog"
    );
    coordinator
        .finish_legacy_bootstrap()
        .expect_err("a duplicate handoff remains illegal");
}
