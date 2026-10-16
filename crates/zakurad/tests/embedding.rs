//! Tests that running a node inside another application leaves that application's
//! process-wide state alone.
//!
//! This is a separate test binary with a single test, so nothing else in the process
//! installs hooks, subscribers or thread pools.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{sync::oneshot, time::timeout};
use tokio_util::sync::CancellationToken;
use zakura_chain::parameters::Network;
use zakurad::{config::ZakuradConfig, node};

/// How long the embedded node may take to start or stop.
const NODE_TIMEOUT: Duration = Duration::from_secs(120);

fn regtest_config(dir: &std::path::Path) -> ZakuradConfig {
    let mut config = ZakuradConfig::default();
    config.network.network = Network::new_regtest(Default::default());
    config.network.listen_addr = "127.0.0.1:0".parse().expect("valid address");
    config.network.p2p_stack = zakura_network::P2pStack::Legacy;
    config.network.cache_dir = zakura_network::CacheDir::disabled();
    config.network.identity_dir = dir.join("identity");
    config.state = zakura_state::Config::ephemeral();
    config
}

#[test]
fn embedded_node_leaves_host_process_state_alone() {
    // The host owns the error report hook. Creating any report installs a default hook,
    // so the host has to install its own before running the node.
    color_eyre::install().expect("nothing else has installed an error hook yet");

    let host_panic_hook_called = Arc::new(AtomicBool::new(false));
    let default_panic_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new({
        let called = host_panic_hook_called.clone();
        move |info| {
            called.store(true, Ordering::SeqCst);
            default_panic_hook(info);
        }
    }));

    let dir = tempfile::tempdir().expect("temporary directory is created");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("host runtime builds");

    // A config error is returned before the node is ready, and the ready channel closes.
    let mut invalid = regtest_config(dir.path());
    invalid.zcashd_compat.block_gossip_peer_ips = vec!["127.0.0.1".parse().expect("valid IP")];
    let (ready_tx, ready_rx) = oneshot::channel();
    let error = runtime
        .block_on(node::run_with_services_ready(
            invalid,
            Vec::new(),
            CancellationToken::new(),
            ready_tx,
        ))
        .expect_err("an invalid config is rejected");
    assert!(
        error
            .to_string()
            .contains("zcashd_compat.block_gossip_peer_ips requires zcashd_compat.enabled = true"),
        "unexpected error: {error:#}"
    );
    assert!(
        runtime.block_on(ready_rx).is_err(),
        "the ready channel closes without services"
    );

    // A node that is cancelled before it starts returns immediately.
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let (ready_tx, ready_rx) = oneshot::channel();
    runtime
        .block_on(node::run_with_services_ready(
            regtest_config(dir.path()),
            Vec::new(),
            cancelled,
            ready_tx,
        ))
        .expect("a cancelled node stops without an error");
    assert!(runtime.block_on(ready_rx).is_err());

    // Neither path configured the global Rayon pool. A node that starts may use the pool,
    // which makes Rayon create it with defaults, so this is only checked before startup.
    rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build_global()
        .expect("the embedded node leaves the global Rayon pool to the host");

    // A node that starts reports its services, then stops when cancelled.
    let shutdown = CancellationToken::new();
    let (ready_tx, ready_rx) = oneshot::channel();
    runtime.block_on(async {
        let node = node::run_with_services_ready(
            regtest_config(dir.path()),
            Vec::new(),
            shutdown.clone(),
            ready_tx,
        );
        tokio::pin!(node);
        let services = tokio::select! {
            result = &mut node => panic!("the node stopped before it was ready: {result:?}"),
            services = timeout(NODE_TIMEOUT, ready_rx) => services
                .expect("the node becomes ready in time")
                .expect("the node sends its services when ready"),
        };
        drop(services);
        shutdown.cancel();
        timeout(NODE_TIMEOUT, node)
            .await
            .expect("the node stops in time")
            .expect("a cancelled node stops without an error");
    });

    // The host runtime still runs tasks after the node stops.
    assert_eq!(
        runtime
            .block_on(runtime.spawn(async { 7 }))
            .expect("host task runs"),
        7
    );

    // The node installed no global tracing subscriber or panic hook.
    assert!(
        !tracing::dispatcher::has_been_set(),
        "the embedded node leaves the global tracing subscriber to the host"
    );
    let _ = std::panic::catch_unwind(|| panic!("probe the host panic hook"));
    assert!(
        host_panic_hook_called.load(Ordering::SeqCst),
        "the host panic hook is still installed"
    );
    runtime.shutdown_timeout(NODE_TIMEOUT);
}
