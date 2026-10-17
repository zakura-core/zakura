//! Tests for restricted unauthenticated and authenticated RPC method surfaces.

use std::{
    collections::BTreeSet,
    fs,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    time::Duration,
};

use reqwest::Client;
use tokio::sync::watch;
use tower::buffer::Buffer;

use crate::{
    config::rpc::{Config, TlsConfig},
    methods::{RpcAccess, RpcImpl, RpcSurface, METHODS, RPC_METHOD_ACCESS},
    server::{enabled_rpc_methods, primary_rpc_surface, MAX_RPC_BATCH_REQUESTS},
};
use zakura_chain::{chain_sync_status::MockSyncStatus, chain_tip::NoChainTip, parameters::Network};
use zakura_network::address_book_peers::MockAddressBookPeers;
use zakura_node_services::BoxError;
use zakura_test::mock_service::MockService;

use super::super::RpcServer;

/// Returns every explicitly classified method name.
fn classified_methods() -> Vec<&'static str> {
    RPC_METHOD_ACCESS.iter().map(|(name, _)| *name).collect()
}

#[test]
fn access_policy_matches_the_openrpc_method_set() {
    let classified: BTreeSet<_> = RPC_METHOD_ACCESS.iter().map(|(name, _)| *name).collect();
    let documented: BTreeSet<_> = METHODS.iter().map(|(name, _)| *name).collect();

    assert_eq!(
        classified.len(),
        RPC_METHOD_ACCESS.len(),
        "each RPC method must be classified exactly once"
    );
    assert_eq!(
        classified, documented,
        "registered OpenRPC methods and access classifications must match"
    );
}

#[test]
fn restricted_surface_contains_only_unauthenticated_methods() {
    let actual: BTreeSet<_> = enabled_rpc_methods(&classified_methods(), RpcSurface::Restricted)
        .expect("the reviewed method classification should be complete")
        .into_iter()
        .collect();
    let expected: BTreeSet<_> = RPC_METHOD_ACCESS
        .iter()
        .filter_map(|(name, access)| (*access == RpcAccess::Unauthenticated).then_some(*name))
        .collect();

    assert_eq!(actual, expected);
    assert!(!actual.contains("invalidateblock"));
    assert!(!actual.contains("reconsiderblock"));
    assert!(!actual.contains("preciousblock"));
    assert!(!actual.contains("stop"));
    assert!(!actual.contains("generate"));
    assert!(!actual.contains("addnode"));
}

#[test]
fn full_surface_retains_admin_and_test_methods() {
    let actual: BTreeSet<_> = enabled_rpc_methods(&classified_methods(), RpcSurface::Full)
        .expect("the reviewed method classification should be complete")
        .into_iter()
        .collect();

    assert!(actual.contains("invalidateblock"));
    assert!(actual.contains("reconsiderblock"));
    assert!(actual.contains("preciousblock"));
    assert!(actual.contains("stop"));
    assert!(actual.contains("generate"));
    assert!(actual.contains("addnode"));
}

#[test]
fn unclassified_methods_fail_closed() {
    let mut methods = classified_methods();
    methods.push("new_unreviewed_method");

    let error = enabled_rpc_methods(&methods, RpcSurface::Restricted)
        .expect_err("an unclassified method must prevent server startup");

    assert!(error.to_string().contains("new_unreviewed_method"));
}

#[test]
fn unauthenticated_production_networks_use_the_restricted_surface() {
    assert_eq!(
        primary_rpc_surface(&Network::Mainnet, false),
        RpcSurface::Restricted
    );
    assert_eq!(
        primary_rpc_surface(&Network::new_default_testnet(), false),
        RpcSurface::Restricted
    );

    assert_eq!(
        primary_rpc_surface(&Network::Mainnet, true),
        RpcSurface::Full
    );
    assert_eq!(
        primary_rpc_surface(&Network::new_regtest(Default::default()), false),
        RpcSurface::Full
    );
}

#[tokio::test]
async fn segmented_listeners_enforce_methods_and_cookie_auth() {
    let _init_guard = zakura_test::init();

    let restricted_port = zakura_test::net::random_known_port();
    let admin_port = loop {
        let port = zakura_test::net::random_known_port();
        if port != restricted_port {
            break port;
        }
    };
    let restricted_addr: SocketAddr =
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, restricted_port).into();
    let admin_addr: SocketAddr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, admin_port).into();
    let cookie_dir = tempfile::tempdir().expect("temporary cookie directory should be created");
    let conf = Config {
        listen_addr: Some(restricted_addr),
        admin_listen_addr: Some(admin_addr),
        cookie_dir: cookie_dir.path().to_path_buf(),
        cookie_file_name: "admin.cookie".to_string(),
        enable_cookie_auth: false,
        ..Config::default()
    };

    let mempool: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let state: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let read_state: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let block_verifier_router: MockService<_, _, _, BoxError> =
        MockService::build().for_unit_tests();
    let (_tx, rx) = watch::channel(None);
    let (rpc_impl, rpc_queue_task) = RpcImpl::new(
        Network::Mainnet,
        Default::default(),
        false,
        "RPC surface test",
        "RPC surface test",
        Buffer::new(mempool, 1),
        Buffer::new(state, 1),
        Buffer::new(read_state, 1),
        Buffer::new(block_verifier_router, 1),
        MockSyncStatus::default(),
        NoChainTip,
        MockAddressBookPeers::default(),
        rx,
        None,
    );

    let restricted_task = RpcServer::start(rpc_impl.clone(), conf.clone())
        .await
        .expect("restricted RPC listener should start");
    let admin_task = RpcServer::start_admin(rpc_impl, conf)
        .await
        .expect("admin RPC listener should start");

    let _ = rustls::crypto::ring::default_provider().install_default();
    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("test HTTP client should build");

    let restricted_discovery = client
        .post(format!("http://{restricted_addr}"))
        .header("content-type", "application/json")
        .body(r#"{"jsonrpc":"2.0","method":"rpc.discover","id":1}"#)
        .send()
        .await
        .expect("restricted discovery request should complete")
        .text()
        .await
        .expect("restricted discovery response body should be readable");
    let restricted_discovery: serde_json::Value = serde_json::from_str(&restricted_discovery)
        .expect("restricted discovery response should be JSON");
    let restricted_methods = discovered_methods(&restricted_discovery);
    for method in [
        "getinfo",
        "getblockchaininfo",
        "getpeerinfo",
        "getblockcount",
        "getrawtransaction",
        "getaddresstxids",
        "getaddressbalance",
        "getaddressutxos",
        "sendrawtransaction",
        "getblocktemplate",
        "submitblock",
        "submitsolution",
    ] {
        assert!(
            restricted_methods.contains(method),
            "existing unauthenticated integrations require {method}"
        );
    }
    assert!(!restricted_methods.contains("invalidateblock"));
    assert!(!restricted_methods.contains("reconsiderblock"));
    assert!(!restricted_methods.contains("preciousblock"));

    let blocked_call = client
        .post(format!("http://{restricted_addr}"))
        .header("content-type", "application/json")
        .body(r#"{"jsonrpc":"2.0","method":"invalidateblock","params":["00"],"id":2}"#)
        .send()
        .await
        .expect("blocked restricted request should complete")
        .text()
        .await
        .expect("blocked restricted response body should be readable");
    let blocked_call: serde_json::Value =
        serde_json::from_str(&blocked_call).expect("blocked restricted response should be JSON");
    assert_eq!(blocked_call["error"]["code"], -32601);

    let batch_call = client
        .post(format!("http://{restricted_addr}"))
        .header("content-type", "application/json")
        .body(
            r#"[{"jsonrpc":"2.0","method":"getblockcount","id":6},{"jsonrpc":"2.0","method":"reconsiderblock","params":["00"],"id":7}]"#,
        )
        .send()
        .await
        .expect("mixed restricted batch request should complete")
        .text()
        .await
        .expect("mixed restricted batch response body should be readable");
    let batch_call: serde_json::Value =
        serde_json::from_str(&batch_call).expect("mixed restricted batch response should be JSON");
    let blocked_batch_item = batch_call
        .as_array()
        .expect("batch response should be an array")
        .iter()
        .find(|response| response["id"] == 7)
        .expect("batch response should contain the blocked request");
    assert_eq!(blocked_batch_item["error"]["code"], -32601);

    // Every call in a batch must pass through the per-call middleware, which
    // rewrites the JSON-RPC invalid-params code to the `zcashd` legacy code.
    let invalid_params_batch = client
        .post(format!("http://{restricted_addr}"))
        .header("content-type", "application/json")
        .body(
            r#"[{"jsonrpc":"2.0","method":"getblockcount","id":8},{"jsonrpc":"2.0","method":"getrawtransaction","params":[123],"id":9}]"#,
        )
        .send()
        .await
        .expect("invalid params batch request should complete")
        .text()
        .await
        .expect("invalid params batch response body should be readable");
    let invalid_params_batch: serde_json::Value = serde_json::from_str(&invalid_params_batch)
        .expect("invalid params batch response should be JSON");
    let invalid_params_item = invalid_params_batch
        .as_array()
        .expect("batch response should be an array")
        .iter()
        .find(|response| response["id"] == 9)
        .expect("batch response should contain the invalid params request");
    // `LegacyCode::Misc`
    assert_eq!(invalid_params_item["error"]["code"], -1);

    let unauthenticated_admin = client
        .post(format!("http://{admin_addr}"))
        .header("content-type", "application/json")
        .body(r#"{"jsonrpc":"2.0","method":"rpc.discover","id":3}"#)
        .send()
        .await;
    if let Ok(response) = unauthenticated_admin {
        assert!(
            !response.status().is_success(),
            "the admin listener must reject requests without its cookie"
        );
    }

    let cookie = fs::read_to_string(cookie_dir.path().join("admin.cookie"))
        .expect("admin listener should write its cookie");
    let (username, password) = cookie
        .split_once(':')
        .expect("cookie should contain basic-auth credentials");
    let admin_discovery = client
        .post(format!("http://{admin_addr}"))
        .basic_auth(username, Some(password))
        .header("content-type", "application/json")
        .body(r#"{"jsonrpc":"2.0","method":"rpc.discover","id":4}"#)
        .send()
        .await
        .expect("authenticated admin request should complete")
        .text()
        .await
        .expect("admin discovery response body should be readable");
    let admin_discovery: serde_json::Value =
        serde_json::from_str(&admin_discovery).expect("admin discovery response should be JSON");
    let admin_methods = discovered_methods(&admin_discovery);
    assert!(admin_methods.contains("invalidateblock"));
    assert!(admin_methods.contains("reconsiderblock"));
    assert!(admin_methods.contains("preciousblock"));

    let admin_call = client
        .post(format!("http://{admin_addr}"))
        .basic_auth(username, Some(password))
        .header("content-type", "application/json")
        .body(r#"{"jsonrpc":"2.0","method":"invalidateblock","params":["00"],"id":5}"#)
        .send()
        .await
        .expect("authenticated admin method request should complete")
        .text()
        .await
        .expect("admin method response body should be readable");
    let admin_call: serde_json::Value =
        serde_json::from_str(&admin_call).expect("admin method response should be JSON");
    let admin_error_code = admin_call["error"]["code"]
        .as_i64()
        .expect("an invalid block hash should return an RPC error");
    assert_ne!(
        admin_error_code, -32601,
        "authenticated admin methods must remain registered"
    );

    restricted_task.abort();
    admin_task.abort();
    rpc_queue_task.abort();
}

/// Every RPC listener serves batches at the entry limit, and rejects larger
/// batches before running any of their entries.
#[tokio::test]
async fn oversized_batches_are_rejected_before_dispatch() {
    let _init_guard = zakura_test::init();

    let mut ports = BTreeSet::new();
    while ports.len() < 3 {
        ports.insert(zakura_test::net::random_known_port());
    }
    let [restricted_addr, admin_addr, tls_addr]: [SocketAddr; 3] = ports
        .into_iter()
        .map(|port| SocketAddrV4::new(Ipv4Addr::LOCALHOST, port).into())
        .collect::<Vec<_>>()
        .try_into()
        .expect("three distinct ports were chosen");
    let cookie_dir = tempfile::tempdir().expect("temporary cookie directory should be created");
    let conf = Config {
        listen_addr: Some(restricted_addr),
        admin_listen_addr: Some(admin_addr),
        cookie_dir: cookie_dir.path().to_path_buf(),
        cookie_file_name: "admin.cookie".to_string(),
        enable_cookie_auth: false,
        ..Config::default()
    };
    let tls_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/src/server/tests");
    let tls_conf = Config {
        listen_addr: Some(tls_addr),
        enable_cookie_auth: false,
        tls: Some(TlsConfig {
            cert_file: format!("{tls_dir}/handshake-cert.pem").into(),
            key_file: format!("{tls_dir}/handshake-key.pem").into(),
        }),
        ..Config::default()
    };

    let mut mempool: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let state: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let read_state: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let block_verifier_router: MockService<_, _, _, BoxError> =
        MockService::build().for_unit_tests();
    let (_tx, rx) = watch::channel(None);
    let (rpc_impl, rpc_queue_task) = RpcImpl::new(
        Network::Mainnet,
        Default::default(),
        false,
        "RPC batch limit test",
        "RPC batch limit test",
        Buffer::new(mempool.clone(), 1),
        Buffer::new(state, 1),
        Buffer::new(read_state, 1),
        Buffer::new(block_verifier_router, 1),
        MockSyncStatus::default(),
        NoChainTip,
        MockAddressBookPeers::default(),
        rx,
        None,
    );

    crate::indexer::server::install_tls_crypto_provider();
    let restricted_task = RpcServer::start(rpc_impl.clone(), conf.clone())
        .await
        .expect("restricted RPC listener should start");
    let admin_task = RpcServer::start_admin(rpc_impl.clone(), conf)
        .await
        .expect("admin RPC listener should start");
    let tls_task = RpcServer::start(rpc_impl, tls_conf)
        .await
        .expect("TLS RPC listener should start");

    let cookie = fs::read_to_string(cookie_dir.path().join("admin.cookie"))
        .expect("admin listener should write its cookie");
    let (username, password) = cookie
        .split_once(':')
        .expect("cookie should contain basic-auth credentials");
    let client = Client::builder()
        .tls_certs_only([
            reqwest::Certificate::from_pem(include_bytes!("handshake-cert.pem"))
                .expect("the test certificate should parse"),
        ])
        .resolve("localhost", tls_addr)
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("test HTTP client should build");
    let listeners = [
        client.post(format!("http://{restricted_addr}")),
        client
            .post(format!("http://{admin_addr}"))
            .basic_auth(username, Some(password)),
        client.post(format!("https://localhost:{}/", tls_addr.port())),
    ];

    let limit = MAX_RPC_BATCH_REQUESTS;
    let invalid_entries = vec!["0"; limit].join(",");
    let full_batch = format!("[{invalid_entries}]");
    let oversized_batch =
        format!(r#"[{{"jsonrpc":"2.0","method":"getrawmempool","id":1}},{invalid_entries}]"#);

    for listener in listeners {
        let send = |body: &str| {
            let request = listener
                .try_clone()
                .expect("a request with a string body can be cloned")
                .header("content-type", "application/json")
                .body(body.to_owned());
            async move {
                let body = request
                    .send()
                    .await
                    .expect("batch request should complete")
                    .text()
                    .await
                    .expect("batch response body should be readable");
                serde_json::from_str::<serde_json::Value>(&body)
                    .expect("batch response should be JSON")
            }
        };

        let full = send(&full_batch).await;
        let full = full
            .as_array()
            .expect("a batch at the limit should get one response per entry");
        assert_eq!(full.len(), limit);
        assert!(full
            .iter()
            .all(|response| response["error"]["code"] == -32600));

        let oversized = send(&oversized_batch).await;
        assert_eq!(oversized["id"], serde_json::Value::Null);
        assert_eq!(oversized["error"]["code"], -32010);
        assert_eq!(
            oversized["error"]["data"],
            format!("Exceeded max limit of {limit}")
        );
    }

    mempool.expect_no_requests().await;

    restricted_task.abort();
    admin_task.abort();
    tls_task.abort();
    rpc_queue_task.abort();
}

/// Returns method names from an `rpc.discover` JSON-RPC response.
fn discovered_methods(response: &serde_json::Value) -> BTreeSet<&str> {
    response["result"]["methods"]
        .as_array()
        .expect("discovery response should contain a method array")
        .iter()
        .map(|method| {
            method["name"]
                .as_str()
                .expect("each discovered method should have a name")
        })
        .collect()
}
