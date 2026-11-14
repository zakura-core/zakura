//! Tests for restricted unauthenticated and authenticated RPC method surfaces.

use crate::tests::test_rpc::TestRpcImpl;
use std::{
    collections::BTreeSet,
    fs,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    time::Duration,
};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use tokio::sync::watch;
use tower::buffer::Buffer;

use crate::{
    config::rpc::{Config, TlsConfig},
    methods::{RpcAccess, RpcSurface, METHODS, RPC_METHOD_ACCESS},
    server::{enabled_rpc_methods, primary_rpc_surface, MAX_RPC_BATCH_REQUESTS},
};

use super::http_client::HttpConnection;
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
fn primary_surface_respects_authentication_network_and_admin_opt_in() {
    for network in [Network::Mainnet, Network::new_default_testnet()] {
        for (auth, admin, expected) in [
            (false, false, RpcSurface::Restricted),
            (false, true, RpcSurface::WithAdmin),
            (true, false, RpcSurface::Full),
            (true, true, RpcSurface::Full),
        ] {
            assert_eq!(primary_rpc_surface(&network, auth, admin), expected);
        }
    }
    let regtest = Network::new_regtest(Default::default());
    for (auth, admin) in [(false, false), (false, true), (true, false), (true, true)] {
        assert_eq!(primary_rpc_surface(&regtest, auth, admin), RpcSurface::Full);
    }
}

#[tokio::test]
async fn segmented_listeners_enforce_methods_and_cookie_auth() {
    assert_segmented_listener_access(false).await;
}

#[tokio::test]
async fn unauthenticated_admin_opt_in_preserves_admin_listener_authentication() {
    assert_segmented_listener_access(true).await;
}

/// Checks discovery, method dispatch, batches, and admin listener authentication.
async fn assert_segmented_listener_access(allow_unauthenticated_admin: bool) {
    let _init_guard = zakura_test::init();

    let primary_port = zakura_test::net::random_known_port();
    let admin_port = loop {
        let port = zakura_test::net::random_known_port();
        if port != primary_port {
            break port;
        }
    };
    let primary_addr: SocketAddr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, primary_port).into();
    let admin_addr: SocketAddr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, admin_port).into();
    let cookie_dir = tempfile::tempdir().expect("temporary cookie directory should be created");
    let conf = Config {
        listen_addr: Some(primary_addr),
        admin_listen_addr: Some(admin_addr),
        cookie_dir: cookie_dir.path().to_path_buf(),
        cookie_file_name: "admin.cookie".to_string(),
        enable_cookie_auth: false,
        allow_unauthenticated_admin,
        ..Config::default()
    };

    let mempool: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let state: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let read_state: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let block_verifier_router: MockService<_, _, _, BoxError> =
        MockService::build().for_unit_tests();
    let (_tx, rx) = watch::channel(None);
    let (rpc_impl, rpc_queue_task) = TestRpcImpl::new_for_tests(
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

    let primary_task = RpcServer::start(rpc_impl.clone(), conf.clone())
        .await
        .expect("primary RPC listener should start");
    assert!(
        !cookie_dir.path().join("admin.cookie").exists(),
        "the unauthenticated primary listener must not create a cookie"
    );
    let admin_task = RpcServer::start_admin(rpc_impl, conf)
        .await
        .expect("admin RPC listener should start");

    let primary_discovery = post(
        primary_addr,
        r#"{"jsonrpc":"2.0","method":"rpc.discover","id":1}"#,
        None,
    )
    .await
    .expect("primary discovery request should complete")
    .1;
    let primary_discovery: serde_json::Value = serde_json::from_str(&primary_discovery)
        .expect("primary discovery response should be JSON");
    let primary_methods = discovered_methods(&primary_discovery);
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
            primary_methods.contains(method),
            "existing unauthenticated integrations require {method}"
        );
    }
    let expected_admin_error = if allow_unauthenticated_admin {
        // A malformed hash reaches the handler and returns InvalidParameter.
        -8
    } else {
        // Admin methods are absent by default (MethodNotFound).
        -32601
    };
    for method in ["invalidateblock", "reconsiderblock", "preciousblock"] {
        assert_eq!(
            primary_methods.contains(method),
            allow_unauthenticated_admin
        );
        let body = serde_json::json!({
            "jsonrpc": "2.0", "method": method, "params": ["00"], "id": 2,
        })
        .to_string();
        let (status, response) = post(primary_addr, &body, None)
            .await
            .expect("unauthenticated admin request should complete");
        assert!(status.is_success());
        let response: serde_json::Value =
            serde_json::from_str(&response).expect("admin response should be JSON");
        assert_eq!(response["error"]["code"], expected_admin_error);
    }
    // A web page can send this cross-origin `POST` without a CORS preflight.
    let (status, _) = tokio::time::timeout(Duration::from_secs(5), async {
        HttpConnection::connect(primary_addr)
            .await?
            .post_from_browser(
                r#"{"jsonrpc":"2.0","method":"getblockcount","id":11}"#,
                "https://attacker.example",
            )
            .await
    })
    .await
    .expect("browser request should finish before the timeout")
    .expect("browser request should complete");
    if allow_unauthenticated_admin {
        assert_eq!(
            status,
            hyper::StatusCode::FORBIDDEN,
            "a listener with unauthenticated admin methods must reject browser requests"
        );
    } else {
        assert!(
            status.is_success(),
            "the restricted listener keeps its existing browser behavior"
        );
    }

    for method in ["stop", "generate", "addnode"] {
        assert!(!primary_methods.contains(method));
        let body = serde_json::json!({"jsonrpc": "2.0", "method": method, "id": 10}).to_string();
        let (_, response) = post(primary_addr, &body, None)
            .await
            .expect("unavailable test-control request should complete");
        let response: serde_json::Value =
            serde_json::from_str(&response).expect("test-control response should be JSON");
        assert_eq!(response["error"]["code"], -32601);
    }

    let batch_call = post(primary_addr, r#"[{"jsonrpc":"2.0","method":"getblockcount","id":6},{"jsonrpc":"2.0","method":"reconsiderblock","params":["00"],"id":7}]"#, None)
        .await
        .expect("mixed primary batch request should complete")
        .1;
    let batch_call: serde_json::Value =
        serde_json::from_str(&batch_call).expect("mixed primary batch response should be JSON");
    let admin_batch_item = batch_call
        .as_array()
        .expect("batch response should be an array")
        .iter()
        .find(|response| response["id"] == 7)
        .expect("batch response should contain the admin request");
    assert_eq!(admin_batch_item["error"]["code"], expected_admin_error);

    // Every call in a batch must pass through the per-call middleware, which
    // rewrites the JSON-RPC invalid-params code to the `zcashd` legacy code.
    let invalid_params_batch = post(primary_addr, r#"[{"jsonrpc":"2.0","method":"getblockcount","id":8},{"jsonrpc":"2.0","method":"getrawtransaction","params":[123],"id":9}]"#, None)
        .await
        .expect("invalid params batch request should complete")
        .1;
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

    let unauthenticated_admin = post(
        admin_addr,
        r#"{"jsonrpc":"2.0","method":"rpc.discover","id":3}"#,
        None,
    )
    .await;
    if let Ok((status, _)) = unauthenticated_admin {
        assert!(
            !status.is_success(),
            "the admin listener must reject requests without its cookie"
        );
    }

    let cookie = fs::read_to_string(cookie_dir.path().join("admin.cookie"))
        .expect("admin listener should write its cookie");
    let (username, password) = cookie
        .split_once(':')
        .expect("cookie should contain basic-auth credentials");
    let authorization = format!(
        "Basic {}",
        STANDARD.encode(format!("{username}:{password}"))
    );
    let admin_discovery = post(
        admin_addr,
        r#"{"jsonrpc":"2.0","method":"rpc.discover","id":4}"#,
        Some(&authorization),
    )
    .await
    .expect("authenticated admin request should complete")
    .1;
    let admin_discovery: serde_json::Value =
        serde_json::from_str(&admin_discovery).expect("admin discovery response should be JSON");
    let admin_methods = discovered_methods(&admin_discovery);
    assert!(admin_methods.contains("invalidateblock"));
    assert!(admin_methods.contains("reconsiderblock"));
    assert!(admin_methods.contains("preciousblock"));

    let admin_call = post(
        admin_addr,
        r#"{"jsonrpc":"2.0","method":"invalidateblock","params":["00"],"id":5}"#,
        Some(&authorization),
    )
    .await
    .expect("authenticated admin method request should complete")
    .1;
    let admin_call: serde_json::Value =
        serde_json::from_str(&admin_call).expect("admin method response should be JSON");
    let admin_error_code = admin_call["error"]["code"]
        .as_i64()
        .expect("an invalid block hash should return an RPC error");
    assert_ne!(
        admin_error_code, -32601,
        "authenticated admin methods must remain registered"
    );

    primary_task.abort();
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
    let (rpc_impl, rpc_queue_task) = TestRpcImpl::new_for_tests(
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
    let authorization = format!(
        "Basic {}",
        STANDARD.encode(format!("{username}:{password}"))
    );
    // Each listener's address, whether it uses TLS, and its `Authorization` header.
    let listeners = [
        (restricted_addr, false, None),
        (admin_addr, false, Some(authorization.as_str())),
        (tls_addr, true, None),
    ];

    let limit = MAX_RPC_BATCH_REQUESTS;
    let invalid_entries = vec!["0"; limit].join(",");
    let full_batch = format!("[{invalid_entries}]");
    let oversized_batch =
        format!(r#"[{{"jsonrpc":"2.0","method":"getrawmempool","id":1}},{invalid_entries}]"#);

    for (address, tls, authorization) in listeners {
        let send = |body: &str| {
            let body = body.to_owned();
            async move {
                let connection = if tls {
                    HttpConnection::connect_tls(address).await
                } else {
                    HttpConnection::connect(address).await
                };
                let (_, body) = connection
                    .expect("the listener accepts HTTP connections")
                    .post_json(body, authorization)
                    .await
                    .expect("batch request should complete");
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

/// Sends `body` to the plain HTTP listener at `address` on a new connection, with an
/// `Authorization` header if it is set, and returns the response status and body.
async fn post(
    address: SocketAddr,
    body: &str,
    authorization: Option<&str>,
) -> Result<(hyper::StatusCode, String), tower::BoxError> {
    tokio::time::timeout(Duration::from_secs(5), async {
        Ok(HttpConnection::connect(address)
            .await?
            .post_json(body, authorization)
            .await?)
    })
    .await?
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
