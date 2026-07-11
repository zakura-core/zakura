//! Tests that `getpeerinfo` RPC method responds with info about at least 1 peer.

use color_eyre::eyre::{eyre, Context, Result};

use zakura_chain::parameters::Network;
use zakura_node_services::rpc_client::RpcRequestClient;
use zakura_rpc::client::PeerInfo;

use crate::common::{
    launch::{can_spawn_zakurad_for_test_type, spawn_zakurad_for_rpc},
    test_type::TestType,
};

pub(crate) async fn run() -> Result<()> {
    let _init_guard = zakura_test::init();

    let test_type = TestType::LaunchWithEmptyState {
        launches_lightwalletd: false,
    };
    let test_name = "get_peer_info_test";
    let network = Network::Mainnet;

    // Skip the test unless the user specifically asked for it and there is a zakurad_state_path
    if !can_spawn_zakurad_for_test_type(test_name, test_type, true) {
        return Ok(());
    }

    tracing::info!(?network, "running getpeerinfo test using zakurad",);

    let (mut zakurad, zakura_rpc_address) =
        spawn_zakurad_for_rpc(network, test_name, test_type, true)?
            .expect("Already checked zebra state path with can_spawn_zakurad_for_test_type");

    let rpc_address = zakura_rpc_address.expect("getpeerinfo test must have RPC port");

    // Wait until port is open.
    zakurad.expect_stdout_line_matches(format!("Opened RPC endpoint at {rpc_address}"))?;

    tracing::info!(?rpc_address, "zakurad opened its RPC port",);

    // call `getpeerinfo` RPC method
    let peer_info_result: Vec<PeerInfo> = RpcRequestClient::new(rpc_address)
        .json_result_from_call("getpeerinfo", "[]")
        .await
        .map_err(|err| eyre!(err))?;

    assert!(
        !peer_info_result.is_empty(),
        "getpeerinfo should return info for at least 1 peer"
    );

    zakurad.kill(false)?;

    let output = zakurad.wait_with_output()?;
    let output = output.assert_failure()?;

    // [Note on port conflict](#Note on port conflict)
    output
        .assert_was_killed()
        .wrap_err("Possible port conflict. Are there other acceptance tests running?")?;

    Ok(())
}
