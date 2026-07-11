//! Tests that `getpeerinfo` RPC method responds with info about at least 1 peer.

use color_eyre::eyre::{eyre, Result};

use zakura_chain::parameters::Network;
use zakura_node_services::rpc_client::RpcRequestClient;
use zakura_rpc::client::PeerInfo;
use zakura_test::{args, net::random_known_port};

use crate::common::{
    config::testdir,
    launch::{can_spawn_zakurad_for_test_type, ZakuradTestDirExt},
    test_type::TestType,
};

pub(crate) async fn run() -> Result<()> {
    let _init_guard = zakura_test::init();

    let test_type = TestType::LaunchWithEmptyState {
        launches_lightwalletd: false,
    };
    let test_name = "get_peer_info_test";
    let network = Network::Mainnet;

    if !can_spawn_zakurad_for_test_type(test_name, test_type, false) {
        return Ok(());
    }

    let mut peer_config = test_type
        .zakurad_config(test_name, false, None, &network)
        .expect("already checked config")?;
    let peer_address = format!("127.0.0.1:{}", random_known_port()).parse()?;
    peer_config.network.listen_addr = peer_address;

    let (failure_messages, ignore_messages) = test_type.zakurad_failure_messages();
    let mut peer = testdir()?
        .with_exact_config(&peer_config)?
        .spawn_child(args!["start"])?
        .with_timeout(test_type.zakurad_timeout())
        .with_failure_regex_iter(failure_messages, ignore_messages);
    peer.expect_stdout_line_matches(format!("Opened Zcash protocol endpoint at {peer_address}"))?;

    let mut config = peer_config;
    config.network.listen_addr = "127.0.0.1:0".parse()?;
    config.network.initial_mainnet_peers = [peer_address.to_string()].into();
    config.rpc.listen_addr = Some(format!("127.0.0.1:{}", random_known_port()).parse()?);
    let rpc_address = config
        .rpc
        .listen_addr
        .expect("getpeerinfo test config has an RPC port");

    let (failure_messages, ignore_messages) = test_type.zakurad_failure_messages();
    let mut zakurad = testdir()?
        .with_exact_config(&config)?
        .spawn_child(args!["start"])?
        .with_timeout(test_type.zakurad_timeout())
        .with_failure_regex_iter(failure_messages, ignore_messages);
    zakurad.expect_stdout_line_matches_all_unordered([
        format!("Opened RPC endpoint at {rpc_address}"),
        "finished connecting to initial seed and disk cache peers".to_string(),
    ])?;

    let peer_info: Vec<PeerInfo> = RpcRequestClient::new(rpc_address)
        .json_result_from_call("getpeerinfo", "[]")
        .await
        .map_err(|err| eyre!(err))?;

    assert_eq!(
        peer_info.len(),
        1,
        "getpeerinfo should return the configured local peer"
    );

    zakurad.kill_and_return_output(false)?;
    peer.kill_and_return_output(false)?;

    Ok(())
}
