//! Test submitblock RPC method.
//!
//! This test requires a cached chain state that is partially synchronized close to the
//! network chain tip height. It will finish the sync and update the cached chain state.
//!
//! After finishing the sync, it will get the first few blocks in the non-finalized state
//! (past the MAX_BLOCK_REORG_HEIGHT) via getblock rpc calls, get the finalized tip height
//! of the updated cached state, restart zebra without peers, and submit blocks above the
//! finalized tip height.

use color_eyre::eyre::{Context, Result};

use zakura_chain::parameters::Network;
use zakura_node_services::rpc_client::RpcRequestClient;

use crate::common::{
    cached_state::raw_future_blocks,
    launch::{can_spawn_zakurad_for_test_type, spawn_zakurad_for_rpc},
    test_type::TestType,
};

/// Number of blocks past the finalized to retrieve and submit.
const MAX_NUM_FUTURE_BLOCKS: u32 = 3;

pub(crate) async fn run() -> Result<()> {
    let _init_guard = zakura_test::init();

    // We want a zebra state dir in place,
    let test_type = TestType::UpdateZebraCachedStateWithRpc;
    let test_name = "submit_block_test";
    let network = Network::Mainnet;

    // Skip the test unless the user specifically asked for it and there is a zakurad_state_path
    if !can_spawn_zakurad_for_test_type(test_name, test_type, true) {
        return Ok(());
    }

    tracing::info!(
        ?network,
        ?test_type,
        "running submitblock test using zakurad",
    );

    let raw_blocks: Vec<String> =
        raw_future_blocks(&network, test_type, test_name, MAX_NUM_FUTURE_BLOCKS).await?;

    tracing::info!("got raw future blocks, spawning isolated zakurad...",);

    // Start zakurad with no peers, we run the rest of the submitblock test without syncing.
    let should_sync = false;
    let (mut zakurad, zakura_rpc_address) =
        spawn_zakurad_for_rpc(network, test_name, test_type, should_sync)?
            .expect("already checked Zakura state path with can_spawn_zakurad_for_test_type");

    let rpc_address = zakura_rpc_address.expect("submitblock test must have RPC port");

    tracing::info!(
        ?test_type,
        ?rpc_address,
        "spawned isolated zakurad with shorter chain, waiting for zakurad to open its RPC port..."
    );
    zakurad.expect_stdout_line_matches(format!("Opened RPC endpoint at {rpc_address}"))?;

    tracing::info!(?rpc_address, "zakurad opened its RPC port",);

    // Create an http client
    let client = RpcRequestClient::new(rpc_address);

    for raw_block in raw_blocks {
        let res = client
            .call("submitblock", format!(r#"["{raw_block}"]"#))
            .await?;

        assert!(res.status().is_success());
        let res_text = res.text().await?;

        // Test rpc endpoint response
        assert!(
            res_text.contains(r#""result":null"#),
            "unexpected response from submitblock RPC, should be null, was: {res_text}"
        );
    }

    zakurad.kill(false)?;

    let output = zakurad.wait_with_output()?;
    let output = output.assert_failure()?;

    // [Note on port conflict](#Note on port conflict)
    output
        .assert_was_killed()
        .wrap_err("Possible port conflict. Are there other acceptance tests running?")?;

    Ok(())
}
