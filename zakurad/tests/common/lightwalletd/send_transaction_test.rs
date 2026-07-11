//! Test sending transactions using a lightwalletd instance connected to a zakurad instance.
//!
//! This test requires a cached chain state that is partially synchronized close to the
//! network chain tip height. It will finish the sync and update the cached chain state.
//!
//! After finishing the sync, it will get the first 20 blocks in the non-finalized state
//! (past the MAX_BLOCK_REORG_HEIGHT) via getblock rpc calls, shuts down the zakurad instance
//! so that the retrieved blocks aren't finalized into the cached state, and get the finalized
//! tip height of the updated cached state.
//!
//! The transactions to use to send are obtained from those blocks that are above the finalized
//! tip height of the updated cached state.
//!
//! The zakurad instance connected to lightwalletd uses the cached state and does not connect to any
//! external peers, which prevents it from downloading the blocks from where the test transactions
//! were obtained. This is to ensure that zebra does not reject the transactions because they have
//! already been seen in a block.

use std::{cmp::min, collections::HashSet, sync::Arc};
use tower::BoxError;

use color_eyre::eyre::{eyre, Result};

use zakura_chain::{
    block::Block,
    parameters::Network::*,
    serialization::{BytesInDisplayOrder, ZcashSerialize},
    transaction::{self, Transaction},
};
use zakura_node_services::rpc_client::RpcRequestClient;
use zakura_rpc::queue::CHANNEL_AND_QUEUE_CAPACITY;
use zakurad::components::mempool::downloads::MAX_INBOUND_CONCURRENCY;

use crate::common::{
    cached_state::future_blocks,
    launch::{can_spawn_zakurad_for_test_type, spawn_zakurad_for_rpc},
    lightwalletd::{
        can_spawn_lightwalletd_for_rpc, spawn_lightwalletd_for_rpc,
        sync::wait_for_zakurad_and_lightwalletd_sync,
        wallet_grpc::{
            self, compact_tx_streamer_client::CompactTxStreamerClient, connect_to_lightwalletd,
            Empty, Exclude,
        },
    },
    regtest::MiningRpcMethods,
    test_type::TestType::*,
};

/// The maximum number of transactions we want to send in the test.
/// This avoids filling the mempool queue and generating errors.
///
/// TODO: replace with a const when `min()` stabilises as a const function:
///       https://github.com/rust-lang/rust/issues/92391
fn max_sent_transactions() -> usize {
    min(CHANNEL_AND_QUEUE_CAPACITY, MAX_INBOUND_CONCURRENCY) / 2
}

/// Number of blocks past the finalized to load transactions from.
const MAX_NUM_FUTURE_BLOCKS: u32 = 50;

/// The test entry point.
pub async fn run() -> Result<()> {
    let _init_guard = zakura_test::init();

    // We want a zebra state dir and a lightwalletd data dir in place,
    // so `UpdateCachedState` can be used as our test type
    let test_type = UpdateCachedState;
    let test_name = "send_transaction_test";
    let network = Mainnet;

    // Skip the test unless the user specifically asked for it
    if !can_spawn_zakurad_for_test_type(test_name, test_type, true) {
        return Ok(());
    }

    if test_type.launches_lightwalletd() && !can_spawn_lightwalletd_for_rpc(test_name, test_type) {
        tracing::info!("skipping test due to missing lightwalletd network or cached state");
        return Ok(());
    }

    let zakurad_state_path = test_type.zakurad_state_path(test_name);
    let zakurad_state_path = match zakurad_state_path {
        Some(zakurad_state_path) => zakurad_state_path,
        None => return Ok(()),
    };

    tracing::info!(
        ?network,
        ?test_type,
        ?zakurad_state_path,
        "running gRPC send transaction test using lightwalletd & zakurad",
    );

    let mut count = 0;
    let blocks: Vec<Block> = future_blocks(&network, test_type, test_name, MAX_NUM_FUTURE_BLOCKS)
        .await?
        .into_iter()
        .take_while(|block| {
            count += block.transactions.len() - 1;
            count <= max_sent_transactions()
        })
        .collect();

    tracing::info!(
        blocks_count = ?blocks.len(),
        partial_sync_path = ?zakurad_state_path,
        "got transactions to send, spawning isolated zakurad...",
    );

    // We run these gRPC tests without a network connection.
    let use_internet_connection = false;

    // Start zakurad with no peers, we want to send transactions without blocks coming in. If `wallet_grpc_test`
    // runs before this test (as it does in `lightwalletd_test_suite`), then we are the most up to date with tip we can.
    let (mut zakurad, zakura_rpc_address) = if let Some(zakurad_and_address) =
        spawn_zakurad_for_rpc(
            network.clone(),
            test_name,
            test_type,
            use_internet_connection,
        )? {
        zakurad_and_address
    } else {
        // Skip the test, we don't have the required cached state
        return Ok(());
    };

    let zakura_rpc_address = zakura_rpc_address.expect("lightwalletd test must have RPC port");

    let zakurad_rpc_client = RpcRequestClient::new(zakura_rpc_address);

    tracing::info!(
        ?test_type,
        ?zakura_rpc_address,
        "spawned isolated zakurad with shorter chain, waiting for zakurad to open its RPC port..."
    );
    zakurad.expect_stdout_line_matches(format!("Opened RPC endpoint at {zakura_rpc_address}"))?;

    tracing::info!(
        ?zakura_rpc_address,
        "zakurad opened its RPC port, spawning lightwalletd...",
    );

    let (lightwalletd, lightwalletd_rpc_port) =
        spawn_lightwalletd_for_rpc(network, test_name, test_type, zakura_rpc_address)?
            .expect("already checked cached state and network requirements");

    tracing::info!(
        ?lightwalletd_rpc_port,
        "spawned lightwalletd connected to zakurad, waiting for them both to sync...",
    );

    let (_lightwalletd, _zakurad) = wait_for_zakurad_and_lightwalletd_sync(
        lightwalletd,
        lightwalletd_rpc_port,
        zakurad,
        zakura_rpc_address,
        test_type,
        // We want to send transactions to the mempool, but we aren't syncing with the network
        true,
        use_internet_connection,
    )?;

    tracing::info!(
        ?lightwalletd_rpc_port,
        "connecting gRPC client to lightwalletd...",
    );

    let mut rpc_client = connect_to_lightwalletd(lightwalletd_rpc_port).await?;

    // Call GetMempoolTx so lightwalletd caches the empty mempool state.
    // This is a workaround for a bug where lightwalletd will skip calling `get_raw_transaction`
    // the first time GetMempoolTx is called because it replaces the cache early and only calls the
    // RPC method for transaction ids that are missing in the old cache as keys.
    // <https://github.com/zcash/lightwalletd/blob/master/frontend/service.go#L495-L502>
    //
    // TODO: Fix this issue in lightwalletd and delete this
    rpc_client
        .get_mempool_tx(Exclude { txid: vec![] })
        .await?
        .into_inner();

    let mut transaction_hashes = HashSet::new();
    let mut has_tx_with_shielded_elements = false;
    let mut counter = 0;

    for block in blocks {
        let (has_shielded_elements, count) = send_transactions_from_block(
            &mut rpc_client,
            &zakurad_rpc_client,
            block.clone(),
            &mut transaction_hashes,
        )
        .await?;

        has_tx_with_shielded_elements |= has_shielded_elements;
        counter += count;

        tracing::info!(
            height = ?block.coinbase_height(),
            "submitting block at height"
        );

        let submit_block_response = zakurad_rpc_client.submit_block(block).await;
        tracing::info!(?submit_block_response, "submitted block");
    }

    // GetMempoolTx: make sure at least one of the transactions were inserted into the mempool.
    assert!(
        !has_tx_with_shielded_elements || counter >= 1,
        "failed to read v4+ transactions with shielded elements \
        from future blocks in mempool via lightwalletd"
    );

    Ok(())
}

/// Sends non-coinbase transactions from a block to the mempool, verifies that the transactions
/// can be found in the mempool via lightwalletd, and commits the block to Zebra's chainstate.
///
/// Returns the zakurad test child that's handling the RPC requests.

#[tracing::instrument(skip_all)]
async fn send_transactions_from_block(
    rpc_client: &mut CompactTxStreamerClient<tonic::transport::Channel>,
    zakurad_rpc_client: &RpcRequestClient,
    block: Block,
    transaction_hashes: &mut HashSet<transaction::Hash>,
) -> Result<(bool, usize)> {
    // Lightwalletd won't call `get_raw_mempool` again until 2 seconds after the last call:
    // <https://github.com/zcash/lightwalletd/blob/master/frontend/service.go#L482>
    //
    // So we need to wait much longer than that here.
    let sleep_until_lwd_last_mempool_refresh =
        tokio::time::sleep(std::time::Duration::from_secs(4));

    let transactions: Vec<_> = block
        .transactions
        .iter()
        .filter(|tx| !tx.is_coinbase())
        .collect();

    if transactions.is_empty() {
        return Ok((false, 0));
    }

    transaction_hashes.extend(transactions.iter().map(|tx| tx.hash()));

    tracing::info!(
        transaction_count = ?transactions.len(),
        ?transaction_hashes,
        "connected gRPC client to lightwalletd, sending transactions...",
    );

    let mut has_tx_with_shielded_elements = false;
    for &transaction in &transactions {
        let transaction_hash = transaction.hash();

        // See <https://github.com/zcash/lightwalletd/blob/master/parser/transaction.go#L367>
        has_tx_with_shielded_elements |= transaction.version() >= 4
            && (transaction.has_shielded_inputs() || transaction.has_shielded_outputs());

        let expected_response = wallet_grpc::SendResponse {
            error_code: 0,
            error_message: format!("\"{transaction_hash}\""),
        };

        tracing::info!(?transaction_hash, "sending transaction...");

        let request = prepare_send_transaction_request(transaction.clone());

        match rpc_client.send_transaction(request).await {
            Ok(response) => assert_eq!(response.into_inner(), expected_response),
            Err(err) => {
                tracing::warn!(?err, "failed to send transaction");
                let send_tx_rsp = zakurad_rpc_client
                    .send_transaction(transaction)
                    .await
                    .map_err(|e| eyre!(e));

                tracing::warn!(?send_tx_rsp, "failed to send tx twice");
            }
        };
    }

    // Wait for more transactions to verify, `GetMempoolTx` only returns txs where tx.HasShieldedElements()
    // <https://github.com/zcash/lightwalletd/blob/master/frontend/service.go#L537>
    sleep_until_lwd_last_mempool_refresh.await;

    tracing::info!("calling GetMempoolTx gRPC to fetch transactions...");
    let mut transactions_stream = rpc_client
        .get_mempool_tx(Exclude { txid: vec![] })
        .await?
        .into_inner();

    tracing::info!("checking the mempool contains some of the sent transactions...");
    let mut counter = 0;
    while let Some(tx) = transactions_stream.message().await? {
        let hash: [u8; 32] = tx.hash.clone().try_into().expect("hash is correct length");
        let hash = transaction::Hash::from_bytes_in_display_order(&hash);

        assert!(
            transaction_hashes.contains(&hash),
            "unexpected transaction {hash:?}\n\
             in isolated mempool: {tx:?}",
        );

        counter += 1;
    }

    tracing::info!("calling GetMempoolStream gRPC to fetch transactions...");
    let mut transaction_stream = rpc_client.get_mempool_stream(Empty {}).await?.into_inner();

    let mut _counter = 0;
    while let Some(_tx) = transaction_stream.message().await? {
        // TODO: check tx.data or tx.height here?
        _counter += 1;
    }

    Ok((has_tx_with_shielded_elements, counter))
}

/// Prepare a request to send to lightwalletd that contains a transaction to be sent.
fn prepare_send_transaction_request(transaction: Arc<Transaction>) -> wallet_grpc::RawTransaction {
    let transaction_bytes = transaction.zcash_serialize_to_vec().unwrap();

    wallet_grpc::RawTransaction {
        data: transaction_bytes,
        height: 0,
    }
}

trait SendTransactionMethod {
    async fn send_transaction(
        &self,
        transaction: &Arc<Transaction>,
    ) -> Result<zakura_rpc::methods::SendRawTransactionResponse, BoxError>;
}

impl SendTransactionMethod for RpcRequestClient {
    async fn send_transaction(
        &self,
        transaction: &Arc<Transaction>,
    ) -> Result<zakura_rpc::methods::SendRawTransactionResponse, BoxError> {
        let tx_data = hex::encode(transaction.zcash_serialize_to_vec()?);
        self.json_result_from_call("sendrawtransaction", format!(r#"["{tx_data}"]"#))
            .await
    }
}
