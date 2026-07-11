//! Tests for checking that Zebra produces valid coinbase transactions.

use std::sync::Arc;

use color_eyre::eyre::{self, Context};
use futures::future::try_join_all;
use strum::IntoEnumIterator;

use zakura_chain::{
    amount::Amount,
    block::Height,
    parameters::{
        subsidy::FundingStreamReceiver,
        testnet::{
            ConfiguredActivationHeights, ConfiguredFundingStreamRecipient,
            ConfiguredFundingStreams, ConfiguredLockboxDisbursement, RegtestParameters,
        },
        Network,
    },
    primitives::byte_array::increment_big_endian,
};
use zakura_consensus::difficulty_is_valid;
use zakura_node_services::rpc_client::RpcRequestClient;
use zakura_rpc::{config::mining::MinerAddressType, server::OPENED_RPC_ENDPOINT_MSG};
use zakura_test::args;
use zakurad::components::With;

use super::{
    config::{os_assigned_rpc_port_config, read_listen_addr_from_logs, testdir},
    launch::{ZakuradTestDirExt, LAUNCH_DELAY},
    regtest::MiningRpcMethods,
};

/// Tests that Zebra can mine blocks with valid coinbase transactions on Regtest.
pub(crate) async fn regtest_coinbase() -> eyre::Result<()> {
    async fn regtest_coinbase(addr_type: MinerAddressType) -> eyre::Result<()> {
        let _init_guard = zakura_test::init();

        let net = Network::new_regtest(RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                // Current coinbase construction can create Orchard outputs for
                // unified miner addresses, so use the fixed Orchard circuit.
                nu6_2: Some(1),
                ..Default::default()
            },
            funding_streams: Some(vec![ConfiguredFundingStreams {
                height_range: Some(Height(1)..Height(100)),
                recipients: Some(vec![ConfiguredFundingStreamRecipient {
                    receiver: FundingStreamReceiver::Deferred,
                    numerator: 1,
                    addresses: None,
                }]),
            }]),
            lockbox_disbursements: Some(vec![ConfiguredLockboxDisbursement {
                address: "t2RnBRiqrN1nW4ecZs1Fj3WWjNdnSs4kiX8".to_string(),
                amount: Amount::new(6_250_000),
            }]),
            ..Default::default()
        });

        let mut config = os_assigned_rpc_port_config(false, &net)?.with(addr_type);
        config.mempool.debug_enable_at_height = Some(0);

        let mut zakurad = testdir()?
            .with_config(&mut config)?
            .spawn_child(args!["start"])?;

        tokio::time::sleep(LAUNCH_DELAY).await;

        let client = RpcRequestClient::new(read_listen_addr_from_logs(
            &mut zakurad,
            OPENED_RPC_ENDPOINT_MSG,
        )?);

        for _ in 0..2 {
            let (mut block, height) = client.block_from_template(&net).await?;

            // If the network requires PoW, find a valid nonce.
            if !net.disable_pow() {
                let header = Arc::make_mut(&mut block.header);

                loop {
                    let hash = header.hash();

                    if difficulty_is_valid(header, &net, &height, &hash).is_ok() {
                        break;
                    }

                    increment_big_endian(header.nonce.as_mut());
                }
            }

            client.submit_block(block).await?;
        }

        zakurad.kill(false)?;

        zakurad
            .wait_with_output()?
            .assert_failure()?
            .assert_was_killed()
            .wrap_err("possible port conflict with another zakurad instance")
    }

    try_join_all(MinerAddressType::iter().map(regtest_coinbase))
        .await
        .map(|_| ())
}
