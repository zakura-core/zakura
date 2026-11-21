//! Tests for checking that Zebra produces valid coinbase transactions.

use std::sync::Arc;

use eyre::{self, Context};

use zakura_chain::{
    amount::Amount,
    block::{Block, Height},
    miner_mesh::MinerMeshCommitment,
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
use zakurad::{components::With, config::ZakuradConfig};

use super::{
    config::{os_assigned_rpc_port_config, read_listen_addr_from_logs, testdir},
    launch::{ZakuradTestDirExt, EXTENDED_LAUNCH_DELAY},
    regtest::MiningRpcMethods,
};

/// Tests shielded coinbase construction through a live Regtest node.
///
/// A node configured with a Unified mining address must return a
/// shielded coinbase through `getblocktemplate`. The resulting block must be
/// accepted by the production `submitblock`, consensus, and state path.
///
/// Detailed address routing and upgrade-matrix behavior is tested by
/// `shielded_coinbase_paths`.
pub(crate) async fn regtest_coinbase() -> eyre::Result<()> {
    async fn regtest_coinbase(addr_type: MinerAddressType) -> eyre::Result<()> {
        let _init_guard = zakura_test::init();

        let net = Network::new_regtest(RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                // Coinbase construction pays unified miner addresses through
                // the Ironwood pool, which activates at NU6.3.
                nu6_3: Some(1),
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

        let config = os_assigned_rpc_port_config(false, &net)?.with(addr_type);
        mine_template_block(&net, config).await?;

        Ok(())
    }

    regtest_coinbase(MinerAddressType::Unified).await?;

    Ok(())
}

/// Tests that a node told to announce its gateway names its own v2 node ID in the coinbase.
///
/// The block must be accepted by the production `submitblock`, consensus, and state path,
/// with exactly one commitment in its coinbase.
pub(crate) async fn regtest_coinbase_names_the_gateway() -> eyre::Result<()> {
    // RFC 8032 section 7.1, test 1: a secret key, and its public key, which is the node ID.
    const NODE_SECRET_KEY: &str =
        "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60";
    const NODE_ID: &str = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";

    let _init_guard = zakura_test::init();
    let net = Network::new_regtest(Default::default());

    let identity_dir = tempfile::tempdir()?;
    std::fs::write(
        identity_dir.path().join("regtest.zakura-iroh-secret-key"),
        NODE_SECRET_KEY,
    )?;

    let mut config = os_assigned_rpc_port_config(false, &net)?.with(MinerAddressType::Transparent);
    config.mining.miner_mesh = true;
    config.network.p2p_stack = zakura_network::P2pStack::Dual;
    config.network.identity_dir = identity_dir.path().to_path_buf();
    config.network.zakura.listen_addr = Some("127.0.0.1:0".parse()?);
    config.network.zakura.bootstrap_peers = Vec::new();

    let block = mine_template_block(&net, config).await?;

    let gateway = MinerMeshCommitment::from_coinbase_outputs(block.transactions[0].outputs())
        .map(|commitment| hex::encode(commitment.gateway_id()));
    assert_eq!(gateway.as_deref(), Some(NODE_ID));

    Ok(())
}

/// Starts a node with `config`, builds a block from its template, and submits it.
///
/// Returns the block once the production `submitblock`, consensus, and state path has
/// accepted it.
async fn mine_template_block(net: &Network, mut config: ZakuradConfig) -> eyre::Result<Block> {
    config.mempool.debug_enable_at_height = Some(0);

    let mut zakurad = testdir()?
        .with_config(&mut config)?
        .spawn_child(args!["start"])?
        .with_timeout(EXTENDED_LAUNCH_DELAY);

    let client = RpcRequestClient::new(read_listen_addr_from_logs(
        &mut zakurad,
        OPENED_RPC_ENDPOINT_MSG,
    )?);
    zakurad.expect_stdout_line_matches("activating mempool")?;

    let (mut block, height) = client.block_from_template(net).await?;

    // If the network requires PoW, find a valid nonce.
    if !net.disable_pow() {
        let header = Arc::make_mut(&mut block.header);

        loop {
            let hash = header.hash();

            if difficulty_is_valid(header, net, &height, &hash).is_ok() {
                break;
            }

            increment_big_endian(header.nonce.as_mut());
        }
    }

    client.submit_block(block.clone()).await?;

    zakurad.kill(false)?;

    zakurad
        .wait_with_output()?
        .assert_failure()?
        .assert_was_killed()
        .wrap_err("possible port conflict with another zakurad instance")?;

    Ok(block)
}
