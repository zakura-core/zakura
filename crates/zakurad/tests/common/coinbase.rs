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
        Network, NetworkUpgrade,
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

/// Tests that a node told to announce its gateway names its own v2 node ID in the coinbase,
/// both beside a transparent reward and beside a shielded reward with NU7 active.
///
/// Each block must be accepted by the production `submitblock`, consensus, and state path,
/// with exactly one commitment in its coinbase, which names the gateway by the first 20 bytes
/// of the node ID.
pub(crate) async fn regtest_coinbase_names_the_gateway() -> eyre::Result<()> {
    let _init_guard = zakura_test::init();

    announcing_block(
        &Network::new_regtest(Default::default()),
        MinerAddressType::Transparent,
    )
    .await?;

    let block = announcing_block(&nu7_regtest(), MinerAddressType::Unified).await?;
    assert_eq!(
        block.transactions[0].network_upgrade(),
        Some(NetworkUpgrade::Nu7),
        "the coinbase is built for NU7, which is active from height 1",
    );
    assert!(
        block.transactions[0].ironwood_shielded_data().is_some(),
        "a unified miner address pays the reward into the Ironwood pool",
    );

    Ok(())
}

/// Mines one block on `net` from a node that announces its gateway and pays `addr_type`,
/// checks the gateway ID in the coinbase, and returns the block.
async fn announcing_block(net: &Network, addr_type: MinerAddressType) -> eyre::Result<Block> {
    // RFC 8032 section 7.1, test 1, as the node's secret key. The node ID is the hash of
    // the key list that seed derives: its Ed25519 public key and its ML-DSA-65 key.
    const NODE_SECRET_KEY: &str =
        "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60";
    const NODE_ID: &str = "12ffc5e1bb88a852797a9b981b411a1b6a87f73aa3758e967d3c0d2bada0cddd";
    // The first 20 of the node ID's 32 bytes.
    const GATEWAY_ID: &str = "12ffc5e1bb88a852797a9b981b411a1b6a87f73a";
    assert!(NODE_ID.starts_with(GATEWAY_ID));

    let identity_dir = tempfile::tempdir()?;
    std::fs::write(
        identity_dir.path().join("regtest.zakura-iroh-secret-key"),
        NODE_SECRET_KEY,
    )?;

    let mut config = os_assigned_rpc_port_config(false, net)?.with(addr_type);
    config.mining.miner_mesh = true;
    config.network.p2p_stack = zakura_network::P2pStack::Dual;
    config.network.identity_dir = identity_dir.path().to_path_buf();
    config.network.zakura.listen_addr = Some("127.0.0.1:0".parse()?);
    config.network.zakura.bootstrap_peers = Vec::new();

    let block = mine_template_block(net, config).await?;

    let gateway = MinerMeshCommitment::from_coinbase_outputs(block.transactions[0].outputs())
        .map(|commitment| commitment.gateway_id().to_string());
    assert_eq!(gateway.as_deref(), Some(GATEWAY_ID));

    Ok(block)
}

/// A Regtest network with Ironwood and NU7 active from height 1.
///
/// Coinbase construction pays unified miner addresses through the Ironwood pool, which
/// activates at NU6.3. Height 1 is also the NU6.1 activation block, whose coinbase pays the
/// configured disbursement out of the lockbox, so the disbursement must fit in what that
/// block's funding stream puts into the lockbox.
fn nu7_regtest() -> Network {
    Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            nu7: Some(1),
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
        // NU7's lower subsidy puts 2,083,333 zatoshis into the first block's lockbox.
        lockbox_disbursements: Some(vec![ConfiguredLockboxDisbursement {
            address: "t2RnBRiqrN1nW4ecZs1Fj3WWjNdnSs4kiX8".to_string(),
            amount: Amount::new(2_000_000),
        }]),
        ..Default::default()
    })
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
    zakurad.expect_stdout_line_matches("mempool activated")?;

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
