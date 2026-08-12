//! Fixed test vectors for indexer RPCs

use std::{fs, sync::Arc, time::Duration};

use futures::StreamExt;
use tokio::{sync::broadcast, task::JoinHandle};
use tower::BoxError;
use zakura_chain::{
    block::{Block, Height},
    chain_tip::mock::{MockChainTip, MockChainTipSender},
    serialization::ZcashDeserializeInto,
    transaction::{self, UnminedTxId},
};
use zakura_node_services::mempool::{MempoolChange, MempoolTxSubscriber};
use zakura_state::{HashOrHeight, ReadRequest, ReadResponse};
use zakura_test::{
    mock_service::{MockService, PanicAssertion},
    prelude::color_eyre::{eyre::eyre, Result},
};

use crate::indexer::{self, indexer_client::IndexerClient, BlockRequest, Empty};
use crate::{
    config::rpc::IndexerTlsConfig,
    indexer::tests::certs::{
        CA_CERT, CLIENT_CERT, CLIENT_KEY, SERVER_CERT, SERVER_KEY, UNTRUSTED_CLIENT_CERT,
        UNTRUSTED_CLIENT_KEY,
    },
    sync::{IndexerClientConfig, IndexerClientTlsConfig},
};

#[tokio::test]
async fn rpc_server_spawn() -> Result<()> {
    let _init_guard = zakura_test::init();

    let (
        _server_task,
        client,
        mock_read_service,
        mock_chain_tip_sender,
        mempool_transaction_sender,
    ) = start_server_and_get_client().await?;

    test_chain_tip_change(client.clone(), mock_chain_tip_sender).await?;
    test_mempool_change(client.clone(), mempool_transaction_sender).await?;
    test_get_block(client.clone(), mock_read_service).await?;

    Ok(())
}

#[tokio::test]
async fn indexer_server_requires_a_trusted_client_certificate() -> Result<()> {
    let _init_guard = zakura_test::init();
    let temp_dir = tempfile::tempdir()?;
    let ca_file = temp_dir.path().join("ca.pem");
    let server_cert_file = temp_dir.path().join("server.pem");
    let server_key_file = temp_dir.path().join("server-key.pem");
    let client_cert_file = temp_dir.path().join("client.pem");
    let client_key_file = temp_dir.path().join("client-key.pem");
    fs::write(&ca_file, CA_CERT)?;
    fs::write(&server_cert_file, SERVER_CERT)?;
    fs::write(&server_key_file, SERVER_KEY)?;
    fs::write(&client_cert_file, CLIENT_CERT)?;
    fs::write(&client_key_file, CLIENT_KEY)?;

    let server_tls = IndexerTlsConfig {
        cert_file: server_cert_file,
        key_file: server_key_file,
        client_ca_file: ca_file,
    };
    let (server_task, listen_addr, _read_state, _tip_sender, _mempool_sender) =
        start_server(Some(server_tls)).await?;

    let unauthenticated_tls = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(CA_CERT))
        .domain_name("localhost");
    let unauthenticated_endpoint =
        tonic::transport::Endpoint::new(format!("https://{listen_addr}"))?
            .tls_config(unauthenticated_tls)?;
    // Every client below sends the same request, which request validation
    // rejects with `InvalidArgument`. Only a client the server accepted can
    // reach request validation, so `InvalidArgument` is the signature of an
    // accepted client and any other status means the connection was refused.
    let mut unauthenticated_client = IndexerClient::connect(unauthenticated_endpoint).await?;
    let status = unauthenticated_client
        .get_block(BlockRequest {
            hash_or_height: Vec::new(),
        })
        .await
        .expect_err("the indexer server must reject clients without a certificate");
    assert_ne!(
        status.code(),
        tonic::Code::InvalidArgument,
        "a client without a certificate reached request validation: {status:?}"
    );

    let untrusted_cert_file = temp_dir.path().join("untrusted-client.pem");
    let untrusted_key_file = temp_dir.path().join("untrusted-client-key.pem");
    fs::write(&untrusted_cert_file, UNTRUSTED_CLIENT_CERT)?;
    fs::write(&untrusted_key_file, UNTRUSTED_CLIENT_KEY)?;
    let untrusted_endpoint = IndexerClientConfig::mtls(
        listen_addr,
        IndexerClientTlsConfig::new(
            temp_dir.path().join("ca.pem"),
            untrusted_cert_file,
            untrusted_key_file,
            "localhost".to_string(),
        ),
    )
    .endpoint()
    .map_err(|error| eyre!(error))?;
    let mut untrusted_client = IndexerClient::connect(untrusted_endpoint).await?;
    let status = untrusted_client
        .get_block(BlockRequest {
            hash_or_height: Vec::new(),
        })
        .await
        .expect_err("the indexer server must reject certificates from an untrusted CA");
    assert_ne!(
        status.code(),
        tonic::Code::InvalidArgument,
        "a certificate signed by an untrusted CA reached request validation: {status:?}"
    );

    let authenticated_endpoint = IndexerClientConfig::mtls(
        listen_addr,
        IndexerClientTlsConfig::new(
            temp_dir.path().join("ca.pem"),
            client_cert_file,
            client_key_file,
            "localhost".to_string(),
        ),
    )
    .endpoint()
    .map_err(|error| eyre!(error))?;
    let mut client = IndexerClient::connect(authenticated_endpoint).await?;
    let status = client
        .get_block(BlockRequest {
            hash_or_height: Vec::new(),
        })
        .await
        .expect_err("the authenticated request should reach request validation");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    server_task.abort();
    Ok(())
}

/// Tests that `GetBlock` returns the requested block and rejects invalid
/// requests.
async fn test_get_block(
    mut client: IndexerClient<tonic::transport::Channel>,
    mut mock_read_service: MockService<ReadRequest, ReadResponse, PanicAssertion, BoxError>,
) -> Result<()> {
    // A request whose bytes are neither a 32-byte hash nor a 4-byte height is
    // rejected without touching the state.
    let status = client
        .get_block(tonic::Request::new(BlockRequest {
            hash_or_height: Vec::new(),
        }))
        .await
        .expect_err("a block request without a valid hash or height should be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    // A height above the maximum valid block height is rejected without
    // touching the state.
    let status = client
        .get_block(tonic::Request::new(BlockRequest {
            hash_or_height: u32::MAX.to_be_bytes().to_vec(),
        }))
        .await
        .expect_err("an out-of-range block height should be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    // A block requested by height is returned along with its hash.
    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES.zcash_deserialize_into()?;
    let expected_hash = block.hash();
    let height = block
        .coinbase_height()
        .expect("test block has a coinbase height");

    let mut request_client = client.clone();
    let request_task = tokio::spawn(async move {
        request_client
            .get_block(tonic::Request::new(BlockRequest {
                hash_or_height: height.0.to_be_bytes().to_vec(),
            }))
            .await
    });

    mock_read_service
        .expect_request(ReadRequest::Block(HashOrHeight::Height(height)))
        .await
        .respond(ReadResponse::Block(Some(block.clone())));

    let response = request_task
        .await?
        .expect("get_block should succeed")
        .into_inner();
    let (decoded_block, decoded_hash) = response.decode().expect("response should decode");
    assert_eq!(decoded_hash, expected_hash);
    assert_eq!(decoded_block.hash(), expected_hash);

    Ok(())
}

async fn test_chain_tip_change(
    mut client: IndexerClient<tonic::transport::Channel>,
    mock_chain_tip_sender: MockChainTipSender,
) -> Result<()> {
    let request = tonic::Request::new(Empty {});
    let mut response = client.chain_tip_change(request).await?.into_inner();
    mock_chain_tip_sender.send_best_tip_height(Height::MIN);
    mock_chain_tip_sender.send_best_tip_hash(zakura_chain::block::Hash([0; 32]));

    // Wait for RPC server to send a message
    tokio::time::sleep(Duration::from_millis(500)).await;

    tokio::time::timeout(Duration::from_secs(3), response.next())
        .await
        .expect("should receive chain tip change notification before timeout")
        .expect("response stream should not be empty")
        .expect("chain tip change response should not be an error message");

    Ok(())
}

async fn test_mempool_change(
    mut client: IndexerClient<tonic::transport::Channel>,
    mempool_transaction_sender: tokio::sync::broadcast::Sender<MempoolChange>,
) -> Result<()> {
    let request = tonic::Request::new(Empty {});
    let mut response = client.mempool_change(request).await?.into_inner();

    let change_tx_ids = [UnminedTxId::Legacy(transaction::Hash::from([0; 32]))]
        .into_iter()
        .collect();

    mempool_transaction_sender
        .send(MempoolChange::added(change_tx_ids))
        .expect("rpc server should have a receiver");

    tokio::time::timeout(Duration::from_secs(3), response.next())
        .await
        .expect("should receive chain tip change notification before timeout")
        .expect("response stream should not be empty")
        .expect("chain tip change response should not be an error message");

    Ok(())
}

async fn start_server_and_get_client() -> Result<(
    JoinHandle<Result<(), BoxError>>,
    IndexerClient<tonic::transport::Channel>,
    MockService<ReadRequest, ReadResponse, PanicAssertion, BoxError>,
    MockChainTipSender,
    broadcast::Sender<MempoolChange>,
)> {
    let (
        server_task,
        listen_addr,
        mock_read_service,
        mock_chain_tip_change_sender,
        mempool_transaction_sender,
    ) = start_server(None).await?;

    // wait for the server to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    let endpoint = tonic::transport::channel::Endpoint::new(format!("http://{listen_addr}"))
        .unwrap()
        .timeout(Duration::from_secs(2));

    // connect to the gRPC server
    let client = IndexerClient::connect(endpoint)
        .await
        .expect("server should receive connection");

    Ok((
        server_task,
        client,
        mock_read_service,
        mock_chain_tip_change_sender,
        mempool_transaction_sender,
    ))
}

async fn start_server(
    tls: Option<IndexerTlsConfig>,
) -> Result<(
    JoinHandle<Result<(), BoxError>>,
    std::net::SocketAddr,
    MockService<ReadRequest, ReadResponse, PanicAssertion, BoxError>,
    MockChainTipSender,
    broadcast::Sender<MempoolChange>,
)> {
    let listen_addr: std::net::SocketAddr = "127.0.0.1:0"
        .parse()
        .expect("hard-coded IP and u16 port should parse successfully");

    let mock_read_service = MockService::build()
        .with_max_request_delay(Duration::from_secs(2))
        .for_unit_tests();

    let (mock_chain_tip_change, mock_chain_tip_change_sender) = MockChainTip::new();
    let (mempool_transaction_sender, _) = tokio::sync::broadcast::channel(1);
    let mempool_tx_subscriber = MempoolTxSubscriber::new(mempool_transaction_sender.clone());
    let (server_task, listen_addr) = indexer::server::init(
        listen_addr,
        tls,
        mock_read_service.clone(),
        mock_chain_tip_change,
        mempool_tx_subscriber.clone(),
    )
    .await
    .map_err(|err| eyre!(err))?;

    Ok((
        server_task,
        listen_addr,
        mock_read_service,
        mock_chain_tip_change_sender,
        mempool_transaction_sender,
    ))
}
