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

#[cfg(feature = "indexer")]
use zakura_chain::serialization::ZcashSerialize;

use crate::indexer::{self, indexer_client::IndexerClient, BlockRangeRequest, BlockRequest, Empty};
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
    test_get_block(client.clone(), mock_read_service.clone()).await?;
    #[cfg(feature = "indexer")]
    test_get_block_range(client.clone(), mock_read_service).await?;
    #[cfg(not(feature = "indexer"))]
    test_get_block_range_unimplemented(client.clone()).await?;

    Ok(())
}

/// Tests that `GetBlockRange` is rejected as unimplemented when the crate is
/// built without the `indexer` feature, which gates the raw block read path.
#[cfg(not(feature = "indexer"))]
async fn test_get_block_range_unimplemented(
    mut client: IndexerClient<tonic::transport::Channel>,
) -> Result<()> {
    let status = client
        .get_block_range(tonic::Request::new(BlockRangeRequest {
            start_height: 1,
            end_height: 2,
        }))
        .await
        .expect_err("get_block_range should be unimplemented without the indexer feature");
    assert_eq!(status.code(), tonic::Code::Unimplemented);

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

#[test]
fn block_and_hash_decode_rejects_a_missing_coinbase() -> Result<()> {
    let mut block: Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES.zcash_deserialize_into()?;
    block.transactions.clear();
    let hash = block.hash();
    let encoded = indexer::BlockAndHash::new(hash, Arc::new(block));

    assert!(
        encoded.decode().is_none(),
        "a block without a coinbase height must be rejected before state preparation"
    );

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

/// Tests that `GetBlockRange` streams the requested blocks in ascending order,
/// ends cleanly when the state stops early, reads the range in bounded chunks,
/// and rejects invalid ranges.
#[cfg(feature = "indexer")]
async fn test_get_block_range(
    client: IndexerClient<tonic::transport::Channel>,
    mut mock_read_service: MockService<ReadRequest, ReadResponse, PanicAssertion, BoxError>,
) -> Result<()> {
    // A range whose end is below its start is rejected without touching the
    // state.
    let status = client
        .clone()
        .get_block_range(tonic::Request::new(BlockRangeRequest {
            start_height: 2,
            end_height: 1,
        }))
        .await
        .expect_err("a block range with end below start should be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    // Heights above the maximum valid block height are rejected without
    // touching the state.
    let status = client
        .clone()
        .get_block_range(tonic::Request::new(BlockRangeRequest {
            start_height: u32::MAX,
            end_height: u32::MAX,
        }))
        .await
        .expect_err("an out-of-range block height should be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    // Blocks requested by height range are streamed in ascending order, with
    // the same bytes `GetBlock` serves for the same heights.
    let block1: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_1_BYTES.zcash_deserialize_into()?;
    let block2: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_2_BYTES.zcash_deserialize_into()?;

    let mut request_client = client.clone();
    let request_task = tokio::spawn(async move {
        request_client
            .get_block_range(tonic::Request::new(BlockRangeRequest {
                start_height: 1,
                end_height: 2,
            }))
            .await
    });

    mock_read_service
        .expect_request(ReadRequest::RawBlocksByHeightRange {
            start: Height(1),
            count: 2,
        })
        .await
        .respond(ReadResponse::RawBlocks(vec![
            (Height(1), block1.zcash_serialize_to_vec()?),
            (Height(2), block2.zcash_serialize_to_vec()?),
        ]));

    let mut stream = request_task
        .await?
        .expect("get_block_range should succeed")
        .into_inner();

    for (expected_height, expected_block) in [(1u32, &block1), (2, &block2)] {
        let response = tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .expect("should receive a streamed block before timeout")
            .expect("response stream should not end before the requested range")
            .expect("streamed block response should not be an error message");

        assert_eq!(response.height, expected_height);
        assert_eq!(response.data, expected_block.zcash_serialize_to_vec()?);

        // The streamed bytes are identical to `GetBlock`'s answer at the same
        // height.
        let mut get_block_client = client.clone();
        let get_block_task = tokio::spawn(async move {
            get_block_client
                .get_block(tonic::Request::new(BlockRequest {
                    hash_or_height: expected_height.to_be_bytes().to_vec(),
                }))
                .await
        });

        mock_read_service
            .expect_request(ReadRequest::Block(HashOrHeight::Height(Height(
                expected_height,
            ))))
            .await
            .respond(ReadResponse::Block(Some(expected_block.clone())));

        let get_block_response = get_block_task
            .await?
            .expect("get_block should succeed")
            .into_inner();
        assert_eq!(response.data, get_block_response.data);

        let (decoded_block, decoded_height) = response.decode().expect("response should decode");
        assert_eq!(decoded_height, Height(expected_height));
        assert_eq!(decoded_block.hash(), expected_block.hash());
    }

    assert!(
        tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .expect("the stream should end before timeout")
            .is_none(),
        "the stream should end after the last requested block"
    );

    // When the state stops before the end of the range — at its finalized tip
    // or before a missing block body — the stream ends cleanly after the last
    // served block instead of returning an error.
    let mut request_client = client.clone();
    let request_task = tokio::spawn(async move {
        request_client
            .get_block_range(tonic::Request::new(BlockRangeRequest {
                start_height: 1,
                end_height: 5,
            }))
            .await
    });

    mock_read_service
        .expect_request(ReadRequest::RawBlocksByHeightRange {
            start: Height(1),
            count: 5,
        })
        .await
        .respond(ReadResponse::RawBlocks(vec![
            (Height(1), block1.zcash_serialize_to_vec()?),
            (Height(2), block2.zcash_serialize_to_vec()?),
        ]));

    let mut stream = request_task
        .await?
        .expect("get_block_range should succeed")
        .into_inner();

    let mut received = Vec::new();
    while let Some(message) = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("the stream should make progress before timeout")
    {
        received.push(
            message
                .expect("streamed block response should not be an error message")
                .height,
        );
    }
    assert_eq!(
        received,
        [1, 2],
        "the stream should end cleanly after the last served block"
    );

    // A long range is read from the state in bounded chunks: each following
    // read starts after the last block of the previous one, and the trailing
    // read requests only the remainder.
    let mut request_client = client.clone();
    let request_task = tokio::spawn(async move {
        request_client
            .get_block_range(tonic::Request::new(BlockRangeRequest {
                start_height: 0,
                end_height: 129,
            }))
            .await
    });

    let mock_driver = {
        let mut mock_read_service = mock_read_service.clone();
        tokio::spawn(async move {
            for (start, count) in [(0u32, 64u32), (64, 64), (128, 2)] {
                mock_read_service
                    .expect_request(ReadRequest::RawBlocksByHeightRange {
                        start: Height(start),
                        count,
                    })
                    .await
                    .respond(ReadResponse::RawBlocks(
                        (start..start + count)
                            .map(|height| (Height(height), height.to_be_bytes().to_vec()))
                            .collect(),
                    ));
            }
        })
    };

    let mut stream = request_task
        .await?
        .expect("get_block_range should succeed")
        .into_inner();

    for expected_height in 0..=129u32 {
        let response = tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .expect("should receive a streamed block before timeout")
            .expect("response stream should not end before the requested range")
            .expect("streamed block response should not be an error message");

        assert_eq!(response.height, expected_height);
    }

    assert!(
        tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .expect("the stream should end before timeout")
            .is_none(),
        "the stream should end after the last requested block"
    );

    mock_driver.await?;

    // A short chunk stops the stream: when a mid-range read returns fewer
    // blocks than requested, the task must end the stream instead of
    // requesting the next chunk.
    let mut request_client = client.clone();
    let request_task = tokio::spawn(async move {
        request_client
            .get_block_range(tonic::Request::new(BlockRangeRequest {
                start_height: 0,
                end_height: 129,
            }))
            .await
    });

    mock_read_service
        .expect_request(ReadRequest::RawBlocksByHeightRange {
            start: Height(0),
            count: 64,
        })
        .await
        .respond(ReadResponse::RawBlocks(
            (0..10)
                .map(|height| (Height(height), height.to_be_bytes().to_vec()))
                .collect(),
        ));

    let mut stream = request_task
        .await?
        .expect("get_block_range should succeed")
        .into_inner();

    let mut received = Vec::new();
    while let Some(message) = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("the stream should make progress before timeout")
    {
        received.push(
            message
                .expect("streamed block response should not be an error message")
                .height,
        );
    }
    assert_eq!(
        received,
        (0..10u32).collect::<Vec<_>>(),
        "the stream should end after a short mid-range read"
    );
    mock_read_service.expect_no_requests().await;

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
