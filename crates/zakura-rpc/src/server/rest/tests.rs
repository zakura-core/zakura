//! Tests for the binary REST endpoints.

use std::{str::FromStr, sync::Arc};

use http_body_util::BodyExt;
use tower::{service_fn, BoxError, Service, ServiceExt};

use zakura_chain::{
    block::{self, Block},
    serialization::{ZcashDeserializeInto, ZcashSerialize},
};
use zakura_node_services::block_memory::{OwnedRawBlock, OwnedRawTransaction, ReadMemoryBudget};
use zakura_state::{HashOrHeight, ReadRequest, ReadResponse};
use zakura_test::mock_service::{MockService, PanicAssertion};

use super::{
    super::{
        cookie::Cookie, http::HttpBody, http_request_compatibility::HttpRequestMiddlewareLayer,
    },
    *,
};

fn retained<T: zakura_node_services::block_memory::DecodedSize>(
    value: T,
) -> zakura_node_services::block_memory::OwnedPayload<T> {
    ReadMemoryBudget::new(std::num::NonZeroU64::MAX)
        .try_reserve(0)
        .and_then(|reservation| reservation.retain(value))
        .expect("an unlimited budget has room")
}

/// A router whose non-REST requests answer `inner`.
fn router(
    read_state: MockService<ReadRequest, ReadResponse, PanicAssertion, BoxError>,
) -> impl Service<
    HttpRequest<()>,
    Response = HttpResponse,
    Error = BoxError,
    Future = impl Future<Output = Result<HttpResponse, BoxError>> + Send,
> {
    let inner = service_fn(|_: HttpRequest<()>| async {
        Ok::<_, BoxError>(body(StatusCode::OK, b"inner".to_vec(), "text/plain"))
    });
    RestRouter::new(inner, read_state)
}

fn get(path: &str) -> HttpRequest<()> {
    hyper::Request::get(path).body(()).expect("valid request")
}

async fn read(response: HttpResponse) -> (StatusCode, Option<String>, Vec<u8>) {
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .map(|value| value.to_str().expect("ASCII").to_owned());
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("full bodies are infallible")
        .to_bytes()
        .to_vec();
    (status, content_type, bytes)
}

#[test]
fn paths_parse_like_zcashd() {
    let hash = "00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08";
    let block_hash = block::Hash::from_str(hash).expect("valid hash");
    let txid = transaction::Hash::from_str(hash).expect("valid hash");

    assert_eq!(
        parse_path(&format!("/rest/block/{hash}.bin")),
        Ok(RestRequest::Block(block_hash, Format::Binary))
    );
    assert_eq!(
        parse_path(&format!("/rest/block/{hash}.hex")),
        Ok(RestRequest::Block(block_hash, Format::Hex))
    );
    assert_eq!(
        parse_path(&format!("/rest/tx/{hash}.bin")),
        Ok(RestRequest::Transaction(txid, Format::Binary))
    );
    assert!(matches!(
        parse_path(&format!("/rest/block/{hash}.json")),
        Err(PathError::NotFound(_))
    ));
    assert!(matches!(
        parse_path(&format!("/rest/block/{hash}")),
        Err(PathError::NotFound(_))
    ));
    assert!(matches!(
        parse_path(&format!("/rest/headers/{hash}.bin")),
        Err(PathError::NotFound(_))
    ));
    assert!(matches!(
        parse_path("/rest/block/zz.bin"),
        Err(PathError::BadRequest(_))
    ));
}

#[tokio::test]
async fn rest_serves_stored_bytes_and_passes_other_requests_on() {
    let _init_guard = zakura_test::init();

    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let block_bytes = block.zcash_serialize_to_vec().unwrap();
    let hash = block.hash();
    let transaction = block.transactions[0].clone();
    let transaction_bytes = transaction.zcash_serialize_to_vec().unwrap();
    let txid = transaction.hash();

    let mut state = MockService::build().for_unit_tests();
    let mut service = router(state.clone());

    // A block, as binary and as hex.
    for (extension, expected, content_type) in [
        ("bin", block_bytes.clone(), "application/octet-stream"),
        (
            "hex",
            format!("{}\n", const_hex::encode(&block_bytes)).into_bytes(),
            "text/plain",
        ),
    ] {
        let response = service
            .ready()
            .await
            .unwrap()
            .call(get(&format!("/rest/block/{hash}.{extension}")));
        let response = tokio::spawn(response);
        state
            .expect_request(ReadRequest::RawBlock(HashOrHeight::Hash(hash)))
            .await
            .respond(ReadResponse::RawBlock(Some(
                retained::<Vec<u8>>(block_bytes.clone()) as OwnedRawBlock,
            )));
        let (status, actual_type, bytes) = read(response.await.unwrap().unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(actual_type.as_deref(), Some(content_type));
        assert_eq!(bytes, expected);
    }

    // A transaction.
    let response = tokio::spawn(
        service
            .ready()
            .await
            .unwrap()
            .call(get(&format!("/rest/tx/{txid}.bin"))),
    );
    state
        .expect_request(ReadRequest::AnyChainRawTransaction(txid))
        .await
        .respond(ReadResponse::RawTransaction(Some(
            retained::<Vec<u8>>(transaction_bytes.clone()) as OwnedRawTransaction,
        )));
    let (status, _, bytes) = read(response.await.unwrap().unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(bytes, transaction_bytes);

    // An unknown block.
    let response = tokio::spawn(
        service
            .ready()
            .await
            .unwrap()
            .call(get(&format!("/rest/block/{hash}.bin"))),
    );
    state
        .expect_request(ReadRequest::RawBlock(HashOrHeight::Hash(hash)))
        .await
        .respond(ReadResponse::RawBlock(None));
    let (status, _, _) = read(response.await.unwrap().unwrap()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Invalid requests are answered without reading the state.
    let (status, _, _) = read(
        service
            .ready()
            .await
            .unwrap()
            .call(get("/rest/block/zz.bin"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    state.expect_no_requests().await;

    // Other paths go to the JSON-RPC service.
    let (status, _, bytes) =
        read(service.ready().await.unwrap().call(get("/")).await.unwrap()).await;
    assert_eq!((status, bytes), (StatusCode::OK, b"inner".to_vec()));
}

/// A listener with cookie authentication refuses REST requests without the cookie,
/// before they read the state.
#[tokio::test]
async fn rest_requires_the_listeners_cookie() {
    let _init_guard = zakura_test::init();

    let mut state: MockService<ReadRequest, ReadResponse, PanicAssertion, BoxError> =
        MockService::build().for_unit_tests();
    let inner = service_fn(|_: HttpRequest| async {
        Ok::<_, BoxError>(body(StatusCode::OK, b"inner".to_vec(), "text/plain"))
    });
    let mut service = tower::ServiceBuilder::new()
        .layer(HttpRequestMiddlewareLayer::new(
            Some(Cookie::default()),
            1024,
        ))
        .service(RestRouter::new(inner, state.clone()));

    let hash = "00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08";
    let request = hyper::Request::get(format!("/rest/block/{hash}.bin"))
        .body(HttpBody::default())
        .expect("valid request");
    let response = service.call(request).await;

    assert!(
        response.is_err(),
        "unauthenticated REST requests are refused"
    );
    state.expect_no_requests().await;
}
