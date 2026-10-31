//! Tests for the HTTP request compatibility middleware.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use http_body::Frame;
use tower::{BoxError, Service};

use crate::server::{
    http::{HttpBody, HttpRequest, HttpResponse},
    http_request_compatibility::HttpRequestMiddleware,
};

/// A body that always returns an error, simulating a TCP RST during body collection.
struct ErrorBody;

impl http_body::Body for ErrorBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(Some(Err("connection reset".into())))
    }
}

/// A mock inner service that returns a minimal JSON-RPC 2.0 response.
#[derive(Clone)]
struct MockRpcService;

impl Service<HttpRequest> for MockRpcService {
    type Response = HttpResponse;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<HttpResponse, BoxError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: HttpRequest) -> Self::Future {
        let body = r#"{"jsonrpc":"2.0","id":1,"result":null}"#;
        let response = HttpResponse::new(HttpBody::from(body.to_string()));
        Box::pin(async { Ok(response) })
    }
}

/// Verifies that body collection errors return `Err` instead of panicking.
///
/// Previously, the middleware called `.expect()` on `body.collect().await`,
/// so a TCP RST during body reading would panic the process.
#[tokio::test]
async fn request_body_error_returns_err_instead_of_panic() {
    let request = HttpRequest::builder()
        .method("POST")
        .header("content-type", "appliion/json")
        .body(ErrorBody)
        .expect("valid request");

    let mut middleware = HttpRequestMiddleware::new(MockRpcService, None, 2_097_152);
    let result = middleware.call(request).await;

    assert!(
        result.is_err(),
        "body collection error should return Err, not panic"
    );
}

/// Verifies that a request body exceeding `max_request_body_size` is rejected.
#[tokio::test]
async fn oversized_request_body_is_rejected() {
    let limit = 64;
    let oversized = vec![b'x'; limit + 1];
    let body = HttpBody::from(oversized);
    let request = HttpRequest::builder()
        .method("POST")
        .header("content-type", "application/json")
        .body(body)
        .expect("valid request");

    let mut middleware = HttpRequestMiddleware::new(MockRpcService, None, limit);
    let result = middleware.call(request).await;

    assert!(result.is_err(), "oversized request body should be rejected");
}

/// A mock JSON-RPC server that, like jsonrpsee, answers each batch member and
/// rejects members that are not JSON-RPC 2.0.
#[derive(Clone)]
struct BatchEchoService;

impl Service<HttpRequest> for BatchEchoService {
    type Response = HttpResponse;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<HttpResponse, BoxError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: HttpRequest) -> Self::Future {
        Box::pin(async move {
            use http_body_util::BodyExt;

            let bytes = request.into_body().collect().await?.to_bytes();
            let batch: Vec<serde_json::Value> = serde_json::from_slice(&bytes)?;
            let responses: Vec<_> = batch
                .iter()
                .map(|member| {
                    if member["jsonrpc"] == "2.0" {
                        serde_json::json!({"jsonrpc": "2.0", "id": member["id"], "result": member["method"]})
                    } else {
                        serde_json::json!({"jsonrpc": "2.0", "id": member["id"], "error": {"code": -32600, "message": "Invalid request"}})
                    }
                })
                .collect();
            Ok(HttpResponse::new(HttpBody::from(serde_json::to_vec(
                &responses,
            )?)))
        })
    }
}

/// Sends `body` through the middleware to [`BatchEchoService`], and returns the response.
async fn batch_round_trip(body: serde_json::Value) -> serde_json::Value {
    use http_body_util::BodyExt;

    let request = HttpRequest::builder()
        .method("POST")
        .header("content-type", "application/json")
        .body(HttpBody::from(
            serde_json::to_vec(&body).expect("valid JSON"),
        ))
        .expect("valid request");
    let response = HttpRequestMiddleware::new(BatchEchoService, None, 2_097_152)
        .call(request)
        .await
        .expect("the mock server responds");
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("the mock body is complete")
        .to_bytes();
    serde_json::from_slice(&bytes).expect("the response is JSON")
}

/// A JSON-RPC 1.0 batch, like s-nomp's startup checks, is answered in JSON-RPC 1.0.
#[tokio::test]
async fn json_rpc_1_batch_is_converted_both_ways() {
    let response = batch_round_trip(serde_json::json!([
        {"jsonrpc": "1.0", "method": "getinfo", "params": [], "id": 1},
        {"jsonrpc": "1.0", "method": "getmininginfo", "params": [], "id": 2},
    ]))
    .await;

    assert_eq!(
        response,
        serde_json::json!([
            {"jsonrpc": "1.0", "id": 1, "result": "getinfo", "error": null},
            {"jsonrpc": "1.0", "id": 2, "result": "getmininginfo", "error": null},
        ])
    );
}

/// A versionless batch is answered without a `jsonrpc` field.
#[tokio::test]
async fn versionless_batch_is_converted_both_ways() {
    let response = batch_round_trip(serde_json::json!([
        {"method": "getblockcount", "params": [], "id": "a"},
    ]))
    .await;

    assert_eq!(
        response,
        serde_json::json!([{"id": "a", "result": "getblockcount", "error": null}])
    );
}

/// A batch mixing formats is passed to the server unchanged.
#[tokio::test]
async fn mixed_batch_is_unchanged() {
    let response = batch_round_trip(serde_json::json!([
        {"jsonrpc": "2.0", "method": "getblockcount", "params": [], "id": 1},
        {"jsonrpc": "1.0", "method": "getinfo", "params": [], "id": 2},
    ]))
    .await;

    assert_eq!(
        response,
        serde_json::json!([
            {"jsonrpc": "2.0", "id": 1, "result": "getblockcount"},
            {"jsonrpc": "2.0", "id": 2, "error": {"code": -32600, "message": "Invalid request"}},
        ])
    );
}
