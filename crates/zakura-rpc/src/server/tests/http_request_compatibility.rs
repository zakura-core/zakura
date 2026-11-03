//! Tests for the HTTP request compatibility middleware.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use bytes::Bytes;
use http_body::Frame;
use http_body_util::BodyExt;
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

/// Sends `request` through the middleware and returns what the inner service received and
/// what the client gets back.
async fn round_trip(request: impl Into<Bytes>, response: &'static str) -> (Bytes, Bytes) {
    let received = Arc::new(Mutex::new(None));
    let recorded = received.clone();
    let service = tower::service_fn(move |request: HttpRequest| {
        let recorded = recorded.clone();
        async move {
            let body = request.into_body().collect().await?.to_bytes();
            *recorded
                .lock()
                .expect("no test thread panics while holding the lock") = Some(body);
            Ok::<_, BoxError>(HttpResponse::new(HttpBody::from(response)))
        }
    });
    // Match the server's limit of twice the maximum block size plus its JSON wrapper.
    let mut middleware = HttpRequestMiddleware::new(service, None, 4_001_024);
    let request = HttpRequest::builder()
        .method("POST")
        .header("content-type", "application/json")
        .body(HttpBody::from(request.into()))
        .expect("valid request");
    let response = middleware
        .call(request)
        .await
        .expect("the inner service answers every request")
        .into_body()
        .collect()
        .await
        .expect("a full body can't fail")
        .to_bytes();
    let received = received
        .lock()
        .expect("no test thread panics while holding the lock")
        .take()
        .expect("the middleware forwards the request to the inner service");
    (received, response)
}

/// A plain JSON-RPC 2.0 call reaches the server exactly as sent, and the server's response
/// reaches the client exactly as sent.
#[tokio::test]
async fn plain_json_rpc_2_call_and_response_pass_through_unchanged() {
    let block = "00".repeat(2_000_000);
    let request =
        format!(r#"{{"id": 3, "method": "submitblock", "jsonrpc": "2.0", "params": ["{block}"]}}"#);
    let response = r#"{"jsonrpc":"2.0","id":3,"result":null}"#;

    let (received, returned) = round_trip(request.clone(), response).await;

    assert_eq!(received, request.as_bytes());
    assert_eq!(returned, response.as_bytes());
}

/// A near-limit numeric array with a noncanonical final number keeps the conversion behavior.
#[tokio::test]
async fn large_numeric_array_is_normalized() {
    let numbers = "0,".repeat(1_999_999);
    let request = format!(
        r#"{{"id": 1, "method": "submitblock", "jsonrpc": "2.0", "params": [{numbers}1e2]}}"#
    );
    let expected =
        format!(r#"{{"jsonrpc":"2.0","method":"submitblock","params":[{numbers}100.0],"id":1}}"#);

    let (received, _) = round_trip(request, r#"{"jsonrpc":"2.0","id":1,"result":null}"#).await;

    assert_eq!(received, expected.as_bytes());
}

/// JSON-RPC 2.0 calls that the server would read differently from their re-encoded form are
/// still re-encoded before they reach it.
#[tokio::test]
async fn json_rpc_2_calls_the_server_reads_differently_are_still_re_encoded() {
    let response = r#"{"jsonrpc":"2.0","id":1,"result":null}"#;
    for (request, forwarded) in [
        (
            r#"{"id": 1, "method": "getinfo", "jsonrpc": "2.0", "params": [0,0]}"#,
            r#"{"jsonrpc":"2.0","method":"getinfo","params":[0,0],"id":1}"#,
        ),
        (
            r#"{"id": 1, "method": "getinfo", "jsonrpc": "2.0", "params": ["a","b"]}"#,
            r#"{"jsonrpc":"2.0","method":"getinfo","params":["a","b"],"id":1}"#,
        ),
        (
            r#"{"jsonrpc":"2.0","method":"getinfo","params":[ ],"id":1}"#,
            r#"{"jsonrpc":"2.0","method":"getinfo","params":[],"id":1}"#,
        ),
        (
            r#"{"jsonrpc":"2.0","method":"getinfo","params":[],"id":null}"#,
            r#"{"jsonrpc":"2.0","method":"getinfo","params":[]}"#,
        ),
        (
            r#"{"jsonrpc":"2.0","method":"getinfo","params":["\u0041"],"id":1}"#,
            r#"{"jsonrpc":"2.0","method":"getinfo","params":["A"],"id":1}"#,
        ),
        (
            r#"{"jsonrpc":"2.0","method":"getinfo","params":[1e2],"id":1}"#,
            r#"{"jsonrpc":"2.0","method":"getinfo","params":[100.0],"id":1}"#,
        ),
        (
            r#"{"jsonrpc":"2.0","method":"getinfo","params":[],"id":1,"extra":0}"#,
            r#"{"jsonrpc":"2.0","method":"getinfo","params":[],"id":1}"#,
        ),
        (
            r#" {"jsonrpc":"2.0","method":"getinfo","params":[],"id":1}"#,
            r#"{"jsonrpc":"2.0","method":"getinfo","params":[],"id":1}"#,
        ),
        (
            r#"{"jsonrpc":"2.0","method":"getinfo","params":{"a":1,"a":2},"id":1}"#,
            r#"{"jsonrpc":"2.0","method":"getinfo","params":{"a":2},"id":1}"#,
        ),
    ] {
        let (received, _) = round_trip(request, response).await;
        assert_eq!(
            received,
            forwarded.as_bytes(),
            "{request} should reach the server as {forwarded}"
        );
    }
}

/// A bitcoind-style request without a `jsonrpc` field reaches the server as JSON-RPC 2.0 with
/// the same method, parameters and ID, and the response goes back without a `jsonrpc` field.
#[tokio::test]
async fn bitcoind_request_is_rewritten_to_json_rpc_2() {
    let request = r#"{"method":"submitblock","params":["00ff"],"id":"miner"}"#;
    let response = r#"{"jsonrpc":"2.0","id":"miner","result":null}"#;

    let (received, returned) = round_trip(request, response).await;

    let received: serde_json::Value =
        serde_json::from_slice(&received).expect("the rewritten request is JSON");
    assert_eq!(
        received,
        serde_json::json!({"jsonrpc": "2.0", "method": "submitblock", "params": ["00ff"], "id": "miner"})
    );
    let returned: serde_json::Value =
        serde_json::from_slice(&returned).expect("the rewritten response is JSON");
    assert_eq!(
        returned,
        serde_json::json!({"id": "miner", "result": null, "error": null})
    );
}

/// A lightwalletd-style `"jsonrpc": "1.0"` request reaches the server as JSON-RPC 2.0, and the
/// response goes back labelled 1.0.
#[tokio::test]
async fn lightwalletd_request_is_rewritten_to_json_rpc_2() {
    let request = r#"{"jsonrpc":"1.0","method":"getinfo","params":[],"id":7}"#;
    let response = r#"{"jsonrpc":"2.0","id":7,"result":{"version":1}}"#;

    let (received, returned) = round_trip(request, response).await;

    let received: serde_json::Value =
        serde_json::from_slice(&received).expect("the rewritten request is JSON");
    assert_eq!(
        received,
        serde_json::json!({"jsonrpc": "2.0", "method": "getinfo", "params": [], "id": 7})
    );
    let returned: serde_json::Value =
        serde_json::from_slice(&returned).expect("the rewritten response is JSON");
    assert_eq!(
        returned,
        serde_json::json!({"jsonrpc": "1.0", "id": 7, "result": {"version": 1}, "error": null})
    );
}
