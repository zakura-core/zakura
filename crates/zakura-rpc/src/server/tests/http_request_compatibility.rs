//! Tests for the HTTP request compatibility middleware and JSON-RPC version mapping.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use bytes::Bytes;
use http_body::Frame;
use http_body_util::BodyExt;
use tower::{BoxError, Layer, Service};

use crate::server::{
    http::{HttpBody, HttpRequest, HttpResponse},
    http_request_compatibility::{
        convert_to_json_rpc_2, json_from_json_rpc_2, request_to_json_rpc_2,
        response_from_json_rpc_2, HttpRequestMiddleware, HttpRequestMiddlewareLayer,
        JsonRpc2Request, JsonRpcVersion,
    },
    jsonrpc::{Body, Id, MethodResponse},
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

/// Returns the status a middleware built from `layer` gives a request with `origin`, if set.
async fn status_for_origin(
    layer: HttpRequestMiddlewareLayer,
    origin: Option<&str>,
) -> hyper::StatusCode {
    let mut request = HttpRequest::builder()
        .method("POST")
        .header("content-type", "text/plain");
    if let Some(origin) = origin {
        request = request.header("origin", origin);
    }
    let request = request
        .body(HttpBody::from(
            r#"{"jsonrpc":"2.0","method":"getinfo","id":1}"#,
        ))
        .expect("valid request");

    layer
        .layer(MockRpcService)
        .call(request)
        .await
        .expect("the middleware answers every well-formed request")
        .status()
}

/// A listener that rejects browser requests refuses any `Origin`, including the `null` origin
/// of sandboxed and `file://` pages, but still serves clients that send none.
#[tokio::test]
async fn browser_requests_are_rejected_only_when_configured() {
    let rejecting = || HttpRequestMiddlewareLayer::new(None, 4_096).reject_browser_requests();
    for origin in ["https://attacker.example", "http://127.0.0.1:8232", "null"] {
        assert_eq!(
            status_for_origin(rejecting(), Some(origin)).await,
            hyper::StatusCode::FORBIDDEN,
            "a browser request from {origin} must be rejected"
        );
    }
    assert_eq!(
        status_for_origin(rejecting(), None).await,
        hyper::StatusCode::OK
    );

    let default = HttpRequestMiddlewareLayer::new(None, 4_096);
    assert_eq!(
        status_for_origin(default, Some("https://attacker.example")).await,
        hyper::StatusCode::OK,
        "listeners keep accepting browser requests unless configured otherwise"
    );
}

/// Sends `request` through the middleware and returns the body the inner service received.
async fn forwarded_body(request: impl Into<Bytes>) -> Bytes {
    let received = Arc::new(Mutex::new(None));
    let recorded = received.clone();
    let service = tower::service_fn(move |request: HttpRequest| {
        let recorded = recorded.clone();
        async move {
            let body = request.into_body().collect().await?.to_bytes();
            *recorded
                .lock()
                .expect("no test thread panics while holding the lock") = Some(body);
            Ok::<_, BoxError>(HttpResponse::new(HttpBody::from("null")))
        }
    });
    // Match the server's limit of twice the maximum block size plus its JSON wrapper.
    let mut middleware = HttpRequestMiddleware::new(service, None, 4_001_024);
    let request = HttpRequest::builder()
        .method("POST")
        .header("content-type", "application/json")
        .body(HttpBody::from(request.into()))
        .expect("valid request");
    middleware
        .call(request)
        .await
        .expect("the inner service answers every request");
    let received = received
        .lock()
        .expect("no test thread panics while holding the lock")
        .take();
    received.expect("the middleware forwards the request to the inner service")
}

/// The middleware passes every body on as sent; the server maps JSON-RPC versions as it parses.
#[tokio::test]
async fn middleware_forwards_bodies_unchanged() {
    for request in [
        r#"{"id": 3, "method": "getinfo", "jsonrpc": "2.0", "params": []}"#,
        r#"{"jsonrpc":"1.0","method":"getinfo","params":[],"id":7}"#,
        r#"{"method":"submitblock","params":["00ff"],"id":"miner"}"#,
        "not json",
    ] {
        assert_eq!(forwarded_body(request).await, request.as_bytes());
    }
}

/// Describes a parsed body, so tests can compare two parses.
fn describe(body: &Body<'_>) -> String {
    match body {
        Body::Call(request) => format!("{request:?}"),
        Body::Notification(notification) => format!("{notification:?}"),
        Body::Error(response) => format!("{:?}", response.as_json()),
        Body::Batch(entries) => format!("batch of {}", entries.len()),
    }
}

/// Returns the converted body and version, failing if `request` is parsed as sent.
fn converted(request: &str) -> (JsonRpcVersion, Vec<u8>) {
    match request_to_json_rpc_2(request.as_bytes()) {
        JsonRpc2Request::Parsed { .. } => panic!("{request} should be converted"),
        JsonRpc2Request::Converted { version, body } => (version, body.into_owned()),
    }
}

/// Every call parsed as sent is the call, and version, that conversion gives the server.
///
/// This covers the field orders, versions, IDs, methods, and parameters that decide whether a
/// call can be parsed as sent, and those that make the server read it as a notification.
#[test]
fn calls_parsed_as_sent_match_their_conversion() {
    let versions = [
        Some(r#""2.0""#),
        Some(r#""2\u002e0""#),
        Some(r#""1.0""#),
        Some(r#""1.1""#),
        None,
        Some("null"),
        Some("2"),
    ];
    let ids = [
        None,
        Some("0"),
        Some("1"),
        Some("18446744073709551615"),
        Some("18446744073709551616"),
        Some("-1"),
        Some("1.5"),
        Some("1e2"),
        Some("1e400"),
        Some("-1e400"),
        Some(r#""x""#),
        Some(r#""\u0041""#),
        Some(r#""""#),
        Some("null"),
        Some("true"),
        Some("[]"),
    ];
    let methods = [r#""getinfo""#, r#""get\u0069nfo""#, "1"];
    let params = [
        None,
        Some("null"),
        Some("[]"),
        Some("[ ]"),
        Some(r#""x""#),
        Some("{}"),
        Some(r#"{"a":1}"#),
        Some("[1]"),
        Some("[0,0]"),
        Some("[1e2]"),
        Some(r#"["00ff"]"#),
        Some(r#"["a\"b"]"#),
        Some(r#"["a","b"]"#),
        Some(r#"["00ff",{"workid":"ab"}]"#),
        Some(r#"["00ff", {"workid":"ab"}]"#),
        Some(r#"["00ff",{"workid": "ab"}]"#),
        Some(r#"["00ff",{"workid":"a","workid":"b"}]"#),
        Some(r#"["00ff",{"workid":"\u0041"}]"#),
        Some(r#"["00ff",{"workid":"ab"} ]"#),
    ];

    let mut parsed = std::collections::BTreeMap::<String, usize>::new();
    for version in versions {
        for id in ids {
            for method in methods {
                for param in params {
                    for extra in [false, true] {
                        let mut fields = vec![format!(r#""method":{method}"#)];
                        fields.extend(version.map(|version| format!(r#""jsonrpc":{version}"#)));
                        fields.extend(id.map(|id| format!(r#""id":{id}"#)));
                        fields.extend(param.map(|params| format!(r#""params":{params}"#)));
                        if extra {
                            fields.push(r#""extra":0"#.to_string());
                        }
                        // Try every field order by rotating, and the reverse of each rotation.
                        for rotation in 0..fields.len() {
                            for reverse in [false, true] {
                                let mut fields = fields.clone();
                                fields.rotate_left(rotation);
                                if reverse {
                                    fields.reverse();
                                }
                                let request = format!("{{{}}}", fields.join(","));

                                let JsonRpc2Request::Parsed { version, body } =
                                    request_to_json_rpc_2(request.as_bytes())
                                else {
                                    continue;
                                };
                                let (converted_version, converted) =
                                    convert_to_json_rpc_2(request.as_bytes());
                                assert_eq!(version, converted_version, "{request}");
                                assert_eq!(
                                    describe(&body),
                                    describe(&Body::parse(&converted, true, 1)),
                                    "{request}",
                                );
                                let kind = match body {
                                    Body::Call(_) => "call",
                                    Body::Notification(_) => "notification",
                                    _ => panic!("{request} is parsed as neither call"),
                                };
                                *parsed.entry(format!("{version:?} {kind}")).or_default() += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    for expected in [
        "TwoPointZero call",
        "TwoPointZero notification",
        "Lightwalletd call",
        "Lightwalletd notification",
        "Bitcoind call",
        "Bitcoind notification",
    ] {
        assert!(parsed.contains_key(expected), "no {expected}: {parsed:?}");
    }
}

/// Legacy requests with unrepresentable numeric IDs retain their parse-error responses.
#[test]
fn legacy_overflow_ids_keep_parse_errors() {
    for version in ["", r#""jsonrpc":"1.0","#] {
        for id in ["1e400", "-1e400"] {
            for params in ["[]", r#"["00ff",{"workid":"ab"}]"#] {
                let request =
                    format!(r#"{{{version}"method":"submitblock","id":{id},"params":{params}}}"#);
                let (version, body) = converted(&request);
                assert_eq!(version, JsonRpcVersion::Unknown, "{request}");
                let Body::Error(response) = Body::parse(&body, true, 1) else {
                    panic!("{request} must return an error, not become a notification");
                };
                assert_eq!(
                    response_from_json_rpc_2(version, response),
                    r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}"#,
                    "{request}",
                );
            }
        }
    }
}

/// A block's hex in `submitblock` is parsed in place, without copying or re-encoding the body,
/// with or without a work ID, and from JSON-RPC 2.0, 1.0, and bitcoind-style clients.
#[test]
fn submitblock_hex_is_parsed_in_place() {
    let block = "00".repeat(2_000_000);
    for (fields, expected_version) in [
        (r#""jsonrpc": "2.0""#, JsonRpcVersion::TwoPointZero),
        (r#""jsonrpc":"1.0""#, JsonRpcVersion::Lightwalletd),
        (r#""x":0"#, JsonRpcVersion::Bitcoind),
    ] {
        for work_id in ["", r#",{"workid":"3f9a"}"#] {
            // A bitcoind-style call has no `jsonrpc` field, or any other.
            let fields = fields.replace(r#""x":0"#, "");
            let separator = if fields.is_empty() { "" } else { ", " };
            let request = format!(
                r#"{{"id": 3, "method": "submitblock"{separator}{fields}, "params": ["{block}"{work_id}]}}"#
            );

            let JsonRpc2Request::Parsed {
                version,
                body: Body::Call(call),
            } = request_to_json_rpc_2(request.as_bytes())
            else {
                panic!("a submitblock call with {fields:?} and {work_id:?} is parsed as sent");
            };
            assert_eq!(version, expected_version);
            let params = call.params.expect("the call has parameters");
            let params = params.get().as_bytes().as_ptr_range();
            let body = request.as_bytes().as_ptr_range();
            assert!(body.start <= params.start && params.end <= body.end);
            assert_eq!(call.method, "submitblock");
        }
    }
}

/// A plain call's response goes back exactly as the server sent it.
#[test]
fn json_rpc_2_responses_are_unchanged() {
    let response = r#"{"jsonrpc":"2.0","id":3,"result":null}"#;
    assert_eq!(
        json_from_json_rpc_2(JsonRpcVersion::TwoPointZero, response.to_string(), None),
        response
    );
}

/// A near-limit numeric array with a noncanonical final number keeps the conversion behavior.
#[test]
fn large_numeric_array_is_normalized() {
    let numbers = "0,".repeat(1_999_999);
    let request = format!(
        r#"{{"id": 1, "method": "submitblock", "jsonrpc": "2.0", "params": [{numbers}1e2]}}"#
    );
    let expected =
        format!(r#"{{"jsonrpc":"2.0","method":"submitblock","params":[{numbers}100.0],"id":1}}"#);

    assert_eq!(
        converted(&request),
        (JsonRpcVersion::TwoPointZero, expected.into_bytes())
    );
}

/// JSON-RPC 2.0 calls that the server would read differently from their re-encoded form are
/// still re-encoded before the server parses them.
#[test]
fn json_rpc_2_calls_the_server_reads_differently_are_still_re_encoded() {
    for (request, forwarded) in [
        (
            r#"{"id": 1, "method": "getinfo", "jsonrpc": "2.0", "params": [0, 0]}"#,
            r#"{"jsonrpc":"2.0","method":"getinfo","params":[0,0],"id":1}"#,
        ),
        (
            r#"{"id": 1, "method": "submitblock", "jsonrpc": "2.0", "params": ["00", {"workid": "a"}]}"#,
            r#"{"jsonrpc":"2.0","method":"submitblock","params":["00",{"workid":"a"}],"id":1}"#,
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
        assert_eq!(
            converted(request),
            (JsonRpcVersion::TwoPointZero, forwarded.as_bytes().to_vec()),
            "{request} should reach the server as {forwarded}"
        );
    }
}

/// A body that isn't a call from a known version reaches the server unchanged.
#[test]
fn unknown_bodies_are_unchanged() {
    for request in [
        "not json",
        "[]",
        r#"[{"jsonrpc":"2.0","method":"getinfo","id":1}]"#,
        r#"{"jsonrpc":"1.1","method":"getinfo","params":[],"id":1}"#,
        r#"{"method":"getinfo","params":[]}"#,
    ] {
        assert_eq!(
            converted(request),
            (JsonRpcVersion::Unknown, request.as_bytes().to_vec())
        );
    }
}

/// Maps `response` to `version`, and parses it.
fn response_in_version(version: JsonRpcVersion, response: &str) -> serde_json::Value {
    serde_json::from_str(&json_from_json_rpc_2(version, response.to_string(), None))
        .expect("the mapped response is JSON")
}

/// A bitcoind-style request without a `jsonrpc` field reaches the server as JSON-RPC 2.0 with
/// the same method, parameters and ID, and the response goes back without a `jsonrpc` field.
#[test]
fn bitcoind_request_is_rewritten_to_json_rpc_2() {
    let request = r#"{"method":"submitblock","params":["00ff"],"id":"miner"}"#;
    let (version, received) = convert_to_json_rpc_2(request.as_bytes());

    assert_eq!(version, JsonRpcVersion::Bitcoind);
    assert!(matches!(
        request_to_json_rpc_2(request.as_bytes()),
        JsonRpc2Request::Parsed {
            version: JsonRpcVersion::Bitcoind,
            ..
        }
    ));
    let received: serde_json::Value =
        serde_json::from_slice(&received).expect("the rewritten request is JSON");
    assert_eq!(
        received,
        serde_json::json!({"jsonrpc": "2.0", "method": "submitblock", "params": ["00ff"], "id": "miner"})
    );
    assert_eq!(
        response_in_version(version, r#"{"jsonrpc":"2.0","id":"miner","result":null}"#),
        serde_json::json!({"id": "miner", "result": null, "error": null})
    );
}

/// A lightwalletd-style `"jsonrpc": "1.0"` request reaches the server as JSON-RPC 2.0, and the
/// response goes back labelled 1.0.
#[test]
fn lightwalletd_request_is_rewritten_to_json_rpc_2() {
    let request = r#"{"jsonrpc":"1.0","method":"getinfo","params":[],"id":7}"#;
    let (version, received) = convert_to_json_rpc_2(request.as_bytes());

    assert_eq!(version, JsonRpcVersion::Lightwalletd);
    assert!(matches!(
        request_to_json_rpc_2(request.as_bytes()),
        JsonRpc2Request::Parsed {
            version: JsonRpcVersion::Lightwalletd,
            ..
        }
    ));
    let received: serde_json::Value =
        serde_json::from_slice(&received).expect("the rewritten request is JSON");
    assert_eq!(
        received,
        serde_json::json!({"jsonrpc": "2.0", "method": "getinfo", "params": [], "id": 7})
    );
    assert_eq!(
        response_in_version(
            version,
            r#"{"jsonrpc":"2.0","id":7,"result":{"version":1}}"#
        ),
        serde_json::json!({"jsonrpc": "1.0", "id": 7, "result": {"version": 1}, "error": null})
    );
}

/// Successful responses are mapped to JSON-RPC 1.0 and bitcoind clients without parsing their
/// result, and the bytes are the same as parsing and re-encoding the whole response.
#[test]
fn success_responses_map_without_parsing_like_re_encoding() {
    let ids = [
        Id::Null,
        Id::Number(0),
        Id::Number(u64::MAX),
        Id::Str("miner".into()),
        Id::Str("quote \" backslash \\ newline \n tab \t nul \u{0} é 😀".into()),
    ];
    let results = [
        serde_json::json!(null),
        serde_json::json!("00ff".repeat(1000)),
        serde_json::json!({"hash": "ab", "tx": ["01", "02"], "size": 7, "nested": {"x": [1.5, -2]}}),
        serde_json::json!([true, false, "a\"b"]),
    ];

    for id in &ids {
        for result in &results {
            for version in [
                JsonRpcVersion::Lightwalletd,
                JsonRpcVersion::Bitcoind,
                JsonRpcVersion::TwoPointZero,
                JsonRpcVersion::Unknown,
            ] {
                let response = MethodResponse::response(id.clone(), &Ok(result), usize::MAX);
                let re_encoded =
                    json_from_json_rpc_2(version, response.as_json().to_string(), None);
                assert_eq!(
                    response_from_json_rpc_2(version, response),
                    re_encoded,
                    "{version:?} response with id {id:?} and result {result}",
                );
            }
        }
    }
}

/// Error responses keep being mapped by parsing and re-encoding them.
#[test]
fn error_responses_map_like_re_encoding() {
    for version in [JsonRpcVersion::Lightwalletd, JsonRpcVersion::Bitcoind] {
        let response = MethodResponse::error(
            Id::Number(4),
            crate::server::jsonrpc::ErrorObject::new(-8, "Block not found"),
        );
        let re_encoded = json_from_json_rpc_2(version, response.as_json().to_string(), None);
        assert_eq!(response_from_json_rpc_2(version, response), re_encoded);
    }
}
