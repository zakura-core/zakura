//! Entry points for the fuzz targets in `qa/fuzz/rpc`.

use std::convert::Infallible;

use bytes::Bytes;
use http_body_util::BodyExt;
use http_body_util::StreamBody;
use hyper::body::Frame;
use tower::Service;

use super::{
    http::{HttpRequest, JsonRpcHttpService},
    http_request_compatibility::HttpRequestMiddleware,
    rpc_service::{RpcMethods, RpcService},
    MAX_RPC_BATCH_REQUESTS, MAX_RPC_REQUEST_BODY_SIZE,
};
use crate::methods::DecodeParamsOnly;

/// Answers a JSON-RPC request body the way the RPC server does, but without running methods.
///
/// The body goes through the HTTP boundary, admitted compatibility decoder, batch handling and
/// response encoding, and each call's parameters are decoded for the method it names. Panics if any of
/// them panics, or if a response is not valid JSON.
///
/// The first byte of `input` picks the maximum response size, so oversized responses are
/// reached. The second byte splits the rest, which is the body, into two chunks.
pub fn json_rpc_request(input: &[u8]) {
    let [size_selector, split, body @ ..] = input else {
        return;
    };
    let max_response_body_size = 16 << (size_selector % 16);
    let split = usize::from(*split).min(body.len());
    let chunks = [&body[..split], &body[split..]]
        .map(|chunk| Ok::<_, Infallible>(Frame::data(Bytes::copy_from_slice(chunk))));
    let body = StreamBody::new(futures::stream::iter(chunks));

    futures::executor::block_on(async {
        let service = RpcService::new(
            DecodeParamsOnly,
            DecodeParamsOnly::METHOD_NAMES.iter().copied(),
            max_response_body_size,
        );
        let service =
            JsonRpcHttpService::new(service, MAX_RPC_BATCH_REQUESTS, max_response_body_size);
        let mut boundary = HttpRequestMiddleware::new(service, None, MAX_RPC_REQUEST_BODY_SIZE);
        let request = HttpRequest::builder()
            .method("POST")
            .body(body)
            .expect("the fuzz request has a valid HTTP method");
        let Ok(response) = boundary.call(request).await else {
            return;
        };
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("the RPC service returns a full response body")
            .to_bytes();

        serde_json::from_slice::<serde_json::Value>(&bytes)
            .unwrap_or_else(|error| panic!("response is not valid JSON: {error}"));
    });
}
