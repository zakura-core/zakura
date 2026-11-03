//! Entry points for the fuzz targets in `qa/fuzz/rpc`.

use std::convert::Infallible;

use bytes::Bytes;
use http_body_util::StreamBody;
use hyper::{body::Frame, header::HeaderMap};

use super::{
    http::{read_body, MAX_REQUEST_BODY_SIZE},
    rpc_service::{handle_rpc_call, RpcMethods, RpcService},
    MAX_RPC_BATCH_REQUESTS,
};
use crate::methods::DecodeParamsOnly;

/// Answers a JSON-RPC request body the way the RPC server does, but without running methods.
///
/// The body goes through the server's body reader, request parser, batch handling and response
/// encoding, and each call's parameters are decoded for the method it names. Panics if any of
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
        let Ok((body, is_single)) = read_body(&HeaderMap::new(), body, MAX_REQUEST_BODY_SIZE).await
        else {
            return;
        };

        let service = RpcService::new(
            DecodeParamsOnly,
            DecodeParamsOnly::METHOD_NAMES.iter().copied(),
            max_response_body_size,
        );
        let response = handle_rpc_call(
            &body,
            is_single,
            &service,
            MAX_RPC_BATCH_REQUESTS,
            max_response_body_size,
        )
        .await;

        let json = response.as_json().get();
        serde_json::from_str::<serde_json::Value>(json)
            .unwrap_or_else(|error| panic!("response is not valid JSON: {error}: {json}"));
    });
}
