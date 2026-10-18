//! Answering JSON-RPC requests over HTTP.
//!
//! The status codes, headers, and bodies match the HTTP-only server of `jsonrpsee` 0.26, which
//! Zakura used before.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::{Buf, Bytes};
use http_body_util::{BodyExt, Full, Limited};
use hyper::{header, Method, StatusCode};
use tokio::sync::Semaphore;
use tower::{BoxError, Service};

use super::{
    jsonrpc::{ErrorCode, ErrorObject, Id, MethodResponse},
    rpc_service::{handle_rpc_call, RpcServiceT},
};

/// The body of HTTP requests passed to [`JsonRpcHttpService`], and of its responses.
pub type HttpBody = Full<Bytes>;

/// An HTTP request.
pub type HttpRequest<B = HttpBody> = hyper::Request<B>;

/// An HTTP response.
pub type HttpResponse<B = HttpBody> = hyper::Response<B>;

/// The largest request body this service reads.
///
/// [`HttpRequestMiddleware`](super::http_request_compatibility::HttpRequestMiddleware) applies a
/// smaller limit first.
pub(super) const MAX_REQUEST_BODY_SIZE: u32 = 10 * 1024 * 1024;

/// The maximum number of requests a server handles at the same time.
const MAX_CONCURRENT_REQUESTS: usize = 100;

/// The content type of JSON responses.
const JSON: &str = "application/json; charset=utf-8";

/// The content type of plain text responses.
const TEXT: &str = "text/plain";

/// An HTTP service that answers JSON-RPC requests with an [`RpcServiceT`].
#[derive(Clone)]
pub struct JsonRpcHttpService<S> {
    rpc_service: S,
    request_permits: Arc<Semaphore>,
    max_batch_entries: usize,
    max_response_body_size: usize,
}

impl<S: RpcServiceT> JsonRpcHttpService<S> {
    /// Returns a service that answers requests with `rpc_service`.
    ///
    /// Batches can have up to `max_batch_entries` entries, and batch responses can be up to
    /// `max_response_body_size` bytes long. Clones of this service share a limit on the number
    /// of requests handled at the same time.
    pub fn new(rpc_service: S, max_batch_entries: usize, max_response_body_size: usize) -> Self {
        Self {
            rpc_service,
            request_permits: Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS)),
            max_batch_entries,
            max_response_body_size,
        }
    }
}

impl<S, B> Service<HttpRequest<B>> for JsonRpcHttpService<S>
where
    S: RpcServiceT,
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    type Response = HttpResponse;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<HttpResponse, BoxError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: HttpRequest<B>) -> Self::Future {
        let Ok(permit) = self.request_permits.clone().try_acquire_owned() else {
            return Box::pin(async { Ok(too_many_requests()) });
        };

        let this = self.clone();
        Box::pin(async move {
            let response = this.answer(request).await;
            drop(permit);
            Ok(response)
        })
    }
}

impl<S: RpcServiceT> JsonRpcHttpService<S> {
    /// Answers a JSON-RPC request.
    async fn answer<B>(&self, request: HttpRequest<B>) -> HttpResponse
    where
        B: hyper::body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<BoxError>,
    {
        if request.method() != Method::POST {
            return text_response(
                StatusCode::METHOD_NOT_ALLOWED,
                "Used HTTP Method is not allowed. POST is required\n",
            );
        }
        if !is_json(request.headers().get(header::CONTENT_TYPE)) {
            return text_response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Supplied content type is not allowed. Content-Type: application/json is required\n",
            );
        }

        let (parts, body) = request.into_parts();
        let (body, is_single) = match read_body(&parts.headers, body, MAX_REQUEST_BODY_SIZE).await {
            Ok(body) => body,
            Err(ReadBodyError::TooLarge) => {
                let response = MethodResponse::too_large_request(MAX_REQUEST_BODY_SIZE);
                return json_response(StatusCode::PAYLOAD_TOO_LARGE, response);
            }
            Err(ReadBodyError::Malformed) => {
                let response = MethodResponse::error(Id::Null, ErrorCode::ParseError.into());
                return json_response(StatusCode::BAD_REQUEST, response);
            }
            Err(ReadBodyError::Stream(error)) => {
                tracing::warn!("Internal error reading request body: {error}");
                let response =
                    MethodResponse::error(Id::Null, ErrorObject::from(ErrorCode::InternalError));
                return json_response(StatusCode::INTERNAL_SERVER_ERROR, response);
            }
        };

        let response = handle_rpc_call(
            &body,
            is_single,
            &self.rpc_service,
            self.max_batch_entries,
            self.max_response_body_size,
        )
        .await;

        json_response(StatusCode::OK, response)
    }
}

/// Whether `content_type` is one of the JSON content types the server accepts.
fn is_json(content_type: Option<&header::HeaderValue>) -> bool {
    content_type
        .and_then(|value| value.to_str().ok())
        .is_some_and(|content_type| {
            [
                "application/json",
                "application/json; charset=utf-8",
                "application/json;charset=utf-8",
                "application/json-rpc",
                "application/json-rpc;charset=utf-8",
                "application/json-rpc; charset=utf-8",
            ]
            .iter()
            .any(|json| content_type.eq_ignore_ascii_case(json))
        })
}

/// Why a request body couldn't be read.
pub(super) enum ReadBodyError {
    /// The body is longer than the limit.
    TooLarge,
    /// The body doesn't start with `{` or `[`, or is empty.
    Malformed,
    /// The body couldn't be received.
    Stream(BoxError),
}

/// Reads a request body of up to `max_body_size` bytes.
///
/// Returns the body without its leading whitespace, and whether it is a single request, which
/// starts with `{`, rather than a batch, which starts with `[`. The first non-whitespace byte
/// must be in the first 128 bytes of the first chunk of the body.
pub(super) async fn read_body<B>(
    headers: &header::HeaderMap,
    body: B,
    max_body_size: u32,
) -> Result<(Vec<u8>, bool), ReadBodyError>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    let content_length = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0);
    if content_length > max_body_size {
        return Err(ReadBodyError::TooLarge);
    }

    // `max_body_size` is a `u32`, so it fits in a `usize` on supported platforms.
    let max_body_size = max_body_size as usize;
    let mut body = std::pin::pin!(Limited::new(body, max_body_size));
    let mut received = Vec::with_capacity((content_length as usize).min(16 * 1024));
    let mut is_single = None;

    while let Some(frame) = body.frame().await {
        // Like `jsonrpsee`, a body that exceeds the limit without a `content-length` header that
        // says so is an internal error.
        let frame = frame.map_err(ReadBodyError::Stream)?;
        let Some(data) = frame.data_ref() else {
            continue;
        };

        if received.is_empty() {
            let first_non_whitespace = data
                .chunk()
                .iter()
                .enumerate()
                .take(128)
                .find(|(_, byte)| !byte.is_ascii_whitespace());
            let start = match first_non_whitespace {
                Some((index, b'{')) => {
                    is_single = Some(true);
                    index
                }
                Some((index, b'[')) => {
                    is_single = Some(false);
                    index
                }
                _ => return Err(ReadBodyError::Malformed),
            };
            received.extend_from_slice(&data.chunk()[start..]);
        } else {
            received.extend_from_slice(data.chunk());
        }
    }

    match is_single {
        Some(is_single) if !received.is_empty() => Ok((received, is_single)),
        _ => Err(ReadBodyError::Malformed),
    }
}

/// Returns a response with `status` and a JSON-RPC `response` body.
fn json_response(status: StatusCode, response: MethodResponse) -> HttpResponse {
    response_with_body(
        status,
        String::from(Box::<str>::from(response.into_json())),
        JSON,
    )
}

/// Returns a response with `status` and a plain text `body`.
fn text_response(status: StatusCode, body: &'static str) -> HttpResponse {
    response_with_body(status, body, TEXT)
}

/// Returns the response to a request when the server is handling too many requests.
fn too_many_requests() -> HttpResponse {
    text_response(
        StatusCode::TOO_MANY_REQUESTS,
        "Too many connections. Please try again later.",
    )
}

fn response_with_body(
    status: StatusCode,
    body: impl Into<HttpBody>,
    content_type: &'static str,
) -> HttpResponse {
    let mut response = HttpResponse::new(body.into());
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static(content_type),
    );
    response
}
