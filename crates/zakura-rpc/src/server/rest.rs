//! Binary REST endpoints for raw blocks and transactions.
//!
//! zcashd and bitcoind serve `GET /rest/block/<hash>.bin` and
//! `GET /rest/tx/<txid>.bin` when started with `-rest`. These endpoints return the
//! consensus encoding itself, so a client that only needs the bytes avoids the
//! hex encoding, JSON escaping, JSON parsing and hex decoding of `getblock` and
//! `getrawtransaction`, and transfers half as many bytes. `.hex` returns the same
//! bytes as hex text followed by a newline, as zcashd does.
//!
//! The endpoints are enabled by `rpc.enable_rest`. They serve public chain data
//! without authentication, like zcashd's and bitcoind's REST interface.

use std::{
    future::Future,
    pin::Pin,
    str::FromStr,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use http_body_util::Full;
use hyper::{header, Method, StatusCode};
use tower::{BoxError, Service, ServiceExt};

use zakura_chain::{block, transaction};
use zakura_state::{HashOrHeight, ReadRequest, ReadResponse};

use super::http::{HttpRequest, HttpResponse};

/// The path prefix of every REST endpoint.
const REST_PREFIX: &str = "/rest/";

/// How long a REST request waits for the state before answering 503.
const STATE_TIMEOUT: Duration = Duration::from_secs(30);

/// Answers REST requests from the state, and passes every other request to `inner`.
#[derive(Clone)]
pub struct RestRouter<S, ReadState> {
    inner: S,
    /// The state that answers REST requests, or `None` when REST is disabled.
    read_state: Option<ReadState>,
}

impl<S, ReadState> RestRouter<S, ReadState> {
    /// Routes REST requests to `read_state` if it is set, and all other requests to `inner`.
    pub fn new(inner: S, read_state: Option<ReadState>) -> Self {
        Self { inner, read_state }
    }
}

impl<S, B, ReadState> Service<HttpRequest<B>> for RestRouter<S, ReadState>
where
    S: Service<HttpRequest<B>, Response = HttpResponse, Error = BoxError> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
    ReadState: zakura_state::ReadState,
{
    type Response = HttpResponse;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<HttpResponse, BoxError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: HttpRequest<B>) -> Self::Future {
        match &self.read_state {
            Some(read_state) if request.uri().path().starts_with(REST_PREFIX) => {
                let read_state = read_state.clone();
                let method = request.method().clone();
                let path = request.uri().path().to_owned();
                Box::pin(async move { Ok(answer(read_state, &method, &path).await) })
            }
            _ => {
                // `poll_ready` readied this service, so call it and leave a fresh clone.
                let clone = self.inner.clone();
                let mut ready = std::mem::replace(&mut self.inner, clone);
                Box::pin(ready.call(request))
            }
        }
    }
}

/// A response format, from the extension of the requested resource.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Format {
    Binary,
    Hex,
}

/// A parsed REST request.
#[derive(Clone, Debug, PartialEq, Eq)]
enum RestRequest {
    Block(block::Hash, Format),
    Transaction(transaction::Hash, Format),
}

/// Why a path is not a REST request this server answers.
#[derive(Clone, Debug, PartialEq, Eq)]
enum PathError {
    NotFound(String),
    BadRequest(String),
}

/// Parses `path`, which starts with [`REST_PREFIX`].
fn parse_path(path: &str) -> Result<RestRequest, PathError> {
    let resource = path.strip_prefix(REST_PREFIX).unwrap_or(path);
    let (kind, name) = resource
        .split_once('/')
        .ok_or_else(|| PathError::NotFound(format!("unknown REST resource: {resource}")))?;
    let format_not_found =
        || PathError::NotFound("output format not found (available: bin, hex)".into());
    let (id, extension) = name.rsplit_once('.').ok_or_else(format_not_found)?;
    let format = match extension {
        "bin" => Format::Binary,
        "hex" => Format::Hex,
        _ => return Err(format_not_found()),
    };
    let invalid = || PathError::BadRequest(format!("Invalid hash: {id}"));
    match kind {
        "block" => Ok(RestRequest::Block(
            block::Hash::from_str(id).map_err(|_| invalid())?,
            format,
        )),
        "tx" => Ok(RestRequest::Transaction(
            transaction::Hash::from_str(id).map_err(|_| invalid())?,
            format,
        )),
        _ => Err(PathError::NotFound(format!(
            "unknown REST resource: {resource}"
        ))),
    }
}

/// Answers the REST request for `path` from `read_state`.
async fn answer<ReadState: zakura_state::ReadState>(
    read_state: ReadState,
    method: &Method,
    path: &str,
) -> HttpResponse {
    if method != Method::GET {
        return text(StatusCode::METHOD_NOT_ALLOWED, "REST requests must use GET");
    }
    let request = match parse_path(path) {
        Ok(request) => request,
        Err(PathError::NotFound(message)) => return text(StatusCode::NOT_FOUND, &message),
        Err(PathError::BadRequest(message)) => return text(StatusCode::BAD_REQUEST, &message),
    };

    let (read_request, format, name) = match &request {
        RestRequest::Block(hash, format) => (
            ReadRequest::RawBlock(HashOrHeight::Hash(*hash)),
            *format,
            hash.to_string(),
        ),
        RestRequest::Transaction(txid, format) => (
            ReadRequest::AnyChainRawTransaction(*txid),
            *format,
            txid.to_string(),
        ),
    };

    let response = tokio::time::timeout(STATE_TIMEOUT, read_state.oneshot(read_request)).await;
    let bytes = match response {
        Ok(Ok(ReadResponse::RawBlock(Some(bytes)))) => bytes.to_vec(),
        Ok(Ok(ReadResponse::RawTransaction(Some(bytes)))) => bytes.to_vec(),
        Ok(Ok(ReadResponse::RawBlock(None) | ReadResponse::RawTransaction(None))) => {
            return text(StatusCode::NOT_FOUND, &format!("{name} not found"));
        }
        Ok(Ok(_)) => unreachable!("the state answers raw reads with raw responses"),
        Ok(Err(error)) => {
            tracing::debug!(?error, "REST state read failed");
            return text(StatusCode::SERVICE_UNAVAILABLE, "state read failed");
        }
        Err(_) => return text(StatusCode::SERVICE_UNAVAILABLE, "state read timed out"),
    };

    match format {
        Format::Binary => body(StatusCode::OK, bytes, "application/octet-stream"),
        Format::Hex => {
            let mut hex = const_hex::encode(bytes);
            hex.push('\n');
            body(StatusCode::OK, hex.into_bytes(), "text/plain")
        }
    }
}

/// A plain text response with `message` and a final newline.
fn text(status: StatusCode, message: &str) -> HttpResponse {
    body(status, format!("{message}\n").into_bytes(), "text/plain")
}

fn body(status: StatusCode, bytes: Vec<u8>, content_type: &'static str) -> HttpResponse {
    hyper::Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .body(Full::new(Bytes::from(bytes)))
        .expect("the status and content type are valid")
}

#[cfg(test)]
mod tests;
