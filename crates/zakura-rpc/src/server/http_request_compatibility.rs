//! Compatibility fixes for JSON-RPC HTTP requests.
//!
//! [`HttpRequestMiddleware`] fixes HTTP headers and authenticates requests before the body is
//! read. [`request_to_json_rpc_2`] and [`response_from_json_rpc_2`] map other JSON-RPC versions
//! to and from the 2.0 that the server speaks, as the server parses each body.

use std::{borrow::Cow, future::Future};

use std::pin::Pin;

use futures::{future, FutureExt};
use http_body_util::{BodyExt, Limited};
use hyper::header;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tower::BoxError;
use tower::Service;

use super::{
    cookie::Cookie,
    http::{HttpBody, HttpRequest, HttpResponse},
    jsonrpc::{Body, ErrorObject, Id, Notification, Request, TwoPointZero},
};

use base64::{engine::general_purpose::STANDARD, Engine as _};

/// HTTP [`HttpRequestMiddleware`] with compatibility workarounds.
///
/// This middleware makes the following changes to HTTP requests:
///
/// ### Read the whole body
///
/// Reads the body, up to the request size limit, before passing it on. A body that is too long
/// or can't be received fails the request without a response.
///
/// ### Add missing `content-type` HTTP header
///
/// Some RPC clients don't include a `content-type` HTTP header.
/// But unlike web browsers, the RPC server does not do content sniffing.
///
/// If there is no `content-type` header, we assume the content is JSON,
/// and let the parser error if we are incorrect.
///
/// ### Authenticate incoming requests
///
/// If the cookie-based RPC authentication is enabled, check that the incoming request contains the
/// authentication cookie.
///
/// This enables compatibility with `zcash-cli`.
///
/// ### Reject browser requests
///
/// If the listener rejects browser requests, refuse any request with an `Origin` header.
/// See [`HttpRequestMiddlewareLayer::reject_browser_requests`].
///
/// ## Security
///
/// Any user-specified data in RPC requests is hex or base58check encoded.
/// We assume lightwalletd validates data encodings before sending it on to Zebra.
/// So any fixes Zebra performs won't change user-specified data.
#[derive(Clone, Debug)]
pub struct HttpRequestMiddleware<S> {
    service: S,
    cookie: Option<Cookie>,
    max_request_body_size: usize,
    reject_browser_requests: bool,
}

impl<S> HttpRequestMiddleware<S> {
    /// Create a new `HttpRequestMiddleware` with the given service, cookie, and request body size limit.
    pub fn new(service: S, cookie: Option<Cookie>, max_request_body_size: usize) -> Self {
        Self {
            service,
            cookie,
            max_request_body_size,
            reject_browser_requests: false,
        }
    }

    /// Returns whether a web browser sent the request.
    ///
    /// Browsers add `Origin` to every `POST`, including same-origin ones, and page scripts
    /// can't remove it. Command-line tools and RPC client libraries don't send it.
    fn is_browser_request(headers: &header::HeaderMap) -> bool {
        headers.contains_key(header::ORIGIN)
    }

    /// Check if the request is authenticated.
    pub fn check_credentials(&self, headers: &header::HeaderMap) -> bool {
        self.cookie.as_ref().is_none_or(|internal_cookie| {
            headers
                .get(header::AUTHORIZATION)
                .and_then(|auth_header| auth_header.to_str().ok())
                .and_then(|auth_header| auth_header.split_whitespace().nth(1))
                .and_then(|encoded| STANDARD.decode(encoded).ok())
                .and_then(|decoded| String::from_utf8(decoded).ok())
                .and_then(|request_cookie| request_cookie.split(':').nth(1).map(String::from))
                .is_some_and(|passwd| internal_cookie.authenticate(passwd))
        })
    }

    /// Insert or replace client supplied `content-type` HTTP header to `application/json` in the following cases:
    ///
    /// - no `content-type` supplied.
    /// - supplied `content-type` start with `text/plain`, for example:
    ///   - `text/plain`
    ///   - `text/plain;`
    ///   - `text/plain; charset=utf-8`
    ///
    /// `application/json` is the only `content-type` accepted by the Zebra rpc endpoint:
    ///
    /// <https://github.com/paritytech/jsonrpc/blob/38af3c9439aa75481805edf6c05c6622a5ab1e70/http/src/handler.rs#L582-L584>
    ///
    /// # Security
    ///
    /// - `content-type` headers exist so that applications know they are speaking the correct protocol with the correct format.
    ///   We can be a bit flexible, but there are some types (such as binary) we shouldn't allow.
    ///   In particular, the "application/x-www-form-urlencoded" header should be rejected, so browser forms can't be used to attack
    ///   a local RPC port. See "The Role of Routers in the CSRF Attack" in
    ///   <https://www.invicti.com/blog/web-security/importance-content-type-header-http-requests/>
    /// - Checking all the headers is secure, but only because hyper has custom code that just reads the first content-type header.
    ///   <https://github.com/hyperium/headers/blob/f01cc90cf8d601a716856bc9d29f47df92b779e4/src/common/content_type.rs#L102-L108>
    pub fn insert_or_replace_content_type_header(headers: &mut header::HeaderMap) {
        if !headers.contains_key(header::CONTENT_TYPE)
            || headers
                .get(header::CONTENT_TYPE)
                .filter(|value| {
                    value
                        .to_str()
                        .ok()
                        .unwrap_or_default()
                        .starts_with("text/plain")
                })
                .is_some()
        {
            headers.insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("application/json"),
            );
        }
    }
}

/// Implement the Layer for HttpRequestMiddleware to allow injecting the cookie
#[derive(Clone)]
pub struct HttpRequestMiddlewareLayer {
    cookie: Option<Cookie>,
    max_request_body_size: usize,
    reject_browser_requests: bool,
}

impl HttpRequestMiddlewareLayer {
    /// Create a new `HttpRequestMiddlewareLayer` with the given cookie and request body size limit.
    pub fn new(cookie: Option<Cookie>, max_request_body_size: usize) -> Self {
        Self {
            cookie,
            max_request_body_size,
            reject_browser_requests: false,
        }
    }

    /// Rejects every request that has an `Origin` header with `403 Forbidden`.
    ///
    /// # Security
    ///
    /// A listener without credentials trusts whoever can reach it. A web browser on a trusted
    /// machine can reach it on behalf of any page the user opens: a cross-site `text/plain`
    /// `POST` needs no CORS preflight, and DNS rebinding makes the listener look same-origin,
    /// so the page can also read responses. Browsers always send `Origin` on `POST`, so
    /// rejecting it stops both attacks without affecting non-browser clients.
    pub fn reject_browser_requests(mut self) -> Self {
        self.reject_browser_requests = true;
        self
    }
}

impl<S> tower::Layer<S> for HttpRequestMiddlewareLayer {
    type Service = HttpRequestMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        HttpRequestMiddleware {
            reject_browser_requests: self.reject_browser_requests,
            ..HttpRequestMiddleware::new(service, self.cookie.clone(), self.max_request_body_size)
        }
    }
}

impl<S, B> Service<HttpRequest<B>> for HttpRequestMiddleware<S>
where
    S: Service<HttpRequest, Response = HttpResponse> + std::clone::Clone + Send + 'static,
    S::Error: Into<BoxError> + 'static,
    S::Future: Send + 'static,
    B: hyper::body::Body<Data = hyper::body::Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, mut request: HttpRequest<B>) -> Self::Future {
        if self.reject_browser_requests && Self::is_browser_request(request.headers()) {
            let mut response = HttpResponse::new(HttpBody::from(
                "browser requests are not accepted on this RPC listener",
            ));
            *response.status_mut() = hyper::StatusCode::FORBIDDEN;
            return future::ok(response).boxed();
        }

        // Check if the request is authenticated
        if !self.check_credentials(request.headers_mut()) {
            let error = ErrorObject::new(401, "unauthenticated method");
            // TODO: Error object is not being returned to the user but an empty response.
            return future::err(BoxError::from(error)).boxed();
        }

        // Fix the request headers.
        Self::insert_or_replace_content_type_header(request.headers_mut());

        let mut service = self.service.clone();
        let max_request_body_size = self.max_request_body_size;

        async move {
            let (parts, body) = request.into_parts();
            let body = Limited::new(body, max_request_body_size)
                .collect()
                .await?
                .to_bytes();
            let request = HttpRequest::from_parts(parts, HttpBody::from(body));
            service.call(request).await.map_err(Into::into)
        }
        .boxed()
    }
}

/// The JSON-RPC version of a client, which its responses are mapped back to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum JsonRpcVersion {
    /// bitcoind used a mishmash of 1.0, 1.1, and 2.0 for its JSON-RPC.
    Bitcoind,
    /// lightwalletd uses the above mishmash, but also breaks spec to include a
    /// `"jsonrpc": "1.0"` key.
    Lightwalletd,
    /// The client is indicating strict 2.0 handling.
    TwoPointZero,
    /// On parse errors we don't modify anything, and let the JSON-RPC server handle it.
    Unknown,
}

/// A request body mapped to JSON-RPC 2.0.
pub(super) enum JsonRpc2Request<'a> {
    /// A plain JSON-RPC 2.0 call, already parsed from the body as sent.
    Plain(Body<'a>),
    /// Any other body, re-encoded as JSON-RPC 2.0 if it is a call from a known version.
    Converted {
        /// The client's version, for [`response_from_json_rpc_2`].
        version: JsonRpcVersion,
        /// The body for the server to parse.
        body: Cow<'a, [u8]>,
    },
}

/// Maps a request body from whatever JSON-RPC version the client is using to JSON-RPC 2.0.
///
/// A plain JSON-RPC 2.0 call is parsed once, here, into the [`Body`] the server would parse
/// from it, so a block's hex in `submitblock` is read once instead of twice. Any other body is
/// re-encoded as JSON-RPC 2.0 for the server to parse, after removing a JSON-RPC 1.0 `jsonrpc`
/// field. That field was only added in JSON-RPC 2.0:
/// <http://www.simple-is-better.org/rpc/#differences-between-1-0-and-2-0>
///
/// A body that isn't a call from a known version is passed on unchanged.
pub(super) fn request_to_json_rpc_2(body: &[u8]) -> JsonRpc2Request<'_> {
    if let Some(call) = parse_plain_json_rpc_2_call(body) {
        return JsonRpc2Request::Plain(call);
    }

    let Ok(request) = serde_json::from_slice::<'_, JsonRpcRequest>(body) else {
        return JsonRpc2Request::Converted {
            version: JsonRpcVersion::Unknown,
            body: Cow::Borrowed(body),
        };
    };
    let version = request.version();
    let body = if version == JsonRpcVersion::Unknown {
        Cow::Borrowed(body)
    } else {
        Cow::Owned(
            serde_json::to_vec(&request.into_2())
                .expect("values parsed from JSON serialize back to JSON"),
        )
    };
    JsonRpc2Request::Converted { version, body }
}

/// Maps a JSON-RPC 2.0 response to the client's JSON-RPC `version`.
pub(super) fn response_from_json_rpc_2(version: JsonRpcVersion, json: Box<RawValue>) -> String {
    let json = String::from(Box::<str>::from(json));
    if version == JsonRpcVersion::TwoPointZero {
        // The server already answers in JSON-RPC 2.0, with a `result` or an `error`.
        return json;
    }

    match serde_json::from_str::<'_, JsonRpcResponse>(&json) {
        Ok(response) => serde_json::to_string(&response.into_version(version))
            .expect("values parsed from JSON serialize back to JSON"),
        Err(_) => json,
    }
}

/// Parses `body` if it is a JSON-RPC 2.0 call that [`JsonRpcRequest::into_2`] would only
/// re-encode, returning the [`Body`] that the server would parse from it.
///
/// The server reads a few things differently from their re-encoded form: whitespace inside
/// positional parameters, duplicate or reordered named parameters, leading whitespace, a `null`
/// ID (which re-encoding drops) and unknown fields (which re-encoding drops). So this accepts
/// only a body that starts with `{`, has no other fields, has a non-`null` ID or none, and has
/// no parameters, an empty array, or one unescaped string argument. This avoids copying and
/// re-encoding a block's hex in `submitblock` without inspecting each array element.
fn parse_plain_json_rpc_2_call(body: &[u8]) -> Option<Body<'_>> {
    if body.first() != Some(&b'{') {
        return None;
    }

    let call = serde_json::from_slice::<'_, PlainJsonRpc2Call<'_>>(body).ok()?;

    if call.jsonrpc != "2.0" || !call.params.is_none_or(params_can_pass_through_unchanged) {
        return None;
    }
    let params = call.params.map(Cow::Borrowed);

    let Some(id) = call.id else {
        // Like the server, a call without an ID is a notification.
        return Some(Body::Notification(Notification {
            jsonrpc: TwoPointZero,
            method: call.method,
            params,
        }));
    };

    // RawValue is valid JSON; the first byte distinguishes strings and numbers.
    if !matches!(id.get().as_bytes().first(), Some(b'"' | b'-' | b'0'..=b'9')) {
        return None;
    }

    // Like the server, a call whose ID is a number that isn't a `u64` is a notification.
    Some(match serde_json::from_str::<Id<'_>>(id.get()) {
        Ok(id) => Body::Call(Request {
            jsonrpc: TwoPointZero,
            id,
            method: call.method,
            params,
        }),
        Err(_) => Body::Notification(Notification {
            jsonrpc: TwoPointZero,
            method: call.method,
            params,
        }),
    })
}

/// Returns whether parameters are `[]` or one compact, unescaped string argument.
///
/// `RawValue` already contains valid JSON. Checking its delimiters and string contents avoids
/// allocating per argument or parsing and serializing values before falling back to conversion.
fn params_can_pass_through_unchanged(params: &RawValue) -> bool {
    let json = params.get();

    if json == "[]" {
        return true;
    }

    let Some(value) = json
        .strip_prefix("[\"")
        .and_then(|value| value.strip_suffix("\"]"))
    else {
        return false;
    };

    // A block's hex is megabytes long, and a vectorized search scans it far faster than a byte
    // loop.
    memchr::memchr2(b'"', b'\\', value.as_bytes()).is_none()
}

/// A JSON-RPC 2.0 call with only the standard fields, borrowed from the request body.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PlainJsonRpc2Call<'a> {
    #[serde(borrow)]
    jsonrpc: Cow<'a, str>,
    /// Required, so that a call without a method isn't treated as a plain call.
    #[serde(borrow)]
    method: Cow<'a, str>,
    #[serde(borrow, default)]
    params: Option<&'a RawValue>,
    /// `Some` whenever the field is present, including `null`.
    #[serde(borrow, default, deserialize_with = "present")]
    id: Option<&'a RawValue>,
}

/// Deserializes a field that is present, even if it is `null`, as `Some`.
fn present<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<&'de RawValue>, D::Error> {
    <&RawValue>::deserialize(deserializer).map(Some)
}

/// A version-agnostic JSON-RPC request.
#[derive(Debug, Deserialize, Serialize)]
struct JsonRpcRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    jsonrpc: Option<String>,
    method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<serde_json::Value>,
}

impl JsonRpcRequest {
    fn version(&self) -> JsonRpcVersion {
        match (self.jsonrpc.as_deref(), &self.params, &self.id) {
            (
                Some("2.0"),
                _,
                None
                | Some(
                    serde_json::Value::Null
                    | serde_json::Value::String(_)
                    | serde_json::Value::Number(_),
                ),
            ) => JsonRpcVersion::TwoPointZero,
            (Some("1.0"), Some(_), Some(_)) => JsonRpcVersion::Lightwalletd,
            (None, Some(_), Some(_)) => JsonRpcVersion::Bitcoind,
            _ => JsonRpcVersion::Unknown,
        }
    }

    fn into_2(mut self) -> Self {
        self.jsonrpc = Some("2.0".into());
        self
    }
}
/// A version-agnostic JSON-RPC response.
#[derive(Debug, Deserialize, Serialize)]
struct JsonRpcResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    jsonrpc: Option<String>,
    id: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Box<serde_json::value::RawValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<serde_json::Value>,
}

impl JsonRpcResponse {
    fn into_version(mut self, version: JsonRpcVersion) -> Self {
        match version {
            JsonRpcVersion::Bitcoind => {
                self.jsonrpc = None;
                self.result = self
                    .result
                    .or_else(|| serde_json::value::to_raw_value(&()).ok());
                self.error = self.error.or(Some(serde_json::Value::Null));
            }
            JsonRpcVersion::Lightwalletd => {
                self.jsonrpc = Some("1.0".into());
                self.result = self
                    .result
                    .or_else(|| serde_json::value::to_raw_value(&()).ok());
                self.error = self.error.or(Some(serde_json::Value::Null));
            }
            JsonRpcVersion::TwoPointZero | JsonRpcVersion::Unknown => (),
        }
        self
    }
}
