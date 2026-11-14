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
    /// A call that re-encoding would not change, parsed once from the body as sent.
    Parsed {
        /// The client's version, for [`response_from_json_rpc_2`].
        version: JsonRpcVersion,
        /// The call the server would parse from the re-encoded body.
        body: Body<'a>,
    },
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
/// A call whose method, parameters and ID re-encoding would not change is parsed once, here,
/// into the [`Body`] the server would parse from its re-encoded form. So a block's hex in
/// `submitblock` is read once, whether or not the call has a work ID or comes from a JSON-RPC
/// 1.0 or bitcoind-style client. Any other body is converted by [`convert_to_json_rpc_2`].
pub(super) fn request_to_json_rpc_2(body: &[u8]) -> JsonRpc2Request<'_> {
    match parse_call_as_sent(body) {
        Some((version, body)) => JsonRpc2Request::Parsed { version, body },
        None => {
            let (version, body) = convert_to_json_rpc_2(body);
            JsonRpc2Request::Converted { version, body }
        }
    }
}

/// Re-encodes a call from a known JSON-RPC version as JSON-RPC 2.0, after removing a JSON-RPC
/// 1.0 `jsonrpc` field. That field was only added in JSON-RPC 2.0:
/// <http://www.simple-is-better.org/rpc/#differences-between-1-0-and-2-0>
///
/// A body that isn't a call from a known version is returned unchanged.
pub(super) fn convert_to_json_rpc_2(body: &[u8]) -> (JsonRpcVersion, Cow<'_, [u8]>) {
    let Ok(request) = serde_json::from_slice::<'_, JsonRpcRequest>(body) else {
        return (JsonRpcVersion::Unknown, Cow::Borrowed(body));
    };
    let version = request.version();
    if version == JsonRpcVersion::Unknown {
        return (version, Cow::Borrowed(body));
    }
    let body = serde_json::to_vec(&request.into_2())
        .expect("values parsed from JSON serialize back to JSON");
    (version, Cow::Owned(body))
}

/// Maps a JSON-RPC 2.0 response to the client's JSON-RPC `version`.
pub(super) fn response_from_json_rpc_2(version: JsonRpcVersion, json: Box<RawValue>) -> String {
    let json = String::from(Box::<str>::from(json));
    let jsonrpc_member = match version {
        // The server already answers in JSON-RPC 2.0, with a `result` or an `error`.
        JsonRpcVersion::TwoPointZero => return json,
        JsonRpcVersion::Bitcoind => Some(""),
        JsonRpcVersion::Lightwalletd => Some(r#""jsonrpc":"1.0","#),
        JsonRpcVersion::Unknown => None,
    };

    // A result can be megabytes long, such as a block template, so it is copied once here
    // rather than parsed and written again.
    if let Some(response) =
        jsonrpc_member.and_then(|member| result_response_with_null_error(&json, member))
    {
        return response;
    }

    match serde_json::from_str::<'_, JsonRpcResponse>(&json) {
        Ok(response) => serde_json::to_string(&response.into_version(version))
            .expect("values parsed from JSON serialize back to JSON"),
        Err(_) => json,
    }
}

/// Returns the server's JSON-RPC 2.0 `response` to a successful call as older clients expect it:
/// `jsonrpc_member` replaces its `"jsonrpc":"2.0",` member, and a null `error` follows its
/// `result`. The ID and the result are copied as they are.
///
/// Returns `None` unless `response` starts the way [`MethodResponse`] writes a result:
/// `{"jsonrpc":"2.0","id":`, the ID, then `,"result":`. Such a response ends with the result and
/// the closing brace. Errors and batches are not written that way.
///
/// [`MethodResponse`]: super::jsonrpc::MethodResponse
fn result_response_with_null_error(response: &str, jsonrpc_member: &str) -> Option<String> {
    const NULL_ERROR: &str = r#","error":null}"#;

    let after_jsonrpc = response.strip_prefix(r#"{"jsonrpc":"2.0","#)?;
    let id_and_result = after_jsonrpc.strip_prefix(r#""id":"#)?;
    let (_id, after_id) = id_and_result.split_at_checked(json_scalar_len(id_and_result)?)?;
    if !after_id.starts_with(r#","result":"#) {
        return None;
    }
    let members = after_jsonrpc.strip_suffix('}')?;

    let mut mapped =
        String::with_capacity(1 + jsonrpc_member.len() + members.len() + NULL_ERROR.len());
    mapped.push('{');
    mapped.push_str(jsonrpc_member);
    mapped.push_str(members);
    mapped.push_str(NULL_ERROR);
    Some(mapped)
}

/// Returns the length of the JSON string, number or `null` that `json` starts with, if another
/// object member follows it.
fn json_scalar_len(json: &str) -> Option<usize> {
    if !json.starts_with('"') {
        return json.find(',');
    }

    let mut is_escaped = false;
    for (index, byte) in json.bytes().enumerate().skip(1) {
        match byte {
            _ if is_escaped => is_escaped = false,
            b'\\' => is_escaped = true,
            b'"' => return index.checked_add(1),
            _ => {}
        }
    }
    None
}

/// Parses `body` if [`convert_to_json_rpc_2`] would only re-encode it without changing what
/// the server reads, returning the client's version and the [`Body`] the server would parse.
///
/// Re-encoding changes some things the server reads: escaped strings and non-canonical numbers
/// in parameters, whitespace inside parameters, duplicate named parameters, a `null` ID (which
/// it drops), unknown fields (which it drops), and leading whitespace. So this accepts only a
/// body that starts with `{`, has no other fields, has a string or number ID or none, and has
/// parameters that re-encode to the same bytes, or none. The method and ID are read the same
/// from either form, and both read `null` parameters as none.
fn parse_call_as_sent(body: &[u8]) -> Option<(JsonRpcVersion, Body<'_>)> {
    if body.first() != Some(&b'{') {
        return None;
    }

    let call = serde_json::from_slice::<'_, CallFields<'_>>(body).ok()?;

    if !call.params.is_none_or(params_re_encode_unchanged) {
        return None;
    }
    // RawValue is valid JSON; the first byte distinguishes strings and numbers.
    if !call
        .id
        .is_none_or(|id| matches!(id.get().as_bytes().first(), Some(b'"' | b'-' | b'0'..=b'9')))
    {
        return None;
    }

    let version = JsonRpcVersion::of_call(
        call.jsonrpc.as_deref(),
        call.params.is_some(),
        call.id.map(|_| IdKind::NullStringOrNumber),
    );
    if version == JsonRpcVersion::Unknown {
        return None;
    }

    let params = call.params.map(Cow::Borrowed);
    // Like the server, a call without an ID, or whose ID is a number that isn't a `u64`, is a
    // notification.
    let body = match call.id.map(|id| serde_json::from_str::<Id<'_>>(id.get())) {
        Some(Ok(id)) => Body::Call(Request {
            jsonrpc: TwoPointZero,
            id,
            method: call.method,
            params,
        }),
        Some(Err(_))
            if call.id.is_some_and(|id| {
                serde_json::from_str::<serde_json::Number>(id.get()).is_err()
            }) =>
        {
            // Conversion rejects unrepresentable numbers. Preserve that error path without
            // parsing ordinary call IDs twice.
            return None;
        }
        None | Some(Err(_)) => Body::Notification(Notification {
            jsonrpc: TwoPointZero,
            method: call.method,
            params,
        }),
    };
    Some((version, body))
}

/// Returns whether re-encoding `params` gives back the same bytes, so the server reads them the
/// same either way.
///
/// A leading unescaped string argument, like a block's hex in `submitblock`, re-encodes
/// unchanged, and a vectorized search checks it far faster than parsing. Any arguments after it,
/// like a work ID, are re-encoded and compared.
fn params_re_encode_unchanged(params: &RawValue) -> bool {
    let json = params.get();
    let rest = match json.strip_prefix("[\"") {
        Some(string_and_rest) => {
            // RawValue is valid JSON, so the string ends at the first `"` unless it has an escape.
            let end = memchr::memchr2(b'"', b'\\', string_and_rest.as_bytes());
            let Some(end) = end.filter(|&end| string_and_rest.as_bytes()[end] == b'"') else {
                return false;
            };
            match &string_and_rest[end + 1..] {
                "]" => return true,
                rest => match rest.strip_prefix(',') {
                    Some(arguments) => Cow::Owned(format!("[{arguments}")),
                    None => return false,
                },
            }
        }
        None => Cow::Borrowed(json),
    };

    serde_json::from_str::<serde_json::Value>(&rest)
        .and_then(|value| serde_json::to_string(&value))
        .is_ok_and(|re_encoded| re_encoded == *rest)
}

/// The standard fields of a call from any JSON-RPC version, borrowed from the request body.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CallFields<'a> {
    #[serde(borrow, default)]
    jsonrpc: Option<Cow<'a, str>>,
    /// Required, so that a body without a method isn't parsed as a call.
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

/// The kind of a call's ID, which its JSON-RPC version depends on.
enum IdKind {
    NullStringOrNumber,
    Other,
}

impl JsonRpcVersion {
    /// Returns the version of a call with a `jsonrpc` field, parameters if `has_params`, and an
    /// `id` of a kind, if it has one.
    fn of_call(jsonrpc: Option<&str>, has_params: bool, id: Option<IdKind>) -> Self {
        match (jsonrpc, has_params, id) {
            (Some("2.0"), _, None | Some(IdKind::NullStringOrNumber)) => Self::TwoPointZero,
            (Some("1.0"), true, Some(_)) => Self::Lightwalletd,
            (None, true, Some(_)) => Self::Bitcoind,
            _ => Self::Unknown,
        }
    }
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
        let id = self.id.as_ref().map(|id| match id {
            serde_json::Value::Null
            | serde_json::Value::String(_)
            | serde_json::Value::Number(_) => IdKind::NullStringOrNumber,
            _ => IdKind::Other,
        });
        JsonRpcVersion::of_call(self.jsonrpc.as_deref(), self.params.is_some(), id)
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
