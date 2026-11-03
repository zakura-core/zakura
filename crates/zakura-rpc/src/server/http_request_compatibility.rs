//! Compatibility fixes for JSON-RPC HTTP requests.
//!
//! These fixes are applied at the HTTP level, before the RPC request is parsed.

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
    jsonrpc::ErrorObject,
};

use base64::{engine::general_purpose::STANDARD, Engine as _};

/// HTTP [`HttpRequestMiddleware`] with compatibility workarounds.
///
/// This middleware makes the following changes to HTTP requests:
///
/// ### Remove `jsonrpc` field in JSON RPC 1.0
///
/// Removes "jsonrpc: 1.0" fields from requests,
/// because the "jsonrpc" field was only added in JSON-RPC 2.0.
///
/// <http://www.simple-is-better.org/rpc/#differences-between-1-0-and-2-0>
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
}

impl<S> HttpRequestMiddleware<S> {
    /// Create a new `HttpRequestMiddleware` with the given service, cookie, and request body size limit.
    pub fn new(service: S, cookie: Option<Cookie>, max_request_body_size: usize) -> Self {
        Self {
            service,
            cookie,
            max_request_body_size,
        }
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

    /// Maps whatever JSON-RPC version the client is using to JSON-RPC 2.0.
    async fn request_to_json_rpc_2<B>(
        request: HttpRequest<B>,
        max_request_body_size: usize,
    ) -> Result<(JsonRpcVersion, HttpRequest<HttpBody>), BoxError>
    where
        B: hyper::body::Body<Data = hyper::body::Bytes> + Send + 'static,
        B::Error: Into<BoxError>,
    {
        let (parts, body) = request.into_parts();
        let bytes = Limited::new(body, max_request_body_size)
            .collect()
            .await?
            .to_bytes();
        if can_forward_json_rpc_2_call_unchanged(&bytes) {
            return Ok((
                JsonRpcVersion::TwoPointZero,
                HttpRequest::from_parts(parts, HttpBody::from(bytes)),
            ));
        }
        let (version, bytes) =
            if let Ok(request) = serde_json::from_slice::<'_, JsonRpcRequest>(bytes.as_ref()) {
                let version = request.version();
                if matches!(version, JsonRpcVersion::Unknown) {
                    (version, bytes)
                } else {
                    (
                        version,
                        serde_json::to_vec(&request.into_2()).expect("valid").into(),
                    )
                }
            } else {
                (JsonRpcVersion::Unknown, bytes)
            };
        Ok((
            version,
            HttpRequest::from_parts(parts, HttpBody::from(bytes)),
        ))
    }
    /// Maps JSON-2.0 to whatever JSON-RPC version the client is using.
    async fn response_from_json_rpc_2(
        version: JsonRpcVersion,
        response: HttpResponse<HttpBody>,
    ) -> Result<HttpResponse<HttpBody>, BoxError> {
        if matches!(version, JsonRpcVersion::TwoPointZero) {
            // The server already answers in JSON-RPC 2.0, with a `result` or an `error`.
            return Ok(response);
        }

        let (parts, body) = response.into_parts();
        let bytes = body.collect().await?.to_bytes();
        let bytes =
            if let Ok(response) = serde_json::from_slice::<'_, JsonRpcResponse>(bytes.as_ref()) {
                serde_json::to_vec(&response.into_version(version))
                    .expect("valid")
                    .into()
            } else {
                bytes
            };
        Ok(HttpResponse::from_parts(parts, HttpBody::from(bytes)))
    }
}

/// Implement the Layer for HttpRequestMiddleware to allow injecting the cookie
#[derive(Clone)]
pub struct HttpRequestMiddlewareLayer {
    cookie: Option<Cookie>,
    max_request_body_size: usize,
}

impl HttpRequestMiddlewareLayer {
    /// Create a new `HttpRequestMiddlewareLayer` with the given cookie and request body size limit.
    pub fn new(cookie: Option<Cookie>, max_request_body_size: usize) -> Self {
        Self {
            cookie,
            max_request_body_size,
        }
    }
}

impl<S> tower::Layer<S> for HttpRequestMiddlewareLayer {
    type Service = HttpRequestMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        HttpRequestMiddleware::new(service, self.cookie.clone(), self.max_request_body_size)
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
            let (version, request) =
                Self::request_to_json_rpc_2(request, max_request_body_size).await?;
            let response = service.call(request).await.map_err(Into::into)?;
            Self::response_from_json_rpc_2(version, response).await
        }
        .boxed()
    }
}

#[derive(Clone, Copy, Debug)]
enum JsonRpcVersion {
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

/// Returns whether `body` is a JSON-RPC 2.0 call that [`JsonRpcRequest::into_2`] would only
/// re-encode, so the server can read it as sent.
///
/// The server reads a few things differently from their re-encoded form: whitespace inside
/// positional parameters, duplicate or reordered named parameters, leading whitespace, a `null`
/// ID (which re-encoding drops) and unknown fields (which re-encoding drops). So this accepts
/// only a body that starts with `{`, has no other fields, has a non-`null` ID or none, and has
/// no parameters, an empty array, or one unescaped string argument. This avoids copying and
/// re-encoding a block's hex in `submitblock` without inspecting each array element.
fn can_forward_json_rpc_2_call_unchanged(body: &[u8]) -> bool {
    if body.first() != Some(&b'{') {
        return false;
    }

    let Ok(call) = serde_json::from_slice::<'_, PlainJsonRpc2Call<'_>>(body) else {
        return false;
    };

    if call.jsonrpc != "2.0" {
        return false;
    }

    let id_is_absent_or_string_or_number = call.id.is_none_or(|id| {
        // RawValue is valid JSON; the first byte distinguishes strings and numbers.
        matches!(id.get().as_bytes().first(), Some(b'"' | b'-' | b'0'..=b'9'))
    });

    id_is_absent_or_string_or_number && call.params.is_none_or(params_can_pass_through_unchanged)
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

    !value.bytes().any(|byte| matches!(byte, b'"' | b'\\'))
}

/// A JSON-RPC 2.0 call with only the standard fields, borrowed from the request body.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PlainJsonRpc2Call<'a> {
    #[serde(borrow)]
    jsonrpc: Cow<'a, str>,
    /// Required, so that a call without a method isn't treated as a plain call.
    #[serde(borrow, rename = "method")]
    _method: Cow<'a, str>,
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
