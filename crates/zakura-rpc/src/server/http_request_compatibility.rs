//! Authentication, compatible content types and bounded HTTP body collection.
//!
//! JSON compatibility processing belongs to the admitted RPC decoder.

use std::future::Future;

use std::pin::Pin;

use futures::{future, FutureExt};
use http_body_util::{BodyExt, Limited};
use hyper::header;
use tower::BoxError;
use tower::Service;

use super::{
    cookie::Cookie,
    http::{HttpBody, HttpRequest, HttpResponse},
    jsonrpc::ErrorObject,
};

use base64::{engine::general_purpose::STANDARD, Engine as _};

/// HTTP boundary that checks credentials, fixes supported content-type headers and
/// collects a bounded body without interpreting JSON. The inner RPC service admits the
/// request before decoding its version, normalizing parameters or formatting a response.
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

    /// Collects a request under the HTTP boundary's body-size limit.
    async fn collect_request<B>(
        request: HttpRequest<B>,
        max_request_body_size: usize,
    ) -> Result<HttpRequest<HttpBody>, BoxError>
    where
        B: hyper::body::Body<Data = hyper::body::Bytes> + Send + 'static,
        B::Error: Into<BoxError>,
    {
        let (mut parts, body) = request.into_parts();
        let bytes = Limited::new(body, max_request_body_size)
            .collect()
            .await?
            .to_bytes();
        parts.extensions.insert(CollectedRequestBody(bytes.clone()));
        Ok(HttpRequest::from_parts(parts, HttpBody::from(bytes)))
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
            let request = Self::collect_request(request, max_request_body_size).await?;
            service.call(request).await.map_err(Into::into)
        }
        .boxed()
    }
}

/// A bounded body collected by the HTTP wrapper, before JSON interpretation.
///
/// The inner service consumes this private extension only after acquiring its request permit.
#[derive(Clone)]
pub(super) struct CollectedRequestBody(pub hyper::body::Bytes);
