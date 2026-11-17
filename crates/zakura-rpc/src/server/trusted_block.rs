//! Binary block submission using the primary HTTP listener's authentication policy.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use http_body_util::{BodyExt, Limited};
use hyper::{body::Bytes, header, Method, StatusCode};
use tokio::sync::Semaphore;
use tower::{BoxError, Service};

use super::{
    cookie::Cookie,
    http::{HttpBody, HttpRequest, HttpResponse},
    http_request_compatibility::HttpRequestMiddleware,
};
use crate::methods::types::submit_block::{SubmitBlockErrorResponse, SubmitBlockResponse};

/// Failures before a block is submitted to validation.
pub(crate) enum TrustedSubmissionError {
    /// The payload is not exactly one structurally decodable block.
    Malformed,
    /// All bounded submission or pending-body slots are occupied.
    Capacity,
}

pub(super) type SubmissionFuture =
    Pin<Box<dyn Future<Output = Result<SubmitBlockResponse, TrustedSubmissionError>> + Send>>;
pub(super) type Submit = Arc<dyn Fn(Bytes) -> SubmissionFuture + Send + Sync>;

/// Routes only the binary endpoint; all other traffic retains its JSON middleware.
#[derive(Clone)]
pub(super) struct TrustedBlockHttpService<S> {
    json: S,
    submit: Option<Submit>,
    credentials: HttpRequestMiddleware<()>,
    body_slots: Arc<Semaphore>,
}

impl<S> TrustedBlockHttpService<S> {
    pub(super) fn new(json: S, cookie: Option<Cookie>, submit: Option<Submit>) -> Self {
        Self {
            json,
            submit,
            credentials: HttpRequestMiddleware::new((), cookie, 0),
            body_slots: Arc::new(Semaphore::new(16)),
        }
    }
}

impl<S, B> Service<HttpRequest<B>> for TrustedBlockHttpService<S>
where
    S: Service<HttpRequest<B>, Response = HttpResponse> + Clone + Send + 'static,
    S::Error: Into<BoxError>,
    S::Future: Send,
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    type Response = HttpResponse;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<HttpResponse, BoxError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        self.json.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: HttpRequest<B>) -> Self::Future {
        if request.uri().path() != "/submittrustedblock" {
            let future = self.json.call(request);
            return Box::pin(async move { future.await.map_err(Into::into) });
        }
        let this = self.clone();
        Box::pin(async move { Ok(this.answer(request).await) })
    }
}

impl<S> TrustedBlockHttpService<S> {
    async fn answer<B>(self, request: HttpRequest<B>) -> HttpResponse
    where
        B: hyper::body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<BoxError>,
    {
        let Some(submit) = &self.submit else {
            return response(StatusCode::NOT_FOUND, "disabled");
        };
        if request.headers().contains_key(header::ORIGIN) {
            return response(StatusCode::FORBIDDEN, "browser requests are not accepted");
        }
        if !self.credentials.check_credentials(request.headers()) {
            return response(StatusCode::UNAUTHORIZED, "unauthenticated");
        }
        if request.method() != Method::POST {
            return response(StatusCode::METHOD_NOT_ALLOWED, "POST required");
        }
        if request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            != Some("application/octet-stream")
        {
            return response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "application/octet-stream required",
            );
        }
        let Ok(_slot) = self.body_slots.clone().try_acquire_owned() else {
            return response(StatusCode::TOO_MANY_REQUESTS, "capacity exhausted");
        };
        let limit = usize::try_from(zakura_chain::block::MAX_BLOCK_BYTES)
            .expect("block limit fits supported platforms");
        if request
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .is_some_and(|length| length > zakura_chain::block::MAX_BLOCK_BYTES)
        {
            return response(StatusCode::PAYLOAD_TOO_LARGE, "block too large");
        }
        let received_at = std::time::Instant::now();
        let body = match tokio::time::timeout(
            Duration::from_secs(30),
            Limited::new(request.into_body(), limit).collect(),
        )
        .await
        {
            Err(_) => return response(StatusCode::REQUEST_TIMEOUT, "body receipt timed out"),
            Ok(Err(error)) => {
                return if error.is::<http_body_util::LengthLimitError>() {
                    response(StatusCode::PAYLOAD_TOO_LARGE, "block too large")
                } else {
                    response(StatusCode::BAD_REQUEST, "body receipt failed")
                };
            }
            Ok(Ok(body)) => body.to_bytes(),
        };
        metrics::histogram!("rpc.trusted_submission.receipt.duration_seconds")
            .record(received_at.elapsed().as_secs_f64());
        metrics::counter!("rpc.trusted_submission.received").increment(1);
        let result = match submit(body).await {
            Err(TrustedSubmissionError::Malformed) => {
                return response(StatusCode::BAD_REQUEST, "malformed block")
            }
            Err(TrustedSubmissionError::Capacity) => {
                return response(StatusCode::TOO_MANY_REQUESTS, "capacity exhausted")
            }
            Ok(result) => result,
        };
        response(StatusCode::OK, outcome(&result))
    }
}

/// Fixed outcome labels shared by HTTP and the connection-independent lifecycle metrics.
pub(crate) fn outcome(result: &SubmitBlockResponse) -> &'static str {
    match result {
        SubmitBlockResponse::Accepted => "accepted",
        SubmitBlockResponse::ErrorResponse(SubmitBlockErrorResponse::Rejected) => "rejected",
        SubmitBlockResponse::ErrorResponse(SubmitBlockErrorResponse::Duplicate) => "duplicate",
        SubmitBlockResponse::ErrorResponse(SubmitBlockErrorResponse::DuplicateInconclusive) => {
            "duplicate-inconclusive"
        }
        SubmitBlockResponse::ErrorResponse(SubmitBlockErrorResponse::Inconclusive) => {
            "inconclusive"
        }
    }
}

fn response(status: StatusCode, text: &'static str) -> HttpResponse {
    metrics::counter!("rpc.trusted_submission.http_responses", "status" => status.as_str().to_owned()).increment(1);
    let mut response = HttpResponse::new(HttpBody::from(text));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}
