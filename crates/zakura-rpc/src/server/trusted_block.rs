//! Binary block submission using the primary HTTP listener's authentication policy.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use http_body_util::{BodyExt, Limited};
use hyper::{body::Bytes, header, Method, StatusCode};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tower::{BoxError, Service, ServiceExt};

use super::{
    cookie::Cookie,
    http::{HttpBody, HttpRequest, HttpResponse},
    http_request_compatibility::HttpRequestMiddleware,
};
use crate::methods::trusted_block::jobs::{JobError, JobId, Operation, CONFIRMATION_BYTES};
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

pub(super) type JobFuture = Pin<Box<dyn Future<Output = Result<(), JobError>> + Send>>;
pub(super) type SubmitJob = Arc<dyn Fn(JobId, Operation, Bytes) -> JobFuture + Send + Sync>;

/// What an admitted request submits.
enum Route {
    /// One complete block, answered with its validation outcome.
    Full(Submit),
    /// One input of a prepared job, acknowledged on receipt.
    Job(JobId, Operation, SubmitJob),
}

/// The largest serialized block header: 140 fixed bytes, a 3-byte length, and the 1,344-byte
/// Equihash solution of the public networks.
const MAX_SOLVED_HEADER_BYTES: usize = 1_487;

impl Route {
    /// The largest request body this route reads.
    fn limit(&self) -> usize {
        match self {
            Self::Job(_, Operation::Solve, _) => MAX_SOLVED_HEADER_BYTES,
            Self::Job(_, Operation::Confirm, _) => CONFIRMATION_BYTES,
            Self::Full(_) | Self::Job(_, Operation::Prepare, _) => {
                usize::try_from(zakura_chain::block::MAX_BLOCK_BYTES)
                    .expect("block limit fits supported platforms")
            }
        }
    }
}

/// Routes only the binary endpoint; all other traffic retains its JSON middleware.
///
/// This is the hyper-level service of both RPC listeners, not a tower service, because hyper
/// calls it synchronously when it parses a request head. Hyper may read the whole body and fail
/// the connection on the client's close before it first polls the returned future, so the
/// trusted route must take ownership of the body inside `call`. JSON requests keep tower's lazy
/// readiness and call, exactly as `hyper_util::service::TowerToHyperService` does.
#[derive(Clone)]
pub(super) struct TrustedBlockHttpService<S> {
    json: S,
    submit: Option<Submit>,
    jobs: Option<SubmitJob>,
    credentials: HttpRequestMiddleware<()>,
    body_slots: Arc<Semaphore>,
}

impl<S> TrustedBlockHttpService<S> {
    pub(super) fn new(json: S, cookie: Option<Cookie>, submit: Option<Submit>) -> Self {
        Self {
            json,
            submit,
            jobs: None,
            credentials: HttpRequestMiddleware::new((), cookie, 0),
            body_slots: Arc::new(Semaphore::new(16)),
        }
    }

    /// Routes the `op` query's job operations to `jobs`. Without it they answer 404.
    pub(super) fn with_jobs(mut self, jobs: Option<SubmitJob>) -> Self {
        self.jobs = jobs;
        self
    }
}

impl<S, B> hyper::service::Service<HttpRequest<B>> for TrustedBlockHttpService<S>
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

    fn call(&self, request: HttpRequest<B>) -> Self::Future {
        if request.uri().path() != "/submittrustedblock" {
            let json = self.json.clone().oneshot(request);
            return Box::pin(async move { json.await.map_err(Into::into) });
        }
        let (submit, slot) = match self.admit(&request) {
            Ok(admitted) => admitted,
            Err(rejection) => return Box::pin(async move { Ok(rejection) }),
        };
        // From here the body belongs to a task the connection cannot cancel. That task can still
        // read every frame hyper queued before it failed the connection on the client's close.
        // A body that ends abnormally reports an error, so it is never mistaken for a complete
        // one. The task keeps the slot until the submission finishes.
        // A complete submission reports an unknown outcome in its 200 body. A job input has
        // no such body, and its 2xx would read as an acknowledgement.
        let unknown_outcome = match &submit {
            Route::Full(_) => StatusCode::OK,
            Route::Job(..) => StatusCode::SERVICE_UNAVAILABLE,
        };
        let submission = tokio::spawn(receive_and_submit(submit, slot, request.into_body()));
        // The task is never aborted, so a join error is a panic or runtime shutdown.
        Box::pin(async move {
            Ok(submission
                .await
                .unwrap_or_else(|_| response(unknown_outcome, "inconclusive")))
        })
    }
}

impl<S> TrustedBlockHttpService<S> {
    /// Applies every check that needs no body, then reserves a body slot.
    fn admit<B>(
        &self,
        request: &HttpRequest<B>,
    ) -> Result<(Route, OwnedSemaphorePermit), HttpResponse> {
        let Some(submit) = &self.submit else {
            return Err(response(StatusCode::NOT_FOUND, "disabled"));
        };
        if request.headers().contains_key(header::ORIGIN) {
            return Err(response(
                StatusCode::FORBIDDEN,
                "browser requests are not accepted",
            ));
        }
        if !self.credentials.check_credentials(request.headers()) {
            return Err(response(StatusCode::UNAUTHORIZED, "unauthenticated"));
        }
        if request.method() != Method::POST {
            return Err(response(StatusCode::METHOD_NOT_ALLOWED, "POST required"));
        }
        if request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            != Some("application/octet-stream")
        {
            return Err(response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "application/octet-stream required",
            ));
        }
        let route = match request.uri().query() {
            None => Route::Full(submit.clone()),
            Some(query) => {
                let op = match query {
                    "op=prepare" => Operation::Prepare,
                    "op=solve" => Operation::Solve,
                    "op=confirm" => Operation::Confirm,
                    _ => {
                        return Err(response(
                            StatusCode::BAD_REQUEST,
                            "unknown trusted operation",
                        ))
                    }
                };
                let Some(jobs) = self.jobs.clone() else {
                    return Err(response(StatusCode::NOT_FOUND, "disabled"));
                };
                let mut ids = request.headers().get_all("x-job-id").iter();
                let id = ids.next().and_then(|v| v.to_str().ok()).filter(|s| {
                    s.len() == 32
                        && s.bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                });
                let mut key = [0; 16];
                if ids.next().is_some()
                    || !id.is_some_and(|id| hex::decode_to_slice(id, &mut key).is_ok())
                {
                    return Err(response(StatusCode::BAD_REQUEST, "invalid X-Job-Id"));
                }
                Route::Job(key, op, jobs)
            }
        };
        let limit = route.limit();
        let Ok(slot) = self.body_slots.clone().try_acquire_owned() else {
            return Err(response(
                StatusCode::TOO_MANY_REQUESTS,
                "capacity exhausted",
            ));
        };
        if request
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .is_some_and(|length| usize::try_from(length).map_or(true, |length| length > limit))
        {
            return Err(response(StatusCode::PAYLOAD_TOO_LARGE, "body too large"));
        }
        Ok((route, slot))
    }
}

/// Receives the body within its deadline and size limit, then submits it. `_slot` bounds the
/// buffered body for the whole submission.
async fn receive_and_submit<B>(route: Route, _slot: OwnedSemaphorePermit, body: B) -> HttpResponse
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    let limit = route.limit();
    let received_at = std::time::Instant::now();
    let body =
        match tokio::time::timeout(Duration::from_secs(30), Limited::new(body, limit).collect())
            .await
        {
            Err(_) => return response(StatusCode::REQUEST_TIMEOUT, "body receipt timed out"),
            Ok(Err(error)) => {
                return if error.is::<http_body_util::LengthLimitError>() {
                    response(StatusCode::PAYLOAD_TOO_LARGE, "body too large")
                } else {
                    response(StatusCode::BAD_REQUEST, "body receipt failed")
                };
            }
            Ok(Ok(body)) => body.to_bytes(),
        };
    metrics::histogram!("rpc.trusted_submission.receipt.duration_seconds")
        .record(received_at.elapsed().as_secs_f64());
    metrics::counter!("rpc.trusted_submission.received").increment(1);
    let submit = match route {
        Route::Full(submit) => submit,
        Route::Job(id, op, jobs) => {
            return match jobs(id, op, body).await {
                Ok(()) => response(StatusCode::ACCEPTED, "received"),
                Err(JobError::Invalid) => response(StatusCode::BAD_REQUEST, "invalid job input"),
                Err(JobError::Conflict) => response(StatusCode::CONFLICT, "conflicting job input"),
                Err(JobError::Capacity) => {
                    response(StatusCode::TOO_MANY_REQUESTS, "capacity exhausted")
                }
                Err(JobError::Expired) => response(StatusCode::GONE, "stale job"),
                Err(JobError::Unavailable) => {
                    response(StatusCode::SERVICE_UNAVAILABLE, "inconclusive")
                }
            }
        }
    };
    match submit(body).await {
        Err(TrustedSubmissionError::Malformed) => {
            response(StatusCode::BAD_REQUEST, "malformed block")
        }
        Err(TrustedSubmissionError::Capacity) => {
            response(StatusCode::TOO_MANY_REQUESTS, "capacity exhausted")
        }
        Ok(result) => response(StatusCode::OK, outcome(&result)),
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
