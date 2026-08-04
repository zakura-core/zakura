//! Public, resource-bounded transaction submission endpoint.

use std::{
    collections::HashMap,
    future::Future,
    net::{IpAddr, Ipv6Addr, SocketAddr},
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use http_body_util::{BodyExt, Limited};
use hyper::{header, server::conn::http1, Method, StatusCode};
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use ipnet::IpNet;
use jsonrpsee::{
    core::{async_trait, BoxError, RpcResult},
    server::{stop_channel, BatchRequestConfig, HttpBody, HttpRequest, HttpResponse, Server},
    Extensions,
};
use jsonrpsee_proc_macros::rpc;
use jsonrpsee_types::ErrorObject;
use tokio::{
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore},
};
use tokio_rustls::TlsAcceptor;
use tower::{Service, ServiceBuilder};
use tracing::{error, info, trace, warn};

use zakura_chain::parameters::NetworkKind;
use zakura_node_services::mempool::MempoolService;

use crate::{
    config::rpc::TransactionSubmissionConfig,
    methods::{submit_raw_transaction_to_mempool, SendRawTransactionResponse},
    server::{
        http_request_compatibility::HttpRequestMiddlewareLayer, load_tls_config, RpcServer,
        ServerTask, OPENED_TRANSACTION_SUBMISSION_ENDPOINT_MSG,
    },
};

const JSON_RPC_WRAPPER_BYTES: usize = 4_096;
const MAX_RESPONSE_BODY_BYTES: u32 = 65_536;
const MAX_HTTP_HEADERS: usize = 64;
const MAX_FORWARDED_FOR_BYTES: usize = 1_024;
const MAX_FORWARDED_HOPS: usize = 16;
const MAX_TRACKED_CLIENTS: usize = 4_096;
const CLIENT_STATE_IDLE_TIME: Duration = Duration::from_secs(5 * 60);
const HTTP_HEADER_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_BODY_TIMEOUT: Duration = Duration::from_secs(10);
const SUBMISSION_RESPONSE_TIMEOUT: Duration = Duration::from_secs(90);

const RATE_LIMIT_ERROR_CODE: i32 = -32005;
const SERVER_ERROR_CODE: i32 = -32000;

type ResponseFuture = Pin<Box<dyn Future<Output = Result<HttpResponse, BoxError>> + Send>>;

#[rpc(server)]
trait PublicTransactionSubmission {
    /// Submits a signed transaction to this node's mempool exactly once.
    #[method(name = "sendrawtransaction", with_extensions)]
    async fn send_raw_transaction(
        &self,
        raw_transaction_hex: String,
        allow_high_fees: Option<bool>,
    ) -> RpcResult<SendRawTransactionResponse>;
}

#[derive(Clone)]
struct PublicTransactionSubmissionImpl<Mempool> {
    mempool: Mempool,
}

#[async_trait]
impl<Mempool> PublicTransactionSubmissionServer for PublicTransactionSubmissionImpl<Mempool>
where
    Mempool: MempoolService,
{
    async fn send_raw_transaction(
        &self,
        extensions: &Extensions,
        raw_transaction_hex: String,
        _allow_high_fees: Option<bool>,
    ) -> RpcResult<SendRawTransactionResponse> {
        let admission = extensions
            .get::<RequestAdmission>()
            .cloned()
            .ok_or_else(internal_admission_error)?;
        let permits = admission.take().ok_or_else(internal_admission_error)?;
        let mempool = self.mempool.clone();
        let started = Instant::now();

        metrics::counter!("rpc.transaction_submission.requests.total").increment(1);

        // The worker owns the permits. Dropping the HTTP request detaches this task instead of
        // freeing capacity while transaction verification is still running.
        let worker = tokio::spawn(async move {
            let _permits = permits;
            submit_raw_transaction_to_mempool(mempool, None, raw_transaction_hex).await
        });

        let result = match tokio::time::timeout(SUBMISSION_RESPONSE_TIMEOUT, worker).await {
            Ok(Ok(result)) => result,
            Ok(Err(join_error)) => {
                error!(?join_error, "public transaction submission task failed");
                Err(ErrorObject::owned(
                    SERVER_ERROR_CODE,
                    "transaction submission failed",
                    None::<()>,
                ))
            }
            Err(_) => Err(ErrorObject::owned(
                SERVER_ERROR_CODE,
                "transaction verification timed out",
                None::<()>,
            )),
        };

        let result_label = if result.is_ok() {
            "accepted"
        } else {
            "rejected"
        };
        metrics::counter!(
            "rpc.transaction_submission.results.total",
            "result" => result_label
        )
        .increment(1);
        metrics::histogram!("rpc.transaction_submission.duration_seconds")
            .record(started.elapsed().as_secs_f64());

        result
    }
}

fn internal_admission_error() -> jsonrpsee_types::ErrorObjectOwned {
    ErrorObject::owned(
        SERVER_ERROR_CODE,
        "transaction submission admission failed",
        None::<()>,
    )
}

struct SubmissionPermits {
    _global: OwnedSemaphorePermit,
    _client: OwnedSemaphorePermit,
}

struct RequestRateAdmission {
    client_in_flight: Arc<Semaphore>,
}

#[derive(Clone)]
struct RequestAdmission(Arc<Mutex<Option<SubmissionPermits>>>);

impl RequestAdmission {
    fn new(permits: SubmissionPermits) -> Self {
        Self(Arc::new(Mutex::new(Some(permits))))
    }

    fn take(&self) -> Option<SubmissionPermits> {
        lock_unpoisoned(&self.0).take()
    }
}

#[derive(Clone)]
struct ConnectionController {
    counts: Arc<Mutex<HashMap<IpAddr, usize>>>,
    max_per_ip: usize,
}

impl ConnectionController {
    fn new(max_per_ip: usize) -> Self {
        Self {
            counts: Arc::new(Mutex::new(HashMap::new())),
            max_per_ip,
        }
    }

    fn admit(&self, ip: IpAddr) -> Option<ConnectionPermit> {
        let ip = client_identity(ip);
        let mut counts = lock_unpoisoned(&self.counts);
        let count = counts.entry(ip).or_default();
        if *count >= self.max_per_ip {
            return None;
        }
        *count += 1;

        Some(ConnectionPermit {
            controller: self.clone(),
            ip,
        })
    }
}

struct ConnectionPermit {
    controller: ConnectionController,
    ip: IpAddr,
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        let mut counts = lock_unpoisoned(&self.controller.counts);
        let Some(count) = counts.get_mut(&self.ip) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            counts.remove(&self.ip);
        }
    }
}

#[derive(Clone)]
struct AdmissionController {
    inner: Arc<AdmissionControllerInner>,
}

struct AdmissionControllerInner {
    rate_state: Mutex<RateState>,
    global_in_flight: Arc<Semaphore>,
    per_ip_rate_per_second: f64,
    per_ip_burst: u32,
    max_in_flight_per_ip: usize,
}

struct RateState {
    global: TokenBucket,
    clients: HashMap<IpAddr, ClientState>,
}

struct ClientState {
    rate: TokenBucket,
    in_flight: Arc<Semaphore>,
    last_seen: Instant,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum AdmissionRejection {
    GlobalRate,
    ClientRate,
    GlobalInFlight,
    ClientInFlight,
    ClientTableFull,
}

impl AdmissionRejection {
    fn metric_label(self) -> &'static str {
        match self {
            Self::GlobalRate => "global_rate",
            Self::ClientRate => "client_rate",
            Self::GlobalInFlight => "global_in_flight",
            Self::ClientInFlight => "client_in_flight",
            Self::ClientTableFull => "client_table_full",
        }
    }
}

impl AdmissionController {
    fn new(config: &TransactionSubmissionConfig) -> Self {
        let now = Instant::now();
        Self {
            inner: Arc::new(AdmissionControllerInner {
                rate_state: Mutex::new(RateState {
                    global: TokenBucket::new(
                        f64::from(config.requests_per_second),
                        config.request_burst,
                        now,
                    ),
                    clients: HashMap::new(),
                }),
                global_in_flight: Arc::new(Semaphore::new(config.max_in_flight)),
                per_ip_rate_per_second: f64::from(config.requests_per_minute_per_ip) / 60.0,
                per_ip_burst: config.request_burst_per_ip,
                max_in_flight_per_ip: config.max_in_flight_per_ip,
            }),
        }
    }

    fn admit_rate(&self, client_ip: IpAddr) -> Result<RequestRateAdmission, AdmissionRejection> {
        let client_ip = client_identity(client_ip);
        let now = Instant::now();
        let client_in_flight = {
            let mut rate_state = lock_unpoisoned(&self.inner.rate_state);
            rate_state.global.refill(now);
            if !rate_state.global.has_token() {
                return Err(AdmissionRejection::GlobalRate);
            }

            if !rate_state.clients.contains_key(&client_ip)
                && rate_state.clients.len() >= MAX_TRACKED_CLIENTS
            {
                rate_state.clients.retain(|_, client| {
                    now.saturating_duration_since(client.last_seen) < CLIENT_STATE_IDLE_TIME
                        || Arc::strong_count(&client.in_flight) > 1
                });
            }

            if !rate_state.clients.contains_key(&client_ip)
                && rate_state.clients.len() >= MAX_TRACKED_CLIENTS
            {
                return Err(AdmissionRejection::ClientTableFull);
            }

            let client_in_flight = {
                let client = rate_state
                    .clients
                    .entry(client_ip)
                    .or_insert_with(|| ClientState {
                        rate: TokenBucket::new(
                            self.inner.per_ip_rate_per_second,
                            self.inner.per_ip_burst,
                            now,
                        ),
                        in_flight: Arc::new(Semaphore::new(self.inner.max_in_flight_per_ip)),
                        last_seen: now,
                    });
                client.last_seen = now;
                client.rate.refill(now);
                if !client.rate.has_token() {
                    return Err(AdmissionRejection::ClientRate);
                }

                client.rate.take_token();
                client.in_flight.clone()
            };
            rate_state.global.take_token();
            client_in_flight
        };

        Ok(RequestRateAdmission { client_in_flight })
    }

    fn admit_in_flight(
        &self,
        rate_admission: RequestRateAdmission,
    ) -> Result<RequestAdmission, AdmissionRejection> {
        let global = self
            .inner
            .global_in_flight
            .clone()
            .try_acquire_owned()
            .map_err(|_| AdmissionRejection::GlobalInFlight)?;
        let client = rate_admission
            .client_in_flight
            .try_acquire_owned()
            .map_err(|_| AdmissionRejection::ClientInFlight)?;

        Ok(RequestAdmission::new(SubmissionPermits {
            _global: global,
            _client: client,
        }))
    }

    #[cfg(test)]
    fn admit(&self, client_ip: IpAddr) -> Result<RequestAdmission, AdmissionRejection> {
        let rate_admission = self.admit_rate(client_ip)?;
        self.admit_in_flight(rate_admission)
    }
}

struct TokenBucket {
    tokens: f64,
    tokens_per_second: f64,
    capacity: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(tokens_per_second: f64, capacity: u32, now: Instant) -> Self {
        Self {
            tokens: f64::from(capacity),
            tokens_per_second,
            capacity: f64::from(capacity),
            last_refill: now,
        }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_refill);
        self.tokens =
            (self.tokens + elapsed.as_secs_f64() * self.tokens_per_second).min(self.capacity);
        self.last_refill = now;
    }

    fn has_token(&self) -> bool {
        self.tokens >= 1.0
    }

    fn take_token(&mut self) {
        self.tokens -= 1.0;
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Clone)]
struct PublicHttpService<S> {
    inner: S,
    admission: AdmissionController,
    remote_ip: IpAddr,
    trusted_proxies: Arc<[IpNet]>,
    max_request_body_size: usize,
}

impl<S> PublicHttpService<S> {
    fn new(
        inner: S,
        admission: AdmissionController,
        remote_ip: IpAddr,
        trusted_proxies: Arc<[IpNet]>,
        max_request_body_size: usize,
    ) -> Self {
        Self {
            inner,
            admission,
            remote_ip: normalize_ip(remote_ip),
            trusted_proxies,
            max_request_body_size,
        }
    }
}

impl<S, B> Service<HttpRequest<B>> for PublicHttpService<S>
where
    S: Service<HttpRequest<HttpBody>, Response = HttpResponse, Error = BoxError>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    B: hyper::body::Body<Data = hyper::body::Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    type Response = HttpResponse;
    type Error = BoxError;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: HttpRequest<B>) -> Self::Future {
        let max_request_body_size_u64 = u64::try_from(self.max_request_body_size)
            .expect("usize fits in u64 on supported platforms");

        let (client_ip, forwarded_for_valid) = resolve_client_ip(
            self.remote_ip,
            request.headers(),
            self.trusted_proxies.as_ref(),
        );
        let rate_admission = match self.admission.admit_rate(client_ip) {
            Ok(admission) => admission,
            Err(rejection) => {
                metrics::counter!(
                    "rpc.transaction_submission.rejections.total",
                    "reason" => rejection.metric_label()
                )
                .increment(1);
                return ready_response(http_error_response(
                    StatusCode::TOO_MANY_REQUESTS,
                    RATE_LIMIT_ERROR_CODE,
                    "request limit exceeded",
                    true,
                ));
            }
        };
        if !forwarded_for_valid {
            metrics::counter!(
                "rpc.transaction_submission.rejections.total",
                "reason" => "invalid_forwarded_for"
            )
            .increment(1);
        }

        if request.method() == Method::GET
            && request.uri().path() == "/healthz"
            && request.uri().query().is_none()
        {
            return ready_response(health_response());
        }

        if request.method() != Method::POST
            || request.uri().path() != "/"
            || request.uri().query().is_some()
        {
            return ready_error_response(
                StatusCode::METHOD_NOT_ALLOWED,
                "only POST / is supported",
                false,
            );
        }

        if request.headers().contains_key(header::TRANSFER_ENCODING)
            || request.headers().contains_key(header::CONTENT_ENCODING)
        {
            return ready_error_response(
                StatusCode::BAD_REQUEST,
                "encoded or chunked request bodies are not supported",
                false,
            );
        }

        if !has_json_content_type(request.headers()) {
            return ready_error_response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "content-type must be application/json",
                false,
            );
        }

        let content_length = request
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        let Some(content_length) = content_length else {
            return ready_error_response(
                StatusCode::LENGTH_REQUIRED,
                "content-length is required",
                false,
            );
        };

        if content_length > max_request_body_size_u64 {
            return ready_error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request body is too large",
                false,
            );
        }

        let admission = match self.admission.admit_in_flight(rate_admission) {
            Ok(admission) => admission,
            Err(rejection) => {
                metrics::counter!(
                    "rpc.transaction_submission.rejections.total",
                    "reason" => rejection.metric_label()
                )
                .increment(1);
                return ready_response(http_error_response(
                    StatusCode::TOO_MANY_REQUESTS,
                    RATE_LIMIT_ERROR_CODE,
                    "transaction submission limit exceeded",
                    true,
                ));
            }
        };

        let replacement = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, replacement);
        let max_request_body_size = self.max_request_body_size;

        Box::pin(async move {
            let (mut parts, body) = request.into_parts();
            let body = match tokio::time::timeout(
                HTTP_BODY_TIMEOUT,
                Limited::new(body, max_request_body_size).collect(),
            )
            .await
            {
                Ok(Ok(body)) => body.to_bytes(),
                Ok(Err(_)) => {
                    return Ok(http_error_response(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        jsonrpsee_types::ErrorCode::InvalidRequest.code(),
                        "request body could not be read within the size limit",
                        false,
                    ));
                }
                Err(_) => {
                    return Ok(http_error_response(
                        StatusCode::REQUEST_TIMEOUT,
                        SERVER_ERROR_CODE,
                        "request body timed out",
                        false,
                    ));
                }
            };

            parts.extensions.insert(admission);
            let request = HttpRequest::from_parts(parts, HttpBody::from(body.to_vec()));
            inner.call(request).await
        })
    }
}

fn ready_response(response: HttpResponse) -> ResponseFuture {
    Box::pin(async move { Ok(response) })
}

fn ready_error_response(status: StatusCode, message: &'static str, retry: bool) -> ResponseFuture {
    ready_response(http_error_response(
        status,
        jsonrpsee_types::ErrorCode::InvalidRequest.code(),
        message,
        retry,
    ))
}

fn has_json_content_type(headers: &header::HeaderMap) -> bool {
    let mut values = headers.get_all(header::CONTENT_TYPE).iter();
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }

    value
        .to_str()
        .ok()
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
}

fn resolve_client_ip(
    remote_ip: IpAddr,
    headers: &header::HeaderMap,
    trusted_proxies: &[IpNet],
) -> (IpAddr, bool) {
    let remote_ip = normalize_ip(remote_ip);
    if !trusted_proxies
        .iter()
        .any(|network| network.contains(&remote_ip))
    {
        return (remote_ip, true);
    }

    let mut values = headers.get_all("x-forwarded-for").iter();
    let Some(value) = values.next() else {
        return (remote_ip, true);
    };
    if values.next().is_some() {
        return (remote_ip, false);
    }

    let Ok(value) = value.to_str() else {
        return (remote_ip, false);
    };
    if value.len() > MAX_FORWARDED_FOR_BYTES {
        return (remote_ip, false);
    }

    let hops = value
        .split(',')
        .map(str::trim)
        .map(str::parse::<IpAddr>)
        .collect::<Result<Vec<_>, _>>();
    let Ok(hops) = hops else {
        return (remote_ip, false);
    };
    if hops.is_empty() || hops.len() > MAX_FORWARDED_HOPS {
        return (remote_ip, false);
    }

    let hops: Vec<IpAddr> = hops.into_iter().map(normalize_ip).collect();
    let client_ip = hops
        .iter()
        .rev()
        .copied()
        .find(|ip| !trusted_proxies.iter().any(|network| network.contains(ip)))
        .or_else(|| hops.first().copied())
        .unwrap_or(remote_ip);

    (client_ip, true)
}

fn normalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(ipv6) => ipv6
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(ipv6)),
        ip => ip,
    }
}

fn client_identity(ip: IpAddr) -> IpAddr {
    match normalize_ip(ip) {
        // Treat one IPv6 /64 like one IPv4 address so privacy address rotation cannot bypass
        // per-client admission limits.
        IpAddr::V6(ipv6) => {
            const IPV6_CLIENT_MASK: u128 = u128::MAX << 64;
            IpAddr::V6(Ipv6Addr::from(u128::from(ipv6) & IPV6_CLIENT_MASK))
        }
        ip => ip,
    }
}

fn http_error_response(
    status: StatusCode,
    code: i32,
    message: &'static str,
    retry: bool,
) -> HttpResponse {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "error": { "code": code, "message": message },
        "id": null,
    })
    .to_string();

    let mut response = HttpResponse::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .header(header::CONNECTION, "close")
        .header("x-content-type-options", "nosniff");
    if retry {
        response = response.header(header::RETRY_AFTER, "1");
    }

    response
        .body(HttpBody::from(body))
        .expect("static response headers are valid")
}

fn health_response() -> HttpResponse {
    HttpResponse::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-store")
        .header("x-content-type-options", "nosniff")
        .body(HttpBody::from("ok\n"))
        .expect("static response headers are valid")
}

impl RpcServer {
    /// Starts the default-on, `sendrawtransaction`-only public listener.
    pub async fn start_transaction_submission<Mempool>(
        mempool: Mempool,
        config: TransactionSubmissionConfig,
        network: NetworkKind,
        max_transaction_bytes: u64,
    ) -> Result<(ServerTask, SocketAddr), BoxError>
    where
        Mempool: MempoolService,
    {
        config
            .validate()
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;

        let max_transaction_bytes = usize::try_from(max_transaction_bytes).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "mempool max_transaction_bytes does not fit in usize",
            )
        })?;
        let max_request_body_size = max_transaction_bytes
            .checked_mul(2)
            .and_then(|size| size.checked_add(JSON_RPC_WRAPPER_BYTES))
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "public transaction submission request limit overflowed",
                )
            })?;
        let max_request_body_size_u32 = u32::try_from(max_request_body_size).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "public transaction submission request limit exceeds the server maximum",
            )
        })?;

        let listen_addr = config.resolved_listen_addr(network);
        let listener = TcpListener::bind(listen_addr).await?;
        let local_addr = listener.local_addr()?;
        let tls_acceptor = config
            .tls
            .as_ref()
            .map(load_tls_config)
            .transpose()?
            .map(TlsAcceptor::from);
        let trusted_proxies: Arc<[IpNet]> = config.trusted_proxies.clone().into();
        let admission = AdmissionController::new(&config);
        let connection_limit = Arc::new(Semaphore::new(config.max_connections));
        let connection_controller = ConnectionController::new(config.max_connections_per_ip);

        let http_middleware = ServiceBuilder::new()
            .layer(HttpRequestMiddlewareLayer::new(None, max_request_body_size));
        let service_builder = Server::builder()
            .http_only()
            .set_batch_request_config(BatchRequestConfig::Disabled)
            .max_request_body_size(max_request_body_size_u32)
            .max_response_body_size(MAX_RESPONSE_BODY_BYTES)
            .set_http_middleware(http_middleware)
            .to_service_builder();
        let methods = PublicTransactionSubmissionImpl { mempool }.into_rpc();
        let (stop_handle, server_handle) = stop_channel();

        info!("{OPENED_TRANSACTION_SUBMISSION_ENDPOINT_MSG}{local_addr}");

        let task = tokio::spawn(async move {
            loop {
                let (socket, remote_addr) = tokio::select! {
                    result = listener.accept() => result?,
                    _ = stop_handle.clone().shutdown() => break,
                };

                let Ok(connection_permit) = connection_limit.clone().try_acquire_owned() else {
                    metrics::counter!(
                        "rpc.transaction_submission.rejections.total",
                        "reason" => "connection_limit"
                    )
                    .increment(1);
                    continue;
                };
                let Some(client_connection_permit) = connection_controller.admit(remote_addr.ip())
                else {
                    metrics::counter!(
                        "rpc.transaction_submission.rejections.total",
                        "reason" => "client_connection_limit"
                    )
                    .increment(1);
                    continue;
                };

                if let Err(connection_error) = socket.set_nodelay(true) {
                    warn!(
                        ?connection_error,
                        "could not configure transaction submission socket"
                    );
                    continue;
                }

                let inner = service_builder
                    .clone()
                    .build(methods.clone(), stop_handle.clone());
                let service = PublicHttpService::new(
                    inner,
                    admission.clone(),
                    remote_addr.ip(),
                    trusted_proxies.clone(),
                    max_request_body_size,
                );
                let tls_acceptor = tls_acceptor.clone();
                let stopped = stop_handle.clone().shutdown();

                tokio::spawn(async move {
                    let _connection_permit = connection_permit;
                    let _client_connection_permit = client_connection_permit;
                    let result = if let Some(tls_acceptor) = tls_acceptor {
                        match tokio::time::timeout(HTTP_HEADER_TIMEOUT, tls_acceptor.accept(socket))
                            .await
                        {
                            Ok(Ok(stream)) => serve_http1(stream, service, stopped).await,
                            Ok(Err(tls_error)) => {
                                metrics::counter!(
                                    "rpc.transaction_submission.rejections.total",
                                    "reason" => "tls_handshake"
                                )
                                .increment(1);
                                trace!(?tls_error, "transaction submission TLS handshake failed");
                                return;
                            }
                            Err(_) => {
                                metrics::counter!(
                                    "rpc.transaction_submission.rejections.total",
                                    "reason" => "tls_handshake_timeout"
                                )
                                .increment(1);
                                trace!("transaction submission TLS handshake timed out");
                                return;
                            }
                        }
                    } else {
                        serve_http1(socket, service, stopped).await
                    };

                    if let Err(connection_error) = result {
                        metrics::counter!(
                            "rpc.transaction_submission.rejections.total",
                            "reason" => "connection_error"
                        )
                        .increment(1);
                        trace!(
                            ?connection_error,
                            "transaction submission connection failed"
                        );
                    }
                });
            }

            drop(server_handle);
            Ok(())
        });

        Ok((task, local_addr))
    }
}

async fn serve_http1<I, S>(
    io: I,
    service: S,
    stopped: impl Future<Output = ()>,
) -> Result<(), BoxError>
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
    S: Service<HttpRequest<hyper::body::Incoming>, Response = HttpResponse, Error = BoxError>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
{
    let io = TokioIo::new(io);
    let service = TowerToHyperService::new(service);
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(HTTP_HEADER_TIMEOUT)
        .max_headers(MAX_HTTP_HEADERS);
    let connection = builder.serve_connection(io, service);

    tokio::pin!(connection, stopped);
    let result = tokio::select! {
        result = &mut connection => result,
        _ = &mut stopped => {
            connection.as_mut().graceful_shutdown();
            connection.await
        }
    };

    result.map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4};

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        sync::oneshot,
    };
    use tower::buffer::Buffer;

    use zakura_chain::{
        serialization::ZcashDeserializeInto,
        transaction::{Transaction, UnminedTx},
    };
    use zakura_node_services::{mempool, rpc_client::RpcRequestClient, BoxError as NodeBoxError};
    use zakura_test::mock_service::MockService;

    use super::*;

    async fn raw_http_response(listen_addr: SocketAddr, request: &[u8]) -> Vec<u8> {
        let mut connection = TcpStream::connect(listen_addr)
            .await
            .expect("HTTP connection should open");
        connection
            .write_all(request)
            .await
            .expect("HTTP request should write");
        let mut response = Vec::new();
        connection
            .read_to_end(&mut response)
            .await
            .expect("HTTP response should read");
        response
    }

    #[tokio::test]
    async fn public_server_exposes_one_strict_rate_limited_method() {
        let _init_guard = zakura_test::init();
        let mut mempool: MockService<_, _, _, NodeBoxError> = MockService::build().for_unit_tests();
        let mempool_service = Buffer::new(mempool.clone(), 1);
        let config = TransactionSubmissionConfig {
            listen_addr: Some(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into()),
            requests_per_second: 100,
            request_burst: 100,
            requests_per_minute_per_ip: 1,
            request_burst_per_ip: 5,
            ..TransactionSubmissionConfig::default()
        };
        let (server_task, listen_addr) = RpcServer::start_transaction_submission(
            mempool_service,
            config,
            NetworkKind::Mainnet,
            1,
        )
        .await
        .expect("public server should start");
        let client = RpcRequestClient::new(listen_addr);

        let health_response = raw_http_response(
            listen_addr,
            b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(health_response.starts_with(b"HTTP/1.1 200 OK\r\n"));

        let unknown_response = client
            .call("getblockchaininfo", "[]")
            .await
            .expect("unknown method request should complete");
        assert_eq!(unknown_response.status().as_u16(), 200);
        let unknown_body: serde_json::Value = serde_json::from_str(
            &unknown_response
                .text()
                .await
                .expect("unknown method response should have a body"),
        )
        .expect("unknown method response should be JSON");
        assert_eq!(unknown_body["error"]["code"], -32601);

        let invalid_transaction_response = client
            .call("sendrawtransaction", r#"["zz"]"#)
            .await
            .expect("invalid transaction request should complete");
        assert_eq!(invalid_transaction_response.status().as_u16(), 200);
        let invalid_transaction_body: serde_json::Value = serde_json::from_str(
            &invalid_transaction_response
                .text()
                .await
                .expect("invalid transaction response should have a body"),
        )
        .expect("invalid transaction response should be JSON");
        assert_eq!(invalid_transaction_body["error"]["code"], -22);

        let content_type_response = client
            .call_with_content_type("sendrawtransaction", r#"["zz"]"#, "text/plain".to_string())
            .await
            .expect("content type rejection should complete");
        assert_eq!(content_type_response.status().as_u16(), 415);

        let oversized_params = format!(r#"["{}"]"#, "0".repeat(5_000));
        let oversized_response = client
            .call("sendrawtransaction", oversized_params)
            .await
            .expect("oversized request should complete");
        assert_eq!(oversized_response.status().as_u16(), 413);

        let rate_limited_response = client
            .call("getblockchaininfo", "[]")
            .await
            .expect("rate limited request should complete");
        assert_eq!(rate_limited_response.status().as_u16(), 429);
        assert_eq!(
            rate_limited_response
                .headers()
                .get(header::RETRY_AFTER)
                .expect("rate limit response should include retry-after"),
            "1"
        );

        mempool.expect_no_requests().await;

        server_task.abort();
    }

    #[tokio::test]
    async fn health_and_unsupported_requests_are_rate_limited() {
        let _init_guard = zakura_test::init();
        let mempool: MockService<_, _, _, NodeBoxError> = MockService::build().for_unit_tests();
        let config = TransactionSubmissionConfig {
            listen_addr: Some(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into()),
            requests_per_second: 100,
            request_burst: 100,
            requests_per_minute_per_ip: 1,
            request_burst_per_ip: 2,
            ..TransactionSubmissionConfig::default()
        };
        let (server_task, listen_addr) = RpcServer::start_transaction_submission(
            Buffer::new(mempool, 1),
            config,
            NetworkKind::Mainnet,
            1,
        )
        .await
        .expect("public server should start");

        let health_response = raw_http_response(
            listen_addr,
            b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(health_response.starts_with(b"HTTP/1.1 200 OK\r\n"));

        let unsupported_response = raw_http_response(
            listen_addr,
            b"GET /unsupported HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(unsupported_response.starts_with(b"HTTP/1.1 405 Method Not Allowed\r\n"));

        let limited_response = raw_http_response(
            listen_addr,
            b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(limited_response.starts_with(b"HTTP/1.1 429 Too Many Requests\r\n"));

        server_task.abort();
    }

    #[tokio::test]
    async fn public_server_submits_a_valid_transaction_once() {
        let _init_guard = zakura_test::init();
        let mut mempool: MockService<_, _, _, NodeBoxError> = MockService::build().for_unit_tests();
        let mempool_service = Buffer::new(mempool.clone(), 1);
        let config = TransactionSubmissionConfig {
            listen_addr: Some(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into()),
            ..TransactionSubmissionConfig::default()
        };
        let (server_task, listen_addr) = RpcServer::start_transaction_submission(
            mempool_service,
            config,
            NetworkKind::Mainnet,
            250_000,
        )
        .await
        .expect("public server should start");
        let client = RpcRequestClient::new(listen_addr);

        let transaction: Transaction = zakura_test::vectors::DUMMY_TX1
            .as_slice()
            .zcash_deserialize_into()
            .expect("dummy transaction should deserialize");
        let expected_hash = transaction.hash();
        let transaction_hex = hex::encode(zakura_test::vectors::DUMMY_TX1.as_slice());
        let params = format!(r#"["{transaction_hex}"]"#);
        let expected_request = mempool::Request::Queue(vec![UnminedTx::from(transaction).into()]);
        let (response_sender, response_receiver) = oneshot::channel();
        response_sender
            .send(Ok(()))
            .expect("mock response receiver should be open");
        let respond_to_mempool = async {
            mempool
                .expect_request(expected_request)
                .await
                .respond(mempool::Response::Queued(vec![Ok(response_receiver)]));
        };

        let (submission_result, ()) = tokio::join!(
            client
                .json_result_from_call::<SendRawTransactionResponse>("sendrawtransaction", params,),
            respond_to_mempool,
        );

        assert_eq!(
            submission_result
                .expect("valid transaction submission should succeed")
                .hash(),
            expected_hash
        );
        mempool.expect_no_requests().await;

        server_task.abort();
    }

    #[tokio::test]
    async fn cancelled_requests_keep_their_in_flight_permits() {
        let _init_guard = zakura_test::init();
        let mut mempool: MockService<_, _, _, NodeBoxError> = MockService::build().for_unit_tests();
        let endpoint = PublicTransactionSubmissionImpl {
            mempool: Buffer::new(mempool.clone(), 1),
        };
        let config = TransactionSubmissionConfig {
            requests_per_second: 100,
            request_burst: 100,
            requests_per_minute_per_ip: 6_000,
            request_burst_per_ip: 100,
            max_in_flight: 1,
            max_in_flight_per_ip: 1,
            ..TransactionSubmissionConfig::default()
        };
        let admission = AdmissionController::new(&config);
        let mut extensions = Extensions::new();
        extensions.insert(
            admission
                .admit("192.0.2.1".parse().expect("valid IP"))
                .expect("first request should be admitted"),
        );

        let transaction: Transaction = zakura_test::vectors::DUMMY_TX1
            .as_slice()
            .zcash_deserialize_into()
            .expect("dummy transaction should deserialize");
        let transaction_hex = hex::encode(zakura_test::vectors::DUMMY_TX1.as_slice());
        let expected_request = mempool::Request::Queue(vec![UnminedTx::from(transaction).into()]);
        let request_task = tokio::spawn(async move {
            endpoint
                .send_raw_transaction(&extensions, transaction_hex, None)
                .await
        });

        let response = mempool.expect_request(expected_request).await;
        let (response_sender, response_receiver) = oneshot::channel();
        response.respond(mempool::Response::Queued(vec![Ok(response_receiver)]));
        request_task.abort();
        request_task
            .await
            .expect_err("cancelled request should stop its response task");

        assert_eq!(admission.inner.global_in_flight.available_permits(), 0);
        response_sender
            .send(Ok(()))
            .expect("detached mempool request should still be waiting");
        tokio::time::timeout(Duration::from_secs(1), async {
            while admission.inner.global_in_flight.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("permit should be released when verification finishes");
    }

    #[test]
    fn untrusted_clients_cannot_spoof_forwarded_for() {
        let mut headers = header::HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            "203.0.113.7".parse().expect("valid header"),
        );

        assert_eq!(
            resolve_client_ip(
                "198.51.100.8".parse().expect("valid IP"),
                &headers,
                &["192.0.2.0/24".parse().expect("valid network")],
            ),
            ("198.51.100.8".parse().expect("valid IP"), true)
        );
    }

    #[test]
    fn trusted_proxies_use_the_first_untrusted_hop_from_the_right() {
        let mut headers = header::HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            "203.0.113.7, 192.0.2.4".parse().expect("valid header"),
        );

        assert_eq!(
            resolve_client_ip(
                "192.0.2.5".parse().expect("valid IP"),
                &headers,
                &["192.0.2.0/24".parse().expect("valid network")],
            ),
            ("203.0.113.7".parse().expect("valid IP"), true)
        );
    }

    #[test]
    fn malformed_forwarded_for_falls_back_to_the_proxy_address() {
        let mut headers = header::HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            "not-an-ip".parse().expect("valid header"),
        );

        assert_eq!(
            resolve_client_ip(
                "192.0.2.5".parse().expect("valid IP"),
                &headers,
                &["192.0.2.0/24".parse().expect("valid network")],
            ),
            ("192.0.2.5".parse().expect("valid IP"), false)
        );
    }

    #[test]
    fn rate_limits_are_enforced() {
        let config = TransactionSubmissionConfig {
            requests_per_second: 2,
            request_burst: 2,
            requests_per_minute_per_ip: 60,
            request_burst_per_ip: 1,
            max_in_flight: 2,
            max_in_flight_per_ip: 1,
            ..TransactionSubmissionConfig::default()
        };
        let admission = AdmissionController::new(&config);
        let first_ip = "192.0.2.1".parse().expect("valid IP");
        let second_ip = "192.0.2.2".parse().expect("valid IP");

        let first = admission
            .admit(first_ip)
            .expect("first request should be admitted");
        assert!(matches!(
            admission.admit(first_ip),
            Err(AdmissionRejection::ClientRate)
        ));
        let second = admission
            .admit(second_ip)
            .expect("another client should retain access to the global budget");
        assert!(matches!(
            admission.admit("192.0.2.3".parse().expect("valid IP")),
            Err(AdmissionRejection::GlobalRate)
        ));

        drop((first, second));
    }

    #[test]
    fn in_flight_limits_are_enforced() {
        let config = TransactionSubmissionConfig {
            requests_per_second: 100,
            request_burst: 100,
            requests_per_minute_per_ip: 6_000,
            request_burst_per_ip: 100,
            max_in_flight: 2,
            max_in_flight_per_ip: 1,
            ..TransactionSubmissionConfig::default()
        };
        let admission = AdmissionController::new(&config);
        let first_ip = "192.0.2.1".parse().expect("valid IP");
        let second_ip = "192.0.2.2".parse().expect("valid IP");

        let first = admission
            .admit(first_ip)
            .expect("first request should be admitted");
        assert!(matches!(
            admission.admit(first_ip),
            Err(AdmissionRejection::ClientInFlight)
        ));
        let second = admission
            .admit(second_ip)
            .expect("another client should be admitted");
        assert!(matches!(
            admission.admit("192.0.2.3".parse().expect("valid IP")),
            Err(AdmissionRejection::GlobalInFlight)
        ));

        drop((first, second));
    }

    #[test]
    fn connections_are_limited_per_direct_client_identity() {
        let controller = ConnectionController::new(1);
        let ip = "192.0.2.1".parse().expect("valid IP");

        let first = controller.admit(ip).expect("first connection is admitted");
        assert!(controller.admit(ip).is_none());
        assert!(controller
            .admit("192.0.2.2".parse().expect("valid IP"))
            .is_some());

        drop(first);
        assert!(controller.admit(ip).is_some());
    }

    #[test]
    fn json_content_type_is_strict_but_allows_parameters() {
        let mut headers = header::HeaderMap::new();
        assert!(!has_json_content_type(&headers));

        headers.insert(
            header::CONTENT_TYPE,
            "application/json; charset=utf-8"
                .parse()
                .expect("valid header"),
        );
        assert!(has_json_content_type(&headers));

        headers.insert(
            header::CONTENT_TYPE,
            "text/plain".parse().expect("valid header"),
        );
        assert!(!has_json_content_type(&headers));
    }

    #[test]
    fn ipv4_mapped_ipv6_addresses_share_one_rate_limit_identity() {
        assert_eq!(
            client_identity(IpAddr::V6(
                "::ffff:192.0.2.1"
                    .parse::<Ipv6Addr>()
                    .expect("valid mapped IPv6 address"),
            )),
            IpAddr::V4("192.0.2.1".parse().expect("valid IPv4 address"))
        );
    }

    #[test]
    fn ipv6_addresses_share_one_identity_per_64_bit_prefix() {
        let first = client_identity("2001:db8:1234:5678::1".parse().expect("valid IPv6 address"));
        let rotated = client_identity(
            "2001:db8:1234:5678::ffff"
                .parse()
                .expect("valid IPv6 address"),
        );
        let other = client_identity("2001:db8:1234:5679::1".parse().expect("valid IPv6 address"));

        assert_eq!(first, rotated);
        assert_ne!(first, other);
    }
}
