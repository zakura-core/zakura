//! RPC metrics middleware for Prometheus metrics collection.
//!
//! This middleware collects metrics for JSON-RPC requests, including:
//! - Request count by method and status
//! - Request duration by method
//! - Active request count
//! - Error count by method and error code
//!
//! These metrics complement the OpenTelemetry tracing in `rpc_tracing.rs`,
//! providing aggregated data suitable for dashboards and alerting.

use std::{future::Future, time::Instant};

use super::{
    jsonrpc::{MethodResponse, Notification, Request},
    rpc_service::RpcServiceT,
};

use crate::methods::RPC_METHOD_ACCESS;

/// Middleware that collects Prometheus metrics for each RPC request.
///
/// This middleware records:
/// - `rpc.requests.total{method, status}` - Counter of requests by method and status
/// - `rpc.request.duration_seconds{method}` - Histogram of request durations
/// - `rpc.active_requests` - Gauge of currently active requests
/// - `rpc.errors.total{method, error_code}` - Counter of errors by method and code
///
/// Calls to methods that aren't registered are recorded with the `unknown`
/// method label, so clients can't add metric series by sending new names.
#[derive(Clone)]
pub struct RpcMetricsMiddleware<S> {
    service: S,
}

impl<S> RpcMetricsMiddleware<S> {
    /// Create a new `RpcMetricsMiddleware` with the given `service`.
    pub fn new(service: S) -> Self {
        Self { service }
    }
}

impl<S: RpcServiceT> RpcServiceT for RpcMetricsMiddleware<S> {
    fn call<'a>(&self, request: Request<'a>) -> impl Future<Output = MethodResponse> + Send + 'a {
        let service = self.service.clone();
        let method = method_label(request.method_name());
        let start = Instant::now();

        // Increment active requests gauge
        metrics::gauge!("rpc.active_requests").increment(1.0);

        async move {
            let response = service.call(request).await;
            let duration = start.elapsed().as_secs_f64();

            // Determine status and record metrics
            let status = if response.is_error() {
                "error"
            } else {
                "success"
            };

            // Record request count
            metrics::counter!("rpc.requests.total", "method" => method, "status" => status)
                .increment(1);

            // Record request duration
            metrics::histogram!("rpc.request.duration_seconds", "method" => method)
                .record(duration);

            // Record errors with error code
            if response.is_error() {
                let error_code = response
                    .as_error_code()
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                metrics::counter!(
                    "rpc.errors.total",
                    "method" => method,
                    "error_code" => error_code
                )
                .increment(1);
            }

            // Decrement active requests gauge
            metrics::gauge!("rpc.active_requests").decrement(1.0);

            response
        }
    }

    fn notification<'a>(
        &self,
        notification: Notification<'a>,
    ) -> impl Future<Output = MethodResponse> + Send + 'a {
        self.service.notification(notification)
    }
}

/// Returns the metrics label for an RPC method: its name if it is a registered
/// method, and `unknown` otherwise.
fn method_label(method_name: &str) -> &'static str {
    RPC_METHOD_ACCESS
        .iter()
        .find_map(|(name, _)| (*name == method_name).then_some(*name))
        .unwrap_or("unknown")
}
