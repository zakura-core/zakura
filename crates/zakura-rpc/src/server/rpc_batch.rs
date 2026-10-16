//! Per-call middleware dispatch for JSON-RPC batch requests.

use std::future::Future;

use jsonrpsee::{
    core::middleware::{Batch, BatchEntry, Notification},
    server::{middleware::rpc::RpcServiceT, BatchResponseBuilder},
    MethodResponse,
};

/// Middleware that runs every call in a JSON-RPC batch through the call middleware stack.
///
/// `jsonrpsee` passes a batch to [`RpcServiceT::batch`], and its root service
/// executes the batched calls directly, so per-call middleware such as
/// [`FixRpcResponseMiddleware`](super::rpc_call_compatibility::FixRpcResponseMiddleware),
/// [`RpcMetricsMiddleware`](super::rpc_metrics::RpcMetricsMiddleware) and
/// [`RpcTracingMiddleware`](super::rpc_tracing::RpcTracingMiddleware) would not see them.
/// This middleware must be the outermost RPC layer: it splits each batch into
/// individual [`RpcServiceT::call`]s on the inner stack and assembles the batch
/// response under the server's response size limit.
#[derive(Clone)]
pub struct RpcBatchMiddleware<S> {
    service: S,
    max_response_body_size: usize,
}

impl<S> RpcBatchMiddleware<S> {
    /// Create a new `RpcBatchMiddleware` with the given `service` and batch response size limit.
    pub fn new(service: S, max_response_body_size: usize) -> Self {
        Self {
            service,
            max_response_body_size,
        }
    }
}

impl<S> RpcServiceT for RpcBatchMiddleware<S>
where
    S: RpcServiceT<
            MethodResponse = MethodResponse,
            NotificationResponse = MethodResponse,
            BatchResponse = MethodResponse,
        > + Send
        + Sync
        + Clone
        + 'static,
{
    type MethodResponse = MethodResponse;
    type NotificationResponse = MethodResponse;
    type BatchResponse = MethodResponse;

    fn call<'a>(
        &self,
        request: jsonrpsee::types::Request<'a>,
    ) -> impl Future<Output = MethodResponse> + Send + 'a {
        self.service.call(request)
    }

    fn batch<'a>(&self, batch: Batch<'a>) -> impl Future<Output = MethodResponse> + Send + 'a {
        let service = self.service.clone();
        let mut batch_response = BatchResponseBuilder::new_with_limit(self.max_response_body_size);

        async move {
            let mut got_notification = false;

            for entry in batch {
                let response = match entry {
                    Ok(BatchEntry::Call(request)) => service.call(request).await,
                    Ok(BatchEntry::Notification(notification)) => {
                        got_notification = true;
                        service.notification(notification).await;
                        continue;
                    }
                    Err(error) => {
                        let (error, id) = error.into_parts();
                        MethodResponse::error(id, error)
                    }
                };

                if let Err(too_large) = batch_response.append(response) {
                    return too_large;
                }
            }

            // Match jsonrpsee: a batch of only notifications gets no response,
            // and an empty batch is an invalid request.
            if batch_response.is_empty() && got_notification {
                MethodResponse::notification()
            } else {
                MethodResponse::from_batch(batch_response.finish())
            }
        }
    }

    fn notification<'a>(
        &self,
        notification: Notification<'a>,
    ) -> impl Future<Output = MethodResponse> + Send + 'a {
        self.service.notification(notification)
    }
}
