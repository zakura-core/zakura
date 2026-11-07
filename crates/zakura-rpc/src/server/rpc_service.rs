//! Running JSON-RPC calls through the call middleware stack.

use std::{collections::HashSet, future::Future, pin::Pin, sync::Arc};

use super::jsonrpc::{
    BatchEntry, BatchResponseBuilder, Body, ErrorCode, Id, MethodResponse, Notification, Params,
    Request,
};

/// A service that answers JSON-RPC calls and notifications.
///
/// Middleware implements this by wrapping an inner service.
pub trait RpcServiceT: Clone + Send + Sync + 'static {
    /// Answers a method call.
    fn call<'a>(&self, request: Request<'a>) -> impl Future<Output = MethodResponse> + Send + 'a;

    /// Handles a notification, which gets a `null` response.
    fn notification<'a>(
        &self,
        notification: Notification<'a>,
    ) -> impl Future<Output = MethodResponse> + Send + 'a;
}

/// A set of RPC methods that can be called by name.
///
/// [`rpc_trait`](super::jsonrpc::rpc_trait) implements this for every type that implements its
/// RPC method trait.
pub trait RpcMethods: Clone + Send + Sync + 'static {
    /// The names of every method.
    const METHOD_NAMES: &'static [&'static str];

    /// Calls `method` with `params`, returning its response with `id`, or `None` if there is no
    /// such method.
    ///
    /// A response longer than `max_response_size` bytes is replaced by an error.
    #[allow(clippy::type_complexity)]
    fn call<'a>(
        &'a self,
        method: &str,
        params: Params<'a>,
        id: Id<'a>,
        max_response_size: usize,
    ) -> Option<Pin<Box<dyn Future<Output = MethodResponse> + Send + 'a>>>;
}

/// The innermost service, which calls the enabled methods.
#[derive(Clone)]
pub struct RpcService<M> {
    // Middleware clones this service for every call, so the methods are shared.
    methods: Arc<M>,
    enabled_methods: Arc<HashSet<&'static str>>,
    max_response_body_size: usize,
}

impl<M: RpcMethods> RpcService<M> {
    /// Returns a service that calls `enabled_methods` of `methods`, and returns responses of up
    /// to `max_response_body_size` bytes.
    pub fn new(
        methods: M,
        enabled_methods: impl IntoIterator<Item = &'static str>,
        max_response_body_size: usize,
    ) -> Self {
        Self {
            methods: Arc::new(methods),
            enabled_methods: Arc::new(enabled_methods.into_iter().collect()),
            max_response_body_size,
        }
    }
}

impl<M: RpcMethods> RpcServiceT for RpcService<M> {
    fn call<'a>(&self, request: Request<'a>) -> impl Future<Output = MethodResponse> + Send + 'a {
        let service = self.clone();

        async move {
            let Request {
                id, method, params, ..
            } = request;

            if !service.enabled_methods.contains(&*method) {
                return MethodResponse::error(id, ErrorCode::MethodNotFound.into());
            }

            let params = Params::new(params.as_deref().map(serde_json::value::RawValue::get));
            let response =
                service
                    .methods
                    .call(&method, params, id.clone(), service.max_response_body_size);

            match response {
                Some(response) => response.await,
                None => MethodResponse::error(id, ErrorCode::MethodNotFound.into()),
            }
        }
    }

    fn notification<'a>(
        &self,
        _notification: Notification<'a>,
    ) -> impl Future<Output = MethodResponse> + Send + 'a {
        // Notifications aren't run, because nothing could see their results.
        std::future::ready(MethodResponse::notification())
    }
}

/// Answers a request body that starts with `{` if `is_single`, or `[` otherwise.
///
/// Each batched call goes through `service` on its own, so per-call middleware sees it. Batches
/// can have up to `max_batch_entries` entries, and batch responses can be up to
/// `max_response_body_size` bytes long.
pub async fn handle_rpc_call<S: RpcServiceT>(
    body: &[u8],
    is_single: bool,
    service: &S,
    max_batch_entries: usize,
    max_response_body_size: usize,
) -> MethodResponse {
    match Body::parse(body, is_single, max_batch_entries) {
        Body::Call(request) => service.call(request).await,
        Body::Notification(notification) => service.notification(notification).await,
        Body::Error(response) => response,
        Body::Batch(entries) => {
            let mut batch_response = BatchResponseBuilder::new(max_response_body_size);
            let mut got_notification = false;

            for entry in entries {
                let response = match entry {
                    BatchEntry::Call(request) => service.call(request).await,
                    BatchEntry::Notification(notification) => {
                        got_notification = true;
                        service.notification(notification).await;
                        continue;
                    }
                    BatchEntry::Invalid(id) => {
                        MethodResponse::error(id, ErrorCode::InvalidRequest.into())
                    }
                };

                if let Err(too_large) = batch_response.append(response) {
                    return too_large;
                }
            }

            // A batch of only notifications gets no response, and an empty batch is an invalid
            // request.
            if batch_response.is_empty() && got_notification {
                MethodResponse::notification()
            } else {
                batch_response.finish()
            }
        }
    }
}

/// Calls `method` of `methods` directly with `params`, returning its result or error.
///
/// Like a request, the call goes through parameter decoding and result serialization, but not
/// through any middleware.
#[cfg(test)]
pub(crate) async fn call_method<M, T>(
    methods: &M,
    method: &str,
    params: impl serde::Serialize,
) -> Result<T, super::jsonrpc::ErrorObject>
where
    M: RpcMethods,
    T: serde::de::DeserializeOwned,
{
    #[derive(serde::Deserialize)]
    struct Response {
        result: Option<serde_json::Value>,
        error: Option<Error>,
    }

    #[derive(serde::Deserialize)]
    struct Error {
        code: i32,
        message: String,
    }

    let params = serde_json::to_string(&params).expect("test params serialize");
    let response = methods
        .call(
            method,
            Params::new(Some(&params)),
            Id::Number(0),
            usize::MAX,
        )
        .unwrap_or_else(|| panic!("{method} is a method"))
        .await;
    let response: Response =
        serde_json::from_str(response.as_json().get()).expect("responses are JSON-RPC responses");

    match (response.result, response.error) {
        (_, Some(error)) => Err(super::jsonrpc::ErrorObject::new(error.code, error.message)),
        (Some(result), None) => {
            Ok(serde_json::from_value(result).expect("test results deserialize"))
        }
        (None, None) => {
            Ok(serde_json::from_value(serde_json::Value::Null).expect("test results deserialize"))
        }
    }
}
