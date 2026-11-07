//! Compatibility fixes for JSON-RPC remote procedure calls.
//!
//! These fixes are applied at the JSON-RPC call level,
//! after the RPC request is parsed and split into calls.

use std::future::Future;

use super::{
    error::LegacyCode,
    jsonrpc::{ErrorCode, ErrorObject, MethodResponse, Notification, Request},
    rpc_service::RpcServiceT,
};

/// JSON-RPC [`FixRpcResponseMiddleware`] with compatibility workarounds.
///
/// This middleware makes the following changes to JSON-RPC calls:
///
/// ## Make RPC framework response codes match `zcashd`
///
/// JSON-RPC 2.0 uses [`ErrorCode::InvalidParams`] for parameters that can't be decoded,
/// but `zcashd` uses [`LegacyCode::Misc`], and some RPC clients rely on the exact code.
/// So this middleware replaces the code of every invalid params error.
#[derive(Clone)]
pub struct FixRpcResponseMiddleware<S> {
    service: S,
}

impl<S> FixRpcResponseMiddleware<S> {
    /// Create a new `FixRpcResponseMiddleware` with the given `service`.
    pub fn new(service: S) -> Self {
        Self { service }
    }
}

impl<S: RpcServiceT> RpcServiceT for FixRpcResponseMiddleware<S> {
    fn call<'a>(&self, request: Request<'a>) -> impl Future<Output = MethodResponse> + Send + 'a {
        let service = self.service.clone();
        let id = request.id.clone();

        async move {
            let response = service.call(request).await;
            if response.as_error_code() != Some(ErrorCode::InvalidParams.code()) {
                return response;
            }

            let new_error_code = LegacyCode::Misc;
            tracing::debug!(
                "Replacing RPC error: {} with {}",
                ErrorCode::InvalidParams.code(),
                i32::from(LegacyCode::Misc),
            );

            #[derive(serde::Deserialize)]
            struct ErrorResponse {
                error: ErrorMessage,
            }

            #[derive(serde::Deserialize)]
            struct ErrorMessage {
                message: String,
            }

            let message = serde_json::from_str::<ErrorResponse>(response.as_json().get())
                .map_or_else(
                    |_| ErrorCode::InvalidParams.message().to_string(),
                    |response| response.error.message,
                );

            MethodResponse::error(id, ErrorObject::new(new_error_code, message))
        }
    }

    fn notification<'a>(
        &self,
        notification: Notification<'a>,
    ) -> impl Future<Output = MethodResponse> + Send + 'a {
        self.service.notification(notification)
    }
}
