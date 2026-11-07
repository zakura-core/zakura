//! TRACE logging of JSON-RPC request and response payloads.

use std::{fmt, future::Future};

use super::{
    jsonrpc::{MethodResponse, Notification, Request},
    rpc_service::RpcServiceT,
};
use serde::Serialize;
use tracing::Instrument;

/// Middleware that logs JSON-RPC payloads at TRACE level, truncated to a
/// maximum number of characters.
///
/// The spans, targets and messages match the `RpcLogger` of `jsonrpsee`, which
/// Zakura used before, so existing filters keep working, including span-scoped
/// ones like `[method_call]=trace`. A payload is only serialized when its event
/// is enabled, and responses are logged without copying them.
#[derive(Clone)]
pub struct RpcLoggerMiddleware<S> {
    service: S,
    max_chars: usize,
}

impl<S> RpcLoggerMiddleware<S> {
    /// Create a new `RpcLoggerMiddleware` that logs at most `max_chars`
    /// characters of each payload.
    pub fn new(service: S, max_chars: usize) -> Self {
        Self { service, max_chars }
    }
}

// The span targets are `jsonrpsee`'s `RpcLogger` module path, so filters keep matching them.
impl<S: RpcServiceT> RpcServiceT for RpcLoggerMiddleware<S> {
    #[tracing::instrument(
        name = "method_call",
        target = "jsonrpsee_core::middleware::layer::logger",
        skip_all,
        fields(method = request.method_name()),
        level = "trace"
    )]
    fn call<'a>(&self, request: Request<'a>) -> impl Future<Output = MethodResponse> + Send + 'a {
        let max_chars = self.max_chars;
        tracing::trace!(target: "jsonrpsee", "request = {}", TruncatedJson(&request, max_chars));

        let service = self.service.clone();
        async move {
            let response = service.call(request).await;
            tracing::trace!(
                target: "jsonrpsee",
                "response = {}",
                Truncated(response.as_json().get(), max_chars)
            );
            response
        }
        .in_current_span()
    }

    #[tracing::instrument(
        name = "notification",
        target = "jsonrpsee_core::middleware::layer::logger",
        skip_all,
        fields(method = &*notification.method),
        level = "trace"
    )]
    fn notification<'a>(
        &self,
        notification: Notification<'a>,
    ) -> impl Future<Output = MethodResponse> + Send + 'a {
        tracing::trace!(
            target: "jsonrpsee",
            "notification request = {}",
            TruncatedJson(&notification, self.max_chars)
        );

        self.service.notification(notification).in_current_span()
    }
}

/// Displays a value as JSON truncated like [`Truncated`], serializing it only
/// when it is formatted.
struct TruncatedJson<'a, T>(&'a T, usize);

impl<T: Serialize> fmt::Display for TruncatedJson<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match serde_json::to_string(self.0) {
            Ok(json) => fmt::Display::fmt(&Truncated(&json, self.1), f),
            Err(_) => f.write_str("<invalid JSON>"),
        }
    }
}

/// Displays at most the given number of characters of a string, cut at a
/// character boundary.
struct Truncated<'a>(&'a str, usize);

impl fmt::Display for Truncated<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(text, max_chars) = *self;
        let end = text
            .char_indices()
            .nth(max_chars)
            .map_or(text.len(), |(index, _)| index);
        f.write_str(&text[..end])
    }
}
