//! JSON-RPC 2.0 error objects returned by RPC methods.

use std::fmt;

use serde::Serialize;

/// The result of an RPC method.
pub type RpcResult<T> = Result<T, ErrorObject>;

/// A JSON-RPC 2.0 error object, serialized as `{"code": ..., "message": ...}`.
///
/// Zakura never sends the optional `data` member, so this type doesn't have one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ErrorObject {
    code: i32,
    message: String,
}

impl ErrorObject {
    /// Returns an error with `code` and `message`.
    pub fn new(code: impl Into<ErrorCode>, message: impl Into<String>) -> Self {
        Self {
            code: code.into().code(),
            message: message.into(),
        }
    }

    /// The error code.
    pub fn code(&self) -> i32 {
        self.code
    }

    /// The error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ErrorObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ErrorObject {}

impl From<ErrorCode> for ErrorObject {
    fn from(code: ErrorCode) -> Self {
        Self::new(code, code.message())
    }
}

impl From<ErrorObject> for jsonrpsee_types::ErrorObjectOwned {
    fn from(error: ErrorObject) -> Self {
        Self::owned(error.code, error.message, None::<()>)
    }
}

/// The error codes defined by the JSON-RPC 2.0 specification, and other server error codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    /// The request isn't valid JSON.
    ParseError,
    /// The request isn't a valid JSON-RPC request.
    InvalidRequest,
    /// The method doesn't exist or isn't available.
    MethodNotFound,
    /// The method parameters are invalid.
    InvalidParams,
    /// An internal JSON-RPC error.
    InternalError,
    /// Any other error code.
    ServerError(i32),
}

impl ErrorCode {
    /// The numeric error code.
    pub const fn code(&self) -> i32 {
        match *self {
            Self::ParseError => -32700,
            Self::InvalidRequest => -32600,
            Self::MethodNotFound => -32601,
            Self::InvalidParams => -32602,
            Self::InternalError => -32603,
            Self::ServerError(code) => code,
        }
    }

    /// The standard message for this error code.
    pub const fn message(&self) -> &'static str {
        match self {
            Self::ParseError => "Parse error",
            Self::InvalidRequest => "Invalid request",
            Self::MethodNotFound => "Method not found",
            Self::InvalidParams => "Invalid params",
            Self::InternalError => "Internal error",
            Self::ServerError(_) => "Server error",
        }
    }
}

impl From<i32> for ErrorCode {
    fn from(code: i32) -> Self {
        Self::ServerError(code)
    }
}
