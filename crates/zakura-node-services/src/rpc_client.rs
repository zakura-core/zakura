//! A client for calling Zakura's JSON-RPC methods.
//!
//! Used in various tests and tools.

use std::{fmt, net::SocketAddr, time::Duration};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{
    client::conn::http1,
    header::{AUTHORIZATION, CONTENT_TYPE, HOST},
    Method, Request, Response,
};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

use crate::BoxError;

/// The default timeout for RPC requests.
///
/// This is a safety net to prevent RPC calls from hanging indefinitely
/// when a server is alive but unresponsive.
const RPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(180);

/// An HTTP client for making JSON-RPC requests.
#[derive(Clone, Debug)]
pub struct RpcRequestClient {
    rpc_address: SocketAddr,
    timeout: Duration,
    /// The value of the `Authorization` header, if requests use HTTP basic authentication.
    authorization: Option<String>,
}

/// Why an RPC request failed.
#[derive(Debug)]
pub enum RpcRequestError {
    /// The server didn't answer within the client's timeout.
    TimedOut(Duration),
    /// The client couldn't connect to the server.
    Connect(std::io::Error),
    /// The HTTP exchange failed.
    Http(hyper::Error),
}

impl RpcRequestError {
    /// Whether the request timed out.
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::TimedOut(_))
    }
}

impl fmt::Display for RpcRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TimedOut(timeout) => write!(f, "RPC request timed out after {timeout:?}"),
            Self::Connect(error) => write!(f, "failed to connect to the RPC server: {error}"),
            Self::Http(error) => write!(f, "RPC request failed: {error}"),
        }
    }
}

impl std::error::Error for RpcRequestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::TimedOut(_) => None,
            Self::Connect(error) => Some(error),
            Self::Http(error) => Some(error),
        }
    }
}

impl RpcRequestClient {
    /// Creates a new RPC request client with the default timeout.
    pub fn new(rpc_address: SocketAddr) -> Self {
        Self::new_with_timeout(rpc_address, RPC_REQUEST_TIMEOUT)
    }

    /// Creates a new RPC request client with a custom request timeout.
    ///
    /// Use [`RpcRequestClient::new()`] for the default timeout.
    pub fn new_with_timeout(rpc_address: SocketAddr, timeout: Duration) -> Self {
        Self {
            rpc_address,
            timeout,
            authorization: None,
        }
    }

    /// Returns this client, sending HTTP basic authentication with `user` and `password`.
    pub fn with_basic_auth(mut self, user: &str, password: &str) -> Self {
        self.authorization = Some(format!(
            "Basic {}",
            STANDARD.encode(format!("{user}:{password}"))
        ));
        self
    }

    /// Builds rpc request
    pub async fn call(
        &self,
        method: impl AsRef<str>,
        params: impl AsRef<str>,
    ) -> Result<Response<Bytes>, RpcRequestError> {
        self.post(method, params, Some("application/json")).await
    }

    /// Builds rpc request with a variable `content-type`.
    pub async fn call_with_content_type(
        &self,
        method: impl AsRef<str>,
        params: impl AsRef<str>,
        content_type: String,
    ) -> Result<Response<Bytes>, RpcRequestError> {
        self.post(method, params, Some(&content_type)).await
    }

    /// Builds rpc request with no content type.
    pub async fn call_with_no_content_type(
        &self,
        method: impl AsRef<str>,
        params: impl AsRef<str>,
    ) -> Result<Response<Bytes>, RpcRequestError> {
        self.post(method, params, None).await
    }

    /// Builds rpc request and gets text from response
    pub async fn text_from_call(
        &self,
        method: impl AsRef<str>,
        params: impl AsRef<str>,
    ) -> Result<String, RpcRequestError> {
        let response = self.call(method, params).await?;
        Ok(String::from_utf8_lossy(response.body()).into_owned())
    }

    /// Sends a `GET` request for the server's root path, for HTTP endpoints like metrics.
    pub async fn get(&self) -> Result<Response<Bytes>, RpcRequestError> {
        self.send(Method::GET, None, Bytes::new()).await
    }

    /// Sends a JSON-RPC request for `method` with `params`, with `content_type` if it is set.
    async fn post(
        &self,
        method: impl AsRef<str>,
        params: impl AsRef<str>,
        content_type: Option<&str>,
    ) -> Result<Response<Bytes>, RpcRequestError> {
        let method = method.as_ref();
        let params = params.as_ref();
        let body =
            format!(r#"{{"jsonrpc": "2.0", "method": "{method}", "params": {params}, "id":123 }}"#);
        self.send(Method::POST, content_type, body.into()).await
    }

    /// Sends a request on a new connection, and receives the whole response, within the
    /// client's timeout.
    async fn send(
        &self,
        method: Method,
        content_type: Option<&str>,
        body: Bytes,
    ) -> Result<Response<Bytes>, RpcRequestError> {
        let mut request = Request::builder()
            .method(method)
            .uri("/")
            .header(HOST, self.rpc_address.to_string());
        if let Some(content_type) = content_type {
            request = request.header(CONTENT_TYPE, content_type);
        }
        if let Some(authorization) = &self.authorization {
            request = request.header(AUTHORIZATION, authorization);
        }
        let request = request
            .body(Full::new(body))
            .expect("the method, path, and headers are valid");

        let exchange = async {
            let stream = TcpStream::connect(self.rpc_address)
                .await
                .map_err(RpcRequestError::Connect)?;
            let (mut sender, connection) = http1::handshake(TokioIo::new(stream))
                .await
                .map_err(RpcRequestError::Http)?;
            tokio::spawn(connection);

            let response = sender
                .send_request(request)
                .await
                .map_err(RpcRequestError::Http)?;
            let (parts, body) = response.into_parts();
            let body = body.collect().await.map_err(RpcRequestError::Http)?;
            Ok(Response::from_parts(parts, body.to_bytes()))
        };

        tokio::time::timeout(self.timeout, exchange)
            .await
            .map_err(|_| RpcRequestError::TimedOut(self.timeout))?
    }

    /// Builds an RPC request, awaits its response, and attempts to deserialize
    /// it to the expected result type.
    ///
    /// Returns Ok with json result from response if successful.
    /// Returns an error if the call or result deserialization fail.
    pub async fn json_result_from_call<T: serde::de::DeserializeOwned>(
        &self,
        method: impl AsRef<str>,
        params: impl AsRef<str>,
    ) -> std::result::Result<T, BoxError> {
        Self::json_result_from_response_text(&self.text_from_call(method, params).await?)
    }

    /// Accepts response text from an RPC call
    /// Returns `Ok` with a deserialized `result` value in the expected type, or an error report.
    fn json_result_from_response_text<T: serde::de::DeserializeOwned>(
        response_text: &str,
    ) -> std::result::Result<T, BoxError> {
        let mut response: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(response_text)?;

        if let Some(error) = response.remove("error").filter(|error| !error.is_null()) {
            return Err(Box::new(serde_json::from_value::<RpcError>(error)?));
        }

        let result = response
            .remove("result")
            .ok_or("JSON-RPC response has neither a result nor an error")?;

        Ok(serde_json::from_value(result)?)
    }
}

/// An error object from a JSON-RPC response.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize)]
pub struct RpcError {
    /// The error code.
    pub code: i32,
    /// The error message.
    pub message: String,
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for RpcError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_results_and_errors() {
        let result: u32 = RpcRequestClient::json_result_from_response_text(
            r#"{"jsonrpc":"2.0","id":1,"result":7}"#,
        )
        .expect("the response has a result");
        assert_eq!(result, 7);

        let result: Option<u32> = RpcRequestClient::json_result_from_response_text(
            r#"{"jsonrpc":"2.0","id":1,"result":null}"#,
        )
        .expect("a null result is a result");
        assert_eq!(result, None);

        let error = RpcRequestClient::json_result_from_response_text::<u32>(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-8,"message":"Block not found"}}"#,
        )
        .expect_err("the response has an error");
        assert_eq!(
            error.downcast_ref::<RpcError>(),
            Some(&RpcError {
                code: -8,
                message: "Block not found".to_string(),
            })
        );
        assert_eq!(error.to_string(), "-8: Block not found");

        RpcRequestClient::json_result_from_response_text::<u32>(r#"{"jsonrpc":"2.0","id":1}"#)
            .expect_err("the response has no result");
    }
    use std::sync::{Arc, Mutex};

    /// Proves that `RpcRequestClient` times out instead of hanging indefinitely
    /// when a server accepts a TCP connection but never sends a response.
    #[tokio::test]
    async fn rpc_client_timeout_on_unresponsive_server() {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("should bind to localhost");
        let addr = listener.local_addr().expect("should have a local address");

        // Hold the accepted stream so the connection stays open until the test ends.
        let held_stream = Arc::new(Mutex::new(None));
        let held_stream_clone = held_stream.clone();

        // Accept the connection but never respond.
        let accept_thread = std::thread::spawn(move || {
            let (stream, _peer_addr) = listener.accept().expect("should accept a connection");
            *held_stream_clone.lock().expect("lock poisoned") = Some(stream);
        });

        let short_timeout = Duration::from_secs(2);
        let client = RpcRequestClient::new_with_timeout(addr, short_timeout);

        // Outer timeout is a safety net — should never fire.
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            client.text_from_call("getinfo", "[]"),
        )
        .await;

        let inner_result =
            result.expect("outer safety timeout should not fire; client timeout should fire first");

        let err =
            inner_result.expect_err("request to unresponsive server should fail with timeout");
        assert!(err.is_timeout(), "error should be a timeout, got: {err}");

        drop(held_stream);
        accept_thread
            .join()
            .expect("accept thread should exit cleanly");
    }
}
