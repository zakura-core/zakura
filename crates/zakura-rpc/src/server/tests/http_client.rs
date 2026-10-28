//! A minimal HTTP/1.1 client for testing RPC listeners.

use std::{net::SocketAddr, sync::Arc};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{
    client::conn::http1,
    header::{AUTHORIZATION, CONTENT_TYPE, HOST},
    Request, StatusCode,
};
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tokio_rustls::{client::TlsStream, TlsConnector};

use super::{super::parse_tls_cert_chain, tls::HANDSHAKE_CERT};

/// An open HTTP/1.1 connection to an RPC listener.
///
/// The connection stays open until this is dropped.
pub(super) struct HttpConnection {
    sender: http1::SendRequest<Full<Bytes>>,
    host: String,
}

impl HttpConnection {
    /// Starts an HTTP/1.1 connection on `stream`, to a server called `host`.
    pub(super) async fn new<S>(stream: S, host: impl Into<String>) -> hyper::Result<Self>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (sender, connection) = http1::handshake(TokioIo::new(stream)).await?;
        tokio::spawn(connection);
        Ok(Self {
            sender,
            host: host.into(),
        })
    }

    /// Opens a plain HTTP connection to `address`.
    pub(super) async fn connect(address: SocketAddr) -> hyper::Result<Self> {
        let stream = TcpStream::connect(address)
            .await
            .expect("the test listener accepts connections");
        Self::new(stream, address.to_string()).await
    }

    /// Opens an HTTPS connection to `address`, which serves [`HANDSHAKE_CERT`].
    pub(super) async fn connect_tls(address: SocketAddr) -> hyper::Result<Self> {
        Self::new(tls_stream(address).await, "localhost").await
    }

    /// Sends `body` as a JSON `POST` request, with an `Authorization` header if it is set,
    /// and returns the response status and body.
    pub(super) async fn post_json(
        &mut self,
        body: impl Into<String>,
        authorization: Option<&str>,
    ) -> hyper::Result<(StatusCode, String)> {
        let mut request = Request::post("/")
            .header(HOST, &self.host)
            .header(CONTENT_TYPE, "application/json");
        if let Some(authorization) = authorization {
            request = request.header(AUTHORIZATION, authorization);
        }
        let request = request
            .body(Full::new(Bytes::from(body.into())))
            .expect("the request is valid");

        self.sender.ready().await?;
        let response = self.sender.send_request(request).await?;
        let status = response.status();
        let body = response.into_body().collect().await?.to_bytes();
        Ok((status, String::from_utf8_lossy(&body).into_owned()))
    }
}

/// Returns a TLS connector that trusts only [`HANDSHAKE_CERT`], which is for `localhost`.
pub(super) fn tls_connector() -> TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    for certificate in parse_tls_cert_chain(HANDSHAKE_CERT.as_bytes()).unwrap() {
        roots.add(certificate).unwrap();
    }
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
}

/// Completes a TLS handshake with `address`, which serves [`HANDSHAKE_CERT`], without
/// sending an HTTP request.
pub(super) async fn tls_stream(address: SocketAddr) -> TlsStream<TcpStream> {
    let socket = TcpStream::connect(address).await.unwrap();
    tls_connector()
        .connect(ServerName::try_from("localhost").unwrap(), socket)
        .await
        .unwrap()
}
