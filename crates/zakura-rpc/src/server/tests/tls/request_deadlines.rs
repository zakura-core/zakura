//! Drive real TLS in memory so request bytes and clock advances reach Hyper in a known order.

use super::*;
use futures::{future::BoxFuture, poll, FutureExt};
use http_body_util::BodyExt;
use hyper::{body::Incoming, Request, Response};
use std::task::Poll;
use tokio::io::{duplex, DuplexStream};
use tokio::time::advance;

use crate::server::http::HttpBody;

const REQUEST: &[u8] = b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1\r\n\r\nx";

struct Session {
    client: TlsStream<DuplexStream>,
    server: BoxFuture<'static, Result<(), BoxError>>,
}

impl Session {
    async fn new() -> Self {
        let (socket, peer) = duplex(8192);
        let acceptor = test_acceptor();
        let connector = tls_connector();
        let (server, client) = tokio::join!(
            acceptor.accept(socket),
            connector.connect(ServerName::try_from("localhost").unwrap(), peer),
        );
        let service = hyper::service::service_fn(|request: Request<Incoming>| async move {
            let status = match request.uri().path() {
                "/" => hyper::StatusCode::OK,
                "/slow" => {
                    sleep(Duration::from_secs(20)).await;
                    hyper::StatusCode::OK
                }
                _ => hyper::StatusCode::NOT_FOUND,
            };
            request.into_body().collect().await?;
            Ok::<_, BoxError>(
                Response::builder()
                    .status(status)
                    .body(HttpBody::from("pong"))?,
            )
        });
        let server = serve_tls_connection(
            server.unwrap(),
            service,
            std::future::pending(),
            TLS_HEADER_READ_TIMEOUT,
        )
        .boxed();
        let mut session = Self {
            client: client.unwrap(),
            server,
        };
        session.pending().await;
        session
    }

    async fn send(&mut self, bytes: &[u8]) {
        self.client.write_all(bytes).await.unwrap();
        self.client.flush().await.unwrap();
        self.pending().await;
    }

    async fn pending(&mut self) {
        assert!(
            poll!(&mut self.server).is_pending(),
            "the TLS connection should still be open"
        );
    }

    async fn response(&mut self) {
        // The service consumes the body asynchronously. Drive it through the response write.
        let mut data = [0; 512];
        let read = self.client.read(&mut data);
        tokio::select! {
            biased;
            result = &mut self.server => panic!("closed before replying: {result:?}"),
            n = read => {
                let response = &data[..n.unwrap()];
                assert!(response.starts_with(b"HTTP/1.1 200 "));
                assert!(response.ends_with(b"pong"));
            }
        }
        self.pending().await;
    }

    async fn expires_after(&mut self, seconds: u64) {
        advance(Duration::from_secs(seconds)).await;
        let Poll::Ready(Err(error)) = poll!(&mut self.server) else {
            panic!("deadline should have elapsed")
        };
        assert!(error.downcast_ref::<hyper::Error>().unwrap().is_timeout());
    }
}

#[tokio::test(start_paused = true)]
async fn first_header_must_finish_within_thirty_seconds_of_handshake() {
    let mut session = Session::new().await;
    advance(Duration::from_secs(29)).await;
    session.send(b"POST ").await;
    session.expires_after(1).await;
}

#[tokio::test(start_paused = true)]
async fn late_request_gets_thirty_seconds_for_headers_then_a_fresh_idle_deadline() {
    let mut session = Session::new().await;
    session.send(REQUEST).await;
    session.response().await;
    advance(Duration::from_secs(89)).await;
    session.send(&REQUEST[..5]).await;
    advance(Duration::from_secs(29)).await;
    session.send(&REQUEST[5..]).await;
    session.response().await;
    advance(Duration::from_secs(89)).await;
    session.pending().await;
    session.expires_after(1).await;
}

#[tokio::test(start_paused = true)]
async fn more_header_bytes_do_not_restart_the_deadline() {
    let mut session = Session::new().await;
    session.send(REQUEST).await;
    session.response().await;
    advance(Duration::from_secs(45)).await;
    session.send(b"POST ").await;
    advance(Duration::from_secs(29)).await;
    session.send(b"/ ").await;
    session.expires_after(1).await;
}

#[tokio::test(start_paused = true)]
async fn a_buffered_partial_next_request_is_timed_from_receipt() {
    let mut session = Session::new().await;
    session
        .send(b"GET /slow HTTP/1.1\r\nHost: localhost\r\n\r\nPOST ")
        .await;
    advance(Duration::from_secs(20)).await;
    session.response().await;
    session.expires_after(10).await;
}

#[tokio::test(start_paused = true)]
async fn an_incomplete_body_does_not_use_the_keep_alive_deadline() {
    let mut session = Session::new().await;
    // Complete the headers, then hold the single body byte past both deadlines.
    session.send(&REQUEST[..REQUEST.len() - 1]).await;
    advance(Duration::from_secs(120)).await;
    session.send(b"x").await;
    session.response().await;
    advance(Duration::from_secs(89)).await;
    session.send(REQUEST).await;
    session.response().await;
}

#[tokio::test(start_paused = true)]
async fn an_http_error_keeps_the_short_deadline() {
    let mut session = Session::new().await;
    session
        .send(b"GET /missing HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await;
    session.expires_after(30).await;
}
