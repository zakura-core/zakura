//! Tests for the plain HTTP listener's connection handling.

use std::time::Duration;

use hyper::Response;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

use super::super::{http::HttpBody, run_listener};

/// Stopping the listener closes keep-alive connections that are still open, as `jsonrpsee` did.
#[tokio::test]
async fn stopping_the_listener_closes_open_connections() {
    let _init_guard = zakura_test::init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = tower::service_fn(|_request| async {
        Ok::<_, tower::BoxError>(Response::new(HttpBody::from("ok")))
    });
    let listener_task = tokio::spawn(run_listener(listener, service));

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"POST / HTTP/1.1\r\nhost: localhost\r\ncontent-length: 0\r\n\r\n")
        .await
        .unwrap();
    let mut response = [0; 1024];
    let read = timeout(Duration::from_secs(10), stream.read(&mut response))
        .await
        .expect("the server answers the first request")
        .unwrap();
    assert!(response[..read].starts_with(b"HTTP/1.1 200 OK"));

    listener_task.abort();

    let read = timeout(Duration::from_secs(10), stream.read(&mut response))
        .await
        .expect("the server closes the idle keep-alive connection when it stops");
    assert_eq!(
        read.unwrap(),
        0,
        "the connection is closed without a response"
    );
}
