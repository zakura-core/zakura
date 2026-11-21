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
    let listener_task = tokio::spawn(run_listener(
        listener,
        hyper_util::service::TowerToHyperService::new(service),
    ));

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

/// A connection that never sends a request is closed after `FIRST_REQUEST_TIMEOUT`, and one
/// that has sent a request is not. Hyper's service call marks the request as received.
#[tokio::test(start_paused = true)]
async fn only_connections_without_a_request_hit_the_first_request_timeout() {
    let _init_guard = zakura_test::init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = tower::service_fn(|_request| async {
        Ok::<_, tower::BoxError>(Response::new(HttpBody::from("ok")))
    });
    let listener_task = tokio::spawn(run_listener(
        listener,
        hyper_util::service::TowerToHyperService::new(service),
    ));

    let mut idle = TcpStream::connect(addr).await.unwrap();
    let mut active = TcpStream::connect(addr).await.unwrap();
    active
        .write_all(b"POST / HTTP/1.1\r\nhost: localhost\r\ncontent-length: 0\r\n\r\n")
        .await
        .unwrap();
    let mut response = [0; 1024];
    let read = active.read(&mut response).await.unwrap();
    assert!(response[..read].starts_with(b"HTTP/1.1 200 OK"));

    tokio::time::sleep(super::super::FIRST_REQUEST_TIMEOUT + Duration::from_secs(1)).await;
    assert_eq!(
        idle.read(&mut response).await.unwrap(),
        0,
        "the idle connection is closed"
    );
    assert!(
        timeout(Duration::from_secs(1), active.read(&mut response))
            .await
            .is_err(),
        "the connection that sent a request stays open"
    );
    listener_task.abort();
}
