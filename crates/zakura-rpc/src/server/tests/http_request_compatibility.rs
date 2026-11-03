//! HTTP boundary checks that run before RPC admission.

use std::{future::Ready, task::Poll};

use bytes::Bytes;
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use tower::{BoxError, Service};

use crate::server::{
    cookie::Cookie,
    http::{HttpBody, HttpRequest, HttpResponse},
    http_request_compatibility::HttpRequestMiddleware,
};

fn unexpected_dispatch(_: HttpRequest) -> Ready<Result<HttpResponse, BoxError>> {
    panic!("rejected requests must not dispatch")
}

#[tokio::test]
async fn unauthenticated_request_does_not_poll_body_or_dispatch() {
    let body = StreamBody::new(futures::stream::poll_fn(
        |_| -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
            panic!("unauthenticated request bodies must not be polled")
        },
    ));
    let service = tower::service_fn(unexpected_dispatch);
    let mut boundary = HttpRequestMiddleware::new(service, Some(Cookie::default()), 64);
    assert!(boundary.call(HttpRequest::new(body)).await.is_err());
}

#[tokio::test]
async fn body_errors_and_size_limit_failures_do_not_dispatch() {
    let service = tower::service_fn(unexpected_dispatch);
    let mut boundary = HttpRequestMiddleware::new(service, None, 64);
    assert!(boundary
        .call(HttpRequest::new(HttpBody::from(vec![b'x'; 65])))
        .await
        .is_err());
    let body = StreamBody::new(futures::stream::iter([Err::<Frame<Bytes>, BoxError>(
        "connection reset".into(),
    )]));
    assert!(boundary.call(HttpRequest::new(body)).await.is_err());
}

#[tokio::test]
async fn boundary_forwards_raw_body_and_response_without_json_conversion() {
    for original in [r#" {"method":"getinfo","params":[ ],"id":1}"#, "not JSON"] {
        let service = tower::service_fn(move |request: HttpRequest| async move {
            assert_eq!(request.headers()["content-type"], "application/json");
            assert_eq!(request.into_body().collect().await?.to_bytes(), original);
            Ok::<_, BoxError>(HttpResponse::new(HttpBody::from("unchanged response")))
        });
        let mut boundary = HttpRequestMiddleware::new(service, None, 64);
        let response = boundary
            .call(HttpRequest::new(HttpBody::from(original)))
            .await
            .unwrap()
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        assert_eq!(response, "unchanged response");
    }
}
