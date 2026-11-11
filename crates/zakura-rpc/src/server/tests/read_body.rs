//! Tests for reading request bodies.

use std::{convert::Infallible, task::Poll, time::Duration};

use bytes::Bytes;
use futures::{stream, StreamExt};
use http_body_util::StreamBody;
use hyper::{body::Frame, header::HeaderMap};

use crate::server::http::{read_body, ReadBodyError, MAX_REQUEST_BODY_SIZE};

/// Returns a body that sends `chunks`, then ends if `ends` is true, or never sends anything more.
fn body(
    chunks: &[&'static str],
    ends: bool,
) -> StreamBody<impl futures::Stream<Item = Result<Frame<Bytes>, Infallible>> + Send + 'static> {
    let frames: Vec<_> = chunks
        .iter()
        .map(|chunk| Ok(Frame::data(Bytes::from_static(chunk.as_bytes()))))
        .collect();
    let end = stream::poll_fn(move |_| {
        if ends {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    });

    StreamBody::new(stream::iter(frames).chain(end))
}

#[tokio::test]
async fn non_json_body_is_rejected_before_the_rest_is_read() {
    // The body never ends, so the read only finishes if it stops at the first byte.
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        read_body(
            &HeaderMap::new(),
            body(&["  x"], false),
            MAX_REQUEST_BODY_SIZE,
        ),
    )
    .await
    .expect("a body that starts with `x` should be rejected without reading the rest");

    assert!(matches!(result, Err(ReadBodyError::Malformed)));
}

#[tokio::test]
async fn first_byte_is_checked_after_whitespace_frames() {
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        read_body(
            &HeaderMap::new(),
            body(&[" \n", "", "  null"], false),
            MAX_REQUEST_BODY_SIZE,
        ),
    )
    .await
    .expect("a body whose first non-whitespace byte is `n` should be rejected");

    assert!(matches!(result, Err(ReadBodyError::Malformed)));
}

#[tokio::test]
async fn object_and_array_bodies_are_read_whole() {
    for chunks in [
        &[" \n", "", r#"{"id":1,"#, r#""method":"getinfo"}"#][..],
        &["[", "1]"][..],
    ] {
        let Ok(body) =
            read_body(&HeaderMap::new(), body(chunks, true), MAX_REQUEST_BODY_SIZE).await
        else {
            panic!("a body that starts with `{{` or `[` should be read: {chunks:?}");
        };

        assert_eq!(body, chunks.concat().into_bytes());
    }
}

#[tokio::test]
async fn whitespace_and_empty_bodies_are_left_to_the_server() {
    // These can't be rejected until the body ends, so the server rejects them after parsing.
    for chunks in [&[][..], &[" \n "][..]] {
        assert!(
            read_body(&HeaderMap::new(), body(chunks, true), MAX_REQUEST_BODY_SIZE)
                .await
                .is_ok(),
            "{chunks:?}",
        );
    }
}
