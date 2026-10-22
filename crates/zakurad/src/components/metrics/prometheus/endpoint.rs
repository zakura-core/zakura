//! Serving metrics over HTTP.

use std::{convert::Infallible, io, net::SocketAddr, thread, time::Instant};

use bytes::Bytes;
use http_body_util::Full;
use hyper::{
    header::{HeaderValue, CONTENT_TYPE},
    server::conn::http1,
    service::service_fn,
    Response,
};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use super::recorder::PrometheusRecorder;

/// Starts serving the metrics of `recorder` at `addr` on a new thread.
///
/// Every request gets the metrics, whatever its method or path. Returns the address the endpoint
/// listens on, or an error if `addr` can't be opened.
pub(super) fn spawn(addr: SocketAddr, recorder: PrometheusRecorder) -> io::Result<SocketAddr> {
    let listener = std::net::TcpListener::bind(addr)?;
    let local_addr = listener.local_addr()?;
    listener.set_nonblocking(true)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    thread::Builder::new()
        .name("metrics-endpoint".to_string())
        .spawn(move || runtime.block_on(serve(listener, recorder)))?;
    Ok(local_addr)
}

/// Answers metrics requests on `listener` until the process exits.
async fn serve(listener: std::net::TcpListener, recorder: PrometheusRecorder) {
    let listener = match TcpListener::from_std(listener) {
        Ok(listener) => listener,
        Err(error) => {
            error!(?error, "metrics endpoint failed to start");
            return;
        }
    };

    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(error) => {
                debug!(?error, "metrics endpoint failed to accept a connection");
                continue;
            }
        };

        let recorder = recorder.clone();
        tokio::spawn(async move {
            let service = service_fn(move |_request| {
                let mut response =
                    Response::new(Full::new(Bytes::from(recorder.render(Instant::now()))));
                response
                    .headers_mut()
                    .insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
                async move { Ok::<_, Infallible>(response) }
            });
            if let Err(error) = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                debug!(?error, "metrics endpoint connection failed");
            }
        });
    }
}
