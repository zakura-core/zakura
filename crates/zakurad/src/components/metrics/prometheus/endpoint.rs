//! Serving metrics over HTTP.

use std::{
    convert::Infallible, io, net::SocketAddr, sync::Arc, thread, time::Duration, time::Instant,
};

use bytes::Bytes;
use http_body_util::Full;
use hyper::{
    header::{HeaderValue, CONTENT_TYPE},
    server::conn::http1,
    service::service_fn,
    Response,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::{net::TcpListener, sync::Semaphore};

use super::recorder::PrometheusRecorder;

/// Bounds on the connections the endpoint serves, so that clients which connect and then stay
/// silent, or read slowly, can't exhaust the process's file descriptors or memory.
#[derive(Clone, Copy, Debug)]
pub(super) struct Limits {
    /// The most connections served at once. Connections beyond this are closed when accepted.
    pub(super) max_connections: usize,
    /// How long a client has to send each request's headers.
    pub(super) header_read_timeout: Duration,
    /// How long a connection is served, including idle keep-alive time, before it is closed.
    pub(super) connection_timeout: Duration,
    /// How long to wait before accepting again after an accept error, such as running out of
    /// file descriptors.
    pub(super) accept_error_delay: Duration,
}

impl Limits {
    /// Limits for scrapers such as Prometheus, which keep one connection per target and finish
    /// each scrape in seconds.
    pub(super) const DEFAULT: Limits = Limits {
        max_connections: 16,
        header_read_timeout: Duration::from_secs(30),
        connection_timeout: Duration::from_secs(120),
        accept_error_delay: Duration::from_millis(100),
    };
}

/// Starts serving the metrics of `recorder` at `addr` on a new thread, with [`Limits::DEFAULT`].
///
/// `/health` returns `OK`; all other paths return metrics, regardless of method.
/// Returns the listening address, or an error if `addr` can't be opened.
pub(super) fn spawn(addr: SocketAddr, recorder: PrometheusRecorder) -> io::Result<SocketAddr> {
    spawn_with_limits(addr, recorder, Limits::DEFAULT)
}

/// Starts serving the metrics of `recorder` at `addr` on a new thread, within `limits`.
///
/// Returns the address the endpoint listens on, or an error if `addr` can't be opened.
pub(super) fn spawn_with_limits(
    addr: SocketAddr,
    recorder: PrometheusRecorder,
    limits: Limits,
) -> io::Result<SocketAddr> {
    let listener = std::net::TcpListener::bind(addr)?;
    let local_addr = listener.local_addr()?;
    listener.set_nonblocking(true)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    thread::Builder::new()
        .name("metrics-endpoint".to_string())
        .spawn(move || runtime.block_on(serve(listener, recorder, limits)))?;
    Ok(local_addr)
}

/// Answers metrics requests on `listener` within `limits` until the process exits.
async fn serve(listener: std::net::TcpListener, recorder: PrometheusRecorder, limits: Limits) {
    let listener = match TcpListener::from_std(listener) {
        Ok(listener) => listener,
        Err(error) => {
            error!(?error, "metrics endpoint failed to start");
            return;
        }
    };
    let connections = Arc::new(Semaphore::new(limits.max_connections));

    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(error) => {
                // Errors such as running out of file descriptors repeat until resources are
                // freed, so retrying at once would spin.
                debug!(?error, "metrics endpoint failed to accept a connection");
                tokio::time::sleep(limits.accept_error_delay).await;
                continue;
            }
        };

        // Dropping the stream closes the connection.
        let Ok(permit) = connections.clone().try_acquire_owned() else {
            debug!("metrics endpoint is serving its maximum number of connections");
            continue;
        };

        let recorder = recorder.clone();
        tokio::spawn(async move {
            let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                let body = if request.uri().path() == "/health" {
                    Bytes::from_static(b"OK")
                } else {
                    Bytes::from(recorder.render(Instant::now()))
                };
                let mut response = Response::new(Full::new(body));
                response
                    .headers_mut()
                    .insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
                async move { Ok::<_, Infallible>(response) }
            });
            let connection = http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(limits.header_read_timeout)
                .serve_connection(TokioIo::new(stream), service);

            match tokio::time::timeout(limits.connection_timeout, connection).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => debug!(?error, "metrics endpoint connection failed"),
                Err(_) => debug!("metrics endpoint closed a connection that was open too long"),
            }
            drop(permit);
        });
    }
}
