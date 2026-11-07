//! The listener, connection limits and responses.

use std::{
    convert::Infallible,
    future::Future,
    io,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures::stream;
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full, StreamBody};
use hyper::{
    body::Frame,
    header::{
        HeaderValue, ACCEPT_RANGES, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE,
        ETAG,
    },
    server::conn::http1,
    service::service_fn,
    Request, Response, StatusCode,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::{JoinHandle, JoinSet},
    time::Sleep,
};
use tokio_util::sync::CancellationToken;
use zakura_node_services::task::{erased, spawn_erased};

use super::{
    file::{self, OpenError, TraceFile},
    fs_threads::FsThreads,
    request::{self, Plan, Refusal, TableRequest, Target},
};

/// Connections served at once. Further clients wait in the listen backlog.
const MAX_CONNECTIONS: usize = 4;
/// How long a request's headers, a client's reading, or the file reading may
/// stall before the connection is dropped. A transfer that keeps making
/// progress has no deadline.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const ACCEPT_BACKOFF: Duration = Duration::from_secs(1);

const BUSY: Refusal = Refusal::new(
    StatusCode::SERVICE_UNAVAILABLE,
    "trace filesystem threads are busy or stalled",
);

type Body = UnsyncBoxBody<Bytes, io::Error>;

/// Bind `address` and serve `directory`, reading files on `fs_threads`, until
/// `shutdown`. Returns the task and the bound address, which differs from
/// `address` when its port is 0.
pub(super) async fn start(
    address: SocketAddr,
    directory: PathBuf,
    fs_threads: FsThreads,
    shutdown: CancellationToken,
) -> io::Result<(JoinHandle<()>, SocketAddr)> {
    let listener = TcpListener::bind(address).await?;
    let bound = listener.local_addr()?;
    info!(%bound, "raw trace endpoint enabled; local users can read sensitive trace data");
    let endpoint = Arc::new(Endpoint {
        directory,
        authority: bound.to_string(),
        fs_threads,
    });
    Ok((spawn_erased(serve(listener, endpoint, shutdown)), bound))
}

/// Accept connections until `shutdown`, serving at most [`MAX_CONNECTIONS`] at
/// once. Returning drops the `JoinSet`, which aborts every open connection; each
/// aborted response drops its channel, which stops its filesystem thread.
async fn serve(listener: TcpListener, endpoint: Arc<Endpoint>, shutdown: CancellationToken) {
    let mut connections = JoinSet::new();
    loop {
        while connections.try_join_next().is_some() {}
        if connections.len() == MAX_CONNECTIONS {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                _ = connections.join_next() => continue,
            }
        }
        let Some((stream, _)) = accept_with_backoff(|| listener.accept(), &shutdown).await else {
            return;
        };
        connections.spawn(erased(serve_connection(stream, endpoint.clone())));
    }
}

/// Retry accept errors without spinning, and let shutdown interrupt the backoff.
async fn accept_with_backoff<T, F, Fut>(mut accept: F, shutdown: &CancellationToken) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = io::Result<T>>,
{
    loop {
        let accepted = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return None,
            accepted = accept() => accepted,
        };
        match accepted {
            Ok(connection) => return Some(connection),
            Err(error) => warn!(?error, "raw trace endpoint accept failed; retrying"),
        }
        tokio::select! {
            _ = shutdown.cancelled() => return None,
            _ = tokio::time::sleep(ACCEPT_BACKOFF) => {}
        }
    }
}

/// Serve one request on `stream`.
async fn serve_connection(stream: TcpStream, endpoint: Arc<Endpoint>) {
    let _ = stream.set_nodelay(true);
    let service = service_fn(|request| {
        let endpoint = endpoint.clone();
        async move { Ok::<_, Infallible>(endpoint.respond(request).await) }
    });
    let _ = http1::Builder::new()
        .keep_alive(false)
        .max_buf_size(8192)
        .timer(TokioTimer::new())
        .header_read_timeout(IDLE_TIMEOUT)
        .serve_connection(TokioIo::new(WriteTimeout::new(stream)), service)
        .await;
}

struct Endpoint {
    directory: PathBuf,
    /// The literal `Host` every request must send.
    authority: String,
    fs_threads: FsThreads,
}

impl Endpoint {
    async fn respond<B>(&self, request: Request<B>) -> Response<Body> {
        let result = match request::parse(&request, &self.authority) {
            Ok(parsed) => match parsed.target {
                Target::Index => self.index().await,
                Target::Table(table) => self.table(table, parsed.head).await,
            },
            Err(refusal) => Err(refusal),
        };
        let mut response = result.unwrap_or_else(refused);
        let headers = response.headers_mut();
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        headers.insert(
            "x-content-type-options",
            HeaderValue::from_static("nosniff"),
        );
        response
    }

    /// Run `job` on a filesystem thread and wait for its first reply.
    async fn on_fs_thread<T: Send + 'static>(
        &self,
        job: impl FnOnce(oneshot::Sender<T>) + Send + 'static,
    ) -> Result<T, Refusal> {
        let (reply, replied) = oneshot::channel();
        self.fs_threads
            .try_spawn(move || job(reply))
            .map_err(|_| BUSY)?;
        match tokio::time::timeout(IDLE_TIMEOUT, replied).await {
            Ok(Ok(value)) => Ok(value),
            // The thread is stalled or panicked; it keeps its own slot.
            Ok(Err(_)) | Err(_) => Err(BUSY),
        }
    }

    async fn index(&self) -> Result<Response<Body>, Refusal> {
        let directory = self.directory.clone();
        let tables = self
            .on_fs_thread(move |reply| {
                let _ = reply.send(file::list_tables(&directory));
            })
            .await?
            .map_err(|error| {
                warn!(
                    ?error,
                    "raw trace endpoint could not list the trace directory"
                );
                Refusal::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "cannot list the trace directory",
                )
            })?;
        let body = serde_json::to_vec(&serde_json::json!({ "tables": tables }))
            .expect("table entries serialize because they hold only strings and integers");
        let mut response = Response::new(full(body));
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(response)
    }

    /// Open the table on a filesystem thread, decide the response there, and
    /// stream the body from the same thread through a one-chunk channel.
    async fn table(&self, request: TableRequest, head: bool) -> Result<Response<Body>, Refusal> {
        let directory = self.directory.clone();
        let (chunks, receiver) = mpsc::channel(1);
        let (plan, len, etag) = self
            .on_fs_thread(move |reply| {
                let file = match TraceFile::open(&directory, &request.name) {
                    Ok(file) => file,
                    Err(error) => {
                        let _ = reply.send(Err(error));
                        return;
                    }
                };
                let len = file.len();
                let plan = request.plan(len, file.etag());
                if reply.send(Ok((plan, len, file.etag().to_owned()))).is_err() || head {
                    return;
                }
                match plan {
                    Plan::Full => file.send_range(0, len, &chunks),
                    Plan::Partial { start, end } => file.send_range(start, end, &chunks),
                    Plan::Unsatisfiable | Plan::PreconditionFailed => {}
                }
            })
            .await?
            .map_err(|error| match error {
                OpenError::NotFound => Refusal::new(StatusCode::NOT_FOUND, "no such table"),
                OpenError::NotRegular => Refusal::new(
                    StatusCode::FORBIDDEN,
                    "the table's path is not a regular file",
                ),
                OpenError::Io(error) => {
                    warn!(?error, "raw trace endpoint could not open a trace file");
                    Refusal::new(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "cannot open the trace file",
                    )
                }
            })?;

        let (status, body_len, body) = match plan {
            Plan::Full => (StatusCode::OK, len, Some(receiver)),
            Plan::Partial { start, end } => {
                (StatusCode::PARTIAL_CONTENT, end - start, Some(receiver))
            }
            Plan::Unsatisfiable => (StatusCode::RANGE_NOT_SATISFIABLE, 0, None),
            Plan::PreconditionFailed => (StatusCode::PRECONDITION_FAILED, 0, None),
        };
        let body = match body.filter(|_| !head) {
            Some(receiver) => chunk_body(receiver),
            None => full(Vec::new()),
        };
        let mut response = Response::new(body);
        *response.status_mut() = status;
        let headers = response.headers_mut();
        headers.insert(CONTENT_LENGTH, body_len.into());
        headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
        headers.insert(
            ETAG,
            HeaderValue::from_str(&etag).expect("tags hold only hex digits, dashes and quotes"),
        );
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-ndjson"),
        );
        let content_range = match plan {
            Plan::Partial { start, end } => Some(format!("bytes {start}-{}/{len}", end - 1)),
            Plan::Unsatisfiable => Some(format!("bytes */{len}")),
            Plan::Full | Plan::PreconditionFailed => None,
        };
        if let Some(content_range) = content_range {
            headers.insert(
                CONTENT_RANGE,
                HeaderValue::from_str(&content_range).expect("ranges hold only digits and ASCII"),
            );
        }
        Ok(response)
    }
}

/// A body fed by a filesystem thread. A thread that sends nothing for
/// [`IDLE_TIMEOUT`] fails the body, which aborts the connection.
fn chunk_body(receiver: mpsc::Receiver<io::Result<Bytes>>) -> Body {
    let frames = stream::unfold(Some(receiver), |receiver| async move {
        let mut receiver = receiver?;
        match tokio::time::timeout(IDLE_TIMEOUT, receiver.recv()).await {
            Ok(Some(chunk)) => Some((chunk.map(Frame::data), Some(receiver))),
            Ok(None) => None,
            Err(_) => Some((
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "trace file read stalled",
                )),
                None,
            )),
        }
    });
    StreamBody::new(frames).boxed_unsync()
}

fn full(body: impl Into<Bytes>) -> Body {
    Full::new(body.into())
        .map_err(|never| match never {})
        .boxed_unsync()
}

fn refused(refusal: Refusal) -> Response<Body> {
    let mut response = Response::new(full(format!("{}\n", refusal.reason)));
    *response.status_mut() = refusal.status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

/// Fails a write that makes no progress for [`IDLE_TIMEOUT`], so a client that
/// stops reading cannot keep its connection slot. Reads pass through; hyper's
/// header read timeout bounds them.
struct WriteTimeout<S> {
    inner: S,
    stalled_since: Option<Pin<Box<Sleep>>>,
}

impl<S> WriteTimeout<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            stalled_since: None,
        }
    }

    /// Map one write-side poll: progress clears the deadline, and a pending
    /// poll starts it or fails once it passes.
    fn track<T>(&mut self, cx: &mut Context<'_>, poll: Poll<io::Result<T>>) -> Poll<io::Result<T>> {
        if poll.is_ready() {
            self.stalled_since = None;
            return poll;
        }
        let deadline = self
            .stalled_since
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(IDLE_TIMEOUT)));
        ready!(deadline.as_mut().poll(cx));
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "trace client stopped reading",
        )))
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for WriteTimeout<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for WriteTimeout<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_write(cx, buf);
        this.track(cx, poll)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
        this.track(cx, poll)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_flush(cx);
        this.track(cx, poll)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_shutdown(cx);
        this.track(cx, poll)
    }
}

#[cfg(test)]
mod tests;
