//! Separate HTTP/1 idle and header deadlines on the TLS listener.

use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use hyper::rt::{Sleep, Timer};
use hyper_util::rt::TokioTimer;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::oneshot,
};

#[derive(Default)]
struct ReadState {
    successful_response: bool,
    /// Cleared when Hyper exhausts the plaintext stream and a read would block.
    last_read: Option<Instant>,
    first_byte: Option<oneshot::Sender<Instant>>,
}

/// Hyper creates and drops its HTTP/1 header timer around parsing each request head.
/// Request bodies, handler execution and response writes have their own lifetimes.
#[derive(Clone)]
pub(super) struct RequestTimer {
    state: Arc<Mutex<ReadState>>,
    header_timeout: Duration,
    idle_timeout: Duration,
}

impl RequestTimer {
    pub(super) fn new(header_timeout: Duration, idle_timeout: Duration) -> Self {
        Self {
            state: Default::default(),
            header_timeout,
            idle_timeout,
        }
    }

    pub(super) fn observe_io<T>(&self, stream: T) -> impl AsyncRead + AsyncWrite + Unpin
    where
        T: AsyncRead + AsyncWrite + Unpin,
    {
        ObservedIo {
            stream,
            state: self.state.clone(),
        }
    }

    pub(super) fn response(&self, status: hyper::StatusCode) {
        self.state
            .lock()
            .expect("updating a connection deadline cannot panic")
            .successful_response = status.is_success();
    }
}

impl Timer for RequestTimer {
    fn now(&self) -> Instant {
        TokioTimer::new().now()
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Sleep>> {
        TokioTimer::new().sleep(duration)
    }

    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Sleep>> {
        let mut state = self
            .state
            .lock()
            .expect("updating a connection deadline cannot panic");
        // HTTP/1 can read the next request in the same read as the previous one. Hyper
        // probes for more input between requests when its read buffer is empty. Only
        // extend the deadline after that probe blocked; buffered headers are already due.
        if !state.successful_response {
            return TokioTimer::new().sleep_until(deadline);
        }
        if let Some(arrived) = state.last_read {
            return TokioTimer::new().sleep_until(arrived + self.header_timeout);
        }
        let (first_byte, arrived) = oneshot::channel();
        state.first_byte = Some(first_byte);
        let idle_deadline = deadline - self.header_timeout + self.idle_timeout;
        let header_timeout = self.header_timeout;
        Box::pin(IdleSleep(Box::pin(async move {
            if let Ok(Ok(start)) = tokio::time::timeout_at(idle_deadline.into(), arrived).await {
                tokio::time::sleep_until((start + header_timeout).into()).await;
            }
        })))
    }
}

struct IdleSleep(Pin<Box<dyn Future<Output = ()> + Send + Sync>>);

impl Future for IdleSleep {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.0.as_mut().poll(cx)
    }
}

impl Sleep for IdleSleep {}

struct ObservedIo<T> {
    stream: T,
    state: Arc<Mutex<ReadState>>,
}

impl<T: AsyncRead + Unpin> AsyncRead for ObservedIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.stream).poll_read(cx, buf);
        let mut state = self
            .state
            .lock()
            .expect("updating a connection deadline cannot panic");
        if result.is_pending() {
            state.last_read = None;
        } else if buf.filled().len() > before {
            let arrived = tokio::time::Instant::now().into_std();
            state.last_read = Some(arrived);
            if let Some(tx) = state.first_byte.take() {
                let _ = tx.send(arrived);
            }
        }
        result
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for ObservedIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write_vectored(cx, bufs)
    }
}
