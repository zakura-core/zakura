//! A controllable inner service for the batch worker tests.

use std::{
    io,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use futures::{future::BoxFuture, FutureExt};
use tokio::sync::{mpsc, oneshot};
use tower::Service;

use super::BoxError;

type RequestMessage<T, U> = (T, SendResponse<U>);

pub(super) struct MockService<T, U> {
    requests: mpsc::UnboundedSender<RequestMessage<T, U>>,
    state: Arc<Mutex<State>>,
    ready: bool,
}

pub(super) struct Handle<T, U> {
    requests: mpsc::UnboundedReceiver<RequestMessage<T, U>>,
    state: Arc<Mutex<State>>,
}

pub(super) struct SendResponse<U>(oneshot::Sender<U>);

struct State {
    remaining: usize,
    error: Option<BoxError>,
    waker: Option<Waker>,
    closed: bool,
}

pub(super) fn pair<T, U>() -> (MockService<T, U>, Handle<T, U>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let state = Arc::new(Mutex::new(State {
        remaining: usize::MAX,
        error: None,
        waker: None,
        closed: false,
    }));
    (
        MockService {
            requests: sender,
            state: Arc::clone(&state),
            ready: false,
        },
        Handle {
            requests: receiver,
            state,
        },
    )
}

fn closed_error() -> BoxError {
    io::Error::new(io::ErrorKind::BrokenPipe, "mock service closed").into()
}

impl<T, U: Send + 'static> Service<T> for MockService<T, U> {
    type Response = U;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<U, BoxError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        let mut state = self.state.lock().expect("mock state lock is not poisoned");
        if state.closed {
            Poll::Ready(Err(closed_error()))
        } else if let Some(error) = state.error.take() {
            Poll::Ready(Err(error))
        } else if self.ready || state.remaining > 0 {
            self.ready = true;
            Poll::Ready(Ok(()))
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    fn call(&mut self, request: T) -> Self::Future {
        // Assert before taking the lock so misuse cannot poison shared state.
        assert!(self.ready, "poll_ready must succeed before call");
        self.ready = false;
        let mut state = self.state.lock().expect("mock state lock is not poisoned");
        if state.closed {
            return futures::future::ready(Err(closed_error())).boxed();
        }
        state.remaining = state.remaining.saturating_sub(1);
        drop(state);

        let (sender, receiver) = oneshot::channel();
        let _ = self.requests.send((request, SendResponse(sender)));
        async move { receiver.await.map_err(|_| closed_error()) }.boxed()
    }
}

impl<T, U> Handle<T, U> {
    pub(super) fn allow(&mut self, remaining: usize) {
        let waker = {
            let mut state = self.state.lock().expect("mock state lock is not poisoned");
            state.remaining = remaining;
            if remaining > 0 {
                state.waker.take()
            } else {
                None
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    pub(super) fn send_error(&mut self, error: impl Into<BoxError>) {
        let error = error.into();
        let waker = {
            let mut state = self.state.lock().expect("mock state lock is not poisoned");
            state.error = Some(error);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    pub(super) async fn next_request(&mut self) -> Option<RequestMessage<T, U>> {
        self.requests.recv().await
    }
}

impl<T, U> Drop for Handle<T, U> {
    fn drop(&mut self) {
        let waker = {
            let mut state = self.state.lock().expect("mock state lock is not poisoned");
            state.closed = true;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<U> SendResponse<U> {
    pub(super) fn send_response(self, response: U) {
        let _ = self.0.send(response);
    }
}

#[cfg(test)]
mod tests {
    use crate::polling::{ready, Task};
    use tower::ServiceExt;

    use super::*;

    #[test]
    fn permitting_requests_wakes_readiness_and_call_consumes_the_permit() {
        let (mut service, mut handle) = pair::<(), ()>();
        handle.allow(0);
        let mut readiness = Task::new(async { service.ready().await.map(|_| ()) });
        assert!(readiness.poll().is_pending());
        handle.allow(1);
        assert!(readiness.is_woken());
        ready(readiness.poll()).expect("one request is permitted");
        drop(readiness);

        // Repeated readiness polls must not consume the request's permit.
        let mut context_task = Task::new(());
        context_task.enter(|cx, _| {
            ready(service.poll_ready(cx)).expect("the permit is still available");
        });
        let _response = service.call(());
        context_task.enter(|cx, _| assert!(service.poll_ready(cx).is_pending()));
    }

    #[test]
    fn dropping_handle_wakes_readiness_and_fails_queued_responses() {
        let (mut service, mut handle) = pair::<(), ()>();
        handle.allow(1);
        let mut readiness = Task::new(async { service.ready().await.map(|_| ()) });
        ready(readiness.poll()).expect("one request is permitted");
        drop(readiness);
        let mut response = Task::new(service.call(()));
        assert!(response.poll().is_pending());

        let mut readiness = Task::new(async { service.ready().await.map(|_| ()) });
        assert!(readiness.poll().is_pending());
        drop(handle);
        assert!(readiness.is_woken());
        assert!(response.is_woken());
        assert!(ready(readiness.poll()).is_err());
        assert!(ready(response.poll()).is_err());
    }
}
