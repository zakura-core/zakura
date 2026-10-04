//! Bounds transport tables and owners independently across every bound socket.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::ConnectError;

/// Serializes construction with the socket-table check. Owners also retain a
/// permit, because a drained connection can still have unread stream handles.
pub(crate) struct Admission {
    pub(crate) construction: Mutex<()>,
    total: usize,
    inbound: usize,
    owners: Arc<Semaphore>,
    inbound_owners: Arc<Semaphore>,
}

impl Admission {
    pub(crate) fn new(total: usize, inbound: usize) -> Self {
        Self {
            construction: Mutex::new(()),
            total,
            inbound,
            owners: Arc::new(Semaphore::new(total)),
            inbound_owners: Arc::new(Semaphore::new(inbound)),
        }
    }

    /// The caller must hold `construction` through creating the noq attempt.
    /// Socket counts cover failed attempts until drain. Permits cover pending
    /// futures and established connections through their final handle's drop.
    pub(crate) fn reserve(&self, inbound: bool, open: usize) -> Result<Reservation, ConnectError> {
        let limit = if inbound { self.inbound } else { self.total };
        if open >= limit {
            return Err(ConnectError::Capacity);
        }
        // Reserve the inbound share first so rejection cannot borrow outbound room.
        let inbound = inbound
            .then(|| self.inbound_owners.clone().try_acquire_owned())
            .transpose()
            .map_err(|_| ConnectError::Capacity)?;
        let global = self
            .owners
            .clone()
            .try_acquire_owned()
            .map_err(|_| ConnectError::Capacity)?;
        Ok(Reservation {
            _inbound: inbound,
            _global: global,
        })
    }
}

pub(crate) struct Reservation {
    _inbound: Option<OwnedSemaphorePermit>,
    _global: OwnedSemaphorePermit,
}

/// Field order drops a cancelled Connecting before returning its reservation.
/// Once it succeeds, the reservation follows all noq owners, including streams.
pub(crate) struct ConnectionAttempt {
    connecting: noq::Connecting,
    reservation: Option<Reservation>,
}

impl ConnectionAttempt {
    pub(crate) fn new(connecting: noq::Connecting, reservation: Reservation) -> Self {
        Self {
            connecting,
            reservation: Some(reservation),
        }
    }
}

impl Future for ConnectionAttempt {
    type Output = Result<noq::Connection, noq::ConnectionError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = std::task::ready!(Pin::new(&mut self.connecting).poll(cx));
        let reservation = self.reservation.take();
        if let Ok(connection) = &result {
            let weak = connection.weak_handle();
            // The task owns no endpoint or strong connection handle. It cannot
            // keep a connection open, and at most `total` such owners can exist.
            tokio::spawn(async move {
                while weak.is_alive() {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                drop(reservation);
            });
        }
        Poll::Ready(result)
    }
}
