//! Bounds transport tables and owners independently across every bound socket.

use std::{
    future::Future,
    net::IpAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{endpoint::PendingTable, ConnectError};

/// Serializes construction with the socket-table check (ADM-11). Live owners
/// are pending or established connections; failed and closed ones move to the
/// draining budget, so while it has room they stay out of the live inbound
/// share (ADM-13).
pub(crate) struct Admission {
    pub(crate) construction: Mutex<()>,
    total: usize,
    draining: usize,
    owners: Arc<Semaphore>,
    inbound_owners: Arc<Semaphore>,
    draining_owners: Arc<Semaphore>,
}

impl Admission {
    /// Builds the live budgets of `total` and `inbound` owners and a separate
    /// budget of `draining` owners.
    pub(crate) fn new(total: usize, inbound: usize, draining: usize) -> Self {
        Self {
            construction: Mutex::new(()),
            total,
            draining,
            owners: Arc::new(Semaphore::new(total)),
            inbound_owners: Arc::new(Semaphore::new(inbound)),
            draining_owners: Arc::new(Semaphore::new(draining)),
        }
    }

    /// The table cap covers failed dials, which hold no permit until drain.
    fn table_full(&self, open: usize) -> bool {
        open >= self.total.saturating_add(self.draining)
    }

    /// Whether a dial would now get past [`Self::reserve`].
    pub(crate) fn has_dial_room(&self, open: usize) -> bool {
        !self.table_full(open) && self.owners.available_permits() > 0
    }

    /// The caller must hold `construction` through creating the noq attempt.
    /// Permits cover pending futures and established connections through their
    /// final handle's drop, moving to the draining budget at failure or close.
    pub(crate) fn reserve(&self, inbound: bool, open: usize) -> Result<Reservation, ConnectError> {
        if self.table_full(open) {
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
            live: Some(LivePermits {
                _inbound: inbound,
                _global: global,
            }),
            draining: None,
            draining_owners: self.draining_owners.clone(),
        })
    }

    /// Live and draining owner permits held, for tests.
    #[cfg(test)]
    pub(crate) fn held(&self) -> (usize, usize) {
        (
            self.total - self.owners.available_permits(),
            self.draining - self.draining_owners.available_permits(),
        )
    }
}

pub(crate) struct Reservation {
    live: Option<LivePermits>,
    draining: Option<OwnedSemaphorePermit>,
    draining_owners: Arc<Semaphore>,
}

struct LivePermits {
    _inbound: Option<OwnedSemaphorePermit>,
    _global: OwnedSemaphorePermit,
}

impl Reservation {
    /// Moves a failed or closed connection to the draining budget. With that
    /// budget full, it keeps its live permits instead (ADM-13).
    pub(crate) fn into_draining(mut self) -> Self {
        if self.draining.is_none() {
            if let Ok(permit) = self.draining_owners.clone().try_acquire_owned() {
                self.draining = Some(permit);
                self.live = None;
            }
        }
        self
    }
}

/// Field order drops a cancelled handshake before returning its reservation.
/// Once it succeeds, the reservation follows all noq owners, including streams.
pub(crate) struct ConnectionAttempt {
    handshake: Handshake,
    reservation: Option<Reservation>,
    close_charge: Option<(PendingTable, IpAddr)>,
}

enum Handshake {
    Outgoing(noq::Connecting),
    /// A server attempt converted to 0.5-RTT, so a failure can be tracked
    /// through its weak handle until noq frees it (ADM-7). Nothing reads or
    /// writes the connection before `accepted` resolves.
    Incoming {
        connection: Option<noq::Connection>,
        accepted: noq::ZeroRttAccepted,
    },
}

impl ConnectionAttempt {
    /// Wraps an outgoing handshake and its reservation.
    pub(crate) fn new(connecting: noq::Connecting, reservation: Reservation) -> Self {
        Self {
            handshake: Handshake::Outgoing(connecting),
            reservation: Some(reservation),
            close_charge: None,
        }
    }

    /// Wraps an incoming handshake and its reservation.
    pub(crate) fn incoming(connecting: noq::Connecting, reservation: Reservation) -> Self {
        let (connection, accepted) = connecting
            .into_0rtt()
            .unwrap_or_else(|_| unreachable!("noq converts every incoming attempt to 0.5-RTT"));
        Self {
            handshake: Handshake::Incoming {
                connection: Some(connection),
                accepted,
            },
            reservation: Some(reservation),
            close_charge: None,
        }
    }

    /// Drops a failed or timed-out handshake, closing it, but keeps its
    /// reservation for the caller to hold until noq frees the state (ADM-7).
    pub(crate) fn abandon(mut self) -> Option<Reservation> {
        self.reservation.take()
    }

    /// Tracks an incoming attempt's transport state, whatever its outcome.
    pub(crate) fn weak_handle(&self) -> Option<noq::WeakConnectionHandle> {
        match &self.handshake {
            Handshake::Outgoing(_) => None,
            Handshake::Incoming { connection, .. } => connection.as_ref().map(|c| c.weak_handle()),
        }
    }

    /// Charges `ip` from the connection's close until noq frees its state (ADM-8).
    pub(crate) fn charge_ip_after_close(mut self, table: PendingTable, ip: IpAddr) -> Self {
        self.close_charge = Some((table, ip));
        self
    }
}

impl Future for ConnectionAttempt {
    type Output = Result<noq::Connection, noq::ConnectionError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = match &mut self.handshake {
            Handshake::Outgoing(connecting) => std::task::ready!(Pin::new(connecting).poll(cx)),
            Handshake::Incoming {
                connection,
                accepted,
            } => {
                std::task::ready!(Pin::new(accepted).poll(cx));
                let connection = connection
                    .take()
                    .expect("polled again after the handshake finished");
                // `accepted` also resolves when the handshake fails.
                match connection.close_reason() {
                    Some(error) => Err(error),
                    None => Ok(connection),
                }
            }
        };
        if let Ok(connection) = &result {
            let reservation = self.reservation.take();
            let close_charge = self.close_charge.take();
            let weak = connection.weak_handle();
            let closed = connection.on_closed();
            // The task owns no endpoint or strong connection handle. It cannot
            // keep a connection open, and at most `total + draining` such owners
            // can exist.
            tokio::spawn(async move {
                // State is freed only after close, so polling can wait for it.
                closed.await;
                let reservation = reservation.map(Reservation::into_draining);
                let charge = close_charge.map(|(table, ip)| table.hold(ip));
                until_freed(&weak).await;
                drop((charge, reservation));
            });
        }
        Poll::Ready(result)
    }
}

/// Resolves once noq has freed the connection's state.
pub(crate) async fn until_freed(weak: &noq::WeakConnectionHandle) {
    let mut wait = Duration::from_millis(10);
    while weak.is_alive() {
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(Duration::from_secs(1));
    }
}
