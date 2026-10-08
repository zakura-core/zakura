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
        let reservation = self.reservation.take();
        let close_charge = self.close_charge.take();
        if let Ok(connection) = &result {
            let weak = connection.weak_handle();
            let closed = connection.on_closed();
            // The task owns no endpoint or strong connection handle. It cannot
            // keep a connection open, and at most `total` such owners can exist.
            tokio::spawn(async move {
                // State is freed only after close, so polling can wait for it.
                closed.await;
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
