//! Response reservations: the only way a response is admitted.
//!
//! A requester reserves a key before it sends a request. The response must
//! claim that key before its payload is decoded. A claim fails if nothing is
//! reserved under the key or if the payload is larger than the reservation.
//!
//! There is no expiry. A local timeout or cancellation leaves the reservation
//! in place until the peer answers or the session ends, because the peer may
//! still answer and the answer is not a violation. The owning session drops
//! the map when it closes. Memory is bounded by `cap` entries.

use std::{collections::HashMap, hash::Hash};

#[cfg(test)]
mod tests;

/// Why a local request could not reserve its response.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum ReserveRefused {
    /// The session already holds `cap` live reservations.
    AtCapacity,
    /// The key already has a live reservation.
    KeyLive,
}

/// Why a peer response could not claim a reservation. Both are violations.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum ClaimRefused {
    /// Nothing is reserved under the key: the response is unsolicited or a
    /// duplicate.
    Unsolicited,
    /// The payload exceeds the bytes the request reserved.
    OverReservation {
        /// Bytes the request reserved.
        reserved: usize,
        /// Bytes the response carried.
        actual: usize,
    },
}

impl ClaimRefused {
    /// Stable metric and trace label.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Unsolicited => "unsolicited_response",
            Self::OverReservation { .. } => "over_reservation",
        }
    }
}

#[derive(Debug)]
struct Reservation<C> {
    max_payload_bytes: usize,
    credit: C,
}

/// Live response reservations for one session, keyed by `K`.
///
/// `C` is the credit the claim returns: whatever the requester needs to
/// decode and route the response.
#[derive(Debug)]
pub(crate) struct Reservations<K, C> {
    cap: usize,
    live: HashMap<K, Reservation<C>>,
}

impl<K: Eq + Hash, C> Reservations<K, C> {
    /// An empty map that holds at most `cap` live reservations.
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            cap,
            live: HashMap::with_capacity(cap),
        }
    }

    /// Reserve `key` for one response of at most `max_payload_bytes`.
    ///
    /// A refusal is a local condition; the peer is not at fault.
    pub(crate) fn reserve(
        &mut self,
        key: K,
        max_payload_bytes: usize,
        credit: C,
    ) -> Result<(), ReserveRefused> {
        if self.live.contains_key(&key) {
            return Err(ReserveRefused::KeyLive);
        }
        if self.live.len() >= self.cap {
            return Err(ReserveRefused::AtCapacity);
        }
        self.live.insert(
            key,
            Reservation {
                max_payload_bytes,
                credit,
            },
        );
        Ok(())
    }

    /// Remove a reservation whose request was never queued for sending.
    ///
    /// Use this only when the send failed locally. A sent request keeps its
    /// reservation until the response claims it.
    pub(crate) fn retract(&mut self, key: &K) -> Option<C> {
        self.live.remove(key).map(|reservation| reservation.credit)
    }

    /// Claim the reservation for a `payload_len`-byte response, before decoding.
    pub(crate) fn claim(&mut self, key: &K, payload_len: usize) -> Result<C, ClaimRefused> {
        let reservation = self.live.remove(key).ok_or(ClaimRefused::Unsolicited)?;
        if payload_len > reservation.max_payload_bytes {
            return Err(ClaimRefused::OverReservation {
                reserved: reservation.max_payload_bytes,
                actual: payload_len,
            });
        }
        Ok(reservation.credit)
    }

    /// Whether `key` has a live reservation.
    pub(crate) fn is_live(&self, key: &K) -> bool {
        self.live.contains_key(key)
    }

    /// Number of live reservations.
    pub(crate) fn len(&self) -> usize {
        self.live.len()
    }
}
