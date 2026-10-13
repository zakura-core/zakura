//! Bounded, single-use proof that this peer was charged for a timed-out request.

use std::{
    collections::{BTreeMap, HashMap},
    num::NonZeroU64,
};

use zakura_chain::block;
use zakura_header_chain::{BodyWorkEpoch, BodyWorkOwner};

#[derive(Debug)]
struct TimeoutCredit {
    epoch: BodyWorkEpoch,
    missing: Vec<(block::Height, block::Hash)>,
}

/// Index timed-out request bodies without retaining block data or request claims.
/// A delivery can refund at most one request failure, once, in the same epoch.
#[derive(Debug)]
pub(super) struct LateDeliveryCredits {
    capacity: usize,
    requests: BTreeMap<NonZeroU64, TimeoutCredit>,
    bodies: HashMap<(block::Height, block::Hash), NonZeroU64>,
}

impl LateDeliveryCredits {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            requests: BTreeMap::new(),
            bodies: HashMap::new(),
        }
    }

    pub(super) fn record(
        &mut self,
        owner: BodyWorkOwner,
        missing: impl Iterator<Item = (block::Height, block::Hash)>,
    ) {
        self.remove_request(owner.request_id);
        let missing: Vec<_> = missing.take(self.capacity).collect();
        if missing.is_empty() {
            return;
        }
        // Repeated timeouts for the same body must not accumulate duplicate
        // refundable failures or leave request records outside the body bound.
        for body in &missing {
            if let Some(previous) = self.bodies.get(body).copied() {
                self.remove_request(previous);
            }
        }
        while self.bodies.len().saturating_add(missing.len()) > self.capacity {
            let oldest = *self
                .requests
                .first_key_value()
                .expect("indexed bodies belong to a retained request")
                .0;
            self.remove_request(oldest);
        }
        for body in &missing {
            self.bodies.insert(*body, owner.request_id);
        }
        self.requests.insert(
            owner.request_id,
            TimeoutCredit {
                epoch: owner.body_work_epoch,
                missing,
            },
        );
    }

    pub(super) fn take(
        &mut self,
        height: block::Height,
        hash: block::Hash,
        epoch: BodyWorkEpoch,
    ) -> bool {
        let Some(request) = self.bodies.get(&(height, hash)).copied() else {
            return false;
        };
        let matches = self
            .requests
            .get(&request)
            .expect("the body index has a corresponding request")
            .epoch
            == epoch;
        self.remove_request(request);
        matches
    }

    pub(super) fn clear(&mut self) {
        self.requests.clear();
        self.bodies.clear();
    }

    fn remove_request(&mut self, request: NonZeroU64) {
        if let Some(credit) = self.requests.remove(&request) {
            for body in credit.missing {
                self.bodies.remove(&body);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner(request: u64) -> BodyWorkOwner {
        super::super::super::test_work_scope().bind(7, NonZeroU64::new(request).unwrap())
    }

    fn body(height: u8) -> (block::Height, block::Hash) {
        (block::Height(u32::from(height)), block::Hash([height; 32]))
    }

    #[test]
    fn a_multi_body_timeout_refunds_only_once() {
        let mut credits = LateDeliveryCredits::new(4);
        credits.record(owner(1), [body(1), body(2)].into_iter());
        assert!(credits.take(body(1).0, body(1).1, owner(1).body_work_epoch));
        assert!(!credits.take(body(2).0, body(2).1, owner(1).body_work_epoch));
        assert!(credits.requests.is_empty());
        assert!(credits.bodies.is_empty());
    }

    #[test]
    fn history_evicts_oldest_requests_and_bounds_repeated_timeouts() {
        let mut credits = LateDeliveryCredits::new(2);
        credits.record(owner(1), [body(1)].into_iter());
        credits.record(owner(2), [body(2)].into_iter());
        credits.record(owner(3), [body(3)].into_iter());
        assert!(!credits.take(body(1).0, body(1).1, owner(1).body_work_epoch));
        for request in 4..100 {
            credits.record(owner(request), [body(3)].into_iter());
            assert!(credits.requests.len() <= 2);
            assert!(credits.bodies.len() <= 2);
        }
        assert!(credits.take(body(2).0, body(2).1, owner(2).body_work_epoch));
        assert!(credits.take(body(3).0, body(3).1, owner(3).body_work_epoch));
    }

    #[test]
    fn credits_require_the_exact_body_and_current_epoch() {
        let mut credits = LateDeliveryCredits::new(4);
        credits.record(owner(1), [body(1)].into_iter());
        assert!(!credits.take(body(1).0, body(2).1, owner(1).body_work_epoch));
        assert!(!credits.take(body(2).0, body(1).1, owner(1).body_work_epoch));
        assert!(!credits.take(body(1).0, body(1).1, BodyWorkEpoch::new(1)));
        assert!(!credits.take(body(1).0, body(1).1, owner(1).body_work_epoch));
        credits.record(owner(2), [body(2)].into_iter());
        credits.clear();
        assert!(!credits.take(body(2).0, body(2).1, owner(2).body_work_epoch));
    }
}
