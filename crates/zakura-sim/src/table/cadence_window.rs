//! Handshake cadence bounds checked from charge and refund counts.
//!
//! A cadence gives each address `capacity` tokens and adds one per
//! `refill_ns`, up to `capacity`. These checks never rebuild the bucket.
//! They count the charges and refunds in each window that starts at a
//! charge, and compare the counts with what the rate allows.

/// One address's charges and refunds, in order.
pub(crate) struct CadenceWindow {
    capacity: u64,
    refill_ns: u64,
    /// Each event's time, and whether it was a charge (`true`) or a refund.
    events: Vec<(u64, bool)>,
}

impl CadenceWindow {
    /// An address that hasn't been charged yet.
    pub(crate) fn new(capacity: u32, refill_ns: u64) -> Self {
        Self {
            capacity: u64::from(capacity),
            refill_ns,
            events: Vec::new(),
        }
    }

    /// Records a charge at `now`. Returns `false` when some window holds
    /// more charges than its start balance, refills and refunds could pay.
    ///
    /// A window that starts at a charge starts with at most `capacity`
    /// tokens, and gains at most one refill per started interval and one
    /// token per refund.
    pub(crate) fn charge(&mut self, now: u64) -> bool {
        self.events.push((now, true));
        let mut charges = 0;
        let mut refunds = 0;
        for &(at, charge) in self.events.iter().rev() {
            if charge {
                charges += 1;
                let refills = (now - at) / self.refill_ns + 1;
                if charges > self.capacity + refills + refunds {
                    return false;
                }
            } else {
                refunds += 1;
            }
        }
        true
    }

    /// Records a refund at `now`.
    pub(crate) fn refund(&mut self, now: u64) {
        self.events.push((now, false));
    }

    /// Whether the charges so far can explain an empty bucket at `now`.
    ///
    /// Let `s` be the first charge after the bucket was last full. The
    /// bucket held `capacity` tokens before `s` and never filled again, so
    /// no refill or refund since `s` was lost. Emptying it took at least
    /// `capacity` charges plus one per refund and per whole refill interval
    /// since `s`. An address without such a window still had a token.
    pub(crate) fn refusal_earned(&self, now: u64) -> bool {
        let mut charges = 0;
        let mut refunds = 0;
        for &(at, charge) in self.events.iter().rev() {
            if charge {
                charges += 1;
                if charges >= self.capacity + refunds + (now - at) / self.refill_ns {
                    return true;
                }
            } else {
                refunds += 1;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::CadenceWindow;

    const SECOND: u64 = 1_000_000_000;

    #[test]
    fn a_burst_spends_capacity_and_one_partial_refill() {
        let mut window = CadenceWindow::new(4, SECOND);
        assert!((0..5).all(|_| window.charge(0)));
        assert!(!window.charge(0));
    }

    #[test]
    fn refills_and_refunds_pay_later_charges() {
        let mut window = CadenceWindow::new(2, SECOND);
        assert!(window.charge(0) && window.charge(0));
        assert!(window.refusal_earned(0));
        window.refund(0);
        assert!(!window.refusal_earned(0));
        assert!(window.charge(0));
        assert!(window.refusal_earned(0));
        assert!(!window.refusal_earned(SECOND));
        assert!(window.charge(SECOND) && window.charge(SECOND));
        assert!(!window.charge(SECOND));
    }

    #[test]
    fn an_uncharged_address_never_earns_a_refusal() {
        assert!(!CadenceWindow::new(1, SECOND).refusal_earned(0));
    }
}
