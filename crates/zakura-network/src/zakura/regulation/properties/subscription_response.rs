//! Future subscription contract adapter. Header v8 still uses GetHeaders.
//! Credit, authorization and resource ownership use production primitives.
//! Cursor and crossed-update policy remain specific to the subscription.

use super::*;
use crate::zakura::CloseCause;
use tokio_util::sync::CancellationToken;

struct Subscription {
    scope: ResponseScope,
    authorization: Option<ResponseAuthorization>,
    credit: ResponseCredit,
    execution: SlotBudget,
    terminal_capacity: Option<SlotPermit>,
    close_sent: bool,
    connection: CancellationToken,
}

impl Subscription {
    fn open(objects: u64, bytes: u64, terminals: &SlotBudget) -> Self {
        let connection = CancellationToken::new();
        let scope = ResponseScope::new(connection.clone(), CloseCause::new());
        let authorization = scope.authorize().unwrap();
        let writer = authorization.write_permission();
        let mut credit = None;
        // Publication must make the credit visible before Open can start.
        assert!(writer.publish(|| credit = Some(ResponseCredit::new(objects, bytes))));
        assert!(writer.try_start(|| true));
        Self {
            scope,
            authorization: Some(authorization),
            credit: credit.unwrap(),
            execution: SlotBudget::new(1).unwrap(),
            terminal_capacity: Some(terminals.try_reserve().unwrap()),
            close_sent: false,
            connection,
        }
    }

    fn grant(&mut self, objects: u64, bytes: u64, publish: impl FnOnce(&ResponseCredit)) -> bool {
        if (objects == 0 && bytes == 0)
            || self.close_sent
            || self.authorization.is_none()
            || self.connection.is_cancelled()
        {
            return false;
        }
        if self.credit.grant(objects, bytes, 4, 40).is_err() {
            return false;
        }
        // This single-owner adapter has no await between reservation and wire
        // publication. Its publisher can immediately inspect the added credit.
        publish(&self.credit);
        true
    }

    fn page(&mut self, objects: u64, bytes: u64, handled: impl FnOnce(&ResponseCredit)) -> bool {
        if self.authorization.is_none() || self.connection.is_cancelled() {
            return false;
        }
        if self.credit.check(objects, bytes).is_err() {
            return false;
        }
        let Some(_work) = self.execution.try_reserve() else {
            return false;
        };
        self.credit.consume(objects, bytes).unwrap();
        handled(&self.credit);
        true
    }

    fn close(&mut self) {
        self.close_sent = true;
    }

    fn terminal(&mut self) -> bool {
        let Some(mut authorization) = self.authorization.take() else {
            return false;
        };
        authorization.finish();
        drop(self.terminal_capacity.take());
        true
    }
}

proptest! {
    #[test]
    fn renewable_credit_histories_bound_outstanding_credit_and_preserve_consumption(
        actions in prop::collection::vec((any::<bool>(), 0u64..9, 0u64..81), 1..100),
    ) {
        let mut credit = ResponseCredit::new(4, 40);
        let (mut available_objects, mut available_bytes) = (4, 40);
        let (mut used_objects, mut used_bytes) = (0, 0);
        for (grant, objects, bytes) in actions {
            if grant {
                let expected = available_objects + objects <= 4 && available_bytes + bytes <= 40;
                prop_assert_eq!(credit.grant(objects, bytes, 4, 40).is_ok(), expected);
                if expected {
                    available_objects += objects;
                    available_bytes += bytes;
                }
            } else {
                let expected = objects <= available_objects && bytes <= available_bytes;
                prop_assert_eq!(credit.consume(objects, bytes).is_ok(), expected);
                if expected {
                    available_objects -= objects;
                    available_bytes -= bytes;
                    used_objects += objects;
                    used_bytes += bytes;
                }
            }
            prop_assert_eq!(credit.consumed_objects(), used_objects);
            prop_assert_eq!(credit.consumed_bytes(), used_bytes);
            prop_assert!(credit.check(available_objects, available_bytes).is_ok());
            prop_assert!(credit.check(available_objects + 1, 0).is_err());
            prop_assert!(credit.check(0, available_bytes + 1).is_err());
        }
    }
}

#[test]
fn subscription_renews_after_publication_without_holding_idle_execution_capacity() {
    let terminals = SlotBudget::new(1).unwrap();
    let mut subscription = Subscription::open(1, 10, &terminals);
    assert!(!subscription.grant(0, 0, |_| panic!("empty Grant published")));
    assert_eq!(subscription.execution.reserved(), 0);
    assert!(subscription.page(1, 10, |credit| {
        assert_eq!(credit.consumed_objects(), 1);
        assert_eq!(credit.consumed_bytes(), 10);
    }));
    assert!(!subscription.page(1, 1, |_| panic!("uncredited page reached handling")));
    assert_eq!(subscription.execution.reserved(), 0);
    assert!(subscription.grant(2, 20, |credit| {
        assert!(credit.check(2, 20).is_ok());
        assert_eq!(credit.consumed_objects(), 1);
        assert_eq!(credit.consumed_bytes(), 10);
    }));
    assert!(subscription.page(2, 20, |credit| {
        assert_eq!(credit.consumed_objects(), 3);
        assert_eq!(credit.consumed_bytes(), 30);
    }));
    assert_eq!(subscription.execution.reserved(), 0);
    assert_eq!(terminals.reserved(), 1);
    assert!(subscription.terminal());
    assert!(!subscription.terminal());
    assert_eq!(terminals.reserved(), 0);
    assert!(subscription.scope.retire());
}

#[test]
fn subscription_close_preserves_crossed_pages_and_separate_terminal_capacity() {
    let terminals = SlotBudget::new(1).unwrap();
    let mut subscription = Subscription::open(2, 20, &terminals);
    subscription.close();
    assert!(!subscription.grant(1, 10, |_| panic!("grant published after Close")));
    assert!(subscription.page(2, 20, |_| {}));
    assert!(!subscription.page(1, 1, |_| panic!("exhausted credit reached handling")));
    // Data credit and execution capacity cannot prevent consuming the terminal.
    let busy = subscription.execution.try_reserve().unwrap();
    assert!(subscription.terminal());
    assert_eq!(terminals.reserved(), 0);
    assert!(!subscription.page(0, 0, |_| panic!("page reached a completed subscription")));
    drop(busy);
    assert!(subscription.scope.retire());
}

#[test]
fn subscription_retirement_keeps_the_connection_lifetime_rule() {
    let terminals = SlotBudget::new(1).unwrap();
    let mut subscription = Subscription::open(1, 10, &terminals);
    subscription.close();
    assert!(!subscription.scope.retire());
    assert!(subscription.connection.is_cancelled());
    assert!(!subscription.grant(1, 10, |_| panic!("grant published after retirement")));
    assert_eq!(terminals.reserved(), 1);
    drop(subscription);
    assert_eq!(terminals.reserved(), 0);
}

#[test]
fn credit_grant_overflow_and_limit_failure_are_atomic() {
    for (objects, bytes) in [(1, 0), (0, 1), (1, 1)] {
        let mut credit = ResponseCredit::new(u64::MAX, u64::MAX);
        credit.consume(u64::MAX, u64::MAX).unwrap();
        assert!(credit.grant(objects, bytes, u64::MAX, u64::MAX).is_err());
        assert_eq!(credit.consumed_objects(), u64::MAX);
        assert_eq!(credit.consumed_bytes(), u64::MAX);
        assert!(credit.check(0, 0).is_ok());
        assert!(credit.check(1, 0).is_err());
        assert!(credit.check(0, 1).is_err());
    }
    let mut credit = ResponseCredit::new(1, 10);
    assert!(credit.grant(2, 31, 4, 40).is_err());
    assert!(credit.check(1, 10).is_ok());
    assert!(credit.check(2, 0).is_err());
    assert!(credit.check(0, 11).is_err());
}
