//! Canonical names for the actions a step can pick.
//!
//! An [`Identity`] names one enabled action within one build. Traces record
//! it as the step's `pick`, and a scripted [`Plan`](crate::Plan) follows picks
//! by it. The grammar is in traces.md, Identities and match keys.

use std::{cmp::Ordering, fmt};

use zakura_scheduler::{At, SimInstant};

/// An action's kind. Enabled sets list categories in this order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Category {
    /// Run a ready task to its next point.
    Task,
    /// Deliver a datagram.
    Deliver,
    /// Drop a datagram.
    Drop,
    /// Complete a storage operation.
    Storage,
    /// A scripted peer's action.
    Peer,
    /// Advance virtual time.
    Time,
}

impl Category {
    /// The identity prefix, such as `task` in `task:root.1:poll#1`.
    pub fn prefix(self) -> &'static str {
        match self {
            Category::Task => "task",
            Category::Deliver => "deliver",
            Category::Drop => "drop",
            Category::Storage => "storage",
            Category::Peer => "peer",
            Category::Time => "advance-time",
        }
    }
}

/// An enabled action's name.
///
/// Displayed as `task:<name>:<kind>#<n>`, `advance-time:<ns>`,
/// `deliver:<link>:<seq>`, `drop:<link>:<seq>`,
/// `storage:<service>:<seq>:<outcome>` or `peer:<peer>:<action>`.
///
/// Identities compare by category, then key. `at` is left out, so a task
/// keeps its round-robin slot as it moves from point to point.
#[derive(Clone, Debug)]
pub struct Identity {
    /// The action's kind.
    pub category: Category,
    /// Unique among enabled actions of one category: a task's name, or the
    /// rest of the identity for other categories.
    pub key: String,
    /// Where a task will resume, such as `poll#1`. `None` for other
    /// categories.
    pub at: Option<String>,
}

impl Identity {
    /// Running task `name`, which will resume at `at`.
    pub fn task(name: impl Into<String>, at: At) -> Self {
        Identity {
            category: Category::Task,
            key: name.into(),
            at: Some(at.to_string()),
        }
    }

    /// Advancing virtual time to `to`.
    pub fn advance_time(to: SimInstant) -> Self {
        Identity::other(Category::Time, to.since_start().as_nanos().to_string())
    }

    /// Delivering the `seq`-th datagram on the directed `link`, such as
    /// `node->p1`.
    pub fn deliver(link: &str, seq: u64) -> Self {
        Identity::other(Category::Deliver, format!("{link}:{seq}"))
    }

    /// Dropping the `seq`-th datagram on the directed `link`.
    pub fn drop(link: &str, seq: u64) -> Self {
        Identity::other(Category::Drop, format!("{link}:{seq}"))
    }

    /// Completing storage operation `seq` of `service` with `outcome`.
    pub fn storage(service: &str, seq: u64, outcome: &str) -> Self {
        Identity::other(Category::Storage, format!("{service}:{seq}:{outcome}"))
    }

    /// Scripted peer `peer` taking `action`.
    pub fn peer(peer: &str, action: &str) -> Self {
        Identity::other(Category::Peer, format!("{peer}:{action}"))
    }

    /// An identity of a category without an `at`.
    fn other(category: Category, key: String) -> Self {
        Identity {
            category,
            key,
            at: None,
        }
    }
}

impl fmt::Display for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.category.prefix(), self.key)?;
        match &self.at {
            Some(at) => write!(f, ":{at}"),
            None => Ok(()),
        }
    }
}

impl PartialEq for Identity {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Identity {}

impl PartialOrd for Identity {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Identity {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.category, &self.key).cmp(&(other.category, &other.key))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use zakura_scheduler::Op;

    use super::*;

    /// A task resuming at its `n`-th point of kind `op`.
    fn at(op: Op, n: u32) -> At {
        At { op, n, site: None }
    }

    #[test]
    fn identities_display_in_trace_grammar() {
        let cases = [
            (
                Identity::task("root.1/peer.2", at(Op::Select, 3)),
                "task:root.1/peer.2:select#3",
            ),
            (
                Identity::advance_time(SimInstant::from_start(Duration::from_millis(5))),
                "advance-time:5000000",
            ),
            (Identity::deliver("node->p1", 4), "deliver:node->p1:4"),
            (Identity::drop("p1->node", 0), "drop:p1->node:0"),
            (
                Identity::storage("header-store", 7, "success"),
                "storage:header-store:7:success",
            ),
            (Identity::peer("p1", "send:hello"), "peer:p1:send:hello"),
        ];
        for (identity, shown) in cases {
            assert_eq!(identity.to_string(), shown);
        }
    }

    #[test]
    fn identities_sort_by_category_then_key() {
        let mut ids = [
            Identity::advance_time(SimInstant::ZERO),
            Identity::peer("p1", "connect"),
            Identity::task("b.1", at(Op::Poll, 1)),
            Identity::storage("s", 1, "success"),
            Identity::drop("l", 1),
            Identity::task("a.1", at(Op::Lock, 9)),
            Identity::deliver("l", 1),
        ];
        ids.sort();
        let shown: Vec<String> = ids.iter().map(ToString::to_string).collect();
        assert_eq!(
            shown,
            [
                "task:a.1:lock#9",
                "task:b.1:poll#1",
                "deliver:l:1",
                "drop:l:1",
                "storage:s:1:success",
                "peer:p1:connect",
                "advance-time:0",
            ]
        );
    }

    #[test]
    fn a_task_keeps_its_slot_as_it_moves() {
        let before = Identity::task("a.1", at(Op::Poll, 1));
        let after = Identity::task("a.1", at(Op::Recv, 4));
        assert_eq!(before, after);
        assert_ne!(before.to_string(), after.to_string());
    }
}
