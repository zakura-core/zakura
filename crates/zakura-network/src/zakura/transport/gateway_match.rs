//! How many known nodes a miner mesh gateway ID names.
//!
//! A gateway ID is the first 20 bytes of a node ID, so the nodes v2
//! networking knows can hold none, one or several with the same gateway ID.
//! The discovery book, the dialer and the connection-slot checks each answer
//! that question about the nodes they know, and carry their own facts about
//! the one node when there is exactly one.

/// The known nodes with one gateway ID: none, exactly one with what this
/// layer knows about it, or two or more.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayMatch<T> {
    /// No known node has the gateway ID.
    None,
    /// Exactly one known node has the gateway ID.
    One(T),
    /// Two or more known nodes have the gateway ID, so nobody can tell which
    /// one the miners named.
    Several,
}

impl<T> GatewayMatch<T> {
    /// Borrows what is known about the one node.
    pub fn as_ref(&self) -> GatewayMatch<&T> {
        match self {
            Self::None => GatewayMatch::None,
            Self::One(one) => GatewayMatch::One(one),
            Self::Several => GatewayMatch::Several,
        }
    }

    /// Keeps the count and converts what is known about the one node.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> GatewayMatch<U> {
        match self {
            Self::None => GatewayMatch::None,
            Self::One(one) => GatewayMatch::One(f(one)),
            Self::Several => GatewayMatch::Several,
        }
    }
}

impl<T: PartialEq> GatewayMatch<T> {
    /// Returns the match once `node`, which has the gateway ID, is known too.
    pub fn with(self, node: T) -> Self {
        match self {
            Self::None => Self::One(node),
            Self::One(known) if known == node => Self::One(known),
            Self::One(_) | Self::Several => Self::Several,
        }
    }

    /// Returns the match for the nodes both `self` and `other` know, which
    /// all have the same gateway ID.
    pub fn merge(self, other: Self) -> Self {
        match other {
            Self::None => self,
            Self::One(node) => self.with(node),
            Self::Several => Self::Several,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::GatewayMatch;

    #[test]
    fn a_second_distinct_node_makes_several() {
        let none = GatewayMatch::<u8>::None;
        assert_eq!(none.with(1), GatewayMatch::One(1));
        assert_eq!(none.with(1).with(1), GatewayMatch::One(1));
        assert_eq!(none.with(1).with(2), GatewayMatch::Several);
        assert_eq!(none.with(1).with(2).with(1), GatewayMatch::Several);
    }

    #[test]
    fn merging_two_answers_counts_each_distinct_node_once() {
        use GatewayMatch::{None, One, Several};
        let answers = [
            (None, None, None),
            (None, One(1), One(1)),
            (One(1), None, One(1)),
            (One(1), One(1), One(1)),
            (One(1), One(2), Several),
            (Several, None, Several),
            (None, Several, Several),
            (One(1), Several, Several),
            (Several, One(1), Several),
        ];
        for (left, right, merged) in answers {
            assert_eq!(left.merge(right), merged, "{left:?} merged with {right:?}");
        }
    }
}
