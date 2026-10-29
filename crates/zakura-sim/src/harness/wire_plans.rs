//! The scenarios of `header-sync/wire`'s saved Plans (M5-F): one honest
//! sync, and one focused run per violation category of M5-E1's action
//! table. A committed input names its scenario with a two-byte tag, as
//! block sync's do, so `zakura-sim record --input` runs it as the test
//! does ([`super::header_sync::HeaderWire`]'s `draw`).
//!
//! Each act's setup history is the one `tests/header_wire_table.rs` gives
//! it, with every header fault's payload sent.

use super::{
    header_node::{Chain, Policy},
    header_sync::WireScenario,
};
use crate::peer::{
    adversary::{Act, AdversarySpec, ContentRule, CursorField, Row},
    honest::{CreditSpec, GrantPolicy, HonestSpec, SubscribeSpec},
    PeerAction, PeerSpec,
};

/// The node's chain at the start of most saved runs.
const PRELOAD: u32 = 50;

/// One plain one-header page's credit.
const ONE_PAGE: CreditSpec = CreditSpec {
    objects: 1,
    bytes: 263,
};

/// The first byte of every saved Plan's tag.
const TAG: u8 = 0xFE;

/// A saved Plan's scenario.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SavedPlan {
    /// M5.1's run: an honest peer holding 120 headers dials the node at 50,
    /// which syncs to its tip.
    HonestSync,
    /// An adversary dials the node and takes `act` once its setup history
    /// is done.
    Act(Act),
}

impl SavedPlan {
    /// Every saved Plan: the honest sync, then one act per violation
    /// category of the action table: a stateless fault on `status`, and
    /// for `bad-cursor` and `bad-content` the case M5-E1's guard test
    /// takes. M5-E1 marks no category unreachable.
    pub const ALL: [SavedPlan; 14] = [
        SavedPlan::HonestSync,
        SavedPlan::Act(Act::ShortPayload { row: Row::Status }),
        SavedPlan::Act(Act::LongPayload { row: Row::Status }),
        SavedPlan::Act(Act::UndeclaredType { message_type: 0 }),
        SavedPlan::Act(Act::ReservedFlags {
            row: Row::Status,
            bit: 0,
        }),
        SavedPlan::Act(Act::Unsolicited { row: Row::Headers }),
        SavedPlan::Act(Act::OverCreditHeader),
        SavedPlan::Act(Act::OverCreditObjects),
        SavedPlan::Act(Act::OverLive),
        SavedPlan::Act(Act::ExtraEnding),
        SavedPlan::Act(Act::ExtraEndingAhead),
        SavedPlan::Act(Act::AfterClose),
        SavedPlan::Act(Act::BadCursor {
            field: CursorField::Hash,
        }),
        SavedPlan::Act(Act::BadContent {
            rule: ContentRule::Linkage,
        }),
    ];

    /// The fixture's name, `header-wire-<name>.bin`: `honest-sync`, or the
    /// act's identity name.
    pub fn name(self) -> &'static str {
        match self {
            SavedPlan::HonestSync => "honest-sync",
            SavedPlan::Act(act) => act.name(),
        }
    }

    /// The two-byte scenario prefix that picks it: `0xFE`, then its place
    /// in [`SavedPlan::ALL`] from 1.
    pub fn tag(self) -> [u8; 2] {
        let index = SavedPlan::ALL
            .iter()
            .position(|plan| *plan == self)
            .expect("every saved Plan is listed");
        // 14 plans fit a byte.
        [TAG, (index + 1) as u8]
    }

    /// The saved Plan `prefix` tags, if any.
    pub fn from_tag(prefix: &[u8]) -> Option<SavedPlan> {
        SavedPlan::ALL
            .into_iter()
            .find(|plan| prefix.get(..2) == Some(&plan.tag()[..]))
    }

    /// Its scenario.
    pub fn scenario(self) -> WireScenario {
        match self {
            SavedPlan::HonestSync => WireScenario {
                node: Chain::main(PRELOAD),
                peers: vec![PeerSpec {
                    honest: Some(HonestSpec {
                        chain: 120,
                        subscribe: None,
                    }),
                    ..PeerSpec::dialing()
                }],
                policy: Policy::converging(),
                ..WireScenario::default()
            },
            SavedPlan::Act(act) => acting(act),
        }
    }
}

/// An adversary that dials the node, holds `chain` headers, subscribes as
/// `subscribe` says, and acts out `script`.
fn adversary(
    chain: u32,
    subscribe: Option<SubscribeSpec>,
    spec: AdversarySpec,
    script: Vec<Act>,
) -> PeerSpec {
    PeerSpec {
        honest: Some(HonestSpec { chain, subscribe }),
        adversary: Some(spec),
        script: script
            .into_iter()
            .map(|act| PeerAction::Act { act })
            .collect(),
        ..PeerSpec::dialing()
    }
}

/// `act`'s focused scenario, M5-E1's setup history for it.
///
/// - Acts on the node's subscription: the peer holds 1,200 headers and
///   announces them, but serves no page, so the subscription stays live with
///   its credit unspent.
/// - `unsolicited`: the peer never announces, so the node never subscribes.
/// - `extra-ending`: the peer serves 120 headers, so the node syncs, closes
///   its subscription and gets `SubscriptionClosed`.
/// - Acts on the peer's own subscriptions: the node holds 120 headers (51
///   for `after-close`, whose one page reaches the target and makes the
///   peer close), and the peer subscribes from 50 with one page of credit,
///   holding every later grant. `over-live` opens a second subscription
///   first.
/// - The rest: the peer holds the node's 50 headers, so nobody subscribes.
fn acting(act: Act) -> WireScenario {
    let quiet = AdversarySpec {
        announce: true,
        serve: false,
        withhold: false,
    };
    let held = SubscribeSpec {
        start: PRELOAD,
        open: ONE_PAGE,
        grants: GrantPolicy::Held {
            grant: ONE_PAGE,
            quiet_ms: 0,
        },
    };
    let script = match act {
        Act::OverLive => vec![Act::SecondOpen, Act::OverLive],
        act => vec![act],
    };
    let (node, peer) = match act {
        Act::Unsolicited { .. } => (
            PRELOAD,
            adversary(
                1_200,
                None,
                AdversarySpec {
                    announce: false,
                    ..quiet
                },
                script,
            ),
        ),
        Act::ExtraEnding => (
            PRELOAD,
            adversary(
                120,
                None,
                AdversarySpec {
                    serve: true,
                    ..quiet
                },
                script,
            ),
        ),
        Act::SecondOpen | Act::OverLive | Act::BadCursor { .. } => {
            (120, adversary(PRELOAD, Some(held), quiet, script))
        }
        Act::AfterClose => (PRELOAD + 1, adversary(PRELOAD, Some(held), quiet, script)),
        Act::OverCreditHeader | Act::OverCreditObjects | Act::ExtraEndingAhead => {
            (PRELOAD, adversary(1_200, None, quiet, script))
        }
        act if matches!(act.row(), Row::Headers | Row::HeadersOutcome) => {
            (PRELOAD, adversary(1_200, None, quiet, script))
        }
        _ => (PRELOAD, adversary(PRELOAD, None, quiet, script)),
    };
    WireScenario {
        node: Chain::main(node),
        peers: vec![peer],
        policy: Policy::converging(),
        ..WireScenario::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every saved Plan has its own tag and name, and its tag picks it.
    #[test]
    fn tags_and_names_are_distinct() {
        let mut names: Vec<&str> = SavedPlan::ALL.iter().map(|plan| plan.name()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), SavedPlan::ALL.len());
        for plan in SavedPlan::ALL {
            assert_eq!(SavedPlan::from_tag(&plan.tag()), Some(plan));
        }
        assert_eq!(SavedPlan::from_tag(&[TAG, 0]), None);
        assert_eq!(SavedPlan::from_tag(&[0, 0]), None);
    }
}
