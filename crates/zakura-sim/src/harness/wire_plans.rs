//! The scenarios of `header-sync/wire`'s saved Plans (M5-F): one honest
//! sync, and one focused run per violation category of M5-E1's action
//! table. A committed input names its scenario with a two-byte tag, as
//! block sync's do, so `zakura-sim record --input` runs it as the test
//! does ([`super::header_sync::HeaderWire`]'s `draw`).
//!
//! Each act's setup history is the one `tests/header_wire_table.rs` gives
//! it, with every header fault's payload sent. [`act_scenario`] builds it
//! for any act, so P3d's witnesses reuse it.
//!
//! fuzz-13 adds the session-replacement histories that fuzz-5's required
//! states need on this harness: a header stream reopened on one connection
//! ([`reopen_scenario`]) and an old session's page judged after that reopen
//! ([`old_page_scenario`]).

use super::{
    header_node::{
        policy::{Allow, Hold, OpKind, Select, Trigger},
        Base, Chain, Policy,
    },
    header_sync::{WireScenario, PROGRESS_LINK, PROGRESS_STORAGE_BOUND_MS},
    LinkEntry, LinkSpec, NetScenario,
};
use crate::peer::{
    adversary::{Act, AdversarySpec, ContentRule, CursorField, Row, CROSSING_LINK_MS},
    honest::{CreditSpec, GrantPolicy, HonestSpec, SubscribeSpec},
    peer_addr, PeerAction, PeerSpec, NODE_ADDR,
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
    /// `header-sync/honest-progress`'s fixture (hdr-8): an honest peer
    /// holding one header past the node's 50 dials it
    /// ([`progress_scenario`]).
    HonestProgress,
    /// The node reopens its header stream to one peer on the same
    /// connection ([`reopen_scenario`]): fuzz-5's stream replacement.
    Reopen,
    /// An old session's invalid page is judged after the node reopens its
    /// header stream ([`old_page_scenario`]): fuzz-5's old-session traffic.
    OldPageAfterReopen,
}

impl SavedPlan {
    /// Every saved Plan: the honest sync, then one act per violation
    /// category of the action table: a stateless fault on `status`, and
    /// for `bad-cursor` and `bad-content` the case M5-E1's guard test
    /// takes. M5-E1 marks no category unreachable. Then the progress
    /// fixture, a lower-locator page, and fuzz-13's reopen, old page and
    /// crossing, each appended so the earlier plans keep their tags.
    pub const ALL: [SavedPlan; 19] = [
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
        SavedPlan::HonestProgress,
        SavedPlan::Act(Act::BadContent {
            rule: ContentRule::LowerLocator,
        }),
        SavedPlan::Reopen,
        SavedPlan::OldPageAfterReopen,
        SavedPlan::Act(Act::OutcomeCrossing),
    ];

    /// The fixture's name, `header-wire-<name>.bin`: `honest-sync`, the
    /// act's identity name, `lower-locator` for that content rule's act,
    /// `honest-progress`, `reopen` or `old-page-after-reopen`.
    pub fn name(self) -> &'static str {
        match self {
            SavedPlan::HonestSync => "honest-sync",
            SavedPlan::Act(Act::BadContent {
                rule: ContentRule::LowerLocator,
            }) => "lower-locator",
            SavedPlan::Act(act) => act.name(),
            SavedPlan::HonestProgress => "honest-progress",
            SavedPlan::Reopen => "reopen",
            SavedPlan::OldPageAfterReopen => "old-page-after-reopen",
        }
    }

    /// The two-byte scenario prefix that picks it: `0xFE`, then its place
    /// in [`SavedPlan::ALL`] from 1.
    pub fn tag(self) -> [u8; 2] {
        let index = SavedPlan::ALL
            .iter()
            .position(|plan| *plan == self)
            .expect("every saved Plan is listed");
        // `ALL` has 19 plans, so an index from 1 fits a byte.
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
            SavedPlan::Act(act) => act_scenario(act),
            SavedPlan::HonestProgress => progress_scenario(),
            SavedPlan::Reopen => reopen_scenario(),
            SavedPlan::OldPageAfterReopen => old_page_scenario(),
        }
    }
}

/// hdr-5's two-generation history: the node at 50 dials `peer-0`, an honest
/// peer holding 120 headers, and both subscribe. Once both subscriptions
/// end, the peer finishes its header stream, so the node reopens it on the
/// same connection. The node's second locator is held, so its second
/// subscription doesn't race the finish.
pub fn reopen_scenario() -> WireScenario {
    let subscribe = SubscribeSpec {
        start: 0,
        open: CreditSpec {
            objects: 250,
            bytes: 65_750,
        },
        grants: GrantPolicy::Immediate,
    };
    WireScenario {
        node: Chain::main(PRELOAD),
        peers: vec![PeerSpec {
            honest: Some(HonestSpec {
                chain: 120,
                subscribe: Some(subscribe),
            }),
            script: vec![PeerAction::FinishHeader],
            ..PeerSpec::listening()
        }],
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(OpKind::Locator, 2),
                until: Trigger::VirtualTime(3_600_000),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        ..WireScenario::default()
    }
}

/// An old session's page judged after the node reopens its header stream:
/// the node at 50 dials `peer-0`, which serves 120 headers whose tip is
/// invalid. The node's page reaches its target, so the reactor closes the
/// subscription and submits the page's preparation, which is held until the
/// reactor admits the peer's second session. Meanwhile the peer finishes its
/// header stream, which it may once its subscriptions end, and the node
/// reopens it on the same connection. The held preparation then fails, and
/// the node judges the old session's page on the old stream.
pub fn old_page_scenario() -> WireScenario {
    WireScenario {
        node: Chain::main(PRELOAD),
        peers: vec![PeerSpec {
            honest: Some(HonestSpec {
                chain: 120,
                subscribe: None,
            }),
            invalid_tip: true,
            script: vec![PeerAction::FinishHeader],
            ..PeerSpec::listening()
        }],
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(OpKind::Prepare, 1),
                until: Trigger::Count(2, Box::new(Trigger::SessionAdmitted("peer-0".to_owned()))),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        ..WireScenario::default()
    }
}

/// `header-sync/honest-progress`'s fixed fixture
/// ([`progress_fixture`](super::header_sync::progress_fixture)): the node at
/// 50, dialed by one honest peer holding 51 headers, on
/// [`PROGRESS_LINK`]s, with storage succeeding within
/// [`PROGRESS_STORAGE_BOUND_MS`].
pub fn progress_scenario() -> WireScenario {
    WireScenario {
        net: NetScenario {
            default_link: PROGRESS_LINK,
            ..NetScenario::default()
        },
        node: Chain::main(PRELOAD),
        peers: vec![PeerSpec {
            honest: Some(HonestSpec {
                chain: PRELOAD + 1,
                subscribe: None,
            }),
            ..PeerSpec::dialing()
        }],
        policy: Policy {
            base: Base::SuccessOnly,
            bound_ms: Some(PROGRESS_STORAGE_BOUND_MS),
            holds: Vec::new(),
            reply_holds: Vec::new(),
            disruptions: Vec::new(),
        },
        budget: None,
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

/// `act`'s focused scenario, M5-E1's setup history for it, with one
/// adversary, `peer-0`.
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
///   first. A conformant `subscribe-headers` frame renews that
///   subscription.
/// - `status-burst`: 30 bursts, which empty the `Status` bucket.
/// - `outcome-crossing`, `crossing-close` and `crossing-grant`: an
///   unretained `Open` first, on a slow link from the node, so the crossing
///   update meets the node's ending in flight.
/// - The rest: the peer holds the node's 50 headers, so nobody subscribes.
pub fn act_scenario(act: Act) -> WireScenario {
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
        // 602 tokens, in bursts of 40 with two more each second, as
        // `tests/header_wire_table.rs` sends them.
        Act::StatusBurst => vec![Act::StatusBurst; 30],
        Act::OutcomeCrossing | Act::CrossingClose | Act::CrossingGrant { .. } => {
            vec![Act::UnretainedOpen, act]
        }
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
        Act::SecondOpen
        | Act::OverLive
        | Act::BadCursor { .. }
        | Act::MinPayload {
            row: Row::SubscribeHeaders,
        } => (120, adversary(PRELOAD, Some(held), quiet, script)),
        Act::AfterClose => (PRELOAD + 1, adversary(PRELOAD, Some(held), quiet, script)),
        Act::OverCreditHeader | Act::OverCreditObjects | Act::ExtraEndingAhead => {
            (PRELOAD, adversary(1_200, None, quiet, script))
        }
        act if matches!(act.row(), Row::Headers | Row::HeadersOutcome) => {
            (PRELOAD, adversary(1_200, None, quiet, script))
        }
        _ => (PRELOAD, adversary(PRELOAD, None, quiet, script)),
    };
    let mut net = NetScenario::default();
    if matches!(
        act,
        Act::UnretainedOpen | Act::OutcomeCrossing | Act::CrossingClose | Act::CrossingGrant { .. }
    ) {
        // The node's ending takes longer to arrive than the crossing
        // update waits, so the update crosses it in flight.
        net.links.push(LinkEntry {
            src: NODE_ADDR,
            dst: peer_addr(0, 0),
            model: LinkSpec {
                min_delay_ms: CROSSING_LINK_MS,
                max_delay_ms: CROSSING_LINK_MS + 100,
                ..LinkSpec::default()
            },
        });
    }
    WireScenario {
        net,
        node: Chain::main(node),
        peers: vec![peer],
        policy: Policy::converging(),
        budget: None,
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
