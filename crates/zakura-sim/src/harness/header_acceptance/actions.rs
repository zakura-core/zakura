//! A node history's declared actions (item 0, "each history's declared
//! actions must run").
//!
//! Concluding every opened obligation doesn't show that every act a history
//! meant to exercise happened: a later scripted step that never ran may
//! have opened nothing, and `Decider::finish` accepts a Plan left
//! unfinished. So [`declared`] derives a run's actions from its bound
//! scenario, the trace's `meta.scenario`, never from a hand list: each
//! storage-family control step, and each wire peer's queued script action.
//! [`judge`] then reads the trace and the full report for whether each one
//! was picked, in its script's order, and reached the effect the run's
//! evidence can show ([`Effect`]). A run is [`Actions::Complete`] only when
//! it completed with every declared action picked and shown and no peer
//! action picked that the scenario doesn't declare.
//!
//! A policy hold is a constraint on storage completions, not an act, so it
//! isn't an action here. Its scripted `Release` step is. Neither is a step
//! a harness source offers by its own condition rather than a script
//! ([`HARNESS_DRIVEN`]), whose own properties judge it.

use serde_json::Value;

use super::{
    evaluate::{Actions, End},
    refs::{ControlRef, LinkRef, Ref, SessionRef},
};
use crate::{
    harness::{
        header_node::ControlAction,
        header_sync::{
            liveness::LivenessScenario, FloodScenario, RepairScenario, StorageScenario,
            WakeScenario, WireScenario,
        },
    },
    peer::PeerAction,
    record::FullReport,
    trace::Trace,
};

/// The `peer:` actions a harness source offers when its own condition
/// comes due, not from a scenario's script: liveness and recovery's
/// `node:finish`, release-wake's `node:settle`, and recovery's `recovered`,
/// `recovery-relink` and `recovery-unreached`. A pick of one isn't an
/// undeclared action.
pub const HARNESS_DRIVEN: [&str; 5] = [
    "finish",
    "settle",
    "recovered",
    "recovery-relink",
    "recovery-unreached",
];

/// Whether `pick` is a [`HARNESS_DRIVEN`] action.
fn harness_driven(pick: &str) -> bool {
    pick.rsplit_once(':')
        .is_some_and(|(_, action)| HARNESS_DRIVEN.contains(&action))
}

/// What shows that a picked action took effect.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// The step loop carries it out at its pick, outside any task, and a
    /// step that can't take effect fails the run as a harness failure: a
    /// reset, release, storage stop, commit, recovery or repair status.
    AtPick,
    /// A task carries it out after its pick, recording an `Environment`
    /// fact for `peer` with `action` (`Close`, `Relink` or `Shutdown`) in
    /// the node's `ends` store as it acts, which names the step's position
    /// in the control script as its cause. A flood or churn step relinks.
    /// The fact's connection must be the link it acted on, as the
    /// `sessions` store shows it: a relink's one opened to `peer`, a
    /// close's the peer's link current at it, whose closure the store
    /// shows (a shutdown names none). No two relinks name one connection,
    /// since each opens a new link.
    Environment {
        /// The peer the fact names.
        peer: String,
        /// The fact's action.
        action: &'static str,
        /// Whether the control task records it at the step's turn: a close,
        /// relink or flood, whose facts therefore follow every earlier such
        /// step's. A churn's task relinks alongside later steps, and a
        /// shutdown is recorded at its pick.
        in_turn: bool,
    },
    /// The control task carries it out after its pick and no store records
    /// it, so it's shown only by a later step of the same script whose
    /// effect is shown: the task carries its queue out in pick order, and
    /// a step the step loop runs is offered only once the queue is empty.
    /// A grow.
    Queued,
    /// An adversary's act, which a passing witness judgement of the run
    /// shows wrote its frames: the judge fails a run where a scripted act
    /// was skipped or never ran.
    ActRan,
    /// A wire peer's act on its own link, which the node's `sessions`
    /// store shows on that link ([`LinkRef`]): `peer`'s `link`th
    /// connection, counted from 0 in the order the node first admitted a
    /// session on each. A peer's script acts on its first link until a
    /// `redial`, which opens the next one.
    Link {
        /// The peer, by harness name.
        peer: String,
        /// Which of its links it acts on.
        link: usize,
        /// What shows it on that link.
        shows: LinkShows,
    },
    /// A peer task carries it out and no checkpointed store records it,
    /// since the peers' records aren't one (C11). It's never shown.
    Unshown,
}

/// What the node's session facts show of a wire peer's act on one link
/// ([`Effect::Link`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LinkShows {
    /// A `close` or `disconnect`: the node ended a session on the link
    /// because the connection stopped under it (`accept_failed`). Every
    /// teardown of the node's own records its reason first, and the first
    /// reason wins, so this one shows a close the node didn't make.
    Ended,
    /// A `redial`: the node admitted a session on the link, the peer's
    /// next one.
    Opened,
    /// A `finish-header`: the node ended a session on the link because the
    /// peer finished its header stream (`stream_closed`), once for each
    /// finish on that link.
    Finished,
}

/// The reason the node's reactor gives a session that ended because its
/// connection stopped under it, as a peer's own close makes it do.
const PEER_CLOSED: &str = "accept_failed";
/// The reason the node's reactor gives a session that ended because the
/// peer finished its header stream.
const STREAM_FINISHED: &str = "stream_closed";

/// One action a history's bound scenario declares.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Declared {
    /// The pick that takes it: `peer:<peer>:<action>`.
    pub pick: String,
    /// Which script it belongs to, whose actions are picked in order: the
    /// control script, or one peer's.
    pub script: String,
    /// Its position in the control script, for a control step, which the
    /// `ends` store names as the cause of each fact its execution records.
    pub control: Option<usize>,
    /// What shows it took effect.
    pub effect: Effect,
}

/// Every action `scenario`, the trace's `meta.scenario`, declares on
/// `harness`, in script order, or why they can't be derived: the harness
/// isn't a node harness this knows, or the scenario isn't its type. The
/// smoke harness declares none.
pub fn declared(harness: &str, scenario: &Value) -> Result<Vec<Declared>, String> {
    let read = |scenario: &Value| -> Result<_, String> {
        Ok(match harness {
            "header-sync/storage" => Some(parse::<StorageScenario>(scenario)?),
            "header-sync/flood" => Some(parse::<FloodScenario>(scenario)?.storage()),
            "header-sync/liveness" => Some(parse::<LivenessScenario>(scenario)?.storage()),
            "header-sync/release-wake" => Some(parse::<WakeScenario>(scenario)?.storage()),
            "header-sync/repair" => Some(parse::<RepairScenario>(scenario)?.storage()),
            _ => None,
        })
    };
    if let Some(storage) = read(scenario)? {
        return Ok(controls(&storage));
    }
    match harness {
        "header-sync/wire" => Ok(peers(&parse::<WireScenario>(scenario)?)),
        "header-sync/smoke" => Ok(Vec::new()),
        other => Err(format!("{other} declares no actions this reader knows")),
    }
}

/// `scenario` as `T`.
fn parse<T: serde::de::DeserializeOwned>(scenario: &Value) -> Result<T, String> {
    serde_json::from_value(scenario.clone()).map_err(|error| format!("the scenario: {error}"))
}

/// A storage scenario's control steps, as the `Control` source offers them.
fn controls(scenario: &StorageScenario) -> Vec<Declared> {
    scenario
        .controls
        .iter()
        .enumerate()
        .map(|(position, step)| {
            let environment = |action| Effect::Environment {
                peer: step.peer.clone(),
                action,
                in_turn: matches!(
                    step.action,
                    ControlAction::Close | ControlAction::Relink | ControlAction::Flood
                ),
            };
            let effect = match step.action {
                ControlAction::Close => environment("Close"),
                ControlAction::Relink | ControlAction::Flood | ControlAction::Churn => {
                    environment("Relink")
                }
                ControlAction::Shutdown => environment("Shutdown"),
                ControlAction::Grow { .. } => Effect::Queued,
                ControlAction::Reset(_)
                | ControlAction::Release { .. }
                | ControlAction::StopStorage
                | ControlAction::Commit { .. }
                | ControlAction::Recover
                | ControlAction::RepairStatus { .. } => Effect::AtPick,
            };
            Declared {
                pick: format!("peer:{}:{}", step.peer, step.action.name()),
                script: "control".to_owned(),
                control: Some(position),
                effect,
            }
        })
        .collect()
}

/// A wire scenario's peer scripts, each peer named `peer-<i>`. A link
/// act's link is the peer's first until a `redial`, which acts on the next
/// one and moves the rest of the script there.
fn peers(scenario: &WireScenario) -> Vec<Declared> {
    let mut declared = Vec::new();
    for (index, peer) in scenario.peers.iter().enumerate() {
        let name = format!("peer-{index}");
        let mut link = 0;
        for action in &peer.script {
            let on_link = |link, shows| Effect::Link {
                peer: name.clone(),
                link,
                shows,
            };
            let effect = match action {
                PeerAction::Act { .. } => Effect::ActRan,
                PeerAction::Close | PeerAction::Disconnect => on_link(link, LinkShows::Ended),
                PeerAction::Redial => {
                    link += 1;
                    on_link(link, LinkShows::Opened)
                }
                PeerAction::FinishHeader => on_link(link, LinkShows::Finished),
                PeerAction::Open { .. }
                | PeerAction::Grant
                | PeerAction::Ask { .. }
                | PeerAction::Discover { .. } => Effect::Unshown,
            };
            declared.push(Declared {
                pick: format!("peer:{name}:{}", action.name()),
                script: name.clone(),
                control: None,
                effect,
            });
        }
    }
    declared
}

/// One `Environment` fact of the node's `ends` store.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Fact {
    /// Its stamp.
    stamp: u64,
    /// The peer it names.
    peer: String,
    /// The connection it names, if any.
    connection: Option<u64>,
    /// Its action, as `Close`, `Relink` or `Shutdown`.
    action: String,
    /// The position in the control script of the step whose execution
    /// recorded it, if one did.
    control: Option<usize>,
}

impl Fact {
    /// The control step whose execution recorded it, with the peer and
    /// action it names, if a step did.
    fn cause(&self) -> Option<ControlRef> {
        Some(ControlRef {
            control: self.control?,
            peer: self.peer.clone(),
            action: self.action.clone(),
        })
    }

    /// The link it names, if it's a relink.
    fn relink(&self) -> Option<LinkRef> {
        (self.action == "Relink").then_some(LinkRef {
            peer: self.peer.clone(),
            connection: self.connection?,
        })
    }

    /// The link it names, if it's a close naming one.
    fn closes(&self) -> Option<LinkRef> {
        (self.action == "Close").then_some(LinkRef {
            peer: self.peer.clone(),
            connection: self.connection?,
        })
    }
}

/// The stores' snapshots in `report`, by name.
fn snapshot<'a>(report: &'a FullReport, name: &str) -> Option<&'a Value> {
    report
        .snapshot
        .as_ref()?
        .as_array()?
        .iter()
        .find(|store| store.get("store").and_then(Value::as_str) == Some(name))?
        .get("snapshot")
}

/// The number at the start of `text`, after any spaces.
fn leading_number<T: std::str::FromStr>(text: &str) -> Option<T> {
    let text = text.trim_start();
    let end = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    text[..end].parse().ok()
}

/// Every `Environment` fact the full report's `ends` store holds, in stamp
/// order, with its stamp, connection and cause, read from its `Debug`
/// snapshot: `facts: [(<stamp>, Environment { peer: "<peer>", connection:
/// Some(<id>) | None, action: <action> }), ...], causes: [(<stamp>,
/// <position>), ...]`. Empty if the store isn't there.
fn environment_facts(report: &FullReport) -> Vec<Fact> {
    let Some(text) = snapshot(report, "ends").and_then(Value::as_str) else {
        return Vec::new();
    };
    let (facts, causes) = text.split_once("causes: [").unwrap_or((text, ""));
    let causes: Vec<(u64, usize)> = causes
        .split(']')
        .next()
        .unwrap_or("")
        .split('(')
        .skip(1)
        .filter_map(|pair| {
            let (stamp, position) = pair.split_once(',')?;
            Some((leading_number(stamp)?, leading_number(position)?))
        })
        .collect();
    let mut found = Vec::new();
    let mut chunks = facts.split("Environment { peer: \"");
    let mut before = chunks.next().unwrap_or("");
    for fact in chunks {
        let stamp = before
            .rfind('(')
            .and_then(|at| leading_number::<u64>(&before[at + 1..]));
        before = fact;
        let (Some(stamp), Some((peer, rest))) = (stamp, fact.split_once('"')) else {
            continue;
        };
        let connection = rest
            .split_once("connection: Some(")
            .filter(|(gap, _)| !gap.contains('}'))
            .and_then(|(_, id)| leading_number(id));
        let Some(action) = rest
            .split_once("action: ")
            .map(|(_, action)| action.split([' ', '}', ',']).next().unwrap_or(""))
        else {
            continue;
        };
        found.push(Fact {
            stamp,
            peer: peer.to_owned(),
            connection,
            action: action.to_owned(),
            control: causes
                .iter()
                .find(|(at, _)| *at == stamp)
                .map(|&(_, position)| position),
        });
    }
    found
}

/// Every link the full report's `sessions` store shows with `fact`, which
/// is `LinkOpened` or `LinkClosed`, in the store's order.
fn session_links(report: &FullReport, fact: &str) -> Vec<LinkRef> {
    let Some(facts) = snapshot(report, "sessions")
        .and_then(|sessions| sessions.get("facts"))
        .and_then(Value::as_str)
    else {
        return Vec::new();
    };
    facts
        .split(&format!("{fact} {{ peer: \""))
        .skip(1)
        .filter_map(|fact| {
            let (peer, rest) = fact.split_once('"')?;
            let (_, id) = rest.split_once("connection: ")?;
            Some(LinkRef {
                peer: peer.to_owned(),
                connection: leading_number(id)?,
            })
        })
        .collect()
}

/// The node's sessions as the full report's `sessions` store shows them:
/// each peer's links in the order the node first admitted a session on
/// each, and each ended session's link with the reason the node gave.
/// Read from the store's `Debug` facts: `SessionAdmitted { peer:
/// "<peer>", session_id: <id>, ..., origin: Some(SessionOrigin {
/// connection: <link>, ... }) }` and `SessionEnded { peer: "<peer>",
/// session_id: <id>, reason: "<reason>" }`. A session admitted with no
/// origin names no link, so its end shows nothing.
#[derive(Debug, Default)]
struct Sessions {
    /// Each peer's links, in first-admission order.
    links: std::collections::BTreeMap<String, Vec<u64>>,
    /// Each admitted session and its link, in the store's order.
    admitted: Vec<(SessionRef, u64)>,
    /// Each ended session's link and reason, in the store's order.
    ended: Vec<(LinkRef, String)>,
}

impl Sessions {
    /// `report`'s sessions, empty if its `sessions` store isn't there.
    fn of(report: &FullReport) -> Sessions {
        let mut sessions = Sessions::default();
        let Some(facts) = snapshot(report, "sessions")
            .and_then(|sessions| sessions.get("facts"))
            .and_then(Value::as_str)
        else {
            return sessions;
        };
        let field = |fact: &str, name: &str| -> Option<u64> {
            let (_, rest) = fact.split_once(&format!("{name}: "))?;
            leading_number(rest)
        };
        let quoted = |fact: &str, name: &str| -> Option<String> {
            let (_, rest) = fact.split_once(&format!("{name}: \""))?;
            Some(rest.split_once('"')?.0.to_owned())
        };
        // Each fact's name starts with `Session`, and so does its origin's
        // type, which isn't a fact.
        let facts = facts.replace("SessionOrigin {", "Origin {");
        for fact in facts.split("Session").skip(1) {
            let (Some(peer), Some(session)) = (quoted(fact, "peer"), field(fact, "session_id"))
            else {
                continue;
            };
            if fact.starts_with("Admitted {") {
                let Some(link) = fact
                    .split_once("origin: Some(Origin {")
                    .and_then(|(_, origin)| field(origin, "connection"))
                else {
                    continue;
                };
                let links = sessions.links.entry(peer.clone()).or_default();
                if !links.contains(&link) {
                    links.push(link);
                }
                sessions.admitted.push((SessionRef { peer, session }, link));
            } else if fact.starts_with("Ended {") {
                let ended = SessionRef { peer, session };
                let (Some(reason), Some(connection)) =
                    (quoted(fact, "reason"), sessions.admitted_on(&ended))
                else {
                    continue;
                };
                let peer = ended.peer;
                sessions.ended.push((LinkRef { peer, connection }, reason));
            }
        }
        sessions
    }

    /// The link the node first admitted `session` on, joined by its whole
    /// ref, its peer and session ID, so an end never takes the link of
    /// another peer's session with the same ID.
    fn admitted_on(&self, session: &SessionRef) -> Option<u64> {
        SessionRef::joined(session, &self.admitted, |(at, _)| Some(at.clone()))
            .first()
            .map(|(_, link)| *link)
    }

    /// `peer`'s `link`th link, if the node admitted that many.
    fn link(&self, peer: &str, link: usize) -> Option<LinkRef> {
        Some(LinkRef {
            peer: peer.to_owned(),
            connection: *self.links.get(peer)?.get(link)?,
        })
    }

    /// How many sessions on `link` the node ended for `reason`.
    fn ends(&self, link: &LinkRef, reason: &str) -> usize {
        let ended = self.ended.iter().filter(|(_, why)| why == reason);
        LinkRef::joined(link, ended, |(at, _)| Some(at.clone())).len()
    }

    /// Whether the node shows `shows` on `link` the `nth` time, from 0, a
    /// script acted that way there: a link it admitted for a redial, and
    /// more ends for the reason a close or finish leaves than acts before
    /// this one.
    fn show(&self, link: &LinkRef, shows: LinkShows, nth: usize) -> bool {
        match shows {
            LinkShows::Opened => {
                let opened: Vec<LinkRef> = self
                    .links
                    .iter()
                    .flat_map(|(peer, links)| {
                        links.iter().map(|&connection| LinkRef {
                            peer: peer.clone(),
                            connection,
                        })
                    })
                    .collect();
                !LinkRef::joined(link, &opened, |at| Some(at.clone())).is_empty()
            }
            LinkShows::Ended => self.ends(link, PEER_CLOSED) > nth,
            LinkShows::Finished => self.ends(link, STREAM_FINISHED) > nth,
        }
    }
}

/// Whether each of `facts`, in stamp order, names the link it acted on,
/// as `report`'s `sessions` store shows it. A relink names a link opened to
/// its peer, which becomes the peer's current one. A close names its peer's
/// current link, whose closure the store shows, and leaves it with none.
/// A peer's first link is its first opened one that no relink opened. A
/// shutdown names no link.
fn names_its_link(facts: &[Fact], report: &FullReport) -> Vec<bool> {
    let opened = session_links(report, "LinkOpened");
    let closed = session_links(report, "LinkClosed");
    let relinked = |link: &LinkRef| !LinkRef::joined(link, facts, Fact::relink).is_empty();
    let among = |links: &[LinkRef], link: &LinkRef| {
        !LinkRef::joined(link, links, |at| Some(at.clone())).is_empty()
    };
    let mut current = std::collections::BTreeMap::new();
    for link in &opened {
        if !current.contains_key(&link.peer) && !relinked(link) {
            current.insert(link.peer.clone(), Some(link.clone()));
        }
    }
    facts
        .iter()
        .map(
            |fact| match (fact.action.as_str(), fact.relink(), fact.closes()) {
                ("Shutdown", ..) => fact.connection.is_none(),
                (_, Some(link), _) => {
                    current.insert(link.peer.clone(), Some(link.clone()));
                    among(&opened, &link)
                }
                (_, _, Some(link)) => {
                    let was = current.insert(link.peer.clone(), None).flatten();
                    LinkRef::joins(&link, &was, Clone::clone)
                        && among(&opened, &link)
                        && among(&closed, &link)
                }
                _ => false,
            },
        )
        .collect()
}

/// The first of `facts` that `cause`'s own execution recorded and that
/// names the link it acted on, by `named` ([`names_its_link`]).
fn own_effect<'a>(facts: &'a [Fact], named: &[bool], cause: &ControlRef) -> Option<&'a Fact> {
    let named = facts.iter().zip(named).filter(|(_, named)| **named);
    let named: Vec<&Fact> = named.map(|(fact, _)| fact).collect();
    ControlRef::joined(cause, named, Fact::cause)
        .first()
        .copied()
}

/// Whether `run`, a node history's recording that ended `end`, carried
/// out `declared`: [`Actions::Complete`] for a completed run whose every
/// declared action was picked, each script's in order, and shown by its
/// [`Effect`], with no `peer:` pick the scenario doesn't declare;
/// [`Actions::Prefix`] for a run a violation stopped whose picks are, per
/// script, an in-order prefix of the declared actions, with nothing
/// undeclared; and [`Actions::Incomplete`] otherwise. `witnessed` says
/// whether a witness judgement of this trace passed, which shows its
/// adversary acts ran.
pub fn judge(
    declared: &[Declared],
    trace: &Trace,
    report: &FullReport,
    end: &End,
    witnessed: bool,
) -> Actions {
    let picks: Vec<&str> = trace
        .steps
        .iter()
        .map(|step| step.pick.as_str())
        .filter(|pick| pick.starts_with("peer:") && !harness_driven(pick))
        .collect();
    // Each script's actions are picked in order, so each pick takes the
    // next undone declared action of its script with that identity.
    let mut picked = vec![false; declared.len()];
    for pick in &picks {
        let next = declared
            .iter()
            .enumerate()
            .find(|(at, action)| !picked[*at] && action.pick == *pick);
        let Some((at, action)) = next else {
            return Actions::Incomplete;
        };
        let earlier_undone = declared[..at]
            .iter()
            .enumerate()
            .any(|(before, other)| other.script == action.script && !picked[before]);
        if earlier_undone {
            return Actions::Incomplete;
        }
        picked[at] = true;
    }
    match end {
        End::Violation { .. } => return Actions::Prefix,
        End::Completed => {}
        _ => return Actions::Incomplete,
    }
    if picked.iter().any(|picked| !picked) {
        return Actions::Incomplete;
    }
    let facts = environment_facts(report);
    let named = names_its_link(&facts, report);
    // Each relink opens a new link, so two naming one connection can't
    // both be the connection their step made.
    let mut relinked = std::collections::BTreeSet::new();
    if facts
        .iter()
        .filter(|fact| fact.action == "Relink")
        .any(|fact| fact.connection.is_none_or(|id| !relinked.insert(id)))
    {
        return Actions::Incomplete;
    }
    let sessions = Sessions::of(report);
    // How many earlier acts of each kind each peer's link has seen.
    let mut acted: std::collections::BTreeMap<(String, usize, LinkShows), usize> =
        std::collections::BTreeMap::new();
    // Each in-turn step's effect, its first fact, follows the one before.
    let mut turn = 0;
    let mut shown: Vec<bool> = declared
        .iter()
        .map(|declared| match &declared.effect {
            Effect::AtPick => true,
            // The effect is the one its own step's execution recorded.
            Effect::Environment {
                peer,
                action,
                in_turn,
            } => {
                let own = declared.control.and_then(|control| {
                    let cause = ControlRef {
                        control,
                        peer: peer.clone(),
                        action: (*action).to_owned(),
                    };
                    own_effect(&facts, &named, &cause)
                });
                match own {
                    Some(fact) if *in_turn && fact.stamp <= turn => false,
                    Some(fact) => {
                        if *in_turn {
                            turn = fact.stamp;
                        }
                        true
                    }
                    None => false,
                }
            }
            Effect::ActRan => witnessed,
            Effect::Link { peer, link, shows } => {
                let nth = acted.entry((peer.clone(), *link, *shows)).or_default();
                let shown = sessions
                    .link(peer, *link)
                    .is_some_and(|at| sessions.show(&at, *shows, *nth));
                *nth += 1;
                shown
            }
            Effect::Queued | Effect::Unshown => false,
        })
        .collect();
    for at in 0..declared.len() {
        if declared[at].effect == Effect::Queued {
            shown[at] = (at + 1..declared.len()).any(|later| {
                declared[later].script == declared[at].script
                    && declared[later].effect != Effect::Queued
                    && shown[later]
            });
        }
    }
    if shown.iter().all(|shown| *shown) {
        Actions::Complete
    } else {
        Actions::Incomplete
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::harness::header_acceptance::refs::one_key_off::assert_one_key_off;

    /// A two-mirror storage history that closes `mirror-0`, relinks it and
    /// closes it again, recorded with no Plan bytes: its trace and full
    /// report, and its declared actions.
    fn close_relink_close() -> (Trace, FullReport, Vec<Declared>) {
        let scenario = json!({
            "controls": [
                {"action": "close", "peer": "mirror-0", "when": null},
                {"action": "relink", "peer": "mirror-0", "when": {"closed": "mirror-0"}},
                {"action": "close", "peer": "mirror-0",
                 "when": {"count": [2, {"linked": "mirror-0"}]}},
            ],
            "mirrors": [
                {"chain": {"fork": null, "main": 5}, "dialer": "mirror"},
                {"chain": {"fork": {"branch": 1, "len": 4}, "main": 3}, "dialer": "node"},
            ],
            "node": {"fork": null, "main": 2},
            "policy": {"base": "success-only", "bound_ms": 5000, "holds": []},
        });
        let dir = tempfile::tempdir().expect("a temp dir can be created");
        let entry = crate::harness::find("header-sync/storage").expect("it's registered");
        let trace = crate::harness::record(
            entry,
            crate::harness::RecordInput::Plan {
                scenario: &scenario,
                plan: &[],
            },
            dir.path(),
        )
        .expect("the history records");
        let text = std::fs::read_to_string(dir.path().join(crate::record::FULL_REPORT_FILE))
            .expect("record writes the full report");
        let report = serde_json::from_str(&text).expect("the full report reads back");
        let declared = declared("header-sync/storage", &trace.meta.scenario).expect("it declares");
        (trace, report, declared)
    }

    /// `report` with `from` replaced by `to` in its `store` snapshot's
    /// text, where `field` is `None` for a store whose snapshot is the
    /// text, or the field that holds it.
    fn edited(
        report: &FullReport,
        store: &str,
        field: Option<&str>,
        from: &str,
        to: &str,
    ) -> FullReport {
        let mut report = report.clone();
        let stores = report
            .snapshot
            .as_mut()
            .and_then(Value::as_array_mut)
            .expect("a node run keeps its stores");
        let store = stores
            .iter_mut()
            .find(|found| found["store"] == store)
            .expect("the store is there");
        let text = match field {
            Some(field) => &mut store["snapshot"][field],
            None => &mut store["snapshot"],
        };
        let before = text.as_str().expect("the snapshot is text").to_owned();
        assert!(before.contains(from), "{before}");
        *text = Value::String(before.replace(from, to));
        report
    }

    /// A `Close`, `Relink`, `Close` history is complete when each close
    /// names the connection current at it, which its link's closure shows.
    /// Its final close naming connection 1, which the first close already
    /// closed, with every cause and stamp the same, leaves it incomplete,
    /// and so does that close's connection showing no closure.
    #[test]
    fn a_close_joins_its_peer_s_current_connection() {
        let (trace, report, declared) = close_relink_close();
        let judged = |report: &FullReport| judge(&declared, &trace, report, &End::Completed, false);
        assert_eq!(judged(&report), Actions::Complete);

        let last = "(6, Environment { peer: \"mirror-0\", connection: Some(3), action: Close })";
        let stale = last.replace("Some(3)", "Some(1)");
        assert_eq!(
            judged(&edited(&report, "ends", None, last, &stale)),
            Actions::Incomplete
        );

        let closed = "LinkClosed { peer: \"mirror-0\", connection: 3 }, ";
        assert_eq!(
            judged(&edited(&report, "sessions", Some("facts"), closed, "")),
            Actions::Incomplete
        );
    }

    /// Each control step's effect joins on its whole cause: the fact its
    /// own step recorded is found, and its cause with only its position,
    /// peer or action moved finds none (Rule 25's template).
    #[test]
    fn a_control_effect_joins_its_whole_cause() {
        let (_, report, declared) = close_relink_close();
        let facts = environment_facts(&report);
        let named = names_its_link(&facts, &report);
        let mut joined = 0;
        for declared in &declared {
            let Effect::Environment { peer, action, .. } = &declared.effect else {
                continue;
            };
            let cause = ControlRef {
                control: declared.control.expect("a control step has its position"),
                peer: peer.clone(),
                action: (*action).to_owned(),
            };
            assert_one_key_off::<ControlRef>(&cause, |cause| {
                own_effect(&facts, &named, cause).is_some()
            });
            joined += 1;
        }
        assert_eq!(joined, 3);
    }

    /// A control fact names its link only by its whole [`LinkRef`]: in the
    /// close-relink-close history each relink names a link the store
    /// opened, and the same fact with only its peer or connection moved
    /// names none (Rule 25's template).
    #[test]
    fn a_relink_fact_names_its_whole_link() {
        let (_, report, _) = close_relink_close();
        let facts = environment_facts(&report);
        let named = names_its_link(&facts, &report);
        let mut relinks = 0;
        for (at, fact) in facts.iter().enumerate() {
            let Some(link) = fact.relink() else {
                continue;
            };
            assert!(named[at], "{fact:?}");
            assert_one_key_off::<LinkRef>(&link, |link| {
                let mut moved = facts.clone();
                moved[at].peer = link.peer.clone();
                moved[at].connection = Some(link.connection);
                names_its_link(&moved, &report)[at]
            });
            relinks += 1;
        }
        assert!(relinks > 0);
    }

    /// A committed fixture's path, from the repository root.
    fn fixture(file: &str) -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(file)
    }

    /// `input` recorded on `header-sync/wire`: its trace, its full report
    /// and its declared actions.
    fn wire(input: crate::harness::RecordInput<'_>) -> (Trace, FullReport, Vec<Declared>) {
        let dir = tempfile::tempdir().expect("a temp dir can be created");
        let entry = crate::harness::find("header-sync/wire").expect("it's registered");
        let trace = crate::harness::record(entry, input, dir.path()).expect("the run records");
        let text = std::fs::read_to_string(dir.path().join(crate::record::FULL_REPORT_FILE))
            .expect("record writes the full report");
        let report = serde_json::from_str(&text).expect("the full report reads back");
        let declared = declared("header-sync/wire", &trace.meta.scenario).expect("it declares");
        (trace, report, declared)
    }

    /// The late-fault fixture, whose one peer's script is a `disconnect`,
    /// recorded with its committed script.
    fn late_fault() -> (Trace, FullReport, Vec<Declared>) {
        let read = |file: &str| -> Value {
            serde_json::from_str(&std::fs::read_to_string(fixture(file)).expect("it reads"))
                .expect("it's JSON")
        };
        let scenario = read("header-wire-late-fault.json");
        let plan: crate::Plan = serde_json::from_value(read("header-wire-late-fault.script.json"))
            .expect("the script is a Plan");
        wire(crate::harness::RecordInput::Script {
            scenario: &scenario,
            plan: &plan,
            derived_from: None,
        })
    }

    /// The late-fault fixture's scenario with its peer's script `close`,
    /// then `redial`, with no invalid tip or holds, recorded with no Plan
    /// bytes.
    fn close_redial() -> (Trace, FullReport, Vec<Declared>) {
        let mut scenario: Value = serde_json::from_str(
            &std::fs::read_to_string(fixture("header-wire-late-fault.json")).expect("it reads"),
        )
        .expect("it's JSON");
        scenario["peers"][0]["script"] = json!([{"action": "close"}, {"action": "redial"}]);
        scenario["peers"][0]["invalid_tip"] = json!(false);
        scenario["policy"]["holds"] = json!([]);
        wire(crate::harness::RecordInput::Plan {
            scenario: &scenario,
            plan: &[],
        })
    }

    /// A wire peer's scripted `disconnect`, `close`, `redial` and
    /// `finish-header` are each shown by the node's own session facts for
    /// the link it acted on: the late-fault fixture's disconnect, a close
    /// and its redial, and the reopen fixture's header-stream finish
    /// (`own9-join`'s committed input). Each completed run carried out
    /// every declared action.
    #[test]
    fn a_wire_peer_s_link_actions_are_shown_by_its_sessions() {
        let reopen = std::fs::read(fixture("header-wire-reopen.bin")).expect("it reads");
        for (name, (trace, report, declared)) in [
            ("late fault", late_fault()),
            ("close and redial", close_redial()),
            ("reopen", wire(crate::harness::RecordInput::Input(&reopen))),
        ] {
            assert!(!declared.is_empty(), "{name}");
            assert_eq!(
                judge(&declared, &trace, &report, &End::Completed, false),
                Actions::Complete,
                "{name}: {declared:?}"
            );
        }
    }

    /// A session's end takes the link its admission names, joined by the
    /// whole session ref (Rule 25's template): in the close-relink-close
    /// history's store, which admits `mirror-0` again on each relink beside
    /// `mirror-1`'s sessions, every admitted session's ref finds its own
    /// link, and the same ref with its peer or its session moved finds
    /// none. The store's ends were each found this way.
    #[test]
    fn a_session_end_joins_its_admission_by_its_whole_ref() {
        let (_, report, _) = close_relink_close();
        let sessions = Sessions::of(&report);
        let peers: std::collections::BTreeSet<&str> = sessions
            .admitted
            .iter()
            .map(|(session, _)| session.peer.as_str())
            .collect();
        assert!(
            sessions.admitted.len() > peers.len() && peers.len() > 1,
            "{:?}",
            sessions.admitted
        );
        assert!(!sessions.ended.is_empty(), "{sessions:?}");
        for (own, link) in &sessions.admitted {
            let keys = assert_one_key_off::<SessionRef>(own, |session| {
                sessions.admitted_on(session) == Some(*link)
            });
            assert_eq!(keys, 2);
        }
    }

    /// Each wire link act joins the node's session facts on its whole link
    /// (Rule 25's template): the late fault's disconnect on `peer-0`'s
    /// first connection is shown, and the same act with only its peer or
    /// connection moved shows nothing. With the session's end removed, or
    /// its reason the node's own, the run's disconnect isn't shown.
    #[test]
    fn a_link_act_joins_its_whole_link() {
        let (trace, report, declared) = late_fault();
        let sessions = Sessions::of(&report);
        let link = sessions
            .link("peer-0", 0)
            .expect("the node admitted peer-0");
        assert_one_key_off::<LinkRef>(&link, |link| sessions.show(link, LinkShows::Ended, 0));
        assert!(!sessions.show(&link, LinkShows::Ended, 1));
        let judged = |report: &FullReport| judge(&declared, &trace, report, &End::Completed, false);
        assert_eq!(judged(&report), Actions::Complete);
        let ended = "SessionEnded { peer: \"peer-0\", session_id: 16294208416658607535, reason: \"accept_failed\" }";
        for to in [
            "",
            "SessionEnded { peer: \"peer-0\", session_id: 16294208416658607535, reason: \"idle_timeout\" }",
            "SessionEnded { peer: \"peer-1\", session_id: 16294208416658607535, reason: \"accept_failed\" }",
        ] {
            let edited = edited(&report, "sessions", Some("facts"), ended, to);
            assert_eq!(judged(&edited), Actions::Incomplete, "{to}");
        }
    }
}
