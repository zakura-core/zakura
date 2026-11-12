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
    refs::{joined, ControlRef, LinkRef},
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
    /// A peer task carries it out and no checkpointed store records it,
    /// since the peers' records aren't one (C11). It's never shown.
    Unshown,
}

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

/// A wire scenario's peer scripts, each peer named `peer-<i>`.
fn peers(scenario: &WireScenario) -> Vec<Declared> {
    let mut declared = Vec::new();
    for (index, peer) in scenario.peers.iter().enumerate() {
        let name = format!("peer-{index}");
        for action in &peer.script {
            declared.push(Declared {
                pick: format!("peer:{name}:{}", action.name()),
                script: name.clone(),
                control: None,
                effect: match action {
                    PeerAction::Act { .. } => Effect::ActRan,
                    _ => Effect::Unshown,
                },
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

/// Whether each of `facts`, in stamp order, names the link it acted on,
/// as `report`'s `sessions` store shows it. A relink names a link opened to
/// its peer, which becomes the peer's current one. A close names its peer's
/// current link, whose closure the store shows, and leaves it with none.
/// A peer's first link is its first opened one that no relink opened. A
/// shutdown names no link.
fn names_its_link(facts: &[Fact], report: &FullReport) -> Vec<bool> {
    let opened = session_links(report, "LinkOpened");
    let closed = session_links(report, "LinkClosed");
    let relinked = |link: &LinkRef| !joined(facts, Fact::relink, link).is_empty();
    let mut current = std::collections::BTreeMap::new();
    for link in &opened {
        if !current.contains_key(&link.peer) && !relinked(link) {
            current.insert(link.peer.clone(), Some(link.clone()));
        }
    }
    facts
        .iter()
        .map(|fact| match (fact.action.as_str(), fact.relink(), fact.closes()) {
            ("Shutdown", ..) => fact.connection.is_none(),
            (_, Some(link), _) => {
                current.insert(link.peer.clone(), Some(link.clone()));
                opened.contains(&link)
            }
            (_, _, Some(link)) => {
                let was = current.insert(link.peer.clone(), None).flatten();
                was.as_ref() == Some(&link) && opened.contains(&link) && closed.contains(&link)
            }
            _ => false,
        })
        .collect()
}

/// The first of `facts` that `cause`'s own execution recorded and that
/// names the link it acted on, by `named` ([`names_its_link`]).
fn own_effect<'a>(facts: &'a [Fact], named: &[bool], cause: &ControlRef) -> Option<&'a Fact> {
    let named = facts.iter().zip(named).filter(|(_, named)| **named);
    let named: Vec<&Fact> = named.map(|(fact, _)| fact).collect();
    joined(named, Fact::cause, cause).first().copied()
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
            assert_one_key_off(&cause, |cause| own_effect(&facts, &named, cause).is_some());
            joined += 1;
        }
        assert_eq!(joined, 3);
    }
}
