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

use super::evaluate::{Actions, End};
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
    /// The fact's connection must be one the `sessions` store shows opened
    /// to `peer` (a shutdown names none), and no two relinks name one
    /// connection, since each opens a new link.
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

/// Every link the full report's `sessions` store shows opened, as its peer
/// and connection, read from its `LinkOpened` facts.
fn opened_links(report: &FullReport) -> Vec<(String, u64)> {
    let Some(facts) = snapshot(report, "sessions")
        .and_then(|sessions| sessions.get("facts"))
        .and_then(Value::as_str)
    else {
        return Vec::new();
    };
    facts
        .split("LinkOpened { peer: \"")
        .skip(1)
        .filter_map(|fact| {
            let (peer, rest) = fact.split_once('"')?;
            let (_, id) = rest.split_once("connection: ")?;
            Some((peer.to_owned(), leading_number(id)?))
        })
        .collect()
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
    let links = opened_links(report);
    // Each relink opens a new link, so two naming one connection can't
    // both be the connection their step made.
    let mut relinked = std::collections::BTreeSet::new();
    if facts
        .iter()
        .filter(|fact| fact.action == "Relink")
        .any(|fact| fact.connection.map_or(true, |id| !relinked.insert(id)))
    {
        return Actions::Incomplete;
    }
    // A fact names its connection's link, and a shutdown none.
    let linked = |fact: &Fact| match (fact.action.as_str(), fact.connection) {
        ("Shutdown", None) => true,
        ("Close" | "Relink", Some(id)) => links.iter().any(|(p, c)| *p == fact.peer && *c == id),
        _ => false,
    };
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
                let own = facts.iter().find(|fact| {
                    fact.control.is_some()
                        && fact.control == declared.control
                        && fact.peer == *peer
                        && fact.action == *action
                        && linked(fact)
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
