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
//! ([`harness_driven`]), whose own properties judge it.

use serde_json::Value;

use super::{
    evaluate::{Actions, End},
    refs::{ControlRef, LinkRef, PeerNameRef, PickRef, Ref, SessionRef, StampRef},
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

/// The actions a harness source offers as `node` when its own condition
/// comes due, not from a scenario's script: liveness and recovery's
/// `finish`, release-wake's `settle`, and recovery's `recovered` and
/// `recovery-unreached`.
const NODE_DRIVEN: [&str; 4] = ["finish", "settle", "recovered", "recovery-unreached"];

/// The action recovery offers as its source peer, the peer a `Recover`
/// step names, while that peer has no live session.
const RECOVERY_RELINK: &str = "recovery-relink";

/// Every `peer:` pick a harness source offers by its own condition in a run
/// that declares `declared`, not from a scenario's script, whole: each of
/// [`NODE_DRIVEN`] as `node`, and [`RECOVERY_RELINK`] as each `Recover`
/// step's peer. A pick of one isn't an undeclared action. Another peer's
/// pick of one of those actions is, since no source offers it.
fn harness_driven(declared: &[Declared]) -> Vec<PickRef> {
    let node = NODE_DRIVEN.iter().map(|action| PickRef {
        peer: "node".to_owned(),
        action: (*action).to_owned(),
    });
    let recover = ControlAction::Recover.name();
    let sources = declared
        .iter()
        .filter(|action| action.control.is_some() && action.pick.action == recover)
        .map(|action| PickRef {
            peer: action.pick.peer.clone(),
            action: RECOVERY_RELINK.to_owned(),
        });
    node.chain(sources).collect()
}

/// Whether `pick` is one of `driven` ([`harness_driven`]), joined by its
/// whole [`PickRef`], so an action's name alone never makes a pick driven.
fn is_harness_driven(driven: &[PickRef], pick: &PickRef) -> bool {
    !PickRef::joined(pick, driven, |at| Some(at.clone())).is_empty()
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
    /// The control task carries it out after its pick and records it in
    /// the node's `grown` store once it completes, naming the step's
    /// position in the control script, so it's shown by its own step's
    /// fact ([`ControlRef`]). A grow.
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

/// The `peer:<peer>:<action>` pick `pick` names, or `None` for one that
/// isn't a `peer:` pick of that shape. A peer's name holds no `:`.
fn pick_ref(pick: &str) -> Option<PickRef> {
    let (peer, action) = pick.strip_prefix("peer:")?.split_once(':')?;
    Some(PickRef {
        peer: peer.to_owned(),
        action: action.to_owned(),
    })
}

/// One action a history's bound scenario declares.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Declared {
    /// The pick that takes it.
    pub pick: PickRef,
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
                pick: PickRef {
                    peer: step.peer.clone(),
                    action: step.action.name().to_owned(),
                },
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
                pick: PickRef {
                    peer: name.clone(),
                    action: action.name().to_owned(),
                },
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

/// The `ends` store's `Debug` snapshot `text` split into its facts and its
/// causes, each cause as the stamp of the fact it names and the position in
/// the control script of the step whose execution recorded that fact:
/// `facts: [...], causes: [(<stamp>, <position>), ...]`.
fn ends_causes(text: &str) -> (&str, Vec<(StampRef, usize)>) {
    let (facts, causes) = text.split_once("causes: [").unwrap_or((text, ""));
    let causes = causes
        .split(']')
        .next()
        .unwrap_or("")
        .split('(')
        .skip(1)
        .filter_map(|pair| {
            let (stamp, position) = pair.split_once(',')?;
            let stamp = StampRef {
                stamp: leading_number(stamp)?,
            };
            Some((stamp, leading_number(position)?))
        })
        .collect();
    (facts, causes)
}

/// The control-script position `causes` names for the fact `fact` stamps,
/// joined by its whole [`StampRef`], so a fact never takes the cause of
/// another fact's stamp.
fn cause_of(causes: &[(StampRef, usize)], fact: &StampRef) -> Option<usize> {
    StampRef::joined(fact, causes, |(at, _)| Some(at.clone()))
        .first()
        .map(|(_, position)| *position)
}

/// Every `Environment` fact the full report's `ends` store holds, in stamp
/// order, with its stamp, connection and cause, read from its `Debug`
/// snapshot: `facts: [(<stamp>, Environment { peer: "<peer>", connection:
/// Some(<id>) | None, action: <action> }), ...], causes: [...]`
/// ([`ends_causes`]). Empty if the store isn't there.
fn environment_facts(report: &FullReport) -> Vec<Fact> {
    let Some(text) = snapshot(report, "ends").and_then(Value::as_str) else {
        return Vec::new();
    };
    let (facts, causes) = ends_causes(text);
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
            control: cause_of(&causes, &StampRef { stamp }),
        });
    }
    found
}

/// Every growth the full report's `grown` store holds that a control step
/// made, as that step's [`ControlRef`] with the action `grow`, read from
/// its snapshot: `{"facts": [{"peer": "<peer>", "control": <position> |
/// null}, ...]}`. Empty if the store isn't there.
fn grown(report: &FullReport) -> Vec<ControlRef> {
    let Some(facts) = snapshot(report, "grown")
        .and_then(|grown| grown.get("facts"))
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    facts
        .iter()
        .filter_map(|fact| {
            Some(ControlRef {
                control: usize::try_from(fact.get("control")?.as_u64()?).ok()?,
                peer: fact.get("peer")?.as_str()?.to_owned(),
                action: GROW.to_owned(),
            })
        })
        .collect()
}

/// A grow's action name, as its pick and its [`ControlRef`] name it.
const GROW: &str = "grow";

/// Whether `growths` ([`grown`]) hold the growth of `declared`'s own step,
/// joined by its whole [`ControlRef`], so a grow is never shown by
/// another step's or another peer's growth.
fn grew(growths: &[ControlRef], declared: &Declared) -> bool {
    let Some(control) = declared.control else {
        return false;
    };
    let own = ControlRef {
        control,
        peer: declared.pick.peer.clone(),
        action: GROW.to_owned(),
    };
    !ControlRef::joined(&own, growths, |at| Some(at.clone())).is_empty()
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
    /// Every link the node admitted a session on, once each, in
    /// first-admission order.
    links: Vec<LinkRef>,
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
                // A link is new unless an earlier admission was on its
                // whole ref, its peer and connection.
                let opened = LinkRef {
                    peer: peer.clone(),
                    connection: link,
                };
                if LinkRef::joined(&opened, &sessions.links, |at| Some(at.clone())).is_empty() {
                    sessions.links.push(opened);
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

    /// `peer`'s `link`th link, if the node admitted that many: its links
    /// joined on the peer's whole [`PeerNameRef`], in first-admission
    /// order.
    fn link(&self, peer: &str, link: usize) -> Option<LinkRef> {
        let peer = PeerNameRef {
            peer: peer.to_owned(),
        };
        let own = PeerNameRef::joined(&peer, &self.links, |at| {
            Some(PeerNameRef {
                peer: at.peer.clone(),
            })
        });
        own.get(link).map(|at| (*at).clone())
    }

    /// How many sessions on `link` the node ended for `reason`.
    fn ends(&self, link: &LinkRef, reason: &str) -> usize {
        let ended = self.ended.iter().filter(|(_, why)| why == reason);
        LinkRef::joined(link, ended, |(at, _)| Some(at.clone())).len()
    }

    /// Whether the node admitted a session on `link`, joined by its whole
    /// ref, its peer and connection: what shows a redial's link.
    fn opened(&self, link: &LinkRef) -> bool {
        !LinkRef::joined(link, &self.links, |at| Some(at.clone())).is_empty()
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
                    closes_current(&link, was.as_ref(), &opened, &closed)
                }
                _ => false,
            },
        )
        .collect()
}

/// Whether a close naming `link` names the link it acted on: `was`, its
/// peer's current link before it, which `opened` shows opened and
/// `closed` shows closed, each joined on its whole [`LinkRef`].
fn closes_current(
    link: &LinkRef,
    was: Option<&LinkRef>,
    opened: &[LinkRef],
    closed: &[LinkRef],
) -> bool {
    let among = |links: &[LinkRef]| !LinkRef::joined(link, links, |at| Some(at.clone())).is_empty();
    LinkRef::joins(link, &was, |was| was.cloned()) && among(opened) && among(closed)
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

/// The first of `declared` not yet `picked` that `pick` takes, joined by
/// its whole [`PickRef`], so a pick never takes another peer's action or
/// another action of its peer.
fn next_declared(declared: &[Declared], picked: &[bool], pick: &PickRef) -> Option<usize> {
    let undone: Vec<(usize, &Declared)> = declared
        .iter()
        .enumerate()
        .filter(|(at, _)| !picked[*at])
        .collect();
    PickRef::joined(pick, &undone, |(_, action)| Some(action.pick.clone()))
        .first()
        .map(|(at, _)| *at)
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
    let driven = harness_driven(declared);
    let mut picks = Vec::new();
    for step in trace
        .steps
        .iter()
        .filter(|step| step.pick.starts_with("peer:"))
    {
        // A `peer:` pick of no peer and action is undeclared.
        let Some(pick) = pick_ref(&step.pick) else {
            return Actions::Incomplete;
        };
        if !is_harness_driven(&driven, &pick) {
            picks.push(pick);
        }
    }
    // Each script's actions are picked in order, so each pick takes the
    // next undone declared action of its script with that identity.
    let mut picked = vec![false; declared.len()];
    for pick in &picks {
        let Some(at) = next_declared(declared, &picked, pick) else {
            return Actions::Incomplete;
        };
        let action = &declared[at];
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
    let growths = grown(report);
    // How many earlier acts of each kind each peer's link has seen.
    let mut acted: std::collections::BTreeMap<(String, usize, LinkShows), usize> =
        std::collections::BTreeMap::new();
    // Each in-turn step's effect, its first fact, follows the one before.
    let mut turn = 0;
    let shown: Vec<bool> = declared
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
            // The node shows a link act the `nth` time, from 0, a script
            // acted that way there: a link it admitted for a redial, and
            // more ends for the reason a close or finish leaves than acts
            // before this one. Each branch is its own join site, with its
            // own control.
            Effect::Link { peer, link, shows } => {
                let nth = acted.entry((peer.clone(), *link, *shows)).or_default();
                let shown = sessions.link(peer, *link).is_some_and(|at| match shows {
                    LinkShows::Opened => sessions.opened(&at),
                    LinkShows::Ended => sessions.ends(&at, PEER_CLOSED) > *nth,
                    LinkShows::Finished => sessions.ends(&at, STREAM_FINISHED) > *nth,
                });
                *nth += 1;
                shown
            }
            Effect::Queued => grew(&growths, declared),
            Effect::Unshown => false,
        })
        .collect();
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

    /// A storage history whose only control step grows `mirror-0` once the
    /// node applies its first headers, recorded with no Plan bytes: its
    /// trace and full report, and its declared actions.
    fn trailing_grow() -> (Trace, FullReport, Vec<Declared>) {
        let scenario = json!({
            "controls": [
                {"action": {"grow": {"len": 2}}, "peer": "mirror-0",
                 "when": {"completed": {"held": false, "kind": "apply", "nth": 1, "session": null}}},
            ],
            "mirrors": [
                {"chain": {"fork": null, "main": 5}, "dialer": "mirror"},
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

    /// `report` with its `grown` store's facts replaced by `facts`.
    fn with_growths(report: &FullReport, facts: Value) -> FullReport {
        let mut report = report.clone();
        let stores = report
            .snapshot
            .as_mut()
            .and_then(Value::as_array_mut)
            .expect("a node run keeps its stores");
        let store = stores
            .iter_mut()
            .find(|found| found["store"] == "grown")
            .expect("the grown store is there");
        store["snapshot"]["facts"] = facts;
        report
    }

    /// A grow that's its script's last step is shown by its own growth,
    /// which the `grown` store records with the step's position. With the
    /// growth gone, or naming another step, the run is incomplete.
    #[test]
    fn a_trailing_grow_is_shown_by_its_growth() {
        let (trace, report, declared) = trailing_grow();
        assert_eq!(declared.len(), 1);
        assert_eq!(declared[0].effect, Effect::Queued);
        let judged = |report: &FullReport| judge(&declared, &trace, report, &End::Completed, false);
        let growths = grown(&report);
        assert_eq!(growths.len(), 1);
        assert!(grew(&growths, &declared[0]));
        assert_eq!(judged(&report), Actions::Complete);
        assert_eq!(
            judged(&with_growths(&report, json!([]))),
            Actions::Incomplete
        );
        assert_eq!(
            judged(&with_growths(
                &report,
                json!([{"peer": "mirror-0", "control": 1}])
            )),
            Actions::Incomplete
        );
    }

    /// A grow joins its own step's growth by the whole [`ControlRef`]
    /// (Rule 25's template): the growth of `mirror-0` by step 0 shows the
    /// declared grow, and the growth with only its position, peer or
    /// action moved doesn't.
    #[test]
    fn a_grow_joins_its_whole_step() {
        let (_, report, declared) = trailing_grow();
        let own = grown(&report)
            .into_iter()
            .next()
            .expect("the history records its growth");
        assert_one_key_off::<ControlRef>(&own, |growth| {
            grew(std::slice::from_ref(growth), &declared[0])
        });
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

    /// Each pick takes its declared action by its whole [`PickRef`] (Rule
    /// 25's template): in the close-relink-close history every declared
    /// action's pick finds the first undone one of its peer and action, and
    /// the same pick with only its peer, `mirror-1`, which declares
    /// nothing, or only its action moved finds none.
    #[test]
    fn a_pick_joins_its_declared_action_whole() {
        let (_, _, declared) = close_relink_close();
        let mut picked = vec![false; declared.len()];
        for (at, action) in declared.iter().enumerate() {
            assert_one_key_off::<PickRef>(&action.pick, |pick| {
                next_declared(&declared, &picked, pick) == Some(at)
            });
            picked[at] = true;
        }
        assert_eq!(declared.len(), 3);
        assert_eq!(
            pick_ref("peer:mirror-0:recovery-relink"),
            Some(PickRef {
                peer: "mirror-0".into(),
                action: "recovery-relink".into(),
            })
        );
        assert_eq!(pick_ref("peer:node"), None);
    }

    /// A harness-driven pick is ignored only by its whole [`PickRef`]
    /// (Rule 25's template): `node`'s `finish` and a `Recover` step's
    /// source's `recovery-relink` are driven, and the same pick with only
    /// its peer or only its action moved isn't. In the close-relink-close
    /// history, a `node:finish` pick leaves the run complete, but
    /// `mirror-0`'s `finish`, which no source offers, is undeclared.
    #[test]
    fn a_harness_driven_pick_joins_its_whole_identity() {
        let (trace, report, declared) = close_relink_close();
        let recover = Declared {
            pick: PickRef {
                peer: "mirror-1".into(),
                action: "recover".into(),
            },
            script: "control".into(),
            control: Some(3),
            effect: Effect::AtPick,
        };
        let driven = harness_driven(&[recover]);
        for (peer, action) in [("node", "finish"), ("mirror-1", "recovery-relink")] {
            let pick = PickRef {
                peer: peer.into(),
                action: action.into(),
            };
            assert_one_key_off::<PickRef>(&pick, |pick| is_harness_driven(&driven, pick));
        }

        let with = |pick: &str| {
            let mut trace = trace.clone();
            let mut extra = trace.steps.last().expect("the run took steps").clone();
            extra.pick = pick.to_owned();
            trace.steps.push(extra);
            judge(&declared, &trace, &report, &End::Completed, false)
        };
        assert_eq!(with("peer:node:finish"), Actions::Complete);
        assert_eq!(with("peer:mirror-0:finish"), Actions::Incomplete);
        assert_eq!(with("peer:mirror-0:recovery-relink"), Actions::Incomplete);
    }

    /// Each fact takes its cause by its whole stamp (Rule 25's template):
    /// in the close-relink-close history every caused fact's stamp finds
    /// its own step's position, and the same stamp moved finds none.
    #[test]
    fn a_fact_joins_its_cause_by_its_whole_stamp() {
        let (_, report, _) = close_relink_close();
        let text = snapshot(&report, "ends")
            .and_then(Value::as_str)
            .expect("a node run keeps its ends store");
        let (_, causes) = ends_causes(text);
        let facts = environment_facts(&report);
        let mut caused = 0;
        for fact in &facts {
            let Some(position) = fact.control else {
                continue;
            };
            let stamp = StampRef { stamp: fact.stamp };
            assert_one_key_off::<StampRef>(&stamp, |stamp| {
                cause_of(&causes, stamp) == Some(position)
            });
            caused += 1;
        }
        assert_eq!(caused, 3, "{facts:?}");
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

    /// A close names the link it acted on only by its whole [`LinkRef`]
    /// (Rule 25's template): in the close-relink-close history each close
    /// names its peer's current link, which the store opened and closed.
    /// The same close with only its peer or connection moved names none,
    /// and so does each close against a current link with only its peer
    /// or connection moved, though the store opened and closed the close's
    /// own link, so the current-link join alone refuses it.
    #[test]
    fn a_close_fact_names_its_peer_s_current_link() {
        let (_, report, _) = close_relink_close();
        let facts = environment_facts(&report);
        let named = names_its_link(&facts, &report);
        let (opened, closed) = (
            session_links(&report, "LinkOpened"),
            session_links(&report, "LinkClosed"),
        );
        let mut closes = 0;
        for (at, fact) in facts.iter().enumerate() {
            let Some(link) = fact.closes() else {
                continue;
            };
            assert!(named[at], "{fact:?}");
            assert_one_key_off::<LinkRef>(&link, |link| {
                let mut moved = facts.clone();
                moved[at].peer = link.peer.clone();
                moved[at].connection = Some(link.connection);
                names_its_link(&moved, &report)[at]
            });
            assert_one_key_off::<LinkRef>(&link, |was| {
                closes_current(&link, Some(was), &opened, &closed)
            });
            assert!(!closes_current(&link, None, &opened, &closed));
            closes += 1;
        }
        assert_eq!(closes, 2);
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

    /// A wire redial joins the node's admissions on its whole link (Rule
    /// 25's template): the close-and-redial run's second `peer-0` link is
    /// one the node opened, and the same link with only its peer or only its
    /// connection moved is none. Its first link is opened too, and the two
    /// differ, so the redial isn't shown by the first.
    #[test]
    fn a_redial_joins_its_whole_link() {
        let (trace, report, declared) = close_redial();
        let sessions = Sessions::of(&report);
        let first = sessions.link("peer-0", 0).expect("peer-0 linked");
        let redialed = sessions.link("peer-0", 1).expect("the redial linked");
        assert_ne!(first, redialed);
        assert!(sessions.opened(&first));
        assert_one_key_off::<LinkRef>(&redialed, |link| sessions.opened(link));
        assert_eq!(
            judge(&declared, &trace, &report, &End::Completed, false),
            Actions::Complete
        );
    }

    /// A session's admission opens a new link unless an earlier admission
    /// was on its whole link (Rule 25's template): after `a` on 7 and `b`
    /// on 8, a third admission on `a`'s link 7 adds none, and the same
    /// admission with only its peer or only its connection moved adds one.
    #[test]
    fn an_admission_joins_its_earlier_link_whole() {
        let report = |probe: &LinkRef| {
            let admitted = |peer: &str, session: u64, connection: u64| {
                format!(
                    "SessionAdmitted {{ peer: \"{peer}\", session_id: {session}, origin: \
                     Some(SessionOrigin {{ connection: {connection} }}) }}"
                )
            };
            let facts = format!(
                "[{}, {}, {}]",
                admitted("a", 1, 7),
                admitted("b", 2, 8),
                admitted(&probe.peer, 3, probe.connection)
            );
            FullReport::new(
                &[],
                Some(json!([{"store": "sessions", "snapshot": {"facts": facts}}])),
            )
        };
        let earlier = LinkRef {
            peer: "a".to_owned(),
            connection: 7,
        };
        let keys = assert_one_key_off::<LinkRef>(&earlier, |probe| {
            Sessions::of(&report(probe)).links.len() == 2
        });
        assert_eq!(keys, 2);
    }

    /// A peer's links are the node's links on the peer's whole name (Rule
    /// 25's template): in the close-relink-close history, each peer's
    /// first link is found by its own name, and by no other.
    #[test]
    fn a_peer_s_links_join_its_whole_name() {
        let (_, report, _) = close_relink_close();
        let sessions = Sessions::of(&report);
        let peers: std::collections::BTreeSet<String> =
            sessions.links.iter().map(|at| at.peer.clone()).collect();
        assert!(peers.len() > 1, "{sessions:?}");
        for peer in peers {
            let first = sessions
                .link(&peer, 0)
                .expect("each linked peer has a first link");
            let keys = assert_one_key_off::<PeerNameRef>(&PeerNameRef { peer }, |want| {
                sessions.link(&want.peer, 0).as_ref() == Some(&first)
            });
            assert_eq!(keys, 1);
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
        assert_one_key_off::<LinkRef>(&link, |link| sessions.ends(link, PEER_CLOSED) > 0);
        assert_eq!(sessions.ends(&link, PEER_CLOSED), 1);
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
