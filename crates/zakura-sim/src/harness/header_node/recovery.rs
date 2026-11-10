//! The storage harness's recovery phase (fuzz-12 item 5).
//!
//! A `Recover` step names a mirror as the source and ends the hostile
//! prefix. At its pick the scenario's controls stop, every invalidation the
//! scenario made is reconsidered, every hold retires, and from the next pick
//! the run's scheduling follows the recovery policy (see the `plan`
//! module). Recovery is then `Establishing`, which the harness drives
//! itself. At each quiet step, where no task is ready and the control task
//! has carried out every step it took, it offers either:
//!
//! - `peer:node:recovered`, once the source has an admitted session that's
//!   still live, or
//! - `peer:<source>:recovery-relink`, while it has none, unless the node's
//!   readmission cooldown for the source still lasts. Time stops where it
//!   ends.
//! - `peer:node:recovery-unreached`, once [`RECOVERY_RELINKS`] relinks are
//!   carried out and settled without a live source. Its pick ends recovery
//!   unreached, which offers nothing more.
//!
//! A quiet step has handled every admission, refusal, replacement and
//! disconnect an earlier step queued, since an unhandled one leaves its
//! receiving task ready. The `recovered` pick is the conditions point, and
//! writes a [`RecoveryRecord`] of the membership, the source and both
//! nodes' selected chains, and each member's.
//!
//! The `Recover` pick also keeps the node's [`Baseline`], its selected
//! chain before the pick reconsiders anything, which
//! `header-sync/bounded-adversary-progress` measures progress from.
//!
//! Only the environment breaks the assumptions recovery declares
//! ([`RecoveryEvidence::holds`]): the harness closing or relinking a peer,
//! or stopping the node. A session the node or a mirror ends by itself
//! breaks none, so a node that drops its honest source still owes progress.
//!
//! A scenario with a `Recover` step runs at most [`PREFIX_BOUND`] hostile
//! events before its pick ([`Prefix`]).

use std::{cell::RefCell, rc::Rc, sync::Arc, time::Duration};

use zakura_header_chain::{Frontier, SuffixWork};
use zakura_network::zakura::ZakuraPeerId;
use zakura_scheduler::SimInstant;
use zakura_storage::header::{adapter::EnginePort, controlled::ControlledPort};

use super::{
    adoption::{AdoptionEvidence, AdoptionFact},
    ends::{EndEvidence, EndFact, EnvironmentAction},
    fault::{CooldownEvidence, CooldownFact},
    session::{SessionEvidence, SessionFact},
    tap,
};
use crate::{property::Assume, Identity};

/// How long after the later of its submission, or its commit for a held
/// reply, and the `Recover` pick each storage operation must complete.
pub const RECOVERY_BOUND: Duration = Duration::from_secs(1);

/// How many times recovery relinks its source before it gives up.
pub const RECOVERY_RELINKS: u8 = 3;

/// The `recovered` action's local index in the control source.
pub(super) const RECOVERED: u64 = u64::MAX;

/// The `recovery-relink` action's local index in the control source.
pub(super) const RELINK: u64 = u64::MAX - 1;

/// The `recovery-unreached` action's local index in the control source.
pub(super) const UNREACHED: u64 = u64::MAX - 2;

/// The most hostile events a scenario with a `Recover` step executes before
/// its pick (hdr-13).
pub const PREFIX_BOUND: u32 = 32;

/// One hostile event of a prefix ([`Prefix`]), as the harness counted it,
/// with the step it executed at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostileEvent {
    /// The step.
    pub step: u64,
    /// What executed.
    pub hostile: Hostile,
}

/// What a hostile event was.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Hostile {
    /// The control source picked the script's step at this index.
    Step(usize),
    /// A hold or reply hold caught this storage operation at its submission.
    Caught(u64),
    /// This storage operation completed with an outcome the success-only
    /// base wouldn't offer.
    Fault(u64),
}

/// A run's hostile prefix: the hostile events that executed before its
/// `Recover` pick, in a scenario that has one ([`Hostile`]). Script slots
/// don't bound these, since a hold with no `nth` catches every operation it
/// selects and the `any` base offers a fault for each, so the harness keeps
/// each where it executes. Once [`PREFIX_BOUND`] have executed, it offers no
/// more until the `Recover` pick: the control source offers only the
/// `Recover` step, holds catch nothing new, and completions offer only what
/// the success-only base would. Below the bound the record changes nothing
/// a run offers. A scenario without a `Recover` step is unbounded and keeps
/// nothing. Cloning shares it.
#[derive(Clone, Default)]
pub struct Prefix(Rc<RefCell<Option<Vec<HostileEvent>>>>);

impl Prefix {
    /// A bounded prefix, for a scenario with a `Recover` step, with nothing
    /// executed yet.
    pub fn bounded() -> Self {
        Prefix(Rc::new(RefCell::new(Some(Vec::new()))))
    }

    /// How many hostile events executed, or `None` for an unbounded prefix.
    pub fn spent(&self) -> Option<u32> {
        self.0
            .borrow()
            .as_ref()
            .map(|events| u32::try_from(events.len()).expect("the bound keeps the record short"))
    }

    /// The hostile events that executed, in order, or `None` for an
    /// unbounded prefix.
    pub fn events(&self) -> Option<Vec<HostileEvent>> {
        self.0.borrow().clone()
    }

    /// Whether [`PREFIX_BOUND`] hostile events executed, so no more may.
    pub fn full(&self) -> bool {
        self.spent().is_some_and(|spent| spent >= PREFIX_BOUND)
    }

    /// Records `hostile`, executed at `step`. Does nothing for an unbounded
    /// prefix.
    pub(super) fn spend(&self, step: u64, hostile: Hostile) {
        if let Some(events) = self.0.borrow_mut().as_mut() {
            events.push(HostileEvent { step, hostile });
        }
    }
}

/// What recovery reads: the node's and the mirrors' engines, the node's
/// session view, cooldown facts and end facts, its configured readmission
/// cooldown, and each node's peer identity.
pub struct RecoveryInputs {
    /// The node under test's port.
    pub node: Arc<ControlledPort>,
    /// Each mirror's name and engine, in topology order.
    pub mirrors: Vec<(String, Arc<EnginePort>)>,
    /// The node's session view.
    pub sessions: SessionEvidence,
    /// The node's cooldown facts.
    pub cooldowns: CooldownEvidence,
    /// The node's end facts, whose stamps are the node's.
    pub ends: EndEvidence,
    /// The node's configured readmission cooldown.
    pub cooldown: Duration,
    /// Each node's peer identity and name.
    pub names: Vec<(ZakuraPeerId, String)>,
}

/// One live admitted session of the node: its peer, as the scenario names
/// it, and its session ID.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Member {
    /// The peer.
    pub peer: String,
    /// The session ID.
    pub session_id: u64,
}

/// A selected chain, read from an engine: every header from the finalized
/// anchor to the selected tip, both included, and the tip's work after the
/// anchor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectedChain {
    /// The chain, anchor first.
    pub chain: Vec<Frontier>,
    /// The tip's work after the anchor.
    pub work: SuffixWork,
}

impl SelectedChain {
    /// `port`'s selected chain now.
    pub fn of(port: &EnginePort) -> Self {
        SelectedChain {
            chain: port.selected_path(),
            work: port.snapshot().header_best_score.suffix_work,
        }
    }
}

/// The node's selected chain at the `Recover` pick, before it reconsiders
/// any invalidation. Storage can commit between the `Recover` and
/// `recovered` picks, so a baseline taken later could already hold what
/// recovery is meant to bring.
pub type Baseline = SelectedChain;

/// One recorded member's selected chain, read from its own engine at the
/// `recovered` pick.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberChain {
    /// The member.
    pub member: Member,
    /// Its selected chain.
    pub selected: SelectedChain,
}

/// What the `recovered` pick recorded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryRecord {
    /// The step that picked `recovered`.
    pub step: u64,
    /// When.
    pub at: SimInstant,
    /// The last stamp the node's facts took by the pick
    /// ([`super::stamp`]), so later facts are the ones after it.
    pub stamp: u64,
    /// Every live admitted session of the node, by peer.
    pub membership: Vec<Member>,
    /// Each member's selected chain, in membership order.
    pub members: Vec<MemberChain>,
    /// The node's selected chain.
    pub node_chain: SelectedChain,
    /// The source's live session.
    pub source: Member,
    /// The node's selected tip and its work after the finalized anchor.
    pub node_tip: (Frontier, SuffixWork),
    /// The source's selected tip and its work after the finalized anchor.
    pub source_tip: (Frontier, SuffixWork),
    /// The source's selected chain from the highest header it shares with
    /// the node's selected chain up to its tip, both ends included.
    pub source_chain: Vec<Frontier>,
    /// How many session facts the view held at the pick, so later ones are
    /// the ones after it.
    pub facts: usize,
    /// Each member's connection, from its link, in membership order, so an
    /// environment end joins the session it ends by identity.
    pub links: Vec<Option<u64>>,
}

impl RecoveryRecord {
    /// Whether a harness close of `peer`'s `connection` ended a recorded
    /// session: a member's, or with `source_only` the source's. A close
    /// whose connection the harness didn't name is judged by its peer.
    fn closes(&self, peer: &str, connection: Option<u64>, source_only: bool) -> bool {
        self.membership
            .iter()
            .zip(&self.links)
            .filter(|(member, _)| !source_only || **member == self.source)
            .any(|(member, link)| match connection {
                Some(closed) => *link == Some(closed),
                None => member.peer == peer,
            })
    }
}

/// The connection whose link carried session `session_id`, in `facts`.
fn link_of(facts: &[SessionFact], session_id: u64) -> Option<u64> {
    facts.iter().find_map(|fact| match fact {
        SessionFact::LinkOpened {
            connection,
            session_id: carried,
            ..
        } if *carried == session_id => Some(*connection),
        _ => None,
    })
}

/// Where recovery is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryState {
    /// The harness drives the conditions. `relinks` counts the relinks it
    /// picked.
    Establishing {
        /// The relinks picked so far.
        relinks: u8,
    },
    /// The conditions held together at the `recovered` pick.
    Recovered(Box<RecoveryRecord>),
    /// Every relink was carried out and settled without a live source, at
    /// the `recovery-unreached` pick.
    Unreached,
}

/// A run's recovery, shared by the control source, the storage source and
/// the harness's view: the recovery, empty until a `Recover` step is
/// picked, and the hostile prefix before it. Cloning shares both.
#[derive(Clone, Default)]
pub struct RecoveryEvidence(Rc<RefCell<Option<Recovery>>>, Prefix);

/// One recovery: its source, when it started, and where it is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recovery {
    /// The mirror the `Recover` step named.
    pub source: String,
    /// When the `Recover` step was picked.
    pub started: SimInstant,
    /// The node's selected chain at that pick.
    pub baseline: Baseline,
    /// Where it is.
    pub state: RecoveryState,
}

impl RecoveryEvidence {
    /// The evidence for a scenario with a `Recover` step, whose prefix is
    /// bounded.
    pub fn bounded() -> Self {
        RecoveryEvidence(Rc::default(), Prefix::bounded())
    }

    /// The run's hostile prefix.
    pub fn prefix(&self) -> &Prefix {
        &self.1
    }

    /// Whether the prefix is still open: the `Recover` step isn't picked
    /// yet, so a hostile event that executes now counts.
    pub(super) fn in_prefix(&self) -> bool {
        self.started().is_none()
    }

    /// The recovery, if a `Recover` step was picked.
    pub fn get(&self) -> Option<Recovery> {
        self.0.borrow().clone()
    }

    /// When the `Recover` step was picked, if one was.
    pub fn started(&self) -> Option<SimInstant> {
        self.0.borrow().as_ref().map(|recovery| recovery.started)
    }

    /// The record, once the `recovered` pick wrote it.
    pub fn record(&self) -> Option<RecoveryRecord> {
        match &self.0.borrow().as_ref()?.state {
            RecoveryState::Recovered(record) => Some((**record).clone()),
            RecoveryState::Establishing { .. } | RecoveryState::Unreached => None,
        }
    }

    /// Whether `assume` holds after a step of a recovering run whose node's
    /// end facts are `ends`. None holds before the `recovered` pick. After
    /// it, only the environment breaks one (Rule 13 of the hdr-9 to hdr-13
    /// plan): `StableMembership` holds until the harness closes a recorded
    /// member's connection or relinks any peer, `HonestPeer` until it
    /// closes the source's connection or relinks it, and both until it
    /// stops the node. A close is joined to the session it ends by its
    /// connection ([`RecoveryRecord::links`]), so closing a peer that was no
    /// member, or another link of a member, changes nothing recorded. The
    /// rest always hold, since recovery enforces them. A session the node,
    /// or a mirror running the same code, ends by itself breaks nothing, so
    /// an obligation the node owes its honest source runs on to its
    /// deadline.
    pub fn holds(&self, ends: &EndEvidence, assume: Assume) -> bool {
        let Some(record) = self.record() else {
            return false;
        };
        let source_only = match assume {
            Assume::StableMembership => false,
            Assume::HonestPeer => true,
            _ => return true,
        };
        !ends.facts().into_iter().any(|(stamp, fact)| {
            stamp > record.stamp
                && match fact {
                    EndFact::Environment {
                        action: EnvironmentAction::Shutdown,
                        ..
                    } => true,
                    // A relink admits a session, so it changes the
                    // membership whoever's it is, and replaces the source's.
                    EndFact::Environment {
                        peer,
                        action: EnvironmentAction::Relink,
                        ..
                    } => !source_only || peer == record.source.peer,
                    // A close breaks one only if it ends a recorded
                    // session, joined by its connection.
                    EndFact::Environment {
                        peer, connection, ..
                    } => record.closes(&peer, connection, source_only),
                    EndFact::ReactorExited { .. } => false,
                }
        })
    }

    /// The record and its objective, once the `recovered` pick wrote the
    /// record. The objective is `None` if no successor exists.
    pub fn objective(&self) -> Option<(RecoveryRecord, Option<RecoveryObjective>)> {
        let recovery = self.get()?;
        let RecoveryState::Recovered(record) = recovery.state else {
            return None;
        };
        let objective = RecoveryObjective::of(&recovery.baseline, &record);
        Some((*record, objective))
    }

    /// Where the objective stands, given the node's `adoption` and `ends`
    /// facts. The node's adoption at the `recovered` pick and every later
    /// one count. `header-sync/bounded-adversary-progress` and the liveness
    /// harness's finish and deadline sources all read this, so they can't
    /// disagree.
    pub fn progress(&self, adoption: &AdoptionEvidence, ends: &EndEvidence) -> RecoveryProgress {
        if self.unreached() {
            return RecoveryProgress::Concluded;
        }
        let Some((record, objective)) = self.objective() else {
            return RecoveryProgress::Pending;
        };
        let Some(objective) = objective else {
            return RecoveryProgress::Concluded;
        };
        let lost = [Assume::StableMembership, Assume::HonestPeer]
            .into_iter()
            .any(|assume| !self.holds(ends, assume));
        let tips: Vec<(u64, Frontier)> = adoption
            .facts()
            .into_iter()
            .filter_map(|stamped| match stamped.fact {
                AdoptionFact::Adopted(adoption) => Some((stamped.stamp, adoption.header_best)),
                _ => None,
            })
            .collect();
        let at_pick = tips.iter().rev().find(|(stamp, _)| *stamp <= record.stamp);
        let met = at_pick
            .into_iter()
            .chain(tips.iter().filter(|(stamp, _)| *stamp > record.stamp))
            .any(|(_, tip)| objective.met_by(tip));
        if lost || met {
            return RecoveryProgress::Concluded;
        }
        RecoveryProgress::Owed { since: record.at }
    }

    /// The node's baseline, once a `Recover` step was picked.
    pub fn baseline(&self) -> Option<Baseline> {
        self.0
            .borrow()
            .as_ref()
            .map(|recovery| recovery.baseline.clone())
    }

    /// Whether recovery ended unreached: every relink settled without a
    /// live source. It offers nothing more then.
    pub fn unreached(&self) -> bool {
        matches!(
            self.0.borrow().as_ref().map(|recovery| &recovery.state),
            Some(RecoveryState::Unreached)
        )
    }

    /// Counts a relink pick and returns the source to relink.
    pub(super) fn relinked(&self) -> String {
        let mut recovery = self.0.borrow_mut();
        let recovery = recovery
            .as_mut()
            .expect("a relink is offered only while recovery establishes");
        if let RecoveryState::Establishing { relinks } = &mut recovery.state {
            *relinks += 1;
        }
        recovery.source.clone()
    }

    /// Ends recovery unreached, at the `recovery-unreached` pick.
    pub(super) fn give_up(&self) {
        if let Some(recovery) = self.0.borrow_mut().as_mut() {
            recovery.state = RecoveryState::Unreached;
        }
    }

    /// Starts recovery from `source` at `started`, from the node's
    /// `baseline`.
    pub(super) fn start(&self, source: String, started: SimInstant, baseline: Baseline) {
        *self.0.borrow_mut() = Some(Recovery {
            source,
            started,
            baseline,
            state: RecoveryState::Establishing { relinks: 0 },
        });
    }
}

/// What a recovery owes the node, by item 7 of the hdr-9 to hdr-13 plan:
/// `W`, the most work of any member's selected tip or the node's own at the
/// `recovered` pick, and the headers that meet it. Those are every header of
/// every such chain whose tip has work `W` that the node's baseline chain
/// doesn't hold. Every header of those chains is eligible, since the
/// `Recover` pick reconsidered every invalidation and nothing invalidates
/// after it. Any supplier counts, and with a tie any of the tied chains
/// does. A heavier chain no member holds can't come, since members only
/// take headers some member holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryObjective {
    /// `W`.
    pub work: SuffixWork,
    /// The headers that meet it, in chain order, each once.
    pub headers: Vec<Frontier>,
}

impl RecoveryObjective {
    /// The objective of a recovery from `baseline` that recorded `record`,
    /// or `None` if no candidate has more work than the baseline, so no
    /// successor exists.
    pub fn of(baseline: &Baseline, record: &RecoveryRecord) -> Option<Self> {
        let candidates: Vec<&SelectedChain> = record
            .members
            .iter()
            .map(|member| &member.selected)
            .chain(std::iter::once(&record.node_chain))
            .collect();
        let work = candidates.iter().map(|chain| chain.work).max()?;
        if work <= baseline.work {
            return None;
        }
        let mut headers = Vec::new();
        for chain in candidates.into_iter().filter(|chain| chain.work == work) {
            for header in &chain.chain {
                if !baseline.chain.contains(header) && !headers.contains(header) {
                    headers.push(*header);
                }
            }
        }
        Some(RecoveryObjective { work, headers })
    }

    /// Whether the node adopting `tip` meets it.
    pub fn met_by(&self, tip: &Frontier) -> bool {
        self.headers.contains(tip)
    }
}

/// Where a recovery's objective stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryProgress {
    /// No `Recover` was picked, or recovery is still establishing.
    Pending,
    /// Nothing is owed any more: recovery ended unreached or found no
    /// successor, the node adopted a header that meets the objective, or
    /// the environment broke an assumption after the `recovered` pick.
    Concluded,
    /// The node owes a successor, since the `recovered` pick at `since`.
    Owed {
        /// The `recovered` pick's time.
        since: SimInstant,
    },
}

/// The node's live admitted sessions in `facts`: each admitted session whose
/// link is still open, which no later session replaced and whose
/// disconnect hasn't ended it. A session admitted after its link closed
/// isn't live.
pub fn live_sessions(facts: &[SessionFact]) -> Vec<Member> {
    let mut live: Vec<(Member, Option<u64>)> = Vec::new();
    let mut connections: Vec<(u64, u64)> = Vec::new();
    let mut closed: Vec<u64> = Vec::new();
    for fact in facts {
        match fact {
            SessionFact::LinkOpened {
                connection,
                session_id,
                ..
            } => connections.push((*session_id, *connection)),
            SessionFact::SessionAdmitted {
                peer,
                session_id,
                replaced,
                ..
            } => {
                live.retain(|(member, _)| Some(member.session_id) != *replaced);
                let connection = connections
                    .iter()
                    .find(|(session, _)| session == session_id)
                    .map(|(_, connection)| *connection);
                if connection.is_some_and(|connection| closed.contains(&connection)) {
                    continue;
                }
                live.push((
                    Member {
                        peer: peer.clone(),
                        session_id: *session_id,
                    },
                    connection,
                ));
            }
            SessionFact::SessionEnded { session_id, .. } => {
                live.retain(|(member, _)| member.session_id != *session_id);
            }
            SessionFact::LinkClosed { connection, .. } => {
                live.retain(|(_, link)| *link != Some(*connection));
                closed.push(*connection);
            }
            SessionFact::SessionRefused { .. } | SessionFact::SessionRetired { .. } => {}
        }
    }
    let mut members: Vec<Member> = live.into_iter().map(|(member, _)| member).collect();
    members.sort();
    members
}

/// Whether `facts` after the first `from` change the node's membership: a
/// session admitted or ended, or a link closed, by anyone.
pub fn membership_changed(facts: &[SessionFact], from: usize) -> bool {
    facts.get(from..).unwrap_or_default().iter().any(|fact| {
        matches!(
            fact,
            SessionFact::SessionAdmitted { .. }
                | SessionFact::SessionEnded { .. }
                | SessionFact::LinkClosed { .. }
        )
    })
}

/// Whether `session` is still live after the first `from` of `facts`: no
/// later fact ended it, replaced it or closed its link.
pub fn session_lasts(facts: &[SessionFact], from: usize, session: &Member) -> bool {
    let connection = facts.iter().find_map(|fact| match fact {
        SessionFact::LinkOpened {
            connection,
            session_id,
            ..
        } if *session_id == session.session_id => Some(*connection),
        _ => None,
    });
    !facts
        .get(from..)
        .unwrap_or_default()
        .iter()
        .any(|fact| match fact {
            SessionFact::SessionEnded { session_id, .. } => *session_id == session.session_id,
            SessionFact::SessionAdmitted { replaced, .. } => *replaced == Some(session.session_id),
            SessionFact::LinkClosed {
                connection: closed, ..
            } => Some(*closed) == connection,
            _ => false,
        })
}

impl RecoveryInputs {
    /// The source's live session, if it has one.
    fn source_session(&self, source: &str) -> Option<Member> {
        live_sessions(&self.sessions.facts())
            .into_iter()
            .rev()
            .find(|member| member.peer == source)
    }

    /// Until when the node's readmission cooldown for `source` lasts, as
    /// virtual time since the run's start: the end of the newest cooldown
    /// it started for the source, or the configured cooldown after its
    /// newest refusal of the source, whichever is later.
    fn cooldown_until(&self, source: &str) -> Option<Duration> {
        let (id, _) = self.names.iter().find(|(_, name)| name == source)?;
        cooldown_end(&self.cooldowns.facts(), &tap::peer(id), self.cooldown)
    }

    /// Mirror `name`'s engine.
    fn mirror_engine(&self, mirror: &str) -> Option<&EnginePort> {
        self.mirrors
            .iter()
            .find(|(name, _)| name == mirror)
            .map(|(_, port)| &**port)
    }

    /// The record of the conditions at the `recovered` pick, at `step` and
    /// `at`, with `source`'s live session `session`.
    fn record(
        &self,
        (step, at): (u64, SimInstant),
        source: &str,
        session: Member,
    ) -> Result<RecoveryRecord, String> {
        let facts = self.sessions.facts();
        let engine = self
            .mirror_engine(source)
            .ok_or_else(|| format!("recovery's source {source} is no mirror"))?;
        let node = self.node.engine();
        let (node_chain, node_snapshot) = (node.selected_path(), node.snapshot());
        let (source_chain, source_snapshot) = (engine.selected_path(), engine.snapshot());
        let shared = node_chain
            .iter()
            .zip(&source_chain)
            .take_while(|(a, b)| a == b)
            .count();
        let membership = live_sessions(&facts);
        let members = membership
            .iter()
            .map(|member| {
                let port = self
                    .mirror_engine(&member.peer)
                    .ok_or_else(|| format!("recovery's member {} is no mirror", member.peer))?;
                Ok(MemberChain {
                    member: member.clone(),
                    selected: SelectedChain::of(port),
                })
            })
            .collect::<Result<_, String>>()?;
        let links = membership
            .iter()
            .map(|member| link_of(&facts, member.session_id))
            .collect();
        Ok(RecoveryRecord {
            step,
            at,
            stamp: self.ends.stamps().issued(),
            members,
            node_chain: SelectedChain {
                chain: node_chain.clone(),
                work: node_snapshot.header_best_score.suffix_work,
            },
            membership,
            source: session,
            node_tip: (
                node_snapshot.frontiers.header_best,
                node_snapshot.header_best_score.suffix_work,
            ),
            source_tip: (
                source_snapshot.frontiers.header_best,
                source_snapshot.header_best_score.suffix_work,
            ),
            source_chain: source_chain[shared.saturating_sub(1)..].to_vec(),
            facts: facts.len(),
            links,
        })
    }
}

/// Recovery's part of the control source: what it offers at quiet steps and
/// what its picks do.
pub(super) struct Driver {
    pub(super) inputs: RecoveryInputs,
    pub(super) evidence: RecoveryEvidence,
}

impl Driver {
    /// What establishing recovery offers next, or `None` once it's recovered
    /// or before it started.
    fn next(&self, now: SimInstant) -> Option<(String, Next)> {
        let Some(Recovery {
            source,
            state: RecoveryState::Establishing { relinks },
            ..
        }) = self.evidence.get()
        else {
            return None;
        };
        let next = next(
            relinks,
            self.inputs.source_session(&source).is_some(),
            self.inputs.cooldown_until(&source),
            now,
        );
        Some((source, next))
    }

    /// Appends what recovery offers at a quiet step at `now`, where the
    /// control task has carried out every step.
    pub(super) fn quiet(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        match self.next(now) {
            Some((_, Next::Recovered)) => {
                out.push((Identity::peer("node", "recovered"), RECOVERED))
            }
            Some((source, Next::Relink)) => {
                out.push((Identity::peer(&source, "recovery-relink"), RELINK));
            }
            Some((_, Next::GiveUp)) => {
                out.push((Identity::peer("node", "recovery-unreached"), UNREACHED));
            }
            Some((_, Next::WaitUntil(_))) | None => {}
        }
    }

    /// Appends where time must stop: the end of the source's cooldown, while
    /// recovery waits for it.
    pub(super) fn boundaries(&self, now: SimInstant, out: &mut Vec<crate::Boundary>) {
        if let Some((_, Next::WaitUntil(until))) = self.next(now) {
            out.push(crate::Boundary::Opens(until));
        }
    }

    /// Writes the record at the `recovered` pick, at `step`.
    pub(super) fn recovered(&self, step: u64) -> Result<(), String> {
        let source = self
            .evidence
            .get()
            .expect("`recovered` is offered only while recovery establishes")
            .source;
        let session = self
            .inputs
            .source_session(&source)
            .expect("`recovered` is offered only while the source has a live session");
        let at = zakura_scheduler::hooks::now()
            .ok_or_else(|| "a recovery is recorded inside a run".to_owned())?;
        let record = self.inputs.record((step, at), &source, session)?;
        if let Some(recovery) = self.evidence.0.borrow_mut().as_mut() {
            recovery.state = RecoveryState::Recovered(Box::new(record));
        }
        Ok(())
    }
}

/// When an operation must be done by, at `now`, once recovery started at
/// `started`: [`RECOVERY_BOUND`] after the later of the start and the
/// operation's own submission, or commit for a held reply, which was `own`
/// before `now`.
pub(super) fn deadline(now: SimInstant, started: SimInstant, own: Duration) -> SimInstant {
    let since = now.saturating_duration_since(started);
    let left = RECOVERY_BOUND.saturating_sub(own.min(since));
    crate::harness::policy::after(now, left)
}

/// What establishing recovery offers next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Next {
    /// The conditions hold, so `recovered` is offered.
    Recovered,
    /// The source has no live session, so a relink is offered.
    Relink,
    /// The source has no live session, but the node's cooldown for it lasts
    /// until this instant, where time stops.
    WaitUntil(SimInstant),
    /// Every relink is carried out and settled without a live source, so
    /// `recovery-unreached` is offered.
    GiveUp,
}

/// What establishing recovery offers at `now`, after `relinks` relinks,
/// with or without a `live` source session, while the node's readmission
/// cooldown for the source lasts until `cooldown`, as virtual time since the
/// run's start.
pub(super) fn next(relinks: u8, live: bool, cooldown: Option<Duration>, now: SimInstant) -> Next {
    if live {
        return Next::Recovered;
    }
    if relinks >= RECOVERY_RELINKS {
        return Next::GiveUp;
    }
    match cooldown.map(SimInstant::from_start) {
        Some(until) if until > now => Next::WaitUntil(until),
        _ => Next::Relink,
    }
}

/// When the node's readmission cooldown for the peer whose digest is `hex`
/// ends, as virtual time since the run's start, from its cooldown `facts`:
/// the end of the newest cooldown it started for the peer, or the
/// `configured` cooldown after its newest refusal of the peer, whichever is
/// later. `None` if it has neither.
pub(super) fn cooldown_end(
    facts: &[CooldownFact],
    hex: &str,
    configured: Duration,
) -> Option<Duration> {
    facts
        .iter()
        .filter_map(|fact| match fact {
            CooldownFact::Started {
                peer, cooldown, at, ..
            } if peer == hex => Some(at.saturating_add(*cooldown)),
            CooldownFact::Refused { peer, at, .. } if peer == hex => {
                Some(at.saturating_add(configured))
            }
            _ => None,
        })
        .max()
}

#[cfg(test)]
mod tests {
    use zakura_chain::block;
    use zakura_network::zakura::{observe::ReactorEvent, ServicePeerDirection, SessionOrigin};

    use super::*;

    /// `ms` milliseconds into the run.
    fn at(ms: u64) -> SimInstant {
        SimInstant::from_start(Duration::from_millis(ms))
    }

    /// Every assumption a recover declares.
    const DECLARED: [Assume; 5] = [
        Assume::StableMembership,
        Assume::HonestPeer,
        Assume::BoundedStorage,
        Assume::BoundedDelivery,
        Assume::FairScheduling,
    ];

    /// `peer`'s peer ID, its one byte repeated.
    fn id(peer: u8) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![peer; 32]).expect("32 bytes is a valid peer ID")
    }

    /// Links `name` (peer ID byte `peer`) to the node on connection
    /// `session_id` and admits its session `session_id`, as a run's topology
    /// and tap report them.
    fn link(sessions: &SessionEvidence, peer: u8, name: &str, session_id: u64) {
        let origin = SessionOrigin {
            connection: session_id,
            quic_stream: None,
            generation: session_id,
        };
        let direction = ServicePeerDirection::Inbound;
        sessions.link_opened(&id(peer), name, origin, session_id, name, direction);
        sessions.reactor(&ReactorEvent::SessionAdmitted {
            peer: id(peer),
            session_id,
            direction,
            replaced: None,
            origin: Some(origin),
        });
    }

    /// Which of [`DECLARED`] `recovery` says hold over `ends`.
    fn holding(recovery: &RecoveryEvidence, ends: &EndEvidence) -> Vec<Assume> {
        DECLARED
            .into_iter()
            .filter(|assume| recovery.holds(ends, *assume))
            .collect()
    }

    /// An empty selected chain.
    fn empty() -> SelectedChain {
        SelectedChain {
            chain: Vec::new(),
            work: SuffixWork::zero(),
        }
    }

    /// `mirror-0`'s recovery, `recovered` at `sessions`' current facts, with
    /// every stamp `ends` handed out so far before the pick.
    fn recovered(sessions: &SessionEvidence, ends: &EndEvidence) -> RecoveryEvidence {
        let recovery = RecoveryEvidence::default();
        recovery.start("mirror-0".into(), at(0), empty());
        let source = Member {
            peer: "mirror-0".into(),
            session_id: 1,
        };
        let tip = (
            Frontier::new(block::Height(8), block::Hash([8; 32])),
            SuffixWork::zero(),
        );
        let record = RecoveryRecord {
            step: 1,
            at: at(0),
            stamp: ends.stamps().issued(),
            membership: live_sessions(&sessions.facts()),
            members: Vec::new(),
            node_chain: empty(),
            source,
            node_tip: tip,
            source_tip: tip,
            source_chain: vec![tip.0],
            facts: sessions.facts().len(),
            links: live_sessions(&sessions.facts())
                .iter()
                .map(|member| link_of(&sessions.facts(), member.session_id))
                .collect(),
        };
        if let Some(recovery) = recovery.0.borrow_mut().as_mut() {
            recovery.state = RecoveryState::Recovered(Box::new(record));
        }
        recovery
    }

    /// A session view with `mirror-0`'s session 1 and `mirror-1`'s session 2
    /// live.
    fn two_links() -> SessionEvidence {
        let sessions = SessionEvidence::default();
        link(&sessions, 2, "mirror-0", 1);
        link(&sessions, 3, "mirror-1", 2);
        sessions
    }

    /// The harness's `action` on `peer`.
    fn environment(peer: &str, action: EnvironmentAction) -> EndFact {
        EndFact::Environment {
            peer: peer.to_owned(),
            connection: None,
            action,
        }
    }

    #[test]
    fn hdr13_assumptions_hold_from_recovered_until_the_environment_breaks_them() {
        // Before the `recovered` pick none holds: with no recovery, while it
        // establishes, and once it ended unreached.
        let sessions = two_links();
        let ends = EndEvidence::default();
        let recovery = RecoveryEvidence::default();
        assert_eq!(holding(&recovery, &ends), []);
        recovery.start("mirror-0".into(), at(0), empty());
        assert_eq!(holding(&recovery, &ends), []);
        recovery.give_up();
        assert_eq!(holding(&recovery, &ends), []);

        // From the pick, all five hold. An environment action before the
        // pick, such as recovery's own relink, breaks nothing.
        ends.record(environment("mirror-0", EnvironmentAction::Relink));
        assert_eq!(holding(&recovered(&sessions, &ends), &ends), DECLARED);

        // The harness closing or relinking another peer after the pick ends
        // `StableMembership` only.
        let rest = [
            Assume::BoundedStorage,
            Assume::BoundedDelivery,
            Assume::FairScheduling,
        ];
        let honest: Vec<Assume> = [Assume::HonestPeer].into_iter().chain(rest).collect();
        for action in [EnvironmentAction::Close, EnvironmentAction::Relink] {
            let ends = EndEvidence::default();
            let recovery = recovered(&sessions, &ends);
            ends.record(environment("mirror-1", action));
            assert_eq!(holding(&recovery, &ends), honest, "{action:?}");
        }

        // A close joins the session it ends by its connection: closing
        // another link of the source ends nothing recorded, and closing the
        // source's own ends `StableMembership` and `HonestPeer`.
        let close = |connection| EndFact::Environment {
            peer: "mirror-0".to_owned(),
            connection: Some(connection),
            action: EnvironmentAction::Close,
        };
        let ends = EndEvidence::default();
        let recovery = recovered(&sessions, &ends);
        ends.record(close(9));
        assert_eq!(holding(&recovery, &ends), DECLARED);
        ends.record(close(1));
        assert_eq!(holding(&recovery, &ends), rest);

        // Closing a peer that was no member at the pick changes nothing
        // recorded, so all five keep holding, and relinking one admits a
        // session, which ends `StableMembership`.
        let ends = EndEvidence::default();
        let recovery = recovered(&sessions, &ends);
        ends.record(environment("mirror-2", EnvironmentAction::Close));
        assert_eq!(holding(&recovery, &ends), DECLARED);
        ends.record(environment("mirror-2", EnvironmentAction::Relink));
        assert_eq!(holding(&recovery, &ends), honest);

        // Closing or relinking the source ends `HonestPeer` too, and so does
        // stopping the node. The other three keep holding.
        for (peer, action) in [
            ("mirror-0", EnvironmentAction::Close),
            ("mirror-0", EnvironmentAction::Relink),
            ("node", EnvironmentAction::Shutdown),
        ] {
            let ends = EndEvidence::default();
            let recovery = recovered(&sessions, &ends);
            ends.record(environment(peer, action));
            assert_eq!(holding(&recovery, &ends), rest, "{peer} {action:?}");
        }

        // Ends the environment didn't make break nothing: the service ending
        // the source's session, a link the source closed itself, another
        // peer admitted, and the reactor exiting by itself. Each would hide
        // a node that drops its honest source.
        let ends = EndEvidence::default();
        let recovery = recovered(&sessions, &ends);
        sessions.reactor(&ReactorEvent::SessionEnded {
            peer: id(2),
            session_id: 1,
            reason: "unproductive",
        });
        sessions.link_closed(2);
        link(&sessions, 4, "mirror-2", 3);
        ends.record(EndFact::ReactorExited { step: 9 });
        assert_eq!(holding(&recovery, &ends), DECLARED);
    }

    #[test]
    fn each_operation_is_bounded_from_the_later_of_its_start_and_recovery() {
        // Recovery started at 10 s. One operation was submitted at 9.7 s and
        // one at 10.5 s, and it's 10.7 s now: they're due at 11 s and 11.5 s.
        let (started, now) = (at(10_000), at(10_700));
        assert_eq!(
            deadline(now, started, Duration::from_millis(1_000)),
            at(11_000)
        );
        assert_eq!(
            deadline(now, started, Duration::from_millis(200)),
            at(11_500)
        );
        // One overdue a second already is due now, and time can't pass it.
        assert_eq!(
            deadline(at(12_000), started, Duration::from_secs(5)),
            at(12_000)
        );
    }

    #[test]
    fn a_live_source_is_recovered_whatever_else_holds() {
        for relinks in 0..=RECOVERY_RELINKS {
            assert_eq!(
                next(relinks, true, Some(Duration::from_secs(9)), at(0)),
                Next::Recovered
            );
        }
    }

    #[test]
    fn a_refused_source_waits_out_its_cooldown_then_relinks() {
        // Refused at 2 s with a 5 s cooldown: time stops at 7 s, then the
        // harness relinks.
        let facts = [CooldownFact::Refused {
            peer: "aa".into(),
            session_id: 3,
            at: Duration::from_secs(2),
        }];
        let until = cooldown_end(&facts, "aa", Duration::from_secs(5));
        assert_eq!(until, Some(Duration::from_secs(7)));
        assert_eq!(cooldown_end(&facts, "bb", Duration::from_secs(5)), None);
        assert_eq!(next(1, false, until, at(3_000)), Next::WaitUntil(at(7_000)));
        assert_eq!(next(1, false, until, at(7_000)), Next::Relink);
        assert_eq!(next(0, false, None, at(0)), Next::Relink);
    }

    #[test]
    fn the_later_of_a_started_cooldown_and_a_refusal_counts() {
        let facts = [
            CooldownFact::Started {
                peer: "aa".into(),
                origin: 1,
                cooldown: Duration::from_secs(10),
                at: Duration::from_secs(1),
                after: 4,
            },
            CooldownFact::Refused {
                peer: "aa".into(),
                session_id: 2,
                at: Duration::from_secs(3),
            },
        ];
        assert_eq!(
            cooldown_end(&facts, "aa", Duration::from_secs(5)),
            Some(Duration::from_secs(11))
        );
    }

    #[test]
    fn recovery_ends_unreached_only_once_the_third_relink_settles() {
        assert_eq!(next(RECOVERY_RELINKS - 1, false, None, at(0)), Next::Relink);
        assert_eq!(next(RECOVERY_RELINKS, false, None, at(0)), Next::GiveUp);
        assert_eq!(
            next(RECOVERY_RELINKS, false, Some(Duration::from_secs(9)), at(0)),
            Next::GiveUp,
            "no time boundary once every relink is spent"
        );
        let evidence = RecoveryEvidence::default();
        evidence.start("mirror-0".into(), at(0), empty());
        for _ in 0..RECOVERY_RELINKS {
            assert_eq!(evidence.relinked(), "mirror-0");
        }
        // The third relink is picked but not yet settled: still
        // establishing, and its admission still recovers.
        assert!(!evidence.unreached());
        assert_eq!(next(RECOVERY_RELINKS, true, None, at(0)), Next::Recovered);
        // It settled without a live source, and the quiet step's pick ends
        // recovery.
        evidence.give_up();
        assert!(evidence.unreached());
        assert_eq!(evidence.record(), None);
    }

    /// Header `height` on fixture branch `branch`, a made-up hash.
    fn header(branch: u8, height: u32) -> Frontier {
        let byte = u8::try_from(height).expect("test heights are small");
        Frontier::new(
            block::Height(height),
            block::Hash([branch.wrapping_mul(64) ^ byte; 32]),
        )
    }

    /// A chain of `main` branch-0 headers, then `fork` more on `branch`,
    /// each header one unit of work, from the anchor at height 0.
    fn chain(main: u32, branch: u8, fork: u32) -> SelectedChain {
        let mut chain: Vec<Frontier> = (0..=main).map(|height| header(0, height)).collect();
        chain.extend((main + 1..=main + fork).map(|height| header(branch, height)));
        let tip = main + fork;
        SelectedChain {
            chain,
            work: SuffixWork::new(zakura_chain::work::difficulty::U256::from(tip)),
        }
    }

    /// A record whose members hold `members` and whose node holds `node`.
    fn record_of(members: Vec<SelectedChain>, node: SelectedChain) -> RecoveryRecord {
        let tip = |chain: &SelectedChain| (*chain.chain.last().unwrap(), chain.work);
        let members: Vec<MemberChain> = members
            .into_iter()
            .enumerate()
            .map(|(index, selected)| MemberChain {
                member: Member {
                    peer: format!("mirror-{index}"),
                    session_id: u64::try_from(index).unwrap(),
                },
                selected,
            })
            .collect();
        let count = members.len();
        RecoveryRecord {
            step: 7,
            at: at(2_000),
            stamp: 0,
            membership: members.iter().map(|chain| chain.member.clone()).collect(),
            members,
            source: Member {
                peer: "mirror-0".into(),
                session_id: 0,
            },
            node_tip: tip(&node),
            source_tip: tip(&node),
            source_chain: Vec::new(),
            node_chain: node,
            facts: 0,
            links: vec![None; count],
        }
    }

    /// The objective is every header of the heaviest chains the baseline
    /// lacks, whoever holds them: a heavier member's, a tied member's, or
    /// the node's own reconsidered branch. A lighter fork isn't in it, a
    /// shared prefix is, and no heavier candidate means none.
    #[test]
    fn the_objective_is_the_heaviest_chains_past_the_baseline() {
        let baseline = chain(3, 0, 0);
        let headers = |objective: Option<RecoveryObjective>| objective.map(|o| o.headers);

        // One member one header ahead: its successor.
        let one = record_of(vec![chain(4, 0, 0)], chain(3, 0, 0));
        assert_eq!(
            headers(RecoveryObjective::of(&baseline, &one)),
            Some(vec![header(0, 4)])
        );

        // A lighter fork from the baseline's tip isn't in it.
        let fork = record_of(vec![chain(5, 0, 0), chain(3, 1, 1)], chain(3, 0, 0));
        let objective = RecoveryObjective::of(&baseline, &fork).unwrap();
        assert_eq!(objective.headers, [header(0, 4), header(0, 5)]);
        assert!(!objective.met_by(&header(1, 4)));
        assert_eq!(objective.work, chain(5, 0, 0).work);

        // A fork that shares the heaviest chain's first header: that header
        // meets it, whoever serves it.
        let shared = record_of(vec![chain(6, 0, 0), chain(4, 1, 1)], chain(3, 0, 0));
        assert!(RecoveryObjective::of(&baseline, &shared)
            .unwrap()
            .met_by(&header(0, 4)));

        // Two tied chains: either one's headers meet it.
        let tied = record_of(vec![chain(4, 0, 0), chain(3, 1, 1)], chain(3, 0, 0));
        assert_eq!(
            headers(RecoveryObjective::of(&baseline, &tied)),
            Some(vec![header(0, 4), header(1, 4)])
        );

        // The node's own branch, reconsidered after a baseline that lost it,
        // outweighs a lighter separate member fork.
        let lost = chain(2, 0, 0);
        let reconsidered = record_of(vec![chain(4, 0, 0)], chain(2, 1, 3));
        assert_eq!(
            headers(RecoveryObjective::of(&lost, &reconsidered)),
            Some(vec![header(1, 3), header(1, 4), header(1, 5)])
        );

        // Nothing heavier than the baseline: no successor.
        let even = record_of(vec![chain(3, 0, 0), chain(2, 1, 1)], chain(3, 0, 0));
        assert_eq!(RecoveryObjective::of(&baseline, &even), None);
    }

    /// The node's adoption at `height` on branch 0, reported to `adoption`.
    fn adopt(adoption: &AdoptionEvidence, height: u32) {
        adopt_tip(adoption, header(0, height));
    }

    /// The node's adoption of `tip`, reported to `adoption`.
    fn adopt_tip(adoption: &AdoptionEvidence, tip: Frontier) {
        use zakura_header_chain::{HeaderGeneration, StateVersion, VerifiedGeneration};

        let anchor = header(0, 0);
        let height = tip.height.0;
        adoption.record(
            Duration::ZERO,
            AdoptionFact::Adopted(super::super::adoption::Adoption {
                state_version: StateVersion::new(u64::from(height)),
                header_generation: HeaderGeneration::new(1),
                finalized: anchor,
                header_best: tip,
                suffix_work: zakura_chain::work::difficulty::U256::from(height),
                oldest_retained: anchor.height,
                verified_generation: VerifiedGeneration::new(1),
                verified_best: anchor,
            }),
        );
    }

    /// A recovery of `mirror-0` from a baseline at 3, recorded once
    /// `adoption`'s and `ends`' facts so far are in, with its source at 4
    /// and `mirror-1` at 3.
    fn owing(ends: &EndEvidence) -> RecoveryEvidence {
        let recovery = RecoveryEvidence::default();
        recovery.start("mirror-0".into(), at(0), chain(3, 0, 0));
        let record = RecoveryRecord {
            stamp: ends.stamps().issued(),
            ..record_of(vec![chain(4, 0, 0), chain(3, 0, 0)], chain(3, 0, 0))
        };
        if let Some(recovery) = recovery.0.borrow_mut().as_mut() {
            recovery.state = RecoveryState::Recovered(Box::new(record));
        }
        recovery
    }

    /// Progress is pending until the record, owed from the pick until the
    /// node adopts a header that meets the objective, either before the
    /// pick or after it, and concluded once the environment breaks an
    /// assumption or recovery ends unreached. An adoption off the objective
    /// answers nothing.
    #[test]
    fn progress_is_owed_from_the_record_until_met_or_ended() {
        let ends = EndEvidence::default();
        let adoption = AdoptionEvidence::stamped_by(ends.stamps());
        let pending = RecoveryEvidence::default();
        assert_eq!(
            pending.progress(&adoption, &ends),
            RecoveryProgress::Pending
        );
        pending.start("mirror-0".into(), at(0), chain(3, 0, 0));
        assert_eq!(
            pending.progress(&adoption, &ends),
            RecoveryProgress::Pending
        );
        pending.give_up();
        assert_eq!(
            pending.progress(&adoption, &ends),
            RecoveryProgress::Concluded
        );

        // Owed from the pick's time, with the node at the baseline.
        adopt(&adoption, 3);
        let recovery = owing(&ends);
        let owed = RecoveryProgress::Owed { since: at(2_000) };
        assert_eq!(recovery.progress(&adoption, &ends), owed);
        // A later adoption of the baseline's own tip answers nothing, and
        // one of the successor meets it.
        adopt(&adoption, 3);
        assert_eq!(recovery.progress(&adoption, &ends), owed);
        adopt(&adoption, 4);
        assert_eq!(
            recovery.progress(&adoption, &ends),
            RecoveryProgress::Concluded
        );

        // The node already at the successor at the pick meets it there. An
        // older adoption at the pick doesn't.
        let ends = EndEvidence::default();
        let adoption = AdoptionEvidence::stamped_by(ends.stamps());
        adopt(&adoption, 4);
        assert_eq!(
            owing(&ends).progress(&adoption, &ends),
            RecoveryProgress::Concluded
        );
        let ends = EndEvidence::default();
        let adoption = AdoptionEvidence::stamped_by(ends.stamps());
        adopt(&adoption, 4);
        adopt(&adoption, 3);
        assert_eq!(owing(&ends).progress(&adoption, &ends), owed);

        // The environment closing a member after the pick ends what's owed,
        // and the node ending the source's session doesn't.
        let ends = EndEvidence::default();
        let adoption = AdoptionEvidence::stamped_by(ends.stamps());
        let recovery = owing(&ends);
        ends.record(EndFact::ReactorExited { step: 9 });
        assert_eq!(recovery.progress(&adoption, &ends), owed);
        ends.record(environment("mirror-1", EnvironmentAction::Close));
        assert_eq!(
            recovery.progress(&adoption, &ends),
            RecoveryProgress::Concluded
        );
    }

    /// A harness whose view holds only a recovery's evidence, for driving
    /// `bounded-adversary-progress`'s check directly.
    struct RecoveryOnly {
        adoption: AdoptionEvidence,
        recovery: RecoveryEvidence,
        ends: EndEvidence,
    }

    impl crate::property::HasView for RecoveryOnly {
        type View = crate::harness::header_sync::StorageView;

        /// A view with the harness's recovery, adoption and end evidence,
        /// and empty everything else.
        fn view(&self) -> Self::View {
            use zakura_storage::header::{controlled::ControlledPort, fixture::HeaderFixture};

            crate::harness::header_sync::StorageView {
                port: Arc::new(ControlledPort::new(&HeaderFixture::regtest())),
                direct_dispatch_broken: false,
                honest_peers: Default::default(),
                adoption: self.adoption.clone(),
                sessions: Vec::new(),
                faults: Default::default(),
                capacity: Default::default(),
                claims: Default::default(),
                rounds: Default::default(),
                stale_calls: Default::default(),
                wake: Default::default(),
                hold_wake: Default::default(),
                ends: self.ends.clone(),
                stamps: self.ends.stamps(),
                round_life: Default::default(),
                retirement: Default::default(),
                chain_effect: Default::default(),
                authority: Default::default(),
                outstanding: Default::default(),
                recovery: self.recovery.clone(),
                fork_choice: Default::default(),
                publication: Default::default(),
            }
        }
    }

    /// At the `recovered` pick with the node still at its baseline, the
    /// property opens one obligation, and only a later adoption of a header
    /// the objective holds discharges it. The baseline's tip again, a fork
    /// tip off the objective and a fact that isn't an adoption answer
    /// nothing.
    #[test]
    fn the_obligation_discharges_only_on_an_adoption_the_objective_holds() {
        use crate::{
            check::{Phase, StepCtx},
            domain::header_sync_progress::bounded_adversary_progress,
            property::{CheckCtx, FactAnswer, HasView, Verdict},
        };

        let ends = EndEvidence::default();
        let adoption = AdoptionEvidence::stamped_by(ends.stamps());
        adopt(&adoption, 3);
        let harness = RecoveryOnly {
            recovery: owing(&ends),
            adoption: adoption.clone(),
            ends: ends.clone(),
        };
        let registered = [bounded_adversary_progress::<RecoveryOnly>()];
        let pick = crate::Identity::advance_time(at(2_000));
        let log = crate::ObservationLog::default();
        // The record's step, so the check judges the pick.
        let step = StepCtx {
            step: 7,
            pick: &pick,
            enabled: &[],
            phase: Phase::Live,
            now: at(2_000),
            status: zakura_scheduler::Status::Quiescent,
            log: &log,
        };
        let mut cx = CheckCtx::new(&step, &harness, (&registered[0], &registered), Some(&[]));
        assert_eq!((registered[0].check)(&mut cx), Verdict::Holds);
        let parts = cx.into_parts();
        let [obligation] = &parts.obliged[..] else {
            panic!("one obligation: {}", parts.obliged.len());
        };
        let facts = obligation.facts.as_ref().expect("a fact obligation");
        let answer = || (facts.answer)(&obligation.key, &harness.view(), ends.stamps().issued());

        adopt(&adoption, 3);
        assert_eq!(answer(), FactAnswer::Unrelated, "the baseline's tip");
        adopt_tip(&adoption, header(1, 4));
        assert_eq!(answer(), FactAnswer::Unrelated, "a fork tip");
        ends.record(EndFact::ReactorExited { step: 9 });
        assert_eq!(answer(), FactAnswer::Unrelated, "not an adoption");
        adopt(&adoption, 4);
        assert_eq!(answer(), FactAnswer::Discharges, "the successor");
    }
}
