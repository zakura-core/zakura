//! What a `header-sync/commit-only-authorized` finding names: the rule it
//! broke and the thing that broke it, beside its human detail.
//!
//! Each subject names only what its evidence holds. A finding about a
//! page names that page, and one about anything else, a round at its
//! `Started`, an update, a cursor, a storage call, a call's hold of a
//! round, a handoff, a session or an engine run, names that instead, so a
//! finding with no page never gets one.
//!
//! The work ledger's claim rules that the property reports name their
//! page, hold or handoff as the ledger's [`ClaimSubject`] gives it
//! ([`ledger_attribution`]).
//!
//! Peers are their digest and hashes their bytes in order, both as hex,
//! as the observation log and storage's facts write them, so a subject
//! joins with the run's other evidence. A finding's detail spells every
//! hash the same way, through the `spell_*` functions here, so a rendered
//! finding names each header once, however often it prints it.

use std::fmt;

use serde::Serialize;
use zakura_chain::block;
use zakura_header_chain::Frontier;
use zakura_network::zakura::{
    observe::{ClaimBreak, ClaimRule, ClaimSubject},
    ZakuraPeerId,
};

use super::Subscription;
use crate::harness::header_node::{
    claim::{Claim, ClaimRecord},
    tap::peer,
};
use crate::storage::hash;

/// The rule a finding broke.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuthorityRule {
    /// A round's owner isn't bound to its subscription's `Open`.
    OwnerBinding,
    /// A staged page isn't on its subscription's requested path.
    Path,
    /// The node admitted a session under another identity than its link's
    /// or transport row's.
    Provenance,
    /// A prepare or apply reached storage other than as dispatched.
    Submission,
    /// The subscriber's credit, an update, a claim or an acceptance broke
    /// its subscription's state.
    Credit,
    /// An engine run left the durable state other than its rules allow.
    Contents,
}

impl AuthorityRule {
    /// The rule's name as a rendered finding and the full report spell it.
    fn name(self) -> &'static str {
        match self {
            AuthorityRule::OwnerBinding => "owner-binding",
            AuthorityRule::Path => "path",
            AuthorityRule::Provenance => "provenance",
            AuthorityRule::Submission => "submission",
            AuthorityRule::Credit => "credit",
            AuthorityRule::Contents => "contents",
        }
    }
}

/// A built update's operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum UpdateKind {
    /// The subscription's `Open`.
    Open,
    /// A later `Grant`.
    Grant,
    /// A built `Close`.
    Close,
}

/// A cursor fact's kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CursorFact {
    /// A `CursorClaimed`, which records a page's cursor.
    Claim,
    /// A `CursorsAccepted`, which stores one.
    Acceptance,
}

/// The `Reset` fact an engine reset run was matched to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ResetFact {
    /// `invalidate` or `reconsider`.
    pub kind: String,
    /// The header it named.
    pub target: String,
    /// The engine's header generation after it, which no other reset of
    /// the run shares.
    pub generation: u64,
}

/// An engine run, as the controlled port recorded it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "run", rename_all = "kebab-case")]
pub enum EngineRunRef {
    /// Apply operation `op`'s run.
    Apply {
        /// The storage operation.
        op: u64,
    },
    /// Locator read `op`'s run.
    Locator {
        /// The storage operation.
        op: u64,
    },
    /// A harness commit straight into the engine, of `headers` in order.
    HarnessCommit {
        /// The committed headers' hashes.
        headers: Vec<String>,
    },
    /// An operator reset's run.
    Reset {
        /// The `Reset` fact it was matched to, or `None` if the drain had
        /// none left for it.
        fact: Option<ResetFact>,
    },
}

/// The thing a finding is about.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "subject", rename_all = "kebab-case")]
pub enum AuthoritySubject {
    /// A staged page: the subscription it was staged on, its headers in
    /// order, and the transport frame it came in, where a reader tagged
    /// one.
    Page {
        /// The subscription's peer.
        peer: String,
        /// The subscription's session.
        session: u64,
        /// The subscription's ID.
        subscription: u64,
        /// The page's header hashes, in order.
        headers: Vec<String>,
        /// The page's position among its stream's reads, if tagged.
        frame: Option<u64>,
    },
    /// A round at its `Started`, which can come before any page.
    Round {
        /// The round's incarnation.
        incarnation: u64,
        /// Its subscription's peer.
        peer: String,
        /// Its subscription's session.
        session: u64,
        /// Its subscription's ID.
        subscription: u64,
    },
    /// An update the subscriber built.
    Update {
        /// The subscription's peer.
        peer: String,
        /// The subscription's session.
        session: u64,
        /// The subscription's ID.
        subscription: u64,
        /// The update's operation.
        kind: UpdateKind,
        /// The update's sequence.
        sequence: u32,
    },
    /// A cursor claim or acceptance, which has no update sequence, by the
    /// cursor's header.
    Cursor {
        /// The subscription's peer.
        peer: String,
        /// The subscription's session.
        session: u64,
        /// The subscription's ID.
        subscription: u64,
        /// Whether it's a claim or an acceptance.
        fact: CursorFact,
        /// The cursor header's height.
        height: u32,
        /// The cursor header's hash.
        hash: String,
    },
    /// A storage call's hold of a round's headers, which a work-ledger
    /// claim names: the call, and the round the event names it taking or
    /// ending.
    Hold {
        /// The reactor's call number.
        call: u64,
        /// The round's incarnation.
        round: u64,
    },
    /// A prepare handing a round's headers to an apply, which a
    /// work-ledger claim names.
    Handoff {
        /// The prepare's call number.
        prepare: u64,
        /// The apply's call number.
        apply: u64,
        /// The round's incarnation.
        round: u64,
    },
    /// A prepare or apply call's submission.
    Call {
        /// The reactor's call number.
        call: u64,
        /// The storage operation it submitted, which a submission finding
        /// always has.
        op: u64,
    },
    /// An admitted session.
    Session {
        /// The session's peer.
        peer: String,
        /// The session's ID.
        session: u64,
        /// The connection its origin names, if it names one.
        connection: Option<u64>,
    },
    /// An engine run.
    Engine(EngineRunRef),
}

impl AuthoritySubject {
    /// The page staged on `subscription` with `hashes`, in `frame`.
    pub(crate) fn page(
        subscription: &Subscription,
        hashes: &[block::Hash],
        frame: Option<u64>,
    ) -> AuthoritySubject {
        let (peer_id, session, subscription) = subscription;
        AuthoritySubject::Page {
            peer: peer(peer_id),
            session: *session,
            subscription: *subscription,
            headers: hashes.iter().copied().map(hash).collect(),
            frame,
        }
    }

    /// Round `incarnation` on `subscription`.
    pub(crate) fn round(incarnation: u64, subscription: &Subscription) -> AuthoritySubject {
        let (peer_id, session, subscription) = subscription;
        AuthoritySubject::Round {
            incarnation,
            peer: peer(peer_id),
            session: *session,
            subscription: *subscription,
        }
    }

    /// `subscription`'s `kind` update at `sequence`.
    pub(crate) fn update(
        subscription: &Subscription,
        kind: UpdateKind,
        sequence: u32,
    ) -> AuthoritySubject {
        let (peer_id, session, subscription) = subscription;
        AuthoritySubject::Update {
            peer: peer(peer_id),
            session: *session,
            subscription: *subscription,
            kind,
            sequence,
        }
    }

    /// `subscription`'s `fact` through `through`.
    pub(crate) fn cursor(
        subscription: &Subscription,
        fact: CursorFact,
        through: Frontier,
    ) -> AuthoritySubject {
        let (peer_id, session, subscription) = subscription;
        AuthoritySubject::Cursor {
            peer: peer(peer_id),
            session: *session,
            subscription: *subscription,
            fact,
            height: through.height.0,
            hash: hash(through.hash),
        }
    }

    /// `peer_id`'s session `session` on `connection`.
    pub(crate) fn session(
        peer_id: &ZakuraPeerId,
        session: u64,
        connection: Option<u64>,
    ) -> AuthoritySubject {
        AuthoritySubject::Session {
            peer: peer(peer_id),
            session,
            connection,
        }
    }
}

/// `hash` as a finding spells it: its bytes in order, as hex.
pub(crate) fn spell_hash(hash: block::Hash) -> String {
    crate::storage::hash(hash)
}

/// `hashes` in order, as `[a, b]`, each as [`spell_hash`] spells it.
pub(crate) fn spell_hashes(hashes: &[block::Hash]) -> String {
    let hashes: Vec<String> = hashes.iter().copied().map(spell_hash).collect();
    format!("[{}]", hashes.join(", "))
}

/// `frontier` as `{height} {hash}`, as a cursor subject renders it.
pub(crate) fn spell_frontier(frontier: Frontier) -> String {
    format!("{} {}", frontier.height.0, spell_hash(frontier.hash))
}

/// [`spell_frontier`] of `frontier`, or `none`.
pub(crate) fn spell_maybe_frontier(frontier: Option<Frontier>) -> String {
    frontier.map_or_else(|| "none".to_owned(), spell_frontier)
}

/// A cursor at `through`, with `headers` headers and `bytes` bytes
/// consumed through it.
pub(crate) fn spell_cursor(through: Frontier, headers: u64, bytes: u64) -> String {
    format!(
        "{} with {headers} headers and {bytes} bytes",
        spell_frontier(through)
    )
}

/// `subscription` as a subject names its keys.
pub(crate) fn spell_subscription(subscription: &Subscription) -> String {
    let (peer_id, session, subscription) = subscription;
    format!(
        "peer {} session {session} subscription {subscription}",
        peer(peer_id)
    )
}

/// Writes `value`, or `none`.
fn or_none(value: Option<u64>) -> String {
    value.map_or_else(|| "none".to_owned(), |value| value.to_string())
}

impl fmt::Display for AuthoritySubject {
    /// Every key the subject holds, so two subjects that differ in any key
    /// render differently.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthoritySubject::Page {
                peer,
                session,
                subscription,
                headers,
                frame,
            } => write!(
                f,
                "page peer {peer} session {session} subscription {subscription} headers [{}] \
                 frame {}",
                headers.join(", "),
                or_none(*frame)
            ),
            AuthoritySubject::Round {
                incarnation,
                peer,
                session,
                subscription,
            } => write!(
                f,
                "round {incarnation} peer {peer} session {session} subscription {subscription}"
            ),
            AuthoritySubject::Update {
                peer,
                session,
                subscription,
                kind,
                sequence,
            } => write!(
                f,
                "update {kind:?} sequence {sequence} peer {peer} session {session} subscription \
                 {subscription}"
            ),
            AuthoritySubject::Cursor {
                peer,
                session,
                subscription,
                fact,
                height,
                hash,
            } => write!(
                f,
                "{} through {height} {hash} peer {peer} session {session} subscription \
                 {subscription}",
                match fact {
                    CursorFact::Claim => "claim",
                    CursorFact::Acceptance => "acceptance",
                }
            ),
            AuthoritySubject::Hold { call, round } => {
                write!(f, "call {call} hold of round {round}")
            }
            AuthoritySubject::Handoff {
                prepare,
                apply,
                round,
            } => write!(
                f,
                "handoff of round {round} from call {prepare} to call {apply}"
            ),
            AuthoritySubject::Call { call, op } => write!(f, "call {call} op {op}"),
            AuthoritySubject::Session {
                peer,
                session,
                connection,
            } => write!(
                f,
                "session {session} peer {peer} connection {}",
                or_none(*connection)
            ),
            AuthoritySubject::Engine(run) => match run {
                EngineRunRef::Apply { op } => write!(f, "engine apply op {op}"),
                EngineRunRef::Locator { op } => write!(f, "engine locator read op {op}"),
                EngineRunRef::HarnessCommit { headers } => {
                    write!(f, "engine harness commit headers [{}]", headers.join(", "))
                }
                EngineRunRef::Reset { fact: Some(fact) } => write!(
                    f,
                    "engine reset {} {} generation {}",
                    fact.kind, fact.target, fact.generation
                ),
                EngineRunRef::Reset { fact: None } => write!(f, "engine reset with no Reset fact"),
            },
        }
    }
}

/// The rule a work-ledger claim breaks, for the claim rules
/// `header-sync/commit-only-authorized` reports, or `None` for the
/// ledger's accounting rules, which other properties report.
///
/// A page from another session or subscription than its round's is the
/// page's provenance, as a supply from another source than its peer's
/// is. A page that left its round holding other headers is its path. A
/// page that overspent its reservation is credit. A call holding a round's
/// headers for another owner, round or lease, more than the round owns, or
/// a round that staged nothing, breaks the owner's binding. A prepare
/// sending other headers, a call that isn't the prepare or apply it names,
/// and a handoff to an apply for other work each break its submission.
fn ledger_rule(rule: ClaimRule) -> Option<AuthorityRule> {
    Some(match rule {
        ClaimRule::ForeignPage | ClaimRule::WrongSource => AuthorityRule::Provenance,
        ClaimRule::UnstagedPage => AuthorityRule::Path,
        ClaimRule::OverReservation => AuthorityRule::Credit,
        ClaimRule::WrongOwner
        | ClaimRule::WrongRound
        | ClaimRule::ForeignLease
        | ClaimRule::HeldUnowned
        | ClaimRule::UnstagedHeaders => AuthorityRule::OwnerBinding,
        ClaimRule::WrongPayload | ClaimRule::WrongCall | ClaimRule::BadHandoff => {
            AuthorityRule::Submission
        }
        ClaimRule::OverBudget
        | ClaimRule::LeakedReservation
        | ClaimRule::LeakedLease
        | ClaimRule::OpenOwnership
        | ClaimRule::OpenReservation
        | ClaimRule::UnreleasedObsoleteRound
        | ClaimRule::UnreleasedRound
        | ClaimRule::UnreleasedDepartedRound => return None,
    })
}

/// The ledger's `subject` as a finding names it.
fn ledger_subject(subject: &ClaimSubject) -> AuthoritySubject {
    match subject {
        ClaimSubject::Page {
            peer: peer_id,
            session_id,
            subscription,
            frame,
            hashes,
        } => AuthoritySubject::page(
            &(peer_id.clone(), *session_id, *subscription),
            hashes,
            *frame,
        ),
        ClaimSubject::Hold { call, incarnation } => AuthoritySubject::Hold {
            call: *call,
            round: *incarnation,
        },
        ClaimSubject::Handoff {
            prepare,
            apply,
            incarnation,
        } => AuthoritySubject::Handoff {
            prepare: *prepare,
            apply: *apply,
            round: *incarnation,
        },
    }
}

/// The attribution a claim record keeps for `claim`, a work-ledger break:
/// its rule ([`ledger_rule`]) and the subject the ledger named, for a
/// rule `header-sync/commit-only-authorized` reports, or `None` for the
/// ledger's other rules.
pub(crate) fn ledger_attribution(claim: &ClaimBreak) -> Option<Attribution> {
    let rule = ledger_rule(claim.rule)?;
    let subject = claim
        .subject
        .as_ref()
        .expect("the ledger names a subject for every rule the property reports");
    Some(Attribution {
        rule,
        subject: ledger_subject(subject),
    })
}

/// What a `CommitOnlyAuthorized` claim record names, for the full report.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Attribution {
    /// The rule it broke.
    pub rule: AuthorityRule,
    /// What it's about.
    pub subject: AuthoritySubject,
}

/// One broken `header-sync/commit-only-authorized` rule.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AuthorityFinding {
    /// Its rule and subject.
    pub attribution: Attribution,
    /// What broke, in words, without the rule or subject.
    pub detail: String,
}

impl AuthorityFinding {
    /// A break of `rule` by `subject`, as `detail` says.
    pub(crate) fn new(
        rule: AuthorityRule,
        subject: AuthoritySubject,
        detail: impl Into<String>,
    ) -> AuthorityFinding {
        AuthorityFinding {
            attribution: Attribution { rule, subject },
            detail: detail.into(),
        }
    }

    /// The claim record the drain of `step` keeps for it: its rendering as
    /// the detail, and its attribution.
    pub(crate) fn record(self, step: u64) -> ClaimRecord {
        ClaimRecord {
            step,
            claim: Claim::CommitOnlyAuthorized,
            detail: self.to_string(),
            attribution: Some(self.attribution),
        }
    }
}

impl fmt::Display for AuthorityFinding {
    /// `CommitOnlyAuthorized [rule] subject: detail`, which the property's
    /// violation carries.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CommitOnlyAuthorized [{}] {}: {}",
            self.attribution.rule.name(),
            self.attribution.subject,
            self.detail
        )
    }
}
