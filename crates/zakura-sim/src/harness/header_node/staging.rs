//! Staged header work as downstream effects (M6a-5a).
//!
//! The reactor reports `Staged` right after it appends a page's headers to
//! a work incarnation, with the frame the page came in and the digest of
//! every entry it then holds, and `Outcome` on each exit of its outcome
//! handler. The session reader digests each page's decoded entries before
//! any other code sees them, and the frame tag carries that digest.
//!
//! [`StagedWork`] turns those into the rows the downstream checks judge,
//! as the evidence hub writes each step, each keyed right after its
//! source:
//!
//! * `WorkAdmitted` for each `Staged`, naming its page frame, and for each
//!   outcome that retired or retried its matching incarnation, with
//!   `ends: true`. Its connection comes from the session's `Session` row.
//! * `UnframedWork` for held work no frame explains. The fold keeps its own
//!   expected digest per incarnation, empty at first and after each
//!   `EntriesTaken`, and extends it at each `Staged` by the frame's decoded
//!   digest alone, never from what the reactor reports. A `Staged` whose
//!   `after` differs from it, an `EntriesTaken` whose `chain` differs from
//!   it, or a settled snapshot whose held digest differs from it is held
//!   work no frame explains. The fold keeps its own value either way, so a
//!   later page can't absorb the difference.
//!
//! Only `header-sync/wire` has it: its frames come from tapped streams. A
//! staged page with no frame tag there, or one whose session no `Session`
//! row maps, is a harness failure.

use std::collections::BTreeMap;

use zakura_network::zakura::MessageRole;

use crate::{
    observe::{
        HeldDigest, ReactorFact, ReactorFrame, ReactorSnapshot, ReactorWorkState, RowAt, StreamId,
        UnframedCause,
    },
    peer::{rules, WireEvidence},
    Observation,
};

/// What the fold keeps for one started work incarnation.
#[derive(Debug)]
struct Work {
    /// The session its request runs on.
    session_id: u64,
    /// Its subscription, which its request ID is.
    subscription: u64,
    /// The digest its staged frames explain.
    expected: HeldDigest,
    /// The last held digest reported as unframed, so a settled snapshot
    /// that keeps it isn't reported again.
    reported: Option<HeldDigest>,
}

/// One tapped reactor's staged work.
#[derive(Debug, Default)]
struct Reactor {
    work: BTreeMap<u64, Work>,
    /// Its latest settled snapshot. The tap logs one only when it changed.
    snapshot: Option<ReactorSnapshot>,
}

/// The staged-work fold, per tapped reactor instance.
#[derive(Debug, Default)]
pub(crate) struct StagedWork {
    reactors: BTreeMap<u64, Reactor>,
}

/// A header stream's rows the derived records name.
struct Rows {
    /// The subscription's row.
    request: RowAt,
    /// Its pages' row.
    page: RowAt,
    /// Its terminal outcome's row.
    outcome: RowAt,
}

impl Rows {
    /// `stream`'s rows, from its table.
    fn of(stream: StreamId) -> Result<Self, String> {
        let rules = rules(stream)
            .ok_or_else(|| format!("UntabledStream: staged work on untabled {stream}"))?;
        let find = |found: &dyn Fn(&MessageRole) -> bool| {
            rules
                .iter()
                .find(|rule| found(&rule.role))
                .map(|rule| RowAt::new(stream, rule.name))
                .ok_or_else(|| format!("UntabledStream: {stream} has no subscription rows"))
        };
        Ok(Rows {
            request: find(&|role| matches!(role, MessageRole::Subscription { .. }))?,
            page: find(&|role| {
                matches!(
                    role,
                    MessageRole::Response {
                        ends_exchange: false,
                        ..
                    }
                )
            })?,
            outcome: find(&|role| {
                matches!(
                    role,
                    MessageRole::Response {
                        ends_exchange: true,
                        ..
                    }
                )
            })?,
        })
    }
}

impl StagedWork {
    /// Folds `obs`, a row the hub is writing in key order, and returns the
    /// rows it derives: `WorkAdmitted` and `UnframedWork`. `evidence` holds
    /// the identity map as the log has it so far.
    pub(crate) fn row(
        &mut self,
        obs: &Observation,
        evidence: &WireEvidence,
    ) -> Result<Vec<Observation>, String> {
        let Observation::Reactor {
            instance, event, ..
        } = obs
        else {
            return Ok(Vec::new());
        };
        let reactor = self.reactors.entry(*instance).or_default();
        let mut derived = Vec::new();
        match event {
            ReactorFact::Started { request, .. } => {
                reactor.work.insert(
                    request.incarnation,
                    Work {
                        session_id: request.session_id,
                        subscription: request.request_id,
                        expected: HeldDigest::default(),
                        reported: None,
                    },
                );
            }
            ReactorFact::Retired { request, .. } => {
                reactor.work.remove(&request.incarnation);
            }
            ReactorFact::Staged {
                incarnation,
                frame,
                count,
                after,
            } => {
                let work = reactor.work.get_mut(incarnation).ok_or_else(|| {
                    format!(
                        "UnknownIncarnation: incarnation {incarnation} staged before it started"
                    )
                })?;
                let frame = frame.ok_or_else(|| {
                    format!(
                        "UntaggedFrame: incarnation {incarnation} staged {count} headers from \
                         no tagged frame"
                    )
                })?;
                let (connection, rows) = joined(evidence, work.session_id, &frame)?;
                work.expected = work.expected.then(frame.received);
                derived.push(Observation::WorkAdmitted {
                    connection,
                    frame: rows.page,
                    seq: frame.seq,
                    request: rows.request.clone(),
                    exchange: work.subscription,
                    objects: *count,
                    bytes: frame.payload_len,
                    ends: false,
                });
                if *after != work.expected {
                    derived.push(unframed(connection, rows.request, work, *after));
                }
            }
            ReactorFact::EntriesTaken {
                incarnation, chain, ..
            } => {
                let Some(work) = reactor.work.get_mut(incarnation) else {
                    return Ok(derived);
                };
                if *chain != work.expected {
                    let (connection, request) = session_rows(evidence, work.session_id)?;
                    derived.push(unframed(connection, request, work, *chain));
                }
                work.expected = HeldDigest::default();
                work.reported = None;
            }
            ReactorFact::Settled { snapshot, .. } => {
                if let Some(snapshot) = snapshot {
                    reactor.snapshot = Some((**snapshot).clone());
                }
                let Some(snapshot) = &reactor.snapshot else {
                    return Ok(derived);
                };
                for entry in &snapshot.work {
                    let ReactorWorkState::Active {
                        incarnation,
                        held_digest,
                        ..
                    } = &entry.state
                    else {
                        continue;
                    };
                    let Some(work) = reactor.work.get_mut(incarnation) else {
                        continue;
                    };
                    if *held_digest == work.expected {
                        work.reported = None;
                    } else if work.reported != Some(*held_digest) {
                        let (connection, request) = session_rows(evidence, work.session_id)?;
                        derived.push(unframed(connection, request, work, *held_digest));
                    }
                }
            }
            ReactorFact::Outcome {
                session_id,
                subscription,
                frame,
                disposition,
                ..
            } if disposition == "retired" || disposition == "retried" => {
                let frame = frame.ok_or_else(|| {
                    format!(
                        "UntaggedFrame: subscription {subscription}'s outcome came in no tagged \
                         frame"
                    )
                })?;
                let (connection, rows) = joined(evidence, *session_id, &frame)?;
                derived.push(Observation::WorkAdmitted {
                    connection,
                    frame: rows.outcome,
                    seq: frame.seq,
                    request: rows.request,
                    exchange: *subscription,
                    objects: 0,
                    bytes: frame.payload_len,
                    ends: true,
                });
            }
            _ => {}
        }
        Ok(derived)
    }
}

/// Session `session_id`'s connection and stream rows, checking that
/// `frame` came on its QUIC stream.
fn joined(
    evidence: &WireEvidence,
    session_id: u64,
    frame: &ReactorFrame,
) -> Result<(u64, Rows), String> {
    let (connection, quic_stream, stream) = mapped(evidence, session_id)?;
    if frame.quic_stream != quic_stream {
        return Err(format!(
            "ForeignFrame: session {session_id} on QUIC stream {quic_stream} staged frame {} \
             of stream {}",
            frame.seq, frame.quic_stream
        ));
    }
    Ok((connection, Rows::of(stream)?))
}

/// Session `session_id`'s connection and subscription row.
fn session_rows(evidence: &WireEvidence, session_id: u64) -> Result<(u64, RowAt), String> {
    let (connection, _, stream) = mapped(evidence, session_id)?;
    Ok((connection, Rows::of(stream)?.request))
}

/// Session `session_id`'s connection, QUIC stream and stream.
fn mapped(evidence: &WireEvidence, session_id: u64) -> Result<(u64, u64, StreamId), String> {
    evidence.session(session_id).ok_or_else(|| {
        format!(
            "UnmappedSession: staged work names session {session_id}, which no Session row maps"
        )
    })
}

/// `UnframedWork` for `work`, whose entries digest to `held` where its
/// frames explain only its expected digest, and notes it reported.
fn unframed(connection: u64, request: RowAt, work: &mut Work, held: HeldDigest) -> Observation {
    work.reported = Some(held);
    let (cause, objects) = if held.len > work.expected.len {
        (UnframedCause::Extra, held.len - work.expected.len)
    } else {
        (UnframedCause::Substituted, held.len)
    };
    Observation::UnframedWork {
        connection,
        request,
        exchange: work.subscription,
        objects,
        bytes: 0,
        cause,
    }
}
