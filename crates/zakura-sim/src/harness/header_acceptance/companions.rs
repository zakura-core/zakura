//! The despite-close case's companions (hdr-14 item 2, gap 6).
//!
//! The planted case [`DESPITE_CLOSE`] keeps the subscriber's content fault,
//! so the node still closes the peer, but forwards the rejected page to the
//! reactor anyway, so it stages. A correct close mustn't excuse that.
//! `commit-only-authorized` owns several claim kinds, so its `Caught` alone
//! could come from another one. Its row names three companions, each judged
//! on the case's own `Draft` run, and each joined to the rejected page by
//! its full identity, a Rule 25 ref ([`super::refs`]):
//!
//! - [`FAULT_CLOSE`]: the node closed the rejected frame's connection with a
//!   cause in [`FAULT_CAUSES`](crate::model::FAULT_CAUSES), after it judged
//!   that frame ([`FrameRef`]), and the session's reader refused a frame for
//!   the page's content fault, [`CONTENT_FAULT`] ([`SessionRef`]: the peer
//!   and the session), so a close for another fault, or another peer's
//!   refusal, doesn't count.
//! - [`STAGED`]: exactly one `PageStaged` came from that frame, as the
//!   session's reader names it ([`ReadRef`]: the session the frame's
//!   connection and QUIC stream carry, and the frame's position among the
//!   stream's reads), and it's the page the peer sent ([`PageRef`]: the
//!   frame's peer, session and subscription, the frame and the hashes the
//!   peer sent). Subscription IDs are per session, so the subscription
//!   alone doesn't name a page.
//! - [`PATH_BREAK`]: the retained `commit-only-authorized` violation is a
//!   `path` break whose subject is that page, its [`PageRef`], which C22
//!   adds to each page finding, and it's the violation the property
//!   reported for that record ([`FindingRef`]).
//!
//! The rejected page is found from the run's own log: the one `headers`
//! frame the peer's models labelled `BadContent` that the node judged
//! ([`rejected_page`]). Its peer and hashes come from the frame's own
//! evidence, apart from anything the node staged ([`sent_page`]): the peer
//! its connection's `Session` row names, and the hashes the peer put in the
//! frame, so a substituted page satisfies neither companion.

use serde_json::Value;
use zakura_network::zakura::observe::ReactorEvent;

use super::{
    binding::{self, Expect, Field},
    refs::{
        joined, FindingRef, FrameRef, PageRef, ReadRef, SessionRef, StreamRef, SubscriptionRef,
    },
    CaseRef,
};
use crate::{
    harness::header_node::authority::{Attribution, AuthorityRule, AuthoritySubject},
    model::{is_fault_cause, Label},
    peer::SentPages,
    Observation, PropertyOutcome, PropertyReport,
};

/// `header-sync/commit-only-authorized`.
const COMMIT: &str = "header-sync/commit-only-authorized";

/// The planted case that stages a page despite closing its connection.
pub const DESPITE_CLOSE: CaseRef = CaseRef {
    property: COMMIT,
    name: "staged-despite-close",
};

/// The companion that the rejected frame's connection closed with a fault.
pub const FAULT_CLOSE: &str = "rejected-page-fault-close";
/// The companion that the rejected frame's page staged.
pub const STAGED: &str = "rejected-page-staged";
/// The companion that the catch is the staged page's path break.
pub const PATH_BREAK: &str = "rejected-page-path-break";
/// The case's companions, in the order its row names them.
pub const COMPANIONS: &[&str] = &[FAULT_CLOSE, STAGED, PATH_BREAK];

/// The subscriber's fault for the case's unlinked page, as the reader names
/// it when it refuses the frame.
pub const CONTENT_FAULT: &str = "linkage";

/// A frame a session's reader refused as a protocol violation, as its
/// `FrameRefused` reports it, with the peer in hex as findings spell it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal {
    /// The peer.
    pub peer: String,
    /// The session the reader read it on.
    pub session: u64,
    /// The fault, such as `linkage`.
    pub reason: String,
}

impl Refusal {
    /// The session whose reader refused it.
    pub fn session(&self) -> SessionRef {
        SessionRef {
            peer: self.peer.clone(),
            session: self.session,
        }
    }

    /// The refusal `event` reports, if it's a `FrameRefused`.
    pub fn of(event: &ReactorEvent) -> Option<Refusal> {
        let ReactorEvent::FrameRefused {
            peer,
            session_id,
            reason,
        } = event
        else {
            return None;
        };
        Some(Refusal {
            peer: crate::harness::header_node::tap::peer(peer),
            session: *session_id,
            reason: (*reason).to_owned(),
        })
    }
}

/// The rejected page's frame, as the run's log names it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RejectedPage {
    /// The frame: its connection, stream, direction and its position among
    /// the peer's frames on the stream, which is its position among the
    /// stream's reads.
    pub frame: FrameRef,
    /// The node's session for that connection and stream.
    pub session: u64,
    /// The subscription it names.
    pub subscription: u64,
    /// The peer its connection's `Session` row names, by harness name.
    pub peer: String,
}

impl RejectedPage {
    /// The frame as the session's reader names it.
    pub fn read(&self) -> ReadRef {
        ReadRef {
            session: self.session,
            frame: self.frame.seq,
        }
    }

    /// The page the peer sent in this frame, `sent`, on its subscription.
    pub fn page(&self, sent: &SentPage) -> PageRef {
        PageRef {
            subscription: SubscriptionRef {
                peer: sent.peer.clone(),
                session: self.session,
                subscription: self.subscription,
            },
            frame: self.frame.seq,
            headers: sent.hashes.clone(),
        }
    }
}

/// What the peer sent as the rejected page, from the frame's evidence: its
/// peer and its header hashes, in hex as findings spell them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SentPage {
    /// The peer.
    pub peer: String,
    /// The page's header hashes, in order.
    pub hashes: Vec<String>,
}

/// A `PageStaged` fact, with peers and hashes in hex as findings spell
/// them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedPage {
    /// The peer it came from.
    pub peer: String,
    /// The session it came on.
    pub session: u64,
    /// Its subscription.
    pub subscription: u64,
    /// Its position among its stream's reads, if a reader tagged it.
    pub frame: Option<u64>,
    /// Its header hashes, in order.
    pub hashes: Vec<String>,
}

impl StagedPage {
    /// The frame it came from, as its session's reader names it, if a
    /// reader tagged it.
    pub fn read(&self) -> Option<ReadRef> {
        Some(ReadRef {
            session: self.session,
            frame: self.frame?,
        })
    }

    /// The page it staged, if a reader tagged its frame.
    pub fn page(&self) -> Option<PageRef> {
        Some(PageRef {
            subscription: SubscriptionRef {
                peer: self.peer.clone(),
                session: self.session,
                subscription: self.subscription,
            },
            frame: self.frame?,
            headers: self.hashes.clone(),
        })
    }

    /// The staged page `event` reports, if it's a `PageStaged`.
    pub fn of(event: &ReactorEvent) -> Option<StagedPage> {
        let ReactorEvent::PageStaged {
            from_peer,
            from_session,
            subscription,
            frame,
            hashes,
            ..
        } = event
        else {
            return None;
        };
        Some(StagedPage {
            peer: crate::harness::header_node::tap::peer(from_peer),
            session: *from_session,
            subscription: *subscription,
            frame: *frame,
            hashes: hashes.iter().copied().map(crate::storage::hash).collect(),
        })
    }
}

/// The frame `obs` says the node judged, if it's a `FrameJudged` on a QUIC
/// stream: a frame the peer sent.
pub fn judged_frame(obs: &Observation) -> Option<FrameRef> {
    match obs {
        Observation::FrameJudged {
            connection,
            stream,
            quic_stream: Some(quic_stream),
            seq,
        } => Some(FrameRef {
            connection: *connection,
            kind: stream.kind,
            version: stream.version,
            quic_stream: *quic_stream,
            inbound: true,
            seq: *seq,
        }),
        _ => None,
    }
}

/// The rejected page in `observations`: the one `headers` frame the peer's
/// models labelled `BadContent` and the node judged, with the session its
/// connection and QUIC stream carry. An error says why there's no single
/// one.
pub fn rejected_page(observations: &[Observation]) -> Result<RejectedPage, String> {
    let judged = |frame: &FrameRef| !joined(observations, judged_frame, frame).is_empty();
    let frames: Vec<(FrameRef, Option<u64>)> = observations
        .iter()
        .filter_map(|obs| match obs {
            Observation::PeerFrame {
                connection,
                stream,
                quic_stream: Some(quic_stream),
                seq,
                row: Some(row),
                label: Label::BadContent,
                exchange,
                ..
            } if row == "headers" => Some((
                FrameRef {
                    connection: *connection,
                    kind: stream.kind,
                    version: stream.version,
                    quic_stream: *quic_stream,
                    inbound: true,
                    seq: *seq,
                },
                *exchange,
            )),
            _ => None,
        })
        .filter(|(frame, _)| judged(frame))
        .collect();
    let [(frame, exchange)] = &frames[..] else {
        return Err(format!(
            "{} judged bad-content pages, not one: {frames:?}",
            frames.len()
        ));
    };
    let subscription =
        exchange.ok_or_else(|| format!("the rejected page {} names no subscription", frame.seq))?;
    // The session row names the stream its session reads.
    let stream = StreamRef::from(frame);
    let sessions = joined(observations, session_stream, &stream);
    let [Observation::Session {
        session_id, peer, ..
    }] = sessions[..]
    else {
        return Err(format!("{stream:?} carries sessions {sessions:?}, not one"));
    };
    Ok(RejectedPage {
        frame: frame.clone(),
        session: *session_id,
        subscription,
        peer: peer.clone(),
    })
}

/// The stream `obs` says its session reads, if it's a `Session` row.
fn session_stream(obs: &Observation) -> Option<StreamRef> {
    match obs {
        Observation::Session {
            connection,
            quic_stream,
            kind,
            version,
            ..
        } => Some(StreamRef {
            connection: *connection,
            kind: *kind,
            version: *version,
            quic_stream: *quic_stream,
        }),
        _ => None,
    }
}

/// The peer `page` came from, as `peers` names it for its harness name, by
/// `(name, peer)`, in hex as findings spell it.
fn page_peer(page: &RejectedPage, peers: &[(String, String)]) -> Result<String, String> {
    peers
        .iter()
        .find(|(name, _)| *name == page.peer)
        .map(|(_, peer)| peer.clone())
        .ok_or_else(|| format!("no peer is named {}", page.peer))
}

/// What the peer sent as `page`: its peer ([`page_peer`]) and the header
/// hashes `sent` holds for its frame. An error says which is missing.
pub fn sent_page(
    page: &RejectedPage,
    peers: &[(String, String)],
    sent: &SentPages,
) -> Result<SentPage, String> {
    let peer = page_peer(page, peers)?;
    let hashes = sent
        .page(&page.frame)
        .ok_or_else(|| format!("the peer sent no page as {page:?}"))?;
    Ok(SentPage { peer, hashes })
}

/// [`FAULT_CLOSE`]: the node's first close of `page`'s connection comes
/// after it judged `page`'s frame, with a fault cause, and `refusals` hold
/// exactly one of `page`'s session ([`page_peer`], from `peers`, and its
/// session), for [`CONTENT_FAULT`]. A reader ends its session at its first
/// refusal, so that one is the close's.
pub fn fault_close(
    observations: &[Observation],
    refusals: &[Refusal],
    page: &RejectedPage,
    peers: &[(String, String)],
) -> Result<(), String> {
    let session = SessionRef {
        peer: page_peer(page, peers)?,
        session: page.session,
    };
    let judged = observations
        .iter()
        .position(|obs| judged_frame(obs).as_ref() == Some(&page.frame))
        .ok_or_else(|| format!("the node never judged {page:?}"))?;
    // A close names only its connection, which is the whole of its
    // identity.
    let connection = page.frame.connection;
    let close = observations
        .iter()
        .enumerate()
        .find_map(|(at, obs)| match obs {
            Observation::ConnectionClosed {
                connection: c,
                cause,
            } if *c == connection => Some((at, cause)),
            _ => None,
        });
    match close {
        None => Err(format!("connection {connection} never closed")),
        Some((at, cause)) if at < judged => Err(format!(
            "connection {connection} closed with {cause} before the node judged frame {}",
            page.frame.seq
        )),
        Some((_, cause)) if !is_fault_cause(cause) => Err(format!(
            "connection {connection} closed with {cause}, not a fault cause"
        )),
        Some(_) => {
            let of_session = joined(refusals, |refusal| Some(refusal.session()), &session);
            match of_session[..] {
                [refusal] if refusal.reason == CONTENT_FAULT => Ok(()),
                _ => Err(format!(
                    "{session:?}'s reader refused {of_session:?}, not one frame for \
                     {CONTENT_FAULT}"
                )),
            }
        }
    }
}

/// [`STAGED`]: exactly one of `staged` came from `page`'s frame, as its
/// session's reader names it ([`ReadRef`]), and it's the page the peer
/// sent there, `sent`, on `page`'s subscription ([`PageRef`]), with headers.
pub fn staged_page(
    staged: &[StagedPage],
    page: &RejectedPage,
    sent: &SentPage,
) -> Result<(), String> {
    let want = page.page(sent);
    let from = joined(staged, StagedPage::read, &page.read());
    match from[..] {
        [fact] if fact.hashes.is_empty() => {
            Err(format!("the rejected page staged no header: {fact:?}"))
        }
        [fact] if fact.page().as_ref() != Some(&want) => Err(format!(
            "the page staged from the rejected frame is {fact:?}, not the peer's {want:?}"
        )),
        [_] => Ok(()),
        _ => Err(format!(
            "{} pages staged from {:?}, not one",
            from.len(),
            page.read()
        )),
    }
}

/// [`PATH_BREAK`]: `commit-only-authorized` is violated in `properties`,
/// and one of its violations is the claim record in `claims`, the claim
/// store's snapshot, whose attribution is a `path` break of `page` as the
/// peer sent it, `sent`. A violation is a record's when it's at the
/// record's step and its `actual` is the record's detail, which is how the
/// property reports a break ([`FindingRef`]), so the same detail retained
/// at another boundary doesn't stand in for a break dropped at its own.
pub fn path_break(
    properties: &[PropertyReport],
    claims: &Value,
    page: &RejectedPage,
    sent: &SentPage,
) -> Result<(), String> {
    let report = properties
        .iter()
        .find(|report| report.property == COMMIT)
        .ok_or_else(|| format!("{COMMIT} isn't reported"))?;
    if !matches!(report.outcome, PropertyOutcome::Violated { .. }) {
        return Err(format!("{COMMIT} is {:?}", report.outcome));
    }
    let subject = page.page(sent);
    let expected = serde_json::to_value(Attribution {
        rule: AuthorityRule::Path,
        subject: AuthoritySubject::Page {
            peer: subject.subscription.peer,
            session: subject.subscription.session,
            subscription: subject.subscription.subscription,
            headers: subject.headers,
            frame: Some(subject.frame),
        },
    })
    .map_err(|error| format!("the expected attribution doesn't serialize: {error}"))?;
    let records = claims
        .as_array()
        .ok_or("the claim store's snapshot isn't a list")?;
    let reported = |violation: &crate::Violation| {
        Some(FindingRef {
            step: violation.step,
            detail: violation.actual.clone(),
        })
    };
    let found = records.iter().any(|record| {
        let finding = record["step"].as_u64().zip(record["detail"].as_str());
        record["claim"] == "CommitOnlyAuthorized"
            && record["attribution"] == expected
            && finding.is_some_and(|(step, detail)| {
                let finding = FindingRef {
                    step,
                    detail: detail.to_owned(),
                };
                !joined(&report.violations, reported, &finding).is_empty()
            })
    });
    if found {
        Ok(())
    } else {
        Err(format!(
            "no retained {COMMIT} violation is the path break {expected}"
        ))
    }
}

/// What a `Draft` run of the case leaves to judge: its log, the node's
/// staged pages, its property reports and its claim store's snapshot.
#[derive(Debug)]
pub struct DraftEvidence {
    /// Every observation, in order.
    pub observations: Vec<Observation>,
    /// Every `PageStaged` the node's work queue reported, in order.
    pub staged: Vec<StagedPage>,
    /// Every frame the node's session readers refused, in order.
    pub refusals: Vec<Refusal>,
    /// Each scenario peer's harness name and its peer in hex, by the
    /// identity the harness gave it.
    pub peers: Vec<(String, String)>,
    /// The header hashes of each page the peers sent.
    pub sent: SentPages,
    /// Each property's report.
    pub properties: Vec<PropertyReport>,
    /// The claim store's snapshot, `Null` if the run kept none.
    pub claims: Value,
}

/// Each companion's name with its verdict: `Ok` if it held, else why not.
pub type Verdicts = Vec<(&'static str, Result<(), String>)>;

/// Judges each of [`COMPANIONS`] on `evidence`, in order: `Ok` if it held,
/// else why not. Each finds the rejected page itself, so a run with no
/// single one fails all three.
pub fn judge(evidence: &DraftEvidence) -> Verdicts {
    let page = rejected_page(&evidence.observations);
    let sent = page
        .as_ref()
        .map_err(Clone::clone)
        .and_then(|page| sent_page(page, &evidence.peers, &evidence.sent).map(|sent| (page, sent)));
    vec![
        (
            FAULT_CLOSE,
            page.as_ref().map_err(Clone::clone).and_then(|page| {
                fault_close(
                    &evidence.observations,
                    &evidence.refusals,
                    page,
                    &evidence.peers,
                )
            }),
        ),
        (
            STAGED,
            sent.clone()
                .and_then(|(page, sent)| staged_page(&evidence.staged, page, &sent)),
        ),
        (
            PATH_BREAK,
            sent.and_then(|(page, sent)| {
                path_break(&evidence.properties, &evidence.claims, page, &sent)
            }),
        ),
    ]
}

/// A sink that keeps only the run's full report.
#[derive(Default)]
struct KeepReport(Option<crate::record::FullReport>);

impl crate::record::RecordSink for KeepReport {
    /// Drops the record.
    fn record(&mut self, _: crate::record::Record) {}

    /// Keeps the report.
    fn report(&mut self, report: &crate::record::FullReport) {
        self.0 = Some(report.clone());
    }
}

/// Runs the fuzz input `input` on `header-sync/wire` with
/// `commit-only-authorized` as `Draft`, as the case's `Draft` run does, and
/// returns what [`judge`] reads. An error says why the run gave none.
pub fn run_draft(input: &[u8]) -> Result<DraftEvidence, String> {
    use crate::harness::{header_sync::HeaderWire, Subsystem};
    let entry = crate::harness::find(HeaderWire::NAME).map_err(|error| error.to_string())?;
    let plan = crate::Plan::from_fuzz(&input[entry.scenario_bytes.min(input.len())..]);
    draft_run(&entry.scenario(input), plan).map(|(evidence, _)| evidence)
}

/// Judges each of [`COMPANIONS`] on the case's recorded `Draft` run in
/// `dir` (C9), as [`judge`] does. The trace must be `header-sync/wire`'s
/// with `commit-only-authorized` as its whole `Draft` override. It's rerun
/// here following the trace's own picks and draws ([`binding::TRACE_META`]),
/// and the rerun's full report, every violation and the stores' snapshot,
/// must equal the one recorded beside the trace, which must report that
/// trace by its recording build ([`binding::FULL_REPORT`]), so what's
/// judged is that run. So this must
/// run in the build that recorded the trace, the case's own patched one,
/// where a patched behavior replays. An error says why there's no verdict.
pub fn judge_trace(dir: &std::path::Path) -> Result<Verdicts, String> {
    use crate::harness::{header_sync::HeaderWire, Subsystem};
    let trace = binding::open_trace(dir)?;
    let expect = Expect::new()
        .with(Field::Harness, HeaderWire::NAME)
        .with(Field::Draft, [COMMIT]);
    let meta = serde_json::to_value(&trace.meta).map_err(|error| error.to_string())?;
    binding::check(&binding::TRACE_META, &meta, &expect)?;
    let script = crate::plan::TraceScript::new(&trace, false).map_err(|error| error.to_string())?;
    let (evidence, rerun) = draft_run(&trace.meta.scenario, crate::PlanInput::Trace(script))?;
    // Bound as `record` bound the recorded one, so it's equal only to a
    // report of this trace by this build.
    let rerun = rerun.map(|report| report.of_trace(&trace.id));
    let reported = Expect::new()
        .with(Field::Trace, &trace.id)
        .with(Field::Source, &trace.meta.build.source);
    let recorded = binding::read_bounded(
        &binding::FULL_REPORT,
        &dir.join(crate::record::FULL_REPORT_FILE),
        crate::trace::MAX_REPORT_BYTES,
        &reported,
    )
    .and_then(|report| {
        serde_json::from_value::<crate::record::FullReport>(report)
            .map_err(|error| format!("its full report: {error}"))
    })
    .map_err(|why| format!("its full report: {why}"))?;
    if rerun.as_ref() != Some(&recorded) {
        return Err("the rerun's full report isn't the recorded one".to_owned());
    }
    Ok(judge(&evidence))
}

/// Runs `scenario` on `header-sync/wire` under `plan` with
/// `commit-only-authorized` as `Draft`, and returns what [`judge`] reads
/// with the run's full report.
fn draft_run(
    scenario: &Value,
    plan: crate::PlanInput,
) -> Result<(DraftEvidence, Option<crate::record::FullReport>), String> {
    use crate::harness::{
        header_sync::{HeaderWire, WireScenario},
        run_prepared, with_draft, HarnessError, Subsystem,
    };
    let scenario: WireScenario =
        serde_json::from_value(scenario.clone()).map_err(|error| error.to_string())?;
    let ports = HeaderWire::prepare(&scenario);
    let reservations = ports.reservations.clone();
    let sent = ports.sent_pages.clone();
    let peers = (0..scenario.peers.len())
        .map(|index| {
            (
                format!("peer-{index}"),
                crate::harness::header_node::tap::peer(&crate::peer::peer_id(
                    scenario.net.seed,
                    index,
                )),
            )
        })
        .collect();
    let mut sink = KeepReport::default();
    let report = with_draft(&[COMMIT.to_owned()], || {
        match run_prepared::<HeaderWire>(&scenario, ports, plan, &mut sink, None) {
            Ok(report) => Ok(report),
            Err(HarnessError::Run { report, .. }) => Ok(*report),
            Err(error) => Err(error.to_string()),
        }
    })?;
    let claims = sink
        .0
        .as_ref()
        .and_then(|full| full.snapshot.as_ref())
        .and_then(|snapshot| {
            snapshot.as_array().and_then(|stores| {
                stores
                    .iter()
                    .find(|store| store["store"] == "claims")
                    .map(|store| store["snapshot"].clone())
            })
        })
        .unwrap_or(Value::Null);
    let evidence = DraftEvidence {
        observations: report
            .log
            .entries()
            .iter()
            .map(|entry| entry.obs.clone())
            .collect(),
        staged: reservations
            .facts()
            .iter()
            .filter_map(StagedPage::of)
            .collect(),
        refusals: reservations
            .facts()
            .iter()
            .filter_map(Refusal::of)
            .collect(),
        peers,
        sent,
        properties: report.properties,
        claims,
    };
    Ok((evidence, sink.0))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{
        harness::header_acceptance::refs::one_key_off::assert_one_key_off, observe::StreamId,
        Violation,
    };

    /// The header-sync stream.
    const STREAM: StreamId = StreamId {
        kind: 5,
        version: 9,
    };

    /// The rejected page's frame on `connection`, with its session's row,
    /// its judgment and the node's close of `closed` with `cause`.
    fn log(connection: u64, closed: u64, cause: &str) -> Vec<Observation> {
        vec![
            Observation::Session {
                connection,
                peer: "peer-0".into(),
                quic_stream: 4,
                kind: 5,
                version: 9,
                session_id: 11,
            },
            Observation::PeerFrame {
                connection,
                stream: STREAM,
                quic_stream: Some(4),
                seq: 3,
                row: Some("headers".into()),
                rule_stream: STREAM,
                label: Label::BadContent,
                exhausted: false,
                exchange: Some(7),
                payload: 444,
            },
            Observation::FrameJudged {
                connection,
                stream: STREAM,
                quic_stream: Some(4),
                seq: 3,
            },
            Observation::ConnectionClosed {
                connection: closed,
                cause: cause.into(),
            },
        ]
    }

    /// The page staged from session `session` subscription 7 at `frame`.
    fn staged(session: u64, frame: u64) -> StagedPage {
        StagedPage {
            peer: "ab".into(),
            session,
            subscription: 7,
            frame: Some(frame),
            hashes: vec!["01".into(), "02".into()],
        }
    }

    /// The claim store holding one `rule` break of `subject`'s page, which
    /// `commit-only-authorized`'s one violation renders.
    fn claims(rule: &str, frame: u64) -> (Value, Vec<PropertyReport>) {
        let detail = "CommitOnlyAuthorized [path] the page";
        let claims = json!([{
            "step": 9,
            "claim": "CommitOnlyAuthorized",
            "detail": detail,
            "reported": true,
            "attribution": {
                "rule": rule,
                "subject": {
                    "subject": "page",
                    "peer": "ab",
                    "session": 11,
                    "subscription": 7,
                    "headers": ["01", "02"],
                    "frame": frame,
                },
            },
        }]);
        let report = PropertyReport {
            violations: vec![Violation {
                subject: None,
                property: COMMIT.into(),
                step: 9,
                witness: vec![9],
                expected: "authorized".into(),
                actual: detail.into(),
            }],
            ..PropertyReport::new(COMMIT, PropertyOutcome::Violated { witness: vec![9] })
        };
        (claims, vec![report])
    }

    /// The reader's refusal of session 11's frame for `reason`.
    fn refused(reason: &str) -> Vec<Refusal> {
        vec![Refusal {
            peer: "ab".into(),
            session: 11,
            reason: reason.into(),
        }]
    }

    /// The case's committed input recorded as its `Draft` run, unpatched,
    /// into a fresh directory.
    fn draft_recording() -> tempfile::TempDir {
        let input = std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/header-wire-bad-content.bin"),
        )
        .expect("the committed input is readable");
        let dir = tempfile::tempdir().expect("a temp dir can be created");
        crate::harness::with_draft(&[COMMIT.to_owned()], || {
            crate::harness::record(
                crate::harness::find("header-sync/wire").expect("the wire harness is registered"),
                crate::harness::RecordInput::Input(&input),
                dir.path(),
            )
        })
        .expect("the Draft run records");
        dir
    }

    /// The trace judge reruns a recorded `Draft` run and judges the same
    /// evidence the in-process run gives: unpatched, the subscriber refuses
    /// the page and closes its connection, so only the fault close holds.
    /// It refuses a
    /// trace whose recorded full report isn't the rerun's, and one recorded
    /// without the case's override.
    #[test]
    fn the_trace_judge_judges_the_recorded_draft_run() {
        let dir = draft_recording();
        let verdicts = judge_trace(dir.path()).expect("the recorded Draft run reruns");
        let names: Vec<&str> = verdicts.iter().map(|(name, _)| *name).collect();
        assert_eq!(names, COMPANIONS);
        let input = std::fs::read(dir.path().join(crate::trace::INPUT_FILE))
            .expect("record keeps the input");
        let direct = judge(&run_draft(&input).expect("the input runs"));
        assert_eq!(verdicts, direct);
        // The unpatched node closes the rejected page's connection with its
        // fault, and stages nothing.
        assert!(
            verdicts[0].1.is_ok() && verdicts[1].1.is_err(),
            "{verdicts:?}"
        );

        let path = dir.path().join(crate::record::FULL_REPORT_FILE);
        let mut report: crate::record::FullReport =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("it exists"))
                .expect("it reads");
        report.snapshot = Some(json!([]));
        std::fs::write(&path, report.to_line()).expect("the temp dir is writable");
        assert!(judge_trace(dir.path()).is_err());

        let plain = tempfile::tempdir().expect("a temp dir can be created");
        crate::harness::record(
            crate::harness::find("header-sync/wire").expect("the wire harness is registered"),
            crate::harness::RecordInput::Input(&input),
            plain.path(),
        )
        .expect("the run records");
        assert!(judge_trace(plain.path()).is_err());
    }

    /// The trace judge reads the recorded full report only as a report of
    /// that trace by the build that recorded it ([`binding::FULL_REPORT`]):
    /// a trace whose meta names another build, beside a report renamed to
    /// that trace, which the rerun by this build would equal, is refused.
    #[test]
    fn the_trace_judge_binds_the_recorded_report() {
        let dir = draft_recording();
        assert!(judge_trace(dir.path()).is_ok());
        let path = dir.path().join("trace.jsonl");
        let text = std::fs::read_to_string(&path).expect("the trace is readable");
        let (first, rest) = text.split_once('\n').expect("the trace has lines");
        let mut meta: Value = serde_json::from_str(first).expect("its meta is JSON");
        meta["meta"]["build"]["source"] = json!("another build");
        std::fs::write(&path, format!("{meta}\n{rest}")).expect("the temp dir is writable");
        let id = crate::trace::TraceReader::open(dir.path())
            .expect("the trace reads")
            .id;
        let report = dir.path().join(crate::record::FULL_REPORT_FILE);
        let mut renamed: Value =
            serde_json::from_str(&std::fs::read_to_string(&report).expect("it exists"))
                .expect("it reads");
        renamed["trace"] = json!(id);
        std::fs::write(&report, renamed.to_string()).expect("the temp dir is writable");
        let refused = judge_trace(dir.path());
        assert!(
            refused
                .as_ref()
                .is_err_and(|why| why.contains("was built from")),
            "{refused:?}"
        );
    }

    /// The fault-close companion joins the rejected frame and its content
    /// refusal: a fault close there after the session's reader refused a
    /// frame for linkage holds. Each key of the frame or of the refusing
    /// session moved on its own fails (Rule 25's template), and so do the
    /// same close of another connection, a close there with a cause that
    /// isn't a fault, a close after a refusal for another fault, or none.
    #[test]
    fn the_fault_close_joins_the_rejected_frames_connection() {
        let linkage = refused(CONTENT_FAULT);
        let peers = vec![("peer-0".to_owned(), "ab".to_owned())];
        let fault_close = |log: &[Observation], refusals: &[Refusal], page: &RejectedPage| {
            fault_close(log, refusals, page, &peers)
        };
        let page = rejected_page(&log(1, 1, "service_protocol_reject")).unwrap();
        assert_eq!(
            page,
            RejectedPage {
                frame: FrameRef {
                    connection: 1,
                    kind: 5,
                    version: 9,
                    quic_stream: 4,
                    inbound: true,
                    seq: 3,
                },
                session: 11,
                subscription: 7,
                peer: "peer-0".into(),
            }
        );
        let closed = log(1, 1, "service_protocol_reject");
        assert_eq!(fault_close(&closed, &linkage, &page), Ok(()));
        assert_one_key_off(&page.frame, |frame| {
            let page = RejectedPage {
                frame: frame.clone(),
                ..page.clone()
            };
            fault_close(&closed, &linkage, &page).is_ok()
        });
        assert_one_key_off(&linkage[0].session(), |session| {
            let refusals = [Refusal {
                peer: session.peer.clone(),
                session: session.session,
                reason: CONTENT_FAULT.into(),
            }];
            fault_close(&closed, &refusals, &page).is_ok()
        });
        assert!(fault_close(&log(1, 2, "service_protocol_reject"), &linkage, &page).is_err());
        assert!(fault_close(&log(1, 1, "service_local_shutdown"), &linkage, &page).is_err());
        // Another fault's refusal closes the same connection.
        assert!(fault_close(&closed, &refused("over_credit"), &page).is_err());
        assert!(fault_close(&closed, &[], &page).is_err());
        // A page whose peer no harness name resolves has no refusal.
        assert!(super::fault_close(&closed, &linkage, &page, &[]).is_err());
    }

    /// What peer `ab` sent as the rejected page: headers `01` and `02`.
    fn sent() -> SentPage {
        SentPage {
            peer: "ab".into(),
            hashes: vec!["01".into(), "02".into()],
        }
    }

    /// The rejected page `page` names, from `peer-0`'s frame 3 on connection
    /// 1, and what its peer sent there.
    fn page_of(page: &PageRef) -> (RejectedPage, SentPage) {
        let mut rejected = rejected_page(&log(1, 1, "service_protocol_reject")).unwrap();
        rejected.frame.seq = page.frame;
        rejected.session = page.subscription.session;
        rejected.subscription = page.subscription.subscription;
        let sent = SentPage {
            peer: page.subscription.peer.clone(),
            hashes: page.headers.clone(),
        };
        (rejected, sent)
    }

    /// The fact staging `page`.
    fn staged_from(page: &PageRef) -> StagedPage {
        StagedPage {
            peer: page.subscription.peer.clone(),
            session: page.subscription.session,
            subscription: page.subscription.subscription,
            frame: Some(page.frame),
            hashes: page.headers.clone(),
        }
    }

    /// The rejected page's session is the one whose `Session` row names its
    /// frame's stream, joined on that whole [`StreamRef`]: a row with any
    /// one key of the stream moved names another stream, so the page has
    /// no session (Rule 25's template).
    #[test]
    fn the_rejected_page_s_session_joins_its_stream() {
        let page = rejected_page(&log(1, 1, "service_protocol_reject")).unwrap();
        let stream = StreamRef::from(&page.frame);
        let keys = assert_one_key_off::<StreamRef>(&stream, |stream| {
            let mut moved = log(1, 1, "service_protocol_reject");
            moved[0] = Observation::Session {
                connection: stream.connection,
                peer: "peer-0".into(),
                quic_stream: stream.quic_stream,
                kind: stream.kind,
                version: stream.version,
                session_id: 11,
            };
            rejected_page(&moved).as_ref() == Ok(&page)
        });
        assert_eq!(keys, 4);
    }

    /// The rejected page's peer and hashes come from its frame's evidence:
    /// the peer its `Session` row names, and the page the peer sent there,
    /// joined on its frame's whole [`FrameRef`], so the same position on
    /// another stream kind, version or direction holds no page.
    #[test]
    fn the_sent_page_comes_from_the_frames_evidence() {
        let page = rejected_page(&log(1, 1, "service_protocol_reject")).unwrap();
        let peers = vec![("peer-0".to_owned(), "ab".to_owned())];
        let pages = SentPages::new(&crate::stores::EvidenceStores::default());
        assert!(sent_page(&page, &peers, &pages).is_err());
        pages.record(page.frame.clone(), sent().hashes);
        assert_eq!(sent_page(&page, &peers, &pages), Ok(sent()));
        assert!(sent_page(&page, &[], &pages).is_err());
        assert_one_key_off(&page.frame, |frame| {
            let page = RejectedPage {
                frame: frame.clone(),
                ..page.clone()
            };
            sent_page(&page, &peers, &pages).is_ok()
        });
    }

    /// The staging companion joins the page staged from the rejected frame:
    /// that page holds, and the same fact with any one key of its page
    /// moved, its peer, session, subscription, frame or a header, doesn't
    /// (Rule 25's template), nor with a key of its read or its
    /// subscription moved on its own. A frame that staged nothing, or two pages,
    /// doesn't either.
    #[test]
    fn the_staged_page_joins_the_rejected_frame() {
        let page = rejected_page(&log(1, 1, "service_protocol_reject")).unwrap();
        let own = staged(11, 3);
        assert_eq!(
            staged_page(std::slice::from_ref(&own), &page, &sent()),
            Ok(())
        );
        let keys = assert_one_key_off(&own.page().expect("it names its frame"), |staged| {
            staged_page(&[staged_from(staged)], &page, &sent()).is_ok()
        });
        assert_eq!(keys, 6);
        // The frame's read, and the page's subscription, each on its own.
        let read = own.read().expect("it names its frame");
        assert_one_key_off::<ReadRef>(&read, |read| {
            let moved = StagedPage {
                session: read.session,
                frame: Some(read.frame),
                ..own.clone()
            };
            staged_page(&[moved], &page, &sent()).is_ok()
        });
        let subscription = own.page().expect("it names its frame").subscription;
        assert_one_key_off::<SubscriptionRef>(&subscription, |subscription| {
            let mut staged = own.page().expect("it names its frame");
            staged.subscription = subscription.clone();
            staged_page(&[staged_from(&staged)], &page, &sent()).is_ok()
        });
        assert!(staged_page(&[], &page, &sent()).is_err());
        let mut twice = own.clone();
        twice.subscription += 1;
        assert!(staged_page(&[own, twice], &page, &sent()).is_err());
    }

    /// The path-break companion requires the retained violation to be a
    /// `path` break of exactly the page the peer sent, and the violation
    /// the property reported for that record: any one key of the page or
    /// of the finding moved on its own fails (Rule 25's template), and so
    /// do another rule, an unretained break, and a violation at the
    /// record's step whose detail holds the record's as a prefix.
    #[test]
    fn the_path_break_names_the_sent_page() {
        let page = rejected_page(&log(1, 1, "service_protocol_reject")).unwrap();
        let (store, properties) = claims("path", 3);
        assert_eq!(path_break(&properties, &store, &page, &sent()), Ok(()));
        assert_one_key_off(&page.page(&sent()), |subject| {
            let (page, sent) = page_of(subject);
            path_break(&properties, &store, &page, &sent).is_ok()
        });
        let violation = &properties[0].violations[0];
        let finding = FindingRef {
            step: violation.step,
            detail: violation.actual.clone(),
        };
        assert_one_key_off::<FindingRef>(&finding, |finding| {
            let mut properties = properties.clone();
            properties[0].violations[0].step = finding.step;
            properties[0].violations[0].actual = finding.detail.clone();
            path_break(&properties, &store, &page, &sent()).is_ok()
        });
        let (store, properties) = claims("credit", 3);
        assert!(path_break(&properties, &store, &page, &sent()).is_err());
        let (store, mut properties) = claims("path", 3);
        properties[0].violations.clear();
        assert!(path_break(&properties, &store, &page, &sent()).is_err());
        // The same step with another break's detail, which holds the
        // record's as a prefix.
        let (store, mut properties) = claims("path", 3);
        properties[0].violations[0].actual.push_str(" of another");
        assert!(path_break(&properties, &store, &page, &sent()).is_err());
    }
}
