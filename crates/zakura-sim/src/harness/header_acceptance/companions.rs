//! The despite-close case's companions (hdr-14 item 2, gap 6).
//!
//! The planted case [`DESPITE_CLOSE`] keeps the subscriber's content fault,
//! so the node still closes the peer, but forwards the rejected page to the
//! reactor anyway, so it stages. A correct close mustn't excuse that.
//! `commit-only-authorized` owns several claim kinds, so its `Caught` alone
//! could come from another one. Its row names three companions, each judged
//! on the case's own `Draft` run, and each joined to the rejected page by
//! its full identity, since Rule 25 isn't adopted:
//!
//! - [`FAULT_CLOSE`]: the node closed the rejected frame's connection with a
//!   cause in [`FAULT_CAUSES`](crate::model::FAULT_CAUSES), after it judged
//!   that frame. It joins on the connection.
//! - [`STAGED`]: exactly one `PageStaged` came from that frame. It joins on
//!   the session the frame's connection and QUIC stream carry, the
//!   subscription the frame names, and the frame's position among the
//!   stream's reads. Subscription IDs are per session, so the subscription
//!   alone doesn't name a page.
//! - [`PATH_BREAK`]: the retained `commit-only-authorized` violation is a
//!   `path` break whose subject is that staged page: its peer, session,
//!   subscription, frame and header hashes, which C22 adds to each page
//!   finding.
//!
//! The rejected page is found from the run's own log: the one `headers`
//! frame the peer's models labelled `BadContent` that the node judged
//! ([`rejected_page`]).

use serde_json::Value;
use zakura_network::zakura::observe::ReactorEvent;

use super::CaseRef;
use crate::{
    harness::header_node::authority::{Attribution, AuthorityRule, AuthoritySubject},
    model::{is_fault_cause, Label},
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

/// The rejected page's frame, as the run's log names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RejectedPage {
    /// The connection it came on.
    pub connection: u64,
    /// Its QUIC stream.
    pub quic_stream: u64,
    /// Its position among the peer's frames on the stream, which is its
    /// position among the stream's reads.
    pub frame: u64,
    /// The node's session for that connection and stream.
    pub session: u64,
    /// The subscription it names.
    pub subscription: u64,
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

/// The rejected page in `observations`: the one `headers` frame the peer's
/// models labelled `BadContent` and the node judged, with the session its
/// connection and QUIC stream carry. An error says why there's no single
/// one.
pub fn rejected_page(observations: &[Observation]) -> Result<RejectedPage, String> {
    let judged = |connection: u64, stream: u64, frame: u64| {
        observations.iter().any(|obs| {
            matches!(obs, Observation::FrameJudged { connection: c, quic_stream: Some(s), seq, .. }
                if (*c, *s, *seq) == (connection, stream, frame))
        })
    };
    let frames: Vec<(u64, u64, u64, Option<u64>)> = observations
        .iter()
        .filter_map(|obs| match obs {
            Observation::PeerFrame {
                connection,
                quic_stream: Some(quic_stream),
                seq,
                row: Some(row),
                label: Label::BadContent,
                exchange,
                ..
            } if row == "headers" => Some((*connection, *quic_stream, *seq, *exchange)),
            _ => None,
        })
        .filter(|(connection, stream, frame, _)| judged(*connection, *stream, *frame))
        .collect();
    let [(connection, quic_stream, frame, exchange)] = frames[..] else {
        return Err(format!(
            "{} judged bad-content pages, not one: {frames:?}",
            frames.len()
        ));
    };
    let subscription =
        exchange.ok_or_else(|| format!("the rejected page {frame} names no subscription"))?;
    let sessions: Vec<u64> = observations
        .iter()
        .filter_map(|obs| match obs {
            Observation::Session {
                connection: c,
                quic_stream: s,
                kind: 5,
                version: 9,
                session_id,
                ..
            } if (*c, *s) == (connection, quic_stream) => Some(*session_id),
            _ => None,
        })
        .collect();
    let [session] = sessions[..] else {
        return Err(format!(
            "connection {connection} stream {quic_stream} carries sessions {sessions:?}, not one"
        ));
    };
    Ok(RejectedPage {
        connection,
        quic_stream,
        frame,
        session,
        subscription,
    })
}

/// [`FAULT_CLOSE`]: the node's first close of `page`'s connection comes
/// after it judged `page`'s frame, with a fault cause.
pub fn fault_close(observations: &[Observation], page: &RejectedPage) -> Result<(), String> {
    let judged = observations
        .iter()
        .position(|obs| {
            matches!(obs, Observation::FrameJudged { connection, quic_stream: Some(s), seq, .. }
                if (*connection, *s, *seq) == (page.connection, page.quic_stream, page.frame))
        })
        .ok_or_else(|| format!("the node never judged {page:?}"))?;
    let close = observations
        .iter()
        .enumerate()
        .find_map(|(at, obs)| match obs {
            Observation::ConnectionClosed { connection, cause }
                if *connection == page.connection =>
            {
                Some((at, cause))
            }
            _ => None,
        });
    match close {
        None => Err(format!("connection {} never closed", page.connection)),
        Some((at, cause)) if at < judged => Err(format!(
            "connection {} closed with {cause} before the node judged frame {}",
            page.connection, page.frame
        )),
        Some((_, cause)) if !is_fault_cause(cause) => Err(format!(
            "connection {} closed with {cause}, not a fault cause",
            page.connection
        )),
        Some(_) => Ok(()),
    }
}

/// [`STAGED`]: exactly one of `staged` came from `page`'s frame, on its
/// session and subscription, and carries headers. Returns it.
pub fn staged_page(staged: &[StagedPage], page: &RejectedPage) -> Result<StagedPage, String> {
    let from: Vec<&StagedPage> = staged
        .iter()
        .filter(|fact| {
            fact.session == page.session
                && fact.subscription == page.subscription
                && fact.frame == Some(page.frame)
        })
        .collect();
    match from[..] {
        [fact] if !fact.hashes.is_empty() => Ok(fact.clone()),
        [fact] => Err(format!("the rejected page staged no header: {fact:?}")),
        _ => Err(format!(
            "{} pages staged from session {} subscription {} frame {}, not one",
            from.len(),
            page.session,
            page.subscription,
            page.frame
        )),
    }
}

/// [`PATH_BREAK`]: `commit-only-authorized` is violated in `properties`,
/// and one of its violations is the claim record in `claims`, the claim
/// store's snapshot, whose attribution is a `path` break of `staged`.
pub fn path_break(
    properties: &[PropertyReport],
    claims: &Value,
    staged: &StagedPage,
) -> Result<(), String> {
    let report = properties
        .iter()
        .find(|report| report.property == COMMIT)
        .ok_or_else(|| format!("{COMMIT} isn't reported"))?;
    if !matches!(report.outcome, PropertyOutcome::Violated { .. }) {
        return Err(format!("{COMMIT} is {:?}", report.outcome));
    }
    let expected = serde_json::to_value(Attribution {
        rule: AuthorityRule::Path,
        subject: AuthoritySubject::Page {
            peer: staged.peer.clone(),
            session: staged.session,
            subscription: staged.subscription,
            headers: staged.hashes.clone(),
            frame: staged.frame,
        },
    })
    .map_err(|error| format!("the expected attribution doesn't serialize: {error}"))?;
    let records = claims
        .as_array()
        .ok_or("the claim store's snapshot isn't a list")?;
    let found = records.iter().any(|record| {
        record["claim"] == "CommitOnlyAuthorized"
            && record["attribution"] == expected
            && record["detail"].as_str().is_some_and(|detail| {
                report
                    .violations
                    .iter()
                    .any(|violation| violation.actual.contains(detail))
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
    /// Each property's report.
    pub properties: Vec<PropertyReport>,
    /// The claim store's snapshot, `Null` if the run kept none.
    pub claims: Value,
}

/// Judges each of [`COMPANIONS`] on `evidence`, in order: `Ok` if it held,
/// else why not. Each finds the rejected page itself, so a run with no
/// single one fails all three.
pub fn judge(evidence: &DraftEvidence) -> Vec<(&'static str, Result<(), String>)> {
    let page = rejected_page(&evidence.observations);
    let staged = page
        .as_ref()
        .map_err(Clone::clone)
        .and_then(|page| staged_page(&evidence.staged, page));
    vec![
        (
            FAULT_CLOSE,
            page.as_ref()
                .map_err(Clone::clone)
                .and_then(|page| fault_close(&evidence.observations, page)),
        ),
        (STAGED, staged.clone().map(|_| ())),
        (
            PATH_BREAK,
            staged.and_then(|staged| path_break(&evidence.properties, &evidence.claims, &staged)),
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
/// here following the trace's own picks and draws, and the rerun's full
/// report, every violation and the stores' snapshot, must equal the one
/// recorded beside the trace, so what's judged is that run. So this must
/// run in the build that recorded the trace, the case's own patched one,
/// where a patched behavior replays. An error says why there's no verdict.
pub fn judge_trace(
    dir: &std::path::Path,
) -> Result<Vec<(&'static str, Result<(), String>)>, String> {
    use crate::harness::{header_sync::HeaderWire, Subsystem};
    let trace = crate::trace::TraceReader::open(dir).map_err(|error| error.to_string())?;
    if trace.meta.sim != HeaderWire::NAME {
        return Err(format!(
            "the trace ran {}, not {}",
            trace.meta.sim,
            HeaderWire::NAME
        ));
    }
    if trace.meta.draft != [COMMIT] {
        return Err(format!(
            "the trace's Draft override is {:?}, not [{COMMIT}]",
            trace.meta.draft
        ));
    }
    let script = crate::plan::TraceScript::new(&trace, false).map_err(|error| error.to_string())?;
    let (evidence, rerun) = draft_run(&trace.meta.scenario, crate::PlanInput::Trace(script))?;
    let recorded = std::fs::read_to_string(dir.join(crate::record::FULL_REPORT_FILE))
        .map_err(|error| format!("its full report: {error}"))
        .and_then(|text| {
            serde_json::from_str::<crate::record::FullReport>(&text)
                .map_err(|error| format!("its full report: {error}"))
        })?;
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
        properties: report.properties,
        claims,
    };
    Ok((evidence, sink.0))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{observe::StreamId, Violation};

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

    /// The fault-close companion joins the rejected frame's connection: a
    /// fault close there holds, and the same close of another connection,
    /// or a close there with a cause that isn't a fault, doesn't.
    #[test]
    fn the_fault_close_joins_the_rejected_frames_connection() {
        let page = rejected_page(&log(1, 1, "service_protocol_reject")).unwrap();
        assert_eq!(
            page,
            RejectedPage {
                connection: 1,
                quic_stream: 4,
                frame: 3,
                session: 11,
                subscription: 7,
            }
        );
        assert_eq!(
            fault_close(&log(1, 1, "service_protocol_reject"), &page),
            Ok(())
        );
        assert!(fault_close(&log(1, 2, "service_protocol_reject"), &page).is_err());
        assert!(fault_close(&log(1, 1, "service_local_shutdown"), &page).is_err());
    }

    /// The staging companion joins the page's session, subscription and
    /// frame: the page staged from that frame holds, and another page on
    /// the same subscription, or the same page on another session, doesn't.
    #[test]
    fn the_staged_page_joins_the_rejected_frame() {
        let page = rejected_page(&log(1, 1, "service_protocol_reject")).unwrap();
        assert_eq!(staged_page(&[staged(11, 3)], &page), Ok(staged(11, 3)));
        assert!(staged_page(&[staged(11, 5)], &page).is_err());
        assert!(staged_page(&[staged(12, 3)], &page).is_err());
        assert!(staged_page(&[], &page).is_err());
    }

    /// The path-break companion requires the retained violation to be a
    /// `path` break of exactly the staged page: another rule, another frame
    /// or an unretained break doesn't count.
    #[test]
    fn the_path_break_names_the_staged_page() {
        let page = staged(11, 3);
        let (store, properties) = claims("path", 3);
        assert_eq!(path_break(&properties, &store, &page), Ok(()));
        let (store, properties) = claims("credit", 3);
        assert!(path_break(&properties, &store, &page).is_err());
        let (store, properties) = claims("path", 4);
        assert!(path_break(&properties, &store, &page).is_err());
        let (store, mut properties) = claims("path", 3);
        properties[0].violations.clear();
        assert!(path_break(&properties, &store, &page).is_err());
    }
}
