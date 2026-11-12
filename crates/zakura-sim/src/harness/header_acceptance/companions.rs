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
//!   that frame, and the session's reader refused a frame for the page's
//!   content fault, [`CONTENT_FAULT`]. It joins on the connection and the
//!   session, so a close for another fault doesn't count.
//! - [`STAGED`]: exactly one `PageStaged` came from that frame. It joins on
//!   the session the frame's connection and QUIC stream carry, the
//!   subscription the frame names, and the frame's position among the
//!   stream's reads, and it carries the frame's peer and the hashes the peer
//!   sent. Subscription IDs are per session, so the subscription alone
//!   doesn't name a page.
//! - [`PATH_BREAK`]: the retained `commit-only-authorized` violation is a
//!   `path` break whose subject is that page: its peer, session,
//!   subscription, frame and header hashes, which C22 adds to each page
//!   finding.
//!
//! The rejected page is found from the run's own log: the one `headers`
//! frame the peer's models labelled `BadContent` that the node judged
//! ([`rejected_page`]). Its peer and hashes come from the frame's own
//! evidence, apart from anything the node staged ([`sent_page`]): the peer
//! its connection's `Session` row names, and the hashes the peer put in the
//! frame, so a substituted page satisfies neither companion.

use serde_json::Value;
use zakura_network::zakura::observe::ReactorEvent;

use super::CaseRef;
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
    /// The peer its connection's `Session` row names, by harness name.
    pub peer: String,
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
    let sessions: Vec<(u64, &str)> = observations
        .iter()
        .filter_map(|obs| match obs {
            Observation::Session {
                connection: c,
                quic_stream: s,
                kind: 5,
                version: 9,
                session_id,
                peer,
            } if (*c, *s) == (connection, quic_stream) => Some((*session_id, peer.as_str())),
            _ => None,
        })
        .collect();
    let [(session, peer)] = sessions[..] else {
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
        peer: peer.to_owned(),
    })
}

/// What the peer sent as `page`: the peer `peers` names for its harness
/// name, by `(name, peer)`, and the header hashes `sent` holds for its
/// frame. An error says which is missing.
pub fn sent_page(
    page: &RejectedPage,
    peers: &[(String, String)],
    sent: &SentPages,
) -> Result<SentPage, String> {
    let peer = peers
        .iter()
        .find(|(name, _)| *name == page.peer)
        .map(|(_, peer)| peer.clone())
        .ok_or_else(|| format!("no peer is named {}", page.peer))?;
    let hashes = sent
        .page(page.connection, page.quic_stream, page.frame)
        .ok_or_else(|| format!("the peer sent no page as {page:?}"))?;
    Ok(SentPage { peer, hashes })
}

/// [`FAULT_CLOSE`]: the node's first close of `page`'s connection comes
/// after it judged `page`'s frame, with a fault cause, and `refusals` hold
/// exactly one of `page`'s session, for [`CONTENT_FAULT`]. A reader ends its
/// session at its first refusal, so that one is the close's.
pub fn fault_close(
    observations: &[Observation],
    refusals: &[Refusal],
    page: &RejectedPage,
) -> Result<(), String> {
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
        Some(_) => {
            let of_session: Vec<&Refusal> = refusals
                .iter()
                .filter(|refusal| refusal.session == page.session)
                .collect();
            match of_session[..] {
                [refusal] if refusal.reason == CONTENT_FAULT => Ok(()),
                _ => Err(format!(
                    "session {}'s reader refused {of_session:?}, not one frame for {CONTENT_FAULT}",
                    page.session
                )),
            }
        }
    }
}

/// [`STAGED`]: exactly one of `staged` came from `page`'s frame, on its
/// session and subscription, and it carries `sent`'s peer and its headers,
/// which aren't none.
pub fn staged_page(
    staged: &[StagedPage],
    page: &RejectedPage,
    sent: &SentPage,
) -> Result<(), String> {
    let from: Vec<&StagedPage> = staged
        .iter()
        .filter(|fact| {
            fact.session == page.session
                && fact.subscription == page.subscription
                && fact.frame == Some(page.frame)
        })
        .collect();
    match from[..] {
        [fact] if fact.hashes.is_empty() => {
            Err(format!("the rejected page staged no header: {fact:?}"))
        }
        [fact] if fact.peer != sent.peer || fact.hashes != sent.hashes => Err(format!(
            "the page staged from the rejected frame is {fact:?}, not the peer's {sent:?}"
        )),
        [_] => Ok(()),
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
/// store's snapshot, whose attribution is a `path` break of `page` as the
/// peer sent it, `sent`.
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
    let expected = serde_json::to_value(Attribution {
        rule: AuthorityRule::Path,
        subject: AuthoritySubject::Page {
            peer: sent.peer.clone(),
            session: page.session,
            subscription: page.subscription,
            headers: sent.hashes.clone(),
            frame: Some(page.frame),
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

/// Judges each of [`COMPANIONS`] on `evidence`, in order: `Ok` if it held,
/// else why not. Each finds the rejected page itself, so a run with no
/// single one fails all three.
pub fn judge(evidence: &DraftEvidence) -> Vec<(&'static str, Result<(), String>)> {
    let page = rejected_page(&evidence.observations);
    let sent = page
        .as_ref()
        .map_err(Clone::clone)
        .and_then(|page| sent_page(page, &evidence.peers, &evidence.sent).map(|sent| (page, sent)));
    vec![
        (
            FAULT_CLOSE,
            page.as_ref()
                .map_err(Clone::clone)
                .and_then(|page| fault_close(&evidence.observations, &evidence.refusals, page)),
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
    use crate::harness::{
        header_sync::{HeaderWire, WireScenario},
        run_prepared, with_draft, HarnessError, Subsystem,
    };
    let entry = crate::harness::find(HeaderWire::NAME).map_err(|error| error.to_string())?;
    let scenario: WireScenario =
        serde_json::from_value(entry.scenario(input)).map_err(|error| error.to_string())?;
    let plan = crate::Plan::from_fuzz(&input[entry.scenario_bytes.min(input.len())..]);
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
        .and_then(|full| full.snapshot)
        .and_then(|snapshot| {
            snapshot.as_array().and_then(|stores| {
                stores
                    .iter()
                    .find(|store| store["store"] == "claims")
                    .map(|store| store["snapshot"].clone())
            })
        })
        .unwrap_or(Value::Null);
    Ok(DraftEvidence {
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
    })
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

    /// The reader's refusal of session 11's frame for `reason`.
    fn refused(reason: &str) -> Vec<Refusal> {
        vec![Refusal {
            peer: "ab".into(),
            session: 11,
            reason: reason.into(),
        }]
    }

    /// The fault-close companion joins the rejected frame's connection and
    /// its content refusal: a fault close there after the session's reader
    /// refused a frame for linkage holds, and the same close of another
    /// connection, a close there with a cause that isn't a fault, or a close
    /// after a refusal for another fault, or none, doesn't.
    #[test]
    fn the_fault_close_joins_the_rejected_frames_connection() {
        let linkage = refused(CONTENT_FAULT);
        let page = rejected_page(&log(1, 1, "service_protocol_reject")).unwrap();
        assert_eq!(
            page,
            RejectedPage {
                connection: 1,
                quic_stream: 4,
                frame: 3,
                session: 11,
                subscription: 7,
                peer: "peer-0".into(),
            }
        );
        let closed = log(1, 1, "service_protocol_reject");
        assert_eq!(fault_close(&closed, &linkage, &page), Ok(()));
        assert!(fault_close(&log(1, 2, "service_protocol_reject"), &linkage, &page).is_err());
        assert!(fault_close(&log(1, 1, "service_local_shutdown"), &linkage, &page).is_err());
        // Another fault's refusal closes the same connection.
        assert!(fault_close(&closed, &refused("over_credit"), &page).is_err());
        assert!(fault_close(&closed, &[], &page).is_err());
        // The linkage refusal of another session isn't this page's.
        let mut other = linkage.clone();
        other[0].session = 12;
        assert!(fault_close(&closed, &other, &page).is_err());
    }

    /// What peer `ab` sent as the rejected page: headers `01` and `02`.
    fn sent() -> SentPage {
        SentPage {
            peer: "ab".into(),
            hashes: vec!["01".into(), "02".into()],
        }
    }

    /// `sent` with only its peer moved, then with only its hashes moved.
    fn one_key_off() -> [SentPage; 2] {
        [
            SentPage {
                peer: "cd".into(),
                ..sent()
            },
            SentPage {
                hashes: vec!["01".into(), "03".into()],
                ..sent()
            },
        ]
    }

    /// The rejected page's peer and hashes come from its frame's evidence:
    /// the peer its `Session` row names, and the page the peer sent there.
    #[test]
    fn the_sent_page_comes_from_the_frames_evidence() {
        let page = rejected_page(&log(1, 1, "service_protocol_reject")).unwrap();
        let peers = vec![("peer-0".to_owned(), "ab".to_owned())];
        let pages = SentPages::default();
        assert!(sent_page(&page, &peers, &pages).is_err());
        pages.record((1, 4, 3), sent().hashes);
        assert_eq!(sent_page(&page, &peers, &pages), Ok(sent()));
        assert!(sent_page(&page, &[], &pages).is_err());
    }

    /// The staging companion joins the page's session, subscription and
    /// frame, and the peer and hashes the peer sent: the page staged from
    /// that frame holds, and another page on the same subscription, the
    /// same page on another session, or the same frame with only its peer
    /// or only its hashes changed, doesn't.
    #[test]
    fn the_staged_page_joins_the_rejected_frame() {
        let page = rejected_page(&log(1, 1, "service_protocol_reject")).unwrap();
        assert_eq!(staged_page(&[staged(11, 3)], &page, &sent()), Ok(()));
        assert!(staged_page(&[staged(11, 5)], &page, &sent()).is_err());
        assert!(staged_page(&[staged(12, 3)], &page, &sent()).is_err());
        assert!(staged_page(&[], &page, &sent()).is_err());
        for off in one_key_off() {
            assert!(
                staged_page(&[staged(11, 3)], &page, &off).is_err(),
                "{off:?}"
            );
        }
    }

    /// The path-break companion requires the retained violation to be a
    /// `path` break of exactly the page the peer sent: another rule,
    /// another frame, another peer, other hashes or an unretained break
    /// doesn't count.
    #[test]
    fn the_path_break_names_the_sent_page() {
        let page = rejected_page(&log(1, 1, "service_protocol_reject")).unwrap();
        let (store, properties) = claims("path", 3);
        assert_eq!(path_break(&properties, &store, &page, &sent()), Ok(()));
        for off in one_key_off() {
            assert!(
                path_break(&properties, &store, &page, &off).is_err(),
                "{off:?}"
            );
        }
        let (store, properties) = claims("credit", 3);
        assert!(path_break(&properties, &store, &page, &sent()).is_err());
        let (store, properties) = claims("path", 4);
        assert!(path_break(&properties, &store, &page, &sent()).is_err());
        let (store, mut properties) = claims("path", 3);
        properties[0].violations.clear();
        assert!(path_break(&properties, &store, &page, &sent()).is_err());
    }
}
