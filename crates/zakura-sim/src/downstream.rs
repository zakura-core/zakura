//! Downstream generated checks (properties.md, Downstream templates).
//!
//! The close templates in [`crate::table`] check the node's reaction to a
//! labelled frame. These check the node's actual effects instead, whatever
//! code path produced them: the work it admits, the requests it serves, and
//! the responses it writes. Each effect is compared with what the role
//! models independently allowed, read from the peer's labelled frames and
//! the models' own records ([`Observation::CreditGranted`] and
//! [`Observation::ExchangeOwed`]). A guard that rejects a frame but lets a
//! second writer admit its work still fails here, and a correct close never
//! excuses an effect the models didn't allow.
//!
//! Every effect is checked at the step that observes it, against only the
//! observations before it in the log. An effect the models never allowed,
//! one after its connection closed, and one beyond its allowed quantity are
//! each a violation of their own. Serving owners are also checked whenever
//! no task is running: an owner whose request ended must have retired by
//! then.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use zakura_scheduler::Status as SchedulerStatus;

use crate::{
    model::Label,
    observe::{Entry, RowAt},
    property::{CheckCtx, HasView, Verdict},
    table::GeneratedId,
    Observation,
};

/// Checks the downstream template `id` names with `effect`, its check of
/// one log entry, over every entry this step appended.
pub(crate) fn check<S: HasView>(
    cx: &mut CheckCtx<'_, S>,
    id: &GeneratedId<'_>,
    effect: fn(&[Entry], usize, &RowAt) -> Found,
) -> Verdict {
    check_with(cx, id, effect, None)
}

/// Checks `receiver-serving-bounded`: [`serving_bounded`] on each entry,
/// and [`owners_retired`] at each step that leaves no task running.
pub(crate) fn check_serving<S: HasView>(cx: &mut CheckCtx<'_, S>, id: &GeneratedId<'_>) -> Verdict {
    let at_rest = cx.status != SchedulerStatus::Running;
    check_with(cx, id, serving_bounded, at_rest.then_some(owners_retired))
}

/// [`check`], also folding in `at_rest`'s verdict on the whole log.
fn check_with<S: HasView>(
    cx: &mut CheckCtx<'_, S>,
    id: &GeneratedId<'_>,
    effect: fn(&[Entry], usize, &RowAt) -> Found,
    at_rest: Option<fn(&[Entry], &RowAt) -> Found>,
) -> Verdict {
    let row = RowAt::new(id.stream, id.row.expect("downstream templates are per row"));
    let entries = cx.log.entries();
    let start = entries.partition_point(|e| e.step < cx.step);
    let mut verdict = Verdict::NotApplicable;
    let mut violations = Vec::new();
    let found = (start..entries.len())
        .map(|at| effect(entries, at, &row))
        .chain(at_rest.map(|check| check(entries, &row)));
    for found in found {
        match found {
            None => {}
            Some(Ok(())) => verdict = Verdict::Holds,
            Some(Err(failure)) => violations.push(failure),
        }
    }
    let mut violations = violations.into_iter();
    let Some(Failure { expected, actual }) = violations.next() else {
        return verdict;
    };
    for failure in violations {
        cx.record_violation(failure.expected, failure.actual);
    }
    cx.violated(expected, actual)
}

/// Why one effect broke its template.
pub(crate) struct Failure {
    expected: String,
    actual: String,
}

impl Failure {
    fn new(expected: impl Into<String>, actual: impl Into<String>) -> Self {
        Failure {
            expected: expected.into(),
            actual: actual.into(),
        }
    }
}

/// One template's verdict on entry `at`: `None` if the entry isn't an
/// effect the template checks.
pub(crate) type Found = Option<Result<(), Failure>>;

/// Objects and bytes, summed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Quantity {
    objects: u64,
    bytes: u64,
}

impl Quantity {
    fn add(&mut self, objects: u64, bytes: u64) {
        self.objects = self.objects.saturating_add(objects);
        self.bytes = self.bytes.saturating_add(bytes);
    }

    fn within(self, allowed: Quantity) -> bool {
        self.objects <= allowed.objects && self.bytes <= allowed.bytes
    }
}

/// `receiver-work-authorized`: work the node admits from a frame of `row`
/// must come from a frame of `row` with the work's exchange key that the
/// peer's models labelled `Conformant` and the node's reader judged, on a
/// connection still open. The work from each frame stays within that
/// frame's bytes: one frame's unused bytes never cover another's work.
pub(crate) fn work_authorized(entries: &[Entry], at: usize, row: &RowAt) -> Found {
    let Observation::WorkAdmitted {
        connection,
        frame,
        seq,
        exchange,
        ..
    } = &entries[at].obs
    else {
        return None;
    };
    if frame != row {
        return None;
    }
    let before = &entries[..at];
    let what = format!("connection {connection}'s work from {row} frame {seq}");
    if closed(before, *connection) {
        return Some(Err(Failure::new(
            format!("{what} stops when the connection closes"),
            "the node admitted it after the close",
        )));
    }
    let delivered = match source_frame(before, *connection, row, *seq) {
        Some(Source {
            label: Label::Conformant,
            exchange: Some(e),
            judged: true,
            payload,
        }) if e == *exchange => payload,
        source => {
            return Some(Err(Failure::new(
                format!("{what} comes from a conformant, judged frame of exchange {exchange}"),
                match source {
                    None => "the peer sent no such frame".to_owned(),
                    Some(source) => format!("the frame was {source}"),
                },
            )))
        }
    };
    let admitted = entries[..=at]
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::WorkAdmitted {
                connection: c,
                frame: f,
                seq: s,
                bytes,
                ..
            } if (c, f, s) == (connection, frame, seq) => Some(*bytes),
            _ => None,
        })
        .fold(0u64, u64::saturating_add);
    Some(if admitted <= delivered {
        Ok(())
    } else {
        Err(Failure::new(
            format!("{what} stays within the {delivered} bytes it delivered"),
            format!("the node admitted {admitted} bytes"),
        ))
    })
}

/// `receiver-credit-conserved`: work the node admits for a subscription of
/// `row`, from all its page rows, stays within the credit the node granted
/// since the subscription opened, in objects and in bytes. A terminal
/// outcome spends no credit, so its work isn't counted.
pub(crate) fn credit_conserved(entries: &[Entry], at: usize, row: &RowAt) -> Found {
    let Observation::WorkAdmitted {
        connection,
        request,
        exchange,
        ends: false,
        ..
    } = &entries[at].obs
    else {
        return None;
    };
    if request != row {
        return None;
    }
    let mut granted = Quantity::default();
    let mut admitted = Quantity::default();
    for entry in &entries[..=at] {
        match &entry.obs {
            Observation::CreditGranted {
                connection: c,
                subscription,
                exchange: e,
                objects,
                bytes,
                opens,
            } if (c, subscription, e) == (connection, request, exchange) => {
                if *opens {
                    granted = Quantity::default();
                    admitted = Quantity::default();
                }
                granted.add(*objects, *bytes);
            }
            Observation::WorkAdmitted {
                connection: c,
                request: r,
                exchange: e,
                objects,
                bytes,
                ends: false,
                ..
            } if (c, r, e) == (connection, request, exchange) => admitted.add(*objects, *bytes),
            _ => {}
        }
    }
    Some(if admitted.within(granted) {
        Ok(())
    } else {
        Err(Failure::new(
            format!(
                "connection {connection}'s work from {row} subscription {exchange} stays within \
                 the {} objects and {} bytes granted",
                granted.objects, granted.bytes
            ),
            format!(
                "the node admitted {} objects and {} bytes",
                admitted.objects, admitted.bytes
            ),
        ))
    })
}

/// `receiver-serving-bounded`: the node starts serving a request of `row`
/// only for a request frame with its key that the peer's models labelled
/// `Conformant` and the reader judged, while the request is open: before
/// the node writes its ending, on a connection still open. Each such
/// request gets at most one owner. The node's output answers only such an
/// open request too, whether or not an owner wrote it. Conformant requests
/// stay within the receiver's margin, so its serving owners and outputs do
/// too.
pub(crate) fn serving_bounded(entries: &[Entry], at: usize, row: &RowAt) -> Found {
    let (connection, request, exchange) = match &entries[at].obs {
        Observation::Serving {
            connection,
            request,
            exchange,
        } => (connection, request, exchange),
        Observation::NodeOutput {
            connection,
            request,
            exchange,
            ..
        } => return answered(&entries[..at], *connection, request, *exchange, row),
        _ => return None,
    };
    if request != row {
        return None;
    }
    let before = &entries[..at];
    let what = format!("connection {connection}'s {row} request {exchange}");
    if closed(before, *connection) {
        return Some(Err(Failure::new(
            format!("{what} is served only while the connection is open"),
            "the node started serving it after the close",
        )));
    }
    let requests = requests(before, *connection, row, *exchange);
    Some(if requests.unserved > 0 {
        Ok(())
    } else if requests.open > 0 {
        Err(Failure::new(
            format!("{what} gets one owner per open conformant request"),
            "the node started a second owner for it",
        ))
    } else {
        Err(Failure::new(
            format!("{what} is served only while a conformant request is open"),
            "the node started serving it with no request open",
        ))
    })
}

/// The output part of `receiver-serving-bounded`: the node's output for a
/// request of `row` with key `exchange` on `connection` answers a
/// conformant, judged request still open in `before`.
fn answered(
    before: &[Entry],
    connection: u64,
    request: &RowAt,
    exchange: u64,
    row: &RowAt,
) -> Found {
    if request != row {
        return None;
    }
    Some(if requests(before, connection, row, exchange).open > 0 {
        Ok(())
    } else {
        Err(Failure::new(
            format!(
                "connection {connection}'s {row} request {exchange} is answered only while a \
                 conformant request is open"
            ),
            "the node answered it with no request open",
        ))
    })
}

/// The rest of `receiver-serving-bounded`, checked when no task is running:
/// every owner for a request of `row` that has ended, by the node's ending
/// or its connection's close, has retired. `None` if the node never served
/// one.
pub(crate) fn owners_retired(entries: &[Entry], row: &RowAt) -> Found {
    // Live owners and open requests per (connection, exchange).
    let mut owners: BTreeMap<(u64, u64), u64> = BTreeMap::new();
    for entry in entries {
        match &entry.obs {
            Observation::Serving {
                connection,
                request,
                exchange,
            } if request == row => *owners.entry((*connection, *exchange)).or_default() += 1,
            Observation::ServingEnded {
                connection,
                request,
                exchange,
            } if request == row => {
                if let Some(live) = owners.get_mut(&(*connection, *exchange)) {
                    *live = live.saturating_sub(1);
                }
            }
            _ => {}
        }
    }
    if owners.is_empty() {
        return None;
    }
    for (&(connection, exchange), &live) in &owners {
        let open = requests(entries, connection, row, exchange).open;
        if live > open {
            return Some(Err(Failure::new(
                format!(
                    "connection {connection}'s owners for {row} request {exchange} retire \
                     once their requests end"
                ),
                format!("{live} owners are live for {open} open requests"),
            )));
        }
    }
    Some(Ok(()))
}

/// `sender-within-response-quantity`: each response the node writes with a
/// frame of `row` answers an exchange the peer opened and hasn't seen end,
/// by an ending or by its connection's close,
/// and the exchange's output since it opened, across its response rows,
/// stays within what it owes: frames before the ending, and bytes with the
/// ending's.
pub(crate) fn within_response_quantity(entries: &[Entry], at: usize, row: &RowAt) -> Found {
    let Observation::NodeOutput {
        connection,
        frame,
        request,
        exchange,
        ..
    } = &entries[at].obs
    else {
        return None;
    };
    if frame != row {
        return None;
    }
    let what = format!("connection {connection}'s {request} exchange {exchange}");
    // The exchange as it stood before this frame: what it owes, and what
    // the node wrote since it opened. An ending closes it.
    let mut open: Option<(u32, u64)> = None;
    let mut written = (0u32, 0u64);
    for entry in &entries[..at] {
        match &entry.obs {
            Observation::ExchangeOwed {
                connection: c,
                request: r,
                exchange: e,
                frames,
                bytes,
            } if (c, r, e) == (connection, request, exchange) => {
                open = Some((*frames, *bytes));
                written = (0, 0);
            }
            Observation::NodeOutput {
                connection: c,
                request: r,
                exchange: e,
                payload,
                ends,
                ..
            } if (c, r, e) == (connection, request, exchange) => {
                if *ends {
                    open = None;
                } else {
                    written.0 = written.0.saturating_add(1);
                }
                written.1 = written.1.saturating_add(*payload);
            }
            // The session's end ends every exchange on it.
            Observation::ConnectionClosed { connection: c, .. } if c == connection => open = None,
            _ => {}
        }
    }
    let Observation::NodeOutput { payload, ends, .. } = &entries[at].obs else {
        unreachable!("matched above");
    };
    let Some((frames, bytes)) = open else {
        return Some(Err(Failure::new(
            format!("{what} is open when the node answers it"),
            format!("the node wrote a {row} frame for it"),
        )));
    };
    let frames_after = written.0.saturating_add(u32::from(!ends));
    let bytes_after = written.1.saturating_add(*payload);
    Some(if frames_after <= frames && bytes_after <= bytes {
        Ok(())
    } else {
        Err(Failure::new(
            format!("{what} gets at most {frames} frames before its ending and {bytes} bytes"),
            format!("the node wrote {frames_after} frames and {bytes_after} bytes"),
        ))
    })
}

/// True if `entries` hold a close of `connection`.
fn closed(entries: &[Entry], connection: u64) -> bool {
    entries.iter().any(|entry| {
        matches!(entry.obs, Observation::ConnectionClosed { connection: c, .. } if c == connection)
    })
}

/// A peer frame as the models labelled it and whether the reader judged
/// it.
struct Source {
    label: Label,
    exchange: Option<u64>,
    judged: bool,
    payload: u64,
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.label)?;
        if let Some(exchange) = self.exchange {
            write!(f, " for exchange {exchange}")?;
        }
        if !self.judged {
            f.write_str(", not yet judged")?;
        }
        Ok(())
    }
}

/// Frame `seq` of `row` on `connection` in `entries`, or `None` if the peer
/// sent no such frame.
fn source_frame(entries: &[Entry], connection: u64, row: &RowAt, seq: u64) -> Option<Source> {
    let mut source = None;
    for entry in entries {
        match &entry.obs {
            Observation::PeerFrame {
                connection: c,
                stream,
                seq: s,
                row: Some(r),
                label,
                exchange,
                payload,
                ..
            } if (*c, *stream, *s) == (connection, row.stream, seq) && *r == row.row => {
                source = Some(Source {
                    label: *label,
                    exchange: *exchange,
                    judged: false,
                    payload: *payload,
                });
            }
            Observation::FrameJudged {
                connection: c,
                stream,
                seq: s,
            } if (*c, *stream, *s) == (connection, row.stream, seq) => {
                if let Some(source) = &mut source {
                    source.judged = true;
                }
            }
            _ => {}
        }
    }
    source
}

/// A request key's conformant requests, as of some point in the log.
#[derive(Clone, Copy, Debug, Default)]
struct Requests {
    /// Judged, and not yet ended by the node's ending or the close.
    open: u64,
    /// Open, and with no serving owner yet.
    unserved: u64,
}

/// The requests of `row` with key `exchange` on `connection` in `entries`
/// that the peer's models labelled `Conformant` and the node's reader then
/// judged, with the node's endings and serving owners applied in order.
fn requests(entries: &[Entry], connection: u64, row: &RowAt, exchange: u64) -> Requests {
    let mut sent = BTreeSet::new();
    let mut found = Requests::default();
    for entry in entries {
        match &entry.obs {
            Observation::PeerFrame {
                connection: c,
                stream,
                seq,
                row: Some(r),
                label: Label::Conformant,
                exchange: Some(e),
                ..
            } if *c == connection && *stream == row.stream && *r == row.row && *e == exchange => {
                sent.insert(*seq);
            }
            Observation::FrameJudged {
                connection: c,
                stream,
                seq,
            } if *c == connection && *stream == row.stream && sent.remove(seq) => {
                found.open += 1;
                found.unserved += 1;
            }
            Observation::Serving {
                connection: c,
                request: r,
                exchange: e,
            } if (*c, r, *e) == (connection, row, exchange) => {
                found.unserved = found.unserved.saturating_sub(1);
            }
            Observation::NodeOutput {
                connection: c,
                request: r,
                exchange: e,
                ends: true,
                ..
            } if (*c, r, *e) == (connection, row, exchange) => {
                found.open = found.open.saturating_sub(1);
                found.unserved = found.unserved.min(found.open);
            }
            Observation::ConnectionClosed { connection: c, .. } if *c == connection => {
                found = Requests::default();
            }
            _ => {}
        }
    }
    found
}
