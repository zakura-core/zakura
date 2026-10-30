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
//!
//! Every join keys on the rows' QUIC stream as well as their connection
//! (M6a-Q8): a header stream that retires and reopens on the same
//! connection numbers its frames and subscriptions from the start again,
//! so a frame, judgment, request, credit or output of one stream
//! incarnation never stands for another's. The tier declares whether its
//! rows name a QUIC stream ([`StreamIdentity`]), and a row that breaks the
//! declaration is a harness failure, so a missing stream can't join every
//! incarnation at once.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use zakura_scheduler::Status as SchedulerStatus;

use crate::{
    model::Label,
    observe::{Entry, RowAt},
    property::{CheckCtx, HasView, Verdict},
    table::{GeneratedId, StreamIdentity},
    Observation,
};

/// Checks the downstream template `id` names with `effect`, its check of
/// one log entry, over every entry this step appended, once every
/// downstream row this step appended keeps the tier's `identity`.
pub(crate) fn check<S: HasView>(
    cx: &mut CheckCtx<'_, S>,
    id: &GeneratedId<'_>,
    identity: StreamIdentity,
    effect: impl Fn(&[Entry], usize, &RowAt) -> Found,
) -> Verdict {
    if !keeps_identity(cx, identity) {
        return Verdict::NotApplicable;
    }
    check_with(cx, id, effect, None)
}

/// [`check`] without the identity rule, for the sender templates, whose
/// rows are frames the peers labelled.
pub(crate) fn check_rows<S: HasView>(
    cx: &mut CheckCtx<'_, S>,
    id: &GeneratedId<'_>,
    effect: impl Fn(&[Entry], usize, &RowAt) -> Found,
) -> Verdict {
    check_with(cx, id, effect, None)
}

/// Checks `receiver-serving-bounded`: [`serving_bounded`] on each entry,
/// and [`owners_retired`] at each step that leaves no task running, as
/// [`check`] does.
pub(crate) fn check_serving<S: HasView>(
    cx: &mut CheckCtx<'_, S>,
    id: &GeneratedId<'_>,
    identity: StreamIdentity,
) -> Verdict {
    if !keeps_identity(cx, identity) {
        return Verdict::NotApplicable;
    }
    let at_rest = cx.status != SchedulerStatus::Running;
    check_with(cx, id, serving_bounded, at_rest.then_some(owners_retired))
}

/// True if every downstream row this step appended names a QUIC stream
/// exactly when `identity` says the tier's rows do. Otherwise records the
/// first that doesn't as missing evidence, which fails the run as a
/// harness failure.
fn keeps_identity<S: HasView>(cx: &mut CheckCtx<'_, S>, identity: StreamIdentity) -> bool {
    let entries = cx.log.entries();
    let start = entries.partition_point(|e| e.step < cx.step);
    let broken = entries[start..].iter().find_map(|entry| {
        let (name, connection, quic_stream) = stream_of(&entry.obs)?;
        match (identity, quic_stream) {
            (StreamIdentity::Exact, None) => Some(format!(
                "connection {connection}'s {name} row names no QUIC stream, on a tier whose \
                 rows must"
            )),
            (StreamIdentity::PerKind, Some(quic)) => Some(format!(
                "connection {connection}'s {name} row names QUIC stream {quic}, on a tier with \
                 one stream of each kind"
            )),
            _ => None,
        }
    });
    match broken {
        Some(detail) => {
            cx.missing_evidence(detail);
            false
        }
        None => true,
    }
}

/// A downstream row's name, connection and QUIC stream, or `None` for any
/// other observation.
fn stream_of(obs: &Observation) -> Option<(&'static str, u64, Option<u64>)> {
    let (name, connection, quic_stream) = match obs {
        Observation::CreditGranted {
            connection,
            quic_stream,
            ..
        } => ("credit-granted", connection, quic_stream),
        Observation::ExchangeOwed {
            connection,
            quic_stream,
            ..
        } => ("exchange-owed", connection, quic_stream),
        Observation::WorkAdmitted {
            connection,
            quic_stream,
            ..
        } => ("work-admitted", connection, quic_stream),
        Observation::UnframedWork {
            connection,
            quic_stream,
            ..
        } => ("unframed-work", connection, quic_stream),
        Observation::Serving {
            connection,
            quic_stream,
            ..
        } => ("serving", connection, quic_stream),
        Observation::ServingEnded {
            connection,
            quic_stream,
            ..
        } => ("serving-ended", connection, quic_stream),
        Observation::NodeOutput {
            connection,
            quic_stream,
            ..
        } => ("node-output", connection, quic_stream),
        _ => return None,
    };
    Some((name, *connection, *quic_stream))
}

/// [`check`], also folding in `at_rest`'s verdict on the whole log.
fn check_with<S: HasView>(
    cx: &mut CheckCtx<'_, S>,
    id: &GeneratedId<'_>,
    effect: impl Fn(&[Entry], usize, &RowAt) -> Found,
    at_rest: Option<fn(&[Entry], &RowAt) -> Found>,
) -> Verdict {
    let row = RowAt::new(id.stream, id.row.expect("these templates are per row"));
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
    pub(crate) fn new(expected: impl Into<String>, actual: impl Into<String>) -> Self {
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
/// must come from a frame of `row` on the work's QUIC stream with the
/// work's exchange key that the peer's models labelled `Conformant` and the
/// node's reader judged, on a connection still open. The work from each
/// frame stays within that frame's bytes: one frame's unused bytes never
/// cover another's work.
///
/// Held work no frame explains, [`Observation::UnframedWork`], fails every
/// row of its stream directly, since no frame of any row delivered it.
pub(crate) fn work_authorized(entries: &[Entry], at: usize, row: &RowAt) -> Found {
    if let Observation::UnframedWork {
        connection,
        request,
        exchange,
        objects,
        cause,
        ..
    } = &entries[at].obs
    {
        return (request.stream == row.stream).then(|| {
            Err(Failure::new(
                format!(
                    "connection {connection}'s held work for {request} exchange {exchange} comes \
                     from the frames it staged"
                ),
                format!("it holds {objects} objects no frame explains ({cause:?})"),
            ))
        });
    }
    let Observation::WorkAdmitted {
        connection,
        quic_stream,
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
    let what = format!(
        "connection {connection}'s work from {row}{} frame {seq}",
        on(*quic_stream)
    );
    if closed(before, *connection) {
        return Some(Err(Failure::new(
            format!("{what} stops when the connection closes"),
            "the node admitted it after the close",
        )));
    }
    let delivered = match source_frame(before, *connection, *quic_stream, row, *seq) {
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
                quic_stream: q,
                frame: f,
                seq: s,
                bytes,
                ..
            } if (c, f, s) == (connection, frame, seq) && same_stream(*q, *quic_stream) => {
                Some(*bytes)
            }
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
/// on its QUIC stream since the subscription opened, in objects and in
/// bytes. A terminal outcome spends no credit, so its work isn't counted.
pub(crate) fn credit_conserved(entries: &[Entry], at: usize, row: &RowAt) -> Found {
    let Observation::WorkAdmitted {
        connection,
        quic_stream,
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
                quic_stream: q,
                subscription,
                exchange: e,
                objects,
                bytes,
                opens,
            } if (c, subscription, e) == (connection, request, exchange)
                && same_stream(*q, *quic_stream) =>
            {
                if *opens {
                    granted = Quantity::default();
                    admitted = Quantity::default();
                }
                granted.add(*objects, *bytes);
            }
            Observation::WorkAdmitted {
                connection: c,
                quic_stream: q,
                request: r,
                exchange: e,
                objects,
                bytes,
                ends: false,
                ..
            } if (c, r, e) == (connection, request, exchange) && same_stream(*q, *quic_stream) => {
                admitted.add(*objects, *bytes)
            }
            _ => {}
        }
    }
    Some(if admitted.within(granted) {
        Ok(())
    } else {
        Err(Failure::new(
            format!(
                "connection {connection}'s work from {row}{} subscription {exchange} stays \
                 within the {} objects and {} bytes granted",
                on(*quic_stream),
                granted.objects,
                granted.bytes
            ),
            format!(
                "the node admitted {} objects and {} bytes",
                admitted.objects, admitted.bytes
            ),
        ))
    })
}

/// `receiver-serving-bounded`: the node starts serving a request of `row`
/// only for a request frame on the owner's QUIC stream with its key that
/// the peer's models labelled `Conformant` and the reader judged, while the
/// request is open: before the node writes its ending, on a connection
/// still open. Each such request gets at most one owner. The node's output
/// answers only such an open request too, whether or not an owner wrote
/// it. Conformant requests stay within the receiver's margin, so its
/// serving owners and outputs do too.
pub(crate) fn serving_bounded(entries: &[Entry], at: usize, row: &RowAt) -> Found {
    let (connection, quic_stream, request, exchange) = match &entries[at].obs {
        Observation::Serving {
            connection,
            quic_stream,
            request,
            exchange,
        } => (connection, quic_stream, request, exchange),
        Observation::NodeOutput {
            connection,
            quic_stream,
            request,
            exchange,
            ..
        } => {
            let at_key = Key::new(*connection, *quic_stream, *exchange);
            return answered(&entries[..at], at_key, request, row);
        }
        _ => return None,
    };
    if request != row {
        return None;
    }
    let key = Key::new(*connection, *quic_stream, *exchange);
    let before = &entries[..at];
    let what = format!("{key}'s {row} request {exchange}");
    if closed(before, *connection) {
        return Some(Err(Failure::new(
            format!("{what} is served only while the connection is open"),
            "the node started serving it after the close",
        )));
    }
    let requests = requests(before, key, row);
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
/// request of `row` with `key` answers a conformant, judged request still
/// open in `before`.
fn answered(before: &[Entry], key: Key, request: &RowAt, row: &RowAt) -> Found {
    if request != row {
        return None;
    }
    Some(if requests(before, key, row).open > 0 {
        Ok(())
    } else {
        Err(Failure::new(
            format!(
                "{key}'s {row} request {} is answered only while a conformant request is open",
                key.exchange
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
    // Live owners per request key.
    let mut owners: BTreeMap<Key, u64> = BTreeMap::new();
    for entry in entries {
        match &entry.obs {
            Observation::Serving {
                connection,
                quic_stream,
                request,
                exchange,
            } if request == row => {
                *owners
                    .entry(Key::new(*connection, *quic_stream, *exchange))
                    .or_default() += 1;
            }
            Observation::ServingEnded {
                connection,
                quic_stream,
                request,
                exchange,
            } if request == row => {
                if let Some(live) = owners.get_mut(&Key::new(*connection, *quic_stream, *exchange))
                {
                    *live = live.saturating_sub(1);
                }
            }
            _ => {}
        }
    }
    if owners.is_empty() {
        return None;
    }
    for (&key, &live) in &owners {
        let open = requests(entries, key, row).open;
        if live > open {
            return Some(Err(Failure::new(
                format!(
                    "{key}'s owners for {row} request {} retire once their requests end",
                    key.exchange
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
        quic_stream,
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
    let what = format!(
        "connection {connection}'s {request}{} exchange {exchange}",
        on(*quic_stream)
    );
    // The exchange as it stood before this frame: what it owes, and what
    // the node wrote since it opened. An ending closes it.
    let mut open: Option<(u32, u64)> = None;
    let mut written = (0u32, 0u64);
    for entry in &entries[..at] {
        match &entry.obs {
            Observation::ExchangeOwed {
                connection: c,
                quic_stream: q,
                request: r,
                exchange: e,
                frames,
                bytes,
            } if (c, r, e) == (connection, request, exchange) && same_stream(*q, *quic_stream) => {
                open = Some((*frames, *bytes));
                written = (0, 0);
            }
            Observation::NodeOutput {
                connection: c,
                quic_stream: q,
                request: r,
                exchange: e,
                payload,
                ends,
                ..
            } if (c, r, e) == (connection, request, exchange) && same_stream(*q, *quic_stream) => {
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

/// Frame `seq` of `row` on `connection` and `quic_stream` in `entries`, or
/// `None` if the peer sent no such frame.
fn source_frame(
    entries: &[Entry],
    connection: u64,
    quic_stream: Option<u64>,
    row: &RowAt,
    seq: u64,
) -> Option<Source> {
    let mut source = None;
    for entry in entries {
        match &entry.obs {
            Observation::PeerFrame {
                connection: c,
                stream,
                quic_stream: q,
                seq: s,
                row: Some(r),
                label,
                exchange,
                payload,
                ..
            } if (*c, *stream, *s) == (connection, row.stream, seq)
                && same_stream(*q, quic_stream)
                && *r == row.row =>
            {
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
                quic_stream: q,
                seq: s,
            } if (*c, *stream, *s) == (connection, row.stream, seq)
                && same_stream(*q, quic_stream) =>
            {
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

/// The requests of `row` with `key` in `entries` that the peer's models
/// labelled `Conformant` and the node's reader then judged, with the node's
/// endings and serving owners applied in order.
fn requests(entries: &[Entry], key: Key, row: &RowAt) -> Requests {
    let mut sent = BTreeSet::new();
    let mut found = Requests::default();
    for entry in entries {
        match &entry.obs {
            Observation::PeerFrame {
                connection,
                stream,
                quic_stream,
                seq,
                row: Some(r),
                label: Label::Conformant,
                exchange: Some(exchange),
                ..
            } if key.names(*connection, *quic_stream, *exchange)
                && *stream == row.stream
                && *r == row.row =>
            {
                sent.insert(*seq);
            }
            Observation::FrameJudged {
                connection,
                stream,
                quic_stream,
                seq,
            } if *connection == key.connection
                && same_stream(*quic_stream, key.quic_stream)
                && *stream == row.stream
                && sent.remove(seq) =>
            {
                found.open += 1;
                found.unserved += 1;
            }
            Observation::Serving {
                connection,
                quic_stream,
                request,
                exchange,
            } if key.names(*connection, *quic_stream, *exchange) && request == row => {
                found.unserved = found.unserved.saturating_sub(1);
            }
            Observation::NodeOutput {
                connection,
                quic_stream,
                request,
                exchange,
                ends: true,
                ..
            } if key.names(*connection, *quic_stream, *exchange) && request == row => {
                found.open = found.open.saturating_sub(1);
                found.unserved = found.unserved.min(found.open);
            }
            Observation::ConnectionClosed { connection, .. } if *connection == key.connection => {
                found = Requests::default();
            }
            _ => {}
        }
    }
    found
}

/// A request's key on one stream incarnation: its connection, its QUIC
/// stream, and its exchange key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Key {
    connection: u64,
    quic_stream: Option<u64>,
    exchange: u64,
}

impl Key {
    /// The key of `exchange` on `connection` and `quic_stream`.
    fn new(connection: u64, quic_stream: Option<u64>, exchange: u64) -> Self {
        Key {
            connection,
            quic_stream,
            exchange,
        }
    }

    /// True if a row on `connection` and `quic_stream` with key `exchange`
    /// has this key.
    fn names(self, connection: u64, quic_stream: Option<u64>, exchange: u64) -> bool {
        (connection, exchange) == (self.connection, self.exchange)
            && same_stream(quic_stream, self.quic_stream)
    }
}

/// True if two rows on one connection are on the same stream incarnation:
/// both on one QUIC stream, or both on a tier whose rows name none. Every
/// join checks it.
fn same_stream(a: Option<u64>, b: Option<u64>) -> bool {
    a == b
}

impl fmt::Display for Key {
    /// Writes its stream incarnation, as `connection 1 (QUIC stream 4)`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "connection {}{}", self.connection, on(self.quic_stream))
    }
}

/// ` (QUIC stream 4)` for a row on QUIC stream 4, or nothing for a row with
/// none.
fn on(quic_stream: Option<u64>) -> String {
    quic_stream.map_or(String::new(), |quic| format!(" (QUIC stream {quic})"))
}
