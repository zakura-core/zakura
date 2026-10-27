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
//! each a violation of their own.

use std::collections::BTreeMap;

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
    let row = RowAt::new(id.stream, id.row.expect("downstream templates are per row"));
    let entries = cx.log.entries();
    let start = entries.partition_point(|e| e.step < cx.step);
    let mut verdict = Verdict::NotApplicable;
    let mut violations = Vec::new();
    for at in start..entries.len() {
        match effect(entries, at, &row) {
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

/// `receiver-work-authorized`: work the node admits from frames of `row`
/// for an exchange must come from frames of `row` with that exchange's key
/// that the peer's models labelled `Conformant` and the node's reader
/// judged, on a connection still open, and its bytes stay within theirs.
pub(crate) fn work_authorized(entries: &[Entry], at: usize, row: &RowAt) -> Found {
    let Observation::WorkAdmitted {
        connection,
        frame,
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
    let what = format!("connection {connection}'s work from {row} frames of exchange {exchange}");
    if closed(before, *connection) {
        return Some(Err(Failure::new(
            format!("{what} stops when the connection closes"),
            "the node admitted it after the close",
        )));
    }
    let delivered = conformant_judged(before, *connection, row, *exchange).bytes;
    let mut admitted = Quantity::default();
    for entry in &entries[..=at] {
        if let Observation::WorkAdmitted {
            connection: c,
            frame: f,
            exchange: e,
            objects,
            bytes,
            ..
        } = &entry.obs
        {
            if (c, f, e) == (connection, frame, exchange) {
                admitted.add(*objects, *bytes);
            }
        }
    }
    Some(if admitted.bytes <= delivered {
        Ok(())
    } else {
        Err(Failure::new(
            format!("{what} stays within the {delivered} bytes conformant frames delivered"),
            format!("the node admitted {} bytes", admitted.bytes),
        ))
    })
}

/// `receiver-credit-conserved`: work the node admits for a subscription of
/// `row`, from all its page rows, stays within the credit the node granted
/// before it, in objects and in bytes.
pub(crate) fn credit_conserved(entries: &[Entry], at: usize, row: &RowAt) -> Found {
    let Observation::WorkAdmitted {
        connection,
        request,
        exchange,
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
            } if (c, subscription, e) == (connection, request, exchange) => {
                granted.add(*objects, *bytes);
            }
            Observation::WorkAdmitted {
                connection: c,
                request: r,
                exchange: e,
                objects,
                bytes,
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

/// `receiver-serving-bounded`: the node serves a request of `row` only for
/// a request frame with its key that the peer's models labelled
/// `Conformant` and the reader judged, on a connection still open, and
/// serves each such frame at most once. Conformant requests stay within the
/// receiver's margin, so its serving owners do too.
pub(crate) fn serving_bounded(entries: &[Entry], at: usize, row: &RowAt) -> Found {
    let Observation::Serving {
        connection,
        request,
        exchange,
    } = &entries[at].obs
    else {
        return None;
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
    let requested = conformant_judged(before, *connection, row, *exchange).frames;
    let served = entries[..=at]
        .iter()
        .filter(|entry| {
            matches!(&entry.obs, Observation::Serving { connection: c, request: r, exchange: e }
                if (c, r, e) == (connection, request, exchange))
        })
        .count();
    let served = u64::try_from(served).unwrap_or(u64::MAX);
    Some(if served <= requested {
        Ok(())
    } else {
        Err(Failure::new(
            format!("{what} is served once per conformant request, {requested} so far"),
            format!("the node started serving it {served} times"),
        ))
    })
}

/// `sender-within-response-quantity`: each response the node writes with a
/// frame of `row` answers an exchange the peer opened and hasn't seen end,
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

/// Frames and their payload bytes.
#[derive(Clone, Copy, Debug, Default)]
struct Judged {
    frames: u64,
    bytes: u64,
}

/// The frames of `row` with key `exchange` on `connection` in `entries`
/// that the peer's models labelled `Conformant` and the node's reader then
/// judged.
fn conformant_judged(entries: &[Entry], connection: u64, row: &RowAt, exchange: u64) -> Judged {
    let mut sent = BTreeMap::new();
    let mut found = Judged::default();
    for entry in entries {
        match &entry.obs {
            Observation::PeerFrame {
                connection: c,
                stream,
                seq,
                row: Some(r),
                label: Label::Conformant,
                exchange: Some(e),
                payload,
                ..
            } if *c == connection && *stream == row.stream && *r == row.row && *e == exchange => {
                sent.insert(*seq, *payload);
            }
            Observation::FrameJudged {
                connection: c,
                stream,
                seq,
            } if *c == connection && *stream == row.stream => {
                if let Some(payload) = sent.remove(seq) {
                    found.frames += 1;
                    found.bytes = found.bytes.saturating_add(payload);
                }
            }
            _ => {}
        }
    }
    found
}
