//! Sender generated checks (properties.md, Generated properties, per row,
//! sender side).
//!
//! These check the node's own frames as a peer's role models labelled them
//! ([`Observation::NodeFrame`]), under the sender's rules. Each
//! template reads the frames whose rule is its row: a page's `OverCredit`
//! belongs to its subscription row, as [`crate::table::rule_row`] picks it.

use zakura_network::zakura::Stream;

use crate::{
    downstream::{Failure, Found},
    model::Label,
    observe::{Entry, RowAt, StreamId, TransitionFact, TransportFact},
    table::{is_page_of, request_of, GeneratedId, Template},
    Observation,
};

/// Sender template `template`'s verdict on `obs`: `None` unless `obs` is a
/// frame the node sent whose rule is `row`.
pub(crate) fn frame(template: Template, obs: &Observation, row: &RowAt) -> Found {
    let Observation::NodeFrame {
        connection,
        seq,
        row: Some(rule_row),
        rule_stream,
        label,
        exhausted,
        ..
    } = obs
    else {
        return None;
    };
    if *rule_stream != row.stream || *rule_row != row.row {
        return None;
    }
    let broken = match template {
        Template::SenderCadence => *exhausted,
        template => Some(*label) == broken_by(template),
    };
    if !broken {
        return Some(Ok(()));
    }
    let what = format!("connection {connection}'s {row} frame {seq} from the node");
    let actual = match template {
        Template::SenderCadence => "the peer charged it to an empty cadence bucket".to_owned(),
        _ => format!("the peer's models labelled it {label}"),
    };
    Some(Err(Failure::new(
        format!("{what} is conformant under {}", template.claim()),
        actual,
    )))
}

/// The label that breaks sender template `template`, if it reads one.
fn broken_by(template: Template) -> Option<Label> {
    match template {
        Template::SenderWithinLimit => Some(Label::OverMargin),
        Template::SenderAnswersOpen | Template::SenderEndsOnce => Some(Label::Unsolicited),
        Template::SenderWithinOwed => Some(Label::OverOwed),
        Template::SenderWithinLive => Some(Label::OverLive),
        Template::SenderWithinCredit => Some(Label::OverCredit),
        _ => None,
    }
}

/// True if `entries` reach sender template `id`'s trigger history over the
/// streams of `layout`: the node sending a frame of the exchange its rule
/// guards (C7), not just any frame of the row.
///
/// - `sender-within-limit` and `sender-cadence`: a frame the node sent on
///   the row.
/// - `sender-within-live`: the node opening or updating a subscription on
///   the row, so a frame of the row that isn't a page past its credit.
/// - `sender-answers-open`, `sender-ends-once`, `sender-within-owed` and
///   `sender-within-response-quantity`: a frame the node sent on the
///   response row after it took up the peer's exchange of the row's request
///   row ([`request_of`]). For the first two, a response the peer's models
///   label `Unsolicited` reaches it alone, since the missing exchange is
///   the violation.
/// - `sender-within-credit`: a page the node sent for the subscription row
///   ([`is_page_of`]) after it took up the peer's `Open` or `Grant`, so the
///   node's own update alone doesn't reach it.
///
/// "Took up" is logged before the node started writing its frame (its
/// `FrameWriteStarted`, or the frame itself on a link with no transport
/// events), on the node frame's connection and QUIC stream. For a node frame
/// that names its exchange (header sync's subscription ID), it's the node's
/// own `Regulated` record of applying the peer's `Open` or `Grant` of that
/// subscription, so a grant the peer offered but the node hadn't applied
/// doesn't count. A discovery frame names no exchange, since its exchanges
/// run in order on their stream, so there it's the peer's conformant frame
/// of the request row itself, not a page its rule redirects there or one
/// the node must punish, that the node read (its `FrameRead`), so a
/// request the peer only offered doesn't count. On a link with no transport
/// events (block sync's relay), the read is the `ExchangeOwed` the relay's
/// model logged for the request row after it, of the exchange the frame's
/// `NodeOutput` names, and not yet ended by an earlier response.
pub(crate) fn trigger(layout: &[Stream], id: &GeneratedId<'_>, entries: &[Entry]) -> bool {
    let Some(row) = id.row else {
        return false;
    };
    entries.iter().enumerate().any(|(at, entry)| {
        let Observation::NodeFrame {
            connection,
            quic_stream,
            seq,
            row: sent_row,
            rule_stream,
            label,
            exchange,
            ..
        } = &entry.obs
        else {
            return false;
        };
        let on_row = *rule_stream == id.stream && sent_row.as_deref() == Some(row);
        // The node started writing this frame here, as far as the log shows.
        let started = entries[..at]
            .iter()
            .rposition(|before| {
                matches!(&before.obs, Observation::Transport {
                    connection: Some(c),
                    event: TransportFact::FrameWriteStarted { quic_stream: q, seq: s, .. },
                    ..
                } if c == connection && Some(*q) == *quic_stream && s == seq)
            })
            .unwrap_or(at);
        // The exchange this frame's `NodeOutput` names, which a relay with
        // no transport events logs after the frame and before the
        // connection's next node frame.
        let output = || {
            entries[at + 1..]
                .iter()
                .map(|after| &after.obs)
                .take_while(|obs| {
                    !matches!(obs, Observation::NodeFrame { connection: c, .. } if c == connection)
                })
                .find_map(|obs| match obs {
                    Observation::NodeOutput {
                        connection: c,
                        exchange,
                        ..
                    } if c == connection => Some(*exchange),
                    _ => None,
                })
        };
        // The peer's exchange the node had taken up before it started
        // writing, on this connection and QUIC stream. For a frame that
        // names its exchange, the node's own record of applying the peer's
        // `Open` or `Grant` of it. For one that names none, the peer's
        // conformant frame of `stream`'s `row`.
        let opened = |stream: StreamId, row: &str| match exchange {
            Some(key) => entries[..started].iter().any(|before| {
                matches!(&before.obs, Observation::Transport {
                    connection: Some(c),
                    event: TransportFact::Regulated {
                        quic_stream: q,
                        transition: TransitionFact::OpenApplied { subscription, .. }
                            | TransitionFact::GrantApplied { subscription, .. },
                    },
                    ..
                } if c == connection && Some(*q) == *quic_stream && subscription == key)
            }),
            None => entries[..started].iter().enumerate().any(|(sent, before)| {
                let Observation::PeerFrame {
                    connection: c,
                    quic_stream: q,
                    seq: request,
                    rule_stream: s,
                    row: Some(r),
                    label: Label::Conformant,
                    ..
                } = &before.obs
                else {
                    return false;
                };
                // The node read the request before it started writing. A
                // link with no transport events (block sync's relay) logs
                // no read, so there it's the exchange the relay's model
                // opened for the request row, as the sender's labels count
                // it: the one this frame's `NodeOutput` names, not yet ended.
                c == connection
                    && q == quic_stream
                    && *s == stream
                    && r == row
                    && (sent..started).any(|read| match &entries[read].obs {
                        Observation::Transport {
                            connection: Some(c),
                            event:
                                TransportFact::FrameRead {
                                    quic_stream: q,
                                    seq: n,
                                },
                            ..
                        } => c == connection && Some(*q) == *quic_stream && n == request,
                        Observation::ExchangeOwed {
                            connection: c,
                            request: owed,
                            exchange: key,
                            ..
                        } => {
                            quic_stream.is_none()
                                && c == connection
                                && owed.stream == stream
                                && owed.row == row
                                && output() == Some(*key)
                                && !entries[read..at].iter().any(|later| {
                                    matches!(&later.obs, Observation::NodeOutput {
                                        connection: c,
                                        exchange: k,
                                        ends: true,
                                        ..
                                    } if c == connection && k == key)
                                })
                        }
                        _ => false,
                    })
            }),
        };
        match id.template {
            Template::SenderWithinLimit | Template::SenderCadence => on_row,
            Template::SenderWithinLive => on_row && *label != Label::OverCredit,
            // A response with no open exchange is the violation these two
            // catch, so it reaches its own trigger.
            Template::SenderAnswersOpen | Template::SenderEndsOnce
                if on_row && *label == Label::Unsolicited =>
            {
                true
            }
            Template::SenderAnswersOpen
            | Template::SenderEndsOnce
            | Template::SenderWithinOwed
            | Template::SenderWithinResponseQuantity => {
                on_row
                    && request_of(layout, id.stream, row)
                        .is_some_and(|(stream, request)| opened(stream, request))
            }
            Template::SenderWithinCredit => {
                is_page_of(
                    layout,
                    (*rule_stream, sent_row.as_deref(), *label),
                    (id.stream, row),
                ) && opened(id.stream, row)
            }
            _ => false,
        }
    })
}
