//! Judging stages (M5-E1): where the node's reader has the bytes that decide
//! a peer frame's label, so `FrameJudged` goes there.
//!
//! A stateless header fault (an undeclared type, a flag bit, a payload
//! outside its row's bounds) is decided by the 8 header bytes, so its frame
//! is judged at `header-read`. A response or ending for a subscription the
//! node opened may also be decided by the reader's state before the
//! payload: no live subscription, or a page past the credit the reader can
//! still reserve. Whether it is comes from [`NodeView`], the reader's
//! credit ledger rebuilt from the node's own `regulated` transitions in the
//! order they happened, and read at the frame's `header-checked`
//! checkpoint. Everything else is judged at `frame-read`.
//!
//! The ledger follows the regulation specification's precheck, not the
//! production check: it never reads a checkpoint's verdict, only the
//! transitions' amounts and order.

use std::collections::BTreeMap;

use zakura_network::zakura::{MessageRole, MessageRule};

use crate::{
    model::{FrameHeader, FrameModel, Label, Labels, ReaderEnd, Totals},
    observe::TransitionFact,
};

/// The node's view of one subscription it opened: what it granted and what
/// the pages it claimed spent.
#[derive(Clone, Copy, Debug, Default)]
struct Opened {
    granted: Totals,
    consumed: Totals,
}

impl Opened {
    /// The credit no claimed page has spent.
    fn unspent(self) -> Totals {
        Totals {
            objects: self.granted.objects.saturating_sub(self.consumed.objects),
            bytes: self.granted.bytes.saturating_sub(self.consumed.bytes),
        }
    }
}

/// The reader's ledger on one header stream: the subscriptions the node
/// opened and their credit, and the pages the reader reserved that the
/// service hasn't claimed yet, one object and their declared bytes each
/// (regulation specification, subscriber precheck).
#[derive(Clone, Debug, Default)]
pub(crate) struct NodeView {
    live: BTreeMap<u64, Opened>,
    queued: Totals,
}

impl NodeView {
    /// Applies the node's `transition`. A transition of the subscriptions
    /// the peer opened on the node changes nothing here. A claimed page
    /// with no single live subscription to charge is an error, since the
    /// ledger can't place it.
    pub(crate) fn apply(&mut self, transition: &TransitionFact) -> Result<(), String> {
        match *transition {
            TransitionFact::OpenIssued {
                subscription,
                objects,
                bytes,
            } => {
                self.live.insert(
                    subscription,
                    Opened {
                        granted: Totals {
                            objects: objects.into(),
                            bytes: bytes.into(),
                        },
                        consumed: Totals::default(),
                    },
                );
            }
            TransitionFact::GrantIssued {
                subscription,
                objects,
                bytes,
            } => {
                if let Some(opened) = self.live.get_mut(&subscription) {
                    opened.granted.objects += u64::from(objects);
                    opened.granted.bytes += u64::from(bytes);
                }
            }
            TransitionFact::OpenRetracted { subscription }
            | TransitionFact::EndClaimed { subscription } => {
                self.live.remove(&subscription);
            }
            TransitionFact::PageClaimed { objects, bytes, .. } => {
                let mut live = self.live.values_mut();
                let (Some(opened), None) = (live.next(), live.next()) else {
                    return Err(format!(
                        "AmbiguousClaim: the node claimed a page with {} live subscriptions",
                        self.live.len()
                    ));
                };
                opened.consumed.objects += u64::from(objects);
                opened.consumed.bytes += bytes;
                self.queued.objects = self.queued.objects.saturating_sub(1);
                self.queued.bytes = self.queued.bytes.saturating_sub(bytes);
            }
            // The checkpoint itself is read by the caller, and the rest are
            // the peer's subscriptions on the node.
            TransitionFact::HeaderChecked { .. }
            | TransitionFact::PublicationEnded { .. }
            | TransitionFact::OpenApplied { .. }
            | TransitionFact::CloseApplied { .. }
            | TransitionFact::GrantApplied { .. } => {}
        }
        Ok(())
    }

    /// The fault the reader can decide from `row`'s header with
    /// `payload_len` declared, as the ledger stands at the header's
    /// checkpoint: `Unsolicited` with no live subscription, and for a page,
    /// `OverCredit` if no live subscription has an object and
    /// `payload_len` bytes unspent, or if reserving it would pass what all
    /// of them have unspent. `None` if the payload decides.
    pub(crate) fn decidable_at_header(
        &self,
        row: &MessageRule,
        payload_len: usize,
    ) -> Option<Label> {
        let MessageRole::Response { ends_exchange, .. } = row.role else {
            return None;
        };
        if self.live.is_empty() {
            return Some(Label::Unsolicited);
        }
        if ends_exchange {
            return None;
        }
        let len = u64::try_from(payload_len).unwrap_or(u64::MAX);
        let fits = |unspent: Totals| unspent.objects >= 1 && unspent.bytes >= len;
        if !self.live.values().any(|opened| fits(opened.unspent())) {
            return Some(Label::OverCredit);
        }
        let unspent = self
            .live
            .values()
            .fold(Totals::default(), |sum, opened| Totals {
                objects: sum.objects.saturating_add(opened.unspent().objects),
                bytes: sum.bytes.saturating_add(opened.unspent().bytes),
            });
        let objects = self.queued.objects.saturating_add(1);
        let bytes = self.queued.bytes.saturating_add(len);
        (objects > unspent.objects || bytes > unspent.bytes).then_some(Label::OverCredit)
    }

    /// Reserves a page the ledger admits at its header, as the reader
    /// does: one object and its declared bytes until the service claims
    /// it.
    pub(crate) fn reserve(&mut self, row: &MessageRule, payload_len: usize) {
        let page = matches!(
            row.role,
            MessageRole::Response {
                ends_exchange: false,
                ..
            }
        );
        if page && self.decidable_at_header(row, payload_len).is_none() {
            self.queued.objects += 1;
            self.queued.bytes += u64::try_from(payload_len).unwrap_or(u64::MAX);
        }
    }
}

/// Where a frame's judgement waits after its header arrives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stage {
    /// Judged at `header-read`: its header alone has a fault.
    Header,
    /// Its row is a response, so it waits for its `header-checked`
    /// checkpoint, where the ledger decides between the header and the
    /// payload.
    Checkpoint,
    /// Judged at `frame-read`.
    Frame,
}

/// The stage a frame with `header` on a reader of `rules` waits for, and
/// its row there, from the header alone.
pub(crate) fn header_stage(
    rules: &'static [MessageRule],
    header: &FrameHeader,
) -> (Stage, Option<&'static MessageRule>) {
    let model = FrameModel::new(rules, ReaderEnd::Persistent);
    let row = model.row(header.message_type);
    if model.labels(header) != Labels::NONE {
        return (Stage::Header, row);
    }
    match row.map(|row| row.role) {
        Some(MessageRole::Response { .. }) => (Stage::Checkpoint, row),
        _ => (Stage::Frame, row),
    }
}

#[cfg(test)]
mod tests {
    use zakura_network::zakura::v9::RegtestMessage;

    use super::*;

    /// The Regtest row named `name`.
    fn row(name: &str) -> &'static MessageRule {
        RegtestMessage::ROWS
            .iter()
            .find(|row| row.name == name)
            .expect("header sync has the row")
    }

    /// A ledger after the node opened subscription 1 with two objects and
    /// 600 bytes.
    fn opened() -> NodeView {
        let mut view = NodeView::default();
        view.apply(&TransitionFact::OpenIssued {
            subscription: 1,
            objects: 2,
            bytes: 600,
        })
        .unwrap();
        view
    }

    /// With no subscription, a page and an ending are unsolicited at their
    /// headers. With one, an ending waits for its payload, and a page waits
    /// while it fits, but not once the reserved pages ahead of it spent
    /// the objects, or once its declared bytes pass the credit.
    #[test]
    fn the_ledger_decides_response_headers() {
        let (page, ending) = (row("headers"), row("headers-outcome"));
        let empty = NodeView::default();
        assert_eq!(
            empty.decidable_at_header(page, 263),
            Some(Label::Unsolicited)
        );
        assert_eq!(
            empty.decidable_at_header(ending, 41),
            Some(Label::Unsolicited)
        );
        let mut view = opened();
        assert_eq!(view.decidable_at_header(ending, 41), None);
        assert_eq!(view.decidable_at_header(page, 601), Some(Label::OverCredit));
        assert_eq!(view.decidable_at_header(page, 263), None);
        view.reserve(page, 263);
        view.reserve(page, 263);
        assert_eq!(
            view.decidable_at_header(page, 263),
            Some(Label::OverCredit),
            "a third page against two reserved objects"
        );
        view.apply(&TransitionFact::PageClaimed {
            seq: Some(1),
            objects: 1,
            bytes: 263,
        })
        .unwrap();
        assert_eq!(
            view.decidable_at_header(page, 263),
            Some(Label::OverCredit),
            "claiming moves the charge, it frees nothing"
        );
        view.apply(&TransitionFact::EndClaimed { subscription: 1 })
            .unwrap();
        assert_eq!(
            view.decidable_at_header(ending, 41),
            Some(Label::Unsolicited),
            "an ending after the end is claimed"
        );
    }

    /// A stateless fault is judged at its header on any row, a response
    /// waits for its checkpoint, and any other frame for its payload.
    #[test]
    fn the_header_picks_the_stage() {
        let rules = &RegtestMessage::ROWS;
        let header = |message_type, flags, payload_len| FrameHeader {
            message_type,
            flags,
            payload_len,
        };
        let status = row("status").payload.min();
        assert_eq!(header_stage(rules, &header(1, 0, status)).0, Stage::Frame);
        assert_eq!(header_stage(rules, &header(1, 4, status)).0, Stage::Header);
        assert_eq!(header_stage(rules, &header(0, 0, status)).0, Stage::Header);
        assert_eq!(header_stage(rules, &header(3, 0, 263)).0, Stage::Checkpoint);
        assert_eq!(header_stage(rules, &header(3, 0, 262)).0, Stage::Header);
    }
}
