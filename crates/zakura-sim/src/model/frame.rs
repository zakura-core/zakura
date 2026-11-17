//! The header model: one stream's table and the reader's end.

use zakura_network::zakura::{MessageRole, MessageRule};

use super::{Label, Labels};

/// A frame header: the facts a reader checks before the payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FrameHeader {
    /// The message type.
    pub message_type: u16,
    /// The flag bits. No row defines one.
    pub flags: u16,
    /// The payload length, frame header excluded.
    pub payload_len: usize,
}

/// The end of a stream that reads a frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReaderEnd {
    /// A persistent stream's reader: every row.
    Persistent,
    /// The responder's end of a request/response stream: request rows.
    Responder,
    /// The requester's end of a request/response stream: response rows.
    Requester,
}

impl ReaderEnd {
    /// True if this end reads frames of `role`.
    fn reads(self, role: MessageRole) -> bool {
        match self {
            ReaderEnd::Persistent => true,
            ReaderEnd::Responder => matches!(role, MessageRole::Request { .. }),
            ReaderEnd::Requester => matches!(role, MessageRole::Response { .. }),
        }
    }
}

/// Labels frame headers on one reader: `UndeclaredType`, `ReservedFlags`,
/// `ShortPayload`, and `LongPayload`. The labels need no state.
#[derive(Clone, Copy, Debug)]
pub struct FrameModel {
    rules: &'static [MessageRule],
    reader: ReaderEnd,
}

impl FrameModel {
    /// The model of `reader` on a stream whose table is `rules`.
    pub fn new(rules: &'static [MessageRule], reader: ReaderEnd) -> Self {
        FrameModel { rules, reader }
    }

    /// The row `reader` reads for `message_type`, if one exists.
    pub fn row(&self, message_type: u16) -> Option<&'static MessageRule> {
        self.rules
            .iter()
            .find(|row| row.message_type == message_type && self.reader.reads(row.role))
    }

    /// Every header fault of `header`.
    pub fn labels(&self, header: &FrameHeader) -> Labels {
        let Some(row) = self.row(header.message_type) else {
            // Neither flags nor lengths have a row to break.
            return Labels::of(Label::UndeclaredType)
                .with_if(header.flags != 0, Label::ReservedFlags);
        };
        Labels::NONE
            .with_if(header.flags != 0, Label::ReservedFlags)
            .with_if(header.payload_len < row.payload.min(), Label::ShortPayload)
            .with_if(header.payload_len > row.payload.max(), Label::LongPayload)
    }

    /// The headers a peer draws for `row`: conformant lengths at both bounds
    /// and between them, each length fault, a flag bit, and a type no row
    /// declares.
    pub fn alphabet(&self, row: &MessageRule) -> Vec<FrameHeader> {
        let header = |message_type, flags, payload_len| FrameHeader {
            message_type,
            flags,
            payload_len,
        };
        let (min, max) = (row.payload.min(), row.payload.max());
        let mut headers = vec![
            header(row.message_type, 0, min),
            header(row.message_type, 0, min + (max - min) / 2),
            header(row.message_type, 0, max),
            header(row.message_type, 0, max.saturating_add(1)),
            header(row.message_type, 1, min),
            header(self.undeclared_type(), 0, min),
        ];
        if let Some(short) = min.checked_sub(1) {
            headers.push(header(row.message_type, 0, short));
        }
        headers.sort();
        headers.dedup();
        headers
    }

    /// The lowest message type this reader reads no row for.
    pub fn undeclared_type(&self) -> u16 {
        (0..=u16::MAX)
            .find(|message_type| self.row(*message_type).is_none())
            .expect("a table declares fewer than 65,536 rows")
    }
}
