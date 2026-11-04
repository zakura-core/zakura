//! The request/response model: one request row and the rows that answer it.

use std::collections::BTreeMap;

use zakura_network::zakura::{MessageRole, MessageRule};

use super::{ExchangeKey, FrameHeader, Label, Labels, SessionId};

/// What an open exchange may still receive: frames before its ending, and
/// payload bytes with the ending's included.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Owed {
    /// Frames before the ending.
    pub frames: u32,
    /// Payload bytes, the ending's included.
    pub bytes: u64,
}

/// One session's requests to the node.
#[derive(Clone, Copy, Debug, Default)]
struct Served {
    /// The highest limit the node advertised, capped by `max_in_flight`.
    limit: u32,
    /// Requests the node has not answered.
    open: u32,
}

/// Labels the frames of one request row's exchanges: `OverMargin` for
/// requests, and `Unsolicited` and `OverOwed` for responses.
///
/// An exchange closes only on its ending or on its session's end. A local
/// timeout never closes it, so a late response within what the exchange owes
/// is `Conformant`.
#[derive(Clone, Debug)]
pub struct ExchangeModel {
    request: &'static MessageRule,
    max_in_flight: u32,
    responses: Vec<&'static MessageRule>,
    /// The largest ending's payload. Every exchange owes room for it.
    ending_bytes: u64,
    open: BTreeMap<(SessionId, ExchangeKey), Owed>,
    served: BTreeMap<SessionId, Served>,
}

impl ExchangeModel {
    /// The model of `rules`' request row `request_type`, or `None` if that
    /// row isn't a request.
    pub fn new(rules: &'static [MessageRule], request_type: u16) -> Option<Self> {
        let request = rules.iter().find(|row| row.message_type == request_type)?;
        let MessageRole::Request { max_in_flight, .. } = request.role else {
            return None;
        };
        let responses: Vec<_> = rules
            .iter()
            .filter(|row| {
                matches!(row.role, MessageRole::Response { request, .. } if request == request_type)
            })
            .collect();
        let ending_bytes = responses
            .iter()
            .filter(|row| {
                matches!(
                    row.role,
                    MessageRole::Response {
                        ends_exchange: true,
                        ..
                    }
                )
            })
            .map(|row| u64::try_from(row.payload.max()).unwrap_or(u64::MAX))
            .max()
            .unwrap_or(0);
        Some(ExchangeModel {
            request,
            max_in_flight,
            responses,
            ending_bytes,
            open: BTreeMap::new(),
            served: BTreeMap::new(),
        })
    }

    /// The request row.
    pub fn request(&self) -> &'static MessageRule {
        self.request
    }

    /// The node advertised `limit` on `session`. The enforced limit is the
    /// highest advertised, at least one and at most `max_in_flight`.
    pub fn advertise(&mut self, session: SessionId, limit: u32) {
        let served = self.served.entry(session).or_default();
        served.limit = served.limit.max(limit.clamp(1, self.max_in_flight));
    }

    /// Labels a request from `session`'s peer, and opens it if conformant.
    /// A request beyond twice the limit is `OverMargin`; one within the
    /// margin is conformant, because a lowered limit can cross requests in
    /// flight.
    ///
    /// # Panics
    ///
    /// If the node hasn't advertised a limit on `session`.
    pub fn request_frame(&mut self, session: SessionId) -> Labels {
        let served = self
            .served
            .get_mut(&session)
            .expect("the node advertises a limit when the session starts");
        let open = served.open.saturating_add(1);
        if open > served.limit.saturating_mul(2) {
            return Labels::of(Label::OverMargin);
        }
        served.open = open;
        Labels::NONE
    }

    /// The node wrote the ending of one answer on `session`. False if none
    /// is open.
    pub fn answered(&mut self, session: SessionId) -> bool {
        let Some(served) = self.served.get_mut(&session).filter(|s| s.open > 0) else {
            return false;
        };
        served.open -= 1;
        true
    }

    /// Requests from `session`'s peer that the node hasn't answered.
    pub fn serving(&self, session: SessionId) -> u32 {
        self.served.get(&session).map_or(0, |served| served.open)
    }

    /// The node sent a request keyed `key` on `session`, owed `owed`. The
    /// owed bytes cover at least the largest ending. False if `key` is open.
    pub fn sent(&mut self, session: SessionId, key: ExchangeKey, owed: Owed) -> bool {
        if self.open.contains_key(&(session, key)) {
            return false;
        }
        let owed = Owed {
            bytes: owed.bytes.max(self.ending_bytes),
            ..owed
        };
        self.open.insert((session, key), owed);
        true
    }

    /// Labels a response frame for `key` on `session`, and charges it to the
    /// exchange if conformant. An ending closes the exchange.
    pub fn response_frame(
        &mut self,
        session: SessionId,
        key: ExchangeKey,
        header: &FrameHeader,
    ) -> Labels {
        let row = self
            .responses
            .iter()
            .find(|row| row.message_type == header.message_type);
        let (Some(row), Some(owed)) = (row, self.open.get_mut(&(session, key))) else {
            return Labels::of(Label::Unsolicited);
        };
        let ends = matches!(
            row.role,
            MessageRole::Response {
                ends_exchange: true,
                ..
            }
        );
        let bytes = u64::try_from(header.payload_len).unwrap_or(u64::MAX);
        if bytes > owed.bytes || (!ends && owed.frames == 0) {
            return Labels::of(Label::OverOwed);
        }
        if ends {
            self.open.remove(&(session, key));
        } else {
            owed.frames -= 1;
            owed.bytes -= bytes;
        }
        Labels::NONE
    }

    /// What `key` on `session` still owes, if it is open.
    pub fn owed(&self, session: SessionId, key: ExchangeKey) -> Option<Owed> {
        self.open.get(&(session, key)).copied()
    }

    /// Open exchanges, in key order.
    pub fn open(&self) -> impl Iterator<Item = (SessionId, ExchangeKey, Owed)> + '_ {
        self.open
            .iter()
            .map(|(&(session, key), &owed)| (session, key, owed))
    }

    /// `session` ended: every exchange on it closes.
    pub fn end_session(&mut self, session: SessionId) {
        self.open.retain(|(at, _), _| *at != session);
        self.served.remove(&session);
    }
}
