//! What a layout's tables say about its exchanges.

use crate::zakura::{regulation::ResponseCap, MessageRole, MessageRule, PayloadLen, Stream};

/// One request row's exchange, as the layout's tables declare it.
#[derive(Copy, Clone, Debug)]
pub(crate) struct RequestPlan {
    pub(crate) row: &'static MessageRule,
    pub(crate) max_in_flight: u32,
    /// The member that carries the request.
    pub(crate) stream: usize,
    /// The member that carries the responses.
    pub(crate) response_stream: usize,
    /// The first response row that does not end the exchange, if any.
    pub(crate) part: Option<&'static MessageRule>,
    /// The first response row that ends the exchange.
    pub(crate) end: &'static MessageRule,
}

impl RequestPlan {
    /// The harness's response bound: one part, if the row has one, then the
    /// ending. The server and the client both use it.
    pub(crate) fn cap(&self) -> ResponseCap {
        let part = self.part.map_or(0, |part| max_payload(part.payload));
        ResponseCap {
            frames: u32::from(self.part.is_some()),
            bytes: part,
        }
    }
}

fn max_payload(payload: PayloadLen) -> u64 {
    // Widening usize to u64 is lossless on supported targets.
    payload.max() as u64
}

/// The exchanges and members of one layout.
#[derive(Debug)]
pub(crate) struct LayoutPlan {
    pub(crate) layout: &'static [Stream],
    pub(crate) requests: Vec<RequestPlan>,
    /// Every response row, with the member that carries it.
    pub(crate) responses: Vec<(&'static MessageRule, usize)>,
}

impl LayoutPlan {
    /// Read `layout`'s tables. Every member must declare one.
    pub(crate) fn new(layout: &'static [Stream]) -> Self {
        let rows = || {
            layout.iter().enumerate().flat_map(|(stream, member)| {
                member
                    .messages
                    .expect("a conformance layout declares a table on every member")
                    .iter()
                    .map(move |row| (row, stream))
            })
        };
        let responses: Vec<_> = rows()
            .filter(|(row, _)| matches!(row.role, MessageRole::Response { .. }))
            .collect();
        let requests = rows()
            .filter_map(|(row, stream)| {
                let MessageRole::Request { max_in_flight, .. } = row.role else {
                    return None;
                };
                let answers = |ends: bool| {
                    responses.iter().find(|(response, _)| {
                        matches!(
                            response.role,
                            MessageRole::Response { request, ends_exchange }
                                if request == row.message_type && ends_exchange == ends
                        )
                    })
                };
                let &(end, response_stream) = answers(true)
                    .expect("the layout validator requires an ending for every request");
                let part = answers(false).map(|&(part, part_stream)| {
                    assert_eq!(
                        part_stream, response_stream,
                        "the harness serves each response on one member"
                    );
                    part
                });
                Some(RequestPlan {
                    row,
                    max_in_flight,
                    stream,
                    response_stream,
                    part,
                    end,
                })
            })
            .collect();
        Self {
            layout,
            requests,
            responses,
        }
    }

    /// The row of `message_type`, and the member that carries it.
    pub(crate) fn row(&self, message_type: u16) -> Option<(&'static MessageRule, usize)> {
        self.layout.iter().enumerate().find_map(|(stream, member)| {
            MessageRule::find(member.messages?, message_type).map(|row| (row, stream))
        })
    }
}
