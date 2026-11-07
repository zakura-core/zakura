//! A subscription family with room for more than one live subscription.
//! Header sync version 9 allows one, which never exercises tombstone
//! trimming.

use zakura_network::zakura::{Credit, MessageRole, MessageRule, PayloadLen, Stream};

/// The subscription row's message type.
pub const WATCH: u16 = 1;
/// The page row's message type.
pub const ITEM: u16 = 2;
/// The terminal outcome's message type.
pub const ENDED: u16 = 3;

/// The row credit: four objects and 64 bytes.
pub const CREDIT: Credit = Credit {
    objects: 4,
    bytes: 64,
};

/// `watch`, with two live subscriptions; `item` pages of 1 to 32 bytes; and
/// the `ended` terminal outcome.
pub const FEED: &[MessageRule] = &[
    MessageRule {
        name: "watch",
        message_type: WATCH,
        payload: PayloadLen::exact(8),
        role: MessageRole::Subscription {
            max_live: 2,
            credit: CREDIT,
            cursor_history: 4,
            cadence: None,
        },
    },
    MessageRule {
        name: "item",
        message_type: ITEM,
        payload: PayloadLen::between(1, 32),
        role: MessageRole::Response {
            request: WATCH,
            ends_exchange: false,
        },
    },
    MessageRule {
        name: "ended",
        message_type: ENDED,
        payload: PayloadLen::exact(4),
        role: MessageRole::Response {
            request: WATCH,
            ends_exchange: true,
        },
    },
];

const _: () = Stream::validate_layout(&[Stream {
    kind: 64,
    version: 1,
    frame_cap: 1024,
    capability: 1 << 16,
    messages: Some(FEED),
    ..Stream::PERSISTENT
}]);
