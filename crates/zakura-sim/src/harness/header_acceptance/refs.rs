//! Rule 25's one identity for joins (adopted 2026-11-12).
//!
//! Every assertion hdr-14 writes about a history (the despite-close case's
//! companions, the action judge's control steps, finding attribution and
//! trigger evidence) names what it joins by a [`Ref`]: the full identity
//! the evidence carries for it, never a hand-picked part. That's, for a
//! frame, its connection, stream, direction and sequence ([`FrameRef`],
//! and [`ReadRef`] as its session's reader names it); for a subscription event, its peer, session and
//! subscription ([`SubscriptionRef`], and [`PageRef`] for a page staged on
//! one); and for work, its full owner ([`WorkRef`]). A finding is named by
//! its step ([`FindingRef`]), within a run every artifact names by its
//! trace's digest ([`super::binding`]). A join is [`joined`]:
//! every item whose ref equals the one wanted. Each ref kind's negative
//! controls come from one template, which moves each of its keys in turn
//! (`tests/support/one_key_off.rs`).

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::observe::WorkOwner;

/// A full identity that joins compare whole. Its serialized form's leaves
/// are its keys.
pub trait Ref: Clone + std::fmt::Debug + PartialEq + Serialize + DeserializeOwned {}

/// Every item of `items` whose ref, by `of`, is `want`, in order: the one
/// join, so no assertion matches on less than a whole ref. An item `of`
/// gives no ref is never joined.
pub fn joined<'a, T: 'a, R: Ref>(
    items: impl IntoIterator<Item = &'a T>,
    of: impl Fn(&T) -> Option<R>,
    want: &R,
) -> Vec<&'a T> {
    items
        .into_iter()
        .filter(|item| of(item).as_ref() == Some(want))
        .collect()
}

/// One transport frame.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameRef {
    /// Its connection.
    pub connection: u64,
    /// Its stream's kind.
    pub kind: u16,
    /// Its stream's version.
    pub version: u16,
    /// Its QUIC stream.
    pub quic_stream: u64,
    /// Whether the peer sent it to the node, rather than the node to it.
    pub inbound: bool,
    /// Its position among its direction's frames on the stream.
    pub seq: u64,
}
impl Ref for FrameRef {}

/// One frame as its session's reader names it: the session, and the
/// frame's position among the stream's reads. A session is one connection
/// and QUIC stream, so this names the frame on the reader's side.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadRef {
    /// The session.
    pub session: u64,
    /// The frame's position among its stream's reads.
    pub frame: u64,
}
impl Ref for ReadRef {}

/// One session of one peer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRef {
    /// The peer, in hex as findings spell it.
    pub peer: String,
    /// The session.
    pub session: u64,
}
impl Ref for SessionRef {}

/// One subscription: its peer, session and ID, which is per session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionRef {
    /// The peer, in hex as findings spell it.
    pub peer: String,
    /// The session it's on.
    pub session: u64,
    /// Its ID within that session.
    pub subscription: u64,
}
impl Ref for SubscriptionRef {}

/// One page of headers on a subscription: the subscription, the frame's
/// position among its stream's reads, and the headers in order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageRef {
    /// The subscription.
    pub subscription: SubscriptionRef,
    /// The frame's position among its stream's reads.
    pub frame: u64,
    /// The page's header hashes, in hex, in order.
    pub headers: Vec<String>,
}
impl Ref for PageRef {}

/// One unit of work: its full owner, and the session and request the
/// event names beside it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkRef {
    /// The owner, every field of its typed form.
    pub owner: WorkOwner,
    /// The session the event names.
    pub session_id: u64,
    /// The request the event names.
    pub request_id: u64,
}
impl Ref for WorkRef {}

/// One finding a property reported: its step and its detail.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FindingRef {
    /// The step it was found at.
    pub step: u64,
    /// What it says, as the property's violation's `actual`.
    pub detail: String,
}
impl Ref for FindingRef {}

/// The effect a scripted control step's own execution recorded: the step's
/// position in the control script, the peer it acted on and its action.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlRef {
    /// The step's position in the control script.
    pub control: usize,
    /// The peer.
    pub peer: String,
    /// The action, as `Close`, `Relink` or `Shutdown`.
    pub action: String,
}
impl Ref for ControlRef {}

/// One link to a peer: the peer and the connection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkRef {
    /// The peer, by harness name.
    pub peer: String,
    /// The connection.
    pub connection: u64,
}
impl Ref for LinkRef {}

/// What a witness judged: its property, what it expected and the other
/// IDs it allowed to break beside it, in order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerRef {
    /// The property.
    pub property: String,
    /// `holds`, `not-applicable`, `violated` or `violated-set`.
    pub expect: String,
    /// The IDs beside it.
    pub also_violates: Vec<String>,
}
impl Ref for TriggerRef {}

/// The one-key-off template, shared with the integration tests.
#[cfg(test)]
#[path = "../../../tests/support/one_key_off.rs"]
pub(crate) mod one_key_off;

#[cfg(test)]
mod tests {
    use super::{one_key_off::assert_one_key_off, *};

    /// The template moves every key of a ref, at any depth, and each moved
    /// copy fails a join the ref itself passes: a page's subscription's
    /// peer, session and ID, its frame and each header.
    #[test]
    fn the_template_moves_every_key() {
        let page = PageRef {
            subscription: SubscriptionRef {
                peer: "ab".into(),
                session: 11,
                subscription: 7,
            },
            frame: 3,
            headers: vec!["01".into(), "02".into()],
        };
        let staged = [page.clone()];
        let joins = |want: &PageRef| joined(&staged, |page| Some(page.clone()), want).len() == 1;
        assert_eq!(assert_one_key_off(&page, joins), 6);
        let frame = FrameRef {
            connection: 1,
            kind: 5,
            version: 9,
            quic_stream: 4,
            inbound: true,
            seq: 3,
        };
        assert_eq!(assert_one_key_off(&frame, |want| *want == frame), 6);
    }
}
