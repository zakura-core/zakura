//! The stream conformance suite: the properties every session layout must
//! keep over real QUIC.
//!
//! One line per layout generates the suite:
//!
//! ```ignore
//! stream_conformance_suite!(single_stream_conformance, SINGLE, ExampleConformance);
//! ```
//!
//! The adapter, [`StreamConformance`], only encodes a valid message for a row
//! and names the exchange a message belongs to. The layout's tables supply the
//! rest: which rows are requests, which responses end an exchange,
//! `max_in_flight`, frame caps, queue depths, and write policies.
//!
//! The harness runs real handlers over loopback QUIC with production limits:
//!
//! - [`LayoutNode`] registers a [`LayoutService`], which serves through
//!   `Serve`, downloads through `Reservations` and the writer fence, and keeps
//!   its sessions in a `SessionTable` with `SessionCapacity` slots. Its
//!   serving answers each request with one part, if the request row has a
//!   part row, then one ending.
//! - Two [`SiblingService`]s share each connection. They echo probes, and
//!   they can stop reading to hold their stream windows.
//! - [`RawLayoutPeer`] opens or accepts a layout by hand and can send any
//!   bytes on any member.
//!
//! Every wait is bounded by [`CONFORMANCE_DEADLINE`] and waits for an event.
//! No property sleeps to let something happen.
//!
//! # Properties
//!
//! | Id | Property |
//! | --- | --- |
//! | P1 | An unsolicited response disconnects its sender before decode, and a control peer keeps working. |
//! | P2 | `limit` requests re-sent at each ending never fault; `2 × limit` at once are served and never fault; one more disconnects. |
//! | P3 | Sustained conformant exchanges never fault. |
//! | P4 | Two peers that saturate each other's serving both finish their downloads. |
//! | P5 | Ending one member retires the session, siblings stay usable, and the layout reopens; a started exchange closes the connection instead. |
//! | P6 | Paused siblings up to the supported count do not stop the layout; after connection credit runs out and returns, the original exchange completes. |
//! | P7 | A stream at a version the peers did not negotiate is refused, and the connection stays usable. |
//! | P8 | A reset mid-frame retires only the session; a FIN mid-payload closes the connection; a stalled frame ends at the read deadline. |

mod layout;
mod node;
pub(crate) mod properties;
mod raw;
mod service;
mod sibling;

use std::{fmt::Debug, time::Duration};

pub(crate) use layout::{LayoutPlan, RequestPlan};
pub(crate) use node::LayoutNode;
pub(crate) use raw::RawLayoutPeer;
pub(crate) use service::{LayoutService, LayoutSession};
pub(crate) use sibling::{SiblingService, SIBLINGS};

use crate::zakura::{wire_codec::WireMessage, MessageRule};

/// The bound on every wait in the suite.
pub(crate) const CONFORMANCE_DEADLINE: Duration = Duration::from_secs(30);

/// What the suite needs from a reactor: a valid message per row.
pub(crate) trait StreamConformance: Debug + Send + Sync + 'static {
    /// The reactor's message family.
    type Message: WireMessage<Error: Debug + std::fmt::Display + Send> + Clone + Debug + Send + Sync;

    /// A valid message for `row` that belongs to exchange `exchange`.
    ///
    /// For a request row, the message asks for that exchange. For a response
    /// row, it answers it: a part row is the exchange's only part, and an
    /// ending row ends the exchange after that part.
    fn message(row: &MessageRule, exchange: u32) -> Self::Message;

    /// The exchange a request or response belongs to.
    fn exchange(message: &Self::Message) -> u32;
}

/// Add the stream conformance suite for `$layout` in a module named `$name`.
macro_rules! stream_conformance_suite {
    ($name:ident, $layout:expr, $adapter:ty) => {
        mod $name {
            #[allow(unused_imports)]
            use super::*;
            use $crate::zakura::testkit::stream_conformance::properties;

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn p1_an_unsolicited_response_disconnects_before_decode(
            ) -> Result<(), $crate::BoxError> {
                properties::unsolicited_response_disconnects::<$adapter>(&$layout).await
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn p2_requests_within_twice_the_limit_never_fault_and_one_more_disconnects(
            ) -> Result<(), $crate::BoxError> {
                properties::request_margin::<$adapter>(&$layout).await
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn p3_sustained_conformant_exchanges_never_fault() -> Result<(), $crate::BoxError>
            {
                properties::sustained_exchanges::<$adapter>(&$layout).await
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn p4_mutually_saturated_peers_both_finish() -> Result<(), $crate::BoxError> {
                properties::mutual_saturation::<$adapter>(&$layout).await
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn p5_ending_one_member_retires_only_the_session() -> Result<(), $crate::BoxError>
            {
                properties::member_retirement::<$adapter>(&$layout).await
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn p6_paused_siblings_do_not_stop_the_layout() -> Result<(), $crate::BoxError> {
                properties::paused_siblings::<$adapter>(&$layout).await
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn p7_an_unnegotiated_version_is_refused_and_the_connection_stays(
            ) -> Result<(), $crate::BoxError> {
                properties::unnegotiated_version::<$adapter>(&$layout).await
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn p8_partial_frames_end_as_the_transport_specifies(
            ) -> Result<(), $crate::BoxError> {
                properties::partial_frames::<$adapter>(&$layout).await
            }
        }
    };
}

pub(crate) use stream_conformance_suite;
