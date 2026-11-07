//! hdr-8's header-sync liveness property: an honest peer's one-header
//! successor reaches the node's selected tip (properties.md, Header sync).
//!
//! [`HONEST_PROGRESS`] applies only in `header-sync/wire`'s progress
//! fixture, the one scenario that declares its assumptions
//! ([`progress_fixture`](crate::harness::header_sync::progress_fixture)).
//! Its trigger is logged: the node's `FrameRead` of the honest peer's first
//! `Status`. Its discharge is view-side: the reactor's `SnapshotAdopted` of
//! the successor, from the node's adoption evidence.

use std::{collections::BTreeSet, time::Duration};

use zakura_chain::block;

use crate::{
    harness::{header_node::adoption::AdoptionFact, header_sync::StorageView},
    observe::{Entry, StreamId, TransportFact},
    property::{
        Assume, CheckCtx, FactAnswer, HasView, Kind, LivenessBound, ObligationKey, Property,
        PropertyId, Source, Status, Subject, TriggerHistory, Verdict,
    },
    Observation, ObservationLog,
};

/// Once the node reads the honest peer's first `Status`, whose tip is the
/// one header past the node's own, the node adopts that header as its
/// selected tip within [`PROGRESS_BOUND`] of virtual time.
pub const HONEST_PROGRESS: &str = "header-sync/honest-progress";

/// [`HONEST_PROGRESS`]'s bound: properties.md's 30 virtual seconds.
pub const PROGRESS_BOUND: Duration = Duration::from_secs(30);

/// The table row an honest peer's `Status` frame is labelled under.
const STATUS_ROW: &str = "status";

/// Header sync's stream, at the version the honest peer speaks.
const HEADER_STREAM: StreamId = StreamId {
    kind: zakura_network::zakura::ZAKURA_STREAM_HEADER_SYNC,
    version: zakura_network::zakura::ZAKURA_HEADER_SYNC_STREAM_VERSION,
};

/// [`HONEST_PROGRESS`], as `Draft` until Evan agrees to promote it.
pub fn honest_progress<S: HasView<View = StorageView>>() -> Property<S> {
    Property {
        id: PropertyId::new(HONEST_PROGRESS),
        claim: "once the node reads an honest peer's first Status advertising the one header \
                past its own tip, it applies that header and adopts it as its selected tip \
                within 30 virtual seconds, while delivery and storage stay bounded and the \
                peer's session is the only membership",
        kind: Kind::Liveness {
            bound: LivenessBound::VirtualTime(PROGRESS_BOUND),
        },
        assumes: &[
            Assume::HonestPeer,
            Assume::BoundedDelivery,
            Assume::BoundedStorage,
            Assume::StableMembership,
        ],
        needs: &[Source::Frames, Source::View],
        status: Status::Draft,
        check: check_honest_progress::<S>,
        witness: Some(TriggerHistory::View(read_honest_status)),
    }
}

/// The step of the node's `FrameRead` of the first `Status` an honest peer
/// sent, if the log has one. A `Status` is an honest peer's when its
/// `PeerFrame` is on the `status` row of a header-sync stream whose
/// `Session` row names a peer in `honest`. The read joins it by connection, QUIC stream and
/// `seq`, which both ends count from the same frames (M5-Q12). The peer's
/// evidence logs a frame's `PeerFrame` no later than the node's read of it
/// ([`crate::peer::evidence`]), so one pass in log order finds both.
fn first_honest_status_read(honest: &BTreeSet<usize>, entries: &[Entry]) -> Option<u64> {
    let honest_names: BTreeSet<String> =
        honest.iter().map(|index| format!("peer-{index}")).collect();
    let mut sessions = BTreeSet::new();
    let mut statuses = BTreeSet::new();
    for entry in entries {
        match &entry.obs {
            Observation::Session {
                connection,
                peer,
                quic_stream,
                kind,
                ..
            } if honest_names.contains(peer) && *kind == HEADER_STREAM.kind => {
                sessions.insert((*connection, *quic_stream));
            }
            Observation::PeerFrame {
                connection,
                stream,
                quic_stream: Some(quic_stream),
                seq,
                row: Some(row),
                ..
            } if *stream == HEADER_STREAM
                && row == STATUS_ROW
                && sessions.contains(&(*connection, *quic_stream)) =>
            {
                statuses.insert((*connection, *quic_stream, *seq));
            }
            Observation::Transport {
                connection: Some(connection),
                event: TransportFact::FrameRead { quic_stream, seq },
                ..
            } if statuses.contains(&(*connection, *quic_stream, *seq)) => {
                return Some(entry.step);
            }
            _ => {}
        }
    }
    None
}

/// [`HONEST_PROGRESS`]'s trigger history: the run read an honest peer's
/// `Status`.
fn read_honest_status(view: &StorageView, log: &ObservationLog) -> bool {
    first_honest_status_read(&view.honest_peers, log.entries()).is_some()
}

/// Checks [`HONEST_PROGRESS`]. At the step whose `FrameRead` is the first
/// honest `Status` read, it opens one obligation on the successor's height,
/// one past the tip the reactor had adopted by then, which only a later
/// `SnapshotAdopted` of that height discharges ([`adopted_successor`]).
/// The progress fixture's chains are one branch of one fixture, so that
/// height names the honest peer's tip, the header the `Status` advertises.
/// Every other step is `NotApplicable`.
fn check_honest_progress<S: HasView<View = StorageView>>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let read_now = cx.observations().any(|obs| {
        matches!(
            obs,
            Observation::Transport {
                event: TransportFact::FrameRead { .. },
                ..
            }
        )
    });
    if !read_now {
        return Verdict::NotApplicable;
    }
    let view = cx.view();
    let first = first_honest_status_read(&view.honest_peers, cx.log.entries());
    if first != Some(cx.step) {
        return Verdict::NotApplicable;
    }
    let Some(adopted) = view.adoption.adoptions().last().cloned() else {
        cx.missing_evidence(
            "the node read an honest Status before its reactor adopted any committed snapshot",
        );
        return Verdict::NotApplicable;
    };
    let successor = adopted
        .header_best
        .height
        .0
        .checked_add(1)
        .expect("fixture heights stay small");
    let obligation = cx.fact_obligation(
        None,
        Subject::Owner(format!("height {successor}")),
        adopted_successor,
    );
    cx.oblige(obligation);
    Verdict::Holds
}

/// What the adoption fact with `stamp` means for the obligation `key`
/// waits on: a `SnapshotAdopted` whose selected tip is at the obligation's
/// height discharges it.
fn adopted_successor(key: &ObligationKey, view: &StorageView, stamp: u64) -> FactAnswer {
    let Subject::Owner(subject) = &key.subject else {
        return FactAnswer::Unrelated;
    };
    let Some(height) = subject
        .strip_prefix("height ")
        .and_then(|height| height.parse::<u32>().ok())
    else {
        return FactAnswer::Unrelated;
    };
    match view.adoption.fact_at(stamp) {
        Some(AdoptionFact::Adopted(adoption))
            if adoption.header_best.height == block::Height(height) =>
        {
            FactAnswer::Discharges
        }
        _ => FactAnswer::Unrelated,
    }
}
