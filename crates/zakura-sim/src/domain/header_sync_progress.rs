//! Header sync's progress properties (properties.md, Header sync): hdr-8's
//! `honest-progress`, an honest peer's one-header successor reaching the
//! node's selected tip, and hdr-11's `bounded-adversary-progress`, the
//! heaviest member chain's successor reaching it after a recovery.
//!
//! [`HONEST_PROGRESS`] applies only in `header-sync/wire`'s progress
//! fixture, the one scenario that declares its assumptions
//! ([`progress_fixture`](crate::harness::header_sync::progress_fixture)).
//! Its trigger is logged: the node's `FrameRead` of the honest peer's first
//! `Status`. Its discharge is view-side: the reactor's `SnapshotAdopted` of
//! the successor, from the node's adoption evidence.
//!
//! [`BOUNDED_ADVERSARY_PROGRESS`] applies only after a `Recover`, the one
//! step that declares its assumptions. Its trigger and its objective are
//! view-side: the recovery's baseline at the `Recover` pick and its record
//! at the `recovered` pick ([`RecoveryObjective`]). Its discharge is the
//! reactor's `SnapshotAdopted` of one of the objective's headers.

use std::{collections::BTreeSet, time::Duration};

use zakura_chain::block;
use zakura_scheduler::SimInstant;

use crate::{
    harness::{
        header_node::{adoption::AdoptionFact, recovery::RecoveryProgress},
        header_sync::StorageView,
    },
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
                peer is the node's only membership until it closes its own connection",
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

/// After a recovery, a header of the heaviest chain the node's members or
/// the node itself held at the `recovered` pick, one the node's chain
/// didn't hold at the `Recover` pick, reaches the node's selected tip within
/// [`RECOVERY_PROGRESS_BOUND`] (properties.md, Header sync; item 7 of the
/// hdr-9 to hdr-13 plan).
pub const BOUNDED_ADVERSARY_PROGRESS: &str = "header-sync/bounded-adversary-progress";

/// [`BOUNDED_ADVERSARY_PROGRESS`]'s bound: hdr-8's 30 virtual seconds.
pub const RECOVERY_PROGRESS_BOUND: Duration = PROGRESS_BOUND;

/// When a recovery recorded at `recovered` owes its successor by.
pub fn recovery_deadline(recovered: SimInstant) -> SimInstant {
    crate::harness::policy::after(recovered, RECOVERY_PROGRESS_BOUND)
}

/// [`BOUNDED_ADVERSARY_PROGRESS`]'s one obligation's subject.
const RECOVERY_SUBJECT: &str = "recovery";

/// [`BOUNDED_ADVERSARY_PROGRESS`], `Active`, so a violation fails the run.
/// `header-sync/storage` registers it, so it applies to every scenario with
/// a `Recover`, hostile prefix and all, and `header-sync/liveness` to its
/// recovery fixtures.
///
/// A scenario without a `Recover` doesn't declare its assumptions, so it's
/// `NotApplicable` there. Its assumptions hold from the `recovered` pick
/// until the environment ends a member ([`RecoveryEvidence::holds`]), so a
/// node that drops its honest source still owes the successor. A recovery
/// that ends unreached holds them too, since only the node, or a mirror
/// running the same code, can keep an honest source away, so it's violated
/// at that pick.
///
/// [`RecoveryEvidence::holds`]: crate::harness::header_node::recovery::RecoveryEvidence::holds
pub fn bounded_adversary_progress<S: HasView<View = StorageView>>() -> Property<S> {
    Property {
        id: PropertyId::new(BOUNDED_ADVERSARY_PROGRESS),
        claim: "after a recovery, the node adopts a header of the heaviest chain its members or \
                it held at the recovered pick, one its chain didn't hold at the Recover pick, \
                within 30 virtual seconds, while membership is stable, the source is honest, \
                delivery and storage are bounded and scheduling is fair",
        kind: Kind::Liveness {
            bound: LivenessBound::VirtualTime(RECOVERY_PROGRESS_BOUND),
        },
        assumes: &[
            Assume::HonestPeer,
            Assume::BoundedDelivery,
            Assume::BoundedStorage,
            Assume::StableMembership,
            Assume::FairScheduling,
        ],
        needs: &[Source::View],
        status: Status::Active,
        check: check_bounded_adversary_progress::<S>,
        witness: Some(TriggerHistory::View(recovered_owing)),
    }
}

/// Checks [`BOUNDED_ADVERSARY_PROGRESS`]. A recovery that ended unreached
/// is violated at that pick. The run's last live step, or any drain step,
/// with the successor still owed and its deadline not yet passed fails the
/// run as missing evidence: the step budget ran out before the deadline
/// could judge the obligation, which proves nothing either way (Rule 13).
/// A step past the deadline is left to the registry's expiry. At the `recovered` pick, with a successor
/// owed ([`RecoveryObjective`]), it applies. If the reactor's
/// adopted tip already meets the objective, the pick discharges it at once.
/// Otherwise it opens one obligation, which the first later
/// `SnapshotAdopted` that meets it discharges ([`recovery_answer`]). Both
/// read [`RecoveryEvidence::progress`], as the liveness harness's finish
/// and deadline sources do. Every other step is `NotApplicable`.
///
/// [`RecoveryObjective`]: crate::harness::header_node::recovery::RecoveryObjective
/// [`RecoveryEvidence::progress`]: crate::harness::header_node::recovery::RecoveryEvidence::progress
fn check_bounded_adversary_progress<S: HasView<View = StorageView>>(
    cx: &mut CheckCtx<'_, S>,
) -> Verdict {
    let view = cx.view();
    if view.recovery.unreached_at() == Some(cx.step) {
        return cx.violated(
            "the node admits its honest source within recovery's relinks".to_owned(),
            "every relink settled without a live session for the source".to_owned(),
        );
    }
    let progress = view.recovery.progress(&view.adoption, &view.ends);
    if let RecoveryProgress::Owed { since } = progress {
        let deadline = recovery_deadline(since);
        // Past the deadline, the registry's expiry judges it a violation.
        if cx.step >= cx.budget && cx.now <= deadline {
            cx.missing_evidence(format!(
                "the step budget ran out at {:?} with the recovery's successor owed since \
                 {:?}, before its deadline at {:?} judged it",
                cx.now, since, deadline,
            ));
        }
    }
    let Some((record, Some(_))) = view.recovery.objective() else {
        return Verdict::NotApplicable;
    };
    if record.step != cx.step {
        return Verdict::NotApplicable;
    }
    if let RecoveryProgress::Owed { .. } = progress {
        let obligation = cx.fact_obligation(
            None,
            Subject::Owner(RECOVERY_SUBJECT.to_owned()),
            recovery_answer,
        );
        cx.oblige(obligation);
    }
    Verdict::Holds
}

/// What the adoption fact with `stamp` means for the recovery obligation: a
/// `SnapshotAdopted` of one of the objective's headers discharges it.
fn recovery_answer(key: &ObligationKey, view: &StorageView, stamp: u64) -> FactAnswer {
    if key.subject != Subject::Owner(RECOVERY_SUBJECT.to_owned()) {
        return FactAnswer::Unrelated;
    }
    let Some(AdoptionFact::Adopted(adoption)) = view.adoption.fact_at(stamp) else {
        return FactAnswer::Unrelated;
    };
    match view.recovery.objective() {
        Some((_, Some(objective))) if objective.met_by(&adoption.header_best) => {
            FactAnswer::Discharges
        }
        _ => FactAnswer::Unrelated,
    }
}

/// [`BOUNDED_ADVERSARY_PROGRESS`]'s trigger history: the run reached the
/// `recovered` pick owing a successor, or ended recovery unreached, which
/// the check judges at that pick.
fn recovered_owing(view: &StorageView, _: &ObservationLog) -> bool {
    view.recovery.unreached() || matches!(view.recovery.objective(), Some((_, Some(_))))
}
