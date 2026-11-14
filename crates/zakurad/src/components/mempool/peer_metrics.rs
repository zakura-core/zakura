//! Counters for what the pool does to peers, under one set of names whichever
//! pool runs, so a canary's rates can be compared with its neighbours'.
//!
//! A refusal is labelled by the advertiser's transport and a penalty by the
//! charged peer's, so a stage can divide each by its own connection count:
//! `zcash.net.peers` for legacy peers, `zakura.p2p.conn.active` for v2 peers.

use zakura_node_services::mempool::QueueSource;

/// Every label value of [`count_peer_limit_refusal`]'s `source`.
const SOURCES: [&str; 2] = ["legacy", "v2"];

/// Every [`Penalty`].
const PENALTIES: [Penalty; 3] = [Penalty::Ban, Penalty::Cooldown, Penalty::Disconnect];

/// Registers every series these counters can have at zero, whichever pool
/// runs. A series that first appears with its first event has no earlier
/// sample, so a rate over a window that holds that event would miss it.
pub(crate) fn register() {
    for source in SOURCES {
        metrics::counter!("mempool.full_queue.per_peer.total", "source" => source).absolute(0);
    }
    for penalty in PENALTIES {
        metrics::counter!(
            "mempool.peer_penalty.total",
            "penalty" => penalty.label(),
            "transport" => "legacy",
        )
        .absolute(0);
    }
}

/// A transaction source's transport, as a metric label.
fn source_label(source: &QueueSource) -> &'static str {
    match source {
        QueueSource::LegacySocket(_) => "legacy",
        QueueSource::Zakura(_) => "v2",
    }
}

/// Counts one transaction refused only because `source` already has its
/// share of undecided transactions.
///
/// The caller counts a refusal only when the peer's limit is the one reason:
/// the id is not pooled, not a cached rejection, not already queued, and the
/// node-wide limit has room. Today's pool checks those first, so both pools
/// count the same events whatever order they check in.
///
/// Core mode differs in three rare cases. Telling the first two apart would
/// cost the core a command per refused item. It counts an unsolicited body
/// from a peer at its limit whose id the core has rejected. While the core
/// checks its transactions again after a reset, it counts an id it holds
/// hidden, which today's pool has queued again and does not count. And when
/// the core does not answer a batch's rejection lookup in time, it counts
/// none of that batch's refusals, since none is known not to be a cached
/// rejection.
///
/// The count follows what each pool does. A pushed body that joins a running
/// verification counts against its peer in core mode, a difference M1 makes
/// on purpose, so core mode can refuse, and count, a later push that today's
/// pool would take.
pub(crate) fn count_peer_limit_refusal(source: &QueueSource) {
    metrics::counter!(
        "mempool.full_queue.per_peer.total",
        "source" => source_label(source),
    )
    .increment(1);
}

/// What a transaction failure did to the peer that served it, as the pool
/// sees it. A ban is counted when its report enters the peer set's
/// misbehavior channel, and a disconnect when the peer set accepts it. The
/// peer set may merge several ban reports for one address into one ban, the
/// same way whichever pool sent them.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum Penalty {
    /// A ban report, the most misbehavior a peer can score.
    Ban,
    /// A transaction cooldown started.
    Cooldown,
    /// A disconnect after a repeated cooldown.
    Disconnect,
}

impl Penalty {
    /// The metric label for this penalty.
    fn label(self) -> &'static str {
        match self {
            Self::Ban => "ban",
            Self::Cooldown => "cooldown",
            Self::Disconnect => "disconnect",
        }
    }
}

/// Counts one `penalty` applied to a legacy peer.
///
/// Both pools charge only a legacy peer's socket address, so every penalty
/// today is a legacy one. The `transport` label gives a v2 penalty its own
/// series once v2 peers can be charged.
pub(crate) fn count_legacy_peer_penalty(penalty: Penalty) {
    metrics::counter!(
        "mempool.peer_penalty.total",
        "penalty" => penalty.label(),
        "transport" => "legacy",
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every series is exported at zero once registered, and counts from
    /// there.
    #[test]
    fn registered_series_start_at_zero() {
        let readings = super::super::tests::Readings::default();
        metrics::with_local_recorder(&readings, || {
            register();
            for source in SOURCES {
                let key = format!("mempool.full_queue.per_peer.total{{source={source}}}");
                assert_eq!(readings.get(&key), Some(0.0), "{key}");
            }
            for penalty in PENALTIES {
                let key = format!(
                    "mempool.peer_penalty.total{{penalty={},transport=legacy}}",
                    penalty.label()
                );
                assert_eq!(readings.get(&key), Some(0.0), "{key}");
            }
            count_legacy_peer_penalty(Penalty::Ban);
            assert_eq!(
                readings.get("mempool.peer_penalty.total{penalty=ban,transport=legacy}"),
                Some(1.0)
            );
        });
    }
}
