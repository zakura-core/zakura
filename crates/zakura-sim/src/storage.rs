//! Storage services as action sources.
//!
//! [`HeaderStorage`] offers each pending operation of a
//! [`ControlledPort`] as `storage:header:<id>:<outcome>` actions, one per
//! outcome its contract allows now, so the `Plan` picks when and how each
//! completes. It logs the port's lifecycle events as
//! [`Observation::Storage`] on the step that caused them.

use std::{sync::Arc, time::Duration};

use zakura_scheduler::SimInstant;
use zakura_storage::header::{
    controlled::{ControlledPort, OpId, StorageEvent as PortEvent},
    lease::LeaseEnd as PortLeaseEnd,
    Fault, Outcome,
};

use crate::{
    observe::{LeaseEnd, StorageEvent},
    ActionSource, Boundary, Identity, Observation, ObservationLog,
};

/// The service name in identities and observations.
const SERVICE: &str = "header";

/// Every outcome, indexed by its code in a source-local action index.
const OUTCOMES: [Outcome; 5] = [
    Outcome::Engine,
    Outcome::Fault(Fault::Timeout),
    Outcome::Fault(Fault::Unavailable),
    Outcome::Fault(Fault::LocalResource),
    Outcome::Rejected,
];

/// Action indices hold the operation ID times this, plus the outcome code.
const CODES: u64 = 8;

/// Header storage driven by the run. Dropping it shuts the port down, which
/// a run does after its log is frozen, so nothing is logged.
pub struct HeaderStorage {
    port: Arc<ControlledPort>,
    max_latency: Option<Duration>,
}

impl HeaderStorage {
    /// A source for `port` with no latency bound: an operation can stay
    /// pending for as long as the `Plan` leaves it.
    pub fn new(port: Arc<ControlledPort>) -> Self {
        HeaderStorage {
            port,
            max_latency: None,
        }
    }

    /// A source for `port` that must complete each operation within
    /// `max_latency` of its submission, for scenarios that assume bounded
    /// storage. Time stops at that deadline until the operation completes.
    pub fn bounded(port: Arc<ControlledPort>, max_latency: Duration) -> Self {
        HeaderStorage {
            port,
            max_latency: Some(max_latency),
        }
    }
}

/// `t` plus `d`, saturating at the largest instant.
fn after(t: SimInstant, d: Duration) -> SimInstant {
    t.checked_add(d)
        .unwrap_or(SimInstant::from_start(Duration::MAX))
}

/// `bytes` as lowercase hex.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// `ends` as observations.
fn lease_ends(ends: Vec<PortLeaseEnd>) -> Vec<LeaseEnd> {
    ends.into_iter()
        .map(|end| LeaseEnd {
            peer: hex(&end.peer.digest()),
            lease: end.lease_id,
            release: end.release.name().to_owned(),
        })
        .collect()
}

/// `event` as an observation, or `None` for shutdown, which happens only
/// after the log is frozen.
fn observation(event: PortEvent) -> Option<Observation> {
    let (id, event) = match event {
        PortEvent::Submitted {
            id,
            op,
            attribution,
            request_digest,
        } => (
            id,
            StorageEvent::Submitted {
                op: op.name().to_owned(),
                request: hex(&request_digest),
                owner: attribution.owner.map(|owner| format!("{owner:?}")),
                peer: attribution.peer.map(|peer| hex(&peer.digest())),
                session: attribution.session_id,
            },
        ),
        PortEvent::CallerDropped { id } => (id, StorageEvent::CallerDropped),
        PortEvent::Completed(completion) => (
            completion.id,
            StorageEvent::Completed {
                op: completion.op.name().to_owned(),
                outcome: completion.outcome.name().to_owned(),
                owner: completion
                    .attribution
                    .owner
                    .map(|owner| format!("{owner:?}")),
                delivered: completion.delivered,
                lease_ends: lease_ends(completion.lease_ends),
                state: hex(&completion.state_digest),
            },
        ),
        PortEvent::ReplyDropped {
            id,
            lease_ends: ends,
        } => (
            id,
            StorageEvent::ReplyDropped {
                lease_ends: lease_ends(ends),
            },
        ),
        PortEvent::ShutDown { .. } => return None,
    };
    Some(Observation::Storage {
        service: SERVICE.to_owned(),
        id: id.0,
        event,
    })
}

impl ActionSource for HeaderStorage {
    fn enabled(&self, _: SimInstant, out: &mut Vec<(Identity, u64)>) {
        for op in self.port.pending() {
            for outcome in op.enabled {
                let code = (0..)
                    .zip(OUTCOMES)
                    .find_map(|(code, known)| (known == outcome).then_some(code))
                    .expect("OUTCOMES lists every outcome");
                out.push((
                    Identity::storage(SERVICE, op.id.0, outcome.name()),
                    op.id.0 * CODES + code,
                ));
            }
        }
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        if let Some(gate) = self.port.next_gate() {
            out.push(Boundary::Opens(after(now, gate)));
        }
        if let Some(max) = self.max_latency {
            for op in self.port.pending() {
                out.push(Boundary::Deadline(after(
                    now,
                    max.saturating_sub(op.since_submit),
                )));
            }
        }
    }

    fn execute(&mut self, local: u64, _: &mut ObservationLog) {
        // `local` came from `enabled` at this step, so the code is in range.
        let outcome = OUTCOMES[(local % CODES) as usize];
        self.port
            .complete(OpId(local / CODES), outcome)
            .expect("`enabled` offered this completion at this step");
    }

    fn observe(&mut self, log: &mut ObservationLog) {
        for event in self.port.take_events() {
            if let Some(observation) = observation(event) {
                log.push(observation);
            }
        }
    }
}

impl Drop for HeaderStorage {
    fn drop(&mut self) {
        self.port.shut_down();
    }
}
