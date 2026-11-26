//! Runtime-owner histories over the real queue and its funding.
//!
//! Generator 2 runs the queue under [`OWNER_CAP`], so admission also refuses a
//! request that would take its session past the owner cap. Generator 1 ran an
//! uncapped queue. Both read the same bytes.
//!
//! The decoder reads at most 128 quadruples without padding the final input.
//! The first byte selects one of 25 operations. The remaining bytes select
//! bounded heights, request/receipt indices, ranges and estimates.
//! Operations cover extend, take, admit, publish, writer start/completion,
//! owner drop/cancellation, matched/competing/claimed receipt, receipt drop,
//! return, release, defer, estimate refresh, floor advance, reset, authority
//! refresh, range retention, clone/drop-clone, delivery failure and audit.
//! Operation 24 injects a funding-pool discrepancy while retaining the displaced
//! reservation. This explicit fault tests containment; it is not a peer action.
//! A final drain observes zero remaining charge before the queue is destroyed.
//!
//! After every input the driver checks the queue's invariants and that the
//! shared budget's charge is exactly what the driver's owners retain: the
//! queue's pooled funding, unpublished requests, receipts and quarantined
//! funding. The pool funds exactly the queue's ledger until the injected
//! discrepancy closes accounting. Each output is compared only with facts
//! that follow from the input: an admission charges the estimates its take
//! captured and keeps its session within the owner cap's floor total; a
//! receipt carries exactly the bytes that left the ledger; nothing is
//! admitted, published or started after the driver cancelled it or closed
//! accounting. Named scenarios pin the admission and receipt rules.

use std::sync::Arc;

use zakura_rt::sync::CancellationToken;

use super::*;
use crate::zakura::{
    block_sync::work_queue::{
        request_write::{RequestWritePhase, UnpublishedRequest},
        test_memory_pool, RequestWrite, TakenRequest, WorkQueue,
    },
    transport::{ByteBudget, ByteReservation, FrameWriteClaim},
};

const LIMIT: u64 = 1_000;
const PROPERTY: &str = "block/work-funding";

/// The owner cap the driver's queue admits against: an above-floor request
/// may bring its session's published reservations to [`LIMIT`], and a floor
/// request to 1,500 bytes.
const OWNER_CAP: OwnerReservationCap = OwnerReservationCap {
    above_floor: LIMIT,
    floor: 1_500,
};

/// A request's retained runtime owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FundingRequestPhase {
    /// Provisional work without funding.
    Taken,
    /// Funding acquired before publication.
    Admitted,
    /// Published request awaiting the writer.
    Queued,
    /// Writer authorized to send the request.
    Started,
    /// Writer completed the request.
    Written,
    /// Request retired before the writer started.
    Skipped,
    /// No request owner remains.
    Dropped,
}

/// One request's retention, read from its runtime owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FundingRequestView {
    /// Exact owner assigned by the take.
    pub owner: Option<BodyWorkOwner>,
    /// Heights retained by the request.
    pub heights: Vec<u32>,
    /// Request lifetime phase.
    pub phase: FundingRequestPhase,
    /// Strong request owners retained by the driver.
    pub copies: usize,
    /// Funding retained before publication.
    pub unpublished_bytes: u64,
    /// Whether the request's session token was cancelled.
    pub cancelled: bool,
}

/// A receipt reservation retained independently of the queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FundingReceiptView {
    /// Owner whose queue charge moved to this receipt.
    pub owner: BodyWorkOwner,
    /// Retained bytes, or zero after the receipt was dropped.
    pub bytes: u64,
}

/// Resource reads at one boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkFundingView {
    /// Logical queue state.
    pub queue: WorkOwnershipView,
    /// Bytes owned by the queue's pooled reservation.
    pub funded: u64,
    /// The shared budget's actual retained charge.
    pub reserved: u64,
    /// Capacity currently available to ordinary admission.
    pub available: u64,
    /// Funding retained outside the pool after an explicit corruption injection.
    pub quarantined: u64,
    /// Whether accounting permanently closed admission.
    pub closed: bool,
    /// Request owners, including stale generations.
    pub requests: Vec<FundingRequestView>,
    /// Receipt owners, including receipts retained through reset.
    pub receipts: Vec<FundingReceiptView>,
}

/// One runtime-owner operation and its evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkFundingStep {
    /// One-based transition number.
    pub step: usize,
    /// Decoder-v1 quadruple; empty only for the final explicit drain.
    pub input: Vec<u8>,
    /// Whether this step drains all driver-owned resources before queue destruction.
    pub drain: bool,
    /// Reads of the actual queue and reservation owners.
    pub actual: WorkFundingView,
    /// First failed transition or invariant.
    pub failure: Option<WorkOwnershipFailure>,
}

/// Bounded semantic history from the runtime-owner driver.
#[derive(Debug)]
pub struct WorkFundingHistory {
    /// Every completed transition, including the final drain or first failure.
    pub steps: Vec<WorkFundingStep>,
}

enum RetainedRequest {
    Taken(TakenRequest),
    // Boxed: an unpublished request carries its pool lease and credit.
    Admitted(Box<UnpublishedRequest>),
    Published(Arc<RequestWrite>),
}

struct ActualRequest {
    owner: Option<BodyWorkOwner>,
    heights: Vec<u32>,
    /// The estimates the take captured. Admission charges these, not later
    /// retry hints.
    estimates: Vec<u64>,
    retained: Option<RetainedRequest>,
    clones: Vec<Arc<RequestWrite>>,
    cancel: CancellationToken,
}

impl ActualRequest {
    fn published(&self) -> Option<&Arc<RequestWrite>> {
        match &self.retained {
            Some(RetainedRequest::Published(claim)) => Some(claim),
            _ => self.clones.first(),
        }
    }

    fn view(&self) -> FundingRequestView {
        let (phase, copies, unpublished_bytes) = match &self.retained {
            Some(RetainedRequest::Taken(_)) => (FundingRequestPhase::Taken, 1, 0),
            Some(RetainedRequest::Admitted(request)) => {
                (FundingRequestPhase::Admitted, 1, request.bytes())
            }
            _ => match self.published() {
                Some(claim) => {
                    let phase = match claim.status().phase() {
                        RequestWritePhase::Unpublished => {
                            unreachable!("published owner has a write phase")
                        }
                        RequestWritePhase::Queued => FundingRequestPhase::Queued,
                        RequestWritePhase::Started => FundingRequestPhase::Started,
                        RequestWritePhase::Written => FundingRequestPhase::Written,
                        RequestWritePhase::Skipped => FundingRequestPhase::Skipped,
                    };
                    (phase, Arc::strong_count(claim), 0)
                }
                None => (FundingRequestPhase::Dropped, 0, 0),
            },
        };
        let (owner, heights) = match &self.retained {
            Some(RetainedRequest::Taken(taken)) => (
                taken
                    .items()
                    .first()
                    .and_then(|(_, item)| item.owner)
                    .or(self.owner),
                taken.items().iter().map(|(raw, _)| raw.0).collect(),
            ),
            Some(RetainedRequest::Admitted(request)) => (
                Some(request.owner()),
                request.items().iter().map(|(raw, _)| raw.0).collect(),
            ),
            _ => self.published().map_or_else(
                || (self.owner, self.heights.clone()),
                |claim| {
                    (
                        Some(claim.owner()),
                        claim.heights().map(|raw| raw.0).collect(),
                    )
                },
            ),
        };
        FundingRequestView {
            owner,
            heights,
            phase,
            copies,
            unpublished_bytes,
            cancelled: self.cancel.is_cancelled(),
        }
    }
}

struct Driver {
    work: Arc<WorkQueue>,
    budget: ByteBudget,
    requests: Vec<ActualRequest>,
    receipts: Vec<(BodyWorkOwner, Option<ByteReservation>)>,
    /// Generation of the authority the next extend installs.
    generation: u64,
    quarantine: Vec<ByteReservation>,
    /// Whether the driver injected a funding discrepancy.
    closed: bool,
    summary: WorkFundingReplaySummary,
}

/// The queue item at `raw`, with whether it is in flight.
fn item_at(state: &WorkQueueState, raw: u32) -> Option<(bool, WorkItem)> {
    state
        .item_for_height(height(raw))
        .map(|item| (state.in_flight_contains(height(raw)), item))
}

/// Whether `owner` holds any queued height.
fn holds_any(state: &WorkQueueState, owner: BodyWorkOwner) -> bool {
    state
        .in_flight
        .values()
        .any(|item| item.owner == Some(owner))
}

fn ensure(holds: bool, boundary: &str) -> Result<(), String> {
    if holds {
        Ok(())
    } else {
        Err(format!("{PROPERTY}: {boundary}"))
    }
}

impl Driver {
    fn new() -> Self {
        let budget = ByteBudget::new(LIMIT);
        // The pool holds every session's owner-cap floor total, so the owner
        // cap, not the pool, decides admission.
        let work = Arc::new(
            WorkQueue::new(height(0), budget.clone(), test_memory_pool(OWNER_CAP.floor))
                .with_owner_cap(OWNER_CAP),
        );
        work.lock().state.set_estimate_floor(1);
        Self {
            work,
            budget,
            requests: Vec::new(),
            receipts: Vec::new(),
            generation: 0,
            quarantine: Vec::new(),
            closed: false,
            summary: WorkFundingReplaySummary::default(),
        }
    }

    fn actual(&self) -> WorkFundingView {
        let inner = self.work.lock();
        WorkFundingView {
            queue: actual_view(&inner.state),
            funded: inner.funding.bytes(),
            reserved: self.budget.reserved(),
            available: self.budget.available(),
            quarantined: self.quarantine.iter().map(ByteReservation::bytes).sum(),
            closed: self.budget.is_closed(),
            requests: self.requests.iter().map(ActualRequest::view).collect(),
            receipts: self
                .receipts
                .iter()
                .map(|(owner, receipt)| FundingReceiptView {
                    owner: *owner,
                    bytes: receipt.as_ref().map_or(0, ByteReservation::bytes),
                })
                .collect(),
        }
    }

    fn state<T>(&self, read: impl FnOnce(&WorkQueueState) -> T) -> T {
        read(&self.work.lock().state)
    }

    fn take(&mut self, a: u8, b: u8, c: u8, d: u8) {
        let low = u32::from(b) % MAX_HEIGHT + 1;
        let high = (low + u32::from(c % 8)).min(MAX_HEIGHT);
        let count = usize::from(c / 8 % 6) + 1;
        let bytes = u64::from(d) * 100;
        let session = u64::from(a / 25 % 3);
        let id = NonZeroU64::new(u64::try_from(self.requests.len() + 1).unwrap()).unwrap();
        let taken = self
            .work
            .take_request(height(low), height(high), count, bytes, session, id);
        self.requests.push(ActualRequest {
            owner: taken.items().first().and_then(|(_, item)| item.owner),
            heights: taken.items().iter().map(|(raw, _)| raw.0).collect(),
            estimates: taken
                .items()
                .iter()
                .map(|(_, item)| item.estimated_bytes)
                .collect(),
            retained: Some(RetainedRequest::Taken(taken)),
            clones: Vec::new(),
            cancel: CancellationToken::new(),
        });
    }

    fn admit(&mut self, index: usize, floor: u32) -> Result<(), String> {
        let request = &mut self.requests[index];
        let taken = match request.retained.take() {
            Some(RetainedRequest::Taken(taken)) => taken,
            other => {
                request.retained = other;
                return Ok(());
            }
        };
        let owner = request.owner;
        let cancelled = request.cancel.is_cancelled();
        let estimates: u64 = request.estimates.iter().sum();
        let cancel = request.cancel.clone();
        let held = owner.map_or(0, |owner| {
            self.state(|state| state.session_reserved(owner.session_id))
        });
        match taken.admit_for_connection(
            height(floor),
            crate::zakura::CommitWindow::default().top(height(floor)),
            0,
            None,
            cancel,
        ) {
            Ok(admitted) => {
                ensure(
                    !self.closed && !cancelled,
                    "admission after closure or cancellation",
                )?;
                equal(
                    admitted.bytes(),
                    estimates,
                    "admission charges the take's estimates",
                )?;
                ensure(
                    held + estimates <= OWNER_CAP.floor,
                    "admission takes a session past the owner cap's floor total",
                )?;
                self.requests[index].retained = Some(RetainedRequest::Admitted(Box::new(admitted)));
                self.summary.admitted += 1;
            }
            Err(_) => {
                if let Some(owner) = owner {
                    ensure(
                        !self.state(|state| holds_any(state, owner)),
                        "a refused admission keeps its heights",
                    )?;
                }
            }
        }
        Ok(())
    }

    fn publish(&mut self, index: usize) -> Result<(), String> {
        let request = &mut self.requests[index];
        let admitted = match request.retained.take() {
            Some(RetainedRequest::Admitted(admitted)) => admitted,
            other => {
                request.retained = other;
                return Ok(());
            }
        };
        let owner = admitted.owner();
        let cancelled = request.cancel.is_cancelled();
        let published = (*admitted).publish(|_| {});
        if published.is_some() {
            ensure(
                !self.closed && !cancelled,
                "publication after closure or cancellation",
            )?;
            self.summary.published += 1;
        } else {
            ensure(
                !self.state(|state| holds_any(state, owner)),
                "a refused publication keeps its heights",
            )?;
        }
        self.requests[index].retained = published.map(RetainedRequest::Published);
        Ok(())
    }

    fn start(&mut self, index: usize) -> Result<(), String> {
        let request = &self.requests[index];
        let Some(claim) = request.published() else {
            return Ok(());
        };
        let current = self.state(|state| {
            state.is_published_claim(claim.owner(), claim.heights().collect::<Vec<_>>())
        });
        let cancelled = request.cancel.is_cancelled();
        let queued = claim.status().phase() == RequestWritePhase::Queued;
        if claim.try_start() {
            ensure(
                current && !self.closed && !cancelled,
                "a writer starts a request that no longer holds its heights",
            )?;
        } else if queued {
            ensure(
                claim.status().phase() == RequestWritePhase::Skipped,
                "a refused writer leaves its request queued",
            )?;
        }
        Ok(())
    }

    fn receipt(&mut self, index: usize, at: usize, kind: u8) -> Result<(), String> {
        let request = &self.requests[index];
        let Some(owner) = request.owner else {
            return Ok(());
        };
        if request.heights.is_empty() {
            return Ok(());
        }
        let raw = request.heights[at % request.heights.len()];
        let (before, reserved) = self.state(|state| (item_at(state, raw), state.reserved_bytes()));
        let actual = match kind {
            8 => self
                .work
                .begin_receipt_for_owner(owner, height(raw))
                .map(|r| r.map(|r| r.funding)),
            9 => self
                .work
                .begin_competing_receipt(owner, height(raw))
                .map(|r| r.map(|r| r.funding)),
            _ => self
                .work
                .claim_receipt_for_owner(owner, height(raw))
                .map(Some),
        };
        let released = reserved - self.state(WorkQueueState::reserved_bytes);
        let funding = match actual {
            Err(error) if self.closed => {
                return equal(
                    format!("{error:?}"),
                    "Amount".to_owned(),
                    "quarantined receipt",
                );
            }
            Err(error) => return Err(format!("{PROPERTY}: receipt transfer failed: {error:?}")),
            Ok(funding) => funding,
        };
        let Some(funding) = funding else {
            return equal(released, 0, "a refused receipt keeps the ledger");
        };
        ensure(
            !self.closed || funding.bytes() == 0,
            "receipt escaped an empty quarantined pool",
        )?;
        equal(
            funding.bytes(),
            released,
            "a receipt carries the bytes that left the ledger",
        )?;
        if kind != 10 {
            ensure(
                before.is_some_and(|(in_flight, item)| {
                    in_flight && item.owner == Some(owner) && item.budget.is_reserved()
                }),
                "a receipt ends a reservation its owner did not hold",
            )?;
        }
        self.receipts.push((owner, Some(funding)));
        self.summary.receipts += 1;
        Ok(())
    }

    fn apply(&mut self, input: &[u8]) -> Result<(), String> {
        let a = input[0];
        let b = input.get(1).copied().unwrap_or(0);
        let c = input.get(2).copied().unwrap_or(0);
        let d = input.get(3).copied().unwrap_or(0);
        let op = a % 25;
        let low = u32::from(b) % MAX_HEIGHT + 1;
        match op {
            0 => {
                let len = u32::from(c % 8);
                let size = u64::from(d) + 1;
                self.work.extend(
                    authority(self.generation),
                    (low..=(low + len).min(MAX_HEIGHT))
                        .map(|raw| (height(raw), hash(raw), confirmed(size))),
                );
                return Ok(());
            }
            1 => {
                self.take(a, b, c, d);
                return Ok(());
            }
            11 => {
                if !self.receipts.is_empty() {
                    let index = usize::from(b) % self.receipts.len();
                    drop(self.receipts[index].1.take());
                }
                return Ok(());
            }
            15 => {
                let size = u64::from(d) + 1;
                self.work.refresh_size_estimates(
                    authority(self.generation),
                    [(height(low), hash(low), confirmed(size))],
                );
                return Ok(());
            }
            16 => {
                self.work
                    .advance_floor(height(u32::from(b) % (MAX_HEIGHT + 1)));
                return Ok(());
            }
            17 => {
                self.generation += 1;
                self.summary.resets += 1;
                self.work
                    .reset_above(height(u32::from(b) % (MAX_HEIGHT + 1)));
                return Ok(());
            }
            18 => {
                self.generation += 1;
                self.work.refresh_authority(authority(self.generation));
                return Ok(());
            }
            23 => {
                return equal(self.work.audit_funding(), !self.closed, "funding audit");
            }
            24 => {
                // Explicit fault input: move the real pool aside without changing
                // its ledger. The production audit must close admission while
                // the driver retains that funding for teardown verification.
                if !self.closed && self.state(WorkQueueState::reserved_bytes) > 0 {
                    self.closed = true;
                    let funding = {
                        let mut inner = self.work.lock();
                        std::mem::replace(&mut inner.funding, self.budget.empty_reservation())
                    };
                    self.quarantine.push(funding);
                    return equal(self.work.audit_funding(), false, "drift closes admission");
                }
                return Ok(());
            }
            _ => {}
        }
        if self.requests.is_empty() {
            return Ok(());
        }
        let index = usize::from(b) % self.requests.len();
        match op {
            2 => self.admit(index, u32::from(c) % (MAX_HEIGHT + 1))?,
            3 => self.publish(index)?,
            4 => self.start(index)?,
            5 => {
                if let Some(claim) = self.requests[index].published() {
                    if claim.status().phase() == RequestWritePhase::Started {
                        claim.written();
                    }
                }
            }
            6 => drop(self.requests[index].retained.take()),
            7 => self.requests[index].cancel.cancel(),
            8..=10 => self.receipt(index, usize::from(c), op)?,
            12..=14 => {
                let request = &self.requests[index];
                let Some(owner) = request.owner else {
                    return Ok(());
                };
                if request.heights.is_empty() {
                    return Ok(());
                }
                let raw = request.heights[usize::from(c) % request.heights.len()];
                match op {
                    12 => {
                        let heights: Vec<_> = if d % 2 == 0 {
                            vec![raw]
                        } else {
                            request.heights.clone()
                        };
                        self.work
                            .release_reserved_and_return_items_detailed_for_owner(
                                owner,
                                heights.into_iter().map(height),
                            );
                    }
                    13 => {
                        self.work
                            .release_reserved_heights_for_owner(owner, [height(raw)]);
                    }
                    _ => {
                        let before = self.state(|state| item_at(state, raw));
                        let floor = self.state(|state| state.floor);
                        if self.work.defer_received_for_owner(
                            owner,
                            height(raw),
                            hash(raw),
                            u64::from(d) + 1,
                            floor,
                        ) {
                            ensure(
                                before.is_some_and(|(in_flight, item)| {
                                    in_flight
                                        && item.owner == Some(owner)
                                        && !item.budget.is_reserved()
                                }),
                                "a deferral returns a height its owner did not hold unreserved",
                            )?;
                            ensure(
                                self.state(|state| state.pending_contains(height(raw))),
                                "a deferred height is not pending",
                            )?;
                        }
                    }
                }
            }
            19 => {
                let request = &mut self.requests[index];
                if let Some(RetainedRequest::Taken(taken)) = &mut request.retained {
                    let len = taken.items().len();
                    let start = usize::from(c) % (len + 1);
                    let end = start + usize::from(d) % (len - start + 1);
                    taken.retain_range(start..end);
                    request.heights = taken.items().iter().map(|(raw, _)| raw.0).collect();
                    request.estimates = request.estimates[start..end].to_vec();
                }
            }
            20 => {
                if let Some(claim) = self.requests[index].published().cloned() {
                    self.requests[index].clones.push(claim);
                }
            }
            21 => drop(self.requests[index].clones.pop()),
            22 => {
                if let Some(claim) = self.requests[index].published() {
                    claim.delivery_failed();
                }
            }
            _ => unreachable!("all decoder operations have a transition"),
        }
        Ok(())
    }

    fn drain(&mut self) {
        for request in &mut self.requests {
            drop(request.retained.take());
            request.clones.clear();
        }
        for (_, receipt) in &mut self.receipts {
            drop(receipt.take());
        }
        self.work.advance_floor(height(MAX_HEIGHT));
        self.quarantine.clear();
    }

    /// Check the queue's invariants and that the budget's charge is exactly
    /// what the driver's owners retain.
    fn check(&self, view: &WorkFundingView, drain: bool) -> Result<(), String> {
        let invariants = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.state(WorkQueueState::assert_invariants);
        }));
        if let Err(payload) = invariants {
            return Err(format!("{PROPERTY}: {}", panic_text(payload.as_ref())));
        }
        equal(view.closed, self.closed, "accounting closes only on drift")?;
        let retained = view.funded
            + view.quarantined
            + view
                .requests
                .iter()
                .map(|request| request.unpublished_bytes)
                .sum::<u64>()
            + view
                .receipts
                .iter()
                .map(|receipt| receipt.bytes)
                .sum::<u64>();
        equal(
            view.reserved,
            retained,
            "the budget charges exactly the retained funding",
        )?;
        if self.closed {
            equal((view.funded, view.available), (0, 0), "closed accounting")?;
        } else {
            equal(
                view.funded,
                view.queue.reserved,
                "the pool funds the queue's ledger",
            )?;
            equal(
                view.available,
                LIMIT.saturating_sub(view.reserved),
                "available capacity",
            )?;
        }
        if drain {
            equal(view.reserved, 0, "a drain returns every reservation")?;
        }
        Ok(())
    }
}

fn equal<T: std::fmt::Debug + PartialEq>(
    actual: T,
    expected: T,
    boundary: &str,
) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "{PROPERTY}: {boundary}: expected {expected:?}; actual {actual:?}"
        ))
    }
}

/// Completed runtime operations in a funding history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct WorkFundingReplaySummary {
    /// Inputs applied, excluding the final drain.
    pub transitions: usize,
    /// Admissions that charged the budget.
    pub admitted: usize,
    /// Publications that moved funding into the pool.
    pub published: usize,
    /// Receipts that took funding from the pool.
    pub receipts: usize,
    /// Frontier resets.
    pub resets: usize,
}

/// Run generator-2 runtime-owner inputs and observe obligations before queue destruction.
/// This target is distinct from the preserved pure-state decoder and consumes at most
/// [`MAX_HISTORY_BYTES`] bytes.
pub fn trace_work_funding_bytes(data: &[u8]) -> WorkFundingHistory {
    trace(data).0
}

fn trace(data: &[u8]) -> (WorkFundingHistory, WorkFundingReplaySummary) {
    let mut driver = Driver::new();
    let mut steps = Vec::new();
    let data = &data[..data.len().min(MAX_HISTORY_BYTES)];
    for input in data.chunks(4).chain(std::iter::once(&[][..])) {
        let drain = input.is_empty();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if drain {
                driver.drain();
                Ok(())
            } else {
                driver.summary.transitions += 1;
                driver.apply(input)
            }
        }));
        let actual = driver.actual();
        let failure = match result {
            Ok(result) => {
                let (phase, result) = match result {
                    Ok(()) => (WorkOwnershipPhase::Invariant, driver.check(&actual, drain)),
                    Err(message) => (WorkOwnershipPhase::Transition, Err(message)),
                };
                result.err().map(|message| WorkOwnershipFailure {
                    property_failure: true,
                    phase,
                    message,
                })
            }
            Err(payload) => Some(WorkOwnershipFailure {
                property_failure: false,
                phase: WorkOwnershipPhase::Transition,
                message: panic_text(payload.as_ref()),
            }),
        };
        let failed = failure.is_some();
        steps.push(WorkFundingStep {
            step: steps.len() + 1,
            input: input.to_vec(),
            drain,
            actual,
            failure,
        });
        if failed {
            break;
        }
    }
    (WorkFundingHistory { steps }, driver.summary)
}

/// Replay at most [`MAX_HISTORY_BYTES`] generator-2 bytes through the runtime
/// owners and drain them. Panics with the first failure.
#[cfg(test)]
fn replay_work_funding_bytes(data: &[u8]) -> WorkFundingReplaySummary {
    let (history, summary) = trace(data);
    if let Some(failure) = history.steps.iter().find_map(|step| step.failure.as_ref()) {
        panic!("{}; input {data:?}", failure.message);
    }
    summary
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn check(data: &[u8]) -> WorkFundingHistory {
        let history = trace_work_funding_bytes(data);
        if let Some(step) = history.steps.iter().find(|step| step.failure.is_some()) {
            panic!("{PROPERTY}: {:?}; input {data:?}", step.failure);
        }
        assert!(history.steps.last().unwrap().drain);
        assert_eq!(history.steps.last().unwrap().actual.reserved, 0);
        history
    }

    #[test]
    fn receipt_retains_funding_through_reset_and_replacement() {
        let history = check(WITNESS);
        assert_eq!(history.steps[6].actual.receipts[0].bytes, 100);
        assert_eq!(history.steps[7].actual.reserved, 100);
        assert_eq!(history.steps[11].actual.reserved, 300);
    }

    const PUBLISHED: &[u8] = &[0, 0, 3, 99, 1, 0, 27, 5, 2, 0, 0, 0, 3, 0, 0, 0];

    fn from_published(suffix: &[u8]) -> WorkFundingHistory {
        let mut data = PUBLISHED.to_vec();
        data.extend(suffix);
        check(&data)
    }

    #[test]
    fn queued_and_written_receipts_transfer_the_correct_funding() {
        for kind in [8, 9, 10] {
            let queued = from_published(&[kind, 0, 0, 0]);
            assert_eq!(queued.steps[4].actual.receipts[0].bytes, 400);
            assert_eq!(queued.steps[4].actual.funded, 0);
            let written = from_published(&[4, 0, 0, 0, 5, 0, 0, 0, kind, 0, 0, 0]);
            assert_eq!(written.steps[6].actual.receipts[0].bytes, 100);
            assert_eq!(written.steps[6].actual.funded, 300);
        }
    }

    #[test]
    fn last_request_clone_settles_and_released_heights_do_not_settle_twice() {
        let history = from_published(&[20, 0, 0, 0, 6, 0, 0, 0, 13, 0, 0, 0, 21, 0, 0, 0]);
        assert_eq!(history.steps[5].actual.reserved, 400);
        assert_eq!(history.steps[6].actual.reserved, 300);
        assert_eq!(history.steps[7].actual.reserved, 0);
    }

    #[test]
    fn deferred_receipt_uses_retry_estimates_without_recharging_its_old_owner() {
        let history = from_published(&[
            4, 0, 0, 0, 5, 0, 0, 0, 8, 0, 3, 0, 15, 3, 0, 149, 14, 0, 3, 199, 1, 3, 0, 5, 16, 1, 0,
            0, 1, 3, 0, 5, 2, 2, 1, 0,
        ]);
        assert_eq!(
            history.steps[8].actual.queue.items.last().unwrap().estimate,
            200
        );
        assert!(history.steps[9].actual.requests[1].heights.is_empty());
        assert_eq!(history.steps[12].actual.reserved, 500);
    }

    #[test]
    fn accounting_closure_quarantines_funding_through_rejected_replacement() {
        let history = from_published(&[
            4, 0, 0, 0, 5, 0, 0, 0, 8, 0, 0, 0, 24, 0, 0, 0, 17, 0, 0, 0, 0, 0, 0, 99, 1, 0, 0, 1,
            2, 1, 0, 0, 11, 0, 0, 0,
        ]);
        let closed = &history.steps[7].actual;
        assert!(closed.closed);
        assert_eq!(closed.quarantined, 300);
        assert_eq!(closed.reserved, 400);
        assert_eq!(
            history.steps[11].actual.requests[1].phase,
            FundingRequestPhase::Dropped
        );
        assert_eq!(history.steps[12].actual.reserved, 300);
        assert_eq!(history.steps.last().unwrap().actual.quarantined, 0);
    }

    /// One session publishes 1,000 above-floor bytes, overdraws 500 more
    /// with a floor request, and its next floor request would pass the floor
    /// total. zakura-sim's `block-work-owner-cap` seed is the same bytes.
    pub(super) const OWNER_CAP_INPUT: &[u8] = &[
        0, 0, 7, 249, 1, 2, 31, 10, 2, 0, 0, 0, 3, 0, 0, 0, 1, 6, 15, 5, 2, 1, 6, 0, 3, 1, 0, 0, 0,
        8, 1, 249, 1, 8, 7, 3, 2, 2, 8, 0,
    ];

    #[test]
    fn owner_cap_refuses_a_floor_request_past_the_floor_overdraft() {
        let history = check(OWNER_CAP_INPUT);
        let published = &history.steps[6].actual;
        assert_eq!(published.queue.reserved, 1_500);
        assert_eq!(published.reserved, 1_500);
        let refused = &history.steps[9].actual;
        assert_eq!(refused.requests[2].phase, FundingRequestPhase::Dropped);
        assert_eq!(refused.reserved, 1_500);
    }

    /// A writer that finds its request no longer holds its heights refuses to
    /// start and skips the request.
    #[test]
    fn a_refused_writer_skips_its_request() {
        let history = from_published(&[13, 0, 0, 0, 4, 0, 0, 0]);
        assert_eq!(
            history.steps[4].actual.requests[0].phase,
            FundingRequestPhase::Queued
        );
        assert_eq!(
            history.steps[5].actual.requests[0].phase,
            FundingRequestPhase::Skipped
        );
    }

    #[test]
    fn stale_provisional_ranges_keep_their_take_estimates() {
        check(&[
            0, 0, 3, 99, 1, 0, 27, 5, 17, 0, 0, 0, 19, 0, 1, 2, 2, 0, 0, 0,
        ]);
    }

    #[test]
    fn stale_receipt_preserves_an_equal_charge_replacement() {
        let history = check(&[
            0, 0, 0, 99, 1, 0, 0, 1, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 17, 0, 0, 0,
            0, 0, 0, 99, 1, 0, 0, 1, 2, 1, 0, 0, 3, 1, 0, 0, 4, 1, 0, 0, 5, 1, 0, 0, 8, 0, 0, 0,
            12, 0, 0, 1, 6, 0, 0, 0,
        ]);
        for step in &history.steps[13..16] {
            assert_eq!(step.actual.reserved, 100);
            assert!(step.actual.receipts.is_empty());
        }
    }

    /// Retains a receipt through a reset, then admits, publishes and returns
    /// a replacement.
    const WITNESS: &[u8] = &[
        0, 0, 1, 99, 1, 0, 9, 3, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 8, 0, 0, 0, 17, 0,
        0, 0, 0, 0, 1, 99, 1, 0, 9, 3, 2, 1, 0, 0, 3, 1, 0, 0, 12, 0, 0, 1, 11, 0, 0, 0,
    ];

    mod funding_target {
        use super::*;
        use crate::zakura::machine_test::machine_target_tests;

        machine_target_tests! {
            replay: replay_work_funding_bytes,
            target: "block_work_funding",
            decoder: 2,
            witness: WITNESS => WorkFundingReplaySummary {
                transitions: 14,
                admitted: 2,
                published: 2,
                receipts: 1,
                resets: 1,
            },
        }
    }

    proptest! {
        /// Histories that start from a published, or written, request.
        #[test]
        fn seeded_funding_histories_keep_every_property(data in prop::collection::vec(any::<u8>(), 0..=MAX_HISTORY_BYTES)) {
            for written in [false, true] {
                let mut seeded = vec![0, 0, 3, 99, 1, 0, 27, 5, 2, 0, 0, 0, 3, 0, 0, 0];
                if written { seeded.extend([4, 0, 0, 0, 5, 0, 0, 0]); }
                seeded.extend(&data);
                check(&seeded);
            }
        }
    }
}
