use super::*;
use proptest::prelude::*;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Phase {
    NeedsContext,
    Querying,
    Backoff,
    Blocked,
    Ready,
    Assigned,
    LocalBackoff,
    Completed,
}

proptest! {
    #[test]
    fn capacity_deadline_matches_continuous_blockage_model(
        operations in prop::collection::vec((0u8..13, 0u64..1801), 1..160),
    ) {
        let mut repair = task(&snapshot());
        let start = Instant::now();
        let mut seconds = 0u64;
        let mut phase = Phase::NeedsContext;
        let mut retry = None;
        let mut since = None;
        let mut sent = false;
        let mut version = 0u64;
        // Every sequence starts with a real capacity refusal.
        for (operation, advance) in [(0, 0), (1, 0)].into_iter().chain(operations) {
            seconds += advance;
            let now = start + Duration::from_secs(seconds);
            match operation {
                0 if phase == Phase::NeedsContext => {
                    repair.mark_context_requested(now + Duration::from_secs(1), now + Duration::from_secs(2)).unwrap();
                    phase = Phase::Querying;
                    retry = Some(seconds + 1);
                }
                1 | 2 if phase == Phase::Querying => {
                    let mut resolved = context();
                    resolved.state_version = StateVersion::new(version);
                    resolved.admission_capacity_available = operation == 2;
                    repair.resolve(resolved, now).unwrap();
                    retry = None;
                    if operation == 2 {
                        phase = Phase::Ready;
                        since = None;
                        sent = false;
                    } else {
                        phase = Phase::Blocked;
                        since.get_or_insert(seconds);
                    }
                }
                3 if phase == Phase::Querying => {
                    repair.context_unavailable(now + Duration::from_secs(1)).unwrap();
                    phase = Phase::Backoff;
                    retry = Some(seconds + 1);
                }
                4 if phase == Phase::Ready => {
                    let RepairPolicyState::Ready { context } = &repair.state else { unreachable!() };
                    repair.assign(repair.owner, context.clone()).unwrap();
                    phase = Phase::Assigned;
                }
                5 if phase == Phase::Assigned => {
                    repair.wait_for_state_change(StateVersion::new(version), now).unwrap();
                    phase = Phase::Blocked;
                    since = Some(seconds);
                    sent = false;
                }
                6 => {
                    version += 1;
                    repair.observe_state_change(StateVersion::new(version));
                    if phase == Phase::Blocked {
                        phase = Phase::NeedsContext;
                    }
                }
                7 if matches!(phase, Phase::Ready | Phase::Assigned) => {
                    repair.defer_local_retry_until(now + Duration::from_secs(1)).unwrap();
                    phase = Phase::LocalBackoff;
                    retry = Some(seconds + 1);
                }
                8 if phase == Phase::Assigned => {
                    repair.retry(SourceId::from_digest([1; 32])).unwrap();
                    phase = Phase::Ready;
                }
                9 if phase == Phase::Assigned => {
                    repair.complete().unwrap();
                    phase = Phase::Completed;
                }
                10 => {
                    let mut slot = RepairRequirementSlot::default();
                    slot.insert(repair);
                    slot.take().unwrap();
                    repair = task(&snapshot());
                    phase = Phase::NeedsContext;
                    retry = None;
                    since = None;
                    sent = false;
                }
                11 => repair.retain_connected_sources(&HashSet::new()),
                12 => {
                    repair.resume_retry(now);
                    if let Some(deadline) = retry.filter(|deadline| *deadline <= seconds) {
                        match phase {
                            Phase::Querying => {
                                phase = Phase::Backoff;
                                retry = Some(deadline + 1);
                            }
                            Phase::Backoff => {
                                phase = Phase::NeedsContext;
                                retry = None;
                            }
                            Phase::LocalBackoff => {
                                phase = Phase::Ready;
                                retry = None;
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                _ => {}
            }
            let observed_phase = match repair.state() {
                RepairPolicyState::NeedsContext => Phase::NeedsContext,
                RepairPolicyState::QueryingContext { .. } => Phase::Querying,
                RepairPolicyState::ContextBackoff { .. } => Phase::Backoff,
                RepairPolicyState::StateBlocked { .. } => Phase::Blocked,
                RepairPolicyState::Ready { .. } => Phase::Ready,
                RepairPolicyState::Assigned { .. } => Phase::Assigned,
                RepairPolicyState::LocalBackoff { .. } => Phase::LocalBackoff,
                RepairPolicyState::Completed => Phase::Completed,
            };
            prop_assert_eq!(observed_phase, phase);
            let expected_since = since.map(|since| start + Duration::from_secs(since));
            prop_assert_eq!(repair.capacity_wait().map(|wait| wait.since), expected_since);
            let capacity_deadline = since.filter(|_| !sent).map(|since| since + 1800);
            let deadline = retry.into_iter().chain(capacity_deadline).min();
            prop_assert_eq!(repair.next_deadline(), deadline.map(|deadline| start + Duration::from_secs(deadline)));
            let expired = since.is_some_and(|since| seconds - since >= 1800) && !sent;
            let claimed = repair.take_capacity_expiry(now);
            prop_assert_eq!(claimed.is_some(), expired);
            if expired {
                sent = true;
                prop_assert_eq!(claimed.unwrap().since, expected_since.unwrap());
                prop_assert!(repair.take_capacity_expiry(now).is_none());
            }
            prop_assert_eq!(repair.capacity_wait().is_some_and(|wait| wait.fatal_sent), sent);
        }
    }
}
