//! Independent lifecycle histories for shared finite-request admission.

use futures::FutureExt;
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

use super::{
    tests::{frame, GetPeersPolicy},
    *,
};

mod waiters;
pub(crate) use waiters::check_admission_waiters;

fn case_error(error: impl std::fmt::Debug) -> TestCaseError {
    TestCaseError::fail(format!("{error:?}"))
}

#[derive(Clone, Copy, Debug)]
enum Action {
    Admit,
    Commit,
    Claim,
    CloneLease,
    DropLease,
    Close,
    Queue,
    FinishWrite,
}

/// Expected ownership is calculated without production permits or counters.
#[derive(Default, Debug)]
struct Model {
    attempt: bool,
    response: bool,
    leases: usize,
    frame: bool,
    started: bool,
    cancelled: bool,
    queued_bytes: u128,
}

impl Model {
    fn owns_work(&self) -> bool {
        self.attempt || self.response || self.leases != 0 || self.frame
    }

    fn actions(&self) -> Vec<Action> {
        if !self.owns_work() {
            return vec![Action::Admit];
        }
        let mut actions = vec![];
        if self.attempt {
            actions.extend([Action::Commit, Action::Close]);
        }
        if self.response {
            actions.push(Action::Close);
            if !self.frame {
                actions.push(Action::Queue);
            }
        }
        if self.leases > 0 {
            actions.extend([Action::Claim, Action::DropLease]);
            if self.leases < 2 {
                actions.push(Action::CloneLease);
            }
        }
        if self.frame {
            actions.push(Action::FinishWrite);
        }
        actions
    }
}

#[derive(Default)]
struct Owners {
    attempt: Option<WorkAttempt>,
    response: Option<ResponsePermit>,
    leases: Vec<WorkLease>,
    frame: Option<FrameGuard>,
}

impl WorkAttempt {
    pub(crate) fn weak_resources(&self) -> std::sync::Weak<WorkResources> {
        Arc::downgrade(&self.resources)
    }
}

/// Run the same ownership contract with any finite request's production policy.
pub(crate) fn check_request_owners<P>(
    policy: P,
    frame: Frame,
    node_capacity: usize,
    choices: &[(u8, u8)],
) -> Result<(), TestCaseError>
where
    P: RequestPolicy + Clone,
    P::Error: std::fmt::Debug,
{
    let response_cap = policy.response_cap(&policy.decode(frame.clone()).map_err(case_error)?);
    let node = SlotBudget::new(node_capacity).map_err(case_error)?;
    let admission = RequestAdmission::new(policy, node.clone(), 1);
    let sessions = [
        admission.session(&ZakuraPeerId::new(vec![1; 32]).map_err(case_error)?),
        admission.session(&ZakuraPeerId::new(vec![2; 32]).map_err(case_error)?),
    ];
    let requests = [
        sessions[0].decode(frame.clone()).map_err(case_error)?,
        sessions[1].decode(frame).map_err(case_error)?,
    ];
    let mut models = [Model::default(), Model::default()];
    let mut owners = [Owners::default(), Owners::default()];
    let mut history = vec![];

    for &(peer_choice, action_choice) in choices {
        let peer = usize::from(peer_choice) % 2;
        let actions = models[peer].actions();
        let action = actions[usize::from(action_choice) % actions.len()];
        history.push((peer, action));
        let occupied = models.iter().filter(|m| m.owns_work()).count();
        let model = &mut models[peer];
        let owner = &mut owners[peer];
        match action {
            Action::Admit => {
                let result = sessions[peer].admit(&requests[peer]).now_or_never();
                if occupied < node_capacity {
                    owner.attempt = Some(result.unwrap());
                    *model = Model {
                        attempt: true,
                        ..Model::default()
                    };
                } else {
                    prop_assert!(result.is_none(), "{:?}", history);
                }
            }
            Action::Commit => {
                let response = owner.attempt.take().unwrap().commit();
                owner.leases.push(response.work_lease());
                owner.response = Some(response);
                model.attempt = false;
                model.response = true;
                model.leases = 1;
            }
            Action::Claim => {
                let expected = !model.started && !model.cancelled;
                prop_assert_eq!(owner.leases[0].try_start(), expected, "{:?}", history);
                model.started |= expected;
            }
            Action::CloneLease => {
                owner.leases.push(owner.leases[0].clone());
                model.leases += 1;
            }
            Action::DropLease => {
                drop(owner.leases.pop());
                model.leases -= 1;
            }
            Action::Close => {
                drop(owner.attempt.take());
                drop(owner.response.take());
                model.attempt = false;
                model.response = false;
                model.cancelled = true;
            }
            Action::Queue => {
                let candidates = [0, 1, response_cap / 2, response_cap, u64::MAX];
                let bytes = candidates[usize::from(action_choice) % candidates.len()];
                let expected = model.queued_bytes + u128::from(bytes) <= u128::from(response_cap);
                let response = owner.response.as_mut().unwrap();
                prop_assert_eq!(response.can_queue_frame(bytes), expected, "{:?}", history);
                if expected {
                    owner.frame = Some(response.frame_guard(bytes));
                    model.queued_bytes += u128::from(bytes);
                    model.frame = true;
                }
            }
            Action::FinishWrite => {
                drop(owner.frame.take());
                model.frame = false;
            }
        }
        for i in 0..2 {
            prop_assert_eq!(
                sessions[i].peer_budget().reserved(),
                usize::from(models[i].owns_work()),
                "{:?}",
                history
            );
            for lease in &owners[i].leases {
                prop_assert_eq!(lease.is_cancelled(), models[i].cancelled, "{:?}", history);
            }
        }
        prop_assert_eq!(
            node.reserved(),
            models.iter().filter(|m| m.owns_work()).count(),
            "{:?}",
            history
        );
    }
    drop(owners);
    prop_assert_eq!(node.reserved(), 0);
    for session in sessions {
        prop_assert_eq!(session.peer_budget().reserved(), 0);
    }
    Ok(())
}

proptest! {
    #[test]
    fn discovery_request_owners_match_shared_model(
        node_capacity in 1usize..=2,
        choices in prop::collection::vec((any::<u8>(), any::<u8>()), 1..160),
    ) {
        check_request_owners(GetPeersPolicy, frame(1), node_capacity, &choices)?;
    }

    #[test]
    fn discovery_admission_waiters_match_shared_model(
        node_capacity in 1usize..=3,
        choices in prop::collection::vec((any::<u8>(), any::<u8>()), 1..160),
    ) {
        check_admission_waiters(GetPeersPolicy, frame(1), node_capacity, &choices)?;
    }
}
