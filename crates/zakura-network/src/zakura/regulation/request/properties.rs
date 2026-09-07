//! Independent lifecycle histories for shared finite-request admission.

use proptest::prelude::*;

use super::{
    tests::{frame, GetPeersPolicy},
    *,
};

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

proptest! {
    #[test]
    fn shared_request_owners_match_model(
        node_capacity in 1usize..=2,
        choices in prop::collection::vec((any::<u8>(), any::<u8>()), 1..160),
    ) {
        let node = SlotBudget::new(node_capacity).unwrap();
        let admission = RequestAdmission::new(GetPeersPolicy, node.clone(), 1);
        let sessions = [admission.session(), admission.session()];
        let requests = [sessions[0].decode(frame(1)).unwrap(), sessions[1].decode(frame(1)).unwrap()];
        let mut models = [Model::default(), Model::default()];
        let mut owners = [Owners::default(), Owners::default()];
        let mut history = vec![];

        for (peer_choice, action_choice) in choices {
            let peer = usize::from(peer_choice) % 2;
            let actions = models[peer].actions();
            let action = actions[usize::from(action_choice) % actions.len()];
            history.push((peer, action));
            let occupied = models.iter().filter(|m| m.owns_work()).count();
            let model = &mut models[peer];
            let owner = &mut owners[peer];
            match action {
                Action::Admit => {
                    let result = sessions[peer].try_admit(&requests[peer], None);
                    if occupied < node_capacity {
                        owner.attempt = Some(result.unwrap());
                        *model = Model { attempt: true, ..Model::default() };
                    } else {
                        prop_assert_eq!(result.unwrap_err().kind(), WorkBound::Node, "{:?}", history);
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
                    // A zero-length test frame retains work just as a nonempty
                    // frame does. Real encoded discovery writes have a separate witness.
                    owner.frame = Some(owner.response.as_mut().unwrap().frame_guard(0));
                    model.frame = true;
                }
                Action::FinishWrite => {
                    drop(owner.frame.take());
                    model.frame = false;
                }
            }
            for i in 0..2 {
                prop_assert_eq!(sessions[i].session_budget().reserved(), usize::from(models[i].owns_work()), "{:?}", history);
                for lease in &owners[i].leases {
                    prop_assert_eq!(lease.is_cancelled(), models[i].cancelled, "{:?}", history);
                }
            }
            prop_assert_eq!(node.reserved(), models.iter().filter(|m| m.owns_work()).count(), "{:?}", history);
        }
        drop(owners);
        prop_assert_eq!(node.reserved(), 0);
        for session in sessions { prop_assert_eq!(session.session_budget().reserved(), 0); }
    }
}
