//! Pending admission owns peer capacity, including between a grant and its next poll.

use std::{collections::VecDeque, future::Future, pin::Pin};

use futures::FutureExt;
use proptest::{prop_assert, prop_assert_eq, test_runner::TestCaseError};

use super::super::*;
use super::case_error;

const PEERS: usize = 4;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum State {
    #[default]
    Idle,
    Waiting,
    Granted,
    Owned,
}

type Pending = Pin<Box<dyn Future<Output = WorkAttempt>>>;

/// This model observes actual pending futures without consulting semaphore state
/// to decide which waiter should receive capacity. Both message adapters run it.
pub(crate) fn check_admission_waiters<P>(
    policy: P,
    frame: Frame,
    capacity: usize,
    choices: &[(u8, u8)],
) -> Result<(), TestCaseError>
where
    P: RequestPolicy + Clone + 'static,
    P::Request: 'static,
    P::Error: std::fmt::Debug,
{
    let node = SlotBudget::new(capacity).map_err(case_error)?;
    let admission = RequestAdmission::new(policy, node.clone(), 1);
    let peers: [_; PEERS] =
        std::array::from_fn(|i| ZakuraPeerId::new(vec![u8::try_from(i + 1).unwrap(); 32]).unwrap());
    let mut sessions = peers.each_ref().map(|peer| Some(admission.session(peer)));
    let mut pending: [Option<Pending>; PEERS] = std::array::from_fn(|_| None);
    let mut owned: [Option<WorkAttempt>; PEERS] = std::array::from_fn(|_| None);
    let mut states = [State::Idle; PEERS];
    let mut fifo = VecDeque::new();
    let mut history = Vec::new();

    for &(peer_choice, action_choice) in choices {
        let peer = usize::from(peer_choice) % PEERS;
        let action = action_choice % 4;
        history.push((peer, action));
        match action {
            0 if states[peer] == State::Idle => {
                let session = sessions[peer].as_ref().unwrap().clone();
                let request = session.decode(frame.clone()).map_err(case_error)?;
                let mut wait: Pending = Box::pin(async move { session.admit(&request).await });
                let held = states
                    .iter()
                    .filter(|s| matches!(s, State::Granted | State::Owned))
                    .count();
                let result = wait.as_mut().now_or_never();
                if held < capacity {
                    prop_assert!(result.is_some(), "free capacity must admit: {history:?}");
                    owned[peer] = result;
                    states[peer] = State::Owned;
                } else {
                    prop_assert!(result.is_none(), "node capacity exceeded: {history:?}");
                    pending[peer] = Some(wait);
                    states[peer] = State::Waiting;
                    fifo.push_back(peer);
                }
            }
            1 if pending[peer].is_some() => {
                let result = pending[peer].as_mut().unwrap().as_mut().now_or_never();
                prop_assert_eq!(
                    result.is_some(),
                    states[peer] == State::Granted,
                    "FIFO grant differs: {:?}",
                    history
                );
                if result.is_some() {
                    owned[peer] = result;
                    pending[peer] = None;
                    states[peer] = State::Owned;
                }
            }
            2 => {
                drop(pending[peer].take());
                drop(owned[peer].take());
                states[peer] = State::Idle;
                fifo.retain(|waiting| *waiting != peer);
            }
            3 => {
                // A replacement must find capacity retained by the original
                // future or result, even if that future has not observed its grant.
                drop(sessions[peer].take());
                sessions[peer] = Some(admission.session(&peers[peer]));
                let session = sessions[peer].as_ref().unwrap();
                let request = session.decode(frame.clone()).map_err(case_error)?;
                if states[peer] != State::Idle {
                    prop_assert!(
                        session.admit(&request).now_or_never().is_none(),
                        "replacement bypassed owned capacity: {history:?}"
                    );
                }
            }
            _ => {}
        }

        // Releasing a permit assigns it to the oldest waiter immediately. The
        // application does not need to poll that future to own the assigned slot.
        let held = states
            .iter()
            .filter(|s| matches!(s, State::Granted | State::Owned))
            .count();
        for _ in held..capacity {
            let Some(next) = fifo.pop_front() else { break };
            states[next] = State::Granted;
        }
        let expected_node = states
            .iter()
            .filter(|s| matches!(s, State::Granted | State::Owned))
            .count();
        prop_assert_eq!(node.reserved(), expected_node, "{:?}", history);
        for i in 0..PEERS {
            prop_assert_eq!(
                sessions[i].as_ref().unwrap().peer_budget().reserved(),
                usize::from(states[i] != State::Idle),
                "peer {}: {:?}",
                i,
                history
            );
        }
    }
    drop(pending);
    drop(owned);
    prop_assert_eq!(node.reserved(), 0);
    prop_assert_eq!(admission.reserved_by_peers(), 0);
    Ok(())
}

#[test]
fn cancelling_an_unobserved_grant_passes_capacity_to_the_next_waiter() {
    use super::{frame, GetPeersPolicy};
    check_admission_waiters(
        GetPeersPolicy,
        frame(1),
        1,
        &[
            (0, 0),
            (1, 0),
            (2, 0),
            (0, 2),
            (1, 3),
            (1, 2),
            (2, 1),
            (2, 2),
        ],
    )
    .unwrap();
}
