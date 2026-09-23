//! Operation sequences over several sessions, checked against Serve's own
//! counters after every step. There is no reference model.

use std::sync::Arc;

use proptest::prelude::*;
use tokio::sync::Semaphore;

use super::*;

const SESSIONS: usize = 3;
const LIMIT: u32 = 2;

#[derive(Clone, Debug)]
enum Op {
    /// Admit a request; it fails locally after its first part if `fails`.
    Admit { session: usize, fails: bool },
    /// Let the oldest held request of a session produce.
    Release { session: usize },
    /// Cancel a session and reconnect its peer.
    Reconnect { session: usize },
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (0..SESSIONS, any::<bool>()).prop_map(|(session, fails)| Op::Admit { session, fails }),
        2 => (0..SESSIONS).prop_map(|session| Op::Release { session }),
        1 => (0..SESSIONS).prop_map(|session| Op::Reconnect { session }),
    ]
}

struct Live {
    session: Session,
    held: std::collections::VecDeque<Arc<Semaphore>>,
    released: Vec<Arc<Semaphore>>,
}

impl Live {
    fn new(capacity: &ServeCapacity, index: usize) -> Self {
        // Sessions 0 and 1 share a peer, so their budgets are shared.
        let peer = u8::try_from(index.min(1)).expect("few sessions") + 1;
        Self {
            session: session(capacity, peer, LIMIT),
            held: Default::default(),
            released: Vec::new(),
        }
    }

    fn drain(&mut self) {
        while self.session.output.try_recv().is_ok() {}
    }
}

fn run(ops: Vec<Op>) -> Result<(), TestCaseError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a test runtime builds");
    runtime.block_on(async move {
        let limits = ServeLimits {
            peer_execution: 1,
            ..LIMITS
        };
        let capacity = capacity(limits);
        let mut live: Vec<Live> = (0..SESSIONS).map(|i| Live::new(&capacity, i)).collect();
        let mut retired = Vec::new();
        for op in ops {
            match op {
                Op::Admit { session, fails } => {
                    let live = &mut live[session];
                    let open = live.session.serve.open();
                    let gate = Arc::new(Semaphore::new(0));
                    let admitted = live.session.serve.admit(Job {
                        parts: 2,
                        part_len: 1,
                        gate: Some(gate.clone()),
                        fail_after: fails.then_some(1),
                        ..Job::default()
                    });
                    prop_assert_eq!(admitted.is_err(), open + 1 > 2 * LIMIT);
                    if admitted.is_ok() {
                        live.held.push_back(gate);
                    }
                }
                Op::Release { session } => {
                    let live = &mut live[session];
                    if let Some(gate) = live.held.pop_front() {
                        gate.add_permits(1);
                        live.released.push(gate);
                    }
                }
                Op::Reconnect { session } => {
                    let old = std::mem::replace(&mut live[session], Live::new(&capacity, session));
                    old.session.cancel.cancel();
                    retired.push(old);
                }
            }
            settle().await;
            for live in &mut live {
                live.drain();
                prop_assert!(live.session.serve.open() <= 2 * LIMIT);
                // Every open request is held or released but not yet run.
                prop_assert!(
                    live.session.serve.open() as usize <= live.held.len() + live.released.len()
                );
            }
            prop_assert!(capacity.node_execution_held() <= limits.node_execution);
            for peer_n in 1..=3 {
                prop_assert!(capacity.peer_held(&peer(peer_n)).0 <= limits.peer_execution);
            }
        }
        // Release everything; every counter returns to zero.
        for live in live.iter_mut().chain(retired.iter_mut()) {
            for gate in live.held.drain(..) {
                gate.add_permits(1);
            }
        }
        for _ in 0..16 {
            settle().await;
            for live in &mut live {
                live.drain();
            }
        }
        for live in &live {
            prop_assert_eq!(live.session.serve.open(), 0);
            prop_assert!(!live.session.cancel.is_cancelled());
        }
        prop_assert_eq!(capacity.node_execution_held(), 0);
        prop_assert_eq!(capacity.node_output_held(), 0);
        prop_assert_eq!(capacity.active_and_waiting(), (0, 0));
        for peer_n in 1..=3 {
            prop_assert_eq!(capacity.peer_held(&peer(peer_n)), (0, 0));
        }
        Ok(())
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn operation_sequences_keep_every_bound(ops in prop::collection::vec(op(), 1..48)) {
        run(ops)?;
    }
}
