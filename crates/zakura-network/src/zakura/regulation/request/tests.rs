//! A second finite-request adapter, using the production discovery codec.
//! This exercises reuse without enabling discovery regulation in production.

use std::time::Duration;

use futures::FutureExt;

use super::*;
use crate::zakura::discovery::{DiscoveryMessage, DiscoveryWireError, MAX_DISCOVERY_MESSAGE_BYTES};

impl ResponsePermit {
    pub(crate) fn weak_resources(&self) -> std::sync::Weak<WorkResources> {
        Arc::downgrade(&self.resources)
    }
}

#[derive(Clone, Debug)]
pub(super) struct GetPeersPolicy;

#[derive(Debug)]
pub(super) struct GetPeersRequest {
    limit: u16,
}

impl RequestPolicy for GetPeersPolicy {
    type Request = GetPeersRequest;
    type Error = String;

    fn decode(&self, frame: Frame) -> Result<GetPeersRequest, String> {
        if frame.message_type != 1 || frame.flags != 0 {
            return Err("invalid discovery envelope".into());
        }
        match DiscoveryMessage::decode(&frame.payload)
            .map_err(|e: DiscoveryWireError| e.to_string())?
        {
            DiscoveryMessage::GetPeers { limit, .. } => Ok(GetPeersRequest { limit }),
            _ => Err("expected GetPeers".into()),
        }
    }

    fn response_cap(&self, request: &GetPeersRequest) -> u64 {
        assert!(request.limit > 0);
        // Today's codec caps the complete discovery response, independently of
        // the smaller per-record limits proposed in the regulation draft.
        u64::try_from(MAX_DISCOVERY_MESSAGE_BYTES).unwrap()
    }
}

pub(super) fn frame(limit: u16) -> Frame {
    Frame {
        message_type: 1,
        flags: 0,
        payload: DiscoveryMessage::GetPeers {
            limit,
            wanted_services: vec![],
            exclude_node_ids: vec![],
        }
        .encode()
        .unwrap(),
    }
}

fn peer(byte: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![byte; 32]).unwrap()
}

fn admission(node: &SlotBudget) -> RequestAdmission<GetPeersPolicy> {
    RequestAdmission::new(GetPeersPolicy, node.clone(), 1)
}

#[test]
fn discovery_codec_runs_before_work_admission() {
    let node = SlotBudget::new(1).unwrap();
    let session = admission(&node).session(&peer(1));
    let mut malformed = frame(1);
    malformed.payload.push(0);
    assert!(session.decode(malformed).is_err());
    assert_eq!(node.reserved(), 0);
    let request = session.decode(frame(1)).unwrap();
    assert_eq!(request.limit, 1);
    let attempt = session.admit(&request).now_or_never().unwrap();
    assert_eq!(node.reserved(), 1);
    drop(attempt);
    assert_eq!(node.reserved(), 0);
}

#[tokio::test]
async fn finite_discovery_response_retains_work_through_its_write() {
    use crate::zakura::transport::worker_framed_channel;
    let node = SlotBudget::new(1).unwrap();
    let session = admission(&node).session(&peer(1));
    let request = session.decode(frame(1)).unwrap();
    let mut response = session.admit(&request).now_or_never().unwrap().commit();
    let lifetime = response.weak_resources();
    let work = response.work_lease();
    assert!(work.try_start());
    assert!(!work.clone().try_start());
    let (send, mut recv) = worker_framed_channel(1);
    let payload = DiscoveryMessage::Peers { records: vec![] }
        .encode()
        .unwrap();
    let payload_bytes = u64::try_from(payload.len()).unwrap();
    send.try_reserve_guarded().unwrap().send(
        Frame {
            message_type: 1,
            flags: 0,
            payload,
        },
        response.frame_guard(payload_bytes),
    );
    let queued = recv.recv().await.unwrap();
    drop(response);
    drop(work);
    assert_eq!(node.reserved(), 1);
    assert!(lifetime.upgrade().is_some());
    assert!(session.admit(&request).now_or_never().is_none());
    queued
        .write_with(|_| async { Ok::<_, std::io::Error>(()) })
        .await
        .unwrap();
    assert_eq!(node.reserved(), 0);
    assert!(lifetime.upgrade().is_none());
}

#[tokio::test]
async fn admission_waiters_retain_peer_capacity_and_receive_node_slots_in_order() {
    let node = SlotBudget::new(1).unwrap();
    let admission = admission(&node);
    let first = admission.session(&peer(1));
    let second = admission.session(&peer(2));
    let third = admission.session(&peer(3));
    let request = first.decode(frame(1)).unwrap();
    let held = first.admit(&request).await;
    let mut second_wait = Box::pin(second.admit(&request));
    let mut third_wait = Box::pin(third.admit(&request));
    assert!(futures::poll!(&mut second_wait).is_pending());
    assert!(futures::poll!(&mut third_wait).is_pending());
    assert_eq!(second.peer_budget().reserved(), 1);
    assert_eq!(third.peer_budget().reserved(), 1);
    assert_eq!(node.reserved(), 1);
    drop(held);
    assert!(
        node.try_reserve().is_none(),
        "the first waiter owns the released slot"
    );
    let second_owner = tokio::time::timeout(Duration::from_secs(1), second_wait)
        .await
        .unwrap();
    assert!(futures::poll!(&mut third_wait).is_pending());
    drop(second_owner);
    let third_owner = tokio::time::timeout(Duration::from_secs(1), third_wait)
        .await
        .unwrap();
    drop(third_owner);
    assert_eq!(node.reserved(), 0);
    assert_eq!(admission.reserved_by_peers(), 0);
}

#[test]
fn closing_response_prevents_an_unclaimed_execution() {
    let node = SlotBudget::new(1).unwrap();
    let session = admission(&node).session(&peer(1));
    let request = session.decode(frame(1)).unwrap();
    let response = session.admit(&request).now_or_never().unwrap().commit();
    let work = response.work_lease();
    drop(response);
    assert!(work.is_cancelled());
    assert!(!work.try_start());
    assert_eq!(node.reserved(), 1);
    drop(work);
    assert_eq!(node.reserved(), 0);
}

#[tokio::test]
async fn cancelling_a_node_waiter_releases_its_partial_peer_claim() {
    let node = SlotBudget::new(1).unwrap();
    let admission = admission(&node);
    let first = admission.session(&peer(1));
    let second = admission.session(&peer(2));
    let request = first.decode(frame(1)).unwrap();
    let held = first.admit(&request).await;
    let mut wait = Box::pin(second.admit(&request));
    assert!(futures::poll!(&mut wait).is_pending());
    assert_eq!(second.peer_budget().reserved(), 1);
    drop(wait);
    assert_eq!(second.peer_budget().reserved(), 0);
    assert_eq!(node.reserved(), 1);
    drop(held);
    assert!(second.admit(&request).now_or_never().is_some());
    assert_eq!(node.reserved(), 0);
}

#[test]
fn reconnect_waits_for_all_old_read_and_frame_owners() {
    for read_finishes_first in [false, true] {
        let node = SlotBudget::new(2).unwrap();
        let admission = admission(&node);
        let original = admission.session(&peer(1));
        let request = original.decode(frame(1)).unwrap();
        let mut response = original.admit(&request).now_or_never().unwrap().commit();
        let mut work = Some(response.work_lease());
        assert!(work.as_ref().unwrap().try_start());
        let writing = response.frame_guard(1);
        let ending = response.frame_guard(1);
        drop(response);
        assert!(
            work.as_ref().unwrap().is_cancelled(),
            "producer closure signals cancellation even after execution starts"
        );
        assert!(!work.as_ref().unwrap().try_start());
        drop(original);

        for _ in 0..64 {
            let replacement = admission.session(&peer(1));
            assert!(replacement.admit(&request).now_or_never().is_none());
            assert_eq!(node.reserved(), 1);
        }
        let replacement = admission.session(&peer(1));
        assert!(admission
            .session(&peer(2))
            .admit(&request)
            .now_or_never()
            .is_some());
        if read_finishes_first {
            drop(work.take());
        }
        drop(writing);
        assert!(replacement.admit(&request).now_or_never().is_none());
        drop(ending);
        if !read_finishes_first {
            assert!(replacement.admit(&request).now_or_never().is_none());
            drop(work);
        }
        assert!(replacement.admit(&request).now_or_never().is_some());
        assert_eq!(node.reserved(), 0);
    }
}

#[test]
fn peer_registry_prunes_churn_but_retains_outstanding_work() {
    let node = SlotBudget::new(2).unwrap();
    let admission = admission(&node);
    let original = admission.session(&peer(0));
    let request = original.decode(frame(1)).unwrap();
    let held = original.admit(&request).now_or_never().unwrap();
    drop(original);
    for id in 1..=255 {
        drop(admission.session(&peer(id)));
        assert_eq!(admission.peers.lock().unwrap().len(), 2);
    }
    assert!(admission
        .session(&peer(0))
        .admit(&request)
        .now_or_never()
        .is_none());
    drop(held);
    let replacement = admission.session(&peer(0));
    assert_eq!(admission.peers.lock().unwrap().len(), 1);
    assert!(replacement.admit(&request).now_or_never().is_some());
}

#[test]
fn concurrent_sessions_for_one_identity_share_capacity() {
    let node = SlotBudget::new(8).unwrap();
    let admission = admission(&node);
    let barrier = std::sync::Barrier::new(8);
    let sessions = std::thread::scope(|scope| {
        let tasks: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    admission.session(&peer(1))
                })
            })
            .collect();
        tasks
            .into_iter()
            .map(|task| task.join().unwrap())
            .collect::<Vec<_>>()
    });
    let request = sessions[0].decode(frame(1)).unwrap();
    let held = sessions[0].admit(&request).now_or_never().unwrap();
    for session in &sessions[1..] {
        assert!(session.admit(&request).now_or_never().is_none());
    }
    drop(held);
    assert_eq!(node.reserved(), 0);
}

#[tokio::test]
async fn a_waiters_peer_slot_survives_session_replacement_and_cancellation() {
    let node = SlotBudget::new(1).unwrap();
    let admission = admission(&node);
    let original = admission.session(&peer(1));
    let request = original.decode(frame(1)).unwrap();
    let held = original.admit(&request).await;
    let mut wait = Box::pin(async move { original.admit(&request).await });
    assert!(futures::poll!(&mut wait).is_pending());
    drop(held);
    let replacement = admission.session(&peer(1));
    let request = replacement.decode(frame(1)).unwrap();
    assert!(replacement.admit(&request).now_or_never().is_none());
    // Cancel after capacity is assigned but before the waiting future resumes.
    drop(wait);
    assert!(replacement.admit(&request).now_or_never().is_some());
    assert_eq!(node.reserved(), 0);
}
