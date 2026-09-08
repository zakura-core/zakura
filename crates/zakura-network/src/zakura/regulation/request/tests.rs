//! A second finite-request adapter, using the production discovery codec.
//! This exercises reuse without enabling discovery regulation in production.

use std::time::Duration;

use super::*;
use crate::zakura::discovery::{DiscoveryMessage, DiscoveryWireError, MAX_DISCOVERY_MESSAGE_BYTES};

#[derive(Clone, Debug)]
struct GetPeersPolicy;

#[derive(Debug)]
struct GetPeersRequest {
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

fn frame(limit: u16) -> Frame {
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
    let attempt = session.try_admit(&request, None).unwrap();
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
    let mut response = session.try_admit(&request, None).unwrap().commit();
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
    assert!(session.try_admit(&request, None).is_err());
    queued
        .write_with(|_| async { Ok::<_, std::io::Error>(()) })
        .await
        .unwrap();
    assert_eq!(node.reserved(), 0);
    assert!(lifetime.upgrade().is_none());
}

#[tokio::test]
async fn failed_admission_rolls_back_and_waiter_keeps_its_capacity() {
    let node = SlotBudget::new(1).unwrap();
    let admission = admission(&node);
    let first = admission.session(&peer(1));
    let second = admission.session(&peer(2));
    let request = first.decode(frame(1)).unwrap();
    let held = first.try_admit(&request, None).unwrap();
    let blocked = second.try_admit(&request, None).unwrap_err();
    assert_eq!(blocked.kind(), WorkBound::Node);
    assert_eq!(second.peer_budget().reserved(), 0);
    let wait = blocked.wait();
    tokio::pin!(wait);
    assert!(futures::poll!(&mut wait).is_pending());
    drop(held);
    let slot = tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .unwrap();
    assert!(node.try_reserve().is_none());
    let admitted = second.try_admit(&request, Some(slot)).unwrap();
    drop(admitted);
    assert_eq!(node.reserved(), 0);
}

#[test]
fn closing_response_prevents_an_unclaimed_execution() {
    let node = SlotBudget::new(1).unwrap();
    let session = admission(&node).session(&peer(1));
    let request = session.decode(frame(1)).unwrap();
    let response = session.try_admit(&request, None).unwrap().commit();
    let work = response.work_lease();
    drop(response);
    assert!(work.is_cancelled());
    assert!(!work.try_start());
    assert_eq!(node.reserved(), 1);
    drop(work);
    assert_eq!(node.reserved(), 0);
}

#[tokio::test]
async fn wait_permit_cannot_be_used_for_another_peers_budget() {
    let node = SlotBudget::new(2).unwrap();
    let admission = admission(&node);
    let first = admission.session(&peer(1));
    let second = admission.session(&peer(2));
    let request = first.decode(frame(1)).unwrap();
    let held_first = first.try_admit(&request, None).unwrap();
    let held_second = second.try_admit(&request, None).unwrap();
    let wait = first.try_admit(&request, None).unwrap_err().wait();
    drop(held_first);
    let slot = tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .unwrap();
    assert_eq!(
        second.try_admit(&request, Some(slot)).unwrap_err().kind(),
        WorkBound::Peer
    );
    assert_eq!(first.peer_budget().reserved(), 0);
    drop(held_second);
    assert_eq!(node.reserved(), 0);
}

#[test]
fn reconnect_waits_for_an_old_response_frame() {
    let node = SlotBudget::new(2).unwrap();
    let admission = admission(&node);
    let original = admission.session(&peer(1));
    let request = original.decode(frame(1)).unwrap();
    let mut response = original.try_admit(&request, None).unwrap().commit();
    let writing = response.frame_guard(1);
    drop(response);
    drop(original);

    let replacement = admission.session(&peer(1));
    assert_eq!(
        replacement.try_admit(&request, None).unwrap_err().kind(),
        WorkBound::Peer
    );
    assert!(admission
        .session(&peer(2))
        .try_admit(&request, None)
        .is_ok());
    drop(writing);
    assert!(replacement.try_admit(&request, None).is_ok());
    assert_eq!(node.reserved(), 0);
}

#[test]
fn peer_registry_prunes_churn_but_retains_outstanding_work() {
    let node = SlotBudget::new(2).unwrap();
    let admission = admission(&node);
    let original = admission.session(&peer(0));
    let request = original.decode(frame(1)).unwrap();
    let held = original.try_admit(&request, None).unwrap();
    drop(original);
    for id in 1..=255 {
        drop(admission.session(&peer(id)));
        assert_eq!(admission.peers.lock().unwrap().len(), 2);
    }
    assert_eq!(
        admission
            .session(&peer(0))
            .try_admit(&request, None)
            .unwrap_err()
            .kind(),
        WorkBound::Peer
    );
    drop(held);
    let replacement = admission.session(&peer(0));
    assert_eq!(admission.peers.lock().unwrap().len(), 1);
    assert!(replacement.try_admit(&request, None).is_ok());
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
    let held = sessions[0].try_admit(&request, None).unwrap();
    for session in &sessions[1..] {
        assert_eq!(
            session.try_admit(&request, None).unwrap_err().kind(),
            WorkBound::Peer
        );
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
    let held = original.try_admit(&request, None).unwrap();
    let wait = original.try_admit(&request, None).unwrap_err().wait();
    tokio::pin!(wait);
    assert!(futures::poll!(&mut wait).is_pending());
    drop(original);
    drop(held);
    let slot = tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .unwrap();
    let replacement = admission.session(&peer(1));
    assert_eq!(
        replacement.try_admit(&request, None).unwrap_err().kind(),
        WorkBound::Peer
    );
    // Cancellation returns a delivered slot even when its original session is gone.
    drop(slot);
    assert!(replacement.try_admit(&request, None).is_ok());
    assert_eq!(node.reserved(), 0);
}
