//! A second finite-request adapter, using the production discovery codec.
//! This exercises reuse without enabling discovery regulation in production.

use std::time::Duration;

use super::*;
use crate::zakura::discovery::{DiscoveryMessage, DiscoveryWireError, MAX_DISCOVERY_MESSAGE_BYTES};

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

fn admission(node: &SlotBudget) -> RequestAdmission<GetPeersPolicy> {
    RequestAdmission::new(GetPeersPolicy, node.clone(), 1)
}

#[test]
fn discovery_codec_runs_before_work_admission() {
    let node = SlotBudget::new(1).unwrap();
    let session = admission(&node).session();
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
    let session = admission(&node).session();
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
    send.try_send_guarded(
        Frame {
            message_type: 1,
            flags: 0,
            payload,
        },
        || response.frame_guard(payload_bytes),
    )
    .unwrap();
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
    let first = admission.session();
    let second = admission.session();
    let request = first.decode(frame(1)).unwrap();
    let held = first.try_admit(&request, None).unwrap();
    let blocked = second.try_admit(&request, None).unwrap_err();
    assert_eq!(blocked.kind(), WorkBound::Node);
    assert_eq!(second.session_budget().reserved(), 0);
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
    let session = admission(&node).session();
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
async fn wait_permit_cannot_be_used_for_another_sessions_budget() {
    let node = SlotBudget::new(2).unwrap();
    let admission = admission(&node);
    let first = admission.session();
    let second = admission.session();
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
        WorkBound::Session
    );
    assert_eq!(first.session_budget().reserved(), 0);
    drop(held_second);
    assert_eq!(node.reserved(), 0);
}
