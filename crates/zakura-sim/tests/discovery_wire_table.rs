//! disc-3.1: `discovery/wire` registers discovery's generated receiver and
//! sender properties over discovery's own stream, and disc-3.4's
//! `discovery/late-response-admitted`.
#![cfg(zakura_sim)]

use zakura_network::zakura::{wire_codec::WireMessage, DiscoveryMessage};
use zakura_sim::{
    domain::discovery::LATE_RESPONSE_ADMITTED,
    harness::{
        discovery_service::{DiscoveryWire, FAMILY, STREAMS},
        Subsystem,
    },
    peer::discovery::discovery_stream,
    property::{Source, Status},
    table::{sender_properties, table_properties},
};

/// The properties name discovery's stream as the service declares it: its
/// kind, version and table.
#[test]
fn the_registered_stream_is_the_services() {
    let [registered] = STREAMS;
    let declared = discovery_stream();
    assert_eq!(
        (registered.kind, registered.version),
        (declared.kind, declared.version)
    );
    assert_eq!(registered.messages, declared.messages);
    assert_eq!(
        registered.messages,
        Some(<DiscoveryMessage as WireMessage>::RULES)
    );
}

/// The harness registers discovery's 31 generated receiver IDs, then its
/// 7 sender IDs, each a `Draft`, then `discovery/late-response-admitted`,
/// an `Active`, and no other property. A receiver ID reads only the peers'
/// frames and the node's closes, and a sender ID only the frames. The
/// late-response ID also reads the step loop's task observations. No
/// downstream template is registered.
#[test]
fn the_harness_registers_the_generated_discovery_ids() {
    let receiver: Vec<String> = table_properties::<DiscoveryWire>(FAMILY, &STREAMS)
        .into_iter()
        .map(|property| property.id.to_string())
        .collect();
    // Two per stream, then each row's: hello's five, get-peers' and
    // get-services' six, peers' and services' six.
    assert_eq!(receiver.len(), 31, "{receiver:?}");
    let sender: Vec<String> = sender_properties::<DiscoveryWire>(FAMILY, &STREAMS)
        .into_iter()
        .map(|property| property.id.to_string())
        .collect();
    // Each row's: hello's cadence, a request row's limit and cadence, and
    // a response row's single ending.
    assert_eq!(sender.len(), 7, "{sender:?}");
    let late = LATE_RESPONSE_ADMITTED.to_owned();
    let expected: Vec<String> = receiver
        .iter()
        .chain(&sender)
        .chain([&late])
        .cloned()
        .collect();
    let registered = DiscoveryWire::properties();
    let ids: Vec<String> = registered
        .iter()
        .map(|property| property.id.to_string())
        .collect();
    assert_eq!(ids, expected);
    for property in &registered {
        let status = if property.id.to_string() == late {
            Status::Active
        } else {
            Status::Draft
        };
        assert_eq!(property.status, status, "{}", property.id);
        let needs: &[Source] = if property.id.to_string() == late {
            &[Source::Schedule, Source::Frames, Source::Closes]
        } else if sender.contains(&property.id.to_string()) {
            &[Source::Frames]
        } else {
            &[Source::Frames, Source::Closes]
        };
        assert_eq!(property.needs, needs, "{}", property.id);
        assert!(
            property.id.to_string().starts_with("discovery/s4v2/")
                || property.id.to_string() == late
        );
    }
}
