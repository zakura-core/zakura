//! disc-3.1: `discovery/wire` registers discovery's generated receiver and
//! sender properties over discovery's own stream.
#![cfg(zakura_sim)]

use zakura_network::zakura::{wire_codec::WireMessage, DiscoveryMessage};
use zakura_sim::{
    harness::{
        discovery_service::{has_planted_case, DiscoveryWire, FAMILY, STREAMS},
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
/// 7 sender IDs, and no other property. The 16 stateless `<label>-closes`
/// IDs have planted cases, are `Active` and check their cases' trigger
/// histories, and the rest are `Draft`s. A receiver ID
/// reads only the peers' frames and the node's closes, and a sender ID only
/// the frames. No downstream template is registered.
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
    let expected: Vec<String> = receiver.iter().chain(&sender).cloned().collect();
    let registered = DiscoveryWire::properties();
    let ids: Vec<String> = registered
        .iter()
        .map(|property| property.id.to_string())
        .collect();
    assert_eq!(ids, expected);
    let planted = ids.iter().filter(|id| has_planted_case(id)).count();
    // One per stream, `undeclared-type-closes`, then three per row.
    assert_eq!(planted, 16, "{ids:?}");
    for property in &registered {
        let status = if has_planted_case(property.id.as_str()) {
            Status::Active
        } else {
            Status::Draft
        };
        assert_eq!(property.status, status, "{}", property.id);
        assert_eq!(
            property.witness.is_some(),
            status == Status::Active,
            "{}: only a planted ID checks its case's trigger history",
            property.id
        );
        let needs: &[Source] = if sender.contains(&property.id.to_string()) {
            &[Source::Frames]
        } else {
            &[Source::Frames, Source::Closes]
        };
        assert_eq!(property.needs, needs, "{}", property.id);
        assert!(property.id.to_string().starts_with("discovery/s4v2/"));
    }
}
