//! disc-3.1: `discovery/wire` registers discovery's generated receiver
//! properties over discovery's own stream.
#![cfg(zakura_sim)]

use zakura_network::zakura::{wire_codec::WireMessage, DiscoveryMessage};
use zakura_sim::{
    harness::{
        discovery_service::{DiscoveryWire, FAMILY, STREAMS},
        Subsystem,
    },
    peer::discovery::discovery_stream,
    property::{Source, Status},
    table::table_properties,
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

/// The harness registers discovery's 31 generated receiver IDs and no
/// other property, each a `Draft` that reads only the peers' frames and
/// the node's closes. No sender or downstream template is registered.
#[test]
fn the_harness_registers_the_generated_discovery_ids() {
    let expected: Vec<String> = table_properties::<DiscoveryWire>(FAMILY, &STREAMS)
        .into_iter()
        .map(|property| property.id.to_string())
        .collect();
    // Two per stream, then each row's: hello's five, get-peers' and
    // get-services' six, peers' and services' six.
    assert_eq!(expected.len(), 31, "{expected:?}");
    let registered = DiscoveryWire::properties();
    let ids: Vec<String> = registered
        .iter()
        .map(|property| property.id.to_string())
        .collect();
    assert_eq!(ids, expected);
    for property in &registered {
        assert_eq!(property.status, Status::Draft, "{}", property.id);
        assert_eq!(
            property.needs,
            &[Source::Frames, Source::Closes],
            "{}",
            property.id
        );
        assert!(property.id.to_string().starts_with("discovery/s4v2/"));
    }
}
