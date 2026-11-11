//! The coverage manifest: one row per property a harness reports, naming
//! the tests that show its check works and where they run.
//!
//! `tests/harness.rs` compares the manifest with the harnesses' properties,
//! so a property without a row, or a row no harness reports, fails CI. It
//! also checks that each named test or patch exists, and that CI runs each
//! test that isn't on demand.

/// One property's coverage.
#[derive(Clone, Copy, Debug)]
pub struct Coverage {
    /// The property's ID.
    pub property: &'static str,
    /// Where the property applies and holds.
    pub passing: Passing,
    /// Where a planted bug violates the property.
    pub bypass: Bypass,
}

/// Where a property applies and holds.
#[derive(Clone, Copy, Debug)]
pub enum Passing {
    /// A test where the property applies and holds.
    Test(Witness),
    /// No scenario reaches the property, for `reason`. The witness test
    /// shows that it stays `NotApplicable`.
    Unreachable {
        /// Why no scenario reaches it.
        reason: &'static str,
        /// The test where it stays `NotApplicable`.
        witness: Witness,
    },
}

impl Passing {
    /// The test this names.
    pub fn witness(self) -> Witness {
        match self {
            Passing::Test(witness) | Passing::Unreachable { witness, .. } => witness,
        }
    }
}

/// Where a planted bug violates a property.
#[derive(Clone, Copy, Debug)]
pub enum Bypass {
    /// A test that runs a planted variant itself, as the race fixtures do.
    Test(Witness),
    /// A patch `qa/planted/<property>/<name>.patch`, which `cargo xtask
    /// planted` applies to a scratch checkout. The name is without
    /// `.patch`.
    Planted(&'static str),
    /// A production patch relative to `qa/native-mutations`, bound to this
    /// property in `cases.json`. The droplet mutation campaign runs it.
    Native(&'static str),
    /// No bug can violate the property yet: its check is missing what the
    /// reason names. Only a `Draft` property names one.
    Pending(&'static str),
}

/// A test that runs a property, and where the test runs.
#[derive(Clone, Copy, Debug)]
pub struct Witness {
    /// The test's file, relative to the crate.
    pub file: &'static str,
    /// The test's name.
    pub test: &'static str,
    /// Where the test runs.
    pub ci: Ci,
}

/// Where a witness test runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ci {
    /// The `tests-unit.yml` job with this name. An ignored test also needs
    /// its name in that workflow.
    Job(&'static str),
    /// No CI job. The test is ignored, because one run takes minutes, and
    /// runs only on demand, with `--ignored` and the `planted` profile. CI
    /// keeps no expensive test in its blocking path.
    OnDemand,
}

/// The job that runs every `zakura-sim` test under `--cfg zakura_sim`.
const SIM_BUILD: &str = "zakura_sim build";

/// A test in [`SIM_BUILD`].
const fn sim_test(file: &'static str, test: &'static str) -> Witness {
    Witness {
        file,
        test,
        ci: Ci::Job(SIM_BUILD),
    }
}

/// An ignored test that runs only on demand. See [`Ci::OnDemand`].
const fn on_demand_test(file: &'static str, test: &'static str) -> Witness {
    Witness {
        file,
        test,
        ci: Ci::OnDemand,
    }
}

/// A [`SIM_BUILD`] test where a property applies and holds.
const fn passes(file: &'static str, test: &'static str) -> Passing {
    Passing::Test(sim_test(file, test))
}

/// A race fixture's row: its witness test runs the fix to a pass and the
/// planted race to its violation, both through the race's trigger history.
const fn race(property: &'static str, test: &'static str) -> Coverage {
    Coverage {
        property,
        passing: passes("tests/race_fixtures.rs", test),
        bypass: Bypass::Test(sim_test("tests/race_fixtures.rs", test)),
    }
}

/// A stream-6 generated property's row: its witness in
/// `tests/block_table.rs`, and the scripted bypass there that violates
/// every generated ID.
const fn table(property: &'static str, passing: &'static str) -> Coverage {
    Coverage {
        property,
        passing: passes("tests/block_table.rs", passing),
        bypass: TABLE_BYPASS,
    }
}

/// A stream-6 generated property no block-sync scenario reaches, for
/// `reason`: [`UNREACHED`] shows it stays `NotApplicable`, and the scripted
/// bypass still violates it.
const fn unreachable(property: &'static str, reason: &'static str) -> Coverage {
    Coverage {
        property,
        passing: Passing::Unreachable {
            reason,
            witness: sim_test("tests/block_table.rs", UNREACHED),
        },
        bypass: TABLE_BYPASS,
    }
}

/// The scripted bypass that violates every stream-6 generated ID.
const TABLE_BYPASS: Bypass = Bypass::Test(sim_test(
    "tests/block_table.rs",
    "scripted_bypasses_violate_every_generated_id",
));

/// An honest sync or honest serving holds it.
const HONEST: &str = "honest_runs_hold_every_generated_id_they_reach";
/// A probe fault outside the node's own exchanges reaches it.
const PROBE_FAULT: &str = "each_probe_fault_reaches_its_exact_id";
/// A probe fault inside the node's own exchange reaches it.
const EXCHANGE_FAULT: &str = "faults_inside_the_nodes_exchange_reach_their_exact_ids";
/// Requests past twice the node's serving limit reach it.
const OVER_MARGIN: &str = "requests_past_twice_the_serving_limit_reach_over_margin";
/// The witness that no scenario reaches an [`unreachable`] row.
const UNREACHED: &str = "ids_no_scenario_reaches_stay_not_applicable";

/// The file of `header-sync/wire`'s witness tests (P3d).
const HEADER_WITNESSES: &str = "tests/header_wire_witness.rs";

/// A header-sync generated property's row: its witness test in
/// [`HEADER_WITNESSES`], and the scripted bypass there that violates every
/// generated header-sync ID.
const fn header(property: &'static str, passing: &'static str) -> Coverage {
    Coverage {
        property,
        passing: passes(HEADER_WITNESSES, passing),
        bypass: HEADER_BYPASS,
    }
}

/// A header-sync generated property no scenario reaches, for `reason`:
/// `witness` shows it stays `NotApplicable`, and the scripted bypass still
/// violates it.
const fn header_unreachable(
    property: &'static str,
    reason: &'static str,
    witness: &'static str,
) -> Coverage {
    Coverage {
        property,
        passing: Passing::Unreachable {
            reason,
            witness: sim_test(HEADER_WITNESSES, witness),
        },
        bypass: HEADER_BYPASS,
    }
}

/// The scripted bypass that violates every header-sync generated ID.
const HEADER_BYPASS: Bypass = Bypass::Test(sim_test(
    HEADER_WITNESSES,
    "scripted_bypasses_violate_every_generated_header_id",
));

/// The honest witness, and the check that the witnesses list every ID.
const EVERY: &str = "every_generated_property_applies_in_its_witness";
/// The `undeclared-type` and `status` fault witnesses.
const STATUS: &str = "status_faults_apply_in_their_witnesses";
/// The `subscribe-headers` fault witnesses.
const SUBSCRIBE: &str = "subscribe_headers_faults_apply_in_their_witnesses";
/// The `headers` fault witnesses.
const HEADERS: &str = "headers_faults_apply_in_their_witnesses";
/// The `headers-outcome` fault witnesses.
const OUTCOME: &str = "headers_outcome_faults_apply_in_their_witnesses";
/// Why no header-sync scenario reaches an `over-owed-closes` ID.
const NOT_OWED: &str = "header sync's responses answer a subscription, whose pages spend credit, \
                        so the models label an overrun over-credit, never over-owed";

/// The file of `discovery/wire`'s witness tests (disc-3.1).
const DISCOVERY_WITNESSES: &str = "tests/discovery_wire_witness.rs";

/// The scripted bypass that violates each discovery ID no scenario reaches,
/// `discovery_service::SCRIPTED`.
const DISCOVERY_BYPASS: Bypass = Bypass::Test(sim_test(
    DISCOVERY_WITNESSES,
    "scripted_bypasses_violate_the_unplanted_discovery_ids",
));

/// The planted cases of the discovery IDs whose guard is the shared frame
/// reader's (disc-3.3). Each patch makes the reader skip one check for one
/// row of discovery's table, and discovery drop that frame instead of
/// closing. For most of the 16 inputs, skipping the reader's check alone
/// would leave discovery's codec to close the connection.
const DROP_UNDECLARED_TYPE: Bypass = Bypass::Planted("drop-undeclared-type");
/// See [`DROP_UNDECLARED_TYPE`].
const DROP_RESERVED_FLAGS: Bypass = Bypass::Planted("drop-reserved-flags");
/// See [`DROP_UNDECLARED_TYPE`].
const DROP_SHORT_PAYLOAD: Bypass = Bypass::Planted("drop-short-payload");
/// See [`DROP_UNDECLARED_TYPE`].
const DROP_LONG_PAYLOAD: Bypass = Bypass::Planted("drop-long-payload");

/// A discovery generated property's row: its witness test in
/// [`DISCOVERY_WITNESSES`], and its bypass: its planted case or the scripted
/// bypass test.
const fn discovery(property: &'static str, passing: &'static str, bypass: Bypass) -> Coverage {
    Coverage {
        property,
        passing: passes(DISCOVERY_WITNESSES, passing),
        bypass,
    }
}

/// A row for a generated discovery ID that no witness can reach, for
/// `reason`. `witness` shows the act's other IDs apply, and
/// [`DISCOVERY_BYPASS`] violates it.
const fn discovery_unreachable(
    property: &'static str,
    reason: &'static str,
    witness: &'static str,
) -> Coverage {
    Coverage {
        property,
        passing: Passing::Unreachable {
            reason,
            witness: sim_test(DISCOVERY_WITNESSES, witness),
        },
        bypass: DISCOVERY_BYPASS,
    }
}

/// The honest exchange, with the node's own sender IDs, and the check that
/// the witnesses list every ID.
const DISCOVERY_EVERY: &str = "every_discovery_property_applies_in_its_witness";
/// The `undeclared-type` and `hello` act witnesses.
const HELLO_ACTS: &str = "hello_acts_apply_in_their_witnesses";
/// The `get-peers` act witnesses, and the node's `peers` answers.
const GET_PEERS_ACTS: &str = "get_peers_acts_apply_in_their_witnesses";
/// The `peers` act witnesses.
const PEERS_ACTS: &str = "peers_acts_apply_in_their_witnesses";
/// The `get-services` act witnesses, and the node's `services` answers.
const GET_SERVICES_ACTS: &str = "get_services_acts_apply_in_their_witnesses";
/// The node's answers to requests that ask for less than everything.
const OWED_ANSWERS: &str = "answers_within_their_requests_hold_sender_within_owed";
/// The `services` act witnesses.
const SERVICES_ACTS: &str = "services_acts_apply_in_their_witnesses";

/// A control-field witness paired with its production guard mutation.
const fn handshake_guard(
    property: &'static str,
    patch: &'static str,
    test: &'static str,
) -> Coverage {
    Coverage {
        property,
        passing: passes("tests/handshake.rs", test),
        bypass: Bypass::Native(patch),
    }
}

/// Every property a harness reports, by ID.
pub static COVERAGE: &[Coverage] = &[
    handshake_guard(
        "handshake/quic/alpn-required",
        "handshake-quic/accept-unsupported-alpn/accept-unsupported-alpn.patch",
        "quic_rejects_an_unsupported_alpn_before_control",
    ),
    handshake_guard(
        "handshake/quic/identity-required",
        "handshake-quic/accept-wrong-dialed-key/accept-wrong-dialed-key.patch",
        "quic_rejects_a_key_different_from_the_dialed_identity",
    ),
    handshake_guard(
        "handshake/quic/pending-per-address-bounded",
        "handshake-quic/skip-pending-address-limit/skip-pending-address-limit.patch",
        "quic_pending_limit_refuses_another_tls_attempt",
    ),
    handshake_guard(
        "handshake/quic/retry-before-unvalidated-accept",
        "handshake-quic/skip-retry-threshold/skip-retry-threshold.patch",
        "quic_retry_validates_sources_before_tls",
    ),
    handshake_guard(
        "handshake/quic/finishes-in-time",
        "handshake-quic/skip-inbound-tls-deadline/skip-inbound-tls-deadline.patch",
        "quic_deadline_ends_a_stalled_tls_attempt",
    ),
    handshake_guard(
        "handshake/hello/refusal-releases-slot",
        "handshake-release/retain-quic-slot/retain-quic-slot.patch",
        "hello_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/ack/refusal-releases-slot",
        "handshake-release/retain-quic-slot/retain-quic-slot.patch",
        "ack_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/hello/conformant-never-refused",
        "handshake-table/refuse-conformant-hello/refuse-conformant-hello.patch",
        "conformant_control_and_prelude_are_accepted",
    ),
    handshake_guard(
        "handshake/ack/conformant-never-refused",
        "handshake-table/refuse-conformant-ack/refuse-conformant-ack.patch",
        "conformant_ack_is_accepted",
    ),
    handshake_guard(
        "handshake/prelude/conformant-never-refused",
        "handshake-table/refuse-conformant-prelude/refuse-conformant-prelude.patch",
        "conformant_control_and_prelude_are_accepted",
    ),
    Coverage {
        property: "handshake/counts-match-permits",
        passing: passes(
            "tests/handshake.rs",
            "counts_trace_replays_with_each_named_rule",
        ),
        bypass: Bypass::Native(
            "handshake-admission/retain-pending-charge/retain-pending-charge.patch",
        ),
    },
    Coverage {
        property: "handshake/pending-bounded",
        passing: passes(
            "tests/handshake.rs",
            "inbound_control_pressure_preserves_a_wire_dial",
        ),
        bypass: Bypass::Native(
            "handshake-admission/remove-inbound-reserve/remove-inbound-reserve.patch",
        ),
    },
    Coverage {
        property: "handshake/dial-reserve-kept",
        passing: passes(
            "tests/handshake.rs",
            "inbound_connection_pressure_preserves_a_wire_dial",
        ),
        bypass: Bypass::Native(
            "handshake-admission/remove-inbound-reserve/remove-inbound-reserve.patch",
        ),
    },
    Coverage {
        property: "handshake/connections-per-address-bounded",
        passing: passes(
            "tests/handshake.rs",
            "shared_address_registrations_stop_at_the_cap",
        ),
        bypass: Bypass::Native(
            "handshake-admission/allow-one-extra-registration/allow-one-extra-registration.patch",
        ),
    },
    handshake_guard(
        "handshake/prelude/bad-magic-resets",
        "handshake-table/skip-prelude-magic/skip-prelude-magic.patch",
        "prelude_guards_apply_each_declared_consequence",
    ),
    handshake_guard(
        "handshake/prelude/unknown-kind-resets",
        "handshake-table/fallback-for-unknown-stream/fallback-for-unknown-stream.patch",
        "prelude_guards_apply_each_declared_consequence",
    ),
    handshake_guard(
        "handshake/prelude/unnegotiated-capability-resets",
        "handshake-table/skip-prelude-capability/skip-prelude-capability.patch",
        "prelude_guards_apply_each_declared_consequence",
    ),
    handshake_guard(
        "handshake/prelude/unselected-version-resets",
        "handshake-table/skip-selected-stream-version/skip-selected-stream-version.patch",
        "prelude_guards_apply_each_declared_consequence",
    ),
    handshake_guard(
        "handshake/prelude/unexpected-request-id-closes",
        "handshake-table/allow-unexpected-request-id/allow-unexpected-request-id.patch",
        "prelude_guards_apply_each_declared_consequence",
    ),
    handshake_guard(
        "handshake/prelude/request-without-id-closes",
        "handshake-table/allow-missing-request-id/allow-missing-request-id.patch",
        "prelude_guards_apply_each_declared_consequence",
    ),
    handshake_guard(
        "handshake/prelude/undersized-cap-closes",
        "handshake-table/skip-prelude-frame-cap/skip-prelude-frame-cap.patch",
        "prelude_guards_apply_each_declared_consequence",
    ),
    handshake_guard(
        "handshake/prelude/ineligible-opener-closes",
        "handshake-table/allow-ineligible-opener/allow-ineligible-opener.patch",
        "prelude_guards_apply_each_declared_consequence",
    ),
    handshake_guard(
        "handshake/hello/oversize-refused",
        "handshake-table/skip-control-read-hard-cap/skip-control-read-hard-cap.patch",
        "hello_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/ack/oversize-refused",
        "handshake-table/skip-control-read-hard-cap/skip-control-read-hard-cap.patch",
        "ack_guards_refuse_each_corrupt_field",
    ),
    Coverage {
        property: "handshake/finishes-in-time",
        passing: passes("tests/handshake.rs", "honest_control_handshake_holds"),
        bypass: Bypass::Test(sim_test(
            "tests/handshake.rs",
            "slow_control_steps_exceed_the_overall_deadline",
        )),
    },
    Coverage {
        property: "session/renegotiation-covers-inflight-handshakes",
        passing: passes(
            "tests/handshake.rs",
            "capability_flip_covers_inflight_handshake",
        ),
        bypass: Bypass::Test(sim_test(
            "tests/handshake.rs",
            "capability_flip_covers_inflight_handshake",
        )),
    },
    handshake_guard(
        "handshake/hello/wrong-network-refused",
        "handshake-table/skip-network-binding/skip-network-binding.patch",
        "hello_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/hello/identity-mismatch-refused",
        "handshake-table/skip-authenticated-identity/skip-authenticated-identity.patch",
        "hello_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/ack/unoffered-capability-refused",
        "handshake-table/accept-unoffered-capability/accept-unoffered-capability.patch",
        "ack_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/ack/bad-nonce-echo-refused",
        "handshake-table/skip-nonce-echo/skip-nonce-echo.patch",
        "ack_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/hello/bad-magic-refused",
        "handshake-table/skip-hello-magic/skip-hello-magic.patch",
        "hello_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/hello/bad-version-refused",
        "handshake-table/skip-hello-version/skip-hello-version.patch",
        "hello_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/hello/bad-protocol-refused",
        "handshake-table/skip-hello-protocol/skip-hello-protocol.patch",
        "hello_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/hello/bad-path-refused",
        "handshake-table/skip-handshake-path/skip-handshake-path.patch",
        "hello_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/hello/bad-role-refused",
        "handshake-table/skip-remote-role/skip-remote-role.patch",
        "hello_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/hello/wrong-chain-refused",
        "handshake-table/skip-chain-binding/skip-chain-binding.patch",
        "hello_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/hello/missing-capability-refused",
        "handshake-table/skip-required-channel/skip-required-channel.patch",
        "hello_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/ack/bad-protocol-refused",
        "handshake-table/skip-ack-protocol/skip-ack-protocol.patch",
        "ack_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/ack/bad-magic-refused",
        "handshake-table/skip-ack-magic/skip-ack-magic.patch",
        "ack_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/ack/bad-version-refused",
        "handshake-table/skip-ack-version/skip-ack-version.patch",
        "ack_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/hello/nonzero-initiator-nonce-refused",
        "handshake-table/skip-native-initiator-nonce/skip-native-initiator-nonce.patch",
        "hello_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/hello/nonzero-responder-nonce-refused",
        "handshake-table/skip-native-responder-nonce/skip-native-responder-nonce.patch",
        "hello_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/hello/nonzero-native-transcript-refused",
        "handshake-table/skip-native-transcript/skip-native-transcript.patch",
        "hello_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/hello/bad-frame-limit-refused",
        "handshake-table/skip-frame-limits/skip-frame-limits.patch",
        "hello_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/ack/bad-frame-limit-refused",
        "handshake-table/skip-frame-limits/skip-frame-limits.patch",
        "ack_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/hello/bad-message-limit-refused",
        "handshake-table/skip-message-limits/skip-message-limits.patch",
        "hello_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/ack/bad-message-limit-refused",
        "handshake-table/skip-message-limits/skip-message-limits.patch",
        "ack_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/hello/bad-stream-limit-refused",
        "handshake-table/skip-stream-limits/skip-stream-limits.patch",
        "hello_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/ack/bad-stream-limit-refused",
        "handshake-table/skip-stream-limits/skip-stream-limits.patch",
        "ack_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/hello/bad-queue-limit-refused",
        "handshake-table/skip-queue-limits/skip-queue-limits.patch",
        "hello_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/ack/bad-queue-limit-refused",
        "handshake-table/skip-queue-limits/skip-queue-limits.patch",
        "ack_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/hello/bad-idle-limit-refused",
        "handshake-table/skip-idle-limits/skip-idle-limits.patch",
        "hello_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/ack/bad-idle-limit-refused",
        "handshake-table/skip-idle-limits/skip-idle-limits.patch",
        "ack_guards_refuse_each_corrupt_field",
    ),
    handshake_guard(
        "handshake/ack/limits-above-request-refused",
        "handshake-table/skip-requested-limit-ceilings/skip-requested-limit-ceilings.patch",
        "ack_guards_refuse_each_corrupt_field",
    ),
    Coverage {
        property: "session/capacity-conserved",
        passing: passes(
            "tests/handshake.rs",
            "partial_session_offers_release_setup_capacity",
        ),
        bypass: Bypass::Planted("leak-setup-permits"),
    },
    Coverage {
        property: "session/setup-slot-bounded",
        passing: passes(
            "tests/handshake.rs",
            "partial_session_offers_release_setup_capacity",
        ),
        bypass: Bypass::Planted("no-outbound-setup-reserve"),
    },
    Coverage {
        property: "session/stream-opens-bounded",
        passing: passes(
            "tests/handshake.rs",
            "invalid_stream_churn_obeys_the_open_rate",
        ),
        bypass: Bypass::Planted("no-stream-open-charge"),
    },
    Coverage {
        property: "session/installed-services-authorized",
        passing: passes(
            "tests/handshake.rs",
            "installed_services_follow_each_peers_negotiation",
        ),
        bypass: Bypass::Planted("install-unnegotiated-service"),
    },
    Coverage {
        property: "handshake/attempts-within-cadence",
        passing: passes(
            "tests/handshake.rs",
            "fast_fail_handshakes_exhaust_the_address_cadence",
        ),
        bypass: Bypass::Planted("no-attempt-charge"),
    },
    Coverage {
        property: "handshake/cadence-never-refuses-conformant",
        passing: passes(
            "tests/handshake.rs",
            "sixteen_conformant_dialers_share_an_address_cadence",
        ),
        bypass: Bypass::Planted("refuse-funded-attempt"),
    },
    Coverage {
        property: "session/renegotiation-covers-inflight-handshakes",
        passing: passes(
            "tests/handshake.rs",
            "capability_flip_covers_inflight_handshake",
        ),
        bypass: Bypass::Planted("stale-capability-snapshot"),
    },
    Coverage {
        property: "handshake/finishes-in-time",
        passing: passes(
            "tests/handshake.rs",
            "slow_control_steps_share_one_deadline",
        ),
        bypass: Bypass::Planted("no-overall-deadline"),
    },
    Coverage {
        property: super::super::domain::block_sync::PAYLOAD_INVENTORY_AGREES,
        passing: passes(
            "tests/block_resource_properties.rs",
            "the_handoff_input_holds_payload_inventory_agrees",
        ),
        bypass: Bypass::Planted("lost-payload-release"),
    },
    Coverage {
        property: "block/decoded-retention",
        passing: passes(
            "tests/replay_exact.rs",
            "decoded_retention_replays_final_holders_in_a_fresh_process",
        ),
        bypass: Bypass::Planted("raw-transaction-escape"),
    },
    Coverage {
        property: "block/reactor-policy",
        passing: passes(
            "tests/replay_exact.rs",
            "reactor_machine_records_and_replays_query_identities",
        ),
        bypass: Bypass::Planted("refusal-consumes-query-id"),
    },
    Coverage {
        property: "block/work-funding",
        passing: passes(
            "tests/replay_exact.rs",
            "work_funding_records_and_replays_retained_runtime_owners",
        ),
        bypass: Bypass::Planted("lost-reservation-release"),
    },
    Coverage {
        property: "block/work-ownership",
        passing: passes(
            "tests/replay_exact.rs",
            "work_machine_records_and_replays_without_transport",
        ),
        bypass: Bypass::Planted("missing-published-charge"),
    },
    Coverage {
        property: "header/reactor-policy",
        passing: passes(
            "tests/replay_exact.rs",
            "reactor_machine_records_and_replays_without_transport",
        ),
        bypass: Bypass::Planted("locator-call-settles-without-removal"),
    },
    Coverage {
        property: "header/publisher-policy",
        passing: passes(
            "tests/replay_exact.rs",
            "publisher_machine_records_and_replays_without_transport",
        ),
        bypass: Bypass::Planted("lost-credit"),
    },
    Coverage {
        property: super::NO_TASK_PANIC,
        passing: passes("tests/harness.rs", "a_clean_run_holds_no_task_panic"),
        bypass: Bypass::Test(sim_test(
            "tests/harness.rs",
            "task_panic_violates_no_task_panic",
        )),
    },
    Coverage {
        property: crate::sim::NO_LIVELOCK,
        passing: passes("tests/harness.rs", "a_moving_clock_holds_no_livelock"),
        bypass: Bypass::Test(sim_test("tests/harness.rs", "livelock_fires_before_budget")),
    },
    race("race/lost-update", "lost_update_witness"),
    race("race/snapshot-reserve", "snapshot_reserve_witness"),
    race("race/missed-notify", "missed_notify_witness"),
    race("race/timer-cancel", "timer_cancel_witness"),
    race("race/ab-ba", "ab_ba_witness"),
    race("race/abort-without-join", "abort_without_join_witness"),
    Coverage {
        property: "discovery/record-book",
        passing: passes(
            "tests/replay_exact.rs",
            "discovery_book_records_and_replays_signed_samples",
        ),
        bypass: Bypass::Planted("skip-book-eviction"),
    },
    Coverage {
        property: "discovery/session-installation",
        passing: passes(
            "tests/replay_exact.rs",
            "discovery_state_records_and_replays_installations",
        ),
        bypass: Bypass::Planted("install-superseded-result"),
    },
    Coverage {
        property: crate::domain::discovery::VERIFICATION_CAPACITY,
        passing: passes(
            "tests/discovery.rs",
            "verification_results_keep_their_slots",
        ),
        bypass: Bypass::Planted("release-before-install"),
    },
    Coverage {
        property: crate::domain::discovery::WAIT_COMPLETE_WAKES,
        passing: passes("tests/witness.rs", "discovery_wait_complete_lost_wakeup"),
        bypass: Bypass::Planted("lost-wakeup"),
    },
    Coverage {
        property: crate::domain::discovery::LATE_RESPONSE_ADMITTED,
        passing: passes(
            DISCOVERY_WITNESSES,
            "late_response_admitted_applies_in_its_saved_plan",
        ),
        bypass: Bypass::Planted("revoke-on-timeout"),
    },
    Coverage {
        property: super::header_sync::OPERATION_IDS,
        passing: passes(
            "tests/header_node.rs",
            "node_syncs_from_a_mirror_on_controlled_storage",
        ),
        bypass: Bypass::Test(sim_test(
            "tests/header_node.rs",
            "planted_operation_ids_violate_the_property",
        )),
    },
    Coverage {
        property: super::header_sync::DIRECT_DISPATCH,
        passing: passes(
            "tests/header_node.rs",
            "node_syncs_from_a_mirror_on_controlled_storage",
        ),
        bypass: Bypass::Test(sim_test(
            "tests/header_node.rs",
            "direct_dispatch_guard_reports_a_violation",
        )),
    },
    Coverage {
        property: super::super::domain::header_sync::CHUNK_BUDGET_BOUND,
        passing: passes(
            "tests/header_node_capacity.rs",
            "the_close_relink_input_holds_chunk_budget_bound",
        ),
        bypass: Bypass::Planted("uncapped-secondary-reservation"),
    },
    Coverage {
        property: super::super::domain::header_sync::TEARDOWN_RETURNS_BASELINE,
        passing: passes(
            "tests/header_teardown.rs",
            "the_close_relink_input_holds_teardown_returns_baseline",
        ),
        bypass: Bypass::Planted("kept-queue-lease"),
    },
    Coverage {
        property: super::super::domain::header_sync::STALE_COMPLETION_INERT,
        passing: passes(
            "tests/header_stale.rs",
            "the_stale_prepare_input_holds_stale_completion_inert",
        ),
        bypass: Bypass::Planted("stale-release"),
    },
    Coverage {
        property: super::super::domain::header_sync::RESET_INVALIDATES_WORK,
        passing: passes(
            "tests/header_reset_invalidates.rs",
            "every_witness_holds_and_applies",
        ),
        bypass: Bypass::Planted("skipped-retirement"),
    },
    Coverage {
        property: super::super::domain::header_sync::STALE_CHAIN_EFFECT_INERT,
        passing: passes(
            "tests/header_stale_chain.rs",
            "every_witness_holds_and_applies",
        ),
        bypass: Bypass::Planted("stale-adoption"),
    },
    Coverage {
        property: super::super::domain::header_sync::OUTSTANDING_WORK_BOUNDED,
        passing: passes(
            "tests/header_outstanding.rs",
            "the_committed_witnesses_hold_outstanding_work_bounded",
        ),
        bypass: Bypass::Planted("call-kept"),
    },
    Coverage {
        property: super::super::domain::header_sync::COMMIT_ONLY_AUTHORIZED,
        passing: passes(
            "tests/header_authority.rs",
            "every_witness_holds_and_applies",
        ),
        bypass: Bypass::Planted("wrong-request"),
    },
    Coverage {
        property: super::super::domain::header_sync::WATCH_OBSERVED_UNDER_FLOOD,
        passing: passes(
            "tests/header_flood_watch.rs",
            "the_committed_flood_inputs_hold_their_watch_rules",
        ),
        bypass: Bypass::Planted("watch-check-revert"),
    },
    Coverage {
        property: super::super::domain::header_sync::WATCH_OBSERVED_UNDER_LIFECYCLE_FLOOD,
        passing: passes(
            "tests/header_flood_watch.rs",
            "the_committed_flood_inputs_hold_their_watch_rules",
        ),
        bypass: Bypass::Planted("watch-check-revert"),
    },
    Coverage {
        property: super::super::domain::header_sync::CAPACITY_RELEASE_WAKES,
        passing: passes(
            "tests/header_release_wake.rs",
            "the_release_wake_input_holds_capacity_release_wakes",
        ),
        bypass: Bypass::Planted("missed-release-wake"),
    },
    Coverage {
        property: super::super::domain::header_sync::HOLD_RELEASE_WAKES,
        passing: passes(
            "tests/header_hold_release.rs",
            "the_committed_fixtures_match_their_names",
        ),
        bypass: Bypass::Planted("completion-arm-removed"),
    },
    Coverage {
        property: super::super::domain::header_sync::PEER_RETIREMENT_PROGRESS,
        passing: passes(
            "tests/header_peer_retirement.rs",
            "the_committed_fixtures_match_their_names",
        ),
        bypass: Bypass::Planted("retained-owner"),
    },
    Coverage {
        property: super::header_sync::HEADER_FAULT_PENALIZED,
        passing: passes(
            "tests/header_wire_prepare_fault.rs",
            "a_disconnect_mid_prepare_is_still_penalized",
        ),
        bypass: Bypass::Test(sim_test(
            "tests/header_wire_prepare_fault.rs",
            "hidden_cooldowns_violate_the_penalty_rule",
        )),
    },
    Coverage {
        property: super::block_sync::OPERATION_IDS,
        passing: passes("tests/block_node.rs", "honest_sync_reaches_the_fixture_tip"),
        bypass: Bypass::Test(sim_test(
            "tests/block_node.rs",
            "planted_block_operation_ids_violate_the_property",
        )),
    },
    table("block-sync/s6v3/block/receiver-work-authorized", HONEST),
    table("block-sync/s6v3/block/sender-answers-open", HONEST),
    table(
        "block-sync/s6v3/block/sender-within-response-quantity",
        HONEST,
    ),
    table("block-sync/s6v3/blocks-done/sender-ends-once", HONEST),
    table(
        "block-sync/s6v3/blocks-done/sender-within-response-quantity",
        HONEST,
    ),
    table("block-sync/s6v3/conformant-never-closes", HONEST),
    table(
        "block-sync/s6v3/get-blocks/receiver-serving-bounded",
        HONEST,
    ),
    table("block-sync/s6v3/get-blocks/sender-within-limit", HONEST),
    table("block-sync/s6v3/range-unavailable/sender-ends-once", HONEST),
    table(
        "block-sync/s6v3/range-unavailable/sender-within-response-quantity",
        HONEST,
    ),
    table("block-sync/s6v3/status/sender-cadence", HONEST),
    table("block-sync/s6v3/block/long-payload-closes", PROBE_FAULT),
    table("block-sync/s6v3/block/reserved-flags-closes", PROBE_FAULT),
    table("block-sync/s6v3/block/short-payload-closes", PROBE_FAULT),
    table("block-sync/s6v3/block/unsolicited-closes", PROBE_FAULT),
    table(
        "block-sync/s6v3/blocks-done/long-payload-closes",
        PROBE_FAULT,
    ),
    table(
        "block-sync/s6v3/blocks-done/reserved-flags-closes",
        PROBE_FAULT,
    ),
    table(
        "block-sync/s6v3/blocks-done/short-payload-closes",
        PROBE_FAULT,
    ),
    table(
        "block-sync/s6v3/blocks-done/unsolicited-closes",
        PROBE_FAULT,
    ),
    table("block-sync/s6v3/get-blocks/bad-content-closes", PROBE_FAULT),
    table(
        "block-sync/s6v3/get-blocks/long-payload-closes",
        PROBE_FAULT,
    ),
    table(
        "block-sync/s6v3/get-blocks/reserved-flags-closes",
        PROBE_FAULT,
    ),
    table(
        "block-sync/s6v3/get-blocks/short-payload-closes",
        PROBE_FAULT,
    ),
    table(
        "block-sync/s6v3/range-unavailable/long-payload-closes",
        PROBE_FAULT,
    ),
    table(
        "block-sync/s6v3/range-unavailable/reserved-flags-closes",
        PROBE_FAULT,
    ),
    table(
        "block-sync/s6v3/range-unavailable/short-payload-closes",
        PROBE_FAULT,
    ),
    table(
        "block-sync/s6v3/range-unavailable/unsolicited-closes",
        PROBE_FAULT,
    ),
    table("block-sync/s6v3/status/bad-content-closes", PROBE_FAULT),
    table("block-sync/s6v3/status/cadence-never-closes", PROBE_FAULT),
    table("block-sync/s6v3/status/long-payload-closes", PROBE_FAULT),
    table("block-sync/s6v3/status/reserved-flags-closes", PROBE_FAULT),
    table("block-sync/s6v3/status/short-payload-closes", PROBE_FAULT),
    table("block-sync/s6v3/undeclared-type-closes", PROBE_FAULT),
    table("block-sync/s6v3/block/bad-content-closes", EXCHANGE_FAULT),
    table("block-sync/s6v3/block/over-owed-closes", EXCHANGE_FAULT),
    table(
        "block-sync/s6v3/blocks-done/bad-content-closes",
        EXCHANGE_FAULT,
    ),
    table(
        "block-sync/s6v3/range-unavailable/bad-content-closes",
        EXCHANGE_FAULT,
    ),
    table("block-sync/s6v3/get-blocks/over-margin-closes", OVER_MARGIN),
    unreachable(
        "block-sync/s6v3/blocks-done/over-owed-closes",
        "every exchange owes room for its ending, and conformant bodies leave it",
    ),
    unreachable(
        "block-sync/s6v3/range-unavailable/over-owed-closes",
        "every exchange owes room for its ending, and conformant bodies leave it",
    ),
    unreachable(
        "block-sync/s6v3/blocks-done/receiver-work-authorized",
        "an ending carries no body, so the node admits no work from it",
    ),
    unreachable(
        "block-sync/s6v3/range-unavailable/receiver-work-authorized",
        "an ending carries no body, so the node admits no work from it",
    ),
    header("header-sync/s5v9/conformant-never-closes", EVERY),
    header("header-sync/s5v9/status/sender-cadence", EVERY),
    header(
        "header-sync/s5v9/subscribe-headers/sender-within-live",
        EVERY,
    ),
    header(
        "header-sync/s5v9/subscribe-headers/sender-within-credit",
        EVERY,
    ),
    header("header-sync/s5v9/headers/sender-answers-open", EVERY),
    header("header-sync/s5v9/headers-outcome/sender-ends-once", EVERY),
    header("header-sync/s5v9/undeclared-type-closes", STATUS),
    header("header-sync/s5v9/status/reserved-flags-closes", STATUS),
    header("header-sync/s5v9/status/short-payload-closes", STATUS),
    header("header-sync/s5v9/status/long-payload-closes", STATUS),
    header("header-sync/s5v9/status/bad-content-closes", STATUS),
    header("header-sync/s5v9/status/cadence-never-closes", STATUS),
    header(
        "header-sync/s5v9/subscribe-headers/reserved-flags-closes",
        SUBSCRIBE,
    ),
    header(
        "header-sync/s5v9/subscribe-headers/short-payload-closes",
        SUBSCRIBE,
    ),
    header(
        "header-sync/s5v9/subscribe-headers/long-payload-closes",
        SUBSCRIBE,
    ),
    header(
        "header-sync/s5v9/subscribe-headers/over-live-closes",
        SUBSCRIBE,
    ),
    header(
        "header-sync/s5v9/subscribe-headers/over-credit-closes",
        SUBSCRIBE,
    ),
    header(
        "header-sync/s5v9/subscribe-headers/after-close-closes",
        SUBSCRIBE,
    ),
    header(
        "header-sync/s5v9/subscribe-headers/bad-cursor-closes",
        SUBSCRIBE,
    ),
    header(
        "header-sync/s5v9/subscribe-headers/bad-content-closes",
        SUBSCRIBE,
    ),
    header("header-sync/s5v9/headers/reserved-flags-closes", HEADERS),
    header("header-sync/s5v9/headers/short-payload-closes", HEADERS),
    header("header-sync/s5v9/headers/long-payload-closes", HEADERS),
    header("header-sync/s5v9/headers/unsolicited-closes", HEADERS),
    header("header-sync/s5v9/headers/bad-content-closes", HEADERS),
    header(
        "header-sync/s5v9/headers-outcome/reserved-flags-closes",
        OUTCOME,
    ),
    header(
        "header-sync/s5v9/headers-outcome/short-payload-closes",
        OUTCOME,
    ),
    header(
        "header-sync/s5v9/headers-outcome/long-payload-closes",
        OUTCOME,
    ),
    header(
        "header-sync/s5v9/headers-outcome/unsolicited-closes",
        OUTCOME,
    ),
    header(
        "header-sync/s5v9/headers-outcome/bad-content-closes",
        OUTCOME,
    ),
    header_unreachable(
        "header-sync/s5v9/headers/over-owed-closes",
        NOT_OWED,
        HEADERS,
    ),
    header_unreachable(
        "header-sync/s5v9/headers-outcome/over-owed-closes",
        NOT_OWED,
        OUTCOME,
    ),
    discovery(
        "discovery/s4v2/conformant-never-closes",
        DISCOVERY_EVERY,
        Bypass::Planted("close-on-expired-hello"),
    ),
    discovery(
        "discovery/s4v2/undeclared-type-closes",
        HELLO_ACTS,
        DROP_UNDECLARED_TYPE,
    ),
    discovery(
        "discovery/s4v2/hello/reserved-flags-closes",
        HELLO_ACTS,
        DROP_RESERVED_FLAGS,
    ),
    discovery(
        "discovery/s4v2/hello/short-payload-closes",
        HELLO_ACTS,
        DROP_SHORT_PAYLOAD,
    ),
    discovery(
        "discovery/s4v2/hello/long-payload-closes",
        HELLO_ACTS,
        DROP_LONG_PAYLOAD,
    ),
    discovery(
        "discovery/s4v2/hello/bad-content-closes",
        HELLO_ACTS,
        Bypass::Planted("drop-forged-hello"),
    ),
    discovery(
        "discovery/s4v2/hello/cadence-never-closes",
        HELLO_ACTS,
        Bypass::Planted("close-on-empty-hello-bucket"),
    ),
    discovery(
        "discovery/s4v2/get-peers/reserved-flags-closes",
        GET_PEERS_ACTS,
        DROP_RESERVED_FLAGS,
    ),
    discovery(
        "discovery/s4v2/get-peers/short-payload-closes",
        GET_PEERS_ACTS,
        DROP_SHORT_PAYLOAD,
    ),
    discovery(
        "discovery/s4v2/get-peers/long-payload-closes",
        GET_PEERS_ACTS,
        DROP_LONG_PAYLOAD,
    ),
    discovery(
        "discovery/s4v2/get-peers/over-margin-closes",
        GET_PEERS_ACTS,
        Bypass::Planted("admit-past-margin"),
    ),
    discovery(
        "discovery/s4v2/get-peers/bad-content-closes",
        GET_PEERS_ACTS,
        Bypass::Planted("drop-refused-get-peers"),
    ),
    discovery(
        "discovery/s4v2/get-peers/cadence-never-closes",
        GET_PEERS_ACTS,
        Bypass::Planted("close-on-empty-get-peers-bucket"),
    ),
    discovery(
        "discovery/s4v2/peers/reserved-flags-closes",
        PEERS_ACTS,
        DROP_RESERVED_FLAGS,
    ),
    discovery(
        "discovery/s4v2/peers/short-payload-closes",
        PEERS_ACTS,
        DROP_SHORT_PAYLOAD,
    ),
    discovery(
        "discovery/s4v2/peers/long-payload-closes",
        PEERS_ACTS,
        DROP_LONG_PAYLOAD,
    ),
    discovery(
        "discovery/s4v2/peers/unsolicited-closes",
        PEERS_ACTS,
        Bypass::Planted("admit-unreserved-peers"),
    ),
    discovery(
        "discovery/s4v2/peers/over-owed-closes",
        PEERS_ACTS,
        Bypass::Planted("reserve-largest-peers"),
    ),
    discovery(
        "discovery/s4v2/peers/bad-content-closes",
        PEERS_ACTS,
        Bypass::Planted("drop-forged-relay"),
    ),
    discovery(
        "discovery/s4v2/get-services/reserved-flags-closes",
        GET_SERVICES_ACTS,
        DROP_RESERVED_FLAGS,
    ),
    discovery(
        "discovery/s4v2/get-services/short-payload-closes",
        GET_SERVICES_ACTS,
        DROP_SHORT_PAYLOAD,
    ),
    discovery(
        "discovery/s4v2/get-services/long-payload-closes",
        GET_SERVICES_ACTS,
        DROP_LONG_PAYLOAD,
    ),
    discovery(
        "discovery/s4v2/get-services/over-margin-closes",
        GET_SERVICES_ACTS,
        Bypass::Planted("admit-past-margin"),
    ),
    discovery(
        "discovery/s4v2/get-services/bad-content-closes",
        GET_SERVICES_ACTS,
        Bypass::Planted("drop-refused-get-services"),
    ),
    discovery(
        "discovery/s4v2/get-services/cadence-never-closes",
        GET_SERVICES_ACTS,
        Bypass::Planted("close-on-empty-get-services-bucket"),
    ),
    discovery(
        "discovery/s4v2/services/reserved-flags-closes",
        SERVICES_ACTS,
        DROP_RESERVED_FLAGS,
    ),
    discovery(
        "discovery/s4v2/services/short-payload-closes",
        SERVICES_ACTS,
        DROP_SHORT_PAYLOAD,
    ),
    discovery(
        "discovery/s4v2/services/long-payload-closes",
        SERVICES_ACTS,
        DROP_LONG_PAYLOAD,
    ),
    discovery(
        "discovery/s4v2/services/unsolicited-closes",
        SERVICES_ACTS,
        Bypass::Planted("admit-unreserved-services"),
    ),
    discovery(
        "discovery/s4v2/services/bad-content-closes",
        SERVICES_ACTS,
        Bypass::Planted("skip-services-node-id"),
    ),
    discovery(
        "discovery/s4v2/hello/sender-cadence",
        DISCOVERY_EVERY,
        Bypass::Planted("repeat-hello"),
    ),
    discovery(
        "discovery/s4v2/get-peers/sender-within-limit",
        DISCOVERY_EVERY,
        Bypass::Planted("ask-peers-twice"),
    ),
    discovery(
        "discovery/s4v2/get-peers/sender-cadence",
        DISCOVERY_EVERY,
        Bypass::Planted("re-ask-peers"),
    ),
    discovery(
        "discovery/s4v2/peers/sender-ends-once",
        GET_PEERS_ACTS,
        Bypass::Planted("answer-peers-twice"),
    ),
    discovery(
        "discovery/s4v2/get-services/sender-within-limit",
        DISCOVERY_EVERY,
        Bypass::Planted("ask-services-twice"),
    ),
    discovery(
        "discovery/s4v2/get-services/sender-cadence",
        DISCOVERY_EVERY,
        Bypass::Planted("re-ask-services"),
    ),
    discovery(
        "discovery/s4v2/services/sender-ends-once",
        GET_SERVICES_ACTS,
        Bypass::Planted("answer-services-twice"),
    ),
    discovery(
        "discovery/s4v2/peers/sender-within-owed",
        OWED_ANSWERS,
        Bypass::Pending(
            "every simulated peer binds an RFC 1918 address, which `is_sample_eligible` refuses, \
             so the node answers every GetPeers with no records and no limit bug can reach this \
             check yet",
        ),
    ),
    discovery(
        "discovery/s4v2/services/sender-within-owed",
        OWED_ANSWERS,
        Bypass::Planted("ignore-wanted-services"),
    ),
    discovery_unreachable(
        "discovery/s4v2/services/over-owed-closes",
        "the node asks for every service, so its GetServices owes the row's largest Services, \
         and a larger one is long-payload",
        SERVICES_ACTS,
    ),
    // Any production change that punishes an honest header-sync peer also
    // violates `conformant-never-closes`, and a planted case's input must
    // violate its property alone. The scripted bypass punishes an honest
    // peer whose frame a label excuses.
    Coverage {
        property: crate::domain::session::HONEST_NEVER_PUNISHED,
        passing: passes(HEADER_WITNESSES, "honest_peer_never_punished"),
        bypass: Bypass::Test(sim_test(
            HEADER_WITNESSES,
            "scripted_punishments_of_an_honest_peer_violate_honest_never_punished",
        )),
    },
    Coverage {
        property: crate::domain::header_sync_progress::BOUNDED_ADVERSARY_PROGRESS,
        passing: passes(
            "tests/header_recovery_progress.rs",
            "the_committed_fixtures_match_their_names",
        ),
        bypass: Bypass::Planted("skipped-apply"),
    },
    Coverage {
        property: crate::domain::header_sync_progress::HONEST_PROGRESS,
        passing: passes(
            "tests/header_wire_progress.rs",
            "the_progress_fixture_holds_honest_progress",
        ),
        bypass: Bypass::Planted("skipped-apply"),
    },
    // The stuck floor's witness test is ignored, since one debug run takes
    // minutes. `cargo xtask planted` runs its committed input unpatched as a
    // witness test that expects a hold, before it runs the patch.
    Coverage {
        property: crate::domain::block_sync::RETAINED_BYTE_BOUND,
        passing: passes(
            "tests/block_resource_properties.rs",
            "honest_runs_hold_every_resource_bound_a_b_a",
        ),
        bypass: Bypass::Planted("wide-commit-window"),
    },
    Coverage {
        property: crate::domain::block_sync::RESERVED_BYTE_BOUND,
        passing: passes(
            "tests/block_resource_properties.rs",
            "the_paused_handoffs_input_holds_reserved_byte_bound",
        ),
        bypass: Bypass::Planted("above-floor-charge"),
    },
    Coverage {
        property: crate::domain::block_sync::BUDGET_LEDGERS_AGREE,
        passing: passes(
            "tests/block_resource_properties.rs",
            "the_handoff_input_holds_budget_ledgers_agree",
        ),
        bypass: Bypass::Planted("handoff-keeps-charge"),
    },
    // The above-window witness test is ignored, since one debug run takes
    // over a minute, and runs only on demand. `cargo xtask planted` runs its
    // committed input unpatched as a witness test that expects a hold, before
    // it runs the patch.
    Coverage {
        property: crate::domain::block_sync::LOOKAHEAD_OVERSHOOT_BOUND,
        passing: Passing::Test(on_demand_test(
            "tests/block_resource_properties.rs",
            "the_above_window_input_holds_lookahead_overshoot_bound",
        )),
        bypass: Bypass::Planted("omit-outstanding-estimates"),
    },
    Coverage {
        property: crate::domain::block_sync::TEARDOWN_RETURNS_BASELINE,
        passing: passes(
            "tests/block_resource_properties.rs",
            "block_sync_teardown_returns_baseline",
        ),
        bypass: Bypass::Planted("skip-return-on-drop"),
    },
    Coverage {
        property: crate::domain::block_sync_semantic::SUBMIT_ONLY_REQUESTED,
        passing: passes(
            "tests/block_semantic_properties.rs",
            "the_reset_mid_flight_input_holds_submit_only_requested",
        ),
        bypass: Bypass::Planted("height-only"),
    },
    Coverage {
        property: crate::domain::block_sync_semantic::STALE_COMPLETION_INERT,
        passing: passes(
            "tests/block_semantic_properties.rs",
            "the_stale_beside_current_input_holds_stale_completion_inert",
        ),
        bypass: Bypass::Planted("release-by-height"),
    },
    // The capacity test is ignored, since a debug run takes about eight
    // minutes, and runs only on demand.
    Coverage {
        property: crate::domain::block_sync_semantic::CAPACITY_RELEASE_WAKES,
        passing: Passing::Test(on_demand_test(
            "tests/block_semantic_properties.rs",
            "the_frontier_first_input_holds_capacity_release_wakes",
        )),
        bypass: Bypass::Planted("stale-completion-no-wake"),
    },
    Coverage {
        property: crate::domain::block_sync_semantic::DETACHED_SUBMISSION_BOUNDED,
        passing: passes(
            "tests/block_semantic_properties.rs",
            "the_stalled_reset_input_holds_detached_submission_bounded",
        ),
        bypass: Bypass::Planted("forget-on-reset"),
    },
    // The decoded-input-stall test is ignored, since its debug run takes
    // minutes, and runs only on demand. The planted case runs the same
    // input.
    Coverage {
        property: crate::domain::block_sync_semantic::INPUT_DECODED_BOUND,
        passing: Passing::Test(on_demand_test(
            "tests/block_semantic_properties.rs",
            "the_decoded_input_stall_input_holds_input_decoded_bound",
        )),
        bypass: Bypass::Planted("no-room-check"),
    },
    Coverage {
        property: crate::domain::session::REPLACEMENT_PRESERVES_AUTHORIZATION,
        passing: passes(
            "tests/sync_wire.rs",
            "a_replaced_connection_reopens_every_service_under_fresh_ids",
        ),
        bypass: Bypass::Planted("stream-scoped-session-ids"),
    },
    Coverage {
        property: crate::domain::cross::TEARDOWN_RETURNS_BASELINE,
        passing: passes(
            "tests/sync_properties.rs",
            "teardown_across_held_work_returns_baseline",
        ),
        bypass: Bypass::Planted("leaked-reservation-entry"),
    },
    Coverage {
        property: crate::domain::session::STALLED_SERVICE_ISOLATED,
        passing: passes(
            "tests/sync_wire.rs",
            "a_stalled_block_session_starves_no_other_service",
        ),
        bypass: Bypass::Planted("close-on-block-stall"),
    },
    Coverage {
        property: crate::domain::connection::CREDIT_ISOLATED,
        passing: passes(
            "tests/sync_wire.rs",
            "a_saturating_service_starves_no_other_on_its_connection",
        ),
        bypass: Bypass::Planted("handshake-window-only"),
    },
    Coverage {
        property: crate::domain::connection::SEND_ISOLATED,
        passing: passes(
            "tests/sync_wire.rs",
            "a_saturating_service_starves_no_other_on_its_connection",
        ),
        bypass: Bypass::Pending(
            "raising block sync's send priority keeps the bound: QUIC buffers one send window \
             per stream and each worker writes one frame at a time, so no single production \
             site blocks other streams across several frames",
        ),
    },
    Coverage {
        property: crate::domain::connection::CONNECTION_SHARES_BOUND,
        passing: passes(
            "tests/sync_wire.rs",
            "a_saturating_service_starves_no_other_on_its_connection",
        ),
        bypass: Bypass::Planted("one-raw-gate"),
    },
    Coverage {
        property: crate::domain::cross::TIPS_CONSISTENT,
        passing: passes(
            "tests/sync_properties.rs",
            "a_replaced_selection_starts_a_new_body_work_epoch",
        ),
        bypass: Bypass::Planted("keep-epoch-on-replaced-selection"),
    },
    Coverage {
        property: crate::domain::cross::BLOCK_REQUESTS_FOLLOW_HEADERS,
        passing: passes(
            "tests/sync_properties.rs",
            "a_reorg_at_each_pipeline_stage_retires_old_epoch_work",
        ),
        bypass: Bypass::Planted("keep-work-queue-authority"),
    },
    Coverage {
        property: crate::domain::cross::HEADER_REORG_RETIRES_BLOCK_WORK,
        passing: passes(
            "tests/sync_properties.rs",
            "a_reorg_at_each_pipeline_stage_retires_old_epoch_work",
        ),
        bypass: Bypass::Planted("accept-stale-scope-bodies"),
    },
    Coverage {
        property: crate::domain::block_sync_semantic::QUERY_WORK_AUTHORIZED,
        passing: passes(
            "tests/sync_properties.rs",
            "a_reply_under_a_retired_authority_installs_nothing",
        ),
        bypass: Bypass::Planted("accept-retired-authority"),
    },
    Coverage {
        property: crate::domain::block_sync_semantic::FRONTIER_WORK_CONSISTENT,
        passing: passes(
            "tests/sync_properties.rs",
            "an_epoch_change_retires_the_old_epochs_work",
        ),
        bypass: Bypass::Planted("omit-work-reset"),
    },
    Coverage {
        property: crate::domain::block_sync_semantic::REACTOR_QUERY_LIFECYCLE,
        passing: passes(
            "tests/block_semantic_properties.rs",
            "the_reset_mid_flight_input_holds_reactor_query_lifecycle",
        ),
        bypass: Bypass::Pending(
            "no committed input is known to fill or close the action channel, so no planted \
             refusal bug can reach this check yet; checker tests cover crafted refusals",
        ),
    },
];
