//! The machines' op tapes, written by hand: the inputs that reach each
//! machine's rules, which `replay_exact.rs` replays and `targets.rs` runs on
//! each machine target.

/// `block/work-ownership`'s op tape: two owners take, publish and receive
/// heights, then return them.
pub const WORK_OWNERSHIP: [u8; 40] = [
    0, 0, 0, 9, 1, 0, 0, 1, 2, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 9, 1, 0, 0, 1, 2, 1, 0, 0, 5, 0, 0, 0,
    4, 1, 0, 0, 5, 1, 0, 0,
];

/// `block/work-funding`'s op tape: it retains a receipt through a reset,
/// then admits, publishes and returns a replacement. zakura-network's
/// funding tests replay the same bytes as their machine target's witness.
pub const WORK_FUNDING: [u8; 56] = [
    0, 0, 1, 99, 1, 0, 9, 3, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 8, 0, 0, 0, 17, 0, 0,
    0, 0, 0, 1, 99, 1, 0, 9, 3, 2, 1, 0, 0, 3, 1, 0, 0, 12, 0, 0, 1, 11, 0, 0, 0,
];

/// `block/decoded-retention`'s op tape. Each op is `[opcode, source slot,
/// destination slot, pool]`, and the opcode modulo 7 picks admit, clone,
/// retain header, retain transaction, drop, nothing or close. Pools 0 and 1
/// hold three blocks each, and pool 2 holds two.
pub const DECODED_RETENTION: [u8; 80] = [
    0, 0, 0, 0, // admit into slot 0 from pool 0
    0, 0, 4, 1, // admit into slot 4 from pool 1
    1, 0, 3, 0, // clone slot 0 into slot 3
    2, 0, 1, 0, // retain slot 0's header in slot 1
    3, 0, 2, 0, // retain slot 0's transaction in slot 2
    4, 0, 0, 0, // drop slot 0
    5, 1, 0, 2, // nothing
    4, 3, 0, 0, // drop slot 3
    4, 1, 0, 0, // drop slot 1
    6, 0, 0, 2, // close pool 2
    5, 2, 0, 1, // nothing
    4, 2, 0, 0, // drop slot 2
    4, 4, 0, 0, // drop slot 4
    0, 0, 0, 2, // admit from pool 2, which is closed
    0, 0, 0, 0, // admit into slot 0 from pool 0
    0, 0, 1, 0, // admit into slot 1 from pool 0
    0, 0, 2, 0, // admit into slot 2 from pool 0, its third block
    0, 0, 3, 0, // admit a fourth block from pool 0, past its limit
    5, 0, 0, 1, // nothing
    6, 0, 0, 0, // close pool 0
];

/// `header/publisher-policy`'s op tape, the publisher machine's `WITNESS`
/// in `publisher/machine/tests.rs`: open, grant, read a page, close, end,
/// take and write the outcome, then a stale page.
pub const PUBLISHER_POLICY: [u8; 32] = [
    0, 0, 4, 35, 10, 0, 0, 0, 15, 0, 0, 0, 8, 0, 0, 0, 23, 0, 0, 0, 24, 0, 0, 0, 25, 0, 0, 0, 15,
    0, 0, 128,
];

/// `header/reactor-policy`'s op tape, the reactor machine's `WITNESS` in
/// `reactor/machine_tests.rs`: two admissions, reads started and refused,
/// stale completions, a close refused, retried and expired, a cached pass, a
/// resource stall, a commit, a repair and its stall, refresh stalls and a
/// rotation.
pub const HEADER_REACTOR_POLICY: [u8; 104] = [
    0, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 1, 3, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 2, 3, 0, 0, 0,
    4, 0, 0, 0, 4, 1, 0, 0, 7, 0, 0, 1, 6, 0, 232, 3, 8, 0, 0, 0, 7, 0, 1, 1, 6, 0, 136, 19, 8, 0,
    0, 0, 9, 0, 0, 0, 10, 2, 0, 0, 11, 3, 0, 0, 13, 0, 0, 0, 14, 0, 0, 0, 15, 0, 0, 0, 6, 0, 96,
    234, 15, 0, 0, 0, 15, 0, 0, 0, 12, 0, 0, 0,
];

/// `block/reactor-policy`'s op tape that offers, refuses and retries
/// queries across refills and committed views.
pub const REACTOR_POLICY: [u8; 60] = [
    0, 0, 1, 200, 0, 8, 2, 200, 7, 0, 200, 0, 3, 0, 0, 0, 1, 0, 0, 1, 6, 2, 1, 0, 6, 3, 0, 0, 5, 0,
    1, 0, 1, 0, 0, 0, 0, 0, 1, 200, 2, 0, 1, 0, 7, 0, 200, 0, 3, 0, 0, 0, 4, 0, 30, 0, 6, 0, 3, 0,
];

/// `block/reactor-policy`'s op tape that keeps a query's identity across a
/// committed view.
pub const REACTOR_IDENTITY: [u8; 16] = [0, 0, 1, 200, 1, 0, 1, 0, 4, 0, 30, 0, 0, 9, 1, 200];

/// `block/work-funding`'s op tape: one session publishes 1,000 above-floor
/// bytes, overdraws 500 more with a floor request, and its next floor
/// request would pass the owner cap's floor total of 1,500 bytes.
/// zakura-network's funding tests replay the same bytes.
pub const WORK_OWNER_CAP: [u8; 40] = [
    0, 0, 7, 249, 1, 2, 31, 10, 2, 0, 0, 0, 3, 0, 0, 0, 1, 6, 15, 5, 2, 1, 6, 0, 3, 1, 0, 0, 0, 8,
    1, 249, 1, 8, 7, 3, 2, 2, 8, 0,
];

/// `handshake/counts`' op tape: one host's connection moves through QUIC,
/// control and registration.
pub const COUNTS_OWNERS: [u8; 12] = [0, 0, 0, 2, 0, 0, 3, 0, 0, 5, 0, 0];

/// `handshake/counts`' op tape: inbound TLS owners contend with outbound
/// control claims for the same budget.
pub const COUNTS_PENDING: [u8; 3] = [128, 1, 128];

/// `handshake/counts`' op tape: thirty-two immediate failures exhaust one
/// address's sixteen-token burst.
pub fn counts_cadence() -> Vec<u8> {
    [8, 0, 0].repeat(32)
}

/// Each machine's op tapes, by the machine's harness name, each named. A
/// machine reads its input as ops, so a tape is its scenario, written here.
pub fn tapes(machine: &str) -> Vec<(&'static str, Vec<u8>)> {
    match machine {
        "block/work-ownership" => vec![("work-ownership", WORK_OWNERSHIP.to_vec())],
        "block/reactor-policy" => vec![
            ("reactor-identity", REACTOR_IDENTITY.to_vec()),
            ("reactor-policy", REACTOR_POLICY.to_vec()),
        ],
        "block/work-funding" => vec![
            ("work-funding", WORK_FUNDING.to_vec()),
            ("work-owner-cap", WORK_OWNER_CAP.to_vec()),
        ],
        "block/decoded-retention" => vec![("decoded-retention", DECODED_RETENTION.to_vec())],
        "discovery/record-book" => vec![(
            "record-book",
            zakura_network::zakura::discovery_book_witness_bytes(),
        )],
        "discovery/session-installation" => vec![(
            "session-installation",
            zakura_network::zakura::DISCOVERY_STATE_WITNESS.to_vec(),
        )],
        "handshake/counts" => vec![
            ("counts-owners", COUNTS_OWNERS.to_vec()),
            ("counts-pending", COUNTS_PENDING.to_vec()),
            ("counts-cadence", counts_cadence()),
        ],
        "header/publisher-policy" => vec![("publisher-policy", PUBLISHER_POLICY.to_vec())],
        "header/reactor-policy" => vec![("reactor-policy", HEADER_REACTOR_POLICY.to_vec())],
        _ => Vec::new(),
    }
}
