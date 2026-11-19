# Handshake guard coverage

The `handshake-table` entry covers hello, ack, and prelude validation once.
Each reachable production guard gets one patch. Every affected generated
property gets a case for that patch. The exceptions below identify checks that
the native production policy cannot reach. Admission and teardown use separate entries because they
also run through the admission machine and QUIC endpoint.

The initiator's second nonce comparison is equivalent: its caller passes
`ack.peer_nonce` as `remote_peer_nonce`, so the comparison cannot reject an
exchange. The nonce-echo comparison remains independently testable. Do not count
a mutation of the second comparison as a killed security guard.

The work list registers the required witnesses. Mutation generation, remote
classification, and survivor triage must still cover every active rule before
this plan is complete.

The `b0ba4955a1` droplet rerun killed both previously inconclusive guards with
their named retained witnesses. The selected-version generator also killed
`handshake/prelude/unselected-version-resets` at seed 11, run 87, and fresh
confirmation passed. Shrinking rejected that trace because it also violated
`session/installed-services-authorized`. This is a confirmed finding without a
successful shrink. The requested-limit generator never applied its property in
100 runs at seed 11; its retained-witness kill does not establish generator reach.

The follow-up requested-limit search reached its named rule at seed 13, run 67.
Fresh confirmation and shrinking passed. Seed 12 did not apply the property in
100 runs. The report is `/root/handshake1-limits-seeds12-13/report/report.json`.

## Native policy reach

`ZakuraHandshakeConfig::for_network_with_dev_cohort` initializes
`required_capabilities` to zero. The production handler keeps that field;
`current_handshake_config` changes only `supported_capabilities`. Consequently,
removing the hello's required-capability-mask comparison cannot admit an extra
native production handshake under this policy. The wire generator does not
claim a kill for that comparison. A future nonzero production requirement must
add a corresponding offered-capability witness.

The adjacent required-channel comparison remains reachable. The peer's
`RequiredChannel` fault sets an unsupported channel mask, and
`skip-required-channel` must fail `handshake/hello/missing-capability-refused`.
These checks share a production error variant, but that shared label does not
establish coverage of both branches.

Upgrade nonce and transcript checks under `ZakuraHandshakePath::Upgraded`
belong to the excluded legacy upgrade path. Native initiator nonce, responder
nonce, and transcript checks each have a separate retained mutation.
