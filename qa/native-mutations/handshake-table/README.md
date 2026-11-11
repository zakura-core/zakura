# Handshake guard coverage

The `handshake-table` entry covers hello, ack, and prelude validation once.
Each production guard gets one patch. Every affected generated property gets a
case for that patch. Admission and teardown use separate entries because they
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
