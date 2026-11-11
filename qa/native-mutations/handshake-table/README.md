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
