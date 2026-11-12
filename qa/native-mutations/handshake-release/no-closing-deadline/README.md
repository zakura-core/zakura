# Remove the connection closing deadline

The patch restores RTT-based draining after a control handshake fails.
A nine-second return path extends QUIC closing-state ownership beyond the
three-second resource-release deadline.

Witness: `slow_link_refusal_releases_the_quic_slot` in `tests/handshake.rs`.

Required failure: the run ends at `handshake/hello/refusal-releases-slot`.

The unchanged witness must hold before the runner applies the patch.
The wire generator reaches this case through mixed TLS/control pressure in
its `dial-reserve` family.
