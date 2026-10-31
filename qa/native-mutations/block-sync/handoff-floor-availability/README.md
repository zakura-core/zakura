# Receipt preemption witness

`missing-availability.patch` removes the availability flag from the production
receipt owner. A scheduler can then pause a synchronous transfer before request
settlement while other peers still rank that peer as available for floor work.

Run `paused_handoffs_pass_m_plus_2b_within_the_handoff_allowance` in the
`zakura-sim` integration test `block_resource_properties` with `RUSTFLAGS="--cfg
zakura_sim"`. The baseline reaches the required peak above `M + 2B`. The mutant
fails that peak assertion. Restore the patch and rerun the test. A compilation
failure or timeout does not count as detection.

This scenario uses real protocol tasks and QUIC over simulated datagrams. The
native test `synchronous_receipts_do_not_wake_floor_rankers` separately ensures
that the synchronous receipt does not broadcast a floor-ranking notification.
Keep the existing notification on an actual sequencer-admission wait.

The peak assertion is an integration-test witness. It is not a registry property
violation, so a passing reserved-byte-bound replay alone does not prove that
this mutation was detected.
