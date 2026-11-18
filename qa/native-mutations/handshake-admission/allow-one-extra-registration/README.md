# Allow one extra registration

The address comparison admits a seventeenth registered connection. The patch
keeps pending admission and the same-address replacement exception unchanged.

The wire witness staggers seventeen conformant handshakes on one address.
The machine generator also draws a burst that registers eighteen connections
on one address. Both harnesses judge the production registration decision.

## Reproduce

Run on the droplet, with both generator harnesses from the work list:

```sh
cargo xtask planted loop --only handshake/connections-per-address-bounded --generate-runs 100
```

Witness: `crates/zakura-sim/tests/handshake.rs::shared_address_registrations_stop_at_the_cap`.

Required failure: the run ends at `handshake/connections-per-address-bounded`.
