# Skip requested limit ceilings

The initiator accepts limits above its request while retaining the local hard limits.

The patch changes production validation only. Honest fields still pass the remaining checks. Each witness corrupts one field at a time and requires its named refusal.

## Reproduce

Run on the droplet:

```sh
cargo xtask planted loop --only handshake/ack/limits-above-request-refused --generate-runs 100 --generate-harness handshake/wire
```

Witness: `crates/zakura-sim/tests/handshake.rs::ack_guards_refuse_each_corrupt_field`.

Required failure: the run ends at `handshake/ack/limits-above-request-refused`.

## Survivor triage

The `d23f46bbb1` campaign survived because the original witness increased the
stream limit above both the request and the local hard ceiling. The earlier
hard-limit check still refused it. The witness now increases the requested idle
timeout by one millisecond. That value stays below the local hard ceiling and
isolates the requested-limit check. A new droplet classification must verify the
named rule.
