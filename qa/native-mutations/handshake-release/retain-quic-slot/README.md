# Retain the QUIC slot

The patch retains one production connection-slot owner after the connection drains. The handler releases its permits, but the QUIC charge survives. The slot probe never reports release because the last owner never drops.

Both control roles must fail their `refusal-releases-slot` property. Run the cases and generated scenarios on the droplet:

```sh
cargo xtask planted loop --only handshake/hello/refusal-releases-slot --only handshake/ack/refusal-releases-slot --generate-runs 100 --generate-harness handshake/wire
```
