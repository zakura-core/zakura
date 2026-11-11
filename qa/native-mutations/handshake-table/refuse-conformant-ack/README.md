# Refuse conformant ack

The patch reverses the production magic comparison. A conformant ack now fails its protocol guard.

The retained witness requires `handshake/ack/conformant-never-refused` to hold before mutation. Run the mutation and generator lane on the droplet:

```sh
cargo xtask planted loop --only handshake/ack/conformant-never-refused --generate-runs 100 --generate-harness handshake/wire
```
