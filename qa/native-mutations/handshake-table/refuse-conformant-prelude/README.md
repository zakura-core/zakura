# Refuse conformant prelude

The patch reverses the production magic comparison. A conformant prelude now fails its protocol guard.

The retained witness requires `handshake/prelude/conformant-never-refused` to hold before mutation. Run the mutation and generator lane on the droplet:

```sh
cargo xtask planted loop --only handshake/prelude/conformant-never-refused --generate-runs 100 --generate-harness handshake/wire
```

Required failure: the run ends at `handshake/prelude/conformant-never-refused`.
