# Spentness artifact tooling and peer distribution

This implementation supplies the first stage of spentness hints. It generates,
audits, distributes, and caches terminal UTXO membership. It does not change
checkpoint commits or enable hinted state construction.

The ordered writer, construction gates, blocking index rebuild at H, recovery
markers, and differential sync benchmarks form the next stage. Activation stays
off until those parts work together. A faster initial pass alone does not establish
an end-to-end sync improvement.

## Artifact and trust

The file stores an 86-byte header followed by one bit per transparent output.
Output order follows height, transaction index, and output index. Ordinal zero
includes the genesis coinbase output, whose bit must be zero. A set bit means
the ordinary state retains the output at the terminal checkpoint H.

The header encodes `ZKSHINT\0`, version 1, the serialized genesis hash, H,
the serialized terminal hash, and the output count. Integers use little-endian
encoding. Ordinal n uses bit `n % 8` of byte `n / 8`. Unused high bits must be zero.
The parser rejects trailing bytes and bounds files at 512 MiB before allocation.

`ParsedArtifact` exposes metadata for generation and review. `VerifiedArtifact`
exposes membership only after the complete file matches an expected commitment.
It owns the exact authenticated bytes. Runtime callers select commitments from
the compiled release list. A descriptor supplied by a peer cannot authorize a hint.

The compiled list starts empty because no public artifact has been reviewed.
The release importer adds descriptors and provenance. It keeps older descriptors
for future incomplete-run recovery. The bitmap never enters source data or the
executable. Checkpoint hashes alone do not authenticate terminal UTXO membership;
maintainers must review the generation evidence before accepting a descriptor.

## Generation and verification

Build both tools:

```sh
cargo build --release -p zakura-utils --features zakura-spentness \
  --bin zakura-checkpoints --bin zakura-spentness
```

`zakura-spentness replay` reads retained blocks from an archive database and
advances a separate ordinary archive state to exactly H. It disables VCT skipping.
It refuses a destination beyond H or on a different chain. It never rolls back
the source. The source must have validated the retained chain; this replay does
not repeat signatures, proof verification, or all contextual consensus checks.

`generate` requires the archive tip to equal H/hash. It merges serialized output
order with the ordered UTXO iterator. It checks complete matched entries and
rejects unmatched UTXOs. It reports output count, survivor count, file size, and hash.
Advancing H regenerates every membership bit, including bits for older outputs.

`verify` builds an independent transparent replay database under `$TMPDIR`, or
under Zakura's cache when `$TMPDIR` is unset. This oracle uses outpoints as keys.
It rejects missing, duplicate, future, and immature coinbase spends. It compares
complete terminal entries with the ordinary state. It then checks each hint bit
and compares salted sums of spent output outpoints and input outpoints modulo
2^256. The oracle does not call the ordinary writer or generator merge helpers.
The source node's full validation remains responsible for signatures, shielded
proofs, and other consensus rules.

```sh
zakura-spentness replay --source /data/archive --destination /data/replay \
  --height "$HEIGHT" --block-hash "$BLOCK_HASH"
zakura-spentness generate --state /data/replay --height "$HEIGHT" \
  --block-hash "$BLOCK_HASH" --output /data/hints.bin \
  --commitment /data/hints.commitment.json
zakura-spentness verify --state /data/replay --artifact /data/hints.bin \
  --commitment /data/hints.commitment.json --report /data/hints.verification.json
```

The publisher also reproduces the artifact from a separately synchronized archive.
Reproducibility and independent transparent replay provide different evidence.
Record the second source's validation software and identity in the bundle.

## Peer protocol and cache

Stream kind 8, version 1, uses capability bit 6. Each request carries the digest,
offset, and requested length. A response carries availability status, digest,
offset, length, and at most 256 KiB. The capability advertises protocol support.
The discovery service advertises availability only when startup loaded a verified
artifact. Nodes can serve newly verified bytes immediately; they advertise those
new cache entries after restart.

The server admits at most four concurrent range preparations. Each successful
preparation holds its slot for 250 ms, limiting aggregate data to 4 MiB/s.
The transport also applies its stream, frame, message, and connection limits.
Unavailable or busy servers return availability failure without a peer penalty.

The downloader selects peers that negotiated the capability. It tries at most
three sources, one source at a time. It retains interrupted progress under names
that bind the expected digest and peer identity. It checks every returned range
and verifies the complete file before durable cache publication. A whole-file
mismatch discards that source's partial file. It does not attribute a whole-file
mismatch to an individual chunk or disconnect the peer.

Enable artifact distribution explicitly:

```toml
[network.zakura]
spentness_cache_dir = "/data/zakura/spentness"
```

The startup task waits up to 60 seconds for a capable peer, then makes bounded
acquisition attempts. An unavailable artifact does not block ordinary sync.
Supported historical cache entries also remain available for serving.

For offline or seed provisioning, use a binary that contains the reviewed pin:

```sh
zakura-spentness install --artifact /data/hints.bin --cache /data/zakura/spentness
```

Cache names use the SHA-256 digest followed by `.bin`. Startup always reverifies
cache bytes. Seed operators retain supported artifacts and load them before
rolling out a release that selects their commitments. Nodes fetch artifacts from
peers; the HTTPS bundle publisher serves release automation, not node acquisition.

## Release-state schema 2

`zakura-checkpoints --mainnet-spentness-output` couples generation to its selected
checkpoint. Supply `--spentness-replay-cache` with the three treestate outputs.
`ZAKURA_SPENTNESS_BIN` selects the installed helper. The exporter writes commitment
and verification JSON sidecars beside the hint. It emits checkpoint stdout only
after generation and verification succeed. Each helper has a 48-hour deadline.

Schema 2 requires the hint, commitment, verification report, and frontier grid.
The publisher uploads data before metadata and moves `latest.json` last.
The importer checks the descriptor, format, digest, counts, genesis, shared H/hash,
and provenance before generating Rust commitments. It never imports bitmap bytes.
Version 2 bundles have no automatic newest-N deletion policy. Retention must cover
all supported incomplete hinted runs when the state writer is introduced.

Deploy the fetcher/importer before enabling the version 2 publisher. Configure
`RELEASE_STATE_ORACLE_SOURCE`, `RELEASE_STATE_ORACLE_ID`, and
`RELEASE_STATE_GENERATOR_REVISION`. `RELEASE_STATE_DATA_DIR` holds both retained
replay states. Budget archive-state disk space and full-history replay time.
Publication fails before moving the pointer if generation or verification fails.

Before rollout, provision each seed with
`deploy/release-state/provision-spentness-seed.sh`, configure its cache, and restart
it. Test a cold client against only those seeds and confirm the verified digest in
its log. The loopback integration test exercises this same peer acquisition path.
