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

`verify` makes one pass over the canonical blocks. In that pass, an independent
transparent replay oracle builds its own UTXO database under `$TMPDIR`, or under
Zakura's cache when `$TMPDIR` is unset. This oracle uses outpoints as keys. It
rejects missing, duplicate, future, and immature coinbase spends. The same pass
reads the ordinary state at each output location. Each bit must equal the
output's presence there, and each present entry must equal its creating output.
After the pass, each oracle survivor must have a set bit. The oracle, the set
bits, and the ordinary state must hold the same number of UTXOs. The oracle does
not call the ordinary writer or generator merge helpers. The source node's full
validation remains responsible for signatures, shielded proofs, and other
consensus rules.

`replay`, `generate`, and `verify` read and deserialize blocks on worker threads.
Each block must match the canonical hash at its height. Its transactions must
match the header's Merkle root and contain no duplicates.

```sh
zakura-spentness replay --source /data/archive --destination /data/replay \
  --height "$HEIGHT" --block-hash "$BLOCK_HASH"
zakura-spentness generate --state /data/replay --height "$HEIGHT" \
  --block-hash "$BLOCK_HASH" --output /data/hints.bin \
  --commitment /data/hints.commitment.json
zakura-spentness verify --state /data/replay --artifact /data/hints.bin \
  --commitment /data/hints.commitment.json --report /data/hints.verification.json
```

The publisher runs two pipelines concurrently. The primary pipeline replays the
publisher's archive and generates the published artifact. The independent pipeline
replays a separately synchronized archive, generates the artifact again, and
verifies that reproduction with `--report`. The publisher requires byte-identical
artifacts and commitments. Reproducibility and independent transparent replay
provide different evidence. Record the second source's validation software and
identity in the bundle.

The primary pipeline does not run `verify`. Identical bytes make the independent
verification cover the published artifact. Both archives hold the same canonical
blocks, because every pass checks each block's hash and Merkle root. Primary
generation requires the primary UTXO set to equal the set bits. The primary
UTXO set therefore equals the membership that the oracle verified.

## Peer protocol and cache

Stream kind 8, version 1, uses capability bit 6. Each request carries the digest,
offset, and requested length. A response carries availability status, digest,
offset, length, and at most 256 KiB. The capability advertises protocol support.
Status values distinguish absent (0), available (1), busy (2), insufficient
response capacity (3), and a range outside the artifact (4). Negative responses
echo the digest and offset with length zero. These statuses do not penalize peers.
The stream declares its message table, whose only response ends the exchange.
The requester therefore admits exactly one response. It closes the connection on
a second or missing response.

Distribution uses only supported commitments: release commitments minus
`REVOKED_COMMITMENTS`. A node never downloads, serves, or advertises a revoked
artifact. The discovery service advertises availability only when startup loaded
a verified artifact. It seeks the service only when startup found a supported
artifact missing. Nodes can serve newly verified bytes immediately; they advertise
those new cache entries after restart.

The server bounds serving with two byte token buckets. The node bucket allows
8 MiB/s across all peers. Each peer bucket allows 2 MiB/s, so one peer takes at
most a quarter of the node rate. Each bucket holds one second of its rate. A
request that either bucket cannot cover gets an immediate busy reply. The server
never delays a reply, so an idle server answers at transport speed.
The transport also applies its stream, frame, message, and connection limits.

The downloader selects peers that negotiated the capability. It tries one source
at a time and rotates sources between rounds. At most three sources per round may
transfer bytes. A source that replies absent before it transfers bytes does not
count. A busy reply pauses the same source for 125 ms to 2 s, then retries the same
range. The downloader reduces the requested range when the peer reports
insufficient response capacity.

The downloader retains interrupted progress under names that bind the expected
digest and peer identity. It keeps at most three partial files per digest and
deletes the shortest ones first. It checks every returned range and verifies the
complete file before durable cache publication. Success deletes every partial file
for that digest. Startup deletes partial files for digests that are held or
unsupported. A whole-file mismatch, a format error, or a truncated file discards
that source's partial file. A local read error keeps it. The downloader does not
attribute a whole-file mismatch to an individual chunk or disconnect the peer.

Enable artifact distribution explicitly:

```toml
[spentness]
cache_dir = "/data/zakura/spentness"
```

The startup task waits up to 60 seconds for a capable peer, then makes bounded
acquisition attempts. It retries missing artifacts after a 60-second pause until
acquisition succeeds or the endpoint shuts down. The first failure for each
artifact logs a warning; later failures log at debug level. Each source has a
ten-minute deadline. An unavailable artifact does not block ordinary sync.
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

The publisher runs `zakura-checkpoints` first and reads H/hash from the last line
of its checkpoint list. It then runs both spentness pipelines at H.
`ZAKURA_SPENTNESS_BIN` selects the installed helper.
`RELEASE_STATE_SPENTNESS_TIMEOUT` bounds each pipeline and defaults to 48 hours.
The helpers keep verification scratch under `$RELEASE_STATE_DATA_DIR/tmp`. The
publisher empties that directory before each run and removes it afterwards.
Publication fails before any upload if either pipeline fails or the artifacts differ.

`generated_at` records when the exporter selected H. Pipeline time therefore
counts against the fetcher's 48-hour freshness window. Both replays resume from
their previous H, so a daily run replays only new blocks. The primary pipeline
then reads the chain once, and the independent pipeline reads it twice. The two
pipelines run concurrently. The first run replays both archives from genesis and
can produce a bundle that is already too old to import.

Schema 2 requires the hint, commitment, verification report, and frontier grid.
The publisher uploads data before metadata and moves `latest.json` last.
The importer checks the descriptor, format, digest, counts, genesis, shared H/hash,
and provenance before generating Rust commitments. It never imports bitmap bytes.
The verification report in the bundle comes from the independent pipeline. After
a spentness descriptor is committed, the fetcher and importer reject schema 1
bundles, which carry no descriptor.
Version 2 bundles have no automatic newest-N deletion policy. Retention must cover
all supported incomplete hinted runs when the state writer is introduced.

Deploy the fetcher/importer before enabling the version 2 publisher. Configure
`RELEASE_STATE_ORACLE_SOURCE`, `RELEASE_STATE_ORACLE_ID`, and
`RELEASE_STATE_GENERATOR_REVISION`. `RELEASE_STATE_DATA_DIR` holds both retained
replay states. Budget archive-state disk space and full-history replay time.
The archive deployment installs `zakura-checkpoints` and `zakura-spentness` at the
revision recorded in `EXPORTER_REVISION`.

Before rollout, provision each seed with
`deploy/release-state/provision-spentness-seed.sh`, configure its cache, and restart
it. Test a cold client against only those seeds and confirm the verified digest in
its log. The loopback integration test exercises this same peer acquisition path.
