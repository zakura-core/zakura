# Full-suite campaign 2026-10-29

This campaign runs every fuzz target of C1's suite manifest. `qa/suite.toml`
maps those targets, and `zakura-sim suite` checks that no target, harness
or property is missing from it. `manifest.toml` adds only the campaign's
choices: the PR heads it integrates, each target's shard, workers and
per-input timeout, and each segment's search budget.
`scripts/fuzz-campaign.py` refuses a `manifest.toml` whose targets differ
from the suite manifest's.

The campaign is incomplete. The suite manifest's `pending` rows list the
planned harnesses, properties, protocol-state features and targets that
haven't landed, such as H1, H2, H4, H5 and P8. They stay unimplemented in
the report even if every target runs clean.

## Validate and freeze

Run these commands from a clean checkout of the campaign commit. Set
`CAMPAIGN_ROOT` to persistent disk, not `/tmp` or `/dev/shm`.

```console
export CAMPAIGN_ROOT=/srv/zakura-fuzz/campaigns/full-suite-20261029
mkdir -p "$CAMPAIGN_ROOT"
RUSTFLAGS="--cfg zakura_sim" cargo test -p zakura-sim --locked
cargo xtask sim-suite --frozen --out "$CAMPAIGN_ROOT/suite.json"
python3 scripts/fuzz-campaign.py --campaign qa/fuzz/sim/campaigns/full-suite-20261029 \
  --suite-manifest "$CAMPAIGN_ROOT/suite.json" --check
```

`suite.json` is the campaign's frozen inventory. It records the commit,
the source and lockfile digests, `qa/suite.toml`'s digest, every target,
harness, property, witness and pending row, and the seed report. Its
target rows hold each target's 60-second smoke command over its seeds. Run
them before the calibration segment.

## Run a segment

A segment gives every target the segment's search budget, which the
launch script passes to libFuzzer as `-max_total_time`. The per-input
`-timeout` is the target's `input-timeout-secs`. An outer `timeout` stops
a run that outlives its budget, one input's timeout and five minutes.

Run the segments in order: `calibration`, `pilot`, then `extended`. Run a
segment's shards one after another, since each shard uses every vCPU:

```console
for shard in sim header-chain; do
  python3 scripts/fuzz-campaign.py --campaign qa/fuzz/sim/campaigns/full-suite-20261029 \
    --suite-manifest "$CAMPAIGN_ROOT/suite.json" --segment calibration --shard "$shard" \
    > "$CAMPAIGN_ROOT/calibration-$shard.sh"
  bash "$CAMPAIGN_ROOT/calibration-$shard.sh"
done
```

The script builds the shard's targets under AddressSanitizer, seeds each
`sim` target's corpus from its harness's admitted seeds once, and runs the
targets together. It leaves `ZAKURA_SIM_FEEDBACK` unset, so semantic
feedback stays off: no harness emits protocol-state features yet. Each run
writes `run.log`, `exit-status`, libFuzzer's artifacts and
`ZAKURA_SIM_ARTIFACTS`' classified inputs under
`$CAMPAIGN_ROOT/segments/<segment>/<shard>/<package>/<target>/`. The
shard's directory also holds `segment.json`, `suite.json`, and the start
and finish times. Every segment of a target grows the same corpus,
`$CAMPAIGN_ROOT/corpus/<package>/<target>/`.

A segment's script refuses to reuse a segment's directory. To continue an
interrupted segment, run its next attempt with `--attempt 2` (then 3, and
so on), optionally limited to the unfinished targets with `--target <id>`.
The attempt writes `segments/<segment>.<attempt>/`, records the previous
attempt as its parent, and leaves the earlier evidence in place.

Set the extended budget from the pilot's measured cost in a new revision of
`manifest.toml` before the extended segment starts. A source, build or
generator change starts a new campaign directory; it can't reuse the old
segments' results as evidence for the new commit.

## Reporting

Treat every crash, timeout, abandonment, nondeterministic result and
property violation as evidence. Preserve the input before triage. Replay
ordinary property failures exactly. Report each target's budget, exit
status and finished segments beside the suite manifest's rows. Report
correctness findings separately from C1–C4 completeness.
