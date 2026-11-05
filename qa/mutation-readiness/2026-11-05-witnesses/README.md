# mut-1.3: witness inventory and selected target readiness

This slice reconciles the old own-2/4/5/7/8 witness list with executable tests.
It prepares five existing simulator targets for the proposed bounded pilot.
It does not close the full-stack witness inventory or score mutations.

## Witness inventory

`witnesses.json` maps all 31 requirements from the superseded own-15 inventory.
Each row names exact test selectors, source files, remaining work, and relevant
open PRs. Thirty-five unique mapped native tests passed individually with
`--exact`; one earlier candidate also passed. The compiled library listed
2,638 tests. Selection required one executed test with no ignored tests.
Each child had a ten-second limit and the first batch had a 90-second limit.
The tests completed within those limits.

The inventory distinguishes 23 requirements with existing native tests,
four with partial native coverage, three requiring new work, and one deferred
multi-service integration requirement. Native tests do not establish a
simulator witness, an intended mutation failure, or fresh-process replay.
Those evidence layers remain open even when a native test passes.

The three new-work rows require shared-work rejection after the block
reactor's observation, writer start during full request publication, and
bounded discovery dial progress. Partial rows preserve the precise missing
interleaving: composed block peer-task panic, session-guard cleanup under
contention, capacity release during waiter setup, and cancellation during
executing decode. An already-registered waiter or a decode-capacity wait does
not prove the harder interleaving.

The native tests built at `8c63fa3ec2348b036e1a24bd9fa6c677eca49c57`
with Rust 1.99.0, locked workspace dependencies, empty `RUSTFLAGS`, and the
test profile. `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, and
`CARGO_INCREMENTAL=0` bounded disk use. The library build took 83 seconds.
The subsequent fuzz-lock commit changes no production Rust source.

## Reuse of pending header work

GitHub reported #2027, #2030, #2032, and #2033 open and unmerged during this
inspection. Their reported tests are not passing evidence at this pinned head.

- [#2027](https://github.com/zakura-core/zakura/pull/2027) supplies header-node
  shutdown and reservation/session triggers. Reuse those hooks for later
  header integration witnesses. It does not supply block-node shutdown.
- [#2030](https://github.com/zakura-core/zakura/pull/2030) supplies session/link
  joins and a replacement witness with late completion. Reuse its exact
  session identities when lifting publisher/retirement cases.
- [#2032](https://github.com/zakura-core/zakura/pull/2032) and
  [#2033](https://github.com/zakura-core/zakura/pull/2033) supply adopted-state
  facts and a growth witness with a held publisher read. They do not replace
  the publisher output-saturation or abandoned-job-panic witnesses.

Multi-service simulator composition remains deferred with sim-9.3/xsync-3.
The native test for another service entering during discovery's answer wait
passes, but it does not close that composition gap. No new deferral approval
is claimed.

## Target readiness

The selected targets are `header_reactor_policy`, `header_publisher_policy`,
`block_reactor_policy`, `block_decoded_retention`, and `discovery_record_book`.
They match the mut-2 candidate catalogue. The existing #1863 readiness command
runs their curated seeds and checks nonzero property applicability, target
identity, seed accounting, and target execution. It does not run a discovery
campaign or prove mutation sensitivity.

The initial build exposed a stale `qa/fuzz/sim/Cargo.lock`. Cargo refreshed
the lock to match current manifests, including renamed Zakura dependencies,
`ref-cast`, and `sha2-asm`. The readiness guard correctly rejected a build
whose lock changed. Commit `b2e0456474e47e6264c912b2f6b50d09c8603115`
retains that refresh. Final readiness uses this source revision and the refreshed lock.

The fuzz lock SHA-256 is
`c12fdc71839a7f02767475d5fbacde8433731fb951c786524dc43769aacca4dc`.
Readiness uses the repository's pinned `nightly-2026-07-15`, cargo-fuzz 0.13.2,
and `--sanitizer none`. It gives each seed a 15-second child limit. Cargo-fuzz
keeps its standard optimized instrumentation and debug assertions. The
environment disables incremental compilation and requests no release debug
information; cargo-fuzz still supplies line-table information.

All five targets passed at the clean source revision above. Six curated seeds
executed, and every intended property had nonzero applicability. No seed was
quarantined. `readiness-initial.json` retains three successful target rows and
the two rejected build rows. `readiness-retry.json` records the clean retries
for discovery and decoded retention. The adjacent logs retain both commands'
output. `targets/` retains each successful target's execution log and statistics.

| Target | Curated seeds | Result | Raw report |
| --- | ---: | --- | --- |
| `header_reactor_policy` | 1 | Ready | `readiness-initial.json` |
| `header_publisher_policy` | 1 | Ready | `readiness-initial.json` |
| `block_reactor_policy` | 2 | Ready | `readiness-initial.json` |
| `block_decoded_retention` | 1 | Ready | `readiness-retry.json` |
| `discovery_record_book` | 1 | Ready | `readiness-retry.json` |

The initial command had a 900-second outer limit. The two-target retry had a
600-second outer limit. Both completed before those limits. The initial
command returned failure because the two builds changed identity; the retry
returned success. The successful rows share the same source and lock hashes.

Block-storage and header-wire targets remain outside this selection. The
baseline discovery lost-trigger and header-wire saved-end failures from
mut-1.1 remain open. Readiness for these five targets does not resolve them.

## Artifact checks

All six mutation inventory tests passed. The artifact check confirmed that all
35 mapped selectors have a passing exact-test log and that every mapped source
file exists. `python3 scripts/changelog.py check` passed. `SHA256SUMS` covers
the retained evidence files.

## Reproduce

Use disk-backed directories for builds and output. Build the simulator from
the pinned source with Rust 1.99.0 and `RUSTFLAGS='--cfg zakura_sim'`. Then run
the existing `targets` subcommand used by `cargo xtask sim-targets`:

```sh
CARGO_BUILD_JOBS=8 CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_INCREMENTAL=0 \
  "$SIM" targets --out "$TMPDIR/readiness" \
  --target header_reactor_policy --target header_publisher_policy \
  --target block_reactor_policy --target block_decoded_retention \
  --target discovery_record_book --sanitizer none --child-secs 15 \
  --target-dir "$TMPDIR/readiness-build"
```

For native checks, compile `cargo +1.99.0 test --locked -p zakura-network
--lib --no-run` with the native build environment above. Set `NETWORK_TEST`
to the emitted test executable. The recorded selector list runs only the
mapped tests:

```sh
while IFS= read -r selector; do
  timeout 10 "$NETWORK_TEST" "$selector" --exact --test-threads=1
done < qa/mutation-readiness/2026-11-05-witnesses/native/selectors.txt
```

Require one passed test per invocation. Keep timeouts, ignored tests, and
zero selections as unresolved evidence.
