# Committed-tip relay validation

This experiment isolates the committed-tip relay scheduler. It does not measure
mainnet orphan rates, consensus validation, an independent Zebra node, or mining
pool job adoption.

## Implementations

The baseline is main commit `a3c599392af5dbffbe9acb784289c7c12ce04dda`.
The fixed implementation and regression fixtures are included in
[PR #1170](https://github.com/zakura-core/zakura/pull/1170).
Both arms use identical regression tests and `Cargo.lock`. The baseline arm
replaces only the production portions of `sync/gossip.rs` and
`zakura/legacy_gossip.rs`, retaining all candidate tests.

The baseline sleeps seven seconds before observing the next committed tip and
consumes the tip before delivery succeeds. The fix observes state independently
of delivery, retains the newest pending tip, permits one ordinary readiness/send
operation at a time, and retries failures after one second. Successful completion
only clears a matching pending tip. Mined-block lifecycle events remain separate;
early inventory cannot suppress committed delivery. Transaction gossip keeps its
seven-second spacing.

The adapter changes make explicit block retries reach the native send path and
report an error when every enabled route fails. Incoming echo deduplication is
preserved. Route acceptance is not a peer acknowledgement: native acceptance can
retain the latest tip for replay to a future peer. Partial delivery and timeouts
can cause bounded duplicate announcements. The existing dual-stack join still
waits for both routes; an unready legacy route can time out even after native
acceptance.

## Measurements

Measurements were made on macOS 26.2, Rust/Cargo 1.98.1, with the default debug
test profile. Synthetic tests use paused Tokio time and direct selected-tip
notifications. TCP tests use wall-clock time, the real legacy peer set and an
isolated TCP peer over loopback. Historical mainnet block vectors are committed
through the trusted checkpoint test interface, bypassing consensus verification.

The TCP origin is the completion of the block commit. Inventory receipt is
recorded when the remote connection decodes `inv`; body receipt follows a
`BlocksByHash` request over the same TCP connection. If inventory arrives before
the commit future returns, the measured post-completion delay is zero.

| Measurement | Baseline | Fixed |
| --- | ---: | ---: |
| Commit completion to legacy peer inventory receipt | 6,995 ms | 4 ms / 3 ms |
| Commit completion to full body receipt | 6,996 ms | 5 ms / 4 ms |
| Closely spaced synthetic selected-tip change | 7,001 ms | 1 ms |

The fixed TCP measurement was repeated twice. The fixed same-height and idle
synthetic cases each took 1 ms. A stalled 20-tip burst produced one request and a
peak of one ordinary operation in flight. Permanent send failure caused 21
attempts over 20 seconds; successful recovery stopped retries.

These are local relay measurements, not a prediction of orphan-rate reduction.

## Regression coverage

The regression tests cover prompt closely spaced and same-height changes, idle
delivery, one in-flight operation during a 20-tip stalled burst, latest-tip
coalescing, unchanged-tip retries and recovery, readiness timeouts, catch-up
suppression, cancellation, closed channels, and state shutdown during failure
or catch-up. They also cover mined success and early inventory, timeout fallback,
queued mined events, obsolete completions, mined completion racing a newer tip,
and progress while 512 mined notifications are queued.

Network coverage includes explicit same-hash retry after a full native queue,
all-route failure followed by recovery, partial route success, incoming echo
deduplication, new-peer replay, existing fanout tests, sidecar replay, and queued
broadcast lifecycle tests. Transaction gossip retains its existing tests.

All five negative-control tests ran exactly one test and failed their behavioral
assertions on the baseline. The fixed targeted run passed 101 test executions:
16 gossip lifecycle tests, one event-priority test, two TCP trials, 60 adapter
tests, three sidecar tests, five queued-broadcast tests, four transaction
gossip tests, and ten inbound fake-peer-set tests.

Clippy passed for both changed crates, all targets, with
`default-release-binaries` enabled and warnings denied. Formatting, Markdown
lint, and changelog validation passed. Fixtures answer actual announcement
requests rather than sleeping past the six-second send timeout; transaction
expiration remains driven by block height.

CI also detected published API breaks inherited from PR #1116. This PR carries
the missing network major bump to 9.0.0. RPC moves to 12.0.0 because its public
bounds expose network traits, and the node moves to 1.5.1 so its updated
dependency requirements can publish. The version correction preserves the
compatibility check and relay behavior.

## Reproduce

From the PR checkout, run the fixed tests:

```sh
cargo test -p zakura --lib --locked components::sync::tests::gossip
RUST_LOG=info cargo test -p zakura --lib --locked committed_tip_relay_over_legacy_tcp_is_prompt -- --nocapture
cargo test -p zakura-network --lib --locked zakura::legacy_gossip::tests
cargo test -p zakura-network --lib --locked sidecar
cargo test -p zakura-network --lib --locked broadcast_all_queued
```

For the negative control, create a separate checkout from the PR revision. Keep
its tests, but restore both production implementations to the pinned baseline:

```sh
git worktree add --detach ../zakura-relay-baseline HEAD
cd ../zakura-relay-baseline
python3 - <<'PY'
from pathlib import Path
import subprocess

baseline = "a3c599392af5dbffbe9acb784289c7c12ce04dda"
marker = "#[cfg(test)]\nmod tests {"
for name in (
    "crates/zakurad/src/components/sync/gossip.rs",
    "crates/zakura-network/src/zakura/legacy_gossip.rs",
):
    path = Path(name)
    candidate = path.read_text()
    original = subprocess.check_output(
        ["git", "show", f"{baseline}:{name}"], text=True
    )
    production, _ = original.split(marker, 1)
    _, tests = candidate.split(marker, 1)
    path.write_text(production + marker + tests)
PY
```

Each following command must run exactly one test and fail its behavioral
assertion, rather than fail compilation or run zero tests:

```sh
RUST_LOG=info cargo test -p zakura --lib --locked committed_tip_relay_is_prompt -- --nocapture
cargo test -p zakura --lib --locked committed_tip_failed_send_recovers_then_stops_retrying
RUST_LOG=info cargo test -p zakura --lib --locked committed_tip_relay_over_legacy_tcp_is_prompt -- --nocapture
cargo test -p zakura-network --lib --locked explicit_block_retry_reaches_native_sender_after_queue_failure
cargo test -p zakura-network --lib --locked block_advertisement_reports_all_route_failures_and_recovers
```

Expected baseline failures are the delayed inventory, absent unchanged-tip retry,
explicit native retry suppressed by deduplication, and swallowed all-route
failure. The blocked-burst test additionally rejects a naive timer-only speedup:
a shorter timer without an explicit concurrency bound creates overlapping sends.
