# Changelog: Parameters

A focused ledger of deliberate changes to **tunable parameters** in this fork —
constants, config defaults, timeouts, window/limit sizes, and congestion-control
coefficients.

This complements `CHANGELOG.md`. The changelog records user-visible behavior in
prose; this file is a compact table of every parameter value we have re-tuned, so
reviewers and operators can see — at a glance — what changed, where it lives, and
why.

## How to use this file

When a PR changes a tunable parameter, add a row to the table below **in the same
PR**. A "tunable parameter" is any value chosen for behavior or performance rather
than correctness — a constant, a `Config` default, a timeout, a window or limit,
or a backoff/growth coefficient.

Keep entries **newest-first**. Each row records:

- **Parameter** — the constant or config field name.
- **Location** — the file where it is defined (crate-relative path).
- **Old → New** — the previous value and the new value.
- **PR** — a link to the pull request that made the change.
- **Why** — a one-line rationale.

## Parameters

| Parameter | Location | Old → New | PR | Why |
| --- | --- | --- | --- | --- |
| `ZAKURA_BLOCK_SYNC_CHECKPOINT_FRONTIER_REFRESH_INTERVAL` | `zebrad/src/commands/start/zakura/block_sync_driver.rs` | `5s` → `200ms` | [#374](https://github.com/valargroup/zebra/pull/374) | Recycle the checkpoint apply window promptly during checkpoint sync so the finalized writer is not left idle for ~5s between frontier refreshes. |
| `DEFAULT_ZAKURA_BOOTSTRAP_PEERS` | `zebra-network/src/zakura/handler.rs` | empty default → 9 native bootstrap peers | [#376](https://github.com/valargroup/zebra/pull/376) | Let Zakura nodes discover the native P2P network without requiring every operator to configure bootstrap peers manually. |
| `DEFAULT_ZAKURA_MAX_CONNECTIONS` | `zebra-network/src/zakura/handler.rs` | `32` → `256` | [#376](https://github.com/valargroup/zebra/pull/376) | Raise the native P2P connection envelope for production sync and peer diversity. |
| `DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES` | `zebra-network/src/zakura/handler.rs` | `8` → `32` | [#376](https://github.com/valargroup/zebra/pull/376) | Allow more simultaneous native control handshakes during bootstrap and peer churn. |
| `DEFAULT_ZAKURA_STREAM_OPEN_RATE_PER_SECOND` | `zebra-network/src/zakura/handler.rs` | `16` → `32` | [#376](https://github.com/valargroup/zebra/pull/376) | Permit higher stream-open churn across the larger default peer set. |
| `DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW` | `zebra-network/src/zakura/handler.rs` | `3 MiB` → `32 MiB` | [#376](https://github.com/valargroup/zebra/pull/376) | Avoid throttling high-throughput native streams with the earlier conservative per-stream receive window. |
| `DEFAULT_ZAKURA_RECEIVE_WINDOW` | `zebra-network/src/zakura/handler.rs` | `16 MiB` → `32 MiB` | [#376](https://github.com/valargroup/zebra/pull/376) | Match the connection receive window to the larger stream window used for production sync. |
| `DEFAULT_ZAKURA_SEND_WINDOW` | `zebra-network/src/zakura/handler.rs` | `16 MiB` → `32 MiB` | [#376](https://github.com/valargroup/zebra/pull/376) | Keep the native QUIC send window from becoming the bottleneck for larger receive windows. |
| `OUTBOUND_WINDOW_FLOOR_TIMEOUTS_BEFORE_DISCONNECT` | `zebra-network/src/zakura/block_sync/state.rs` | `3` → `2 * OUTBOUND_WINDOW_REDUCTION_EPOCH_TIMEOUTS` (`32`) | [#303](https://github.com/valargroup/zebra/pull/303) | Tolerate two full reduction epochs (~256s at the 8s request timeout) of floor-pinned timeouts before disconnecting a block-sync peer, instead of ~24s, so briefly-congested peers are not churned. Any successful response resets the streak. |
