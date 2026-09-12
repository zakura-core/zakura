# Native transport capacity

## Current scope

The compliance stack uses published `zakura-iroh` 1.1.0-rc.1 and `noq`/`noq-proto`
1.2.0. Transport dependency modifications are being reviewed separately in the
maintained transport forks. The node's application work and requester metadata
budgets remain enabled. Complete transport memory and cleanup bounds are deferred.

The published APIs support the enabled receive policy: at most 16 remotely
initiated bidirectional streams, 256 KiB per stream and 9.5 MiB per connection.
Unidirectional application streams are disabled. Locally initiated stream state
does not yet have the proposed transport lifetime limit.

For the fixed scenario with 32 paused sibling receive windows, 8 MiB remains
unread and 1.5 MiB remains available for independent progress. This exceeds the
transport's connection-credit update threshold of 9.5 MiB / 8. T02 and the
32-sibling regression remain enabled because they use published APIs. Passing
these scenarios does not establish a bound over arbitrary local stream churn.

## Dependency-blocked witnesses

The full bodies of these five tests remain in
`handler/tests/transport_ownership.rs`. Each has an explicit `ignore` reason.
The module also uses `cfg(any())` because Rust type-checks ignored tests and the
published packages lack the constructors and configuration methods they call.
They are excluded from passing test totals and cannot run with `--ignored` alone.

| Test | Required change before enabling |
| --- | --- |
| `closed_transport_keeps_its_owner_while_an_unread_receive_half_exists` | Publish owned incoming construction. The owner must release after retained connection and stream state is destroyed. |
| `transport_owner_releases_on_preconstruction_error_and_cancelled_handshake` | Publish outgoing owner transfer that handles failed construction and cancelled handshakes. |
| `native_router_reserves_transport_before_handshake` | Publish the Router admission hook and restore native admission before connection construction. |
| `closing_inbound_transport_blocks_native_dial_until_last_handle_retires` | Restore one inbound/outbound transport pool whose reservations survive application closure and final transport cleanup. |
| `stopped_local_stream_reopens_only_after_transport_final_offset` | Publish local stream limits that retain stopped receive records until FIN or RESET supplies their final offset. |

The complete native integration is preserved in commit `26ad05407`. Once the
dependency APIs are reviewed and published, restore the corresponding integration,
remove the module exclusion and individual ignores, and run all five tests.
Do not replace their ownership assertions with application-handler completion.

## Remaining transport bounds

| Resource | Deferred work |
| --- | --- |
| Connection lifetime | Reserve before inbound/outbound construction and retain ownership through failed handshakes and final cleanup. |
| Receive queues | Fund the raw incoming queue and bound datagrams waiting for a connection driver before protocol flow control runs. |
| Send storage | Count retained bytes after partial acknowledgments and reset. Bound backing allocations held by small slices. |
| Receive storage | Account for fragment records, backing allocations, compaction peaks and retained collection capacity. |
| Transport metadata | Bound acknowledgment/retransmission ranges, packet history including sparse indices, and closing/unused multipath state. |
| Local streams | Count locally opened and retiring streams until their protocol state is freed. |
| Endpoint state | Bound identity mappings and idle actors across sequential new identities as well as simultaneous connections. |
| Node total | Fund all live allocation owners from one node-wide transport budget, including pending control state, receive batching and allocator overhead. |

The extracted patch addresses parts of this inventory. It does not complete the
endpoint or node-wide bound. Its numeric limits and buffer-copying policy remain
subject to review and performance qualification.

## Qualification

Record fresh results on the published-package stack. Earlier passing totals from
the dependency-patched integration do not qualify this revision. Runtime skips
must be reported separately from the five compile-excluded witnesses above.

Full transport qualification requires the completed allocation inventory, denial,
handshake failure, retirement and reuse at capacity, plus sustained load and
optimized throughput comparisons. The existing five-sample median threshold is
90 percent of the baseline on both lossless and impaired links. These gates remain
open while the dependency work is deferred.
