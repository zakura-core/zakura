# Compatibility and adoption review

Reviewed Zakura main `2b2b9bec56240f407b665b865a60121e847d27f9` and
[bbtree source at the pinned commit](https://github.com/XiangpengHao/bbtree/tree/8adae97a0ed97e5c98fa2790312eb0aec9719846).

## Upstream

- License: root `LICENSE` contains both MIT and Apache-2.0 texts; the package
  declares `MIT OR Apache-2.0`. Compatible in principle with Zakura's license;
  retaining notices would be required for a future vendor copy.
- Maturity: package 0.1.5, first repository commit dated 2026-09-19, continuing
  changes through 2026-10-24. Its testing investment does not establish a long
  production history. No external fork or package publication is needed here.
- Dependencies: the manifest has an exact `vstd` dependency, whose procedural
  macro dependencies compile even when proof code is erased. The experiment
  lockfile records 16 packages including bbtree. The README's zero-dependency
  statement is not an accurate description of the build graph at this revision.
- MSRV: no `rust-version` is declared upstream; edition 2024 establishes only a
  language-edition floor, not a tested MSRV. Upstream CI uses nightly-2026-10-07.
  This experiment builds successfully on Zakura's stable 1.97.1.
  Do not infer broader compiler
  support from one successful build.
- Features: `file` and `inspect`, neither enabled here. The library is `no_std`
  with `alloc`. It contains x86_64 SIMD, AArch64, and fallback paths. This run
  exercises only x86_64 with the baseline target, not native AVX-512, ARM,
  32-bit, Windows, or no-std integration. Atomics and allocator assumptions
  require a target-specific audit before promising arbitrary platform support.
- Safety: the unsync facade forbids unsafe code, but the shared tree underneath
  uses unsafe allocation, pointer movement, vector loads, and Send/Sync impls.
  Miri, sanitizers, fuzzing, Shuttle and Verus workflows exist upstream; they
  were not rerun here. Verus proves selected inline-key and byte-comparison
  routines, **not the whole tree**, and has explicit trusted specifications.
  See `design/08-fuzzing-verification.md` and `bbtree/src/tree/`.

## API and semantics

The unsync map supports arbitrary `Ord` keys and owned values; ordinary get and
insert do not impose `Copy` on values. Cloning requires cloneable keys/values.
`Auto` recognizes select built-in types for inline keys; Zakura's custom newtypes
and the height index's u32 chunk IDs use generic comparison. A custom `InlineKey`
implementation is additional correctness-sensitive work, not a free conversion.

The source implements borrowed lookup, replacement returning the previous value,
entry access, sorted/double-ended iteration and ranges, split_off, retain, and
first/last operations. Sets support ordered uniqueness and set algebra. The
experiment checks a subset against std, including duplicate replacement, bounded
lookups, ordered traversal, removal, snapshots and split_off-driven finalization.
It does not establish every public API's panic/drop behavior or serialization.
There is no serde feature or serde implementation in this revision's source.

`bbtree::BBTreeMap` (the concurrent type) uses a different interface, including
copied/cloned reads and range_cloned. It is excluded. Existing ownership,
copy-on-write isolation, synchronization and deterministic traversal remain intact.

## Zakura inventory and selection

`results/inventory.txt` records all 595 textual references across 120 Rust files
at the base SHA (imports, comments and test uses included, not 595 live indexes).
Reproduce with `rg -n 'BTree(Map|Set)' crates --glob '*.rs'`.

<!-- markdownlint-disable MD013 -->

| Area | References | Decision and migration effort |
| --- | ---: | --- |
| state | 196 | HeightMap selected: private u32 to Arc chunk map, existing benchmark, small import change. Other maps expose response types, UTXO ordering, subtree results, chain sets and persistence boundaries; require separate API/serialization and reorg tests. |
| network | 98 | Reorder, work queues, peer registry and range ownership are plausible next candidates; active state-machine PRs make overlapping edits undesirable. Preserve timeout, supplier, generation and deterministic schedule semantics. |
| chain | 55 | Checkpoints and network parameter maps include public collection types and serialization boundaries; not an isolated alias change. Requires API and serialized-format compatibility checks. |
| scheduler | 49 | Ordered task/timer sets are part of deterministic replay. Excluded until full replay equivalence is tested; no measured node benefit from changing simulator bookkeeping. |
| header-chain | 35 | Checkpoint maps and eligibility/supplier sets feed public contracts and digests. Excluded pending canonical-order and persistence tests. |
| zakurad | 29 | Config, metrics and mempool structures need workload-specific evidence. Config/string maps are not established hot paths. Existing mempool benchmark is available in ordinary CI. |
| xtask | 29 | Tooling/verification collections, no established runtime hotspot. |
| consensus | 26 | Checkpoint queue is plausible, but owns queued blocks and asynchronous completion state; requires queue/error tests and real verification workload. |
| sim | 24 | Deterministic simulation and test support; excluded for the same replay reason. |
| quic | 20 | Simulation scheduling and transport test collections require replay equivalence; no production hotspot established. |
| rpc | 17 | Mostly surface validation/test sets; public result types need API checks. |
| test | 13 | Fixtures, no runtime adoption rationale. |
| rt | 4 | Test/support deterministic scheduling, not a node hotspot. |

<!-- markdownlint-enable MD013 -->

Repository AGENTS.md and both relevant local skill files were read. CODEOWNERS
contains only commented rules. No matching btree PR or bbtree commit was found;
branch search found no bbtree experiment. The height-index implementation was last
changed by #1576 (Dev Ojha), following #1443 and #1399 (Roman Akhtariev).
Open work includes extensive network state-machine changes and state/spentness
work. This experiment changes none of their production modules.

## Limits and decision gate

A speedup here can justify a focused feature-gated integration PR, not wholesale
replacement. Before production use: real state-crate tests, drop/panic and range
edge cases, supported platforms, dependency/license tooling, and a measured
whole-node workload showing that these indexes matter. A fork would additionally
need a demonstrated upstream limitation and a maintenance owner. No such need
has been established. Results and recommendation are recorded separately.
