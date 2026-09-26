### Added

- Link sampled CPU stacks to explicitly recorded block execution contexts, with stage filtering and raw/unassigned views. Add configurable deeper stack capture on the internal profiling service. Historical profiles retain their original evidence.

Show shared proof worker execution once with its participating transactions. Remove per-stage CPU links and retain the block-level flame graph.

Support a 2,000 Hz CPU sampling trial while preserving recorded CPU-period weights.
