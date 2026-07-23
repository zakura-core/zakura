# Header-chain v1.4 migration

Header-chain v1.4 is a protocol and storage cutover, not a backward-compatible
upgrade of the predecessor header overlay.

- A database containing predecessor header-overlay rows is rejected before the
  new header DAG is initialized or published. Zakura does not decode, import,
  reinterpret, or delete those rows. Start with a fresh state database and
  resynchronize.
- A clean database initializes the header DAG from authenticated finalized and
  reconstructed full-state facts. Headers above the verified block tip are
  downloaded again.
- `network.zakura.header_sync.accept_new_blocks` no longer exists because header
  sync does not relay blocks. Configuration parsing rejects the stale field;
  remove it before starting the upgraded node.
- `getblockchaininfo.header_chain` reports the authoritative engine mode,
  `header_best` (header-valid only), `verified_best`, finalized frontier, and
  persistent alarms. In headers-only mode it also displays the irreversible
  1,000-deep local-finality warning.

The supported headers-only-to-integrated engine-mode migration remains
non-rollback and preserves migrated pins. It is separate from predecessor
overlay compatibility.
