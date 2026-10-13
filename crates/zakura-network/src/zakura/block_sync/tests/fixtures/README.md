# Testnet fork size regression

These public Zcash testnet blocks share height 4,351,709 and were captured on
2026-09-15 during investigation of the EU native sync stall.

- Losing branch, 1,670 bytes: `0000735e2f776cecc0218bd74ae4e9cae75c25a675c41c2f18bf4f52ff782679`.
  Read from the node's existing non-finalized block backup. The eight-byte
  backup accounting prefix was removed.
- Selected branch, 7,592 bytes: `0001273cd47339af8834b03d872abfcf31f407047463db9a526ed40ec5d40b55`.
  Read with `getblock` from the US testnet node.

The network test checks download and submission behavior. It does not replay
full consensus verification or recreate the original twenty pending commits.
