# Body failure attribution and upgrade recovery

## Rejection contract

A transaction ID from ZIP 244 does not commit to its authorization bytes. A failed signature therefore cannot invalidate the header until the exact delivered body's authorization root has been checked against that header.

The semantic verifier performs this check on rejection, before returning a permanent body verdict. Successful verification keeps its existing pipeline. RPC proposals and template preparation return candidate verdicts without this extra attribution work because they cannot invalidate a stored header. The rejection path first checks the transaction Merkle root, then asks state for the actual parent's height and history tree. It does not select the commitment rules using the supplied coinbase height.

A mismatched commitment is a supplier payload failure. Missing parent context, an unavailable history tree, a state error, or a five-second timeout is retryable with no peer score. A matching commitment preserves the original consensus error. The timeout does not cancel a state read already executing on a blocking worker.

## Upgrade audit for #1012

Audited against main `c8a104d759d47d1e104c6ce9aa1fa85b7b6512c8`. This change prevents new authorization-substitution tombstones. It does not remove existing records.

The issue's statement that PR #999 already prevents new records is stale. That PR is closed without merging. Existing `ConsensusInvalidBodyTombstone` records contain a hash, height, evidence identifier, and rule identifier. They contain neither the rejected body nor proof that authorization attribution was checked. Startup reloads them, and reinserting a previously pruned header reapplies the tombstone. Operator reconsideration only removes operator reasons. A subsequent verified-body verdict cannot overwrite permanent body invalidity.

Consequently, upgrading an existing v2 database can retain an incorrectly excluded branch and its descendants after this fix. A fresh database has no historical records to repair. This audit did not inspect any production database and does not establish whether deployed nodes have affected records.

## Recovery scope

The proposed rule list in #1012 is a starting point, not a demonstrated complete migration predicate. The confirmed signature case uses `transaction.*`. Early body checks also have block-level rule identifiers, and older download paths and versions have different height checks. A stored rule alone cannot establish that the original delivered body was bound to its header.

Prefer a versioned amnesty for pre-fix, unfinalized body-invalid evidence unless a complete historical rule audit proves a narrower predicate. Preserve finalized chain data, header consensus failures, and operator invalidations. Legitimately invalid bodies can be rejected again when delivered under the corrected attribution rules.

Clearing only the tombstone column is insufficient. Recovery must update the retained header's body verdict, body-evidence authority records, inherited descendant eligibility, eligibility indexes, selection, and published generations consistently. Records whose header rows were pruned also need removal. Use the state writer and a crash-safe migration marker. Test interrupted migration, repeated startup, pruned-header reinsertion, valid descendants becoming eligible, and genuine invalidity being rediscovered. The existing body-conclusion crash fixtures show the persistence boundaries that this migration must respect.

## Remaining prevention dependency

This authorization fix does not implement the duplicate-padding work in #1039. Pre-V5 Merkle padding can preserve a header while adding duplicate transactions. Bare duplicate errors still map to permanent invalidity in both semantic and checkpoint verification on the audited main. Prevent that class of new false records before clearing its historical tombstones, or a peer can recreate them after the amnesty. #1039 is currently stacked on #1038 and #1036 and needs reconciliation with this rejection-path design.

Existing-node rollout therefore needs either the recovery migration plus the relevant prevention fixes, or evidence that the particular database has no affected tombstones. Restarting or operator reconsideration is not a substitute. Database deletion is not the proposed recovery procedure.

## Validation boundaries

The regression tests exercise real transparent ECDSA rejection, a header that commits to an invalid signature, honest-body commit after substitution, forged coinbase heights, and missing/error/timeout attribution. A real QUIC regression delivers a substituted signature through the native block scheduler and consensus router, retries the honest body, and asserts that no permanent body-invalid write occurs. Separate subprocesses persist and reopen the database to check that the descendant header remains selected. A deterministic parent-height/history-tree race test checks that finalization cannot turn missing context into permanent invalidity. These local regressions do not implement a migration or measure recovery on a deployed network.
