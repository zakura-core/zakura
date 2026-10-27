# Post-quantum account regtest demo

This demo spends from a post-quantum transparent account on an isolated regtest
node. An account has one fixed P2SH address. The WOTS+ key allowed to spend
from it changes on every spend: a zero-value control output names the current
step's four keys, each spend is signed by one of them, and each spend creates
the control output for the next step. Other payments to the address are spent
alongside without signatures of their own.

From the Zakura checkout, run:

```sh
python3 tools/wots-regtest/run.py
```

The runner verifies the pinned Git dependencies for the node and harness,
builds both, starts an isolated ephemeral regtest node, and stops that node on
exit. It does not use a wallet database or an existing node. Python 3.11+ and
the repository's Rust toolchain are required. Git dependencies must be reachable
or already present in Cargo's cache.

The scenario, with every key derived from a fixed test seed:

1. A first spend is rejected before the account rules activate. Ordinary P2PKH
   spends are mined before activation and after the scenario.
2. After activation, the first spend is accepted: one deposit carries the step-0
   signature, a second deposit is covered by it, and the spend creates step 1.
3. A late deposit arrives, and the step-1 key spends it while both it and the
   first spend are still unmined. All three are then mined in one block.
4. Several attempts at one step:
   - at step 2, a second attempt by another spare is rejected while the first
     is pending, and again after the first is mined;
   - at step 3, an attempt with a short expiry is held back until it expires,
     is then rejected, and a retry by another spare is mined;
   - at step 4, a spare index outside the tree is rejected and the last spare
     is mined.
5. Forged spends at step 5 are rejected: a control output created without the
   account's signature, successors at the wrong step, missing or doubled,
   a deposit spent without any signature, an altered signature or spare path,
   and a signature by the next step's key.

Rejections must come from the node with the expected reason; transport failures
never count.

A control input's script signature is 2,258 bytes: the 2,144-byte signature as
four 512-byte chunks and one 96-byte tail, the 32-byte public seed, the spare
index and the two 32-byte path hashes. On the first spend, the 36-byte account
script follows. A covered deposit's script signature is the account script
alone. Each signature is charged 100 sigops; this is not a measured production
cost model.

Both the `experimental-pq-transparent` build feature and an explicit regtest
activation height are required. Public-network configuration is rejected.

No spare signs two different transactions: the altered-signature and
altered-path cases alter copies of one signature over the same transaction.
Durable used-spare records, wallet retry automation, reorg handling, seed
recovery, multi-device use and UI are outside this prototype.
