# Iroh 1.1 preparation evidence

## Source and package boundary

The integration consumes compatibility commit
`9e6cbf251d2edd6304c2d9799fc29bf39788d305` from the networking fork. Compared
with upstream Iroh 1.1.0, all 115 packaged Rust files are unchanged. Three
requirements retain published ed25519-dalek 2.2.0 and curve25519-dalek 4.1.3.
BIP32 and the selected Zcash cryptographic package versions and sources remain
unchanged in both the integration and the prepared registry-consumer graph.

The fork's `scripts/prepare-zakura-packages.py` prepares four names at
`1.1.0-rc.1`: `zakura-iroh-base`, `zakura-iroh-dns`, `zakura-iroh-relay`, and
`zakura-iroh`. Sibling dependencies use exact registry versions and package
aliases. Library names remain unchanged. DNS server and benchmark packages
are excluded from publication. The script only writes a new output directory,
records its source commit and hashes Rust files; it has no publication command.

All four local package archives build with Cargo's verification enabled.
A separate copy of the full Zakura workspace also passes `cargo check --workspace
--locked` using the prepared package aliases and temporary local source patches.
Those local patches stand in for packages that have not been published; this is
not a claim that crates.io can resolve the proposed names today. The fork's
`PUBLICATION.md` and generated `zakura-consumer.toml` define the eventual switch.
No registry names were reserved and no packages were uploaded.

The prepared archives were generated from fork commit
`e1f60bfeac7c314b943654658c11a4e81b929aab`. That commit adds tests and preparation
tooling to the consumed compatibility source; production Rust remains unchanged.

## Authentication review

The TLS verifier requires TLS 1.3 and raw public keys. The server's presented
Ed25519 key must match the endpoint identity requested by the client. Both
certificate-verifier roles delegate transcript signatures to Iroh's public-key
verification method, which calls Dalek `verify_strict`. The retained version
rejects small-order keys and signature R points and checks canonical scalars.
The resolved feature set does not enable `legacy_compatibility`.

The added authentication tests cover an RFC8032 known-answer signature, wrong
keys, changed messages, a bit mutation in every signature byte, weak-key forgery,
noncanonical scalars and invalid key/signature lengths. They pass with both the
compatibility fork and unmodified upstream Iroh 1.1. Existing key and serialization
tests also pass. These checks support the narrow dependency delta; they do not
constitute a new audit of every dependency or all upstream transport code.

## Supply-chain coverage

Explicit `audit-as-crates-io = true` policies preserve the registry baseline
requirement and add review of each Git delta. The four recorded deltas cover
only the compared source and manifest changes. They do not exempt the upstream
baseline or trust all future publications from any maintainer.

Cargo-vet still reports 27 versions missing `safe-to-deploy` coverage. Most
are unfinished reviews; the specific concerns below remain audit holds.
The exact versions and suggested review bases are recorded in
`qa/supply-chain/iroh-1.1-audit-backlog.json`. Refresh that file from
`cargo vet check --output-format json` after dependency or audit changes.

Refreshing the eight configured audit imports and recording eleven source
reviews first reduced missing coverage from 69 to 58. A further 27 records
reduce it to 31, covering supporting libraries, Iroh base/DNS/metrics, Netlink
core/protocol changes, WebSocket and browser-stream changes, and Windows import
libraries. Delta records reuse reviewed baselines; full records cover the named
package only. Notes state review scope, caller constraints and test limitations.
Cargo-vet removed the obsolete Iroh metrics 0.35 exemption after its replacement
was reviewed. No new exemptions or publisher trust were added.

Four further records cover base64, quick-xml, simple-dns and LRU 0.18.3,
reducing missing coverage from 31 to 27. Base64's new SIMD code passed
3,356,628 comparisons with its scalar implementation on AArch64, including
malformed input, padding modes, vector boundaries and output canaries; AVX2
was inspected but not executed. XML/DNS boundary probes completed 123,136
malformed-input cases without panic. These are targeted checks, not exhaustive
parser conformance or hostile-load validation. Audit notes identify caller
limits and untested feature paths.

For the Windows import libraries, all 21 compared source/archive files match
Microsoft commit `00fe13a47c3cdce3287069be671aaa3c273f1e0a`. LLVM inspection found
import data and indirect import stubs, without startup code; Windows execution
and complete API ABI validation were not performed. Conditional-move tests
passed on native AArch64 and the portable fallback: 105 core tests and four
regressions on each. This is not a timing guarantee for every architecture.

The remaining 27 reviews include 15 without a reusable same-package baseline.
They include Iroh/noq transport, DNS parsers/resolvers, OS bindings and concurrent
collections. Passing integration tests does not complete those source reviews.

### LRU version selection

The workspace lockfile now selects published LRU 0.18.3, which retains the
earlier lifetime, mutable-iterator and panic-safety fixes and does not contain
0.18.4's new `retain` method. The latter can free a list node after a failed map
removal when a key changes its hash through interior mutability, leaving a
dangling entry. A small safe-Rust reproduction observed the inconsistent map
length and skipped the destructor; the memory-safety conclusion comes from
source inspection, not Miri or a sanitizer. Iroh relay uses immutable public-key
keys and does not call `retain`; no peer-triggered path was found.

The reviewed 0.13.0-to-0.18.3 delta has an audit record. Focused checks with
hashbrown 0.17.1 pass for key-destructor panic recovery, bounded public-key-shaped
caching, unbounded clone and double-ended mutable iteration. The consuming
Iroh relay library passes `cargo check --locked`. Cargo metadata and the inverse
dependency tree confirm that only LRU 0.18.3 is selected, and cargo-deny bans pass.
An explicit ban on 0.18.4 prevents a routine lockfile update from reintroducing
it unnoticed. A future version must be reviewed before adoption.

This resolves the LRU hold for the current workspace graph without a new fork.
A downstream workspace does not inherit this lockfile or cargo-deny policy;
the eventual registry-package validation must check its resolved LRU version.

### Review holds

The two remaining holds concern upstream cryptographic packages, not changes to
cryptographic implementations in the compatibility fork. AES-GCM and POLYVAL
are newly selected dependencies relative to the PR base; cipher remains the
existing registry version 0.4.4. Neither held package has a new audit record.

- **aes-gcm 0.10.3:** Its plaintext bound permits `2^36` bytes, exceeding the
  `2^36 - 32` byte limit in [NIST SP 800-38D, section 5.2.1.1][gcm-spec]. Its
  counter helper in cipher 0.4.4 also uses remainder instead of division when
  checking required blocks. A 32-byte reproduction starting with one counter
  block remaining returns success. Source inspection shows that excessive GCM
  input can wrap the counter; a 64 GiB encryption was not run. Noq uses this crate
  for Retry tags with empty plaintext, so that path cannot reach this length
  failure. This does not establish a vulnerability in existing Zcash consumers
  of cipher, or a need to modify them.
- **polyval 0.6.2:** Directly compiling the packaged software-32, software-64
  and ARM implementations reproduces three different tags when initializing
  with integer one and immediately finalizing. All agree for zero. The normal
  AES-GCM/GHASH constructor uses zero, so the nonzero-initialization problem was
  not found reachable from noq. Separately, the autodetect wrapper stores its
  backend in `ManuallyDrop` without a destructor, bypassing optional clearing
  when an unfinished value is dropped; ARM clearing is also unimplemented.

Upstream has [corrected the AES-GCM length bound][gcm-fix]. The registry has
no later 0.10 patch release than 0.10.3, and no later POLYVAL 0.6 patch release
than 0.6.2 as checked on September 8, 2026. Newer release series exist, but they
change cryptographic trait dependencies and are not drop-in lockfile updates.
They have not been adopted or certified here. The upstream fix references an
advisory identifier whose details were unavailable; no advisory disposition is
inferred from that reference.

These holds need a supported upstream fix/version or an explicit review of the
restricted usage before coverage can be completed. They are not evidence that
Zakura must fork BIP32 or another cryptographic package. The large transport and
platform reviews remain unfinished independently of these holds.

[gcm-spec]: https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38d.pdf
[gcm-fix]: https://github.com/RustCrypto/AEADs/commit/94366496b72126872292d8e99631560383db4471

Publication under the proposed names does not erase this review requirement.
The new registry packages will need audit records tied to their published
contents. The existing cargo-vet CI gate remains enabled and fails until the
remaining coverage is supplied; no blanket exemption has been added.

## CI test correction

The historical-tree-error RPC test returned its mocked Orchard error before
receiving the other concurrent block-info requests. On Ubuntu, early error
propagation could cancel those requests, leaving the test waiting for calls that
would never arrive. The test now receives both requests before releasing the
error. Production RPC code is unchanged. Six focused RPC tests and 100 repeated
runs of the affected test pass; all three Ubuntu unit-test shards pass at
`22f7af9b93d95d34f4219a9e09d9a835312d75c2`.

## Automated feedback

The WMI warning concerns registry package wmi 0.18.4, selected through netwatch
on Windows; the base used 0.17.3. All 24 packaged Rust files match upstream commit
`90dad22895ce91691470d15952f40cef87b4d856` after line-ending normalization. Review
of the complete delta found no obfuscation or hidden execution. Netwatch uses a
fixed local route-table query and does not call the added WMI mutation APIs.
This supports a likely false-positive assessment, but Socket's detailed detector
evidence was unavailable behind sign-in. Both Socket checks subsequently pass at `d13381d4dc4fee27c9ea33c4c1347a6e7e24a790`;
that check status does not establish a formal detector disposition. Windows
runtime tests were not run.

The nine V12 findings from run 7562 describe mechanisms already present at base
`0c854abf187d1baf001de6af85f2d7e15972e7cd`. Source review supports concerns around
native ban admission, peer-directed internal-address probes, advertised-address
construction, stale maintained dials, address-success bookkeeping and handoff
metadata binding. The Sybil/backoff finding involves an existing anti-poisoning
tradeoff: replacing identity/IP backoff with global IP backoff alone would allow
forged records to suppress honest addresses. These need separate fixes or explicit
security dispositions before broader native deployment. Their pre-existing origin
does not establish safety; exploitability and the tool's severity labels have not
been independently demonstrated. No finding or alert state was changed.

## Network validation

The regtest fixture now assigns node2 and node4 distinct native UDP ports,
18334 and 18534. The previous host-network collision prevented node4 startup.
The full Linux regtest PR gate passes with those explicit ports.

Same-cohort native process tests transfer 1 MiB, while mixed native cohorts
fail within the bounded operation deadline. Mixed dual-stack process tests
complete Ping/Pong both ways without a native handoff. The production-limit
transfer test sends 64 MiB in each direction, and the exact noq 1.2 tail-loss
regression passes on macOS and Linux.
The complete noq-proto 1.2 library suite also passes on both platforms: 421
tests each, including simulated loss, reordering, congestion and MTU changes.
Those protocol simulations do not substitute for full-node Linux impairment
or resource measurements. Cargo-deny advisories, bans, licenses and sources pass.

`scripts/test_iroh_full_nodes.py` runs independently pinned old and new local
binaries with fresh regtest state and explicit loopback ports. It mines blocks
on each peer, requires propagation to the other, restarts the client with empty
state, and requires catch-up while the seed remains idle. It also checks that
both peers retain TCP and that native connection and upgrade metrics remain zero.

The full-node comparison uses old source
`44f5afa38272932271789e0a854979887d23a0d5` and new source
`90bb8cd16111d1a8dc7794f279ed18402e22cea7`. Both mixed directions pass block
propagation and fresh-client recovery at a 66-block gap, recovering in 30.9
seconds with two retained legacy connections on each peer.

After building separate binaries from those revisions, run:

```sh
python3 scripts/test_iroh_full_nodes.py --old-bin /path/to/old-zakurad \
  --new-bin /path/to/new-zakurad --output-dir /tmp/iroh-small-gap
python3 scripts/test_iroh_full_nodes.py --old-bin /path/to/old-zakurad \
  --new-bin /path/to/new-zakurad --output-dir /tmp/iroh-large-gap \
  --blocks-per-peer 33 --catchup-timeout 180
```

Each output directory must be new. The harness records configurations, node
logs, metrics and JSON results, and terminates only its own processes.

The small-gap restart check must allow the existing ten-minute fallback window.
The faster legacy probe requires a gap of at least 64 blocks; a six-block gap
does not meet it. A two-minute catch-up deadline failed despite working TCP and
block propagation. Do not infer fast restart recovery from the Ping/Pong check.
With the full fallback window, both six-block runs pass: the new client recovers
in 603.58 seconds and the old client in 603.26 seconds. Both peers reach the same
tip, retain two legacy connections each and report no native handoff. This
existing recovery delay remains an operational limitation for a mixed cohort.

## Linux native impairment comparison

The local comparison runs Ubuntu 24.04 amd64 in a disposable Docker container
with network administration confined to that container. Base source
`0c854abf187d1baf001de6af85f2d7e15972e7cd` comes from CI run 34290566584;
upgrade source `5f94410f97507652f58e194ed17112ef6727cbb5` comes from CI run
34296029999. The later RPC test correction changes no production code.

The binary SHA-256 values are:

- Base: `2cfb748d8db40618b9617022be082103f8eaabd41d0502cc07992418fe801e71`.
- Upgrade: `2b481d04747ab468eec0880817d9496dbce02ba5e9b640ea52f49530c1b7cc72`.

Each same-version pair mines 33 blocks on each peer, requires propagation both
ways, and restarts the client with empty ephemeral state. The seed has no TCP
peers and the client enables only native transport. Both retain a native
connection and report zero TCP peers at completion. Propagation and fresh-state
catch-up each have a 180-second deadline. The outage profile drops all native
UDP for five seconds, mines one extra block, verifies that it cannot propagate
while blocked, then checks recovery after removing the impairment.

The profiles use 50 ms delay with 10 ms variation and 1% loss; 20 ms delay with
5 ms variation and 25% reordering at 50% correlation; loopback MTU 1280; and the
five-second outage. Packet counters confirm that netem affected native traffic.

All ten cases pass. Restart catch-up times, in seconds:

| Profile | Base | Upgrade |
| --- | ---: | ---: |
| baseline | 13.21 | 4.24 |
| delay-loss | 7.60 | 7.83 |
| reorder | 5.15 | 16.49 |
| mtu1280 | 1.60 | 4.27 |
| outage | 13.27 | 14.85 |

Outage recovery after restoring traffic takes 53.74 seconds on the base and
53.17 seconds on the upgrade. Both finish at height 67; other cases finish at
height 66. Across all cases, maximum sampled per-process RSS is 421 MiB and
summed process CPU time stays below 22 seconds per case. All pass the declared
sanity limits of 2 GiB per process and 660 CPU seconds per case.

The host emulates amd64, and the base binary uses the CI test profile while the
upgrade uses the debug profile. CPU and RSS are therefore local sanity checks,
not a controlled performance comparison or evidence of an Iroh memory regression.
These small regtest blocks and short runs do not establish production throughput,
long-term memory stability, or behavior under sustained hostile load.
Long-duration authorized deployment soak remains a separate release gate;
no deployment has been performed.
