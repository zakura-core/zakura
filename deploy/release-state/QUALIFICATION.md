# Complete release-state qualification

## Local evidence

Executed on Roman's Mac on 2026-10-15 with Python 3.12.13 and Cargo 1.98.1.
The local object store and registry listen only on loopback and use disposable
storage and credentials. No live crate upload or production storage write was
performed.

- Release-state test discovery: 44 tests passed, including 17 integration
  scenarios using the real Cargo publisher, resolver, and lockfile updater,
  and the pinned S3 client over HTTP.
- Existing fetch, import, packaging, and release-readiness self-tests passed:
  23, 16, 14, and 13 tests respectively.
- Workflow lint and Markdown lint passed.
- A read-only probe verified the existing production asset's archive and
  Cargo-resolved grid against committed provenance.

The integration path exercises the promotion and ready-reader command
entrypoints, the real importer, and the pinning command. Fault cases cover
failed crate uploads, delayed visibility, access denials, conflicting bytes,
partial object uploads, crashes after each immutable write and after pointer
commit, conditional-write races, concurrent publishers, pointer regression,
candidate cleanup isolation, cached missing objects, tampered public files,
unavailable or yanked crates, a pointer changing during consumption, and
freshness boundaries. Rejected inputs leave no consumable local snapshot;
failures before pointer commit preserve the previous pointer. A lost reply
after pointer commit can leave the new complete set selected, and retries
verify and reuse it.

The synthetic fixtures do not prove cryptographic subtree correctness. Separate
validation tests assert that append-only and subtree-verifier failures stop the
publisher, and the publishing workflow runs the node's real verifier before
issuing credentials. Actual node verification against a fresh export is still
part of qualification below. These local results do not qualify R2, CDN cache
rules, crates.io authentication, or GitHub environment restrictions.

## Live qualification: pending explicit authorization

Schedules are gated by repository variables and remain disabled during rollout.
Follow the setup order in [README.md](README.md#production-qualification-and-activation).
Record links and outcomes here after each authorized step:

- [ ] Merge the reviewed implementation.
- [ ] Issue the dedicated bucket credential in the existing Infisical project
      and environment, and configure the main-only storage environment.
- [ ] Audit lifecycle retention and bypass caching for the ready pointer;
      ensure missing published objects are not cached.
- [ ] Manually publish a fresh real candidate from `main`, including the node's
      subtree/frontier proof and real crates.io authentication when needed.
- [ ] Verify the expected crate, Cargo resolution, all public objects, and
      readiness advancement; rerun and verify identical reuse.
- [ ] Run adoption and preparation dry runs against that same complete set.
- [ ] Enable daily publication at 07:00 UTC and weekly adoption on Monday at
      06:00 UTC.
