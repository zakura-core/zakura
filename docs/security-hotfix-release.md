# Security Hotfix Release Process

This document describes how we ship a release for a **significant security
issue**: the fix is developed under embargo, and the public goes from "no
information" to "signed, tagged GitHub release + crates.io release" in one
coordinated event. It complements [`SECURITY.md`](../SECURITY.md) (how
vulnerabilities are reported and disclosed), the
[release checklist](../.github/PULL_REQUEST_TEMPLATE/release-checklist.md)
(the standard runbook), and the
[hotfix release checklist](../.github/PULL_REQUEST_TEMPLATE/hotfix-release-checklist.md)
(the hotfix runbook this process feeds into). Where this document is silent,
the standard release process applies. Ordinary security fixes — low severity,
already public, or fine to ship in a scheduled release — don't need this
process.

**Design principle: one maintainer can execute this end-to-end.** Nothing in
the process requires a second person. A second person makes it better (see
[Scaling up](#scaling-up-when-a-second-person-is-available)), and for the
worst bug classes you should try hard to get one, but the process must not
deadlock because nobody else is awake.

## Goals and non-goals

Goals:

1. Develop and validate the fix entirely in private — in a private staging
   repo with CI, plus a soak on nodes we operate — before anything is public.
2. At a chosen time (**T-0**), go from private to a public tag, GitHub
   release, and crates.io release with a minimal, well-understood exposure
   window.
3. Keep the existing release protections: `v*` tags are created only by the
   release App via the `Create release` workflow (with a break-glass org-owner
   bypass), tags stay immutable, and binaries are always built and
   provenance-attested by the public repository's CI
   (see [release tag protection](release-tag-protection.md)).
4. Every step is rehearsable in advance and resumable if interrupted.

Non-goals:

1. **Hiding the fix after T-0.** Once public, the diff is the disclosure for a
   capable attacker; the only real mitigation is how fast operators upgrade.
2. **Hiding that a release is happening.** Workflow runs and release-state
   refreshes are public. We protect the content, not the schedule.
3. Automating signing or crates publication (both stay manual).
4. Zero-gap binaries (they're built by public CI after the code is public).

## Overview

```text
        PRIVATE                                  │  PUBLIC (T-0 onward)
                                                 │
reporter ─> GHSA draft advisory ─────────────────┼─> advisory published
                 │                               │        ▲
                 ▼                               │        │
zakura-core/zakura-private (staging repo w/ CI)  │        │
  fix PR in staging: review + full test suites   │        │
  tag dress rehearsal -> private release+assets  │        │
  canary soak on nodes we operate                │        │
                 │                               │        │
                 └──────────── T-0 ──────────────┼─> push -> Create release
                                                 │   tag + release (~T+10–30)
                                                 │   crates, sign, promote,
                                                 │   announce
```

## The private staging repo

All embargoed work happens in a **dedicated local clone** whose only remote is
private. Do not work in your everyday checkout: pushing an embargoed branch to
the public remote by muscle memory is the most likely catastrophic mistake in
this design, and solo there's nobody to catch it. Name branches
`security/<codename>` (codename carries no content); install
[`scripts/hooks/pre-push`](../scripts/hooks/pre-push) in the everyday clone —
it rejects `security/*` pushes — as cheap insurance.

**`zakura-core/zakura-private`** is where validation and review happen: a
private repo, created and curated once, maintainers-only. Validation runs in
this repo's GitHub Actions rather than on laptops, so it doesn't depend on any
one maintainer's hardware. It is deliberately **not a synced mirror**: there
is no sync automation to maintain — at incident start you push current public
`main` (and tags) into it from your clone, and its freshness is whatever you
pushed. It doubles as the backup and handoff point: another maintainer can
take over from what's pushed there plus the GHSA draft.

One-time curation makes a pushed copy of this repository quiet and safe by
construction:

- **Scheduled and infrastructure workflows are disabled** (continuous sync,
  e2e, deploys, benchmarks, release-state refresh, PR-node automation, …);
  lint, unit/crate tests, Docker tests, and `release-binaries.yml` stay
  enabled. Disabled state is keyed by workflow path and survives later pushes;
  glance at the Actions tab at incident start in case public `main` gained
  new scheduled workflows since.
- **Repo guards in the public workflows**: steps that touch the outside world
  — Docker Hub login and pushes, build attestation — are skipped, and
  push-variant Docker builds degrade to build-only, whenever
  `github.repository != 'zakura-core/zakura'`. Missing secrets already make
  publishing from the staging repo impossible; the guards are what make its
  rehearsals green-by-design instead of red noise.
- **No publishing credentials, ever**: no Docker Hub token, no crates token,
  no release App key. The only optional secret is the deploy SSH key, if we
  adopt workflow-driven soak deploys (see
  [Scaling up](#scaling-up-when-a-second-person-is-available)).

Two things a private venue can never be: a GitHub fork (forks of public repos
cannot be private — at T-0 you push commits to the public repo directly, there
is no cross-repo PR), and a GHSA temporary fork used for development (they run
no Actions and we don't need them).

### Rehearsing the release privately

Everything the public T-0 run gates on can be rehearsed in the staging repo:

| Public T-0 gate | Staging rehearsal |
| --- | --- |
| `Create release` validate job | `make pre-release RELEASE_TAG=v<version> BASE_TAG=v<previous>` (a CI job or locally — it's a check script, not a build) |
| Release asset build + version self-check | The tag dress rehearsal (below) |
| Docker image builds | The tag dress rehearsal (below) |
| Crates publishability | `./scripts/check-crate-packaging.sh --verify`, plus the no-git-dependencies check |

**The tag dress rehearsal.** Once the branch is final, push the real
`vX.Y.Z` tag _to the staging repo_. Nothing collides — it is a separate
repository with no tag rulesets — and the push triggers
`release-binaries.yml` exactly as the public T-0 tag will: version checks,
both release binaries, Docker images (build-only, under the repo guards), and
a complete **private release** with the assembled asset set, which you
download, inspect, and deploy for the soak. This is the closest possible
approximation of T-0 with zero public footprint.

Correctness testing is separate from release mechanics: no test suite runs at
public T-0 (the `main` ruleset requires no status checks), so run the fix as
a PR in the staging repo — lint, unit/crate, and Docker test workflows run
there, and the PR is where review (cold self-review, the reporter, or another
maintainer) happens — plus whichever nextest profiles the change warrants.
Maintainers with capable hardware can substitute local runs for any of this;
the staging repo is the baseline that depends on nobody's laptop.

Staging CI is near-parity, not parity: repository `vars` don't replicate
(builds fall back to slower GitHub-hosted runners, billed as private-repo
Actions minutes), caches differ, and attestation is skipped — see caveat 6.

## GHSA advisory (parallel track)

For each incident, open a **draft security advisory** on the public repo
(visible only to maintainers and invitees). It is the reporter channel (per
`SECURITY.md`, reports already arrive there), the CVE request vehicle, the
canonical description published at T-0 — and, solo, the **continuity record**:
keep it current enough (state, branch name in the staging repo, planned T-0)
that another maintainer could pick up the incident if you become unavailable.
Detail level at publication is per-incident; `SECURITY.md` already reserves
the right to withhold reproduction details for counterfeiting-class bugs.

## Developing the fix

### Choosing the base

| Base | When | Notes |
| --- | --- | --- |
| `hotfix/vX.Y.Z` cut from the last release tag | Default for solo, and whenever `main` has unreleased work operators shouldn't absorb mid-emergency | Minimal auditable diff; **mechanically simplest solo**: direct maintainer push, no PR review rule, no Mergify interaction. The branch must be named for the exact tag it releases — the `Create release` validate job enforces this. Fix must be forward-merged to `main` right after release. |
| `main` | `main` is essentially the last release plus safe changes | Direct pushes to `main` are blocked for everyone: at T-0, open a public PR **from `hotfix/vX.Y.Z`** and **squash-merge** it — the only method the `main` ruleset allows, and safe in this order because `Create release` tags the squash commit _after_ the merge. Freeze the Mergify `batched` queue first. |

Record the choice and reasoning in the advisory.

**Branch namespace rule.** Whatever the base, the hotfix process creates
exactly two kinds of branches: `security/<codename>` in the private clone and
staging repo, and `hotfix/vX.Y.Z` on the public repo — in main-base mode the
T-0 PR branch is also named `hotfix/vX.Y.Z`. Never push to `release/v*` or
`bump-v*`: those names belong to the regular release process, and under
embargo neither side can see the other coming. Disjoint namespaces make a
hotfix-vs-regular branch collision structurally impossible — at v1.0.3's T-0
the hotfix and a concurrent regular release both claimed `release/v1.0.3`,
and only an accidental non-fast-forward rejection surfaced it. Same-version
collisions at the tag level are handled by the hold-releases heads-up
([the day before](#the-day-before)) and the T-0 collision checks
(see [Caveats](#caveats-and-risks), item 12).

### What the branch must contain before T-0

T-0 involves zero authoring — only pushing and clicking. The branch carries:

1. **The fix**, minimal and with tests. Solo review protocol: write it one
   day, review the diff cold the next; where an external reporter exists,
   they are your reviewer. (See caveat 1 — this is the process's sharpest
   compromise.)
2. **The complete release prep**, per the
   [hotfix release checklist](../.github/PULL_REQUEST_TEMPLATE/hotfix-release-checklist.md):
   - `zakura` package version bump (hotfixes are `patch`) and changed-crate
     bumps (`cargo release version ...`). When de-rc'ing an rc line to its
     stable version, two things need explicit care: re-run
     `cargo semver-checks` against the published **stable** baselines —
     post-rc changes can raise the required bump level (v1.0.3's planned
     patch de-rcs became three major bumps this way) — and normalize
     internal dependency requirements, because `dependent-version = "fix"`
     leaves stale `^X.Y.Z-rcN` requirements in place when the stable version
     still matches them;
   - the stored config snapshot
     (`zakurad/tests/common/configs/v<version>.toml` — `last_config_is_stored`
     fails without it);
   - the assembled changelog section. There is no public PR to hang a
     fragment on, so write the release section by hand, exactly as fragment
     assembly would have produced it (for a stable tag that includes
     absorbing any `v<version>-rc*` sections into the release section), and
     verify with `./scripts/changelog.py release v<version> --check` — it is
     part of `make pre-release` and fails on anything assembly wouldn't have
     written;
   - `ESTIMATED_RELEASE_HEIGHT` in `end_of_support.rs` deliberately **not**
     bumped. A hotfix inherits the base release's end-of-support halt height:
     it protects operators now without extending the schedule on which they
     must take the next regular release, and it removes a fiddly
     estimate-the-height step from the emergency path. If the base release is
     already old, the hotfix ships with a short remaining runway — check it,
     state it in the release notes and announcement, and plan the next
     scheduled release accordingly;
   - `make pre-release` passing, packaging checked with `--verify` — run
     both in a **dedicated worktree** (`git worktree add ../zakura-verify
     <commit>`): at v1.0.3, a branch checkout under a still-running
     background test chain invalidated a completed verification pass and
     forced a full re-run.
3. **Commit messages written for public consumption** — world-readable at T-0.
   Recommended: accurate but minimal conventional commits
   ("fix(consensus): harden <area> validation"), detail deferred to the
   advisory.

Release state (checkpoints + VCT frontier): staleness beyond 14 days only
warns, so a hotfix normally ships whatever the base already has. If it's
badly stale, refreshing it runs the _public_ `update-release-state.yml`
workflow and merges a public draft PR — a mild, deniable timing signal. If
that PR floors `ESTIMATED_RELEASE_HEIGHT` upward, drop that hunk: hotfixes
never move end-of-support. The `allow_bootstrap_release_state` override exists
for a broken publisher; note it in the release PR if used.

### Canary soak

Deploy the binaries from the tag dress rehearsal (download the private
release's assets) **manually** to nodes we operate — no workflow needed.
Testnet first; for consensus-relevant fixes, at least one mainnet canary.
Soak until confident: in sync with the public network, no divergence, no
restarts, clean metrics. Suggested floor: 24h for consensus-touching changes;
compressing it is an explicit, recorded decision, not a default. Canary
binaries are unsigned and private-built — fine for our own infrastructure,
never for anything public.

## Going public: the solo T-0 runbook

### Standing preconditions (verify now, not during an incident)

- [ ] The `release` environment lists each release-capable maintainer as a
      required reviewer and **"Prevent self-review" is unchecked** — otherwise
      a solo release deadlocks at the approval gate.
- [ ] The `release` environment's deployment branch policy includes
      `hotfix/v*`, and the `hotfix/v*` branch ruleset (block deletion and
      force-push) is active.
- [ ] `gh` authenticated; crates.io login current; minisign secret key
      accessible; you can reach the announcement channels.
- [ ] The hotfix-branch forward-merge path is executable as written. The
      `main` ruleset allows only squash and rebase, but the post-release
      forward-merge needs a **merge commit**
      (see [Post-release](#post-release)): either keep `merge` permanently
      in the ruleset's allowed merge methods (recommended — the merge button
      still defaults to squash) or verify the temporary-edit procedure in a
      drill. At v1.0.3 this conflict was discovered live, mid-T-0.

### The day before

This checklist is a hard gate: skipping or compressing any item is an
explicit decision, recorded in the advisory with the reasoning — the same
rule the canary soak already applies. At v1.0.3 the whole list collapsed
into the incident hour with nothing recorded; the rc drills two days earlier
are what made that survivable.

- [ ] Staging rehearsals green **on the final commit** — CI suites and the
      tag dress rehearsal; soak criteria met.
- [ ] Quiet heads-up to every release-capable maintainer: "a security
      release is being prepared — hold releases until further notice." This
      leaks only the schedule, which is public by design (non-goal 2), and
      it is the main defense against two release trains claiming the same
      version: under embargo you are invisible to a teammate correctly
      running the regular checklist (this happened at v1.0.3's T-0).
- [ ] Decide who executes the public T-0 triggers (branch push, PR merge,
      workflow dispatch, promotion): the operator runs them directly by
      default; if an agent drives the release, pre-authorize its T-0 command
      set now — permission prompts mid-incident are unplanned friction at
      exactly the wrong moment.
- [ ] Optional [pre-announcement](#pre-announcement) published.
- [ ] Main-base only: freeze the Mergify `batched` queue.
- [ ] Partner/upstream notifications sent per `SECURITY.md`
      (see [Coordination](#coordination-with-upstream-and-the-ecosystem)).
- [ ] Block ~2 hours of uninterrupted time. Every step below is resumable
      (`Create release` reuses drafts and is safe to re-dispatch; the crates
      loop restarts per-crate; signing is idempotent) — but don't start T-0 if
      you can't plausibly finish it.

### T-0 sequence — Mode A (default)

Complete, verified assets exist before the tag does. Times from recent
`Create release` runs (~18–30 min dispatch-to-tag).

| Clock | Step |
| --- | --- |
| T-0 | Branch base: push `hotfix/vX.Y.Z`. Main base: open the public PR from `hotfix/vX.Y.Z` and squash-merge it into `main`. |
| T+2 | Dispatch `Create release` **from the hotfix branch** (or `main`, for main base) with the exact tag. |
| T+2–25 | Hands-off build. Meanwhile: fresh checkout for crates publish, final announcement text. |
| T+25 | Approve the `release` environment (yourself — this is your deliberate stop-and-check point, not a formality: right commit? right tag?). |
| T+30 | Tag + complete pre-release published; tag-push run starts Docker publishing. |
| T+30 | Start the crates publish loop (below) — it runs ~30–60 min, mostly waiting. |
| T+35 | `make sign-release TAG=v<version>` (signs `SHA256SUMS.txt`, uploads `.minisig`). |
| T+40 | Promote (stable only: clear pre-release **and** set latest). Publish the GHSA advisory. Announce. |
| T+45–75 | Docker images land; confirm assets, images, and manifest per the standard checklist. |

### Mode B — source-first (active exploitation only)

Dispatching `Create release` with the `source_first_release` input skips the
staged asset build: validation only (~4–8 min), approve, and the **tag +
source-only release publish at ~T+10**. The tag-push run of
`release-binaries.yml` then sees an incomplete asset set and rebuilds and
attaches assets to the existing release, so binaries land ~T+35–50, then
sign, promote, follow-up announcement. Announce at T+10 stating explicitly
what exists: "source and crates now; signed binaries by ~HH:MM UTC"
(`cargo install --locked zakura` is the fastest verified install path in this
window).

What Mode B trades, explicitly:

- an **immutable tag exists before its binaries are verified**; if the
  post-tag build fails, the repair path is `release-binaries.yml`
  `workflow_dispatch` with `publish_assets_to_release`. The tag dress
  rehearsal of the same commit is what keeps this risk small — and juggling a
  repair alone is exactly the failure mode Mode A avoids;
- until signing, the release page has no `SHA256SUMS.txt`/signature, so
  `VERIFY.md` verification is impossible;
- it saves ~15–25 minutes to tag/source/crates and does **not** meaningfully
  accelerate signed binaries. Worth it under active exploitation; not
  otherwise. **Solo default is Mode A.**

### Crates publish

From a fresh checkout of the new tag, per the standard checklist: `cargo
login`, then the publish loop in dependency order (`zakura-test … zakura`),
edited to the crates that changed. Hotfix notes: crates.io is **public and
irreversible from the first crate** — it's part of T-0, never earlier;
`release.toml` allows publishing from `hotfix/v*` checkouts; a mid-loop
failure is fixed forward by publishing the remainder, not by yanking.

### Pre-announcement

Optional, recommended for high severity: 24–72h before T-0, announce that **a
security release will be published at \<date, time UTC\>** — nothing else — so
operators are watching when it lands. Long-standing ecosystem practice (Zcash
2018; Bitcoin Core CVE-2018-17144). Trade-off: it also tells attackers when to
start diffing; net positive when operator reaction time dominates. If
severity warrants partner notifications under `SECURITY.md`, it usually
warrants a pre-announcement.

### Post-release

- [ ] Hotfix base: forward-merge `hotfix/vX.Y.Z` into `main` via a normal
      public PR, immediately, using a **merge commit** so the tagged commit
      becomes an ancestor of `main`. Never squash it: a squash copies the
      content but orphans the tagged commit, breaking later `BASE_TAG`
      ancestry checks (both rc-drill forward-merges, #350 and #354, were
      squashed by accident). The `main` ruleset only offers squash and
      rebase, so this depends on the merge-method standing precondition: if
      `merge` is not permanently allowed there, temporarily add it (Settings
      → Rules → `main` → Require a pull request → allowed merge methods, or
      `gh api --method PUT repos/zakura-core/zakura/rulesets/<id>` with the
      amended rule), merge with the dropdown set to "Create a merge commit"
      — **the button defaults to squash** — then revert the ruleset edit.
- [ ] Main base: nothing to forward-merge — the fix entered `main` at T-0 as
      the squash commit the tag points to. Un-freeze Mergify.
- [ ] Confirm installer-metadata/post-release automation completed as usual.
- [ ] Delete embargoed branches, rehearsal tags, and the private rehearsal
      release from the staging repo **before the next release preparation
      begins**: a leftover rehearsal tag shadows the real tag of the same
      name and breaks the `make pre-release` base-tag ancestry check.
- [ ] Update the advisory with fuller detail when appropriate; credit the
      reporter.
- [ ] Retro: what leaked, what was slow, what the rehearsal missed; update
      this document.

## The T-0 exposure window

Mode A: the fix is publicly visible ~30 minutes before a complete signed
release exists. Mode B: ~10 minutes before a tag + source release, ~35–50
before signed binaries. The window cannot be zero: binaries are
provenance-attested by public-repo CI and the release manifest embeds the
repository and URLs (verified at publish) — attaching private-built binaries
would permanently weaken what `VERIFY.md` means, on exactly the release where
it matters most — and GitHub has no "staged but hidden" state for commits.
Mitigations: the private dress rehearsal makes the public run one-shot;
pre-announcement compresses operator reaction time; Mode B compresses
time-to-source when it matters.

## Coordination with upstream and the ecosystem

Zakura is a Zebra fork; incidents come in three shapes:

- **Inherited from Zebra**: `SECURITY.md` obliges responsible disclosure with
  the Zcash Foundation and ECC; T-0 is then chosen _jointly_ (expect the
  slower party to set it).
- **Zebra ships a fix first**: the clock is already running — this process
  executes compressed (base decision, port, soak as severity allows, release;
  skip the pre-announcement — upstream's release was it).
- **Zakura-only code** (e.g. the P2P v2 stack): we control timing; notify
  upstream/partners per `SECURITY.md` where relevant.

Partner notifications follow `SECURITY.md`'s RD-Crypto-Spec commitments,
including the counterfeiting-bug deviation (partners may be told to upgrade
without full detail).

## Caveats and risks

1. **No second review of consensus-critical code.** The sharpest compromise
   in the solo design. Mitigations: minimal diffs, the cold-review protocol,
   the reporter as reviewer, a longer soak. For counterfeiting-class bugs,
   try hard to break solo and bring in one other maintainer — the embargo
   widens by one person; the review exists.
2. **Two-person controls become one-person checkpoints.** The environment
   approval still forces a pause and leaves an audit trail, but it no longer
   proves a second person agreed. (The App-only tag creation and tag
   immutability are unchanged.)
3. **You are the single point of failure at T-0.** ~90 minutes of attention,
   interruptions happen; every step is resumable, and the GHSA draft holds
   enough state for another maintainer to take over.
4. **Patch-diffing is inevitable** from T-0. Only upgrade speed mitigates it.
5. **Accidental early push** of an embargoed branch is unrecoverable and,
   solo, uncaught: dedicated clone, `security/*` naming, the pre-push hook.
   If it happens anyway: assume disclosure, go to Mode B immediately.
6. **Staging CI is near-parity, not parity** (hosted-runner fallbacks,
   different caches, no attestation): the dress rehearsal is the best signal
   available before T-0, and the public run can still surprise — Mode A
   contains that risk before the tag exists; Mode B moves part of it after.
7. **Mode B's tag precedes its verified binaries** and its release page is
   temporarily unverifiable; the announcement must say exactly what exists.
8. **crates.io is irreversible** and partial publishes are possible; the
   `--verify` packaging rehearsal exists to prevent them.
9. **Hotfix branches weaken the everything-releases-from-`main` invariant**,
   and solo the compensating controls are thin by design (the branch ruleset,
   the tag-name/branch-name match, the environment checkpoint, mandatory
   forward-merge, audit trail).
10. **Canaries run embargoed code on the public network.** Usually invisible;
    for fixes with observable behavior changes (peering, message patterns),
    consider whether the canary itself leaks.
11. **Unrehearsed processes rot.** Drills (below) are part of the process.
12. **A concurrent regular release can claim your version.** The embargo
    blinds both sides: at v1.0.3's T-0, a teammate correctly following the
    public checklist pushed their own `release/v1.0.3` and release PR at the
    same hour — caught only because our branch push happened to be rejected
    non-fast-forward. Had their dispatch run first, the version would have
    shipped without the fix and been burned (tags are immutable, never
    reused). Defenses, in depth: the branch namespace rule (the hotfix
    process never touches `release/v*`), the hold-releases heads-up the day
    before, and checking for open release PRs, `release/v*`/`bump-v*`
    branches, and running `Create release` dispatches before pushing
    anything at T-0.

## Drills

Run a full solo drill on a fake "vulnerability": trivial private fix, real
staging-repo rehearsals (including the tag dress rehearsal), real releases —
**always tagged as release candidates** (`vX.Y.Z-rcN`). RCs are the
designed-safe drill vehicle: hyphenated tags never move the Docker `latest`
aliases, RCs are never promoted to the GitHub "Latest" release, and published
pre-releases are already a normal, permanent part of the repository's
history. Exercise Mode A once and Mode B once (separate RCs of the same
line), including `make sign-release` against the RC tag — signing an RC is
harmless and exercises the signing tooling; promotion stays un-exercised by
design. Skip the crates publish: the packaging `--verify` rehearsal covers it
without spending permanent registry versions. Time every step; replace the
estimates in the runbook with measured numbers.

## Scaling up (when a second person is available)

The process is solo-complete; each additional person upgrades a specific
weakness, in this order of value:

1. **A reviewer for the fix** (removes caveat 1) — they only need access to
   the private repo, nothing else.
2. **A real second approver** at T-0: they approve the `release` environment
   and act as standby if you're interrupted (upgrades caveats 2 and 3).
3. **Workflow-driven soak deploys** — register the self-hosted deploy
   runner(s) and the deploy SSH key in the staging repo so canaries deploy
   through the same workflow as production instead of by hand — and, with
   more hands, keeping the staging repo continuously synced instead of pushed
   on demand.

## Open policy points

1. **Is solo acceptable for every severity class?** Position above: yes as a
   floor, but counterfeiting-class bugs should break solo for review if at
   all possible.
2. **Pre-announcement**: opt-in per incident (as written) or standard for
   every hotfix? 24h or 72h lead?
3. **Soak floor**: 24h + 1 mainnet canary for consensus-touching fixes —
   right floor? Who signs off on compressing it?
4. **Drill cadence**: quarterly, or aligned to release cadence?
