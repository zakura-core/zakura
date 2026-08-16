# Protected Release Tags

Zakura release tags must be created by the
[`Create release`](../.github/workflows/create-release.yml) workflow. The
workflow validates that the requested `v*` tag matches the `zakura` package
version, builds and verifies the release assets from that exact commit, and
then creates the tag by publishing a complete pre-release. The tag push then
triggers [`release-binaries.yml`](../.github/workflows/release-binaries.yml) to
publish Docker images.

## GitHub App

In the `zakura-core` organization settings, open **Developer settings > GitHub
Apps**, select **New GitHub App**, and create an organization-owned app with:

- Name `zakura-release-bot`
- Homepage URL set to the `zakura-core/zakura` repository
- Repository permission `Contents: Read and write`
- All other permissions set to `No access`
- Webhooks disabled
- Installation restricted to `zakura-core`

After creating the app, select **Install App**, install it on `zakura-core`, and
grant it access only to the `zakura` repository.

Create a private key for the app. Configure a GitHub Actions environment named
`release`, add the app's client ID as the environment variable
`RELEASE_APP_CLIENT_ID`, and add its private key as the environment secret
`RELEASE_APP_PRIVATE_KEY`. Configure required reviewers for the environment so
that creating a release requires explicit approval. Restrict this environment
to the `main` deployment branch.

The app private key is a credential. Store its source copy in the team's secret
manager and do not commit it or paste it into issues, pull requests, or logs.

## Tag Rulesets

In the repository settings, create an active tag ruleset named
`Release tag creation`:

- Target tags matching `v*`
- Enable `Restrict creations`
- Add only `zakura-release-bot` to the bypass list with `Always allow`

Create a second active tag ruleset named `Immutable release tags`:

- Target tags matching `v*`
- Enable `Restrict updates`
- Enable `Restrict deletions`
- Do not add any bypass actors

Keeping immutability in a separate ruleset prevents the release app from
rewriting or deleting an existing release tag. Repository administrators and
organization owners who can edit rulesets can still disable these controls, so
ruleset administration must remain limited.

## Creating a Release

1. Merge the release version bump into `main`.
2. Open **Actions > Create release > Run workflow**.
3. Select the `main` branch, enter the exact release tag, and set the expected
   tag delay. Dispatching the workflow acknowledges that no release hold is
   active. Leave `publish_crates` on `auto` unless this release departs from
   the norm: `auto` publishes stable tags to crates.io and leaves release
   candidates to an explicit dispatch.
4. The workflow resolves the latest digest-verified Mainnet release-state
   bundle and warns about release-level, committed-state, or
   `ESTIMATED_RELEASE_HEIGHT` readiness problems. Then wait for it to build and
   verify the release assets and no-push Docker builds. Nothing is tagged or
   published during this stage.
5. Approve the `release` environment deployment. The workflow publishes the
   complete pre-release, creating the protected tag as its final step.
6. Confirm that `Release binaries` starts from the new tag, skips rebuilding
   the existing assets, publishes the Docker images, and opens the installer
   metadata update pull request.
7. If the run dispatched crates.io publishing, approve the `crates-io`
   deployment on that run. See
   [Crates.io Trusted Publishing](#cratesio-trusted-publishing).

The workflow always builds the commit selected when it was dispatched, even if
`main` advances before approval. It is safe to rerun after a partial failure:
it reuses an unpublished draft or exits successfully for a release already
published from the expected commit. It refuses to reuse a tag that points
elsewhere. Every release is initially a pre-release; see
[Promotion and the "Latest" release](#promotion-and-the-latest-release) for
when and how a release is promoted.

## Crates.io Trusted Publishing

The [`Publish crates`](../.github/workflows/publish-crates.yml) workflow
publishes Zakura's crates from a published release tag, authenticated by
short-lived tokens crates.io mints against this repository's GitHub OIDC
identity. No registry credential is stored anywhere.

`Create release` dispatches it as the last thing it does, once the tag exists,
subject to its `publish_crates` input:

- **`auto`** (the default) publishes a stable tag and skips a release
  candidate. Release candidates have gone both ways — `v1.1.0-rc1` and
  `v1.1.0-rc2` were published while `v1.0.3-rc1`, `v1.1.0-rc0`, and
  `v1.2.0-rc0` deliberately were not — so `auto` asks for a decision instead
  of assuming one.
- **`always`** publishes whatever the tag looks like.
- **`never`**, and any release using `allow_unpublishable_crate_graph`, does
  not dispatch. That override means the publish graph does not resolve, so the
  release is GitHub-only by definition.

Dispatching only queues the upload behind the `crates-io` environment
reviewer. Because that approval waits in the dispatched run rather than in
`Create release`, a release left pending a publishing decision does not hold
the release workflow open. Dispatch `Publish crates` by hand at any later
point for a tag that was skipped, or to retry one that failed.

The workflow takes a `mode`:

- **`verify`** (default) checks out the release tag, computes the exact
  workspace versions absent from crates.io, runs the publish-graph check and
  Cargo's complete dry-run publishing checks, and then exchanges and
  immediately revokes a real token in a checkout-free job. Nothing is
  uploaded. This is the pre-flight: run it before a release to confirm the
  crate selection and that the trusted-publisher configuration still works.
- **`publish`** repeats the plan and uploads. It is irreversible — a crates.io
  version can be yanked but never replaced or reused.

Verification and publication are separate jobs on purpose. The verify builds
take much longer than the 30-minute token lifetime, so they run before any
token exists; the publish job packages with `--no-verify` and only its single
`cargo publish` step sees the token, which the action revokes when the job
ends.

Publishing is resumable. The plan excludes every crate already on the index at
its workspace version, so a run that fails partway through is recovered by
dispatching it again: the crates that landed are skipped and the rest are
published. Fix forward — never try to repair a partial publish by yanking and
re-uploading the same version.

### One-time setup

This requires two distinct permission levels:

1. A repository administrator creates a GitHub Actions environment named
   `crates-io`, restricts its deployment branch to `main`, configures required
   reviewers, and disables administrator bypass of protection rules. Add no
   environment secrets or variables. The environment constrains the OIDC
   identity, and its reviewers are the human gate on an irreversible upload.
2. An owner of **each** crate opens that crate's **Settings > Trusted
   Publishing** page on crates.io and adds a GitHub Actions publisher with:
   - repository owner `zakura-core`
   - repository name `zakura`
   - workflow filename `publish-crates.yml`
   - environment `crates-io`

Only a crates.io owner can configure a crate's trusted publisher, and Zakura's
crates do not all share one owner. An existing owner can grant another
maintainer access from the crate's Owners settings or with `cargo owner`.

Configure every publishable crate, and audit the list when adding one. A
crate missing its entry is not an error at exchange time: crates.io scopes the
minted token to whichever crates did match, the run starts publishing, and the
unconfigured crate fails with a permission error partway through — a partial,
irreversible publish. There is no way to read another owner's configuration,
so this audit is a checklist, backed by the resumable retry above.

A crate that has never been published cannot be bootstrapped this way, because
its trusted-publisher configuration lives on a crate that does not yet exist.
Reserve the name manually with a narrowly scoped token from a trusted
maintainer machine, add the configuration, and only then let CI publish it;
never store that bootstrap token in GitHub. `publish` mode refuses to run when
the plan contains an unpublished crate name, rather than discovering it
mid-upload.

The workflow filename is part of the crates.io configuration: it is matched
against the OIDC `workflow_ref` claim, which names the workflow a run _starts
from_. A reusable workflow does not change it — `create-release.yml` calling
this file would present `create-release.yml`. That is why release automation
dispatches this workflow instead of calling it, and why renaming this file
means reconfiguring every crate.

### Approving a publish

Approving needs no crates.io account permission, only membership in the
`crates-io` environment's reviewers:

1. Open the pending `Publish crates` run and read the crate/version/status
   table in the `Plan and verify crate publication` summary. This is the last
   point at which the upload can be stopped.
2. Watch for rows flagged as below the newest published version. That is
   expected for a hotfix on an older release line, and otherwise means the run
   is publishing from a stale tag — most likely a tag that was deliberately
   never published.
3. Approve the `crates-io` deployment.
4. Confirm every job passes. `Verify the published versions` asserts that each
   new version's public crates.io record names this workflow run and the
   dispatch commit on `main` (the OIDC `sha` claim), and `Install zakurad from
   crates.io` installs and runs the published binary.

Both post-publish jobs run after the uploads are already irreversible, so a
failure there reports a problem rather than preventing one. `Install zakurad
from crates.io` in particular is a cold release build of the whole node and can
exhaust its timeout on a slow runner; re-run the job before concluding the
published crates are broken.

### Dispatching by hand

An operator needs repository Write access or higher. Open **Actions > Publish
crates > Run workflow**, select `main`, enter the published release tag, and
choose the mode. Always dispatch from `main`, including for a hotfix release:
the workflow resolves the tag by name and checks it out on its own, so the
`crates-io` environment stays restricted to `main`.

Reach for this to publish a tag `Create release` skipped, to retry a failed or
partial publish, or — with `mode: verify` — to confirm before a release that
the crate selection and the trusted-publisher configuration are still good.

## Promotion and the "Latest" Release

Both release workflows publish every release with `prerelease: true`, whatever
the tag looks like — nothing promotes a release automatically. Promotion is a
deliberate manual step, governed by this convention:

- **Pre-releases are never promoted.** The "Latest" badge only ever points at
  a stable release, so anything automating against `releases/latest` is never
  handed a release candidate. `v1.0.0-rc3` is the cautionary tale: it was
  hand-promoted, external instructions adopted `releases/latest/download/...`
  URLs, and deleting the release left those links as dangling 404s. (The
  `v1.0.0-rc*` rehearsal line is removed before the first stable release as a
  one-time, pre-1.0.0 exception; after `v1.0.0`, pre-releases stay published
  permanently — the never-promote rule stands on its own and does not depend
  on pre-release removal.)
- **The first "Latest" release is `v1.0.0`.** Until it exists,
  `releases/latest` intentionally returns 404. Any published download
  instructions must use versioned URLs (`releases/download/<tag>/...`) until
  the first stable release is promoted.
- **Stable releases are promoted after testing and signing.** Once
  `make sign-release` has run against the tag, edit the release: clear the
  pre-release flag _and_ check **Set as the latest release** (`make_latest:
  true` via the API). Do both explicitly — the "Latest" badge never points at
  a pre-release, and an unpromoted stable release leaves the repository with
  no "Latest" release at all.
- **Expect brief Docker skew.** The tag-triggered workflow moves the Docker
  `latest` aliases automatically for non-hyphenated tags, before the manual
  GitHub promotion. A short window where the Docker `latest` alias is ahead
  of the GitHub "Latest" release is normal.
