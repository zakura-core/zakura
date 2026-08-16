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
   active.
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

The workflow always builds the commit selected when it was dispatched, even if
`main` advances before approval. It is safe to rerun after a partial failure:
it reuses an unpublished draft or exits successfully for a release already
published from the expected commit. It refuses to reuse a tag that points
elsewhere. Every release is initially a pre-release; see
[Promotion and the "Latest" release](#promotion-and-the-latest-release) for
when and how a release is promoted.

## Crates.io Trusted Publishing Dry Run

The manually dispatched
[`Dry-run crate publishing`](../.github/workflows/publish-crates.yml) workflow
is a no-upload proof of concept. It checks out an existing published release
tag, computes the exact workspace versions absent from crates.io, and runs
Cargo's complete dry-run publishing checks. A separate checkout-free job then
exchanges GitHub's OIDC identity for a short-lived production crates.io token
and immediately revokes it. The token is never passed to repository code, and
the workflow contains no registry upload command.

One-time setup requires two distinct permission levels:

1. A repository administrator creates a GitHub Actions environment named
   `crates-io`, restricts its deployment branch to `main`, configures a
   required reviewer, and disables administrator bypass of protection rules.
   Add no environment secrets or variables. The environment constrains the
   OIDC identity; manually dispatching the workflow is the POC's only trigger,
   and the `oidc-smoke` job waits for that reviewer before exchanging a token.
2. An owner of each existing crate opens that crate's **Settings > Trusted
   Publishing** page on crates.io and adds a GitHub Actions publisher with:
   - repository owner `zakura-core`
   - repository name `zakura`
   - workflow filename `publish-crates.yml`
   - environment `crates-io`

Only a crates.io owner can configure a crate's trusted publisher. An existing
owner can grant another maintainer access from the crate's Owners settings or
with `cargo owner`. A crate that has never been published must first be
published manually with a narrowly scoped token from a trusted maintainer
machine; never store that bootstrap token in GitHub.

After setup, an operator needs repository Write access or higher, but no
crates.io account permission:

1. Open **Actions > Dry-run crate publishing > Run workflow**.
2. Select `main`, enter the existing release tag, and dispatch the run.
3. Ask a `crates-io` environment reviewer to approve the pending
   `oidc-smoke` deployment.
4. Review the crate/version/status table in the workflow summary and confirm
   both jobs pass.

A successful OIDC exchange proves that at least one trusted-publisher
configuration matches; Cargo dry-run does not exercise registry upload
authorization for every crate. Audit every crate's configuration before adding
real publication in a later change. Continue to use the manual crates.io
publishing procedure during this POC.

The `crates-io` environment has a required reviewer from day one, and
administrator bypass of protection rules is disabled. Publishing to crates.io
has historically been a separate decision from tagging — `v1.0.3-rc1`,
`v1.1.0-rc0`, and `v1.2.0-rc0` were tagged but intentionally never published —
so the reviewer preserves that decision point once this POC graduates to a
real upload path. For the dry-run workflow, the `oidc-smoke` job pauses at
"Review pending deployments" until a reviewer approves; ping a reviewer after
dispatching. When trusted publishing is folded into the release CI, keep the
reviewer on `crates-io` and move the trusted publisher's workflow filename to
whichever workflow actually publishes. Until then the trusted-publisher
entries grant only what the POC exercises — minting and revoking a token — so
remove them if this POC is abandoned rather than wired into the release path.

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
