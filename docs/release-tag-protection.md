# Protected Release Tags

Zakura release tags must be created by the
[`Create release`](../.github/workflows/create-release.yml) workflow. The
workflow validates that the requested `v*` tag matches the `zakura` package
version, builds and verifies the release assets from that exact commit, and
then creates the tag by publishing a complete pre-release. The tag push then
triggers [`release-binaries.yml`](../.github/workflows/release-binaries.yml) to
publish Docker images and open the installer metadata update pull request.

## GitHub App

In the `zakura-core` organization settings, open **Developer settings > GitHub
Apps**, select **New GitHub App**, and create an organization-owned app with:

- Name `zakura-release-bot`
- Homepage URL set to the `zakura-core/zakura` repository
- Repository permission `Contents: Read and write`
- Repository permission `Pull requests: Read and write`
- All other permissions set to `No access`
- Webhooks disabled
- Installation restricted to `zakura-core`

The pull-request permission allows the app to open and update pull requests,
while the contents permission allows it to create their branches and commits.

After creating the app, select **Install App**, install it on `zakura-core`, and
grant it access only to the `zakura` repository.

Create a private key for the app. Configure a GitHub Actions environment named
`release`, add the app's client ID as the environment variable
`RELEASE_APP_CLIENT_ID`, and add its private key as the environment secret
`RELEASE_APP_PRIVATE_KEY`. Configure required reviewers for the environment so
that creating a release requires explicit approval. Restrict this environment
to the `main` deployment branch.

Configure a second environment named `release-automation` with the same
variable and secret. Allow deployments from the `main` branch and tags matching
`v*`. This environment is used only after assets are published to open the
installer metadata update pull request. It is separate so tag deployment rules
or approvals cannot block post-release automation.

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
3. Select the `main` branch and enter the exact release tag.
4. Wait for the workflow to build and verify the release assets and no-push
   Docker builds. Nothing is tagged or published during this stage.
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

## Promotion and the "Latest" Release

Both release workflows publish every release with `prerelease: true`, whatever
the tag looks like — nothing promotes a release automatically. Promotion is a
deliberate manual step, governed by this convention:

- **Pre-releases are never promoted.** Hyphenated tags (`v1.0.0-rc4`) stay
  pre-releases from publication until deletion. `v1.0.0-rc3` is the cautionary
  tale: it was hand-promoted, external instructions adopted
  `releases/latest/download/...` URLs, and deleting the release left those
  links as dangling 404s. Pre-release artifacts are removed before the next
  stable release as a matter of policy, so a promoted pre-release always
  becomes a dangling "Latest" eventually.
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
