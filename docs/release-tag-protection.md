# Protected Release Tags

Zakura release tags must be created by the
[`Create release`](../.github/workflows/create-release.yml) workflow. The
workflow validates that the requested `v*` tag matches the `zakura` package
version, builds and verifies the release assets from that exact commit, and
then creates the tag by publishing a complete pre-release. The tag push then
triggers [`release-binaries.yml`](../.github/workflows/release-binaries.yml) to
publish Docker images and open the installer checksum update pull request.

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
installer checksum update pull request. It is separate so tag deployment rules
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
   checksum update pull request.

The workflow always builds the commit selected when it was dispatched, even if
`main` advances before approval. It is safe to rerun after a partial failure:
it reuses an unpublished draft or exits successfully for a release already
published from the expected commit. It refuses to reuse a tag that points
elsewhere. Every release is initially a pre-release; promotion remains a manual
GitHub step after testing and signing. Existing Docker behavior is unchanged:
a non-hyphenated stable tag moves the Docker `latest` aliases during the
tag-triggered workflow, before that GitHub promotion.
