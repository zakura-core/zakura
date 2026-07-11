# Protected Release Tags

Zakura release tags must be created by the
[`Create release`](../.github/workflows/create-release.yml) workflow. The
workflow validates that the requested `v*` tag matches the `zakura` package
version before creating the tag and GitHub release. The tag push then triggers
[`release-binaries.yml`](../.github/workflows/release-binaries.yml).

## GitHub App

Create an organization-owned GitHub App named `zakura-release-bot` with:

- Repository permission `Contents: Read and write`
- Installation access limited to `zakura-core/zakura`
- No webhook subscription

Create a private key for the app. Configure a GitHub Actions environment named
`release`, add the app's client ID as the environment variable
`RELEASE_APP_CLIENT_ID`, and add its private key as the environment secret
`RELEASE_APP_PRIVATE_KEY`. Configure required reviewers for the environment so
that creating a release requires explicit approval.

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
4. Approve the `release` environment deployment.
5. Confirm that the workflow creates the tag and GitHub release.
6. Confirm that `Release binaries` starts from the new tag.

The workflow is safe to rerun after a partial failure when the tag already
points to the same commit. It refuses to reuse a tag that points elsewhere.
