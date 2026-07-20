# Changelog fragments

Every ordinary pull request owns exactly one file in this directory. This
keeps concurrent work out of the shared changelogs and makes the unreleased
notes reviewable with the change that introduced them.

After opening a draft PR, create `changelog-unreleased/<PR-number>.md`. Put
each operator-visible change under its Keep a Changelog category:

```markdown
## Fixed

- Fixed the operator-visible behavior
  ([#123](https://github.com/zakura-core/zakura/pull/123)).
```

Valid categories are `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, and
`Security`. The repository does not maintain per-crate changelogs; release
preparation uses semver and public-API checks to choose crate version bumps.

Write complete Keep a Changelog list items, including the PR link. Multiple
targets and categories belong in the same fragment. Parameter changes still
need a row in `CHANGELOG_PARAMS.md`.

For an internal-only PR, use an explicit marker and explain the exclusion:

```markdown
<!-- changelog: none -->

This PR only changes tests and has no operator- or crate-consumer-visible
effect.
```

Run `./scripts/changelog.py check` to validate pending fragments. Release PRs
run `make prepare-release-changelog RELEASE_TAG=vX.Y.Z` after version bumps;
that command consumes the fragments into the root changelog.
