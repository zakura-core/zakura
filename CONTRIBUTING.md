# Contributing

- [Contributing](#contributing)
  - [Running and Debugging](#running-and-debugging)
  - [Bug Reports](#bug-reports)
  - [Pull Requests](#pull-requests)
  - [Code Standards](#code-standards)

## Running and Debugging

See the [getting started guide](README.md#getting-started) for details on how to
build and run Zakura.

## Bug Reports

Please [create an issue](https://github.com/zakura-core/zakura/issues/new?assignees=&labels=C-bug%2C+S-needs-triage&projects=&template=bug_report.yml&title=) on the Zakura issue tracker.

## Pull Requests

PRs are welcome. Please:

1. **Explain the change.** Describe the motivation and solution in the PR body. For fixes, identify the root cause and explain how the solution addresses it. An issue link is welcome when relevant, but is not required.
2. **Keep PRs focused.** Prefer one logical change per PR and avoid unrelated refactors.
3. **Test the change.** Run checks appropriate to the affected code. Explain how the tests exercise the root cause and verify the solution, rather than only listing commands.
4. **Follow conventional commits.** PRs are squash-merged to `main`, so the PR title becomes the commit message. Follow the [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/#specification) standard.

Zakura is a validator node — it excludes features not strictly needed for block validation and chain sync. Features like wallets, block explorers, and mining pools belong in [Zaino](https://github.com/zingolabs/zaino), [Zallet](https://github.com/zcash/wallet), or [librustzcash](https://github.com/zcash/librustzcash).

Check out the [help wanted][hw] or [good first issue][gfi] labels if you're looking for a place to get started.

[hw]: https://github.com/zakura-core/zakura/labels/E-help-wanted
[gfi]: https://github.com/zakura-core/zakura/labels/good%20first%20issue

## Code Standards

Zakura enforces code quality through review. For the full list of architecture rules, code patterns, testing requirements, and security considerations, see [`AGENTS.md`](AGENTS.md). The key points:

- **Build requirements**: Run `cargo fmt` and the relevant `cargo clippy` and `cargo test` commands
- **Architecture**: Dependencies flow downward only; `zakura-chain` is sync-only
- **Error handling**: Use `thiserror`; `expect()` messages explain why the invariant holds
- **Async**: CPU-heavy work in `spawn_blocking`; all waits need timeouts
- **Security**: Bound allocations from untrusted data; validate at system boundaries
- **Changelog**: After opening a draft PR, add one
  `changelog-unreleased/<PR-number>.md` fragment. Internal-only PRs use an
  explicit no-changelog marker (see
  [`CHANGELOG_GUIDELINES.md`](CHANGELOG_GUIDELINES.md)).
