---
name: create-zakura-pr
description: >-
  Create and finish Zakura pull requests with a draft-first workflow. Use when
  the user mentions PR, pull request, open PR, draft PR, create PR, publish PR,
  or prepare PR.
---

# Create a Zakura Pull Request

Open pull requests in draft mode by default. Only create a ready-for-review PR
when the user explicitly requests it.

## Workflow

1. Review the complete branch diff and commit history.
2. Check IDE diagnostics for edited files and run any quick mandatory checks,
   including Markdown lint for changed Markdown.
3. Commit and push the focused change.
4. Open the PR immediately with `gh pr create --draft`, using the repository PR
   template and a conventional-commit title.
5. After the draft PR exists, run compilation and risk-proportional local
   verification.
6. Fix failures, commit the fixes as follow-up commits, push them, and update
   the draft PR's test evidence.
7. Keep the PR in draft until required checks pass and the user asks to mark it
   ready.

Do not wait for the branch to compile before opening the draft PR. A draft PR
provides early visibility and starts CI while local compilation proceeds.

If compilation or tests cannot run, leave the PR in draft and state the blocker
in the PR summary. Never represent an unrun check as passing.

## Verification

- High-risk changes: run all relevant CI-equivalent tests and lints after the
  draft is open and before marking it ready.
  - Use this for changes over 300 LOC, or changes touching consensus, P2P,
    cryptography, serialization, database migrations, or other
    validation-critical logic.
- Low-risk changes: skip local compilation unless the user requests it; rely on
  draft CI and report the skipped checks.
  - Use this for small PRs, configuration changes, CI boilerplate, docs-only
    changes, or other low-risk edits.
- Markdown changes: always run Markdown lint for the changed files.
- Check IDE diagnostics for recently edited files before opening the draft and
  again after follow-up fixes.
