# Native Codex approval adapter

The [adapter workflow](../workflows/codex-approval-adapter.yml) runs on
GitHub-hosted Ubuntu. It observes the existing Codex GitHub integration and can
submit an `APPROVED` review through a dedicated GitHub App. It starts in read-only
audit mode. Merging this implementation does not enable approval writes.

## Contributor flow

Use the normal automatic Codex review, or comment exactly `@codex review`.
After a clean review of the current commit, eligible PRs receive an approval from
the adapter App. The contributor can merge when GitHub's other requirements pass.
The adapter does not merge PRs or start another model review.

After fixes, push the commits and request `@codex review` again, or let the existing
automatic review setting trigger it. Resolve addressed Codex threads before the
new clean review. Resolving a finding by itself does not qualify the PR.
Personal automatic-review settings and native Codex comments stay as they are.
The adapter does not need an OpenAI API key or consume a second review.

The App supplies an approval through the repository's normal approval process.
No dedicated reviewer team or additional path-based reviewer rule is required.
The adapter enforces its own file eligibility policy. If it cannot approve a PR,
the contributor requests review from the usual reviewers under the existing
repository rules. Existing reviewer or code-owner requirements still apply.

If a PR does not qualify, get a normal human approval. The adapter never submits
`REQUEST_CHANGES` or installs a mandatory Codex status check. Its Actions summary
explains why it withheld approval. Scoped prompts, security-only reviews, drafts,
and native output formats it cannot verify take the human-review path.

## Eligible files

[policy.json](policy.json) is the authoritative scope. Existing files under
`deploy/`, `.github/workflows/`, and `.github/scripts/` qualify, except for:

- Release creation, preparation, publishing, readiness, drafting, and release-state workflows.
- Release-state fetch/import scripts, checkpoint validation, and `deploy/release-state/`.
- The adapter workflow itself.

Everything outside those roots, including `.github/review-policy/`,
`.github/actions/`, root `scripts/`, and application code, requires human review.
One excluded file makes the whole PR ineligible. Both names in a rename are
checked; additions and renames require human classification before they can be
treated as existing eligible files in later PRs.

There is one addition exception: an otherwise eligible PR may add its own
`docs/changelog/unreleased/<PR-number>.md` fragment. For example, PR #123 may
modify `deploy/zakura-watchdog/src/main.rs` and add
`docs/changelog/unreleased/123.md`. Later edits to that new fragment within the
same PR still qualify because it remains an addition relative to the base.
The fragment must be a regular text file without `release-readiness` directives;
release-policy waivers still need human review. The root `CHANGELOG.md`, other
PRs' fragments, existing fragment edits/deletions, and changelog-only PRs remain
on the human-review path. Changelog CI continues to validate fragment syntax.

Release exclusions cover the existing release gates and publishing helpers.
Ordinary fleet deployment and the advisory VCT canary remain in the deployment
scope; the release workflow treats these as advisory operations. When adding or
moving a release helper into an eligible root, add it to `human_only` in the same
human-reviewed PR.

## Native review evidence

The adapter requires all of the following from live GitHub API reads:

- The immutable Codex App and Bot IDs on the summary comment, plus Bot-authored
  GraphQL edit history showing the current `Running` → `Completed` episode.
- A reviewed commit abbreviation that GitHub resolves to the full current PR
  head, with the same commit and trigger throughout the episode.
- A new PR-level thumbs-up from that Bot after completion, with no running-review
  reaction. `Completed` alone also describes reviews that found problems.
- No Codex findings submitted during that episode and no unresolved Codex threads,
  including outdated threads. Older resolved findings allow a new clean review.
- For manual reviews, a visible, recent, unedited `@codex review` request.
  A newer visible review request invalidates the previous receipt.

The parser supports the native Code Review summary table. A changed format,
missing history, additional unsupported review rows, ambiguous commit resolution,
or incomplete pagination withholds approval. The public integration does not
provide a versioned machine verdict or a run ID tying a manual request to its
result, so this adapter is deliberately conservative about observable evidence.
It does not treat comment text as a cryptographic attestation of review coverage.

Comment and PR metadata events trigger evaluation. Because reactions have no
webhook event, a completion event gets one short retry for the thumbs-up. Hourly
reconciliation catches missed events, removed reactions, changed policy, and
interrupted runners. Manual dispatch on `main` rechecks evidence without asking
Codex for another review.

## Administrator setup

Keep `CODEX_APPROVAL_ENABLED` unset or `false` until setup and validation finish.

1. Merge this PR through human review and inspect read-only audit results.
2. Create a dedicated GitHub App and install it only on `zakura-core/zakura`.
   Grant repository **Pull requests: read and write** and the mandatory metadata
   access. It needs no webhook, contents write, administration, Actions write, or
   ruleset bypass. Do not reuse a release App or a person's token.
3. Create the `codex-approval` environment, restricted to the `main` branch.
   Keep the App key in Infisical with a dedicated service scope, and sync it to
   the environment secret `CODEX_APPROVAL_APP_PRIVATE_KEY`. Required environment
   reviewers would make each adapter execution manual, so leave them unset for
   normal automatic operation.
4. Set these repository variables from the App metadata:

   | Variable | Value |
   | --- | --- |
   | `CODEX_APPROVAL_APP_CLIENT_ID` | The dedicated App's client ID |
   | `CODEX_APPROVAL_APP_ID` | Its numeric App ID |
   | `CODEX_APPROVAL_BOT_ID` | Numeric ID of its `[bot]` account |

5. Leave the current `main` ruleset unchanged: one required approval, the required
   `test success` check, and no bypass actors. The adapter does not require
   dismissing stale approvals or approval of the most recent reviewable push.
   Preserve existing reviewer and code-owner requirements; no dedicated team or
   path-based reviewer rule is needed. Release and other excluded changes get
   no adapter approval and follow the normal approval process.
6. Run the validation below with disposable PRs before enabling normal use.
   Finally set `CODEX_APPROVAL_ENABLED=true`.

The writer independently rereads the ruleset and checks its approval requirement
and required test check before every approval. Missing or
weakened configuration prevents new approvals. A trusted `main` checkout is used
in both jobs; PR code, artifacts, and commands never execute with the App token.
Keep GitHub Actions' general permission to approve PRs disabled; this workflow
uses its own narrowly scoped App token.

## Validation and operations

Run the offline tests with:

```sh
python3 .github/review-policy/test_adapter.py
```

In a repository with matching rules and App permissions, validate a clean
eligible PR, a PR with findings, fixes followed by another review, and a push
after approval. Also test a mixed PR, a release helper edit, and a source file
renamed into an eligible directory. The App's review must count for the eligible
PR while excluded paths receive no adapter approval and need normal review.
Test an eligible change accompanied by its own new changelog fragment, plus
rejections for another PR's fragment and a fragment containing a release waiver.

Verify that the adapter withholds a new approval when Codex reviewed an older
commit, and withdraws an existing approval after detecting a push. The adapter
submits the full reviewed `commit_id` and rereads state before and after approval.
The review API has no atomic expected-current-head precondition, so these checks
and later withdrawal do not create a synchronous merge restriction.

The existing repository policy intentionally permits approval to remain valid
after changes. For example, after the App approves commit A, a contributor may
push commit B and merge before the adapter detects it and withdraws its approval.
Enabling GitHub's stale-review settings is not a prerequisite for this adapter;
their behavior remains the maintainer's existing choice.

Only this App's marked approvals are withdrawn. An explicit dismissal of an
episode is respected until a fresh clean Codex review supplies a new receipt.
An unavailable API withholds approval and attempts to withdraw an existing one;
failed withdrawal surfaces as a failed Actions run. Updates caused by review
comments and reaction changes are asynchronous, with hourly reconciliation as a
backstop. There is no synchronous Codex merge check.

To stop new approval writes, set `CODEX_APPROVAL_ENABLED=false`. Dismiss existing
adapter approvals if they must stop counting immediately; disabling the workflow
or revoking the key does not erase reviews already submitted. Retain the global
approval requirement so ordinary human review keeps working.

## References

- [Native Codex GitHub reviews](https://learn.chatgpt.com/docs/third-party/github)
- [GitHub PR approval and freshness rules](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/available-rules-for-rulesets)
- [GitHub pull request review API](https://docs.github.com/en/rest/pulls/reviews#create-a-review-for-a-pull-request)
