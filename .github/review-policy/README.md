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

Release exclusions cover the existing release gates and publishing helpers.
Ordinary fleet deployment and the advisory VCT canary remain in the deployment
scope; the release workflow treats these as advisory operations. When adding or
moving a release helper into an eligible root, add it to `human_only` in the same
human-reviewed PR. Update the GitHub reviewer patterns with every scope change.

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
   reviewers would make each adapter execution manual, so use the native PR
   reviewer rules below for human approval requirements.
4. Set these repository variables from the App and human team metadata:

   | Variable | Value |
   | --- | --- |
   | `CODEX_APPROVAL_APP_CLIENT_ID` | The dedicated App's client ID |
   | `CODEX_APPROVAL_APP_ID` | Its numeric App ID |
   | `CODEX_APPROVAL_BOT_ID` | Numeric ID of its `[bot]` account |
   | `CODEX_APPROVAL_HUMAN_TEAM_ID` | Numeric ID of the human reviewer team with repository write access |

5. Update the active `main` repository ruleset. Retain the global one-approval
   requirement and the existing required `test success` check. Enable both
   **Dismiss stale pull request approvals when new commits are pushed** and
   **Require approval of the most recent reviewable push**. Require one approval
   from the human team on the ordered patterns printed by:

   ```sh
   python3 .github/review-policy/adapter.py --patterns
   ```

   Add them as one **Required reviewers** entry. In the REST ruleset schema this
   is `required_reviewers[{file_patterns, minimum_approvals: 1,
   reviewer: {id: TEAM_ID, type: "Team"}}]`. Keep the ruleset active with no bypass
   actors. Human team approval can satisfy the global requirement too; the App
   cannot satisfy the human team requirement for release or application changes.
6. Run the validation below with disposable PRs before enabling normal use.
   Finally set `CODEX_APPROVAL_ENABLED=true`.

The writer independently rereads the ruleset and checks its patterns, team,
freshness controls, and required test check before every approval. Missing or
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
PR while protected paths still require a human team member.

Specifically delay an approval request until after a new commit is pushed and
confirm GitHub will not permit merging based on that old-commit review. The
review API has no atomic expected-current-head precondition. The adapter submits
the full reviewed `commit_id`, rereads state before and after approval, and
withdraws its approval when those reads disagree; GitHub's native freshness
rules must enforce the merge boundary. Mocked tests cannot establish that server
behavior. Do not activate this adapter if that disposable-PR check fails.

Only this App's marked approvals are withdrawn. An explicit dismissal of an
episode is respected until a fresh clean Codex review supplies a new receipt.
An unavailable API withholds approval and attempts to withdraw an existing one;
failed withdrawal surfaces as a failed Actions run. Updates caused by review
comments and reaction changes are asynchronous, with hourly reconciliation as a
backstop. There is no synchronous Codex merge check.

To stop new approval writes, set `CODEX_APPROVAL_ENABLED=false`. Dismiss existing
adapter approvals if they must stop counting immediately; disabling the workflow
or revoking the key does not erase reviews already submitted. Retain the global
approval requirement and human path rules so ordinary human review keeps working.

## References

- [Native Codex GitHub reviews](https://learn.chatgpt.com/docs/third-party/github)
- [GitHub required reviewers and freshness rules](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/available-rules-for-rulesets)
- [GitHub pull request review API](https://docs.github.com/en/rest/pulls/reviews#create-a-review-for-a-pull-request)
