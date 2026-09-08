# Native Codex approval adapter

The [adapter workflow](../workflows/codex-approval-adapter.yml) runs on
GitHub-hosted Ubuntu. It observes the existing Codex GitHub integration and can
submit an `APPROVED` review through a dedicated GitHub App. It starts in read-only
audit mode. Merging this implementation does not enable approval writes.

## Contributor flow

Bot approval is available only when the person who opened the PR currently has
Write, Maintain, or Admin access to `zakura-core/zakura`. Authors with Read,
Triage, or no repository access use the normal human review process, even if someone with
more access requests Codex review on their PR. Bot-authored PRs also use normal
review. This check uses the author's live repository permissions, including team
and organization grants; membership labels and the event sender do not qualify.

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
The adapter requires this fragment when an eligible PR changes a Rust source
file or `Cargo.toml`, including internal changes with a no-changelog fragment.
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
- For manual reviews, a visible, recent, unedited `@codex review` request from
  an account with current Write, Maintain, or Admin access. Commands from other
  accounts are ignored, including for invalidation of an existing approval.
  A newer visible request from an authorized account invalidates the previous receipt.

The parser supports the native Code Review summary table. A changed format,
missing history, additional unsupported review rows, ambiguous commit resolution,
or incomplete pagination withholds approval. The public integration does not
provide a versioned machine verdict or a run ID tying a manual request to its
result, so this adapter is deliberately conservative about observable evidence.
It does not treat comment text as a cryptographic attestation of review coverage.
In particular, deleting a scoped manual request can make its result appear to
belong to an older ordinary request. This limitation is accepted for PRs opened
by trusted authors; the author permission gate restricts eligible PRs but does
not prove which comment triggered a native review. Normal `@codex review` remains
supported without a separate request journal.

Comment and PR metadata events trigger evaluation. Because reactions have no
webhook event, a completion event gets one short retry for the thumbs-up. Hourly
reconciliation catches missed events, removed reactions, changed policy, and
interrupted runners. Manual dispatch on `main` rechecks evidence without asking
Codex for another review.

A read-only permission preflight skips the App job when no target PR has an
authorized author. The exception is cleanup: a PR with an existing App approval
still reaches the writer so it can withdraw that approval if access was revoked
or cannot be verified. The writer independently checks the author's access on
every evaluation before and after approval, including scheduled runs.

## Administrator setup

After this PR is merged, keep `CODEX_APPROVAL_ENABLED` unset or `false` while
configuring the credentials. No GitHub environment or review-rule changes are
needed.

1. Create a dedicated GitHub App with repository **Pull requests: read and write**
   permission and webhooks disabled. Install it only on `zakura-core/zakura` and
   generate a private key. The App needs no additional permissions beyond the
   mandatory metadata access; do not grant administration access or ruleset bypass.
2. Under repository **Settings → Secrets and variables → Actions**, add the
   repository secret `CODEX_APPROVAL_APP_PRIVATE_KEY` with the full PEM key
   contents. Keep the key in a dedicated Infisical scope and sync it here.
3. On the **Variables** tab, add these repository variables:

   | Variable | Value |
   | --- | --- |
   | `CODEX_APPROVAL_APP_CLIENT_ID` | The dedicated App's client ID |
   | `CODEX_APPROVAL_APP_ID` | Its numeric App ID |
   | `CODEX_APPROVAL_BOT_ID` | Numeric ID of its `[bot]` account |

   To get the bot ID, replace `APP_SLUG` with the App's actual slug:

   ```sh
   gh api 'users/APP_SLUG[bot]' --jq '.id'
   ```

4. Set repository variable `CODEX_APPROVAL_ENABLED=true` and run the live
   validation below. Confirm that a clean eligible PR receives an App approval
   and an excluded PR does not. Set the variable back to `false` if validation
   fails. Personal Codex settings and the normal review process stay unchanged.

The key is a repository Actions secret, so no environment approval or branch
restriction gates access to it. The adapter still checks out trusted `main`.
The existing `main` ruleset already supplies the one required approval,
`test success` check, and empty bypass list; leave those settings unchanged.

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
Verify that Write, Maintain, and Admin authors qualify, while Read, Triage, and
outside authors do not, regardless of who requested review. Also verify that
revoking an author's access withdraws an existing App approval on reconciliation.

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
The adapter can restore its own automatic withdrawal after all evidence qualifies
again. It verifies the dismissal's actor and message in GitHub's timeline;
missing or ambiguous dismissal history keeps the approval withheld.
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
