# Release AI review

This advisory workflow reviews the complete release diff from the preparation
baseline to the release PR head, including changes already merged into `main`.
It reports concrete consensus, cryptography, mining, and block propagation bugs
as Critical, High, or Medium. It does not execute reviewed code, run tests, send
Slack messages, change release gates, or approve a release.

## Enable a provider

Merge the workflows and review implementation into `main` before using them.
GitHub loads the reviewer, prompts, and configuration from that trusted revision.
The PR request workflow carries no secrets and performs no checkout. The review
workflow rechecks the triggering repository, workflow, PR branch, draft status,
and current head. Forks and embargoed `hotfix/*` branches are excluded.

Set the `OPENAI_API_KEY` repository secret for OpenAI. For an OpenAI-compatible
Abliterated endpoint, set a separate `ABLITERATED_API_KEY`. Use the organization's
existing Infisical source and secret-delivery process; never put credentials in
the configuration or prompts.

Set the repository variable `RELEASE_REVIEW_MODELS` to a JSON object keyed by
`openai` and/or `abliterated`. Each configured provider requires these fields:

| Field | Meaning |
| --- | --- |
| `model` | Explicit API model identifier; there is no implicit default. |
| `input_usd_per_million` | Positive current input price, as a number or decimal string. |
| `output_usd_per_million` | Positive current output price, including reasoning tokens. |
| `max_input_bytes` | Conservative request allowance, including JSON, schema, and framing. Use at most the model's input-token limit minus the output reservation. |
| `max_output_tokens` | Optional output/reasoning reservation; defaults to 4096. |
| `reasoning_effort` | OpenAI only: `none`, `low`, `medium` (default), or `high`. `none` omits the API option. |
| `base_url` | Abliterated only: HTTPS API prefix, normally ending in `/v1`; no embedded credentials. |

Configure only providers you actually use. Use the highest applicable input and
output prices for the selected model and request size; do not assume prompt-cache
discounts. Price changes require updating this variable. The $25 ceiling is an
estimate based on these configured rates, not a provider billing guarantee.

The runner reserves conservative input and maximum output cost before every
request, including retries. It replaces a reservation with reported usage after
a successful response. A timeout or missing usage keeps the full reservation.
Unknown usage, incompatible responses, missing prices, and exhausted budgets
cannot produce a clean review. The code never switches providers on failure.

## Prompts and manual runs

Edit the Markdown files in `prompts/` and the profile lists in `config.toml`.
Add a named profile there to compare instructions without changing Python.
Prompt changes take effect after merging into `main`. The report records their
digest and the exact reviewer commit.

Automatic runs start when a same-repository `release/v*` PR into `main` becomes
ready and when its head changes. Opening or reopening an already-ready release
PR also requests a review. Manual runs can target draft release PRs:

```bash
gh workflow run release-ai-review.yml -R zakura-core/zakura --ref main \
  -f pr_number=123 -f provider=openai -f prompt_profile=default
```

Replace `123` with the actual open release PR number. Optionally add
`-f base_tag=v1.7.0-rc2` to select a different ancestor release tag. Without an
override, the workflow uses the preparation's machine-readable tag/commit marker
or, for older PRs, the nearest reachable release tag. Moved recorded tags and
non-ancestor baselines are rejected.

Each rerun has its own budget. It updates the bot's existing PR summary, with
artifacts retained per run for 14 days. Newer reviews cancel older review jobs
for that PR. The publisher rechecks the current PR head and the existing
comment's run identity before writing. Results for old heads remain artifacts.

## Interpret the result

Every changed file is listed in the JSON coverage manifest. Text diffs are split
into bounded units without silently discarding hunks. The model can read source
at either pinned revision and search related unchanged files. Findings must cite
an exact source quotation and identify a changed release file. A synthesis pass
challenges candidates and examines cross-component interactions.
Large sets of summaries are reduced in bounded groups before the final synthesis;
every intermediate synthesis is retained in the JSON evidence.

Binary files, symlinks, oversized sources or lines, unfinished units, invalid
responses, and an unfinished synthesis are explicit gaps. Filling the finding
limit also makes the result incomplete, since more findings may be missing.
An incomplete review fails the advisory workflow and says **INCOMPLETE** in its comment. No finding
does not mean no bugs, and partial coverage is never labeled clean. Preliminary
candidates remain in the JSON artifact when synthesis cannot finish.

The report contains the baseline and head commits, reviewer commit, prompt
digest, provider/model, pricing, usage, duration, findings, and coverage. The PR
comment links to commit-pinned source because the release PR's ordinary diff may
contain only preparation changes. All findings in this first stage are supported
by source evidence; no reproduction is claimed.

## Validation and rollout

Run the standard-library fixture suite with Python 3.11 or later:

```bash
python3 -m unittest discover -s .github/scripts/tests -p 'test_release_review*.py'
```

Before calling this stage operational, run it against an actual release PR using
OpenAI, verify range and coverage, then dispatch it again and verify that it
updates the same comment. Fixture tests establish behavior for invalid model
output, missing configuration, budget exhaustion, stale heads, and provider
contracts. A provider's fixture tests are not evidence of a successful live API
run. Compilation and notification stages are separate follow-up changes after
this live acceptance gate passes.
