#!/usr/bin/env bash
# Validate Codex output and generated changes for upstream sync PRs.

set -euo pipefail

RESULT_JSON="${1:?Usage: upstream-sync-validate.sh <result-json> [candidate-json]}"
CANDIDATE_JSON="${2:-}"

if [ ! -f "$RESULT_JSON" ]; then
  echo "ERROR: result JSON not found: $RESULT_JSON" >&2
  exit 1
fi

jq -e '
  type == "object" and
  (.status | IN("applied", "already_present", "needs_human", "failed", "skipped")) and
  (.source_pr | type == "number") and
  (.confidence_percent | type == "number" and . >= 0 and . <= 100) and
  (.recommendation | type == "string" and length > 0) and
  (.branch_name | type == "string" and test("^upstream-sync/pr-[0-9]+$")) and
  (.pr_title | type == "string" and length > 0) and
  (.pr_body | type == "string" and length > 0) and
  (.files_changed | type == "array") and
  (.validation | type == "array") and
  (.risks | type == "array") and
  (.follow_up | type == "array") and
  (.triage_decisions | type == "array") and
  all(.triage_decisions[]?; (
    (.source_pr | type == "number") and
    (.status | IN("already_present", "skipped")) and
    (.confidence_percent | type == "number" and . >= 0 and . <= 100) and
    (.recommendation | type == "string" and length > 0)
  ))
' "$RESULT_JSON" >/dev/null

if [ -n "$CANDIDATE_JSON" ]; then
  if [ ! -f "$CANDIDATE_JSON" ]; then
    echo "ERROR: candidate JSON not found: $CANDIDATE_JSON" >&2
    exit 1
  fi
  ACTUAL_PR="$(jq -r '.source_pr' "$RESULT_JSON")"
  CANDIDATE_MATCH="$(
    jq -c --argjson source_pr "$ACTUAL_PR" '
      if (.candidates | type) == "array" then
        .candidates[] | select(.source_pr == $source_pr)
      elif .source_pr == $source_pr then
        .
      else
        empty
      end
    ' "$CANDIDATE_JSON" | head -n 1
  )"
  if [ -z "$CANDIDATE_MATCH" ]; then
    echo "ERROR: result source_pr ${ACTUAL_PR} does not match any discovered candidate" >&2
    exit 1
  fi

  EXPECTED_BRANCH="$(jq -r '.branch_name' <<<"$CANDIDATE_MATCH")"
  ACTUAL_BRANCH="$(jq -r '.branch_name' "$RESULT_JSON")"
  if [ "$EXPECTED_BRANCH" != "$ACTUAL_BRANCH" ]; then
    echo "ERROR: result branch_name ${ACTUAL_BRANCH} does not match candidate ${EXPECTED_BRANCH}" >&2
    exit 1
  fi

  if ! jq -n -e \
    --slurpfile result "$RESULT_JSON" \
    --slurpfile candidate "$CANDIDATE_JSON" \
    '
      ($result[0]) as $result_root |
      ($candidate[0]) as $candidate_root |
      (if ($candidate_root.candidates | type) == "array" then
        [$candidate_root.candidates[].source_pr]
      else
        [$candidate_root.source_pr]
      end) as $candidate_prs |
      ($candidate_prs | index($result_root.source_pr)) as $top_level_index |
      ([$result_root.triage_decisions[]?.source_pr]) as $decision_prs |
      all($result_root.triage_decisions[]?; (. as $decision |
        (($candidate_prs | index($decision.source_pr)) != null) and
        (($candidate_prs | index($decision.source_pr)) < $top_level_index) and
        ($decision.status == "already_present" or $decision.status == "skipped")
      )) and
      (($decision_prs | length) == ($decision_prs | unique | length)) and
      (($decision_prs | index($result_root.source_pr)) == null)
    ' >/dev/null; then
    echo "ERROR: triage_decisions must be unique, quiet decisions for discovered candidates before the top-level result" >&2
    exit 1
  fi

  EXPECTED_PR_MARKER="$(jq -r '.body_markers.upstream_pr // "Upstream-Zebra-PR: \(.source_pr)"' <<<"$CANDIDATE_MATCH")"
  EXPECTED_MERGE_MARKER="$(jq -r '.body_markers.upstream_merge // (if .source_merge_commit then "Upstream-Zebra-Merge: \(.source_merge_commit)" else "" end)' <<<"$CANDIDATE_MATCH")"
else
  EXPECTED_PR_MARKER="Upstream-Zebra-PR: $(jq -r '.source_pr' "$RESULT_JSON")"
  EXPECTED_MERGE_MARKER=""
fi

TITLE="$(jq -r '.pr_title' "$RESULT_JSON")"
BODY="$(jq -r '.pr_body' "$RESULT_JSON")"
BRANCH="$(jq -r '.branch_name' "$RESULT_JSON")"
STATUS="$(jq -r '.status' "$RESULT_JSON")"

case "$TITLE" in
  *$'\n'*|*$'\r'*)
    echo "ERROR: PR title must be a single line" >&2
    exit 1
    ;;
esac

case "$BRANCH" in
  *$'\n'*|*$'\r'*)
    echo "ERROR: branch name must be a single line" >&2
    exit 1
    ;;
esac

if ! printf '%s\n' "$TITLE" | grep -Eq '^(feat|fix|perf|refactor|build|chore|docs|test|ci|style|revert|release)(\([A-Za-z0-9_.-]+\))?: .+'; then
  echo "ERROR: PR title is not a conventional commit title: $TITLE" >&2
  exit 1
fi

require_validation_passed() {
  local pattern="$1"
  local description="$2"

  if ! jq -e --arg pattern "$pattern" '
    any(.validation[]?; (.command | test($pattern)) and .status == "passed")
  ' "$RESULT_JSON" >/dev/null; then
    echo "ERROR: validation must include passing ${description}" >&2
    exit 1
  fi
}

if [ "$STATUS" = "applied" ]; then
  require_validation_passed '^cargo fmt --all -- --check$' 'cargo fmt --all -- --check'
  require_validation_passed '^git diff --check$' 'git diff --check'
fi

if ! printf '%s\n' "$BODY" | grep -Eq '^(#{2,6}[[:space:]]+AI Disclosure|\*\*AI Disclosure\*\*|AI Disclosure):?[[:space:]]*$'; then
  echo "ERROR: PR body must include an AI Disclosure section" >&2
  exit 1
fi

if ! printf '%s\n' "$BODY" | grep -q 'Codex was used'; then
  echo "ERROR: PR body must disclose Codex usage" >&2
  exit 1
fi

if ! printf '%s\n' "$BODY" | grep -Fqx "$EXPECTED_PR_MARKER"; then
  echo "ERROR: PR body must include ${EXPECTED_PR_MARKER}" >&2
  exit 1
fi

if [ -n "$EXPECTED_MERGE_MARKER" ] && ! printf '%s\n' "$BODY" | grep -Fqx "$EXPECTED_MERGE_MARKER"; then
  echo "ERROR: PR body must include ${EXPECTED_MERGE_MARKER}" >&2
  exit 1
fi

if printf '%s\n' "$BODY" | grep -Eq '(^|[^A-Za-z0-9_])#[0-9]+'; then
  echo "ERROR: PR body contains a bare issue/PR autolink" >&2
  exit 1
fi

if printf '%s\n' "$BODY" | grep -Eq '[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+#[0-9]+'; then
  echo "ERROR: PR body contains an owner/repo issue/PR autolink" >&2
  exit 1
fi

if printf '%s\n' "$BODY" | grep -Eq 'https://github\.com/[^[:space:]]+/(pull|issues)/[0-9]+'; then
  echo "ERROR: PR body contains a GitHub PR/issue URL" >&2
  exit 1
fi

if [ "${UPSTREAM_SYNC_SKIP_PROTECTED_CHECK:-false}" != "true" ] && git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  PROTECTED_PATTERN='^(\.github/workflows/upstream-sync\.yml|\.github/upstream-sync/|\.github/scripts/upstream-sync-)'
  CHANGED_PROTECTED="$(
    {
      git diff --name-only
      git diff --cached --name-only
      git ls-files --others --exclude-standard
    } | sort -u | grep -E "$PROTECTED_PATTERN" || true
  )"
  if [ -n "$CHANGED_PROTECTED" ]; then
    echo "ERROR: generated patch modified protected upstream-sync automation files:" >&2
    printf '%s\n' "$CHANGED_PROTECTED" >&2
    exit 1
  fi
fi

echo "OK: upstream sync result is valid"
