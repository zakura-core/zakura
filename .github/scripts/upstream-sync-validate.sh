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
  (.status | IN("applied", "already_present", "needs_human", "failed")) and
  (.source_pr | type == "number") and
  (.confidence_percent | type == "number" and . >= 0 and . <= 100) and
  (.recommendation | type == "string" and length > 0) and
  (.branch_name | type == "string" and test("^upstream-sync/pr-[0-9]+$")) and
  (.pr_title | type == "string" and length > 0) and
  (.pr_body | type == "string" and length > 0) and
  (.files_changed | type == "array") and
  (.validation | type == "array") and
  (.risks | type == "array") and
  (.follow_up | type == "array")
' "$RESULT_JSON" >/dev/null

if [ -n "$CANDIDATE_JSON" ]; then
  if [ ! -f "$CANDIDATE_JSON" ]; then
    echo "ERROR: candidate JSON not found: $CANDIDATE_JSON" >&2
    exit 1
  fi
  EXPECTED_PR="$(jq -r '.source_pr' "$CANDIDATE_JSON")"
  ACTUAL_PR="$(jq -r '.source_pr' "$RESULT_JSON")"
  if [ "$EXPECTED_PR" != "$ACTUAL_PR" ]; then
    echo "ERROR: result source_pr ${ACTUAL_PR} does not match candidate ${EXPECTED_PR}" >&2
    exit 1
  fi

  EXPECTED_BRANCH="$(jq -r '.branch_name' "$CANDIDATE_JSON")"
  ACTUAL_BRANCH="$(jq -r '.branch_name' "$RESULT_JSON")"
  if [ "$EXPECTED_BRANCH" != "$ACTUAL_BRANCH" ]; then
    echo "ERROR: result branch_name ${ACTUAL_BRANCH} does not match candidate ${EXPECTED_BRANCH}" >&2
    exit 1
  fi

  EXPECTED_PR_MARKER="$(jq -r '.body_markers.upstream_pr // "Upstream-Zebra-PR: \(.source_pr)"' "$CANDIDATE_JSON")"
  EXPECTED_MERGE_MARKER="$(jq -r '.body_markers.upstream_merge // (if .source_merge_commit then "Upstream-Zebra-Merge: \(.source_merge_commit)" else "" end)' "$CANDIDATE_JSON")"
else
  EXPECTED_PR_MARKER="Upstream-Zebra-PR: $(jq -r '.source_pr' "$RESULT_JSON")"
  EXPECTED_MERGE_MARKER=""
fi

TITLE="$(jq -r '.pr_title' "$RESULT_JSON")"
BODY="$(jq -r '.pr_body' "$RESULT_JSON")"
BRANCH="$(jq -r '.branch_name' "$RESULT_JSON")"

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

if ! printf '%s\n' "$BODY" | grep -q '### AI Disclosure'; then
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
