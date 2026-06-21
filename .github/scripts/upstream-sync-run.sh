#!/usr/bin/env bash
# Helper operations for the manual upstream sync workflow.

set -euo pipefail

COMMAND="${1:?Usage: upstream-sync-run.sh <prepare|collect-patch|apply-patch|delete-stale-branch|write-pr-body>}"
WORK_DIR="${UPSTREAM_SYNC_WORK_DIR:-.github/upstream-sync/work}"
CANDIDATE_JSON="${UPSTREAM_SYNC_CANDIDATE_JSON:-${WORK_DIR}/candidate.json}"
RESULT_JSON="${UPSTREAM_SYNC_RESULT_JSON:-${WORK_DIR}/result.json}"
PATCH_FILE="${UPSTREAM_SYNC_PATCH_FILE:-${WORK_DIR}/upstream-sync.patch}"
PR_BODY_FILE="${UPSTREAM_SYNC_PR_BODY_FILE:-${WORK_DIR}/pr-body.md}"

require_file() {
  local file="$1"
  if [ ! -f "$file" ]; then
    echo "ERROR: missing required file: $file" >&2
    exit 1
  fi
}

validate_pr_body_autolinks() {
  local file="$1"
  if grep -Eq '(^|[^A-Za-z0-9_])#[0-9]+' "$file"; then
    echo "ERROR: final PR body contains a bare issue/PR autolink" >&2
    exit 1
  fi
  if grep -Eq '[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+#[0-9]+' "$file"; then
    echo "ERROR: final PR body contains an owner/repo issue/PR autolink" >&2
    exit 1
  fi
  if grep -Eq 'https://github\.com/[^[:space:]]+/(pull|issues)/[0-9]+' "$file"; then
    echo "ERROR: final PR body contains a GitHub PR/issue URL" >&2
    exit 1
  fi
}

case "$COMMAND" in
  prepare)
    require_file "$CANDIDATE_JSON"
    require_file "${WORK_DIR}/source.patch"
    require_file "${WORK_DIR}/source.diff"
    mkdir -p "$WORK_DIR"
    jq -r '.source_files[]' "$CANDIDATE_JSON" > "${WORK_DIR}/source-files.txt"
    {
      echo "Prepared upstream sync context"
      echo "Candidate: upstream PR $(jq -r '.source_pr' "$CANDIDATE_JSON")"
      echo "Branch: $(jq -r '.branch_name' "$CANDIDATE_JSON")"
    } | tee "${WORK_DIR}/prepare.log"
    ;;

  collect-patch)
    require_file "$RESULT_JSON"
    mkdir -p "$WORK_DIR"
    STATUS="$(jq -r '.status' "$RESULT_JSON")"
    git reset --mixed >/dev/null
    while IFS= read -r -d '' path; do
      git add -N -- "$path"
    done < <(git ls-files --others --exclude-standard -z)
    if [ "$STATUS" = "applied" ]; then
      if git diff --quiet; then
        echo "ERROR: result status is applied, but Codex produced no patch" >&2
        exit 1
      fi
      git diff --binary > "$PATCH_FILE"
      git diff --stat > "${WORK_DIR}/upstream-sync.diffstat"
      git diff --name-only > "${WORK_DIR}/changed-files.txt"
    else
      : > "$PATCH_FILE"
      : > "${WORK_DIR}/upstream-sync.diffstat"
      : > "${WORK_DIR}/changed-files.txt"
    fi
    echo "status=${STATUS}" >> "${GITHUB_OUTPUT:-/dev/null}"
    echo "Collected Codex patch with status: ${STATUS}"
    ;;

  apply-patch)
    require_file "$PATCH_FILE"
    if [ ! -s "$PATCH_FILE" ]; then
      echo "ERROR: patch file is empty; nothing to apply" >&2
      exit 1
    fi
    git apply --3way --index "$PATCH_FILE"
    ;;

  delete-stale-branch)
    require_file "$RESULT_JSON"
    BRANCH="$(jq -r '.branch_name' "$RESULT_JSON")"
    case "$BRANCH" in
      upstream-sync/pr-[0-9]*) ;;
      *)
        echo "ERROR: refusing to delete non upstream-sync PR branch: $BRANCH" >&2
        exit 1
        ;;
    esac

    if git ls-remote --exit-code --heads origin "$BRANCH" >/dev/null 2>&1; then
      OPEN_PRS="$(
        gh pr list \
          --repo "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must be set}" \
          --state open \
          --head "$BRANCH" \
          --json number,headRefName \
          | jq --arg branch "$BRANCH" '[.[] | select(.headRefName == $branch)] | length'
      )"
      if [ "$OPEN_PRS" != "0" ]; then
        echo "ERROR: refusing to delete $BRANCH because it has an open PR" >&2
        exit 1
      fi

      git push origin --delete "$BRANCH"
      echo "Deleted stale upstream sync branch: $BRANCH"
    else
      echo "No stale upstream sync branch to delete: $BRANCH"
    fi
    ;;

  write-pr-body)
    require_file "$RESULT_JSON"
    BODY="$(jq -r '.pr_body' "$RESULT_JSON")"
    CONFIDENCE="$(jq -r '.confidence_percent' "$RESULT_JSON")"
    RECOMMENDATION="$(
      jq -r '.recommendation' "$RESULT_JSON" \
        | tr '\n' ' ' \
        | sed -E 's/[[:space:]]+/ /g; s/^ //; s/ $//; s/([.!?]) .*/\1/'
    )"
    if [ -z "$RECOMMENDATION" ]; then
      RECOMMENDATION="Review the generated patch and validation evidence before merging."
    fi
    if [ "${#RECOMMENDATION}" -gt 180 ]; then
      RECOMMENDATION="${RECOMMENDATION:0:177}..."
    fi

    if printf '%s\n' "$BODY" | head -n 1 | grep -Eq '^AI Confidence: [0-9]+% - .+'; then
      printf '%s\n' "$BODY" > "$PR_BODY_FILE"
    else
      {
        printf 'AI Confidence: %s%% - %s\n\n' "$CONFIDENCE" "$RECOMMENDATION"
        printf '%s\n' "$BODY"
      } > "$PR_BODY_FILE"
    fi
    validate_pr_body_autolinks "$PR_BODY_FILE"
    TITLE="$(jq -r '.pr_title' "$RESULT_JSON")"
    BRANCH="$(jq -r '.branch_name' "$RESULT_JSON")"
    {
      printf 'title=%s\n' "$TITLE"
      printf 'branch=%s\n' "$BRANCH"
    } >> "${GITHUB_OUTPUT:?GITHUB_OUTPUT must be set}"
    ;;

  *)
    echo "ERROR: unknown command: $COMMAND" >&2
    exit 1
    ;;
esac
