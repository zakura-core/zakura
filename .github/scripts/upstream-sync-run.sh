#!/usr/bin/env bash
# Helper operations for the manual upstream sync workflow.

set -euo pipefail

COMMAND="${1:?Usage: upstream-sync-run.sh <prepare|collect-patch|apply-patch|write-pr-body>}"
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

  write-pr-body)
    require_file "$RESULT_JSON"
    jq -r '.pr_body' "$RESULT_JSON" > "$PR_BODY_FILE"
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
