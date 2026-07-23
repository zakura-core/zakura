#!/usr/bin/env bash
# release-t0.sh — idempotent release-day (T-0) orchestrator.
#
# Turns the release-day sequence (merge or push -> dispatch `Create release`
# -> watch to the approval gate -> verify the published release) into one
# resumable command. Every step verifies its preconditions before acting and
# its postconditions after; already-satisfied steps skip cleanly, so the
# script is safe to re-run from any interruption. Born from the v1.0.3 retro,
# where two pasted `&&` chains broke silently mid-sequence (a merge of an
# already-closed PR, and a merge method the `main` ruleset forbids).
#
# The preflight actively detects an in-flight REGULAR release (open release
# PRs, `release/v*`/`bump-v*` branches, running `Create release` dispatches):
# under embargo, a hotfix and a regular release cannot see each other coming,
# and at v1.0.3 both claimed the same version at the same hour. See
# docs/security-hotfix-release.md (branch namespace rule, caveat 12).
#
# All release state is read from the REMOTE via `gh` — never from local refs:
# operator clones accumulate stale rehearsal tags that shadow real ones.
#
# The script never approves the `release` environment (the human
# stop-and-check point), never publishes crates, and never signs.
#
# Usage:
#   scripts/release-t0.sh <subcommand> --tag vX.Y.Z [options]
#
# Subcommands:
#   preflight      run every read-only check and print the execution plan
#   publish        preflight, then merge-or-push -> dispatch -> watch -> verify
#   promote        clear pre-release + set latest on a signed stable release
#   forward-merge  post-release: merge hotfix/vX.Y.Z into main (branch base)
#   status         read-only view of each step's state for --tag
#
# Options:
#   --tag vX.Y.Z       release tag (required)
#   --mode main|branch dispatch `Create release` from `main` (regular release
#                      or main-base hotfix) or from `hotfix/<tag>`
#   --hotfix           enforce the hotfix branch namespace rule and make
#                      in-flight-regular-release findings hard aborts
#   --pr N             release PR to merge (mode main) / forward-merge PR
#   --head-sha SHA     the exact commit intended to ship (required for
#                      publish; pins merge, push, and dispatch verification)
#   --source-first     Mode B: pass source_first_release=true to the workflow
#   --allow-bootstrap-release-state  pass the emergency workflow input through
#   --repo OWNER/NAME  default zakura-core/zakura (or $REPOSITORY); any other
#                      value needs --allow-nondefault-repo (staging drills)
#   --allow-nondefault-repo  explicit staging-repo override
#   --run-id N         adopt an existing `Create release` run (resume)
#   --dry-run          run all read-only checks; print mutations instead of
#                      executing them (the drill vehicle)
#   --no-wait          exit after dispatch verification instead of watching
#   --yes              non-interactive: soft prompts become warnings
#   --allow-concurrent-release  downgrade the in-flight-regular-release abort
#   --allow-squash     forward-merge only: accept a squash despite orphaning
#                      the tagged commit (breaks BASE_TAG ancestry checks)
set -euo pipefail

CANONICAL_REPO="zakura-core/zakura"
REPO="${REPOSITORY:-$CANONICAL_REPO}"
SUBCOMMAND=""
TAG=""
MODE=""
HOTFIX=0
PR_NUM=""
HEAD_SHA=""
SOURCE_FIRST=0
ALLOW_BOOTSTRAP=0
ALLOW_NONDEFAULT_REPO=0
RUN_ID_ARG=""
DRY_RUN=0
NO_WAIT=0
ASSUME_YES=0
ALLOW_CONCURRENT=0
ALLOW_SQUASH=0

repo_root="$(cd "$(dirname "$0")/.." && pwd)"

usage() { sed -n '2,60p' "$0" | sed 's/^# \{0,1\}//'; }

# --- output helpers ---------------------------------------------------------

info() { printf 'INFO    %s\n' "$*"; }
ok()   { printf 'OK      %s\n' "$*"; }
warn() { printf 'WARN    %s\n' "$*" >&2; }
skip() { printf 'SKIP    %s (already satisfied: %s)\n' "$1" "$2"; }

# die <step> <checked> <observed> <expected> <recover...>
die() {
  local step="$1" checked="$2" observed="$3" expected="$4"
  shift 4
  {
    printf 'FAILED  %s\n' "$step"
    printf '  checked:  %s\n' "$checked"
    printf '  observed: %s\n' "$observed"
    printf '  expected: %s\n' "$expected"
    printf '  recover:  %s\n' "$1"
    shift || true
    local line
    for line in "$@"; do printf '            %s\n' "$line"; done
    printf '            then re-run this script — completed steps skip cleanly.\n'
  } >&2
  exit 1
}

# confirm <question>: soft interactive gate. --yes turns it into a warning.
confirm() {
  local question="$1" reply
  if [ "$ASSUME_YES" = 1 ]; then
    warn "assumed yes (--yes): $question"
    return 0
  fi
  if [ ! -t 0 ]; then
    die "confirm" "interactive prompt" "stdin is not a TTY" "a terminal" \
      "re-run interactively, or pass --yes to accept soft prompts"
  fi
  printf 'CONFIRM %s [y/N] ' "$question"
  read -r reply
  case "$reply" in
    y | Y | yes | YES) return 0 ;;
    *)
      die "confirm" "operator confirmation" "declined" "confirmed" \
        "resolve the concern above, or re-run with --yes if it is acceptable"
      ;;
  esac
}

# act <description> <cmd...>: run a mutation, or print it under --dry-run.
act() {
  local description="$1"
  shift
  if [ "$DRY_RUN" = 1 ]; then
    printf 'DRY-RUN %s — would run: %s\n' "$description" "$*"
    return 0
  fi
  info "$description"
  "$@"
}

# --- gh helpers -------------------------------------------------------------

# gh_api_optional <path...>: prints the response; returns 1 on HTTP 404,
# dies on any other failure so network errors never read as "not found".
gh_api_optional() {
  local out
  if out=$(gh api "$@" 2>&1); then
    printf '%s' "$out"
    return 0
  fi
  if grep -q 'HTTP 404' <<<"$out"; then
    return 1
  fi
  die "gh api" "gh api $*" "$out" "success or HTTP 404" \
    "check network and gh auth, then re-run"
}

# resolve_tag_commit <tag>: prints the commit SHA the remote tag points at
# (dereferencing annotated tags); returns 1 if the tag does not exist.
resolve_tag_commit() {
  local tag="$1" ref_json obj_type obj_sha
  if ! ref_json=$(gh_api_optional "repos/${REPO}/git/ref/tags/${tag}"); then
    return 1
  fi
  obj_type=$(jq -r '.object.type' <<<"$ref_json")
  obj_sha=$(jq -r '.object.sha' <<<"$ref_json")
  if [ "$obj_type" = "tag" ]; then
    gh api "repos/${REPO}/git/tags/${obj_sha}" --jq '.object.sha'
  else
    printf '%s' "$obj_sha"
  fi
}

# ancestor_of_main <sha>: succeeds when main contains <sha>.
ancestor_of_main() {
  local sha="$1" cmp_status
  cmp_status=$(gh api "repos/${REPO}/compare/${sha}...main" --jq '.status')
  [ "$cmp_status" = "identical" ] || [ "$cmp_status" = "ahead" ]
}

# --- global preconditions ---------------------------------------------------

check_globals() {
  local cmd
  for cmd in gh git jq; do
    command -v "$cmd" >/dev/null \
      || die "tools" "command -v $cmd" "missing" "installed" "install $cmd"
  done
  gh auth status >/dev/null 2>&1 \
    || die "tools" "gh auth status" "not authenticated" "authenticated" \
        "run: gh auth login"
  gh pr merge --help 2>/dev/null | grep -q -- '--match-head-commit' \
    || die "tools" "gh pr merge --help" "no --match-head-commit flag" \
        "gh with --match-head-commit support" "upgrade gh (>= 2.30)"

  if [ "$REPO" != "$CANONICAL_REPO" ] && [ "$ALLOW_NONDEFAULT_REPO" != 1 ]; then
    die "repo pin" "--repo / \$REPOSITORY" "$REPO" "$CANONICAL_REPO" \
      "public releases run against ${CANONICAL_REPO} only;" \
      "for a staging-repo drill, pass --allow-nondefault-repo explicitly"
  fi

  [ -n "$TAG" ] || die "arguments" "--tag" "missing" "vX.Y.Z" "pass --tag vX.Y.Z"
  [[ "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$ ]] \
    || die "arguments" "--tag shape" "$TAG" "v<major>.<minor>.<patch>[-pre]" \
        "pass a tag like v1.0.4 or v1.0.4-rc0"

  # Never trust local tags: warn when one shadows or diverges from the remote.
  local local_commit remote_commit=""
  if local_commit=$(git -C "$repo_root" rev-parse -q --verify \
      "refs/tags/${TAG}^{commit}" 2>/dev/null); then
    if remote_commit=$(resolve_tag_commit "$TAG"); then
      if [ "$local_commit" != "$remote_commit" ]; then
        warn "local tag ${TAG} (${local_commit:0:9}) diverges from the remote tag (${remote_commit:0:9})" \
          "— likely a stale rehearsal tag; this script only trusts the remote. Consider: git tag -d ${TAG}"
      fi
    else
      warn "local tag ${TAG} (${local_commit:0:9}) exists but the remote has no such tag" \
        "— likely a stale rehearsal tag. Consider: git tag -d ${TAG}"
    fi
  fi
}

is_stable_tag() { [[ "$TAG" != *-* ]]; }

# --- preflight: collision detection and standing checks ----------------------

# Populated by preflight for later steps.
TAG_COMMIT=""            # remote commit for --tag, when the tag exists
RELEASE_STATE="absent"   # absent | draft | published
RELEASE_TARGET=""
ADOPTED_RUN_ID=""

release_pr_verdict() {
  # <what> <detail>: hard abort under --hotfix, soft prompt otherwise.
  local what="$1" detail="$2"
  if [ "$HOTFIX" = 1 ] && [ "$ALLOW_CONCURRENT" != 1 ]; then
    die "preflight: collision" "$what" "$detail" "no in-flight regular release" \
      "coordinate with its author (they may be about to claim ${TAG}), or" \
      "pass --allow-concurrent-release after confirming the trains cannot collide"
  fi
  warn "in-flight release signal — $what: $detail"
  confirm "Proceed despite the release signal above?"
}

preflight_release_prs() {
  # P1: open PRs labeled A-release that are not this release's own PR/branch.
  local prs
  prs=$(gh pr list --repo "$REPO" --label A-release --state open \
    --json number,title,headRefName,author,url \
    | jq --arg ours "hotfix/${TAG}" --arg pr "${PR_NUM:-none}" \
        '[ .[] | select(.headRefName != $ours and ((.number | tostring) != $pr)) ]')
  if [ "$(jq 'length' <<<"$prs")" -gt 0 ]; then
    release_pr_verdict "open A-release PRs" \
      "$(jq -r '.[] | "PR #\(.number) \"\(.title)\" from \(.headRefName) by \(.author.login) — \(.url)"' <<<"$prs" | paste -sd '; ' -)"
  else
    ok "no open A-release PRs from other release trains"
  fi

  # P2: same check by title, for release PRs missing the label.
  prs=$(gh pr list --repo "$REPO" --state open --search 'Release Zakura in:title' \
    --json number,title,headRefName,url \
    | jq --arg ours "hotfix/${TAG}" --arg pr "${PR_NUM:-none}" \
        '[ .[] | select(.headRefName != $ours and ((.number | tostring) != $pr)) ]')
  if [ "$(jq 'length' <<<"$prs")" -gt 0 ]; then
    release_pr_verdict "open release-titled PRs (unlabeled)" \
      "$(jq -r '.[] | "PR #\(.number) from \(.headRefName) — \(.url)"' <<<"$prs" | paste -sd '; ' -)"
  else
    ok "no unlabeled release-titled PRs"
  fi
}

preflight_release_branches() {
  # P3/P4: regular-namespace branches on the remote, with staleness triage.
  local remote_refs line sha ref branch age_days pr_count
  remote_refs=$(git -C "$repo_root" ls-remote origin \
    'refs/heads/release/*' 'refs/heads/bump-v*' 2>/dev/null || true)
  if [ -z "$remote_refs" ]; then
    ok "no release/* or bump-v* branches on origin"
    return 0
  fi
  while IFS= read -r line; do
    sha="${line%%$'\t'*}"
    ref="${line#*$'\t'}"
    branch="${ref#refs/heads/}"
    case "$branch" in
      "release/${TAG}" | "release/${TAG#v}" | "bump-${TAG}")
        die "preflight: collision" "git ls-remote origin '${ref}'" \
          "branch ${branch} exists at ${sha:0:9}" "no branch claiming ${TAG}" \
          "another release train has claimed this exact version;" \
          "coordinate with its owner before anything is pushed or merged"
        ;;
    esac
    pr_count=$(gh pr list --repo "$REPO" --head "$branch" --state open \
      --json number --jq 'length')
    age_days=$(gh api "repos/${REPO}/commits/${sha}" \
      --jq '((now - (.commit.committer.date | fromdateiso8601)) / 86400 | floor)' \
      2>/dev/null || printf 'unknown')
    if [ "$pr_count" -gt 0 ] || { [ "$age_days" != "unknown" ] && [ "$age_days" -lt 14 ]; }; then
      release_pr_verdict "active regular-namespace branch" \
        "${branch} (last commit ${age_days} days ago, ${pr_count} open PR(s))"
    else
      info "stale regular-namespace branch: ${branch} (last commit ${age_days} days ago) — ignoring"
    fi
  done <<<"$remote_refs"
}

preflight_tag_and_release() {
  # P5: the target tag.
  if TAG_COMMIT=$(resolve_tag_commit "$TAG"); then
    if [ -n "$HEAD_SHA" ] && [ "$TAG_COMMIT" != "$HEAD_SHA" ]; then
      die "preflight: tag" "repos/${REPO}/git/ref/tags/${TAG}" \
        "tag exists at ${TAG_COMMIT}" "absent, or at --head-sha ${HEAD_SHA}" \
        "tags are immutable and never reused: if that commit is not yours," \
        "start a new patch version (checklist: Release Failures)"
    fi
    info "tag ${TAG} already exists at ${TAG_COMMIT:0:9} — resume mode"
  else
    ok "tag ${TAG} does not exist yet"
  fi

  # P6: an existing GitHub release for the tag.
  local rel
  if rel=$(gh_api_optional "repos/${REPO}/releases/tags/${TAG}"); then
    RELEASE_STATE=$(jq -r 'if .draft then "draft" else "published" end' <<<"$rel")
    RELEASE_TARGET=$(jq -r '.target_commitish' <<<"$rel")
    if [ "$RELEASE_STATE" = "published" ] && [ -z "$TAG_COMMIT" ]; then
      die "preflight: release" "repos/${REPO}/releases/tags/${TAG}" \
        "published release exists but no tag" "consistent tag+release state" \
        "inspect https://github.com/${REPO}/releases — this state is unexpected"
    fi
    info "release for ${TAG}: ${RELEASE_STATE} (target ${RELEASE_TARGET})"
    if [ "$RELEASE_STATE" = "draft" ]; then
      info "an unpublished draft is reused by Create release — leaving it alone"
    fi
  else
    ok "no GitHub release for ${TAG} yet"
  fi

  # P7: active Create release runs.
  local runs run_count our_run
  runs=$(gh run list --repo "$REPO" --workflow=create-release.yml \
    --json databaseId,status,headBranch,headSha,url \
    --jq '[ .[] | select(.status == "queued" or .status == "in_progress" or .status == "waiting") ]')
  run_count=$(jq 'length' <<<"$runs")
  if [ "$run_count" -gt 0 ]; then
    our_run=$(jq --arg sha "${HEAD_SHA:-none}" '[ .[] | select(.headSha == $sha) ]' <<<"$runs")
    if [ "$(jq 'length' <<<"$our_run")" -gt 0 ]; then
      ADOPTED_RUN_ID=$(jq -r '.[0].databaseId' <<<"$our_run")
      info "adopting active Create release run ${ADOPTED_RUN_ID} on our commit"
      warn "run inputs (the tag) are not readable via the API — verified by branch+commit only"
    else
      die "preflight: collision" "gh run list --workflow=create-release.yml" \
        "$(jq -r '.[] | "run \(.databaseId) [\(.status)] on \(.headBranch)@\(.headSha[0:9]) — \(.url)"' <<<"$runs" | paste -sd '; ' -)" \
        "no active Create release run on another commit" \
        "a waiting run may be a regular release parked at the approval gate —" \
        "exactly the collision that matters; coordinate before dispatching anything"
    fi
  else
    ok "no active Create release runs"
  fi
}

preflight_merge_methods() {
  # P10: what the main ruleset allows (verified live instead of assumed).
  local methods
  methods=$(gh api "repos/${REPO}/rules/branches/main" \
    --jq '[ .[] | select(.type == "pull_request") ][0].parameters.allowed_merge_methods // empty' \
    2>/dev/null | jq -c '.' 2>/dev/null || printf '')
  if [ -z "$methods" ] || [ "$methods" = "null" ]; then
    warn "could not read allowed merge methods for main (no pull_request rule?) — assuming all allowed"
    return 0
  fi
  info "main ruleset allowed merge methods: ${methods}"
  if [ "$SUBCOMMAND" = "publish" ] && [ "$MODE" = "main" ]; then
    grep -q '"squash"' <<<"$methods" \
      || die "preflight: merge method" "repos/${REPO}/rules/branches/main" \
          "$methods" "squash allowed" \
          "the T-0 merge into main squash-merges; adjust the ruleset"
  fi
}

preflight_standing() {
  # P11: cheap re-verification of the standing preconditions (warn-only).
  local policies psr
  if [ "$MODE" = "branch" ]; then
    policies=$(gh api "repos/${REPO}/environments/release/deployment-branch-policies" \
      --jq '[.branch_policies[].name]' 2>/dev/null || printf '[]')
    grep -q 'hotfix/v\*' <<<"$policies" \
      || warn "release environment deployment branches lack 'hotfix/v*': ${policies} — the publish job will not start from the hotfix branch"
  fi
  psr=$(gh api "repos/${REPO}/environments/release" \
    --jq '[.protection_rules[]? | select(.type == "required_reviewers") | .prevent_self_review] | first' \
    2>/dev/null || printf 'unknown')
  if [ "$psr" = "true" ]; then
    warn "release environment has 'Prevent self-review' enabled — a solo release will deadlock at the approval gate"
  fi
}

preflight_expected_head() {
  # P9: the pinned head commit is where we expect it.
  [ -n "$HEAD_SHA" ] || die "arguments" "--head-sha" "missing" "the exact commit to ship" \
    "pass --head-sha <sha> (pin exactly what ships)"
  if [ "$MODE" = "branch" ]; then
    local remote_head
    remote_head=$(git -C "$repo_root" ls-remote origin "refs/heads/hotfix/${TAG}" | cut -f1)
    if [ -n "$remote_head" ] && [ "$remote_head" != "$HEAD_SHA" ]; then
      die "preflight: head" "git ls-remote origin refs/heads/hotfix/${TAG}" \
        "$remote_head" "$HEAD_SHA (or absent, to be pushed)" \
        "the remote hotfix branch differs from what you intend to ship;" \
        "reconcile locally — never force-push a hotfix branch (the ruleset blocks it)"
    fi
    if [ -z "$remote_head" ]; then
      git -C "$repo_root" cat-file -e "${HEAD_SHA}^{commit}" 2>/dev/null \
        || die "preflight: head" "git cat-file -e ${HEAD_SHA}" "object missing locally" \
            "the commit available to push" "fetch or build the release commit first"
    fi
  fi
}

run_preflight() {
  check_globals
  if [ "$SUBCOMMAND" = "publish" ] || [ "$SUBCOMMAND" = "preflight" ]; then
    [ -n "$MODE" ] || die "arguments" "--mode" "missing" "main or branch" \
      "pass --mode main (PR merge into main) or --mode branch (hotfix branch push)"
    preflight_expected_head
  fi
  preflight_tag_and_release
  preflight_release_prs
  preflight_release_branches
  preflight_merge_methods
  preflight_standing
}

# --- publish steps -----------------------------------------------------------

MERGE_SHA=""     # the commit Create release must build

step_merge_or_push() {
  local step="merge-or-push"
  if [ -n "$TAG_COMMIT" ]; then
    MERGE_SHA="$TAG_COMMIT"
    skip "$step" "tag ${TAG} already exists"
    return 0
  fi

  if [ "$MODE" = "branch" ]; then
    local remote_head
    remote_head=$(git -C "$repo_root" ls-remote origin "refs/heads/hotfix/${TAG}" | cut -f1)
    if [ "$remote_head" = "$HEAD_SHA" ]; then
      MERGE_SHA="$HEAD_SHA"
      skip "$step" "hotfix/${TAG} already at ${HEAD_SHA:0:9}"
      return 0
    fi
    act "pushing ${HEAD_SHA:0:9} to hotfix/${TAG}" \
      git -C "$repo_root" push origin "${HEAD_SHA}:refs/heads/hotfix/${TAG}"
    [ "$DRY_RUN" = 1 ] && { MERGE_SHA="$HEAD_SHA"; return 0; }
    remote_head=$(git -C "$repo_root" ls-remote origin "refs/heads/hotfix/${TAG}" | cut -f1)
    [ "$remote_head" = "$HEAD_SHA" ] \
      || die "$step" "git ls-remote origin refs/heads/hotfix/${TAG}" \
          "${remote_head:-absent}" "$HEAD_SHA" \
          "the push did not land; inspect the branch on GitHub"
    MERGE_SHA="$HEAD_SHA"
    ok "hotfix/${TAG} is at ${HEAD_SHA:0:9}"
    return 0
  fi

  # Mode main: merge the release PR (or verify main is already there).
  if [ -z "$PR_NUM" ]; then
    local main_head
    main_head=$(gh api "repos/${REPO}/branches/main" --jq '.commit.sha')
    [ "$main_head" = "$HEAD_SHA" ] \
      || die "$step" "repos/${REPO}/branches/main" "$main_head" "$HEAD_SHA" \
          "without --pr, main must already be at --head-sha;" \
          "pass --pr <N> to merge the release PR, or re-pin --head-sha after review"
    MERGE_SHA="$HEAD_SHA"
    ok "main is already at ${HEAD_SHA:0:9}"
    return 0
  fi

  local pr_json state head_ref head_oid base labels
  pr_json=$(gh pr view "$PR_NUM" --repo "$REPO" \
    --json state,headRefName,headRefOid,baseRefName,labels,mergeCommit,url)
  state=$(jq -r '.state' <<<"$pr_json")
  head_ref=$(jq -r '.headRefName' <<<"$pr_json")
  head_oid=$(jq -r '.headRefOid' <<<"$pr_json")
  base=$(jq -r '.baseRefName' <<<"$pr_json")
  labels=$(jq -r '[.labels[].name] | join(",")' <<<"$pr_json")

  if [ "$state" = "MERGED" ]; then
    MERGE_SHA=$(jq -r '.mergeCommit.oid // empty' <<<"$pr_json")
    [ -n "$MERGE_SHA" ] \
      || die "$step" "gh pr view ${PR_NUM} (mergeCommit)" "merged, no merge commit" \
          "a merge commit" "identify the commit that merged PR #${PR_NUM} and dispatch from it manually"
    ancestor_of_main "$MERGE_SHA" \
      || die "$step" "repos/${REPO}/compare/${MERGE_SHA}...main" \
          "merge commit not on main" "merge commit reachable from main" \
          "PR #${PR_NUM} merged somewhere unexpected; inspect $(jq -r '.url' <<<"$pr_json")"
    skip "$step" "PR #${PR_NUM} already merged as ${MERGE_SHA:0:9}"
    return 0
  fi

  # This is the `gh pr merge 378` failure class: never merge a PR whose
  # identity does not check out.
  [ "$state" = "OPEN" ] \
    || die "$step" "gh pr view ${PR_NUM} (state)" "$state" "OPEN or MERGED" \
        "PR #${PR_NUM} is not the open release PR you meant;" \
        "find the right PR number and re-run"
  [ "$base" = "main" ] \
    || die "$step" "gh pr view ${PR_NUM} (base)" "$base" "main" \
        "this is not a PR into main; check the PR number"
  if [ "$HOTFIX" = 1 ] && [ "$head_ref" != "hotfix/${TAG}" ]; then
    die "$step" "gh pr view ${PR_NUM} (head branch)" "$head_ref" "hotfix/${TAG}" \
      "the branch namespace rule requires the T-0 PR branch to be hotfix/${TAG};" \
      "see docs/security-hotfix-release.md (branch namespace rule)"
  fi
  [ "$head_oid" = "$HEAD_SHA" ] \
    || die "$step" "gh pr view ${PR_NUM} (head commit)" "$head_oid" "$HEAD_SHA" \
        "the PR head moved after you pinned it; review the new commits," \
        "then re-run with the new --head-sha"
  grep -q 'A-release' <<<"$labels" \
    || warn "PR #${PR_NUM} lacks the A-release label (release-only checks did not run)"

  local merge_args=(gh pr merge "$PR_NUM" --repo "$REPO" --squash
    --match-head-commit "$HEAD_SHA")
  if ! act "squash-merging PR #${PR_NUM}" "${merge_args[@]}"; then
    warn "plain squash-merge refused (review requirements?)"
    confirm "Retry with --admin (bypasses review requirements)?"
    act "squash-merging PR #${PR_NUM} with --admin" "${merge_args[@]}" --admin
  fi
  [ "$DRY_RUN" = 1 ] && { MERGE_SHA="$HEAD_SHA"; return 0; }

  pr_json=$(gh pr view "$PR_NUM" --repo "$REPO" --json state,mergeCommit)
  [ "$(jq -r '.state' <<<"$pr_json")" = "MERGED" ] \
    || die "$step" "gh pr view ${PR_NUM} (state after merge)" \
        "$(jq -r '.state' <<<"$pr_json")" "MERGED" \
        "the merge did not complete; inspect the PR on GitHub"
  MERGE_SHA=$(jq -r '.mergeCommit.oid' <<<"$pr_json")

  local main_head
  main_head=$(gh api "repos/${REPO}/branches/main" --jq '.commit.sha')
  if [ "$main_head" != "$MERGE_SHA" ]; then
    die "$step" "repos/${REPO}/branches/main" "$main_head" "$MERGE_SHA" \
      "main advanced past the release merge — Create release builds the CURRENT" \
      "head of the dispatched ref, which now includes commits you did not verify." \
      "Inspect: gh api repos/${REPO}/compare/${MERGE_SHA}...${main_head}" \
      "Freeze the merge queue, review, and re-run with the new --head-sha"
  fi
  ok "PR #${PR_NUM} squash-merged as ${MERGE_SHA:0:9}; main is at it"
}

DISPATCHED_RUN_ID=""

step_dispatch() {
  local step="dispatch" dispatch_ref
  if [ "$MODE" = "branch" ]; then dispatch_ref="hotfix/${TAG}"; else dispatch_ref="main"; fi

  if [ -n "$TAG_COMMIT" ]; then
    skip "$step" "tag ${TAG} already exists"
    return 0
  fi
  if [ -n "$RUN_ID_ARG" ]; then
    DISPATCHED_RUN_ID="$RUN_ID_ARG"
    info "adopting run ${RUN_ID_ARG} (--run-id); inputs are not API-readable — verify the tag on its page"
    return 0
  fi
  if [ -n "$ADOPTED_RUN_ID" ]; then
    DISPATCHED_RUN_ID="$ADOPTED_RUN_ID"
    skip "$step" "active run ${ADOPTED_RUN_ID} already on ${MERGE_SHA:0:9}"
    return 0
  fi

  local prev_max_id
  prev_max_id=$(gh run list --repo "$REPO" --workflow=create-release.yml \
    --limit 1 --json databaseId --jq '.[0].databaseId // 0')

  local dispatch_args=(gh workflow run create-release.yml --repo "$REPO"
    --ref "$dispatch_ref" -f "release_tag=${TAG}")
  [ "$SOURCE_FIRST" = 1 ] && dispatch_args+=(-f source_first_release=true)
  [ "$ALLOW_BOOTSTRAP" = 1 ] && dispatch_args+=(-f allow_bootstrap_release_state=true)
  act "dispatching Create release for ${TAG} from ${dispatch_ref}" "${dispatch_args[@]}"
  [ "$DRY_RUN" = 1 ] && return 0

  # Correlate: the first run newer than prev_max_id on our exact commit.
  local waited=0 runs
  while [ "$waited" -lt 120 ]; do
    sleep 5; waited=$((waited + 5))
    runs=$(gh run list --repo "$REPO" --workflow=create-release.yml \
      --event workflow_dispatch --branch "$dispatch_ref" --limit 10 \
      --json databaseId,headSha,url \
      --jq "[ .[] | select(.databaseId > ${prev_max_id}) ]")
    [ "$(jq 'length' <<<"$runs")" -gt 0 ] && break
  done
  [ "$(jq 'length' <<<"$runs")" -gt 0 ] \
    || die "$step" "gh run list (new run after dispatch)" "none after ${waited}s" \
        "a new Create release run" \
        "the dispatch did not produce a run; check Actions is enabled and re-run"

  local run_sha
  DISPATCHED_RUN_ID=$(jq -r '.[0].databaseId' <<<"$runs")
  run_sha=$(jq -r '.[0].headSha' <<<"$runs")
  if [ "$run_sha" != "$MERGE_SHA" ]; then
    die "$step" "run ${DISPATCHED_RUN_ID} headSha" "$run_sha" "$MERGE_SHA" \
      "the ref advanced between merge and dispatch — the workflow builds the" \
      "dispatched ref's current head. Cancel it: gh run cancel ${DISPATCHED_RUN_ID} --repo ${REPO}" \
      "then investigate what moved ${dispatch_ref}"
  fi
  ok "Create release run ${DISPATCHED_RUN_ID} on ${run_sha:0:9}: $(jq -r '.[0].url' <<<"$runs")"
}

step_watch() {
  local step="watch"
  if [ -n "$TAG_COMMIT" ]; then
    skip "$step" "tag ${TAG} already exists"
    return 0
  fi
  if [ "$DRY_RUN" = 1 ]; then
    info "dry-run: would watch the run to the approval gate and beyond"
    return 0
  fi
  [ -n "$DISPATCHED_RUN_ID" ] || die "$step" "run id" "none" "a dispatched run" \
    "dispatch first (or pass --run-id)"

  local run_url
  run_url=$(gh run view "$DISPATCHED_RUN_ID" --repo "$REPO" --json url --jq '.url')
  if [ "$NO_WAIT" = 1 ]; then
    info "not waiting (--no-wait). Watch: ${run_url}"
    info "when the run reaches the release environment: APPROVE it yourself —"
    info "deliberate stop-and-check: right commit (${MERGE_SHA:0:9})? right tag (${TAG})?"
    info "resume afterwards with: $0 publish --tag ${TAG} --mode ${MODE} --head-sha ${MERGE_SHA}$( [ "$HOTFIX" = 1 ] && printf ' --hotfix')"
    exit 0
  fi

  info "watching run ${DISPATCHED_RUN_ID} (${run_url})"
  local status conclusion announced=0 last_reminder=0 started
  started=$SECONDS
  while :; do
    status=$(gh run view "$DISPATCHED_RUN_ID" --repo "$REPO" --json status --jq '.status')
    case "$status" in
      completed)
        conclusion=$(gh run view "$DISPATCHED_RUN_ID" --repo "$REPO" \
          --json conclusion --jq '.conclusion')
        if [ "$conclusion" != "success" ]; then
          if resolve_tag_commit "$TAG" >/dev/null; then
            die "$step" "run ${DISPATCHED_RUN_ID} conclusion" "$conclusion" "success" \
              "the run failed AFTER the tag was created; do not re-dispatch —" \
              "repair assets with release-binaries.yml workflow_dispatch" \
              "(publish_assets_to_release), see docs/security-hotfix-release.md Mode B notes"
          fi
          die "$step" "run ${DISPATCHED_RUN_ID} conclusion" "$conclusion" "success" \
            "no tag exists yet, so this is safe: inspect the logs" \
            "(gh run view ${DISPATCHED_RUN_ID} --repo ${REPO} --log-failed)," \
            "fix the branch, and re-dispatch the SAME version"
        fi
        ok "run ${DISPATCHED_RUN_ID} completed successfully"
        return 0
        ;;
      waiting)
        if gh api "repos/${REPO}/actions/runs/${DISPATCHED_RUN_ID}/pending_deployments" \
            --jq '[.[].environment.name]' 2>/dev/null | grep -q 'release'; then
          if [ "$announced" = 0 ] || [ $((SECONDS - last_reminder)) -ge 300 ]; then
            printf '\nAPPROVE NOW  %s\n' "$run_url"
            printf '  This is the deliberate stop-and-check, not a formality:\n'
            printf '  right commit (%s)? right tag (%s)?\n' "${MERGE_SHA:0:9}" "$TAG"
            printf '  The script keeps waiting and verifies after approval.\n\n'
            announced=1
            last_reminder=$SECONDS
          fi
        fi
        ;;
      queued | in_progress | requested | pending) : ;;
      *) warn "unexpected run status: ${status}" ;;
    esac
    if [ "$announced" = 0 ] && [ $((SECONDS - started)) -gt 5400 ]; then
      warn "run still not at the approval gate after 90 minutes"
      info "resume later with the same command; exiting with status 2"
      exit 2
    fi
    sleep 20
  done
}

step_verify_publish() {
  local step="verify-publish"
  if [ "$DRY_RUN" = 1 ]; then
    info "dry-run: would verify tag, release, and assets"
    return 0
  fi

  local tag_commit
  tag_commit=$(resolve_tag_commit "$TAG") \
    || die "$step" "repos/${REPO}/git/ref/tags/${TAG}" "absent" "the release tag" \
        "the run reported success but no tag exists; inspect the run logs"
  [ -z "$MERGE_SHA" ] || [ "$tag_commit" = "$MERGE_SHA" ] \
    || die "$step" "tag ${TAG} commit" "$tag_commit" "$MERGE_SHA" \
        "the tag points at a different commit than this run shipped;" \
        "STOP and inspect — tags are immutable, never reused"

  local rel draft prerelease assets
  rel=$(gh api "repos/${REPO}/releases/tags/${TAG}") \
    || die "$step" "repos/${REPO}/releases/tags/${TAG}" "absent" "a published release" \
        "the tag exists without a release; inspect the Create release run"
  draft=$(jq -r '.draft' <<<"$rel")
  prerelease=$(jq -r '.prerelease' <<<"$rel")
  assets=$(jq -r '[.assets[].name]' <<<"$rel")
  [ "$draft" = "false" ] \
    || die "$step" "release draft flag" "draft" "published" \
        "the release is still a draft; approve/finish the Create release run"
  [ "$prerelease" = "true" ] \
    || info "release is already promoted (prerelease=false)"

  if [ "$SOURCE_FIRST" = 1 ]; then
    info "Mode B: assets attach after the tag-push release-binaries.yml run:"
    gh run list --repo "$REPO" --workflow=release-binaries.yml --event push \
      --limit 5 --json headBranch,status,url \
      --jq ".[] | select(.headBranch == \"${TAG}\") | \"  [\(.status)] \(.url)\"" \
      || true
  else
    local expected missing=""
    for expected in "SHA256SUMS.txt" \
      "zakurad-${TAG}-linux-x86_64.tar.gz" \
      "zakurad-${TAG}-linux-aarch64.tar.gz" \
      "zakurad-manifest-${TAG}.json"; do
      grep -q "\"${expected}\"" <<<"$assets" || missing="${missing} ${expected}"
    done
    [ -z "$missing" ] \
      || die "$step" "release assets" "missing:${missing}" "complete Mode A asset set" \
          "repair with release-binaries.yml workflow_dispatch (publish_assets_to_release)"
  fi
  ok "tag ${TAG} at ${tag_commit:0:9}; release published; assets verified"

  cat <<EOF

HAND-OFF — the script deliberately stops here. Remaining, per the runbook:
  - crates publish loop (fresh checkout of ${TAG}; irreversible from the first crate)
  - make sign-release TAG=${TAG}
  - stable only: $0 promote --tag ${TAG}$( [ "$REPO" != "$CANONICAL_REPO" ] && printf ' --repo %s --allow-nondefault-repo' "$REPO")
  - release description from the changelog; GHSA publish; announce
  - main base: un-freeze Mergify; branch base: $0 forward-merge --tag ${TAG} --pr <N>
EOF
}

# --- subcommands -------------------------------------------------------------

cmd_publish() {
  run_preflight
  if [ "$MODE" = "main" ] && [ "$HOTFIX" = 1 ] && [ "$DRY_RUN" = 0 ] && [ -z "$TAG_COMMIT" ]; then
    confirm "Is the merge queue frozen (no auto-merge can land on main mid-sequence)?"
  fi
  step_merge_or_push
  step_dispatch
  step_watch
  step_verify_publish
}

cmd_promote() {
  check_globals
  is_stable_tag \
    || die "promote" "--tag" "$TAG" "a stable (non-hyphenated) tag" \
        "release candidates are never promoted (docs/release-tag-protection.md)"

  local rel rel_id prerelease assets latest
  rel=$(gh_api_optional "repos/${REPO}/releases/tags/${TAG}") \
    || die "promote" "repos/${REPO}/releases/tags/${TAG}" "absent" "a published release" \
        "publish the release first ($0 publish ...)"
  rel_id=$(jq -r '.id' <<<"$rel")
  prerelease=$(jq -r '.prerelease' <<<"$rel")
  [ "$(jq -r '.draft' <<<"$rel")" = "false" ] \
    || die "promote" "release draft flag" "draft" "published" \
        "finish the Create release run first"
  assets=$(jq -r '[.assets[].name]' <<<"$rel")
  grep -q '"SHA256SUMS.txt.minisig"' <<<"$assets" \
    || die "promote" "release assets" "no SHA256SUMS.txt.minisig" "a signed release" \
        "run: make sign-release TAG=${TAG}   (signing precedes promotion)"

  latest=$(gh api "repos/${REPO}/releases/latest" --jq '.tag_name' 2>/dev/null || printf 'none')
  if [ "$prerelease" = "false" ] && [ "$latest" = "$TAG" ]; then
    skip "promote" "${TAG} is already the promoted latest release"
    return 0
  fi

  # make_latest is a string enum in the REST API: -f (string), not -F.
  act "promoting ${TAG} (clear pre-release, set latest)" \
    gh api --method PATCH "repos/${REPO}/releases/${rel_id}" \
    -F prerelease=false -f make_latest=true >/dev/null
  [ "$DRY_RUN" = 1 ] && return 0

  prerelease=$(gh api "repos/${REPO}/releases/tags/${TAG}" --jq '.prerelease')
  latest=$(gh api "repos/${REPO}/releases/latest" --jq '.tag_name')
  { [ "$prerelease" = "false" ] && [ "$latest" = "$TAG" ]; } \
    || die "promote" "post-promotion state" \
        "prerelease=${prerelease}, latest=${latest}" "prerelease=false, latest=${TAG}" \
        "inspect https://github.com/${REPO}/releases and re-run"
  ok "${TAG} promoted: pre-release cleared, latest set"
}

cmd_forward_merge() {
  check_globals
  [ -n "$PR_NUM" ] || die "arguments" "--pr" "missing" "the forward-merge PR number" \
    "open the forward-merge PR from hotfix/${TAG} into main, then pass --pr <N>"

  local tag_commit
  tag_commit=$(resolve_tag_commit "$TAG") \
    || die "forward-merge" "repos/${REPO}/git/ref/tags/${TAG}" "absent" "the release tag" \
        "forward-merge happens after the release; publish first"

  local pr_json state head_ref
  pr_json=$(gh pr view "$PR_NUM" --repo "$REPO" --json state,headRefName,baseRefName,url)
  state=$(jq -r '.state' <<<"$pr_json")
  head_ref=$(jq -r '.headRefName' <<<"$pr_json")

  if [ "$state" != "MERGED" ]; then
    [ "$state" = "OPEN" ] \
      || die "forward-merge" "gh pr view ${PR_NUM} (state)" "$state" "OPEN" \
          "PR #${PR_NUM} is closed; reopen it or open a fresh forward-merge PR"
    [ "$head_ref" = "hotfix/${TAG}" ] \
      || die "forward-merge" "gh pr view ${PR_NUM} (head)" "$head_ref" "hotfix/${TAG}" \
          "this is not the forward-merge PR for ${TAG}; check the PR number"
    [ "$(jq -r '.baseRefName' <<<"$pr_json")" = "main" ] \
      || die "forward-merge" "gh pr view ${PR_NUM} (base)" \
          "$(jq -r '.baseRefName' <<<"$pr_json")" "main" "check the PR number"

    if [ "$ALLOW_SQUASH" = 1 ]; then
      warn "squash-merging the forward-merge ORPHANS tagged commit ${tag_commit:0:9} from main"
      warn "and breaks later BASE_TAG ancestry checks (this bit drills #350/#354)"
      confirm "Proceed with a squash anyway?"
      act "squash-merging forward-merge PR #${PR_NUM} (--allow-squash)" \
        gh pr merge "$PR_NUM" --repo "$REPO" --squash --admin
    else
      # Whether ruleset bypass exempts allowed_merge_methods is unverified;
      # a refusal here is expected when the ruleset lacks "merge".
      if ! act "merge-committing forward-merge PR #${PR_NUM}" \
          gh pr merge "$PR_NUM" --repo "$REPO" --merge --admin; then
        die "forward-merge" "gh pr merge --merge --admin" "refused" "a merge commit" \
          "the main ruleset likely allows only squash/rebase. Either:" \
          "(a) temporarily add 'merge' to the ruleset's allowed merge methods" \
          "    (Settings > Rules > main > Require a pull request), merge, revert; or" \
          "(b) re-run with --allow-squash — WARNING: orphans the tagged commit." \
          "See docs/security-hotfix-release.md (standing preconditions, post-release)"
      fi
    fi
    [ "$DRY_RUN" = 1 ] && return 0
  else
    skip "forward-merge" "PR #${PR_NUM} already merged"
  fi

  if ancestor_of_main "$tag_commit"; then
    ok "tagged commit ${tag_commit:0:9} is an ancestor of main"
  elif [ "$ALLOW_SQUASH" = 1 ]; then
    warn "tagged commit ${tag_commit:0:9} is NOT an ancestor of main (accepted via --allow-squash);"
    warn "future 'make pre-release BASE_TAG=${TAG}' runs from main will fail the ancestry check"
  else
    die "forward-merge" "repos/${REPO}/compare/${tag_commit}...main" \
      "tag commit not reachable from main" "tag commit is an ancestor of main" \
      "the merge did not preserve the tagged commit (was it squashed?);" \
      "merge hotfix/${TAG} into main with a true merge commit"
  fi

  if act "deleting branch hotfix/${TAG}" \
      gh api --method DELETE "repos/${REPO}/git/refs/heads/hotfix/${TAG}" 2>/dev/null; then
    ok "branch hotfix/${TAG} deleted (the tag is permanent)"
  else
    info "could not delete hotfix/${TAG} (the hotfix/v* ruleset blocks deletion for non-bypass actors);"
    info "delete it from the PR page button or as a ruleset bypass actor"
  fi
}

cmd_status() {
  check_globals
  printf 'status for %s on %s\n' "$TAG" "$REPO"
  local tag_commit rel runs
  if tag_commit=$(resolve_tag_commit "$TAG"); then
    printf '  tag:      exists at %s\n' "${tag_commit:0:9}"
    if ancestor_of_main "$tag_commit"; then
      printf '  ancestry: tag commit is on main\n'
    else
      printf '  ancestry: tag commit NOT on main (forward-merge pending or squashed)\n'
    fi
  else
    printf '  tag:      absent\n'
  fi
  if rel=$(gh_api_optional "repos/${REPO}/releases/tags/${TAG}"); then
    printf '  release:  %s\n' "$(jq -r \
      'if .draft then "draft" else (if .prerelease then "published pre-release" else "published + promoted" end) end + " — assets: " + ([.assets[].name] | length | tostring) + " (" + (if ([.assets[].name] | index("SHA256SUMS.txt.minisig")) then "signed" else "unsigned" end) + ")"' \
      <<<"$rel")"
  else
    printf '  release:  absent\n'
  fi
  runs=$(gh run list --repo "$REPO" --workflow=create-release.yml --limit 5 \
    --json databaseId,status,conclusion,headBranch,headSha,url \
    --jq '.[] | "    run \(.databaseId) [\(.status)\(if .conclusion then "/" + .conclusion else "" end)] \(.headBranch)@\(.headSha[0:9]) \(.url)"')
  printf '  recent Create release runs:\n%s\n' "${runs:-    none}"
}

# --- main --------------------------------------------------------------------

main() {
  [ $# -ge 1 ] || { usage >&2; exit 1; }
  SUBCOMMAND="$1"
  shift
  while [ $# -gt 0 ]; do
    case "$1" in
      --tag) TAG="$2"; shift 2 ;;
      --mode)
        MODE="$2"
        [ "$MODE" = "main" ] || [ "$MODE" = "branch" ] \
          || die "arguments" "--mode" "$MODE" "main or branch" "pass --mode main|branch"
        shift 2
        ;;
      --hotfix) HOTFIX=1; shift ;;
      --pr) PR_NUM="$2"; shift 2 ;;
      --head-sha) HEAD_SHA="$2"; shift 2 ;;
      --source-first) SOURCE_FIRST=1; shift ;;
      --allow-bootstrap-release-state) ALLOW_BOOTSTRAP=1; shift ;;
      --repo) REPO="$2"; shift 2 ;;
      --allow-nondefault-repo) ALLOW_NONDEFAULT_REPO=1; shift ;;
      --run-id) RUN_ID_ARG="$2"; shift 2 ;;
      --dry-run) DRY_RUN=1; shift ;;
      --no-wait) NO_WAIT=1; shift ;;
      --yes) ASSUME_YES=1; shift ;;
      --allow-concurrent-release) ALLOW_CONCURRENT=1; shift ;;
      --allow-squash) ALLOW_SQUASH=1; shift ;;
      -h | --help) usage; exit 0 ;;
      *) die "arguments" "argument parsing" "$1" "a known option" "see --help" ;;
    esac
  done

  case "$SUBCOMMAND" in
    preflight)
      run_preflight
      printf '\npreflight complete. Execution plan for publish:\n'
      if [ "$MODE" = "branch" ]; then
        printf '  1. git push origin %s:refs/heads/hotfix/%s\n' "${HEAD_SHA:0:9}" "$TAG"
        printf '  2. gh workflow run create-release.yml --repo %s --ref hotfix/%s -f release_tag=%s\n' "$REPO" "$TAG" "$TAG"
      else
        printf '  1. gh pr merge %s --repo %s --squash --match-head-commit %s\n' "${PR_NUM:-<N>}" "$REPO" "$HEAD_SHA"
        printf '  2. gh workflow run create-release.yml --repo %s --ref main -f release_tag=%s\n' "$REPO" "$TAG"
      fi
      printf '  3. watch to the release-environment gate; YOU approve\n'
      printf '  4. verify tag + release + assets\n'
      ;;
    publish) cmd_publish ;;
    promote) cmd_promote ;;
    forward-merge) cmd_forward_merge ;;
    status) cmd_status ;;
    -h | --help) usage ;;
    *) usage >&2; die "arguments" "subcommand" "$SUBCOMMAND" \
        "preflight|publish|promote|forward-merge|status" "see --help" ;;
  esac
}

main "$@"
