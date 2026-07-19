---
name: deploy-specific
description: >-
  Deploy one or more explicitly named Zakura testnet or mainnet nodes through
  GitHub Actions. Use when the user asks to deploy specific nodes or provides a
  node list. Use deploy-all for a complete fleet rollout.
---

# Deploy Specific Zakura Nodes

Deploy only the nodes explicitly named by the user. One or many nodes are
allowed. Never interpret an omitted node list as the whole fleet, and never
leave the workflow's `node` input blank.

For requests to deploy every node, use `deploy-all`, which applies the fixed
testnet-first, half-by-half rollout strategy.

## Inputs

Require or confirm:

- `ref`: Git branch, tag, or SHA to build and deploy.
- `nodes`: one or more exact names from the inventories below.
- `network`: required when a node name is ambiguous, such as `zakura-compat`.
- `force_rebuild`: optional; defaults to `false`.
- `no_restart`: optional; defaults to `false`.

For any mainnet node, confirm the `ref` and complete node list explicitly with
the user immediately before dispatch.

## Node inventory

Testnet:

- `zakura-testnet-1`
- `zakura-testnet-2`
- `zakura-testnet-3`
- `zakura-testnet-eu`
- `zakura-testnet-as`
- `zakura-compat`

Mainnet:

- `asia-0`
- `us-0`
- `us-east-0`
- `us-west-0`
- `canada-0`
- `europe-west-0`
- `europe-central-0`
- `asia-south-0`
- `asia-pacific-0`
- `zakura-compat`
- `zakura-compat-docker`

Reject unknown names rather than guessing. If `zakura-compat` is requested
without a network, ask which network.

## Preflight

From the repository root:

```bash
git fetch origin <ref>
REF=$(git rev-parse FETCH_HEAD)
gh workflow view zakura-testnet-deploy.yml --repo zakura-core/zakura
gh workflow view zakura-mainnet-deploy.yml --repo zakura-core/zakura
```

Use the resolved full commit SHA for every requested node.

## Dispatch

The workflows accept one node per run. For multiple nodes, dispatch and verify
each explicit node in order so a failure stops the remaining deployment:

```bash
deploy_specific() {
  local workflow=$1
  shift
  local run_url
  local run_id

  test "$#" -gt 0

  for node in "$@"; do
    run_url=$(gh workflow run "$workflow" \
      --repo zakura-core/zakura \
      -f ref="$REF" \
      -f node="$node")
    test -n "$run_url"
    run_id="${run_url##*/}"
    gh run watch "$run_id" \
      --repo zakura-core/zakura \
      --exit-status
  done
}
```

Examples:

```bash
deploy_specific zakura-testnet-deploy.yml \
  zakura-testnet-1 zakura-testnet-eu

deploy_specific zakura-mainnet-deploy.yml \
  us-east-0 europe-central-0
```

When a request contains both networks, dispatch and verify the explicitly named
testnet nodes before requesting mainnet confirmation and dispatching the named
mainnet nodes.

Append these workflow fields only when requested:

```bash
-f force_rebuild=true
-f no_restart=true
```

## Verification

- Confirm every workflow run succeeded and its final node status is healthy.
- Confirm each node reports the expected commit/version.
- For restarted nodes, confirm RPC height is current and advances.
- For testnet `zakura-compat`, confirm the zcashd sidecar sync check passes.
- Stop on failure; do not continue to additional requested nodes.
- Report each requested node and its workflow run URL.

## Hard guards

- Never dispatch with an empty `node` input.
- Never add unrequested nodes to the deployment.
- Do not change cache, state, identity, or node configuration paths.
- Only one deploy per network runs at a time (`cancel-in-progress: false`).

## References

- Full-fleet rollout: `../deploy-all/SKILL.md`
- Deployer: `deploy/deployer/README.md`
- Testnet workflow: `.github/workflows/zakura-testnet-deploy.yml`
- Mainnet workflow: `.github/workflows/zakura-mainnet-deploy.yml`
