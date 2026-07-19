---
name: deploy-all
description: >-
  Roll out one Zakura ref to every managed node in staged halves: testnet first,
  then mainnet. Use when the user asks to deploy all nodes, deploy the whole
  fleet, or perform a full Zakura rollout.
---

# Deploy All Zakura Nodes

Roll out one immutable branch, tag, or SHA through the existing single-node
GitHub Actions deploy inputs. Never deploy a whole network by leaving `node`
blank.

## Safety gates

- Require one `ref` for the entire rollout and verify it exists on the remote.
- Deploy testnet before mainnet.
- Deploy each network in the two fixed batches below.
- Wait for every run in a batch and verify node health before starting the next
  batch.
- Stop the rollout on any build, deploy, restart, sidecar, status, or health
  failure. Do not continue with the remaining batches.
- After both testnet batches pass, get explicit user confirmation of the same
  `ref` immediately before dispatching mainnet.
- Do not change cache, state, identity, or node configuration paths.

## Fixed rollout batches

Testnet:

1. `zakura-testnet-1`, `zakura-testnet-2`, `zakura-testnet-3`
2. `zakura-testnet-eu`, `zakura-testnet-as`, `zakura-compat`

Mainnet:

1. `asia-0`, `us-0`, `us-west-0`, `canada-0`, `europe-west-0`
2. `us-east-0`, `europe-central-0`, `asia-south-0`,
   `asia-pacific-0`, `zakura-compat`, `zakura-compat-docker`

These batches cover all 6 testnet and all 11 mainnet nodes. Keep compatibility
nodes in the second half so ordinary nodes establish the candidate version
first.

## Preflight

From the repository root:

```bash
git fetch origin <ref>
REF=$(git rev-parse FETCH_HEAD)
gh workflow view zakura-testnet-deploy.yml --repo zakura-core/zakura
gh workflow view zakura-mainnet-deploy.yml --repo zakura-core/zakura
```

Use the resolved full commit SHA for every dispatch so a moving branch cannot
change during the rollout.

## Dispatch one batch

Use Bash. Dispatch every node in the batch with an explicit `node`, record each
returned run ID, then wait for all of them:

```bash
deploy_batch() {
  local workflow=$1
  shift
  local run_url
  local -a run_ids=()

  for node in "$@"; do
    run_url=$(gh workflow run "$workflow" \
      --repo zakura-core/zakura \
      -f ref="$REF" \
      -f node="$node")
    test -n "$run_url"
    run_ids+=("${run_url##*/}")
  done

  for run_id in "${run_ids[@]}"; do
    gh run watch "$run_id" \
      --repo zakura-core/zakura \
      --exit-status
  done
}
```

The workflows serialize runs per network. Dispatching a batch together queues
only that half; the next half is not dispatched until all queued runs pass.

Call the function in this order:

```bash
REF=<full-commit-sha>

deploy_batch zakura-testnet-deploy.yml \
  zakura-testnet-1 zakura-testnet-2 zakura-testnet-3

# Complete the testnet first-half health gate.

deploy_batch zakura-testnet-deploy.yml \
  zakura-testnet-eu zakura-testnet-as zakura-compat

# Complete the full-testnet health gate and obtain explicit mainnet confirmation.

deploy_batch zakura-mainnet-deploy.yml \
  asia-0 us-0 us-west-0 canada-0 europe-west-0

# Complete the mainnet first-half health gate.

deploy_batch zakura-mainnet-deploy.yml \
  us-east-0 europe-central-0 asia-south-0 asia-pacific-0 \
  zakura-compat zakura-compat-docker
```

Append `-f force_rebuild=true` only when explicitly needed. Use
`-f no_restart=true` only for a separately requested staging-only rollout; it
does not satisfy a completed deployment.

## Health gate after each batch

- Confirm every workflow run succeeded, including its final fleet status.
- Confirm each deployed node reports the expected commit/version and healthy
  service or container state.
- Confirm RPC height is current and advances after the restart.
- For testnet `zakura-compat`, confirm the workflow's zcashd sidecar sync check
  passes.
- For compatibility nodes, confirm both Zakura and zcashd remain healthy.
- Record failed nodes and run URLs. Stop rather than rolling forward around a
  failed node.

Dashboards:

- Testnet: `http://167.99.103.111:8090/`
- Mainnet: `http://159.65.183.89:8090/`

## Completion report

Report the immutable ref, the four batches, run URLs, health verification, any
retries, and whether all 17 nodes completed successfully.

## References

- Explicit node-list operations: `../deploy-specific/SKILL.md`
- Testnet workflow: `.github/workflows/zakura-testnet-deploy.yml`
- Mainnet workflow: `.github/workflows/zakura-mainnet-deploy.yml`
- Deployer: `deploy/deployer/README.md`
