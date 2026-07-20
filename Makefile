.PHONY: help

include make/zcashd-compat.mk
include make/perf.mk
include make/zakura-dev.mk
include make/release.mk
include make/install.mk

help:
	@echo "Available targets:"
	@echo ""
	@echo "  Install:"
	@echo "  BRANCH=<branch> make install-branch   Install zakurad from a Git branch (default: main)"
	@echo ""
	@echo "  Dev Zakura (local node):"
	@echo "  zakura-build-dev                 Build release zakurad"
	@echo "  zakura-dev-init                  Create ~/.local/zakura-dev config + dirs"
	@echo "  zakura-start-dev                 Start local dev node (pruned, VCT, v2-only)"
	@echo ""
	@echo "  Perf harness (deterministic isolated-cohort bench):"
	@echo "  perf-build-local                 Build the instrumented (commit-metrics) bench binary"
	@echo "  perf-build-stage-bin <name>      Build + stage the bench binary as perf-artifacts/<name>"
	@echo "  perf-run [bin-name]              Run an isolated bench (PERF_LABEL/PERF_STOP, optional staged bin)"
	@echo "  perf-run-mainnet [bin-name]      Run against public Mainnet Zakura peers (PERF_MAINNET_LABEL/PERF_STOP)"
	@echo "  perf-analyze                     Bottleneck attribution over the CSV window (PERF_LABEL/PERF_LO/PERF_HI)"
	@echo "  perf-dashboard                   Live metrics dashboard for the running bench node"
	@echo "  perf-verify-isolation            Confirm the bench sees only the two cohort peers"
	@echo "  perf-seed-serving                Deploy + sync the two serving nodes from public mainnet"
	@echo "  perf-peers                       Capture serving node_id@ip:8234 into cohort.env"
	@echo "  perf-freeze-serving              Redeploy serving nodes cohort-isolated (static range)"
	@echo "  perf-status                      Report serving node service state + version"
	@echo ""
	@echo "  zcashd-compat:"
	@echo "  compat-docker-build              Build Docker zcashd-compat image"
	@echo "  compat-zcashd-prepare            Fetch/verify zcashd-compat artifact for Docker build"
	@echo "  compat-docker-start              Start Docker zcashd-compat with mounted snapshots"
	@echo "  compat-zakurad-start-supervised   Start zakurad with zcashd supervision enabled"
	@echo "  compat-zakurad-start-unsupervised Start zakurad with zcashd supervision disabled"
	@echo "  compat-zcashd-start-standalone   Start zcashd -zebra-compat as a standalone process"
	@echo "  compat-zakurad-status             Check zakurad liveness and Zebra RPC health"
	@echo "  compat-zcashd-status             Check zcashd liveness and zebra-compat RPC health"
	@echo "  compat-status-sync               Run both status checks and enforce max drift"
	@echo "  compat-test-regtest              Run full zcashd-compat test suite (regtest, spawns processes)"
	@echo "  compat-test-mainnet              Run read-only zcashd-compat tests against live mainnet"
	@echo "  compat-test-testnet              Run read-only zcashd-compat tests against live testnet"
	@echo ""
	@echo "  Release:"
	@echo "  pre-release RELEASE_TAG=vX.Y.Z BASE_TAG=vX.Y.Z   Run release version, Mainnet release-state, and packaging checks"
	@echo "  sign-release TAG=vX.Y.Z          Sign a release's SHA256SUMS.txt with the maintainer minisign key (see VERIFY.md)"
