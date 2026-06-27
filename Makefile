.PHONY: help

include make/zcashd-compat.mk
include make/perf.mk

help:
	@echo "Available targets:"
	@echo ""
	@echo "  Perf harness (deterministic isolated-cohort bench):"
	@echo "  perf-build-local                 Build the instrumented (commit-metrics) bench binary"
	@echo "  perf-run                         Run an isolated bench against the cohort (PERF_LABEL/PERF_STOP)"
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
	@echo "  compat-zebrad-start-supervised   Start zebrad with zcashd supervision enabled"
	@echo "  compat-zebrad-start-unsupervised Start zebrad with zcashd supervision disabled"
	@echo "  compat-zcashd-start-standalone   Start zcashd -zebra-compat as a standalone process"
	@echo "  compat-zebrad-status             Check zebrad liveness and Zebra RPC health"
	@echo "  compat-zcashd-status             Check zcashd liveness and zebra-compat RPC health"
	@echo "  compat-status-sync               Run both status checks and enforce max drift"
	@echo "  compat-test-regtest              Run full zcashd-compat test suite (regtest, spawns processes)"
	@echo "  compat-test-mainnet              Run read-only zcashd-compat tests against live mainnet"
	@echo "  compat-test-testnet              Run read-only zcashd-compat tests against live testnet"
