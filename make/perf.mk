# Perf harness wrapper — thin Make targets over deploy/runner/perf.sh, the
# deterministic isolated-cohort benchmark driver (see deploy/runner/runbook.md).
#
# The serving cohort + bench live in deploy/runner/ (local tooling); these
# targets are stable entry points over it. Override the vars below on the
# command line, e.g.:
#   make perf-run     PERF_LABEL=r2 PERF_STOP=1825000
#   make perf-run     PERF_LABEL=r2 PERF_BIN_NAME=my-build
#   make perf-run     my-build
#   make perf-analyze PERF_LABEL=r2 PERF_LO=1806000 PERF_HI=1824000

.PHONY: \
	perf-build-local \
	perf-build-stage-bin \
	perf-run \
	perf-run-mainnet \
	perf-analyze \
	perf-logs \
	perf-dashboard \
	perf-verify-isolation \
	perf-seed-serving \
	perf-peers \
	perf-freeze-serving \
	perf-status

PERF_SH ?= $(CURDIR)/deploy/runner/perf.sh
FEED_RUN ?= $(CURDIR)/deploy/runner/feed_run.sh
PERF_MAINNET_CONFIG ?= $(CURDIR)/deploy/runner/zebra-mainnet-bench-config.toml

PERF_GOAL_ARG := $(word 2,$(MAKECMDGOALS))
ifneq ($(filter perf-build-stage-bin perf-run perf-run-mainnet,$(firstword $(MAKECMDGOALS))),)
ifneq ($(PERF_GOAL_ARG),)
PERF_BIN_NAME ?= $(PERF_GOAL_ARG)
.PHONY: $(PERF_GOAL_ARG)
$(PERF_GOAL_ARG):
	@:
endif
endif

# perf-run / perf-analyze parameters (override on the command line).
PERF_LABEL         ?= r1
PERF_MAINNET_LABEL ?= r1-mainnet
PERF_STOP          ?= 1900000
PERF_LO            ?= 1810000
PERF_HI            ?= 1895000
PERF_BIN_NAME      ?=

# Build the instrumented (commit-metrics) local bench binary -> $BENCH_BIN.
perf-build-local:
	"$(PERF_SH)" build-local

# Build the instrumented bench binary and stage it as perf-artifacts/<name>.
perf-build-stage-bin:
	@if [ -n "$(PERF_BIN_NAME)" ]; then :; else echo "usage: make perf-build-stage-bin <name> (or PERF_BIN_NAME=<name>)" >&2; exit 2; fi
	"$(PERF_SH)" stage-bin "$(PERF_BIN_NAME)"

# Fork the snapshot and run an isolated bench against the cohort, emitting a CSV.
perf-run:
	PERF_BIN_NAME="$(PERF_BIN_NAME)" "$(PERF_SH)" run $(PERF_LABEL) $(PERF_STOP)

# Fork the snapshot and run against public Mainnet Zakura bootstrap peers.
perf-run-mainnet:
	bin="$$(PERF_BIN_NAME="$(PERF_BIN_NAME)" "$(PERF_SH)" bench-bin)" && CONFIG_SRC="$(PERF_MAINNET_CONFIG)" "$(FEED_RUN)" $(PERF_MAINNET_LABEL) "$$bin" $(PERF_STOP)

# Steady-state bottleneck attribution over the CSV window [PERF_LO, PERF_HI].
perf-analyze:
	"$(PERF_SH)" analyze $(PERF_LABEL) $(PERF_LO) $(PERF_HI)

# Follow the running bench node's log (byte-budget drift spam filtered).
# Pass RAW=1 to include everything, LINES=N for a different backlog size.
perf-logs:
	"$(PERF_SH)" logs $(PERF_LABEL)

# Live metrics dashboard (auto-detects the running bench node's metrics port).
perf-dashboard:
	"$(PERF_SH)" dashboard

# Confirm the bench node sees only the two cohort peers and no rejects.
perf-verify-isolation:
	"$(PERF_SH)" verify-isolation

# ─── Serving-cohort lifecycle (one-time / occasional) ─────────────────────────

# Deploy both serving nodes and sync them from public mainnet to SEED_HEIGHT.
perf-seed-serving:
	"$(PERF_SH)" seed-serving

# Capture each serving node's node_id@ip:8234 into deploy/runner/cohort.env.
perf-peers:
	"$(PERF_SH)" peers

# Redeploy the serving nodes cohort-isolated (legacy off) to serve a static range.
perf-freeze-serving:
	"$(PERF_SH)" freeze-serving

# Report each serving node's service state and version.
perf-status:
	"$(PERF_SH)" status
