# Historical treestate audit helpers.
#
# Override variables on the command line, for example:
#   make treestate-audit-walk TREESTATE_CACHE_DIR=/path/to/cache TREESTATE_FROM=419200 TREESTATE_TO=435000

.PHONY: \
	treestate-audit-inventory \
	treestate-audit-subtrees \
	treestate-audit-walk \
	treestate-audit-samples \
	treestate-audit-roots \
	treestate-audit-differential

TREESTATE_ZAKURAD_BIN ?= $(CURDIR)/target/release/zakurad
TREESTATE_CHECK_SCRIPT ?= $(CURDIR)/scripts/differential-treestate-check.py
TREESTATE_CACHE_DIR ?= $(HOME)/.local/zakura-dev/cache
TREESTATE_NETWORK ?= Mainnet
TREESTATE_FROM ?=
TREESTATE_TO ?=
TREESTATE_STEP ?= 1
TREESTATE_RPC_URL ?=
TREESTATE_EXTRA_ARGS ?=

TREESTATE_AUDIT = "$(TREESTATE_ZAKURAD_BIN)" audit-historical-treestates \
	--cache-dir "$(TREESTATE_CACHE_DIR)" \
	--network "$(TREESTATE_NETWORK)"
TREESTATE_RANGE_ARGS = \
	$(if $(TREESTATE_FROM),--from "$(TREESTATE_FROM)") \
	$(if $(TREESTATE_TO),--to "$(TREESTATE_TO)") \
	--step "$(TREESTATE_STEP)"

# Report the database inventory and explicitly verify full-band block-body retention.
treestate-audit-inventory:
	$(TREESTATE_AUDIT) --scan-block-bodies $(TREESTATE_EXTRA_ARGS)

# Replay the post-checkpoint band and compare the resulting subtree roots with stored rows.
treestate-audit-subtrees:
	$(TREESTATE_AUDIT) --verify-subtrees $(TREESTATE_EXTRA_ARGS)

# Derive and authenticate historical treestates over a configurable height range.
treestate-audit-walk:
	$(TREESTATE_AUDIT) --walk $(TREESTATE_RANGE_ARGS) $(TREESTATE_EXTRA_ARGS)

# Measure independent cold replays and emit one `SAMPLE` cost/input line per sampled height.
treestate-audit-samples:
	$(TREESTATE_AUDIT) --walk --cold --print-samples $(TREESTATE_RANGE_ARGS) $(TREESTATE_EXTRA_ARGS)

# Derive treestates and emit one `ROOT` line per sampled height for external comparison.
treestate-audit-roots:
	$(TREESTATE_AUDIT) --walk --print-roots $(TREESTATE_RANGE_ARGS) $(TREESTATE_EXTRA_ARGS)

# Compare locally derived roots with a legacy node's `z_gettreestate` response over JSON-RPC.
treestate-audit-differential:
	@if [ -n "$(TREESTATE_FROM)" ] && [ -n "$(TREESTATE_TO)" ] && [ -n "$(TREESTATE_RPC_URL)" ]; then :; else \
		echo "usage: make treestate-audit-differential TREESTATE_FROM=<height> TREESTATE_TO=<height> TREESTATE_RPC_URL=<url> [TREESTATE_CACHE_DIR=<path>] [TREESTATE_STEP=<n>]" >&2; \
		exit 2; \
	fi
	"$(TREESTATE_CHECK_SCRIPT)" \
		--cache-dir "$(TREESTATE_CACHE_DIR)" \
		--network "$(TREESTATE_NETWORK)" \
		--rpc-url "$(TREESTATE_RPC_URL)" \
		--from-height "$(TREESTATE_FROM)" \
		--to-height "$(TREESTATE_TO)" \
		--step "$(TREESTATE_STEP)" \
		--zakurad "$(TREESTATE_ZAKURAD_BIN)"
