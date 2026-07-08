.PHONY: \
	compat-docker-build \
	compat-docker-start \
	compat-zakurad-start-supervised-managed \
	compat-zakurad-start-supervised \
	compat-zakurad-start-unsupervised \
	compat-zcashd-start-standalone \
	compat-zakurad-status \
	compat-zcashd-status \
	compat-status-sync \
	compat-test-regtest \
	compat-test-soak \
	compat-test-mainnet \
	compat-test-testnet

ZAKURAD_BIN ?= $(CURDIR)/target/release/zakurad
ZCASHD_BIN ?= /root/unity/zcash/src/zcashd
ZCASH_CLI_BIN ?= /root/unity/zcash/src/zcash-cli

# TODO: make more general
NETWORK ?= Mainnet
ZAKURA_STATE_CACHE_DIR ?= /mnt/data/zakura-state
ZCASHD_DATADIR ?= /mnt/data/zcashd-mainnet
ZCASHD_CONF ?= $(ZCASHD_DATADIR)/zcash.conf
ZCASHD_EXTRA_ARGS ?= -printtoconsole
# Zakura's legacy P2P listener; standalone zcashd pins its single peer to it.
ZAKURA_P2P_ADDR ?= 127.0.0.1:8233
# Dedicated zcashd-compat Zakura RPC listener (operator tooling only; the P2P
# sidecar zcashd does not use it).
ZCASHD_ZAKURA_RPC_URL ?= http://127.0.0.1:28232

ZAKURA_COOKIE_DIR ?= $(ZAKURA_STATE_CACHE_DIR)
ZAKURA_COOKIE_FILE ?= $(ZAKURA_COOKIE_DIR)/.zcashd-compat.cookie
HEIGHT_MAX_DRIFT ?= 10

ZAKURA_DOCKER_IMAGE ?= zakura:zcashd-compat
ZCASHD_COMPAT_MANIFEST ?= $(CURDIR)/zebrad/zcashd-compat-manifest.json
ZCASHD_COMPAT_TARGET_TRIPLE ?= x86_64-pc-linux-gnu
ZCASHD_COMPAT_RELEASE_TAG ?= $(shell jq -er '.release_tag' $(ZCASHD_COMPAT_MANIFEST))
ZCASHD_COMPAT_URL ?= $(shell jq -er --arg target '$(ZCASHD_COMPAT_TARGET_TRIPLE)' '.artifacts[] | select(.target_triple == $$target) | .runtime_archive_url' $(ZCASHD_COMPAT_MANIFEST))
ZCASHD_COMPAT_SHA256 ?= $(shell jq -er --arg target '$(ZCASHD_COMPAT_TARGET_TRIPLE)' '.artifacts[] | select(.target_triple == $$target) | .runtime_archive_sha256' $(ZCASHD_COMPAT_MANIFEST))
ZCASHD_COMPAT_ARTIFACT_DIR ?= $(CURDIR)/target/zcashd-compat
ZCASHD_COMPAT_ARCHIVE_PATH ?= $(ZCASHD_COMPAT_ARTIFACT_DIR)/zcashd-compat.tar.gz
ZCASHD_COMPAT_EXTRACT_DIR ?= $(ZCASHD_COMPAT_ARTIFACT_DIR)/extracted
# Optional override for callers that prepare zcashd by other means.
# This directory must contain a Linux executable at ./bin/zcashd.
ZCASHD_COMPAT_BUILD_CONTEXT ?=

.PHONY: compat-zcashd-prepare

compat-zcashd-prepare:
	@set -eu; \
	if [ -n "$(ZCASHD_COMPAT_BUILD_CONTEXT)" ]; then \
		echo "Using provided zcashd build context: $(ZCASHD_COMPAT_BUILD_CONTEXT)"; \
		test -x "$(ZCASHD_COMPAT_BUILD_CONTEXT)/bin/zcashd"; \
	else \
		echo "Fetching hash-pinned zcashd-compat archive..."; \
		mkdir -p "$(ZCASHD_COMPAT_ARTIFACT_DIR)"; \
		curl -fsSL "$(ZCASHD_COMPAT_URL)" -o "$(ZCASHD_COMPAT_ARCHIVE_PATH)"; \
		echo "$(ZCASHD_COMPAT_SHA256)  $(ZCASHD_COMPAT_ARCHIVE_PATH)" | sha256sum -c -; \
		rm -rf "$(ZCASHD_COMPAT_EXTRACT_DIR)"; \
		mkdir -p "$(ZCASHD_COMPAT_EXTRACT_DIR)"; \
		tar -xzf "$(ZCASHD_COMPAT_ARCHIVE_PATH)" -C "$(ZCASHD_COMPAT_EXTRACT_DIR)"; \
		test -x "$(ZCASHD_COMPAT_EXTRACT_DIR)/bin/zcashd"; \
	fi

compat-docker-build: compat-zcashd-prepare
	@echo "Building Docker zcashd-compat image..."
	@set -eu; \
	context_dir="$(ZCASHD_COMPAT_BUILD_CONTEXT)"; \
	if [ -z "$$context_dir" ]; then \
		context_dir="$(ZCASHD_COMPAT_EXTRACT_DIR)"; \
	fi; \
	docker build -f ./docker/Dockerfile --target runtime-zcashd-compat \
		--build-context "zcashd_compat=$$context_dir" \
		--tag "$(ZAKURA_DOCKER_IMAGE)" .

# The Zakura compat listener is internal to the supervised zcashd process in this
# container, so keep it on container loopback and publish only zcashd's RPC port.
compat-docker-start:
	@echo "Starting Docker zcashd-compat container..."
	docker run --rm -it \
		-e ZCASHD_COMPAT_ENABLED=true \
		-e ZAKURA_NETWORK__NETWORK="$(NETWORK)" \
		-e ZAKURA_NETWORK__LISTEN_ADDR="[::]:8233" \
		-e ZAKURA_STATE__CACHE_DIR="/home/zebra/.cache/zakura" \
		-e ZAKURA_ZCASHD_COMPAT__ZCASHD_DATADIR="/home/zebra/.cache/zcashd" \
		-e ZAKURA_ZCASHD_COMPAT__LISTEN_ADDR="127.0.0.1:28232" \
		-e ZAKURA_ZCASHD_COMPAT__ZCASHD_EXTRA_ARGS='["-rpcbind=0.0.0.0","-rpcallowip=0.0.0.0/0"]' \
		--mount type=bind,src="$(ZAKURA_STATE_CACHE_DIR)",dst="/home/zebra/.cache/zakura" \
		--mount type=bind,src="$(ZCASHD_DATADIR)",dst="/home/zebra/.cache/zcashd" \
		-p 8233:8233 \
		-p 127.0.0.1:8232:8232 \
		"$(ZAKURA_DOCKER_IMAGE)" \
		zakurad start --zcashd-compat

compat-zakurad-start-supervised-managed:
	@echo "Starting zakurad in zcashd-compat mode with managed zcashd download..."
	ZAKURA_NETWORK__NETWORK="$(NETWORK)" \
	ZAKURA_STATE__CACHE_DIR="$(ZAKURA_STATE_CACHE_DIR)" \
	ZAKURA_ZCASHD_COMPAT__COOKIE_DIR="$(ZAKURA_COOKIE_DIR)" \
	ZAKURA_ZCASHD_COMPAT__ZCASHD_SOURCE=managed \
	ZAKURA_ZCASHD_COMPAT__ZCASHD_DATADIR="$(ZCASHD_DATADIR)" \
	"$(ZAKURAD_BIN)" start --zcashd-compat

compat-zakurad-start-supervised:
	@echo "Starting zakurad in zcashd-compat mode with supervision enabled..."
	ZAKURA_NETWORK__NETWORK="$(NETWORK)" \
	ZAKURA_STATE__CACHE_DIR="$(ZAKURA_STATE_CACHE_DIR)" \
	ZAKURA_ZCASHD_COMPAT__COOKIE_DIR="$(ZAKURA_COOKIE_DIR)" \
	ZAKURA_ZCASHD_COMPAT__ZCASHD_SOURCE=path \
	ZAKURA_ZCASHD_COMPAT__ZCASHD_PATH="$(ZCASHD_BIN)" \
	ZAKURA_ZCASHD_COMPAT__ZCASHD_DATADIR="$(ZCASHD_DATADIR)" \
	"$(ZAKURAD_BIN)" start --zcashd-compat

compat-zakurad-start-unsupervised:
	@echo "Starting zakurad in zcashd-compat mode with supervision disabled..."
	ZAKURA_NETWORK__NETWORK="$(NETWORK)" \
	ZAKURA_STATE__CACHE_DIR="$(ZAKURA_STATE_CACHE_DIR)" \
	ZAKURA_ZCASHD_COMPAT__COOKIE_DIR="$(ZAKURA_COOKIE_DIR)" \
	ZAKURA_ZCASHD_COMPAT__MANAGE_ZCASHD=false \
	ZAKURA_ZCASHD_COMPAT__ZCASHD_SOURCE=path \
	ZAKURA_ZCASHD_COMPAT__ZCASHD_PATH="$(ZCASHD_BIN)" \
	ZAKURA_ZCASHD_COMPAT__ZCASHD_DATADIR="$(ZCASHD_DATADIR)" \
	"$(ZAKURAD_BIN)" start --zcashd-compat

compat-zcashd-start-standalone:
	@echo "Starting zcashd as a standalone P2P sidecar of Zakura..."
	"$(ZCASHD_BIN)" \
		-datadir="$(ZCASHD_DATADIR)" \
		-conf="$(ZCASHD_CONF)" \
		$(ZCASHD_EXTRA_ARGS) \
		-connect="$(ZAKURA_P2P_ADDR)" \
		-listen=0 \
		-dnsseed=0 \
		-listenonion=0 \
		-discover=0

compat-zakurad-status:
	@echo "Checking zakurad process..."
	@if pgrep -f "zakurad start --zcashd-compat" >/dev/null; then \
		echo "zakurad process: OK"; \
	else \
		echo "zakurad process: NOT RUNNING"; \
		exit 1; \
	fi
	@echo "Checking Zakura RPC getblockcount..."
	@if [ ! -f "$(ZAKURA_COOKIE_FILE)" ]; then \
		echo "Zakura cookie file missing: $(ZAKURA_COOKIE_FILE)"; \
		exit 1; \
	fi
	@zebra_height="$$(curl -sS --fail --user "$$(cat "$(ZAKURA_COOKIE_FILE)")" \
		-H 'Content-Type: application/json' \
		--data '{"jsonrpc":"1.0","id":"make","method":"getblockcount","params":[]}' \
		"$(ZCASHD_ZAKURA_RPC_URL)" | python3 -c 'import sys,json; print(json.load(sys.stdin)["result"])')"; \
		echo "zakurad RPC height: $$zebra_height"

compat-zcashd-status:
	@echo "Checking zcashd process..."
	@if pgrep -f "zcashd.*-connect" >/dev/null; then \
		echo "zcashd process: OK"; \
	else \
		echo "zcashd process: NOT RUNNING"; \
		exit 1; \
	fi
	@echo "Checking zcashd peer pinning..."
	@peers="$$( "$(ZCASH_CLI_BIN)" -conf="$(ZCASHD_CONF)" -datadir="$(ZCASHD_DATADIR)" getconnectioncount )"; \
		echo "zcashd connections: $$peers (expected: 1, the Zakura node)"; \
		if [ "$$peers" != "1" ]; then \
			echo "WARNING: sidecar zcashd should have exactly one peer"; \
		fi
	@zcashd_height="$$( "$(ZCASH_CLI_BIN)" -conf="$(ZCASHD_CONF)" -datadir="$(ZCASHD_DATADIR)" getblockcount )"; \
		echo "zcashd height: $$zcashd_height"

compat-status-sync:
	@$(MAKE) compat-zakurad-status
	@$(MAKE) compat-zcashd-status
	@zebra_height="$$(curl -sS --fail --user "$$(cat "$(ZAKURA_COOKIE_FILE)")" \
		-H 'Content-Type: application/json' \
		--data '{"jsonrpc":"1.0","id":"make","method":"getblockcount","params":[]}' \
		"$(ZCASHD_ZAKURA_RPC_URL)" | python3 -c 'import sys,json; print(json.load(sys.stdin)["result"])')"; \
		zcashd_height="$$( "$(ZCASH_CLI_BIN)" -conf="$(ZCASHD_CONF)" -datadir="$(ZCASHD_DATADIR)" getblockcount )"; \
		drift=$$(( zebra_height - zcashd_height )); \
		if [ $$drift -lt 0 ]; then drift=$$(( -drift )); fi; \
		echo "zakurad height: $$zebra_height"; \
		echo "zcashd height: $$zcashd_height"; \
		echo "height drift: $$drift (max allowed: $(HEIGHT_MAX_DRIFT))"; \
		if [ $$drift -gt "$(HEIGHT_MAX_DRIFT)" ]; then \
			echo "ERROR: height drift exceeded threshold"; \
			exit 1; \
		fi

# ─── Integration test targets ─────────────────────────────────────────────────

# Optional: path to a local zcashd binary for regtest tests.
# If unset, the managed download embedded in the zebrad binary is used.
# Override with: make compat-test-regtest TEST_ZCASHD_PATH=/path/to/zcashd
TEST_ZCASHD_PATH ?=
TEST_ZCASHD_COMPAT_REORG_ITERATIONS ?= 500

# External-mode test addresses and credentials.
# Set these before running compat-test-mainnet or compat-test-testnet.
TEST_ZEBRAD_RPC_ADDR ?= 127.0.0.1:8232
TEST_ZCASHD_RPC_ADDR ?= 127.0.0.1:28232
# Set one of the following for zcashd authentication (cookie file is preferred):
TEST_ZCASHD_COOKIE_FILE ?=
TEST_ZCASHD_RPC_USER ?=
TEST_ZCASHD_RPC_PASSWORD ?=

# Run the full zcashd-compat integration test suite against a fresh regtest
# environment.  zebrad and zcashd are spawned automatically by the test harness.
#
# Prerequisites: a zcashd binary (set TEST_ZCASHD_PATH) or let the
#   managed download provide one.
# When to use: CI smoke-testing and developer local verification after code changes.
compat-test-regtest:
	TEST_ZCASHD_COMPAT=1 \
	TEST_ZCASHD_PATH="$(TEST_ZCASHD_PATH)" \
	cargo nextest run --profile zcashd-compat-integration --run-ignored=only

# Run a long zcashd-compat reorg churn soak against a fresh regtest environment.
# Override TEST_ZCASHD_COMPAT_REORG_ITERATIONS for shorter local smoke runs.
compat-test-soak:
	TEST_ZCASHD_COMPAT=1 \
	TEST_ZCASHD_PATH="$(TEST_ZCASHD_PATH)" \
	TEST_ZCASHD_COMPAT_REORG_ITERATIONS="$(TEST_ZCASHD_COMPAT_REORG_ITERATIONS)" \
	cargo nextest run --profile zcashd-compat-soak --run-ignored=only

# Run the read-only zcashd-compat test suite against a live mainnet deployment.
# Requires a fully-synced zebrad and zcashd already running on this host.
# Tests that require block mining (sendtoaddress, generate, etc.) are skipped.
#
# Prerequisites:
#   - zebrad running with --zcashd-compat on mainnet
#   - zcashd -zebra-compat connected to that zebrad
#   - TEST_ZEBRAD_RPC_ADDR and TEST_ZCASHD_RPC_ADDR pointing to them
#   - TEST_ZCASHD_COOKIE_FILE or TEST_ZCASHD_RPC_USER/PASSWORD set
# When to use: validating a live mainnet deployment after an upgrade.
compat-test-mainnet:
	TEST_ZCASHD_COMPAT=1 \
	TEST_ZCASHD_COMPAT_NETWORK=Mainnet \
	TEST_ZEBRAD_RPC_ADDR="$(TEST_ZEBRAD_RPC_ADDR)" \
	TEST_ZCASHD_RPC_ADDR="$(TEST_ZCASHD_RPC_ADDR)" \
	TEST_ZCASHD_COOKIE_FILE="$(TEST_ZCASHD_COOKIE_FILE)" \
	TEST_ZCASHD_RPC_USER="$(TEST_ZCASHD_RPC_USER)" \
	TEST_ZCASHD_RPC_PASSWORD="$(TEST_ZCASHD_RPC_PASSWORD)" \
	cargo nextest run --profile zcashd-compat-external --run-ignored=only

# Run the read-only zcashd-compat test suite against a live testnet deployment.
# Identical to compat-test-mainnet but targets testnet instances.
# All mutation tests (mining, sending) are skipped automatically.
#
# Prerequisites: same as compat-test-mainnet, but with testnet instances.
# When to use: validating a testnet deployment before promoting changes to mainnet.
compat-test-testnet:
	TEST_ZCASHD_COMPAT=1 \
	TEST_ZCASHD_COMPAT_NETWORK=Testnet \
	TEST_ZEBRAD_RPC_ADDR="$(TEST_ZEBRAD_RPC_ADDR)" \
	TEST_ZCASHD_RPC_ADDR="$(TEST_ZCASHD_RPC_ADDR)" \
	TEST_ZCASHD_COOKIE_FILE="$(TEST_ZCASHD_COOKIE_FILE)" \
	TEST_ZCASHD_RPC_USER="$(TEST_ZCASHD_RPC_USER)" \
	TEST_ZCASHD_RPC_PASSWORD="$(TEST_ZCASHD_RPC_PASSWORD)" \
	cargo nextest run --profile zcashd-compat-external --run-ignored=only
