.PHONY: \
	zakura-build-dev \
	zakura-dev-init \
	zakura-start-dev

ZAKURA_DEV_HOME ?= $(HOME)/.local/zakura-dev
ZAKURA_DEV_CACHE ?= $(ZAKURA_DEV_HOME)/cache
ZAKURA_DEV_IDENTITY ?= $(ZAKURA_DEV_HOME)/identity
ZAKURA_DEV_TRACES ?= $(ZAKURA_DEV_HOME)/traces
ZAKURA_DEV_CONFIG ?= $(ZAKURA_DEV_HOME)/zakura.toml
ZAKURA_DEV_CONFIG_TEMPLATE ?= $(CURDIR)/make/zakura-dev.toml
ZAKURA_DEV_RENDER_CONFIG := $(CURDIR)/make/zakura-dev-render-config.py
ZAKURA_DEV_ENSURE_TRACE_DIR := $(CURDIR)/make/zakura-dev-ensure-trace-dir.py

ZAKURA_DEV_ZAKURAD_BIN ?= $(CURDIR)/target/release/zakurad
ifneq ($(filter command line environment override,$(origin ZAKURAD_BIN)),)
ZAKURA_DEV_ZAKURAD_BIN := $(ZAKURAD_BIN)
endif
NETWORK ?= Mainnet

zakura-build-dev:
	cargo build --release --bin zakurad

zakura-dev-init:
	@set -eu; \
	case "$(NETWORK)" in \
		Mainnet|Testnet) ;; \
		*) echo "unsupported NETWORK=$(NETWORK); use Mainnet or Testnet" >&2; exit 2 ;; \
	esac; \
	mkdir -p "$(ZAKURA_DEV_CACHE)" "$(ZAKURA_DEV_IDENTITY)" "$(ZAKURA_DEV_TRACES)"; \
	if [ -f "$(ZAKURA_DEV_CONFIG)" ]; then \
		echo "Using existing Zakura dev config: $(ZAKURA_DEV_CONFIG)"; \
		CONFIG="$(ZAKURA_DEV_CONFIG)" TRACE_DIR="$(ZAKURA_DEV_TRACES)" \
			python3 "$(ZAKURA_DEV_ENSURE_TRACE_DIR)"; \
	else \
		echo "Rendering Zakura dev config: $(ZAKURA_DEV_CONFIG)"; \
		NETWORK="$(NETWORK)" \
		CACHE_DIR="$(ZAKURA_DEV_CACHE)" \
		IDENTITY_DIR="$(ZAKURA_DEV_IDENTITY)" \
		TRACE_DIR="$(ZAKURA_DEV_TRACES)" \
		TEMPLATE="$(ZAKURA_DEV_CONFIG_TEMPLATE)" \
		OUTPUT="$(ZAKURA_DEV_CONFIG)" \
			python3 "$(ZAKURA_DEV_RENDER_CONFIG)"; \
	fi

zakura-start-dev: zakura-dev-init
	@if [ ! -x "$(ZAKURA_DEV_ZAKURAD_BIN)" ]; then \
		echo "Missing $(ZAKURA_DEV_ZAKURAD_BIN); run: make zakura-build-dev" >&2; \
		exit 2; \
	fi
	"$(ZAKURA_DEV_ZAKURAD_BIN)" -c "$(ZAKURA_DEV_CONFIG)" start
