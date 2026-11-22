.PHONY: \
	zakura-build-dev \
	zakura-dev-init \
	zakura-start-dev

ZAKURA_DEV_HOME ?= $(HOME)/.local/zakura-dev
ZAKURA_DEV_CACHE ?= $(ZAKURA_DEV_HOME)/cache
ZAKURA_DEV_IDENTITY ?= $(ZAKURA_DEV_HOME)/identity
ZAKURA_DEV_CONFIG ?= $(ZAKURA_DEV_HOME)/zakura.toml
ZAKURA_DEV_CONFIG_TEMPLATE ?= $(CURDIR)/scripts/make/zakura-dev.toml

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
	mkdir -p "$(ZAKURA_DEV_CACHE)" "$(ZAKURA_DEV_IDENTITY)"; \
	if [ -f "$(ZAKURA_DEV_CONFIG)" ]; then \
		echo "Using existing Zakura dev config: $(ZAKURA_DEV_CONFIG)"; \
	else \
		echo "Rendering Zakura dev config: $(ZAKURA_DEV_CONFIG)"; \
		NETWORK="$(NETWORK)" \
		CACHE_DIR="$(ZAKURA_DEV_CACHE)" \
		IDENTITY_DIR="$(ZAKURA_DEV_IDENTITY)" \
		TEMPLATE="$(ZAKURA_DEV_CONFIG_TEMPLATE)" \
		OUTPUT="$(ZAKURA_DEV_CONFIG)" \
		python3 -c 'import os, pathlib; peers = {"Mainnet": ["a5f148b592404c5fcdc27f06919681c1dceddb274336abf5c91e70780471bc12@165.22.54.66:8234", "10e8b19b6aabb5ccd80d2df14fabc991602aea0cfb82783ebdabb3b1b699e9b7@104.131.184.123:8234", "ca22584f4392a26c46f9553e45db00fc8de06f1ff8f2c48f5d28b5cd58f134b6@159.65.183.89:8234", "27fec1ed7f52ca389f8ad09e79e896fe6f0097dea6d74b71628dce992cb50b57@143.244.184.176:8234", "a4ef400514ea1eaceea73e2b1a86f4102264e88cfc8b3ace9da73d43c7ccb7c2@159.203.38.10:8234", "9addeca7e4ea8aced48fcf9e3a9f8ce21056fecf9c0bbce368b61ec35b4e5bdc@64.227.44.93:8234", "5f0cc2933a058472140348ebe708ff7a10167cd7e9cd96fa19263ecdf62a3fbe@161.35.156.226:8234", "6a13975563f5883fda0b8b0b1cee54163dedf786dc8f1b4990d2130e7cc56b51@139.59.64.115:8234", "a8a2de1c199100dcd1e7985a30a6a07858d45ee3703794d9d214827724067431@168.144.173.250:8234"], "Testnet": ["672cb8d35831d6020f14e35215de0f7fc1ee771ecd3c9d5fe67c32acb0b53fab@167.99.103.111:8234", "3783033db00b137ed3d11cf2d30853fce65db4967d3910bb66ff4d14191764c9@164.92.209.78:8234", "fe1f7911e1675f0f3cfb17033a71ff2221d03d9f170b0209a73deba439886efe@206.189.148.0:8234"]}; network = os.environ["NETWORK"]; peer_lines = "\n".join(f"    \"{peer}\"," for peer in peers[network]); template = pathlib.Path(os.environ["TEMPLATE"]); output = pathlib.Path(os.environ["OUTPUT"]); text = template.read_text(); text = text.replace("@@NETWORK@@", network).replace("@@CACHE_DIR@@", os.environ["CACHE_DIR"]).replace("@@IDENTITY_DIR@@", os.environ["IDENTITY_DIR"]).replace("@@BOOTSTRAP_PEERS@@", peer_lines); output.write_text(text)'; \
	fi

zakura-start-dev: zakura-dev-init
	@if [ ! -x "$(ZAKURA_DEV_ZAKURAD_BIN)" ]; then \
		echo "Missing $(ZAKURA_DEV_ZAKURAD_BIN); run: make zakura-build-dev" >&2; \
		exit 2; \
	fi
	"$(ZAKURA_DEV_ZAKURAD_BIN)" -c "$(ZAKURA_DEV_CONFIG)" start
