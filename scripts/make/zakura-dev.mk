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
		python3 -c 'import os, pathlib; peers = {"Mainnet": ["078a60720f0d474743a51674b152f49f575704a77905c16475683cff9d18f246@165.22.54.66:8234", "1bb8f2f70bdbf9d44ef2ccc3de1a5cbbba56cea2cf371e227d501ce19a8bdeb9@104.131.184.123:8234", "a98d73e0114f9f8ae3d5a0b4ec0b8cef20b51d715de6898a39fc3f23948196b7@159.65.183.89:8234", "02ef9b05d81ad2f6176a68fd047bda5e4e277d69ac51b3b2536d43ea8f38de1c@143.244.184.176:8234", "0018a0f4fc1f3cc3b4a84e8ebcb77008343c9e54fb80f77fd03271c7a33063b7@159.203.38.10:8234", "4607c5353e2c07ecd10d73c6c19d0b0de092939328f33e8f04d5ced382fadf7b@64.227.44.93:8234", "8ac35544d02141e7a3cc3c8b8661a3e726985a266673b60cf45f5b33c3f36e73@161.35.156.226:8234", "42623a00c2632e0cf99d1c10cec024b0217d12d5fecc91947a3a040f142df06b@139.59.64.115:8234", "ba06d75332e3a430d4e838b5838a2356338124cf2930bb0f8e2962e01223ed95@168.144.173.250:8234"], "Testnet": ["884eb8c4e0d6005bd3a6e086df064efc12b866632dababa4249b2ceb42d43982@167.99.103.111:8234", "1d0dd618bb699637322dcbf20a0830fe9078733ba13c3ee01768bfb518653064@164.92.209.78:8234", "b5410aedf93d00bbc9f93405a4c054b30289da9b76269432a5e6e0e132f0480c@206.189.148.0:8234"]}; network = os.environ["NETWORK"]; peer_lines = "\n".join(f"    \"{peer}\"," for peer in peers[network]); template = pathlib.Path(os.environ["TEMPLATE"]); output = pathlib.Path(os.environ["OUTPUT"]); text = template.read_text(); text = text.replace("@@NETWORK@@", network).replace("@@CACHE_DIR@@", os.environ["CACHE_DIR"]).replace("@@IDENTITY_DIR@@", os.environ["IDENTITY_DIR"]).replace("@@BOOTSTRAP_PEERS@@", peer_lines); output.write_text(text)'; \
	fi

zakura-start-dev: zakura-dev-init
	@if [ ! -x "$(ZAKURA_DEV_ZAKURAD_BIN)" ]; then \
		echo "Missing $(ZAKURA_DEV_ZAKURAD_BIN); run: make zakura-build-dev" >&2; \
		exit 2; \
	fi
	"$(ZAKURA_DEV_ZAKURAD_BIN)" -c "$(ZAKURA_DEV_CONFIG)" start
