.PHONY: pre-release pre-release-version pre-release-state pre-release-packaging sign-release

PRE_RELEASE_WARN_CRATE_VERSION_BUMPS ?= $(if $(CI),0,1)
CRATE_PACKAGING_VERIFY ?= 0

pre-release:
	@test -n "$(RELEASE_TAG)" || { echo "RELEASE_TAG is required, e.g. make pre-release RELEASE_TAG=v1.0.0" >&2; exit 1; }
	@printf '\n==> [1/3] Checking release version\n\n'
	$(MAKE) pre-release-version RELEASE_TAG="$(RELEASE_TAG)" BASE_TAG="$(BASE_TAG)" PRE_RELEASE_WARN_CRATE_VERSION_BUMPS="$(PRE_RELEASE_WARN_CRATE_VERSION_BUMPS)"
	@printf '\n==> [2/3] Checking committed Mainnet release state\n\n'
	$(MAKE) pre-release-state
	@printf '\n==> [3/3] Checking crate packaging\n\n'
	$(MAKE) pre-release-packaging CRATE_PACKAGING_VERIFY="$(CRATE_PACKAGING_VERIFY)"

pre-release-version:
	@test -n "$(RELEASE_TAG)" || { echo "RELEASE_TAG is required, e.g. make pre-release-version RELEASE_TAG=v1.0.0" >&2; exit 1; }
	./scripts/check-release-version.sh "$(RELEASE_TAG)"
	@if [ "$(PRE_RELEASE_WARN_CRATE_VERSION_BUMPS)" = "0" ]; then \
		echo "Skipping changed-crate version advisory."; \
	else \
		BASE_TAG="$(BASE_TAG)" bash ./scripts/check-crate-version-bumps.sh; \
	fi

pre-release-state:
	cargo run --locked \
		-p zakura-utils \
		--features zakura-release-state \
		--bin zakura-release-state \
		-- verify-source \
		--source-dir . \
		--require-bundle-provenance

pre-release-packaging:
	@if [ "$(CRATE_PACKAGING_VERIFY)" = "1" ]; then \
		./scripts/check-crate-packaging.sh --verify; \
	else \
		./scripts/check-crate-packaging.sh; \
	fi

sign-release:
	@test -n "$(TAG)" || { echo "TAG is required, e.g. make sign-release TAG=v1.0.0" >&2; exit 1; }
	./scripts/sign-release.sh "$(TAG)"
