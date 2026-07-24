.PHONY: prepare-release prepare-release-changelog pre-release \
	pre-release-changelog pre-release-version pre-release-requirements \
	pre-release-state pre-release-packaging sign-release

PRE_RELEASE_WARN_CRATE_VERSION_BUMPS ?= $(if $(CI),0,1)
CRATE_PACKAGING_VERIFY ?= 0

prepare-release:
	@test -n "$(RELEASE_TAG)" || { echo "RELEASE_TAG is required, e.g. make prepare-release RELEASE_TAG=v1.0.0" >&2; exit 1; }
	./scripts/prepare-release.sh --release-tag "$(RELEASE_TAG)" \
		$(if $(BASE_TAG),--base-tag "$(BASE_TAG)") \
		$(if $(filter 1,$(NO_CRATES)),--no-crates) \
		$(if $(filter 1,$(DRY_RUN)),--dry-run)

prepare-release-changelog:
	@test -n "$(RELEASE_TAG)" || { echo "RELEASE_TAG is required, e.g. make prepare-release-changelog RELEASE_TAG=v1.0.0" >&2; exit 1; }
	./scripts/check-release-version.sh "$(RELEASE_TAG)"
	./scripts/changelog.py release "$(RELEASE_TAG)" $(if $(CHANGELOG_DATE),--date "$(CHANGELOG_DATE)")

pre-release:
	@test -n "$(RELEASE_TAG)" || { echo "RELEASE_TAG is required, e.g. make pre-release RELEASE_TAG=v1.0.0" >&2; exit 1; }
	@printf '\n==> [1/5] Checking release version\n\n'
	$(MAKE) pre-release-version RELEASE_TAG="$(RELEASE_TAG)" BASE_TAG="$(BASE_TAG)" PRE_RELEASE_WARN_CRATE_VERSION_BUMPS="$(PRE_RELEASE_WARN_CRATE_VERSION_BUMPS)"
	@printf '\n==> [2/5] Checking internal dependency requirements\n\n'
	$(MAKE) pre-release-requirements RELEASE_TAG="$(RELEASE_TAG)"
	@printf '\n==> [3/5] Checking assembled changelog\n\n'
	$(MAKE) pre-release-changelog RELEASE_TAG="$(RELEASE_TAG)"
	@printf '\n==> [4/5] Checking committed Mainnet release state\n\n'
	$(MAKE) pre-release-state
	@printf '\n==> [5/5] Checking crate packaging\n\n'
	$(MAKE) pre-release-packaging CRATE_PACKAGING_VERIFY="$(CRATE_PACKAGING_VERIFY)"

pre-release-changelog:
	@test -n "$(RELEASE_TAG)" || { echo "RELEASE_TAG is required, e.g. make pre-release-changelog RELEASE_TAG=v1.0.0" >&2; exit 1; }
	@./scripts/changelog.py release "$(RELEASE_TAG)" --check || { \
		echo "WARNING: release changelog is stale; run make prepare-release-changelog RELEASE_TAG=$(RELEASE_TAG), review the diff, and commit it." >&2; \
		exit 1; \
	}

pre-release-version:
	@test -n "$(RELEASE_TAG)" || { echo "RELEASE_TAG is required, e.g. make pre-release-version RELEASE_TAG=v1.0.0" >&2; exit 1; }
	./scripts/check-release-version.sh "$(RELEASE_TAG)"
	@if [ "$(PRE_RELEASE_WARN_CRATE_VERSION_BUMPS)" = "0" ]; then \
		echo "Skipping changed-crate version advisory."; \
	else \
		BASE_TAG="$(BASE_TAG)" bash ./scripts/check-crate-version-bumps.sh; \
	fi

pre-release-requirements:
	@test -n "$(RELEASE_TAG)" || { echo "RELEASE_TAG is required, e.g. make pre-release-requirements RELEASE_TAG=v1.0.0" >&2; exit 1; }
	./scripts/check-release-requirements.sh "$(RELEASE_TAG)"

pre-release-state:
	./scripts/check-release-state.sh

pre-release-packaging:
	@if [ "$(CRATE_PACKAGING_VERIFY)" = "1" ]; then \
		./scripts/check-crate-packaging.sh --verify; \
	else \
		./scripts/check-crate-packaging.sh; \
	fi

sign-release:
	@test -n "$(TAG)" || { echo "TAG is required, e.g. make sign-release TAG=v1.0.0" >&2; exit 1; }
	./scripts/sign-release.sh "$(TAG)"
