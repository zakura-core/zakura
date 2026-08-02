.PHONY: install-branch

ZAKURA_GIT_REPOSITORY ?= https://github.com/zakura-core/zakura.git

install-branch:
	@if [ -z "$${BRANCH:-}" ]; then \
		echo "warning: BRANCH is not set; defaulting to main" >&2; \
	fi
	cargo install \
		--git "$(ZAKURA_GIT_REPOSITORY)" \
		--branch "$${BRANCH:-main}" \
		zakura
