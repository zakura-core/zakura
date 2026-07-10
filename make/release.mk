.PHONY: sign-release

sign-release:
	@test -n "$(TAG)" || { echo "TAG is required, e.g. make sign-release TAG=v1.0.0" >&2; exit 1; }
	./scripts/sign-release.sh "$(TAG)"
