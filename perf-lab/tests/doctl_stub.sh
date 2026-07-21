#!/usr/bin/env bash
# perf-lab/tests/doctl_stub.sh — records argv; fakes minimal JSON output.
echo "$@" >> "${DOCTL_LOG:?}"
case "$*" in
  *"droplet get"*)
    # simulate an UNTAGGED droplet named like ours
    echo '[{"id":123,"name":"perf-lab-x","tags":[]}]' ;;
  *"droplet list"*) echo '[]' ;;
  *) echo '[]' ;;
esac
