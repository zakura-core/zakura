# Temporary PR CI during an Ubuntu outage

The temporary runner keeps formatting and script checks, Rust lint/build checks,
header conformance, changelog validation, and the normal workspace unit-test
profile. Extended tests and the workflows listed in `set-temporary-mode.sh` are
paused. The existing `test success` check still requires unit tests to pass.
A newer commit cancels the previous lint, unit-test, and runner-smoke runs for
the same PR, including their queued jobs. Different PRs retain separate runs.
Each workflow summary records the reduced coverage. This mode also reduces
push, manual, and scheduled coverage until it is turned off.

## Prepare the dedicated host

Provision a Valargroup Ubuntu 26.04 machine with 16 dedicated CPUs, 32 GiB RAM,
and 400 GiB disk. Authorize only the operator's public SSH key. Do not copy a
private key, cloud token, global environment file, or tailnet identity to it.
Confirm its project and price before provisioning. The initial operating window
is 24 hours; the operator must restore normal CI and delete the Droplet when
that window ends, or explicitly extend it. Powering it off does not stop billing.

Copy this directory to the host. As root, run `bootstrap.sh`; it installs build
dependencies through Princeton's Ubuntu mirror while retaining Ubuntu's signed
package metadata. Then pipe a short-lived repository runner-registration token
to `install-runner.sh`. Fetch that token on the operator machine:

```sh
gh api -X POST \
  repos/zakura-core/zakura/actions/runners/registration-token --jq .token \
  | ssh runner-host 'bash /root/ci/install-runner.sh'
```

Pipe the output directly over verified SSH into the installer; do not display,
log, or save it. No GitHub administration credential belongs on the runner.
The installer verifies the pinned Actions runner archive before registering it.

The service accepts one job at a time as an account without sudo. It starts a
new runner process for each job, clears the job's files, supplies independent
Rust toolchains and a Python virtual environment, and gives it private temporary
and device filesystems. The prepared toolchains and original runner registration
remain root-owned. Job traffic to cloud metadata, private networks, and SSH is
blocked. Loopback remains available for tests. This is a dedicated self-hosted
machine, not a separate virtual machine for every job: jobs still share its
kernel. External-contributor workflow approval must remain enabled.

The runner service deliberately does not start at boot: its network restrictions
must be restored and verified before it is started again after a reboot. Do not
add this service to a production, continuous-sync, or development-worker host.

## Activate and validate

Once the runner is online, run `bash deploy/ci/set-temporary-mode.sh on` on the
operator machine. It saves workflow states outside Git, enables
`TEMPORARY_PR_CI`, and disables only the listed CI workflows. It does not touch
deployment, release, or fleet-monitoring workflows. Merge the workflow changes
through the normal PR process; existing PR heads need a new run to use them.
Do not treat a partially completed activation as success; the saved snapshot is
retained if an API operation fails, so the operator can restore the old state.

The `Temporary runner checks` workflow verifies installed tools, account
privileges, blocked metadata access, and removal of a file written by a previous
job. Keep the PR draft until these checks and the retained lint and unit-test
jobs pass. Dependency installation on the prepared host verifies the installed
packages without contacting Ubuntu's package servers.

## Restore normal CI

On the same operator machine, run:

```sh
bash deploy/ci/set-temporary-mode.sh off
```

This restores only workflows that were active in the saved snapshot and turns
off the temporary routing. Before deleting the host, wait for its running job to
finish, stop `zakura-ci-runner.service`, remove `zakura-ci-temporary-01` from
GitHub's repository runner settings, and delete the exact temporary Droplet.
Check for queued jobs targeting its label before removing it. The host must not
be reused for privileged workflows or another repository.
