# Release Approval with a Security Key

Anyone with write access can start a release with the `Create release`
workflow. Before it publishes anything, a release signer approves it with
`scripts/approve-release.sh` and their YubiKey. The
script signs exactly what is about to ship, and the workflow refuses to tag
or publish unless that signature is valid. The GitHub **Approve and deploy**
button on its own is not enough. If someone clicks it, or a stolen GitHub
session does, the publish job fails before any tag exists.

The signature is attached to every release as `release-approval.sig` and
`release-approval.txt`, so users can check it themselves (see
[verify.md](verify.md)).

## One-time setup

You need a YubiKey 5 series key (firmware 5.2.3 or newer), a Mac or Linux
machine, and a clone of this repository. Other FIDO2 keys and software
authenticators are rejected: the key must be attested by Yubico.

1. Install tools and sign in to GitHub. On macOS the built-in `ssh-keygen`
   cannot use security keys, so install Homebrew's:

   ```bash
   brew install openssh ykman gh
   gh auth login
   ```

   The approve script uses your `gh` login to read the run and post the
   approval as you. That login alone cannot release anything: the publish job
   also requires your YubiKey signature.

2. Plug in the key and give it a FIDO2 PIN if it has none.
   `ykman fido info` shows `PIN: Not set` if so.

   ```bash
   ykman fido access change-pin
   ```

3. From a branch off `main`, with only this YubiKey plugged in, create your
   release key. Enter the PIN and touch the key when asked.

   ```bash
   ./scripts/enroll-release-key.sh <your-github-username>
   ```

   Use your GitHub username, the name in your profile URL, for example
   `./scripts/enroll-release-key.sh ebfull`.

   This writes the key handle to `~/.ssh/id_ed25519_sk_zakura_release` (only a
   handle: useless without the YubiKey and its PIN), adds a line for the key to
   [`.github/release-signers`](../.github/release-signers), and saves the
   YubiKey's attestation to
   `.github/release-attestations/<username>-<key fingerprint>.attestation`.
   It then checks that the attestation chains to Yubico.

   **Backup YubiKey.** Plug in only the backup and run:

   ```bash
   ./scripts/enroll-release-key.sh <your-github-username> --key ~/.ssh/id_ed25519_sk_zakura_release_backup
   ```

4. Commit the changed files (`.github/release-signers` and one attestation per
   key) and open a PR.

5. Check that you are a required reviewer on the `release` environment:

   ```bash
   gh api repos/zakura-core/zakura/environments/release \
     --jq '[.protection_rules[] | select(.type == "required_reviewers") | .reviewers[].reviewer.login]'
   ```

   If your username is not in the list, ask a repository admin to add you
   (Settings > Environments > release > Required reviewers).

## Approving a release

1. Whoever started the release sends you the `Create release` run link. The
   run waits at "Publish release and create tag" once the build is done.
2. From an up-to-date checkout of `main`, with the key plugged in, run:

   ```bash
   ./scripts/approve-release.sh <run URL or id>
   ```

   With your backup YubiKey, add
   `--key ~/.ssh/id_ed25519_sk_zakura_release_backup`.

3. Read what it shows: the tag, the commit and its subject, the job results
   (including the `us-east-0` start canary), and the staged assets, whose
   checksums the script re-checks on your machine. If anything looks wrong,
   stop. The run waits, and nothing is published.
4. Type the tag to confirm, enter your PIN, and touch the key within about 30
   seconds. The script approves the run with your signature.
5. The publish job verifies the signature, creates the tag and pre-release, and
   attaches the signature. You are done.

## Troubleshooting

| What you see | What to do |
| --- | --- |
| `invalid format` while signing or enrolling | The touch timed out. Run the script again. Nothing was approved. |
| `failed the YubiKey attestation check` while enrolling | The key is not a genuine YubiKey, or its firmware predates Ed25519 (5.2.3). |
| `No FIDO SecurityKeyProvider specified` | macOS `ssh-keygen` was used. Install Homebrew's OpenSSH (step 1). |
| `gh is not signed in` | Run `gh auth login` (step 1). |
| `your GitHub account is not a reviewer` | Ask an admin to add you to the `release` environment (step 5). |
| Publish job failed with "no security-key signature" | Someone clicked the GitHub button. Nothing was tagged, so start a new `Create release` run for the same tag and approve that one with the script. Avoid "Re-run failed jobs": in testing, GitHub left those re-runs stuck in "queued". |
| Publish job failed with "not from exactly one listed release signer" | Your key is not in `.github/release-signers` at the release commit. |
| PIN locked after too many wrong tries | `ykman fido reset` wipes the key. Remove its line and attestation, move the old handle aside, and enroll again (step 3). |
| Lost or replaced key | Open a PR removing that key's line from `.github/release-signers` and its attestation file (the fingerprint in the file name matches the key). Your backup key keeps working meanwhile. Enroll a replacement with step 3. |
| New machine | With the YubiKey plugged in, `ssh-keygen -K` recovers its handle; save it as `~/.ssh/id_ed25519_sk_zakura_release`. |

## How the check works

The script signs this message with the `zakura-release` SSH signature
namespace:

```text
zakura-release-approval v1
repository zakura-core/zakura
tag v1.8.0
commit <full commit SHA>
sha256sums <SHA-256 of SHA256SUMS.txt, or none for a source-first release>
```

[`release_approval.py`](../.github/scripts/release_approval.py) accepts the
approval only if:

- the signature comes from a key listed in `.github/release-signers`,
- it records both a touch and a PIN (`ssh-keygen -Y verify` ignores those two
  flags, so the script reads them from the signature itself), and
- that signer's attestation proves the key was generated on a YubiKey: its
  certificate chains to a Yubico FIDO root pinned in `.github/yubico-ca/`, and
  it attests exactly the listed key.

Because the message names the tag, commit, and checksums, a signature cannot
approve any other release or different binaries.
