#!/usr/bin/env python3
"""Sign and verify security-key release approvals.

A release approval is an SSH signature, made with a FIDO2 security key
(`sk-ssh-ed25519` or `sk-ecdsa`), over the repository, tag, commit, and the
SHA-256 of the staged `SHA256SUMS.txt` that a `Create release` run is about to
publish. `scripts/approve-release.sh` makes it and posts it as the `release`
environment approval comment. The publish job verifies it before minting the
release token, then attaches it to the release so anyone can check it against
`.github/release-signers` (see docs/verify.md).

`ssh-keygen -Y verify` checks the cryptography but ignores the FIDO flags, and
its allowed-signers format rejects `verify-required`. This script therefore
parses the signature itself and requires both the user-presence (touch) and
user-verification (PIN) flags. Those flags are covered by the signature, so
they cannot be edited without breaking it.

A `sk-` key type alone does not prove hardware, since software can emulate a
FIDO authenticator. Each listed key therefore also has the attestation that
`scripts/enroll-release-key.sh` saved when the key was created on the
YubiKey. It must chain to a pinned Yubico FIDO root and attest that exact key.
A signer may list several keys, such as a backup YubiKey, each with its own
attestation.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import shutil
import struct
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

NAMESPACE = "zakura-release"
# Approval comments carry the signature after this marker.
COMMENT_PREFIX = "zakura-release-approval "
SK_KEY_TYPES = frozenset(
    {
        "sk-ssh-ed25519@openssh.com",
        "sk-ecdsa-sha2-nistp256@openssh.com",
    }
)
FLAG_USER_PRESENT = 0x01
FLAG_USER_VERIFIED = 0x04
FLAG_ATTESTED_CREDENTIAL = 0x40
ATTESTATION_MAGIC = b"ssh-sk-attest-v01"
MAX_ATTESTATION_BYTES = 16 * 1024
# A security-key SSHSIG blob is a few hundred bytes; bound untrusted input.
MAX_SIGNATURE_CHARS = 4096
MAX_APPROVALS = 50

REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
TAG_RE = re.compile(r"^v[0-9A-Za-z.+-]+$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256SUMS_RE = re.compile(r"^(?:[0-9a-f]{64}|none)$")
PRINCIPAL_RE = re.compile(r"^[A-Za-z0-9-]+$")
SIGNER_RE = re.compile(
    r'^(?P<principal>\S+) namespaces="(?P<namespace>[^"]*)" '
    r"(?P<key_type>\S+) (?P<key>[A-Za-z0-9+/=]+)(?: .*)?$"
)


class ApprovalError(Exception):
    """A release approval or signer list was rejected."""


@dataclass(frozen=True)
class SignatureInfo:
    """Fields parsed from an SSHSIG blob, before cryptographic verification."""

    key_type: str
    namespace: str
    flags: int
    public_key: bytes


@dataclass(frozen=True)
class SignerKey:
    """One key line from a release-signers list."""

    principal: str
    key_type: str
    key: str
    line: str

    @property
    def attestation_name(self) -> str:
        """Names this key's attestation file: the principal plus a key fingerprint."""
        fingerprint = hashlib.sha256(base64.b64decode(self.key)).hexdigest()[:16]
        return f"{self.principal}-{fingerprint}.attestation"


def approval_message(repository: str, tag: str, commit: str, sha256sums: str) -> bytes:
    """Returns the canonical signed message, rejecting malformed fields.

    `sha256sums` is the hex SHA-256 of `SHA256SUMS.txt`, or `none` for a
    source-first release that has no staged assets. The field checks keep a
    value from smuggling a newline into the message and impersonating another
    field.
    """
    if not REPOSITORY_RE.fullmatch(repository):
        raise ApprovalError(f"Invalid repository {repository!r}.")
    if not TAG_RE.fullmatch(tag):
        raise ApprovalError(f"Invalid release tag {tag!r}.")
    if not COMMIT_RE.fullmatch(commit):
        raise ApprovalError(f"Invalid commit {commit!r}; expected a full SHA-1.")
    if not SHA256SUMS_RE.fullmatch(sha256sums):
        raise ApprovalError(f"Invalid SHA256SUMS.txt digest {sha256sums!r}.")
    return (
        "zakura-release-approval v1\n"
        f"repository {repository}\n"
        f"tag {tag}\n"
        f"commit {commit}\n"
        f"sha256sums {sha256sums}\n"
    ).encode()


def decode_signature(text: str) -> bytes:
    """Decodes a compact or armored signature into the raw SSHSIG blob."""
    if len(text) > MAX_SIGNATURE_CHARS:
        raise ApprovalError("Signature is too long.")
    body = re.sub(r"-----(BEGIN|END) SSH SIGNATURE-----", "", text)
    body = "".join(body.split())
    if not body:
        raise ApprovalError("Signature is empty.")
    try:
        return base64.b64decode(body, validate=True)
    except ValueError as error:
        raise ApprovalError("Signature is not valid base64.") from error


def armor(blob: bytes) -> str:
    """Wraps a raw SSHSIG blob in the armor `ssh-keygen -Y verify` reads."""
    encoded = base64.b64encode(blob).decode()
    lines = [encoded[i : i + 70] for i in range(0, len(encoded), 70)]
    return "-----BEGIN SSH SIGNATURE-----\n" + "\n".join(lines) + "\n-----END SSH SIGNATURE-----\n"


class _Reader:
    """Reads SSH wire-format fields, failing closed on truncation."""

    def __init__(self, data: bytes) -> None:
        self.data = data
        self.offset = 0

    def take(self, size: int) -> bytes:
        end = self.offset + size
        if end > len(self.data):
            raise ApprovalError("Signature is truncated.")
        chunk = self.data[self.offset : end]
        self.offset = end
        return chunk

    def u32(self) -> int:
        return struct.unpack(">I", self.take(4))[0]

    def string(self) -> bytes:
        return self.take(self.u32())

    def done(self) -> bool:
        return self.offset == len(self.data)


def parse_signature(blob: bytes) -> SignatureInfo:
    """Parses an SSHSIG blob and the FIDO flags inside its signature."""
    outer = _Reader(blob)
    if outer.take(6) != b"SSHSIG" or outer.u32() != 1:
        raise ApprovalError("Signature is not an SSHSIG v1 signature.")
    public_key_blob = outer.string()
    public_key = _Reader(public_key_blob)
    namespace = outer.string().decode(errors="replace")
    outer.string()  # reserved
    outer.string()  # hash algorithm
    signature = _Reader(outer.string())
    if not outer.done():
        raise ApprovalError("Signature has trailing data.")

    key_type = public_key.string().decode(errors="replace")
    if key_type not in SK_KEY_TYPES:
        raise ApprovalError(f"Signature uses a {key_type} key; a FIDO security key is required.")
    if signature.string().decode(errors="replace") != key_type:
        raise ApprovalError("Signature type does not match the signing key.")
    signature.string()  # raw signature
    flags = signature.take(1)[0]
    signature.u32()  # signature counter
    if not signature.done():
        raise ApprovalError("Signature has trailing data after its counter.")
    return SignatureInfo(
        key_type=key_type, namespace=namespace, flags=flags, public_key=public_key_blob
    )


def parse_signers(text: str) -> list[SignerKey]:
    """Parses a release-signers list into one `SignerKey` per line.

    Every entry must be a security key scoped to the release namespace. A
    principal may list several keys, but each key appears only once.
    """
    entries = []
    seen_keys = set()
    for number, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = SIGNER_RE.fullmatch(line)
        if match is None:
            raise ApprovalError(f"Release signers line {number} is malformed.")
        if not PRINCIPAL_RE.fullmatch(match["principal"]):
            raise ApprovalError(f"Release signers line {number} has an invalid principal.")
        if match["namespace"] != NAMESPACE:
            raise ApprovalError(
                f'Release signers line {number} must be scoped to namespaces="{NAMESPACE}".'
            )
        if match["key_type"] not in SK_KEY_TYPES:
            raise ApprovalError(
                f"Release signers line {number} is a {match['key_type']} key, "
                "not a FIDO security key."
            )
        if match["key"] in seen_keys:
            raise ApprovalError(f"Release signers line {number} repeats an earlier key.")
        seen_keys.add(match["key"])
        entries.append(SignerKey(match["principal"], match["key_type"], match["key"], line))
    if not entries:
        raise ApprovalError("No release signers are configured.")
    return entries


def check_signers(text: str) -> str:
    """Validates a release-signers list (see `parse_signers`) and returns it normalized."""
    return "".join(entry.line + "\n" for entry in parse_signers(text))


def _cbor_item(data: bytes, offset: int, depth: int = 0):
    """Decodes one CBOR item of the kinds COSE keys use; returns `(value, offset)`."""
    if depth > 4 or offset >= len(data):
        raise ApprovalError("Attestation key data is malformed.")
    major, info = data[offset] >> 5, data[offset] & 0x1F
    offset += 1
    if info < 24:
        value = info
    elif info in (24, 25, 26, 27):
        size = 1 << (info - 24)
        if offset + size > len(data):
            raise ApprovalError("Attestation key data is truncated.")
        value = int.from_bytes(data[offset : offset + size], "big")
        offset += size
    else:
        raise ApprovalError("Attestation key data uses unsupported CBOR.")
    if major == 0:
        return value, offset
    if major == 1:
        return -1 - value, offset
    if major in (2, 3):
        if offset + value > len(data):
            raise ApprovalError("Attestation key data is truncated.")
        return data[offset : offset + value], offset + value
    if major == 5 and value <= 16:
        result = {}
        for _ in range(value):
            key, offset = _cbor_item(data, offset, depth + 1)
            result[key], offset = _cbor_item(data, offset, depth + 1)
        return result, offset
    raise ApprovalError("Attestation key data uses unsupported CBOR.")


def _openssl(*args: str, data: bytes | None = None) -> subprocess.CompletedProcess:
    """Runs `openssl` with arguments, failing closed when it is missing."""
    found = shutil.which("openssl")
    if found is None:
        raise ApprovalError("openssl is not installed.")
    return subprocess.run([found, *args], input=data, capture_output=True, check=False)


def check_attestation(
    principal: str, key_type: str, key: str, attestation: bytes, yubico_ca: Path
) -> None:
    """Checks that a signer's key was generated on a genuine YubiKey.

    `attestation` is the `ssh-keygen -O write-attestation` output made with the
    challenge `zakura-release-attestation:<principal>`. Its certificate must
    chain to a root in `yubico_ca/roots.pem`, its signature must cover the
    authenticator data, and that data must attest exactly `key`.
    """
    if len(attestation) > MAX_ATTESTATION_BYTES:
        raise ApprovalError(f"Attestation for {principal} is too large.")
    reader = _Reader(attestation)
    if reader.string() != ATTESTATION_MAGIC:
        raise ApprovalError(f"Attestation for {principal} is not an OpenSSH FIDO attestation.")
    certificate, signature, encoded = reader.string(), reader.string(), reader.string()
    auth_data, end = _cbor_item(encoded, 0)
    if not isinstance(auth_data, bytes) or end != len(encoded) or len(auth_data) < 55:
        raise ApprovalError(f"Attestation for {principal} has malformed authenticator data.")

    public = _Reader(base64.b64decode(key))
    if public.string().decode() != key_type:
        raise ApprovalError(f"Release signer key for {principal} does not match its type.")
    if key_type == "sk-ssh-ed25519@openssh.com":
        point = public.string()
        expected = {1: 1, 3: -8, -1: 6, -2: point}
    else:
        public.string()  # curve name
        point = public.string()
        expected = {1: 2, 3: -7, -1: 1, -2: point[1:33], -3: point[33:]}
    application = public.string()

    if auth_data[:32] != hashlib.sha256(application).digest():
        raise ApprovalError(f"Attestation for {principal} is for a different application.")
    if not auth_data[32] & FLAG_ATTESTED_CREDENTIAL:
        raise ApprovalError(f"Attestation for {principal} attests no credential.")
    id_length = struct.unpack(">H", auth_data[53:55])[0]
    cose_key, _ = _cbor_item(auth_data, 55 + id_length)
    if not isinstance(cose_key, dict) or any(cose_key.get(k) != v for k, v in expected.items()):
        raise ApprovalError(f"Attestation for {principal} attests a different key.")

    with tempfile.TemporaryDirectory() as workdir:
        leaf = Path(workdir, "leaf.pem")
        converted = _openssl("x509", "-inform", "der", "-out", str(leaf), data=certificate)
        # -trusted makes the pinned roots the only trust anchors. With -CAfile,
        # OpenSSL also trusts the system CA directory, so any public CA would pass.
        chained = _openssl(
            "verify",
            "-trusted", str(yubico_ca / "roots.pem"),
            "-untrusted", str(yubico_ca / "intermediates.pem"),
            str(leaf),
        )
        if converted.returncode != 0 or chained.returncode != 0:
            raise ApprovalError(
                f"Attestation for {principal} does not chain to a Yubico FIDO root."
            )
        leaf_key = Path(workdir, "leaf-key.pem")
        leaf_key.write_bytes(_openssl("x509", "-pubkey", "-noout", "-in", str(leaf)).stdout)
        signature_path = Path(workdir, "signature")
        signature_path.write_bytes(signature)
        challenge = f"zakura-release-attestation:{principal}".encode()
        signed = auth_data + hashlib.sha256(challenge).digest()
        checked = _openssl(
            "dgst", "-sha256", "-verify", str(leaf_key), "-signature", str(signature_path),
            data=signed,
        )
        if checked.returncode != 0:
            raise ApprovalError(f"Attestation signature for {principal} is invalid.")


def check_key_attestation(entry: SignerKey, attestations: Path, yubico_ca: Path) -> None:
    """Checks one listed key against its attestation file (see `check_attestation`)."""
    path = attestations / entry.attestation_name
    if not path.is_file():
        raise ApprovalError(f"No YubiKey attestation for {entry.principal} at {path}.")
    check_attestation(entry.principal, entry.key_type, entry.key, path.read_bytes(), yubico_ca)


def check_signer_attestations(signers: str, attestations: Path, yubico_ca: Path) -> list[str]:
    """Checks every listed key's attestation and returns their attestation names."""
    entries = parse_signers(signers)
    for entry in entries:
        check_key_attestation(entry, attestations, yubico_ca)
    return [entry.attestation_name for entry in entries]


def _ssh_keygen() -> str:
    """Finds `ssh-keygen`, preferring `$SSH_KEYGEN` and then Homebrew's FIDO build."""
    if os.environ.get("SSH_KEYGEN"):
        return os.environ["SSH_KEYGEN"]
    # macOS's bundled ssh-keygen has no FIDO provider and cannot sign.
    homebrew = Path("/opt/homebrew/opt/openssh/bin/ssh-keygen")
    if sys.platform == "darwin" and homebrew.exists():
        return str(homebrew)
    found = shutil.which("ssh-keygen")
    if found is None:
        raise ApprovalError("ssh-keygen is not installed.")
    return found


def verify(signers: str, signature: str, message: bytes) -> SignerKey:
    """Verifies one signature over `message` and returns the listed key that made it."""
    entries = parse_signers(signers)
    signers = check_signers(signers)
    blob = decode_signature(signature)
    info = parse_signature(blob)
    if info.namespace != NAMESPACE:
        raise ApprovalError(f"Signature namespace {info.namespace!r} is not {NAMESPACE!r}.")
    if not info.flags & FLAG_USER_PRESENT:
        raise ApprovalError("Signature was made without touching the security key.")
    if not info.flags & FLAG_USER_VERIFIED:
        raise ApprovalError("Signature was made without the security key's PIN.")

    keygen = _ssh_keygen()
    with tempfile.TemporaryDirectory() as workdir:
        signers_path = Path(workdir, "release-signers")
        signature_path = Path(workdir, "release-approval.sig")
        signers_path.write_text(signers)
        signature_path.write_text(armor(blob))
        found = subprocess.run(
            [keygen, "-Y", "find-principals", "-f", signers_path, "-s", signature_path],
            capture_output=True,
            text=True,
            check=False,
        )
        principals = found.stdout.split()
        if found.returncode != 0 or len(principals) != 1:
            raise ApprovalError("Signature is not from exactly one listed release signer.")
        principal = principals[0]
        checked = subprocess.run(
            [
                keygen, "-Y", "verify", "-f", signers_path, "-I", principal,
                "-n", NAMESPACE, "-s", signature_path,
            ],
            input=message,
            capture_output=True,
            check=False,
        )
        if checked.returncode != 0:
            raise ApprovalError(f"Signature from {principal} does not approve this release.")
    for entry in entries:
        if entry.principal == principal and base64.b64decode(entry.key) == info.public_key:
            return entry
    raise ApprovalError("Signature key is not listed for its principal.")


def approval_signatures(approvals: list) -> list[tuple[str, str]]:
    """Returns `(approver, signature)` for each `release` approval with a signature.

    `approvals` is the JSON from GitHub's run approvals endpoint.
    """
    if not isinstance(approvals, list):
        raise ApprovalError("Run approvals are not a list.")
    found = []
    for approval in approvals[:MAX_APPROVALS]:
        if not isinstance(approval, dict) or approval.get("state") != "approved":
            continue
        environments = approval.get("environments") or []
        if not any(isinstance(env, dict) and env.get("name") == "release" for env in environments):
            continue
        comment = approval.get("comment") or ""
        if not isinstance(comment, str) or COMMENT_PREFIX not in comment:
            continue
        approver = (approval.get("user") or {}).get("login", "unknown")
        found.append((approver, comment.split(COMMENT_PREFIX, 1)[1].strip()))
    return found


def verify_approvals(
    signers: str, approvals: list, message: bytes, attestations: Path, yubico_ca: Path
) -> tuple[str, str, bytes]:
    """Finds a valid signed approval and returns `(approver, principal, blob)`.

    Any one valid signature suffices, from a signer whose YubiKey attestation
    checks out. A button-only approval, or one whose signature fails, is
    reported but cannot satisfy the gate.
    """
    candidates = approval_signatures(approvals)
    if not candidates:
        raise ApprovalError(
            "The release approval carries no security-key signature, so nothing was "
            "published. Start a new Create release run for the same tag and approve it "
            "with scripts/approve-release.sh, not the GitHub button."
        )
    failures = []
    for approver, signature in candidates:
        try:
            entry = verify(signers, signature, message)
            check_key_attestation(entry, attestations, yubico_ca)
        except ApprovalError as error:
            failures.append(f"approval by {approver}: {error}")
            continue
        return approver, entry.principal, decode_signature(signature)
    raise ApprovalError("No valid signed approval. " + " ".join(failures))


def sign(key: str, message: bytes) -> str:
    """Signs `message` with a security key and returns the compact signature."""
    with tempfile.TemporaryDirectory() as workdir:
        message_path = Path(workdir, "message")
        message_path.write_bytes(message)
        subprocess.run(
            [_ssh_keygen(), "-Y", "sign", "-n", NAMESPACE, "-f", key, message_path],
            check=True,
        )
        blob = decode_signature(Path(workdir, "message.sig").read_text())
    info = parse_signature(blob)
    if not info.flags & FLAG_USER_VERIFIED:
        raise ApprovalError(
            "This key signs without a PIN, so CI will reject it. "
            "Generate it with `-O verify-required` (see docs/release-approval.md)."
        )
    return base64.b64encode(blob).decode()


def main(argv: list[str] | None = None) -> int:
    """Runs a subcommand from `argv` (default: the process arguments)."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    name_cmd = sub.add_parser("attestation-name", help="print the attestation file name for a key")
    name_cmd.add_argument("--signer-line", required=True, help="one release-signers line")
    check = sub.add_parser("check-signers", help="check every signer's YubiKey attestation")
    check.add_argument("--signers", required=True, help="release-signers file")
    check.add_argument("--attestations", required=True, help="release-attestations directory")
    check.add_argument("--yubico-ca", required=True, help="pinned Yubico CA directory")
    for name, help_text in (
        ("message", "print the message an approval signs"),
        ("sign", "sign an approval with a FIDO security key"),
        ("verify-run", "verify a run's approvals and write the release files"),
    ):
        cmd = sub.add_parser(name, help=help_text)
        cmd.add_argument("--repository", required=True)
        cmd.add_argument("--tag", required=True)
        cmd.add_argument("--commit", required=True)
        cmd.add_argument("--sha256sums", required=True, help="SHA-256 of SHA256SUMS.txt, or none")
        if name == "sign":
            cmd.add_argument("--key", required=True, help="path to the security-key handle")
        if name == "verify-run":
            cmd.add_argument("--signers", required=True, help="release-signers file")
            cmd.add_argument("--attestations", required=True, help="release-attestations directory")
            cmd.add_argument("--yubico-ca", required=True, help="pinned Yubico CA directory")
            cmd.add_argument("--approvals", required=True, help="run approvals JSON file")
            cmd.add_argument("--out-dir", required=True, help="where to write release-approval.*")
    args = parser.parse_args(argv)

    try:
        if args.command == "attestation-name":
            entries = parse_signers(args.signer_line)
            if len(entries) != 1:
                raise ApprovalError("Pass exactly one release-signers line.")
            print(entries[0].attestation_name)
            return 0
        if args.command == "check-signers":
            names = check_signer_attestations(
                Path(args.signers).read_text(), Path(args.attestations), Path(args.yubico_ca)
            )
            print("YubiKey attestation OK for: " + ", ".join(names))
            return 0
        message = approval_message(args.repository, args.tag, args.commit, args.sha256sums)
        if args.command == "message":
            sys.stdout.write(message.decode())
            return 0
        if args.command == "sign":
            print(sign(args.key, message))
            return 0
        approver, principal, blob = verify_approvals(
            Path(args.signers).read_text(),
            json.loads(Path(args.approvals).read_text()),
            message,
            Path(args.attestations),
            Path(args.yubico_ca),
        )
    except ApprovalError as error:
        print(f"::error title=Release approval rejected::{error}", file=sys.stderr)
        return 1

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "release-approval.txt").write_bytes(message)
    (out / "release-approval.sig").write_text(armor(blob))
    note = f"Release approved by `{principal}` with a security key (touch and PIN)"
    if approver != principal:
        note += f", posted by `{approver}`"
    print(note.replace("`", "") + ".")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as handle:
            handle.write(note + ".\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
