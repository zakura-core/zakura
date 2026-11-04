"""Release approvals accept only touch-and-PIN security-key signatures."""

from __future__ import annotations

import base64
import contextlib
import hashlib
import io
import os
import shutil
import struct
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import release_approval as ra

REPOSITORY = "zakura-core/zakura"
TAG = "v1.2.3"
COMMIT = "0123456789abcdef0123456789abcdef01234567"
SUMS = "ab" * 32
SK_ED25519 = "sk-ssh-ed25519@openssh.com"
MESSAGE = ra.approval_message(REPOSITORY, TAG, COMMIT, SUMS)
GITHUB_DIR = Path(__file__).resolve().parents[2]
YUBICO_CA = GITHUB_DIR / "yubico-ca"
# The 2026-10-31 drill key, kept apart from the live signer list so enrolling
# or revoking real keys never breaks these tests.
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "release_approval"


def drill_signers() -> str:
    """Returns the fixture signer list holding only the drill key."""
    return (FIXTURES / "release-signers").read_text()


def ssh_string(value: bytes) -> bytes:
    return struct.pack(">I", len(value)) + value


def sk_blob(*, flags, key_type=SK_ED25519, namespace=ra.NAMESPACE, trailing=b""):
    """Builds an unsigned SSHSIG blob shaped like a security-key signature."""
    public_key = ssh_string(key_type.encode()) + ssh_string(bytes(32)) + ssh_string(b"ssh:")
    signature = (
        ssh_string(key_type.encode())
        + ssh_string(bytes(64))
        + bytes([flags])
        + struct.pack(">I", 7)
    )
    return (
        b"SSHSIG"
        + struct.pack(">I", 1)
        + ssh_string(public_key)
        + ssh_string(namespace.encode())
        + ssh_string(b"")
        + ssh_string(b"sha512")
        + ssh_string(signature)
        + trailing
    )


def compact(blob: bytes) -> str:
    return base64.b64encode(blob).decode()


def signer_line(key_type=SK_ED25519, namespace=ra.NAMESPACE, principal="adam", seed=0):
    key = base64.b64encode(bytes([seed]) * 32).decode()
    return f'{principal} namespaces="{namespace}" {key_type} {key}'


def approval(comment, *, state="approved", environment="release", login="ebfull"):
    return {
        "state": state,
        "comment": comment,
        "environments": [{"name": environment}],
        "user": {"login": login},
    }


class MessageTests(unittest.TestCase):
    def test_message_binds_release_and_checksums(self):
        self.assertEqual(
            MESSAGE,
            b"zakura-release-approval v1\n"
            b"repository zakura-core/zakura\n"
            b"tag v1.2.3\n"
            b"commit 0123456789abcdef0123456789abcdef01234567\n"
            b"sha256sums " + SUMS.encode() + b"\n",
        )

    def test_source_first_has_no_checksums(self):
        self.assertTrue(
            ra.approval_message(REPOSITORY, TAG, COMMIT, "none").endswith(b"sha256sums none\n")
        )

    def test_fields_cannot_inject_lines(self):
        for fields in (
            (REPOSITORY, "v1.2.3\ncommit " + COMMIT, COMMIT, SUMS),
            ("zakura-core/zakura\ntag v9", TAG, COMMIT, SUMS),
            (REPOSITORY, TAG, COMMIT[:12], SUMS),
            (REPOSITORY, TAG, COMMIT, SUMS + "\ntag v9"),
            (REPOSITORY, TAG, COMMIT, "NONE"),
        ):
            with self.subTest(fields=fields), self.assertRaises(ra.ApprovalError):
                ra.approval_message(*fields)


class SignatureParsingTests(unittest.TestCase):
    def test_reads_flags_from_security_key_signature(self):
        info = ra.parse_signature(sk_blob(flags=0x05))
        self.assertEqual((info.key_type, info.namespace, info.flags), (SK_ED25519, ra.NAMESPACE, 0x05))

    def test_compact_and_armored_forms_decode_the_same(self):
        blob = sk_blob(flags=0x05)
        self.assertEqual(ra.decode_signature(compact(blob)), blob)
        self.assertEqual(ra.decode_signature(ra.armor(blob)), blob)

    def test_rejects_malformed_blobs(self):
        blob = sk_blob(flags=0x05)
        for bad in (blob[:-3], blob + b"x", b"SSHSIH" + blob[6:], sk_blob(flags=5, trailing=b"\0")):
            with self.assertRaises(ra.ApprovalError):
                ra.parse_signature(bad)

    def test_rejects_software_key_type(self):
        with self.assertRaisesRegex(ra.ApprovalError, "FIDO security key"):
            ra.parse_signature(sk_blob(flags=0x05, key_type="ssh-ed25519"))

    def test_rejects_oversized_input(self):
        with self.assertRaisesRegex(ra.ApprovalError, "too long"):
            ra.decode_signature("A" * (ra.MAX_SIGNATURE_CHARS + 1))


class FlagTests(unittest.TestCase):
    """Flag checks run before ssh-keygen, so unsigned blobs exercise them."""

    def test_requires_touch(self):
        with self.assertRaisesRegex(ra.ApprovalError, "without touching"):
            ra.verify(signer_line(), compact(sk_blob(flags=ra.FLAG_USER_VERIFIED)), MESSAGE)

    def test_requires_pin(self):
        with self.assertRaisesRegex(ra.ApprovalError, "without the security key's PIN"):
            ra.verify(signer_line(), compact(sk_blob(flags=ra.FLAG_USER_PRESENT)), MESSAGE)

    def test_requires_release_namespace(self):
        with self.assertRaisesRegex(ra.ApprovalError, "namespace"):
            ra.verify(signer_line(), compact(sk_blob(flags=0x05, namespace="git")), MESSAGE)


class SignersTests(unittest.TestCase):
    def test_accepts_security_keys_and_skips_comments(self):
        text = f"# release signers\n\n{signer_line()}\n{signer_line(principal='ebfull', seed=1)} laptop\n"
        self.assertEqual(ra.check_signers(text).count("\n"), 2)

    def test_rejects_repeated_key(self):
        with self.assertRaisesRegex(ra.ApprovalError, "repeats an earlier key"):
            ra.check_signers(f"{signer_line()}\n{signer_line(principal='ebfull')}\n")

    def test_signer_may_list_a_backup_key(self):
        entries = ra.parse_signers(f"{signer_line()}\n{signer_line(seed=1)}\n")
        self.assertEqual([entry.principal for entry in entries], ["adam", "adam"])
        self.assertEqual(len({entry.attestation_name for entry in entries}), 2)
        self.assertTrue(all(entry.attestation_name.startswith("adam-") for entry in entries))

    def test_rejects_weakened_lists(self):
        for text in (
            "",
            "# nobody",
            signer_line(key_type="ssh-ed25519"),
            signer_line(namespace="git"),
            signer_line(principal="adam,ebfull"),
            f'adam namespaces="{ra.NAMESPACE}",cert-authority {SK_ED25519} AAAA',
            f"adam {SK_ED25519} AAAA",
        ):
            with self.subTest(text=text), self.assertRaises(ra.ApprovalError):
                ra.check_signers(text)


class ApprovalCommentTests(unittest.TestCase):
    def test_extracts_signed_release_approvals_only(self):
        approvals = [
            approval(ra.COMMENT_PREFIX + "AAAA", login="ebfull"),
            approval("LGTM"),
            approval(ra.COMMENT_PREFIX + "BBBB", state="rejected"),
            approval(ra.COMMENT_PREFIX + "CCCC", environment="crates-io"),
            approval(None),
        ]
        self.assertEqual(ra.approval_signatures(approvals), [("ebfull", "AAAA")])

    def test_button_only_approval_is_rejected(self):
        with self.assertRaisesRegex(ra.ApprovalError, "approve-release.sh"):
            ra.verify_approvals(signer_line(), [approval("")], MESSAGE, FIXTURES, YUBICO_CA)

    def test_reports_each_failed_signature(self):
        approvals = [
            approval(ra.COMMENT_PREFIX + compact(sk_blob(flags=ra.FLAG_USER_PRESENT)), login="adam"),
            approval(ra.COMMENT_PREFIX + "!!!", login="mallory"),
        ]
        with self.assertRaisesRegex(ra.ApprovalError, "adam: .*PIN.*mallory: .*base64"):
            ra.verify_approvals(signer_line(), approvals, MESSAGE, FIXTURES, YUBICO_CA)

    def test_rejects_non_list_payload(self):
        with self.assertRaises(ra.ApprovalError):
            ra.approval_signatures({"message": "Not Found"})


def ssh_key_parts(key: str):
    """Returns `(public point, application)` from an sk-ssh-ed25519 key blob."""
    reader = ra._Reader(base64.b64decode(key))
    reader.string()
    return reader.string(), reader.string()


def make_certificate(workdir: Path, name: str, issuer: str | None = None) -> None:
    """Writes `<name>.key` and `<name>.pem`, self-signed or issued by `<issuer>`."""
    key, pem = str(workdir / f"{name}.key"), str(workdir / f"{name}.pem")
    new_key = ["-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256",
               "-pkeyopt", "ec_param_enc:named_curve", "-nodes", "-subj", f"/CN={name}"]
    if issuer is None:
        # `req -x509` marks the certificate as a CA, so it can issue others.
        subprocess.run(
            ["openssl", "req", "-x509", *new_key, "-days", "2", "-keyout", key, "-out", pem],
            check=True, capture_output=True,
        )
        return
    csr = str(workdir / f"{name}.csr")
    subprocess.run(
        ["openssl", "req", *new_key, "-keyout", key, "-out", csr],
        check=True, capture_output=True,
    )
    subprocess.run(
        ["openssl", "x509", "-req", "-days", "2", "-in", csr, "-set_serial", "2",
         "-CA", str(workdir / f"{issuer}.pem"), "-CAkey", str(workdir / f"{issuer}.key"),
         "-out", pem],
        check=True, capture_output=True,
    )


def fake_attestation(workdir: Path, key: str, principal: str, issuer: str | None = None) -> bytes:
    """Builds an attestation from a certificate we control, as software would.

    The certificate is self-signed, or issued by the CA `issuer` in `workdir`.
    """
    point, application = ssh_key_parts(key)
    cose = bytes.fromhex("a4010103272006215820") + point
    auth_data = (
        hashlib.sha256(application).digest() + bytes([0x45]) + bytes(4) + bytes(16)
        + struct.pack(">H", 1) + b"\x00" + cose
    )
    make_certificate(workdir, "fake", issuer)
    der = subprocess.run(
        ["openssl", "x509", "-in", str(workdir / "fake.pem"), "-outform", "der"],
        check=True, capture_output=True,
    ).stdout
    signed = auth_data + hashlib.sha256(f"zakura-release-attestation:{principal}".encode()).digest()
    signature = subprocess.run(
        ["openssl", "dgst", "-sha256", "-sign", str(workdir / "fake.key")],
        input=signed, check=True, capture_output=True,
    ).stdout
    encoded = bytes([0x58, len(auth_data)]) + auth_data
    return (
        ssh_string(ra.ATTESTATION_MAGIC) + ssh_string(der) + ssh_string(signature)
        + ssh_string(encoded) + bytes(4) + ssh_string(b"")
    )


@unittest.skipIf(shutil.which("openssl") is None, "openssl is not installed")
class AttestationTests(unittest.TestCase):
    """Signer keys must be attested by a genuine YubiKey."""

    def setUp(self):
        self.signers = drill_signers()
        (entry,) = ra.parse_signers(self.signers)
        self.key_type, self.key = entry.key_type, entry.key
        self.attestation = (FIXTURES / entry.attestation_name).read_bytes()

    def test_genuine_yubikey_attestation_passes(self):
        self.assertEqual(
            ra.check_signer_attestations(self.signers, FIXTURES, YUBICO_CA),
            ["czarcas7ic-bee0b3725e09ac6a.attestation"],
        )

    def test_attestation_is_bound_to_its_principal(self):
        with self.assertRaisesRegex(ra.ApprovalError, "signature for ebfull is invalid"):
            ra.check_attestation("ebfull", self.key_type, self.key, self.attestation, YUBICO_CA)

    def test_attestation_must_match_the_listed_key(self):
        other = base64.b64encode(
            ssh_string(SK_ED25519.encode()) + ssh_string(bytes(32)) + ssh_string(b"ssh:zakura-release")
        ).decode()
        with self.assertRaisesRegex(ra.ApprovalError, "attests a different key"):
            ra.check_attestation("czarcas7ic", SK_ED25519, other, self.attestation, YUBICO_CA)

    def test_tampered_attestation_is_rejected(self):
        tampered = bytearray(self.attestation)
        tampered[-60] ^= 1  # inside the authenticator data's COSE key
        with self.assertRaises(ra.ApprovalError):
            ra.check_attestation("czarcas7ic", self.key_type, self.key, bytes(tampered), YUBICO_CA)

    def test_software_attestation_does_not_chain_to_yubico(self):
        with tempfile.TemporaryDirectory() as workdir:
            fake = fake_attestation(Path(workdir), self.key, "czarcas7ic")
            with self.assertRaisesRegex(ra.ApprovalError, "does not chain to a Yubico FIDO root"):
                ra.check_attestation("czarcas7ic", self.key_type, self.key, fake, YUBICO_CA)

    def test_system_trust_store_is_not_trusted(self):
        """A certificate from any CA the runner trusts, such as a public TLS CA, is not Yubico."""
        with tempfile.TemporaryDirectory() as workdir:
            workdir = Path(workdir)
            make_certificate(workdir, "public-ca")
            fake = fake_attestation(workdir, self.key, "czarcas7ic", issuer="public-ca")
            store = workdir / "store"
            store.mkdir()
            subject_hash = subprocess.run(
                ["openssl", "x509", "-noout", "-subject_hash", "-in", str(workdir / "public-ca.pem")],
                check=True, capture_output=True, text=True,
            ).stdout.strip()
            shutil.copy(workdir / "public-ca.pem", store / f"{subject_hash}.0")
            with mock.patch.dict(os.environ, {"SSL_CERT_DIR": str(store)}):
                with self.assertRaisesRegex(ra.ApprovalError, "does not chain to a Yubico FIDO root"):
                    ra.check_attestation("czarcas7ic", self.key_type, self.key, fake, YUBICO_CA)

    def test_signer_without_attestation_is_rejected(self):
        with tempfile.TemporaryDirectory() as workdir:
            with self.assertRaisesRegex(ra.ApprovalError, "No YubiKey attestation for czarcas7ic"):
                ra.check_signer_attestations(self.signers, Path(workdir), YUBICO_CA)


# A real YubiKey approval from the 2026-10-31 drill on the czarcas7ic/zakura
# fork, made with the czarcas7ic key in .github/release-signers. It only
# approves that fork's drill release, so it is safe to publish.
DRILL_APPROVAL = ra.COMMENT_PREFIX + (
    "U1NIU0lHAAAAAQAAAFgAAAAac2stc3NoLWVkMjU1MTlAb3BlbnNzaC5jb20AAAAghc6gfVXv"
    "EOMh2PkEfY/c5liCblCq5PFRs6jpUf20tgUAAAASc3NoOnpha3VyYS1yZWxlYXNlAAAADnph"
    "a3VyYS1yZWxlYXNlAAAAAAAAAAZzaGE1MTIAAABnAAAAGnNrLXNzaC1lZDI1NTE5QG9wZW5z"
    "c2guY29tAAAAQBynMEST2u4InNWbPkQkic6Y3rmmlJWPf0vwdvKeaFLKeJapHGirbc/ZF+I4"
    "a/17khJWIhp/y+D2lOz6t8IMcwoFAAAAAw=="
)
DRILL_RELEASE = (
    "czarcas7ic/zakura",
    "v1.7.1-drill8",
    "f966f5c98273e5e065209a9ab1e73e7874bac744",
    "f22d7924926d5c72843e627947700b851a8cf3f1249d1f4232f385568109c140",
)


@unittest.skipIf(
    shutil.which("ssh-keygen") is None or shutil.which("openssl") is None,
    "ssh-keygen or openssl is not installed",
)
class YubiKeyApprovalTests(unittest.TestCase):
    """The full gate, run on a genuine YubiKey approval."""

    def setUp(self):
        self.signers = drill_signers()

    def verify(self, *release):
        return ra.verify_approvals(
            self.signers,
            [approval("LGTM"), approval(DRILL_APPROVAL, login="czarcas7ic")],
            ra.approval_message(*release),
            FIXTURES,
            YUBICO_CA,
        )

    def test_genuine_approval_publishes(self):
        approver, principal, blob = self.verify(*DRILL_RELEASE)
        self.assertEqual((approver, principal), ("czarcas7ic", "czarcas7ic"))
        self.assertEqual(ra.parse_signature(blob).flags & 0x05, 0x05)

    def test_approval_does_not_carry_to_other_releases_or_binaries(self):
        repository, tag, commit, sums = DRILL_RELEASE
        for release in (
            (REPOSITORY, tag, commit, sums),
            (repository, "v1.7.1-drill9", commit, sums),
            (repository, tag, COMMIT, sums),
            (repository, tag, commit, SUMS),
            (repository, tag, commit, "none"),
        ):
            with self.subTest(release=release):
                with self.assertRaisesRegex(ra.ApprovalError, "does not approve this release"):
                    self.verify(*release)

    def test_approval_from_one_of_several_keys(self):
        self.signers = signer_line(principal="czarcas7ic", seed=9) + "\n" + self.signers
        approver, principal, _ = self.verify(*DRILL_RELEASE)
        self.assertEqual((approver, principal), ("czarcas7ic", "czarcas7ic"))

    def test_approval_needs_the_signers_attestation(self):
        with tempfile.TemporaryDirectory() as workdir:
            with self.assertRaisesRegex(ra.ApprovalError, "No YubiKey attestation"):
                ra.verify_approvals(
                    self.signers,
                    [approval(DRILL_APPROVAL)],
                    ra.approval_message(*DRILL_RELEASE),
                    Path(workdir),
                    YUBICO_CA,
                )


@unittest.skipIf(shutil.which("openssl") is None, "openssl is not installed")
class LiveSignersTests(unittest.TestCase):
    """Every key in .github/release-signers must carry a valid Yubico attestation."""

    def test_every_listed_key_is_attested(self):
        signers = (GITHUB_DIR / "release-signers").read_text()
        if not any(line.strip() and not line.lstrip().startswith("#") for line in signers.splitlines()):
            self.skipTest("no release signers are enrolled yet; releases fail closed until one is")
        names = ra.check_signer_attestations(
            signers, GITHUB_DIR / "release-attestations", YUBICO_CA
        )
        self.assertEqual(len(names), len(ra.parse_signers(signers)))


class CommandLineTests(unittest.TestCase):
    def test_main_takes_an_explicit_argument_list(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            status = ra.main(
                ["message", "--repository", REPOSITORY, "--tag", TAG,
                 "--commit", COMMIT, "--sha256sums", SUMS]
            )
        self.assertEqual(status, 0)
        self.assertEqual(out.getvalue().encode(), MESSAGE)


class WorkflowTests(unittest.TestCase):
    def test_publish_runs_the_verifier_isolated(self):
        """Without -I, a sibling such as .github/scripts/argparse.py would run first."""
        workflow = (GITHUB_DIR / "workflows" / "create-release.yml").read_text()
        self.assertIn("python3 -I release-source/.github/scripts/release_approval.py", workflow)
        self.assertNotIn("python3 release-source/.github/scripts/release_approval.py", workflow)

    def test_sibling_module_cannot_shadow_an_isolated_verifier(self):
        with tempfile.TemporaryDirectory() as workdir:
            script = Path(workdir, "release_approval.py")
            shutil.copy(GITHUB_DIR / "scripts" / "release_approval.py", script)
            Path(workdir, "argparse.py").write_text("raise SystemExit('shadowed')\n")
            shadowed = subprocess.run(
                [sys.executable, str(script), "--help"], capture_output=True, text=True
            )
            isolated = subprocess.run(
                [sys.executable, "-I", str(script), "--help"], capture_output=True, text=True
            )
        self.assertIn("shadowed", shadowed.stderr)
        self.assertEqual(isolated.returncode, 0, isolated.stderr)


@unittest.skipIf(shutil.which("ssh-keygen") is None, "ssh-keygen is not installed")
class SoftwareKeyTests(unittest.TestCase):
    """A real software-key signature is rejected even when listed as a signer."""

    def test_software_signature_is_rejected(self):
        with tempfile.TemporaryDirectory() as workdir:
            key = Path(workdir, "key")
            subprocess.run(
                ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", key], check=True
            )
            message = Path(workdir, "message")
            message.write_bytes(MESSAGE)
            subprocess.run(
                ["ssh-keygen", "-q", "-Y", "sign", "-n", ra.NAMESPACE, "-f", key, message],
                check=True,
            )
            signature = Path(workdir, "message.sig").read_text()
            public = Path(workdir, "key.pub").read_text().split()[1]
            signers = f'adam namespaces="{ra.NAMESPACE}" {SK_ED25519} {public}'
            with self.assertRaisesRegex(ra.ApprovalError, "FIDO security key"):
                ra.verify(signers, signature, MESSAGE)


if __name__ == "__main__":
    unittest.main()
