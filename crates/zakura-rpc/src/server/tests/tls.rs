//! Tests for TLS PEM parsing.

use rustls::pki_types::{pem::PemObject, CertificateDer};

use super::super::{
    certificate_validity, parse_tls_cert_chain, parse_tls_private_key, CertificateValidity,
};

/// A self-signed test certificate whose `notBefore` and `notAfter` are both before 2050, so
/// both are encoded as `UTCTime`: 2025-01-01 00:00:00 UTC to 2025-02-01 00:00:00 UTC.
const UTC_TIME_CERT: &str = "\
-----BEGIN CERTIFICATE-----
MIIBXzCCAQWgAwIBAgIUOqpDeLLE9L/b+m+mfxMBjnCetbswCgYIKoZIzj0EAwIw
HjEcMBoGA1UEAwwTemFrdXJhLXJwYy10bHMtdGVzdDAeFw0yNTAxMDEwMDAwMDBa
Fw0yNTAyMDEwMDAwMDBaMB4xHDAaBgNVBAMME3pha3VyYS1ycGMtdGxzLXRlc3Qw
WTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAQ+wHoX5FRv42/6K6QTK/S4bo9He6nb
Er3si2BfQwWbPWAsEokgpCoP3GBM3xbK5u2GuL6w5kVrIz1XDI1cAnnNoyEwHzAd
BgNVHQ4EFgQUVqQ+oR1le2DmrIi5cNKCaXdiBF4wCgYIKoZIzj0EAwIDSAAwRQIh
ALWt669Gzty6dj95pi6imo8sIFtBzUBYMy97W/vHGC9dAiAtdr3+QKUEAuPw7DfK
Q4y1hbo0mHmIo4gHWo93MnAJ1A==
-----END CERTIFICATE-----
";

/// The same certificate, but valid until 2060-01-01 00:00:00 UTC, so its `notAfter` is encoded
/// as a `GeneralizedTime`.
const GENERALIZED_TIME_CERT: &str = "\
-----BEGIN CERTIFICATE-----
MIIBYjCCAQegAwIBAgIUKl5vhHmfrnymJ7fL+vr+XmAmt/gwCgYIKoZIzj0EAwIw
HjEcMBoGA1UEAwwTemFrdXJhLXJwYy10bHMtdGVzdDAgFw0yNTAxMDEwMDAwMDBa
GA8yMDYwMDEwMTAwMDAwMFowHjEcMBoGA1UEAwwTemFrdXJhLXJwYy10bHMtdGVz
dDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABD7AehfkVG/jb/orpBMr9Lhuj0d7
qdsSveyLYF9DBZs9YCwSiSCkKg/cYEzfFsrm7Ya4vrDmRWsjPVcMjVwCec2jITAf
MB0GA1UdDgQWBBRWpD6hHWV7YOasiLlw0oJpd2IEXjAKBggqhkjOPQQDAgNJADBG
AiEA+dmdhqGXyRHuEuNg0iJzhFM/Axx+gHq/U63Hw1rijdUCIQDjmsEOZr2vEZQv
EVH9mWqX5H2mIDsHB02Nw+Iebwao5A==
-----END CERTIFICATE-----
";

/// 2025-01-01 00:00:00 UTC, the `notBefore` of both test certificates.
const NOT_BEFORE: i64 = 1_735_689_600;
/// 2025-02-01 00:00:00 UTC, the `notAfter` of [`UTC_TIME_CERT`].
const UTC_TIME_NOT_AFTER: i64 = 1_738_368_000;
/// 2060-01-01 00:00:00 UTC, the `notAfter` of [`GENERALIZED_TIME_CERT`].
const GENERALIZED_TIME_NOT_AFTER: i64 = 2_840_140_800;
/// 1960-01-01 00:00:00 UTC, before the Unix epoch but valid in an X.509 `UTCTime`.
const PRE_UNIX_EPOCH_NOT_BEFORE: i64 = -315_619_200;

fn certificate(pem: &str) -> CertificateDer<'static> {
    CertificateDer::from_pem_slice(pem.as_bytes()).expect("test certificate should be valid PEM")
}

/// Replaces the test certificate's `notBefore` without re-signing it.
///
/// [`certificate_validity`] only parses the validity fields, so the unchanged
/// signature is irrelevant to this focused test.
fn certificate_with_not_before(not_before: &[u8; 13]) -> CertificateDer<'static> {
    let mut certificate = certificate(UTC_TIME_CERT).as_ref().to_vec();
    let original_not_before = b"250101000000Z";
    let offset = certificate
        .windows(original_not_before.len())
        .position(|window| window == original_not_before)
        .expect("test certificate should contain its notBefore value");

    certificate[offset..offset + not_before.len()].copy_from_slice(not_before);
    CertificateDer::from(certificate)
}

#[test]
fn parses_certificate_chain_and_first_private_key() {
    let cert_pem = b"\
-----BEGIN CERTIFICATE-----\n\
AQID\n\
-----END CERTIFICATE-----\n\
-----BEGIN CERTIFICATE-----\n\
BAUG\n\
-----END CERTIFICATE-----\n";
    let key_pem = b"\
-----BEGIN CERTIFICATE-----\n\
AQID\n\
-----END CERTIFICATE-----\n\
-----BEGIN PRIVATE KEY-----\n\
BwgJ\n\
-----END PRIVATE KEY-----\n\
-----BEGIN PRIVATE KEY-----\n\
CgsM\n\
-----END PRIVATE KEY-----\n";

    let cert_chain =
        parse_tls_cert_chain(cert_pem.as_slice()).expect("valid certificate PEM sections");
    let private_key =
        parse_tls_private_key(key_pem.as_slice()).expect("valid private key PEM sections");

    assert_eq!(cert_chain.len(), 2);
    assert_eq!(cert_chain[0].as_ref(), [1, 2, 3]);
    assert_eq!(cert_chain[1].as_ref(), [4, 5, 6]);
    assert_eq!(
        private_key
            .expect("private key section should be loaded")
            .secret_der(),
        [7, 8, 9]
    );
}

#[test]
fn reads_utc_time_validity_dates() {
    let certificate = certificate(UTC_TIME_CERT);

    assert_eq!(
        certificate_validity(&certificate, NOT_BEFORE - 1),
        Ok(CertificateValidity::NotYetValid {
            not_before: NOT_BEFORE
        }),
    );
    assert_eq!(
        certificate_validity(&certificate, NOT_BEFORE),
        Ok(CertificateValidity::Current),
    );
    assert_eq!(
        certificate_validity(&certificate, UTC_TIME_NOT_AFTER),
        Ok(CertificateValidity::Current),
    );
    assert_eq!(
        certificate_validity(&certificate, UTC_TIME_NOT_AFTER + 1),
        Ok(CertificateValidity::Expired {
            not_after: UTC_TIME_NOT_AFTER
        }),
    );
}

#[test]
fn reads_generalized_time_validity_dates() {
    let certificate = certificate(GENERALIZED_TIME_CERT);

    assert_eq!(
        certificate_validity(&certificate, NOT_BEFORE),
        Ok(CertificateValidity::Current),
    );
    assert_eq!(
        certificate_validity(&certificate, GENERALIZED_TIME_NOT_AFTER + 1),
        Ok(CertificateValidity::Expired {
            not_after: GENERALIZED_TIME_NOT_AFTER
        }),
    );
}

#[test]
fn reads_pre_unix_epoch_utc_time() {
    let certificate = certificate_with_not_before(b"600101000000Z");

    assert_eq!(
        certificate_validity(&certificate, PRE_UNIX_EPOCH_NOT_BEFORE - 1),
        Ok(CertificateValidity::NotYetValid {
            not_before: PRE_UNIX_EPOCH_NOT_BEFORE,
        }),
    );
    assert_eq!(
        certificate_validity(&certificate, 0),
        Ok(CertificateValidity::Current),
    );
    assert_eq!(
        certificate_validity(&certificate, UTC_TIME_NOT_AFTER + 1),
        Ok(CertificateValidity::Expired {
            not_after: UTC_TIME_NOT_AFTER,
        }),
    );
}

#[test]
fn ignores_certificates_that_are_not_valid_der() {
    // The same placeholder bytes that `parses_certificate_chain_and_first_private_key` loads:
    // rustls accepts them at config time, so reading their dates must fail without panicking.
    let certificate = CertificateDer::from(vec![1, 2, 3]);

    assert!(certificate_validity(&certificate, NOT_BEFORE).is_err());
}
