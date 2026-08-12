//! Tests for TLS PEM parsing.

use std::time::Duration;

use rustls::pki_types::{pem::PemObject, CertificateDer};

use super::super::{
    certificate_dates, parse_tls_cert_chain, parse_tls_private_key, CertificateDates,
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
const NOT_BEFORE: Duration = Duration::from_secs(1_735_689_600);
/// 2025-02-01 00:00:00 UTC, the `notAfter` of [`UTC_TIME_CERT`].
const UTC_TIME_NOT_AFTER: Duration = Duration::from_secs(1_738_368_000);
/// 2060-01-01 00:00:00 UTC, the `notAfter` of [`GENERALIZED_TIME_CERT`].
const GENERALIZED_TIME_NOT_AFTER: Duration = Duration::from_secs(2_840_140_800);

fn certificate(pem: &str) -> CertificateDer<'static> {
    CertificateDer::from_pem_slice(pem.as_bytes()).expect("test certificate should be valid PEM")
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
        certificate_dates(&certificate, NOT_BEFORE - Duration::from_secs(1)),
        Ok(CertificateDates::NotYetValid {
            not_before: NOT_BEFORE
        }),
    );
    assert_eq!(
        certificate_dates(&certificate, NOT_BEFORE),
        Ok(CertificateDates::Current),
    );
    assert_eq!(
        certificate_dates(&certificate, UTC_TIME_NOT_AFTER),
        Ok(CertificateDates::Current),
    );
    assert_eq!(
        certificate_dates(&certificate, UTC_TIME_NOT_AFTER + Duration::from_secs(1)),
        Ok(CertificateDates::Expired {
            not_after: UTC_TIME_NOT_AFTER
        }),
    );
}

#[test]
fn reads_generalized_time_validity_dates() {
    let certificate = certificate(GENERALIZED_TIME_CERT);

    assert_eq!(
        certificate_dates(&certificate, NOT_BEFORE),
        Ok(CertificateDates::Current),
    );
    assert_eq!(
        certificate_dates(
            &certificate,
            GENERALIZED_TIME_NOT_AFTER + Duration::from_secs(1)
        ),
        Ok(CertificateDates::Expired {
            not_after: GENERALIZED_TIME_NOT_AFTER
        }),
    );
}

#[test]
fn ignores_certificates_that_are_not_valid_der() {
    // The same placeholder bytes that `parses_certificate_chain_and_first_private_key` loads:
    // rustls accepts them at config time, so reading their dates must fail without panicking.
    let certificate = CertificateDer::from(vec![1, 2, 3]);

    assert!(certificate_dates(&certificate, NOT_BEFORE).is_err());
}
