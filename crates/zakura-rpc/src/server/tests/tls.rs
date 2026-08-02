//! Tests for TLS PEM parsing.

use super::super::{parse_tls_cert_chain, parse_tls_private_key};

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
