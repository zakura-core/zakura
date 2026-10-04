//! Compatibility checks for the legacy bitflags Serde representation.

use crate::orchard::Flags;

#[test]
fn serde_flags_preserve_all_bits_and_the_legacy_format() {
    for bits in u8::MIN..=u8::MAX {
        let flags = Flags::from_bits_retain(bits);
        let json = format!(r#"{{"bits":{bits}}}"#);
        assert_eq!(serde_json::to_string(&flags).unwrap(), json);
        assert_eq!(serde_json::from_str::<Flags>(&json).unwrap().bits(), bits);
        assert_eq!(bincode::serialize(&flags).unwrap(), [bits]);
    }
}

#[test]
fn serde_flags_reject_missing_duplicate_unknown_and_invalid_fields() {
    for (json, message) in [
        ("{}", "missing field `bits`"),
        (r#"{"bits":1,"bits":2}"#, "duplicate field `bits`"),
        (r#"{"bits":1,"other":2}"#, "unknown field `other`"),
        (r#"{"bits":256}"#, "invalid value"),
        (r#"{"bits":-1}"#, "invalid value"),
        ("[1]", "invalid type"),
    ] {
        let error = serde_json::from_str::<Flags>(json).unwrap_err();
        assert!(error.to_string().contains(message), "{json}: {error}");
    }

    // The old adapter accepts maps only when deserializing, even in binary
    // formats; retaining that behavior avoids broadening the accepted input.
    assert!(bincode::deserialize::<Flags>(&[1]).is_err());
}
