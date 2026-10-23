//! The `discovery/wait-complete` checker matches the discovery code the
//! binary was built from, not whatever `service.rs` holds at run time.
//!
//! This test edits the checkout's `service.rs` while it runs, and puts its
//! contents and modified time back when it ends, so cargo sees no change. It
//! lives in its own test binary, so no other test reads the file meanwhile.
#![cfg(zakura_sim)]

use std::{
    fs::{self, File},
    path::PathBuf,
    time::SystemTime,
};

use zakura_sim::{
    harness::{discovery::WaitComplete, run_harness},
    MemorySink, Plan, PropertyOutcome,
};

/// The committed input that reaches the property's check.
const INPUT: &[u8] = include_bytes!("fixtures/discovery-wait-complete.bin");

/// The checkout's discovery service source.
fn service_rs() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../zakura-network/src/zakura/discovery/service.rs")
}

/// Restores `service.rs`'s contents and modified time when dropped, even if
/// the test panics.
struct Restore {
    bytes: Vec<u8>,
    modified: SystemTime,
}

impl Restore {
    /// Saves `service.rs` as it is now.
    fn save() -> Self {
        let path = service_rs();
        Restore {
            bytes: fs::read(&path).unwrap(),
            modified: fs::metadata(&path).unwrap().modified().unwrap(),
        }
    }
}

impl Drop for Restore {
    /// Writes the saved contents and time back.
    fn drop(&mut self) {
        let path = service_rs();
        fs::write(&path, &self.bytes).expect("service.rs can be restored");
        File::options()
            .write(true)
            .open(&path)
            .and_then(|file| file.set_modified(self.modified))
            .expect("service.rs's modified time can be restored");
    }
}

/// The committed input's records and the property's outcome.
fn run() -> (Vec<String>, PropertyOutcome) {
    let mut sink = MemorySink::default();
    let report =
        run_harness::<WaitComplete>(&Default::default(), Plan::from_fuzz(INPUT), &mut sink, None)
            .unwrap_or_else(|error| panic!("{error}"));
    (sink.lines(), report.properties[0].outcome.clone())
}

#[test]
fn the_checker_ignores_the_checkout_at_run_time() {
    let (lines, outcome) = run();
    assert!(
        matches!(outcome, PropertyOutcome::Held { applicable } if applicable > 0),
        "{outcome:?}"
    );
    let restore = Restore::save();

    // 100 blank lines move every line of the edited file, but not the
    // locations compiled into this binary.
    let mut shifted = vec![b'\n'; 100];
    shifted.extend_from_slice(&restore.bytes);
    fs::write(service_rs(), shifted).unwrap();
    assert_eq!(
        run(),
        (lines.clone(), outcome.clone()),
        "with service.rs shifted"
    );

    fs::remove_file(service_rs()).unwrap();
    assert_eq!(run(), (lines, outcome), "with service.rs gone");
}
