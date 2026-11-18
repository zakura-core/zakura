//! `ZAKURA_SIM_KNOWN` treats each named `Active` property as `Known` for a
//! run, at the issue it gives or 0, and leaves every other status alone. It
//! has its own test binary, since it sets a variable every test in a binary
//! shares.
#![cfg(zakura_sim)]

use zakura_sim::{
    harness::{self, KNOWN_VAR},
    property::Status,
};

#[test]
fn named_active_properties_become_known() {
    let entry = harness::find("sync/wire").unwrap();
    let isolated = "session/stalled-service-isolated";
    let panic = "sim/no-task-panic";
    assert_eq!(entry.status(isolated), Some(Status::Active));
    std::env::set_var(KNOWN_VAR, format!("{isolated}=2303, {panic}"));
    assert_eq!(entry.status(isolated), Some(Status::Known { issue: 2303 }));
    assert_eq!(entry.status(panic), Some(Status::Known { issue: 0 }));
    assert_eq!(
        entry.status("session/replacement-preserves-authorization"),
        Some(Status::Active)
    );
    std::env::remove_var(KNOWN_VAR);
    assert_eq!(entry.status(isolated), Some(Status::Active));
}
