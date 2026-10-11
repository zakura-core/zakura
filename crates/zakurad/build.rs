//! Build script for zakurad.
//!
//! Turns Zakura version information into build-time environmental variables,
//! so that it can be compiled into `zakurad`, and used in diagnostics.

#[path = "build/metadata.rs"]
mod metadata;

/// Process entry point for `zakurad`'s build script.
fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=build");

    metadata::emit();
}
