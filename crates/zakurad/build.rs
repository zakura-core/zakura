//! Build script for zakurad.
//!
//! Turns Zakura version information into build-time environmental variables,
//! so that it can be compiled into `zakurad`, and used in diagnostics.
//!
//! When compiling the `lightwalletd` gRPC tests, also builds a gRPC client
//! Rust API for `lightwalletd`.

#[path = "build/metadata.rs"]
mod metadata;

#[cfg(feature = "lightwalletd-grpc-tests")]
#[path = "build/lightwalletd.rs"]
mod lightwalletd;

/// Process entry point for `zakurad`'s build script.
fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=build");

    metadata::emit();

    #[cfg(feature = "lightwalletd-grpc-tests")]
    lightwalletd::generate_client();
}
