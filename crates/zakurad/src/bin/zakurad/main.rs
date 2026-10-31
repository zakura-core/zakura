//! Main entry point for zakurad

use zakurad::application::boot;

#[cfg(all(target_os = "linux", target_env = "gnu"))]
const GLIBC_MALLOC_ARENA_MAX: i32 = 16;

#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[allow(
    unsafe_code,
    reason = "calling glibc mallopt is required to bound allocator arena retention"
)]
fn limit_glibc_malloc_arenas() {
    if std::env::var_os("MALLOC_ARENA_MAX").is_some() {
        return;
    }

    // SAFETY: this process-global allocator setting is configured before worker threads start.
    let configured = unsafe { libc::mallopt(libc::M_ARENA_MAX, GLIBC_MALLOC_ARENA_MAX) };

    assert_ne!(
        configured, 0,
        "glibc accepts M_ARENA_MAX before worker threads start"
    );
}

/// Returns the extensions used by the Pasta field assembly that this binary
/// was compiled to assume, but that the running CPU lacks.
///
/// Builds with flags such as `-C target-cpu=native` or
/// `-C target-feature=+adx,+bmi2` select the BMI2 and ADX field assembly at
/// build time, and crash with an illegal instruction on CPUs without them.
///
/// This reads CPUID directly: `is_x86_feature_detected!` reports every
/// compiled-in feature as present without asking the CPU.
#[cfg(target_arch = "x86_64")]
fn missing_pasta_assembly_features() -> Vec<&'static str> {
    use core::arch::x86_64::{__cpuid, __cpuid_count};

    /// CPUID leaf 7 EBX bits, which report BMI2 and ADX.
    const BMI2_BIT: u32 = 8;
    const ADX_BIT: u32 = 19;

    let compiled = [
        ("bmi2", cfg!(target_feature = "bmi2"), BMI2_BIT),
        ("adx", cfg!(target_feature = "adx"), ADX_BIT),
    ];
    if !compiled.iter().any(|(_, compiled, _)| *compiled) {
        return Vec::new();
    }

    // CPUs whose highest basic leaf is below 7 report neither extension.
    let extended_features = if __cpuid(0).eax >= 7 {
        __cpuid_count(7, 0).ebx
    } else {
        0
    };

    compiled
        .iter()
        .filter(|(_, compiled, bit)| *compiled && extended_features & (1 << bit) == 0)
        .map(|(name, _, _)| *name)
        .collect()
}

/// Exits with an explanation instead of crashing later with an illegal instruction.
///
/// This runs first in `main`, and is kept out of line so CPU-specific code
/// generation elsewhere doesn't run before it.
#[cfg(target_arch = "x86_64")]
#[inline(never)]
#[allow(
    clippy::print_stderr,
    reason = "this runs before tracing is initialized, so stderr is the only output"
)]
fn exit_if_cpu_lacks_pasta_assembly_features() {
    let missing = missing_pasta_assembly_features();
    if missing.is_empty() {
        return;
    }

    eprintln!(
        "zakurad was built to use Pasta field assembly that needs CPU features this machine \
         lacks: {}. Rebuild with the `portable` feature: \
         `cargo build --locked --release --package zakura --features portable`.",
        missing.join(", ")
    );
    std::process::exit(1);
}

/// Process entry point for `zakurad`
fn main() {
    #[cfg(target_arch = "x86_64")]
    exit_if_cpu_lacks_pasta_assembly_features();

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    limit_glibc_malloc_arenas();

    // Enable backtraces by default for zakurad, but allow users to override it.
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        std::env::set_var("RUST_BACKTRACE", "1");
        // Disable library backtraces (i.e. eyre) to avoid performance hit for
        // non-panic errors, but allow users to override it.
        if std::env::var_os("RUST_LIB_BACKTRACE").is_none() {
            std::env::set_var("RUST_LIB_BACKTRACE", "0");
        }
    }
    boot();
}

#[cfg(all(test, target_arch = "x86_64"))]
mod tests {
    /// Tests run on the machine that built them, so nothing compiled in can be missing.
    #[test]
    fn running_cpu_has_compiled_pasta_assembly_features() {
        assert_eq!(super::missing_pasta_assembly_features(), Vec::<&str>::new());
    }
}
