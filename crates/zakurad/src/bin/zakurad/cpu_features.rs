//! Check CPU compatibility once, before starting the node or its worker threads.

const BMI2_BIT: u32 = 8;
const ADX_BIT: u32 = 19;

/// Returns missing CPU extensions required by assembly or global compiler flags.
fn missing_features(extended_features: u32) -> Vec<&'static str> {
    // `portable` disables explicit assembly, but cannot undo instructions the
    // compiler may emit elsewhere when CPU-specific target flags are enabled.
    let forced_assembly = cfg!(all(feature = "x86_64-asm", not(feature = "portable")));
    let required = [
        (
            "bmi2",
            forced_assembly || cfg!(target_feature = "bmi2"),
            BMI2_BIT,
        ),
        (
            "adx",
            forced_assembly || cfg!(target_feature = "adx"),
            ADX_BIT,
        ),
    ];
    required
        .iter()
        .filter(|(_, required, bit)| *required && extended_features & (1 << bit) == 0)
        .map(|(name, _, _)| *name)
        .collect()
}

/// Reads CPUID directly because Rust's feature-detection macro treats globally
/// enabled compiler target features as present without checking the actual CPU.
fn missing_pasta_assembly_features() -> Vec<&'static str> {
    use core::arch::x86_64::{__cpuid, __cpuid_count};

    if missing_features(0).is_empty() {
        return Vec::new();
    }
    // CPUs whose highest basic leaf is below 7 report neither extension.
    let extended_features = if __cpuid(0).eax >= 7 {
        __cpuid_count(7, 0).ebx
    } else {
        0
    };
    missing_features(extended_features)
}

/// Exits with an explanation instead of entering unsupported arithmetic.
///
/// Kept out of line so CPU-specific node code does not run before this check.
#[inline(never)]
#[allow(
    clippy::print_stderr,
    reason = "this runs before tracing is initialized, so stderr is the only output"
)]
pub(crate) fn exit_if_cpu_lacks_pasta_assembly_features() {
    let missing = missing_pasta_assembly_features();
    if missing.is_empty() {
        return;
    }
    eprintln!(
        "This Zakura binary requires CPU features this machine lacks: {}. \
         Rebuild with `--features portable` and without CPU-specific compiler flags.",
        missing.join(", ")
    );
    std::process::exit(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supporting_cpu_never_reports_missing_extensions() {
        assert!(missing_features((1 << BMI2_BIT) | (1 << ADX_BIT)).is_empty());
    }

    #[cfg(all(feature = "x86_64-asm", not(feature = "portable")))]
    #[test]
    fn forced_assembly_requires_both_extensions() {
        assert_eq!(missing_features(0), ["bmi2", "adx"]);
        assert_eq!(missing_features(1 << BMI2_BIT), ["adx"]);
        assert_eq!(missing_features(1 << ADX_BIT), ["bmi2"]);
    }

    #[cfg(all(
        not(all(feature = "x86_64-asm", not(feature = "portable"))),
        not(target_feature = "bmi2"),
        not(target_feature = "adx")
    ))]
    #[test]
    fn baseline_and_portable_builds_accept_cpus_without_extensions() {
        assert!(missing_features(0).is_empty());
        // This also exercises the actual startup entry point in safe builds.
        exit_if_cpu_lacks_pasta_assembly_features();
    }

    #[cfg(all(feature = "portable", target_feature = "bmi2", target_feature = "adx"))]
    #[test]
    fn portable_does_not_override_global_compiler_requirements() {
        assert_eq!(missing_features(0), ["bmi2", "adx"]);
    }
}
