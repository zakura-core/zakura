//! Shared configuration for reproducible generated contract tests.

use std::env;

use proptest::test_runner::{Config as ProptestConfig, RngSeed, TestRunner};
use rand::{rngs::OsRng, RngCore};

/// Prove a contract manifest contains every required ID and points only to
/// registered tests whose names begin with that ID.
pub(super) fn assert_contract_test_manifest(expected_ids: &[&str], manifest: &[(&str, &[&str])]) {
    let actual: std::collections::BTreeSet<_> = manifest.iter().map(|(id, _)| *id).collect();
    let expected: std::collections::BTreeSet<_> = expected_ids.iter().copied().collect();
    assert_eq!(
        actual.len(),
        manifest.len(),
        "contract requirement IDs must be unique"
    );
    assert_eq!(
        actual, expected,
        "missing or duplicate contract requirement IDs"
    );

    let test_binary = std::env::current_exe().expect("the test binary path is available");
    let listed = std::process::Command::new(test_binary)
        .args(["--list", "--format", "terse", "--color", "never"])
        .output()
        .expect("the test binary can list its registered tests");
    assert!(
        listed.status.success(),
        "the test inventory command succeeds"
    );
    let listed = String::from_utf8(listed.stdout).expect("test inventory names are UTF-8");
    let registered: std::collections::BTreeSet<_> = listed
        .lines()
        .filter_map(|line| line.strip_suffix(": test"))
        .filter_map(|path| path.rsplit("::").next())
        .collect();
    for (id, test_names) in manifest {
        assert!(!test_names.is_empty(), "{id} must name at least one test");
        let id_prefix = format!("{}_", id.to_ascii_lowercase().replace('-', "_"));
        for test_name in *test_names {
            assert!(
                test_name.starts_with(&id_prefix),
                "{id} names test {test_name} without its ID prefix"
            );
            assert!(
                registered.contains(test_name),
                "{id} names unregistered test {test_name}"
            );
        }
    }
}

/// Effective case count and seed for one generated test lane.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct GeneratedTestConfig {
    cases: u32,
    seed: u64,
}

impl GeneratedTestConfig {
    /// Read strict numeric overrides, choosing and retaining a seed when none is
    /// supplied so every generated run prints a same-generator rerun command.
    pub(super) fn from_env(
        cases_variable: &str,
        seed_variable: &str,
        default_cases: u32,
    ) -> Result<Self, String> {
        let cases = read_optional(cases_variable)?;
        let seed = read_optional(seed_variable)?;
        Self::from_values(
            cases.as_deref(),
            seed.as_deref(),
            cases_variable,
            seed_variable,
            default_cases,
        )
    }

    /// Return the effective generated case count.
    pub(super) fn cases(self) -> u32 {
        self.cases
    }

    /// Build a runner pinned to the effective seed.
    pub(super) fn runner(self, source_file: &'static str) -> TestRunner {
        let mut config = ProptestConfig::with_source_file(source_file);
        config.cases = self.cases;
        config.rng_seed = RngSeed::Fixed(self.seed);
        TestRunner::new(config)
    }

    /// Print the inputs needed to rerun an unchanged generator.
    #[allow(clippy::print_stdout)]
    pub(super) fn announce(self, lane: &str, cases_variable: &str, seed_variable: &str) {
        println!(
            "{lane}: cases={}, seed={}; replay with {cases_variable}={} {seed_variable}={}",
            self.cases, self.seed, self.cases, self.seed,
        );
    }

    fn from_values(
        cases: Option<&str>,
        seed: Option<&str>,
        cases_variable: &str,
        seed_variable: &str,
        default_cases: u32,
    ) -> Result<Self, String> {
        let cases = match cases {
            Some(value) => parse_numeric::<u32>(cases_variable, value)?,
            None => default_cases,
        };
        if cases == 0 {
            return Err(format!("{cases_variable} must be greater than zero"));
        }
        let seed = match seed {
            Some(value) => parse_numeric::<u64>(seed_variable, value)?,
            None => OsRng.next_u64(),
        };
        Ok(Self { cases, seed })
    }
}

fn read_optional(variable: &str) -> Result<Option<String>, String> {
    match env::var(variable) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(format!("{variable} is not valid UTF-8")),
    }
}

fn parse_numeric<T>(variable: &str, value: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| format!("invalid {variable} value {value:?}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::GeneratedTestConfig;

    #[test]
    fn generated_test_config_uses_explicit_values() {
        let config = GeneratedTestConfig::from_values(Some("128"), Some("42"), "CASES", "SEED", 64)
            .expect("valid overrides parse");
        assert_eq!(
            config,
            GeneratedTestConfig {
                cases: 128,
                seed: 42
            }
        );
    }

    #[test]
    fn generated_test_config_rejects_invalid_or_empty_case_budgets() {
        for cases in ["", "0", "abc", "-1"] {
            assert!(
                GeneratedTestConfig::from_values(Some(cases), Some("42"), "CASES", "SEED", 64,)
                    .is_err(),
                "case override {cases:?} should be rejected",
            );
        }
    }

    #[test]
    fn generated_test_config_rejects_invalid_seeds() {
        assert!(GeneratedTestConfig::from_values(
            Some("64"),
            Some("not-a-seed"),
            "CASES",
            "SEED",
            64,
        )
        .is_err());
    }
}
