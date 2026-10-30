//! The counters libFuzzer reads as extra features, and the switch that
//! hides them (explorer implementation plan, E2a).

use std::{env, sync::OnceLock};

use zakura_sim::explore::feedback::{
    Counters, Feedback, NewCollector, FEATURES, FEEDBACK_BACKEND, FEEDBACK_VERSION,
};

/// The variable that turns semantic feedback `on`, the default, or `off`.
pub const FEEDBACK_VAR: &str = "ZAKURA_SIM_FEEDBACK";

/// The counters libFuzzer reads. On Linux they sit in the section whose
/// `__start_` and `__stop_` symbols libFuzzer reads as its extra counters.
/// libFuzzer's Darwin build reads no extra counters, and Mach-O section
/// names take another form, so other platforms get a plain static.
#[cfg_attr(target_os = "linux", link_section = "__libfuzzer_extra_counters")]
#[used]
static COUNTERS: Counters = Counters::new();

/// The counters a collector writes with feedback `off`. libFuzzer never
/// reads them. The collector still runs and writes, so `on` and `off` runs
/// take the same code path, and differ only in what libFuzzer reads.
static HIDDEN: Counters = Counters::new();

/// Feedback that writes `collector`'s features where `ZAKURA_SIM_FEEDBACK`
/// says.
pub fn feedback(collector: NewCollector) -> Feedback {
    Feedback::new(counters(), collector)
}

/// The counters `ZAKURA_SIM_FEEDBACK` picks, read once per process. The
/// first call logs the backend, its version, the feature count and the
/// mode, as `zakura-sim feedback: backend=… version=… features=… mode=…`.
///
/// # Panics
///
/// If the variable holds anything but `on` or `off`.
fn counters() -> &'static Counters {
    static PICKED: OnceLock<&'static Counters> = OnceLock::new();
    PICKED.get_or_init(|| {
        // A value that isn't UTF-8 reaches the panic below, not `on`.
        let mode = env::var_os(FEEDBACK_VAR).map_or_else(
            || "on".to_owned(),
            |value| value.to_string_lossy().into_owned(),
        );
        let counters = match mode.as_str() {
            "on" if cfg!(target_os = "linux") => &COUNTERS,
            "on" => panic!("{FEEDBACK_VAR}=on: libFuzzer reads extra counters only on Linux"),
            "off" => &HIDDEN,
            other => panic!("{FEEDBACK_VAR} must be `on` or `off`, not `{other}`"),
        };
        eprintln!(
            "zakura-sim feedback: backend={FEEDBACK_BACKEND} version={FEEDBACK_VERSION} \
             features={FEATURES} mode={mode}"
        );
        counters
    })
}

/// Reads `ZAKURA_SIM_FEEDBACK` and logs the mode, so a bad value fails
/// before the first input. A target with a collector calls this in its
/// `init`.
pub fn init_feedback() {
    counters();
}
