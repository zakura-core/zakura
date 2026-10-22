//! Record metrics without compiling the recorder lookup once per call site.
//!
//! The `metrics` crate's `counter!`, `gauge!` and `histogram!` pass a new closure to
//! [`metrics::with_recorder`] at every call site, so each site gets its own copy of the
//! recorder lookup and its thread-local access: about 100 lines of LLVM IR per site, or
//! 3-4% of the crates that record the most metrics. The macros here take the same
//! arguments and return the same handles, but register through one shared function per
//! metric kind.
//!
//! Like the `metrics` macros, they register with the thread's local recorder if there is
//! one, and otherwise with the global recorder.

use metrics::{Counter, Gauge, Histogram, Key, Metadata};

#[doc(hidden)]
pub use metrics as __metrics;

/// Returns the counter for `key` from the current recorder.
#[doc(hidden)]
#[inline(never)]
pub fn __register_counter(key: &Key, metadata: &'static Metadata<'static>) -> Counter {
    metrics::with_recorder(|recorder| recorder.register_counter(key, metadata))
}

/// Returns the gauge for `key` from the current recorder.
#[doc(hidden)]
#[inline(never)]
pub fn __register_gauge(key: &Key, metadata: &'static Metadata<'static>) -> Gauge {
    metrics::with_recorder(|recorder| recorder.register_gauge(key, metadata))
}

/// Returns the histogram for `key` from the current recorder.
#[doc(hidden)]
#[inline(never)]
pub fn __register_histogram(key: &Key, metadata: &'static Metadata<'static>) -> Histogram {
    metrics::with_recorder(|recorder| recorder.register_histogram(key, metadata))
}

/// Builds the key and metadata for one metric, and registers it with `$register`.
///
/// Literal names and labels are built once, in statics, as the `metrics` macros do.
#[doc(hidden)]
#[macro_export]
macro_rules! __register_metric {
    ($register:ident, $name:literal $(,)?) => {{
        static KEY: $crate::metrics::__metrics::Key =
            $crate::metrics::__metrics::Key::from_static_name($name);
        $crate::metrics::$register(&KEY, $crate::__metric_metadata!())
    }};
    ($register:ident, $name:literal, $($label:literal => $value:literal),+ $(,)?) => {{
        static LABELS: &[$crate::metrics::__metrics::Label] =
            &[$($crate::metrics::__metrics::Label::from_static_parts($label, $value)),+];
        static KEY: $crate::metrics::__metrics::Key =
            $crate::metrics::__metrics::Key::from_static_parts($name, LABELS);
        $crate::metrics::$register(&KEY, $crate::__metric_metadata!())
    }};
    ($register:ident, $name:expr $(, $label:expr => $value:expr)* $(,)?) => {{
        let key = $crate::metrics::__metrics::Key::from_parts(
            $name,
            ::std::vec![$($crate::metrics::__metrics::Label::new($label, $value)),*],
        );
        $crate::metrics::$register(&key, $crate::__metric_metadata!())
    }};
}

/// The metadata the `metrics` macros give a metric registered in the calling module.
#[doc(hidden)]
#[macro_export]
macro_rules! __metric_metadata {
    () => {{
        static METADATA: $crate::metrics::__metrics::Metadata<'static> =
            $crate::metrics::__metrics::Metadata::new(
                ::core::module_path!(),
                $crate::metrics::__metrics::Level::INFO,
                ::core::option::Option::Some(::core::module_path!()),
            );
        &METADATA
    }};
}

/// Registers a counter, like `metrics::counter!`.
///
/// Takes a name, then optional `label => value` pairs.
#[doc(hidden)]
#[macro_export]
macro_rules! __counter {
    ($($input:tt)*) => { $crate::__register_metric!(__register_counter, $($input)*) };
}

/// Registers a gauge, like `metrics::gauge!`.
///
/// Takes a name, then optional `label => value` pairs.
#[doc(hidden)]
#[macro_export]
macro_rules! __gauge {
    ($($input:tt)*) => { $crate::__register_metric!(__register_gauge, $($input)*) };
}

/// Registers a histogram, like `metrics::histogram!`.
///
/// Takes a name, then optional `label => value` pairs.
#[doc(hidden)]
#[macro_export]
macro_rules! __histogram {
    ($($input:tt)*) => { $crate::__register_metric!(__register_histogram, $($input)*) };
}

pub use crate::{__counter as counter, __gauge as gauge, __histogram as histogram};

#[cfg(test)]
mod tests;
