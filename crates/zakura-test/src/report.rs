//! Labelled sections attached to [`eyre::Report`]s, for test failure diagnostics.
//!
//! Test helpers attach context such as the command line, exit status, and unread
//! output to a report. [`init`](crate::init) installs [`SectionHandler`], which
//! prints those sections after eyre's default report. Reports created without
//! that handler fall back to wrapping the section as context.

use std::{error::Error as StdError, fmt};

use eyre::{EyreHandler, Report};

/// An eyre handler that prints labelled sections after the default report.
pub struct SectionHandler {
    inner: Box<dyn EyreHandler>,
    sections: Vec<String>,
}

impl SectionHandler {
    /// Installs this handler as the global eyre hook.
    ///
    /// Does nothing if another hook is already installed.
    pub fn install() {
        let _ = eyre::set_hook(Box::new(|error| {
            Box::new(SectionHandler {
                inner: eyre::DefaultHandler::default_with(error),
                sections: Vec::new(),
            })
        }));
    }
}

impl EyreHandler for SectionHandler {
    fn debug(&self, error: &(dyn StdError + 'static), f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.debug(error, f)?;

        if f.alternate() {
            return Ok(());
        }

        for section in &self.sections {
            write!(f, "\n\n{section}")?;
        }

        Ok(())
    }

    fn display(&self, error: &(dyn StdError + 'static), f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.display(error, f)
    }

    fn track_caller(&mut self, location: &'static std::panic::Location<'static>) {
        self.inner.track_caller(location);
    }
}

/// A section body under a header, with the body indented by three spaces, as
/// color-eyre formatted it.
pub struct IndentedSection<H, B> {
    header: H,
    body: B,
}

impl<H: fmt::Display, B: fmt::Display> fmt::Display for IndentedSection<H, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.header)?;

        for line in self.body.to_string().lines() {
            write!(f, "\n   {line}")?;
        }

        Ok(())
    }
}

/// Adds a header to a section body.
pub trait SectionExt: Sized {
    /// Returns this value as a section body under `header`.
    fn header<H: fmt::Display>(self, header: H) -> IndentedSection<H, Self>;
}

impl<T: fmt::Display> SectionExt for T {
    fn header<H: fmt::Display>(self, header: H) -> IndentedSection<H, Self> {
        IndentedSection { header, body: self }
    }
}

/// Attaches labelled sections to a report, or to the error in a result.
pub trait Section: Sized {
    /// The type returned after attaching a section.
    type Return;

    /// Attaches `section` to the report.
    fn section<D: fmt::Display>(self, section: D) -> Self::Return;

    /// Attaches the section returned by `section`, only evaluating it for an error.
    fn with_section<D: fmt::Display, F: FnOnce() -> D>(self, section: F) -> Self::Return;

    /// Attaches a warning to the report.
    fn warning<D: fmt::Display>(self, warning: D) -> Self::Return {
        self.with_section(|| format!("Warning: {warning}"))
    }
}

impl Section for Report {
    type Return = Report;

    fn section<D: fmt::Display>(mut self, section: D) -> Report {
        match self.handler_mut().downcast_mut::<SectionHandler>() {
            Some(handler) => {
                handler.sections.push(section.to_string());
                self
            }
            None => self.wrap_err(section.to_string()),
        }
    }

    fn with_section<D: fmt::Display, F: FnOnce() -> D>(self, section: F) -> Report {
        self.section(section())
    }
}

impl<T, E: Into<Report>> Section for Result<T, E> {
    type Return = Result<T, Report>;

    fn section<D: fmt::Display>(self, section: D) -> Self::Return {
        self.map_err(|error| error.into().section(section))
    }

    fn with_section<D: fmt::Display, F: FnOnce() -> D>(self, section: F) -> Self::Return {
        self.map_err(|error| error.into().section(section()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Formats `error` with `handler`, independently of the global eyre hook.
    struct WithHandler<'a>(&'a SectionHandler, &'a (dyn StdError + 'static));

    impl fmt::Debug for WithHandler<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.debug(self.1, f)
        }
    }

    #[test]
    fn sections_are_printed_after_the_report() {
        let error = std::io::Error::other("command failed");
        let handler = SectionHandler {
            inner: eyre::DefaultHandler::default_with(&error),
            sections: vec![
                "first line\nsecond line".header("Stdout:").to_string(),
                "Warning: possible port conflict".to_string(),
            ],
        };

        let output = format!("{:?}", WithHandler(&handler, &error));

        assert!(
            output.starts_with(
                "command failed\n\nStdout:\n   first line\n   second line\n\n\
                 Warning: possible port conflict"
            ),
            "{output}"
        );
    }

    #[test]
    fn result_sections_are_only_built_for_errors() {
        let ok: Result<u8, Report> = Ok(1);
        let ok = ok.with_section(|| -> String { panic!("not evaluated for Ok") });

        assert_eq!(ok.expect("the value is Ok"), 1);
    }
}
