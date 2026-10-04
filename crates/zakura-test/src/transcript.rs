//! A [`Service`] implementation based on a fixed transcript.

use std::{
    fmt::Debug,
    task::{Context, Poll},
};

use color_eyre::{
    eyre::{eyre, Report},
    section::SectionExt,
};
use futures::future::{ready, Ready};
use tower::{Service, ServiceExt};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// An expected error in a transcript.
#[derive(Debug, Clone)]
pub enum ExpectedTranscriptError {
    /// Match any error.
    Any,
}

#[derive(Debug, thiserror::Error)]
#[error("ErrorChecker Error: {0}")]
struct ErrorCheckerError(BoxError);

/// A transcript: a list of requests and expected results.
#[must_use]
pub struct Transcript<R, S, I>
where
    I: Iterator<Item = (R, Result<S, ExpectedTranscriptError>)>,
{
    messages: I,
}

impl<R, S, I> From<I> for Transcript<R, S, I::IntoIter>
where
    I: IntoIterator<Item = (R, Result<S, ExpectedTranscriptError>)>,
{
    fn from(messages: I) -> Self {
        Self {
            messages: messages.into_iter(),
        }
    }
}

impl<R, S, I> Transcript<R, S, I>
where
    I: Iterator<Item = (R, Result<S, ExpectedTranscriptError>)>,
    R: Debug,
    S: Debug + Eq,
{
    /// Check this transcript against the responses from the `to_check` service
    pub async fn check<C>(mut self, mut to_check: C) -> Result<(), Report>
    where
        C: Service<R, Response = S>,
        C::Error: Into<BoxError>,
    {
        for (req, expected_rsp) in &mut self.messages {
            // These unwraps could propagate errors with the correct
            // bound on C::Error
            let fut = to_check
                .ready()
                .await
                .map_err(Into::into)
                .map_err(|e| eyre!(e))
                .expect("expected service to not fail during execution of transcript");

            let response = fut.call(req).await;

            match (response, expected_rsp) {
                (Ok(rsp), Ok(expected_rsp)) => {
                    if rsp != expected_rsp {
                        Err(eyre!(
                            "response doesn't match transcript's expected response"
                        ))
                        .with_section(|| format!("{expected_rsp:?}").header("Expected Response:"))
                        .with_section(|| format!("{rsp:?}").header("Found Response:"))?;
                    }
                }
                (Ok(rsp), Err(_)) => {
                    return Err(eyre!("received a response when an error was expected"))
                        .with_section(|| format!("{rsp:?}").header("Found Response:"));
                }
                (Err(e), Ok(expected_rsp)) => {
                    Err(eyre!("received an error when a response was expected"))
                        .with_error(|| ErrorCheckerError(e.into()))
                        .with_section(|| format!("{expected_rsp:?}").header("Expected Response:"))?
                }
                (Err(_), Err(_)) => continue,
            }
        }
        Ok(())
    }
}

impl<R, S, I> Service<R> for Transcript<R, S, I>
where
    R: Debug + Eq,
    I: Iterator<Item = (R, Result<S, ExpectedTranscriptError>)>,
{
    type Response = S;
    type Error = Report;
    type Future = Ready<Result<S, Report>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    #[track_caller]
    fn call(&mut self, request: R) -> Self::Future {
        if let Some((expected_request, response)) = self.messages.next() {
            match response {
                Ok(response) => {
                    if request == expected_request {
                        ready(Ok(response))
                    } else {
                        ready(
                            Err(eyre!("received unexpected request"))
                                .with_section(|| {
                                    format!("{expected_request:?}").header("Expected Request:")
                                })
                                .with_section(|| format!("{request:?}").header("Found Request:")),
                        )
                    }
                }
                Err(_) => ready(Err(eyre!("mock error"))),
            }
        } else {
            ready(Err(eyre!("Got request after transcript ended")))
        }
    }
}
