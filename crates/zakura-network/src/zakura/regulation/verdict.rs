//! The outcome of checking one inbound message.

use super::ClaimRefused;
use crate::zakura::SinkReject;

/// What the receiver does with one inbound message.
///
/// The four results follow the peer-message regulation specification. A
/// `Drop` is not a violation: the message was valid but no longer useful.
#[must_use]
#[derive(Debug)]
pub(crate) enum Verdict {
    /// Deliver the message.
    Continue,
    /// Discard the message and keep the connection.
    Drop {
        /// Stable metric label.
        reason: &'static str,
    },
    /// The peer violated the protocol; close the connection.
    Disconnect {
        /// Stable metric and trace label.
        reason: &'static str,
        /// Human-readable detail for logs.
        detail: String,
    },
    /// A local fault stopped processing; the peer is not at fault.
    LocalFault {
        /// Stable metric label.
        reason: &'static str,
        /// Human-readable detail for logs.
        detail: String,
    },
}

impl Verdict {
    /// Map this verdict onto a stream sink's result.
    ///
    /// `Continue` and `Drop` keep the stream open; a drop is counted.
    pub(crate) fn into_stream_result(self) -> Result<(), SinkReject> {
        match self {
            Self::Continue => Ok(()),
            Self::Drop { reason } => {
                metrics::counter!("zakura.p2p.message.dropped", "reason" => reason).increment(1);
                Ok(())
            }
            Self::Disconnect { reason, detail } => Err(SinkReject::protocol(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{reason}: {detail}"),
            ))),
            Self::LocalFault { reason, detail } => {
                Err(SinkReject::local(format!("{reason}: {detail}")))
            }
        }
    }
}

impl From<ClaimRefused> for Verdict {
    fn from(refused: ClaimRefused) -> Self {
        Self::Disconnect {
            reason: refused.label(),
            detail: format!("{refused:?}"),
        }
    }
}
