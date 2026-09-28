//! The response of one served request: frames, then exactly one ending.

use std::{
    fmt,
    sync::{Arc, Mutex, PoisonError},
};

use thiserror::Error;
use tokio::sync::mpsc;

use super::{Commitment, ResponseGrants};
use crate::zakura::{
    wire_codec::{encode_frame, WireMessage},
    FrameGuard, MessageRole, MessageRule, FRAME_HEADER_BYTES,
};

/// The most a response may carry, chosen from its request.
///
/// The requester's [`Reservations`](crate::zakura::regulation::Reservations)
/// bound the same response with the same value, so a reactor computes it
/// once for both sides.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResponseCap {
    /// Frames before the ending. The ending is not counted.
    pub(crate) frames: u32,
    /// Payload bytes of every frame, the ending included. Frame headers are
    /// not counted.
    pub(crate) bytes: u64,
}

impl ResponseCap {
    /// Encoded bytes of the largest response, frame headers included.
    pub(crate) fn output_bytes(self) -> u64 {
        // Widening usize to u64 is lossless on supported targets.
        let header = FRAME_HEADER_BYTES as u64;
        let frames = u64::from(self.frames).saturating_add(1);
        self.bytes.saturating_add(frames.saturating_mul(header))
    }
}

/// Frames and payload bytes a response has queued so far.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct SinkProgress {
    /// Frames queued before the ending.
    pub(crate) frames: u32,
    /// Payload bytes queued, the ending included.
    pub(crate) bytes: u64,
}

/// A local defect in a response. The peer is not at fault.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum SinkError {
    /// No row answers this request with this message type.
    #[error("message type {message_type} does not answer this request")]
    NotAResponse {
        /// The message's type.
        message_type: u16,
    },
    /// `send` got an ending.
    #[error("message type {message_type} ends the exchange; use finish")]
    EndingOnSend {
        /// The message's type.
        message_type: u16,
    },
    /// `finish` got a message that does not end the exchange.
    #[error("message type {message_type} does not end the exchange")]
    NotAnEnding {
        /// The message's type.
        message_type: u16,
    },
    /// The message would exceed the response cap, or leave no room for the
    /// ending.
    #[error("response would exceed its cap {cap:?} after {sent:?}")]
    OverCap {
        /// The response's cap.
        cap: ResponseCap,
        /// What the response queued before this message.
        sent: SinkProgress,
    },
    /// The message did not encode.
    #[error("response did not encode: {0}")]
    Encode(String),
    /// The session's output closed.
    #[error("the session's output closed")]
    Closed,
}

/// One frame of a response, handed to the session's ordered output.
#[derive(Debug)]
pub(super) struct ResponseFrame {
    pub(super) frame: crate::zakura::Frame,
    /// Holds the response's output grants until the frame's write finishes.
    pub(super) guard: FrameGuard,
    pub(super) ends: bool,
}

/// State shared by a [`ResponseSink`] and the task that runs its `produce`.
///
/// The task keeps a handle so it can queue the ending after `produce` returns
/// without one.
pub(super) struct SinkCore {
    request: u16,
    rules: &'static [MessageRule],
    cap: ResponseCap,
    /// Payload bytes kept for the largest ending, so the ending always fits.
    ending_reserve: u64,
    progress: SinkProgress,
    frames: Option<mpsc::UnboundedSender<ResponseFrame>>,
    /// Output grants, shared with every queued frame.
    grants: Arc<ResponseGrants>,
    /// Released when the ending enters the ordered output.
    commitment: Option<Commitment>,
}

impl fmt::Debug for SinkCore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SinkCore")
            .field("request", &self.request)
            .field("cap", &self.cap)
            .field("progress", &self.progress)
            .field("ended", &self.frames.is_none())
            .finish_non_exhaustive()
    }
}

impl SinkCore {
    pub(super) fn new(
        request: &'static MessageRule,
        rules: &'static [MessageRule],
        cap: ResponseCap,
        frames: mpsc::UnboundedSender<ResponseFrame>,
        grants: Arc<ResponseGrants>,
        commitment: Commitment,
    ) -> Self {
        let ending_reserve = ending_reserve(request.message_type, rules);
        Self {
            request: request.message_type,
            rules,
            cap: ResponseCap {
                // A cap always holds the largest ending.
                bytes: cap.bytes.max(ending_reserve),
                ..cap
            },
            ending_reserve,
            progress: SinkProgress::default(),
            frames: Some(frames),
            grants,
            commitment: Some(commitment),
        }
    }

    /// The cap, raised to hold the largest ending.
    pub(super) fn cap(&self) -> ResponseCap {
        self.cap
    }

    pub(super) fn progress(&self) -> SinkProgress {
        self.progress
    }

    /// Whether the ending entered the ordered output.
    pub(super) fn ended(&self) -> bool {
        self.frames.is_none()
    }

    /// Check `message` against the request and the cap, then queue it.
    pub(super) fn push<M: WireMessage>(&mut self, message: &M, ends: bool) -> Result<(), SinkError>
    where
        M::Error: fmt::Display,
    {
        let message_type = message.message_type();
        let answers = matches!(
            MessageRule::find(self.rules, message_type).map(|row| row.role),
            Some(MessageRole::Response { request, .. }) if request == self.request
        );
        let row_ends = matches!(
            MessageRule::find(self.rules, message_type).map(|row| row.role),
            Some(MessageRole::Response {
                ends_exchange: true,
                ..
            })
        );
        match (answers, row_ends, ends) {
            (false, ..) => return Err(SinkError::NotAResponse { message_type }),
            (true, true, false) => return Err(SinkError::EndingOnSend { message_type }),
            (true, false, true) => return Err(SinkError::NotAnEnding { message_type }),
            _ => {}
        }
        let Some(frames) = &self.frames else {
            return Err(SinkError::Closed);
        };
        let frame = encode_frame(message).map_err(|error| SinkError::Encode(error.to_string()))?;
        // Widening usize to u64 is lossless on supported targets.
        let len = frame.payload.len() as u64;
        let over = SinkError::OverCap {
            cap: self.cap,
            sent: self.progress,
        };
        let bytes = self.progress.bytes.checked_add(len).ok_or(over.clone())?;
        let kept = if ends { 0 } else { self.ending_reserve };
        if bytes.saturating_add(kept) > self.cap.bytes
            || (!ends && self.progress.frames >= self.cap.frames)
        {
            return Err(over);
        }
        frames
            .send(ResponseFrame {
                frame,
                guard: FrameGuard::new(self.grants.clone()),
                ends,
            })
            .map_err(|_| SinkError::Closed)?;
        self.progress.bytes = bytes;
        if ends {
            // The ending is in the ordered output: the exchange is complete on
            // this side, so the peer's commitment is free.
            self.frames = None;
            self.commitment = None;
        } else {
            self.progress.frames += 1;
        }
        Ok(())
    }
}

/// The largest ending payload for `request`.
pub(in crate::zakura::regulation) fn ending_reserve(request: u16, rules: &[MessageRule]) -> u64 {
    rules
        .iter()
        .filter_map(|row| match row.role {
            MessageRole::Response {
                request: answered,
                ends_exchange: true,
            } if answered == request => Some(row.payload.max()),
            _ => None,
        })
        .max()
        // Widening usize to u64 is lossless on supported targets.
        .map_or(0, |max| max as u64)
}

/// Accepts one request's response: frames, then exactly one ending.
///
/// `finish` consumes the sink, so a response has at most one ending.
///
/// Dropping the sink without an ending lets Serve queue the reactor's
/// `local_failure` ending instead.
#[derive(Debug)]
pub(crate) struct ResponseSink<M> {
    core: Arc<Mutex<SinkCore>>,
    message: std::marker::PhantomData<fn(&M)>,
}

/// Proof that the exchange ended. Only [`ResponseSink::finish`] makes one.
#[derive(Debug)]
pub(crate) struct Responded(());

impl<M: WireMessage> ResponseSink<M>
where
    M::Error: fmt::Display,
{
    pub(super) fn new(core: Arc<Mutex<SinkCore>>) -> Self {
        Self {
            core,
            message: std::marker::PhantomData,
        }
    }

    fn core(&self) -> std::sync::MutexGuard<'_, SinkCore> {
        self.core.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Encode and queue a frame that does not end the exchange.
    ///
    /// Never waits for the peer: Serve reserved the whole cap before
    /// `produce` started. The sink keeps room for the largest ending.
    pub(crate) fn send(&mut self, message: &M) -> Result<(), SinkError> {
        self.core().push(message, false)
    }

    /// Encode and queue the ending, consuming the sink.
    pub(crate) fn finish(self, ending: &M) -> Result<Responded, SinkError> {
        self.core().push(ending, true)?;
        Ok(Responded(()))
    }

    /// Frames and bytes queued so far.
    pub(crate) fn progress(&self) -> SinkProgress {
        self.core().progress()
    }

    /// The response's cap.
    pub(crate) fn cap(&self) -> ResponseCap {
        self.core().cap()
    }
}
