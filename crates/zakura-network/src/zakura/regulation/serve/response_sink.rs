//! The single response of one served request.

use crate::zakura::{Frame, FRAME_HEADER_BYTES};

use super::ServeEnd;

/// Accepts the one response of a request. `respond` consumes the sink.
#[derive(Debug)]
pub(crate) struct ResponseSink {
    response_cap: u32,
}

/// Proof that `produce` responded. Only [`ResponseSink::respond`] makes one.
#[derive(Debug)]
pub(crate) struct Responded(Frame);

impl ResponseSink {
    pub(super) fn new(response_cap: u32) -> Self {
        Self { response_cap }
    }

    /// Accept `frame` as the response if it fits the declared bound.
    pub(crate) fn respond(self, frame: Frame) -> Result<Responded, ServeEnd> {
        let frame_len = FRAME_HEADER_BYTES.saturating_add(frame.payload.len());
        // The cast widens u32 to usize on supported targets.
        if frame_len > self.response_cap as usize {
            return Err(ServeEnd::LocalFault(format!(
                "response of {frame_len} bytes exceeds its {} byte bound",
                self.response_cap
            )));
        }
        Ok(Responded(frame))
    }
}

impl Responded {
    pub(super) fn into_frame(self) -> Frame {
        self.0
    }
}
