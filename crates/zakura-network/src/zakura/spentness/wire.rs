//! Range request and response encoding.
//!
//! A request carries the artifact digest, byte offset, and length. A response
//! carries a status byte, the echoed request, and at most [`RANGE_BYTES`] of data.
//! All integers use little-endian encoding.

use std::mem::size_of;

use crate::{zakura::Frame, BoxError};

/// Largest range payload (256 KiB).
pub const RANGE_BYTES: u32 = 256 * 1024;
/// Request message type: digest, offset, and length.
pub const GET_RANGE: u16 = 1;
/// Response message type: absent (0), available (1), busy (2), cap too small (3),
/// or range outside the artifact (4).
pub const RANGE: u16 = 2;

pub(super) const DIGEST_LEN: usize = 32;
pub(super) const REQUEST_LEN: usize = DIGEST_LEN + size_of::<u64>() + size_of::<u32>();
/// A status byte followed by the echoed request.
pub(super) const RESPONSE_HEADER_LEN: usize = size_of::<u8>() + REQUEST_LEN;
const UNAVAILABLE: u8 = 0;
const AVAILABLE: u8 = 1;
const BUSY: u8 = 2;
const TOO_LARGE: u8 = 3;
const OUT_OF_RANGE: u8 = 4;

/// One bounded byte range of an artifact identified by its digest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RangeRequest {
    pub(super) digest: [u8; DIGEST_LEN],
    pub(super) offset: u64,
    pub(super) length: u32,
}

impl RangeRequest {
    /// Decode and bound-check a request frame.
    pub(super) fn parse(frame: &Frame) -> Result<Self, BoxError> {
        if frame.message_type != GET_RANGE || frame.flags != 0 {
            return Err("invalid spentness range request".into());
        }
        let request = Self::parse_payload(&frame.payload)?;
        request.checked_end()?;
        Ok(request)
    }

    pub(super) fn parse_payload(payload: &[u8]) -> Result<Self, BoxError> {
        if payload.len() != REQUEST_LEN {
            return Err("invalid spentness range request length".into());
        }
        let (digest, rest) = payload.split_at(DIGEST_LEN);
        let (offset, length) = rest.split_at(size_of::<u64>());

        Ok(Self {
            digest: digest.try_into()?,
            offset: u64::from_le_bytes(offset.try_into()?),
            length: u32::from_le_bytes(length.try_into()?),
        })
    }

    pub(super) fn payload(self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(REQUEST_LEN);
        payload.extend_from_slice(&self.digest);
        payload.extend_from_slice(&self.offset.to_le_bytes());
        payload.extend_from_slice(&self.length.to_le_bytes());
        payload
    }

    /// Return the exclusive end offset, rejecting empty, oversized, or overflowing ranges.
    pub(super) fn checked_end(self) -> Result<u64, BoxError> {
        if self.length == 0 || self.length > RANGE_BYTES {
            return Err("spentness range exceeds limits".into());
        }
        self.offset
            .checked_add(u64::from(self.length))
            .ok_or_else(|| "spentness range exceeds limits".into())
    }
}

/// A decoded response that borrows its range bytes from the frame.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum RangeResponse<'a> {
    /// The server lacks the artifact.
    Unavailable(RangeRequest),
    /// All serving slots are occupied.
    Busy(RangeRequest),
    /// The negotiated response cap cannot fit the requested range.
    TooLarge(RangeRequest),
    /// The requested range extends beyond this artifact.
    OutOfRange(RangeRequest),
    /// The exact requested range.
    Available {
        request: RangeRequest,
        bytes: &'a [u8],
    },
}

impl<'a> RangeResponse<'a> {
    pub(super) fn parse(frame: &'a Frame) -> Result<Self, BoxError> {
        if frame.message_type != RANGE || frame.flags != 0 {
            return Err("invalid spentness response".into());
        }
        let (status, payload) = frame
            .payload
            .split_first()
            .ok_or("truncated spentness response")?;
        let (request, bytes) = payload
            .split_at_checked(REQUEST_LEN)
            .ok_or("truncated spentness response header")?;
        let request = RangeRequest::parse_payload(request)?;

        if request.length == 0 && bytes.is_empty() {
            match *status {
                UNAVAILABLE => return Ok(Self::Unavailable(request)),
                BUSY => return Ok(Self::Busy(request)),
                TOO_LARGE => return Ok(Self::TooLarge(request)),
                OUT_OF_RANGE => return Ok(Self::OutOfRange(request)),
                _ => {}
            }
        }
        if *status != AVAILABLE || bytes.len() != usize::try_from(request.length)? {
            return Err("invalid spentness response status or length".into());
        }
        request.checked_end()?;
        Ok(Self::Available { request, bytes })
    }

    /// Encode an unavailable response. The echoed request carries length zero.
    pub(super) fn unavailable(request: RangeRequest) -> Frame {
        Self::negative(UNAVAILABLE, request)
    }

    pub(super) fn busy(request: RangeRequest) -> Frame {
        Self::negative(BUSY, request)
    }

    pub(super) fn too_large(request: RangeRequest) -> Frame {
        Self::negative(TOO_LARGE, request)
    }

    pub(super) fn out_of_range(request: RangeRequest) -> Frame {
        Self::negative(OUT_OF_RANGE, request)
    }

    fn negative(status: u8, request: RangeRequest) -> Frame {
        let request = RangeRequest {
            length: 0,
            ..request
        };
        Self::frame(status, request, &[])
    }

    pub(super) fn available(request: RangeRequest, bytes: &[u8]) -> Frame {
        debug_assert_eq!(u32::try_from(bytes.len()).ok(), Some(request.length));
        Self::frame(AVAILABLE, request, bytes)
    }

    fn frame(status: u8, request: RangeRequest, bytes: &[u8]) -> Frame {
        let mut payload = Vec::with_capacity(RESPONSE_HEADER_LEN + bytes.len());
        payload.push(status);
        payload.extend_from_slice(&request.payload());
        payload.extend_from_slice(bytes);
        Frame {
            message_type: RANGE,
            flags: 0,
            payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_frame(offset: u64, length: u32) -> Frame {
        Frame {
            message_type: GET_RANGE,
            flags: 0,
            payload: RangeRequest {
                digest: [0; DIGEST_LEN],
                offset,
                length,
            }
            .payload(),
        }
    }

    #[test]
    fn range_requests_are_bounded() {
        assert!(RangeRequest::parse(&request_frame(0, RANGE_BYTES)).is_ok());
        assert!(RangeRequest::parse(&request_frame(0, 0)).is_err());
        assert!(RangeRequest::parse(&request_frame(0, RANGE_BYTES + 1)).is_err());
        assert!(RangeRequest::parse(&request_frame(u64::MAX, RANGE_BYTES)).is_err());

        let mut truncated = request_frame(0, RANGE_BYTES);
        truncated.payload.truncate(REQUEST_LEN - 1);
        assert!(RangeRequest::parse(&truncated).is_err());
    }

    #[test]
    fn responses_round_trip() {
        let request = RangeRequest {
            digest: [7; DIGEST_LEN],
            offset: 3,
            length: 2,
        };
        let available = RangeResponse::available(request, &[1, 2]);
        assert_eq!(
            RangeResponse::parse(&available).unwrap(),
            RangeResponse::Available {
                request,
                bytes: &[1, 2]
            }
        );
        let unavailable = RangeResponse::unavailable(request);
        assert_eq!(unavailable.payload.len(), RESPONSE_HEADER_LEN);
        assert!(matches!(
            RangeResponse::parse(&unavailable).unwrap(),
            RangeResponse::Unavailable(_)
        ));
        for frame in [
            RangeResponse::busy(request),
            RangeResponse::too_large(request),
            RangeResponse::out_of_range(request),
        ] {
            assert_eq!(frame.payload.len(), RESPONSE_HEADER_LEN);
            assert!(RangeResponse::parse(&frame).is_ok());
        }
        let mut invalid = unavailable;
        invalid.payload[0] = 255;
        assert!(RangeResponse::parse(&invalid).is_err());
    }
}
