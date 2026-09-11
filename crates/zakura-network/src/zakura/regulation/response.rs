//! Requester-side credit, independent of scheduling and handler resources.

use thiserror::Error;

/// Authorized response parts and bytes. Identity and ending rules belong to the message.
///
/// Consuming the last part does not consume an ending. The owner retains this
/// value until its message-specific terminal or connection closure. There is no
/// local-work expiry operation and spent credit cannot be restored.
#[derive(Debug)]
pub(crate) struct ResponseCredit {
    objects: u64,
    bytes: u64,
    consumed_objects: u64,
    consumed_bytes: u64,
}

#[derive(Debug, Error)]
#[error("response exceeds its authorized object or byte credit")]
pub(crate) struct ResponseCreditExceeded;

impl ResponseCredit {
    pub(crate) fn new(objects: u64, bytes: u64) -> Self {
        Self {
            objects,
            bytes,
            consumed_objects: 0,
            consumed_bytes: 0,
        }
    }

    /// Check before allocation or waiting for handler capacity. Consumption is separate.
    pub(crate) fn check(&self, objects: u64, bytes: u64) -> Result<(), ResponseCreditExceeded> {
        if objects > self.objects - self.consumed_objects
            || bytes > self.bytes - self.consumed_bytes
        {
            return Err(ResponseCreditExceeded);
        }
        Ok(())
    }

    /// Spend validated parts before calling the handler, even when it discards them.
    pub(crate) fn consume(
        &mut self,
        objects: u64,
        bytes: u64,
    ) -> Result<(), ResponseCreditExceeded> {
        self.check(objects, bytes)?;
        // The subtraction check proves both additions fit within their limits.
        self.consumed_objects += objects;
        self.consumed_bytes += bytes;
        Ok(())
    }

    pub(crate) fn consumed_objects(&self) -> u64 {
        self.consumed_objects
    }
    pub(crate) fn consumed_bytes(&self) -> u64 {
        self.consumed_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_bounds_and_rejected_parts_preserve_consumption() {
        let mut credit = ResponseCredit::new(2, 10);
        credit.consume(1, 6).unwrap();
        assert!(credit.consume(1, 5).is_err());
        assert_eq!((credit.consumed_objects(), credit.consumed_bytes()), (1, 6));
        credit.consume(1, 4).unwrap();
        assert!(credit.consume(1, 0).is_err());
        assert!(credit.consume(0, 1).is_err());
    }

    #[test]
    fn counters_cannot_wrap_at_the_integer_boundary() {
        let mut credit = ResponseCredit::new(u64::MAX, u64::MAX);
        credit.consume(u64::MAX, u64::MAX).unwrap();
        assert!(credit.consume(1, 1).is_err());
        assert_eq!(credit.consumed_objects(), u64::MAX);
        assert_eq!(credit.consumed_bytes(), u64::MAX);
    }
}
