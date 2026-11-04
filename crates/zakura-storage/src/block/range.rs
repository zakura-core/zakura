//! Serving's range reads, as pending operations of the controlled block
//! service.
//!
//! [`ControlledRangeSource`] implements [`BlockRangeSource`]. A read's future
//! submits it on its first poll, and the read then waits as a pending
//! operation with the lease it carries, since the lease type has no public
//! constructor. The run completes it, and the future returns the result,
//! which carries the lease back. Dropping the future cancels only the
//! caller: a pending read keeps its lease until it completes, as
//! zakura-state's blocking read keeps it.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures::future::BoxFuture;
use zakura_network::{
    zakura::{BlockRangeRead, BlockRangeReadResult, BlockRangeSource},
    BoxError,
};

use super::{
    controlled::{ControlledBlockService, OpId},
    BlockRequest,
};

/// A [`BlockRangeSource`] whose reads wait for the run to complete them.
#[derive(Clone, Debug)]
pub struct ControlledRangeSource {
    service: ControlledBlockService,
}

impl ControlledRangeSource {
    pub(super) fn new(service: ControlledBlockService) -> Self {
        ControlledRangeSource { service }
    }
}

impl BlockRangeSource for ControlledRangeSource {
    fn read(
        &self,
        request: BlockRangeRead,
    ) -> BoxFuture<'static, Result<BlockRangeReadResult, BoxError>> {
        Box::pin(ReadCall {
            service: self.service.clone(),
            state: ReadState::Unsubmitted(Some(request)),
        })
    }
}

/// One caller's view of a read.
struct ReadCall {
    service: ControlledBlockService,
    state: ReadState,
}

enum ReadState {
    /// Not polled yet. A read dropped here never reaches storage.
    Unsubmitted(Option<BlockRangeRead>),
    /// Submitted as this operation.
    Submitted(OpId),
    /// Polled after shutdown, so never submitted. It never completes.
    Orphaned,
    /// The reply was taken.
    Done,
}

impl Future for ReadCall {
    type Output = Result<BlockRangeReadResult, BoxError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        match &mut this.state {
            ReadState::Unsubmitted(read) => {
                let read = read.take().expect("an unsubmitted read holds its request");
                this.state = match this
                    .service
                    .submit_read(BlockRequest::ReadRange(read), cx.waker().clone())
                {
                    Some(id) => ReadState::Submitted(id),
                    None => ReadState::Orphaned,
                };
                Poll::Pending
            }
            ReadState::Submitted(id) => match this.service.poll_read(*id, cx.waker()) {
                Some(Some(result)) => {
                    this.state = ReadState::Done;
                    Poll::Ready(result)
                }
                // Waiting, or dropped by shutdown.
                Some(None) | None => Poll::Pending,
            },
            ReadState::Orphaned => Poll::Pending,
            ReadState::Done => panic!("a range read was polled after it returned"),
        }
    }
}

impl Drop for ReadCall {
    fn drop(&mut self) {
        if let ReadState::Submitted(id) = self.state {
            self.service.drop_read(id);
        }
    }
}
