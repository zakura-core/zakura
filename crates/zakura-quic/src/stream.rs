//! Stream handles (API-5).
//!
//! Each handle sends its requests to the connection task in order. A send
//! handle pipelines writes up to [`WRITE_AHEAD_BYTES`] ahead of quinn-proto, so
//! a burst of small writes leaves in one packet. A receive handle has at most
//! one read in flight. A cancelled operation's reply stays in the handle, and
//! the next call waits for it first, so cancelling a call never reorders or
//! loses stream data.

use std::collections::VecDeque;

use bytes::{Buf as _, Bytes};
use quinn_proto::{StreamId, VarInt};
use tokio::sync::oneshot::{self, error::TryRecvError};

use crate::{
    driver::{ConnCmd, ConnRef, ReadReply},
    error::{ClosedStream, ReadError, ReadExactError, ReadToEndError, WriteError},
};

/// Bytes one stream may queue at the connection task before a write waits for
/// quinn-proto to accept them. Without write-ahead, every write would cost a
/// round trip to the connection task and its own packet.
pub(crate) const WRITE_AHEAD_BYTES: usize = 64 * 1024;

type WriteReply = oneshot::Receiver<Result<(), WriteError>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SendState {
    Open,
    Finished,
    Reset,
}

/// The sending half of a bidirectional stream.
///
/// Dropping it without [`finish`](Self::finish) or [`reset`](Self::reset)
/// finishes the stream once every write is delivered to quinn-proto.
#[derive(Debug)]
pub struct SendStream {
    conn: ConnRef,
    id: StreamId,
    state: SendState,
    /// Writes the connection task hasn't fully handed to quinn-proto, oldest
    /// first, with their sizes.
    pending: VecDeque<(usize, WriteReply)>,
    pending_bytes: usize,
}

impl SendStream {
    pub(crate) fn new(conn: ConnRef, id: StreamId) -> Self {
        Self {
            conn,
            id,
            state: SendState::Open,
            pending: VecDeque::new(),
            pending_bytes: 0,
        }
    }

    /// Writes all of `buf`. Returns once at most [`WRITE_AHEAD_BYTES`] of this
    /// stream's data wait for quinn-proto, which waits on the peer's flow
    /// control and the send window.
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), WriteError> {
        self.write_chunk(Bytes::copy_from_slice(buf)).await
    }

    /// Writes `data` without copying it.
    ///
    /// A failure of an earlier write surfaces here, because writes pipeline.
    pub async fn write_chunk(&mut self, data: Bytes) -> Result<(), WriteError> {
        if self.state != SendState::Open {
            return Err(WriteError::ClosedStream);
        }
        self.reap()?;
        if data.is_empty() {
            return Ok(());
        }
        let len = data.len();
        let (reply, result) = oneshot::channel();
        self.conn
            .send(ConnCmd::Write {
                id: self.id,
                data,
                reply,
            })
            .map_err(WriteError::ConnectionLost)?;
        self.pending.push_back((len, result));
        self.pending_bytes += len;
        while self.pending_bytes > WRITE_AHEAD_BYTES {
            self.settle_oldest().await?;
        }
        Ok(())
    }

    /// Collects the replies that already arrived, without waiting.
    fn reap(&mut self) -> Result<(), WriteError> {
        while let Some((len, reply)) = self.pending.front_mut() {
            let result = match reply.try_recv() {
                Ok(result) => result,
                Err(TryRecvError::Empty) => return Ok(()),
                Err(TryRecvError::Closed) => Err(lost(&self.conn)),
            };
            self.pending_bytes -= *len;
            self.pending.pop_front();
            result?;
        }
        Ok(())
    }

    /// Waits for the oldest write. Cancelling the wait keeps its reply.
    async fn settle_oldest(&mut self) -> Result<(), WriteError> {
        let Some((len, reply)) = self.pending.front_mut() else {
            return Ok(());
        };
        let len = *len;
        let result = reply.await.unwrap_or_else(|_| Err(lost(&self.conn)));
        self.pending_bytes -= len;
        self.pending.pop_front();
        result
    }

    /// Finishes the stream after the data already written.
    pub fn finish(&mut self) -> Result<(), ClosedStream> {
        if self.state != SendState::Open {
            return Err(ClosedStream::new());
        }
        self.state = SendState::Finished;
        let _ = self.conn.send(ConnCmd::Finish { id: self.id });
        Ok(())
    }

    /// Abandons the stream with an application error code.
    pub fn reset(&mut self, code: VarInt) -> Result<(), ClosedStream> {
        if self.state == SendState::Reset {
            return Err(ClosedStream::new());
        }
        self.state = SendState::Reset;
        self.pending.clear();
        self.pending_bytes = 0;
        let _ = self.conn.send(ConnCmd::Reset { id: self.id, code });
        Ok(())
    }
}

fn lost(conn: &ConnRef) -> WriteError {
    WriteError::ConnectionLost(conn.shared.close_reason())
}

impl Drop for SendStream {
    fn drop(&mut self) {
        if self.state == SendState::Open {
            let _ = self.conn.send(ConnCmd::DropSend { id: self.id });
        }
    }
}

/// The receiving half of a bidirectional stream.
///
/// Dropping it before the stream ends stops the stream with code 0.
#[derive(Debug)]
pub struct RecvStream {
    conn: ConnRef,
    id: StreamId,
    buffered: VecDeque<Bytes>,
    finished: bool,
    stopped: bool,
    pending: Option<oneshot::Receiver<ReadReply>>,
}

impl RecvStream {
    pub(crate) fn new(conn: ConnRef, id: StreamId) -> Self {
        Self {
            conn,
            id,
            buffered: VecDeque::new(),
            finished: false,
            stopped: false,
            pending: None,
        }
    }

    /// Waits until data is buffered; returns `false` at the end of the stream.
    async fn fill(&mut self) -> Result<bool, ReadError> {
        if !self.buffered.is_empty() {
            return Ok(true);
        }
        if self.finished {
            return Ok(false);
        }
        if self.stopped {
            return Err(ReadError::ClosedStream);
        }
        let pending = match self.pending.as_mut() {
            Some(pending) => pending,
            None => {
                let (reply, result) = oneshot::channel();
                self.conn
                    .send(ConnCmd::Read { id: self.id, reply })
                    .map_err(ReadError::ConnectionLost)?;
                self.pending.insert(result)
            }
        };
        let result = pending.await;
        self.pending = None;
        match result {
            Ok(Ok(Some(chunks))) => {
                self.buffered
                    .extend(chunks.into_iter().filter(|chunk| !chunk.is_empty()));
                Ok(!self.buffered.is_empty() || !self.finished)
            }
            Ok(Ok(None)) => {
                self.finished = true;
                Ok(false)
            }
            Ok(Err(error)) => Err(error),
            Err(_) => Err(ReadError::ConnectionLost(self.conn.shared.close_reason())),
        }
    }

    /// Reads into `buf`. Returns the byte count, or `None` at the end of the
    /// stream.
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, ReadError> {
        if buf.is_empty() {
            return Ok(Some(0));
        }
        loop {
            if !self.fill().await? {
                return Ok(None);
            }
            if let Some(read) = self.copy_out(buf) {
                return Ok(Some(read));
            }
        }
    }

    /// Reads the next chunk of at most `max` bytes, or `None` at the end of
    /// the stream.
    pub async fn read_chunk(&mut self, max: usize) -> Result<Option<Bytes>, ReadError> {
        loop {
            if !self.fill().await? {
                return Ok(None);
            }
            if let Some(front) = self.buffered.front_mut() {
                let chunk = front.split_to(front.len().min(max.max(1)));
                if front.is_empty() {
                    self.buffered.pop_front();
                }
                return Ok(Some(chunk));
            }
        }
    }

    fn copy_out(&mut self, buf: &mut [u8]) -> Option<usize> {
        let mut read = 0;
        while read < buf.len() {
            let Some(front) = self.buffered.front_mut() else {
                break;
            };
            let take = front.len().min(buf.len() - read);
            buf[read..read + take].copy_from_slice(&front[..take]);
            front.advance(take);
            read += take;
            if front.is_empty() {
                self.buffered.pop_front();
            }
        }
        (read > 0).then_some(read)
    }

    /// Fills `buf` exactly.
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), ReadExactError> {
        let mut filled = 0;
        while filled < buf.len() {
            match self.read(&mut buf[filled..]).await? {
                Some(read) => filled += read,
                None => return Err(ReadExactError::FinishedEarly(filled)),
            }
        }
        Ok(())
    }

    /// Reads to the end of the stream, refusing more than `size_limit` bytes.
    pub async fn read_to_end(&mut self, size_limit: usize) -> Result<Vec<u8>, ReadToEndError> {
        let mut out = Vec::new();
        while self.fill().await? {
            while let Some(chunk) = self.buffered.pop_front() {
                if out.len().saturating_add(chunk.len()) > size_limit {
                    return Err(ReadToEndError::TooLong);
                }
                out.extend_from_slice(&chunk);
            }
        }
        Ok(out)
    }

    /// Tells the peer to stop sending, with an application error code.
    pub fn stop(&mut self, code: VarInt) -> Result<(), ClosedStream> {
        if self.stopped || self.finished {
            return Err(ClosedStream::new());
        }
        self.stopped = true;
        self.buffered.clear();
        self.pending = None;
        let _ = self.conn.send(ConnCmd::Stop { id: self.id, code });
        Ok(())
    }
}

impl Drop for RecvStream {
    fn drop(&mut self) {
        if !self.stopped && !self.finished {
            let _ = self.conn.send(ConnCmd::DropRecv { id: self.id });
        }
    }
}
