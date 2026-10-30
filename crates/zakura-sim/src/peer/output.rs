//! The output ledger (M6a-6): every frame the node queues on a header
//! stream is an output, from the moment its producer makes it until its
//! life's `Drop` ends it, and this ledger checks that each output's reported
//! life is one the node can have lived, and that the frames the node wrote
//! and the peers read are the frames its producers queued.
//!
//! The node reports, per stream, each output's `output-allocated` with the
//! fingerprint of the frame as its producer queued it, `output-enqueued`
//! right after the insert with its place in the queue, `output-dequeued`
//! when the writer takes it, the writer's `frame-write-started` naming it
//! with the writer's own fingerprint of the frame, and one `output-ended`.
//! The ledger reads them in log order and fails the run, as a harness
//! failure, on:
//!
//! - **One end each.** An output ended twice, or reported before its
//!   allocation or after its end. An output still live when the log freezes
//!   is pending, not a failure.
//! - **Ends that match the life.** `not-queued` for an output never
//!   enqueued, `dropped` for one enqueued and never dequeued, `written` for
//!   one whose write started and finished, an `unwritten` skip or encode
//!   failure for one dequeued and never started, and a cancellation, failed
//!   write or `abandoned` for one dequeued.
//! - **Queue order.** Outputs are allocated and enqueued in counting order,
//!   and each stream's writer dequeues them in their enqueue `order`.
//! - **Abandoned.** A writer whose future is dropped mid-write abandons its
//!   output, which is valid. A writer that goes on to dequeue another output
//!   after that is not, since its cancellation arms record `unwritten`
//!   before they break.
//! - **Writer evidence.** Each started write names its output, and the
//!   writer's fingerprint equals the one its producer queued.
//! - **Actual output.** A frame a peer read is mapped to its output by the
//!   stream's write `seq`, the same way the sender adapter maps it to its
//!   write, so skipped obsolete claims never shift the mapping. Its
//!   fingerprint must equal the output's.
//!
//! - **Attempts.** A producer reports what it meant each frame to be, from
//!   its own state, as the output's `output-attempt`, before the frame is
//!   queued. Every started write of a producer-owned message type
//!   (`Status`, `SubscribeHeaders`, `Headers`, `HeadersOutcome`) on a
//!   header stream needs one, or it's output without an attempt. The
//!   writer's decoded summary of the frame must equal the attempt, flags
//!   included, and so must the summary of the frame a peer read, decoded
//!   here. A page's summary is its own count, bytes and last frontier, so
//!   two pages never compare by running totals.
//!
//! Enqueue order is a simulator contract: only there does nothing run
//! between an insert and its `output-enqueued`.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use zakura_network::zakura::{
    frame_fingerprint, v9::summary::regtest_frame_summary, Frame, ZAKURA_STREAM_HEADER_SYNC,
};

use crate::{
    harness::wire_node::summary_fact,
    observe::{FrameSummaryFact, OutputEnd, TransportFact},
};

/// The version 9 message types a header-sync producer owns: `Status`,
/// `SubscribeHeaders`, `Headers` and `HeadersOutcome`.
const PRODUCER_OWNED: [u16; 4] = [1, 2, 3, 4];

/// A header stream, by connection and QUIC stream.
type StreamKey = (u64, u64);

/// Every output on every stream of the node, as the log reports them.
#[derive(Debug, Default)]
pub(crate) struct OutputLedger {
    streams: BTreeMap<StreamKey, Outputs>,
    /// The header-sync streams, whose producer-owned frames need attempts.
    header: BTreeSet<StreamKey>,
}

/// One stream's outputs.
#[derive(Debug, Default)]
struct Outputs {
    /// Every output allocated so far, by number. Ended ones stay, since a
    /// peer can read a frame after its output ended.
    outputs: BTreeMap<u64, Output>,
    /// The next output number the node allocates.
    next_output: u64,
    /// The next place in the queue.
    next_order: u64,
    /// Outputs enqueued and not yet dequeued, oldest first.
    queue: VecDeque<u64>,
    /// The output each write `seq` started.
    writes: BTreeMap<u64, u64>,
    /// The first output on the stream that ended `abandoned`.
    abandoned: Option<u64>,
}

/// One output's life so far.
#[derive(Debug)]
struct Output {
    /// The fingerprint of its frame as the producer queued it.
    expected: u64,
    /// The frame's message type.
    message_type: u16,
    /// What its producer meant it to be.
    attempt: Option<FrameSummaryFact>,
    enqueued: bool,
    dequeued: bool,
    /// The write `seq` its write started as.
    started: Option<u64>,
    /// Whether its write finished.
    written: bool,
    ended: Option<OutputEnd>,
}

impl OutputLedger {
    /// Reads the node's transport `event` on `connection`, in log order.
    ///
    /// # Errors
    ///
    /// An `OutputLedger` failure for an output life the node can't have
    /// lived, as the module docs list.
    pub(crate) fn node(&mut self, connection: u64, event: &TransportFact) -> Result<(), String> {
        let fail = |quic_stream: u64, what: String| {
            Err(format!(
                "OutputLedger: on connection {connection}'s stream {quic_stream}, {what}"
            ))
        };
        match event {
            TransportFact::Admitted {
                quic_stream, kind, ..
            } if *kind == ZAKURA_STREAM_HEADER_SYNC => {
                self.header.insert((connection, *quic_stream));
            }
            TransportFact::OutputAttempt {
                quic_stream,
                output,
                attempt,
            } => {
                let stream = self.streams.entry((connection, *quic_stream)).or_default();
                let life = match stream.live(*output) {
                    Ok(life) => life,
                    Err(why) => return fail(*quic_stream, format!("{why} reported an attempt")),
                };
                if life.attempt.is_some() || life.enqueued {
                    return fail(
                        *quic_stream,
                        format!(
                            "output {output} reported an attempt {}",
                            if life.enqueued {
                                "after it was enqueued"
                            } else {
                                "a second time"
                            }
                        ),
                    );
                }
                life.attempt = Some(attempt.clone());
            }
            TransportFact::FrameWriteStarted {
                quic_stream,
                seq,
                output: Some(output),
                summary,
                ..
            } => {
                if let Some(life) = self
                    .streams
                    .get(&(connection, *quic_stream))
                    .and_then(|stream| stream.outputs.get(output))
                {
                    let owned = self.header.contains(&(connection, *quic_stream))
                        && PRODUCER_OWNED.contains(&life.message_type);
                    match (&life.attempt, summary) {
                        (None, _) if owned => {
                            return fail(
                                *quic_stream,
                                format!(
                                    "output {output}, type {}, started write {seq} with no attempt: output \
                                     without attempt",
                                    life.message_type
                                ),
                            )
                        }
                        (Some(attempt), summary) if summary.as_ref() != Some(attempt) => {
                            return fail(
                                *quic_stream,
                                format!(
                                    "the writer started output {output} as write {seq} saying {summary:?}, \
                                     and its producer attempted {attempt:?}"
                                ),
                            )
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
        match *event {
            TransportFact::OutputAllocated {
                quic_stream,
                output,
                expected,
                message_type,
                ..
            } => {
                let stream = self.streams.entry((connection, quic_stream)).or_default();
                if output != stream.next_output {
                    return fail(
                        quic_stream,
                        format!(
                            "output {output} was allocated where output {} was next",
                            stream.next_output
                        ),
                    );
                }
                stream.next_output += 1;
                stream.outputs.insert(
                    output,
                    Output {
                        expected,
                        message_type,
                        attempt: None,
                        enqueued: false,
                        dequeued: false,
                        started: None,
                        written: false,
                        ended: None,
                    },
                );
            }
            TransportFact::OutputEnqueued {
                quic_stream,
                output,
                order,
            } => {
                let stream = self.streams.entry((connection, quic_stream)).or_default();
                if order != stream.next_order {
                    return fail(
                        quic_stream,
                        format!(
                            "output {output} was enqueued at place {order} where place {} \
                             was next",
                            stream.next_order
                        ),
                    );
                }
                stream.next_order += 1;
                let life = match stream.live(output) {
                    Ok(life) => life,
                    Err(why) => return fail(quic_stream, format!("{why} was enqueued")),
                };
                if life.enqueued {
                    return fail(quic_stream, format!("output {output} was enqueued twice"));
                }
                life.enqueued = true;
                stream.queue.push_back(output);
            }
            TransportFact::OutputDequeued {
                quic_stream,
                output,
            } => {
                let stream = self.streams.entry((connection, quic_stream)).or_default();
                if let Some(abandoned) = stream.abandoned {
                    return fail(
                        quic_stream,
                        format!(
                            "the writer dequeued output {output} after it abandoned output \
                             {abandoned}, so a write ended with no outcome while the writer \
                             went on"
                        ),
                    );
                }
                match stream.queue.front() {
                    Some(front) if *front == output => {
                        stream.queue.pop_front();
                    }
                    front => {
                        return fail(
                            quic_stream,
                            format!(
                                "the writer dequeued output {output} where the queue's oldest \
                                 was {front:?}"
                            ),
                        )
                    }
                }
                let life = match stream.live(output) {
                    Ok(life) => life,
                    Err(why) => return fail(quic_stream, format!("{why} was dequeued")),
                };
                life.dequeued = true;
            }
            TransportFact::FrameWriteStarted {
                quic_stream,
                seq,
                output,
                fingerprint,
                ..
            } => {
                let stream = self.streams.entry((connection, quic_stream)).or_default();
                let Some(output) = output else {
                    return fail(
                        quic_stream,
                        format!("write {seq} started a frame that is no output"),
                    );
                };
                let life = match stream.live(output) {
                    Ok(life) => life,
                    Err(why) => return fail(quic_stream, format!("{why} started write {seq}")),
                };
                if !life.dequeued || life.started.is_some() {
                    return fail(
                        quic_stream,
                        format!(
                            "output {output} started write {seq} {}",
                            if life.dequeued {
                                "a second time"
                            } else {
                                "before it was dequeued"
                            }
                        ),
                    );
                }
                if fingerprint != life.expected {
                    return fail(
                        quic_stream,
                        format!(
                            "the writer started output {output} as write {seq} with fingerprint \
                             {fingerprint:#018x}, and its producer queued {:#018x}",
                            life.expected
                        ),
                    );
                }
                life.started = Some(seq);
                stream.writes.insert(seq, output);
            }
            TransportFact::FrameWritten {
                quic_stream, seq, ..
            } => {
                let stream = self.streams.entry((connection, quic_stream)).or_default();
                let Some(output) = stream.writes.get(&seq).copied() else {
                    return fail(
                        quic_stream,
                        format!("write {seq} finished and never started"),
                    );
                };
                let life = match stream.live(output) {
                    Ok(life) => life,
                    Err(why) => return fail(quic_stream, format!("{why} finished write {seq}")),
                };
                life.written = true;
            }
            TransportFact::OutputEnded {
                quic_stream,
                output,
                outcome,
            } => {
                let stream = self.streams.entry((connection, quic_stream)).or_default();
                let life = match stream.live(output) {
                    Ok(life) => life,
                    Err(why) => return fail(quic_stream, format!("{why} ended {outcome:?}")),
                };
                let lived = match outcome {
                    OutputEnd::NotQueued => !life.enqueued,
                    OutputEnd::Dropped => life.enqueued && !life.dequeued,
                    OutputEnd::Written => life.started.is_some() && life.written,
                    OutputEnd::UnwrittenObsolete | OutputEnd::UnwrittenEncode => {
                        life.dequeued && life.started.is_none()
                    }
                    OutputEnd::UnwrittenCancelled
                    | OutputEnd::UnwrittenFailed
                    | OutputEnd::Abandoned => life.dequeued && !life.written,
                };
                if !lived {
                    return fail(
                        quic_stream,
                        format!(
                            "output {output} ended {outcome:?} after it was enqueued: {}, \
                             dequeued: {}, started: {:?}, written: {}",
                            life.enqueued, life.dequeued, life.started, life.written
                        ),
                    );
                }
                life.ended = Some(outcome);
                if outcome == OutputEnd::Dropped {
                    stream.queue.retain(|queued| *queued != output);
                }
                if outcome == OutputEnd::Abandoned {
                    stream.abandoned.get_or_insert(output);
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// A peer read `frame`, the node's write `seq` on `connection`'s
    /// `quic_stream`.
    ///
    /// # Errors
    ///
    /// An `OutputLedger` failure if no output started that write, or the
    /// frame isn't the one its output's producer queued.
    pub(crate) fn received(
        &self,
        connection: u64,
        quic_stream: u64,
        seq: u64,
        frame: &Frame,
    ) -> Result<(), String> {
        let stream = self.streams.get(&(connection, quic_stream));
        let Some((output, life)) = stream.and_then(|stream| {
            let output = *stream.writes.get(&seq)?;
            Some((output, stream.outputs.get(&output)?))
        }) else {
            return Err(format!(
                "OutputLedger: a peer read the node's write {seq} on connection {connection}'s \
                 stream {quic_stream}, which no output started"
            ));
        };
        let received = frame_fingerprint(frame);
        if received != life.expected {
            return Err(format!(
                "OutputLedger: a peer read the node's write {seq} on connection {connection}'s \
                 stream {quic_stream} with fingerprint {received:#018x}, and its output \
                 {output} was queued as {:#018x}",
                life.expected
            ));
        }
        if let Some(attempt) = &life.attempt {
            // The header harnesses run on Regtest.
            let read = regtest_frame_summary(frame).map(summary_fact).transpose()?;
            if read.as_ref() != Some(attempt) {
                return Err(format!(
                    "OutputLedger: a peer read the node's write {seq} on connection \
                     {connection}'s stream {quic_stream} saying {read:?}, and its output \
                     {output}'s producer attempted {attempt:?}"
                ));
            }
        }
        Ok(())
    }
}

impl Outputs {
    /// Output `output`, allocated and not ended, or what it is instead.
    fn live(&mut self, output: u64) -> Result<&mut Output, String> {
        match self.outputs.get_mut(&output) {
            None => Err(format!("output {output}, never allocated,")),
            Some(life) => match life.ended {
                Some(ended) => Err(format!("output {output}, already ended {ended:?},")),
                None => Ok(life),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONN: u64 = 1;
    const STREAM: u64 = 4;

    fn frame(payload: u8) -> Frame {
        Frame {
            message_type: 3,
            flags: 0,
            payload: vec![payload; 4],
        }
    }

    fn allocated(output: u64, frame: &Frame) -> TransportFact {
        TransportFact::OutputAllocated {
            quic_stream: STREAM,
            output,
            message_type: frame.message_type,
            payload_len: 4,
            expected: frame_fingerprint(frame),
        }
    }

    fn enqueued(output: u64, order: u64) -> TransportFact {
        TransportFact::OutputEnqueued {
            quic_stream: STREAM,
            output,
            order,
        }
    }

    fn dequeued(output: u64) -> TransportFact {
        TransportFact::OutputDequeued {
            quic_stream: STREAM,
            output,
        }
    }

    fn started(seq: u64, output: u64, frame: &Frame) -> TransportFact {
        TransportFact::FrameWriteStarted {
            quic_stream: STREAM,
            seq,
            message_type: frame.message_type,
            output: Some(output),
            fingerprint: frame_fingerprint(frame),
            summary: None,
        }
    }

    fn written(seq: u64) -> TransportFact {
        TransportFact::FrameWritten {
            quic_stream: STREAM,
            seq,
            message_type: 3,
        }
    }

    fn ended(output: u64, outcome: OutputEnd) -> TransportFact {
        TransportFact::OutputEnded {
            quic_stream: STREAM,
            output,
            outcome,
        }
    }

    /// Feeds `events` in order, and returns the first failure.
    fn feed(ledger: &mut OutputLedger, events: &[TransportFact]) -> Result<(), String> {
        events.iter().try_for_each(|event| ledger.node(CONN, event))
    }

    /// A written output, a skipped one and a queued one dropped with the
    /// queue live lives the node can have, and a peer's read of the
    /// written one maps to it by write `seq`.
    #[test]
    fn whole_lives_pass() {
        let (a, b, c) = (frame(1), frame(2), frame(3));
        let mut ledger = OutputLedger::default();
        feed(
            &mut ledger,
            &[
                allocated(0, &a),
                allocated(1, &b),
                allocated(2, &c),
                // Output 1's producer enqueued first.
                enqueued(1, 0),
                enqueued(0, 1),
                enqueued(2, 2),
                dequeued(1),
                ended(1, OutputEnd::UnwrittenObsolete),
                dequeued(0),
                started(0, 0, &a),
                written(0),
                ended(0, OutputEnd::Written),
                ended(2, OutputEnd::Dropped),
            ],
        )
        .unwrap();
        ledger.received(CONN, STREAM, 0, &a).unwrap();
    }

    /// Fails if a second end passes, as a terminal reported from a
    /// cancellation arm as well as from `Drop` would be.
    #[test]
    fn a_second_end_fails() {
        let a = frame(1);
        let mut ledger = OutputLedger::default();
        let why = feed(
            &mut ledger,
            &[
                allocated(0, &a),
                enqueued(0, 0),
                dequeued(0),
                ended(0, OutputEnd::UnwrittenCancelled),
                ended(0, OutputEnd::Abandoned),
            ],
        )
        .unwrap_err();
        assert!(why.contains("already ended"), "{why}");
    }

    /// Fails if dequeues are checked by output number, not enqueue order.
    #[test]
    fn dequeues_follow_enqueue_order() {
        let (a, b) = (frame(1), frame(2));
        let mut ledger = OutputLedger::default();
        let why = feed(
            &mut ledger,
            &[
                allocated(0, &a),
                allocated(1, &b),
                enqueued(1, 0),
                enqueued(0, 1),
                dequeued(0),
            ],
        )
        .unwrap_err();
        assert!(why.contains("queue's oldest was Some(1)"), "{why}");
    }

    /// An abandoned output is valid alone, and a later dequeue on its
    /// stream fails.
    #[test]
    fn a_dequeue_after_an_abandoned_output_fails() {
        let (a, b) = (frame(1), frame(2));
        let mut ledger = OutputLedger::default();
        feed(
            &mut ledger,
            &[
                allocated(0, &a),
                allocated(1, &b),
                enqueued(0, 0),
                enqueued(1, 1),
                dequeued(0),
                started(0, 0, &a),
                ended(0, OutputEnd::Abandoned),
            ],
        )
        .unwrap();
        let why = feed(&mut ledger, &[dequeued(1)]).unwrap_err();
        assert!(why.contains("after it abandoned output 0"), "{why}");
    }

    /// An end that doesn't match the life fails: `written` with no
    /// finished write, and `not-queued` after an enqueue.
    #[test]
    fn an_end_must_match_its_life() {
        let a = frame(1);
        for (events, outcome) in [
            (
                vec![
                    allocated(0, &a),
                    enqueued(0, 0),
                    dequeued(0),
                    started(0, 0, &a),
                ],
                OutputEnd::Written,
            ),
            (vec![allocated(0, &a), enqueued(0, 0)], OutputEnd::NotQueued),
        ] {
            let mut ledger = OutputLedger::default();
            feed(&mut ledger, &events).unwrap();
            let why = feed(&mut ledger, &[ended(0, outcome)]).unwrap_err();
            assert!(why.contains("after it was enqueued"), "{why}");
        }
    }

    /// The writer's fingerprint and a peer's read are each checked against
    /// the fingerprint the producer queued.
    #[test]
    fn a_changed_frame_fails_at_the_writer_and_at_the_receipt() {
        let (a, b) = (frame(1), frame(2));
        let mut ledger = OutputLedger::default();
        feed(
            &mut ledger,
            &[allocated(0, &a), enqueued(0, 0), dequeued(0)],
        )
        .unwrap();
        let why = feed(&mut ledger, &[started(0, 0, &b)]).unwrap_err();
        assert!(why.contains("its producer queued"), "{why}");

        let mut ledger = OutputLedger::default();
        feed(
            &mut ledger,
            &[
                allocated(0, &a),
                enqueued(0, 0),
                dequeued(0),
                started(0, 0, &a),
            ],
        )
        .unwrap();
        let why = ledger.received(CONN, STREAM, 0, &b).unwrap_err();
        assert!(why.contains("was queued as"), "{why}");
        let why = ledger.received(CONN, STREAM, 1, &a).unwrap_err();
        assert!(why.contains("which no output started"), "{why}");
    }

    /// An admitted header stream, so its producer-owned frames need
    /// attempts.
    fn header_stream() -> TransportFact {
        TransportFact::Admitted {
            quic_stream: STREAM,
            kind: ZAKURA_STREAM_HEADER_SYNC,
            version: 9,
            session_id: 7,
            limits: crate::observe::TransportLimits {
                max_frame_bytes: 1,
                max_message_bytes: 1,
                max_open_streams: 1,
                max_inbound_queue_depth: 1,
                idle_timeout_ms: 1,
                prelude_timeout_ms: 1,
                control_timeout_ms: 1,
                stream_open_rate_per_second: 1,
                message_rate_per_second: 1,
            },
            inbound_depth: 1,
            outbound_depth: 1,
        }
    }

    /// A Regtest `Status` frame, a producer-owned type.
    fn status_frame() -> Frame {
        use zakura_chain::block;
        use zakura_network::zakura::{v9, wire_codec::encode_frame};
        encode_frame(&v9::RegtestMessage::Status(v9::Status {
            work_anchor_height: block::Height(0),
            work_anchor_hash: block::Hash([1; 32]),
            selected_tip_height: block::Height(0),
            selected_tip_hash: block::Hash([2; 32]),
            suffix_cumulative_work: Default::default(),
            oldest_retained_height: block::Height(0),
            max_headers_per_response: 1,
            max_subscriptions: 1,
            max_message_bytes: 1 << 20,
            tree_aux_schema_mask: 0,
        }))
        .expect("the status encodes")
    }

    /// A summary fact for `producer` on `subscription`.
    fn summary(producer: &str, subscription: Option<u64>) -> FrameSummaryFact {
        FrameSummaryFact {
            producer: producer.to_owned(),
            subscription,
            flags: 0,
            objects: 0,
            bytes: 0,
            cursor: None,
        }
    }

    fn attempt(output: u64, attempt: FrameSummaryFact) -> TransportFact {
        TransportFact::OutputAttempt {
            quic_stream: STREAM,
            output,
            attempt,
        }
    }

    /// `started` with the writer's `summary`.
    fn started_saying(
        seq: u64,
        output: u64,
        frame: &Frame,
        summary: Option<FrameSummaryFact>,
    ) -> TransportFact {
        let TransportFact::FrameWriteStarted {
            quic_stream,
            message_type,
            fingerprint,
            ..
        } = started(seq, output, frame)
        else {
            unreachable!("`started` builds a write start")
        };
        TransportFact::FrameWriteStarted {
            quic_stream,
            seq,
            message_type,
            output: Some(output),
            fingerprint,
            summary,
        }
    }

    /// A producer-owned frame on a header stream whose producer reported
    /// no attempt is output without attempt, and the same frame with its
    /// attempt passes.
    ///
    /// This fails if attempts are required only of outputs that have one.
    #[test]
    fn a_started_frame_needs_its_attempt() {
        let status = status_frame();
        let mut ledger = OutputLedger::default();
        feed(
            &mut ledger,
            &[
                header_stream(),
                allocated(0, &status),
                enqueued(0, 0),
                dequeued(0),
            ],
        )
        .unwrap();
        let why = feed(
            &mut ledger,
            &[started_saying(0, 0, &status, Some(summary("status", None)))],
        )
        .unwrap_err();
        assert!(why.contains("output without attempt"), "{why}");

        let mut ledger = OutputLedger::default();
        feed(
            &mut ledger,
            &[
                header_stream(),
                allocated(0, &status),
                attempt(0, summary("status", None)),
                enqueued(0, 0),
                dequeued(0),
                started_saying(0, 0, &status, Some(summary("status", None))),
            ],
        )
        .unwrap();
        ledger.received(CONN, STREAM, 0, &status).unwrap();
    }

    /// The writer's summary and the frame a peer read are each checked
    /// against the attempt, fields and flags included: a write saying
    /// subscription 2 where the producer attempted 1, a write with flags
    /// set, and a receipt that decodes to another producer's frame.
    ///
    /// This fails if only the fingerprint is compared.
    #[test]
    fn a_wrong_frame_disagrees_with_its_attempt() {
        let status = status_frame();
        let lived = |attempted: FrameSummaryFact| {
            vec![
                header_stream(),
                allocated(0, &status),
                attempt(0, attempted),
                enqueued(0, 0),
                dequeued(0),
            ]
        };
        for (attempted, said) in [
            (summary("page", Some(1)), Some(summary("page", Some(2)))),
            (
                summary("status", None),
                Some(FrameSummaryFact {
                    flags: 1,
                    ..summary("status", None)
                }),
            ),
            (summary("status", None), None),
        ] {
            let mut ledger = OutputLedger::default();
            feed(&mut ledger, &lived(attempted)).unwrap();
            let why = feed(&mut ledger, &[started_saying(0, 0, &status, said)]).unwrap_err();
            assert!(why.contains("its producer attempted"), "{why}");
        }

        let mut ledger = OutputLedger::default();
        feed(&mut ledger, &lived(summary("open", Some(1)))).unwrap();
        feed(
            &mut ledger,
            &[started_saying(
                0,
                0,
                &status,
                Some(summary("open", Some(1))),
            )],
        )
        .unwrap();
        let why = ledger.received(CONN, STREAM, 0, &status).unwrap_err();
        assert!(why.contains("producer attempted"), "{why}");
    }
}
