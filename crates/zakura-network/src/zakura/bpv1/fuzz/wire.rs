//! The `bpv1_wire` fuzz target: every BPv1 frame decoder on arbitrary bytes.
//!
//! Input layout: `[selector, edits…, payload…]`.
//!
//! - `selector % 8` picks the message type: the seven BPv1 types, or with
//!   7 an unknown type. Bit 3 sets reserved frame flags.
//! - Bit 4 starts the payload from a real `Part` frame of the `TINY`
//!   multi-stripe layout instead of the input: the next byte picks the
//!   part, and each following pair `(offset, xor)` up to a zero offset edits
//!   one byte. Bit 5 truncates that frame by the next byte.
//! - Otherwise the rest of the input is the payload.
//!
//! Checks:
//! - `decode_frame` requests no single allocation above the family's
//!   `max_heap_bytes` for the frame's type and length;
//! - a decoded message re-encodes to the same frame (canonical encoding);
//! - the control and data stream decoders accept exactly the frames the
//!   family decodes for their stream, and close with the row the frame
//!   breaks (`bpv1-61`);
//! - `decode_meta_envelope` admits exactly the `HeaderMeta`s whose header
//!   parses;
//! - a `Part` frame proves only as the honest part of its index (bpv1-12);
//! - every data-queue item the decoded frame names encodes to that frame;
//! - a decoded `Have`'s indices re-encode in the smaller form for the
//!   smallest layout that holds them, and decode back (decision-33).
//!   The `Have` refusals reach each `OutOfRange` reason by name.
//!
//! The target excludes the stream reader that splits bytes into frames. It
//! is the transport's shared frame reader, which reads every v2 stream and
//! checks each frame header against the stream's rows before BPv1 sees it.

use super::{geometries, quality::Reached, FuzzBytes};
use crate::zakura::{
    bpv1::{
        close::Bpv1CloseRow,
        peer::PeerInput,
        profile::Profile,
        service::{control_input, data_input, decode_meta_envelope, parse_and_verify_part_frame},
        wire::{
            encode_have_frame, encode_part_frame, message_type, Bpv1Message, DataQueueItem,
            HaveForm,
        },
    },
    wire_codec::{decode_frame, encode_frame, WireError, WireMessage},
    Frame,
};

/// Summary returned by [`replay_bpv1_wire_bytes`].
#[derive(Clone, Debug, Default)]
pub struct WireReplaySummary {
    /// The largest single allocation the decode requested.
    pub largest_allocation: usize,
    /// Every outcome the replay reached.
    pub reached: Reached,
}

/// The names this target must be able to reach.
pub(super) const EXPECTED: &[&str] = &[
    "wire:HeaderMeta",
    "wire:Want",
    "wire:StripeDone",
    "wire:BlockDone",
    "wire:Part",
    "wire:Have",
    "wire:WantEnd",
    "wire:unknown-type",
    "wire:decoded",
    "wire:refused",
    "wire:flags-refused",
    "control:accepted",
    "data:accepted",
    "bpv1-close-1",
    "bpv1-close-2",
    "meta:envelope",
    "part:verified",
    "part:refused",
    "queue:round-trip",
    "have:list",
    "have:bitmap",
    "have:smaller-form",
    "have index count",
    "have indices not strictly increasing",
    "have bitmap length",
];

/// The property the wire checks name.
const WIRE: &str = "bpv1/wire";

/// The seven BPv1 message types, in selector order.
const TYPES: [(u16, &str); 7] = [
    (message_type::HEADER_META, "wire:HeaderMeta"),
    (message_type::WANT, "wire:Want"),
    (message_type::STRIPE_DONE, "wire:StripeDone"),
    (message_type::BLOCK_DONE, "wire:BlockDone"),
    (message_type::PART, "wire:Part"),
    (message_type::HAVE, "wire:Have"),
    (message_type::WANT_END, "wire:WantEnd"),
];

/// Replays `bytes` as one BPv1 frame through every decoder, measuring the
/// family decode with `largest_allocation` (the fuzz crate's metered
/// allocator, or a stub that returns zero).
pub fn replay_bpv1_wire_bytes(
    bytes: &[u8],
    largest_allocation: &mut dyn FnMut(&mut dyn FnMut()) -> usize,
) -> WireReplaySummary {
    let mut input = FuzzBytes::new(bytes);
    let selector = input.byte();
    let mut summary = WireReplaySummary::default();
    let reached = &mut summary.reached;

    let encoded = geometries(&Profile::TINY, 0)
        .into_iter()
        .find(|g| g.name == "layout:multi-stripe")
        .expect("the TINY bodies include a multi-stripe layout")
        .encoded;
    let block_id = zakura_chain::block::Hash([0x5A; 32]);

    let message_type = match TYPES.get(usize::from(selector % 8)) {
        Some(&(message_type, name)) => {
            reached.hit(name);
            message_type
        }
        None => {
            reached.hit("wire:unknown-type");
            input.u16()
        }
    };
    let flags = if selector & 0x08 != 0 {
        u16::from(input.byte()) | 1
    } else {
        0
    };
    let payload = if selector & 0x10 != 0 {
        let part = &encoded.parts[usize::from(input.byte()) % encoded.parts.len()];
        let mut payload = encode_part_frame(block_id, part).payload;
        if selector & 0x20 != 0 {
            let cut = usize::from(input.byte()) + 1;
            payload.truncate(payload.len().saturating_sub(cut));
        }
        loop {
            let offset = usize::from(input.byte());
            if offset == 0 || input.is_empty() {
                break;
            }
            let xor = input.byte();
            if let Some(byte) = payload.get_mut(offset - 1) {
                *byte ^= xor;
            }
        }
        payload
    } else {
        input.take(usize::MAX).to_vec()
    };
    let frame = Frame {
        message_type,
        flags,
        payload,
    };

    // 1. The family decode, allocation-bounded and canonical.
    let mut decoded = None;
    summary.largest_allocation = largest_allocation(&mut || {
        decoded = Some(decode_frame::<Bpv1Message>(&frame));
    });
    let decoded = decoded.expect("the meter runs the decode it measures");
    let bound = Bpv1Message::max_heap_bytes(frame.message_type, frame.payload.len());
    assert!(
        summary.largest_allocation <= bound,
        "{WIRE}: decoding a {}-byte type {} payload allocated {} bytes at once; the bound is {bound}",
        frame.payload.len(),
        frame.message_type,
        summary.largest_allocation
    );
    match &decoded {
        Ok(message) => {
            reached.hit("wire:decoded");
            let again = encode_frame(message)
                .unwrap_or_else(|e| panic!("{WIRE}: a decoded {message:?} does not encode: {e:?}"));
            assert_eq!(again, frame, "{WIRE}: decoding is not canonical");
        }
        Err(WireError::ReservedFlags(_)) => reached.hit("wire:flags-refused"),
        Err(error) => {
            reached.hit("wire:refused");
            if let (message_type::HAVE, WireError::OutOfRange(reason)) = (frame.message_type, error)
            {
                reached.hit(reason);
            }
        }
    }
    if let Ok(Bpv1Message::Have { form, indices, .. }) = &decoded {
        reached.hit(match form {
            HaveForm::List => "have:list",
            HaveForm::Bitmap { .. } => "have:bitmap",
        });
        check_smaller_form(indices, reached);
    }

    // 2. The control stream decoder decodes first, then refuses data types
    // with row 1 and a `HeaderMeta` whose header does not parse with row 2.
    let control = control_input(&frame);
    let expected_control = match &decoded {
        Err(_) => Err(Bpv1CloseRow::Row2Decode),
        Ok(Bpv1Message::Part { .. } | Bpv1Message::Have { .. } | Bpv1Message::WantEnd { .. }) => {
            Err(Bpv1CloseRow::Row1FrameHeader)
        }
        Ok(message @ Bpv1Message::HeaderMeta { .. }) => {
            match decode_meta_envelope(zakura_chain::block::Hash([0; 32]), message.clone()) {
                Some(_) => Ok(()),
                None => Err(Bpv1CloseRow::Row2Decode),
            }
        }
        Ok(_) => Ok(()),
    };
    assert_eq!(
        control.as_ref().map(|_| ()).map_err(|row| *row),
        expected_control,
        "{WIRE}: bpv1-61 the control decoder disagrees with the family decode"
    );
    match &control {
        Ok(input) => {
            reached.hit("control:accepted");
            check_meta(input, &frame, reached);
        }
        Err(row) => reached.hit(row.label()),
    }

    // 3. The data stream decoder.
    let data = data_input(frame.clone());
    match frame.message_type {
        message_type::PART => {
            let prefix_ok = frame.payload.len() >= 36;
            assert_eq!(
                data.is_ok(),
                prefix_ok,
                "{WIRE}: bpv1-13 the data decoder read a part prefix wrongly"
            );
            match data {
                Ok(_) => reached.hit("data:accepted"),
                Err(row) => {
                    assert_eq!(
                        row,
                        Bpv1CloseRow::Row2Decode,
                        "{WIRE}: bpv1-61 short prefix row"
                    );
                    reached.hit(row.label());
                }
            }
            check_part(&frame, &encoded, block_id, reached);
        }
        message_type::HAVE | message_type::WANT_END => {
            let expected = match &decoded {
                Ok(_) => Ok(()),
                Err(WireError::ReservedFlags(_)) => Err(Bpv1CloseRow::Row1FrameHeader),
                Err(_) => Err(Bpv1CloseRow::Row2Decode),
            };
            assert_eq!(
                data.as_ref().map(|_| ()).map_err(|row| *row),
                expected,
                "{WIRE}: bpv1-61 the data decoder disagrees with the family decode"
            );
            match data {
                Ok(_) => reached.hit("data:accepted"),
                Err(row) => reached.hit(row.label()),
            }
            if let Ok(message) = &decoded {
                check_queue_item(message, &frame, reached);
            }
        }
        _ => {
            assert_eq!(
                data.as_ref().map(|_| ()).map_err(|row| *row),
                Err(Bpv1CloseRow::Row1FrameHeader),
                "{WIRE}: bpv1-61 the data stream accepted a control type"
            );
            reached.hit(Bpv1CloseRow::Row1FrameHeader.label());
        }
    }
    summary
}

/// A `HeaderMeta` the control decoder accepted yields the table's envelope,
/// keyed by its header's hash.
fn check_meta(input: &PeerInput, frame: &Frame, reached: &mut Reached) {
    let PeerInput::RecvHeaderMeta { block_id, message } = input else {
        return;
    };
    let envelope = decode_meta_envelope(*block_id, (**message).clone())
        .unwrap_or_else(|| panic!("{WIRE}: an accepted HeaderMeta has no envelope"));
    assert_eq!(
        envelope.block_id, *block_id,
        "{WIRE}: the envelope lost its block id"
    );
    assert_eq!(
        frame.message_type,
        message_type::HEADER_META,
        "{WIRE}: a non-HeaderMeta frame became metadata"
    );
    reached.hit("meta:envelope");
}

/// A `Part` frame proves under the honest layout only as the honest part of
/// the index it names, and only when that part's frame is byte-identical.
fn check_part(
    frame: &Frame,
    encoded: &crate::zakura::bpv1::codec::EncodedBody,
    block_id: zakura_chain::block::Hash,
    reached: &mut Reached,
) {
    let Some(index_bytes) = frame.payload.get(32..36) else {
        reached.hit("part:refused");
        return;
    };
    let index = u32::from_le_bytes(index_bytes.try_into().expect("the slice has four bytes"));
    let proved =
        parse_and_verify_part_frame(&frame.payload, &encoded.coding, encoded.part_root, index);
    let honest = usize::try_from(index)
        .ok()
        .and_then(|i| encoded.parts.get(i));
    match proved {
        Some(part) => {
            let honest = honest.unwrap_or_else(|| {
                panic!("{WIRE}: bpv1-12 part {index} proved outside the layout")
            });
            assert_eq!(
                &part, honest,
                "{WIRE}: bpv1-12 a forged part {index} proved"
            );
            // The block id in the prefix is not under proof: the shell has
            // already routed the frame to the block it names.
            let mut canonical = encode_part_frame(block_id, honest).payload;
            canonical[..32].copy_from_slice(&frame.payload[..32]);
            assert_eq!(
                canonical, frame.payload,
                "{WIRE}: bpv1-12 a non-canonical part frame proved"
            );
            reached.hit("part:verified");
        }
        None => reached.hit("part:refused"),
    }
}

/// For the smallest layout that holds `indices`, the encoder's form is the
/// shorter of a `u16` list and the layout's bitmap, a tie keeping the list,
/// and it decodes back to the same indices (decision-33).
fn check_smaller_form(indices: &[u32], reached: &mut Reached) {
    let last = *indices.last().expect("a decoded Have lists an index");
    let total = last + 1;
    let form = HaveForm::smaller(total, indices.len());
    let list_len = HaveForm::List.indices_len(indices.len());
    // Widening u32 to usize is lossless on supported targets.
    let bitmap_len = total.div_ceil(8) as usize;
    assert_eq!(
        form.indices_len(indices.len()),
        list_len.min(bitmap_len),
        "{WIRE}: decision-33 the encoder picked the longer form"
    );
    assert!(
        form.fits(total),
        "{WIRE}: decision-33 the encoder sized a bitmap for another layout"
    );
    let block_id = zakura_chain::block::Hash([0x5B; 32]);
    let frame = encode_have_frame(block_id, Default::default(), form, indices)
        .unwrap_or_else(|e| panic!("{WIRE}: {form:?} does not encode {indices:?}: {e:?}"));
    match decode_frame::<Bpv1Message>(&frame) {
        Ok(Bpv1Message::Have {
            form: again,
            indices: decoded,
            ..
        }) => {
            assert_eq!(again, form, "{WIRE}: decision-33 the form changed");
            assert_eq!(decoded, indices, "{WIRE}: decision-33 the indices changed");
        }
        other => panic!("{WIRE}: decision-33 {form:?} decoded as {other:?}"),
    }
    reached.hit("have:smaller-form");
}

/// A decoded `Have` or `WantEnd` is a data-queue item that encodes to the
/// same frame.
fn check_queue_item(message: &Bpv1Message, frame: &Frame, reached: &mut Reached) {
    let item = match message {
        Bpv1Message::Have {
            block_id,
            flags,
            form,
            indices,
        } => DataQueueItem::Have {
            block_id: *block_id,
            flags: *flags,
            form: *form,
            indices: indices.clone(),
        },
        Bpv1Message::WantEnd {
            want_id,
            sent,
            reason,
        } => DataQueueItem::WantEnd {
            want_id: *want_id,
            sent: *sent,
            reason: *reason,
        },
        _ => return,
    };
    assert_eq!(
        item.encode().as_ref(),
        Some(frame),
        "{WIRE}: the data queue encodes {item:?} differently"
    );
    reached.hit("queue:round-trip");
}
