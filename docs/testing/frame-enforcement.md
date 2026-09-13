# Frame and codec enforcement

Reject invalid message declarations before waiting for their payload or decoding
its body. A frame is the header and payload sent over a stream. The header names
the message, its flags and its payload length.

For example, a Block header can declare 2,000,002 payload bytes and send nothing
else. The stream's general cap previously allowed that declaration. The Block
limit now rejects it immediately. A legal Block has at most 2,000,000 body bytes
plus its one-byte message tag.

## Shared boundary

Each service declares its message types, payload limits and allowed flag bits.
The shared reader checks those declarations before allocating the payload. The
negotiated stream cap can tighten a message's limit, but cannot enlarge it.
Block sync, header sync and discovery currently allow no flag bits. A custom
service can declare its own mask. Omitting a mask preserves its existing behavior.

The Block codec checks that the frame's message type matches the payload's tag
before decoding or retaining the Block. It keeps the configured network decoder
from the [bounded decoding path](bounded-decoding.md). Terminal messages also
check their height when encoding, so we cannot send a height our decoder rejects.

## Properties

| Requirement | What the tests establish | Test module |
| --- | --- | --- |
| F01 | Message and negotiated caps reject absent oversized payloads. Unsupported flags reject before a payload wait. Custom flag masks still work. | `handler::tests::frame_policy` |
| F02 requests | Generated legal requests round trip. Malformed fields, tags, flags and lengths match independent acceptance rules. | `serving_regulation::policy::codec_properties` |
| F02 responses | Both terminal encoders and decoders accept legal fields only. Blocks reject truncation, noncanonical counts and excess bytes. | `block_sync::wire::frame_codec` |
| F04 frame checks | Conflicting frame and payload tags fail without allocating a decoded Block. | `block_sync::wire::frame_codec` |
| T03 partial frames | Every request byte split resumes correctly. FIN, reset and stalled partial frames never become complete messages. | `handler::tests::frame_policy` |

F04 also includes authorization checks in the receiver. T03 also includes
resuming after a local consumer pause. Those are separate contracts and are not
claimed by these tests.

## Local execution

```sh
PROPTEST_CASES=2048 PROPTEST_RNG_SEED=896 cargo nextest run --locked \
  -p zakura-network --lib --profile frame-enforcement
```

The profile includes the corresponding fixed boundary tests and existing reader
regressions, with finite timeouts and no retries. No CI trigger is added.
