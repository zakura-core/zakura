//! Target quality for the BPv1 fuzz targets (#2714).
//!
//! Each replay records the named alternatives it reached in a [`Reached`]
//! set: the machine inputs and outputs it stepped, the close rows it hit,
//! the block phases it saw, and target-specific outcomes. Each target lists
//! the names it must be able to reach. [`measure_bpv1_targets`] replays the
//! in-code seeds and a run of generated inputs, then reports per target what
//! the run reached and what it missed.
//!
//! The name functions match exhaustively, so a new machine input fails the
//! build until it has a name. Each `*_INPUTS` list beside a name function
//! holds every name that function returns.

use std::collections::BTreeSet;

use super::{replay_bpv1_codec_bytes, replay_bpv1_mesh_bytes, replay_bpv1_wire_bytes, seed_inputs};
use crate::zakura::bpv1::{
    blocks::{replay_bpv1_block_bytes, BlockInput, BlockOutput, BlockPhase},
    peer::{replay_bpv1_peer_bytes, PeerInput, PeerOutput},
    proposer::ProposerInput,
};

/// The named alternatives one or more replays reached.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Reached(BTreeSet<&'static str>);

impl Reached {
    /// Records that a replay reached `name`.
    pub fn hit(&mut self, name: &'static str) {
        self.0.insert(name);
    }

    /// Whether a replay reached `name`.
    pub fn contains(&self, name: &str) -> bool {
        self.0.contains(name)
    }

    /// How many distinct names a replay reached.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether no replay reached anything.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Adds every name `other` reached.
    pub fn merge(&mut self, other: &Reached) {
        self.0.extend(other.0.iter().copied());
    }

    /// The reached names, in order.
    pub fn iter(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.0.iter().copied()
    }
}

/// The name of `input`'s variant.
pub fn peer_input_name(input: &PeerInput) -> &'static str {
    match input {
        PeerInput::BlockAdmitted(_) => "peer-in:BlockAdmitted",
        PeerInput::RecvHeaderMeta { .. } => "peer-in:RecvHeaderMeta",
        PeerInput::RecvPartPrefix(_) => "peer-in:RecvPartPrefix",
        PeerInput::PartVerified { .. } => "peer-in:PartVerified",
        PeerInput::PartFailed { .. } => "peer-in:PartFailed",
        PeerInput::PartRefused { .. } => "peer-in:PartRefused",
        PeerInput::Ready { .. } => "peer-in:Ready",
        PeerInput::RecvHave(_) => "peer-in:RecvHave",
        PeerInput::PullGranted(_) => "peer-in:PullGranted",
        PeerInput::Seeds(_) => "peer-in:Seeds",
        PeerInput::RecvWant(_) => "peer-in:RecvWant",
        PeerInput::ServePartQueued(_) => "peer-in:ServePartQueued",
        PeerInput::ServeDone(_) => "peer-in:ServeDone",
        PeerInput::WantEndWritten => "peer-in:WantEndWritten",
        PeerInput::RecvWantEnd(_) => "peer-in:RecvWantEnd",
        PeerInput::LocalStripeDone { .. } => "peer-in:LocalStripeDone",
        PeerInput::LocalBlockDone { .. } => "peer-in:LocalBlockDone",
        PeerInput::RecvStripeDone { .. } => "peer-in:RecvStripeDone",
        PeerInput::RecvBlockDone { .. } => "peer-in:RecvBlockDone",
        PeerInput::Retire(_) => "peer-in:Retire",
        PeerInput::MetaRejected { .. } => "peer-in:MetaRejected",
        PeerInput::EvictRetired { .. } => "peer-in:EvictRetired",
        PeerInput::Tick => "peer-in:Tick",
        PeerInput::DataIdle => "peer-in:DataIdle",
        PeerInput::ControlStalled => "peer-in:ControlStalled",
        PeerInput::SessionEnd => "peer-in:SessionEnd",
    }
}

/// Every name [`peer_input_name`] returns.
pub const PEER_INPUTS: [&str; 26] = [
    "peer-in:BlockAdmitted",
    "peer-in:RecvHeaderMeta",
    "peer-in:RecvPartPrefix",
    "peer-in:PartVerified",
    "peer-in:PartFailed",
    "peer-in:PartRefused",
    "peer-in:Ready",
    "peer-in:RecvHave",
    "peer-in:PullGranted",
    "peer-in:Seeds",
    "peer-in:RecvWant",
    "peer-in:ServePartQueued",
    "peer-in:ServeDone",
    "peer-in:WantEndWritten",
    "peer-in:RecvWantEnd",
    "peer-in:LocalStripeDone",
    "peer-in:LocalBlockDone",
    "peer-in:RecvStripeDone",
    "peer-in:RecvBlockDone",
    "peer-in:Retire",
    "peer-in:MetaRejected",
    "peer-in:EvictRetired",
    "peer-in:Tick",
    "peer-in:DataIdle",
    "peer-in:ControlStalled",
    "peer-in:SessionEnd",
];

/// The name of `output`'s variant.
pub fn peer_output_name(output: &PeerOutput) -> &'static str {
    match output {
        PeerOutput::SendControl(_) => "peer-out:SendControl",
        PeerOutput::SendHeaderMeta(_) => "peer-out:SendHeaderMeta",
        PeerOutput::SendHave { .. } => "peer-out:SendHave",
        PeerOutput::PushPart { .. } => "peer-out:PushPart",
        PeerOutput::SendSeedPart { .. } => "peer-out:SendSeedPart",
        PeerOutput::PushFlagged { .. } => "peer-out:PushFlagged",
        PeerOutput::SendWantEnd { .. } => "peer-out:SendWantEnd",
        PeerOutput::SubmitHeaderMeta { .. } => "peer-out:SubmitHeaderMeta",
        PeerOutput::VerifyPart { .. } => "peer-out:VerifyPart",
        PeerOutput::InsertPart { .. } => "peer-out:InsertPart",
        PeerOutput::PeerHas { .. } => "peer-out:PeerHas",
        PeerOutput::PeerBlockDone { .. } => "peer-out:PeerBlockDone",
        PeerOutput::ServeWant { .. } => "peer-out:ServeWant",
        PeerOutput::CancelQueuedPush { .. } => "peer-out:CancelQueuedPush",
        PeerOutput::CancelQueuedStripe { .. } => "peer-out:CancelQueuedStripe",
        PeerOutput::PurgeQueuedBlock { .. } => "peer-out:PurgeQueuedBlock",
        PeerOutput::ReleaseClaims { .. } => "peer-out:ReleaseClaims",
        PeerOutput::RateSample(_) => "peer-out:RateSample",
        PeerOutput::WantCompleted { .. } => "peer-out:WantCompleted",
        PeerOutput::PauseDataReads => "peer-out:PauseDataReads",
        PeerOutput::ResumeDataReads => "peer-out:ResumeDataReads",
        PeerOutput::CloseProtocol(_) => "peer-out:CloseProtocol",
        PeerOutput::CloseNeutralStall => "peer-out:CloseNeutralStall",
    }
}

/// The name of `input`'s variant.
pub fn block_input_name(input: &BlockInput) -> &'static str {
    match input {
        BlockInput::TipHeight { .. } => "block-in:TipHeight",
        BlockInput::ParentAdmitted { .. } => "block-in:ParentAdmitted",
        BlockInput::MetaReceived { .. } => "block-in:MetaReceived",
        BlockInput::HeaderAdmitted { .. } => "block-in:HeaderAdmitted",
        BlockInput::UnknownParentChecked { .. } => "block-in:UnknownParentChecked",
        BlockInput::CommitmentChecked { .. } => "block-in:CommitmentChecked",
        BlockInput::BlockAdmittedLocal { .. } => "block-in:BlockAdmittedLocal",
        BlockInput::PushFlagged { .. } => "block-in:PushFlagged",
        BlockInput::PathObserved { .. } => "block-in:PathObserved",
        BlockInput::RateSample { .. } => "block-in:RateSample",
        BlockInput::MetaSessionEnded { .. } => "block-in:MetaSessionEnded",
        BlockInput::SessionEnded { .. } => "block-in:SessionEnded",
        BlockInput::PeerBlockDone { .. } => "block-in:PeerBlockDone",
        BlockInput::PeerAnnounced { .. } => "block-in:PeerAnnounced",
        BlockInput::PlanPulls { .. } => "block-in:PlanPulls",
        BlockInput::ReleaseClaims { .. } => "block-in:ReleaseClaims",
        BlockInput::InsertPart { .. } => "block-in:InsertPart",
        BlockInput::ReleasePartCharge { .. } => "block-in:ReleasePartCharge",
        BlockInput::PartUploaded { .. } => "block-in:PartUploaded",
        BlockInput::StripeJobDone { .. } => "block-in:StripeJobDone",
        BlockInput::BlockAssembled { .. } => "block-in:BlockAssembled",
        BlockInput::VerifierVerdict { .. } => "block-in:VerifierVerdict",
        BlockInput::FallbackDone { .. } => "block-in:FallbackDone",
        BlockInput::ProposedCommitFailed { .. } => "block-in:ProposedCommitFailed",
        BlockInput::Tick { .. } => "block-in:Tick",
    }
}

/// Every name [`block_input_name`] returns.
pub const BLOCK_INPUTS: [&str; 25] = [
    "block-in:TipHeight",
    "block-in:ParentAdmitted",
    "block-in:MetaReceived",
    "block-in:HeaderAdmitted",
    "block-in:UnknownParentChecked",
    "block-in:CommitmentChecked",
    "block-in:BlockAdmittedLocal",
    "block-in:PushFlagged",
    "block-in:PathObserved",
    "block-in:RateSample",
    "block-in:MetaSessionEnded",
    "block-in:SessionEnded",
    "block-in:PeerBlockDone",
    "block-in:PeerAnnounced",
    "block-in:PlanPulls",
    "block-in:ReleaseClaims",
    "block-in:InsertPart",
    "block-in:ReleasePartCharge",
    "block-in:PartUploaded",
    "block-in:StripeJobDone",
    "block-in:BlockAssembled",
    "block-in:VerifierVerdict",
    "block-in:FallbackDone",
    "block-in:ProposedCommitFailed",
    "block-in:Tick",
];

/// The name of `output`'s variant.
pub fn block_output_name(output: &BlockOutput) -> &'static str {
    match output {
        BlockOutput::AdmitHeader { .. } => "block-out:AdmitHeader",
        BlockOutput::CheckUnknownParent { .. } => "block-out:CheckUnknownParent",
        BlockOutput::VerifyCommitment { .. } => "block-out:VerifyCommitment",
        BlockOutput::ClosePeer { .. } => "block-out:ClosePeer",
        BlockOutput::ClaimBody { .. } => "block-out:ClaimBody",
        BlockOutput::ExtendBodyClaim { .. } => "block-out:ExtendBodyClaim",
        BlockOutput::ReleaseBodyClaim { .. } => "block-out:ReleaseBodyClaim",
        BlockOutput::PublishAdmitted { .. } => "block-out:PublishAdmitted",
        BlockOutput::GrantPull { .. } => "block-out:GrantPull",
        BlockOutput::PartReady { .. } => "block-out:PartReady",
        BlockOutput::NotifyStripeDone { .. } => "block-out:NotifyStripeDone",
        BlockOutput::NotifyBlockDone { .. } => "block-out:NotifyBlockDone",
        BlockOutput::StartStripeJob { .. } => "block-out:StartStripeJob",
        BlockOutput::SubmitVerifier { .. } => "block-out:SubmitVerifier",
        BlockOutput::PublishRetire { .. } => "block-out:PublishRetire",
        BlockOutput::MetaRejected { .. } => "block-out:MetaRejected",
        BlockOutput::PublishEvictRetired { .. } => "block-out:PublishEvictRetired",
    }
}

/// The name of `input`'s variant.
pub fn proposer_input_name(input: &ProposerInput) -> &'static str {
    match input {
        ProposerInput::TemplateBuilt { .. } => "proposer-in:TemplateBuilt",
        ProposerInput::EncodeDone { .. } => "proposer-in:EncodeDone",
        ProposerInput::Solved { .. } => "proposer-in:Solved",
    }
}

/// Every name [`proposer_input_name`] returns.
pub const PROPOSER_INPUTS: [&str; 3] = [
    "proposer-in:TemplateBuilt",
    "proposer-in:EncodeDone",
    "proposer-in:Solved",
];

/// The name of `phase`.
pub fn phase_name(phase: BlockPhase) -> &'static str {
    match phase {
        BlockPhase::Collecting => "phase:Collecting",
        BlockPhase::Verifying => "phase:Verifying",
        BlockPhase::Done => "phase:Done",
        BlockPhase::Fallback => "phase:Fallback",
        BlockPhase::Retired => "phase:Retired",
    }
}

/// What a completion delivered back to a machine: the reply its work
/// expects, a reply for work the machine already finished, or one for work
/// it never started. The replays record each under these names.
pub const COMPLETIONS: [&str; 3] = ["work:current", "work:stale", "work:unknown"];

/// The `Have` forms and malformed indices the `bpv1_peer` replay sends
/// (decision-33): a list, a bitmap, an index past the layout, a bitmap
/// padding bit, and a bitmap sized for another layout.
pub const HAVE_FORMS: [&str; 5] = [
    "have:list",
    "have:bitmap",
    "have:past-layout",
    "have:bitmap-padding",
    "have:bitmap-misfit",
];

/// One target's measurement.
#[derive(Clone, Debug)]
pub struct TargetQuality {
    /// The fuzz target.
    pub target: &'static str,
    /// Inputs replayed: the target's seeds, then the generated inputs.
    pub inputs: usize,
    /// Every name the inputs reached.
    pub reached: Reached,
    /// The names this target must be able to reach.
    pub expected: Vec<&'static str>,
}

impl TargetQuality {
    /// The expected names no input reached.
    pub fn missing(&self) -> Vec<&'static str> {
        self.expected
            .iter()
            .copied()
            .filter(|name| !self.reached.contains(name))
            .collect()
    }
}

/// A target's replay, its expected names, and the longest input it reads.
struct Target {
    name: &'static str,
    replay: fn(&[u8]) -> Reached,
    expected: fn() -> Vec<&'static str>,
    max_len: usize,
}

const TARGETS: [Target; 5] = [
    Target {
        name: "bpv1_peer",
        replay: |bytes| replay_bpv1_peer_bytes(bytes).reached,
        expected: || {
            PEER_INPUTS
                .iter()
                .copied()
                .chain(COMPLETIONS)
                .chain(HAVE_FORMS)
                .collect()
        },
        max_len: 512,
    },
    Target {
        name: "bpv1_block",
        replay: |bytes| replay_bpv1_block_bytes(bytes).reached,
        expected: || BLOCK_INPUTS.iter().copied().chain(COMPLETIONS).collect(),
        max_len: 512,
    },
    Target {
        name: "bpv1_mesh",
        replay: |bytes| replay_bpv1_mesh_bytes(bytes).reached,
        expected: || {
            PEER_INPUTS
                .iter()
                .chain(&BLOCK_INPUTS)
                .chain(&PROPOSER_INPUTS)
                .copied()
                .collect()
        },
        max_len: 1024,
    },
    Target {
        name: "bpv1_wire",
        replay: |bytes| {
            replay_bpv1_wire_bytes(bytes, &mut |run: &mut dyn FnMut()| {
                run();
                0
            })
            .reached
        },
        expected: || super::wire::EXPECTED.to_vec(),
        max_len: 512,
    },
    Target {
        name: "bpv1_codec",
        replay: |bytes| replay_bpv1_codec_bytes(bytes).reached,
        expected: || super::codec::EXPECTED.to_vec(),
        max_len: 256,
    },
];

/// Replays every BPv1 target's in-code seeds, then `generated` inputs per
/// target drawn from `seed`, and reports what each target reached.
///
/// Generated inputs are uniform bytes of a uniform length up to the target's
/// campaign `-max_len`. The report measures applicability: how many of a
/// target's expected names its seeds and a short run reach. It runs no
/// campaign and keeps no corpus.
pub fn measure_bpv1_targets(generated: usize, seed: u64) -> Vec<TargetQuality> {
    let seeds = seed_inputs();
    let mut rng = SplitMix64(seed);
    TARGETS
        .iter()
        .map(|target| {
            let mut reached = Reached::default();
            let mut inputs = 0;
            for seed in seeds.iter().filter(|s| s.target == target.name) {
                reached.merge(&(target.replay)(&seed.bytes));
                inputs += 1;
            }
            for _ in 0..generated {
                let len = usize::try_from(rng.next() % (target.max_len as u64 + 1))
                    .expect("the length is at most max_len");
                // Each `u64` gives eight bytes; the cast keeps the low byte.
                let bytes: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
                reached.merge(&(target.replay)(&bytes));
                inputs += 1;
            }
            TargetQuality {
                target: target.name,
                inputs,
                reached,
                expected: (target.expected)(),
            }
        })
        .collect()
}

/// A small deterministic generator for the measured inputs. The thread RNG
/// would make a measurement unrepeatable.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// Records `step`'s machine names in `reached`: what each step took and put
/// out. Shared by the replays so each names alternatives one way.
pub(crate) fn hit_peer_step(reached: &mut Reached, input: &PeerInput, outputs: &[PeerOutput]) {
    reached.hit(peer_input_name(input));
    for output in outputs {
        reached.hit(peer_output_name(output));
        if let PeerOutput::CloseProtocol(cause) = output {
            reached.hit(cause.row.label());
        }
    }
}

/// Records a table step's input, outputs and close rows in `reached`.
pub(crate) fn hit_block_step(reached: &mut Reached, input: &BlockInput, outputs: &[BlockOutput]) {
    reached.hit(block_input_name(input));
    for output in outputs {
        reached.hit(block_output_name(output));
        if let BlockOutput::ClosePeer { row, .. } = output {
            reached.hit(row.label());
        }
    }
}
