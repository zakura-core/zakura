//! Stable error categories and peer-attribution boundaries.

use std::{error::Error as StdError, fmt, num::NonZeroU64};

use zakura_chain::BoxError;

use crate::{BodyPayloadMismatch, BranchId, ConsensusBodyInvalid, EvidenceId, HeaderId, SourceId};

/// Stable normative rule identifier attached to a failure.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct RuleId(&'static str);

impl RuleId {
    /// Construct a rule ID from a checked-in stable identifier.
    pub const fn new(id: &'static str) -> Self {
        Self(id)
    }

    /// Return the stable identifier.
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

/// Exact subject of a header-chain operation or failure.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum ErrorSubject {
    /// One hash-qualified header.
    Header(HeaderId),
    /// One exact anchor/target branch.
    Branch(BranchId),
    /// One correlated peer request.
    Request {
        /// Stable peer/session source.
        source: SourceId,
        /// Nonzero request ID.
        request_id: NonZeroU64,
    },
    /// A named local subsystem invariant or resource.
    Local(&'static str),
}

/// Stable category preserved across service, driver, reactor, and metrics boundaries.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum ErrorCategory {
    /// Structurally invalid peer protocol data.
    MalformedProtocol,
    /// A deterministically invalid network header.
    InvalidHeader,
    /// A valid but non-selected fork.
    ValidLosingFork,
    /// A header deferred only by an injected-clock rule.
    DeferredHeader,
    /// A body payload did not match its requested header.
    BodyPayloadMismatch,
    /// A matching body failed consensus validation.
    ConsensusBodyInvalid,
    /// Local operator policy made a header ineligible.
    OperatorIneligible,
    /// Completion belongs to a stale target, version, or generation.
    StaleTargetOrGeneration,
    /// A local anchor, retained path, or durable invariant is incoherent.
    LocalAnchorOrIncoherence,
    /// A local resource, storage, or transient execution failure.
    LocalResourceOrStorage,
}

impl ErrorCategory {
    /// Stable low-cardinality label for metrics and traces.
    pub const fn metrics_label(self) -> &'static str {
        match self {
            Self::MalformedProtocol => "malformed_protocol",
            Self::InvalidHeader => "invalid_header",
            Self::ValidLosingFork => "valid_losing_fork",
            Self::DeferredHeader => "deferred_header",
            Self::BodyPayloadMismatch => "body_payload_mismatch",
            Self::ConsensusBodyInvalid => "consensus_body_invalid",
            Self::OperatorIneligible => "operator_ineligible",
            Self::StaleTargetOrGeneration => "stale_target_or_generation",
            Self::LocalAnchorOrIncoherence => "local_anchor_or_incoherence",
            Self::LocalResourceOrStorage => "local_resource_or_storage",
        }
    }
}

/// Explicit source attribution for evidence and peer scoring.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum Attribution {
    /// No peer is responsible.
    None,
    /// Header-delivery peer is responsible.
    HeaderPeer(SourceId),
    /// Body-delivery peer is responsible.
    BodyPeer(SourceId),
    /// Auxiliary-metadata peer is responsible.
    AuxPeer(SourceId),
}

impl Attribution {
    /// Stable low-cardinality label for metrics and traces.
    pub const fn metrics_label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::HeaderPeer(_) => "header_peer",
            Self::BodyPeer(_) => "body_peer",
            Self::AuxPeer(_) => "aux_peer",
        }
    }
}

/// Complete typed header-chain failure record.
pub struct HeaderChainError {
    /// Stable error category.
    pub category: ErrorCategory,
    /// Exact affected object or local subsystem.
    pub subject: ErrorSubject,
    /// Normative rule that failed, when applicable.
    pub rule: Option<RuleId>,
    /// Stable evidence record, when one was retained.
    pub evidence: Option<EvidenceId>,
    /// Explicit peer attribution boundary.
    pub attribution: Attribution,
    /// Original typed error, retained without recategorization.
    pub source: Option<BoxError>,
}

impl HeaderChainError {
    /// Construct a category-preserving error with explicit attribution.
    pub fn new(
        category: ErrorCategory,
        subject: ErrorSubject,
        rule: Option<RuleId>,
        evidence: Option<EvidenceId>,
        attribution: Attribution,
        source: Option<BoxError>,
    ) -> Self {
        Self {
            category,
            subject,
            rule,
            evidence,
            attribution,
            source,
        }
    }

    /// Construct malformed-protocol evidence attributed to its header peer.
    pub fn malformed_protocol(
        subject: ErrorSubject,
        rule: RuleId,
        source_id: SourceId,
        source: Option<BoxError>,
    ) -> Self {
        Self::new(
            ErrorCategory::MalformedProtocol,
            subject,
            Some(rule),
            None,
            Attribution::HeaderPeer(source_id),
            source,
        )
    }

    /// Construct invalid-header evidence attributed to its header peer.
    pub fn invalid_header(
        subject: ErrorSubject,
        rule: RuleId,
        evidence: EvidenceId,
        source_id: SourceId,
        source: Option<BoxError>,
    ) -> Self {
        Self::new(
            ErrorCategory::InvalidHeader,
            subject,
            Some(rule),
            Some(evidence),
            Attribution::HeaderPeer(source_id),
            source,
        )
    }

    /// Construct mismatched-body evidence attributed only to its body supplier.
    pub fn body_payload_mismatch(evidence: BodyPayloadMismatch) -> Self {
        Self::new(
            ErrorCategory::BodyPayloadMismatch,
            ErrorSubject::Header(HeaderId::new(evidence.requested)),
            None,
            Some(evidence.evidence),
            Attribution::BodyPeer(evidence.source),
            None,
        )
    }

    /// Construct deterministic invalid-body evidence attributed only to its body supplier.
    pub fn consensus_body_invalid(evidence: &ConsensusBodyInvalid) -> Self {
        Self::new(
            ErrorCategory::ConsensusBodyInvalid,
            ErrorSubject::Header(HeaderId::new(evidence.hash)),
            None,
            Some(evidence.evidence),
            Attribution::BodyPeer(evidence.source),
            None,
        )
    }

    /// Construct a local unknown-anchor or retained-path incoherence.
    pub fn unknown_anchor(subject: ErrorSubject, source: Option<BoxError>) -> Self {
        Self::new(
            ErrorCategory::LocalAnchorOrIncoherence,
            subject,
            None,
            None,
            Attribution::None,
            source,
        )
    }

    /// Construct a stale target, version, generation, or validation owner.
    pub fn stale_target(subject: ErrorSubject) -> Self {
        Self::new(
            ErrorCategory::StaleTargetOrGeneration,
            subject,
            None,
            None,
            Attribution::None,
            None,
        )
    }

    /// Construct a local resource, task, service, or storage failure.
    pub fn local_resource(subject: ErrorSubject, source: Option<BoxError>) -> Self {
        Self::new(
            ErrorCategory::LocalResourceOrStorage,
            subject,
            None,
            None,
            Attribution::None,
            source,
        )
    }

    /// Return true only for categories that automatically justify header-peer scoring.
    pub fn is_automatic_header_peer_fault(&self) -> bool {
        matches!(
            (&self.category, &self.attribution),
            (
                ErrorCategory::MalformedProtocol | ErrorCategory::InvalidHeader,
                Attribution::HeaderPeer(_)
            )
        )
    }

    /// Return true only for evidence-bearing body failures attributed to their supplier.
    pub fn is_automatic_body_peer_fault(&self) -> bool {
        self.evidence.is_some()
            && matches!(
                (&self.category, &self.attribution),
                (
                    ErrorCategory::BodyPayloadMismatch | ErrorCategory::ConsensusBodyInvalid,
                    Attribution::BodyPeer(_)
                )
            )
    }
}

impl fmt::Debug for HeaderChainError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HeaderChainError")
            .field("category", &self.category)
            .field("subject", &self.subject)
            .field("rule", &self.rule)
            .field("evidence", &self.evidence)
            .field("attribution", &self.attribution)
            .field("source", &self.source.as_ref().map(ToString::to_string))
            .finish()
    }
}

impl fmt::Display for HeaderChainError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?} for {:?}", self.category, self.subject)
    }
}

impl StdError for HeaderChainError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| -> &(dyn StdError + 'static) { source })
    }
}
