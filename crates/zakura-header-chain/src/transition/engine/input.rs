//! Engine-boundary transition inputs and the durable facts they may carry.

use crate::{
    AuxEvidence, BodyEvidence, BodySupplierDiscovered, FinalityRecord, Frontier,
    FullStateFinalized, InsertHeaders, MigratedPinRefutation, OperatorBodyRetry,
    OperatorInvalidate, OperatorReconsider, StateVersion, TransitionDomain, TransitionEvent,
    ValidationLease, VerifiedBlockAccepted, VerifiedChainChanged,
};

/// Durable predecessor leases used for contextual header validation.
///
/// # Adapter loading obligation
///
/// Before planning, the state adapter must load every [`ValidationLease`] needed
/// to reconstruct difficulty/time context for parents that are no longer retained
/// in the header graph. Leases must be coherent with the active network and trust
/// anchor and authorized via
/// [`crate::FullStateEvidenceAuthority::authorizes_validation_lease`]. Omitting
/// required leases fails planning as [`crate::TransitionFailure::MissingDurableFacts`],
/// not as store I/O.
#[derive(Clone, Debug, Default)]
pub struct HeaderValidationFacts {
    /// Exact predecessor leases available for missing retained parents.
    pub validation_leases: Vec<ValidationLease>,
}

/// Durable facts consumed by prepared header insertion, including finality rebase history.
///
/// # Adapter loading obligation
///
/// In addition to [`HeaderValidationFacts`], the adapter must supply the contiguous
/// append-only [`FinalityRecord`] chain from the work's stable finality anchor to
/// current finality whenever monotone finality may rebase the insertion. Missing
/// or non-contiguous rebase history fails as
/// [`crate::TransitionFailure::MissingDurableFacts`] or stale preparation—never as
/// a successful partial admit.
#[derive(Clone, Debug, Default)]
pub struct HeaderInsertionFacts {
    /// Predecessor leases for the original and rebased parents.
    pub validation: HeaderValidationFacts,
    /// Contiguous finality records from the work's stable anchor to current finality.
    pub finality_rebase_history: Vec<FinalityRecord>,
}

/// Engine-boundary package that binds one [`TransitionEvent`] to the durable
/// facts that event may consume.
///
/// The state write adapter builds this from a [`crate::TransitionRequest`]: it
/// authenticates the event, loads only the store rows that variant needs, and
/// hands the result to [`super::HeaderChainEngine::plan_transition`]. The planner
/// never reads the durable store itself.
///
/// Exhaustiveness is the contract. Each variant carries exactly its allowed
/// facts (for example validation leases and finality rebase history for header
/// insertion, or a preserved migration pin for pin refutation). Unrelated store
/// facts are unrepresentable.
///
/// Freshness is also variant-specific: most inputs are version-qualified via
/// `expected_version`, while [`Self::InsertHeaders`] and [`Self::AuxEvidence`]
/// omit it and rely on work ownership instead.
#[derive(Clone, Debug)]
pub enum TransitionInput {
    /// Prepared header admission with contextual leases and optional rebase history.
    InsertHeaders {
        /// Authenticated prepared insertion.
        event: Box<InsertHeaders>,
        /// Durable validation and rebase facts for this insertion.
        facts: HeaderInsertionFacts,
    },
    /// Full-state selected-path replacement with contextual header leases.
    VerifiedChainChanged {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated verified-path change.
        event: VerifiedChainChanged,
        /// Durable predecessor leases for missing path headers.
        facts: HeaderValidationFacts,
    },
    /// Full-state side-path acceptance with contextual header leases.
    VerifiedBlockAccepted {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated side-path acceptance.
        event: VerifiedBlockAccepted,
        /// Durable predecessor leases for missing path headers.
        facts: HeaderValidationFacts,
    },
    /// Body delivery or verification evidence.
    BodyEvidence {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated body evidence.
        event: BodyEvidence,
    },
    /// Newly eligible body-supplier discovery.
    BodySupplierDiscovered {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated supplier discovery.
        event: BodySupplierDiscovered,
    },
    /// Authenticated operator body retry.
    OperatorBodyRetry {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated retry.
        event: OperatorBodyRetry,
    },
    /// Reversible operator invalidation.
    OperatorInvalidate {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated invalidation.
        event: OperatorInvalidate,
    },
    /// Reason-scoped operator reconsideration.
    OperatorReconsider {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated reconsideration.
        event: OperatorReconsider,
    },
    /// Integrated full-state finality advancement.
    FullStateFinalized {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated finality evidence.
        event: FullStateFinalized,
    },
    /// Migrated headers-only pin refutation with the preserved durable pin fact.
    MigratedPinRefutation {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated refutation.
        event: MigratedPinRefutation,
        /// The exact preserved migration pin when durable history contains it.
        preserved_pin: Option<Frontier>,
    },
    /// Hash-scoped auxiliary evidence; freshness is owner-qualified.
    AuxEvidence {
        /// Authenticated auxiliary update.
        event: Box<AuxEvidence>,
    },
    /// Reevaluate all locally due future-time deferrals.
    ReevaluateDeferred {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
    },
}

impl TransitionInput {
    /// Return the submitted event domain.
    pub fn domain(&self) -> TransitionDomain {
        self.event().domain()
    }

    /// Return the typed event carried by this input.
    pub fn event(&self) -> TransitionEvent {
        match self {
            Self::InsertHeaders { event, .. } => TransitionEvent::InsertHeaders(event.clone()),
            Self::VerifiedChainChanged { event, .. } => {
                TransitionEvent::VerifiedChainChanged(event.clone())
            }
            Self::VerifiedBlockAccepted { event, .. } => {
                TransitionEvent::VerifiedBlockAccepted(event.clone())
            }
            Self::BodyEvidence { event, .. } => TransitionEvent::BodyEvidence(event.clone()),
            Self::BodySupplierDiscovered { event, .. } => {
                TransitionEvent::BodySupplierDiscovered(*event)
            }
            Self::OperatorBodyRetry { event, .. } => TransitionEvent::OperatorBodyRetry(*event),
            Self::OperatorInvalidate { event, .. } => TransitionEvent::OperatorInvalidate(*event),
            Self::OperatorReconsider { event, .. } => TransitionEvent::OperatorReconsider(*event),
            Self::FullStateFinalized { event, .. } => {
                TransitionEvent::FullStateFinalized(event.clone())
            }
            Self::MigratedPinRefutation { event, .. } => {
                TransitionEvent::MigratedPinRefutation(event.clone())
            }
            Self::AuxEvidence { event } => TransitionEvent::AuxEvidence(event.clone()),
            Self::ReevaluateDeferred { .. } => TransitionEvent::ReevaluateDeferred,
        }
    }

    /// Return the caller-observed durable version when the input is version-qualified.
    ///
    /// Owner-qualified insertion and auxiliary inputs return `None` because their
    /// freshness is enforced by work ownership rather than state version.
    pub const fn expected_version(&self) -> Option<StateVersion> {
        match self {
            Self::InsertHeaders { .. } | Self::AuxEvidence { .. } => None,
            Self::VerifiedChainChanged {
                expected_version, ..
            }
            | Self::VerifiedBlockAccepted {
                expected_version, ..
            }
            | Self::BodyEvidence {
                expected_version, ..
            }
            | Self::BodySupplierDiscovered {
                expected_version, ..
            }
            | Self::OperatorBodyRetry {
                expected_version, ..
            }
            | Self::OperatorInvalidate {
                expected_version, ..
            }
            | Self::OperatorReconsider {
                expected_version, ..
            }
            | Self::FullStateFinalized {
                expected_version, ..
            }
            | Self::MigratedPinRefutation {
                expected_version, ..
            }
            | Self::ReevaluateDeferred { expected_version } => Some(*expected_version),
        }
    }

    /// Return header-validation leases when this input carries them.
    pub fn header_validation_facts(&self) -> Option<&HeaderValidationFacts> {
        match self {
            Self::InsertHeaders { facts, .. } => Some(&facts.validation),
            Self::VerifiedChainChanged { facts, .. }
            | Self::VerifiedBlockAccepted { facts, .. } => Some(facts),
            _ => None,
        }
    }

    /// Return finality rebase history when this input is a header insertion.
    pub fn finality_rebase_history(&self) -> Option<&[FinalityRecord]> {
        match self {
            Self::InsertHeaders { facts, .. } => Some(&facts.finality_rebase_history),
            _ => None,
        }
    }

    /// Return the preserved migrated pin fact when this input is a pin refutation.
    pub const fn preserved_migrated_pin(&self) -> Option<Option<Frontier>> {
        match self {
            Self::MigratedPinRefutation { preserved_pin, .. } => Some(*preserved_pin),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU64, sync::Arc};

    use zakura_chain::{
        block::{self, genesis::regtest_genesis_block},
        parameters::{testnet::RegtestParameters, Network},
    };

    use super::*;
    use crate::{
        AuxAuthentication, BodyRuleId, BodyUnavailableSummary, BodyWorkAuthority, BranchId,
        EvidenceId, FinalityEpoch, FinalitySource, HeaderContextFact, HeaderGeneration,
        HeaderValidationState, HeaderWorkAuthority, OperatorInvalidationId, PreparedHeader,
        PreparedHeaderBatch, SourceId, TargetCompletion, VerifiedBodyEvidence, VerifiedChangeCause,
        VerifiedGeneration,
    };

    fn frontier(byte: u8, height: u32) -> Frontier {
        Frontier::new(block::Height(height), block::Hash([byte; 32]))
    }

    fn header_owner() -> crate::HeaderSyncWorkOwner {
        HeaderWorkAuthority {
            header_generation: HeaderGeneration::new(3),
            branch: BranchId::new(block::Hash([1; 32]), block::Hash([2; 32])),
        }
        .bind(
            4,
            NonZeroU64::new(5).expect("the fixture request ID is nonzero"),
        )
        .into()
    }

    fn body_owner() -> crate::BodyWorkOwner {
        BodyWorkAuthority {
            header: HeaderWorkAuthority {
                header_generation: HeaderGeneration::new(3),
                branch: BranchId::new(block::Hash([1; 32]), block::Hash([2; 32])),
            },
            verified_generation: VerifiedGeneration::new(6),
        }
        .bind(
            7,
            NonZeroU64::new(8).expect("the fixture request ID is nonzero"),
        )
    }

    fn prepared_batch() -> PreparedHeaderBatch {
        let genesis = regtest_genesis_block();
        let parent = Frontier::new(block::Height(0), genesis.hash());
        let mut header = *genesis.header;
        header.previous_block_hash = parent.hash;
        header.nonce = [9; 32].into();
        let header = Arc::new(header);
        let prepared = PreparedHeader {
            hash: header.hash(),
            height: block::Height(1),
            block_work: header
                .difficulty_threshold
                .to_work()
                .expect("the regtest target has valid work"),
            validation: HeaderValidationState::Valid,
            header,
        };
        PreparedHeaderBatch::new(
            vec![prepared],
            parent,
            Network::new_regtest(RegtestParameters::default()),
            [10; 32],
            EvidenceId::from_digest([11; 32]),
        )
        .expect("the fixture batch is nonempty")
    }

    fn validation_facts() -> HeaderValidationFacts {
        let genesis = regtest_genesis_block();
        let parent = Frontier::new(block::Height(0), genesis.hash());
        HeaderValidationFacts {
            validation_leases: vec![ValidationLease::new(
                parent,
                vec![HeaderContextFact {
                    frontier: parent,
                    header: genesis.header.clone(),
                }],
                Network::new_regtest(RegtestParameters::default()),
                [10; 32],
            )],
        }
    }

    struct InputCase {
        name: &'static str,
        input: TransitionInput,
        event: TransitionEvent,
        expected_version: Option<StateVersion>,
        validation_lease_count: Option<usize>,
        rebase_history: Option<Vec<FinalityRecord>>,
        preserved_pin: Option<Option<Frontier>>,
    }

    #[test]
    fn transition_input_accessors_match_each_variant() {
        let version = StateVersion::new(12);
        let parent = prepared_batch().receipt().parent();
        let insert = InsertHeaders {
            owner: header_owner(),
            source: SourceId::from_digest([13; 32]),
            parent_hash: parent.hash,
            target_tip_hash: block::Hash([14; 32]),
            completion: TargetCompletion::TargetComplete {
                common_ancestor: parent,
            },
            batch: prepared_batch(),
            aux: Vec::new(),
        };
        let verified_change = VerifiedChainChanged {
            full_state_transition_id: EvidenceId::from_digest([15; 32]),
            old_tip: parent,
            new_path: Vec::new(),
            cause: VerifiedChangeCause::Reset,
        };
        let verified_accept = VerifiedBlockAccepted {
            full_state_transition_id: EvidenceId::from_digest([16; 32]),
            path: Vec::new(),
        };
        let body = BodyEvidence::Verified(VerifiedBodyEvidence {
            hash: block::Hash([17; 32]),
            evidence: EvidenceId::from_digest([18; 32]),
        });
        let supplier = BodySupplierDiscovered {
            hash: block::Hash([19; 32]),
            evidence: EvidenceId::from_digest([20; 32]),
            availability: BodyUnavailableSummary::default(),
        };
        let retry = OperatorBodyRetry {
            hash: block::Hash([21; 32]),
            evidence: EvidenceId::from_digest([22; 32]),
            availability: BodyUnavailableSummary::default(),
        };
        let invalidate = OperatorInvalidate {
            target: block::Hash([23; 32]),
            id: OperatorInvalidationId::new([24; 16]),
            operator_reason_digest: [25; 32],
            evidence: EvidenceId::from_digest([26; 32]),
        };
        let reconsider = OperatorReconsider {
            target: block::Hash([27; 32]),
            id: OperatorInvalidationId::new([28; 16]),
            invalidation_evidence: Some(EvidenceId::from_digest([29; 32])),
            evidence: EvidenceId::from_digest([30; 32]),
        };
        let finalized = FullStateFinalized {
            full_state_transition_id: EvidenceId::from_digest([31; 32]),
            new_finalized: frontier(32, 4),
            verified_path_proof: vec![block::Hash([33; 32])],
        };
        let pin = frontier(34, 5);
        let refutation = MigratedPinRefutation {
            full_state_transition_id: EvidenceId::from_digest([35; 32]),
            pin,
            invalid_header: frontier(36, 3),
            rule: BodyRuleId::new("body.rule"),
        };
        let auxiliary = AuxEvidence {
            owner: body_owner(),
            deliveries: Vec::new(),
            authentication: AuxAuthentication::Unauthenticated,
        };
        let rebase = FinalityRecord {
            previous: parent,
            current: frontier(37, 1),
            source: FinalitySource::FullState {
                evidence: EvidenceId::from_digest([38; 32]),
            },
            epoch: FinalityEpoch::new(1),
        };

        let cases = vec![
            InputCase {
                name: "insert headers",
                event: TransitionEvent::InsertHeaders(Box::new(insert.clone())),
                input: TransitionInput::InsertHeaders {
                    event: Box::new(insert),
                    facts: HeaderInsertionFacts {
                        validation: validation_facts(),
                        finality_rebase_history: vec![rebase],
                    },
                },
                expected_version: None,
                validation_lease_count: Some(1),
                rebase_history: Some(vec![rebase]),
                preserved_pin: None,
            },
            InputCase {
                name: "verified chain changed",
                event: TransitionEvent::VerifiedChainChanged(verified_change.clone()),
                input: TransitionInput::VerifiedChainChanged {
                    expected_version: version,
                    event: verified_change,
                    facts: validation_facts(),
                },
                expected_version: Some(version),
                validation_lease_count: Some(1),
                rebase_history: None,
                preserved_pin: None,
            },
            InputCase {
                name: "verified block accepted",
                event: TransitionEvent::VerifiedBlockAccepted(verified_accept.clone()),
                input: TransitionInput::VerifiedBlockAccepted {
                    expected_version: version,
                    event: verified_accept,
                    facts: validation_facts(),
                },
                expected_version: Some(version),
                validation_lease_count: Some(1),
                rebase_history: None,
                preserved_pin: None,
            },
            InputCase {
                name: "body evidence",
                event: TransitionEvent::BodyEvidence(body.clone()),
                input: TransitionInput::BodyEvidence {
                    expected_version: version,
                    event: body,
                },
                expected_version: Some(version),
                validation_lease_count: None,
                rebase_history: None,
                preserved_pin: None,
            },
            InputCase {
                name: "body supplier discovered",
                event: TransitionEvent::BodySupplierDiscovered(supplier),
                input: TransitionInput::BodySupplierDiscovered {
                    expected_version: version,
                    event: supplier,
                },
                expected_version: Some(version),
                validation_lease_count: None,
                rebase_history: None,
                preserved_pin: None,
            },
            InputCase {
                name: "operator body retry",
                event: TransitionEvent::OperatorBodyRetry(retry),
                input: TransitionInput::OperatorBodyRetry {
                    expected_version: version,
                    event: retry,
                },
                expected_version: Some(version),
                validation_lease_count: None,
                rebase_history: None,
                preserved_pin: None,
            },
            InputCase {
                name: "operator invalidate",
                event: TransitionEvent::OperatorInvalidate(invalidate),
                input: TransitionInput::OperatorInvalidate {
                    expected_version: version,
                    event: invalidate,
                },
                expected_version: Some(version),
                validation_lease_count: None,
                rebase_history: None,
                preserved_pin: None,
            },
            InputCase {
                name: "operator reconsider",
                event: TransitionEvent::OperatorReconsider(reconsider),
                input: TransitionInput::OperatorReconsider {
                    expected_version: version,
                    event: reconsider,
                },
                expected_version: Some(version),
                validation_lease_count: None,
                rebase_history: None,
                preserved_pin: None,
            },
            InputCase {
                name: "full-state finalized",
                event: TransitionEvent::FullStateFinalized(finalized.clone()),
                input: TransitionInput::FullStateFinalized {
                    expected_version: version,
                    event: finalized,
                },
                expected_version: Some(version),
                validation_lease_count: None,
                rebase_history: None,
                preserved_pin: None,
            },
            InputCase {
                name: "migrated pin present",
                event: TransitionEvent::MigratedPinRefutation(refutation.clone()),
                input: TransitionInput::MigratedPinRefutation {
                    expected_version: version,
                    event: refutation.clone(),
                    preserved_pin: Some(pin),
                },
                expected_version: Some(version),
                validation_lease_count: None,
                rebase_history: None,
                preserved_pin: Some(Some(pin)),
            },
            InputCase {
                name: "migrated pin absent",
                event: TransitionEvent::MigratedPinRefutation(refutation.clone()),
                input: TransitionInput::MigratedPinRefutation {
                    expected_version: version,
                    event: refutation,
                    preserved_pin: None,
                },
                expected_version: Some(version),
                validation_lease_count: None,
                rebase_history: None,
                preserved_pin: Some(None),
            },
            InputCase {
                name: "auxiliary evidence",
                event: TransitionEvent::AuxEvidence(Box::new(auxiliary.clone())),
                input: TransitionInput::AuxEvidence {
                    event: Box::new(auxiliary),
                },
                expected_version: None,
                validation_lease_count: None,
                rebase_history: None,
                preserved_pin: None,
            },
            InputCase {
                name: "reevaluate deferred",
                event: TransitionEvent::ReevaluateDeferred,
                input: TransitionInput::ReevaluateDeferred {
                    expected_version: version,
                },
                expected_version: Some(version),
                validation_lease_count: None,
                rebase_history: None,
                preserved_pin: None,
            },
        ];

        for case in cases {
            assert_eq!(case.input.event(), case.event, "{} event", case.name);
            assert_eq!(
                case.input.domain(),
                case.event.domain(),
                "{} domain",
                case.name
            );
            assert_eq!(
                case.input.expected_version(),
                case.expected_version,
                "{} version",
                case.name
            );
            assert_eq!(
                case.input
                    .header_validation_facts()
                    .map(|facts| facts.validation_leases.len()),
                case.validation_lease_count,
                "{} validation facts",
                case.name
            );
            assert_eq!(
                case.input.finality_rebase_history(),
                case.rebase_history.as_deref(),
                "{} rebase history",
                case.name
            );
            assert_eq!(
                case.input.preserved_migrated_pin(),
                case.preserved_pin,
                "{} preserved pin",
                case.name
            );
        }
    }
}
