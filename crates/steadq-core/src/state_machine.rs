// Auto-generated from spec/state-machine.json. Do not edit by hand.
// Source SHA-256: 8eba1a6b7a3d72aca483b07f52bbb1c97bee9828a0b338cdbdaf02dbfdf1ba92

pub const PROTOCOL_IR_IDENTITY: &str = "steadq-state-machine";
pub const PROTOCOL_IR_VERSION: u32 = 3;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum Operation {
    EnqueueImmediate,
    EnqueueDelayed,
    Promote,
    Claim,
    ExhaustedReadyCleanup,
    Renew,
    Acknowledge,
    RetryNow,
    RetryLater,
    Bury,
    ReapExpiredToReady,
    ReapExpiredToDead,
    Quarantine,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum State {
    Hidden,
    Ready,
    Leased,
    Delayed,
    Dead,
    Receipt,
    Quarantine,
    Active,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum ObjectKind {
    FullJob,
    FullReceipt,
    CompactReceipt,
    RawObject,
    WatermarkRecord,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum GenerationChange {
    Zero,
    Increment,
    IncrementOrSame,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum AttemptChange {
    Zero,
    Increment,
    Unchanged,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum TokenChange {
    None,
    New,
    Same,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum ReasonClass {
    AttemptsExhausted,
    ApplicationDefined,
    Corruption,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum ClockRequirement {
    None,
    AuthenticatedWallFloor,
    BoottimeAndAuthenticatedWallFloor,
    LeaseExpirationEvidence,
    LeaseExpirationEvidenceAndAuthenticatedWallFloor,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum TransitionQualification {
    None,
    AttemptsRemaining,
    AttemptsExhausted,
    RawBytesPreserved,
    ReceiptBucketEndPlusRetentionNotAfterWallFloor,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum ResolverProbeTopology {
    DestinationOnly,
    SourceAndDestination,
    ReceiptCandidatesAndSource,
    SourcePresence,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum SyncStep {
    File,
    DestinationDirectory,
    SourceDirectory,
    SameOrDestinationDirectory,
    SourceDirectoryIfDistinct,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum LinearizationPrimitive {
    PublishNoreplace,
    RenameNoreplace,
    RenameReplace,
    Unlink,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum FailureOutcome {
    NotCommitted,
    OutcomeUnknown,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum MutationClass {
    NoOverwriteMove,
    ReplacingMove,
    Publication,
    Unlink,
    InPlaceReadOnlyBarrier,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum ExceptionName {
    ReceiptCompaction,
    WallWatermarkAdvancement,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum UnlinkName {
    FullReceiptRetentionDeletion,
    CompactReceiptRetentionDeletion,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum SourceAuthentication {
    None,
    StrictReceipt,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum ReentryName {
    RequeueDead,
    RequeueQuarantine,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransitionDef {
    pub operation: Operation,
    pub source: State,
    pub source_object_kind: ObjectKind,
    pub destination: State,
    pub destination_object_kind: ObjectKind,
    pub generation_change: GenerationChange,
    pub attempt_change: AttemptChange,
    pub token_change: TokenChange,
    pub reason_class: Option<ReasonClass>,
    pub clock_requirement: ClockRequirement,
    pub required_syncs: &'static [SyncStep],
    pub linearization: LinearizationPrimitive,
    pub before_linearization_failure: FailureOutcome,
    pub after_linearization_failure: FailureOutcome,
    pub resolver_probe_topology: ResolverProbeTopology,
    pub qualification: TransitionQualification,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExceptionDef {
    pub name: ExceptionName,
    pub description: &'static str,
    pub source_object_kind: ObjectKind,
    pub destination_object_kind: ObjectKind,
    pub clock_requirement: ClockRequirement,
    pub mutation_class: MutationClass,
    pub linearization: LinearizationPrimitive,
    pub required_syncs: &'static [SyncStep],
    pub before_linearization_failure: FailureOutcome,
    pub after_linearization_failure: FailureOutcome,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnlinkDef {
    pub name: UnlinkName,
    pub description: &'static str,
    pub source: State,
    pub source_object_kind: ObjectKind,
    pub source_authentication: SourceAuthentication,
    pub clock_requirement: ClockRequirement,
    pub qualification: TransitionQualification,
    pub mutation_class: MutationClass,
    pub linearization: LinearizationPrimitive,
    pub required_syncs: &'static [SyncStep],
    pub before_linearization_failure: FailureOutcome,
    pub after_linearization_failure: FailureOutcome,
    pub resolver_probe_topology: ResolverProbeTopology,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReentryDef {
    pub name: ReentryName,
    pub source: State,
    pub description: &'static str,
    pub creates_new_identity: bool,
}

pub const TRANSITIONS: &[TransitionDef] = &[
    TransitionDef {
        operation: Operation::EnqueueImmediate,
        source: State::Hidden,
        source_object_kind: ObjectKind::FullJob,
        destination: State::Ready,
        destination_object_kind: ObjectKind::FullJob,
        generation_change: GenerationChange::Zero,
        attempt_change: AttemptChange::Zero,
        token_change: TokenChange::None,
        reason_class: None,
        clock_requirement: ClockRequirement::AuthenticatedWallFloor,
        required_syncs: &[SyncStep::File, SyncStep::DestinationDirectory],
        linearization: LinearizationPrimitive::PublishNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolver_probe_topology: ResolverProbeTopology::DestinationOnly,
        qualification: TransitionQualification::None,
    },
    TransitionDef {
        operation: Operation::EnqueueDelayed,
        source: State::Hidden,
        source_object_kind: ObjectKind::FullJob,
        destination: State::Delayed,
        destination_object_kind: ObjectKind::FullJob,
        generation_change: GenerationChange::Zero,
        attempt_change: AttemptChange::Zero,
        token_change: TokenChange::None,
        reason_class: None,
        clock_requirement: ClockRequirement::AuthenticatedWallFloor,
        required_syncs: &[SyncStep::File, SyncStep::DestinationDirectory],
        linearization: LinearizationPrimitive::PublishNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolver_probe_topology: ResolverProbeTopology::DestinationOnly,
        qualification: TransitionQualification::None,
    },
    TransitionDef {
        operation: Operation::Promote,
        source: State::Delayed,
        source_object_kind: ObjectKind::FullJob,
        destination: State::Ready,
        destination_object_kind: ObjectKind::FullJob,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        reason_class: None,
        clock_requirement: ClockRequirement::AuthenticatedWallFloor,
        required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolver_probe_topology: ResolverProbeTopology::SourceAndDestination,
        qualification: TransitionQualification::None,
    },
    TransitionDef {
        operation: Operation::Claim,
        source: State::Ready,
        source_object_kind: ObjectKind::FullJob,
        destination: State::Leased,
        destination_object_kind: ObjectKind::FullJob,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Increment,
        token_change: TokenChange::New,
        reason_class: None,
        clock_requirement: ClockRequirement::BoottimeAndAuthenticatedWallFloor,
        required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolver_probe_topology: ResolverProbeTopology::SourceAndDestination,
        qualification: TransitionQualification::None,
    },
    TransitionDef {
        operation: Operation::ExhaustedReadyCleanup,
        source: State::Ready,
        source_object_kind: ObjectKind::FullJob,
        destination: State::Dead,
        destination_object_kind: ObjectKind::FullJob,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        reason_class: Some(ReasonClass::AttemptsExhausted),
        clock_requirement: ClockRequirement::AuthenticatedWallFloor,
        required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolver_probe_topology: ResolverProbeTopology::SourceAndDestination,
        qualification: TransitionQualification::None,
    },
    TransitionDef {
        operation: Operation::Renew,
        source: State::Leased,
        source_object_kind: ObjectKind::FullJob,
        destination: State::Leased,
        destination_object_kind: ObjectKind::FullJob,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::Same,
        reason_class: None,
        clock_requirement: ClockRequirement::BoottimeAndAuthenticatedWallFloor,
        required_syncs: &[
            SyncStep::SameOrDestinationDirectory,
            SyncStep::SourceDirectoryIfDistinct,
        ],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolver_probe_topology: ResolverProbeTopology::SourceAndDestination,
        qualification: TransitionQualification::None,
    },
    TransitionDef {
        operation: Operation::Acknowledge,
        source: State::Leased,
        source_object_kind: ObjectKind::FullJob,
        destination: State::Receipt,
        destination_object_kind: ObjectKind::FullReceipt,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::Same,
        reason_class: None,
        clock_requirement: ClockRequirement::AuthenticatedWallFloor,
        required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolver_probe_topology: ResolverProbeTopology::ReceiptCandidatesAndSource,
        qualification: TransitionQualification::None,
    },
    TransitionDef {
        operation: Operation::RetryNow,
        source: State::Leased,
        source_object_kind: ObjectKind::FullJob,
        destination: State::Ready,
        destination_object_kind: ObjectKind::FullJob,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        reason_class: None,
        clock_requirement: ClockRequirement::None,
        required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolver_probe_topology: ResolverProbeTopology::SourceAndDestination,
        qualification: TransitionQualification::None,
    },
    TransitionDef {
        operation: Operation::RetryLater,
        source: State::Leased,
        source_object_kind: ObjectKind::FullJob,
        destination: State::Delayed,
        destination_object_kind: ObjectKind::FullJob,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        reason_class: None,
        clock_requirement: ClockRequirement::AuthenticatedWallFloor,
        required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolver_probe_topology: ResolverProbeTopology::SourceAndDestination,
        qualification: TransitionQualification::None,
    },
    TransitionDef {
        operation: Operation::Bury,
        source: State::Leased,
        source_object_kind: ObjectKind::FullJob,
        destination: State::Dead,
        destination_object_kind: ObjectKind::FullJob,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        reason_class: Some(ReasonClass::ApplicationDefined),
        clock_requirement: ClockRequirement::AuthenticatedWallFloor,
        required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolver_probe_topology: ResolverProbeTopology::SourceAndDestination,
        qualification: TransitionQualification::None,
    },
    TransitionDef {
        operation: Operation::ReapExpiredToReady,
        source: State::Leased,
        source_object_kind: ObjectKind::FullJob,
        destination: State::Ready,
        destination_object_kind: ObjectKind::FullJob,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        reason_class: None,
        clock_requirement: ClockRequirement::LeaseExpirationEvidence,
        required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolver_probe_topology: ResolverProbeTopology::SourceAndDestination,
        qualification: TransitionQualification::AttemptsRemaining,
    },
    TransitionDef {
        operation: Operation::ReapExpiredToDead,
        source: State::Leased,
        source_object_kind: ObjectKind::FullJob,
        destination: State::Dead,
        destination_object_kind: ObjectKind::FullJob,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        reason_class: Some(ReasonClass::AttemptsExhausted),
        clock_requirement: ClockRequirement::LeaseExpirationEvidenceAndAuthenticatedWallFloor,
        required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolver_probe_topology: ResolverProbeTopology::SourceAndDestination,
        qualification: TransitionQualification::AttemptsExhausted,
    },
    TransitionDef {
        operation: Operation::Quarantine,
        source: State::Active,
        source_object_kind: ObjectKind::RawObject,
        destination: State::Quarantine,
        destination_object_kind: ObjectKind::RawObject,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        reason_class: Some(ReasonClass::Corruption),
        clock_requirement: ClockRequirement::None,
        required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolver_probe_topology: ResolverProbeTopology::SourceAndDestination,
        qualification: TransitionQualification::RawBytesPreserved,
    },
];

pub const EXCEPTIONS: &[ExceptionDef] = &[
    ExceptionDef {
        name: ExceptionName::ReceiptCompaction,
        description: "Terminal full-job receipt replaced by byte-deterministic compact receipt at same pathname",
        source_object_kind: ObjectKind::FullReceipt,
        destination_object_kind: ObjectKind::CompactReceipt,
        clock_requirement: ClockRequirement::None,
        mutation_class: MutationClass::ReplacingMove,
        linearization: LinearizationPrimitive::RenameReplace,
        required_syncs: &[SyncStep::File, SyncStep::SameOrDestinationDirectory],
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
    },
    ExceptionDef {
        name: ExceptionName::WallWatermarkAdvancement,
        description: "Monotone wall-watermark record replaced under exclusive OFD lock",
        source_object_kind: ObjectKind::WatermarkRecord,
        destination_object_kind: ObjectKind::WatermarkRecord,
        clock_requirement: ClockRequirement::AuthenticatedWallFloor,
        mutation_class: MutationClass::ReplacingMove,
        linearization: LinearizationPrimitive::RenameReplace,
        required_syncs: &[SyncStep::File, SyncStep::SameOrDestinationDirectory],
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
    },
];

pub const UNLINKS: &[UnlinkDef] = &[
    UnlinkDef {
        name: UnlinkName::FullReceiptRetentionDeletion,
        description: "Authenticated retention deletion of an eligible full receipt",
        source: State::Receipt,
        source_object_kind: ObjectKind::FullReceipt,
        source_authentication: SourceAuthentication::StrictReceipt,
        clock_requirement: ClockRequirement::AuthenticatedWallFloor,
        qualification: TransitionQualification::ReceiptBucketEndPlusRetentionNotAfterWallFloor,
        mutation_class: MutationClass::Unlink,
        linearization: LinearizationPrimitive::Unlink,
        required_syncs: &[SyncStep::SourceDirectory],
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolver_probe_topology: ResolverProbeTopology::SourcePresence,
    },
    UnlinkDef {
        name: UnlinkName::CompactReceiptRetentionDeletion,
        description: "Authenticated retention deletion of an eligible compact receipt",
        source: State::Receipt,
        source_object_kind: ObjectKind::CompactReceipt,
        source_authentication: SourceAuthentication::StrictReceipt,
        clock_requirement: ClockRequirement::AuthenticatedWallFloor,
        qualification: TransitionQualification::ReceiptBucketEndPlusRetentionNotAfterWallFloor,
        mutation_class: MutationClass::Unlink,
        linearization: LinearizationPrimitive::Unlink,
        required_syncs: &[SyncStep::SourceDirectory],
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolver_probe_topology: ResolverProbeTopology::SourcePresence,
    },
];

pub const REENTRY: &[ReentryDef] = &[
    ReentryDef {
        name: ReentryName::RequeueDead,
        source: State::Dead,
        description: "Verified resubmission: creates new job identity, copies payload and safe metadata, adds old job_id as provenance",
        creates_new_identity: true,
    },
    ReentryDef {
        name: ReentryName::RequeueQuarantine,
        source: State::Quarantine,
        description: "Verified resubmission after full structural and payload verification: creates new job identity",
        creates_new_identity: true,
    },
];

/// Return the complete protocol definition for an operation.
pub fn transition(operation: Operation) -> &'static TransitionDef {
    match operation {
        Operation::EnqueueImmediate => &TRANSITIONS[0],
        Operation::EnqueueDelayed => &TRANSITIONS[1],
        Operation::Promote => &TRANSITIONS[2],
        Operation::Claim => &TRANSITIONS[3],
        Operation::ExhaustedReadyCleanup => &TRANSITIONS[4],
        Operation::Renew => &TRANSITIONS[5],
        Operation::Acknowledge => &TRANSITIONS[6],
        Operation::RetryNow => &TRANSITIONS[7],
        Operation::RetryLater => &TRANSITIONS[8],
        Operation::Bury => &TRANSITIONS[9],
        Operation::ReapExpiredToReady => &TRANSITIONS[10],
        Operation::ReapExpiredToDead => &TRANSITIONS[11],
        Operation::Quarantine => &TRANSITIONS[12],
    }
}

/// Check if a transition from source to destination is legal.
pub fn is_legal_transition(source: State, destination: State) -> bool {
    TRANSITIONS
        .iter()
        .any(|transition| transition.source == source && transition.destination == destination)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legal_transitions() {
        assert!(is_legal_transition(State::Hidden, State::Ready));
        assert!(is_legal_transition(State::Hidden, State::Delayed));
        assert!(is_legal_transition(State::Delayed, State::Ready));
        assert!(is_legal_transition(State::Ready, State::Leased));
        assert!(is_legal_transition(State::Ready, State::Dead));
        assert!(is_legal_transition(State::Leased, State::Leased));
        assert!(is_legal_transition(State::Leased, State::Receipt));
        assert!(is_legal_transition(State::Leased, State::Ready));
        assert!(is_legal_transition(State::Leased, State::Delayed));
        assert!(is_legal_transition(State::Leased, State::Dead));
        assert!(is_legal_transition(State::Leased, State::Ready));
        assert!(is_legal_transition(State::Leased, State::Dead));
        assert!(is_legal_transition(State::Active, State::Quarantine));
    }

    #[test]
    fn illegal_transitions() {
        for (source, destination) in [
            (State::Receipt, State::Ready),
            (State::Dead, State::Ready),
            (State::Quarantine, State::Ready),
            (State::Ready, State::Ready),
            (State::Hidden, State::Leased),
            (State::Ready, State::Receipt),
        ] {
            assert!(!is_legal_transition(source, destination));
        }
    }

    #[test]
    fn generated_collections_are_complete() {
        assert_eq!(TRANSITIONS.len(), 13);
        assert_eq!(EXCEPTIONS.len(), 2);
        assert_eq!(REENTRY.len(), 2);
        assert!(TRANSITIONS
            .iter()
            .all(|transition| !transition.required_syncs.is_empty()));
        assert!(TRANSITIONS.iter().all(|transition| {
            transition.before_linearization_failure == FailureOutcome::NotCommitted
                && transition.after_linearization_failure == FailureOutcome::OutcomeUnknown
        }));
        assert!(EXCEPTIONS.iter().all(|exception| exception.mutation_class
            == MutationClass::ReplacingMove
            && exception.linearization == LinearizationPrimitive::RenameReplace
            && exception.required_syncs == [SyncStep::File, SyncStep::SameOrDestinationDirectory]
            && exception.before_linearization_failure == FailureOutcome::NotCommitted
            && exception.after_linearization_failure == FailureOutcome::OutcomeUnknown));
        assert!(REENTRY.iter().all(|reentry| reentry.creates_new_identity));
    }

    #[test]
    fn claim_projects_complete_semantics() {
        let claim = TRANSITIONS
            .iter()
            .find(|transition| transition.operation == Operation::Claim)
            .unwrap();
        assert_eq!(claim.source, State::Ready);
        assert_eq!(claim.source_object_kind, ObjectKind::FullJob);
        assert_eq!(claim.destination, State::Leased);
        assert_eq!(claim.destination_object_kind, ObjectKind::FullJob);
        assert_eq!(claim.attempt_change, AttemptChange::Increment);
        assert_eq!(claim.generation_change, GenerationChange::Increment);
        assert_eq!(claim.token_change, TokenChange::New);
        assert_eq!(claim.reason_class, None);
        assert_eq!(
            claim.clock_requirement,
            ClockRequirement::BoottimeAndAuthenticatedWallFloor
        );
        assert_eq!(
            claim.required_syncs,
            &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory]
        );
        assert_eq!(claim.linearization, LinearizationPrimitive::RenameNoreplace);
        assert_eq!(
            claim.before_linearization_failure,
            FailureOutcome::NotCommitted
        );
        assert_eq!(
            claim.after_linearization_failure,
            FailureOutcome::OutcomeUnknown
        );
        assert_eq!(
            claim.resolver_probe_topology,
            ResolverProbeTopology::SourceAndDestination
        );
        assert_eq!(claim.qualification, TransitionQualification::None);
        let renew = transition(Operation::Renew);
        assert_eq!(
            renew.required_syncs,
            &[
                SyncStep::SameOrDestinationDirectory,
                SyncStep::SourceDirectoryIfDistinct,
            ]
        );
    }

    #[test]
    fn enqueue_uses_no_overwrite_publication() {
        let enqueue = TRANSITIONS
            .iter()
            .find(|transition| transition.operation == Operation::EnqueueImmediate)
            .unwrap();
        assert_eq!(
            enqueue.linearization,
            LinearizationPrimitive::PublishNoreplace
        );
    }

    #[test]
    fn terminal_and_exception_metadata_are_projected() {
        let reap = TRANSITIONS
            .iter()
            .find(|transition| transition.operation == Operation::ReapExpiredToDead)
            .unwrap();
        assert_eq!(reap.reason_class, Some(ReasonClass::AttemptsExhausted));
        assert_eq!(
            reap.qualification,
            TransitionQualification::AttemptsExhausted
        );
        let acknowledge = transition(Operation::Acknowledge);
        assert_eq!(acknowledge.source_object_kind, ObjectKind::FullJob);
        assert_eq!(acknowledge.destination_object_kind, ObjectKind::FullReceipt);
        let quarantine = transition(Operation::Quarantine);
        assert_eq!(quarantine.source_object_kind, ObjectKind::RawObject);
        assert_eq!(quarantine.destination_object_kind, ObjectKind::RawObject);
        assert_eq!(EXCEPTIONS[0].name, ExceptionName::ReceiptCompaction);
        assert_eq!(EXCEPTIONS[0].source_object_kind, ObjectKind::FullReceipt);
        assert_eq!(
            EXCEPTIONS[0].destination_object_kind,
            ObjectKind::CompactReceipt
        );
        assert_eq!(EXCEPTIONS[0].clock_requirement, ClockRequirement::None);
        assert_eq!(
            EXCEPTIONS[1].source_object_kind,
            ObjectKind::WatermarkRecord
        );
        assert_eq!(
            EXCEPTIONS[1].destination_object_kind,
            ObjectKind::WatermarkRecord
        );
        assert_eq!(
            EXCEPTIONS[1].clock_requirement,
            ClockRequirement::AuthenticatedWallFloor
        );
        assert_eq!(REENTRY[0].name, ReentryName::RequeueDead);
        assert_eq!(REENTRY[0].source, State::Dead);
    }
}

#[cfg(test)]
mod unlink_tests {
    use super::*;

    #[test]
    fn receipt_retention_unlinks_are_complete() {
        assert_eq!(UNLINKS.len(), 2);

        let full = UNLINKS
            .iter()
            .find(|unlink| unlink.name == UnlinkName::FullReceiptRetentionDeletion)
            .unwrap();
        assert_eq!(full.source_object_kind, ObjectKind::FullReceipt);
        let compact = UNLINKS
            .iter()
            .find(|unlink| unlink.name == UnlinkName::CompactReceiptRetentionDeletion)
            .unwrap();
        assert_eq!(compact.source_object_kind, ObjectKind::CompactReceipt);
        for unlink in UNLINKS {
            assert_eq!(unlink.source, State::Receipt);
            assert_eq!(
                unlink.source_authentication,
                SourceAuthentication::StrictReceipt
            );
            assert_eq!(
                unlink.clock_requirement,
                ClockRequirement::AuthenticatedWallFloor
            );
            assert_eq!(
                unlink.qualification,
                TransitionQualification::ReceiptBucketEndPlusRetentionNotAfterWallFloor
            );
            assert_eq!(unlink.mutation_class, MutationClass::Unlink);
            assert_eq!(unlink.linearization, LinearizationPrimitive::Unlink);
            assert_eq!(unlink.required_syncs, &[SyncStep::SourceDirectory]);
            assert_eq!(
                unlink.before_linearization_failure,
                FailureOutcome::NotCommitted
            );
            assert_eq!(
                unlink.after_linearization_failure,
                FailureOutcome::OutcomeUnknown
            );
            assert_eq!(
                unlink.resolver_probe_topology,
                ResolverProbeTopology::SourcePresence
            );
        }
    }
}

#[cfg(test)]
mod resolver_probe_tests {
    use super::*;

    #[test]
    fn transition_lookup_is_total() {
        for definition in TRANSITIONS {
            assert_eq!(transition(definition.operation), definition);
        }
    }

    #[test]
    fn resolver_probe_topology_matrix() {
        assert_eq!(
            transition(Operation::EnqueueImmediate).resolver_probe_topology,
            ResolverProbeTopology::DestinationOnly
        );
        assert_eq!(
            transition(Operation::Claim).resolver_probe_topology,
            ResolverProbeTopology::SourceAndDestination
        );
        assert_eq!(
            transition(Operation::Acknowledge).resolver_probe_topology,
            ResolverProbeTopology::ReceiptCandidatesAndSource
        );
    }
}
