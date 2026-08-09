// Auto-generated from spec/state-machine.json. Do not edit by hand.
// Source SHA-256: 98d2c334df4679cf27cbc45ce270590345f7dd1050305e46ce8fd1b960948e93

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
pub enum SyncStep {
    File,
    DestinationDirectory,
    SourceDirectory,
    SameOrDestinationDirectory,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum LinearizationPrimitive {
    PublishNoreplace,
    RenameNoreplace,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum FailureOutcome {
    NotCommitted,
    OutcomeUnknown,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum ExceptionName {
    ReceiptCompaction,
    WallWatermarkAdvancement,
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
    pub destination: State,
    pub generation_change: GenerationChange,
    pub attempt_change: AttemptChange,
    pub token_change: TokenChange,
    pub reason_class: Option<ReasonClass>,
    pub required_syncs: &'static [SyncStep],
    pub linearization: LinearizationPrimitive,
    pub before_linearization_failure: FailureOutcome,
    pub after_linearization_failure: FailureOutcome,
    /// Human-readable resolver documentation, not an executable rule.
    pub resolution_behavior: &'static str,
    /// Human-readable qualification, not an executable precondition.
    pub notes: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExceptionDef {
    pub name: ExceptionName,
    pub description: &'static str,
    pub uses_replacing_rename: bool,
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
        destination: State::Ready,
        generation_change: GenerationChange::Zero,
        attempt_change: AttemptChange::Zero,
        token_change: TokenChange::None,
        reason_class: None,
        required_syncs: &[SyncStep::File, SyncStep::DestinationDirectory],
        linearization: LinearizationPrimitive::PublishNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolution_behavior: "probe destination: observed = committed, absent = not committed",
        notes: None,
    },
    TransitionDef {
        operation: Operation::EnqueueDelayed,
        source: State::Hidden,
        destination: State::Delayed,
        generation_change: GenerationChange::Zero,
        attempt_change: AttemptChange::Zero,
        token_change: TokenChange::None,
        reason_class: None,
        required_syncs: &[SyncStep::File, SyncStep::DestinationDirectory],
        linearization: LinearizationPrimitive::PublishNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolution_behavior: "probe destination: observed = committed, absent = not committed",
        notes: None,
    },
    TransitionDef {
        operation: Operation::Promote,
        source: State::Delayed,
        destination: State::Ready,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        reason_class: None,
        required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolution_behavior:
            "probe both: destination observed = committed, source only = not committed",
        notes: None,
    },
    TransitionDef {
        operation: Operation::Claim,
        source: State::Ready,
        destination: State::Leased,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Increment,
        token_change: TokenChange::New,
        reason_class: None,
        required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolution_behavior: "probe both directories",
        notes: None,
    },
    TransitionDef {
        operation: Operation::ExhaustedReadyCleanup,
        source: State::Ready,
        destination: State::Dead,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        reason_class: Some(ReasonClass::AttemptsExhausted),
        required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolution_behavior: "probe both",
        notes: None,
    },
    TransitionDef {
        operation: Operation::Renew,
        source: State::Leased,
        destination: State::Leased,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::Same,
        reason_class: None,
        required_syncs: &[SyncStep::SameOrDestinationDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolution_behavior:
            "probe destination: new generation observed = renewed, old gen observed = lease lost",
        notes: None,
    },
    TransitionDef {
        operation: Operation::Acknowledge,
        source: State::Leased,
        destination: State::Receipt,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::Same,
        reason_class: None,
        required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolution_behavior: "probe receipt buckets by exact name",
        notes: None,
    },
    TransitionDef {
        operation: Operation::RetryNow,
        source: State::Leased,
        destination: State::Ready,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        reason_class: None,
        required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolution_behavior: "probe both",
        notes: None,
    },
    TransitionDef {
        operation: Operation::RetryLater,
        source: State::Leased,
        destination: State::Delayed,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        reason_class: None,
        required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolution_behavior: "probe both",
        notes: None,
    },
    TransitionDef {
        operation: Operation::Bury,
        source: State::Leased,
        destination: State::Dead,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        reason_class: Some(ReasonClass::ApplicationDefined),
        required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolution_behavior: "probe both",
        notes: None,
    },
    TransitionDef {
        operation: Operation::ReapExpiredToReady,
        source: State::Leased,
        destination: State::Ready,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        reason_class: None,
        required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolution_behavior: "probe both",
        notes: Some("attempt < maximum_attempts"),
    },
    TransitionDef {
        operation: Operation::ReapExpiredToDead,
        source: State::Leased,
        destination: State::Dead,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        reason_class: Some(ReasonClass::AttemptsExhausted),
        required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolution_behavior: "probe both",
        notes: Some("attempt >= maximum_attempts"),
    },
    TransitionDef {
        operation: Operation::Quarantine,
        source: State::Active,
        destination: State::Quarantine,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        reason_class: Some(ReasonClass::Corruption),
        required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolution_behavior: "probe both",
        notes: Some("raw bytes preserved"),
    },
];

pub const EXCEPTIONS: &[ExceptionDef] = &[
    ExceptionDef {
        name: ExceptionName::ReceiptCompaction,
        description: "Terminal full-job receipt replaced by byte-deterministic compact receipt at same pathname",
        uses_replacing_rename: true,
    },
    ExceptionDef {
        name: ExceptionName::WallWatermarkAdvancement,
        description: "Monotone wall-watermark record replaced under exclusive OFD lock",
        uses_replacing_rename: true,
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
        assert!(TRANSITIONS
            .iter()
            .all(|transition| !transition.resolution_behavior.is_empty()));
        assert!(TRANSITIONS.iter().all(|transition| {
            transition.before_linearization_failure == FailureOutcome::NotCommitted
                && transition.after_linearization_failure == FailureOutcome::OutcomeUnknown
        }));
        assert!(EXCEPTIONS
            .iter()
            .all(|exception| exception.uses_replacing_rename));
        assert!(REENTRY.iter().all(|reentry| reentry.creates_new_identity));
    }

    #[test]
    fn claim_projects_complete_semantics() {
        let claim = TRANSITIONS
            .iter()
            .find(|transition| transition.operation == Operation::Claim)
            .unwrap();
        assert_eq!(claim.source, State::Ready);
        assert_eq!(claim.destination, State::Leased);
        assert_eq!(claim.attempt_change, AttemptChange::Increment);
        assert_eq!(claim.generation_change, GenerationChange::Increment);
        assert_eq!(claim.token_change, TokenChange::New);
        assert_eq!(claim.reason_class, None);
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
        assert!(claim.resolution_behavior.contains("both"));
        assert_eq!(claim.notes, None);
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
        assert_eq!(reap.notes, Some("attempt >= maximum_attempts"));
        assert_eq!(EXCEPTIONS[0].name, ExceptionName::ReceiptCompaction);
        assert_eq!(REENTRY[0].name, ReentryName::RequeueDead);
        assert_eq!(REENTRY[0].source, State::Dead);
    }
}
