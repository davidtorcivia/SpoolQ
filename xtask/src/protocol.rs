use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs;
use std::path::Path;

pub(crate) mod render;
mod schema;

const SPEC: &str = "spec/state-machine.json";

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct StateMachineSpec {
    transitions: Vec<Transition>,
    exceptions: Vec<Exception>,
    reentry: Vec<Reentry>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Transition {
    operation: Operation,
    source: State,
    destination: State,
    generation_change: GenerationChange,
    attempt_change: AttemptChange,
    token_change: TokenChange,
    reason_class: Nullable<ReasonClass>,
    required_syncs: Vec<SyncStep>,
    linearization: LinearizationPrimitive,
    before_linearization_failure: FailureOutcome,
    after_linearization_failure: FailureOutcome,
    resolution_behavior: String,
    notes: Nullable<String>,
}

#[derive(Clone, Deserialize)]
#[serde(untagged)]
enum Nullable<T> {
    Value(T),
    Null,
}

#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Operation {
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

#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "snake_case")]
enum State {
    Hidden,
    Ready,
    Leased,
    Delayed,
    Dead,
    Receipt,
    Quarantine,
    Active,
}

#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "snake_case")]
enum GenerationChange {
    Zero,
    Increment,
    IncrementOrSame,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum AttemptChange {
    Zero,
    Increment,
    Unchanged,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum TokenChange {
    None,
    New,
    Same,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum ReasonClass {
    AttemptsExhausted,
    ApplicationDefined,
    Corruption,
}

#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "snake_case")]
enum SyncStep {
    #[serde(rename = "file_fsync")]
    File,
    #[serde(rename = "destination_dir_fsync")]
    DestinationDir,
    #[serde(rename = "source_dir_fsync")]
    SourceDir,
    #[serde(rename = "same_or_destination_dir_fsync")]
    SameOrDestinationDir,
}

#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "snake_case")]
enum LinearizationPrimitive {
    PublishNoreplace,
    RenameNoreplace,
    RenameReplace,
}

#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "snake_case")]
enum FailureOutcome {
    NotCommitted,
    OutcomeUnknown,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Exception {
    name: ExceptionName,
    description: String,
    mutation_class: MutationClass,
    linearization: LinearizationPrimitive,
    required_syncs: Vec<SyncStep>,
    before_linearization_failure: FailureOutcome,
    after_linearization_failure: FailureOutcome,
}

#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "snake_case")]
enum MutationClass {
    NoOverwriteMove,
    ReplacingMove,
    Publication,
    Unlink,
    InPlaceReadOnlyBarrier,
}

#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "snake_case")]
enum ExceptionName {
    ReceiptCompaction,
    WallWatermarkAdvancement,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reentry {
    name: ReentryName,
    source: State,
    description: String,
    creates_new_identity: bool,
}

#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "snake_case")]
enum ReentryName {
    RequeueDead,
    RequeueQuarantine,
}

struct TransitionInvariant {
    source: State,
    destination: State,
    generation_change: GenerationChange,
    attempt_change: AttemptChange,
    token_change: TokenChange,
    reason_class: Option<ReasonClass>,
    required_syncs: &'static [SyncStep],
}

struct ExceptionInvariant {
    mutation_class: MutationClass,
    linearization: LinearizationPrimitive,
    required_syncs: &'static [SyncStep],
}

fn load_spec(root: &Path) -> Result<(StateMachineSpec, String), String> {
    schema::validate_schema(root)?;
    let bytes =
        fs::read(root.join(SPEC)).map_err(|error| format!("cannot read {SPEC}: {error}"))?;
    let spec: StateMachineSpec =
        serde_json::from_slice(&bytes).map_err(|error| format!("cannot parse {SPEC}: {error}"))?;
    validate_spec(&spec)?;
    let digest = format!("{:x}", Sha256::digest(bytes));
    Ok((spec, digest))
}

fn validate_spec(spec: &StateMachineSpec) -> Result<(), String> {
    if spec.transitions.is_empty() {
        return Err("state-machine spec has no transitions".into());
    }

    let mut operation_names = HashSet::new();
    for transition in &spec.transitions {
        if !operation_names.insert(transition.operation) {
            return Err(format!(
                "duplicate transition operation: {}",
                transition.operation.as_str()
            ));
        }
        if !transition.destination.is_destination() {
            return Err(format!(
                "transition {} has unknown destination state {}",
                transition.operation.as_str(),
                transition.destination.as_str()
            ));
        }
        if transition.required_syncs.is_empty() {
            return Err(format!(
                "transition {} has no required syncs",
                transition.operation.as_str()
            ));
        }
        let mut syncs = HashSet::new();
        for sync in &transition.required_syncs {
            if !syncs.insert(*sync) {
                return Err(format!(
                    "transition {} contains duplicate sync {}",
                    transition.operation.as_str(),
                    sync.as_str()
                ));
            }
        }
        if transition.resolution_behavior.trim().is_empty() {
            return Err(format!(
                "transition {} has empty resolution behavior",
                transition.operation.as_str()
            ));
        }
        if matches!(
            &transition.notes,
            Nullable::Value(value) if value.trim().is_empty()
        ) {
            return Err(format!(
                "transition {} has empty notes",
                transition.operation.as_str()
            ));
        }
        validate_transition_invariant(transition)?;
    }
    for operation in Operation::ALL {
        if !operation_names.contains(&operation) {
            return Err(format!(
                "state-machine spec is missing transition operation: {}",
                operation.as_str()
            ));
        }
    }

    let mut exception_names = HashSet::new();
    for exception in &spec.exceptions {
        if !exception_names.insert(exception.name) {
            return Err(format!("duplicate exception: {}", exception.name.as_str()));
        }
        if exception.description.trim().is_empty() {
            return Err(format!(
                "exception {} has no description",
                exception.name.as_str()
            ));
        }
        let mut syncs = HashSet::new();
        for sync in &exception.required_syncs {
            if !syncs.insert(*sync) {
                return Err(format!(
                    "exception {} contains duplicate sync {}",
                    exception.name.as_str(),
                    sync.as_str()
                ));
            }
        }
        validate_exception_invariant(exception)?;
    }
    for exception in ExceptionName::ALL {
        if !exception_names.contains(&exception) {
            return Err(format!(
                "state-machine spec is missing exception: {}",
                exception.as_str()
            ));
        }
    }

    let mut reentry_names = HashSet::new();
    for reentry in &spec.reentry {
        if !reentry_names.insert(reentry.name) {
            return Err(format!("duplicate reentry: {}", reentry.name.as_str()));
        }
        if reentry.source != reentry.name.expected_source() {
            return Err(format!(
                "reentry {} has source state {}; expected {}",
                reentry.name.as_str(),
                reentry.source.as_str(),
                reentry.name.expected_source().as_str()
            ));
        }
        if reentry.description.trim().is_empty() {
            return Err(format!(
                "reentry {} has no description",
                reentry.name.as_str()
            ));
        }
        if !reentry.creates_new_identity {
            return Err(format!(
                "reentry {} must create a new identity",
                reentry.name.as_str()
            ));
        }
    }
    for reentry in ReentryName::ALL {
        if !reentry_names.contains(&reentry) {
            return Err(format!(
                "state-machine spec is missing reentry: {}",
                reentry.as_str()
            ));
        }
    }
    Ok(())
}

fn validate_exception_invariant(exception: &Exception) -> Result<(), String> {
    let expected = exception.name.invariant();
    if exception.mutation_class != expected.mutation_class {
        return Err(format!(
            "exception {} has mutation class {}; expected {}",
            exception.name.as_str(),
            exception.mutation_class.as_str(),
            expected.mutation_class.as_str()
        ));
    }
    if exception.linearization != expected.linearization {
        return Err(format!(
            "exception {} has linearization {}; expected {}",
            exception.name.as_str(),
            exception.linearization.as_str(),
            expected.linearization.as_str()
        ));
    }
    if exception.before_linearization_failure != FailureOutcome::NotCommitted {
        return Err(format!(
            "exception {} must classify pre-linearization failure as not_committed",
            exception.name.as_str()
        ));
    }
    if exception.after_linearization_failure != FailureOutcome::OutcomeUnknown {
        return Err(format!(
            "exception {} must classify post-linearization failure as outcome_unknown",
            exception.name.as_str()
        ));
    }
    if exception.required_syncs != expected.required_syncs {
        let actual = exception
            .required_syncs
            .iter()
            .copied()
            .map(SyncStep::as_str)
            .collect::<Vec<_>>();
        let expected = expected
            .required_syncs
            .iter()
            .copied()
            .map(SyncStep::as_str)
            .collect::<Vec<_>>();
        return Err(format!(
            "exception {} has syncs {actual:?}; expected {expected:?}",
            exception.name.as_str()
        ));
    }
    Ok(())
}

fn validate_transition_invariant(transition: &Transition) -> Result<(), String> {
    let expected = transition.operation.invariant();
    let expected_linearization = transition.operation.linearization();
    if transition.linearization != expected_linearization {
        return Err(format!(
            "transition {} has linearization {}; expected {}",
            transition.operation.as_str(),
            transition.linearization.as_str(),
            expected_linearization.as_str()
        ));
    }
    if transition.before_linearization_failure != FailureOutcome::NotCommitted {
        return Err(format!(
            "transition {} must classify pre-linearization failure as not_committed",
            transition.operation.as_str()
        ));
    }
    if transition.after_linearization_failure != FailureOutcome::OutcomeUnknown {
        return Err(format!(
            "transition {} must classify post-linearization failure as outcome_unknown",
            transition.operation.as_str()
        ));
    }
    if transition.source != expected.source {
        return Err(format!(
            "transition {} has source {}; expected {}",
            transition.operation.as_str(),
            transition.source.as_str(),
            expected.source.as_str()
        ));
    }
    if transition.destination != expected.destination {
        return Err(format!(
            "transition {} has destination {}; expected {}",
            transition.operation.as_str(),
            transition.destination.as_str(),
            expected.destination.as_str()
        ));
    }
    if transition.generation_change != expected.generation_change {
        return Err(format!(
            "transition {} has generation change {}; expected {}",
            transition.operation.as_str(),
            transition.generation_change.as_str(),
            expected.generation_change.as_str()
        ));
    }
    if transition.attempt_change != expected.attempt_change {
        return Err(format!(
            "transition {} has attempt change {}; expected {}",
            transition.operation.as_str(),
            transition.attempt_change.as_str(),
            expected.attempt_change.as_str()
        ));
    }
    if transition.token_change != expected.token_change {
        return Err(format!(
            "transition {} has token change {}; expected {}",
            transition.operation.as_str(),
            transition.token_change.as_str(),
            expected.token_change.as_str()
        ));
    }
    let reason_class = match &transition.reason_class {
        Nullable::Value(reason) => Some(*reason),
        Nullable::Null => None,
    };
    if reason_class != expected.reason_class {
        return Err(format!(
            "transition {} has reason class {}; expected {}",
            transition.operation.as_str(),
            reason_class.map_or("none", ReasonClass::as_str),
            expected.reason_class.map_or("none", ReasonClass::as_str)
        ));
    }
    if transition.required_syncs != expected.required_syncs {
        let actual = transition
            .required_syncs
            .iter()
            .copied()
            .map(SyncStep::as_str)
            .collect::<Vec<_>>();
        let expected = expected
            .required_syncs
            .iter()
            .copied()
            .map(SyncStep::as_str)
            .collect::<Vec<_>>();
        return Err(format!(
            "transition {} has syncs {actual:?}; expected {expected:?}",
            transition.operation.as_str()
        ));
    }
    Ok(())
}

impl GenerationChange {
    const ALL: [Self; 3] = [Self::Zero, Self::Increment, Self::IncrementOrSame];

    fn as_str(self) -> &'static str {
        match self {
            Self::Zero => "zero",
            Self::Increment => "increment",
            Self::IncrementOrSame => "increment_or_same",
        }
    }

    fn rust_name(self) -> &'static str {
        match self {
            Self::Zero => "Zero",
            Self::Increment => "Increment",
            Self::IncrementOrSame => "IncrementOrSame",
        }
    }
}

impl Operation {
    const ALL: [Self; 13] = [
        Self::EnqueueImmediate,
        Self::EnqueueDelayed,
        Self::Promote,
        Self::Claim,
        Self::ExhaustedReadyCleanup,
        Self::Renew,
        Self::Acknowledge,
        Self::RetryNow,
        Self::RetryLater,
        Self::Bury,
        Self::ReapExpiredToReady,
        Self::ReapExpiredToDead,
        Self::Quarantine,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::EnqueueImmediate => "enqueue_immediate",
            Self::EnqueueDelayed => "enqueue_delayed",
            Self::Promote => "promote",
            Self::Claim => "claim",
            Self::ExhaustedReadyCleanup => "exhausted_ready_cleanup",
            Self::Renew => "renew",
            Self::Acknowledge => "acknowledge",
            Self::RetryNow => "retry_now",
            Self::RetryLater => "retry_later",
            Self::Bury => "bury",
            Self::ReapExpiredToReady => "reap_expired_to_ready",
            Self::ReapExpiredToDead => "reap_expired_to_dead",
            Self::Quarantine => "quarantine",
        }
    }

    fn rust_name(self) -> &'static str {
        match self {
            Self::EnqueueImmediate => "EnqueueImmediate",
            Self::EnqueueDelayed => "EnqueueDelayed",
            Self::Promote => "Promote",
            Self::Claim => "Claim",
            Self::ExhaustedReadyCleanup => "ExhaustedReadyCleanup",
            Self::Renew => "Renew",
            Self::Acknowledge => "Acknowledge",
            Self::RetryNow => "RetryNow",
            Self::RetryLater => "RetryLater",
            Self::Bury => "Bury",
            Self::ReapExpiredToReady => "ReapExpiredToReady",
            Self::ReapExpiredToDead => "ReapExpiredToDead",
            Self::Quarantine => "Quarantine",
        }
    }

    fn linearization(self) -> LinearizationPrimitive {
        match self {
            Self::EnqueueImmediate | Self::EnqueueDelayed => {
                LinearizationPrimitive::PublishNoreplace
            }
            Self::Promote
            | Self::Claim
            | Self::ExhaustedReadyCleanup
            | Self::Renew
            | Self::Acknowledge
            | Self::RetryNow
            | Self::RetryLater
            | Self::Bury
            | Self::ReapExpiredToReady
            | Self::ReapExpiredToDead
            | Self::Quarantine => LinearizationPrimitive::RenameNoreplace,
        }
    }

    fn invariant(self) -> TransitionInvariant {
        use AttemptChange::{Increment as AttemptIncrement, Unchanged, Zero as AttemptZero};
        use GenerationChange::{Increment as GenerationIncrement, Zero as GenerationZero};
        use ReasonClass::{ApplicationDefined, AttemptsExhausted, Corruption};
        use State::{Active, Dead, Delayed, Hidden, Leased, Quarantine, Ready, Receipt};
        use SyncStep::{DestinationDir, File, SameOrDestinationDir, SourceDir};
        use TokenChange::{New, None as NoToken, Same};

        match self {
            Self::EnqueueImmediate => TransitionInvariant {
                source: Hidden,
                destination: Ready,
                generation_change: GenerationZero,
                attempt_change: AttemptZero,
                token_change: NoToken,
                reason_class: None,
                required_syncs: &[File, DestinationDir],
            },
            Self::EnqueueDelayed => TransitionInvariant {
                source: Hidden,
                destination: Delayed,
                generation_change: GenerationZero,
                attempt_change: AttemptZero,
                token_change: NoToken,
                reason_class: None,
                required_syncs: &[File, DestinationDir],
            },
            Self::Promote => TransitionInvariant {
                source: Delayed,
                destination: Ready,
                generation_change: GenerationIncrement,
                attempt_change: Unchanged,
                token_change: NoToken,
                reason_class: None,
                required_syncs: &[DestinationDir, SourceDir],
            },
            Self::Claim => TransitionInvariant {
                source: Ready,
                destination: Leased,
                generation_change: GenerationIncrement,
                attempt_change: AttemptIncrement,
                token_change: New,
                reason_class: None,
                required_syncs: &[DestinationDir, SourceDir],
            },
            Self::ExhaustedReadyCleanup => TransitionInvariant {
                source: Ready,
                destination: Dead,
                generation_change: GenerationIncrement,
                attempt_change: Unchanged,
                token_change: NoToken,
                reason_class: Some(AttemptsExhausted),
                required_syncs: &[DestinationDir, SourceDir],
            },
            Self::Renew => TransitionInvariant {
                source: Leased,
                destination: Leased,
                generation_change: GenerationIncrement,
                attempt_change: Unchanged,
                token_change: Same,
                reason_class: None,
                required_syncs: &[SameOrDestinationDir],
            },
            Self::Acknowledge => TransitionInvariant {
                source: Leased,
                destination: Receipt,
                generation_change: GenerationIncrement,
                attempt_change: Unchanged,
                token_change: Same,
                reason_class: None,
                required_syncs: &[DestinationDir, SourceDir],
            },
            Self::RetryNow => TransitionInvariant {
                source: Leased,
                destination: Ready,
                generation_change: GenerationIncrement,
                attempt_change: Unchanged,
                token_change: NoToken,
                reason_class: None,
                required_syncs: &[DestinationDir, SourceDir],
            },
            Self::RetryLater => TransitionInvariant {
                source: Leased,
                destination: Delayed,
                generation_change: GenerationIncrement,
                attempt_change: Unchanged,
                token_change: NoToken,
                reason_class: None,
                required_syncs: &[DestinationDir, SourceDir],
            },
            Self::Bury => TransitionInvariant {
                source: Leased,
                destination: Dead,
                generation_change: GenerationIncrement,
                attempt_change: Unchanged,
                token_change: NoToken,
                reason_class: Some(ApplicationDefined),
                required_syncs: &[DestinationDir, SourceDir],
            },
            Self::ReapExpiredToReady => TransitionInvariant {
                source: Leased,
                destination: Ready,
                generation_change: GenerationIncrement,
                attempt_change: Unchanged,
                token_change: NoToken,
                reason_class: None,
                required_syncs: &[DestinationDir, SourceDir],
            },
            Self::ReapExpiredToDead => TransitionInvariant {
                source: Leased,
                destination: Dead,
                generation_change: GenerationIncrement,
                attempt_change: Unchanged,
                token_change: NoToken,
                reason_class: Some(AttemptsExhausted),
                required_syncs: &[DestinationDir, SourceDir],
            },
            Self::Quarantine => TransitionInvariant {
                source: Active,
                destination: Quarantine,
                generation_change: GenerationIncrement,
                attempt_change: Unchanged,
                token_change: NoToken,
                reason_class: Some(Corruption),
                required_syncs: &[DestinationDir, SourceDir],
            },
        }
    }
}

impl State {
    const ALL: [Self; 8] = [
        Self::Hidden,
        Self::Ready,
        Self::Leased,
        Self::Delayed,
        Self::Dead,
        Self::Receipt,
        Self::Quarantine,
        Self::Active,
    ];
    const DESTINATIONS: [Self; 6] = [
        Self::Ready,
        Self::Leased,
        Self::Delayed,
        Self::Dead,
        Self::Receipt,
        Self::Quarantine,
    ];
    const REENTRY_SOURCES: [Self; 2] = [Self::Dead, Self::Quarantine];

    fn as_str(self) -> &'static str {
        match self {
            Self::Hidden => "hidden",
            Self::Ready => "ready",
            Self::Leased => "leased",
            Self::Delayed => "delayed",
            Self::Dead => "dead",
            Self::Receipt => "receipt",
            Self::Quarantine => "quarantine",
            Self::Active => "active",
        }
    }

    fn rust_name(self) -> &'static str {
        match self {
            Self::Hidden => "Hidden",
            Self::Ready => "Ready",
            Self::Leased => "Leased",
            Self::Delayed => "Delayed",
            Self::Dead => "Dead",
            Self::Receipt => "Receipt",
            Self::Quarantine => "Quarantine",
            Self::Active => "Active",
        }
    }

    fn is_destination(self) -> bool {
        matches!(
            self,
            Self::Ready
                | Self::Leased
                | Self::Delayed
                | Self::Dead
                | Self::Receipt
                | Self::Quarantine
        )
    }
}

impl AttemptChange {
    const ALL: [Self; 3] = [Self::Zero, Self::Increment, Self::Unchanged];

    fn as_str(self) -> &'static str {
        match self {
            Self::Zero => "zero",
            Self::Increment => "increment",
            Self::Unchanged => "unchanged",
        }
    }

    fn rust_name(self) -> &'static str {
        match self {
            Self::Zero => "Zero",
            Self::Increment => "Increment",
            Self::Unchanged => "Unchanged",
        }
    }
}

impl TokenChange {
    const ALL: [Self; 3] = [Self::None, Self::New, Self::Same];

    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::New => "new",
            Self::Same => "same",
        }
    }

    fn rust_name(self) -> &'static str {
        match self {
            Self::None => "None",
            Self::New => "New",
            Self::Same => "Same",
        }
    }
}

impl ReasonClass {
    const ALL: [Self; 3] = [
        Self::AttemptsExhausted,
        Self::ApplicationDefined,
        Self::Corruption,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::AttemptsExhausted => "attempts_exhausted",
            Self::ApplicationDefined => "application_defined",
            Self::Corruption => "corruption",
        }
    }

    fn rust_name(self) -> &'static str {
        match self {
            Self::AttemptsExhausted => "AttemptsExhausted",
            Self::ApplicationDefined => "ApplicationDefined",
            Self::Corruption => "Corruption",
        }
    }
}

impl SyncStep {
    const ALL: [Self; 4] = [
        Self::File,
        Self::DestinationDir,
        Self::SourceDir,
        Self::SameOrDestinationDir,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::File => "file_fsync",
            Self::DestinationDir => "destination_dir_fsync",
            Self::SourceDir => "source_dir_fsync",
            Self::SameOrDestinationDir => "same_or_destination_dir_fsync",
        }
    }

    fn rust_name(self) -> &'static str {
        match self {
            Self::File => "File",
            Self::DestinationDir => "DestinationDirectory",
            Self::SourceDir => "SourceDirectory",
            Self::SameOrDestinationDir => "SameOrDestinationDirectory",
        }
    }
}

impl LinearizationPrimitive {
    const ALL: [Self; 3] = [
        Self::PublishNoreplace,
        Self::RenameNoreplace,
        Self::RenameReplace,
    ];
    const TRANSITIONS: [Self; 2] = [Self::PublishNoreplace, Self::RenameNoreplace];

    fn as_str(self) -> &'static str {
        match self {
            Self::PublishNoreplace => "publish_noreplace",
            Self::RenameNoreplace => "rename_noreplace",
            Self::RenameReplace => "rename_replace",
        }
    }

    fn rust_name(self) -> &'static str {
        match self {
            Self::PublishNoreplace => "PublishNoreplace",
            Self::RenameNoreplace => "RenameNoreplace",
            Self::RenameReplace => "RenameReplace",
        }
    }
}

impl MutationClass {
    const ALL: [Self; 5] = [
        Self::NoOverwriteMove,
        Self::ReplacingMove,
        Self::Publication,
        Self::Unlink,
        Self::InPlaceReadOnlyBarrier,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::NoOverwriteMove => "no_overwrite_move",
            Self::ReplacingMove => "replacing_move",
            Self::Publication => "publication",
            Self::Unlink => "unlink",
            Self::InPlaceReadOnlyBarrier => "in_place_read_only_barrier",
        }
    }

    fn rust_name(self) -> &'static str {
        match self {
            Self::NoOverwriteMove => "NoOverwriteMove",
            Self::ReplacingMove => "ReplacingMove",
            Self::Publication => "Publication",
            Self::Unlink => "Unlink",
            Self::InPlaceReadOnlyBarrier => "InPlaceReadOnlyBarrier",
        }
    }
}

impl FailureOutcome {
    const ALL: [Self; 2] = [Self::NotCommitted, Self::OutcomeUnknown];

    fn as_str(self) -> &'static str {
        match self {
            Self::NotCommitted => "not_committed",
            Self::OutcomeUnknown => "outcome_unknown",
        }
    }

    fn rust_name(self) -> &'static str {
        match self {
            Self::NotCommitted => "NotCommitted",
            Self::OutcomeUnknown => "OutcomeUnknown",
        }
    }
}

impl ExceptionName {
    const ALL: [Self; 2] = [Self::ReceiptCompaction, Self::WallWatermarkAdvancement];

    fn as_str(self) -> &'static str {
        match self {
            Self::ReceiptCompaction => "receipt_compaction",
            Self::WallWatermarkAdvancement => "wall_watermark_advancement",
        }
    }

    fn rust_name(self) -> &'static str {
        match self {
            Self::ReceiptCompaction => "ReceiptCompaction",
            Self::WallWatermarkAdvancement => "WallWatermarkAdvancement",
        }
    }

    fn invariant(self) -> ExceptionInvariant {
        ExceptionInvariant {
            mutation_class: MutationClass::ReplacingMove,
            linearization: LinearizationPrimitive::RenameReplace,
            required_syncs: &[SyncStep::File, SyncStep::SameOrDestinationDir],
        }
    }
}

impl ReentryName {
    const ALL: [Self; 2] = [Self::RequeueDead, Self::RequeueQuarantine];

    fn as_str(self) -> &'static str {
        match self {
            Self::RequeueDead => "requeue_dead",
            Self::RequeueQuarantine => "requeue_quarantine",
        }
    }

    fn rust_name(self) -> &'static str {
        match self {
            Self::RequeueDead => "RequeueDead",
            Self::RequeueQuarantine => "RequeueQuarantine",
        }
    }

    fn expected_source(self) -> State {
        match self {
            Self::RequeueDead => State::Dead,
            Self::RequeueQuarantine => State::Quarantine,
        }
    }
}

#[cfg(test)]
mod tests;
