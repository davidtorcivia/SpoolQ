-------------------------- MODULE SteadQProtocol --------------------------
(* Auto-generated from spec/state-machine.json. Do not edit by hand. *)
(* Source SHA-256: 8eba1a6b7a3d72aca483b07f52bbb1c97bee9828a0b338cdbdaf02dbfdf1ba92 *)

ProtocolIRIdentity == "steadq-state-machine"
ProtocolIRVersion == 3

OperationEnqueueImmediate == "enqueue_immediate"
OperationEnqueueDelayed == "enqueue_delayed"
OperationPromote == "promote"
OperationClaim == "claim"
OperationExhaustedReadyCleanup == "exhausted_ready_cleanup"
OperationRenew == "renew"
OperationAcknowledge == "acknowledge"
OperationRetryNow == "retry_now"
OperationRetryLater == "retry_later"
OperationBury == "bury"
OperationReapExpiredToReady == "reap_expired_to_ready"
OperationReapExpiredToDead == "reap_expired_to_dead"
OperationQuarantine == "quarantine"
ProtocolOperations == {OperationEnqueueImmediate, OperationEnqueueDelayed, OperationPromote, OperationClaim, OperationExhaustedReadyCleanup, OperationRenew, OperationAcknowledge, OperationRetryNow, OperationRetryLater, OperationBury, OperationReapExpiredToReady, OperationReapExpiredToDead, OperationQuarantine}

StateHidden == "hidden"
StateReady == "ready"
StateLeased == "leased"
StateDelayed == "delayed"
StateDead == "dead"
StateReceipt == "receipt"
StateQuarantine == "quarantine"
StateActive == "active"
ProtocolStates == {StateHidden, StateReady, StateLeased, StateDelayed, StateDead, StateReceipt, StateQuarantine, StateActive}

ObjectKindFullJob == "full_job"
ObjectKindFullReceipt == "full_receipt"
ObjectKindCompactReceipt == "compact_receipt"
ObjectKindRawObject == "raw_object"
ObjectKindWatermarkRecord == "watermark_record"
ProtocolObjectKinds == {ObjectKindFullJob, ObjectKindFullReceipt, ObjectKindCompactReceipt, ObjectKindRawObject, ObjectKindWatermarkRecord}

GenerationChangeZero == "zero"
GenerationChangeIncrement == "increment"
GenerationChangeIncrementOrSame == "increment_or_same"
ProtocolGenerationChanges == {GenerationChangeZero, GenerationChangeIncrement, GenerationChangeIncrementOrSame}

AttemptChangeZero == "zero"
AttemptChangeIncrement == "increment"
AttemptChangeUnchanged == "unchanged"
ProtocolAttemptChanges == {AttemptChangeZero, AttemptChangeIncrement, AttemptChangeUnchanged}

TokenChangeNone == "none"
TokenChangeNew == "new"
TokenChangeSame == "same"
ProtocolTokenChanges == {TokenChangeNone, TokenChangeNew, TokenChangeSame}

ReasonClassAttemptsExhausted == "attempts_exhausted"
ReasonClassApplicationDefined == "application_defined"
ReasonClassCorruption == "corruption"
ProtocolReasonClasses == {ReasonClassAttemptsExhausted, ReasonClassApplicationDefined, ReasonClassCorruption}

NoReasonClass == "none"

ClockRequirementNone == "none"
ClockRequirementAuthenticatedWallFloor == "authenticated_wall_floor"
ClockRequirementBoottimeAndAuthenticatedWallFloor == "boottime_and_authenticated_wall_floor"
ClockRequirementLeaseExpirationEvidence == "lease_expiration_evidence"
ClockRequirementLeaseExpirationEvidenceAndAuthenticatedWallFloor == "lease_expiration_evidence_and_authenticated_wall_floor"
ProtocolClockRequirements == {ClockRequirementNone, ClockRequirementAuthenticatedWallFloor, ClockRequirementBoottimeAndAuthenticatedWallFloor, ClockRequirementLeaseExpirationEvidence, ClockRequirementLeaseExpirationEvidenceAndAuthenticatedWallFloor}

SyncStepFile == "file_fsync"
SyncStepDestinationDirectory == "destination_dir_fsync"
SyncStepSourceDirectory == "source_dir_fsync"
SyncStepSameOrDestinationDirectory == "same_or_destination_dir_fsync"
SyncStepSourceDirectoryIfDistinct == "source_dir_fsync_if_distinct"
ProtocolSyncSteps == {SyncStepFile, SyncStepDestinationDirectory, SyncStepSourceDirectory, SyncStepSameOrDestinationDirectory, SyncStepSourceDirectoryIfDistinct}

LinearizationPublishNoreplace == "publish_noreplace"
LinearizationRenameNoreplace == "rename_noreplace"
LinearizationRenameReplace == "rename_replace"
LinearizationUnlink == "unlink"
ProtocolLinearizationPrimitives == {LinearizationPublishNoreplace, LinearizationRenameNoreplace, LinearizationRenameReplace, LinearizationUnlink}

FailureOutcomeNotCommitted == "not_committed"
FailureOutcomeOutcomeUnknown == "outcome_unknown"
ProtocolFailureOutcomes == {FailureOutcomeNotCommitted, FailureOutcomeOutcomeUnknown}

ResolverProbeTopologyDestinationOnly == "destination_only"
ResolverProbeTopologySourceAndDestination == "source_and_destination"
ResolverProbeTopologyReceiptCandidatesAndSource == "receipt_candidates_and_source"
ResolverProbeTopologySourcePresence == "source_presence"
ProtocolResolverProbeTopologies == {ResolverProbeTopologyDestinationOnly, ResolverProbeTopologySourceAndDestination, ResolverProbeTopologyReceiptCandidatesAndSource, ResolverProbeTopologySourcePresence}

TransitionQualificationNone == "none"
TransitionQualificationAttemptsRemaining == "attempts_remaining"
TransitionQualificationAttemptsExhausted == "attempts_exhausted"
TransitionQualificationRawBytesPreserved == "raw_bytes_preserved"
TransitionQualificationReceiptBucketEndPlusRetentionNotAfterWallFloor == "receipt_bucket_end_plus_retention_not_after_wall_floor"
ProtocolTransitionQualifications == {TransitionQualificationNone, TransitionQualificationAttemptsRemaining, TransitionQualificationAttemptsExhausted, TransitionQualificationRawBytesPreserved, TransitionQualificationReceiptBucketEndPlusRetentionNotAfterWallFloor}

MutationClassNoOverwriteMove == "no_overwrite_move"
MutationClassReplacingMove == "replacing_move"
MutationClassPublication == "publication"
MutationClassUnlink == "unlink"
MutationClassInPlaceReadOnlyBarrier == "in_place_read_only_barrier"
ProtocolMutationClasses == {MutationClassNoOverwriteMove, MutationClassReplacingMove, MutationClassPublication, MutationClassUnlink, MutationClassInPlaceReadOnlyBarrier}

ExceptionReceiptCompaction == "receipt_compaction"
ExceptionWallWatermarkAdvancement == "wall_watermark_advancement"
ProtocolExceptionNames == {ExceptionReceiptCompaction, ExceptionWallWatermarkAdvancement}

UnlinkFullReceiptRetentionDeletion == "full_receipt_retention_deletion"
UnlinkCompactReceiptRetentionDeletion == "compact_receipt_retention_deletion"
ProtocolUnlinkNames == {UnlinkFullReceiptRetentionDeletion, UnlinkCompactReceiptRetentionDeletion}

SourceAuthenticationNone == "none"
SourceAuthenticationStrictReceipt == "strict_receipt"
ProtocolSourceAuthentications == {SourceAuthenticationNone, SourceAuthenticationStrictReceipt}

ReentryRequeueDead == "requeue_dead"
ReentryRequeueQuarantine == "requeue_quarantine"
ProtocolReentryNames == {ReentryRequeueDead, ReentryRequeueQuarantine}

ProtocolTransitions == <<
    [operation |-> OperationEnqueueImmediate,
     source |-> StateHidden,
     sourceObjectKind |-> ObjectKindFullJob,
     destination |-> StateReady,
     destinationObjectKind |-> ObjectKindFullJob,
     generationChange |-> GenerationChangeZero,
     attemptChange |-> AttemptChangeZero,
     tokenChange |-> TokenChangeNone,
     reasonClass |-> NoReasonClass,
     clockRequirement |-> ClockRequirementAuthenticatedWallFloor,
     requiredSyncs |-> <<SyncStepFile, SyncStepDestinationDirectory>>,
     linearization |-> LinearizationPublishNoreplace,
     beforeLinearizationFailure |-> FailureOutcomeNotCommitted,
     afterLinearizationFailure |-> FailureOutcomeOutcomeUnknown,
     resolverProbeTopology |-> ResolverProbeTopologyDestinationOnly,
     qualification |-> TransitionQualificationNone],
    [operation |-> OperationEnqueueDelayed,
     source |-> StateHidden,
     sourceObjectKind |-> ObjectKindFullJob,
     destination |-> StateDelayed,
     destinationObjectKind |-> ObjectKindFullJob,
     generationChange |-> GenerationChangeZero,
     attemptChange |-> AttemptChangeZero,
     tokenChange |-> TokenChangeNone,
     reasonClass |-> NoReasonClass,
     clockRequirement |-> ClockRequirementAuthenticatedWallFloor,
     requiredSyncs |-> <<SyncStepFile, SyncStepDestinationDirectory>>,
     linearization |-> LinearizationPublishNoreplace,
     beforeLinearizationFailure |-> FailureOutcomeNotCommitted,
     afterLinearizationFailure |-> FailureOutcomeOutcomeUnknown,
     resolverProbeTopology |-> ResolverProbeTopologyDestinationOnly,
     qualification |-> TransitionQualificationNone],
    [operation |-> OperationPromote,
     source |-> StateDelayed,
     sourceObjectKind |-> ObjectKindFullJob,
     destination |-> StateReady,
     destinationObjectKind |-> ObjectKindFullJob,
     generationChange |-> GenerationChangeIncrement,
     attemptChange |-> AttemptChangeUnchanged,
     tokenChange |-> TokenChangeNone,
     reasonClass |-> NoReasonClass,
     clockRequirement |-> ClockRequirementAuthenticatedWallFloor,
     requiredSyncs |-> <<SyncStepDestinationDirectory, SyncStepSourceDirectory>>,
     linearization |-> LinearizationRenameNoreplace,
     beforeLinearizationFailure |-> FailureOutcomeNotCommitted,
     afterLinearizationFailure |-> FailureOutcomeOutcomeUnknown,
     resolverProbeTopology |-> ResolverProbeTopologySourceAndDestination,
     qualification |-> TransitionQualificationNone],
    [operation |-> OperationClaim,
     source |-> StateReady,
     sourceObjectKind |-> ObjectKindFullJob,
     destination |-> StateLeased,
     destinationObjectKind |-> ObjectKindFullJob,
     generationChange |-> GenerationChangeIncrement,
     attemptChange |-> AttemptChangeIncrement,
     tokenChange |-> TokenChangeNew,
     reasonClass |-> NoReasonClass,
     clockRequirement |-> ClockRequirementBoottimeAndAuthenticatedWallFloor,
     requiredSyncs |-> <<SyncStepDestinationDirectory, SyncStepSourceDirectory>>,
     linearization |-> LinearizationRenameNoreplace,
     beforeLinearizationFailure |-> FailureOutcomeNotCommitted,
     afterLinearizationFailure |-> FailureOutcomeOutcomeUnknown,
     resolverProbeTopology |-> ResolverProbeTopologySourceAndDestination,
     qualification |-> TransitionQualificationNone],
    [operation |-> OperationExhaustedReadyCleanup,
     source |-> StateReady,
     sourceObjectKind |-> ObjectKindFullJob,
     destination |-> StateDead,
     destinationObjectKind |-> ObjectKindFullJob,
     generationChange |-> GenerationChangeIncrement,
     attemptChange |-> AttemptChangeUnchanged,
     tokenChange |-> TokenChangeNone,
     reasonClass |-> ReasonClassAttemptsExhausted,
     clockRequirement |-> ClockRequirementAuthenticatedWallFloor,
     requiredSyncs |-> <<SyncStepDestinationDirectory, SyncStepSourceDirectory>>,
     linearization |-> LinearizationRenameNoreplace,
     beforeLinearizationFailure |-> FailureOutcomeNotCommitted,
     afterLinearizationFailure |-> FailureOutcomeOutcomeUnknown,
     resolverProbeTopology |-> ResolverProbeTopologySourceAndDestination,
     qualification |-> TransitionQualificationNone],
    [operation |-> OperationRenew,
     source |-> StateLeased,
     sourceObjectKind |-> ObjectKindFullJob,
     destination |-> StateLeased,
     destinationObjectKind |-> ObjectKindFullJob,
     generationChange |-> GenerationChangeIncrement,
     attemptChange |-> AttemptChangeUnchanged,
     tokenChange |-> TokenChangeSame,
     reasonClass |-> NoReasonClass,
     clockRequirement |-> ClockRequirementBoottimeAndAuthenticatedWallFloor,
     requiredSyncs |-> <<SyncStepSameOrDestinationDirectory, SyncStepSourceDirectoryIfDistinct>>,
     linearization |-> LinearizationRenameNoreplace,
     beforeLinearizationFailure |-> FailureOutcomeNotCommitted,
     afterLinearizationFailure |-> FailureOutcomeOutcomeUnknown,
     resolverProbeTopology |-> ResolverProbeTopologySourceAndDestination,
     qualification |-> TransitionQualificationNone],
    [operation |-> OperationAcknowledge,
     source |-> StateLeased,
     sourceObjectKind |-> ObjectKindFullJob,
     destination |-> StateReceipt,
     destinationObjectKind |-> ObjectKindFullReceipt,
     generationChange |-> GenerationChangeIncrement,
     attemptChange |-> AttemptChangeUnchanged,
     tokenChange |-> TokenChangeSame,
     reasonClass |-> NoReasonClass,
     clockRequirement |-> ClockRequirementAuthenticatedWallFloor,
     requiredSyncs |-> <<SyncStepDestinationDirectory, SyncStepSourceDirectory>>,
     linearization |-> LinearizationRenameNoreplace,
     beforeLinearizationFailure |-> FailureOutcomeNotCommitted,
     afterLinearizationFailure |-> FailureOutcomeOutcomeUnknown,
     resolverProbeTopology |-> ResolverProbeTopologyReceiptCandidatesAndSource,
     qualification |-> TransitionQualificationNone],
    [operation |-> OperationRetryNow,
     source |-> StateLeased,
     sourceObjectKind |-> ObjectKindFullJob,
     destination |-> StateReady,
     destinationObjectKind |-> ObjectKindFullJob,
     generationChange |-> GenerationChangeIncrement,
     attemptChange |-> AttemptChangeUnchanged,
     tokenChange |-> TokenChangeNone,
     reasonClass |-> NoReasonClass,
     clockRequirement |-> ClockRequirementNone,
     requiredSyncs |-> <<SyncStepDestinationDirectory, SyncStepSourceDirectory>>,
     linearization |-> LinearizationRenameNoreplace,
     beforeLinearizationFailure |-> FailureOutcomeNotCommitted,
     afterLinearizationFailure |-> FailureOutcomeOutcomeUnknown,
     resolverProbeTopology |-> ResolverProbeTopologySourceAndDestination,
     qualification |-> TransitionQualificationNone],
    [operation |-> OperationRetryLater,
     source |-> StateLeased,
     sourceObjectKind |-> ObjectKindFullJob,
     destination |-> StateDelayed,
     destinationObjectKind |-> ObjectKindFullJob,
     generationChange |-> GenerationChangeIncrement,
     attemptChange |-> AttemptChangeUnchanged,
     tokenChange |-> TokenChangeNone,
     reasonClass |-> NoReasonClass,
     clockRequirement |-> ClockRequirementAuthenticatedWallFloor,
     requiredSyncs |-> <<SyncStepDestinationDirectory, SyncStepSourceDirectory>>,
     linearization |-> LinearizationRenameNoreplace,
     beforeLinearizationFailure |-> FailureOutcomeNotCommitted,
     afterLinearizationFailure |-> FailureOutcomeOutcomeUnknown,
     resolverProbeTopology |-> ResolverProbeTopologySourceAndDestination,
     qualification |-> TransitionQualificationNone],
    [operation |-> OperationBury,
     source |-> StateLeased,
     sourceObjectKind |-> ObjectKindFullJob,
     destination |-> StateDead,
     destinationObjectKind |-> ObjectKindFullJob,
     generationChange |-> GenerationChangeIncrement,
     attemptChange |-> AttemptChangeUnchanged,
     tokenChange |-> TokenChangeNone,
     reasonClass |-> ReasonClassApplicationDefined,
     clockRequirement |-> ClockRequirementAuthenticatedWallFloor,
     requiredSyncs |-> <<SyncStepDestinationDirectory, SyncStepSourceDirectory>>,
     linearization |-> LinearizationRenameNoreplace,
     beforeLinearizationFailure |-> FailureOutcomeNotCommitted,
     afterLinearizationFailure |-> FailureOutcomeOutcomeUnknown,
     resolverProbeTopology |-> ResolverProbeTopologySourceAndDestination,
     qualification |-> TransitionQualificationNone],
    [operation |-> OperationReapExpiredToReady,
     source |-> StateLeased,
     sourceObjectKind |-> ObjectKindFullJob,
     destination |-> StateReady,
     destinationObjectKind |-> ObjectKindFullJob,
     generationChange |-> GenerationChangeIncrement,
     attemptChange |-> AttemptChangeUnchanged,
     tokenChange |-> TokenChangeNone,
     reasonClass |-> NoReasonClass,
     clockRequirement |-> ClockRequirementLeaseExpirationEvidence,
     requiredSyncs |-> <<SyncStepDestinationDirectory, SyncStepSourceDirectory>>,
     linearization |-> LinearizationRenameNoreplace,
     beforeLinearizationFailure |-> FailureOutcomeNotCommitted,
     afterLinearizationFailure |-> FailureOutcomeOutcomeUnknown,
     resolverProbeTopology |-> ResolverProbeTopologySourceAndDestination,
     qualification |-> TransitionQualificationAttemptsRemaining],
    [operation |-> OperationReapExpiredToDead,
     source |-> StateLeased,
     sourceObjectKind |-> ObjectKindFullJob,
     destination |-> StateDead,
     destinationObjectKind |-> ObjectKindFullJob,
     generationChange |-> GenerationChangeIncrement,
     attemptChange |-> AttemptChangeUnchanged,
     tokenChange |-> TokenChangeNone,
     reasonClass |-> ReasonClassAttemptsExhausted,
     clockRequirement |-> ClockRequirementLeaseExpirationEvidenceAndAuthenticatedWallFloor,
     requiredSyncs |-> <<SyncStepDestinationDirectory, SyncStepSourceDirectory>>,
     linearization |-> LinearizationRenameNoreplace,
     beforeLinearizationFailure |-> FailureOutcomeNotCommitted,
     afterLinearizationFailure |-> FailureOutcomeOutcomeUnknown,
     resolverProbeTopology |-> ResolverProbeTopologySourceAndDestination,
     qualification |-> TransitionQualificationAttemptsExhausted],
    [operation |-> OperationQuarantine,
     source |-> StateActive,
     sourceObjectKind |-> ObjectKindRawObject,
     destination |-> StateQuarantine,
     destinationObjectKind |-> ObjectKindRawObject,
     generationChange |-> GenerationChangeIncrement,
     attemptChange |-> AttemptChangeUnchanged,
     tokenChange |-> TokenChangeNone,
     reasonClass |-> ReasonClassCorruption,
     clockRequirement |-> ClockRequirementNone,
     requiredSyncs |-> <<SyncStepDestinationDirectory, SyncStepSourceDirectory>>,
     linearization |-> LinearizationRenameNoreplace,
     beforeLinearizationFailure |-> FailureOutcomeNotCommitted,
     afterLinearizationFailure |-> FailureOutcomeOutcomeUnknown,
     resolverProbeTopology |-> ResolverProbeTopologySourceAndDestination,
     qualification |-> TransitionQualificationRawBytesPreserved]
>>

(* descriptionUtf8Hex stores exact UTF-8 bytes as lowercase hex. *)
ProtocolExceptions == <<
    [name |-> ExceptionReceiptCompaction,
     descriptionUtf8Hex |-> "5465726d696e616c2066756c6c2d6a6f622072656365697074207265706c6163656420627920627974652d64657465726d696e697374696320636f6d7061637420726563656970742061742073616d6520706174686e616d65",
     sourceObjectKind |-> ObjectKindFullReceipt,
     destinationObjectKind |-> ObjectKindCompactReceipt,
     clockRequirement |-> ClockRequirementNone,
     mutationClass |-> MutationClassReplacingMove,
     linearization |-> LinearizationRenameReplace,
     requiredSyncs |-> <<SyncStepFile, SyncStepSameOrDestinationDirectory>>,
     beforeLinearizationFailure |-> FailureOutcomeNotCommitted,
     afterLinearizationFailure |-> FailureOutcomeOutcomeUnknown],
    [name |-> ExceptionWallWatermarkAdvancement,
     descriptionUtf8Hex |-> "4d6f6e6f746f6e652077616c6c2d77617465726d61726b207265636f7264207265706c6163656420756e646572206578636c7573697665204f4644206c6f636b",
     sourceObjectKind |-> ObjectKindWatermarkRecord,
     destinationObjectKind |-> ObjectKindWatermarkRecord,
     clockRequirement |-> ClockRequirementAuthenticatedWallFloor,
     mutationClass |-> MutationClassReplacingMove,
     linearization |-> LinearizationRenameReplace,
     requiredSyncs |-> <<SyncStepFile, SyncStepSameOrDestinationDirectory>>,
     beforeLinearizationFailure |-> FailureOutcomeNotCommitted,
     afterLinearizationFailure |-> FailureOutcomeOutcomeUnknown]
>>

ProtocolUnlinks == <<
    [name |-> UnlinkFullReceiptRetentionDeletion,
     descriptionUtf8Hex |-> "41757468656e7469636174656420726574656e74696f6e2064656c6574696f6e206f6620616e20656c696769626c652066756c6c2072656365697074",
     source |-> StateReceipt,
     sourceObjectKind |-> ObjectKindFullReceipt,
     sourceAuthentication |-> SourceAuthenticationStrictReceipt,
     clockRequirement |-> ClockRequirementAuthenticatedWallFloor,
     qualification |-> TransitionQualificationReceiptBucketEndPlusRetentionNotAfterWallFloor,
     mutationClass |-> MutationClassUnlink,
     linearization |-> LinearizationUnlink,
     requiredSyncs |-> <<SyncStepSourceDirectory>>,
     beforeLinearizationFailure |-> FailureOutcomeNotCommitted,
     afterLinearizationFailure |-> FailureOutcomeOutcomeUnknown,
     resolverProbeTopology |-> ResolverProbeTopologySourcePresence],
    [name |-> UnlinkCompactReceiptRetentionDeletion,
     descriptionUtf8Hex |-> "41757468656e7469636174656420726574656e74696f6e2064656c6574696f6e206f6620616e20656c696769626c6520636f6d706163742072656365697074",
     source |-> StateReceipt,
     sourceObjectKind |-> ObjectKindCompactReceipt,
     sourceAuthentication |-> SourceAuthenticationStrictReceipt,
     clockRequirement |-> ClockRequirementAuthenticatedWallFloor,
     qualification |-> TransitionQualificationReceiptBucketEndPlusRetentionNotAfterWallFloor,
     mutationClass |-> MutationClassUnlink,
     linearization |-> LinearizationUnlink,
     requiredSyncs |-> <<SyncStepSourceDirectory>>,
     beforeLinearizationFailure |-> FailureOutcomeNotCommitted,
     afterLinearizationFailure |-> FailureOutcomeOutcomeUnknown,
     resolverProbeTopology |-> ResolverProbeTopologySourcePresence]
>>

ProtocolReentry == <<
    [name |-> ReentryRequeueDead,
     source |-> StateDead,
     descriptionUtf8Hex |-> "56657269666965642072657375626d697373696f6e3a2063726561746573206e6577206a6f62206964656e746974792c20636f70696573207061796c6f616420616e642073616665206d657461646174612c2061646473206f6c64206a6f625f69642061732070726f76656e616e6365",
     createsNewIdentity |-> TRUE],
    [name |-> ReentryRequeueQuarantine,
     source |-> StateQuarantine,
     descriptionUtf8Hex |-> "56657269666965642072657375626d697373696f6e2061667465722066756c6c207374727563747572616c20616e64207061796c6f616420766572696669636174696f6e3a2063726561746573206e6577206a6f62206964656e74697479",
     createsNewIdentity |-> TRUE]
>>

=============================================================================
