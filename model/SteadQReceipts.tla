---------------------------- MODULE SteadQReceipts ----------------------------
(**************************************************************************)
(* Bounded receipt evidence, compaction, duplicate-ack, and retention model. *)
(**************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets, TLC, SteadQProtocol

CONSTANTS MaxTime, RetentionDeadline

Times == 0..MaxTime
NoTime == MaxTime + 1
OptionalTimes == 0..NoTime

NoReceipt == "no-receipt"
FullUnverified == "full-unverified"
FullVerified == "full-verified"
CompactUnverified == "compact-unverified"
CompactVerified == "compact-verified"
Deleted == "deleted"

EvidenceClasses == {
    FullUnverified,
    FullVerified,
    CompactUnverified,
    CompactVerified
}
ReceiptClasses == EvidenceClasses \cup {NoReceipt, Deleted}
InitialReceiptClasses == {
    NoReceipt,
    FullUnverified,
    FullVerified,
    CompactUnverified
}

EventInit == "init"
EventWallAuthorityAcquired == "wall-authority-acquired"
EventWallAuthorityReleased == "wall-authority-released"
EventWallAuthorityFailure == "wall-authority-failure"
EventAcknowledged == "acknowledged"
EventDuplicateAcknowledged == "duplicate-acknowledged"
EventCompacted == "compacted"
EventDeleted == "deleted"
EventOperationFailure == "operation-failure"
Events == {
    EventInit,
    EventWallAuthorityAcquired,
    EventWallAuthorityReleased,
    EventWallAuthorityFailure,
    EventAcknowledged,
    EventDuplicateAcknowledged,
    EventCompacted,
    EventDeleted,
    EventOperationFailure
}

TransitionIndicesFor(operationValue) == {
    index \in 1..Len(ProtocolTransitions) :
        ProtocolTransitions[index].operation = operationValue
}

ExceptionIndicesFor(exceptionName) == {
    index \in 1..Len(ProtocolExceptions) :
        ProtocolExceptions[index].name = exceptionName
}

UnlinkIndicesFor(unlinkName) == {
    index \in 1..Len(ProtocolUnlinks) :
        ProtocolUnlinks[index].name = unlinkName
}

AcknowledgeMetadataMatches ==
    /\ Cardinality(TransitionIndicesFor(OperationAcknowledge)) = 1
    /\ \A index \in TransitionIndicesFor(OperationAcknowledge) :
        LET row == ProtocolTransitions[index] IN
            /\ row.source = StateLeased
            /\ row.sourceObjectKind = ObjectKindFullJob
            /\ row.destination = StateReceipt
            /\ row.destinationObjectKind = ObjectKindFullReceipt
            /\ row.generationChange = GenerationChangeIncrement
            /\ row.attemptChange = AttemptChangeUnchanged
            /\ row.tokenChange = TokenChangeSame
            /\ row.reasonClass = NoReasonClass
            /\ row.clockRequirement = ClockRequirementAuthenticatedWallFloor
            /\ row.requiredSyncs =
                <<SyncStepDestinationDirectory, SyncStepSourceDirectory>>
            /\ row.linearization = LinearizationRenameNoreplace
            /\ row.beforeLinearizationFailure = FailureOutcomeNotCommitted
            /\ row.afterLinearizationFailure = FailureOutcomeOutcomeUnknown
            /\ row.resolverProbeTopology =
                ResolverProbeTopologyReceiptCandidatesAndSource
            /\ row.qualification = TransitionQualificationNone

CompactionMetadataMatches ==
    /\ Cardinality(ExceptionIndicesFor(ExceptionReceiptCompaction)) = 1
    /\ \A index \in ExceptionIndicesFor(ExceptionReceiptCompaction) :
        LET row == ProtocolExceptions[index] IN
            /\ row.sourceObjectKind = ObjectKindFullReceipt
            /\ row.destinationObjectKind = ObjectKindCompactReceipt
            /\ row.clockRequirement = ClockRequirementNone
            /\ row.mutationClass = MutationClassReplacingMove
            /\ row.linearization = LinearizationRenameReplace
            /\ row.requiredSyncs =
                <<SyncStepFile, SyncStepSameOrDestinationDirectory>>
            /\ row.beforeLinearizationFailure = FailureOutcomeNotCommitted
            /\ row.afterLinearizationFailure = FailureOutcomeOutcomeUnknown

RetentionMetadataMatches(unlinkName, objectKind) ==
    /\ Cardinality(UnlinkIndicesFor(unlinkName)) = 1
    /\ \A index \in UnlinkIndicesFor(unlinkName) :
        LET row == ProtocolUnlinks[index] IN
            /\ row.source = StateReceipt
            /\ row.sourceObjectKind = objectKind
            /\ row.sourceAuthentication = SourceAuthenticationStrictReceipt
            /\ row.clockRequirement = ClockRequirementAuthenticatedWallFloor
            /\ row.qualification =
                TransitionQualificationReceiptBucketEndPlusRetentionNotAfterWallFloor
            /\ row.mutationClass = MutationClassUnlink
            /\ row.linearization = LinearizationUnlink
            /\ row.requiredSyncs = <<SyncStepSourceDirectory>>
            /\ row.beforeLinearizationFailure = FailureOutcomeNotCommitted
            /\ row.afterLinearizationFailure = FailureOutcomeOutcomeUnknown
            /\ row.resolverProbeTopology = ResolverProbeTopologySourcePresence

ProtocolReceiptMetadataMatches ==
    /\ AcknowledgeMetadataMatches
    /\ CompactionMetadataMatches
    /\ RetentionMetadataMatches(
        UnlinkFullReceiptRetentionDeletion,
        ObjectKindFullReceipt)
    /\ RetentionMetadataMatches(
        UnlinkCompactReceiptRetentionDeletion,
        ObjectKindCompactReceipt)

VARIABLES
    receiptClass,
    verifiedFullSeen,
    terminalSeen,
    wallFloor,
    wallAuthority,
    duplicateEvidenceClass,
    failureInputClass,
    deletionFloor,
    deletionAuthenticated,
    lastEvent

Vars == <<receiptClass, verifiedFullSeen, terminalSeen, wallFloor,
          wallAuthority, duplicateEvidenceClass, failureInputClass,
          deletionFloor, deletionAuthenticated, lastEvent>>

TypeInvariant ==
    /\ MaxTime \in Nat
    /\ RetentionDeadline \in Times
    /\ ProtocolReceiptMetadataMatches
    /\ receiptClass \in ReceiptClasses
    /\ verifiedFullSeen \in BOOLEAN
    /\ terminalSeen \in BOOLEAN
    /\ wallFloor \in Times
    /\ wallAuthority \in BOOLEAN
    /\ duplicateEvidenceClass \in ReceiptClasses
    /\ failureInputClass \in ReceiptClasses
    /\ deletionFloor \in OptionalTimes
    /\ deletionAuthenticated \in BOOLEAN
    /\ lastEvent \in Events

Init ==
    /\ receiptClass \in InitialReceiptClasses
    /\ verifiedFullSeen = (receiptClass = FullVerified)
    /\ terminalSeen = (receiptClass # NoReceipt)
    /\ wallFloor = 0
    /\ wallAuthority = FALSE
    /\ duplicateEvidenceClass = NoReceipt
    /\ failureInputClass = NoReceipt
    /\ deletionFloor = NoTime
    /\ deletionAuthenticated = FALSE
    /\ lastEvent = EventInit

AcquireWallAuthority(newFloor) ==
    /\ ~wallAuthority
    /\ newFloor \in Times
    /\ wallFloor' = newFloor
    /\ wallAuthority' = TRUE
    /\ lastEvent' = EventWallAuthorityAcquired
    /\ UNCHANGED <<receiptClass, verifiedFullSeen, terminalSeen,
                    duplicateEvidenceClass, failureInputClass, deletionFloor,
                    deletionAuthenticated>>

ReleaseWallAuthority ==
    /\ wallAuthority
    /\ wallAuthority' = FALSE
    /\ lastEvent' = EventWallAuthorityReleased
    /\ UNCHANGED <<receiptClass, verifiedFullSeen, terminalSeen, wallFloor,
                    duplicateEvidenceClass, failureInputClass, deletionFloor,
                    deletionAuthenticated>>

WallAuthorityFailure ==
    /\ ~wallAuthority
    /\ lastEvent' = EventWallAuthorityFailure
    /\ UNCHANGED <<receiptClass, verifiedFullSeen, terminalSeen, wallFloor,
                    wallAuthority, duplicateEvidenceClass, failureInputClass,
                    deletionFloor, deletionAuthenticated>>

AcknowledgeVerified ==
    /\ receiptClass = NoReceipt
    /\ wallAuthority
    /\ receiptClass' = FullVerified
    /\ verifiedFullSeen' = TRUE
    /\ terminalSeen' = TRUE
    /\ lastEvent' = EventAcknowledged
    /\ UNCHANGED <<wallFloor, wallAuthority, duplicateEvidenceClass,
                    failureInputClass, deletionFloor, deletionAuthenticated>>

DuplicateAcknowledge ==
    /\ receiptClass \in EvidenceClasses
    /\ duplicateEvidenceClass' = receiptClass
    /\ lastEvent' = EventDuplicateAcknowledged
    /\ UNCHANGED <<receiptClass, verifiedFullSeen, terminalSeen, wallFloor,
                    wallAuthority, failureInputClass, deletionFloor,
                    deletionAuthenticated>>

CompactVerifiedReceipt ==
    /\ receiptClass = FullVerified
    /\ receiptClass' = CompactVerified
    /\ lastEvent' = EventCompacted
    /\ UNCHANGED <<verifiedFullSeen, terminalSeen, wallFloor, wallAuthority,
                    duplicateEvidenceClass, failureInputClass, deletionFloor,
                    deletionAuthenticated>>

DeleteRetainedReceipt ==
    /\ receiptClass \in EvidenceClasses
    /\ wallAuthority
    /\ wallFloor >= RetentionDeadline
    /\ receiptClass' = Deleted
    /\ deletionFloor' = wallFloor
    /\ deletionAuthenticated' = wallAuthority
    /\ lastEvent' = EventDeleted
    /\ UNCHANGED <<verifiedFullSeen, terminalSeen, wallFloor, wallAuthority,
                    duplicateEvidenceClass, failureInputClass>>

OperationFailure ==
    /\ failureInputClass' = receiptClass
    /\ lastEvent' = EventOperationFailure
    /\ UNCHANGED <<receiptClass, verifiedFullSeen, terminalSeen, wallFloor,
                    wallAuthority, duplicateEvidenceClass, deletionFloor,
                    deletionAuthenticated>>

Next ==
    \/ \E newFloor \in Times : AcquireWallAuthority(newFloor)
    \/ ReleaseWallAuthority
    \/ WallAuthorityFailure
    \/ AcknowledgeVerified
    \/ DuplicateAcknowledge
    \/ CompactVerifiedReceipt
    \/ DeleteRetainedReceipt
    \/ OperationFailure

Spec == Init /\ [][Next]_Vars

(* ---- Invariants ---- *)

VerifiedCompactRequiresVerifiedFull ==
    receiptClass = CompactVerified => verifiedFullSeen

ReceiptStateRemainsTerminal ==
    terminalSeen => receiptClass \in EvidenceClasses \cup {Deleted}

DuplicateAckPreservesEvidence ==
    lastEvent = EventDuplicateAcknowledged =>
        receiptClass = duplicateEvidenceClass

RetentionDeletionUsesAuthenticatedEligibility ==
    receiptClass = Deleted =>
        /\ deletionAuthenticated
        /\ deletionFloor \in Times
        /\ deletionFloor >= RetentionDeadline

FailurePreservesReceiptEvidence ==
    lastEvent = EventOperationFailure => receiptClass = failureInputClass

UnverifiedEvidenceCannotBeCompacted ==
    receiptClass = FullUnverified => ~ENABLED CompactVerifiedReceipt

(* ---- End invariants ---- *)

=============================================================================
