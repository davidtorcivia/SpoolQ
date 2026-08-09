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
VerifiedEvidenceClasses == {FullVerified, CompactVerified}
ReceiptClasses == EvidenceClasses \cup {NoReceipt, Deleted}
InitialReceiptClasses == {
    NoReceipt,
    FullUnverified,
    FullVerified,
    CompactUnverified
}

EventNeutral == "neutral"
EventDuplicateAcknowledged == "duplicate-acknowledged"
EventAcknowledgeNotCommitted == "acknowledge-not-committed"
EventAcknowledgeCommitted == "acknowledge-committed"
EventAcknowledgeOutcomeUnknown == "acknowledge-outcome-unknown"
EventCompactionNotCommitted == "compaction-not-committed"
EventCompactionCommitted == "compaction-committed"
EventCompactionOutcomeUnknown == "compaction-outcome-unknown"
EventDeletionNotCommitted == "deletion-not-committed"
EventDeletionCommitted == "deletion-committed"
EventDeletionOutcomeUnknown == "deletion-outcome-unknown"
Events == {
    EventNeutral,
    EventDuplicateAcknowledged,
    EventAcknowledgeNotCommitted,
    EventAcknowledgeCommitted,
    EventAcknowledgeOutcomeUnknown,
    EventCompactionNotCommitted,
    EventCompactionCommitted,
    EventCompactionOutcomeUnknown,
    EventDeletionNotCommitted,
    EventDeletionCommitted,
    EventDeletionOutcomeUnknown
}

AcknowledgeLinearizedEvents == {
    EventAcknowledgeCommitted,
    EventAcknowledgeOutcomeUnknown
}

CompactionLinearizedEvents == {
    EventCompactionCommitted,
    EventCompactionOutcomeUnknown
}

DeletionLinearizedEvents == {
    EventDeletionCommitted,
    EventDeletionOutcomeUnknown
}

NotCommittedEvents == {
    EventAcknowledgeNotCommitted,
    EventCompactionNotCommitted,
    EventDeletionNotCommitted
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
    operationInputClass,
    deletionFloor,
    deletionAuthenticated,
    lastEvent

Vars == <<receiptClass, verifiedFullSeen, terminalSeen, wallFloor,
          wallAuthority, duplicateEvidenceClass, operationInputClass,
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
    /\ operationInputClass \in ReceiptClasses
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
    /\ operationInputClass = NoReceipt
    /\ deletionFloor = NoTime
    /\ deletionAuthenticated = FALSE
    /\ lastEvent = EventNeutral

AcquireWallAuthority(newFloor) ==
    /\ ~wallAuthority
    /\ newFloor \in Times
    /\ wallFloor' = newFloor
    /\ wallAuthority' = TRUE
    /\ lastEvent' = EventNeutral
    /\ UNCHANGED <<receiptClass, verifiedFullSeen, terminalSeen,
                    duplicateEvidenceClass, operationInputClass, deletionFloor,
                    deletionAuthenticated>>

ReleaseWallAuthority ==
    /\ wallAuthority
    /\ wallAuthority' = FALSE
    /\ lastEvent' = EventNeutral
    /\ UNCHANGED <<receiptClass, verifiedFullSeen, terminalSeen, wallFloor,
                    duplicateEvidenceClass, operationInputClass, deletionFloor,
                    deletionAuthenticated>>

AcknowledgeLinearized(event) ==
    /\ receiptClass = NoReceipt
    /\ wallAuthority
    /\ event \in AcknowledgeLinearizedEvents
    /\ operationInputClass' = receiptClass
    /\ receiptClass' = FullVerified
    /\ verifiedFullSeen' = TRUE
    /\ terminalSeen' = TRUE
    /\ lastEvent' = event
    /\ UNCHANGED <<wallFloor, wallAuthority, duplicateEvidenceClass,
                    deletionFloor, deletionAuthenticated>>

AcknowledgeNotCommitted ==
    /\ receiptClass = NoReceipt
    /\ wallAuthority
    /\ operationInputClass' = receiptClass
    /\ lastEvent' = EventAcknowledgeNotCommitted
    /\ UNCHANGED <<receiptClass, verifiedFullSeen, terminalSeen, wallFloor,
                    wallAuthority, duplicateEvidenceClass, deletionFloor,
                    deletionAuthenticated>>

DuplicateAcknowledge ==
    /\ receiptClass \in VerifiedEvidenceClasses
    /\ wallAuthority
    /\ duplicateEvidenceClass' = receiptClass
    /\ lastEvent' = EventDuplicateAcknowledged
    /\ UNCHANGED <<receiptClass, verifiedFullSeen, terminalSeen, wallFloor,
                    wallAuthority, operationInputClass, deletionFloor,
                    deletionAuthenticated>>

CompactVerifiedReceipt(event) ==
    /\ receiptClass = FullVerified
    /\ event \in CompactionLinearizedEvents
    /\ operationInputClass' = receiptClass
    /\ receiptClass' = CompactVerified
    /\ lastEvent' = event
    /\ UNCHANGED <<verifiedFullSeen, terminalSeen, wallFloor, wallAuthority,
                    duplicateEvidenceClass, deletionFloor,
                    deletionAuthenticated>>

CompactionNotCommitted ==
    /\ receiptClass = FullVerified
    /\ operationInputClass' = receiptClass
    /\ lastEvent' = EventCompactionNotCommitted
    /\ UNCHANGED <<receiptClass, verifiedFullSeen, terminalSeen, wallFloor,
                    wallAuthority, duplicateEvidenceClass, deletionFloor,
                    deletionAuthenticated>>

DeleteRetainedReceipt(event) ==
    /\ receiptClass \in EvidenceClasses
    /\ wallAuthority
    /\ wallFloor >= RetentionDeadline
    /\ event \in DeletionLinearizedEvents
    /\ operationInputClass' = receiptClass
    /\ receiptClass' = Deleted
    /\ deletionFloor' = wallFloor
    /\ deletionAuthenticated' = wallAuthority
    /\ lastEvent' = event
    /\ UNCHANGED <<verifiedFullSeen, terminalSeen, wallFloor, wallAuthority,
                    duplicateEvidenceClass>>

DeletionNotCommitted ==
    /\ receiptClass \in EvidenceClasses
    /\ wallAuthority
    /\ wallFloor >= RetentionDeadline
    /\ operationInputClass' = receiptClass
    /\ lastEvent' = EventDeletionNotCommitted
    /\ UNCHANGED <<receiptClass, verifiedFullSeen, terminalSeen, wallFloor,
                    wallAuthority, duplicateEvidenceClass, deletionFloor,
                    deletionAuthenticated>>

Next ==
    \/ \E newFloor \in Times : AcquireWallAuthority(newFloor)
    \/ ReleaseWallAuthority
    \/ \E event \in AcknowledgeLinearizedEvents :
        AcknowledgeLinearized(event)
    \/ AcknowledgeNotCommitted
    \/ DuplicateAcknowledge
    \/ \E event \in CompactionLinearizedEvents :
        CompactVerifiedReceipt(event)
    \/ CompactionNotCommitted
    \/ \E event \in DeletionLinearizedEvents :
        DeleteRetainedReceipt(event)
    \/ DeletionNotCommitted

Spec == Init /\ [][Next]_Vars

(* ---- Invariants ---- *)

VerifiedCompactRequiresVerifiedFull ==
    receiptClass = CompactVerified => verifiedFullSeen

ReceiptStateRemainsTerminal ==
    terminalSeen => receiptClass \in EvidenceClasses \cup {Deleted}

DuplicateAckPreservesEvidence ==
    lastEvent = EventDuplicateAcknowledged =>
        /\ receiptClass = duplicateEvidenceClass
        /\ receiptClass \in VerifiedEvidenceClasses
        /\ wallAuthority

RetentionDeletionUsesAuthenticatedEligibility ==
    receiptClass = Deleted =>
        /\ deletionAuthenticated
        /\ deletionFloor \in Times
        /\ deletionFloor >= RetentionDeadline

LinearizationOutcomeMatchesEvidence ==
    /\ lastEvent \in AcknowledgeLinearizedEvents =>
        /\ operationInputClass = NoReceipt
        /\ receiptClass = FullVerified
    /\ lastEvent \in CompactionLinearizedEvents =>
        /\ operationInputClass = FullVerified
        /\ receiptClass = CompactVerified
    /\ lastEvent \in DeletionLinearizedEvents =>
        /\ operationInputClass \in EvidenceClasses
        /\ receiptClass = Deleted

NotCommittedPreservesReceiptEvidence ==
    lastEvent \in NotCommittedEvents =>
        receiptClass = operationInputClass

UnverifiedEvidenceCannotSatisfyDuplicateAck ==
    receiptClass \in {FullUnverified, CompactUnverified} =>
        ~ENABLED DuplicateAcknowledge

UnverifiedEvidenceCannotBeCompacted ==
    receiptClass = FullUnverified =>
        ~ENABLED \E event \in CompactionLinearizedEvents :
            CompactVerifiedReceipt(event)

(* ---- End invariants ---- *)

=============================================================================
