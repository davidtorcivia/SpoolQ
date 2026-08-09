--------------------------- MODULE SteadQScheduling --------------------------
(**************************************************************************)
(* Bounded authenticated-wall and boottime scheduling model.              *)
(**************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets, TLC, SteadQProtocol

CONSTANTS MaxTime, DelayDeadline, RetentionDeadline, LeaseDeadline

Times == 0..MaxTime
NoTime == MaxTime + 1
OptionalTimes == 0..NoTime

EventInit == "init"
EventRealtimeChanged == "realtime-changed"
EventBoottimeAdvanced == "boottime-advanced"
EventWallAuthorityAcquired == "wall-authority-acquired"
EventWallAuthorityReleased == "wall-authority-released"
EventWallAuthorityFailure == "wall-authority-failure"
EventBoottimeFailure == "boottime-failure"
EventDelayedPromoted == "delayed-promoted"
EventReceiptDeleted == "receipt-deleted"
EventLeaseExpired == "lease-expired"
Events == {
    EventInit,
    EventRealtimeChanged,
    EventBoottimeAdvanced,
    EventWallAuthorityAcquired,
    EventWallAuthorityReleased,
    EventWallAuthorityFailure,
    EventBoottimeFailure,
    EventDelayedPromoted,
    EventReceiptDeleted,
    EventLeaseExpired
}

Maximum(left, right) == IF left >= right THEN left ELSE right

TransitionIndicesFor(operationValue) == {
    index \in 1..Len(ProtocolTransitions) :
        ProtocolTransitions[index].operation = operationValue
}

TransitionRowsFor(operationValue) == {
    ProtocolTransitions[index] :
        index \in TransitionIndicesFor(operationValue)
}

OperationHasClock(operationValue, clockValue) ==
    /\ Cardinality(TransitionIndicesFor(operationValue)) = 1
    /\ \A row \in TransitionRowsFor(operationValue) :
        row.clockRequirement = clockValue

WatermarkExceptionIndices == {
    index \in 1..Len(ProtocolExceptions) :
        ProtocolExceptions[index].name = ExceptionWallWatermarkAdvancement
}

WatermarkExceptionRows == {
    ProtocolExceptions[index] : index \in WatermarkExceptionIndices
}

ProtocolSchedulingMetadataMatches ==
    /\ OperationHasClock(
        OperationEnqueueImmediate,
        ClockRequirementAuthenticatedWallFloor)
    /\ OperationHasClock(
        OperationEnqueueDelayed,
        ClockRequirementAuthenticatedWallFloor)
    /\ OperationHasClock(
        OperationPromote,
        ClockRequirementAuthenticatedWallFloor)
    /\ OperationHasClock(
        OperationClaim,
        ClockRequirementBoottimeAndAuthenticatedWallFloor)
    /\ OperationHasClock(
        OperationExhaustedReadyCleanup,
        ClockRequirementAuthenticatedWallFloor)
    /\ OperationHasClock(
        OperationRenew,
        ClockRequirementBoottimeAndAuthenticatedWallFloor)
    /\ OperationHasClock(
        OperationAcknowledge,
        ClockRequirementAuthenticatedWallFloor)
    /\ OperationHasClock(
        OperationRetryNow,
        ClockRequirementNone)
    /\ OperationHasClock(
        OperationRetryLater,
        ClockRequirementAuthenticatedWallFloor)
    /\ OperationHasClock(
        OperationBury,
        ClockRequirementAuthenticatedWallFloor)
    /\ OperationHasClock(
        OperationReapExpiredToReady,
        ClockRequirementLeaseExpirationEvidence)
    /\ OperationHasClock(
        OperationReapExpiredToDead,
        ClockRequirementLeaseExpirationEvidenceAndAuthenticatedWallFloor)
    /\ OperationHasClock(
        OperationQuarantine,
        ClockRequirementNone)
    /\ Cardinality(WatermarkExceptionIndices) = 1
    /\ \A row \in WatermarkExceptionRows :
        /\ row.sourceObjectKind = ObjectKindWatermarkRecord
        /\ row.destinationObjectKind = ObjectKindWatermarkRecord
        /\ row.clockRequirement = ClockRequirementAuthenticatedWallFloor
        /\ row.mutationClass = MutationClassReplacingMove
        /\ row.linearization = LinearizationRenameReplace
        /\ row.requiredSyncs =
            <<SyncStepFile, SyncStepSameOrDestinationDirectory>>
        /\ row.beforeLinearizationFailure = FailureOutcomeNotCommitted
        /\ row.afterLinearizationFailure = FailureOutcomeOutcomeUnknown

VARIABLES
    realTime,
    bootTime,
    watermark,
    watermarkHigh,
    wallFloor,
    wallAuthority,
    delayedPresent,
    delayedPromoted,
    promotionFloor,
    promotionAuthenticated,
    receiptPresent,
    receiptDeleted,
    deletionFloor,
    deletionAuthenticated,
    leasePresent,
    leaseExpired,
    rollbackSeen,
    lastEvent

Vars == <<realTime, bootTime, watermark, watermarkHigh, wallFloor, wallAuthority,
          delayedPresent, delayedPromoted, promotionFloor,
          promotionAuthenticated, receiptPresent, receiptDeleted,
          deletionFloor, deletionAuthenticated, leasePresent, leaseExpired,
          rollbackSeen, lastEvent>>

TypeInvariant ==
    /\ MaxTime \in Nat
    /\ DelayDeadline \in Times
    /\ RetentionDeadline \in Times
    /\ LeaseDeadline \in Times
    /\ ProtocolSchedulingMetadataMatches
    /\ realTime \in Times
    /\ bootTime \in Times
    /\ watermark \in Times
    /\ watermarkHigh \in Times
    /\ wallFloor \in Times
    /\ wallAuthority \in BOOLEAN
    /\ delayedPresent \in BOOLEAN
    /\ delayedPromoted \in BOOLEAN
    /\ promotionFloor \in OptionalTimes
    /\ promotionAuthenticated \in BOOLEAN
    /\ receiptPresent \in BOOLEAN
    /\ receiptDeleted \in BOOLEAN
    /\ deletionFloor \in OptionalTimes
    /\ deletionAuthenticated \in BOOLEAN
    /\ leasePresent \in BOOLEAN
    /\ leaseExpired \in BOOLEAN
    /\ rollbackSeen \in BOOLEAN
    /\ lastEvent \in Events

Init ==
    /\ realTime = 0
    /\ bootTime = 0
    /\ watermark = 0
    /\ watermarkHigh = 0
    /\ wallFloor = 0
    /\ wallAuthority = FALSE
    /\ delayedPresent = TRUE
    /\ delayedPromoted = FALSE
    /\ promotionFloor = NoTime
    /\ promotionAuthenticated = FALSE
    /\ receiptPresent = TRUE
    /\ receiptDeleted = FALSE
    /\ deletionFloor = NoTime
    /\ deletionAuthenticated = FALSE
    /\ leasePresent = TRUE
    /\ leaseExpired = FALSE
    /\ rollbackSeen = FALSE
    /\ lastEvent = EventInit

SetRealtime(newTime) ==
    /\ newTime \in Times
    /\ newTime # realTime
    /\ realTime' = newTime
    /\ rollbackSeen' = (rollbackSeen \/ newTime < realTime)
    /\ lastEvent' = EventRealtimeChanged
    /\ UNCHANGED <<bootTime, watermark, watermarkHigh, wallFloor, wallAuthority,
                    delayedPresent, delayedPromoted, promotionFloor,
                    promotionAuthenticated, receiptPresent, receiptDeleted,
                    deletionFloor, deletionAuthenticated, leasePresent,
                    leaseExpired>>

AdvanceBoottime ==
    /\ bootTime < MaxTime
    /\ bootTime' = bootTime + 1
    /\ lastEvent' = EventBoottimeAdvanced
    /\ UNCHANGED <<realTime, watermark, watermarkHigh, wallFloor, wallAuthority,
                    delayedPresent, delayedPromoted, promotionFloor,
                    promotionAuthenticated, receiptPresent, receiptDeleted,
                    deletionFloor, deletionAuthenticated, leasePresent,
                    leaseExpired, rollbackSeen>>

AcquireWallAuthority ==
    /\ ~wallAuthority
    /\ LET nextFloor == Maximum(realTime, watermark) IN
        /\ wallFloor' = nextFloor
        /\ watermark' = nextFloor
        /\ watermarkHigh' = Maximum(watermarkHigh, nextFloor)
    /\ wallAuthority' = TRUE
    /\ lastEvent' = EventWallAuthorityAcquired
    /\ UNCHANGED <<realTime, bootTime, delayedPresent, delayedPromoted,
                    promotionFloor, promotionAuthenticated, receiptPresent,
                    receiptDeleted, deletionFloor, deletionAuthenticated,
                    leasePresent, leaseExpired, rollbackSeen>>

ReleaseWallAuthority ==
    /\ wallAuthority
    /\ wallAuthority' = FALSE
    /\ lastEvent' = EventWallAuthorityReleased
    /\ UNCHANGED <<realTime, bootTime, watermark, watermarkHigh, wallFloor,
                    delayedPresent, delayedPromoted, promotionFloor,
                    promotionAuthenticated, receiptPresent, receiptDeleted,
                    deletionFloor, deletionAuthenticated, leasePresent,
                    leaseExpired, rollbackSeen>>

WallAuthorityFailure ==
    /\ ~wallAuthority
    /\ delayedPresent
    /\ receiptPresent
    /\ lastEvent' = EventWallAuthorityFailure
    /\ UNCHANGED <<realTime, bootTime, watermark, watermarkHigh, wallFloor, wallAuthority,
                    delayedPresent, delayedPromoted, promotionFloor,
                    promotionAuthenticated, receiptPresent, receiptDeleted,
                    deletionFloor, deletionAuthenticated, leasePresent,
                    leaseExpired, rollbackSeen>>

BoottimeFailure ==
    /\ leasePresent
    /\ ~leaseExpired
    /\ lastEvent' = EventBoottimeFailure
    /\ UNCHANGED <<realTime, bootTime, watermark, watermarkHigh, wallFloor, wallAuthority,
                    delayedPresent, delayedPromoted, promotionFloor,
                    promotionAuthenticated, receiptPresent, receiptDeleted,
                    deletionFloor, deletionAuthenticated, leasePresent,
                    leaseExpired, rollbackSeen>>

PromoteDelayed ==
    /\ delayedPresent
    /\ ~delayedPromoted
    /\ wallAuthority
    /\ wallFloor >= DelayDeadline
    /\ delayedPresent' = FALSE
    /\ delayedPromoted' = TRUE
    /\ promotionFloor' = wallFloor
    /\ promotionAuthenticated' = wallAuthority
    /\ lastEvent' = EventDelayedPromoted
    /\ UNCHANGED <<realTime, bootTime, watermark, watermarkHigh, wallFloor, wallAuthority,
                    receiptPresent, receiptDeleted, deletionFloor,
                    deletionAuthenticated, leasePresent, leaseExpired,
                    rollbackSeen>>

DeleteReceipt ==
    /\ receiptPresent
    /\ ~receiptDeleted
    /\ wallAuthority
    /\ wallFloor >= RetentionDeadline
    /\ receiptPresent' = FALSE
    /\ receiptDeleted' = TRUE
    /\ deletionFloor' = wallFloor
    /\ deletionAuthenticated' = wallAuthority
    /\ lastEvent' = EventReceiptDeleted
    /\ UNCHANGED <<realTime, bootTime, watermark, watermarkHigh, wallFloor, wallAuthority,
                    delayedPresent, delayedPromoted, promotionFloor,
                    promotionAuthenticated, leasePresent, leaseExpired,
                    rollbackSeen>>

ExpireCurrentBootLease ==
    /\ leasePresent
    /\ ~leaseExpired
    /\ bootTime >= LeaseDeadline
    /\ leasePresent' = FALSE
    /\ leaseExpired' = TRUE
    /\ lastEvent' = EventLeaseExpired
    /\ UNCHANGED <<realTime, bootTime, watermark, watermarkHigh, wallFloor, wallAuthority,
                    delayedPresent, delayedPromoted, promotionFloor,
                    promotionAuthenticated, receiptPresent, receiptDeleted,
                    deletionFloor, deletionAuthenticated, rollbackSeen>>

Next ==
    \/ \E newTime \in Times : SetRealtime(newTime)
    \/ AdvanceBoottime
    \/ AcquireWallAuthority
    \/ ReleaseWallAuthority
    \/ WallAuthorityFailure
    \/ BoottimeFailure
    \/ PromoteDelayed
    \/ DeleteReceipt
    \/ ExpireCurrentBootLease

Spec == Init /\ [][Next]_Vars

(* ---- Invariants ---- *)

AuthenticatedFloorIsDurable ==
    wallAuthority => wallFloor = watermark

AuthenticatedFloorDoesNotExceedWatermark ==
    wallFloor <= watermark

WatermarkEqualsHistoricalHigh ==
    watermark = watermarkHigh

RealtimeBelowWatermarkRequiresObservedRollback ==
    watermark > realTime => rollbackSeen

DelayedPromotionUsesAuthenticatedFloor ==
    delayedPromoted =>
        /\ promotionAuthenticated
        /\ promotionFloor \in Times
        /\ promotionFloor >= DelayDeadline

ReceiptDeletionUsesAuthenticatedFloor ==
    receiptDeleted =>
        /\ deletionAuthenticated
        /\ deletionFloor \in Times
        /\ deletionFloor >= RetentionDeadline

CurrentBootLeaseUsesBoottime ==
    leaseExpired => bootTime >= LeaseDeadline

EligibleCurrentBootLeaseCanExpire ==
    leasePresent /\ bootTime >= LeaseDeadline =>
        ENABLED ExpireCurrentBootLease

FailureDoesNotCreateAuthority ==
    /\ (lastEvent = EventWallAuthorityFailure =>
        /\ ~wallAuthority
        /\ delayedPresent
        /\ receiptPresent)
    /\ (lastEvent = EventBoottimeFailure =>
        /\ leasePresent
        /\ ~leaseExpired)

ObjectStateIsConsistent ==
    /\ delayedPresent = ~delayedPromoted
    /\ receiptPresent = ~receiptDeleted
    /\ leasePresent = ~leaseExpired

(* ---- End invariants ---- *)

=============================================================================
