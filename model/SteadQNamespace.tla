--------------------------- MODULE SteadQNamespace ---------------------------
(************************************************************************)
(* Bounded namespace and directory-durability model for a cross-directory *)
(* no-overwrite move and its resolver observations.                       *)
(************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets, TLC, SteadQProtocol

CONSTANT CrashProfile

ProfileOrdered == "ordered"
ProfileWeak == "weak"
Profiles == {ProfileOrdered, ProfileWeak}

NoObject == "none"
SourceObject == "source-object"
ConflictObject == "conflict-object"
Objects == {NoObject, SourceObject, ConflictObject}

PhasePrepared == "prepared"
PhaseNotCommitted == "not-committed"
PhaseLinearized == "linearized"
PhaseOutcomeUnknown == "outcome-unknown"
PhaseDestinationDurable == "destination-durable"
PhaseCommitted == "committed"
PhaseObservedSourceOnly == "observed-source-only"
PhaseObservedDestinationOnly == "observed-destination-only"
PhaseObservedBothSame == "observed-both-same"
PhaseObservedBothDifferent == "observed-both-different"
PhaseObservedNeither == "observed-neither"
PhaseResolverDestinationDurable == "resolver-destination-durable"
PhaseResolverSourceRemoved == "resolver-source-removed"
Phases == {
    PhasePrepared,
    PhaseNotCommitted,
    PhaseLinearized,
    PhaseOutcomeUnknown,
    PhaseDestinationDurable,
    PhaseCommitted,
    PhaseObservedSourceOnly,
    PhaseObservedDestinationOnly,
    PhaseObservedBothSame,
    PhaseObservedBothDifferent,
    PhaseObservedNeither,
    PhaseResolverDestinationDurable,
    PhaseResolverSourceRemoved
}

ObservationPhases == {
    PhaseObservedSourceOnly,
    PhaseObservedDestinationOnly,
    PhaseObservedBothSame,
    PhaseObservedBothDifferent,
    PhaseObservedNeither
}

ObservationSourceOnly == "source-only"
ObservationDestinationOnly == "destination-only"
ObservationBothSame == "both-same"
ObservationBothDifferent == "both-different"
ObservationNeither == "neither"
Observations == {
    ObservationSourceOnly,
    ObservationDestinationOnly,
    ObservationBothSame,
    ObservationBothDifferent,
    ObservationNeither
}

ObservationOf(sourceValue, destinationValue) ==
    IF sourceValue = SourceObject /\ destinationValue = NoObject
    THEN ObservationSourceOnly
    ELSE IF sourceValue = NoObject /\ destinationValue = SourceObject
    THEN ObservationDestinationOnly
    ELSE IF sourceValue = SourceObject /\ destinationValue = SourceObject
    THEN ObservationBothSame
    ELSE IF sourceValue # NoObject /\ destinationValue # NoObject
    THEN ObservationBothDifferent
    ELSE ObservationNeither

PhaseForObservation(observationValue) ==
    IF observationValue = ObservationSourceOnly
    THEN PhaseObservedSourceOnly
    ELSE IF observationValue = ObservationDestinationOnly
    THEN PhaseObservedDestinationOnly
    ELSE IF observationValue = ObservationBothSame
    THEN PhaseObservedBothSame
    ELSE IF observationValue = ObservationBothDifferent
    THEN PhaseObservedBothDifferent
    ELSE PhaseObservedNeither

DistinctStateMoveIndices == {
    index \in 1..Len(ProtocolTransitions) :
        /\ ProtocolTransitions[index].linearization =
            LinearizationRenameNoreplace
        /\ ProtocolTransitions[index].source #
            ProtocolTransitions[index].destination
}

DistinctStateMoveRows == {
    ProtocolTransitions[index] : index \in DistinctStateMoveIndices
}

RenewMoveIndices == {
    index \in 1..Len(ProtocolTransitions) :
        /\ ProtocolTransitions[index].operation = OperationRenew
        /\ ProtocolTransitions[index].linearization =
            LinearizationRenameNoreplace
}

RenewMoveRows == {
    ProtocolTransitions[index] : index \in RenewMoveIndices
}

ProtocolMoveMetadataMatches ==
    /\ DistinctStateMoveRows # {}
    /\ \A row \in DistinctStateMoveRows :
        /\ row.requiredSyncs =
            <<SyncStepDestinationDirectory, SyncStepSourceDirectory>>
        /\ row.beforeLinearizationFailure = FailureOutcomeNotCommitted
        /\ row.afterLinearizationFailure = FailureOutcomeOutcomeUnknown
        /\ row.resolverProbeTopology \in
            {ResolverProbeTopologySourceAndDestination,
             ResolverProbeTopologyReceiptCandidatesAndSource}
    /\ Cardinality(RenewMoveRows) = 1
    /\ \A row \in RenewMoveRows :
        /\ row.source = StateLeased
        /\ row.destination = StateLeased
        /\ row.requiredSyncs =
            <<SyncStepSameOrDestinationDirectory,
              SyncStepSourceDirectoryIfDistinct>>
        /\ row.beforeLinearizationFailure = FailureOutcomeNotCommitted
        /\ row.afterLinearizationFailure = FailureOutcomeOutcomeUnknown
        /\ row.resolverProbeTopology =
            ResolverProbeTopologySourceAndDestination

VARIABLES
    source,
    destination,
    durableSource,
    durableDestination,
    phase,
    linearized,
    committed,
    conflictSeen

Vars == <<source, destination, durableSource, durableDestination, phase,
          linearized, committed, conflictSeen>>

TypeInvariant ==
    /\ CrashProfile \in Profiles
    /\ ProtocolMoveMetadataMatches
    /\ source \in Objects
    /\ destination \in Objects
    /\ durableSource \in Objects
    /\ durableDestination \in Objects
    /\ phase \in Phases
    /\ linearized \in BOOLEAN
    /\ committed \in BOOLEAN
    /\ conflictSeen \in BOOLEAN

Init ==
    /\ source = SourceObject
    /\ destination = NoObject
    /\ durableSource = SourceObject
    /\ durableDestination = NoObject
    /\ phase = PhasePrepared
    /\ linearized = FALSE
    /\ committed = FALSE
    /\ conflictSeen = FALSE

PrepareConflictingDestination ==
    /\ phase = PhasePrepared
    /\ destination = NoObject
    /\ destination' = ConflictObject
    /\ durableDestination' = ConflictObject
    /\ conflictSeen' = TRUE
    /\ UNCHANGED <<source, durableSource, phase, linearized, committed>>

FailBeforeLinearization ==
    /\ phase = PhasePrepared
    /\ destination = NoObject
    /\ phase' = PhaseNotCommitted
    /\ UNCHANGED <<source, destination, durableSource, durableDestination,
                    linearized, committed, conflictSeen>>

Linearize ==
    /\ phase = PhasePrepared
    /\ source = SourceObject
    /\ destination = NoObject
    /\ source' = NoObject
    /\ destination' = SourceObject
    /\ phase' = PhaseLinearized
    /\ linearized' = TRUE
    /\ UNCHANGED <<durableSource, durableDestination, committed, conflictSeen>>

FailAfterLinearization ==
    /\ phase \in {PhaseLinearized, PhaseDestinationDurable}
    /\ phase' = PhaseOutcomeUnknown
    /\ UNCHANGED <<source, destination, durableSource, durableDestination,
                    linearized, committed, conflictSeen>>

DestinationDirectorySync ==
    /\ phase \in {PhaseLinearized, PhaseOutcomeUnknown}
    /\ destination = SourceObject
    /\ durableDestination' = destination
    /\ phase' = PhaseDestinationDurable
    /\ UNCHANGED <<source, destination, durableSource, linearized, committed,
                    conflictSeen>>

SourceDirectorySync ==
    /\ phase = PhaseDestinationDurable
    /\ source = NoObject
    /\ durableSource' = source
    /\ phase' = PhaseCommitted
    /\ committed' = TRUE
    /\ UNCHANGED <<source, destination, durableDestination, linearized,
                    conflictSeen>>

WeakPersistDestination ==
    /\ CrashProfile = ProfileWeak
    /\ phase \in {PhaseLinearized, PhaseOutcomeUnknown}
    /\ durableDestination' = destination
    /\ UNCHANGED <<source, destination, durableSource, phase, linearized,
                    committed, conflictSeen>>

WeakPersistSource ==
    /\ CrashProfile = ProfileWeak
    /\ phase \in {PhaseLinearized, PhaseOutcomeUnknown}
    /\ durableSource' = source
    /\ UNCHANGED <<source, destination, durableDestination, phase, linearized,
                    committed, conflictSeen>>

Crash ==
    /\ source' = durableSource
    /\ destination' = durableDestination
    /\ phase' = PhaseForObservation(
        ObservationOf(durableSource, durableDestination))
    /\ UNCHANGED <<durableSource, durableDestination, linearized, committed,
                    conflictSeen>>

ResolveBothSameDestination ==
    /\ phase \in ObservationPhases
    /\ source = SourceObject
    /\ destination = SourceObject
    /\ durableDestination' = destination
    /\ phase' = PhaseResolverDestinationDurable
    /\ UNCHANGED <<source, destination, durableSource, linearized, committed,
                    conflictSeen>>

ResolveRemoveExactSource ==
    /\ phase = PhaseResolverDestinationDurable
    /\ source = SourceObject
    /\ destination = SourceObject
    /\ source' = NoObject
    /\ phase' = PhaseResolverSourceRemoved
    /\ UNCHANGED <<destination, durableSource, durableDestination, linearized,
                    committed, conflictSeen>>

ResolveSourceDirectory ==
    /\ phase = PhaseResolverSourceRemoved
    /\ source = NoObject
    /\ durableSource' = source
    /\ phase' = PhaseCommitted
    /\ committed' = TRUE
    /\ UNCHANGED <<source, destination, durableDestination, linearized,
                    conflictSeen>>

ResolveDestinationOnly ==
    /\ phase \in ObservationPhases
    /\ source = NoObject
    /\ destination = SourceObject
    /\ durableSource' = source
    /\ durableDestination' = destination
    /\ phase' = PhaseCommitted
    /\ committed' = TRUE
    /\ UNCHANGED <<source, destination, linearized, conflictSeen>>

Next ==
    \/ PrepareConflictingDestination
    \/ FailBeforeLinearization
    \/ Linearize
    \/ FailAfterLinearization
    \/ DestinationDirectorySync
    \/ SourceDirectorySync
    \/ WeakPersistDestination
    \/ WeakPersistSource
    \/ Crash
    \/ ResolveBothSameDestination
    \/ ResolveRemoveExactSource
    \/ ResolveSourceDirectory
    \/ ResolveDestinationOnly

Observation == ObservationOf(source, destination)

Spec == Init /\ [][Next]_Vars

(* ---- Invariants ---- *)

BeforeLinearizationPreservesSource ==
    ~linearized => source = SourceObject

ConflictingDestinationIsNeverOverwritten ==
    conflictSeen =>
        /\ ~linearized
        /\ source = SourceObject
        /\ destination = ConflictObject

PostLinearizationFailureIsIndeterminate ==
    phase = PhaseOutcomeUnknown => linearized /\ ~committed

ObservedPhaseMatchesNamespace ==
    phase \in ObservationPhases => phase = PhaseForObservation(Observation)

CommittedIsDurableDestinationOnly ==
    committed =>
        /\ source = NoObject
        /\ destination = SourceObject
        /\ durableSource = NoObject
        /\ durableDestination = SourceObject

BothSameResolutionPreservesIdentity ==
    phase \in {PhaseResolverDestinationDurable, PhaseResolverSourceRemoved} =>
        /\ destination = SourceObject
        /\ source \in {SourceObject, NoObject}

BothDifferentIsNeverRepairable ==
    Observation = ObservationBothDifferent =>
        /\ ~ENABLED ResolveBothSameDestination
        /\ ~ENABLED ResolveRemoveExactSource
        /\ ~ENABLED ResolveSourceDirectory
        /\ ~ENABLED ResolveDestinationOnly

OrderedProfileExcludesNeither ==
    CrashProfile = ProfileOrdered => Observation # ObservationNeither

(* ---- End invariants ---- *)

=============================================================================
