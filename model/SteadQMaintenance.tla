--------------------------- MODULE SteadQMaintenance ---------------------------
(**************************************************************************)
(* Bounded persisted recovery progress, replay, and retry-frontier model. *)
(**************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    EntryCount,
    PassBudget,
    MaxTransientFailures,
    MaxCrashes,
    BlockedEntry

Entries == 1..EntryCount
CursorValues == 0..EntryCount

Pending == "pending"
Applied == "applied"
EntryStates == {Pending, Applied}

EligibleEntries == Entries \ {BlockedEntry}

Minimum(set) == CHOOSE entry \in set :
    \A other \in set : entry <= other

NextRetry(ledger, frontier) ==
    LET after == {entry \in ledger : entry > frontier} IN
        IF after # {} THEN Minimum(after) ELSE Minimum(ledger)

VARIABLES
    entryState,
    applyCount,
    failuresRemaining,
    cursor,
    durableCursor,
    retryLedger,
    durableRetryLedger,
    retryFrontier,
    durableRetryFrontier,
    active,
    retryAttempted,
    budget,
    crashesRemaining

Vars == <<entryState, applyCount, failuresRemaining, cursor,
          durableCursor, retryLedger, durableRetryLedger, retryFrontier,
          durableRetryFrontier, active, retryAttempted, budget,
          crashesRemaining>>

TypeInvariant ==
    /\ EntryCount \in Nat \ {0}
    /\ PassBudget \in 1..EntryCount
    /\ MaxTransientFailures \in Nat
    /\ MaxCrashes \in Nat
    /\ BlockedEntry \in Entries
    /\ entryState \in [Entries -> EntryStates]
    /\ applyCount \in [Entries -> 0..1]
    /\ failuresRemaining \in [Entries -> 0..MaxTransientFailures]
    /\ cursor \in CursorValues
    /\ durableCursor \in CursorValues
    /\ retryLedger \subseteq Entries
    /\ durableRetryLedger \subseteq Entries
    /\ retryFrontier \in CursorValues
    /\ durableRetryFrontier \in CursorValues
    /\ active \in BOOLEAN
    /\ retryAttempted \in BOOLEAN
    /\ budget \in 0..PassBudget
    /\ crashesRemaining \in 0..MaxCrashes

Init ==
    /\ entryState = [entry \in Entries |-> Pending]
    /\ applyCount = [entry \in Entries |-> 0]
    /\ failuresRemaining = [entry \in Entries |->
        IF entry = BlockedEntry THEN 0 ELSE MaxTransientFailures]
    /\ cursor = 0
    /\ durableCursor = 0
    /\ retryLedger = {}
    /\ durableRetryLedger = {}
    /\ retryFrontier = 0
    /\ durableRetryFrontier = 0
    /\ active = FALSE
    /\ retryAttempted = FALSE
    /\ budget = 0
    /\ crashesRemaining = MaxCrashes

BeginPass ==
    /\ ~active
    /\ cursor' = durableCursor
    /\ retryLedger' = durableRetryLedger
    /\ retryFrontier' = durableRetryFrontier
    /\ active' = TRUE
    /\ retryAttempted' = FALSE
    /\ budget' = PassBudget
    /\ UNCHANGED <<entryState, applyCount, failuresRemaining,
                    durableCursor, durableRetryLedger, durableRetryFrontier,
                    crashesRemaining>>

NoRetry ==
    /\ retryLedger = {}
    /\ retryAttempted' = TRUE
    /\ UNCHANGED <<entryState, applyCount, failuresRemaining, cursor,
                    durableCursor, retryLedger, durableRetryLedger,
                    retryFrontier, durableRetryFrontier, active, budget,
                    crashesRemaining>>

DiscardAppliedRetry(entry) ==
    /\ entry = NextRetry(retryLedger, retryFrontier)
    /\ entryState[entry] = Applied
    /\ retryLedger' = retryLedger \ {entry}
    /\ retryFrontier' = entry
    /\ retryAttempted' = TRUE
    /\ UNCHANGED <<entryState, applyCount, failuresRemaining, cursor,
                    durableCursor, durableRetryLedger, durableRetryFrontier,
                    active, budget, crashesRemaining>>

DeferBlockedRetry(entry) ==
    /\ entry = NextRetry(retryLedger, retryFrontier)
    /\ entry = BlockedEntry
    /\ retryFrontier' = entry
    /\ retryAttempted' = TRUE
    /\ UNCHANGED <<entryState, applyCount, failuresRemaining, cursor,
                    durableCursor, retryLedger, durableRetryLedger,
                    durableRetryFrontier, active, budget, crashesRemaining>>

FailTransientRetry(entry) ==
    /\ entry = NextRetry(retryLedger, retryFrontier)
    /\ entry \in EligibleEntries
    /\ entryState[entry] = Pending
    /\ failuresRemaining[entry] > 0
    /\ failuresRemaining' =
        [failuresRemaining EXCEPT ![entry] = @ - 1]
    /\ retryFrontier' = entry
    /\ retryAttempted' = TRUE
    /\ UNCHANGED <<entryState, applyCount, cursor, durableCursor,
                    retryLedger, durableRetryLedger, durableRetryFrontier,
                    active, budget, crashesRemaining>>

ApplyRetry(entry) ==
    /\ entry = NextRetry(retryLedger, retryFrontier)
    /\ entry \in EligibleEntries
    /\ entryState[entry] = Pending
    /\ failuresRemaining[entry] = 0
    /\ entryState' = [entryState EXCEPT ![entry] = Applied]
    /\ applyCount' = [applyCount EXCEPT ![entry] = @ + 1]
    /\ retryLedger' = retryLedger \ {entry}
    /\ retryFrontier' = entry
    /\ retryAttempted' = TRUE
    /\ UNCHANGED <<failuresRemaining, cursor, durableCursor,
                    durableRetryLedger, durableRetryFrontier, active, budget,
                    crashesRemaining>>

PrepareRetry ==
    /\ active
    /\ ~retryAttempted
    /\ \/ NoRetry
       \/ \E entry \in retryLedger :
            \/ DiscardAppliedRetry(entry)
            \/ DeferBlockedRetry(entry)
            \/ FailTransientRetry(entry)
            \/ ApplyRetry(entry)

ReplayAppliedEntry(entry) ==
    /\ entryState[entry] = Applied
    /\ cursor' = entry
    /\ budget' = budget - 1
    /\ UNCHANGED <<entryState, applyCount, failuresRemaining, durableCursor,
                    retryLedger, durableRetryLedger, retryFrontier,
                    durableRetryFrontier, active, retryAttempted,
                    crashesRemaining>>

DeferBlockedEntry(entry) ==
    /\ entry = BlockedEntry
    /\ entryState[entry] = Pending
    /\ cursor' = entry
    /\ retryLedger' = retryLedger \cup {entry}
    /\ budget' = budget - 1
    /\ UNCHANGED <<entryState, applyCount, failuresRemaining, durableCursor,
                    durableRetryLedger, retryFrontier, durableRetryFrontier,
                    active, retryAttempted, crashesRemaining>>

FailTransientEntry(entry) ==
    /\ entry \in EligibleEntries
    /\ entryState[entry] = Pending
    /\ failuresRemaining[entry] > 0
    /\ failuresRemaining' =
        [failuresRemaining EXCEPT ![entry] = @ - 1]
    /\ cursor' = entry
    /\ retryLedger' = retryLedger \cup {entry}
    /\ budget' = budget - 1
    /\ UNCHANGED <<entryState, applyCount, durableCursor,
                    durableRetryLedger, retryFrontier, durableRetryFrontier,
                    active, retryAttempted, crashesRemaining>>

ApplyEntry(entry) ==
    /\ entry \in EligibleEntries
    /\ entryState[entry] = Pending
    /\ failuresRemaining[entry] = 0
    /\ entryState' = [entryState EXCEPT ![entry] = Applied]
    /\ applyCount' = [applyCount EXCEPT ![entry] = @ + 1]
    /\ cursor' = entry
    /\ retryLedger' = retryLedger \ {entry}
    /\ budget' = budget - 1
    /\ UNCHANGED <<failuresRemaining, durableCursor, durableRetryLedger,
                    retryFrontier, durableRetryFrontier, active,
                    retryAttempted, crashesRemaining>>

ScanNext ==
    /\ active
    /\ retryAttempted
    /\ budget > 0
    /\ cursor < EntryCount
    /\ LET entry == cursor + 1 IN
        \/ ReplayAppliedEntry(entry)
        \/ DeferBlockedEntry(entry)
        \/ FailTransientEntry(entry)
        \/ ApplyEntry(entry)

EndPass ==
    /\ active
    /\ retryAttempted
    /\ \/ budget = 0
       \/ cursor = EntryCount
    /\ durableCursor' = cursor
    /\ durableRetryLedger' = retryLedger
    /\ durableRetryFrontier' = retryFrontier
    /\ active' = FALSE
    /\ retryAttempted' = FALSE
    /\ budget' = 0
    /\ UNCHANGED <<entryState, applyCount, failuresRemaining, cursor,
                    retryLedger, retryFrontier, crashesRemaining>>

CrashAndReopen ==
    /\ crashesRemaining > 0
    /\ cursor' = durableCursor
    /\ retryLedger' = durableRetryLedger
    /\ retryFrontier' = durableRetryFrontier
    /\ active' = FALSE
    /\ retryAttempted' = FALSE
    /\ budget' = 0
    /\ crashesRemaining' = crashesRemaining - 1
    /\ UNCHANGED <<entryState, applyCount, failuresRemaining,
                    durableCursor, durableRetryLedger, durableRetryFrontier>>

Next ==
    \/ BeginPass
    \/ PrepareRetry
    \/ ScanNext
    \/ EndPass
    \/ CrashAndReopen

Spec == Init /\ [][Next]_Vars

FairSpec ==
    /\ Spec
    /\ WF_Vars(BeginPass)
    /\ WF_Vars(PrepareRetry)
    /\ WF_Vars(ScanNext)
    /\ WF_Vars(EndPass)

ClassifiedOrDeferred(entry, ledger) ==
    \/ entryState[entry] = Applied
    \/ entry \in ledger

SafeThrough(frontier, ledger) ==
    \A entry \in Entries :
        entry <= frontier => ClassifiedOrDeferred(entry, ledger)

(* ---- Invariants ---- *)

VolatileCursorNeverSkipsWork ==
    SafeThrough(cursor, retryLedger)

PersistedCursorNeverSkipsWork ==
    SafeThrough(durableCursor, durableRetryLedger)

PersistedProgressNeverExceedsVolatileProgress ==
    durableCursor <= cursor

ClosedPassUsesOnlyPersistedProgress ==
    ~active =>
        /\ cursor = durableCursor
        /\ retryLedger = durableRetryLedger
        /\ retryFrontier = durableRetryFrontier
        /\ ~retryAttempted
        /\ budget = 0

AppliedEntriesAreIdempotent ==
    \A entry \in Entries :
        (entryState[entry] = Applied) <=> (applyCount[entry] = 1)

PermanentBlockNeverBecomesApplied ==
    /\ entryState[BlockedEntry] = Pending
    /\ applyCount[BlockedEntry] = 0

(* ---- End invariants ---- *)

(* ---- Properties ---- *)

PersistedMainScanEventuallyCompletes ==
    <> (durableCursor = EntryCount)

EligibleWorkEventuallyApplies ==
    <> (\A entry \in EligibleEntries : entryState[entry] = Applied)

TransientRetriesEventuallyClear ==
    [] ((durableRetryLedger \cap EligibleEntries # {}) =>
        <> (durableRetryLedger \cap EligibleEntries = {}))

ActivePassEventuallyCloses ==
    [] (active => <> ~active)

(* ---- End properties ---- *)

=============================================================================
