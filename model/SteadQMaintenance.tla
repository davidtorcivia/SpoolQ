--------------------------- MODULE SteadQMaintenance ---------------------------
(**************************************************************************)
(* Bounded production-shaped recovery cursor and hierarchy-retry model.  *)
(**************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    EntryCount,
    RetryCount,
    TraversalCost,
    PassBudget,
    MaxTransientFailures,
    MaxRetryBudgetFailures,
    MaxCrashes,
    BlockedRetry

Entries == 1..EntryCount
RetryTargets == (EntryCount + 1)..(EntryCount + RetryCount)
CursorValues == 0..EntryCount
RetryFrontiers == {0} \cup RetryTargets

Pending == "pending"
Applied == "applied"
EntryStates == {Pending, Applied}

TransientRetries == RetryTargets \ {BlockedRetry}

Minimum(set) == CHOOSE value \in set :
    \A other \in set : value <= other

NextRetry(ledger, frontier) ==
    LET after == {target \in ledger : target > frontier} IN
        IF after # {} THEN Minimum(after) ELSE Minimum(ledger)

VARIABLES
    entryState,
    applyCount,
    retryFailuresRemaining,
    cursor,
    durableCursor,
    retryLedger,
    durableRetryLedger,
    retryFrontier,
    durableRetryFrontier,
    selectedRetry,
    active,
    cycleFinished,
    traversalRemaining,
    budget,
    workUsed,
    retryBudgetFailuresRemaining,
    crashesRemaining,
    fullScanSeen,
    retryOrderSound

Vars == <<entryState, applyCount, retryFailuresRemaining, cursor,
          durableCursor, retryLedger, durableRetryLedger, retryFrontier,
          durableRetryFrontier, selectedRetry, active, cycleFinished, budget,
          traversalRemaining, workUsed, retryBudgetFailuresRemaining,
          crashesRemaining, fullScanSeen, retryOrderSound>>

TypeInvariant ==
    /\ EntryCount \in Nat \ {0}
    /\ RetryCount \in Nat \ {0}
    /\ TraversalCost \in Nat \ {0}
    /\ PassBudget \in 1..(TraversalCost + EntryCount)
    /\ MaxTransientFailures \in Nat
    /\ MaxRetryBudgetFailures \in Nat
    /\ MaxCrashes \in Nat
    /\ BlockedRetry \in RetryTargets
    /\ entryState \in [Entries -> EntryStates]
    /\ applyCount \in [Entries -> 0..1]
    /\ retryFailuresRemaining \in
        [RetryTargets -> 0..MaxTransientFailures]
    /\ cursor \in CursorValues
    /\ durableCursor \in CursorValues
    /\ retryLedger \subseteq RetryTargets
    /\ durableRetryLedger \subseteq RetryTargets
    /\ retryFrontier \in RetryFrontiers
    /\ durableRetryFrontier \in RetryFrontiers
    /\ selectedRetry \in RetryFrontiers
    /\ active \in BOOLEAN
    /\ cycleFinished \in BOOLEAN
    /\ traversalRemaining \in 0..TraversalCost
    /\ budget \in 0..PassBudget
    /\ workUsed \in 0..PassBudget
    /\ retryBudgetFailuresRemaining \in 0..MaxRetryBudgetFailures
    /\ crashesRemaining \in 0..MaxCrashes
    /\ fullScanSeen \in BOOLEAN
    /\ retryOrderSound \in BOOLEAN

Init ==
    /\ entryState = [entry \in Entries |-> Pending]
    /\ applyCount = [entry \in Entries |-> 0]
    /\ retryFailuresRemaining = [target \in RetryTargets |->
        IF target = BlockedRetry THEN 0 ELSE MaxTransientFailures]
    /\ cursor = 0
    /\ durableCursor = 0
    /\ retryLedger = RetryTargets
    /\ durableRetryLedger = RetryTargets
    /\ retryFrontier = 0
    /\ durableRetryFrontier = 0
    /\ selectedRetry = 0
    /\ active = FALSE
    /\ cycleFinished = FALSE
    /\ traversalRemaining = 0
    /\ budget = 0
    /\ workUsed = 0
    /\ retryBudgetFailuresRemaining = MaxRetryBudgetFailures
    /\ crashesRemaining = MaxCrashes
    /\ fullScanSeen = FALSE
    /\ retryOrderSound = TRUE

BeginPass ==
    /\ ~active
    /\ cursor' = durableCursor
    /\ retryLedger' = durableRetryLedger
    /\ retryFrontier' = durableRetryFrontier
    /\ selectedRetry' =
        IF durableRetryLedger = {}
        THEN 0
        ELSE NextRetry(durableRetryLedger, durableRetryFrontier)
    /\ active' = TRUE
    /\ cycleFinished' = FALSE
    /\ traversalRemaining' = TraversalCost
    /\ budget' = PassBudget
    /\ workUsed' = 0
    /\ UNCHANGED <<entryState, applyCount, retryFailuresRemaining,
                    durableCursor, durableRetryLedger, durableRetryFrontier,
                    retryBudgetFailuresRemaining, crashesRemaining,
                    fullScanSeen, retryOrderSound>>

TraverseHierarchy ==
    /\ active
    /\ ~cycleFinished
    /\ traversalRemaining > 0
    /\ budget > 0
    /\ traversalRemaining' = traversalRemaining - 1
    /\ budget' = budget - 1
    /\ workUsed' = workUsed + 1
    /\ UNCHANGED <<entryState, applyCount, retryFailuresRemaining, cursor,
                    durableCursor, retryLedger, durableRetryLedger,
                    retryFrontier, durableRetryFrontier, selectedRetry,
                    active, cycleFinished, retryBudgetFailuresRemaining,
                    crashesRemaining, fullScanSeen, retryOrderSound>>

ReplayAppliedEntry(entry) ==
    /\ entryState[entry] = Applied
    /\ cursor' = entry
    /\ budget' = budget - 1
    /\ workUsed' = workUsed + 1
    /\ fullScanSeen' = (fullScanSeen \/ (entry = EntryCount))
    /\ UNCHANGED <<entryState, applyCount, retryFailuresRemaining,
                    durableCursor, retryLedger, durableRetryLedger,
                    retryFrontier, durableRetryFrontier, selectedRetry,
                    active, cycleFinished, traversalRemaining,
                    retryBudgetFailuresRemaining, crashesRemaining,
                    retryOrderSound>>

ApplyEntry(entry) ==
    /\ entryState[entry] = Pending
    /\ entryState' = [entryState EXCEPT ![entry] = Applied]
    /\ applyCount' = [applyCount EXCEPT ![entry] = @ + 1]
    /\ cursor' = entry
    /\ budget' = budget - 1
    /\ workUsed' = workUsed + 1
    /\ fullScanSeen' = (fullScanSeen \/ (entry = EntryCount))
    /\ UNCHANGED <<retryFailuresRemaining, durableCursor, retryLedger,
                    durableRetryLedger, retryFrontier, durableRetryFrontier,
                    selectedRetry, active, cycleFinished, crashesRemaining,
                    traversalRemaining, retryBudgetFailuresRemaining,
                    retryOrderSound>>

ScanNext ==
    /\ active
    /\ ~cycleFinished
    /\ traversalRemaining = 0
    /\ budget > 0
    /\ cursor < EntryCount
    /\ LET entry == cursor + 1 IN
        \/ ReplayAppliedEntry(entry)
        \/ ApplyEntry(entry)

FailBlockedRetry ==
    /\ selectedRetry = BlockedRetry
    /\ retryFrontier' = selectedRetry
    /\ selectedRetry' = 0
    /\ cursor' = 0
    /\ cycleFinished' = TRUE
    /\ budget' = budget - 1
    /\ workUsed' = workUsed + 1
    /\ retryOrderSound' = (retryOrderSound /\ (cursor = EntryCount))
    /\ UNCHANGED <<entryState, applyCount, retryFailuresRemaining,
                    durableCursor, retryLedger, durableRetryLedger,
                    durableRetryFrontier, active, crashesRemaining,
                    traversalRemaining, retryBudgetFailuresRemaining,
                    fullScanSeen>>

FailTransientRetry ==
    /\ selectedRetry \in TransientRetries
    /\ retryFailuresRemaining[selectedRetry] > 0
    /\ retryFailuresRemaining' =
        [retryFailuresRemaining EXCEPT ![selectedRetry] = @ - 1]
    /\ retryFrontier' = selectedRetry
    /\ selectedRetry' = 0
    /\ cursor' = 0
    /\ cycleFinished' = TRUE
    /\ budget' = budget - 1
    /\ workUsed' = workUsed + 1
    /\ retryOrderSound' = (retryOrderSound /\ (cursor = EntryCount))
    /\ UNCHANGED <<entryState, applyCount, durableCursor, retryLedger,
                    durableRetryLedger, durableRetryFrontier, active,
                    traversalRemaining, retryBudgetFailuresRemaining,
                    crashesRemaining, fullScanSeen>>

ResolveTransientRetry ==
    /\ selectedRetry \in TransientRetries
    /\ retryFailuresRemaining[selectedRetry] = 0
    /\ LET remaining == retryLedger \ {selectedRetry} IN
        /\ retryLedger' = remaining
        /\ retryFrontier' = IF remaining = {} THEN 0 ELSE selectedRetry
    /\ selectedRetry' = 0
    /\ cursor' = 0
    /\ cycleFinished' = TRUE
    /\ budget' = budget - 1
    /\ workUsed' = workUsed + 1
    /\ retryOrderSound' = (retryOrderSound /\ (cursor = EntryCount))
    /\ UNCHANGED <<entryState, applyCount, retryFailuresRemaining,
                    durableCursor, durableRetryLedger, durableRetryFrontier,
                    active, traversalRemaining, retryBudgetFailuresRemaining,
                    crashesRemaining, fullScanSeen>>

RetryAfterScan ==
    /\ active
    /\ ~cycleFinished
    /\ traversalRemaining = 0
    /\ cursor = EntryCount
    /\ selectedRetry # 0
    /\ budget > 0
    /\ \/ FailBlockedRetry
       \/ FailTransientRetry
       \/ ResolveTransientRetry

ExhaustRetryBudget ==
    /\ active
    /\ ~cycleFinished
    /\ traversalRemaining = 0
    /\ cursor = EntryCount
    /\ selectedRetry # 0
    /\ budget > 0
    /\ retryBudgetFailuresRemaining > 0
    /\ retryBudgetFailuresRemaining' = retryBudgetFailuresRemaining - 1
    /\ workUsed' = workUsed + budget
    /\ budget' = 0
    /\ UNCHANGED <<entryState, applyCount, retryFailuresRemaining, cursor,
                    durableCursor, retryLedger, durableRetryLedger,
                    retryFrontier, durableRetryFrontier, selectedRetry,
                    active, cycleFinished, traversalRemaining,
                    crashesRemaining, fullScanSeen, retryOrderSound>>

FinishScanWithoutRetry ==
    /\ active
    /\ ~cycleFinished
    /\ traversalRemaining = 0
    /\ cursor = EntryCount
    /\ selectedRetry = 0
    /\ cursor' = 0
    /\ cycleFinished' = TRUE
    /\ UNCHANGED <<entryState, applyCount, retryFailuresRemaining,
                    durableCursor, retryLedger, durableRetryLedger,
                    retryFrontier, durableRetryFrontier, selectedRetry,
                    active, traversalRemaining, budget, workUsed,
                    retryBudgetFailuresRemaining, crashesRemaining,
                    fullScanSeen, retryOrderSound>>

EndPass ==
    /\ active
    /\ \/ budget = 0
       \/ cycleFinished
    /\ durableCursor' = cursor
    /\ durableRetryLedger' = retryLedger
    /\ durableRetryFrontier' = retryFrontier
    /\ selectedRetry' = 0
    /\ active' = FALSE
    /\ cycleFinished' = FALSE
    /\ traversalRemaining' = 0
    /\ budget' = 0
    /\ workUsed' = 0
    /\ UNCHANGED <<entryState, applyCount, retryFailuresRemaining, cursor,
                    retryLedger, retryFrontier, retryBudgetFailuresRemaining,
                    crashesRemaining, fullScanSeen, retryOrderSound>>

CrashAndReopen ==
    /\ crashesRemaining > 0
    /\ cursor' = durableCursor
    /\ retryLedger' = durableRetryLedger
    /\ retryFrontier' = durableRetryFrontier
    /\ selectedRetry' = 0
    /\ active' = FALSE
    /\ cycleFinished' = FALSE
    /\ traversalRemaining' = 0
    /\ budget' = 0
    /\ workUsed' = 0
    /\ crashesRemaining' = crashesRemaining - 1
    /\ UNCHANGED <<entryState, applyCount, retryFailuresRemaining,
                    durableCursor, durableRetryLedger, durableRetryFrontier,
                    retryBudgetFailuresRemaining, fullScanSeen,
                    retryOrderSound>>

Next ==
    \/ BeginPass
    \/ TraverseHierarchy
    \/ ScanNext
    \/ RetryAfterScan
    \/ ExhaustRetryBudget
    \/ FinishScanWithoutRetry
    \/ EndPass
    \/ CrashAndReopen

Spec == Init /\ [][Next]_Vars

FairSpec ==
    /\ Spec
    /\ WF_Vars(BeginPass)
    /\ WF_Vars(TraverseHierarchy)
    /\ WF_Vars(ScanNext)
    /\ WF_Vars(RetryAfterScan)
    /\ WF_Vars(ExhaustRetryBudget)
    /\ WF_Vars(FinishScanWithoutRetry)
    /\ WF_Vars(EndPass)

SafeThrough(frontier) ==
    \A entry \in Entries :
        entry <= frontier => entryState[entry] = Applied

(* ---- Invariants ---- *)

VolatileCursorNeverSkipsWork ==
    SafeThrough(cursor)

PersistedCursorNeverSkipsWork ==
    SafeThrough(durableCursor)

ClosedPassUsesOnlyPersistedProgress ==
    ~active =>
        /\ cursor = durableCursor
        /\ retryLedger = durableRetryLedger
        /\ retryFrontier = durableRetryFrontier
        /\ selectedRetry = 0
        /\ ~cycleFinished
        /\ traversalRemaining = 0
        /\ budget = 0

AppliedEntriesAreIdempotent ==
    \A entry \in Entries :
        (entryState[entry] = Applied) <=> (applyCount[entry] = 1)

SharedBudgetAccountsForAllWork ==
    IF active
    THEN budget + workUsed = PassBudget
    ELSE workUsed = 0

RetryRunsOnlyAfterFullScan ==
    retryOrderSound

LeafAndRetryDomainsAreDisjoint ==
    /\ retryLedger \cap Entries = {}
    /\ durableRetryLedger \cap Entries = {}
    /\ selectedRetry = 0 \/ selectedRetry \notin Entries

PermanentHierarchyRetryRemainsDeferred ==
    /\ BlockedRetry \in retryLedger
    /\ BlockedRetry \in durableRetryLedger

SelectedRetryIsPersistedHierarchyWork ==
    selectedRetry # 0 => selectedRetry \in retryLedger

(* ---- End invariants ---- *)

(* ---- Properties ---- *)

MainScanEventuallyCompletes ==
    <> fullScanSeen

LeafWorkEventuallyApplies ==
    <> (\A entry \in Entries : entryState[entry] = Applied)

TransientHierarchyRetriesEventuallyClear ==
    [] ((durableRetryLedger \cap TransientRetries # {}) =>
        <> (durableRetryLedger \cap TransientRetries = {}))

ActivePassEventuallyCloses ==
    [] (active => <> ~active)

(* ---- End properties ---- *)

=============================================================================
