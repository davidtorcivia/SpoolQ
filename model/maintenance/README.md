# Maintenance progress model

`SteadQMaintenance.tla` models one canonical recovery leaf and a separate persisted hierarchy-retry ledger. It follows production order: choose and attempt at most one directory retry, pay the recurring hierarchy traversal cost, then resume the main leaf scan. Retry, traversal, and leaf work consume one shared bounded pass budget.

The retry ledger contains directory open or enumeration work, not leaf jobs. Resolving a directory retry removes it and clears the phase cursor so the canonical scan restarts safely. A non-budget retry failure advances the rotating retry frontier while retaining the phase cursor. Budget exhaustion preserves both for replay. A crash restores only the last persisted cursor, ledger, and frontier, while already applied leaf work remains safe to replay.

The configuration starts with one permanently blocked directory retry and one transient directory retry. A pass can spend one of its five work units on a retry before spending three units reaching a saved leaf cursor, leaving one unit for canonical leaf progress. This matches `RecoveryScanBudget::minimum_for_progress`: one retry enumeration plus four canonical directory enumerations. One retry attempt loses its remaining budget without advancing the retry frontier, modeling a directory read or deadline that exhausts the pass. The failure bound makes later retry headroom explicit.

## Invariants checked

- `TypeInvariant`: constants and variables remain in their finite domains, including positive shared pass budget and disjoint bounded leaf and retry ranges.

- `VolatileCursorNeverSkipsWork`: every leaf entry at or before volatile `resume_after` is applied.

- `PersistedCursorNeverSkipsWork`: every leaf entry at or before durable `resume_after` is applied.

- `ClosedPassUsesOnlyPersistedProgress`: after a completed pass or reopen, cursor, retry ledger, and retry frontier equal their durable records and no session work remains.

- `AppliedEntriesAreIdempotent`: an applied leaf has exactly one authoritative application and a pending leaf has none, including after replay.

- `SharedBudgetAccountsForAllWork`: recurring hierarchy traversal, canonical leaf reads, and hierarchy retries consume the same positive pass budget.

- `RetryRunsBeforeCanonicalScan`: a selected directory retry executes before recurring hierarchy traversal or canonical leaf work.

- `LeafAndRetryDomainsAreDisjoint`: leaf entries cannot be mistaken for hierarchy-directory retry targets.

- `PermanentHierarchyRetryRemainsDeferred`: the designated persistent directory fault remains in both volatile and durable retry ledgers.

- `SelectedRetryIsPersistedHierarchyWork`: a pass can select only a directory target present in its retry ledger.

## Temporal properties checked

- `MainScanEventuallyCompletes`: recurring fair passes eventually scan through the bounded leaf despite shared budget and retry work.

- `LeafWorkEventuallyApplies`: every leaf entry is eventually applied after safe replay.

- `TransientHierarchyRetriesEventuallyClear`: whenever a transient directory retry is durable, rotating fair passes eventually resolve it.

- `ActivePassEventuallyCloses`: every active pass eventually publishes progress or is interrupted by a bounded crash.

The configuration checks every invariant and temporal property separately. The checksum-pinned TLA+ tools 1.7.4 artifact used by CI completes with 603 generated states and 405 distinct states, leaves no queued states, and reports no errors.

## Fairness and bounds

Weak fairness applies to pass start, pre-scan retry, recurring hierarchy traversal, canonical leaf scanning, retry-budget exhaustion, scan completion, and end-of-pass publication. Crashes are finite rather than fair. The model uses three leaf entries, two separate directory retries, three recurring traversal units, five shared work units per pass, one retry-budget failure, one transient retry failure, and at most one crash. No new leaf entries or retry targets arrive.

The liveness properties require capacity for one retry, the recurring traversal cost, and one canonical leaf unit, and require retry budget or deadline exhaustion to be finite. Production rejects a scan budget below this bound before filesystem work. Deadline availability remains an explicit environment assumption: the model does not claim progress when every pass deadline expires before retry execution.

## Not modeled

The model does not establish directory byte accounting, individual syscalls, wall or boottime acquisition, multiple hierarchy depths, phase rotation, retry-ledger overflow, cursor encoding, filesystem durability, or implementation equivalence. It models the production traversal, scan, retry-selection, retry-budget exhaustion, frontier, cursor-clear, persistence, and reopen order for one bounded phase.
