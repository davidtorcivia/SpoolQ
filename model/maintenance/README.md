# Maintenance progress model

`SteadQMaintenance.tla` models one canonical recovery leaf and a separate persisted hierarchy-retry ledger. It follows production order: choose one directory retry, pay the recurring hierarchy traversal cost, resume the main leaf scan, then attempt the chosen retry only after the scan reaches its end. Traversal, leaf scanning, and retry work consume one shared bounded pass budget.

The retry ledger contains directory open or enumeration work, not leaf jobs. Resolving a directory retry removes it and clears the phase cursor so a later pass rescans the leaf. A completed retry with a non-budget failure advances the rotating retry frontier and also clears the phase cursor. Budget exhaustion preserves both for replay. A crash restores only the last persisted cursor, ledger, and frontier, while already applied leaf work remains safe to replay.

The configuration starts with one permanently blocked directory retry and one transient directory retry. Every pass spends four of its five work units reaching the saved cursor, matching `RecoveryScanBudget::minimum_for_progress` and leaving one unit for leaf work or retry. One retry attempt loses its remaining budget without advancing the retry frontier, modeling a directory read or deadline that exhausts the pass. The failure bound makes later retry headroom explicit.

## Invariants checked

- `TypeInvariant`: constants and variables remain in their finite domains, including positive shared pass budget and disjoint bounded leaf and retry ranges.

- `VolatileCursorNeverSkipsWork`: every leaf entry at or before volatile `resume_after` is applied.

- `PersistedCursorNeverSkipsWork`: every leaf entry at or before durable `resume_after` is applied.

- `ClosedPassUsesOnlyPersistedProgress`: after a completed pass or reopen, cursor, retry ledger, and retry frontier equal their durable records and no session work remains.

- `AppliedEntriesAreIdempotent`: an applied leaf has exactly one authoritative application and a pending leaf has none, including after replay.

- `SharedBudgetAccountsForAllWork`: recurring hierarchy traversal, canonical leaf reads, and hierarchy retries consume the same positive pass budget.

- `RetryRunsOnlyAfterFullScan`: a selected directory retry cannot execute before the canonical leaf scan reaches its end.

- `LeafAndRetryDomainsAreDisjoint`: leaf entries cannot be mistaken for hierarchy-directory retry targets.

- `PermanentHierarchyRetryRemainsDeferred`: the designated persistent directory fault remains in both volatile and durable retry ledgers.

- `SelectedRetryIsPersistedHierarchyWork`: a pass can select only a directory target present in its retry ledger.

## Temporal properties checked

- `MainScanEventuallyCompletes`: recurring fair passes eventually scan through the bounded leaf despite shared budget and retry work.

- `LeafWorkEventuallyApplies`: every leaf entry is eventually applied after safe replay.

- `TransientHierarchyRetriesEventuallyClear`: whenever a transient directory retry is durable, rotating fair passes eventually resolve it.

- `ActivePassEventuallyCloses`: every active pass eventually publishes progress or is interrupted by a bounded crash.

The configuration checks every invariant and temporal property separately. The checksum-pinned TLA+ tools 1.7.4 artifact used by CI completes with 1,018 generated states and 660 distinct states, leaves no queued states, and reports no errors.

## Fairness and bounds

Weak fairness applies to pass start, recurring hierarchy traversal, canonical leaf scanning, post-scan retry, retry-budget exhaustion, no-retry completion, and end-of-pass publication. Crashes are finite rather than fair. The model uses three leaf entries, two separate directory retries, four recurring traversal units, five shared work units per pass, one retry-budget failure, one transient retry failure, and at most one crash. No new leaf entries or retry targets arrive.

The liveness properties require each pass budget to exceed the recurring traversal cost and require retry budget or deadline exhaustion to be finite. Production rejects a scan budget below this bound before filesystem work. Deadline availability remains an explicit environment assumption: the model does not claim progress when every pass deadline expires before retry execution.

## Not modeled

The model does not establish directory byte accounting, individual syscalls, wall or boottime acquisition, multiple hierarchy depths, phase rotation, retry-ledger overflow, cursor encoding, filesystem durability, or implementation equivalence. It models the production traversal, scan, retry-selection, retry-budget exhaustion, frontier, cursor-clear, persistence, and reopen order for one bounded phase.
