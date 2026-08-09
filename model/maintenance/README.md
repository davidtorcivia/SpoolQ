# Maintenance progress model

`SteadQMaintenance.tla` models one canonical recovery leaf and a separate persisted hierarchy-retry ledger. It follows production order: choose one directory retry, resume the main leaf scan, then attempt the chosen retry only after the scan reaches its end. Leaf scanning and retry work consume one shared bounded pass budget.

The retry ledger contains directory open or enumeration work, not leaf jobs. Resolving a directory retry removes it and clears the phase cursor so a later pass rescans the leaf. A failed retry advances the rotating retry frontier and also clears the phase cursor. A crash restores only the last persisted cursor, ledger, and frontier, while already applied leaf work remains safe to replay.

The configuration starts with one permanently blocked directory retry and one transient directory retry. The shared pass budget is one unit, so partial leaf progress must persist across passes before any retry can run.

## Invariants checked

- `TypeInvariant`: constants and variables remain in their finite domains, including positive shared pass budget and disjoint bounded leaf and retry ranges.

- `VolatileCursorNeverSkipsWork`: every leaf entry at or before volatile `resume_after` is applied.

- `PersistedCursorNeverSkipsWork`: every leaf entry at or before durable `resume_after` is applied.

- `ClosedPassUsesOnlyPersistedProgress`: after a completed pass or reopen, cursor, retry ledger, and retry frontier equal their durable records and no session work remains.

- `AppliedEntriesAreIdempotent`: an applied leaf has exactly one authoritative application and a pending leaf has none, including after replay.

- `SharedBudgetAccountsForAllWork`: canonical leaf reads and hierarchy retries consume the same positive pass budget.

- `RetryRunsOnlyAfterFullScan`: a selected directory retry cannot execute before the canonical leaf scan reaches its end.

- `LeafAndRetryDomainsAreDisjoint`: leaf entries cannot be mistaken for hierarchy-directory retry targets.

- `PermanentHierarchyRetryRemainsDeferred`: the designated persistent directory fault remains in both volatile and durable retry ledgers.

- `SelectedRetryIsPersistedHierarchyWork`: a pass can select only a directory target present in its retry ledger.

## Temporal properties checked

- `MainScanEventuallyCompletes`: recurring fair passes eventually scan through the bounded leaf despite shared budget and retry work.

- `LeafWorkEventuallyApplies`: every leaf entry is eventually applied after safe replay.

- `TransientHierarchyRetriesEventuallyClear`: whenever a transient directory retry is durable, rotating fair passes eventually resolve it.

- `ActivePassEventuallyCloses`: every active pass eventually publishes progress or is interrupted by a bounded crash.

The configuration checks every invariant and temporal property separately. The checksum-pinned TLA+ tools 1.7.4 artifact used by CI completes with 235 generated states and 150 distinct states, leaves no queued states, and reports no errors.

## Fairness and bounds

Weak fairness applies to pass start, canonical leaf scanning, post-scan retry, no-retry completion, and end-of-pass publication. Crashes are finite rather than fair. The model uses three leaf entries, two separate directory retries, one transient failure, one unit of shared pass budget, and at most one crash. No new leaf entries or retry targets arrive.

## Not modeled

The model does not establish directory byte accounting, individual syscalls, wall or boottime acquisition, multiple hierarchy depths, phase rotation, retry-ledger overflow, cursor encoding, filesystem durability, or implementation equivalence. It models the production scan, retry-selection, frontier, cursor-clear, budget, persistence, and reopen order for one bounded phase.
