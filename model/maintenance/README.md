# Maintenance progress model

`SteadQMaintenance.tla` is a bounded model of one canonically ordered recovery leaf, exact `resume_after` progress, a persisted retry ledger and rotating retry frontier, finite transient faults, one permanently blocked entry, bounded positive pass work, and crash-safe reopen. It represents the recovery semantics implemented under issue #55; it does not introduce another recovery algorithm.

Each pass attempts at most one persisted retry before using an independent positive entry-classification budget. An entry advances the volatile cursor only after it is durably applied or recorded in the retry ledger. End-of-pass publication copies the volatile cursor, ledger, and retry frontier into their durable forms. A crash reopens the durable forms, so incomplete cursor publication causes safe replay. Applied entries retain an exact-once count, making replay idempotence observable.

The permanently blocked entry is first in canonical order. Eligible entries each encounter one transient failure. This deliberately exercises the starvation case: repeated retry of the blocked prefix cannot consume the main scan's entry budget, and the rotating frontier cannot pin later eligible retries.

## Invariants checked

- `TypeInvariant`: constants and variables remain in their finite domains, including a nonzero leaf, positive pass budget, bounded crashes, and a valid blocked entry.

- `VolatileCursorNeverSkipsWork`: every entry at or before volatile `resume_after` is applied or present in the volatile retry ledger.

- `PersistedCursorNeverSkipsWork`: every entry at or before durable `resume_after` is applied or present in the durable retry ledger.

- `PersistedProgressNeverExceedsVolatileProgress`: durable progress cannot move ahead of the in-memory classification frontier.

- `ClosedPassUsesOnlyPersistedProgress`: after a completed pass or reopen, cursor, retry ledger, and retry frontier equal their durable records and no work budget remains active.

- `AppliedEntriesAreIdempotent`: an applied entry has exactly one authoritative application and a pending entry has none, including after replay.

- `PermanentBlockNeverBecomesApplied`: the designated persistent fault remains deferred and cannot be mistaken for completed work.

## Temporal properties checked

- `PersistedMainScanEventuallyCompletes`: recurring fair passes with positive budget durably classify through the bounded leaf despite the blocked first entry and finite crashes.

- `EligibleWorkEventuallyApplies`: every non-blocked entry is eventually applied after its finite transient failures.

- `TransientRetriesEventuallyClear`: whenever the persisted retry ledger contains an eligible entry, it eventually clears every eligible retry; only the permanent block may remain.

- `ActivePassEventuallyCloses`: every active pass eventually publishes progress or is interrupted by one of the finitely bounded crashes.

The configuration checks every invariant and temporal property separately. The checksum-pinned TLA+ tools 1.7.4 artifact used by CI completes with 90 generated states and 64 distinct states, leaves no queued states, and reports no errors.

## Fairness and bounds

Weak fairness applies separately to pass start, one retry attempt, canonical entry classification, and end-of-pass cursor publication. Crashes are not fair; `MaxCrashes = 1` makes the crash assumption explicit and finite. The model uses three entries, puts the permanent block at entry one, gives each eligible entry one transient failure, and gives every pass one main-scan entry of budget. No new entries arrive.

## Not modeled

The model does not establish directory enumeration byte accounting, syscall accounting, wall or boottime acquisition, multiple hierarchy depths, phase rotation, retry-ledger capacity overflow, cursor wire encoding, filesystem durability, transition executor behavior, or implementation equivalence. It checks the abstract progress contract for one finite leaf under recurring invocations, positive main-scan budget, finite transient faults, finite crashes, and weakly fair maintenance actions.
