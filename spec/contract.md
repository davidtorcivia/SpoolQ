# SpoolQ/1 Contract

## Assumptions

Queue root is on one certified local filesystem and one mount. All queue state directories remain on the same filesystem device. Producers, consumers, and administrators with direct write access belong to one trusted local security domain.

The kernel and filesystem satisfy a named certification profile. At least one producer, consumer, or recovery process eventually performs fair bounded recovery work.

`CLOCK_BOOTTIME` advances while the machine is running or suspended. Delayed scheduling uses `CLOCK_REALTIME`; the host is responsible for its wall-clock policy.

Consumers do not begin processing until `lease()` has returned a committed lease.

## Guarantees

Multiple concurrent producers and consumers. At-least-once job execution. No partially written job is returned by `lease()`.

A successful strict enqueue remains represented by one recoverable or terminal object after a certified crash. A successful claim returns at most one current lease token for a job.

A stale or lost token cannot acknowledge, renew, retry, or bury a later lease. An unacknowledged committed lease eventually becomes ready or dead, subject to liveness assumptions.

A successful acknowledgment creates a terminal receipt. Repeating acknowledgment is non-destructive.

Corrupt, malformed, or structurally ambiguous objects are never delivered automatically. Recovery may be interrupted after any filesystem operation and safely rerun.

No transition overwrites a distinct active job.

`maximum_attempts` bounds the number of committed claim returns, not internal rename attempts and not external side effects.

## Non-Goals

Exactly-once external side effects. Transactions spanning jobs. Atomic batches. Strict FIFO ordering. Priorities, selectors, or routing expressions.

A queue-wide exact counter or mutable index. Transparent online format migration. Hostile multi-tenant isolation for processes sharing direct filesystem access.

Generic POSIX portability. Network filesystem support. Overlay filesystem support in strict mode. Transparent deduplication after an indeterminate enqueue.

An authoritative event history.

## Terms

**Linearization point**: the successful no-overwrite link or exact-source rename that selects the winner of a transition.

**Durability barrier**: the file and directory sync sequence required before a transition may be reported committed.

**Committed**: the linearization point and all required durability barriers completed successfully.

**Not committed**: the implementation can prove that the linearization point did not occur.

**Outcome unknown**: the linearization point occurred or may have occurred, but a required post-linearization barrier failed or its result could not be established.

**Lease lost**: the supplied exact source and token no longer authorize a transition.

**Terminal**: receipt, dead, or quarantine state; normal recovery does not reactivate it.

Claims are always strict in v1.
