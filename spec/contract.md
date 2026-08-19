# SteadQ/1 Contract

## Assumptions

Queue root is on one certified local filesystem and one mount, and all queue state directories remain on the same filesystem device. Producers, consumers, and administrators with direct write access belong to one trusted local security domain.

The kernel and filesystem satisfy a named certification profile, and at least one producer, consumer, or recovery process eventually performs fair bounded recovery work.

`CLOCK_BOOTTIME` advances while the machine is running or suspended. Delayed scheduling uses `CLOCK_REALTIME`; the host is responsible for its wall-clock policy.

Consumers do not begin processing until `lease()` has returned a committed lease.

## Guarantees

Supports multiple concurrent producers and consumers with at-least-once job execution. A partially written job is never returned by `lease()`.

A successful strict enqueue remains represented by one recoverable or terminal object after a certified crash, and a successful claim returns at most one current lease token for a job.

A stale or lost token cannot acknowledge, renew, retry, or bury a later lease. An unacknowledged committed lease eventually becomes ready or dead, subject to liveness assumptions.

A successful acknowledgment re-verifies the payload and creates a terminal receipt. SteadQ/1 exposes no unverified acknowledgment operation. Repeating acknowledgment is non-destructive only when the existing receipt passes the same queue, path, identity, envelope, and payload-evidence checks used by recovery and integrity tooling.

Receipt compaction is permitted only after strict verification of the complete full receipt, including its payload digest. A compact receipt preserves that verified evidence class; malformed or unverified full receipts are not compacted.

Corrupt, malformed, or structurally ambiguous objects are never delivered automatically, and recovery may be interrupted after any filesystem operation and safely rerun without data loss.

### Disk-full classification

The queue shares its filesystem with the payloads it stores, so space exhaustion during an operation is a normal operating condition, not an environment fault. Storage exhaustion (`ENOSPC`, `EDQUOT`) is classified as resource exhaustion: it never poisons the handle and never causes quarantine.

An operation that hits storage exhaustion before its linearizing rename or link reports NotCommitted with the resource-exhausted error: the caller knows the job was not enqueued or the transition did not happen, and may retry once space is available.

Storage exhaustion after the linearizing rename but before the durability barrier completes reports OutcomeUnknown. This is the same indeterminate class as any barrier failure: the object may or may not become durable, and the ticket resolves it through the standard resolution and recovery paths. The linearization ordering is unchanged by disk state.

Partially written temporary files never appear in active state directories because publication writes and syncs the temp file before the name appears. A full disk can leave orphaned files under `tmp/`; the recovery retention pass removes them (temp files from dead boots immediately, and current-boot temps past their creation window), bounded by its work budget, and rerunning the pass is safe. An orphaned temp file holds space but is never delivered, acknowledged, or mistaken for a job. When space is needed to make progress, removing quarantined objects and dead jobs via the administrative commands reclaims it before recovery work must allocate.

No transition overwrites a distinct active job.

`maximum_attempts` bounds the number of committed claim returns, not internal rename attempts and not external side effects.

## Non-Goals

SteadQ/1 does not provide exactly-once external side effects, transactions spanning jobs, atomic batches, strict FIFO ordering, or priorities, selectors, or routing expressions.

It does not maintain a queue-wide exact counter or mutable index, does not support transparent online format migration, and does not provide hostile multi-tenant isolation for processes sharing direct filesystem access.

It targets Linux specifically: no generic POSIX portability, no network filesystem support, and no overlay filesystem support in strict mode. It does not support transparent deduplication after an indeterminate enqueue and does not maintain an authoritative event history.

## Terms

**Linearization point**: the successful no-overwrite link or exact-source rename that selects the winner of a transition.

**Durability barrier**: the file and directory sync sequence required before a transition may be reported committed.

**Committed**: the linearization point and all required durability barriers completed successfully.

**Not committed**: the implementation can prove that the linearization point did not occur.

**Outcome unknown**: the linearization point occurred or may have occurred, but a required post-linearization barrier failed or its result could not be established.

**Lease lost**: the supplied exact source and token no longer authorize a transition.

**Terminal**: receipt, dead, or quarantine state; normal recovery does not reactivate it.

Claims are always strict in v1.
