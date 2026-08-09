# Scheduling model

`SteadQScheduling.tla` is a bounded model of authenticated wall snapshots, realtime rollback, boottime lease expiration, delayed promotion, and receipt retention. It validates every generated transition clock requirement plus watermark replacement metadata from the protocol IR.

The model starts with delayed work, a retained receipt, and a current-boot lease. Realtime can move to any bounded value, including backward. Boottime advances monotonically. A successful wall snapshot records `max(realtime, watermark)` durably before wall-sensitive work may proceed.

## Invariants checked

- `TypeInvariant`: variables, deadlines, generated scheduling metadata, and the watermark exception remain in their bounded domains.

- `AuthenticatedFloorIsDurable`: an active authenticated wall snapshot equals the durable watermark.

- `AuthenticatedFloorDoesNotExceedWatermark`: the retained authenticated floor never exceeds the durable watermark used by the next snapshot.

- `WatermarkEqualsHistoricalHigh`: the durable watermark remains equal to the highest authenticated floor the model has observed.

- `RealtimeBelowWatermarkRequiresObservedRollback`: realtime can be below the durable floor only after the model has observed a rollback.

- `DelayedPromotionUsesAuthenticatedFloor`: delayed work is promoted only with authenticated evidence at or after its deadline.

- `ReceiptDeletionUsesAuthenticatedFloor`: a receipt is deleted only with authenticated evidence at or after its retention deadline.

- `CurrentBootLeaseUsesBoottime`: current-boot lease expiration cannot precede its boottime deadline.

- `EligibleCurrentBootLeaseCanExpire`: a current-boot lease at its boottime deadline has an enabled expiration transition independent of realtime.

- `FailureDoesNotCreateAuthority`: wall or boottime acquisition failure does not create authority or perform the guarded work.

- `ObjectStateIsConsistent`: pending and completed forms of delayed work, receipts, and leases remain exclusive.

The configuration checks every predicate separately. The checksum-pinned TLA+ tools 1.7.4 artifact used by CI completes with 16,438 generated states and 2,647 distinct states, leaves no queued states, and reports no errors.

## Not modeled

The model does not establish clock syscall behavior, watermark file authentication, filesystem durability, old-boot lease policy, retry arithmetic, terminal bucket construction, production trace conformance, or implementation equivalence. It models one delayed item, one receipt, and one current-boot lease with small scalar time bounds.
