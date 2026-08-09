# SteadQ/1 Formal Model

TLA+ specification of the SteadQ/1 queue protocol.

## Running

Requires Java 17 and the TLA+ toolkit.

```
java -cp /tmp/tla2tools.jar tlc2.TLC -config model/SteadQ.cfg model/SteadQ.tla -workers auto
```

The separate namespace durability model and its two crash profiles are documented in `model/namespace/README.md`.
The separate authenticated-wall and boottime scheduling model is documented in `model/scheduling/README.md`.
The separate receipt-evidence, compaction, duplicate-ack, and retention model is documented in `model/receipts/README.md`.
The separate persisted-maintenance progress and fairness model is documented in `model/maintenance/README.md`.

Download `tla2tools.jar` from https://github.com/tlaplus/tlaplus/releases.

Local check (after `curl -fsSL .../tla2tools.jar -o /tmp/tla2tools.jar`):

```
java -cp /tmp/tla2tools.jar tlc2.TLC -config model/SteadQ.cfg model/SteadQ.tla
```

CI runs the same command in the `tla` job.

## Model scope

Bounded configuration: 2 jobs, 1 worker, 2 lease tokens, MaxAttempts=2, and MaxGeneration=4. The generation bound makes repeated renewal finite while permitting a retired capability to be presented against a later lease on the same job. The `Crash` action is not count-bounded.

TLC 2026.07.31 with `-workers auto` completes this configuration with 264,897 generated states, 25,152 distinct states, no queued states, and no errors. Exploration depth is omitted because parallel worker scheduling changes the reported value without changing the reachable state set.

`model/SteadQProtocol.tla` is generated from the versioned protocol IR. It supplies the model's state values and complete transition, exceptional mutation, and re-entry metadata. The current actions still model only the bounded abstract behavior described below; generating the metadata does not make those actions implementation-complete.

## Invariants checked

- `TypeInvariant`: every model variable remains in its declared bounded domain.

- `CompleteVisibleEnvelope`: every modeled visible job retains its abstract file-durability witness.

- `LeaseHasToken`: every modeled lease carries an issued lease capability.

- `TokenAuthorityRequiresLease`: a non-null capability exists only while its job is leased.

- `ActiveLeaseTokensAreUnique`: two active leases cannot share a capability.

- `RetiredTokenCannotMutate`: a retired capability cannot mutate any job.

- `OtherJobTokenCannotMutate`: a capability current for one job cannot mutate another leased job.

- `AttemptWithinLimit`: modeled attempts do not exceed `MaxAttempts`.

- `ReceiptRemainsTerminal`: every job that reached receipt in the modeled history remains in receipt.

- `DeliveredAttemptIsPositive`: every modeled lease has a positive attempt.

`model/SteadQ.cfg` names each predicate separately. `cargo xtask check` rejects drift between this list, the invariant section in `model/SteadQ.tla`, and the TLC configuration.

## Fixes applied

Enqueue sets `fileSynced` to `TRUE` before publish, so `CompleteVisibleEnvelope` holds for `StateReady`.
`Claim` chooses a fresh value from `LeaseTokens` independently from the worker. `Renew`, `Ack`, `RetryNow`, and `Bury` require the exact current capability. `ReapExpired` revokes it. `Crash` preserves file
durability (`fileSynced[j]` stays `TRUE` if it was) and clears a stale lease
token when `Leased /\ ~fileSynced` rolls back to `Ready`. `ELSEIF` syntax
fixed to `ELSE IF`.

## Crash model

The Crash action preserves file content durability and resets volatile
directory sync flags. In the strong profile, a leased job whose file was
synced stays leased (atomic rename persistence). A leased job whose file
was not synced rolls back to ready (claim never completed) and its token
is cleared. A ready job that was never synced or dir-synced rolls back to
hidden.

## Not modeled

The main model remains an abstract state-machine model. Separate bounded models now check namespace crash observations, authenticated scheduling, strict receipt evidence, and persisted maintenance progress under their individually documented assumptions. The model suite does not establish cryptographic collision bounds, real filesystem behavior, resolver implementation soundness, production trace conformance, queue-root containment, or implementation equivalence. Delayed, receipt, dead, and quarantine states remain abstract rather than byte- and path-level objects. These gaps remain open A-013 work under issue #59.
