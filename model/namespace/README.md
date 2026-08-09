# Namespace durability model

`SteadQNamespace.tla` is a bounded model of one cross-directory no-overwrite move. It imports the generated protocol IR vocabulary and validates both metadata forms that can produce such a move: distinct-state transitions with destination and source directory barriers, and renewal with a same-or-destination barrier followed by a source barrier when the directories differ.

The model keeps separate volatile and durable source and destination entries. `Crash` restores the volatile namespace from the durable snapshots. `SteadQNamespaceOrdered.cfg` changes durable entries only through explicit barriers. `SteadQNamespaceWeak.cfg` also permits independent source-removal and destination-addition persistence before an explicit barrier.

Run both profiles from the repository root:

```sh
java -cp /tmp/tla2tools.jar tlc2.TLC -config model/namespace/SteadQNamespaceOrdered.cfg model/SteadQNamespace.tla -workers auto
java -cp /tmp/tla2tools.jar tlc2.TLC -config model/namespace/SteadQNamespaceWeak.cfg model/SteadQNamespace.tla -workers auto
```

TLC 2026.07.31 with `-workers auto` completes the ordered profile with 29 generated states and 15 distinct states. It completes the weak profile with 64 generated states and 23 distinct states. Both runs leave no queued states and report no errors.

## Invariants checked

- `TypeInvariant`: variables and the crash profile remain in their bounded domains. Distinct-state no-overwrite moves use destination and source barriers; renewal uses same-or-destination followed by conditional-source. Both forms use not-committed before linearization, outcome-unknown after linearization, and a source-aware resolver topology.

- `BeforeLinearizationPreservesSource`: an operation that has not linearized retains the source object.

- `ConflictingDestinationIsNeverOverwritten`: observing a distinct destination identity prevents linearization and preserves both identities.

- `PostLinearizationFailureIsIndeterminate`: an explicit post-linearization failure is linearized but not committed.

- `ObservedPhaseMatchesNamespace`: every resolver observation phase agrees with the current source and destination identities.

- `CommittedIsDurableDestinationOnly`: a committed move has only the source identity at the durable destination and remains so after another crash.

- `BothSameResolutionPreservesIdentity`: both-same stabilization keeps the exact source identity through destination sync and source removal.

- `BothDifferentIsNeverRepairable`: no resolver repair action is enabled for two different identities.

- `OrderedProfileExcludesNeither`: the ordered profile cannot produce a neither observation.

Both configurations check every predicate separately. `cargo xtask check` rejects drift between this list, the marked invariant section, and either configuration. `TypeInvariant` also validates the generated metadata for every cross-directory no-overwrite move.

## Observations

The ordered profile represents source-only, destination-only, both-same, and both-different. It excludes neither because destination durability must precede source-removal durability. The weak profile additionally represents neither through independent source-removal persistence before destination persistence.

Both-same stabilization syncs the destination, removes only the exact source identity, and syncs the source directory. A crash after either of the first two steps returns to both-same. A crash after the source-directory barrier returns destination-only.

## Not modeled

The model does not establish a real filesystem profile, file-content durability, filename authentication, token authority, generation or attempt arithmetic, wall scheduling, receipt evidence, cursor progress, or production trace conformance. It models one abstract identity pair and does not prove implementation equivalence.
