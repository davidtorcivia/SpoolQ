# Protocol traceability matrix

Status: Transition metadata is generated from the closed protocol IR and checked for drift by `cargo xtask check`. Six bounded TLA+/TLC models check their configured invariants. Formal evidence is abstract and bounded; it is not implementation-equivalence proof. Remaining gaps are tracked as P1 items (#73 production-coupled testkit, #74 stateful fuzzing).

| Contract claim | Operation or invariant | Rust authority | Resolver rule | Formal evidence | Fault/crash evidence | Status |
| --- | --- | --- | --- | --- | --- | --- |
| No incomplete delivery | I1 / claim | `queue`, `verified` | Operation-bound job and receipt verification | Bounded `CompleteVisibleEnvelope` abstraction only | Unit and observation harness | Partial |
| No overwrite of distinct active job | I2 / moves | `queue`, phase-aware executor | Operation-bound source identity (A-002) | Bounded `ConflictingDestinationIsNeverOverwritten` namespace model for one abstract cross-directory move | Selected fault tests | Partial |
| Committed enqueue remains represented | I3 / enqueue | `queue`, phase-aware executor | Operation-bound source identity (A-003) | Bounded `CommittedIsDurableDestinationOnly` namespace predicate for one abstract move, not enqueue publication | Observation harness only | Partial |
| Lease authority uniqueness | I4 / claim | `queue` | Operation-bound source identity | Bounded `LeaseHasToken`, `TokenAuthorityRequiresLease`, and `ActiveLeaseTokensAreUnique` capability model | Concurrency unit tests | Partial |
| Stale-token exclusion | I5 | `queue` | Operation-bound source identity | Bounded `RetiredTokenCannotMutate` and `OtherJobTokenCannotMutate` capability checks | Unit tests | Partial |
| Generation monotonicity | I6 | Generated transition metadata and production queue logic | Closed protocol IR (A-007) | Bounded model checks | Unit tests | Partial |
| Attempt discipline | I7 | Generated transition metadata and production queue logic | Operation-bound attempt derivation | Bounded `AttemptWithinLimit` and `DeliveredAttemptIsPositive` | Unit tests | Partial |
| Terminal monotonicity | I8 | Recovery and queue | Operation-bound terminal verification | Bounded `ReceiptRemainsTerminal` covers modeled acknowledgment history | Recovery fault tests | Partial |
| Strict receipt evidence | I9 | Strict ack and central receipt verifier | Operation-bound receipt verification | Bounded receipt evidence, compaction, and retention model | Payload corruption, legacy receipt, resolver, fsck, recovery, and mutation tests | Partial |
| Resolver soundness | I10 | Resolver | Both-same stabilization and both-different refusal (A-003) | Bounded both-same stabilization and both-different refusal for one abstract cross-directory move | Observation harness | Partial |
| Recovery idempotence/progress | I11/I12 | Recovery, phase-aware executor | N/A | Bounded maintenance progress and fairness model; not implementation equivalence | Recovery fault and budget tests | Partial |
| Wall rollback safety | I13 | Watermark/recovery | N/A | Bounded authenticated-wall scheduling and rollback model, not implementation equivalence | Clock, watermark, and recovery fault tests | Partial |
| Queue containment | I14 | Resolver/Linux paths | N/A | Not modeled; path containment is tested via malicious ticket corpus (SQ-P0-001) | Path containment corpus | Partial |

“Partial” means evidence exists but does not yet justify the full contract claim. “Open” means a release-blocking evidence gap remains.
