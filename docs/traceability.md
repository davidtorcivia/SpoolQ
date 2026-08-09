# Protocol traceability matrix

Status: Evolving evidence matrix. Transition metadata is generated from the closed protocol IR; model, resolver, and fault-evidence links remain incomplete.

| Contract claim | Operation or invariant | Rust authority | Resolver rule | Formal evidence | Fault/crash evidence | Status |
| --- | --- | --- | --- | --- | --- | --- |
| No incomplete delivery | I1 / claim | `queue`, `verified` | Operation-bound job and receipt verification | Bounded `CompleteVisibleEnvelope` abstraction only | Unit and observation harness | Partial |
| No overwrite of distinct active job | I2 / moves | `queue`, Linux rename wrapper | Pending A-002 | Bounded `ConflictingDestinationIsNeverOverwritten` namespace model for one abstract cross-directory move | Selected fault tests | Partial |
| Committed enqueue remains represented | I3 / enqueue | `queue` | Pending A-003 | Bounded `CommittedIsDurableDestinationOnly` namespace predicate for one abstract move, not enqueue publication | Observation harness only | Partial |
| Lease authority uniqueness | I4 / claim | `queue` | Operation-bound source identity | Bounded `LeaseHasToken`, `TokenAuthorityRequiresLease`, and `ActiveLeaseTokensAreUnique` capability model | Concurrency unit tests | Partial |
| Stale-token exclusion | I5 | `queue` | Operation-bound source identity | Bounded `RetiredTokenCannotMutate` and `OtherJobTokenCannotMutate` capability checks | Unit tests | Partial |
| Generation monotonicity | I6 | Duplicated production logic | Pending A-007 | Partial | Unit tests | Partial |
| Attempt discipline | I7 | Generated transition metadata and production queue logic | Operation-bound attempt derivation | Bounded `AttemptWithinLimit` and `DeliveredAttemptIsPositive` | Unit tests | Partial |
| Terminal monotonicity | I8 | Recovery and queue | Operation-bound terminal verification | Bounded `ReceiptRemainsTerminal` covers modeled acknowledgment history | Partial | Open |
| Strict receipt evidence | I9 | Strict ack and central receipt verifier | Operation-bound receipt verification | Not modeled | Payload corruption, legacy receipt, resolver, fsck, recovery, and mutation tests | Partial |
| Resolver soundness | I10 | Resolver | Handwritten | Bounded both-same stabilization and both-different refusal for one abstract cross-directory move | Observation harness | Partial |
| Recovery idempotence/progress | I11/I12 | Recovery | N/A | Not modeled | Partial | Open |
| Wall rollback safety | I13 | Watermark/recovery | N/A | Bounded authenticated-wall scheduling and rollback model, not implementation equivalence | Clock, watermark, and recovery fault tests | Partial |
| Queue containment | I14 | Resolver/Linux paths | N/A | Not modeled | Partial | Open |

“Partial” means evidence exists but does not yet justify the full contract claim. “Open” means a release-blocking evidence gap remains.
