# Protocol traceability matrix

Status: Phase 0 skeleton. A-007 will generate or mechanically validate this matrix from the closed protocol IR.

| Contract claim | Operation or invariant | Rust authority | Resolver rule | Formal evidence | Fault/crash evidence | Status |
| --- | --- | --- | --- | --- | --- | --- |
| No incomplete delivery | I1 / claim | `queue`, `verified` | Operation-bound job and receipt verification | Bounded `CompleteVisibleEnvelope` abstraction only | Unit and observation harness | Partial |
| No overwrite of distinct active job | I2 / moves | `queue`, Linux rename wrapper | Pending A-002 | Not comprehensively configured | Selected fault tests | Partial |
| Committed enqueue remains represented | I3 / enqueue | `queue` | Pending A-003 | Abstract crash action, no checked namespace-representation predicate | Observation harness only | Partial |
| Lease authority uniqueness | I4 / claim | `queue` | Operation-bound source identity | `LeaseHasToken` checks presence only; uniqueness is not modeled | Concurrency unit tests | Partial |
| Stale-token exclusion | I5 | `queue` | Operation-bound source identity | Not modeled; worker identity is not a lease capability | Unit tests | Partial |
| Generation monotonicity | I6 | Duplicated production logic | Pending A-007 | Partial | Unit tests | Partial |
| Attempt discipline | I7 | Generated transition metadata and production queue logic | Operation-bound attempt derivation | Bounded `AttemptWithinLimit` and `DeliveredAttemptIsPositive` | Unit tests | Partial |
| Terminal monotonicity | I8 | Recovery and queue | Operation-bound terminal verification | Bounded `ReceiptIsTerminal` covers acknowledgment only | Partial | Open |
| Strict receipt evidence | I9 | Strict ack and central receipt verifier | Operation-bound receipt verification | Not modeled | Payload corruption, legacy receipt, resolver, fsck, recovery, and mutation tests | Partial |
| Resolver soundness | I10 | Resolver | Handwritten | Not modeled | Observation harness | Open |
| Recovery idempotence/progress | I11/I12 | Recovery | N/A | Not modeled | Partial | Open |
| Wall rollback safety | I13 | Watermark/recovery | N/A | Not modeled | Partial | Open |
| Queue containment | I14 | Resolver/Linux paths | N/A | Not modeled | Partial | Open |

“Partial” means evidence exists but does not yet justify the full contract claim. “Open” means a release-blocking evidence gap remains.
