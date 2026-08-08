# Protocol traceability matrix

Status: Phase 0 skeleton. A-007 will generate or mechanically validate this matrix from the closed protocol IR.

| Contract claim | Operation or invariant | Rust authority | Resolver rule | Formal evidence | Fault/crash evidence | Status |
| --- | --- | --- | --- | --- | --- | --- |
| No incomplete delivery | I1 / claim | `queue`, `verified` | Pending A-002 | Bounded abstract I1 | Unit and observation harness | Partial |
| No overwrite of distinct active job | I2 / moves | `queue`, Linux rename wrapper | Pending A-002 | Not comprehensively configured | Selected fault tests | Partial |
| Committed enqueue remains represented | I3 / enqueue | `queue` | Pending A-002 | Abstract crash action | Observation harness only | Partial |
| Lease authority uniqueness | I4 / claim | `queue` | Pending A-002 | Worker abstraction only | Concurrency unit tests | Partial |
| Stale-token exclusion | I5 | `queue` | Pending A-002 | Worker-as-token abstraction | Unit tests | Partial |
| Generation monotonicity | I6 | Duplicated production logic | Pending A-007 | Partial | Unit tests | Partial |
| Attempt discipline | I7 | Duplicated production logic | Pending A-007 | Bounded I9/I11 | Unit tests | Partial |
| Terminal monotonicity | I8 | Recovery and queue | Pending A-007 | Not complete | Partial | Open |
| Strict receipt evidence | I9 | Ack/receipt/compaction | Pending A-005 | Not modeled | Partial | Open |
| Resolver soundness | I10 | Resolver | Handwritten | Not modeled | Observation harness | Open |
| Recovery idempotence/progress | I11/I12 | Recovery | N/A | Not modeled | Partial | Open |
| Wall rollback safety | I13 | Watermark/recovery | N/A | Not modeled | Partial | Open |
| Queue containment | I14 | Resolver/Linux paths | N/A | Not modeled | Partial | Open |

“Partial” means evidence exists but does not yet justify the full contract claim. “Open” means a release-blocking evidence gap remains.
