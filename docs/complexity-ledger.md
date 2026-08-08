# Residual complexity ledger

Status: Phase 0 skeleton. Reviewer fields must name someone who did not author the implementation before a certified release.

| ID | Subsystem | Why complexity may be intrinsic | Required invariants/evidence | Owner | Independent reviewer | Revisit trigger |
| --- | --- | --- | --- | --- | --- | --- |
| CX-001 | Outcome-unknown resolution | Linearization can precede failed durability barriers | I2, I3, I10; phase fault and second-crash matrix | Maintainer | Unassigned | A-002/A-003 complete |
| CX-002 | Wall watermark | Wall rollback must not make work early | I13; clock/watermark fault model | Maintainer | Unassigned | A-004 complete |
| CX-003 | No-overwrite publication fallback | Kernel/filesystem capabilities vary | I2, I3, I14; profile crash lab | Maintainer | Unassigned | A-009/A-015 complete |
| CX-004 | Recovery cursor | Bounded progress under mutable unordered directories | I11, I12; randomized order and reopen properties | Maintainer | Unassigned | A-006 complete |
| CX-005 | Compact receipts | Payload evidence is destructively summarized | I9; model, mutation, fuzz, crash evidence | Maintainer | Unassigned | A-005 complete |
| CX-006 | Cross-directory durability | Destination addition and source removal have separate barriers | I2, I3, I10; phase matrix | Maintainer | Unassigned | A-008 complete |
| CX-007 | Quarantine repair | Evidence and raw bytes must publish together | I11, I14; repair-plan crash matrix | Maintainer | Unassigned | A-012 complete |
| CX-008 | C ABI panic boundary | Foreign pointers, ownership, panic and result semantics intersect | ABI corpus and sanitizer evidence | Maintainer | Unassigned | A-018 complete |
