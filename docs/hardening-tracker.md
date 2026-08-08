# Elite hardening tracker

Baseline: `main` at `80b8b20da3171509bccbde00ab021bb5b1f7c2dc`, audited 2026-08-08.

This local tracker uses the audit finding IDs as stable issue identifiers. External issue URLs are intentionally not fabricated; they may be attached after maintainers create them. “Owner: maintainer” is provisional and does not satisfy independent-review gates.

## Release-blocking findings

| Finding | Reproducer/evidence plan | Dependency | Status | Owner |
| --- | --- | --- | --- | --- |
| SQ-P0-001 | Malicious ticket component corpus plus syscall trace proving no out-of-root open | A-001 | Open | Maintainer |
| SQ-P0-002 | Mutate operation and every identity field for each ticket/resolver row | A-002, ADR-0033 | Open | Maintainer |
| SQ-P0-003 | Source/destination/both/neither/conflict and second-crash matrix | A-003, A-009 | Open | Maintainer |
| SQ-P0-004 | Random readdir order, every budget boundary, faults, and reopen property | A-006 | Open | Maintainer |
| SQ-P0-005 | Clock/watermark syscall fault matrix and rollback property | A-004 | Open | Maintainer |
| SQ-P0-006 | Corrupt payload through every ack/compaction/receipt consumer | A-005, ADR-0033 | Open | Maintainer |
| SQ-P0-007 | Generated fault at every mutation phase; no flattened post-linearization result | A-008 | Open | Maintainer |
| SQ-P0-008 | Deliberate barrier/token/generation model mutations and checked invariant list | A-013 | Open | Maintainer |

## Priority-one findings

| Finding | Work package | Status |
| --- | --- | --- |
| SQ-P1-001 Public raw structs | A-010 | Open |
| SQ-P1-002 Incomplete typed layout | A-009/A-010 | Open |
| SQ-P1-003 Verification witness | A-011 | Open |
| SQ-P1-004 Bounds and conversions | A-009/A-010 | Open |
| SQ-P1-005 Lossy directory names | A-009 | Open |
| SQ-P1-006 Raw-FD ownership | A-009 | Open |
| SQ-P1-007 Init/open protocol | A-008 | Open |
| SQ-P1-008 Incomplete fsck namespace accounting | A-012 | Open |
| SQ-P1-009 Inconsistent compact receipt validation | A-005 | Open |
| SQ-P1-010 Destructive maintenance TOCTOU | A-008/A-012 | Open |
| SQ-P1-011 String-flattened errors | A-008/A-017/A-018 | Open |
| SQ-P1-012 Critical mutation exclusions | A-019 | Open |
| SQ-P1-013 Self-referential testkit | A-014 | Open |
| SQ-P1-014 Missing stateful fuzzing | A-014/A-019 | Open |
| SQ-P1-015 Incomplete C ABI | A-018 | Open |
| SQ-P1-016 CLI bypasses core safety | A-017 | Open |
| SQ-P1-017 Streaming inefficiency/ambiguity | A-011/A-016 | Open |
| SQ-P1-018 Missing target/toolchain/version policy | A-000 | Complete for prototype target; expansion remains profile-gated |

## Freeze blockers

- ADR-0033 is absent. A-007 protocol-IR closure and authority-bearing public API stabilization cannot complete until it is added and reconciled.
- No filesystem profile is certified.
- Independent Rust, Linux-filesystem, formal-methods, and adversarial-operator reviewers are unassigned.
