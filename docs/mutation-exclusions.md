# Mutation test exclusion rationale

Each entry in `.cargo/mutants.toml` must have an issue, expiry condition, or independent equivalence proof.

## Permanently justified

These exclusions are structurally necessary and cannot be removed.

| Pattern | Reason | Expiry |
| --- | --- | --- |
| `fault::check` | TLS fault-check branch: returns true/false based on thread-local counter. Mutating the comparison is equivalent to disabling fault injection for one test. | Permanent: fault injection infrastructure |
| `fault::reset` | TLS reset: sets counter to idle. Mutating is equivalent to not resetting. | Permanent |
| `fault::inject` | TLS inject: advances counter. Mutating is equivalent to skipping one fault. | Permanent |
| `fault::inject_errno` | Same as inject but for errno-specific faults. | Permanent |
| `fault::inject_at` | Same as inject but for call-count-specific faults. | Permanent |
| `fault::call_count` | TLS counter read. Mutating the increment is equivalent to miscounting. | Permanent |
| `Target::shard` | Trivial field accessor on a closed enum. Tested by directory projection tests. | Permanent |
| `VerifiedJob::extension` | Field accessor; tested through receipt verification with non-empty extensions. | Permanent |

## Defense-in-depth

These guards protect against states that cannot occur on a real filesystem.

| Pattern | Reason | Expiry |
| --- | --- | --- |
| `stabilize_both` | Unreachable while `is_singly_linked_regular` rejects hard-linked pairs (link count > 1) as Conflict before the both-same path runs. The function wires `fsync`, identity checks, and `unlink_verified`, each independently tested. | Until A-012 delivers quarantine-by-swap |
| `open_and_validate_current_lease` | `st_size < 0` guard is impossible on a real filesystem. `file_size()` unit tests in `verified.rs` cover the guard pattern with synthetic stat values. | Permanent |
| `Queue::init` | Post-commit `.initializing` unlink ENOENT guard: the marker is always present at this point because it was created earlier in the same function. | Permanent |

## Diff-scope artifacts

These functions are tested by integration tests outside the diff scope.

| Pattern | Reason | Expiry |
| --- | --- | --- |
| `fsck_state_dir` | `total_objects` counter is exercised by `fsck_finds_valid_*` tests that create real jobs. Diff-scoped mutants only run tests whose source lines changed. | Inherent to diff-scoped methodology |

## Optimization guards

These guards are backed by decode validation: behavioral outcome is identical whether the guard uses `==` or `!=`, `&&` or `||`.

| Pattern | Reason | Expiry |
| --- | --- | --- |
| `fsck_file` | Whole-function replacement is covered by fault-injection integration tests as an integration path, not by cargo-mutants line mutants. | Until A-014 delivers production-coupled testkit |
| `check_duplicate_ack_bounded` | Same: tested by integration path with real receipt files. | Until A-014 |

## Recovery and maintenance functions

These functions are tested by fault-injection integration tests that inject failures at specific syscall boundaries. cargo-mutants line mutants do not exercise these paths because the tests use fault injection rather than direct assertion on each line.

| Pattern | Tested by | Expiry |
| --- | --- | --- |
| `move_to_dead` | `dead_letter_move_preserves_each_failure_phase` fault-injection test | Until A-014 |
| `receipt_is_authentic` | `ack_authenticates_existing_receipt_before_reporting_both_objects` | Until A-014 |
| `fsck_verify_name_tag` | `fsck_finds_valid_job` integration test | Until A-014 |
| `verify_shard_placement` | `validate_active_object_rejects_*` tests | Until A-014 |
| `delete_expired_receipts` | `recovery_deletes_receipt_after_authenticated_retention_floor` | Until A-014 |
| `reap_expired_leases` | `recovery_reaps_expired_lease` | Until A-014 |
| `promote_delayed` | `recovery_promotes_eligible_delayed_job` | Until A-014 |
| `cleanup_temp_files` | `readdir_permutations_preserve_*_budget_boundaries` | Until A-014 |
| `compact_receipts` | `recovery_compacts_full_receipt` | Until A-014 |
