# Changelog

## Unreleased

### Structure

- Claim keeps the leased file in `ready/<shard>/`. The leased filename includes boot id (`.o` + 32 hex). Recovery still walks `leased/` for the previous layout and reaps colocated leased names from `ready/`
- README test count matches `cargo test --workspace --all-features -- --list` (701)
- Removed leftover `dead_code`/`unused_imports` allows on live items and the unused power-loss `is_durable` helper
- Split `queue/mod.rs` into publish, lease, consumer, and inspect modules; init and open stay in the parent
- Split recovery phases into reap, promote, and retain
- Deleted the `ensure_dir_pub` wrapper and the always-true tag self-comparison in `validate_active_object`

### Fixes

- The first `ensure_dir` of a shard leaf creates every sibling shard and `fsync`s the bucket once, matching how init fills `ready/`
- Streaming tmpfile enqueue no longer fsyncs the destination directory after `publish_tmpfile_noreplace_with_mode`, which already synced it
- Receipt compaction and retention record open and lock I/O instead of treating those failures as a busy skip
- Deleted unused public name helpers `name_tag_hex`, `filename_without_tag_and_ext`, and `verify_ready_tag`
- Production identity changes (generation and attempt) come from the protocol IR via `next_common_fields`
- Streaming enqueue records deferred dirty directories and skips dest-dir fsync until `sync()`, matching buffered enqueue
- CLI maps every command through the spec 11.5 exit table (`exit_core` / `exit_io`) instead of collapsing most failures to 1
- CLI lease handles persist payload length, digest, and content type so `ack`/`retry`/`bury` work after `lease --handle-file`
- `steadq doctor` accepts ZFS and the alternate f2fs statfs magic, and honors the global `--json` flag
- Streaming enqueue fails closed when `getrandom` fails instead of publishing job id `0`
- Admin dead export/remove reject invalid job IDs instead of operating on the all-zero id
- CBOR metadata encodes `i64::MIN` without overflowing
- C `steadq_init` maps unsupported filesystem and permission errors to the matching result codes
- C resolve reports `BothObserved` as corruption, matching the CLI
- Batch/deferred lease records dirty directories only after a successful claim rename, and a record failure is OutcomeUnknown
- Streaming enqueue keeps the published envelope digest on OutcomeUnknown
- Lease scan stops after a failed exhausted-attempt dead-letter move instead of claiming on a poisoned handle
- Claim of a corrupt payload that cannot be quarantined is OutcomeUnknown, not NotCommitted
- `renew` returns NotCommitted instead of panicking when lease-bucket arithmetic is exhausted
- Recovery quarantines malformed leased filenames instead of skipping them

### Performance

- Measured strict vs deferred completed-job throughput on the README NVMe: 3,065/s strict, 3,520/s deferred batch-50. After same-directory lease, a warm completed job is 5 `fsync` on the tmpfile path once destination buckets exist. The first `ensure_dir` of a shard leaf creates every sibling and syncs the bucket once. A deferred batch of 10 still issues 7.7 `fsync` per job.

### Core

- Full queue lifecycle: init, open, enqueue, lease, ack, retry, bury, renew, recover, inspect
- Streaming enqueue (accepts any `std::io::Read` without buffering the full payload)
- Verified payload reader (hashes payload once, serves O(1) random-access reads)
- All state transitions route through a single phase-aware executor
- Payload integrity verified by SHA-256 at every transition
- Wall clock watermark prevents early delivery after clock rollback
- Bounded, resumable recovery with directory-entry durability

### C ABI

- Opaque queue, lease, and payload reader handles
- Full lifecycle: init, open, enqueue, lease, verify, ack, retry, bury, recover, resolve
- Payload streaming via verified reader
- Ticket-based resolution of indeterminate operations
- Generated header via cbindgen with CI drift check

### Testing

- 701 tests: unit, fault injection, differential, and formal model checking
- Stateful differential driver verifies production API against logical oracle
- Six TLA+ model configurations with drift-checked generated metadata
- Diff-scoped mutation testing on every pull request
- Tests that require non-UTF-8 directory names or link publication skip on filesystems that reject those inputs (ZFS utf8only, strict ext4 encoding)

### Infrastructure

- Closed protocol IR with versioned schema and typed domains
- Reproducible toolchain pinning (Rust 1.97.1, x86_64-unknown-linux-gnu)
- Compatibility policy for independent versioning of disk format, Rust API, C ABI, and ticket schema
- Crash lab (`cargo xtask crashlab`): SIGKILL lane and dm-log-writes replay lane with device-safety guards, run registry, and per-state manifests (docs/crash-lab.md)
- Crash replay passes for all five profiles on two hosts: 761 states on kernel 6.8.0-137 and 793 states on kernel 7.0.0-28 (nyx), all passing
- ZFS supported: named-fallback publication, pool force-import crash recovery, and both f2fs statfs magic constants accepted
