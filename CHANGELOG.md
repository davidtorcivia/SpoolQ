# Changelog

## Unreleased

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

- 622 tests: unit, fault injection, differential, and formal model checking
- Stateful differential driver verifies production API against logical oracle
- Six TLA+ model configurations with drift-checked generated metadata
- Diff-scoped mutation testing on every pull request

### Infrastructure

- Closed protocol IR with versioned schema and typed domains
- Reproducible toolchain pinning (Rust 1.97.1, x86_64-unknown-linux-gnu)
- Compatibility policy for independent versioning of disk format, Rust API, C ABI, and ticket schema
