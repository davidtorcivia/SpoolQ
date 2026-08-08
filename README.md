# SteadQ

Experimental filesystem queue protocol with lease-based ownership transfer via atomic rename on local Linux filesystems.

## Overview

SteadQ is a brokerless, language-neutral filesystem queue. Jobs are immutable files with state-bearing pathnames. Ownership transfers through atomic no-overwrite rename operations.

The intended contract includes at-least-once execution, crash-aware publication and recovery, no daemon or leader or mutable index, bounded lease expiry and retry, and quarantine for corrupt objects. These are design targets while the hardening program is in progress, not production certification claims.

## Status

**Prototype / experimental only.** Do not use for workloads where job loss,
duplicate execution, silent attempt consumption, or an unrecoverable queue
would be materially harmful.

Core protocol is implemented. Format, deterministic CBOR, filename parsing,
shard math, and retry jitter have canonical validation. The lifecycle (init,
open, enqueue, lease, ack, retry, bury, renew, recover, inspect) enforces
source identity (device, inode, generation, token, header, digest, name tag,
shard), re-verifies payload at ack, advances the wall watermark, and
classifies errors with bounded duplicate-ack probing. Recovery is resumable
per phase, payload reads stream without full materialization, and deep fsck
checks headers, digests, name tags, and payloads. Thread-local fault
injection covers post-linearization `OutcomeUnknown` paths and the in-memory
simulator covers directory-entry durability.

A bounded abstract TLA+/TLC model has checked its configured invariants for
two jobs, two workers, and `MaxAttempts=2`; crash actions are not count-bounded.
The in-process resolver observation harness injects transition failures and
manufactures five namespace observations; it is not evidence of real storage
power-loss behavior. Parser fuzz smoke tests, diff-scoped mutation tests,
concurrency tests, and a directory-durability simulator provide additional
evidence with the limitations recorded in
[`docs/formal-evidence-scope.md`](docs/formal-evidence-scope.md).

## Building

```
cargo build --release
```

## Usage

```
steadq init /path/to/queue
echo "payload" | steadq put /path/to/queue - --content-type text/plain
steadq lease /path/to/queue --duration-seconds 30 --handle-file /tmp/handle.json
steadq inspect /path/to/queue <job_id>
steadq verify /path/to/queue/FORMAT
steadq recover /path/to/queue
steadq stats /path/to/queue
steadq doctor /path/to/queue
```

Handle-based operations (ack, retry, bury) read the JSON handle file saved by lease:

```
steadq ack /path/to/queue --handle-file /tmp/handle.json
steadq retry /path/to/queue --handle-file /tmp/handle.json
steadq bury /path/to/queue --handle-file /tmp/handle.json --reason 1
```

## Specification

User-facing spec documents are in [`spec/`](spec/):

- [`contract.md`](spec/contract.md) covers assumptions, guarantees, non-goals, and terminology
- [`format.md`](spec/format.md) documents all binary record layouts with offsets and digest formulas
- [`filenames.abnf`](spec/filenames.abnf) is the normative filename grammar
- [`reasons.md`](spec/reasons.md) lists dead and quarantine reason registries

## Architecture

The workspace is split into crates with one-way dependency direction:

`steadq-format` handles binary format encoding (FORMAT record, fixed job header, compact receipt, wall watermark, deterministic CBOR extension header).

`steadq-names` handles canonical filename parsing and formatting for all states, 64-bit name integrity tags, shard derivation and scan permutation.

`steadq-math` provides bucket arithmetic, delayed eligibility rounding, retry jitter with rejection sampling, and checked arithmetic.

`steadq-fs-linux` wraps Linux syscalls (openat, mkdirat, renameat2, linkat, O_TMPFILE, fsync, OFD locks, clocks, getrandom). All unsafe code is confined here.

`steadq-core` implements the queue state machine: init, open, enqueue, lease, ack, retry, bury, renew, recover, inspect.

`steadq-cli` provides the command-line interface.

## License

Apache-2.0
