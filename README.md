# spoolq

Crash-safe filesystem queue protocol. At-least-once execution with lease-based ownership transfer via atomic rename on local Linux filesystems.

## Overview

SpoolQ is a brokerless, language-neutral filesystem queue. Jobs are immutable files with state-bearing pathnames. Ownership transfers through atomic no-overwrite rename operations.

Properties: at-least-once execution, crash-safe publication and recovery, no daemon or leader or mutable index, bounded lease expiry and retry, quarantine for corrupt objects.

## Status

**Prototype / experimental only.** Do not use for workloads where job loss,
duplicate execution, silent attempt consumption, or an unrecoverable queue
would be materially harmful.

Core protocol implemented with static-code audit remediations applied.
Format encoding, filename parsing, deterministic CBOR, shard math, and retry
jitter are complete with canonical parser validation. The lifecycle (init,
open, enqueue, lease, ack, retry, bury, renew, recover) includes source
identity validation, wall watermark advancement, bounded duplicate-ack
probing, error classification, and post-linearization outcome handling.
Formal verification, fault injection, and real filesystem crash testing
remain future work.

## Building

```
cargo build --release
```

## Usage

```
spoolq init /path/to/queue
echo "payload" | spoolq put /path/to/queue - --content-type text/plain
spoolq lease /path/to/queue --duration-seconds 30 --handle-file /tmp/handle.json
spoolq inspect /path/to/queue <job_id>
spoolq verify /path/to/queue/FORMAT
spoolq recover /path/to/queue
spoolq stats /path/to/queue
spoolq doctor /path/to/queue
```

Handle-based operations (ack, retry, bury) read the JSON handle file saved by lease:

```
spoolq ack /path/to/queue --handle-file /tmp/handle.json
spoolq retry /path/to/queue --handle-file /tmp/handle.json
spoolq bury /path/to/queue --handle-file /tmp/handle.json --reason 1
```

## Specification

User-facing spec documents are in [`spec/`](spec/):

- [`contract.md`](spec/contract.md) covers assumptions, guarantees, non-goals, and terminology
- [`format.md`](spec/format.md) documents all binary record layouts with offsets and digest formulas
- [`filenames.abnf`](spec/filenames.abnf) is the normative filename grammar
- [`reasons.md`](spec/reasons.md) lists dead and quarantine reason registries

## Architecture

The workspace is split into crates with one-way dependency direction:

`spoolq-format` handles binary format encoding (FORMAT record, fixed job header, compact receipt, wall watermark, deterministic CBOR extension header).

`spoolq-names` handles canonical filename parsing and formatting for all states, 64-bit name integrity tags, shard derivation and scan permutation.

`spoolq-math` provides bucket arithmetic, delayed eligibility rounding, retry jitter with rejection sampling, and checked arithmetic.

`spoolq-fs-linux` wraps Linux syscalls (openat, mkdirat, renameat2, linkat, O_TMPFILE, fsync, OFD locks, clocks, getrandom). All unsafe code is confined here.

`spoolq-core` implements the queue state machine: init, open, enqueue, lease, ack, retry, bury, renew, recover, inspect.

`spoolq-cli` provides the command-line interface.

## License

Apache-2.0
