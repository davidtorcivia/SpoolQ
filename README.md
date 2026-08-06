# spoolq

Crash-safe filesystem queue protocol. At-least-once execution with lease-based ownership transfer via atomic rename on local Linux filesystems.

## Overview

SpoolQ is a brokerless, language-neutral filesystem queue. Jobs are immutable files with state-bearing pathnames. Ownership transfers through atomic no-overwrite rename operations.

Properties: at-least-once execution, crash-safe publication and recovery, no daemon or leader or mutable index, bounded lease expiry and retry, quarantine for corrupt objects.

## Status

Early development. Core format, names, math, filesystem substrate, and lifecycle operations are implemented. Recovery, quarantine, fsck, and formal verification are in progress.

## Building

```
cargo build --release
```

## Usage

```
spoolq init /path/to/queue
echo "payload" | spoolq put /path/to/queue - --content-type text/plain
spoolq lease /path/to/queue --duration-seconds 30
spoolq stats /path/to/queue
spoolq doctor /path/to/queue
```

## Architecture

The workspace is split into crates with one-way dependency direction:

`spoolq-format` binary format encoding (FORMAT record, fixed job header, compact receipt, wall watermark, deterministic CBOR extension header)

`spoolq-names` canonical filename parsing and formatting for all states, 64-bit name integrity tags, shard derivation and scan permutation

`spoolq-math` bucket arithmetic, delayed eligibility rounding, retry jitter with rejection sampling, checked arithmetic

`spoolq-fs-linux` Linux syscall wrappers (openat, mkdirat, renameat2, linkat, O_TMPFILE, fsync, OFD locks, clocks, getrandom). All unsafe code confined here.

`spoolq-core` queue state machine: init, open, enqueue, lease, ack, retry, bury, renew

`spoolq-cli` command-line interface

## Specification

User-facing spec documents are in [`spec/`](spec/):

- [`contract.md`](spec/contract.md) - assumptions, guarantees, non-goals, terminology
- [`format.md`](spec/format.md) - binary record layouts, offsets, digest formulas
- [`filenames.abnf`](spec/filenames.abnf) - normative filename grammar
- [`reasons.md`](spec/reasons.md) - dead and quarantine reason registries

## License

Apache-2.0
