# spoolq

Crash-safe filesystem queue protocol. At-least-once execution with
lease-based ownership transfer via atomic rename on local Linux filesystems.

## Overview

SpoolQ is a brokerless, language-neutral filesystem queue. Jobs are
immutable files with state-bearing pathnames. Ownership transfers through
atomic no-overwrite rename operations.

Key properties:
- At-least-once job execution
- Crash-safe publication and recovery
- No daemon, leader, or mutable index
- Bounded lease expiry and retry
- Quarantine for corrupt objects

## Status

Early development. Not all features are implemented yet.

## Building

```
cargo build
cargo test
```

## License

Apache-2.0
