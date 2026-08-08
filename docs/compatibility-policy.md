# Compatibility and target policy

Status: prototype policy, not a stable compatibility promise.

## Supported compilation and release targets

The prototype release and certification target is `x86_64-unknown-linux-gnu` on Rust 1.97.1. `steadq-core` compiles only when Rust reports 64-bit `x86_64` Linux with the GNU environment. This compilation class intentionally admits Rust sanitizer targets with the same target configuration so verification can run under ASan, MSan, and TSan. Sanitizer targets are verification tools, not certified deployment targets. A future deployment target requires checked offset/size conversions and its own filesystem certification profile.

The development toolchain and minimum supported Rust version are both Rust 1.97.1. The project tracks current stable Rust before its first stable release because the Rust project supplies bug and security fixes only for the latest release. Each toolchain update must pass the full local gate and be recorded in the changelog.

## Independent version domains

- Rust crates and public Rust API: Cargo semantic version.
- Disk format: FORMAT major/minor plus defined feature compatibility.
- C ABI: ABI major and versioned option/result structures.
- Transition tickets: ticket schema version.
- Executor traces: trace schema version.
- Recovery cursors: cursor schema version.
- Quarantine manifests: manifest schema version.
- Filesystem evidence: certification-profile version.

No compatibility between unknown minor disk-format versions is promised until feature-bit and decoder policy is written and tested.
