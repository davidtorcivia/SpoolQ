# Compatibility and target policy

Status: prototype policy, not a stable compatibility promise.

## Supported build target

The strict prototype build target is `x86_64-unknown-linux-gnu` on Rust 1.97.1. `steadq-core` rejects every other target at compile time. A future target requires checked offset/size conversions and its own filesystem certification profile.

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

## ADR-0033 freeze gate

ADR-0033 was referenced by the audit but is absent from commit `80b8b20da3171509bccbde00ab021bb5b1f7c2dc`. Protocol-IR closure and stabilization of authority-bearing public APIs are blocked until the accepted ADR is committed, mapped into `docs/traceability.md`, and any conflicts are resolved by a superseding ADR.
