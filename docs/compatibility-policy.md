# Compatibility and target policy

Status: prototype policy, not a stable compatibility promise.

## Supported build target

The strict prototype build target is `x86_64-unknown-linux-gnu` on Rust 1.88.0. Other architectures and operating systems are unsupported and must not inherit strict durability or containment claims. A future target requires checked offset/size conversions and its own filesystem certification profile.

The current MSRV is Rust 1.88. Raising it before a stable release is allowed when documented in the changelog.

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
