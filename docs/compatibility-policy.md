# Compatibility and target policy

Status: prototype policy, not a stable compatibility promise.

## Supported compilation and release targets

The prototype release and certification target is `x86_64-unknown-linux-gnu` on Rust 1.97.1. `steadq-core` compiles for 64-bit `x86_64` or `aarch64` Linux with the `gnu` or `musl` environment: `x86_64-unknown-linux-gnu`, `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-gnu`, and `aarch64-unknown-linux-musl`. All supported targets are 64-bit, so offset and size conversions are unchanged between them. The `musl` targets exist for static single-file binaries on appliances; the syscall substrate (O_TMPFILE, renameat2, openat2, OFD locks, syncfs) is identical across the set. Filesystem certification is per profile, not per target; the certified profile remains ext4, XFS, btrfs, f2fs, and ZFS on the release target, and other targets inherit it subject to their own filesystem testing. This compilation class intentionally admits Rust sanitizer targets with the same target configuration so verification can run under ASan, MSan, and TSan. Sanitizer targets are verification tools, not certified deployment targets. A further deployment target requires checked offset/size conversions and its own filesystem certification profile.

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
- Protocol IR and generated projections: protocol IR version.

No compatibility between unknown minor disk-format versions is promised until feature-bit and decoder policy is written and tested.
