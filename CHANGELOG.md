# Changelog

All notable changes to SteadQ will be documented here.

The project does not yet publish stable releases. Disk format, Rust API, C ABI, ticket schema, trace schema, cursor schema, and certification-profile compatibility are tracked independently; see `docs/compatibility-policy.md`.

## Unreleased

### Hardening program (from audited commit `80b8b20`)

- Closed all 8 P0 release-blocking findings (path containment, ticket binding,
  resolver durability, recovery cursor, wall authority, receipt evidence,
  executor convergence, formal evidence alignment)
- Closed 16 of 18 P1 structural findings (validated domain types, typed
  locations, owned verifier witness, checked conversions, init/open protocol,
  manifest fsck, maintenance TOCTOU, structured errors, mutation exclusions,
  CLI safety, streaming enqueue and verified reads, C ABI v2 lifecycle)
- All authoritative mutations route through one phase-aware executor family
- Six bounded TLA+/TLC models with drift-checked generated metadata
- Production-coupled testkit driver executing real Queue API against logical oracle
- C ABI with payload reading, ticket resolution, opaque handles, and generated header
- VerifiedPayloadReader for O(n) lease payload reads without re-hashing
- Streaming enqueue accepting any std::io::Read without buffering full payload
- Nightly CI lane with bounded full-workspace mutation testing

### Protocol and format

- Bumped the protocol IR to version 2 and added the conditional source-directory
  barrier required when renewal crosses lease directories
- Closed protocol IR schema with typed domains, object kinds, transition
  qualifications, clock requirements, resolver topology, and linearization outcomes

### Infrastructure

- Reproducible toolchain, audit, governance, and claim-scope scaffolding
- Diff-scoped mutation testing on every pull request
- cbindgen-generated C header with CI drift check
