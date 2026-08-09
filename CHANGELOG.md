# Changelog

All notable changes to SteadQ will be documented here.

The project does not yet publish stable releases. Disk format, Rust API, C ABI, ticket schema, trace schema, cursor schema, and certification-profile compatibility are tracked independently; see `docs/compatibility-policy.md`.

## Unreleased

- Began the Elite Codebase hardening program from audited commit `80b8b20da3171509bccbde00ab021bb5b1f7c2dc`.
- Added reproducible toolchain, audit, governance, and claim-scope scaffolding without changing queue semantics.
- Bumped the protocol IR to version 2 and added the conditional source-directory barrier required when renewal crosses lease directories.
