# Release checklist

No stable or certified release may proceed until the tracked hardening findings and the release gates below are satisfied.

Minimum gates:

- Zero unresolved P0 findings.
- Closed protocol IR and complete traceability matrix.
- All authoritative mutations use the phase-aware executor.
- All release-critical CI lanes are gating and reproducible.
- Named filesystem profiles pass the real crash lab.
- Rust/unsafe, Linux-filesystem, and formal-methods reviews complete.
- Two approvals from reviewers who did not author the implementation.
- Reproducible signed artifacts, checksums, SBOM, compatibility notes, and evidence bundle.
