# Security policy

SteadQ is an experimental prototype. Do not use it for workloads where job loss, duplicate execution, silent attempt consumption, or an unrecoverable queue would be materially harmful.

## Reporting a vulnerability

Use GitHub private vulnerability reporting for this repository. If private reporting is unavailable, contact the repository owner without publishing exploit details first.

Include the affected commit, operating system and filesystem profile, a minimal reproducer, and whether the issue crosses the queue-root authority boundary or changes durable state.

## Supported versions

No version is currently security-supported. Fixes land on `main`; a supported-release table will be added only after the stable-release gates in `docs/release-checklist.md` are satisfied.

The trusted-local-domain assumption does not permit path escape, symlink traversal, forged queue authority, or silent destructive repair.
