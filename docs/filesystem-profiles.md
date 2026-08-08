# Filesystem certification profiles

No filesystem profile is currently certified.

A future profile must record filesystem type, kernel range, mount options, architecture, `openat2` support for `RESOLVE_BENEATH`, `RESOLVE_NO_SYMLINKS`, and `RESOLVE_NO_MAGICLINKS`, directory `fsync` behavior, no-overwrite rename support, `O_TMPFILE` publication behavior, crash-observation assumptions, harness version, and independent reviewer.

Naming a filesystem such as ext4 or XFS without that evidence is not certification.
