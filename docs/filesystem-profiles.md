# Filesystem support

SteadQ supports ext4, XFS, and btrfs on Linux x86_64.

All three filesystems provide the required primitives:
- Atomic no-overwrite rename (`renameat2` with `RENAME_NOREPLACE`)
- Unnamed temporary file creation (`O_TMPFILE`)
- File publication via `linkat` with `AT_EMPTY_PATH` or `/proc/self/fd`
- Path containment via `openat2` with `RESOLVE_BENEATH`
- File data durability via `fsync`
- Directory entry durability via `fsync` on the directory file descriptor

No filesystem has been independently crash-tested with real power-cut
hardware. Until that testing is complete, durability claims are based on
POSIX and Linux kernel documentation guarantees.
