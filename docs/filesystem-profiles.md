# Filesystem support

SteadQ runs on local Linux filesystems that provide the following guarantees:

- **Atomic no-overwrite rename** via `renameat2` with `RENAME_NOREPLACE`
- **Unnamed temporary files** via `O_TMPFILE`
- **File publication** via `linkat` with `AT_EMPTY_PATH` or `/proc/self/fd`, or via a named temporary plus `renameat2` with `RENAME_NOREPLACE` (ZFS)
- **Path containment** via `openat2` with `RESOLVE_BENEATH`
- **File data durability** via `fsync`
- **Directory entry durability** via `fsync` on the directory file descriptor

## Supported filesystems

| Filesystem | Magic | Notes |
| --- | --- | --- |
| ext4 | `0xEF53` | Reference filesystem. Journal-based durability. |
| XFS | `0x58465342` | Reference filesystem. Journal-based durability. |
| btrfs | `0x9123683E` | Copy-on-write B-tree durability. |
| f2fs | `0xF00D` | Flash-optimized for SSDs and eMMC. Log-structured durability. |
| ZFS | `0x2FC12FC1` | Pooled copy-on-write. Publication uses the named-temp rename path (O_TMPFILE linking stalls on OpenZFS). Crash recovery is pool import. |

## Rejected filesystems

| Filesystem | Reason |
| --- | --- |
| tmpfs | No durability guarantee; data lost on reboot. |
| NFS | Rename atomicity and fsync ordering are not guaranteed across network. |
| FUSE | Durability depends on the FUSE implementation; cannot be guaranteed. |
| overlayfs | Copy-up semantics break rename atomicity assumptions. |

## Crash testing

Block-level crash replay (dm-log-writes) passes for all five profiles
(761 crash states; see [crash-lab.md](crash-lab.md) for per-profile
results and manifests). No filesystem has been tested with real power-cut
hardware. Durability claims additionally rest on POSIX and Linux kernel
documentation guarantees for each filesystem.
