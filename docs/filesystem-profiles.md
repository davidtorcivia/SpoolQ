# Filesystem support

SteadQ runs on local Linux filesystems that provide the following guarantees:

- **Atomic no-overwrite rename** via `renameat2` with `RENAME_NOREPLACE`
- **Unnamed temporary files** via `O_TMPFILE`
- **File publication** via `linkat` with `AT_EMPTY_PATH` or `/proc/self/fd`
- **Path containment** via `openat2` with `RESOLVE_BENEATH`
- **File data durability** via `fsync`
- **Directory entry durability** via `fsync` on the directory file descriptor

## Supported filesystems

| Filesystem | Magic | Notes |
|---|---|---|
| ext4 | `0xEF53` | Reference filesystem. Journal-based durability. |
| XFS | `0x58465342` | Reference filesystem. Journal-based durability. |
| btrfs | `0x9123683E` | Copy-on-write B-tree durability. |
| f2fs | `0xF00D` | Flash-optimized for SSDs and eMMC. Log-structured durability. |

## Rejected filesystems

| Filesystem | Reason |
|---|---|
| tmpfs | No durability guarantee; data lost on reboot. |
| NFS | Rename atomicity and fsync ordering are not guaranteed across network. |
| FUSE | Durability depends on the FUSE implementation; cannot be guaranteed. |
| overlayfs | Copy-up semantics break rename atomicity assumptions. |

## Crash testing

No filesystem has been independently crash-tested with real power-cut
hardware. Until that testing is complete, durability claims are based on
POSIX and Linux kernel documentation guarantees for each filesystem.
