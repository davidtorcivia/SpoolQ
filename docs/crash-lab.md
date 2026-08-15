# Crash lab

Reproducible storage-crash testing for SteadQ queue directories.

The crash lab records every block write issued by a workload on a real
filesystem, replays the log up to each persistence barrier (fsync, FUA),
and verifies each resulting on-disk state against the queue contract.

Two lanes:

- **tier0** (no root required): SIGKILL lane. A workload process is killed
  after a target number of completed operations; the surviving operation
  prefix defines the expectations. Process-crash evidence only.
- **tier1** (root required): dm-log-writes lane. Every crash state at every
  persistence barrier is mounted and checked. Exhaustive over crash states
  reachable at persistence boundaries, for the recorded workload.

## Gates

Each crash state must satisfy:

- No committed enqueue is lost.
- No acknowledged job is active.
- No phantom job is delivered.
- Recovery completes without errors.
- fsck reports no error-severity findings.
- Corrupt payloads are quarantined, never delivered.

## Usage

```sh
cargo xtask crashlab doctor                 # tool and device preflight
cargo xtask crashlab tier0 --runs 24        # SIGKILL lane
sudo cargo xtask crashlab tier1 --fs ext4   # also xfs, btrfs, f2fs, zfs
cargo xtask crashlab teardown               # release resources after an interrupted run
```

Tier 1 requires `replay-log` from xfstests (`src/log-writes/`), `mkfs.<fs>`,
`losetup`, `dmsetup`, and the `dm-log-writes` kernel module. The ZFS
profile additionally requires `zpool`/`zfs` and creates its pool on the
log device, so pool creation is itself crash-tested; crash states are
recovered by force-importing the run's pool from exactly the run's loop
device (never a bare `zpool import`, which scans every host device), and
states that predate pool creation are vacuous passes. Pools use
`cachefile=none` so runs never touch the host pool cache.

## Device safety

The crash lab never writes to the OS drive or to any device holding other
data. Block targets are restricted to loop devices created by the tooling
itself, backed by image files under allowlisted scratch directories
(`/dev/shm/crashlab`, `target/crashlab`, or the path in `$CRASHLAB_STORE`).

Guards, enforced on every operation:

- Backing files resolve under an allowlisted store; traversal and symlink
  escape are rejected.
- The target must be a loop device attached to the run's own backing file
  (verified via `losetup`), with exact device-name matching.
- Whole-disk, partition, device-mapper, and md nodes are refused.
- Devices that are the source (or parent) of a mounted filesystem are refused.
- Device-mapper tables and mount points are namespaced per run and recorded
  in a registry; `teardown` releases a crashed run's resources.
- ZFS pools are created and imported scoped to the run's own loop device
  and pool name only; the host pool cache is never written.

## Output

Each tier 1 run writes a manifest recording the kernel, mkfs and mount
options, seed, entry and barrier counts, and one verdict per checked crash
state. The first failing state stops the run and preserves the images for
reproduction.
