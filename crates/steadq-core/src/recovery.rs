// SteadQ/1 cooperative recovery operations.

use std::io;
use std::os::unix::io::{AsRawFd, OwnedFd};

use steadq_fs_linux as fs;
use steadq_math;
use steadq_names::{self, bucket_hex};

use crate::errors::*;
use crate::queue::{
    open_relative, FourLevelCursor, Queue, RecoveryCursor, RecoveryPhase, ThreeLevelCursor,
    WallFloor,
};

const RECOVERY_CURSOR_SCHEMA: &str = "steadq-recovery-cursor";
const RECOVERY_CURSOR_VERSION: u16 = 1;
const RECOVERY_CURSOR_FILE: &str = "recovery-cursor.json";
const RECOVERY_CURSOR_MAX_BYTES: u64 = 16 * 1024;
const RECOVERY_CURSOR_OPEN_FLAGS: i32 = libc::O_CLOEXEC + libc::O_NOFOLLOW;
const RECOVERY_LOCK_OPEN_FLAGS: i32 = libc::O_CLOEXEC + libc::O_NOFOLLOW + libc::O_RDWR;
const MAX_RECOVERY_DIRECTORY_ENTRIES: usize = 65_536;
const MAX_RECOVERY_DIRECTORY_NAME_BYTES: usize = MAX_RECOVERY_DIRECTORY_ENTRIES * 255;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryCursorRecord {
    schema: String,
    version: u16,
    queue_id: String,
    cursor: RecoveryCursor,
}

fn cursor_component_is_valid(component: &[u8]) -> bool {
    !component.is_empty()
        && component.len() <= 255
        && component != b"."
        && component != b".."
        && !component.contains(&b'/')
        && !component.contains(&b'\0')
}

fn read_recovery_directory(
    dir_fd: std::os::unix::io::RawFd,
    deadline_mono: u64,
) -> Result<Vec<fs::DirEntryName>, RecoveryDirectoryError> {
    fs::read_dir_entry_names_bounded_owned_until(
        dir_fd,
        MAX_RECOVERY_DIRECTORY_ENTRIES,
        MAX_RECOVERY_DIRECTORY_NAME_BYTES,
        || Queue::budget_time_exceeded(deadline_mono),
    )
    .map_err(|error| match error {
        fs::DirectoryEnumerationError::Cancelled => RecoveryDirectoryError::BudgetExhausted,
        fs::DirectoryEnumerationError::CancellationCheck(error) => {
            RecoveryDirectoryError::Clock(error)
        }
        fs::DirectoryEnumerationError::Io(error) => RecoveryDirectoryError::Io(error),
    })
}

#[derive(Debug)]
enum RecoveryDirectoryError {
    BudgetExhausted,
    Clock(io::Error),
    Io(io::Error),
}

fn raw_name_for_error(name: &fs::DirEntryName) -> String {
    format!("{name:?}")
}

fn cursor_is_valid(cursor: &RecoveryCursor) -> bool {
    let three_level_is_valid = |scan: &ThreeLevelCursor| {
        cursor_component_is_valid(&scan.first)
            && cursor_component_is_valid(&scan.second)
            && cursor_component_is_valid(&scan.resume_after)
    };
    let four_level_is_valid = |scan: &FourLevelCursor| {
        [&scan.first, &scan.second, &scan.third, &scan.resume_after]
            .into_iter()
            .all(|component| cursor_component_is_valid(component))
    };

    cursor.reap_leases.as_ref().is_none_or(four_level_is_valid)
        && cursor
            .promote_delayed
            .as_ref()
            .is_none_or(three_level_is_valid)
        && cursor
            .cleanup_temp
            .as_ref()
            .is_none_or(three_level_is_valid)
        && cursor
            .compact_receipts
            .as_ref()
            .is_none_or(three_level_is_valid)
        && cursor
            .delete_receipts
            .as_ref()
            .is_none_or(three_level_is_valid)
}

fn cursor_file_metadata_is_valid(mode: libc::mode_t, link_count: libc::nlink_t) -> bool {
    mode & libc::S_IFMT == libc::S_IFREG && link_count == 1
}

fn cursor_record_size_is_valid(size: u64) -> bool {
    (1..=RECOVERY_CURSOR_MAX_BYTES).contains(&size)
}

fn cursor_record_version_is_supported(record: &RecoveryCursorRecord) -> bool {
    record.schema == RECOVERY_CURSOR_SCHEMA && record.version == RECOVERY_CURSOR_VERSION
}

fn cursor_record_bytes_fit(size: usize) -> bool {
    u64::try_from(size).is_ok_and(cursor_record_size_is_valid)
}

fn cursor_file_is_absent(error: &io::Error) -> bool {
    error.raw_os_error() == Some(libc::ENOENT)
}

fn recovery_lock_exists(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::AlreadyExists
}

pub(crate) fn load_recovery_cursor(
    root_fd: std::os::unix::io::RawFd,
    queue_id: &[u8; 16],
) -> Result<RecoveryCursor, Error> {
    let control_fd = fs::open_directory(root_fd, "control")
        .map_err(|error| Error::IoFailure(error.to_string()))?;
    let cursor_fd = match fs::openat(
        control_fd.as_raw_fd(),
        RECOVERY_CURSOR_FILE,
        RECOVERY_CURSOR_OPEN_FLAGS,
        0,
    ) {
        Ok(fd) => fd,
        Err(error) if cursor_file_is_absent(&error) => {
            return Ok(RecoveryCursor::default());
        }
        Err(error) => return Err(Error::IoFailure(error.to_string())),
    };
    let stat =
        fs::fstat(cursor_fd.as_raw_fd()).map_err(|error| Error::IoFailure(error.to_string()))?;
    if !cursor_file_metadata_is_valid(stat.st_mode, stat.st_nlink) {
        return Err(Error::QueueCorrupt(
            "recovery cursor is not a singly linked regular file".into(),
        ));
    }
    let size = u64::try_from(stat.st_size)
        .map_err(|_| Error::QueueCorrupt("recovery cursor has negative size".into()))?;
    if !cursor_record_size_is_valid(size) {
        return Err(Error::QueueCorrupt(
            "recovery cursor size is invalid".into(),
        ));
    }
    let mut bytes = vec![
        0;
        usize::try_from(size).map_err(|_| Error::QueueCorrupt(
            "recovery cursor size is unsupported".into()
        ))?
    ];
    fs::pread_exact(cursor_fd.as_raw_fd(), &mut bytes, 0)
        .map_err(|error| Error::IoFailure(error.to_string()))?;
    let record: RecoveryCursorRecord = serde_json::from_slice(&bytes)
        .map_err(|error| Error::QueueCorrupt(format!("recovery cursor decode: {error}")))?;
    if !cursor_record_version_is_supported(&record) {
        return Err(Error::QueueCorrupt(
            "recovery cursor schema or version is unsupported".into(),
        ));
    }
    if record.queue_id != steadq_names::hex_encode(queue_id) {
        return Err(Error::QueueCorrupt(
            "recovery cursor belongs to another queue".into(),
        ));
    }
    if !cursor_is_valid(&record.cursor) {
        return Err(Error::QueueCorrupt(
            "recovery cursor contains an invalid component".into(),
        ));
    }
    Ok(record.cursor)
}

/// Recovery work budget.
#[derive(Clone, Debug)]
pub struct WorkBudget {
    pub max_operations: u32,
    pub max_duration_ms: u64,
}

impl Default for WorkBudget {
    fn default() -> Self {
        Self {
            max_operations: 1000,
            max_duration_ms: 100,
        }
    }
}

/// Recovery statistics.
#[derive(Clone, Debug, Default)]
pub struct RecoveryStats {
    pub operations_attempted: u32,
    pub temp_files_deleted: u32,
    pub delayed_promoted: u32,
    pub leases_reaped: u32,
    pub leases_to_dead: u32,
    pub buckets_removed: u32,
    pub receipts_compacted: u32,
    pub receipts_expired: u32,
    pub budget_exhausted: bool,
    pub phase_blocked: bool,
    pub errors: Vec<RecoveryError>,
    pub scan_skips: u32,
}

#[derive(Clone, Debug)]
pub struct RecoveryError {
    pub operation: String,
    pub relative_path: String,
    pub error: String,
}

impl Queue {
    fn acquire_recovery_lock(&self) -> Result<OwnedFd, Error> {
        let control_fd = fs::open_directory(self.root_fd(), "control")
            .map_err(|error| Error::IoFailure(error.to_string()))?;
        let lock_fd = match fs::create_exclusive(control_fd.as_raw_fd(), "recovery.lock", 0o600) {
            Ok(fd) => {
                fs::fsync(fd.as_raw_fd()).map_err(|error| Error::IoFailure(error.to_string()))?;
                fs::fsync_dir_fd(control_fd.as_raw_fd())
                    .map_err(|error| Error::IoFailure(error.to_string()))?;
                fd
            }
            Err(error) if recovery_lock_exists(&error) => fs::openat(
                control_fd.as_raw_fd(),
                "recovery.lock",
                RECOVERY_LOCK_OPEN_FLAGS,
                0,
            )
            .map_err(|error| Error::IoFailure(error.to_string()))?,
            Err(error) => return Err(Error::IoFailure(error.to_string())),
        };
        if !fs::try_ofd_write_lock(lock_fd.as_raw_fd())
            .map_err(|error| Error::IoFailure(error.to_string()))?
        {
            return Err(Error::MaintenanceBusy);
        }
        Ok(lock_fd)
    }

    fn persist_recovery_cursor(&self) -> Result<(), Error> {
        let record = RecoveryCursorRecord {
            schema: RECOVERY_CURSOR_SCHEMA.to_string(),
            version: RECOVERY_CURSOR_VERSION,
            queue_id: steadq_names::hex_encode(&self.format.queue_id),
            cursor: self.recovery_cursor.clone(),
        };
        let bytes = serde_json::to_vec(&record)
            .map_err(|error| Error::IoFailure(format!("recovery cursor encode: {error}")))?;
        if !cursor_record_bytes_fit(bytes.len()) {
            return Err(Error::InvalidInput(
                "recovery cursor exceeds maximum encoded size".into(),
            ));
        }
        let control_fd = fs::open_directory(self.root_fd(), "control")
            .map_err(|error| Error::IoFailure(error.to_string()))?;
        let temp_name = format!(
            ".recovery-cursor.{}.tmp",
            steadq_names::hex_encode(
                &fs::random_128bit().map_err(|error| Error::IoFailure(error.to_string()))?
            )
        );
        let temp_fd = fs::create_exclusive(control_fd.as_raw_fd(), &temp_name, 0o600)
            .map_err(|error| Error::IoFailure(error.to_string()))?;
        if let Err(error) = fs::write_all(temp_fd.as_raw_fd(), &bytes)
            .and_then(|()| fs::fsync(temp_fd.as_raw_fd()))
            .and_then(|()| {
                fs::durable_move_replace(
                    control_fd.as_raw_fd(),
                    &temp_name,
                    control_fd.as_raw_fd(),
                    RECOVERY_CURSOR_FILE,
                )
            })
        {
            let _ = fs::unlinkat(control_fd.as_raw_fd(), &temp_name);
            return Err(Error::IoFailure(error.to_string()));
        }
        Ok(())
    }

    /// Run one bounded recovery pass.
    pub fn recover(&mut self, budget: &WorkBudget) -> RecoveryStats {
        let mut stats = RecoveryStats::default();
        let _recovery_lock = match self.acquire_recovery_lock() {
            Ok(lock) => lock,
            Err(error) => {
                stats.errors.push(RecoveryError {
                    operation: "recovery_lock".into(),
                    relative_path: "control/recovery.lock".into(),
                    error: error.to_string(),
                });
                return stats;
            }
        };
        self.recovery_cursor = match load_recovery_cursor(self.root_fd(), &self.format.queue_id) {
            Ok(cursor) => cursor,
            Err(error) => {
                stats.errors.push(RecoveryError {
                    operation: "recovery_cursor_reload".into(),
                    relative_path: format!("control/{RECOVERY_CURSOR_FILE}"),
                    error: error.to_string(),
                });
                return stats;
            }
        };
        let boottime_now = match fs::clock_boottime_ns() {
            Ok(t) => t,
            Err(e) => {
                let mut s = RecoveryStats::default();
                s.errors.push(RecoveryError {
                    operation: "clock_boottime".into(),
                    relative_path: "/".into(),
                    error: e.to_string(),
                });
                return s;
            }
        };
        // P1-12: Use checked wall floor. If unavailable, record error and
        // skip wall-sensitive phases (delayed promotion, receipt retention).
        let wall_floor = self.stabilized_wall_floor();
        if let Err(error) = &wall_floor {
            stats.errors.push(RecoveryError {
                operation: "wall_floor".into(),
                relative_path: "/".into(),
                error: format!(
                    "wall floor unavailable, skipping wall-sensitive recovery actions: {error}"
                ),
            });
        }
        let wall_floor = wall_floor.ok();
        // C-31: Use CLOCK_MONOTONIC for budget enforcement
        let start_mono = match fs::clock_monotonic_ns() {
            Ok(t) => t,
            Err(e) => {
                let mut s = RecoveryStats::default();
                s.errors.push(RecoveryError {
                    operation: "clock_monotonic".into(),
                    relative_path: "/".into(),
                    error: e.to_string(),
                });
                return s;
            }
        };
        let deadline_mono =
            start_mono.saturating_add(budget.max_duration_ms.saturating_mul(1_000_000));

        loop {
            if !Self::has_recovery_budget(&stats) {
                break;
            }
            let phase = self.recovery_cursor.phase;
            let next_phase = match phase {
                RecoveryPhase::ReapLeases => {
                    self.reap_expired_leases(
                        boottime_now,
                        wall_floor,
                        budget,
                        &mut stats,
                        deadline_mono,
                    );
                    RecoveryPhase::PromoteDelayed
                }
                RecoveryPhase::PromoteDelayed => {
                    if let Some(wall_floor) = wall_floor {
                        self.promote_delayed(wall_floor, budget, &mut stats, deadline_mono);
                    }
                    RecoveryPhase::CleanupTemp
                }
                RecoveryPhase::CleanupTemp => {
                    self.cleanup_temp_files(boottime_now, budget, &mut stats, deadline_mono);
                    RecoveryPhase::CompactReceipts
                }
                RecoveryPhase::CompactReceipts => {
                    self.compact_receipts(budget, &mut stats, deadline_mono);
                    RecoveryPhase::DeleteReceipts
                }
                RecoveryPhase::DeleteReceipts => {
                    if let Some(wall_floor) = wall_floor {
                        self.delete_expired_receipts(
                            wall_floor,
                            self.options.receipt_retention_ns,
                            budget,
                            &mut stats,
                            deadline_mono,
                        );
                    }
                    RecoveryPhase::ReapLeases
                }
            };
            if Self::has_recovery_budget(&stats) {
                self.recovery_cursor.phase = next_phase;
            }
            if Self::budget_exhausted(&mut stats, budget, deadline_mono) {
                stats.budget_exhausted = true;
            }
            if phase == RecoveryPhase::DeleteReceipts {
                break;
            }
        }

        if let Err(error) = self.persist_recovery_cursor() {
            stats.errors.push(RecoveryError {
                operation: "recovery_cursor_persist".into(),
                relative_path: format!("control/{RECOVERY_CURSOR_FILE}"),
                error: error.to_string(),
            });
        }

        stats
    }

    /// B1: Quarantine an object during recovery.
    fn quarantine_recovery_object(
        &self,
        src_dir_fd: std::os::unix::io::RawFd,
        filename: &str,
        full_path: &str,
        reason: crate::QuarantineReason,
    ) -> Result<(), Error> {
        let qid = fs::random_128bit().map_err(|e| Error::IoFailure(e.to_string()))?;
        let q_name = steadq_names::quarantine_filename(&qid, reason as u16);
        let _ = self.ensure_dir("quarantine");
        let q_dir_fd = crate::queue::open_relative(self.root_fd(), "quarantine")
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        fs::durable_move_noreplace(src_dir_fd, filename, q_dir_fd.as_raw_fd(), &q_name)
            .map_err(|e| Error::IoFailure(format!("quarantine move failed: {e}")))?;
        let _ = full_path; // logged by caller
        Ok(())
    }

    /// R2-H05: Check if the monotonic deadline has been exceeded.
    fn budget_time_exceeded(deadline_mono: u64) -> io::Result<bool> {
        fs::clock_monotonic_ns().map(|now| now >= deadline_mono)
    }

    /// Check if either operations or time budget is exhausted.
    fn budget_exhausted(
        stats: &mut RecoveryStats,
        budget: &WorkBudget,
        deadline_mono: u64,
    ) -> bool {
        if stats.operations_attempted >= budget.max_operations {
            return true;
        }
        match Self::budget_time_exceeded(deadline_mono) {
            Ok(exceeded) => exceeded,
            Err(error) => {
                Self::block_phase(
                    stats,
                    "clock_monotonic",
                    "/",
                    &format!("recovery budget clock unavailable: {error}"),
                );
                true
            }
        }
    }

    fn has_recovery_budget(stats: &RecoveryStats) -> bool {
        !stats.budget_exhausted
    }

    /// R2-H06: Record a recovery error with full context.
    #[allow(dead_code)]
    fn record_error(stats: &mut RecoveryStats, op: &str, path: &str, err: &str) {
        stats.errors.push(RecoveryError {
            operation: op.into(),
            relative_path: path.into(),
            error: err.into(),
        });
    }

    fn block_phase(stats: &mut RecoveryStats, op: &str, path: &str, err: &str) {
        Self::record_error(stats, op, path, err);
        stats.phase_blocked = true;
    }

    fn stop_for_directory_error(
        stats: &mut RecoveryStats,
        op: &str,
        path: &str,
        error: &RecoveryDirectoryError,
    ) {
        match error {
            RecoveryDirectoryError::BudgetExhausted => stats.budget_exhausted = true,
            RecoveryDirectoryError::Clock(error) => Self::block_phase(
                stats,
                "clock_monotonic",
                path,
                &format!("directory budget clock unavailable during {op}: {error}"),
            ),
            RecoveryDirectoryError::Io(error) => {
                Self::block_phase(stats, op, path, &error.to_string());
            }
        }
    }

    fn reap_expired_leases(
        &mut self,
        boottime_now: u64,
        wall_floor: Option<WallFloor>,
        budget: &WorkBudget,
        stats: &mut RecoveryStats,
        deadline_mono: u64,
    ) {
        let root_fd = self.root_fd();

        // Scan leased/ directories
        let leased_fd = match fs::open_directory(root_fd, "leased") {
            Ok(fd) => fd,
            Err(e) => {
                Self::block_phase(stats, "open_leased_dir", "leased", &e.to_string());
                return;
            }
        };

        let mut boot_dirs = match read_recovery_directory(leased_fd.as_raw_fd(), deadline_mono) {
            Ok(e) => e,
            Err(e) => {
                Self::stop_for_directory_error(stats, "read_leased_dirs", "leased", &e);
                return;
            }
        };
        boot_dirs.sort();

        for boot_dir_entry in &boot_dirs {
            if let Some(cursor) = &self.recovery_cursor.reap_leases {
                if boot_dir_entry.as_bytes() < cursor.first.as_slice() {
                    continue;
                }
            }
            if Self::budget_exhausted(stats, budget, deadline_mono) {
                stats.budget_exhausted = true;
                return;
            }
            let Some(boot_dir_name) = boot_dir_entry.as_str() else {
                Self::record_error(
                    stats,
                    "reap_boot_name",
                    &raw_name_for_error(boot_dir_entry),
                    "boot directory name is not UTF-8",
                );
                continue;
            };
            if steadq_names::boot_id_bytes(boot_dir_name).is_none() {
                Self::record_error(
                    stats,
                    "reap_boot_name",
                    boot_dir_name,
                    "boot directory name is not canonical",
                );
                continue;
            }

            let is_current_boot = boot_dir_name == self.boot_id;

            let boot_dir_fd = match fs::open_directory(leased_fd.as_raw_fd(), boot_dir_name) {
                Ok(fd) => fd,
                Err(error) => {
                    stats.scan_skips += 1;
                    Self::block_phase(stats, "reap_boot_open", boot_dir_name, &error.to_string());
                    return;
                }
            };

            let mut bucket_dirs =
                match read_recovery_directory(boot_dir_fd.as_raw_fd(), deadline_mono) {
                    Ok(e) => e,
                    Err(error) => {
                        stats.scan_skips += 1;
                        Self::stop_for_directory_error(
                            stats,
                            "reap_bucket_read",
                            boot_dir_name,
                            &error,
                        );
                        return;
                    }
                };
            bucket_dirs.sort();

            for bucket_entry in &bucket_dirs {
                if let Some(cursor) = &self.recovery_cursor.reap_leases {
                    if boot_dir_entry.as_bytes() == cursor.first
                        && bucket_entry.as_bytes() < cursor.second.as_slice()
                    {
                        continue;
                    }
                }
                if Self::budget_exhausted(stats, budget, deadline_mono) {
                    stats.budget_exhausted = true;
                    return;
                }
                let Some(bucket_name) = bucket_entry.as_str() else {
                    Self::record_error(
                        stats,
                        "reap_bucket_name",
                        &raw_name_for_error(bucket_entry),
                        "bucket directory name is not UTF-8",
                    );
                    continue;
                };
                let Some(bucket_num) = steadq_names::bucket_from_hex(bucket_name) else {
                    Self::record_error(
                        stats,
                        "reap_bucket_name",
                        bucket_name,
                        "bucket directory name is not canonical",
                    );
                    continue;
                };

                // For current boot, check if bucket is expired
                if is_current_boot {
                    let Some(current_bucket) =
                        steadq_math::bucket_number(boottime_now, self.format.lease_bucket_width_ns)
                    else {
                        Self::block_phase(
                            stats,
                            "reap_bucket_check",
                            &format!("leased/{boot_dir_name}/{bucket_name}"),
                            "invalid lease bucket width",
                        );
                        return;
                    };
                    if bucket_num > current_bucket {
                        continue; // Not yet eligible
                    }
                }

                let bucket_fd = match fs::open_directory(boot_dir_fd.as_raw_fd(), bucket_name) {
                    Ok(fd) => fd,
                    Err(error) => {
                        stats.scan_skips += 1;
                        Self::block_phase(
                            stats,
                            "reap_bucket_open",
                            &format!("leased/{boot_dir_name}/{bucket_name}"),
                            &error.to_string(),
                        );
                        return;
                    }
                };

                let mut shard_dirs =
                    match read_recovery_directory(bucket_fd.as_raw_fd(), deadline_mono) {
                        Ok(e) => e,
                        Err(error) => {
                            stats.scan_skips += 1;
                            Self::stop_for_directory_error(
                                stats,
                                "reap_shard_read",
                                &format!("leased/{boot_dir_name}/{bucket_name}"),
                                &error,
                            );
                            return;
                        }
                    };
                shard_dirs.sort();

                for shard_entry in &shard_dirs {
                    if let Some(cursor) = &self.recovery_cursor.reap_leases {
                        if boot_dir_entry.as_bytes() == cursor.first
                            && bucket_entry.as_bytes() == cursor.second
                            && shard_entry.as_bytes() < cursor.third.as_slice()
                        {
                            continue;
                        }
                    }
                    let Some(shard_name) = shard_entry.as_str() else {
                        Self::record_error(
                            stats,
                            "reap_shard_name",
                            &raw_name_for_error(shard_entry),
                            "shard directory name is not UTF-8",
                        );
                        continue;
                    };
                    let Some(shard) = steadq_names::shard_from_hex(shard_name) else {
                        Self::record_error(
                            stats,
                            "reap_shard_name",
                            shard_name,
                            "shard directory name is not canonical",
                        );
                        continue;
                    };
                    if shard >= self.format.shard_count {
                        Self::record_error(
                            stats,
                            "reap_shard_name",
                            shard_name,
                            "shard directory is outside the queue shard range",
                        );
                        continue;
                    }
                    let shard_fd = match fs::open_directory(bucket_fd.as_raw_fd(), shard_name) {
                        Ok(fd) => fd,
                        Err(error) => {
                            stats.scan_skips += 1;
                            Self::block_phase(
                                stats,
                                "reap_shard_open",
                                &format!("leased/{boot_dir_name}/{bucket_name}/{shard_name}"),
                                &error.to_string(),
                            );
                            return;
                        }
                    };

                    let mut entries =
                        match read_recovery_directory(shard_fd.as_raw_fd(), deadline_mono) {
                            Ok(e) => e,
                            Err(error) => {
                                stats.scan_skips += 1;
                                Self::stop_for_directory_error(
                                    stats,
                                    "reap_entry_read",
                                    &format!("leased/{boot_dir_name}/{bucket_name}/{shard_name}"),
                                    &error,
                                );
                                return;
                            }
                        };
                    entries.sort();

                    for raw_entry in &entries {
                        if let Some(cursor) = &self.recovery_cursor.reap_leases {
                            if cursor.should_skip(
                                boot_dir_entry.as_bytes(),
                                bucket_entry.as_bytes(),
                                shard_entry.as_bytes(),
                                raw_entry.as_bytes(),
                            ) {
                                continue;
                            }
                        }
                        if Self::budget_exhausted(stats, budget, deadline_mono) {
                            stats.budget_exhausted = true;
                            return;
                        }
                        self.recovery_cursor.reap_leases = Some(FourLevelCursor::new(
                            boot_dir_entry.as_bytes(),
                            bucket_entry.as_bytes(),
                            shard_entry.as_bytes(),
                            raw_entry.as_bytes(),
                        ));
                        let Some(entry) = raw_entry.as_str() else {
                            Self::record_error(
                                stats,
                                "reap_entry_name",
                                &raw_name_for_error(raw_entry),
                                "entry name is not UTF-8",
                            );
                            continue;
                        };

                        if !entry.ends_with(".sqj") {
                            continue;
                        }

                        stats.operations_attempted += 1;

                        // Parse the leased filename to get deadline and attempt info
                        let parsed = match steadq_names::parse_leased(entry) {
                            Ok(p) => p,
                            // C-34: Malformed entries should be quarantined, not skipped
                            Err(_) => {
                                stats.errors.push(RecoveryError {
                                    operation: "reap_parse".into(),
                                    relative_path: format!(
                                        "leased/{boot_dir_name}/{bucket_name}/{shard_name}/{entry}"
                                    ),
                                    error: "malformed leased filename".into(),
                                });
                                continue;
                            }
                        };

                        // For current boot, check actual deadline
                        if is_current_boot && parsed.boottime_deadline_ns > boottime_now {
                            continue;
                        }

                        // B1: Validate object structure before recovery transition
                        let leased_ctx = crate::ActivePathContext::Leased {
                            boot_id: boot_dir_name.to_string(),
                            bucket: bucket_name.to_string(),
                            shard: shard_name.to_string(),
                        };
                        if let Err(e) =
                            self.validate_active_object(shard_fd.as_raw_fd(), entry, &leased_ctx)
                        {
                            Self::record_error(
                                stats,
                                "reap_validate",
                                &format!(
                                    "leased/{boot_dir_name}/{bucket_name}/{shard_name}/{entry}"
                                ),
                                &format!("{e}"),
                            );
                            // B1: Quarantine corrupt objects
                            if matches!(e, Error::QueueCorrupt(_)) {
                                let _ = self.quarantine_recovery_object(
                                    shard_fd.as_raw_fd(),
                                    entry,
                                    &format!(
                                        "leased/{boot_dir_name}/{bucket_name}/{shard_name}/{entry}"
                                    ),
                                    crate::QuarantineReason::EnvelopeCorrupt,
                                );
                            }
                            continue;
                        }

                        // R4-B02: Verify bucket placement matches deadline-derived bucket
                        let Some(expected_lease_bucket) = steadq_math::lease_bucket(
                            parsed.boottime_deadline_ns,
                            self.format.lease_bucket_width_ns,
                        ) else {
                            Self::record_error(
                                stats,
                                "reap_bucket_check",
                                &format!(
                                    "leased/{boot_dir_name}/{bucket_name}/{shard_name}/{entry}"
                                ),
                                "invalid lease bucket width",
                            );
                            return;
                        };
                        let actual_bucket = match u64::from_str_radix(bucket_name, 16) {
                            Ok(bucket) => bucket,
                            Err(_) => {
                                Self::record_error(
                                    stats,
                                    "reap_bucket_check",
                                    &format!(
                                        "leased/{boot_dir_name}/{bucket_name}/{shard_name}/{entry}"
                                    ),
                                    "invalid lease bucket name",
                                );
                                continue;
                            }
                        };
                        if actual_bucket != expected_lease_bucket {
                            Self::record_error(
                                stats,
                                "reap_bucket_check",
                                &format!(
                                    "leased/{boot_dir_name}/{bucket_name}/{shard_name}/{entry}"
                                ),
                                &format!(
                                    "bucket mismatch: dir {actual_bucket} != deadline-derived {expected_lease_bucket}"
                                ),
                            );
                            continue;
                        }

                        // Determine destination: ready or dead
                        if parsed.common.attempt >= parsed.common.maximum_attempts {
                            let Some(wall_floor) = wall_floor else {
                                Self::record_error(
                                    stats,
                                    "reap_to_dead",
                                    &format!(
                                        "leased/{boot_dir_name}/{bucket_name}/{shard_name}/{entry}"
                                    ),
                                    "authenticated wall floor unavailable",
                                );
                                continue;
                            };
                            // Move to dead
                            if self
                                .reap_to_dead(
                                    boot_dir_name,
                                    bucket_name,
                                    shard_name,
                                    entry,
                                    &parsed.common,
                                    DeadReason::AttemptsExhausted,
                                    wall_floor,
                                )
                                .is_ok()
                            {
                                stats.leases_to_dead += 1;
                            }
                        } else {
                            // Move to ready
                            if self
                                .reap_to_ready(
                                    boot_dir_name,
                                    bucket_name,
                                    shard_name,
                                    entry,
                                    &parsed.common,
                                )
                                .is_ok()
                            {
                                stats.leases_reaped += 1;
                            }
                        }
                    }
                }
            }
        }
        self.recovery_cursor.reap_leases = None;
    }

    fn reap_to_ready(
        &self,
        boot_dir: &str,
        bucket: &str,
        shard: &str,
        leased_name: &str,
        common: &steadq_names::CommonFields,
    ) -> io::Result<()> {
        let src_dir = format!("leased/{boot_dir}/{bucket}/{shard}");
        let dest_dir = format!("ready/{shard}");

        let new_gen = common
            .generation
            .checked_add(1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "generation overflow"))?;
        let ready_common = steadq_names::CommonFields {
            job_id: common.job_id,
            generation: new_gen,
            attempt: common.attempt,
            maximum_attempts: common.maximum_attempts,
        };

        let ready_name = steadq_names::make_ready_name(&self.format.queue_id, shard, &ready_common);

        let src_fd = open_relative(self.root_fd(), &src_dir)?;
        let dest_fd = open_relative(self.root_fd(), &dest_dir)?;

        fs::renameat2_noreplace(
            src_fd.as_raw_fd(),
            leased_name,
            dest_fd.as_raw_fd(),
            &ready_name,
        )?;
        fs::fsync_dir_fd(dest_fd.as_raw_fd())?;
        fs::fsync_dir_fd(src_fd.as_raw_fd())?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn reap_to_dead(
        &self,
        boot_dir: &str,
        bucket: &str,
        shard: &str,
        leased_name: &str,
        common: &steadq_names::CommonFields,
        reason: DeadReason,
        wall_floor: WallFloor,
    ) -> io::Result<()> {
        let src_dir = format!("leased/{boot_dir}/{bucket}/{shard}");
        let terminal_bucket =
            steadq_math::bucket_number(wall_floor.unix_ns(), self.format.terminal_bucket_width_ns)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "terminal bucket overflow")
                })?;
        let bucket_str = bucket_hex(terminal_bucket);
        let dest_dir = format!("dead/{bucket_str}/{shard}");

        let new_gen = common
            .generation
            .checked_add(1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "generation overflow"))?;
        let dead_common = steadq_names::CommonFields {
            job_id: common.job_id,
            generation: new_gen,
            attempt: common.attempt,
            maximum_attempts: common.maximum_attempts,
        };

        let dead_name = steadq_names::make_dead_name(
            &self.format.queue_id,
            &bucket_str,
            shard,
            &dead_common,
            reason as u16,
        );

        let _ = self.ensure_dir_pub(&dest_dir);
        let src_fd = open_relative(self.root_fd(), &src_dir)?;
        let dest_fd = open_relative(self.root_fd(), &dest_dir)?;

        fs::renameat2_noreplace(
            src_fd.as_raw_fd(),
            leased_name,
            dest_fd.as_raw_fd(),
            &dead_name,
        )?;
        fs::fsync_dir_fd(dest_fd.as_raw_fd())?;
        fs::fsync_dir_fd(src_fd.as_raw_fd())?;
        Ok(())
    }

    fn promote_delayed(
        &mut self,
        wall_floor: WallFloor,
        budget: &WorkBudget,
        stats: &mut RecoveryStats,
        deadline_mono: u64,
    ) {
        let root_fd = self.root_fd();
        let delayed_fd = match fs::open_directory(root_fd, "delayed") {
            Ok(fd) => fd,
            Err(error) => {
                Self::block_phase(stats, "promote_root_open", "delayed", &error.to_string());
                return;
            }
        };

        let mut bucket_dirs = match read_recovery_directory(delayed_fd.as_raw_fd(), deadline_mono) {
            Ok(e) => e,
            Err(error) => {
                Self::stop_for_directory_error(stats, "promote_bucket_read", "delayed", &error);
                return;
            }
        };
        bucket_dirs.sort();

        for bucket_entry in &bucket_dirs {
            // R4-RES: Skip buckets already processed in a prior pass.
            if let Some(cursor) = &self.recovery_cursor.promote_delayed {
                if bucket_entry.as_bytes() < cursor.first.as_slice() {
                    continue;
                }
            }

            if Self::budget_exhausted(stats, budget, deadline_mono) {
                stats.budget_exhausted = true;
                return;
            }
            let Some(bucket_name) = bucket_entry.as_str() else {
                Self::record_error(
                    stats,
                    "promote_bucket_name",
                    &raw_name_for_error(bucket_entry),
                    "bucket directory name is not UTF-8",
                );
                continue;
            };

            let bucket_num = match steadq_names::bucket_from_hex(bucket_name) {
                Some(bucket) => bucket,
                None => {
                    Self::record_error(
                        stats,
                        "promote_bucket_name",
                        bucket_name,
                        "bucket directory name is not canonical",
                    );
                    continue;
                }
            };

            // Read effective wall floor
            let current_wall_bucket = match steadq_math::bucket_number(
                wall_floor.unix_ns(),
                self.format.delayed_bucket_width_ns,
            ) {
                Some(bucket) => bucket,
                None => return,
            };

            // Only promote buckets at or below the current wall bucket
            if bucket_num > current_wall_bucket {
                continue;
            }

            let bucket_fd = match fs::open_directory(delayed_fd.as_raw_fd(), bucket_name) {
                Ok(fd) => fd,
                Err(error) => {
                    stats.scan_skips += 1;
                    Self::block_phase(
                        stats,
                        "promote_bucket_open",
                        &format!("delayed/{bucket_name}"),
                        &error.to_string(),
                    );
                    return;
                }
            };

            let mut shard_dirs = match read_recovery_directory(bucket_fd.as_raw_fd(), deadline_mono)
            {
                Ok(e) => e,
                Err(error) => {
                    stats.scan_skips += 1;
                    Self::stop_for_directory_error(
                        stats,
                        "promote_shard_read",
                        &format!("delayed/{bucket_name}"),
                        &error,
                    );
                    return;
                }
            };
            shard_dirs.sort();

            for shard_entry in &shard_dirs {
                // Entry level cursor: skip shards before cursor when bucket matches.
                if let Some(cursor) = &self.recovery_cursor.promote_delayed {
                    if bucket_entry.as_bytes() == cursor.first
                        && shard_entry.as_bytes() < cursor.second.as_slice()
                    {
                        continue;
                    }
                }
                let Some(shard_name) = shard_entry.as_str() else {
                    Self::record_error(
                        stats,
                        "promote_shard_name",
                        &raw_name_for_error(shard_entry),
                        "shard directory name is not UTF-8",
                    );
                    continue;
                };
                let Some(shard) = steadq_names::shard_from_hex(shard_name) else {
                    Self::record_error(
                        stats,
                        "promote_shard_name",
                        shard_name,
                        "shard directory name is not canonical",
                    );
                    continue;
                };
                if shard >= self.format.shard_count {
                    Self::record_error(
                        stats,
                        "promote_shard_name",
                        shard_name,
                        "shard directory is outside the queue shard range",
                    );
                    continue;
                }
                let shard_fd = match fs::open_directory(bucket_fd.as_raw_fd(), shard_name) {
                    Ok(fd) => fd,
                    Err(error) => {
                        Self::block_phase(
                            stats,
                            "promote_shard_open",
                            &format!("{bucket_name}/{shard_name}"),
                            &error.to_string(),
                        );
                        return;
                    }
                };

                let mut entries = match read_recovery_directory(shard_fd.as_raw_fd(), deadline_mono)
                {
                    Ok(e) => e,
                    Err(error) => {
                        stats.scan_skips += 1;
                        Self::stop_for_directory_error(
                            stats,
                            "promote_entry_read",
                            &format!("delayed/{bucket_name}/{shard_name}"),
                            &error,
                        );
                        return;
                    }
                };
                entries.sort();

                for raw_entry in &entries {
                    // Entry level cursor: skip entries at or before cursor when bucket and shard match.
                    if let Some(cursor) = &self.recovery_cursor.promote_delayed {
                        if cursor.should_skip(
                            bucket_entry.as_bytes(),
                            shard_entry.as_bytes(),
                            raw_entry.as_bytes(),
                        ) {
                            continue;
                        }
                    }
                    if Self::budget_exhausted(stats, budget, deadline_mono) {
                        stats.budget_exhausted = true;
                        return;
                    }
                    self.recovery_cursor.promote_delayed = Some(ThreeLevelCursor::new(
                        bucket_entry.as_bytes(),
                        shard_entry.as_bytes(),
                        raw_entry.as_bytes(),
                    ));
                    let Some(entry) = raw_entry.as_str() else {
                        Self::record_error(
                            stats,
                            "promote_entry_name",
                            &raw_name_for_error(raw_entry),
                            "entry name is not UTF-8",
                        );
                        continue;
                    };

                    if !entry.ends_with(".sqj") {
                        continue;
                    }

                    stats.operations_attempted += 1;

                    let parsed = match steadq_names::parse_delayed(entry) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };

                    // B1: Validate object structure before promotion
                    {
                        let delayed_ctx = crate::ActivePathContext::Delayed {
                            bucket: bucket_name.to_string(),
                            shard: shard_name.to_string(),
                        };
                        if let Err(e) =
                            self.validate_active_object(shard_fd.as_raw_fd(), entry, &delayed_ctx)
                        {
                            Self::record_error(
                                stats,
                                "promote_validate",
                                &format!("delayed/{bucket_name}/{shard_name}/{entry}"),
                                &format!("{e}"),
                            );
                            if matches!(e, Error::QueueCorrupt(_)) {
                                let _ = self.quarantine_recovery_object(
                                    shard_fd.as_raw_fd(),
                                    entry,
                                    &format!("delayed/{bucket_name}/{shard_name}/{entry}"),
                                    crate::QuarantineReason::EnvelopeCorrupt,
                                );
                            }
                            continue;
                        }
                    }

                    // Promote: move delayed -> ready
                    let src_dir = format!("delayed/{bucket_name}/{shard_name}");
                    let dest_dir = format!("ready/{shard_name}");

                    let new_gen = match parsed.common.generation.checked_add(1) {
                        Some(g) => g,
                        None => continue,
                    };
                    let ready_common = steadq_names::CommonFields {
                        job_id: parsed.common.job_id,
                        generation: new_gen,
                        attempt: parsed.common.attempt,
                        maximum_attempts: parsed.common.maximum_attempts,
                    };

                    let ready_name = steadq_names::make_ready_name(
                        &self.format.queue_id,
                        shard_name,
                        &ready_common,
                    );

                    let src_fd = match open_relative(self.root_fd(), &src_dir) {
                        Ok(fd) => fd,
                        Err(_) => continue,
                    };
                    let dest_fd = match open_relative(self.root_fd(), &dest_dir) {
                        Ok(fd) => fd,
                        Err(_) => continue,
                    };

                    if fs::renameat2_noreplace(
                        src_fd.as_raw_fd(),
                        entry,
                        dest_fd.as_raw_fd(),
                        &ready_name,
                    )
                    .is_ok()
                    {
                        // B-06: Only count as success if syncs complete
                        let dest_ok = fs::fsync_dir_fd(dest_fd.as_raw_fd()).is_ok();
                        let src_ok = fs::fsync_dir_fd(src_fd.as_raw_fd()).is_ok();
                        if dest_ok && src_ok {
                            stats.delayed_promoted += 1;
                        } else {
                            stats.errors.push(RecoveryError {
                                operation: "promote_sync".into(),
                                relative_path: format!(
                                    "delayed/{bucket_name}/{shard_name}/{entry}"
                                ),
                                error: "directory sync failed after promotion".into(),
                            });
                        }
                    }
                }
            }
        }

        // R4-RES: All buckets processed, reset cursor for next full pass.
        self.recovery_cursor.promote_delayed = None;
    }

    fn cleanup_temp_files(
        &mut self,
        boottime_now: u64,
        budget: &WorkBudget,
        stats: &mut RecoveryStats,
        deadline_mono: u64,
    ) {
        let root_fd = self.root_fd();
        let tmp_fd = match fs::open_directory(root_fd, "tmp") {
            Ok(fd) => fd,
            Err(error) => {
                Self::block_phase(stats, "temp_root_open", "tmp", &error.to_string());
                return;
            }
        };

        let mut boot_dirs = match read_recovery_directory(tmp_fd.as_raw_fd(), deadline_mono) {
            Ok(e) => e,
            Err(error) => {
                Self::stop_for_directory_error(stats, "temp_boot_read", "tmp", &error);
                return;
            }
        };
        boot_dirs.sort();

        for boot_entry in &boot_dirs {
            if let Some(cursor) = &self.recovery_cursor.cleanup_temp {
                if boot_entry.as_bytes() < cursor.first.as_slice() {
                    continue;
                }
            }
            if Self::budget_exhausted(stats, budget, deadline_mono) {
                stats.budget_exhausted = true;
                return;
            }
            let Some(boot_dir_name) = boot_entry.as_str() else {
                Self::record_error(
                    stats,
                    "temp_boot_name",
                    &raw_name_for_error(boot_entry),
                    "boot directory name is not UTF-8",
                );
                continue;
            };
            if steadq_names::boot_id_bytes(boot_dir_name).is_none() {
                Self::record_error(
                    stats,
                    "temp_boot_name",
                    boot_dir_name,
                    "boot directory name is not canonical",
                );
                continue;
            }

            let is_current_boot = boot_dir_name == self.boot_id;

            let boot_dir_fd = match fs::open_directory(tmp_fd.as_raw_fd(), boot_dir_name) {
                Ok(fd) => fd,
                Err(error) => {
                    stats.scan_skips += 1;
                    Self::block_phase(stats, "temp_boot_open", boot_dir_name, &error.to_string());
                    return;
                }
            };

            let mut shard_dirs =
                match read_recovery_directory(boot_dir_fd.as_raw_fd(), deadline_mono) {
                    Ok(e) => e,
                    Err(error) => {
                        stats.scan_skips += 1;
                        Self::stop_for_directory_error(
                            stats,
                            "temp_shard_read",
                            boot_dir_name,
                            &error,
                        );
                        return;
                    }
                };
            shard_dirs.sort();

            for shard_entry in &shard_dirs {
                if let Some(cursor) = &self.recovery_cursor.cleanup_temp {
                    if boot_entry.as_bytes() == cursor.first
                        && shard_entry.as_bytes() < cursor.second.as_slice()
                    {
                        continue;
                    }
                }
                let Some(shard_name) = shard_entry.as_str() else {
                    Self::record_error(
                        stats,
                        "temp_shard_name",
                        &raw_name_for_error(shard_entry),
                        "shard directory name is not UTF-8",
                    );
                    continue;
                };
                let Some(shard) = steadq_names::shard_from_hex(shard_name) else {
                    Self::record_error(
                        stats,
                        "temp_shard_name",
                        shard_name,
                        "shard directory name is not canonical",
                    );
                    continue;
                };
                if shard >= self.format.shard_count {
                    Self::record_error(
                        stats,
                        "temp_shard_name",
                        shard_name,
                        "shard directory is outside the queue shard range",
                    );
                    continue;
                }
                let shard_fd = match fs::open_directory(boot_dir_fd.as_raw_fd(), shard_name) {
                    Ok(fd) => fd,
                    Err(error) => {
                        stats.scan_skips += 1;
                        Self::block_phase(
                            stats,
                            "temp_shard_open",
                            &format!("tmp/{boot_dir_name}/{shard_name}"),
                            &error.to_string(),
                        );
                        return;
                    }
                };

                let mut entries = match read_recovery_directory(shard_fd.as_raw_fd(), deadline_mono)
                {
                    Ok(e) => e,
                    Err(error) => {
                        stats.scan_skips += 1;
                        Self::stop_for_directory_error(
                            stats,
                            "temp_entry_read",
                            &format!("tmp/{boot_dir_name}/{shard_name}"),
                            &error,
                        );
                        return;
                    }
                };
                entries.sort();

                for raw_entry in &entries {
                    if let Some(cursor) = &self.recovery_cursor.cleanup_temp {
                        if cursor.should_skip(
                            boot_entry.as_bytes(),
                            shard_entry.as_bytes(),
                            raw_entry.as_bytes(),
                        ) {
                            continue;
                        }
                    }
                    if Self::budget_exhausted(stats, budget, deadline_mono) {
                        stats.budget_exhausted = true;
                        return;
                    }
                    self.recovery_cursor.cleanup_temp = Some(ThreeLevelCursor::new(
                        boot_entry.as_bytes(),
                        shard_entry.as_bytes(),
                        raw_entry.as_bytes(),
                    ));
                    let Some(entry) = raw_entry.as_str() else {
                        Self::record_error(
                            stats,
                            "temp_entry_name",
                            &raw_name_for_error(raw_entry),
                            "entry name is not UTF-8",
                        );
                        continue;
                    };

                    if !entry.ends_with(".tmp") {
                        continue;
                    }

                    stats.operations_attempted += 1;

                    let should_delete = if !is_current_boot {
                        true
                    } else if let Ok(parsed) = steadq_names::parse_temp(entry) {
                        boottime_now.saturating_sub(parsed.created_boottime_ns)
                            > self.options.temporary_file_ttl_ns
                    } else {
                        false
                    };

                    if should_delete && fs::unlinkat(shard_fd.as_raw_fd(), entry).is_ok() {
                        stats.temp_files_deleted += 1;
                    }
                }
            }
        }
        self.recovery_cursor.cleanup_temp = None;
    }

    // Public version of ensure_dir for recovery
    fn ensure_dir_pub(&self, relative: &str) -> io::Result<()> {
        // Reuse the existing ensure_dir logic
        self.ensure_dir(relative)
    }

    /// Compact full-job receipts into compact receipts.
    /// Replaces a full-job .rct file with a 128-byte compact receipt at the same pathname.
    pub fn compact_receipts(
        &mut self,
        budget: &WorkBudget,
        stats: &mut RecoveryStats,
        deadline_mono: u64,
    ) {
        let root_fd = self.root_fd();
        let receipts_fd = match fs::open_directory(root_fd, "receipts") {
            Ok(fd) => fd,
            Err(error) => {
                Self::block_phase(stats, "compact_root_open", "receipts", &error.to_string());
                return;
            }
        };

        let mut bucket_dirs = match read_recovery_directory(receipts_fd.as_raw_fd(), deadline_mono)
        {
            Ok(e) => e,
            Err(error) => {
                Self::stop_for_directory_error(stats, "compact_bucket_read", "receipts", &error);
                return;
            }
        };
        bucket_dirs.sort();

        for bucket_entry in &bucket_dirs {
            // R4-RES: Skip buckets already processed in a prior pass.
            if let Some(cursor) = &self.recovery_cursor.compact_receipts {
                if bucket_entry.as_bytes() < cursor.first.as_slice() {
                    continue;
                }
            }

            if Self::budget_exhausted(stats, budget, deadline_mono) {
                stats.budget_exhausted = true;
                return;
            }
            let Some(bucket_name) = bucket_entry.as_str() else {
                Self::record_error(
                    stats,
                    "compact_bucket_name",
                    &raw_name_for_error(bucket_entry),
                    "bucket directory name is not UTF-8",
                );
                continue;
            };
            if steadq_names::bucket_from_hex(bucket_name).is_none() {
                Self::record_error(
                    stats,
                    "compact_bucket_name",
                    bucket_name,
                    "bucket directory name is not canonical",
                );
                continue;
            }

            let bucket_fd = match fs::open_directory(receipts_fd.as_raw_fd(), bucket_name) {
                Ok(fd) => fd,
                Err(error) => {
                    stats.scan_skips += 1;
                    Self::block_phase(
                        stats,
                        "compact_bucket_open",
                        &format!("receipts/{bucket_name}"),
                        &error.to_string(),
                    );
                    return;
                }
            };

            let mut shard_dirs = match read_recovery_directory(bucket_fd.as_raw_fd(), deadline_mono)
            {
                Ok(e) => e,
                Err(error) => {
                    stats.scan_skips += 1;
                    Self::stop_for_directory_error(
                        stats,
                        "compact_shard_read",
                        &format!("receipts/{bucket_name}"),
                        &error,
                    );
                    return;
                }
            };
            shard_dirs.sort();

            for shard_entry in &shard_dirs {
                // Entry level cursor: skip shards before cursor when bucket matches.
                if let Some(cursor) = &self.recovery_cursor.compact_receipts {
                    if bucket_entry.as_bytes() == cursor.first
                        && shard_entry.as_bytes() < cursor.second.as_slice()
                    {
                        continue;
                    }
                }
                let Some(shard_name) = shard_entry.as_str() else {
                    Self::record_error(
                        stats,
                        "compact_shard_name",
                        &raw_name_for_error(shard_entry),
                        "shard directory name is not UTF-8",
                    );
                    continue;
                };
                let Some(shard) = steadq_names::shard_from_hex(shard_name) else {
                    Self::record_error(
                        stats,
                        "compact_shard_name",
                        shard_name,
                        "shard directory name is not canonical",
                    );
                    continue;
                };
                if shard >= self.format.shard_count {
                    Self::record_error(
                        stats,
                        "compact_shard_name",
                        shard_name,
                        "shard directory is outside the queue shard range",
                    );
                    continue;
                }
                let shard_fd = match fs::open_directory(bucket_fd.as_raw_fd(), shard_name) {
                    Ok(fd) => fd,
                    Err(error) => {
                        stats.scan_skips += 1;
                        Self::block_phase(
                            stats,
                            "compact_shard_open",
                            &format!("receipts/{bucket_name}/{shard_name}"),
                            &error.to_string(),
                        );
                        return;
                    }
                };

                let mut entries = match read_recovery_directory(shard_fd.as_raw_fd(), deadline_mono)
                {
                    Ok(e) => e,
                    Err(error) => {
                        stats.scan_skips += 1;
                        Self::stop_for_directory_error(
                            stats,
                            "compact_entry_read",
                            &format!("receipts/{bucket_name}/{shard_name}"),
                            &error,
                        );
                        return;
                    }
                };
                entries.sort();

                for raw_entry in &entries {
                    // Entry level cursor: skip entries at or before cursor when bucket and shard match.
                    if let Some(cursor) = &self.recovery_cursor.compact_receipts {
                        if cursor.should_skip(
                            bucket_entry.as_bytes(),
                            shard_entry.as_bytes(),
                            raw_entry.as_bytes(),
                        ) {
                            continue;
                        }
                    }
                    if Self::budget_exhausted(stats, budget, deadline_mono) {
                        stats.budget_exhausted = true;
                        return;
                    }
                    self.recovery_cursor.compact_receipts = Some(ThreeLevelCursor::new(
                        bucket_entry.as_bytes(),
                        shard_entry.as_bytes(),
                        raw_entry.as_bytes(),
                    ));
                    let Some(entry) = raw_entry.as_str() else {
                        Self::record_error(
                            stats,
                            "compact_entry_name",
                            &raw_name_for_error(raw_entry),
                            "entry name is not UTF-8",
                        );
                        continue;
                    };

                    if !entry.ends_with(".rct") {
                        continue;
                    }

                    stats.operations_attempted += 1;

                    // C-35: Open with write-capable mode for OFD write lock
                    let receipt_fd = match fs::openat(
                        shard_fd.as_raw_fd(),
                        entry,
                        crate::queue::verified::receipt_write_open_flags(),
                        0,
                    ) {
                        Ok(fd) => fd,
                        Err(_) => continue,
                    };

                    if !fs::try_ofd_write_lock(receipt_fd.as_raw_fd()).unwrap_or(false) {
                        continue; // busy, skip
                    }

                    let verified_receipt = match crate::queue::verified::verify_receipt_on_fd(
                        receipt_fd.as_raw_fd(),
                        crate::queue::verified::ReceiptContext {
                            queue_id: &self.format.queue_id,
                            shard_count: self.format.shard_count,
                            terminal_bucket_width_ns: self.format.terminal_bucket_width_ns,
                            max_payload_length: self.format.max_payload_length,
                            bucket: bucket_name,
                            shard: shard_name,
                            filename: entry,
                        },
                        None,
                    ) {
                        Ok(receipt) => receipt,
                        Err(error) => {
                            Self::record_error(
                                stats,
                                "receipt_compact_invalid",
                                &format!("receipts/{bucket_name}/{shard_name}/{entry}"),
                                &error.to_string(),
                            );
                            continue;
                        }
                    };

                    let crate::queue::verified::VerifiedReceipt {
                        name: parsed,
                        bucket_number,
                        kind,
                        device,
                        inode,
                    } = verified_receipt;
                    let header = match kind {
                        crate::queue::verified::VerifiedReceiptKind::Full(job) => job.header,
                        crate::queue::verified::VerifiedReceiptKind::Compact => continue,
                    };
                    let bucket_start =
                        match bucket_number.checked_mul(self.format.terminal_bucket_width_ns) {
                            Some(bucket_start) => bucket_start,
                            None => continue,
                        };

                    // Build compact receipt
                    let compact = steadq_format::CompactReceipt {
                        job_id: header.job_id,
                        envelope_digest: header.envelope_digest,
                        final_attempt: parsed.common.attempt,
                        lease_token: parsed.token,
                        receipt_bucket_start_unix_ns: bucket_start,
                        original_payload_length: header.payload_length,
                    };

                    let compact_bytes = compact.encode();

                    // Write to a temp file in the same directory
                    let tmp_name = format!(
                        ".compact-{}.tmp",
                        match steadq_fs_linux::random_128bit() {
                            Ok(r) => r,
                            Err(_) => continue,
                        }
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>()
                    );

                    let tmp_fd = match fs::create_exclusive(shard_fd.as_raw_fd(), &tmp_name, 0o600)
                    {
                        Ok(fd) => fd,
                        Err(_) => continue,
                    };

                    if fs::write_all(tmp_fd.as_raw_fd(), &compact_bytes).is_err() {
                        let _ = fs::unlinkat(shard_fd.as_raw_fd(), &tmp_name);
                        continue;
                    }
                    if fs::fsync(tmp_fd.as_raw_fd()).is_err() {
                        let _ = fs::unlinkat(shard_fd.as_raw_fd(), &tmp_name);
                        continue;
                    }

                    // The lock protects the opened inode, so prove the pathname
                    // still names that inode before replacing it.
                    let current = match fs::fstatat(shard_fd.as_raw_fd(), entry) {
                        Ok(stat) => stat,
                        Err(_) => {
                            let _ = fs::unlinkat(shard_fd.as_raw_fd(), &tmp_name);
                            continue;
                        }
                    };
                    if !crate::queue::verified::receipt_path_identity_matches(
                        &current, device, inode,
                    ) {
                        let _ = fs::unlinkat(shard_fd.as_raw_fd(), &tmp_name);
                        continue;
                    }

                    // Replace the original with the compact version
                    if fs::durable_move_replace(
                        shard_fd.as_raw_fd(),
                        &tmp_name,
                        shard_fd.as_raw_fd(),
                        entry,
                    )
                    .is_ok()
                    {
                        stats.receipts_compacted += 1;
                    } else {
                        let _ = fs::unlinkat(shard_fd.as_raw_fd(), &tmp_name);
                    }
                }
            }
        }

        // R4-RES: All buckets processed, reset cursor for next full pass.
        self.recovery_cursor.compact_receipts = None;
    }

    /// Delete expired receipts based on retention policy.
    fn delete_expired_receipts(
        &mut self,
        wall_floor: WallFloor,
        retention_ns: u64,
        budget: &WorkBudget,
        stats: &mut RecoveryStats,
        deadline_mono: u64,
    ) {
        let root_fd = self.root_fd();
        let wall_floor = wall_floor.unix_ns();

        let receipts_fd = match fs::open_directory(root_fd, "receipts") {
            Ok(fd) => fd,
            Err(error) => {
                Self::block_phase(stats, "delete_root_open", "receipts", &error.to_string());
                return;
            }
        };

        let mut bucket_dirs = match read_recovery_directory(receipts_fd.as_raw_fd(), deadline_mono)
        {
            Ok(e) => e,
            Err(error) => {
                Self::stop_for_directory_error(stats, "delete_bucket_read", "receipts", &error);
                return;
            }
        };
        bucket_dirs.sort();

        for bucket_entry in &bucket_dirs {
            // R4-RES: Skip buckets already processed in a prior pass.
            if let Some(cursor) = &self.recovery_cursor.delete_receipts {
                if bucket_entry.as_bytes() < cursor.first.as_slice() {
                    continue;
                }
            }

            if Self::budget_exhausted(stats, budget, deadline_mono) {
                stats.budget_exhausted = true;
                return;
            }
            let Some(bucket_name) = bucket_entry.as_str() else {
                Self::record_error(
                    stats,
                    "delete_bucket_name",
                    &raw_name_for_error(bucket_entry),
                    "bucket directory name is not UTF-8",
                );
                continue;
            };

            let bucket_num = match steadq_names::bucket_from_hex(bucket_name) {
                Some(bucket) => bucket,
                None => {
                    Self::record_error(
                        stats,
                        "delete_bucket_name",
                        bucket_name,
                        "bucket directory name is not canonical",
                    );
                    continue;
                }
            };

            let bucket_start = match bucket_num.checked_mul(self.format.terminal_bucket_width_ns) {
                Some(s) => s,
                None => continue,
            };
            let bucket_end = match bucket_start.checked_add(self.format.terminal_bucket_width_ns) {
                Some(e) => e,
                None => continue,
            };

            // Check retention: bucket_end + retention <= wall_floor
            let eligible = match bucket_end.checked_add(retention_ns) {
                Some(threshold) => threshold <= wall_floor,
                None => false,
            };

            if !eligible {
                continue;
            }

            let bucket_fd = match fs::open_directory(receipts_fd.as_raw_fd(), bucket_name) {
                Ok(fd) => fd,
                Err(error) => {
                    stats.scan_skips += 1;
                    Self::block_phase(
                        stats,
                        "delete_bucket_open",
                        &format!("receipts/{bucket_name}"),
                        &error.to_string(),
                    );
                    return;
                }
            };

            let mut shard_dirs = match read_recovery_directory(bucket_fd.as_raw_fd(), deadline_mono)
            {
                Ok(e) => e,
                Err(error) => {
                    stats.scan_skips += 1;
                    Self::stop_for_directory_error(
                        stats,
                        "delete_shard_read",
                        &format!("receipts/{bucket_name}"),
                        &error,
                    );
                    return;
                }
            };
            shard_dirs.sort();

            for shard_entry in &shard_dirs {
                // Entry level cursor: skip shards before cursor when bucket matches.
                if let Some(cursor) = &self.recovery_cursor.delete_receipts {
                    if bucket_entry.as_bytes() == cursor.first
                        && shard_entry.as_bytes() < cursor.second.as_slice()
                    {
                        continue;
                    }
                }
                let Some(shard_name) = shard_entry.as_str() else {
                    Self::record_error(
                        stats,
                        "delete_shard_name",
                        &raw_name_for_error(shard_entry),
                        "shard directory name is not UTF-8",
                    );
                    continue;
                };
                let Some(shard) = steadq_names::shard_from_hex(shard_name) else {
                    Self::record_error(
                        stats,
                        "delete_shard_name",
                        shard_name,
                        "shard directory name is not canonical",
                    );
                    continue;
                };
                if shard >= self.format.shard_count {
                    Self::record_error(
                        stats,
                        "delete_shard_name",
                        shard_name,
                        "shard directory is outside the queue shard range",
                    );
                    continue;
                }
                let shard_fd = match fs::open_directory(bucket_fd.as_raw_fd(), shard_name) {
                    Ok(fd) => fd,
                    Err(error) => {
                        stats.scan_skips += 1;
                        Self::block_phase(
                            stats,
                            "delete_shard_open",
                            &format!("receipts/{bucket_name}/{shard_name}"),
                            &error.to_string(),
                        );
                        return;
                    }
                };

                let mut entries = match read_recovery_directory(shard_fd.as_raw_fd(), deadline_mono)
                {
                    Ok(e) => e,
                    Err(error) => {
                        stats.scan_skips += 1;
                        Self::stop_for_directory_error(
                            stats,
                            "delete_entry_read",
                            &format!("receipts/{bucket_name}/{shard_name}"),
                            &error,
                        );
                        return;
                    }
                };
                entries.sort();

                for raw_entry in &entries {
                    // Entry level cursor: skip entries at or before cursor when bucket and shard match.
                    if let Some(cursor) = &self.recovery_cursor.delete_receipts {
                        if cursor.should_skip(
                            bucket_entry.as_bytes(),
                            shard_entry.as_bytes(),
                            raw_entry.as_bytes(),
                        ) {
                            continue;
                        }
                    }
                    if Self::budget_exhausted(stats, budget, deadline_mono) {
                        stats.budget_exhausted = true;
                        return;
                    }
                    self.recovery_cursor.delete_receipts = Some(ThreeLevelCursor::new(
                        bucket_entry.as_bytes(),
                        shard_entry.as_bytes(),
                        raw_entry.as_bytes(),
                    ));
                    let Some(entry) = raw_entry.as_str() else {
                        Self::record_error(
                            stats,
                            "delete_entry_name",
                            &raw_name_for_error(raw_entry),
                            "entry name is not UTF-8",
                        );
                        continue;
                    };
                    // R4-H08: Only process receipt files.
                    if !entry.ends_with(".rct") {
                        continue;
                    }
                    stats.operations_attempted += 1;

                    // R4-H08: Validate the receipt filename before operating.
                    if steadq_names::parse_receipt(entry).is_err() {
                        Self::record_error(
                            stats,
                            "receipt_delete_parse",
                            &format!("receipts/{bucket_name}/{shard_name}/{entry}"),
                            "receipt filename does not parse",
                        );
                        continue;
                    }

                    // C-35: Open with write-capable mode for lock
                    let receipt_fd = match fs::openat(
                        shard_fd.as_raw_fd(),
                        entry,
                        crate::queue::verified::receipt_write_open_flags(),
                        0,
                    ) {
                        Ok(fd) => fd,
                        Err(_) => continue,
                    };

                    if !fs::try_ofd_write_lock(receipt_fd.as_raw_fd()).unwrap_or(false) {
                        continue;
                    }

                    let verified_receipt = match crate::queue::verified::verify_receipt_on_fd(
                        receipt_fd.as_raw_fd(),
                        crate::queue::verified::ReceiptContext {
                            queue_id: &self.format.queue_id,
                            shard_count: self.format.shard_count,
                            terminal_bucket_width_ns: self.format.terminal_bucket_width_ns,
                            max_payload_length: self.format.max_payload_length,
                            bucket: bucket_name,
                            shard: shard_name,
                            filename: entry,
                        },
                        None,
                    ) {
                        Ok(receipt) => receipt,
                        Err(error) => {
                            Self::record_error(
                                stats,
                                "receipt_delete_invalid",
                                &format!("receipts/{bucket_name}/{shard_name}/{entry}"),
                                &error.to_string(),
                            );
                            continue;
                        }
                    };
                    let current = match fs::fstatat(shard_fd.as_raw_fd(), entry) {
                        Ok(stat) => stat,
                        Err(_) => continue,
                    };
                    if !crate::queue::verified::receipt_path_identity_matches(
                        &current,
                        verified_receipt.device,
                        verified_receipt.inode,
                    ) {
                        Self::record_error(
                            stats,
                            "receipt_delete_replaced",
                            &format!("receipts/{bucket_name}/{shard_name}/{entry}"),
                            "receipt pathname changed after verification",
                        );
                        continue;
                    }

                    if fs::unlinkat(shard_fd.as_raw_fd(), entry).is_ok() {
                        // P1-11: Only count as durable success after fsync.
                        if fs::fsync_dir_fd(shard_fd.as_raw_fd()).is_ok() {
                            stats.receipts_expired += 1;
                        } else {
                            Self::record_error(
                                stats,
                                "receipt_expire_indeterminate",
                                &format!("receipts/{bucket_name}/{shard_name}/{entry}"),
                                "unlink succeeded but shard dir fsync failed",
                            );
                        }
                    }
                }

                // Remove empty shard dir
                let _ = fs::unlinkat_dir(bucket_fd.as_raw_fd(), shard_name);
            }

            // Remove empty bucket dir
            // H15: Only count if removal and sync both succeed
            if fs::unlinkat_dir(receipts_fd.as_raw_fd(), bucket_name).is_ok() {
                if fs::fsync_dir_fd(receipts_fd.as_raw_fd()).is_ok() {
                    stats.buckets_removed += 1;
                } else {
                    Self::record_error(
                        stats,
                        "bucket_removal_sync",
                        &format!("receipts/{bucket_name}"),
                        "receipts dir sync failed after bucket removal",
                    );
                }
            }
        }

        // R4-RES: All buckets processed, reset cursor for next full pass.
        self.recovery_cursor.delete_receipts = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AckOutcome, CreateOptions, EnqueueInput, LeaseOutcome, OpenOptions};
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    fn create_test_queue() -> (TempDir, Queue) {
        let tmp = TempDir::new().unwrap();
        Queue::init(tmp.path(), &CreateOptions::default()).unwrap();
        let queue = Queue::open(
            tmp.path(),
            &OpenOptions {
                allow_unsupported_fs: true,
                ..Default::default()
            },
        )
        .unwrap();
        (tmp, queue)
    }

    fn find_file(root: &Path, extension: &str) -> Option<PathBuf> {
        for entry in std::fs::read_dir(root).ok()? {
            let path = entry.ok()?.path();
            if path.is_dir() {
                if let Some(found) = find_file(&path, extension) {
                    return Some(found);
                }
            } else if path.extension().and_then(|value| value.to_str()) == Some(extension) {
                return Some(path);
            }
        }
        None
    }

    fn find_files(root: &Path, extension: &str, found: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(root).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                find_files(&path, extension, found);
            } else if path.extension().and_then(|value| value.to_str()) == Some(extension) {
                found.push(path);
            }
        }
    }

    fn write_wall_watermark(tmp: &TempDir, highest_observed_bucket: u64) {
        let path = tmp.path().join("control/wall-watermark");
        let bytes = std::fs::read(&path).unwrap();
        let current = steadq_format::WatermarkRecord::decode(&bytes).unwrap();
        let updated = steadq_format::WatermarkRecord {
            highest_observed_bucket,
            sequence: current.sequence.checked_add(1).unwrap(),
        };
        std::fs::write(path, updated.encode()).unwrap();
    }

    fn enqueue_and_ack(queue: &mut Queue) -> crate::LeaseInfo {
        assert!(matches!(
            queue.enqueue(EnqueueInput {
                maximum_attempts: 3,
                content_type: "x".into(),
                payload: b"receipt".to_vec(),
                ..Default::default()
            }),
            EnqueueOutcome::Committed(_)
        ));
        let lease = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(lease) => lease,
            outcome => panic!("lease failed: {outcome:?}"),
        };
        assert!(matches!(queue.ack(&lease), AckOutcome::Acked));
        lease
    }

    fn valid_cursor_record(queue: &Queue) -> RecoveryCursorRecord {
        RecoveryCursorRecord {
            schema: RECOVERY_CURSOR_SCHEMA.into(),
            version: RECOVERY_CURSOR_VERSION,
            queue_id: steadq_names::hex_encode(&queue.format.queue_id),
            cursor: RecoveryCursor::default(),
        }
    }

    #[test]
    fn recovery_phase_budget_table() {
        let mut stats = RecoveryStats::default();
        assert!(Queue::has_recovery_budget(&stats));
        stats.budget_exhausted = true;
        assert!(!Queue::has_recovery_budget(&stats));
        stats.budget_exhausted = false;
        stats.phase_blocked = true;
        assert!(Queue::has_recovery_budget(&stats));
    }

    #[test]
    fn recovery_scan_bounds_are_fixed_and_finite() {
        assert_eq!(MAX_RECOVERY_DIRECTORY_ENTRIES, 65_536);
        assert_eq!(MAX_RECOVERY_DIRECTORY_NAME_BYTES, 16_711_680);
    }

    #[test]
    fn recovery_budget_predicates_cover_operation_time_and_clock_failure() {
        fs::fault::reset();
        assert!(Queue::budget_time_exceeded(0).unwrap());
        assert!(!Queue::budget_time_exceeded(u64::MAX).unwrap());

        let budget = WorkBudget {
            max_operations: 1,
            max_duration_ms: u64::MAX,
        };
        let mut stats = RecoveryStats::default();
        assert!(!Queue::budget_exhausted(&mut stats, &budget, u64::MAX));
        stats.operations_attempted = 1;
        assert!(Queue::budget_exhausted(&mut stats, &budget, u64::MAX));
        stats.operations_attempted = 0;
        assert!(Queue::budget_exhausted(&mut stats, &budget, 0));

        fs::fault::inject("clock_monotonic_ns", 1);
        assert!(Queue::budget_exhausted(&mut stats, &budget, u64::MAX));
        fs::fault::reset();
        assert!(stats.phase_blocked);
        assert!(stats
            .errors
            .iter()
            .any(|error| error.operation == "clock_monotonic"));
    }

    #[test]
    fn recovery_error_helpers_preserve_context_and_block_state() {
        let mut stats = RecoveryStats::default();
        Queue::record_error(&mut stats, "operation", "path", "error");
        assert_eq!(stats.errors.len(), 1);
        assert_eq!(stats.errors[0].operation, "operation");
        assert_eq!(stats.errors[0].relative_path, "path");
        assert_eq!(stats.errors[0].error, "error");
        assert!(!stats.phase_blocked);

        Queue::block_phase(&mut stats, "blocked", "blocked-path", "blocked-error");
        assert!(stats.phase_blocked);
        assert_eq!(stats.errors.len(), 2);
        assert_eq!(stats.errors[1].operation, "blocked");
        assert_eq!(stats.errors[1].relative_path, "blocked-path");
        assert_eq!(stats.errors[1].error, "blocked-error");

        let mut timed_out = RecoveryStats::default();
        Queue::stop_for_directory_error(
            &mut timed_out,
            "read",
            "directory",
            &RecoveryDirectoryError::BudgetExhausted,
        );
        assert!(timed_out.budget_exhausted);
        assert!(!timed_out.phase_blocked);
        assert!(timed_out.errors.is_empty());

        let mut io_failed = RecoveryStats::default();
        Queue::stop_for_directory_error(
            &mut io_failed,
            "read",
            "directory",
            &RecoveryDirectoryError::Io(io::Error::from_raw_os_error(libc::ETIMEDOUT)),
        );
        assert!(!io_failed.budget_exhausted);
        assert!(io_failed.phase_blocked);
        assert_eq!(io_failed.errors.len(), 1);
        assert_eq!(io_failed.errors[0].operation, "read");

        let mut clock_failed = RecoveryStats::default();
        Queue::stop_for_directory_error(
            &mut clock_failed,
            "read",
            "directory",
            &RecoveryDirectoryError::Clock(io::Error::from_raw_os_error(libc::EIO)),
        );
        assert!(!clock_failed.budget_exhausted);
        assert!(clock_failed.phase_blocked);
        assert_eq!(clock_failed.errors.len(), 1);
        assert_eq!(clock_failed.errors[0].operation, "clock_monotonic");
    }

    #[test]
    fn recovery_phase_progress_prevents_early_phase_starvation_after_reopen() {
        let (tmp, mut queue) = create_test_queue();
        let receipt_dir = tmp.path().join("receipts/0000000000000000/0000");
        std::fs::create_dir_all(&receipt_dir).unwrap();
        std::fs::write(receipt_dir.join("invalid.rct"), b"invalid").unwrap();

        let budget = WorkBudget {
            max_operations: 1,
            max_duration_ms: 5_000,
        };
        let first = queue.recover(&budget);
        assert_eq!(first.operations_attempted, 1, "errors: {:?}", first.errors);
        assert_eq!(first.leases_reaped, 0, "errors: {:?}", first.errors);
        assert!(first.budget_exhausted);
        assert_eq!(queue.recovery_cursor.phase, RecoveryPhase::DeleteReceipts);
        drop(queue);

        let mut reopened = Queue::open(
            tmp.path(),
            &OpenOptions {
                allow_unsupported_fs: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            reopened.recovery_cursor.phase,
            RecoveryPhase::DeleteReceipts
        );

        let second = reopened.recover(&budget);
        assert_eq!(
            second.operations_attempted, 1,
            "errors: {:?}",
            second.errors
        );
        assert!(second
            .errors
            .iter()
            .any(|error| error.operation == "receipt_delete_parse"));
        assert_eq!(reopened.recovery_cursor.phase, RecoveryPhase::ReapLeases);
    }

    #[test]
    fn recovery_reloads_cursor_after_lock_acquisition() {
        let (tmp, mut first) = create_test_queue();
        let mut stale = Queue::open(
            tmp.path(),
            &OpenOptions {
                allow_unsupported_fs: true,
                ..Default::default()
            },
        )
        .unwrap();
        let receipt_dir = tmp.path().join("receipts/0000000000000000/0000");
        std::fs::create_dir_all(&receipt_dir).unwrap();
        std::fs::write(receipt_dir.join("a.rct"), b"invalid a").unwrap();
        std::fs::write(receipt_dir.join("b.rct"), b"invalid b").unwrap();
        let budget = WorkBudget {
            max_operations: 1,
            max_duration_ms: 5_000,
        };

        let first_stats = first.recover(&budget);
        assert!(first_stats.budget_exhausted);
        assert!(first_stats
            .errors
            .iter()
            .any(|error| error.relative_path.ends_with("/a.rct")));

        let stale_stats = stale.recover(&budget);
        assert!(stale_stats
            .errors
            .iter()
            .any(|error| error.relative_path.ends_with("/b.rct")));
        assert!(!stale_stats
            .errors
            .iter()
            .any(|error| error.relative_path.ends_with("/a.rct")));
    }

    #[test]
    fn hierarchy_open_failure_cannot_advance_past_unclassified_work() {
        use std::os::unix::fs::symlink;

        let (tmp, mut queue) = create_test_queue();
        assert!(matches!(
            queue.enqueue(EnqueueInput {
                maximum_attempts: 3,
                content_type: "x".into(),
                payload: b"later work".to_vec(),
                ..Default::default()
            }),
            EnqueueOutcome::Committed(_)
        ));
        assert!(matches!(
            queue.lease(0, 1_000_000_000),
            LeaseOutcome::Leased(_)
        ));
        let receipt_dir = tmp.path().join("receipts/0000000000000000/0000");
        std::fs::create_dir_all(&receipt_dir).unwrap();
        std::fs::write(receipt_dir.join("invalid.rct"), b"invalid").unwrap();
        let blocked = tmp
            .path()
            .join("leased/00000000-0000-0000-0000-000000000000");
        symlink(tmp.path(), &blocked).unwrap();

        let first = queue.recover(&WorkBudget::default());
        assert!(first.phase_blocked);
        assert!(first
            .errors
            .iter()
            .any(|error| error.operation == "receipt_compact_invalid"));
        assert_eq!(queue.recovery_cursor.phase, RecoveryPhase::ReapLeases);
        assert!(queue.recovery_cursor.reap_leases.is_none());

        std::fs::remove_file(blocked).unwrap();
        let second = queue.recover(&WorkBudget::default());
        assert!(!second.phase_blocked);
        assert!(second
            .errors
            .iter()
            .any(|error| error.operation == "receipt_compact_invalid"));
    }

    #[test]
    fn scan_cursor_skips_only_canonical_processed_prefix() {
        let cursor = ThreeLevelCursor::new(b"0002", b"0003", b"middle.rct");
        for (bucket, shard, entry, expected) in [
            ("0001", "ffff", "later.rct", true),
            ("0002", "0002", "later.rct", true),
            ("0002", "0003", "earlier.rct", true),
            ("0002", "0003", "middle.rct", true),
            ("0002", "0003", "z-later.rct", false),
            ("0002", "0004", "earlier.rct", false),
            ("0003", "0000", "earlier.rct", false),
        ] {
            assert_eq!(
                cursor.should_skip(bucket.as_bytes(), shard.as_bytes(), entry.as_bytes()),
                expected
            );
        }
    }

    #[test]
    fn scan_cursor_preserves_non_utf8_order_exactly() {
        let cursor = ThreeLevelCursor::new(b"0002", b"0003", b"bad-\x80.rct");
        assert!(cursor.should_skip(b"0002", b"0003", b"bad-\x80.rct"));
        assert!(!cursor.should_skip(b"0002", b"0003", b"bad-\x81.rct"));
        let encoded = serde_json::to_vec(&cursor).unwrap();
        let decoded: ThreeLevelCursor = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, cursor);
    }

    #[test]
    fn four_level_cursor_skips_only_canonical_processed_prefix() {
        let cursor = FourLevelCursor::new(b"boot-b", b"0002", b"0003", b"middle.sqj");
        for (first, second, third, entry, expected) in [
            ("boot-a", "ffff", "ffff", "later.sqj", true),
            ("boot-b", "0001", "ffff", "later.sqj", true),
            ("boot-b", "0002", "0002", "later.sqj", true),
            ("boot-b", "0002", "0003", "middle.sqj", true),
            ("boot-b", "0002", "0003", "z-later.sqj", false),
            ("boot-b", "0002", "0004", "earlier.sqj", false),
            ("boot-c", "0000", "0000", "earlier.sqj", false),
        ] {
            assert_eq!(
                cursor.should_skip(
                    first.as_bytes(),
                    second.as_bytes(),
                    third.as_bytes(),
                    entry.as_bytes(),
                ),
                expected
            );
        }
    }

    #[test]
    fn recovery_cursor_component_validation_table() {
        for (component, expected) in [
            ("0000", true),
            ("job.rct", true),
            ("", false),
            (".", false),
            ("..", false),
            ("a/b", false),
            ("a\0b", false),
        ] {
            assert_eq!(cursor_component_is_valid(component.as_bytes()), expected);
        }
        assert!(!cursor_component_is_valid("x".repeat(256).as_bytes()));
    }

    #[test]
    fn recovery_cursor_validation_checks_every_component() {
        let valid_three = ThreeLevelCursor::new(b"first", b"second", b"entry");
        let valid_four = FourLevelCursor::new(b"first", b"second", b"third", b"entry");
        let valid = RecoveryCursor {
            phase: RecoveryPhase::CompactReceipts,
            reap_leases: Some(valid_four),
            promote_delayed: Some(valid_three.clone()),
            cleanup_temp: Some(valid_three.clone()),
            compact_receipts: Some(valid_three.clone()),
            delete_receipts: Some(valid_three),
        };
        assert!(cursor_is_valid(&valid));

        let mut invalid = Vec::new();
        for field in 0..4 {
            let mut cursor = valid.clone();
            let scan = cursor.reap_leases.as_mut().unwrap();
            match field {
                0 => scan.first.clear(),
                1 => scan.second.clear(),
                2 => scan.third.clear(),
                3 => scan.resume_after.clear(),
                _ => unreachable!(),
            }
            invalid.push(cursor);
        }
        for phase in 0..4 {
            for field in 0..3 {
                let mut cursor = valid.clone();
                let scan = match phase {
                    0 => cursor.promote_delayed.as_mut().unwrap(),
                    1 => cursor.cleanup_temp.as_mut().unwrap(),
                    2 => cursor.compact_receipts.as_mut().unwrap(),
                    3 => cursor.delete_receipts.as_mut().unwrap(),
                    _ => unreachable!(),
                };
                match field {
                    0 => scan.first.clear(),
                    1 => scan.second.clear(),
                    2 => scan.resume_after.clear(),
                    _ => unreachable!(),
                }
                invalid.push(cursor);
            }
        }
        assert_eq!(invalid.len(), 16);
        for (index, cursor) in invalid.iter().enumerate() {
            assert!(!cursor_is_valid(cursor), "invalid cursor {index} accepted");
        }
    }

    #[test]
    fn recovery_cursor_record_boundary_table() {
        assert_eq!(RECOVERY_CURSOR_MAX_BYTES, 16_384);
        assert_eq!(
            RECOVERY_CURSOR_OPEN_FLAGS,
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW
        );
        assert_eq!(
            RECOVERY_LOCK_OPEN_FLAGS,
            libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW
        );
        for (size, expected) in [
            (0, false),
            (1, true),
            (RECOVERY_CURSOR_MAX_BYTES, true),
            (RECOVERY_CURSOR_MAX_BYTES + 1, false),
        ] {
            assert_eq!(cursor_record_size_is_valid(size), expected);
            assert_eq!(cursor_record_bytes_fit(size as usize), expected);
        }
        assert!(cursor_file_metadata_is_valid(libc::S_IFREG, 1));
        assert!(!cursor_file_metadata_is_valid(libc::S_IFDIR, 1));
        assert!(!cursor_file_metadata_is_valid(libc::S_IFREG, 2));
        assert!(!cursor_file_metadata_is_valid(libc::S_IFDIR, 2));

        let valid = RecoveryCursorRecord {
            schema: RECOVERY_CURSOR_SCHEMA.into(),
            version: RECOVERY_CURSOR_VERSION,
            queue_id: steadq_names::hex_encode(&[0; 16]),
            cursor: RecoveryCursor::default(),
        };
        assert!(cursor_record_version_is_supported(&valid));
        let mut wrong_schema = RecoveryCursorRecord {
            schema: "wrong".into(),
            ..valid
        };
        assert!(!cursor_record_version_is_supported(&wrong_schema));
        wrong_schema.schema = RECOVERY_CURSOR_SCHEMA.into();
        wrong_schema.version = RECOVERY_CURSOR_VERSION + 1;
        assert!(!cursor_record_version_is_supported(&wrong_schema));

        assert!(cursor_file_is_absent(&io::Error::from_raw_os_error(
            libc::ENOENT
        )));
        assert!(!cursor_file_is_absent(&io::Error::from_raw_os_error(
            libc::EIO
        )));
        assert!(recovery_lock_exists(&io::Error::from_raw_os_error(
            libc::EEXIST
        )));
        assert!(!recovery_lock_exists(&io::Error::from_raw_os_error(
            libc::EIO
        )));
    }

    #[test]
    fn recovery_raw_name_diagnostic_preserves_non_utf8_bytes() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;

        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join(OsStr::from_bytes(b"bad-\x80")), b"x").unwrap();
        let dir = std::fs::File::open(tmp.path()).unwrap();
        let entries = read_recovery_directory(dir.as_raw_fd(), u64::MAX).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(raw_name_for_error(&entries[0]), "b\"bad-\\x80\"");
    }

    #[test]
    fn recovery_directory_read_observes_expired_deadline_before_enumeration() {
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join("entry"), b"x").unwrap();
        let dir = std::fs::File::open(tmp.path()).unwrap();

        let error = read_recovery_directory(dir.as_raw_fd(), 0).unwrap_err();

        assert!(matches!(error, RecoveryDirectoryError::BudgetExhausted));
    }

    #[test]
    fn recovery_directory_read_propagates_clock_failure() {
        let tmp = TempDir::new().unwrap();
        let dir = std::fs::File::open(tmp.path()).unwrap();
        fs::fault::reset();
        fs::fault::inject_errno("clock_monotonic_ns", 1, libc::EIO);

        let error = read_recovery_directory(dir.as_raw_fd(), u64::MAX).unwrap_err();

        fs::fault::reset();
        assert!(matches!(
            error,
            RecoveryDirectoryError::Clock(ref source)
                if source.raw_os_error() == Some(libc::EIO)
        ));
    }

    #[test]
    fn recovery_cursor_load_distinguishes_absence_from_io_failure() {
        let (_tmp, queue) = create_test_queue();
        let absent = load_recovery_cursor(queue.root_fd(), &queue.format.queue_id).unwrap();
        assert_eq!(absent, RecoveryCursor::default());

        fs::fault::reset();
        fs::fault::inject_errno("openat", 1, libc::EIO);
        let error = load_recovery_cursor(queue.root_fd(), &queue.format.queue_id).unwrap_err();
        fs::fault::reset();
        assert!(matches!(
            error,
            Error::IoFailure(ref message) if message.contains("Input/output error")
        ));
    }

    #[test]
    fn recovery_cursor_load_rejects_invalid_metadata_and_sizes() {
        let (directory_tmp, directory_queue) = create_test_queue();
        std::fs::create_dir(directory_tmp.path().join("control/recovery-cursor.json")).unwrap();
        assert!(matches!(
            load_recovery_cursor(
                directory_queue.root_fd(),
                &directory_queue.format.queue_id
            ),
            Err(Error::QueueCorrupt(ref message))
                if message == "recovery cursor is not a singly linked regular file"
        ));

        let (link_tmp, link_queue) = create_test_queue();
        let source = link_tmp.path().join("control/cursor-source");
        std::fs::write(
            &source,
            serde_json::to_vec(&valid_cursor_record(&link_queue)).unwrap(),
        )
        .unwrap();
        std::fs::hard_link(
            &source,
            link_tmp.path().join("control/recovery-cursor.json"),
        )
        .unwrap();
        assert!(matches!(
            load_recovery_cursor(link_queue.root_fd(), &link_queue.format.queue_id),
            Err(Error::QueueCorrupt(ref message))
                if message == "recovery cursor is not a singly linked regular file"
        ));

        for bytes in [Vec::new(), vec![0; RECOVERY_CURSOR_MAX_BYTES as usize + 1]] {
            let (tmp, queue) = create_test_queue();
            std::fs::write(tmp.path().join("control/recovery-cursor.json"), bytes).unwrap();
            assert!(matches!(
                load_recovery_cursor(queue.root_fd(), &queue.format.queue_id),
                Err(Error::QueueCorrupt(ref message))
                    if message == "recovery cursor size is invalid"
            ));
        }
    }

    #[test]
    fn recovery_cursor_load_rejects_schema_version_and_components() {
        let (schema_tmp, schema_queue) = create_test_queue();
        let mut wrong_schema = valid_cursor_record(&schema_queue);
        wrong_schema.schema = "wrong".into();
        std::fs::write(
            schema_tmp.path().join("control/recovery-cursor.json"),
            serde_json::to_vec(&wrong_schema).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            load_recovery_cursor(schema_queue.root_fd(), &schema_queue.format.queue_id),
            Err(Error::QueueCorrupt(ref message))
                if message == "recovery cursor schema or version is unsupported"
        ));

        let (version_tmp, version_queue) = create_test_queue();
        let mut wrong_version = valid_cursor_record(&version_queue);
        wrong_version.version += 1;
        std::fs::write(
            version_tmp.path().join("control/recovery-cursor.json"),
            serde_json::to_vec(&wrong_version).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            load_recovery_cursor(version_queue.root_fd(), &version_queue.format.queue_id),
            Err(Error::QueueCorrupt(ref message))
                if message == "recovery cursor schema or version is unsupported"
        ));

        let (component_tmp, component_queue) = create_test_queue();
        let mut invalid_component = valid_cursor_record(&component_queue);
        invalid_component.cursor.promote_delayed =
            Some(ThreeLevelCursor::new(b"", b"shard", b"entry"));
        std::fs::write(
            component_tmp.path().join("control/recovery-cursor.json"),
            serde_json::to_vec(&invalid_component).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            load_recovery_cursor(
                component_queue.root_fd(),
                &component_queue.format.queue_id
            ),
            Err(Error::QueueCorrupt(ref message))
                if message == "recovery cursor contains an invalid component"
        ));
    }

    #[test]
    fn recovery_cursor_load_refuses_symlink() {
        use std::os::unix::fs::symlink;

        let (tmp, queue) = create_test_queue();
        let target = tmp.path().join("control/cursor-target");
        std::fs::write(
            &target,
            serde_json::to_vec(&valid_cursor_record(&queue)).unwrap(),
        )
        .unwrap();
        symlink(
            "cursor-target",
            tmp.path().join("control/recovery-cursor.json"),
        )
        .unwrap();

        assert!(matches!(
            load_recovery_cursor(queue.root_fd(), &queue.format.queue_id),
            Err(Error::IoFailure(_))
        ));
    }

    #[test]
    fn recovery_cursor_persist_rejects_oversized_record() {
        let (_tmp, mut queue) = create_test_queue();
        queue.recovery_cursor.promote_delayed = Some(ThreeLevelCursor::new(
            &vec![b'x'; RECOVERY_CURSOR_MAX_BYTES as usize],
            b"shard",
            b"entry",
        ));
        assert!(matches!(
            queue.persist_recovery_cursor(),
            Err(Error::InvalidInput(ref message))
                if message == "recovery cursor exceeds maximum encoded size"
        ));
    }

    #[test]
    fn recovery_lock_creation_error_is_not_treated_as_contention() {
        let (_tmp, queue) = create_test_queue();
        fs::fault::reset();
        fs::fault::inject_errno("openat", 1, libc::EIO);
        let error = queue.acquire_recovery_lock().unwrap_err();
        fs::fault::reset();
        assert!(matches!(
            error,
            Error::IoFailure(ref message) if message.contains("Input/output error")
        ));
    }

    #[test]
    fn recovery_lock_refuses_symlink() {
        use std::os::unix::fs::symlink;

        let (tmp, queue) = create_test_queue();
        let target = tmp.path().join("lock-target");
        std::fs::write(&target, b"target").unwrap();
        let lock_path = tmp.path().join("control/recovery.lock");
        std::fs::remove_file(&lock_path).unwrap();
        symlink(&target, lock_path).unwrap();

        assert!(matches!(
            queue.acquire_recovery_lock(),
            Err(Error::IoFailure(_))
        ));
    }

    #[test]
    fn recovery_cursor_persists_exact_budget_progress_across_reopen() {
        let (tmp, mut queue) = create_test_queue();
        enqueue_and_ack(&mut queue);
        enqueue_and_ack(&mut queue);
        let receipt_root = tmp.path().join("receipts");
        let mut original_receipts = Vec::new();
        find_files(&receipt_root, "rct", &mut original_receipts);
        original_receipts
            .sort_by_key(|path| path.strip_prefix(&receipt_root).unwrap().to_path_buf());
        let first_parts = original_receipts[0]
            .strip_prefix(&receipt_root)
            .unwrap()
            .components()
            .map(|component| component.as_os_str().to_str().unwrap())
            .collect::<Vec<_>>();
        let budget = WorkBudget {
            max_operations: 1,
            max_duration_ms: 5_000,
        };

        let first = queue.recover(&budget);
        assert_eq!(first.receipts_compacted, 1, "errors: {:?}", first.errors);
        assert!(first.budget_exhausted);
        assert_eq!(
            queue.recovery_cursor.compact_receipts,
            Some(ThreeLevelCursor::new(
                first_parts[0].as_bytes(),
                first_parts[1].as_bytes(),
                first_parts[2].as_bytes()
            ))
        );
        assert!(tmp.path().join("control/recovery-cursor.json").exists());
        drop(queue);

        let mut reopened = Queue::open(
            tmp.path(),
            &OpenOptions {
                allow_unsupported_fs: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(reopened.recovery_cursor.compact_receipts.is_some());
        let second = reopened.recover(&budget);
        assert_eq!(second.receipts_compacted, 1, "errors: {:?}", second.errors);
        drop(reopened);

        let mut receipts = Vec::new();
        find_files(&tmp.path().join("receipts"), "rct", &mut receipts);
        assert_eq!(receipts.len(), 2);
        assert!(receipts
            .iter()
            .all(|path| std::fs::metadata(path).unwrap().len() == 128));
    }

    #[test]
    fn persistent_malformed_receipt_does_not_starve_valid_receipt() {
        let (tmp, mut queue) = create_test_queue();
        enqueue_and_ack(&mut queue);
        let receipt = find_file(&tmp.path().join("receipts"), "rct").unwrap();
        let malformed = receipt.parent().unwrap().join("000-malformed.rct");
        std::fs::write(&malformed, b"malformed").unwrap();
        let budget = WorkBudget {
            max_operations: 1,
            max_duration_ms: 5_000,
        };

        let first = queue.recover(&budget);
        assert_eq!(first.receipts_compacted, 0);
        assert!(first
            .errors
            .iter()
            .any(|error| error.operation == "receipt_compact_invalid"));
        assert_eq!(
            queue
                .recovery_cursor
                .compact_receipts
                .as_ref()
                .unwrap()
                .resume_after,
            b"000-malformed.rct"
        );
        drop(queue);

        let mut reopened = Queue::open(
            tmp.path(),
            &OpenOptions {
                allow_unsupported_fs: true,
                ..Default::default()
            },
        )
        .unwrap();
        let second = reopened.recover(&budget);

        assert_eq!(second.receipts_compacted, 1, "errors: {:?}", second.errors);
        assert_eq!(std::fs::metadata(receipt).unwrap().len(), 128);
        assert!(malformed.exists());
    }

    #[test]
    fn busy_receipt_does_not_pin_recovery_cursor() {
        let (tmp, mut queue) = create_test_queue();
        enqueue_and_ack(&mut queue);
        enqueue_and_ack(&mut queue);
        let receipt_root = tmp.path().join("receipts");
        let mut receipts = Vec::new();
        find_files(&receipt_root, "rct", &mut receipts);
        receipts.sort_by_key(|path| path.strip_prefix(&receipt_root).unwrap().to_path_buf());
        let held = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&receipts[0])
            .unwrap();
        let held_original_len = std::fs::metadata(&receipts[0]).unwrap().len();
        assert!(fs::try_ofd_write_lock(held.as_raw_fd()).unwrap());
        let budget = WorkBudget {
            max_operations: 1,
            max_duration_ms: 5_000,
        };

        let first = queue.recover(&budget);
        assert_eq!(first.receipts_compacted, 0);
        drop(queue);
        let mut reopened = Queue::open(
            tmp.path(),
            &OpenOptions {
                allow_unsupported_fs: true,
                ..Default::default()
            },
        )
        .unwrap();
        let second = reopened.recover(&budget);

        assert_eq!(second.receipts_compacted, 1, "errors: {:?}", second.errors);
        assert_eq!(
            std::fs::metadata(&receipts[0]).unwrap().len(),
            held_original_len
        );
        assert_eq!(std::fs::metadata(&receipts[1]).unwrap().len(), 128);
    }

    #[test]
    fn recovery_cursor_rejects_foreign_queue_identity() {
        let (tmp, mut queue) = create_test_queue();
        queue.recovery_cursor.compact_receipts =
            Some(ThreeLevelCursor::new(b"0001", b"0002", b"entry.rct"));
        queue.persist_recovery_cursor().unwrap();
        drop(queue);
        let path = tmp.path().join("control/recovery-cursor.json");
        let mut record: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        record["queue_id"] = serde_json::Value::String(steadq_names::hex_encode(&[0xff; 16]));
        std::fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();

        let error = Queue::open(
            tmp.path(),
            &OpenOptions {
                allow_unsupported_fs: true,
                ..Default::default()
            },
        )
        .err()
        .expect("foreign cursor must reject queue open");
        assert!(matches!(
            error,
            Error::QueueCorrupt(ref message)
                if message == "recovery cursor belongs to another queue"
        ));
    }

    #[test]
    fn concurrent_recovery_pass_is_rejected_by_lock() {
        let (_tmp, first) = create_test_queue();
        let mut second = Queue::open(
            _tmp.path(),
            &OpenOptions {
                allow_unsupported_fs: true,
                ..Default::default()
            },
        )
        .unwrap();
        let _held = first.acquire_recovery_lock().unwrap();

        let stats = second.recover(&WorkBudget::default());

        assert_eq!(stats.errors.len(), 1);
        assert_eq!(stats.errors[0].operation, "recovery_lock");
        assert_eq!(stats.errors[0].error, Error::MaintenanceBusy.to_string());
    }

    #[test]
    fn recovery_reaps_expired_lease() {
        let (_tmp, mut queue) = create_test_queue();

        // Enqueue and lease
        queue.enqueue(EnqueueInput {
            maximum_attempts: 3,
            content_type: "x".to_string(),
            payload: b"data".to_vec(),
            ..Default::default()
        });
        let _lease = match queue.lease(0, 1_000_000_000) {
            // 1s lease
            LeaseOutcome::Leased(l) => l,
            _ => panic!("lease failed"),
        };

        // Manually expire the lease by modifying the deadline in the filename
        // We can't easily do that, so instead we'll just verify that recovery
        // with a future boottime reaps it. For testing, we'll sleep briefly.
        std::thread::sleep(std::time::Duration::from_secs(2));

        let stats = queue.recover(&WorkBudget::default());
        // The lease should have been reaped to ready (attempt < max)
        assert!(stats.leases_reaped >= 1 || stats.leases_to_dead >= 1);

        // Should be able to lease again
        let result = queue.lease(0, 30_000_000_000);
        assert!(matches!(result, LeaseOutcome::Leased(_)));
    }

    #[test]
    fn recovery_empty_queue() {
        let (_tmp, mut queue) = create_test_queue();
        let stats = queue.recover(&WorkBudget::default());
        assert_eq!(stats.operations_attempted, 0);
        assert!(!stats.budget_exhausted);
    }

    #[test]
    fn reap_to_dead_rejects_generation_overflow() {
        let (_tmp, queue) = create_test_queue();
        let common = steadq_names::CommonFields {
            job_id: [0xAB; 16],
            generation: u64::MAX,
            attempt: 0,
            maximum_attempts: 3,
        };
        let res = queue.reap_to_dead(
            "boot",
            "0000000000000000",
            "0000",
            "dummy",
            &common,
            crate::errors::DeadReason::AttemptsExhausted,
            queue.authenticated_wall_floor().unwrap(),
        );
        assert!(res.is_err(), "generation overflow must be Err, got {res:?}");
    }

    #[test]
    fn recovery_skips_wall_sensitive_actions_without_watermark() {
        let (tmp, mut queue) = create_test_queue();
        let not_before = queue.authenticated_wall_floor().unwrap().unix_ns() + 60_000_000_000;
        let ticket = match queue.enqueue(crate::queue::EnqueueInput {
            maximum_attempts: 3,
            content_type: "x".into(),
            initial_not_before: Some(not_before),
            payload: b"delayed".to_vec(),
            ..Default::default()
        }) {
            EnqueueOutcome::Committed(ticket) => ticket,
            outcome => panic!("enqueue failed: {outcome:?}"),
        };
        std::fs::remove_file(tmp.path().join("control/wall-watermark")).unwrap();

        let stats = queue.recover(&WorkBudget::default());
        assert_eq!(stats.delayed_promoted, 0);
        assert!(stats
            .errors
            .iter()
            .any(|error| error.operation == "wall_floor"));
        assert!(tmp.path().join(ticket.expected_relative_path).exists());
    }

    #[test]
    fn recovery_promotes_eligible_delayed_job() {
        let (tmp, mut queue) = create_test_queue();
        let width = queue.format.delayed_bucket_width_ns;
        let not_before = queue
            .authenticated_wall_floor()
            .unwrap()
            .unix_ns()
            .checked_add(width)
            .unwrap();
        let ticket = match queue.enqueue(EnqueueInput {
            maximum_attempts: 3,
            content_type: "x".into(),
            initial_not_before: Some(not_before),
            payload: b"delayed".to_vec(),
            ..Default::default()
        }) {
            EnqueueOutcome::Committed(ticket) => ticket,
            outcome => panic!("enqueue failed: {outcome:?}"),
        };
        let eligible_bucket = steadq_math::ceiling_bucket(not_before, width).unwrap();
        write_wall_watermark(&tmp, eligible_bucket);

        let stats = queue.recover(&WorkBudget::default());
        assert_eq!(stats.delayed_promoted, 1, "errors: {:?}", stats.errors);
        assert!(!tmp.path().join(ticket.expected_relative_path).exists());
        assert!(find_file(&tmp.path().join("ready"), "sqj").is_some());
    }

    #[test]
    fn recovery_compacts_full_receipt() {
        let (tmp, mut queue) = create_test_queue();
        enqueue_and_ack(&mut queue);
        let receipt = find_file(&tmp.path().join("receipts"), "rct").unwrap();
        assert!(std::fs::metadata(&receipt).unwrap().len() > 128);

        let stats = queue.recover(&WorkBudget::default());
        assert_eq!(stats.receipts_compacted, 1, "errors: {:?}", stats.errors);
        assert_eq!(std::fs::metadata(receipt).unwrap().len(), 128);
    }

    #[test]
    fn corrupt_full_receipt_is_never_compacted_or_accepted_as_duplicate() {
        let (tmp, mut queue) = create_test_queue();
        let lease = enqueue_and_ack(&mut queue);
        let receipt = find_file(&tmp.path().join("receipts"), "rct").unwrap();
        let mut bytes = std::fs::read(&receipt).unwrap();
        let payload_byte = bytes.last_mut().expect("full receipt has payload bytes");
        *payload_byte ^= 0xff;
        std::fs::write(&receipt, &bytes).unwrap();

        assert!(matches!(
            queue.check_duplicate_ack(&lease),
            AckOutcome::LeaseLost
        ));
        assert!(!queue
            .inspect(&lease.job_id)
            .iter()
            .any(|snapshot| snapshot.state == "receipt"));

        let stats = queue.recover(&WorkBudget::default());
        assert_eq!(stats.receipts_compacted, 0);
        assert!(stats
            .errors
            .iter()
            .any(|error| error.operation == "receipt_compact_invalid"));
        assert!(std::fs::metadata(&receipt).unwrap().len() > 128);

        let report = queue.fsck(&crate::FsckOptions::default());
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.finding_type == "receipt_verification_failed"));

        let repair = queue.fsck(&crate::FsckOptions {
            mode: crate::FsckMode::Repair,
            depth: crate::FsckDepth::Structural,
        });
        assert_eq!(repair.quarantined.len(), 1);
        assert!(!receipt.exists());
    }

    #[test]
    fn legacy_compact_receipt_is_not_strict_evidence() {
        let (tmp, mut queue) = create_test_queue();
        let lease = enqueue_and_ack(&mut queue);
        let receipt = find_file(&tmp.path().join("receipts"), "rct").unwrap();
        let stats = queue.recover(&WorkBudget::default());
        assert_eq!(stats.receipts_compacted, 1, "errors: {:?}", stats.errors);

        let mut bytes = std::fs::read(&receipt).unwrap();
        bytes[10..12].copy_from_slice(&0u16.to_be_bytes());
        let digest = steadq_format::receipt_digest(&bytes[0..96]);
        bytes[96..128].copy_from_slice(&digest);
        std::fs::write(&receipt, bytes).unwrap();

        assert!(matches!(
            queue.check_duplicate_ack(&lease),
            AckOutcome::LeaseLost
        ));
        let report = queue.fsck(&crate::FsckOptions::default());
        assert!(report
            .findings
            .iter()
            .any(|finding| finding.finding_type == "receipt_verification_failed"));
    }

    #[test]
    fn recovery_deletes_receipt_after_authenticated_retention_floor() {
        let (tmp, mut queue) = create_test_queue();
        queue.options.receipt_retention_ns = 0;
        enqueue_and_ack(&mut queue);
        let receipt = find_file(&tmp.path().join("receipts"), "rct").unwrap();
        let receipt_bucket = receipt
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .file_name()
            .unwrap()
            .to_str()
            .and_then(|name| u64::from_str_radix(name, 16).ok())
            .unwrap();
        let expiration_floor = receipt_bucket
            .checked_add(1)
            .and_then(|bucket| bucket.checked_mul(queue.format.terminal_bucket_width_ns))
            .unwrap();
        let watermark_bucket =
            steadq_math::ceiling_bucket(expiration_floor, queue.format.delayed_bucket_width_ns)
                .unwrap();
        write_wall_watermark(&tmp, watermark_bucket);

        let stats = queue.recover(&WorkBudget::default());
        assert_eq!(stats.receipts_expired, 1, "errors: {:?}", stats.errors);
        assert!(!receipt.exists());
    }

    #[test]
    fn recovery_uses_one_wall_snapshot() {
        let (_tmp, mut queue) = create_test_queue();
        fs::fault::reset();
        fs::fault::inject("clock_realtime_ns", 2);
        let stats = queue.recover(&WorkBudget::default());
        assert!(!stats
            .errors
            .iter()
            .any(|error| error.operation == "wall_floor"));
        assert_eq!(fs::fault::call_count("clock_realtime_ns"), 1);
        fs::fault::reset();
    }

    #[test]
    fn recovery_stabilizes_wall_floor_before_wall_sensitive_phases() {
        let (tmp, mut queue) = create_test_queue();
        write_wall_watermark(&tmp, 0);

        let stats = queue.recover(&WorkBudget::default());
        assert!(!stats
            .errors
            .iter()
            .any(|error| error.operation == "wall_floor"));
        let bytes = std::fs::read(tmp.path().join("control/wall-watermark")).unwrap();
        let watermark = steadq_format::WatermarkRecord::decode(&bytes).unwrap();
        assert!(watermark.highest_observed_bucket > 0);
    }

    #[test]
    fn recovery_does_not_invent_terminal_bucket_without_wall_floor() {
        let (tmp, mut queue) = create_test_queue();
        assert!(matches!(
            queue.enqueue(crate::queue::EnqueueInput {
                maximum_attempts: 1,
                content_type: "x".into(),
                payload: b"terminal".to_vec(),
                ..Default::default()
            }),
            EnqueueOutcome::Committed(_)
        ));
        let lease = match queue.lease(0, 1_000_000_000) {
            LeaseOutcome::Leased(lease) => lease,
            outcome => panic!("lease failed: {outcome:?}"),
        };
        let mut stats = RecoveryStats::default();
        queue.reap_expired_leases(u64::MAX, None, &WorkBudget::default(), &mut stats, u64::MAX);

        assert_eq!(stats.leases_to_dead, 0);
        assert!(stats
            .errors
            .iter()
            .any(|error| error.operation == "reap_to_dead"));
        assert!(tmp.path().join(&lease.exact_source_path).exists());
        assert!(!tmp.path().join("dead/0000000000000000").exists());
    }
}
