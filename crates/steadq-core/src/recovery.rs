// SteadQ/1 cooperative recovery operations.

use std::io;
use std::os::unix::io::{AsFd, BorrowedFd, OwnedFd};

use steadq_fs_linux as fs;
use steadq_math;
use steadq_names;

use crate::errors::*;
use crate::queue::engine::{
    move_verified_noreplace, remove_empty_directory_verified, replace_verified, unlink_verified,
    MoveActor, MoveFailure, MovePhase, RemoveDirectoryFailure, ReplaceFailure, ReplaceIdentity,
    UnlinkFailure,
};
use crate::queue::{
    open_relative, FourLevelCursor, Queue, RecoveryCursor, RecoveryHierarchyRetry,
    RecoveryHierarchyRetryKind, RecoveryPhase, ThreeLevelCursor, WallFloor,
};

const RECOVERY_CURSOR_SCHEMA: &str = "steadq-recovery-cursor";
const RECOVERY_CURSOR_VERSION: u16 = 1;
const RECOVERY_CURSOR_FILE: &str = "recovery-cursor.json";
const RECOVERY_CURSOR_MAX_BYTES: u64 = 16 * 1024;
const RECOVERY_CURSOR_OPEN_FLAGS: i32 = libc::O_CLOEXEC + libc::O_NOFOLLOW;
const RECOVERY_LOCK_OPEN_FLAGS: i32 = libc::O_CLOEXEC + libc::O_NOFOLLOW + libc::O_RDWR;
const MAX_RECOVERY_DIRECTORY_ENTRIES: usize = 65_536;
const MAX_RECOVERY_DIRECTORY_NAME_BYTES: usize = MAX_RECOVERY_DIRECTORY_ENTRIES * 255;
const MAX_RECOVERY_DIRECTORY_ENTRY_CHARGE: u64 = MAX_RECOVERY_DIRECTORY_ENTRIES as u64 + 1;
const MAX_RECOVERY_DIRECTORY_NAME_BYTE_CHARGE: u64 = MAX_RECOVERY_DIRECTORY_NAME_BYTES as u64 + 255;
const MAX_RECOVERY_HIERARCHY_RETRIES: usize = 64;
const MAX_RECOVERY_RESUMED_TRAVERSAL_READS: u64 = 4;
const RECOVERY_RETRY_READS: u64 = 1;
const MIN_RECOVERY_PROGRESS_READS: u64 =
    MAX_RECOVERY_RESUMED_TRAVERSAL_READS + RECOVERY_RETRY_READS;
const MIN_RECOVERY_PROGRESS_ENTRIES: u64 =
    MAX_RECOVERY_DIRECTORY_ENTRIES as u64 * MIN_RECOVERY_PROGRESS_READS + 1;
const MIN_RECOVERY_PROGRESS_NAME_BYTES: u64 =
    MAX_RECOVERY_DIRECTORY_NAME_BYTES as u64 * MIN_RECOVERY_PROGRESS_READS + 255;
const DEFAULT_RECOVERY_DIRECTORY_READS: u64 = 1024;

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
    dir_fd: BorrowedFd<'_>,
    deadline_mono: u64,
    budget: &RecoveryScanBudget,
    stats: &mut RecoveryScanStats,
) -> Result<Vec<fs::DirEntryName>, RecoveryDirectoryError> {
    if stats.directories_read >= budget.max_directories_read {
        return Err(RecoveryDirectoryError::BudgetExhausted);
    }
    let remaining_entries = budget.max_entries_read.saturating_sub(stats.entries_read);
    let remaining_name_bytes = budget
        .max_name_bytes_read
        .saturating_sub(stats.name_bytes_read);
    if remaining_entries < MAX_RECOVERY_DIRECTORY_ENTRY_CHARGE
        || remaining_name_bytes < MAX_RECOVERY_DIRECTORY_NAME_BYTE_CHARGE
    {
        return Err(RecoveryDirectoryError::BudgetExhausted);
    }
    stats.directories_read = stats.directories_read.saturating_add(1);

    let result = fs::read_dir_entries_bounded_until_with_progress(
        dir_fd,
        MAX_RECOVERY_DIRECTORY_ENTRIES,
        MAX_RECOVERY_DIRECTORY_NAME_BYTES,
        || Queue::budget_time_exceeded(deadline_mono),
    );
    let progress = match &result {
        Ok(enumeration) => enumeration.progress,
        Err(error) => error.progress(),
    };
    let entries_read = u64::try_from(progress.entries_read).unwrap_or(u64::MAX);
    let name_bytes_read = u64::try_from(progress.name_bytes_read).unwrap_or(u64::MAX);
    stats.entries_read = stats.entries_read.saturating_add(entries_read);
    stats.name_bytes_read = stats.name_bytes_read.saturating_add(name_bytes_read);

    result
        .map(|enumeration| enumeration.entries)
        .map_err(|error| match error {
            fs::DirectoryEnumerationProgressError::Cancelled(_) => {
                RecoveryDirectoryError::BudgetExhausted
            }
            fs::DirectoryEnumerationProgressError::CancellationCheck { error, .. } => {
                RecoveryDirectoryError::Clock(error)
            }
            fs::DirectoryEnumerationProgressError::Io { error, .. } => {
                RecoveryDirectoryError::Io(error)
            }
        })
}

#[derive(Debug)]
enum RecoveryDirectoryError {
    BudgetExhausted,
    Clock(io::Error),
    Io(io::Error),
}

struct RecoveryQuarantineCandidate<'a> {
    source_directory_fd: BorrowedFd<'a>,
    filename: &'a str,
    relative_path: &'a str,
    reason: crate::QuarantineReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RememberHierarchyRetry {
    Exact,
    Overflow,
    Invalid,
}

fn raw_name_for_error(name: &fs::DirEntryName) -> String {
    format!("{name:?}")
}

fn all_observed_children_absent(absent: usize, observed: usize) -> bool {
    absent == observed
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

    let retry_depth_is_valid = |retry: &RecoveryHierarchyRetry| {
        let allowed_depth = match retry.phase {
            RecoveryPhase::ReapLeases => 1..=3,
            RecoveryPhase::PromoteDelayed
            | RecoveryPhase::CleanupTemp
            | RecoveryPhase::CompactReceipts
            | RecoveryPhase::DeleteReceipts => 1..=2,
        };
        if !allowed_depth.contains(&retry.components.len()) {
            return false;
        }
        retry
            .components
            .iter()
            .enumerate()
            .all(|(index, component)| match retry.phase {
                RecoveryPhase::ReapLeases => match index {
                    0 => steadq_names::boot_id_bytes(component).is_some(),
                    1 => steadq_names::bucket_from_hex(component).is_some(),
                    2 => steadq_names::shard_from_hex(component).is_some(),
                    _ => false,
                },
                RecoveryPhase::CleanupTemp => match index {
                    0 => steadq_names::boot_id_bytes(component).is_some(),
                    1 => steadq_names::shard_from_hex(component).is_some(),
                    _ => false,
                },
                RecoveryPhase::PromoteDelayed
                | RecoveryPhase::CompactReceipts
                | RecoveryPhase::DeleteReceipts => match index {
                    0 => steadq_names::bucket_from_hex(component).is_some(),
                    1 => steadq_names::shard_from_hex(component).is_some(),
                    _ => false,
                },
            })
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
        && cursor.hierarchy_retries.len() <= MAX_RECOVERY_HIERARCHY_RETRIES
        && cursor.hierarchy_retries.iter().all(retry_depth_is_valid)
        && cursor
            .hierarchy_retries
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        && cursor.hierarchy_retry_frontiers.len() <= 5
        && cursor
            .hierarchy_retry_frontiers
            .iter()
            .all(retry_depth_is_valid)
        && cursor
            .hierarchy_retry_frontiers
            .windows(2)
            .all(|pair| pair[0].phase < pair[1].phase)
        && cursor
            .hierarchy_retry_overflow
            .windows(2)
            .all(|pair| pair[0] < pair[1])
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

fn compaction_temporary_name(name: &str) -> bool {
    let Some(random_hex) = name
        .strip_prefix(".compact-")
        .and_then(|name| name.strip_suffix(".tmp"))
    else {
        return false;
    };
    random_hex.len() == 32
        && random_hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn recovery_lock_exists(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::AlreadyExists
}

pub(crate) fn load_recovery_cursor(
    root_fd: BorrowedFd<'_>,
    queue_id: &[u8; 16],
) -> Result<RecoveryCursor, Error> {
    let control_fd = fs::open_directory(root_fd, "control")
        .map_err(|error| Error::IoFailure(error.to_string()))?;
    let cursor_fd = match fs::openat(
        control_fd.as_fd(),
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
    let stat = fs::fstat(cursor_fd.as_fd()).map_err(|error| Error::IoFailure(error.to_string()))?;
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
    fs::pread_exact(cursor_fd.as_fd(), &mut bytes, 0)
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
    /// Maximum state-changing filesystem operations attempted after an entry
    /// has passed syntax, eligibility, locking, and identity checks.
    pub max_operations: u32,
    pub max_duration_ms: u64,
}

/// Recovery directory-enumeration budget.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryScanBudget {
    /// Maximum directory enumerations attempted during the pass.
    ///
    /// Public recovery requires one retry enumeration plus four canonical
    /// enumerations to resume its deepest hierarchy.
    pub max_directories_read: u64,
    /// Maximum protocol-visible directory entries returned by `readdir`.
    ///
    /// Enumeration starts only when the remaining budget can cover one
    /// complete bounded directory plus the sentinel entry needed to prove
    /// overflow. Public recovery requires enough capacity for the deepest
    /// resumed traversal plus one hierarchy retry.
    pub max_entries_read: u64,
    /// Maximum raw filename bytes across protocol-visible directory entries.
    ///
    /// Enumeration starts only when the remaining budget can cover one
    /// complete bounded directory plus the sentinel name needed to prove
    /// overflow. Public recovery requires enough capacity for the deepest
    /// resumed traversal plus one hierarchy retry.
    pub max_name_bytes_read: u64,
}

impl Default for WorkBudget {
    fn default() -> Self {
        Self {
            max_operations: 1000,
            max_duration_ms: 100,
        }
    }
}

impl Default for RecoveryScanBudget {
    fn default() -> Self {
        Self {
            max_directories_read: DEFAULT_RECOVERY_DIRECTORY_READS,
            ..Self::minimum_for_progress()
        }
    }
}

impl RecoveryScanBudget {
    /// Smallest scan budget that can resume the deepest hierarchy and retry
    /// one deferred directory in the same pass.
    pub fn minimum_for_progress() -> Self {
        Self {
            max_directories_read: MIN_RECOVERY_PROGRESS_READS,
            max_entries_read: MIN_RECOVERY_PROGRESS_ENTRIES,
            max_name_bytes_read: MIN_RECOVERY_PROGRESS_NAME_BYTES,
        }
    }

    /// Validate that this budget can make bounded recovery progress.
    pub fn validate(&self) -> Result<(), Error> {
        if self.max_directories_read < MIN_RECOVERY_PROGRESS_READS {
            return Err(Error::InvalidInput(format!(
                "recovery max_directories_read must be at least {MIN_RECOVERY_PROGRESS_READS}"
            )));
        }
        if self.max_entries_read < MIN_RECOVERY_PROGRESS_ENTRIES {
            return Err(Error::InvalidInput(format!(
                "recovery max_entries_read must be at least {MIN_RECOVERY_PROGRESS_ENTRIES}"
            )));
        }
        if self.max_name_bytes_read < MIN_RECOVERY_PROGRESS_NAME_BYTES {
            return Err(Error::InvalidInput(format!(
                "recovery max_name_bytes_read must be at least {MIN_RECOVERY_PROGRESS_NAME_BYTES}"
            )));
        }
        Ok(())
    }
}

/// Recovery statistics.
#[derive(Clone, Debug, Default)]
pub struct RecoveryStats {
    /// State-changing filesystem operations attempted after classification.
    pub operations_attempted: u32,
    pub temp_files_deleted: u32,
    pub delayed_promoted: u32,
    pub leases_reaped: u32,
    pub leases_to_dead: u32,
    pub buckets_removed: u32,
    pub shards_removed: u32,
    pub receipts_compacted: u32,
    pub receipts_expired: u32,
    pub quarantined: Vec<RecoveryQuarantine>,
    pub budget_exhausted: bool,
    pub phase_blocked: bool,
    pub errors: Vec<RecoveryError>,
    pub scan_skips: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryQuarantine {
    pub relative_path: String,
    pub quarantine_id: [u8; 16],
    pub quarantine_name: String,
}

/// Exact directory-enumeration work completed by a recovery pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RecoveryScanStats {
    pub directories_read: u64,
    pub entries_read: u64,
    pub name_bytes_read: u64,
}

/// Recovery results including scan accounting from the extended API.
#[derive(Clone, Debug, Default)]
pub struct RecoveryReport {
    pub stats: RecoveryStats,
    pub scan: RecoveryScanStats,
}

struct RecoveryScanContext<'a> {
    budget: &'a RecoveryScanBudget,
    stats: &'a mut RecoveryScanStats,
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
        let lock_fd = match fs::create_exclusive(control_fd.as_fd(), "recovery.lock", 0o600) {
            Ok(fd) => {
                fs::fsync(fd.as_fd()).map_err(|error| Error::IoFailure(error.to_string()))?;
                fs::fsync_dir_fd(control_fd.as_fd())
                    .map_err(|error| Error::IoFailure(error.to_string()))?;
                fd
            }
            Err(error) if recovery_lock_exists(&error) => fs::openat(
                control_fd.as_fd(),
                "recovery.lock",
                RECOVERY_LOCK_OPEN_FLAGS,
                0,
            )
            .map_err(|error| Error::IoFailure(error.to_string()))?,
            Err(error) => return Err(Error::IoFailure(error.to_string())),
        };
        if !fs::try_ofd_write_lock(lock_fd.as_fd())
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
            queue_id: steadq_names::hex_encode(self.format.queue_id()),
            cursor: self.recovery_cursor.clone(),
        };
        let bytes = serde_json::to_vec(&record)
            .map_err(|error| Error::IoFailure(format!("recovery cursor encode: {error}")))?;
        if !cursor_record_bytes_fit(bytes.len()) {
            return Err(Error::InvalidInput(
                "recovery cursor exceeds maximum encoded size".into(),
            ));
        }
        let control_fd = fs::open_directory(self.root_fd(), "control").map_err(|error| {
            Error::IoFailure(format!(
                "recovery cursor publication not committed at phase=ControlOpen: {error}"
            ))
        })?;
        let temp_name = format!(
            ".recovery-cursor.{}.tmp",
            steadq_names::hex_encode(&fs::random_128bit().map_err(|error| {
                Error::IoFailure(format!(
                    "recovery cursor publication not committed at phase=TempName: {error}"
                ))
            })?)
        );
        let temp_fd =
            fs::create_exclusive(control_fd.as_fd(), &temp_name, 0o600).map_err(|error| {
                Error::IoFailure(format!(
                    "recovery cursor publication not committed at phase=TempCreate: {error}"
                ))
            })?;
        if let Err(error) = fs::write_all(temp_fd.as_fd(), &bytes) {
            return Err(Self::cleanup_cursor_temporary_file(
                control_fd.as_fd(),
                &temp_name,
                format!("recovery cursor publication not committed at phase=TempWrite: {error}"),
            ));
        }
        if let Err(error) = fs::fsync(temp_fd.as_fd()) {
            return Err(Self::cleanup_cursor_temporary_file(
                control_fd.as_fd(),
                &temp_name,
                format!("recovery cursor publication not committed at phase=TempFsync: {error}"),
            ));
        }

        match replace_verified(
            control_fd.as_fd(),
            &temp_name,
            control_fd.as_fd(),
            RECOVERY_CURSOR_FILE,
            None,
            MoveActor::Recovery,
        ) {
            Ok(()) => Ok(()),
            Err(failure) => {
                let outcome_unknown = failure.is_outcome_unknown();
                let failure = Self::cursor_replace_failure(failure);
                if outcome_unknown {
                    Err(Error::IoFailure(failure))
                } else {
                    Err(Self::cleanup_cursor_temporary_file(
                        control_fd.as_fd(),
                        &temp_name,
                        failure,
                    ))
                }
            }
        }
    }

    fn cursor_replace_failure(failure: ReplaceFailure) -> String {
        match failure {
            ReplaceFailure::NotCommitted { phase, source } => format!(
                "recovery cursor replacement not committed at phase={phase:?}: {source}"
            ),
            ReplaceFailure::OutcomeUnknown { phase, source } => format!(
                "recovery cursor replacement outcome unknown at phase={phase:?}: {source}"
            ),
            ReplaceFailure::SourceMissing => {
                "recovery cursor replacement not committed at phase=Rename: source is missing"
                    .into()
            }
            ReplaceFailure::DestinationChanged => {
                "recovery cursor replacement not committed at phase=DestinationIdentity: destination identity changed"
                    .into()
            }
        }
    }

    fn cleanup_cursor_temporary_file(
        control_fd: BorrowedFd<'_>,
        temp_name: &str,
        primary_failure: String,
    ) -> Error {
        match unlink_verified(control_fd, temp_name, MoveActor::Recovery) {
            Ok(()) | Err(UnlinkFailure::SourceMissing) => Error::IoFailure(primary_failure),
            Err(UnlinkFailure::NotCommitted { phase, source }) => Error::IoFailure(format!(
                "{primary_failure}; stale recovery cursor temporary file requires later cleanup at control/{temp_name}: cleanup not committed at phase={phase:?}: {source}"
            )),
            Err(UnlinkFailure::OutcomeUnknown { phase, source }) => Error::IoFailure(format!(
                "{primary_failure}; cleanup durability is unknown for stale recovery cursor temporary file control/{temp_name}: phase={phase:?}: {source}"
            )),
        }
    }

    /// Run one bounded recovery pass.
    pub fn recover(&mut self, budget: &WorkBudget) -> RecoveryStats {
        self.recover_with_scan_budget(budget, &RecoveryScanBudget::default())
            .stats
    }

    /// Run one bounded recovery pass with explicit directory scan limits.
    pub fn recover_with_scan_budget(
        &mut self,
        budget: &WorkBudget,
        scan_budget: &RecoveryScanBudget,
    ) -> RecoveryReport {
        let mut stats = RecoveryStats::default();
        let mut scan_stats = RecoveryScanStats::default();
        if let Err(error) = scan_budget.validate() {
            stats.phase_blocked = true;
            stats.errors.push(RecoveryError {
                operation: "recovery_scan_budget".into(),
                relative_path: "/".into(),
                error: error.to_string(),
            });
            return RecoveryReport {
                stats,
                scan: scan_stats,
            };
        }
        let _recovery_lock = match self.acquire_recovery_lock() {
            Ok(lock) => lock,
            Err(error) => {
                stats.errors.push(RecoveryError {
                    operation: "recovery_lock".into(),
                    relative_path: "control/recovery.lock".into(),
                    error: error.to_string(),
                });
                return RecoveryReport {
                    stats,
                    scan: scan_stats,
                };
            }
        };
        self.recovery_cursor = match load_recovery_cursor(self.root_fd(), self.format.queue_id()) {
            Ok(cursor) => cursor,
            Err(error) => {
                stats.errors.push(RecoveryError {
                    operation: "recovery_cursor_reload".into(),
                    relative_path: format!("control/{RECOVERY_CURSOR_FILE}"),
                    error: error.to_string(),
                });
                return RecoveryReport {
                    stats,
                    scan: scan_stats,
                };
            }
        };
        let boottime_now = match fs::clock_boottime_ns() {
            Ok(t) => t,
            Err(e) => {
                stats.errors.push(RecoveryError {
                    operation: "clock_boottime".into(),
                    relative_path: "/".into(),
                    error: e.to_string(),
                });
                return RecoveryReport {
                    stats,
                    scan: scan_stats,
                };
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
                stats.errors.push(RecoveryError {
                    operation: "clock_monotonic".into(),
                    relative_path: "/".into(),
                    error: e.to_string(),
                });
                return RecoveryReport {
                    stats,
                    scan: scan_stats,
                };
            }
        };
        let deadline_mono =
            start_mono.saturating_add(budget.max_duration_ms.saturating_mul(1_000_000));
        let mut scan = RecoveryScanContext {
            budget: scan_budget,
            stats: &mut scan_stats,
        };

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
                        &mut scan,
                        &mut stats,
                        deadline_mono,
                    );
                    RecoveryPhase::PromoteDelayed
                }
                RecoveryPhase::PromoteDelayed => {
                    if let Some(wall_floor) = wall_floor {
                        self.promote_delayed(
                            wall_floor,
                            budget,
                            &mut scan,
                            &mut stats,
                            deadline_mono,
                        );
                    }
                    RecoveryPhase::CleanupTemp
                }
                RecoveryPhase::CleanupTemp => {
                    self.cleanup_temp_files(
                        boottime_now,
                        budget,
                        &mut scan,
                        &mut stats,
                        deadline_mono,
                    );
                    RecoveryPhase::CompactReceipts
                }
                RecoveryPhase::CompactReceipts => {
                    self.compact_receipts_with_scan_budget(
                        budget,
                        &mut scan,
                        &mut stats,
                        deadline_mono,
                    );
                    RecoveryPhase::DeleteReceipts
                }
                RecoveryPhase::DeleteReceipts => {
                    if let Some(wall_floor) = wall_floor {
                        self.delete_expired_receipts(
                            wall_floor,
                            self.options.receipt_retention_ns,
                            budget,
                            &mut scan,
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
            if Self::work_budget_exhausted(&mut stats, budget, deadline_mono) {
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

        RecoveryReport {
            stats,
            scan: scan_stats,
        }
    }

    /// B1: Quarantine an object during recovery.
    fn quarantine_recovery_object(
        &self,
        candidate: RecoveryQuarantineCandidate<'_>,
        stats: &mut RecoveryStats,
        budget: &WorkBudget,
    ) -> bool {
        self.quarantine_recovery_object_with_ids(candidate, stats, budget, fs::random_128bit)
    }

    fn quarantine_recovery_object_with_ids<F>(
        &self,
        candidate: RecoveryQuarantineCandidate<'_>,
        stats: &mut RecoveryStats,
        budget: &WorkBudget,
        next_id: F,
    ) -> bool
    where
        F: FnMut() -> io::Result<[u8; 16]>,
    {
        let remaining_attempts = budget
            .max_operations
            .saturating_sub(stats.operations_attempted);
        if remaining_attempts == 0 {
            stats.budget_exhausted = true;
            return false;
        }
        let result = self.publish_quarantine_object_with_ids(
            candidate.source_directory_fd,
            candidate.filename,
            candidate.reason,
            usize::try_from(remaining_attempts).unwrap_or(usize::MAX),
            next_id,
        );
        let attempts_consumed = match &result {
            Ok(publication) => publication.attempts_consumed,
            Err(error) => error.attempts_consumed(),
        };
        stats.operations_attempted = stats
            .operations_attempted
            .saturating_add(u32::try_from(attempts_consumed).unwrap_or(u32::MAX));
        match result {
            Ok(publication) => {
                stats.quarantined.push(RecoveryQuarantine {
                    relative_path: candidate.relative_path.to_string(),
                    quarantine_id: publication.quarantine_id,
                    quarantine_name: publication.quarantine_name,
                });
                true
            }
            Err(crate::quarantine::QuarantinePublishFailure::BudgetExhausted { .. }) => {
                Self::record_error(
                    stats,
                    "quarantine_budget_exhausted",
                    candidate.relative_path,
                    "quarantine collision retries exhausted the remaining operation budget",
                );
                stats.budget_exhausted = true;
                false
            }
            Err(error) => {
                Self::record_error(
                    stats,
                    "quarantine",
                    candidate.relative_path,
                    &error.to_string(),
                );
                true
            }
        }
    }

    /// R2-H05: Check if the monotonic deadline has been exceeded.
    fn budget_time_exceeded(deadline_mono: u64) -> io::Result<bool> {
        fs::clock_monotonic_ns().map(|now| now >= deadline_mono)
    }

    /// Check whether classification or mutation work must stop.
    ///
    /// Directory enumeration limits are enforced before starting the next
    /// read. Reaching a scan limit must not discard entries already returned.
    fn work_budget_exhausted(
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

    fn record_move_failure(
        stats: &mut RecoveryStats,
        operation: &str,
        path: &str,
        failure: MoveFailure,
    ) {
        let (category, detail) = match failure {
            MoveFailure::NotCommitted { phase, source } => {
                ("not_committed", format!("phase={phase:?}: {source}"))
            }
            MoveFailure::OutcomeUnknown { phase, source } => {
                ("outcome_unknown", format!("phase={phase:?}: {source}"))
            }
            MoveFailure::AlreadyExists => (
                "collision",
                "phase=Rename: destination already exists".into(),
            ),
            MoveFailure::SourceMissing => {
                ("source_missing", "phase=Rename: source is missing".into())
            }
        };
        Self::record_error(stats, &format!("{operation}_{category}"), path, &detail);
    }

    fn record_unlink_failure(
        stats: &mut RecoveryStats,
        operation: &str,
        path: &str,
        failure: UnlinkFailure,
    ) {
        let (category, detail) = match failure {
            UnlinkFailure::NotCommitted { phase, source } => {
                ("not_committed", format!("phase={phase:?}: {source}"))
            }
            UnlinkFailure::OutcomeUnknown { phase, source } => {
                ("outcome_unknown", format!("phase={phase:?}: {source}"))
            }
            UnlinkFailure::SourceMissing => {
                ("source_missing", "phase=Unlink: source is missing".into())
            }
        };
        Self::record_error(stats, &format!("{operation}_{category}"), path, &detail);
    }

    fn record_remove_directory_failure(
        stats: &mut RecoveryStats,
        operation: &str,
        path: &str,
        failure: RemoveDirectoryFailure,
    ) {
        let (category, detail) = match failure {
            RemoveDirectoryFailure::NotCommitted { phase, source } => {
                ("not_committed", format!("phase={phase:?}: {source}"))
            }
            RemoveDirectoryFailure::OutcomeUnknown { phase, source } => {
                ("outcome_unknown", format!("phase={phase:?}: {source}"))
            }
            RemoveDirectoryFailure::SourceMissing => (
                "source_missing",
                "phase=Remove: directory is missing".into(),
            ),
            RemoveDirectoryFailure::NotEmpty => {
                ("not_empty", "phase=Remove: directory is not empty".into())
            }
        };
        Self::record_error(stats, &format!("{operation}_{category}"), path, &detail);
    }

    fn record_replace_failure(
        stats: &mut RecoveryStats,
        operation: &str,
        path: &str,
        failure: ReplaceFailure,
    ) {
        let (category, detail) = match failure {
            ReplaceFailure::NotCommitted { phase, source } => {
                ("not_committed", format!("phase={phase:?}: {source}"))
            }
            ReplaceFailure::OutcomeUnknown { phase, source } => {
                ("outcome_unknown", format!("phase={phase:?}: {source}"))
            }
            ReplaceFailure::SourceMissing => {
                ("source_missing", "phase=Rename: source is missing".into())
            }
            ReplaceFailure::DestinationChanged => (
                "destination_changed",
                "phase=DestinationIdentity: destination identity changed".into(),
            ),
        };
        Self::record_error(stats, &format!("{operation}_{category}"), path, &detail);
    }

    fn cleanup_compaction_temp(
        stats: &mut RecoveryStats,
        directory_fd: BorrowedFd<'_>,
        name: &str,
        relative_path: &str,
    ) {
        match unlink_verified(directory_fd, name, MoveActor::Recovery) {
            Ok(()) | Err(UnlinkFailure::SourceMissing) => {}
            Err(failure) => Self::record_unlink_failure(
                stats,
                "receipt_compact_temp_cleanup",
                relative_path,
                failure,
            ),
        }
    }

    fn block_phase(stats: &mut RecoveryStats, op: &str, path: &str, err: &str) {
        Self::record_error(stats, op, path, err);
        stats.phase_blocked = true;
    }

    fn record_directory_error(
        stats: &mut RecoveryStats,
        op: &str,
        path: &str,
        error: &RecoveryDirectoryError,
    ) -> bool {
        match error {
            RecoveryDirectoryError::BudgetExhausted => {
                stats.budget_exhausted = true;
                true
            }
            RecoveryDirectoryError::Clock(error) => {
                Self::block_phase(
                    stats,
                    "clock_monotonic",
                    path,
                    &format!("directory budget clock unavailable during {op}: {error}"),
                );
                stats.budget_exhausted = true;
                true
            }
            RecoveryDirectoryError::Io(error) => {
                Self::block_phase(stats, op, path, &error.to_string());
                false
            }
        }
    }

    fn remember_hierarchy_retry(
        &mut self,
        phase: RecoveryPhase,
        kind: RecoveryHierarchyRetryKind,
        components: &[&[u8]],
    ) -> RememberHierarchyRetry {
        let Some(components) = components
            .iter()
            .map(|component| std::str::from_utf8(component).ok().map(str::to_owned))
            .collect::<Option<Vec<_>>>()
        else {
            return RememberHierarchyRetry::Invalid;
        };
        let retry = RecoveryHierarchyRetry {
            phase,
            kind,
            components,
        };
        match self.recovery_cursor.hierarchy_retries.binary_search(&retry) {
            Ok(_) => RememberHierarchyRetry::Exact,
            Err(index)
                if self.recovery_cursor.hierarchy_retries.len()
                    < MAX_RECOVERY_HIERARCHY_RETRIES =>
            {
                self.recovery_cursor.hierarchy_retries.insert(index, retry);
                RememberHierarchyRetry::Exact
            }
            Err(_) => {
                if let Err(index) = self
                    .recovery_cursor
                    .hierarchy_retry_overflow
                    .binary_search(&phase)
                {
                    self.recovery_cursor
                        .hierarchy_retry_overflow
                        .insert(index, phase);
                }
                RememberHierarchyRetry::Overflow
            }
        }
    }

    fn remember_hierarchy_retry_or_block(
        &mut self,
        phase: RecoveryPhase,
        kind: RecoveryHierarchyRetryKind,
        components: &[&[u8]],
        stats: &mut RecoveryStats,
        path: &str,
    ) -> bool {
        match self.remember_hierarchy_retry(phase, kind, components) {
            RememberHierarchyRetry::Exact => true,
            RememberHierarchyRetry::Overflow => {
                Self::block_phase(
                    stats,
                    "hierarchy_retry_overflow",
                    path,
                    "recovery hierarchy retry ledger is full; phase will be fully rescanned",
                );
                true
            }
            RememberHierarchyRetry::Invalid => {
                Self::block_phase(
                    stats,
                    "hierarchy_retry_invalid",
                    path,
                    "recovery hierarchy retry path is not canonical UTF-8",
                );
                false
            }
        }
    }

    fn clear_phase_cursor(&mut self, phase: RecoveryPhase) {
        match phase {
            RecoveryPhase::ReapLeases => self.recovery_cursor.reap_leases = None,
            RecoveryPhase::PromoteDelayed => self.recovery_cursor.promote_delayed = None,
            RecoveryPhase::CleanupTemp => self.recovery_cursor.cleanup_temp = None,
            RecoveryPhase::CompactReceipts => self.recovery_cursor.compact_receipts = None,
            RecoveryPhase::DeleteReceipts => self.recovery_cursor.delete_receipts = None,
        }
    }

    fn prepare_hierarchy_retry_phase(
        &mut self,
        phase: RecoveryPhase,
    ) -> Option<RecoveryHierarchyRetry> {
        if let Ok(index) = self
            .recovery_cursor
            .hierarchy_retry_overflow
            .binary_search(&phase)
        {
            self.recovery_cursor.hierarchy_retry_overflow.remove(index);
            self.clear_phase_cursor(phase);
        }
        self.next_hierarchy_retry(phase)
    }

    fn next_hierarchy_retry(&self, phase: RecoveryPhase) -> Option<RecoveryHierarchyRetry> {
        let retries = self
            .recovery_cursor
            .hierarchy_retries
            .iter()
            .filter(|retry| retry.phase == phase)
            .collect::<Vec<_>>();
        let first = (*retries.first()?).clone();
        let Some(frontier) = self
            .recovery_cursor
            .hierarchy_retry_frontiers
            .iter()
            .find(|frontier| frontier.phase == phase)
        else {
            return Some(first);
        };
        retries
            .into_iter()
            .find(|retry| *retry > frontier)
            .cloned()
            .or(Some(first))
    }

    fn advance_hierarchy_retry_frontier(&mut self, retry: RecoveryHierarchyRetry) {
        match self
            .recovery_cursor
            .hierarchy_retry_frontiers
            .binary_search_by_key(&retry.phase, |frontier| frontier.phase)
        {
            Ok(index) => self.recovery_cursor.hierarchy_retry_frontiers[index] = retry,
            Err(index) => self
                .recovery_cursor
                .hierarchy_retry_frontiers
                .insert(index, retry),
        }
    }

    fn retry_one_hierarchy_directory(
        &mut self,
        phase: RecoveryPhase,
        retry: Option<RecoveryHierarchyRetry>,
        phase_root_fd: BorrowedFd<'_>,
        scan: &mut RecoveryScanContext<'_>,
        stats: &mut RecoveryStats,
        deadline_mono: u64,
    ) -> bool {
        let Some(retry) = retry else {
            self.recovery_cursor
                .hierarchy_retry_frontiers
                .retain(|frontier| frontier.phase != phase);
            return false;
        };
        match Self::budget_time_exceeded(deadline_mono) {
            Ok(true) => {
                stats.budget_exhausted = true;
                return true;
            }
            Ok(false) => {}
            Err(error) => {
                Self::block_phase(
                    stats,
                    "clock_monotonic",
                    "/",
                    &format!("recovery retry budget clock unavailable: {error}"),
                );
                stats.budget_exhausted = true;
                return true;
            }
        }
        let mut current = None::<OwnedFd>;
        let mut failure = None;
        let mut absent = false;
        for component in &retry.components {
            match Self::budget_time_exceeded(deadline_mono) {
                Ok(true) => {
                    stats.budget_exhausted = true;
                    return true;
                }
                Ok(false) => {}
                Err(error) => {
                    Self::block_phase(
                        stats,
                        "clock_monotonic",
                        "/",
                        &format!("recovery retry budget clock unavailable: {error}"),
                    );
                    stats.budget_exhausted = true;
                    return true;
                }
            }
            let parent_fd = current
                .as_ref()
                .map_or(phase_root_fd, |directory| directory.as_fd());
            match fs::open_directory(parent_fd, component) {
                Ok(directory) => current = Some(directory),
                Err(error) if error.raw_os_error() == Some(libc::ENOENT) => {
                    absent = true;
                    break;
                }
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            }
        }
        if let Some(error) = failure {
            stats.scan_skips += 1;
            Self::block_phase(
                stats,
                "hierarchy_retry_open",
                &retry
                    .components
                    .iter()
                    .map(|component| steadq_names::hex_encode(component.as_bytes()))
                    .collect::<Vec<_>>()
                    .join("/"),
                &error.to_string(),
            );
            self.advance_hierarchy_retry_frontier(retry);
            return false;
        }

        if !absent && retry.kind == RecoveryHierarchyRetryKind::Enumerate {
            let directory = current
                .as_ref()
                .expect("validated retry paths contain at least one component");
            if let Err(error) =
                read_recovery_directory(directory.as_fd(), deadline_mono, scan.budget, scan.stats)
            {
                if Self::record_directory_error(
                    stats,
                    "hierarchy_retry_read",
                    &retry
                        .components
                        .iter()
                        .map(|component| steadq_names::hex_encode(component.as_bytes()))
                        .collect::<Vec<_>>()
                        .join("/"),
                    &error,
                ) {
                    return true;
                }
                stats.scan_skips += 1;
                self.advance_hierarchy_retry_frontier(retry);
                return false;
            }
        }

        self.recovery_cursor
            .hierarchy_retries
            .retain(|candidate| candidate != &retry);
        self.clear_phase_cursor(phase);
        if self
            .recovery_cursor
            .hierarchy_retries
            .iter()
            .any(|candidate| candidate.phase == phase)
        {
            self.advance_hierarchy_retry_frontier(retry);
        } else {
            self.recovery_cursor
                .hierarchy_retry_frontiers
                .retain(|frontier| frontier.phase != phase);
        }
        false
    }

    fn reap_expired_leases(
        &mut self,
        boottime_now: u64,
        wall_floor: Option<WallFloor>,
        budget: &WorkBudget,
        scan: &mut RecoveryScanContext<'_>,
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
        let hierarchy_retry = self.prepare_hierarchy_retry_phase(RecoveryPhase::ReapLeases);
        if self.retry_one_hierarchy_directory(
            RecoveryPhase::ReapLeases,
            hierarchy_retry,
            leased_fd.as_fd(),
            scan,
            stats,
            deadline_mono,
        ) {
            return;
        }

        let mut boot_dirs = match read_recovery_directory(
            leased_fd.as_fd(),
            deadline_mono,
            scan.budget,
            scan.stats,
        ) {
            Ok(e) => e,
            Err(e) => {
                Self::record_directory_error(stats, "read_leased_dirs", "leased", &e);
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
            if Self::work_budget_exhausted(stats, budget, deadline_mono) {
                stats.budget_exhausted = true;
                return;
            }
            let Some(boot_dir_name) = boot_dir_entry.as_ascii_str() else {
                Self::record_error(
                    stats,
                    "reap_boot_name",
                    &raw_name_for_error(boot_dir_entry),
                    "boot directory name is not ASCII",
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

            let boot_dir_fd = match fs::open_directory(leased_fd.as_fd(), boot_dir_name) {
                Ok(fd) => fd,
                Err(error) => {
                    stats.scan_skips += 1;
                    Self::block_phase(stats, "reap_boot_open", boot_dir_name, &error.to_string());
                    if !self.remember_hierarchy_retry_or_block(
                        RecoveryPhase::ReapLeases,
                        RecoveryHierarchyRetryKind::Open,
                        &[boot_dir_entry.as_bytes()],
                        stats,
                        boot_dir_name,
                    ) {
                        return;
                    }
                    continue;
                }
            };

            let mut bucket_dirs = match read_recovery_directory(
                boot_dir_fd.as_fd(),
                deadline_mono,
                scan.budget,
                scan.stats,
            ) {
                Ok(e) => e,
                Err(error) => {
                    stats.scan_skips += 1;
                    if Self::record_directory_error(
                        stats,
                        "reap_bucket_read",
                        boot_dir_name,
                        &error,
                    ) {
                        return;
                    }
                    if !self.remember_hierarchy_retry_or_block(
                        RecoveryPhase::ReapLeases,
                        RecoveryHierarchyRetryKind::Enumerate,
                        &[boot_dir_entry.as_bytes()],
                        stats,
                        boot_dir_name,
                    ) {
                        return;
                    }
                    continue;
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
                if Self::work_budget_exhausted(stats, budget, deadline_mono) {
                    stats.budget_exhausted = true;
                    return;
                }
                let Some(bucket_name) = bucket_entry.as_ascii_str() else {
                    Self::record_error(
                        stats,
                        "reap_bucket_name",
                        &raw_name_for_error(bucket_entry),
                        "bucket directory name is not ASCII",
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
                    let Some(current_bucket) = steadq_math::bucket_number(
                        boottime_now,
                        self.format.lease_bucket_width_ns(),
                    ) else {
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

                let bucket_fd = match fs::open_directory(boot_dir_fd.as_fd(), bucket_name) {
                    Ok(fd) => fd,
                    Err(error) => {
                        stats.scan_skips += 1;
                        Self::block_phase(
                            stats,
                            "reap_bucket_open",
                            &format!("leased/{boot_dir_name}/{bucket_name}"),
                            &error.to_string(),
                        );
                        if !self.remember_hierarchy_retry_or_block(
                            RecoveryPhase::ReapLeases,
                            RecoveryHierarchyRetryKind::Open,
                            &[boot_dir_entry.as_bytes(), bucket_entry.as_bytes()],
                            stats,
                            &format!("leased/{boot_dir_name}/{bucket_name}"),
                        ) {
                            return;
                        }
                        continue;
                    }
                };

                let mut shard_dirs = match read_recovery_directory(
                    bucket_fd.as_fd(),
                    deadline_mono,
                    scan.budget,
                    scan.stats,
                ) {
                    Ok(e) => e,
                    Err(error) => {
                        stats.scan_skips += 1;
                        if Self::record_directory_error(
                            stats,
                            "reap_shard_read",
                            &format!("leased/{boot_dir_name}/{bucket_name}"),
                            &error,
                        ) {
                            return;
                        }
                        if !self.remember_hierarchy_retry_or_block(
                            RecoveryPhase::ReapLeases,
                            RecoveryHierarchyRetryKind::Enumerate,
                            &[boot_dir_entry.as_bytes(), bucket_entry.as_bytes()],
                            stats,
                            &format!("leased/{boot_dir_name}/{bucket_name}"),
                        ) {
                            return;
                        }
                        continue;
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
                    let Some(shard_name) = shard_entry.as_ascii_str() else {
                        Self::record_error(
                            stats,
                            "reap_shard_name",
                            &raw_name_for_error(shard_entry),
                            "shard directory name is not ASCII",
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
                    if shard >= self.format.shard_count() {
                        Self::record_error(
                            stats,
                            "reap_shard_name",
                            shard_name,
                            "shard directory is outside the queue shard range",
                        );
                        continue;
                    }
                    let shard_fd = match fs::open_directory(bucket_fd.as_fd(), shard_name) {
                        Ok(fd) => fd,
                        Err(error) => {
                            stats.scan_skips += 1;
                            Self::block_phase(
                                stats,
                                "reap_shard_open",
                                &format!("leased/{boot_dir_name}/{bucket_name}/{shard_name}"),
                                &error.to_string(),
                            );
                            if !self.remember_hierarchy_retry_or_block(
                                RecoveryPhase::ReapLeases,
                                RecoveryHierarchyRetryKind::Open,
                                &[
                                    boot_dir_entry.as_bytes(),
                                    bucket_entry.as_bytes(),
                                    shard_entry.as_bytes(),
                                ],
                                stats,
                                &format!("leased/{boot_dir_name}/{bucket_name}/{shard_name}"),
                            ) {
                                return;
                            }
                            continue;
                        }
                    };

                    let mut entries = match read_recovery_directory(
                        shard_fd.as_fd(),
                        deadline_mono,
                        scan.budget,
                        scan.stats,
                    ) {
                        Ok(e) => e,
                        Err(error) => {
                            stats.scan_skips += 1;
                            if Self::record_directory_error(
                                stats,
                                "reap_entry_read",
                                &format!("leased/{boot_dir_name}/{bucket_name}/{shard_name}"),
                                &error,
                            ) {
                                return;
                            }
                            if !self.remember_hierarchy_retry_or_block(
                                RecoveryPhase::ReapLeases,
                                RecoveryHierarchyRetryKind::Enumerate,
                                &[
                                    boot_dir_entry.as_bytes(),
                                    bucket_entry.as_bytes(),
                                    shard_entry.as_bytes(),
                                ],
                                stats,
                                &format!("leased/{boot_dir_name}/{bucket_name}/{shard_name}"),
                            ) {
                                return;
                            }
                            continue;
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
                        if Self::work_budget_exhausted(stats, budget, deadline_mono) {
                            stats.budget_exhausted = true;
                            return;
                        }
                        let previous_entry_cursor = self.recovery_cursor.reap_leases.clone();
                        self.recovery_cursor.reap_leases = Some(FourLevelCursor::new(
                            boot_dir_entry.as_bytes(),
                            bucket_entry.as_bytes(),
                            shard_entry.as_bytes(),
                            raw_entry.as_bytes(),
                        ));
                        let Some(entry) = raw_entry.as_ascii_str() else {
                            Self::record_error(
                                stats,
                                "reap_entry_name",
                                &raw_name_for_error(raw_entry),
                                "entry name is not ASCII",
                            );
                            continue;
                        };

                        if !entry.ends_with(".sqj") {
                            continue;
                        }

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
                            self.validate_active_object(shard_fd.as_fd(), entry, &leased_ctx)
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
                            if matches!(e, Error::QueueCorrupt(_))
                                && !self.quarantine_recovery_object(
                                    RecoveryQuarantineCandidate {
                                        source_directory_fd: shard_fd.as_fd(),
                                        filename: entry,
                                        relative_path: &format!(
                                            "leased/{boot_dir_name}/{bucket_name}/{shard_name}/{entry}"
                                        ),
                                        reason: crate::QuarantineReason::EnvelopeCorrupt,
                                    },
                                    stats,
                                    budget,
                                )
                            {
                                self.recovery_cursor.reap_leases = previous_entry_cursor;
                                return;
                            }
                            continue;
                        }

                        // R4-B02: Verify bucket placement matches deadline-derived bucket
                        let Some(expected_lease_bucket) = steadq_math::lease_bucket(
                            parsed.boottime_deadline_ns,
                            self.format.lease_bucket_width_ns(),
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
                            let relative_path = format!(
                                "leased/{boot_dir_name}/{bucket_name}/{shard_name}/{entry}"
                            );
                            stats.operations_attempted += 1;
                            match self.reap_to_dead(
                                boot_dir_name,
                                bucket_name,
                                shard_name,
                                entry,
                                &parsed.common,
                                DeadReason::AttemptsExhausted,
                                wall_floor,
                            ) {
                                Ok(()) => stats.leases_to_dead += 1,
                                Err(failure) => Self::record_move_failure(
                                    stats,
                                    "reap_to_dead",
                                    &relative_path,
                                    failure,
                                ),
                            }
                        } else {
                            // Move to ready
                            let relative_path = format!(
                                "leased/{boot_dir_name}/{bucket_name}/{shard_name}/{entry}"
                            );
                            stats.operations_attempted += 1;
                            match self.reap_to_ready(
                                boot_dir_name,
                                bucket_name,
                                shard_name,
                                entry,
                                &parsed.common,
                            ) {
                                Ok(()) => stats.leases_reaped += 1,
                                Err(failure) => Self::record_move_failure(
                                    stats,
                                    "reap_to_ready",
                                    &relative_path,
                                    failure,
                                ),
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
    ) -> Result<(), MoveFailure> {
        let leased_bucket =
            steadq_names::bucket_from_hex(bucket).ok_or_else(|| MoveFailure::NotCommitted {
                phase: MovePhase::PreRename,
                source: std::io::Error::other(format!("invalid bucket: {bucket}")),
            })?;
        let shard_num = match u32::from_str_radix(shard, 16) {
            Ok(n) => n,
            Err(_) => {
                return Err(MoveFailure::NotCommitted {
                    phase: MovePhase::PreRename,
                    source: std::io::Error::other(format!("invalid shard: {shard}")),
                })
            }
        };
        let src_dir = self
            .layout()
            .leased_shard_dir(boot_dir, leased_bucket, shard_num);
        let dest_dir = self.layout().ready_shard_dir(shard_num);

        let new_gen =
            common
                .generation
                .checked_add(1)
                .ok_or_else(|| MoveFailure::NotCommitted {
                    phase: MovePhase::PreRename,
                    source: std::io::Error::other("generation overflow"),
                })?;
        let ready_common = steadq_names::CommonFields {
            job_id: common.job_id,
            generation: new_gen,
            attempt: common.attempt,
            maximum_attempts: common.maximum_attempts,
        };

        let ready_target = self.layout().ready(&ready_common);
        let ready_name = ready_target.filename;

        let src_fd =
            open_relative(self.root_fd(), &src_dir).map_err(|error| MoveFailure::NotCommitted {
                phase: MovePhase::PreRename,
                source: error,
            })?;
        let dest_fd = open_relative(self.root_fd(), &dest_dir).map_err(|error| {
            MoveFailure::NotCommitted {
                phase: MovePhase::EnsureDest,
                source: error,
            }
        })?;

        move_verified_noreplace(
            src_fd.as_fd(),
            leased_name,
            dest_fd.as_fd(),
            &ready_name,
            MoveActor::Recovery,
        )
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
    ) -> Result<(), MoveFailure> {
        let leased_bucket =
            steadq_names::bucket_from_hex(bucket).ok_or_else(|| MoveFailure::NotCommitted {
                phase: MovePhase::PreRename,
                source: std::io::Error::other(format!("invalid bucket: {bucket}")),
            })?;
        let shard_num = match u32::from_str_radix(shard, 16) {
            Ok(n) => n,
            Err(_) => {
                return Err(MoveFailure::NotCommitted {
                    phase: MovePhase::PreRename,
                    source: std::io::Error::other(format!("invalid shard: {shard}")),
                })
            }
        };
        let src_dir = self
            .layout()
            .leased_shard_dir(boot_dir, leased_bucket, shard_num);
        let terminal_bucket = steadq_math::bucket_number(
            wall_floor.unix_ns(),
            self.format.terminal_bucket_width_ns(),
        )
        .ok_or_else(|| MoveFailure::NotCommitted {
            phase: MovePhase::PreRename,
            source: std::io::Error::other("terminal bucket overflow"),
        })?;

        let new_gen =
            common
                .generation
                .checked_add(1)
                .ok_or_else(|| MoveFailure::NotCommitted {
                    phase: MovePhase::PreRename,
                    source: std::io::Error::other("generation overflow"),
                })?;
        let dead_common = steadq_names::CommonFields {
            job_id: common.job_id,
            generation: new_gen,
            attempt: common.attempt,
            maximum_attempts: common.maximum_attempts,
        };

        let dead_target =
            self.layout()
                .dead_in_bucket(&dead_common, reason as u16, terminal_bucket);
        let dest_dir = dead_target.directory();
        let dead_name = dead_target.filename;

        self.ensure_dir_pub(&dest_dir)
            .map_err(|error| MoveFailure::NotCommitted {
                phase: MovePhase::EnsureDest,
                source: error,
            })?;
        let src_fd =
            open_relative(self.root_fd(), &src_dir).map_err(|error| MoveFailure::NotCommitted {
                phase: MovePhase::PreRename,
                source: error,
            })?;
        let dest_fd = open_relative(self.root_fd(), &dest_dir).map_err(|error| {
            MoveFailure::NotCommitted {
                phase: MovePhase::EnsureDest,
                source: error,
            }
        })?;

        move_verified_noreplace(
            src_fd.as_fd(),
            leased_name,
            dest_fd.as_fd(),
            &dead_name,
            MoveActor::Recovery,
        )
    }

    fn promote_delayed(
        &mut self,
        wall_floor: WallFloor,
        budget: &WorkBudget,
        scan: &mut RecoveryScanContext<'_>,
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
        let hierarchy_retry = self.prepare_hierarchy_retry_phase(RecoveryPhase::PromoteDelayed);
        if self.retry_one_hierarchy_directory(
            RecoveryPhase::PromoteDelayed,
            hierarchy_retry,
            delayed_fd.as_fd(),
            scan,
            stats,
            deadline_mono,
        ) {
            return;
        }

        let mut bucket_dirs = match read_recovery_directory(
            delayed_fd.as_fd(),
            deadline_mono,
            scan.budget,
            scan.stats,
        ) {
            Ok(e) => e,
            Err(error) => {
                Self::record_directory_error(stats, "promote_bucket_read", "delayed", &error);
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

            if Self::work_budget_exhausted(stats, budget, deadline_mono) {
                stats.budget_exhausted = true;
                return;
            }
            let Some(bucket_name) = bucket_entry.as_ascii_str() else {
                Self::record_error(
                    stats,
                    "promote_bucket_name",
                    &raw_name_for_error(bucket_entry),
                    "bucket directory name is not ASCII",
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
                self.format.delayed_bucket_width_ns(),
            ) {
                Some(bucket) => bucket,
                None => return,
            };

            // Only promote buckets at or below the current wall bucket
            if bucket_num > current_wall_bucket {
                continue;
            }

            let bucket_fd = match fs::open_directory(delayed_fd.as_fd(), bucket_name) {
                Ok(fd) => fd,
                Err(error) => {
                    stats.scan_skips += 1;
                    Self::block_phase(
                        stats,
                        "promote_bucket_open",
                        &format!("delayed/{bucket_name}"),
                        &error.to_string(),
                    );
                    if !self.remember_hierarchy_retry_or_block(
                        RecoveryPhase::PromoteDelayed,
                        RecoveryHierarchyRetryKind::Open,
                        &[bucket_entry.as_bytes()],
                        stats,
                        &format!("delayed/{bucket_name}"),
                    ) {
                        return;
                    }
                    continue;
                }
            };

            let mut shard_dirs = match read_recovery_directory(
                bucket_fd.as_fd(),
                deadline_mono,
                scan.budget,
                scan.stats,
            ) {
                Ok(e) => e,
                Err(error) => {
                    stats.scan_skips += 1;
                    if Self::record_directory_error(
                        stats,
                        "promote_shard_read",
                        &format!("delayed/{bucket_name}"),
                        &error,
                    ) {
                        return;
                    }
                    if !self.remember_hierarchy_retry_or_block(
                        RecoveryPhase::PromoteDelayed,
                        RecoveryHierarchyRetryKind::Enumerate,
                        &[bucket_entry.as_bytes()],
                        stats,
                        &format!("delayed/{bucket_name}"),
                    ) {
                        return;
                    }
                    continue;
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
                let Some(shard_name) = shard_entry.as_ascii_str() else {
                    Self::record_error(
                        stats,
                        "promote_shard_name",
                        &raw_name_for_error(shard_entry),
                        "shard directory name is not ASCII",
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
                if shard >= self.format.shard_count() {
                    Self::record_error(
                        stats,
                        "promote_shard_name",
                        shard_name,
                        "shard directory is outside the queue shard range",
                    );
                    continue;
                }
                let shard_fd = match fs::open_directory(bucket_fd.as_fd(), shard_name) {
                    Ok(fd) => fd,
                    Err(error) => {
                        stats.scan_skips += 1;
                        Self::block_phase(
                            stats,
                            "promote_shard_open",
                            &format!("{bucket_name}/{shard_name}"),
                            &error.to_string(),
                        );
                        if !self.remember_hierarchy_retry_or_block(
                            RecoveryPhase::PromoteDelayed,
                            RecoveryHierarchyRetryKind::Open,
                            &[bucket_entry.as_bytes(), shard_entry.as_bytes()],
                            stats,
                            &format!("delayed/{bucket_name}/{shard_name}"),
                        ) {
                            return;
                        }
                        continue;
                    }
                };

                let mut entries = match read_recovery_directory(
                    shard_fd.as_fd(),
                    deadline_mono,
                    scan.budget,
                    scan.stats,
                ) {
                    Ok(e) => e,
                    Err(error) => {
                        stats.scan_skips += 1;
                        if Self::record_directory_error(
                            stats,
                            "promote_entry_read",
                            &format!("delayed/{bucket_name}/{shard_name}"),
                            &error,
                        ) {
                            return;
                        }
                        if !self.remember_hierarchy_retry_or_block(
                            RecoveryPhase::PromoteDelayed,
                            RecoveryHierarchyRetryKind::Enumerate,
                            &[bucket_entry.as_bytes(), shard_entry.as_bytes()],
                            stats,
                            &format!("delayed/{bucket_name}/{shard_name}"),
                        ) {
                            return;
                        }
                        continue;
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
                    if Self::work_budget_exhausted(stats, budget, deadline_mono) {
                        stats.budget_exhausted = true;
                        return;
                    }
                    let previous_entry_cursor = self.recovery_cursor.promote_delayed.clone();
                    self.recovery_cursor.promote_delayed = Some(ThreeLevelCursor::new(
                        bucket_entry.as_bytes(),
                        shard_entry.as_bytes(),
                        raw_entry.as_bytes(),
                    ));
                    let Some(entry) = raw_entry.as_ascii_str() else {
                        Self::record_error(
                            stats,
                            "promote_entry_name",
                            &raw_name_for_error(raw_entry),
                            "entry name is not ASCII",
                        );
                        continue;
                    };

                    if !entry.ends_with(".sqj") {
                        continue;
                    }

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
                            self.validate_active_object(shard_fd.as_fd(), entry, &delayed_ctx)
                        {
                            Self::record_error(
                                stats,
                                "promote_validate",
                                &format!("delayed/{bucket_name}/{shard_name}/{entry}"),
                                &format!("{e}"),
                            );
                            if matches!(e, Error::QueueCorrupt(_))
                                && !self.quarantine_recovery_object(
                                    RecoveryQuarantineCandidate {
                                        source_directory_fd: shard_fd.as_fd(),
                                        filename: entry,
                                        relative_path: &format!(
                                            "delayed/{bucket_name}/{shard_name}/{entry}"
                                        ),
                                        reason: crate::QuarantineReason::EnvelopeCorrupt,
                                    },
                                    stats,
                                    budget,
                                )
                            {
                                self.recovery_cursor.promote_delayed = previous_entry_cursor;
                                return;
                            }
                            continue;
                        }
                    }

                    stats.operations_attempted += 1;
                    let relative_path = format!("delayed/{bucket_name}/{shard_name}/{entry}");
                    match self.promote_to_ready(bucket_name, shard_name, entry, &parsed.common) {
                        Ok(()) => stats.delayed_promoted += 1,
                        Err(failure) => Self::record_move_failure(
                            stats,
                            "promote_delayed",
                            &relative_path,
                            failure,
                        ),
                    }
                }
            }
        }

        // R4-RES: All buckets processed, reset cursor for next full pass.
        self.recovery_cursor.promote_delayed = None;
    }

    fn promote_to_ready(
        &self,
        bucket: &str,
        shard: &str,
        delayed_name: &str,
        common: &steadq_names::CommonFields,
    ) -> Result<(), MoveFailure> {
        let new_gen =
            common
                .generation
                .checked_add(1)
                .ok_or_else(|| MoveFailure::NotCommitted {
                    phase: MovePhase::PreRename,
                    source: std::io::Error::other("generation overflow"),
                })?;
        let ready_common = steadq_names::CommonFields {
            job_id: common.job_id,
            generation: new_gen,
            attempt: common.attempt,
            maximum_attempts: common.maximum_attempts,
        };
        let ready_name =
            steadq_names::make_ready_name(self.format.queue_id(), shard, &ready_common);
        let src_dir = format!("delayed/{bucket}/{shard}");
        let dest_dir = format!("ready/{shard}");
        let src_fd =
            open_relative(self.root_fd(), &src_dir).map_err(|error| MoveFailure::NotCommitted {
                phase: MovePhase::PreRename,
                source: error,
            })?;
        let dest_fd = open_relative(self.root_fd(), &dest_dir).map_err(|error| {
            MoveFailure::NotCommitted {
                phase: MovePhase::EnsureDest,
                source: error,
            }
        })?;

        move_verified_noreplace(
            src_fd.as_fd(),
            delayed_name,
            dest_fd.as_fd(),
            &ready_name,
            MoveActor::Recovery,
        )
    }

    fn cleanup_temp_files(
        &mut self,
        boottime_now: u64,
        budget: &WorkBudget,
        scan: &mut RecoveryScanContext<'_>,
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
        let hierarchy_retry = self.prepare_hierarchy_retry_phase(RecoveryPhase::CleanupTemp);
        if self.retry_one_hierarchy_directory(
            RecoveryPhase::CleanupTemp,
            hierarchy_retry,
            tmp_fd.as_fd(),
            scan,
            stats,
            deadline_mono,
        ) {
            return;
        }

        let mut boot_dirs =
            match read_recovery_directory(tmp_fd.as_fd(), deadline_mono, scan.budget, scan.stats) {
                Ok(e) => e,
                Err(error) => {
                    Self::record_directory_error(stats, "temp_boot_read", "tmp", &error);
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
            if Self::work_budget_exhausted(stats, budget, deadline_mono) {
                stats.budget_exhausted = true;
                return;
            }
            let Some(boot_dir_name) = boot_entry.as_ascii_str() else {
                Self::record_error(
                    stats,
                    "temp_boot_name",
                    &raw_name_for_error(boot_entry),
                    "boot directory name is not ASCII",
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

            let boot_dir_fd = match fs::open_directory(tmp_fd.as_fd(), boot_dir_name) {
                Ok(fd) => fd,
                Err(error) => {
                    stats.scan_skips += 1;
                    Self::block_phase(stats, "temp_boot_open", boot_dir_name, &error.to_string());
                    if !self.remember_hierarchy_retry_or_block(
                        RecoveryPhase::CleanupTemp,
                        RecoveryHierarchyRetryKind::Open,
                        &[boot_entry.as_bytes()],
                        stats,
                        boot_dir_name,
                    ) {
                        return;
                    }
                    continue;
                }
            };

            let mut shard_dirs = match read_recovery_directory(
                boot_dir_fd.as_fd(),
                deadline_mono,
                scan.budget,
                scan.stats,
            ) {
                Ok(e) => e,
                Err(error) => {
                    stats.scan_skips += 1;
                    if Self::record_directory_error(stats, "temp_shard_read", boot_dir_name, &error)
                    {
                        return;
                    }
                    if !self.remember_hierarchy_retry_or_block(
                        RecoveryPhase::CleanupTemp,
                        RecoveryHierarchyRetryKind::Enumerate,
                        &[boot_entry.as_bytes()],
                        stats,
                        boot_dir_name,
                    ) {
                        return;
                    }
                    continue;
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
                let Some(shard_name) = shard_entry.as_ascii_str() else {
                    Self::record_error(
                        stats,
                        "temp_shard_name",
                        &raw_name_for_error(shard_entry),
                        "shard directory name is not ASCII",
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
                if shard >= self.format.shard_count() {
                    Self::record_error(
                        stats,
                        "temp_shard_name",
                        shard_name,
                        "shard directory is outside the queue shard range",
                    );
                    continue;
                }
                let shard_fd = match fs::open_directory(boot_dir_fd.as_fd(), shard_name) {
                    Ok(fd) => fd,
                    Err(error) => {
                        stats.scan_skips += 1;
                        Self::block_phase(
                            stats,
                            "temp_shard_open",
                            &format!("tmp/{boot_dir_name}/{shard_name}"),
                            &error.to_string(),
                        );
                        if !self.remember_hierarchy_retry_or_block(
                            RecoveryPhase::CleanupTemp,
                            RecoveryHierarchyRetryKind::Open,
                            &[boot_entry.as_bytes(), shard_entry.as_bytes()],
                            stats,
                            &format!("tmp/{boot_dir_name}/{shard_name}"),
                        ) {
                            return;
                        }
                        continue;
                    }
                };

                let mut entries = match read_recovery_directory(
                    shard_fd.as_fd(),
                    deadline_mono,
                    scan.budget,
                    scan.stats,
                ) {
                    Ok(e) => e,
                    Err(error) => {
                        stats.scan_skips += 1;
                        if Self::record_directory_error(
                            stats,
                            "temp_entry_read",
                            &format!("tmp/{boot_dir_name}/{shard_name}"),
                            &error,
                        ) {
                            return;
                        }
                        if !self.remember_hierarchy_retry_or_block(
                            RecoveryPhase::CleanupTemp,
                            RecoveryHierarchyRetryKind::Enumerate,
                            &[boot_entry.as_bytes(), shard_entry.as_bytes()],
                            stats,
                            &format!("tmp/{boot_dir_name}/{shard_name}"),
                        ) {
                            return;
                        }
                        continue;
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
                    if Self::work_budget_exhausted(stats, budget, deadline_mono) {
                        stats.budget_exhausted = true;
                        return;
                    }
                    self.recovery_cursor.cleanup_temp = Some(ThreeLevelCursor::new(
                        boot_entry.as_bytes(),
                        shard_entry.as_bytes(),
                        raw_entry.as_bytes(),
                    ));
                    let Some(entry) = raw_entry.as_ascii_str() else {
                        Self::record_error(
                            stats,
                            "temp_entry_name",
                            &raw_name_for_error(raw_entry),
                            "entry name is not ASCII",
                        );
                        continue;
                    };

                    if !entry.ends_with(".tmp") {
                        continue;
                    }

                    let should_delete = if !is_current_boot {
                        true
                    } else if let Ok(parsed) = steadq_names::parse_temp(entry) {
                        boottime_now.saturating_sub(parsed.created_boottime_ns)
                            > self.options.temporary_file_ttl_ns
                    } else {
                        false
                    };

                    if should_delete {
                        let relative_path = format!("tmp/{boot_dir_name}/{shard_name}/{entry}");
                        stats.operations_attempted += 1;
                        match unlink_verified(shard_fd.as_fd(), entry, MoveActor::Recovery) {
                            Ok(()) => stats.temp_files_deleted += 1,
                            Err(failure) => Self::record_unlink_failure(
                                stats,
                                "temp_delete",
                                &relative_path,
                                failure,
                            ),
                        }
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
        let scan_budget = RecoveryScanBudget::default();
        let mut scan_stats = RecoveryScanStats::default();
        let mut scan = RecoveryScanContext {
            budget: &scan_budget,
            stats: &mut scan_stats,
        };
        self.compact_receipts_with_scan_budget(budget, &mut scan, stats, deadline_mono);
    }

    fn compact_receipts_with_scan_budget(
        &mut self,
        budget: &WorkBudget,
        scan: &mut RecoveryScanContext<'_>,
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
        let hierarchy_retry = self.prepare_hierarchy_retry_phase(RecoveryPhase::CompactReceipts);
        if self.retry_one_hierarchy_directory(
            RecoveryPhase::CompactReceipts,
            hierarchy_retry,
            receipts_fd.as_fd(),
            scan,
            stats,
            deadline_mono,
        ) {
            return;
        }

        let mut bucket_dirs = match read_recovery_directory(
            receipts_fd.as_fd(),
            deadline_mono,
            scan.budget,
            scan.stats,
        ) {
            Ok(e) => e,
            Err(error) => {
                Self::record_directory_error(stats, "compact_bucket_read", "receipts", &error);
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

            if Self::work_budget_exhausted(stats, budget, deadline_mono) {
                stats.budget_exhausted = true;
                return;
            }
            let Some(bucket_name) = bucket_entry.as_ascii_str() else {
                Self::record_error(
                    stats,
                    "compact_bucket_name",
                    &raw_name_for_error(bucket_entry),
                    "bucket directory name is not ASCII",
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

            let bucket_fd = match fs::open_directory(receipts_fd.as_fd(), bucket_name) {
                Ok(fd) => fd,
                Err(error) => {
                    stats.scan_skips += 1;
                    Self::block_phase(
                        stats,
                        "compact_bucket_open",
                        &format!("receipts/{bucket_name}"),
                        &error.to_string(),
                    );
                    if !self.remember_hierarchy_retry_or_block(
                        RecoveryPhase::CompactReceipts,
                        RecoveryHierarchyRetryKind::Open,
                        &[bucket_entry.as_bytes()],
                        stats,
                        &format!("receipts/{bucket_name}"),
                    ) {
                        return;
                    }
                    continue;
                }
            };

            let mut shard_dirs = match read_recovery_directory(
                bucket_fd.as_fd(),
                deadline_mono,
                scan.budget,
                scan.stats,
            ) {
                Ok(e) => e,
                Err(error) => {
                    stats.scan_skips += 1;
                    if Self::record_directory_error(
                        stats,
                        "compact_shard_read",
                        &format!("receipts/{bucket_name}"),
                        &error,
                    ) {
                        return;
                    }
                    if !self.remember_hierarchy_retry_or_block(
                        RecoveryPhase::CompactReceipts,
                        RecoveryHierarchyRetryKind::Enumerate,
                        &[bucket_entry.as_bytes()],
                        stats,
                        &format!("receipts/{bucket_name}"),
                    ) {
                        return;
                    }
                    continue;
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
                let Some(shard_name) = shard_entry.as_ascii_str() else {
                    Self::record_error(
                        stats,
                        "compact_shard_name",
                        &raw_name_for_error(shard_entry),
                        "shard directory name is not ASCII",
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
                if shard >= self.format.shard_count() {
                    Self::record_error(
                        stats,
                        "compact_shard_name",
                        shard_name,
                        "shard directory is outside the queue shard range",
                    );
                    continue;
                }
                let shard_fd = match fs::open_directory(bucket_fd.as_fd(), shard_name) {
                    Ok(fd) => fd,
                    Err(error) => {
                        stats.scan_skips += 1;
                        Self::block_phase(
                            stats,
                            "compact_shard_open",
                            &format!("receipts/{bucket_name}/{shard_name}"),
                            &error.to_string(),
                        );
                        if !self.remember_hierarchy_retry_or_block(
                            RecoveryPhase::CompactReceipts,
                            RecoveryHierarchyRetryKind::Open,
                            &[bucket_entry.as_bytes(), shard_entry.as_bytes()],
                            stats,
                            &format!("receipts/{bucket_name}/{shard_name}"),
                        ) {
                            return;
                        }
                        continue;
                    }
                };

                let mut entries = match read_recovery_directory(
                    shard_fd.as_fd(),
                    deadline_mono,
                    scan.budget,
                    scan.stats,
                ) {
                    Ok(e) => e,
                    Err(error) => {
                        stats.scan_skips += 1;
                        if Self::record_directory_error(
                            stats,
                            "compact_entry_read",
                            &format!("receipts/{bucket_name}/{shard_name}"),
                            &error,
                        ) {
                            return;
                        }
                        if !self.remember_hierarchy_retry_or_block(
                            RecoveryPhase::CompactReceipts,
                            RecoveryHierarchyRetryKind::Enumerate,
                            &[bucket_entry.as_bytes(), shard_entry.as_bytes()],
                            stats,
                            &format!("receipts/{bucket_name}/{shard_name}"),
                        ) {
                            return;
                        }
                        continue;
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
                    if Self::work_budget_exhausted(stats, budget, deadline_mono) {
                        stats.budget_exhausted = true;
                        return;
                    }
                    self.recovery_cursor.compact_receipts = Some(ThreeLevelCursor::new(
                        bucket_entry.as_bytes(),
                        shard_entry.as_bytes(),
                        raw_entry.as_bytes(),
                    ));
                    let Some(entry) = raw_entry.as_ascii_str() else {
                        Self::record_error(
                            stats,
                            "compact_entry_name",
                            &raw_name_for_error(raw_entry),
                            "entry name is not ASCII",
                        );
                        continue;
                    };

                    if compaction_temporary_name(entry) {
                        let temp_path = format!("receipts/{bucket_name}/{shard_name}/{entry}");
                        stats.operations_attempted += 1;
                        if let Err(failure) =
                            unlink_verified(shard_fd.as_fd(), entry, MoveActor::Recovery)
                        {
                            Self::record_unlink_failure(
                                stats,
                                "receipt_compact_stale_temp_cleanup",
                                &temp_path,
                                failure,
                            );
                        }
                        continue;
                    }

                    if !entry.ends_with(".rct") {
                        continue;
                    }

                    // C-35: Open with write-capable mode for OFD write lock
                    let receipt_fd = match fs::openat(
                        shard_fd.as_fd(),
                        entry,
                        crate::queue::verified::receipt_write_open_flags(),
                        0,
                    ) {
                        Ok(fd) => fd,
                        Err(_) => continue,
                    };

                    if !fs::try_ofd_write_lock(receipt_fd.as_fd()).unwrap_or(false) {
                        continue; // busy, skip
                    }

                    let verified_receipt = match crate::queue::verified::verify_receipt_on_fd(
                        receipt_fd.as_fd(),
                        crate::queue::verified::ReceiptContext {
                            queue_id: self.format.queue_id(),
                            shard_count: self.format.shard_count(),
                            terminal_bucket_width_ns: self.format.terminal_bucket_width_ns(),
                            max_payload_length: self.format.max_payload_length(),
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
                        crate::queue::verified::VerifiedReceiptKind::Full(job) => {
                            job.header().clone()
                        }
                        crate::queue::verified::VerifiedReceiptKind::Compact => continue,
                    };
                    let bucket_start =
                        match bucket_number.checked_mul(self.format.terminal_bucket_width_ns()) {
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
                    let receipt_path = format!("receipts/{bucket_name}/{shard_name}/{entry}");

                    stats.operations_attempted += 1;
                    let random = match steadq_fs_linux::random_128bit() {
                        Ok(random) => random,
                        Err(error) => {
                            Self::record_error(
                                stats,
                                "receipt_compact_temp_name_not_committed",
                                &receipt_path,
                                &format!("phase=TempName: {error}"),
                            );
                            continue;
                        }
                    };

                    // Write to a temp file in the same directory
                    let tmp_name = format!(
                        ".compact-{}.tmp",
                        random
                            .iter()
                            .map(|b| format!("{b:02x}"))
                            .collect::<String>()
                    );

                    let temp_path = format!("receipts/{bucket_name}/{shard_name}/{tmp_name}");

                    let tmp_fd = match fs::create_exclusive(shard_fd.as_fd(), &tmp_name, 0o600) {
                        Ok(fd) => fd,
                        Err(error) => {
                            Self::record_error(
                                stats,
                                "receipt_compact_temp_create_not_committed",
                                &temp_path,
                                &format!("phase=TempCreate: {error}"),
                            );
                            continue;
                        }
                    };

                    if let Err(error) = fs::write_all(tmp_fd.as_fd(), &compact_bytes) {
                        Self::record_error(
                            stats,
                            "receipt_compact_temp_write_not_committed",
                            &temp_path,
                            &format!("phase=TempWrite: {error}"),
                        );
                        Self::cleanup_compaction_temp(
                            stats,
                            shard_fd.as_fd(),
                            &tmp_name,
                            &temp_path,
                        );
                        continue;
                    }
                    if let Err(error) = fs::fsync(tmp_fd.as_fd()) {
                        Self::record_error(
                            stats,
                            "receipt_compact_temp_fsync_not_committed",
                            &temp_path,
                            &format!("phase=TempFsync: {error}"),
                        );
                        Self::cleanup_compaction_temp(
                            stats,
                            shard_fd.as_fd(),
                            &tmp_name,
                            &temp_path,
                        );
                        continue;
                    }

                    // Replace the original with the compact version
                    match replace_verified(
                        shard_fd.as_fd(),
                        &tmp_name,
                        shard_fd.as_fd(),
                        entry,
                        Some(ReplaceIdentity::new(device, inode)),
                        MoveActor::Recovery,
                    ) {
                        Ok(()) => stats.receipts_compacted += 1,
                        Err(failure) => {
                            let source_missing = matches!(failure, ReplaceFailure::SourceMissing);
                            let outcome_unknown = failure.is_outcome_unknown();
                            Self::record_replace_failure(
                                stats,
                                "receipt_compact_replace",
                                &receipt_path,
                                failure,
                            );
                            if !outcome_unknown || source_missing {
                                Self::cleanup_compaction_temp(
                                    stats,
                                    shard_fd.as_fd(),
                                    &tmp_name,
                                    &temp_path,
                                );
                            }
                        }
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
        scan: &mut RecoveryScanContext<'_>,
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
        let hierarchy_retry = self.prepare_hierarchy_retry_phase(RecoveryPhase::DeleteReceipts);
        if self.retry_one_hierarchy_directory(
            RecoveryPhase::DeleteReceipts,
            hierarchy_retry,
            receipts_fd.as_fd(),
            scan,
            stats,
            deadline_mono,
        ) {
            return;
        }

        let mut bucket_dirs = match read_recovery_directory(
            receipts_fd.as_fd(),
            deadline_mono,
            scan.budget,
            scan.stats,
        ) {
            Ok(e) => e,
            Err(error) => {
                Self::record_directory_error(stats, "delete_bucket_read", "receipts", &error);
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

            if Self::work_budget_exhausted(stats, budget, deadline_mono) {
                stats.budget_exhausted = true;
                return;
            }
            let Some(bucket_name) = bucket_entry.as_ascii_str() else {
                Self::record_error(
                    stats,
                    "delete_bucket_name",
                    &raw_name_for_error(bucket_entry),
                    "bucket directory name is not ASCII",
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

            let bucket_start = match bucket_num.checked_mul(self.format.terminal_bucket_width_ns())
            {
                Some(s) => s,
                None => continue,
            };
            let bucket_end = match bucket_start.checked_add(self.format.terminal_bucket_width_ns())
            {
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

            let bucket_fd = match fs::open_directory(receipts_fd.as_fd(), bucket_name) {
                Ok(fd) => fd,
                Err(error) => {
                    stats.scan_skips += 1;
                    Self::block_phase(
                        stats,
                        "delete_bucket_open",
                        &format!("receipts/{bucket_name}"),
                        &error.to_string(),
                    );
                    if !self.remember_hierarchy_retry_or_block(
                        RecoveryPhase::DeleteReceipts,
                        RecoveryHierarchyRetryKind::Open,
                        &[bucket_entry.as_bytes()],
                        stats,
                        &format!("receipts/{bucket_name}"),
                    ) {
                        return;
                    }
                    continue;
                }
            };

            let mut shard_dirs = match read_recovery_directory(
                bucket_fd.as_fd(),
                deadline_mono,
                scan.budget,
                scan.stats,
            ) {
                Ok(e) => e,
                Err(error) => {
                    stats.scan_skips += 1;
                    if Self::record_directory_error(
                        stats,
                        "delete_shard_read",
                        &format!("receipts/{bucket_name}"),
                        &error,
                    ) {
                        return;
                    }
                    if !self.remember_hierarchy_retry_or_block(
                        RecoveryPhase::DeleteReceipts,
                        RecoveryHierarchyRetryKind::Enumerate,
                        &[bucket_entry.as_bytes()],
                        stats,
                        &format!("receipts/{bucket_name}"),
                    ) {
                        return;
                    }
                    continue;
                }
            };
            shard_dirs.sort();
            let mut absent_shards = 0usize;

            for shard_entry in &shard_dirs {
                // Entry level cursor: skip shards before cursor when bucket matches.
                if let Some(cursor) = &self.recovery_cursor.delete_receipts {
                    if bucket_entry.as_bytes() == cursor.first
                        && shard_entry.as_bytes() < cursor.second.as_slice()
                    {
                        continue;
                    }
                }
                let Some(shard_name) = shard_entry.as_ascii_str() else {
                    Self::record_error(
                        stats,
                        "delete_shard_name",
                        &raw_name_for_error(shard_entry),
                        "shard directory name is not ASCII",
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
                if shard >= self.format.shard_count() {
                    Self::record_error(
                        stats,
                        "delete_shard_name",
                        shard_name,
                        "shard directory is outside the queue shard range",
                    );
                    continue;
                }
                let shard_fd = match fs::open_directory(bucket_fd.as_fd(), shard_name) {
                    Ok(fd) => fd,
                    Err(error) => {
                        stats.scan_skips += 1;
                        Self::block_phase(
                            stats,
                            "delete_shard_open",
                            &format!("receipts/{bucket_name}/{shard_name}"),
                            &error.to_string(),
                        );
                        if !self.remember_hierarchy_retry_or_block(
                            RecoveryPhase::DeleteReceipts,
                            RecoveryHierarchyRetryKind::Open,
                            &[bucket_entry.as_bytes(), shard_entry.as_bytes()],
                            stats,
                            &format!("receipts/{bucket_name}/{shard_name}"),
                        ) {
                            return;
                        }
                        continue;
                    }
                };

                let mut entries = match read_recovery_directory(
                    shard_fd.as_fd(),
                    deadline_mono,
                    scan.budget,
                    scan.stats,
                ) {
                    Ok(e) => e,
                    Err(error) => {
                        stats.scan_skips += 1;
                        if Self::record_directory_error(
                            stats,
                            "delete_entry_read",
                            &format!("receipts/{bucket_name}/{shard_name}"),
                            &error,
                        ) {
                            return;
                        }
                        if !self.remember_hierarchy_retry_or_block(
                            RecoveryPhase::DeleteReceipts,
                            RecoveryHierarchyRetryKind::Enumerate,
                            &[bucket_entry.as_bytes(), shard_entry.as_bytes()],
                            stats,
                            &format!("receipts/{bucket_name}/{shard_name}"),
                        ) {
                            return;
                        }
                        continue;
                    }
                };
                entries.sort();
                let mut absent_entries = 0usize;

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
                    if Self::work_budget_exhausted(stats, budget, deadline_mono) {
                        stats.budget_exhausted = true;
                        return;
                    }
                    self.recovery_cursor.delete_receipts = Some(ThreeLevelCursor::new(
                        bucket_entry.as_bytes(),
                        shard_entry.as_bytes(),
                        raw_entry.as_bytes(),
                    ));
                    let Some(entry) = raw_entry.as_ascii_str() else {
                        Self::record_error(
                            stats,
                            "delete_entry_name",
                            &raw_name_for_error(raw_entry),
                            "entry name is not ASCII",
                        );
                        continue;
                    };
                    // R4-H08: Only process receipt files.
                    if !entry.ends_with(".rct") {
                        continue;
                    }
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
                        shard_fd.as_fd(),
                        entry,
                        crate::queue::verified::receipt_write_open_flags(),
                        0,
                    ) {
                        Ok(fd) => fd,
                        Err(_) => continue,
                    };

                    if !fs::try_ofd_write_lock(receipt_fd.as_fd()).unwrap_or(false) {
                        continue;
                    }

                    let verified_receipt = match crate::queue::verified::verify_receipt_on_fd(
                        receipt_fd.as_fd(),
                        crate::queue::verified::ReceiptContext {
                            queue_id: self.format.queue_id(),
                            shard_count: self.format.shard_count(),
                            terminal_bucket_width_ns: self.format.terminal_bucket_width_ns(),
                            max_payload_length: self.format.max_payload_length(),
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
                    let current = match fs::fstatat(shard_fd.as_fd(), entry) {
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

                    stats.operations_attempted += 1;
                    let relative_path = format!("receipts/{bucket_name}/{shard_name}/{entry}");
                    match unlink_verified(shard_fd.as_fd(), entry, MoveActor::Recovery) {
                        Ok(()) => {
                            stats.receipts_expired += 1;
                            absent_entries += 1;
                        }
                        Err(UnlinkFailure::SourceMissing) => {
                            absent_entries += 1;
                            Self::record_unlink_failure(
                                stats,
                                "receipt_delete",
                                &relative_path,
                                UnlinkFailure::SourceMissing,
                            );
                        }
                        Err(failure) => Self::record_unlink_failure(
                            stats,
                            "receipt_delete",
                            &relative_path,
                            failure,
                        ),
                    }
                }

                if !all_observed_children_absent(absent_entries, entries.len()) {
                    continue;
                }
                if Self::work_budget_exhausted(stats, budget, deadline_mono) {
                    stats.budget_exhausted = true;
                    return;
                }
                stats.operations_attempted += 1;
                let shard_path = format!("receipts/{bucket_name}/{shard_name}");
                match remove_empty_directory_verified(
                    bucket_fd.as_fd(),
                    shard_name,
                    MoveActor::Recovery,
                ) {
                    Ok(()) => {
                        stats.shards_removed += 1;
                        absent_shards += 1;
                    }
                    Err(RemoveDirectoryFailure::SourceMissing) => absent_shards += 1,
                    Err(RemoveDirectoryFailure::NotEmpty) => {}
                    Err(failure) => Self::record_remove_directory_failure(
                        stats,
                        "receipt_shard_remove",
                        &shard_path,
                        failure,
                    ),
                }
            }

            if !all_observed_children_absent(absent_shards, shard_dirs.len()) {
                continue;
            }
            if Self::work_budget_exhausted(stats, budget, deadline_mono) {
                stats.budget_exhausted = true;
                return;
            }
            stats.operations_attempted += 1;
            let bucket_path = format!("receipts/{bucket_name}");
            match remove_empty_directory_verified(
                receipts_fd.as_fd(),
                bucket_name,
                MoveActor::Recovery,
            ) {
                Ok(()) => stats.buckets_removed += 1,
                Err(RemoveDirectoryFailure::SourceMissing | RemoveDirectoryFailure::NotEmpty) => {}
                Err(failure) => Self::record_remove_directory_failure(
                    stats,
                    "receipt_bucket_remove",
                    &bucket_path,
                    failure,
                ),
            }
        }

        // R4-RES: All buckets processed, reset cursor for next full pass.
        self.recovery_cursor.delete_receipts = None;
    }
}

#[cfg(test)]
mod tests;
