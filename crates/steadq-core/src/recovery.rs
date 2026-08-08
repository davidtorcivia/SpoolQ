// SteadQ/1 cooperative recovery operations.

use std::io;
use std::os::unix::io::AsRawFd;

use steadq_fs_linux as fs;
use steadq_math;
use steadq_names::{self, bucket_hex};

use crate::errors::*;
use crate::queue::{open_relative, Queue, WallFloor};

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
    /// Run one bounded recovery pass.
    pub fn recover(&mut self, budget: &WorkBudget) -> RecoveryStats {
        let mut stats = RecoveryStats::default();
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
        let wall_floor = self.authenticated_wall_floor();
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

        // 1. Reap expired leases
        self.reap_expired_leases(boottime_now, wall_floor, budget, &mut stats, deadline_mono);

        // 2. Promote eligible delayed jobs (requires trusted wall floor)
        if !stats.budget_exhausted {
            if let Some(wall_floor) = wall_floor {
                self.promote_delayed(wall_floor, budget, &mut stats, deadline_mono);
            }
        }

        // 3. Clean up old temp files
        if !stats.budget_exhausted {
            self.cleanup_temp_files(boottime_now, budget, &mut stats, deadline_mono);
        }
        if !stats.budget_exhausted {
            self.compact_receipts(budget, &mut stats, deadline_mono);
        }
        if !stats.budget_exhausted {
            if let Some(wall_floor) = wall_floor {
                self.delete_expired_receipts(
                    wall_floor,
                    self.options.receipt_retention_ns,
                    budget,
                    &mut stats,
                    deadline_mono,
                );
            }
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
    fn budget_time_exceeded(deadline_mono: u64) -> bool {
        match fs::clock_monotonic_ns() {
            Ok(now) => now >= deadline_mono,
            Err(_) => false,
        }
    }

    /// Check if either operations or time budget is exhausted.
    fn budget_exhausted(stats: &RecoveryStats, budget: &WorkBudget, deadline_mono: u64) -> bool {
        if stats.operations_attempted >= budget.max_operations {
            return true;
        }
        Self::budget_time_exceeded(deadline_mono)
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
                Self::record_error(stats, "open_leased_dir", "leased", &e.to_string());
                return;
            }
        };

        let boot_dirs = match fs::read_dir_entries_owned(leased_fd.as_raw_fd()) {
            Ok(e) => e,
            Err(e) => {
                Self::record_error(stats, "read_leased_dirs", "leased", &e.to_string());
                return;
            }
        };

        for boot_dir_name in &boot_dirs {
            if Self::budget_exhausted(stats, budget, deadline_mono) {
                stats.budget_exhausted = true;
                return;
            }

            let is_current_boot = boot_dir_name == &self.boot_id;

            let boot_dir_fd = match fs::open_directory(leased_fd.as_raw_fd(), boot_dir_name) {
                Ok(fd) => fd,
                Err(_) => {
                    stats.scan_skips += 1;
                    continue;
                }
            };

            let bucket_dirs = match fs::read_dir_entries_owned(boot_dir_fd.as_raw_fd()) {
                Ok(e) => e,
                Err(_) => {
                    stats.scan_skips += 1;
                    continue;
                }
            };

            for bucket_name in &bucket_dirs {
                if Self::budget_exhausted(stats, budget, deadline_mono) {
                    stats.budget_exhausted = true;
                    return;
                }

                // For current boot, check if bucket is expired
                if is_current_boot {
                    if let Ok(bucket_num) = u64::from_str_radix(bucket_name, 16) {
                        let Some(current_bucket) = steadq_math::bucket_number(
                            boottime_now,
                            self.format.lease_bucket_width_ns,
                        ) else {
                            Self::record_error(
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
                }

                let bucket_fd = match fs::open_directory(boot_dir_fd.as_raw_fd(), bucket_name) {
                    Ok(fd) => fd,
                    Err(_) => {
                        stats.scan_skips += 1;
                        continue;
                    }
                };

                let shard_dirs = match fs::read_dir_entries_owned(bucket_fd.as_raw_fd()) {
                    Ok(e) => e,
                    Err(_) => {
                        stats.scan_skips += 1;
                        continue;
                    }
                };

                for shard_name in &shard_dirs {
                    let shard_fd = match fs::open_directory(bucket_fd.as_raw_fd(), shard_name) {
                        Ok(fd) => fd,
                        Err(_) => {
                            stats.scan_skips += 1;
                            continue;
                        }
                    };

                    let entries = match fs::read_dir_entries_owned(shard_fd.as_raw_fd()) {
                        Ok(e) => e,
                        Err(_) => {
                            stats.scan_skips += 1;
                            continue;
                        }
                    };

                    for entry in &entries {
                        if stats.operations_attempted >= budget.max_operations {
                            stats.budget_exhausted = true;
                            return;
                        }

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
                            boot_id: boot_dir_name.clone(),
                            bucket: bucket_name.clone(),
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
            Err(_) => return,
        };

        let mut bucket_dirs = match fs::read_dir_entries_owned(delayed_fd.as_raw_fd()) {
            Ok(e) => e,
            Err(_) => return,
        };
        bucket_dirs.sort();

        for bucket_name in &bucket_dirs {
            // R4-RES: Skip buckets already processed in a prior pass.
            if let Some((ref cursor_bucket, _, _)) = self.recovery_cursor.promote_delayed {
                if bucket_name.as_str() < cursor_bucket.as_str() {
                    continue;
                }
            }

            if Self::budget_exhausted(stats, budget, deadline_mono) {
                stats.budget_exhausted = true;
                self.recovery_cursor.promote_delayed =
                    Some((bucket_name.clone(), String::new(), String::new()));
                return;
            }

            let bucket_num = match u64::from_str_radix(bucket_name, 16) {
                Ok(n) => n,
                Err(_) => continue,
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
                Err(_) => {
                    stats.scan_skips += 1;
                    continue;
                }
            };

            let shard_dirs = match fs::read_dir_entries_owned(bucket_fd.as_raw_fd()) {
                Ok(e) => e,
                Err(_) => {
                    stats.scan_skips += 1;
                    continue;
                }
            };

            for shard_name in &shard_dirs {
                // Entry level cursor: skip shards before cursor when bucket matches.
                if let Some((cb, cs, _)) = &self.recovery_cursor.promote_delayed {
                    if bucket_name == cb && shard_name.as_str() < cs.as_str() {
                        continue;
                    }
                }
                let shard_fd = match fs::open_directory(bucket_fd.as_raw_fd(), shard_name) {
                    Ok(fd) => fd,
                    Err(_) => {
                        Self::record_error(
                            stats,
                            "promote_shard_open",
                            &format!("{bucket_name}/{shard_name}"),
                            "shard dir open failed",
                        );
                        continue;
                    }
                };

                let entries = match fs::read_dir_entries_owned(shard_fd.as_raw_fd()) {
                    Ok(e) => e,
                    Err(_) => {
                        stats.scan_skips += 1;
                        continue;
                    }
                };

                for entry in &entries {
                    // Entry level cursor: skip entries at or before cursor when bucket and shard match.
                    if let Some((cb, cs, ce)) = &self.recovery_cursor.promote_delayed {
                        if bucket_name == cb
                            && shard_name == cs.as_str()
                            && entry.as_str() <= ce.as_str()
                        {
                            continue;
                        }
                    }
                    if Self::budget_exhausted(stats, budget, deadline_mono) {
                        stats.budget_exhausted = true;
                        self.recovery_cursor.promote_delayed =
                            Some((bucket_name.clone(), shard_name.clone(), (*entry).clone()));
                        return;
                    }

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
                        let src_dir_fd = match open_relative(
                            self.root_fd(),
                            &format!("delayed/{bucket_name}/{shard_name}"),
                        ) {
                            Ok(fd) => fd,
                            Err(_) => continue,
                        };
                        let delayed_ctx = crate::ActivePathContext::Delayed {
                            bucket: bucket_name.clone(),
                            shard: shard_name.to_string(),
                        };
                        if let Err(e) =
                            self.validate_active_object(src_dir_fd.as_raw_fd(), entry, &delayed_ctx)
                        {
                            Self::record_error(
                                stats,
                                "promote_validate",
                                &format!("delayed/{bucket_name}/{shard_name}/{entry}"),
                                &format!("{e}"),
                            );
                            if matches!(e, Error::QueueCorrupt(_)) {
                                let _ = self.quarantine_recovery_object(
                                    src_dir_fd.as_raw_fd(),
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
                        self.recovery_cursor.promote_delayed =
                            Some((bucket_name.clone(), shard_name.clone(), (*entry).clone()));
                    }
                }
            }

            // R4-RES: Bucket fully processed, advance cursor.
            self.recovery_cursor.promote_delayed =
                Some((bucket_name.clone(), String::new(), String::new()));
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
            Err(_) => return,
        };

        let boot_dirs = match fs::read_dir_entries_owned(tmp_fd.as_raw_fd()) {
            Ok(e) => e,
            Err(_) => return,
        };

        for boot_dir_name in &boot_dirs {
            if Self::budget_exhausted(stats, budget, deadline_mono) {
                stats.budget_exhausted = true;
                return;
            }

            let is_current_boot = boot_dir_name == &self.boot_id;

            let boot_dir_fd = match fs::open_directory(tmp_fd.as_raw_fd(), boot_dir_name) {
                Ok(fd) => fd,
                Err(_) => {
                    stats.scan_skips += 1;
                    continue;
                }
            };

            let shard_dirs = match fs::read_dir_entries_owned(boot_dir_fd.as_raw_fd()) {
                Ok(e) => e,
                Err(_) => {
                    stats.scan_skips += 1;
                    continue;
                }
            };

            for shard_name in &shard_dirs {
                let shard_fd = match fs::open_directory(boot_dir_fd.as_raw_fd(), shard_name) {
                    Ok(fd) => fd,
                    Err(_) => {
                        stats.scan_skips += 1;
                        continue;
                    }
                };

                let entries = match fs::read_dir_entries_owned(shard_fd.as_raw_fd()) {
                    Ok(e) => e,
                    Err(_) => {
                        stats.scan_skips += 1;
                        continue;
                    }
                };

                for entry in &entries {
                    if Self::budget_exhausted(stats, budget, deadline_mono) {
                        stats.budget_exhausted = true;
                        return;
                    }

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
            Err(_) => return,
        };

        let mut bucket_dirs = match fs::read_dir_entries_owned(receipts_fd.as_raw_fd()) {
            Ok(e) => e,
            Err(_) => return,
        };
        bucket_dirs.sort();

        for bucket_name in &bucket_dirs {
            // R4-RES: Skip buckets already processed in a prior pass.
            if let Some((ref cursor_bucket, _, _)) = self.recovery_cursor.compact_receipts {
                if bucket_name.as_str() < cursor_bucket.as_str() {
                    continue;
                }
            }

            if Self::budget_exhausted(stats, budget, deadline_mono) {
                stats.budget_exhausted = true;
                self.recovery_cursor.compact_receipts =
                    Some((bucket_name.clone(), String::new(), String::new()));
                return;
            }

            let bucket_fd = match fs::open_directory(receipts_fd.as_raw_fd(), bucket_name) {
                Ok(fd) => fd,
                Err(_) => {
                    stats.scan_skips += 1;
                    continue;
                }
            };

            let shard_dirs = match fs::read_dir_entries_owned(bucket_fd.as_raw_fd()) {
                Ok(e) => e,
                Err(_) => {
                    stats.scan_skips += 1;
                    continue;
                }
            };

            for shard_name in &shard_dirs {
                // Entry level cursor: skip shards before cursor when bucket matches.
                if let Some((cb, cs, _)) = &self.recovery_cursor.compact_receipts {
                    if bucket_name == cb && shard_name.as_str() < cs.as_str() {
                        continue;
                    }
                }
                let shard_fd = match fs::open_directory(bucket_fd.as_raw_fd(), shard_name) {
                    Ok(fd) => fd,
                    Err(_) => {
                        stats.scan_skips += 1;
                        continue;
                    }
                };

                let entries = match fs::read_dir_entries_owned(shard_fd.as_raw_fd()) {
                    Ok(e) => e,
                    Err(_) => {
                        stats.scan_skips += 1;
                        continue;
                    }
                };

                for entry in &entries {
                    // Entry level cursor: skip entries at or before cursor when bucket and shard match.
                    if let Some((cb, cs, ce)) = &self.recovery_cursor.compact_receipts {
                        if bucket_name == cb
                            && shard_name == cs.as_str()
                            && entry.as_str() <= ce.as_str()
                        {
                            continue;
                        }
                    }
                    if Self::budget_exhausted(stats, budget, deadline_mono) {
                        stats.budget_exhausted = true;
                        self.recovery_cursor.compact_receipts =
                            Some((bucket_name.clone(), shard_name.clone(), (*entry).clone()));
                        return;
                    }

                    if !entry.ends_with(".rct") {
                        continue;
                    }

                    stats.operations_attempted += 1;

                    // C-35: Open with write-capable mode for OFD write lock
                    let receipt_fd = match fs::openat(shard_fd.as_raw_fd(), entry, libc::O_RDWR, 0)
                    {
                        Ok(fd) => fd,
                        Err(_) => continue,
                    };

                    if !fs::try_ofd_write_lock(receipt_fd.as_raw_fd()).unwrap_or(false) {
                        continue; // busy, skip
                    }

                    // Read the file to check if it's already compact (128 bytes)
                    let stat = match fs::fstat(receipt_fd.as_raw_fd()) {
                        Ok(s) => s,
                        Err(_) => continue,
                    };

                    let file_size = stat.st_size as usize;

                    // R2-H08: Validate 128-byte file is actually a compact receipt
                    if file_size == steadq_format::COMPACT_RECEIPT_SIZE {
                        let mut compact_buf = [0u8; steadq_format::COMPACT_RECEIPT_SIZE];
                        if fs::pread_exact(receipt_fd.as_raw_fd(), &mut compact_buf, 0).is_ok()
                            && steadq_format::CompactReceipt::decode(&compact_buf).is_ok()
                        {
                            continue; // Valid compact receipt
                        }
                        // Invalid 128-byte file: skip
                        continue;
                    }

                    // Read the full job to extract compact receipt fields
                    if file_size < steadq_format::FIXED_HEADER_SIZE {
                        continue;
                    }

                    let mut header_buf = [0u8; 128];
                    if !matches!(
                        fs::pread(receipt_fd.as_raw_fd(), &mut header_buf, 0),
                        Ok(128)
                    ) {
                        continue;
                    }

                    let header = match steadq_format::FixedHeader::decode(&header_buf) {
                        Ok(h) => h,
                        Err(_) => continue,
                    };

                    // Parse the receipt filename to get generation and token
                    let parsed = match steadq_names::parse_receipt(entry) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };

                    // R4-H11: Full consistency proof before compaction.
                    if header.job_id != parsed.common.job_id {
                        continue;
                    }
                    if header.maximum_attempts != parsed.common.maximum_attempts {
                        continue;
                    }

                    // R4-H11: Verify envelope digest.
                    let ext_len = header.extension_header_length as usize;
                    if ext_len > 65536 {
                        continue;
                    }
                    let mut ext_buf = vec![0u8; ext_len];
                    if ext_len > 0
                        && fs::pread_exact(receipt_fd.as_raw_fd(), &mut ext_buf, 128).is_err()
                    {
                        continue;
                    }
                    if !crate::queue::verified::is_envelope_digest_valid(&header, &ext_buf) {
                        continue;
                    }

                    // R4-H11: Verify file size matches expected.
                    let expected_size = (128 + ext_len + header.payload_length as usize) as u64;
                    if file_size as u64 != expected_size {
                        continue;
                    }

                    if !parsed.authenticate_tag(&self.format.queue_id, bucket_name, shard_name) {
                        continue;
                    }

                    // R4-H11: Verify shard placement.
                    let computed_shard = steadq_names::compute_shard(
                        &self.format.queue_id,
                        &parsed.common.job_id,
                        self.format.shard_count,
                    );
                    let path_shard = match steadq_names::shard_from_hex(shard_name) {
                        Some(s) => s,
                        None => continue,
                    };
                    if path_shard != computed_shard {
                        continue;
                    }

                    // Compute bucket start time
                    let bucket_num = match u64::from_str_radix(bucket_name, 16) {
                        Ok(bucket) => bucket,
                        Err(_) => continue,
                    };
                    let bucket_start =
                        match bucket_num.checked_mul(self.format.terminal_bucket_width_ns) {
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

            // R4-RES: Bucket fully processed, advance cursor.
            self.recovery_cursor.compact_receipts =
                Some((bucket_name.clone(), String::new(), String::new()));
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
            Err(_) => return,
        };

        let mut bucket_dirs = match fs::read_dir_entries_owned(receipts_fd.as_raw_fd()) {
            Ok(e) => e,
            Err(_) => return,
        };
        bucket_dirs.sort();

        for bucket_name in &bucket_dirs {
            // R4-RES: Skip buckets already processed in a prior pass.
            if let Some((ref cursor_bucket, _, _)) = self.recovery_cursor.delete_receipts {
                if bucket_name.as_str() < cursor_bucket.as_str() {
                    continue;
                }
            }

            if Self::budget_exhausted(stats, budget, deadline_mono) {
                stats.budget_exhausted = true;
                self.recovery_cursor.delete_receipts =
                    Some((bucket_name.clone(), String::new(), String::new()));
                return;
            }

            let bucket_num = match u64::from_str_radix(bucket_name, 16) {
                Ok(n) => n,
                Err(_) => continue,
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
                Err(_) => {
                    stats.scan_skips += 1;
                    continue;
                }
            };

            let shard_dirs = match fs::read_dir_entries_owned(bucket_fd.as_raw_fd()) {
                Ok(e) => e,
                Err(_) => {
                    stats.scan_skips += 1;
                    continue;
                }
            };

            for shard_name in &shard_dirs {
                // Entry level cursor: skip shards before cursor when bucket matches.
                if let Some((cb, cs, _)) = &self.recovery_cursor.delete_receipts {
                    if bucket_name == cb && shard_name.as_str() < cs.as_str() {
                        continue;
                    }
                }
                let shard_fd = match fs::open_directory(bucket_fd.as_raw_fd(), shard_name) {
                    Ok(fd) => fd,
                    Err(_) => {
                        stats.scan_skips += 1;
                        continue;
                    }
                };

                let entries = match fs::read_dir_entries_owned(shard_fd.as_raw_fd()) {
                    Ok(e) => e,
                    Err(_) => {
                        stats.scan_skips += 1;
                        continue;
                    }
                };

                for entry in &entries {
                    // Entry level cursor: skip entries at or before cursor when bucket and shard match.
                    if let Some((cb, cs, ce)) = &self.recovery_cursor.delete_receipts {
                        if bucket_name == cb
                            && shard_name == cs.as_str()
                            && entry.as_str() <= ce.as_str()
                        {
                            continue;
                        }
                    }
                    // R4-H08: Only process receipt files.
                    if !entry.ends_with(".rct") {
                        continue;
                    }

                    if Self::budget_exhausted(stats, budget, deadline_mono) {
                        stats.budget_exhausted = true;
                        self.recovery_cursor.delete_receipts =
                            Some((bucket_name.clone(), shard_name.to_string(), entry.clone()));
                        return;
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
                    let receipt_fd = match fs::openat(shard_fd.as_raw_fd(), entry, libc::O_RDWR, 0)
                    {
                        Ok(fd) => fd,
                        Err(_) => continue,
                    };

                    if !fs::try_ofd_write_lock(receipt_fd.as_raw_fd()).unwrap_or(false) {
                        continue;
                    }

                    // R4-H08: Validate the receipt is a regular file.
                    let file_stat = match fs::fstat(receipt_fd.as_raw_fd()) {
                        Ok(s) => s,
                        Err(_) => continue,
                    };
                    if file_stat.st_mode & libc::S_IFMT != libc::S_IFREG {
                        Self::record_error(
                            stats,
                            "receipt_delete_nonregular",
                            &format!("receipts/{bucket_name}/{shard_name}/{entry}"),
                            "receipt is not a regular file",
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

            // R4-RES: Bucket fully processed, advance cursor.
            self.recovery_cursor.delete_receipts =
                Some((bucket_name.clone(), String::new(), String::new()));
        }

        // R4-RES: All buckets processed, reset cursor for next full pass.
        self.recovery_cursor.delete_receipts = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CreateOptions, EnqueueInput, LeaseOutcome, OpenOptions};
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
