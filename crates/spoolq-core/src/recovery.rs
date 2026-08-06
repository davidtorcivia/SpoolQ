// SpoolQ/1 cooperative recovery operations.

use std::io;
use std::os::unix::io::AsRawFd;

use spoolq_fs_linux as fs;
use spoolq_math;
use spoolq_names::{self, bucket_hex, compute_name_tag, ready_context};

use crate::errors::*;
use crate::queue::{open_relative, Queue};

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
    pub budget_exhausted: bool,
    pub errors: Vec<RecoveryError>,
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
        let boottime_now = fs::clock_boottime_ns().unwrap_or(0);
        let wall_now = fs::clock_realtime_ns().unwrap_or(0);

        // 1. Reap expired leases
        self.reap_expired_leases(boottime_now, wall_now, budget, &mut stats);

        // 2. Promote eligible delayed jobs
        if !stats.budget_exhausted {
            self.promote_delayed(wall_now, budget, &mut stats);
        }

        // 3. Clean up old temp files
        if !stats.budget_exhausted {
            self.cleanup_temp_files(boottime_now, budget, &mut stats);
        }

        stats
    }

    fn reap_expired_leases(
        &mut self,
        boottime_now: u64,
        wall_now: u64,
        budget: &WorkBudget,
        stats: &mut RecoveryStats,
    ) {
        let root_fd = self.root_fd();

        // Scan leased/ directories
        let leased_fd = match fs::open_directory(root_fd, "leased") {
            Ok(fd) => fd,
            Err(_) => return,
        };

        let boot_dirs = match fs::read_dir_entries_owned(leased_fd.as_raw_fd()) {
            Ok(e) => e,
            Err(_) => return,
        };

        for boot_dir_name in &boot_dirs {
            if stats.operations_attempted >= budget.max_operations {
                stats.budget_exhausted = true;
                return;
            }

            let is_current_boot = boot_dir_name == &self.boot_id;

            let boot_dir_fd = match fs::open_directory(leased_fd.as_raw_fd(), boot_dir_name) {
                Ok(fd) => fd,
                Err(_) => continue,
            };

            let bucket_dirs = match fs::read_dir_entries_owned(boot_dir_fd.as_raw_fd()) {
                Ok(e) => e,
                Err(_) => continue,
            };

            for bucket_name in &bucket_dirs {
                if stats.operations_attempted >= budget.max_operations {
                    stats.budget_exhausted = true;
                    return;
                }

                // For current boot, check if bucket is expired
                if is_current_boot {
                    if let Ok(bucket_num) = u64::from_str_radix(bucket_name, 16) {
                        let current_bucket = spoolq_math::bucket_number(
                            boottime_now,
                            self.format.lease_bucket_width_ns,
                        );
                        if bucket_num > current_bucket {
                            continue; // Not yet eligible
                        }
                    }
                }

                let bucket_fd = match fs::open_directory(boot_dir_fd.as_raw_fd(), bucket_name) {
                    Ok(fd) => fd,
                    Err(_) => continue,
                };

                let shard_dirs = match fs::read_dir_entries_owned(bucket_fd.as_raw_fd()) {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                for shard_name in &shard_dirs {
                    let shard_fd = match fs::open_directory(bucket_fd.as_raw_fd(), shard_name) {
                        Ok(fd) => fd,
                        Err(_) => continue,
                    };

                    let entries = match fs::read_dir_entries_owned(shard_fd.as_raw_fd()) {
                        Ok(e) => e,
                        Err(_) => continue,
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
                        let parsed = match spoolq_names::parse_leased(entry) {
                            Ok(p) => p,
                            Err(_) => continue,
                        };

                        // For current boot, check actual deadline
                        if is_current_boot && parsed.boottime_deadline_ns > boottime_now {
                            continue;
                        }

                        // Determine destination: ready or dead
                        if parsed.common.attempt >= parsed.common.maximum_attempts {
                            // Move to dead
                            if self
                                .reap_to_dead(
                                    boot_dir_name,
                                    bucket_name,
                                    shard_name,
                                    entry,
                                    &parsed.common,
                                    DeadReason::AttemptsExhausted,
                                    wall_now,
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
        common: &spoolq_names::CommonFields,
    ) -> io::Result<()> {
        let src_dir = format!("leased/{}/{}/{}", boot_dir, bucket, shard);
        let dest_dir = format!("ready/{}", shard);

        let new_gen = common.generation.wrapping_add(1);
        let ready_common = spoolq_names::CommonFields {
            job_id: common.job_id,
            generation: new_gen,
            attempt: common.attempt,
            maximum_attempts: common.maximum_attempts,
        };

        let base = format!(
            "{}.g{:016x}.a{:08x}.m{:08x}",
            spoolq_names::hex_encode(&ready_common.job_id),
            new_gen,
            ready_common.attempt,
            ready_common.maximum_attempts,
        );
        let ctx = ready_context(shard, &base);
        let tag = compute_name_tag(&self.format.queue_id, &ctx);
        let ready_name = spoolq_names::ready_filename(&ready_common, &tag);

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
        common: &spoolq_names::CommonFields,
        reason: DeadReason,
        wall_now: u64,
    ) -> io::Result<()> {
        let src_dir = format!("leased/{}/{}/{}", boot_dir, bucket, shard);
        let terminal_bucket =
            spoolq_math::bucket_number(wall_now, self.format.terminal_bucket_width_ns);
        let bucket_str = bucket_hex(terminal_bucket);
        let dest_dir = format!("dead/{}/{}", bucket_str, shard);

        let new_gen = common.generation.wrapping_add(1);
        let dead_common = spoolq_names::CommonFields {
            job_id: common.job_id,
            generation: new_gen,
            attempt: common.attempt,
            maximum_attempts: common.maximum_attempts,
        };

        let base = format!(
            "{}.g{:016x}.a{:08x}.m{:08x}.x{:04x}",
            spoolq_names::hex_encode(&dead_common.job_id),
            new_gen,
            dead_common.attempt,
            dead_common.maximum_attempts,
            reason as u16,
        );
        let ctx =
            spoolq_names::terminal_context(spoolq_names::State::Dead, &bucket_str, shard, &base);
        let tag = compute_name_tag(&self.format.queue_id, &ctx);
        let dead_name = spoolq_names::dead_filename(&dead_common, reason as u16, &tag);

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

    fn promote_delayed(&mut self, wall_now: u64, budget: &WorkBudget, stats: &mut RecoveryStats) {
        let root_fd = self.root_fd();
        let delayed_fd = match fs::open_directory(root_fd, "delayed") {
            Ok(fd) => fd,
            Err(_) => return,
        };

        let bucket_dirs = match fs::read_dir_entries_owned(delayed_fd.as_raw_fd()) {
            Ok(e) => e,
            Err(_) => return,
        };

        for bucket_name in &bucket_dirs {
            if stats.operations_attempted >= budget.max_operations {
                stats.budget_exhausted = true;
                return;
            }

            let bucket_num = match u64::from_str_radix(bucket_name, 16) {
                Ok(n) => n,
                Err(_) => continue,
            };

            // Read effective wall floor
            let current_wall_bucket =
                spoolq_math::bucket_number(wall_now, self.format.delayed_bucket_width_ns);

            // Only promote buckets at or below the current wall bucket
            if bucket_num > current_wall_bucket {
                continue;
            }

            let bucket_fd = match fs::open_directory(delayed_fd.as_raw_fd(), bucket_name) {
                Ok(fd) => fd,
                Err(_) => continue,
            };

            let shard_dirs = match fs::read_dir_entries_owned(bucket_fd.as_raw_fd()) {
                Ok(e) => e,
                Err(_) => continue,
            };

            for shard_name in &shard_dirs {
                let shard_fd = match fs::open_directory(bucket_fd.as_raw_fd(), shard_name) {
                    Ok(fd) => fd,
                    Err(_) => continue,
                };

                let entries = match fs::read_dir_entries_owned(shard_fd.as_raw_fd()) {
                    Ok(e) => e,
                    Err(_) => continue,
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

                    let parsed = match spoolq_names::parse_delayed(entry) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };

                    // Promote: move delayed -> ready
                    let src_dir = format!("delayed/{}/{}", bucket_name, shard_name);
                    let dest_dir = format!("ready/{}", shard_name);

                    let new_gen = parsed.common.generation.wrapping_add(1);
                    let ready_common = spoolq_names::CommonFields {
                        job_id: parsed.common.job_id,
                        generation: new_gen,
                        attempt: parsed.common.attempt,
                        maximum_attempts: parsed.common.maximum_attempts,
                    };

                    let base = format!(
                        "{}.g{:016x}.a{:08x}.m{:08x}",
                        spoolq_names::hex_encode(&ready_common.job_id),
                        new_gen,
                        ready_common.attempt,
                        ready_common.maximum_attempts,
                    );
                    let ctx = ready_context(shard_name, &base);
                    let tag = compute_name_tag(&self.format.queue_id, &ctx);
                    let ready_name = spoolq_names::ready_filename(&ready_common, &tag);

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
                        let _ = fs::fsync_dir_fd(dest_fd.as_raw_fd());
                        let _ = fs::fsync_dir_fd(src_fd.as_raw_fd());
                        stats.delayed_promoted += 1;
                    }
                }
            }
        }
    }

    fn cleanup_temp_files(
        &mut self,
        boottime_now: u64,
        budget: &WorkBudget,
        stats: &mut RecoveryStats,
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
            if stats.operations_attempted >= budget.max_operations {
                stats.budget_exhausted = true;
                return;
            }

            let is_current_boot = boot_dir_name == &self.boot_id;

            let boot_dir_fd = match fs::open_directory(tmp_fd.as_raw_fd(), boot_dir_name) {
                Ok(fd) => fd,
                Err(_) => continue,
            };

            let shard_dirs = match fs::read_dir_entries_owned(boot_dir_fd.as_raw_fd()) {
                Ok(e) => e,
                Err(_) => continue,
            };

            for shard_name in &shard_dirs {
                let shard_fd = match fs::open_directory(boot_dir_fd.as_raw_fd(), shard_name) {
                    Ok(fd) => fd,
                    Err(_) => continue,
                };

                let entries = match fs::read_dir_entries_owned(shard_fd.as_raw_fd()) {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                for entry in &entries {
                    if stats.operations_attempted >= budget.max_operations {
                        stats.budget_exhausted = true;
                        return;
                    }

                    if !entry.ends_with(".tmp") {
                        continue;
                    }

                    stats.operations_attempted += 1;

                    let should_delete = if !is_current_boot {
                        true
                    } else if let Ok(parsed) = spoolq_names::parse_temp(entry) {
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
}
