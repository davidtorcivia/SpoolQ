// Recovery phase passes: lease reap, delayed promotion, temp
// cleanup, receipt compaction, retention deletion.
use super::*;

impl Queue {
    pub(crate) fn reap_expired_leases(
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
                            Err(_) => {
                                let relative_path = format!(
                                    "leased/{boot_dir_name}/{bucket_name}/{shard_name}/{entry}"
                                );
                                Self::record_error(
                                    stats,
                                    "reap_parse",
                                    &relative_path,
                                    "malformed leased filename",
                                );
                                if !self.quarantine_recovery_object(
                                    RecoveryQuarantineCandidate {
                                        source_directory_fd: shard_fd.as_fd(),
                                        filename: entry,
                                        relative_path: &relative_path,
                                        reason: crate::QuarantineReason::FilenameParseFailed,
                                    },
                                    stats,
                                    budget,
                                ) {
                                    self.recovery_cursor.reap_leases = previous_entry_cursor;
                                    return;
                                }
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

    pub(crate) fn reap_to_ready(
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

        let ready_common =
            crate::next_common_fields(crate::state_machine::Operation::ReapExpiredToReady, common)
                .map_err(|_| MoveFailure::NotCommitted {
                    phase: MovePhase::PreRename,
                    source: std::io::Error::other("generation or attempt overflow"),
                })?;

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
    pub(crate) fn reap_to_dead(
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

        let dead_common =
            crate::next_common_fields(crate::state_machine::Operation::ReapExpiredToDead, common)
                .map_err(|_| MoveFailure::NotCommitted {
                phase: MovePhase::PreRename,
                source: std::io::Error::other("generation or attempt overflow"),
            })?;

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

    pub(crate) fn promote_delayed(
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

    pub(crate) fn promote_to_ready(
        &self,
        bucket: &str,
        shard: &str,
        delayed_name: &str,
        common: &steadq_names::CommonFields,
    ) -> Result<(), MoveFailure> {
        let ready_common =
            crate::next_common_fields(crate::state_machine::Operation::Promote, common).map_err(
                |_| MoveFailure::NotCommitted {
                    phase: MovePhase::PreRename,
                    source: std::io::Error::other("generation or attempt overflow"),
                },
            )?;
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

    pub(crate) fn cleanup_temp_files(
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
    pub(crate) fn ensure_dir_pub(&self, relative: &str) -> io::Result<()> {
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

    pub(crate) fn compact_receipts_with_scan_budget(
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
    pub(crate) fn delete_expired_receipts(
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
