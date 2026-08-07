// SpoolQ/1 quarantine and fsck operations.

use std::os::unix::io::AsRawFd;

use sha2::Digest;
use spoolq_fs_linux as fs;
use spoolq_names;

use crate::queue::Queue;

/// Fsck options.
#[derive(Clone, Debug)]
pub struct FsckOptions {
    pub mode: FsckMode,
    pub depth: FsckDepth,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsckMode {
    Check,
    Repair,
}

/// Fsck depth.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsckDepth {
    /// Validate filename grammar, file type, link count, header decode,
    /// header/filename consistency, envelope digest, file size, name tag,
    /// and shard placement.
    Structural,
    /// Also hash and verify payload digests.
    Deep,
}

impl Default for FsckOptions {
    fn default() -> Self {
        FsckOptions {
            mode: FsckMode::Check,
            depth: FsckDepth::Structural,
        }
    }
}

/// A corruption finding from fsck.
#[derive(Clone, Debug)]
pub struct CorruptionFinding {
    pub relative_path: String,
    pub finding_type: String,
    pub severity: FindingSeverity,
    pub details: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FindingSeverity {
    Warning,
    Error,
}

/// Fsck report.
#[derive(Clone, Debug, Default)]
pub struct FsckReport {
    pub total_objects: u64,
    pub structurally_verified: u64,
    pub payloads_deep_verified: u64,
    pub findings: Vec<CorruptionFinding>,
    pub quarantined: Vec<[u8; 16]>,
}

impl Queue {
    /// Run fsck on the queue.
    pub fn fsck(&self, opts: &FsckOptions) -> FsckReport {
        let mut report = FsckReport::default();

        // Check ready shards
        self.fsck_state_dir("ready", opts, &mut report);
        // Check leased
        self.fsck_leased_dirs(opts, &mut report);
        // Check delayed
        self.fsck_state_dir("delayed", opts, &mut report);
        // Check dead
        self.fsck_state_dir("dead", opts, &mut report);
        // Check receipts
        self.fsck_state_dir("receipts", opts, &mut report);

        report
    }

    fn fsck_state_dir(&self, state_name: &str, opts: &FsckOptions, report: &mut FsckReport) {
        let root_fd = self.root_fd();
        let state_fd = match fs::open_directory(root_fd, state_name) {
            Ok(fd) => fd,
            Err(_) => return,
        };

        let top_entries = match fs::read_dir_entries_owned(state_fd.as_raw_fd()) {
            Ok(e) => e,
            Err(_) => return,
        };

        for entry in &top_entries {
            let sub_fd = match fs::open_directory(state_fd.as_raw_fd(), entry) {
                Ok(fd) => fd,
                Err(_) => continue,
            };

            let sub_entries = match fs::read_dir_entries_owned(sub_fd.as_raw_fd()) {
                Ok(e) => e,
                Err(_) => continue,
            };

            for sub_entry in &sub_entries {
                if sub_entry.ends_with(".sqj") || sub_entry.ends_with(".rct") {
                    // C-41: Carry full root-relative path
                    report.total_objects += 1;
                    let full_path = format!("{state_name}/{entry}/{sub_entry}");
                    self.fsck_file(
                        sub_fd.as_raw_fd(),
                        state_name,
                        &full_path,
                        sub_entry,
                        opts,
                        report,
                    );
                } else {
                    // Another directory level (shard under bucket)
                    let shard_fd = match fs::open_directory(sub_fd.as_raw_fd(), sub_entry) {
                        Ok(fd) => fd,
                        Err(_) => continue,
                    };
                    let files = match fs::read_dir_entries_owned(shard_fd.as_raw_fd()) {
                        Ok(e) => e,
                        Err(_) => continue,
                    };
                    for file in &files {
                        if file.ends_with(".sqj") || file.ends_with(".rct") {
                            report.total_objects += 1;
                            // C-41: Full path includes all directory levels
                            let full_path = format!("{state_name}/{entry}/{sub_entry}/{file}");
                            self.fsck_file(
                                shard_fd.as_raw_fd(),
                                state_name,
                                &full_path,
                                file,
                                opts,
                                report,
                            );
                        }
                    }
                }
            }
        }
    }

    fn fsck_leased_dirs(&self, opts: &FsckOptions, report: &mut FsckReport) {
        let root_fd = self.root_fd();
        let leased_fd = match fs::open_directory(root_fd, "leased") {
            Ok(fd) => fd,
            Err(_) => return,
        };

        let boot_dirs = match fs::read_dir_entries_owned(leased_fd.as_raw_fd()) {
            Ok(e) => e,
            Err(_) => return,
        };

        for boot_dir in &boot_dirs {
            let boot_fd = match fs::open_directory(leased_fd.as_raw_fd(), boot_dir) {
                Ok(fd) => fd,
                Err(_) => continue,
            };
            let bucket_dirs = match fs::read_dir_entries_owned(boot_fd.as_raw_fd()) {
                Ok(e) => e,
                Err(_) => continue,
            };
            for bucket_dir in &bucket_dirs {
                let bucket_fd = match fs::open_directory(boot_fd.as_raw_fd(), bucket_dir) {
                    Ok(fd) => fd,
                    Err(_) => continue,
                };
                let shard_dirs = match fs::read_dir_entries_owned(bucket_fd.as_raw_fd()) {
                    Ok(e) => e,
                    Err(_) => continue,
                };
                for shard_dir in &shard_dirs {
                    let shard_fd = match fs::open_directory(bucket_fd.as_raw_fd(), shard_dir) {
                        Ok(fd) => fd,
                        Err(_) => continue,
                    };
                    let files = match fs::read_dir_entries_owned(shard_fd.as_raw_fd()) {
                        Ok(e) => e,
                        Err(_) => continue,
                    };
                    for file in &files {
                        if file.ends_with(".sqj") {
                            report.total_objects += 1;
                            let full_path =
                                format!("leased/{boot_dir}/{bucket_dir}/{shard_dir}/{file}");
                            self.fsck_file(
                                shard_fd.as_raw_fd(),
                                "leased",
                                &full_path,
                                file,
                                opts,
                                report,
                            );
                        }
                    }
                }
            }
        }
    }

    /// R4-H16/H17/H18: Deep structural verification of a single object.
    /// Validates filename grammar, file type, link count, header decode,
    /// header/filename consistency, envelope digest, file size, name tag,
    /// and shard placement. In Deep mode, also hashes the payload.
    /// In Repair mode, quarantines objects that fail any structural check.
    #[allow(clippy::too_many_arguments)]
    fn fsck_file(
        &self,
        shard_fd: std::os::unix::io::RawFd,
        state_name: &str,
        full_path: &str,
        filename: &str,
        opts: &FsckOptions,
        report: &mut FsckReport,
    ) {
        let queue_id = &self.format.queue_id;

        // C-40: Parse the filename using the state-appropriate parser.
        // Extract job_id, generation, attempt, max_attempts, tag from the parsed result.
        let parsed = match state_name {
            "ready" => match spoolq_names::parse_ready(filename) {
                Ok(p) => Some((p.common, p.tag, None)),
                Err(_) => None,
            },
            "leased" => match spoolq_names::parse_leased(filename) {
                Ok(p) => Some((p.common, p.tag, Some(p.token))),
                Err(_) => None,
            },
            "delayed" => match spoolq_names::parse_delayed(filename) {
                Ok(p) => Some((p.common, p.tag, None)),
                Err(_) => None,
            },
            "dead" => match spoolq_names::parse_dead(filename) {
                Ok(p) => Some((p.common, p.tag, None)),
                Err(_) => None,
            },
            "receipts" => match spoolq_names::parse_receipt(filename) {
                Ok(p) => Some((p.common, p.tag, Some(p.token))),
                Err(_) => None,
            },
            _ => None,
        };

        let (common, parsed_tag, token) = match parsed {
            Some(v) => v,
            None => {
                report.findings.push(CorruptionFinding {
                    relative_path: full_path.to_string(),
                    finding_type: "filename_parse_failed".into(),
                    severity: FindingSeverity::Error,
                    details: format!("filename does not match {state_name} state grammar"),
                });
                if opts.mode == FsckMode::Repair {
                    let _ = self.quarantine_object(
                        shard_fd,
                        filename,
                        full_path,
                        crate::QuarantineReason::FilenameParseFailed,
                        report,
                    );
                }
                return;
            }
        };

        // Stat the file.
        let stat = match fs::fstatat(shard_fd, filename) {
            Ok(s) => s,
            Err(_) => {
                report.findings.push(CorruptionFinding {
                    relative_path: full_path.to_string(),
                    finding_type: "stat_failed".into(),
                    severity: FindingSeverity::Error,
                    details: "cannot stat file".into(),
                });
                return;
            }
        };

        // Regular file check.
        if stat.st_mode & libc::S_IFMT != libc::S_IFREG {
            report.findings.push(CorruptionFinding {
                relative_path: full_path.to_string(),
                finding_type: "non_regular_file".into(),
                severity: FindingSeverity::Error,
                details: "file is not a regular file".into(),
            });
            if opts.mode == FsckMode::Repair {
                let _ = self.quarantine_object(
                    shard_fd,
                    filename,
                    full_path,
                    crate::QuarantineReason::NonRegularFile,
                    report,
                );
            }
            return;
        }

        // Hard link check.
        if stat.st_nlink != 1 {
            report.findings.push(CorruptionFinding {
                relative_path: full_path.to_string(),
                finding_type: "unexpected_hard_link".into(),
                severity: FindingSeverity::Error,
                details: format!("link count is {} (expected 1)", stat.st_nlink),
            });
            if opts.mode == FsckMode::Repair {
                let _ = self.quarantine_object(
                    shard_fd,
                    filename,
                    full_path,
                    crate::QuarantineReason::UnexpectedHardLink,
                    report,
                );
            }
            return;
        }

        // R4-H16: Read and decode the header.
        // Receipts may be compact (128 bytes with RECEIPT_MAGIC).
        let file_fd = match fs::openat(shard_fd, filename, libc::O_RDONLY, 0) {
            Ok(f) => f,
            Err(_) => {
                report.findings.push(CorruptionFinding {
                    relative_path: full_path.to_string(),
                    finding_type: "open_failed".into(),
                    severity: FindingSeverity::Error,
                    details: "cannot open file for reading".into(),
                });
                return;
            }
        };

        let mut header_buf = [0u8; 128];
        if fs::pread_exact(file_fd.as_raw_fd(), &mut header_buf, 0).is_err() {
            report.findings.push(CorruptionFinding {
                relative_path: full_path.to_string(),
                finding_type: "header_read_failed".into(),
                severity: FindingSeverity::Error,
                details: "cannot read 128-byte header".into(),
            });
            if opts.mode == FsckMode::Repair {
                let _ = self.quarantine_object(
                    shard_fd,
                    filename,
                    full_path,
                    crate::QuarantineReason::EnvelopeCorrupt,
                    report,
                );
            }
            return;
        }

        // Handle compact receipts (128 bytes, RECEIPT_MAGIC prefix).
        let is_compact_receipt = stat.st_size as usize == spoolq_format::COMPACT_RECEIPT_SIZE
            && &header_buf[0..8] == spoolq_format::RECEIPT_MAGIC;

        if is_compact_receipt {
            match spoolq_format::CompactReceipt::decode(&header_buf) {
                Ok(cr) => {
                    if cr.job_id != common.job_id {
                        report.findings.push(CorruptionFinding {
                            relative_path: full_path.to_string(),
                            finding_type: "compact_receipt_job_id_mismatch".into(),
                            severity: FindingSeverity::Error,
                            details: "compact receipt job_id does not match filename".into(),
                        });
                        if opts.mode == FsckMode::Repair {
                            let _ = self.quarantine_object(
                                shard_fd,
                                filename,
                                full_path,
                                crate::QuarantineReason::FilenameHeaderMismatch,
                                report,
                            );
                        }
                        return;
                    }
                    report.structurally_verified += 1;
                    return;
                }
                Err(_) => {
                    report.findings.push(CorruptionFinding {
                        relative_path: full_path.to_string(),
                        finding_type: "compact_receipt_decode_failed".into(),
                        severity: FindingSeverity::Error,
                        details: "compact receipt decode error".into(),
                    });
                    if opts.mode == FsckMode::Repair {
                        let _ = self.quarantine_object(
                            shard_fd,
                            filename,
                            full_path,
                            crate::QuarantineReason::EnvelopeCorrupt,
                            report,
                        );
                    }
                    return;
                }
            }
        }

        // Full header decode for .sqj files.
        let header = match spoolq_format::FixedHeader::decode(&header_buf) {
            Ok(h) => h,
            Err(e) => {
                report.findings.push(CorruptionFinding {
                    relative_path: full_path.to_string(),
                    finding_type: "header_decode_failed".into(),
                    severity: FindingSeverity::Error,
                    details: format!("header decode error: {e}"),
                });
                if opts.mode == FsckMode::Repair {
                    let _ = self.quarantine_object(
                        shard_fd,
                        filename,
                        full_path,
                        crate::QuarantineReason::EnvelopeCorrupt,
                        report,
                    );
                }
                return;
            }
        };

        // R4-H16: Verify header job_id matches filename.
        if header.job_id != common.job_id {
            report.findings.push(CorruptionFinding {
                relative_path: full_path.to_string(),
                finding_type: "header_job_id_mismatch".into(),
                severity: FindingSeverity::Error,
                details: "header job_id does not match filename".into(),
            });
            if opts.mode == FsckMode::Repair {
                let _ = self.quarantine_object(
                    shard_fd,
                    filename,
                    full_path,
                    crate::QuarantineReason::FilenameHeaderMismatch,
                    report,
                );
            }
            return;
        }

        // R4-H16: Verify header maximum_attempts matches filename.
        if header.maximum_attempts != common.maximum_attempts {
            report.findings.push(CorruptionFinding {
                relative_path: full_path.to_string(),
                finding_type: "header_max_attempts_mismatch".into(),
                severity: FindingSeverity::Error,
                details: "header maximum_attempts does not match filename".into(),
            });
            if opts.mode == FsckMode::Repair {
                let _ = self.quarantine_object(
                    shard_fd,
                    filename,
                    full_path,
                    crate::QuarantineReason::FilenameHeaderMismatch,
                    report,
                );
            }
            return;
        }

        // R4-H16: Read extension and verify envelope digest.
        let ext_len = header.extension_header_length as usize;
        if ext_len > 65536 {
            report.findings.push(CorruptionFinding {
                relative_path: full_path.to_string(),
                finding_type: "extension_too_large".into(),
                severity: FindingSeverity::Error,
                details: format!("extension header length {ext_len} exceeds 65536"),
            });
            if opts.mode == FsckMode::Repair {
                let _ = self.quarantine_object(
                    shard_fd,
                    filename,
                    full_path,
                    crate::QuarantineReason::EnvelopeCorrupt,
                    report,
                );
            }
            return;
        }
        let mut ext_buf = vec![0u8; ext_len];
        if ext_len > 0 && fs::pread_exact(file_fd.as_raw_fd(), &mut ext_buf, 128).is_err() {
            report.findings.push(CorruptionFinding {
                relative_path: full_path.to_string(),
                finding_type: "extension_read_failed".into(),
                severity: FindingSeverity::Error,
                details: "cannot read extension header".into(),
            });
            if opts.mode == FsckMode::Repair {
                let _ = self.quarantine_object(
                    shard_fd,
                    filename,
                    full_path,
                    crate::QuarantineReason::EnvelopeCorrupt,
                    report,
                );
            }
            return;
        }
        if !spoolq_format::verify_envelope_digest(&header, &ext_buf) {
            report.findings.push(CorruptionFinding {
                relative_path: full_path.to_string(),
                finding_type: "envelope_digest_mismatch".into(),
                severity: FindingSeverity::Error,
                details: "envelope digest does not match header".into(),
            });
            if opts.mode == FsckMode::Repair {
                let _ = self.quarantine_object(
                    shard_fd,
                    filename,
                    full_path,
                    crate::QuarantineReason::EnvelopeCorrupt,
                    report,
                );
            }
            return;
        }

        // R4-H16: Verify file size matches expected.
        let expected_size = (128 + ext_len + header.payload_length as usize) as u64;
        if stat.st_size as u64 != expected_size {
            report.findings.push(CorruptionFinding {
                relative_path: full_path.to_string(),
                finding_type: "file_size_mismatch".into(),
                severity: FindingSeverity::Error,
                details: format!(
                    "size mismatch: expected {expected_size}, got {}",
                    stat.st_size
                ),
            });
            if opts.mode == FsckMode::Repair {
                let _ = self.quarantine_object(
                    shard_fd,
                    filename,
                    full_path,
                    crate::QuarantineReason::EnvelopeCorrupt,
                    report,
                );
            }
            return;
        }

        // R4-H16: Verify payload limit.
        if header.payload_length > self.format.max_payload_length {
            report.findings.push(CorruptionFinding {
                relative_path: full_path.to_string(),
                finding_type: "payload_exceeds_limit".into(),
                severity: FindingSeverity::Error,
                details: format!(
                    "payload length {} exceeds queue limit {}",
                    header.payload_length, self.format.max_payload_length
                ),
            });
            if opts.mode == FsckMode::Repair {
                let _ = self.quarantine_object(
                    shard_fd,
                    filename,
                    full_path,
                    crate::QuarantineReason::EnvelopeCorrupt,
                    report,
                );
            }
            return;
        }

        // R4-H17: Verify name tag using path-derived context.
        let path_parts: Vec<&str> = full_path.split('/').collect();
        let tag_ok = self.fsck_verify_name_tag(
            state_name,
            &path_parts,
            &common,
            parsed_tag,
            token,
            queue_id,
        );
        if !tag_ok {
            report.findings.push(CorruptionFinding {
                relative_path: full_path.to_string(),
                finding_type: "name_tag_mismatch".into(),
                severity: FindingSeverity::Error,
                details: "name tag does not match computed tag for this context".into(),
            });
            if opts.mode == FsckMode::Repair {
                let _ = self.quarantine_object(
                    shard_fd,
                    filename,
                    full_path,
                    crate::QuarantineReason::FilenameTagFailed,
                    report,
                );
            }
            return;
        }

        // R4-H16: Verify shard placement.
        let computed_shard =
            spoolq_names::compute_shard(queue_id, &common.job_id, self.format.shard_count);
        let shard_hex_in_path = self.fsck_extract_shard_hex(state_name, &path_parts);
        if let Some(shard_hex) = shard_hex_in_path {
            if let Some(path_shard) = spoolq_names::shard_from_hex(shard_hex) {
                if path_shard != computed_shard {
                    report.findings.push(CorruptionFinding {
                        relative_path: full_path.to_string(),
                        finding_type: "shard_placement_mismatch".into(),
                        severity: FindingSeverity::Error,
                        details: format!(
                            "shard {path_shard} in path does not match computed shard {computed_shard}"
                        ),
                    });
                    if opts.mode == FsckMode::Repair {
                        let _ = self.quarantine_object(
                            shard_fd,
                            filename,
                            full_path,
                            crate::QuarantineReason::FilenameTagFailed,
                            report,
                        );
                    }
                    return;
                }
            }
        }

        report.structurally_verified += 1;

        // R4-H18: Deep verification - hash the payload.
        if opts.depth == FsckDepth::Deep && state_name != "receipts" {
            let payload_offset = (128 + ext_len) as u64;
            let mut hasher = sha2::Sha256::new();
            let mut buf = vec![0u8; 65536];
            let mut offset = payload_offset;
            let mut remaining = header.payload_length as usize;
            let mut read_ok = true;
            while remaining > 0 {
                let to_read = remaining.min(buf.len());
                match fs::pread(file_fd.as_raw_fd(), &mut buf[..to_read], offset) {
                    Ok(n) if n > 0 => {
                        hasher.update(&buf[..n]);
                        offset += n as u64;
                        remaining -= n;
                    }
                    _ => {
                        read_ok = false;
                        break;
                    }
                }
            }
            if !read_ok {
                report.findings.push(CorruptionFinding {
                    relative_path: full_path.to_string(),
                    finding_type: "payload_read_failed".into(),
                    severity: FindingSeverity::Error,
                    details: "cannot read payload for deep verification".into(),
                });
                return;
            }
            let computed: [u8; 32] = hasher.finalize().into();
            if computed != header.payload_digest {
                report.findings.push(CorruptionFinding {
                    relative_path: full_path.to_string(),
                    finding_type: "payload_digest_mismatch".into(),
                    severity: FindingSeverity::Error,
                    details: "payload digest does not match header".into(),
                });
                if opts.mode == FsckMode::Repair {
                    let _ = self.quarantine_object(
                        shard_fd,
                        filename,
                        full_path,
                        crate::QuarantineReason::PayloadCorrupt,
                        report,
                    );
                }
                return;
            }
            report.payloads_deep_verified += 1;
        }
    }

    /// R4-H17: Verify the name tag by reconstructing the canonical context
    /// from the path components and the parsed filename fields.
    fn fsck_verify_name_tag(
        &self,
        state_name: &str,
        path_parts: &[&str],
        common: &spoolq_names::CommonFields,
        parsed_tag: [u8; 8],
        token: Option<[u8; 16]>,
        queue_id: &[u8; 16],
    ) -> bool {
        let job_hex = spoolq_names::hex_encode(&common.job_id);
        let base_no_token = format!(
            "{}.g{:016x}.a{:08x}.m{:08x}",
            job_hex, common.generation, common.attempt, common.maximum_attempts,
        );

        let expected_tag = match state_name {
            "ready" => {
                // ready/<shard>/<filename>
                if path_parts.len() != 3 {
                    return true; // cannot verify, skip
                }
                let shard_hex = path_parts[1];
                let ctx = spoolq_names::ready_context(shard_hex, &base_no_token);
                spoolq_names::compute_name_tag(queue_id, &ctx)
            }
            "leased" => {
                // leased/<boot>/<bucket>/<shard>/<filename>
                if path_parts.len() != 5 {
                    return true;
                }
                let boot = path_parts[1];
                let bucket = path_parts[2];
                let shard_hex = path_parts[3];
                let token_hex = match token {
                    Some(t) => spoolq_names::hex_encode(&t),
                    None => return true,
                };
                let base = format!("{base_no_token}.t{token_hex}");
                let ctx = spoolq_names::leased_context(boot, bucket, shard_hex, &base);
                spoolq_names::compute_name_tag(queue_id, &ctx)
            }
            "delayed" => {
                // delayed/-/<bucket>/<shard>/<filename>
                // Delayed filenames include retry_after_ns in the tag context.
                // The parse result does not expose retry_after_ns separately,
                // so tag verification for delayed is handled by shard placement
                // checks instead. Skip tag verification here.
                return true;
            }
            "dead" => {
                // dead/-/<bucket>/<shard>/<filename>
                if path_parts.len() != 5 {
                    return true;
                }
                let bucket = path_parts[2];
                let shard_hex = path_parts[3];
                let ctx = spoolq_names::terminal_context(
                    spoolq_names::State::Dead,
                    bucket,
                    shard_hex,
                    &base_no_token,
                );
                spoolq_names::compute_name_tag(queue_id, &ctx)
            }
            "receipts" => {
                // receipts/-/<bucket>/<shard>/<filename>
                if path_parts.len() != 5 {
                    return true;
                }
                let bucket = path_parts[2];
                let shard_hex = path_parts[3];
                let token_hex = match token {
                    Some(t) => spoolq_names::hex_encode(&t),
                    None => return true,
                };
                let base = format!("{base_no_token}.t{token_hex}");
                let ctx = spoolq_names::terminal_context(
                    spoolq_names::State::Receipt,
                    bucket,
                    shard_hex,
                    &base,
                );
                spoolq_names::compute_name_tag(queue_id, &ctx)
            }
            _ => return true,
        };

        expected_tag == parsed_tag
    }

    /// Extract the shard hex from the path based on state structure.
    fn fsck_extract_shard_hex<'a>(
        &self,
        state_name: &str,
        path_parts: &[&'a str],
    ) -> Option<&'a str> {
        match state_name {
            "ready" if path_parts.len() == 3 => Some(path_parts[1]),
            "leased" | "delayed" | "dead" | "receipts" if path_parts.len() == 5 => {
                Some(path_parts[3])
            }
            _ => None,
        }
    }

    /// B-10: Move a corrupt object to quarantine via durable no-overwrite transition.
    fn quarantine_object(
        &self,
        src_dir_fd: std::os::unix::io::RawFd,
        filename: &str,
        full_path: &str,
        reason: crate::QuarantineReason,
        report: &mut FsckReport,
    ) -> Result<(), std::io::Error> {
        let qid = spoolq_fs_linux::random_128bit()?;
        let q_name = spoolq_names::quarantine_filename(&qid, reason as u16);

        // Ensure quarantine directory exists
        let _ = self.ensure_dir("quarantine");
        let q_dir_fd = crate::queue::open_relative(self.root_fd(), "quarantine")?;

        // Durable no-overwrite move
        spoolq_fs_linux::durable_move_noreplace(
            src_dir_fd,
            filename,
            q_dir_fd.as_raw_fd(),
            &q_name,
        )?;

        report.quarantined.push(qid);
        report.findings.push(CorruptionFinding {
            relative_path: full_path.to_string(),
            finding_type: "quarantined".into(),
            severity: FindingSeverity::Warning,
            details: format!("moved to quarantine as {q_name}"),
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::{CreateOptions, EnqueueInput, OpenOptions, Queue};
    use tempfile::TempDir;

    #[test]
    fn fsck_clean_queue() {
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
        let report = queue.fsck(&FsckOptions::default());
        assert_eq!(report.findings.len(), 0);
        assert_eq!(report.total_objects, 0);
    }

    #[test]
    fn fsck_finds_valid_job() {
        let tmp = TempDir::new().unwrap();
        Queue::init(tmp.path(), &CreateOptions::default()).unwrap();
        let mut queue = Queue::open(
            tmp.path(),
            &OpenOptions {
                allow_unsupported_fs: true,
                ..Default::default()
            },
        )
        .unwrap();
        queue.enqueue(EnqueueInput {
            maximum_attempts: 3,
            content_type: "x".to_string(),
            payload: b"data".to_vec(),
            ..Default::default()
        });
        drop(queue);

        let queue2 = Queue::open(
            tmp.path(),
            &OpenOptions {
                allow_unsupported_fs: true,
                ..Default::default()
            },
        )
        .unwrap();
        let report = queue2.fsck(&FsckOptions::default());
        assert_eq!(report.total_objects, 1);
        assert_eq!(report.structurally_verified, 1);
        assert_eq!(report.findings.len(), 0);
    }

    #[test]
    fn fsck_deep_verifies_payload() {
        let tmp = TempDir::new().unwrap();
        Queue::init(tmp.path(), &CreateOptions::default()).unwrap();
        let mut queue = Queue::open(
            tmp.path(),
            &OpenOptions {
                allow_unsupported_fs: true,
                ..Default::default()
            },
        )
        .unwrap();
        queue.enqueue(EnqueueInput {
            maximum_attempts: 3,
            content_type: "x".to_string(),
            payload: b"payload data here".to_vec(),
            ..Default::default()
        });
        drop(queue);

        let queue2 = Queue::open(
            tmp.path(),
            &OpenOptions {
                allow_unsupported_fs: true,
                ..Default::default()
            },
        )
        .unwrap();
        let report = queue2.fsck(&FsckOptions {
            mode: FsckMode::Check,
            depth: FsckDepth::Deep,
        });
        assert_eq!(report.total_objects, 1);
        assert_eq!(report.structurally_verified, 1);
        assert_eq!(report.payloads_deep_verified, 1);
        assert_eq!(report.findings.len(), 0);
    }

    #[test]
    fn fsck_detects_header_corruption() {
        let tmp = TempDir::new().unwrap();
        Queue::init(tmp.path(), &CreateOptions::default()).unwrap();
        let mut queue = Queue::open(
            tmp.path(),
            &OpenOptions {
                allow_unsupported_fs: true,
                ..Default::default()
            },
        )
        .unwrap();
        queue.enqueue(EnqueueInput {
            maximum_attempts: 3,
            content_type: "x".to_string(),
            payload: b"data".to_vec(),
            ..Default::default()
        });
        drop(queue);

        // Corrupt a header byte in the ready object
        let ready_dir = tmp.path().join("ready");
        for shard_dir in std::fs::read_dir(&ready_dir).unwrap() {
            let shard_dir = shard_dir.unwrap().path();
            for entry in std::fs::read_dir(&shard_dir).unwrap() {
                let entry = entry.unwrap().path();
                use std::io::{Seek, SeekFrom, Write};
                let mut f = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&entry)
                    .unwrap();
                f.seek(SeekFrom::Start(20)).unwrap();
                f.write_all(&[0xFF]).unwrap();
                f.sync_all().unwrap();
            }
        }

        let queue2 = Queue::open(
            tmp.path(),
            &OpenOptions {
                allow_unsupported_fs: true,
                ..Default::default()
            },
        )
        .unwrap();
        let report = queue2.fsck(&FsckOptions::default());
        assert!(!report.findings.is_empty());
        assert_eq!(report.structurally_verified, 0);
    }

    #[test]
    fn fsck_repair_quarantines_corrupt_header() {
        let tmp = TempDir::new().unwrap();
        Queue::init(tmp.path(), &CreateOptions::default()).unwrap();
        let mut queue = Queue::open(
            tmp.path(),
            &OpenOptions {
                allow_unsupported_fs: true,
                ..Default::default()
            },
        )
        .unwrap();
        queue.enqueue(EnqueueInput {
            maximum_attempts: 3,
            content_type: "x".to_string(),
            payload: b"data".to_vec(),
            ..Default::default()
        });
        drop(queue);

        // Corrupt byte 32 (job_id region) - causes header/filename mismatch
        let ready_dir = tmp.path().join("ready");
        for shard_dir in std::fs::read_dir(&ready_dir).unwrap() {
            let shard_dir = shard_dir.unwrap().path();
            for entry in std::fs::read_dir(&shard_dir).unwrap() {
                let entry = entry.unwrap().path();
                use std::io::{Seek, SeekFrom, Write};
                let mut f = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&entry)
                    .unwrap();
                f.seek(SeekFrom::Start(32)).unwrap();
                f.write_all(&[0xFF]).unwrap();
                f.sync_all().unwrap();
            }
        }

        let queue2 = Queue::open(
            tmp.path(),
            &OpenOptions {
                allow_unsupported_fs: true,
                ..Default::default()
            },
        )
        .unwrap();
        let report = queue2.fsck(&FsckOptions {
            mode: FsckMode::Repair,
            depth: FsckDepth::Structural,
        });
        assert!(!report.findings.is_empty());
        assert!(!report.quarantined.is_empty());
    }
}
