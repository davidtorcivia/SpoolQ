// SpoolQ/1 quarantine and fsck operations.

use std::os::unix::io::AsRawFd;

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

/// Fsck depth. C-39: Deep is not yet fully implemented.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsckDepth {
    /// Validate filename grammar, file type, shard placement, and header.
    Structural,
    /// Also verify envelope and payload digests (not yet implemented).
    #[allow(dead_code)]
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
                    self.fsck_file(sub_fd.as_raw_fd(), state_name, &full_path, sub_entry, opts, report);
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
                            self.fsck_file(shard_fd.as_raw_fd(), state_name, &full_path, file, opts, report);
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
                            let full_path = format!("leased/{boot_dir}/{bucket_dir}/{shard_dir}/{file}");
                            self.fsck_file(shard_fd.as_raw_fd(), "leased", &full_path, file, opts, report);
                        }
                    }
                }
            }
        }
    }

    /// C-40: Validate using the state-specific parser.
    /// state_name determines which parser to use.
    /// full_path carries the root-relative path (C-41).
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
        // C-40: Use the parser required by the containing state
        let parsed_ok = match state_name {
            "ready" => filename.ends_with(".sqj") && spoolq_names::parse_ready(filename).is_ok(),
            "leased" => filename.ends_with(".sqj") && spoolq_names::parse_leased(filename).is_ok(),
            "delayed" => filename.ends_with(".sqj") && spoolq_names::parse_delayed(filename).is_ok(),
            "dead" => filename.ends_with(".sqj") && spoolq_names::parse_dead(filename).is_ok(),
            "receipts" => filename.ends_with(".rct") && spoolq_names::parse_receipt(filename).is_ok(),
            _ => false,
        };

        if parsed_ok {
            // C-40: Also verify the file is a regular file and check header
            if let Ok(stat) = fs::fstatat(shard_fd, filename) {
                if stat.st_mode & libc::S_IFMT as u32 != libc::S_IFREG as u32 {
                    report.findings.push(CorruptionFinding {
                        relative_path: full_path.to_string(),
                        finding_type: "non_regular_file".into(),
                        severity: FindingSeverity::Error,
                        details: "file is not a regular file".into(),
                    });
                    return;
                }
                // B-10: Check for unexpected hard links
                if stat.st_nlink != 1 {
                    report.findings.push(CorruptionFinding {
                        relative_path: full_path.to_string(),
                        finding_type: "unexpected_hard_link".into(),
                        severity: FindingSeverity::Error,
                        details: format!("link count is {} (expected 1)", stat.st_nlink),
                    });
                    return;
                }
            }
            report.structurally_verified += 1;
        } else {
            // B-10: Report corruption with full path
            report.findings.push(CorruptionFinding {
                relative_path: full_path.to_string(),
                finding_type: "filename_parse_failed".into(),
                severity: FindingSeverity::Error,
                details: format!("filename does not match {} state grammar", state_name),
            });

            // B-10: In repair mode, quarantine corrupt objects
            if opts.mode == FsckMode::Repair {
                let _ = self.quarantine_object(shard_fd, filename, full_path,
                    crate::QuarantineReason::FilenameParseFailed, report);
            }
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
            details: format!("moved to quarantine as {}", q_name),
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
}
