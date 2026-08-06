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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsckDepth {
    Structural,
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

        // State directories have either shard dirs (ready) or bucket/shard dirs
        let top_entries = match fs::read_dir_entries_owned(state_fd.as_raw_fd()) {
            Ok(e) => e,
            Err(_) => return,
        };

        for entry in &top_entries {
            // For ready/, entries are shard dirs
            // For delayed/dead/receipts/, entries are bucket dirs
            let sub_fd = match fs::open_directory(state_fd.as_raw_fd(), entry) {
                Ok(fd) => fd,
                Err(_) => continue,
            };

            let sub_entries = match fs::read_dir_entries_owned(sub_fd.as_raw_fd()) {
                Ok(e) => e,
                Err(_) => continue,
            };

            // Check if these are shard dirs (files directly) or another level of dirs
            for sub_entry in &sub_entries {
                if sub_entry.ends_with(".sqj") || sub_entry.ends_with(".rct") {
                    // It's a file - verify it
                    report.total_objects += 1;
                    self.fsck_file(state_fd.as_raw_fd(), entry, sub_entry, opts, report);
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
                            self.fsck_file(shard_fd.as_raw_fd(), sub_entry, file, opts, report);
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
                            self.fsck_file(shard_fd.as_raw_fd(), shard_dir, file, opts, report);
                        }
                    }
                }
            }
        }
    }

    fn fsck_file(
        &self,
        _shard_fd: std::os::unix::io::RawFd,
        shard_name: &str,
        filename: &str,
        opts: &FsckOptions,
        report: &mut FsckReport,
    ) {
        // Try to parse the filename as each active state type
        let parsed_ok = if filename.ends_with(".sqj") {
            spoolq_names::parse_ready(filename).is_ok()
                || spoolq_names::parse_leased(filename).is_ok()
                || spoolq_names::parse_delayed(filename).is_ok()
                || spoolq_names::parse_dead(filename).is_ok()
        } else if filename.ends_with(".rct") {
            spoolq_names::parse_receipt(filename).is_ok()
        } else {
            false
        };

        if parsed_ok {
            report.structurally_verified += 1;
        } else {
            report.findings.push(CorruptionFinding {
                relative_path: format!("{}/{}", shard_name, filename),
                finding_type: "filename_parse_failed".into(),
                severity: FindingSeverity::Error,
                details: "unparseable filename".into(),
            });
        }

        if opts.mode == FsckMode::Repair {
            // TODO: move malformed files to quarantine
        }
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
