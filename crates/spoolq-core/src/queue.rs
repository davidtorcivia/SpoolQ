// SpoolQ/1 queue initialization, open, and enqueue operations.

use std::io;
use std::os::unix::io::{AsRawFd, OwnedFd, RawFd};
use std::path::{Path, PathBuf};

use sha2::Digest;
use spoolq_format::cbor::ExtensionHeader;
use spoolq_format::{
    envelope_digest, payload_digest, FixedHeader, FormatRecord, WatermarkRecord,
    DIGEST_ALGORITHM_SHA256, MAX_PAYLOAD_LENGTH,
};
use spoolq_fs_linux as fs;
use spoolq_math::{self, bucket_number, ceiling_bucket, eligibility_bucket_and_ns};
use spoolq_names::{
    self, bucket_hex, compute_name_tag, compute_shard, delayed_context, delayed_filename,
    ready_context, ready_filename, shard_hex, temp_filename, CommonFields,
};

use crate::errors::*;

/// Configuration for creating a new queue.
#[derive(Clone, Debug)]
pub struct CreateOptions {
    pub shard_count: u32,
    pub lease_bucket_width_ns: u64,
    pub delayed_bucket_width_ns: u64,
    pub terminal_bucket_width_ns: u64,
    pub max_payload_length: u64,
}

impl Default for CreateOptions {
    fn default() -> Self {
        Self {
            shard_count: 64,
            lease_bucket_width_ns: 10_000_000_000,
            delayed_bucket_width_ns: 10_000_000_000,
            terminal_bucket_width_ns: 3_600_000_000_000,
            max_payload_length: MAX_PAYLOAD_LENGTH,
        }
    }
}

/// Operational options for opening a queue.
#[derive(Clone, Debug)]
pub struct OpenOptions {
    pub create: bool,
    pub create_options: CreateOptions,
    pub allow_unsupported_fs: bool,
    pub receipt_retention_ns: u64,
    pub temporary_file_ttl_ns: u64,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            create: false,
            create_options: CreateOptions::default(),
            allow_unsupported_fs: false,
            receipt_retention_ns: 7 * 24 * 60 * 60 * 1_000_000_000,
            temporary_file_ttl_ns: 24 * 60 * 60 * 1_000_000_000,
        }
    }
}

/// Internal queue state.
#[allow(dead_code)]
pub struct Queue {
    pub(crate) root_fd: OwnedFd,
    pub(crate) root_path: PathBuf,
    pub(crate) format: FormatRecord,
    pub(crate) boot_id: String,
    pub(crate) boot_id_bytes: [u8; 16],
    pub(crate) poisoned: bool,
    pub(crate) worker_nonce: [u8; 16],
    pub(crate) options: OpenOptions,
}

impl Queue {
    /// Initialize a new queue at the given path.
    pub fn init(root: &Path, opts: &CreateOptions) -> io::Result<FormatRecord> {
        // Validate options
        if opts.shard_count == 0 || !opts.shard_count.is_power_of_two() || opts.shard_count > 4096 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid shard count",
            ));
        }
        if !(60_000_000_000..=86_400_000_000_000).contains(&opts.terminal_bucket_width_ns) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid terminal bucket width",
            ));
        }
        if opts.max_payload_length > MAX_PAYLOAD_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "payload limit exceeds maximum",
            ));
        }

        // Create root directory if needed
        if !root.exists() {
            std::fs::create_dir_all(root)?;
            // Sync the parent directory so the root entry persists
            if let Some(parent) = root.parent() {
                if let Ok(parent_fd) = fs::open_dir_absolute(parent) {
                    let _ = fs::fsync_dir_fd(parent_fd.as_raw_fd());
                }
            }
        }

        let root_fd = fs::open_dir_absolute(root)?;

        // Generate queue ID
        let queue_id = fs::random_128bit()?;
        let created_at = fs::clock_realtime_ns()?;

        let format_rec = FormatRecord {
            queue_id,
            created_at_unix_ns: created_at,
            shard_count: opts.shard_count,
            lease_bucket_width_ns: opts.lease_bucket_width_ns,
            delayed_bucket_width_ns: opts.delayed_bucket_width_ns,
            terminal_bucket_width_ns: opts.terminal_bucket_width_ns,
            max_payload_length: opts.max_payload_length,
        };

        // Create static directories
        for dir in [
            "control",
            "tmp",
            "ready",
            "leased",
            "delayed",
            "receipts",
            "dead",
            "quarantine",
        ] {
            fs::mkdirat_eexist_ok(root_fd.as_raw_fd(), dir, 0o700)?;
        }
        // Sync root after directory creation
        fs::fsync_dir_fd(root_fd.as_raw_fd())?;

        // Create static shard directories under ready/
        let ready_fd = fs::open_directory(root_fd.as_raw_fd(), "ready")?;
        for i in 0..opts.shard_count {
            let shard_name = format!("{:04x}", i);
            fs::mkdirat_eexist_ok(ready_fd.as_raw_fd(), &shard_name, 0o700)?;
        }
        // Sync ready/ after shard creation
        fs::fsync_dir_fd(ready_fd.as_raw_fd())?;
        // Sync root
        fs::fsync_dir_fd(root_fd.as_raw_fd())?;

        // Create control lock files
        let control_fd = fs::open_directory(root_fd.as_raw_fd(), "control")?;
        for lock_file in ["maintenance.lock", "wall-watermark.lock"] {
            let fd =
                fs::create_exclusive(control_fd.as_raw_fd(), lock_file, 0o600).or_else(|e| {
                    if e.kind() == io::ErrorKind::AlreadyExists {
                        fs::openat(control_fd.as_raw_fd(), lock_file, 0o2, 0o600)
                    } else {
                        Err(e)
                    }
                })?;
            fs::fsync(fd.as_raw_fd())?;
        }
        fs::fsync_dir_fd(control_fd.as_raw_fd())?;
        fs::fsync_dir_fd(root_fd.as_raw_fd())?;

        // Write initial wall watermark
        let wall_now = fs::clock_realtime_ns()?;
        let wall_bucket = bucket_number(wall_now, opts.delayed_bucket_width_ns);
        let wm = WatermarkRecord {
            highest_observed_bucket: wall_bucket,
            sequence: 0,
        };
        let wm_bytes = wm.encode();
        // Write via temp file then rename
        let wm_tmp = fs::create_exclusive(control_fd.as_raw_fd(), ".wm.tmp", 0o600)?;
        fs::write_all(wm_tmp.as_raw_fd(), &wm_bytes)?;
        fs::fsync(wm_tmp.as_raw_fd())?;
        fs::renameat(
            control_fd.as_raw_fd(),
            ".wm.tmp",
            control_fd.as_raw_fd(),
            "wall-watermark",
        )?;
        fs::fsync_dir_fd(control_fd.as_raw_fd())?;

        // Write FORMAT file
        let format_bytes = format_rec.encode();
        let fmt_tmp = fs::create_exclusive(root_fd.as_raw_fd(), ".format.tmp", 0o600)?;
        fs::write_all(fmt_tmp.as_raw_fd(), &format_bytes)?;
        fs::fsync(fmt_tmp.as_raw_fd())?;
        fs::renameat(
            root_fd.as_raw_fd(),
            ".format.tmp",
            root_fd.as_raw_fd(),
            "FORMAT",
        )?;
        // Set FORMAT to read-only (0400)
        let _ = fs::fchmodat(root_fd.as_raw_fd(), "FORMAT", 0o400);
        fs::fsync_dir_fd(root_fd.as_raw_fd())?;

        // Reopen and verify as a normal client (step 13)
        let verify_format = std::fs::read(root.join("FORMAT"))?;
        FormatRecord::decode(&verify_format)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        Ok(format_rec)
    }

    /// Open an existing queue.
    pub fn open(root: &Path, opts: &OpenOptions) -> Result<Self, Error> {
        // Read and validate FORMAT
        let format_path = root.join("FORMAT");
        let format_bytes =
            std::fs::read(&format_path).map_err(|e| Error::IoFailure(e.to_string()))?;
        let format_rec = FormatRecord::decode(&format_bytes)
            .map_err(|e| Error::InvalidInput(format!("FORMAT decode: {}", e)))?;

        // Validate retention bound: ceil(retention / terminal_width) + 2 <= 4096
        let probe_count = ceiling_bucket(
            opts.receipt_retention_ns,
            format_rec.terminal_bucket_width_ns,
        ) + 2;
        if probe_count > 4096 {
            return Err(Error::InvalidInput(
                "receipt retention exceeds duplicate-ack probe bound".into(),
            ));
        }

        // Open root directory
        let root_fd = fs::open_dir_absolute(root).map_err(|e| Error::IoFailure(e.to_string()))?;

        // Check filesystem type
        if !opts.allow_unsupported_fs {
            let magic = fs::statfs(root).map_err(|e| Error::IoFailure(e.to_string()))?;
            let ft = magic.f_type as i64;
            match ft {
                fs::EXT4_SUPER_MAGIC | fs::XFS_SUPER_MAGIC => {}
                fs::TMPFS_MAGIC => {
                    return Err(Error::UnsupportedFilesystem);
                }
                fs::NFS_SUPER_MAGIC | fs::FUSE_SUPER_MAGIC | fs::OVERLAYFS_SUPER_MAGIC => {
                    return Err(Error::UnsupportedFilesystem);
                }
                _ => {
                    return Err(Error::UnsupportedFilesystem);
                }
            }
        }

        // Read boot ID
        let boot_id = fs::read_boot_id().map_err(|e| Error::IoFailure(e.to_string()))?;
        let boot_id_bin = spoolq_names::boot_id_bytes(&boot_id)
            .ok_or_else(|| Error::InvalidInput("invalid boot_id format".into()))?;

        // Generate worker nonce
        let worker_nonce = fs::random_128bit().map_err(|e| Error::IoFailure(e.to_string()))?;

        // Acquire shared maintenance lock
        let maint_fd = fs::openat(root_fd.as_raw_fd(), "control/maintenance.lock", 0o0, 0o600)
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        let locked = fs::try_ofd_read_lock(maint_fd.as_raw_fd())
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        if !locked {
            return Err(Error::MaintenanceBusy);
        }
        std::mem::forget(maint_fd);

        Ok(Queue {
            root_fd,
            root_path: root.to_path_buf(),
            format: format_rec,
            boot_id,
            boot_id_bytes: boot_id_bin,
            poisoned: false,
            worker_nonce,
            options: opts.clone(),
        })
    }

    pub fn format(&self) -> &FormatRecord {
        &self.format
    }

    pub fn queue_id(&self) -> &[u8; 16] {
        &self.format.queue_id
    }

    pub fn boot_id(&self) -> &str {
        &self.boot_id
    }

    pub fn root_fd(&self) -> RawFd {
        self.root_fd.as_raw_fd()
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    fn check_not_poisoned(&self) -> Result<(), Error> {
        if self.poisoned {
            return Err(Error::QueuePoisoned("handle is poisoned".into()));
        }
        Ok(())
    }

    fn poison(&mut self) {
        self.poisoned = true;
    }
    /// Compute the effective wall floor: max(CLOCK_REALTIME, stored watermark bucket * width)
    pub(crate) fn effective_wall_floor_ns(&self) -> u64 {
        let clock = spoolq_fs_linux::clock_realtime_ns().unwrap_or(0);
        // Read the wall watermark
        match self.read_wall_watermark() {
            Some(wm) => spoolq_math::effective_wall_floor(
                clock,
                wm.highest_observed_bucket,
                self.format.delayed_bucket_width_ns,
            ),
            None => clock,
        }
    }

    /// Read the wall watermark record from control/wall-watermark.
    fn read_wall_watermark(&self) -> Option<spoolq_format::WatermarkRecord> {
        let data = std::fs::read(self.root_path.join("control/wall-watermark")).ok()?;
        if data.len() != spoolq_format::WATERMARK_SIZE {
            return None;
        }
        spoolq_format::WatermarkRecord::decode(&data).ok()
    }

    /// Enqueue a job with the given payload and metadata.
    pub fn enqueue(&mut self, job: EnqueueInput) -> EnqueueOutcome {
        if let Err(e) = self.check_not_poisoned() {
            let ticket = EnqueueTicket {
                job_id: [0; 16],
                envelope_digest: [0; 32],
                expected_initial_state: InitialState::Ready,
                expected_relative_path: String::new(),
            };
            return EnqueueOutcome::NotCommitted(ticket, e);
        }

        // Generate job ID before any filesystem operation
        let job_id = match fs::random_128bit() {
            Ok(id) => id,
            Err(e) => {
                let ticket = EnqueueTicket {
                    job_id: [0; 16],
                    envelope_digest: [0; 32],
                    expected_initial_state: InitialState::Ready,
                    expected_relative_path: String::new(),
                };
                return EnqueueOutcome::NotCommitted(ticket, Error::IoFailure(e.to_string()));
            }
        };

        let created_at = match fs::clock_realtime_ns() {
            Ok(t) => t,
            Err(e) => {
                let ticket = EnqueueTicket {
                    job_id,
                    envelope_digest: [0; 32],
                    expected_initial_state: InitialState::Ready,
                    expected_relative_path: String::new(),
                };
                return EnqueueOutcome::NotCommitted(ticket, Error::IoFailure(e.to_string()));
            }
        };

        // Validate maximum_attempts
        if job.maximum_attempts == 0 {
            let ticket = EnqueueTicket {
                job_id,
                envelope_digest: [0; 32],
                expected_initial_state: InitialState::Ready,
                expected_relative_path: String::new(),
            };
            return EnqueueOutcome::NotCommitted(
                ticket,
                Error::InvalidInput("maximum_attempts must be >= 1".into()),
            );
        }

        // Encode extension header
        let ext = ExtensionHeader {
            initial_not_before_unix_ns: job.initial_not_before,
            content_type: job.content_type.clone(),
            metadata: job.metadata.clone(),
            producer_id: job.producer_id.clone(),
            trace_context: job.trace_context.clone(),
        };

        let ext_bytes = match ext.encode() {
            Ok(b) => b,
            Err(e) => {
                let ticket = EnqueueTicket {
                    job_id,
                    envelope_digest: [0; 32],
                    expected_initial_state: InitialState::Ready,
                    expected_relative_path: String::new(),
                };
                return EnqueueOutcome::NotCommitted(ticket, Error::InvalidInput(e.to_string()));
            }
        };

        // Compute payload digest
        let pdig = payload_digest(&job.payload);

        // Validate payload size
        if job.payload.len() as u64 > self.format.max_payload_length.min(MAX_PAYLOAD_LENGTH) {
            let ticket = EnqueueTicket {
                job_id,
                envelope_digest: [0; 32],
                expected_initial_state: InitialState::Ready,
                expected_relative_path: String::new(),
            };
            return EnqueueOutcome::NotCommitted(
                ticket,
                Error::InvalidInput("payload exceeds limit".into()),
            );
        }

        // Build fixed header
        let mut header = FixedHeader {
            extension_header_length: ext_bytes.len() as u32,
            payload_length: job.payload.len() as u64,
            flags: 0,
            digest_algorithm: DIGEST_ALGORITHM_SHA256,
            job_id,
            maximum_attempts: job.maximum_attempts,
            created_at_unix_ns: created_at,
            payload_digest: pdig,
            envelope_digest: [0; 32],
        };
        header.envelope_digest = envelope_digest(&header, &ext_bytes);

        // Compute shard
        let shard = compute_shard(&self.format.queue_id, &job_id, self.format.shard_count);
        let shard_str = shard_hex(shard);

        // Determine initial state: ready or delayed
        let now_wall = self.effective_wall_floor_ns();
        let (initial_state, eligibility_bucket) = match job.initial_not_before {
            Some(nb) if nb > now_wall => {
                let (eb, _) =
                    match eligibility_bucket_and_ns(nb, self.format.delayed_bucket_width_ns) {
                        Some(v) => v,
                        None => {
                            let ticket = EnqueueTicket {
                                job_id,
                                envelope_digest: header.envelope_digest,
                                expected_initial_state: InitialState::Ready,
                                expected_relative_path: String::new(),
                            };
                            return EnqueueOutcome::NotCommitted(
                                ticket,
                                Error::InvalidInput("eligibility overflow".into()),
                            );
                        }
                    };
                (InitialState::Delayed, eb)
            }
            _ => (InitialState::Ready, 0),
        };

        // Build the canonical filename and path
        let common = CommonFields {
            job_id,
            generation: 0,
            attempt: 0,
            maximum_attempts: job.maximum_attempts,
        };

        let (dest_dir_relative, filename, expected_path) = match initial_state {
            InitialState::Ready => {
                let base = format!(
                    "{}.g{:016x}.a{:08x}.m{:08x}",
                    spoolq_names::hex_encode(&job_id),
                    0u64,
                    0u32,
                    job.maximum_attempts
                );
                let ctx = ready_context(&shard_str, &base);
                let tag = compute_name_tag(&self.format.queue_id, &ctx);
                let fname = ready_filename(&common, &tag);
                let path = format!("ready/{}/{}", shard_str, fname);
                (format!("ready/{}", shard_str), fname, path)
            }
            InitialState::Delayed => {
                let bucket_str = bucket_hex(eligibility_bucket);
                let base = format!(
                    "{}.g{:016x}.a{:08x}.m{:08x}.d{:016x}",
                    spoolq_names::hex_encode(&job_id),
                    0u64,
                    0u32,
                    job.maximum_attempts,
                    nb_to_u64(job.initial_not_before)
                );
                let ctx = delayed_context(&bucket_str, &shard_str, &base);
                let tag = compute_name_tag(&self.format.queue_id, &ctx);
                let fname = delayed_filename(&common, nb_to_u64(job.initial_not_before), &tag);
                let path = format!("delayed/{}/{}/{}", bucket_str, shard_str, fname);
                (format!("delayed/{}/{}", bucket_str, shard_str), fname, path)
            }
        };

        let ticket = EnqueueTicket {
            job_id,
            envelope_digest: header.envelope_digest,
            expected_initial_state: initial_state,
            expected_relative_path: expected_path.clone(),
        };

        // Create the job file using O_TMPFILE in the destination directory
        let result = self.write_and_publish(
            &dest_dir_relative,
            &filename,
            &header,
            &ext_bytes,
            &job.payload,
        );

        match result {
            Ok(()) => EnqueueOutcome::Committed(ticket),
            Err(PublishError::NotCommitted(e)) => EnqueueOutcome::NotCommitted(ticket, e),
            Err(PublishError::OutcomeUnknown(e)) => {
                self.poison();
                EnqueueOutcome::OutcomeUnknown(ticket, e)
            }
        }
    }

    /// Write the job envelope to a temp file and publish via rename.
    fn write_and_publish(
        &mut self,
        dest_dir_relative: &str,
        dest_name: &str,
        header: &FixedHeader,
        ext_bytes: &[u8],
        payload: &[u8],
    ) -> Result<(), PublishError> {
        // Ensure destination directory exists
        self.ensure_dir(dest_dir_relative)
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;

        // Open destination directory
        let dest_fd = open_relative(self.root_fd.as_raw_fd(), dest_dir_relative)
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;

        // Try O_TMPFILE path first
        match fs::open_tmpfile(dest_fd.as_raw_fd()) {
            Ok(tmp_fd) => {
                // Write header (zeroed placeholder)
                let header_bytes = header.encode(ext_bytes);
                fs::write_all(tmp_fd.as_raw_fd(), &header_bytes)
                    .map_err(PublishError::classify_write)?;
                // Write extension
                if !ext_bytes.is_empty() {
                    fs::write_all(tmp_fd.as_raw_fd(), ext_bytes)
                        .map_err(PublishError::classify_write)?;
                }
                // Write payload
                if !payload.is_empty() {
                    fs::write_all(tmp_fd.as_raw_fd(), payload)
                        .map_err(PublishError::classify_write)?;
                }
                // Patch header with final values
                fs::pwrite_all(tmp_fd.as_raw_fd(), &header_bytes, 0)
                    .map_err(PublishError::classify_write)?;
                // fsync file
                fs::fsync(tmp_fd.as_raw_fd()).map_err(PublishError::classify_post_fsync)?;

                // Publish via linkat
                // Try AT_EMPTY_PATH first, then proc/self/fd
                if fs::linkat_empty_path(tmp_fd.as_raw_fd(), dest_fd.as_raw_fd(), dest_name).is_ok()
                {
                    // Sync destination directory
                    fs::fsync_dir_fd(dest_fd.as_raw_fd())
                        .map_err(PublishError::classify_post_fsync)?;
                    return Ok(());
                }

                if fs::linkat_proc_self_fd(tmp_fd.as_raw_fd(), dest_fd.as_raw_fd(), dest_name)
                    .is_ok()
                {
                    fs::fsync_dir_fd(dest_fd.as_raw_fd())
                        .map_err(PublishError::classify_post_fsync)?;
                    return Ok(());
                }

                // Fall back to named temp file
                self.named_fallback(dest_dir_relative, dest_name, header, ext_bytes, payload)
            }
            Err(_) => self.named_fallback(dest_dir_relative, dest_name, header, ext_bytes, payload),
        }
    }

    /// Named temporary file fallback for enqueue.
    fn named_fallback(
        &self,
        dest_dir_relative: &str,
        dest_name: &str,
        header: &FixedHeader,
        ext_bytes: &[u8],
        payload: &[u8],
    ) -> Result<(), PublishError> {
        // Ensure tmp/<boot-id>/<shard>/ exists
        // Extract shard from dest_dir
        let shard_part = dest_dir_relative.rsplit('/').next().unwrap_or("0000");
        let tmp_dir = format!("tmp/{}/{}", self.boot_id, shard_part);

        self.ensure_dir(&tmp_dir)
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;

        let tmp_dir_fd = open_relative(self.root_fd.as_raw_fd(), &tmp_dir)
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;

        // Create temp file name
        let boottime = fs::clock_boottime_ns().unwrap_or(0);
        let random = fs::random_128bit().unwrap_or([0; 16]);
        let temp_name = temp_filename(boottime, &random);

        let tmp_file = fs::create_exclusive(tmp_dir_fd.as_raw_fd(), &temp_name, 0o600)
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;

        // Write header
        let header_bytes = header.encode(ext_bytes);
        fs::write_all(tmp_file.as_raw_fd(), &header_bytes).map_err(PublishError::classify_write)?;
        if !ext_bytes.is_empty() {
            fs::write_all(tmp_file.as_raw_fd(), ext_bytes).map_err(PublishError::classify_write)?;
        }
        if !payload.is_empty() {
            fs::write_all(tmp_file.as_raw_fd(), payload).map_err(PublishError::classify_write)?;
        }
        // Patch header
        fs::pwrite_all(tmp_file.as_raw_fd(), &header_bytes, 0)
            .map_err(PublishError::classify_write)?;
        // fsync file
        fs::fsync(tmp_file.as_raw_fd()).map_err(PublishError::classify_post_fsync)?;

        // Open destination directory for rename
        let dest_fd = open_relative(self.root_fd.as_raw_fd(), dest_dir_relative)
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;

        // Rename with NOREPLACE
        match fs::renameat2_noreplace(
            tmp_dir_fd.as_raw_fd(),
            &temp_name,
            dest_fd.as_raw_fd(),
            dest_name,
        ) {
            Ok(()) => {
                // Sync destination first, then source
                fs::fsync_dir_fd(dest_fd.as_raw_fd()).map_err(PublishError::classify_post_fsync)?;
                fs::fsync_dir_fd(tmp_dir_fd.as_raw_fd())
                    .map_err(PublishError::classify_post_fsync)?;
                Ok(())
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                // Clean up temp file
                let _ = fs::unlinkat(tmp_dir_fd.as_raw_fd(), &temp_name);
                Err(PublishError::NotCommitted(Error::IdentityCollision))
            }
            Err(e) => {
                let _ = fs::unlinkat(tmp_dir_fd.as_raw_fd(), &temp_name);
                Err(PublishError::classify_write(e))
            }
        }
    }

    /// Create a directory path recursively, syncing parents.
    pub(crate) fn ensure_dir(&self, relative: &str) -> io::Result<()> {
        let components: Vec<&str> = relative.split('/').filter(|s| !s.is_empty()).collect();
        let mut current_fd = self.root_fd.as_raw_fd();
        let mut owned_fds = Vec::new();

        for (i, comp) in components.iter().enumerate() {
            match fs::mkdirat_eexist_ok(current_fd, comp, 0o700)? {
                true => {}
                false => {
                    // EEXIST: verify it's a directory and sync parent
                }
            }
            // Open the child and verify
            let child = fs::open_directory(current_fd, comp)?;
            // Sync parent before using child
            if i > 0 {
                fs::fsync_dir_fd(current_fd)?;
            } else {
                // First component: sync root
                fs::fsync_dir_fd(self.root_fd.as_raw_fd())?;
            }
            current_fd = child.as_raw_fd();
            owned_fds.push(child);
        }
        Ok(())
    }
    /// Claim a ready job, returning a lease.
    pub fn lease(&mut self, _max_wait_ns: u64, lease_duration_ns: u64) -> LeaseOutcome {
        if let Err(e) = self.check_not_poisoned() {
            return LeaseOutcome::NotCommitted(e);
        }

        // Validate lease duration: 1s to 7d
        let min_dur = 1_000_000_000u64;
        let max_dur = 7 * 24 * 60 * 60 * 1_000_000_000u64;
        if lease_duration_ns < min_dur || lease_duration_ns > max_dur {
            return LeaseOutcome::NotCommitted(Error::InvalidInput(
                "lease duration must be 1s to 7d".into(),
            ));
        }

        let boottime_now = match fs::clock_boottime_ns() {
            Ok(t) => t,
            Err(e) => return LeaseOutcome::NotCommitted(Error::IoFailure(e.to_string())),
        };
        let wall_now = match fs::clock_realtime_ns() {
            Ok(t) => t,
            Err(e) => return LeaseOutcome::NotCommitted(Error::IoFailure(e.to_string())),
        };

        // Scan shards for a ready job
        let scan_round = 0u64;
        let (start, stride) = spoolq_names::shard_scan_params(
            &self.format.queue_id,
            &self.boot_id_bytes,
            &self.worker_nonce,
            scan_round,
            self.format.shard_count,
        );

        for i in 0..self.format.shard_count {
            let shard = spoolq_names::shard_at(start, stride, i, self.format.shard_count);
            let shard_str = shard_hex(shard);

            // Open the ready shard directory
            let ready_dir = format!("ready/{}", shard_str);
            let shard_fd = match open_relative(self.root_fd.as_raw_fd(), &ready_dir) {
                Ok(fd) => fd,
                Err(_) => continue,
            };

            // List entries
            let entries = match fs::read_dir_entries_owned(shard_fd.as_raw_fd()) {
                Ok(e) => e,
                Err(_) => continue,
            };

            for entry in &entries {
                if !entry.ends_with(".sqj") {
                    continue;
                }

                // Parse and verify the ready filename
                let parsed = match spoolq_names::parse_ready(entry) {
                    Ok(p) => p,
                    Err(_) => continue,
                };

                // Verify name tag
                let base = format!(
                    "{}.g{:016x}.a{:08x}.m{:08x}",
                    spoolq_names::hex_encode(&parsed.common.job_id),
                    parsed.common.generation,
                    parsed.common.attempt,
                    parsed.common.maximum_attempts,
                );
                let ctx = ready_context(&shard_str, &base);
                let expected_tag = compute_name_tag(&self.format.queue_id, &ctx);
                if expected_tag != parsed.tag {
                    continue;
                }

                // Verify shard matches job_id
                let computed_shard = compute_shard(
                    &self.format.queue_id,
                    &parsed.common.job_id,
                    self.format.shard_count,
                );
                if computed_shard != shard {
                    continue;
                }

                // Check attempt limit
                if parsed.common.attempt >= parsed.common.maximum_attempts {
                    // Move to dead
                    let _ = self.move_to_dead(
                        &ready_dir,
                        entry,
                        &parsed.common,
                        DeadReason::AttemptsExhausted,
                    );
                    continue;
                }

                // Attempt claim: rename ready -> leased
                let lease_token = fs::random_128bit().unwrap_or([0; 16]);
                let boottime_deadline = boottime_now.saturating_add(lease_duration_ns);
                let wall_deadline = wall_now.saturating_add(lease_duration_ns);
                let lease_bucket =
                    spoolq_math::lease_bucket(boottime_deadline, self.format.lease_bucket_width_ns);
                let bucket_str = bucket_hex(lease_bucket);

                let new_generation = parsed.common.generation.wrapping_add(1);
                let new_attempt = parsed.common.attempt.wrapping_add(1);

                let leased_common = CommonFields {
                    job_id: parsed.common.job_id,
                    generation: new_generation,
                    attempt: new_attempt,
                    maximum_attempts: parsed.common.maximum_attempts,
                };

                // Build leased filename
                let leased_base = format!(
                    "{}.g{:016x}.a{:08x}.m{:08x}.b{:016x}.w{:016x}.t{}",
                    spoolq_names::hex_encode(&leased_common.job_id),
                    leased_common.generation,
                    leased_common.attempt,
                    leased_common.maximum_attempts,
                    boottime_deadline,
                    wall_deadline,
                    spoolq_names::hex_encode(&lease_token),
                );
                let leased_ctx = spoolq_names::leased_context(
                    &self.boot_id,
                    &bucket_str,
                    &shard_str,
                    &leased_base,
                );
                let leased_tag = compute_name_tag(&self.format.queue_id, &leased_ctx);
                let leased_name = spoolq_names::leased_filename(
                    &leased_common,
                    boottime_deadline,
                    wall_deadline,
                    &lease_token,
                    &leased_tag,
                );

                // Ensure lease directory exists
                let leased_dir = format!("leased/{}/{}/{}", self.boot_id, bucket_str, shard_str);
                if let Err(e) = self.ensure_dir(&leased_dir) {
                    let _ = e;
                    continue;
                }

                let leased_dir_fd = match open_relative(self.root_fd.as_raw_fd(), &leased_dir) {
                    Ok(fd) => fd,
                    Err(_) => continue,
                };

                // Rename ready -> leased with NOREPLACE
                match fs::renameat2_noreplace(
                    shard_fd.as_raw_fd(),
                    entry,
                    leased_dir_fd.as_raw_fd(),
                    &leased_name,
                ) {
                    Ok(()) => {
                        // Sync both directories
                        let same_dir = false; // different directories
                        if !same_dir {
                            if fs::fsync_dir_fd(leased_dir_fd.as_raw_fd()).is_err() {
                                self.poison();
                                return LeaseOutcome::OutcomeUnknown(TransitionTicket {
                                    job_id: parsed.common.job_id,
                                    source_state: "ready".into(),
                                    source_generation: parsed.common.generation,
                                    source_attempt: parsed.common.attempt,
                                    source_relative_path: format!("{}/{}", ready_dir, entry),
                                    attempted_destination_state: "leased".into(),
                                    attempted_destination_relative_path: format!(
                                        "{}/{}",
                                        leased_dir, leased_name
                                    ),
                                    lease_token: Some(lease_token),
                                    envelope_digest: [0; 32],
                                });
                            }
                            if fs::fsync_dir_fd(shard_fd.as_raw_fd()).is_err() {
                                self.poison();
                                return LeaseOutcome::OutcomeUnknown(TransitionTicket {
                                    job_id: parsed.common.job_id,
                                    source_state: "ready".into(),
                                    source_generation: parsed.common.generation,
                                    source_attempt: parsed.common.attempt,
                                    source_relative_path: format!("{}/{}", ready_dir, entry),
                                    attempted_destination_state: "leased".into(),
                                    attempted_destination_relative_path: format!(
                                        "{}/{}",
                                        leased_dir, leased_name
                                    ),
                                    lease_token: Some(lease_token),
                                    envelope_digest: [0; 32],
                                });
                            }
                        }

                        // Post-rename: open and verify the leased object
                        let leased_stat = match fs::fstatat(leased_dir_fd.as_raw_fd(), &leased_name)
                        {
                            Ok(s) => s,
                            Err(_) => continue,
                        };

                        // Verify link count is exactly 1 (rejects external hard links)
                        if leased_stat.st_nlink != 1 {
                            continue;
                        }

                        // Read and validate the fixed header
                        let leased_file = match fs::openat(
                            leased_dir_fd.as_raw_fd(),
                            &leased_name,
                            0o0, // O_RDONLY
                            0,
                        ) {
                            Ok(f) => f,
                            Err(_) => continue,
                        };

                        let mut header_buf = [0u8; 128];
                        if fs::pread(leased_file.as_raw_fd(), &mut header_buf, 0).unwrap_or(0)
                            != 128
                        {
                            continue;
                        }

                        let header = match FixedHeader::decode(&header_buf) {
                            Ok(h) => h,
                            Err(_) => continue,
                        };

                        // Verify job_id matches
                        if header.job_id != parsed.common.job_id {
                            continue;
                        }

                        let lease_info = LeaseInfo {
                            job_id: parsed.common.job_id,
                            envelope_digest: header.envelope_digest,
                            generation: new_generation,
                            attempt: new_attempt,
                            maximum_attempts: parsed.common.maximum_attempts,
                            token: lease_token,
                            boot_id: self.boot_id.clone(),
                            expires_boottime_ns: boottime_deadline,
                            expires_wall_ns: wall_deadline,
                            content_type: String::new(),
                            payload_length: header.payload_length,
                            payload_digest: header.payload_digest,
                            expected_dev: leased_stat.st_dev as u64,
                            expected_inode: leased_stat.st_ino as u64,
                            exact_source_path: format!("{}/{}", leased_dir, leased_name),
                            payload_verified: false,
                        };

                        return LeaseOutcome::Leased(lease_info);
                    }
                    Err(_) => continue,
                }
            }
        }

        LeaseOutcome::Empty
    }

    /// Acknowledge a lease: move to terminal receipt.
    pub fn ack(&mut self, lease: &LeaseInfo) -> AckOutcome {
        if let Err(e) = self.check_not_poisoned() {
            return AckOutcome::NotCommitted(e);
        }

        // Compute receipt path
        let wall_now = fs::clock_realtime_ns().unwrap_or(0);
        let terminal_bucket =
            spoolq_math::bucket_number(wall_now, self.format.terminal_bucket_width_ns);
        let bucket_str = bucket_hex(terminal_bucket);

        let shard = compute_shard(
            &self.format.queue_id,
            &lease.job_id,
            self.format.shard_count,
        );
        let shard_str = shard_hex(shard);

        let receipt_common = CommonFields {
            job_id: lease.job_id,
            generation: lease.generation.wrapping_add(1),
            attempt: lease.attempt,
            maximum_attempts: lease.maximum_attempts,
        };

        // Build receipt filename
        let receipt_base = format!(
            "{}.g{:016x}.a{:08x}.m{:08x}.t{}",
            spoolq_names::hex_encode(&receipt_common.job_id),
            receipt_common.generation,
            receipt_common.attempt,
            receipt_common.maximum_attempts,
            spoolq_names::hex_encode(&lease.token),
        );
        let receipt_ctx = spoolq_names::terminal_context(
            spoolq_names::State::Receipt,
            &bucket_str,
            &shard_str,
            &receipt_base,
        );
        let receipt_tag = compute_name_tag(&self.format.queue_id, &receipt_ctx);
        let receipt_name =
            spoolq_names::receipt_filename(&receipt_common, &lease.token, &receipt_tag);

        let receipt_dir = format!("receipts/{}/{}", bucket_str, shard_str);
        if let Err(e) = self.ensure_dir(&receipt_dir) {
            return AckOutcome::NotCommitted(Error::IoFailure(e.to_string()));
        }

        let receipt_dir_fd = match open_relative(self.root_fd.as_raw_fd(), &receipt_dir) {
            Ok(fd) => fd,
            Err(e) => return AckOutcome::NotCommitted(Error::IoFailure(e.to_string())),
        };

        // Open source leased directory
        let src_path_parts: Vec<&str> = lease.exact_source_path.split('/').collect();
        if src_path_parts.len() < 2 {
            return AckOutcome::NotCommitted(Error::InvalidInput("bad source path".into()));
        }
        let src_name = src_path_parts.last().unwrap();
        let src_dir = src_path_parts[..src_path_parts.len() - 1].join("/");
        let src_dir_fd = match open_relative(self.root_fd.as_raw_fd(), &src_dir) {
            Ok(fd) => fd,
            Err(_) => return AckOutcome::LeaseLost,
        };

        // Rename leased -> receipt with NOREPLACE
        match fs::renameat2_noreplace(
            src_dir_fd.as_raw_fd(),
            src_name,
            receipt_dir_fd.as_raw_fd(),
            &receipt_name,
        ) {
            Ok(()) => {
                // Sync both directories
                if fs::fsync_dir_fd(receipt_dir_fd.as_raw_fd()).is_err() {
                    self.poison();
                    return AckOutcome::OutcomeUnknown(TransitionTicket {
                        job_id: lease.job_id,
                        source_state: "leased".into(),
                        source_generation: lease.generation,
                        source_attempt: lease.attempt,
                        source_relative_path: lease.exact_source_path.clone(),
                        attempted_destination_state: "receipts".into(),
                        attempted_destination_relative_path: format!(
                            "{}/{}",
                            receipt_dir, receipt_name
                        ),
                        lease_token: Some(lease.token),
                        envelope_digest: lease.envelope_digest,
                    });
                }
                if fs::fsync_dir_fd(src_dir_fd.as_raw_fd()).is_err() {
                    self.poison();
                    return AckOutcome::OutcomeUnknown(TransitionTicket {
                        job_id: lease.job_id,
                        source_state: "leased".into(),
                        source_generation: lease.generation,
                        source_attempt: lease.attempt,
                        source_relative_path: lease.exact_source_path.clone(),
                        attempted_destination_state: "receipts".into(),
                        attempted_destination_relative_path: format!(
                            "{}/{}",
                            receipt_dir, receipt_name
                        ),
                        lease_token: Some(lease.token),
                        envelope_digest: lease.envelope_digest,
                    });
                }
                AckOutcome::Acked
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => AckOutcome::AlreadyAcked,
            Err(e) if e.raw_os_error() == Some(libc::ENOENT) => AckOutcome::LeaseLost,
            Err(_) => AckOutcome::LeaseLost,
        }
    }

    /// Retry a lease immediately (move to ready).
    pub fn retry_now(&mut self, lease: &LeaseInfo) -> TransitionOutcome {
        self.retry(lease, None)
    }

    /// Retry a lease at a future time (move to delayed).
    pub fn retry_at(&mut self, lease: &LeaseInfo, not_before_ns: u64) -> TransitionOutcome {
        self.retry(lease, Some(not_before_ns))
    }

    /// Retry a lease after a duration.
    pub fn retry_after(&mut self, lease: &LeaseInfo, duration_ns: u64) -> TransitionOutcome {
        let wall_now = self.effective_wall_floor_ns();
        let deadline = match spoolq_math::retry_wall_deadline(wall_now, duration_ns / 1_000_000) {
            Some(d) => d,
            None => {
                return TransitionOutcome::NotCommitted(Error::InvalidInput(
                    "deadline overflow".into(),
                ))
            }
        };
        self.retry_at(lease, deadline)
    }

    /// Retry with a policy (computes delay from attempt and policy).
    pub fn retry_with_policy(
        &mut self,
        lease: &LeaseInfo,
        policy: &spoolq_math::RetryPolicy,
    ) -> TransitionOutcome {
        if let Err(e) = policy.validate() {
            return TransitionOutcome::NotCommitted(Error::InvalidInput(e.to_string()));
        }

        let delay_ms = match spoolq_math::retry_delay_ms(
            &self.format.queue_id,
            &lease.job_id,
            lease.attempt,
            policy,
        ) {
            Ok(d) => d,
            Err(e) => return TransitionOutcome::NotCommitted(Error::InvalidInput(e.to_string())),
        };

        if delay_ms == 0 {
            self.retry_now(lease)
        } else {
            self.retry_after(lease, delay_ms * 1_000_000)
        }
    }

    fn retry(&mut self, lease: &LeaseInfo, delayed_ns: Option<u64>) -> TransitionOutcome {
        if let Err(e) = self.check_not_poisoned() {
            return TransitionOutcome::NotCommitted(e);
        }

        // Check attempt limit for retry
        if lease.attempt >= lease.maximum_attempts {
            // Move to dead with attempts_exhausted
            return match self.bury_internal(lease, DeadReason::AttemptsExhausted) {
                TransitionOutcome::Committed => TransitionOutcome::Committed,
                other => other,
            };
        }

        let shard = compute_shard(
            &self.format.queue_id,
            &lease.job_id,
            self.format.shard_count,
        );
        let shard_str = shard_hex(shard);
        let new_gen = lease.generation.wrapping_add(1);

        let (dest_dir, dest_name) = match delayed_ns {
            Some(nb) => {
                let (elig_bucket, _) = match spoolq_math::eligibility_bucket_and_ns(
                    nb,
                    self.format.delayed_bucket_width_ns,
                ) {
                    Some(v) => v,
                    None => {
                        return TransitionOutcome::NotCommitted(Error::InvalidInput(
                            "eligibility overflow".into(),
                        ))
                    }
                };
                let bucket_str = bucket_hex(elig_bucket);
                let common = CommonFields {
                    job_id: lease.job_id,
                    generation: new_gen,
                    attempt: lease.attempt,
                    maximum_attempts: lease.maximum_attempts,
                };
                let base = format!(
                    "{}.g{:016x}.a{:08x}.m{:08x}.d{:016x}",
                    spoolq_names::hex_encode(&lease.job_id),
                    new_gen,
                    lease.attempt,
                    lease.maximum_attempts,
                    nb,
                );
                let ctx = spoolq_names::delayed_context(&bucket_str, &shard_str, &base);
                let tag = compute_name_tag(&self.format.queue_id, &ctx);
                let fname = spoolq_names::delayed_filename(&common, nb, &tag);
                let dir = format!("delayed/{}/{}", bucket_str, shard_str);
                (dir, fname)
            }
            None => {
                let common = CommonFields {
                    job_id: lease.job_id,
                    generation: new_gen,
                    attempt: lease.attempt,
                    maximum_attempts: lease.maximum_attempts,
                };
                let base = format!(
                    "{}.g{:016x}.a{:08x}.m{:08x}",
                    spoolq_names::hex_encode(&lease.job_id),
                    new_gen,
                    lease.attempt,
                    lease.maximum_attempts,
                );
                let ctx = ready_context(&shard_str, &base);
                let tag = compute_name_tag(&self.format.queue_id, &ctx);
                let fname = ready_filename(&common, &tag);
                (format!("ready/{}", shard_str), fname)
            }
        };

        self.move_leased(lease, &dest_dir, &dest_name)
    }

    /// Bury a lease (move to dead).
    pub fn bury(&mut self, lease: &LeaseInfo, reason: DeadReason) -> TransitionOutcome {
        if let Err(e) = self.check_not_poisoned() {
            return TransitionOutcome::NotCommitted(e);
        }
        self.bury_internal(lease, reason)
    }

    fn bury_internal(&mut self, lease: &LeaseInfo, reason: DeadReason) -> TransitionOutcome {
        let shard = compute_shard(
            &self.format.queue_id,
            &lease.job_id,
            self.format.shard_count,
        );
        let shard_str = shard_hex(shard);
        let new_gen = lease.generation.wrapping_add(1);

        let wall_now = fs::clock_realtime_ns().unwrap_or(0);
        let terminal_bucket =
            spoolq_math::bucket_number(wall_now, self.format.terminal_bucket_width_ns);
        let bucket_str = bucket_hex(terminal_bucket);

        let common = CommonFields {
            job_id: lease.job_id,
            generation: new_gen,
            attempt: lease.attempt,
            maximum_attempts: lease.maximum_attempts,
        };

        let base = format!(
            "{}.g{:016x}.a{:08x}.m{:08x}.x{:04x}",
            spoolq_names::hex_encode(&lease.job_id),
            new_gen,
            lease.attempt,
            lease.maximum_attempts,
            reason as u16,
        );
        let ctx = spoolq_names::terminal_context(
            spoolq_names::State::Dead,
            &bucket_str,
            &shard_str,
            &base,
        );
        let tag = compute_name_tag(&self.format.queue_id, &ctx);
        let fname = spoolq_names::dead_filename(&common, reason as u16, &tag);
        let dest_dir = format!("dead/{}/{}", bucket_str, shard_str);

        self.move_leased(lease, &dest_dir, &fname)
    }

    /// Renew a lease with a new deadline.
    pub fn renew(&mut self, lease: &LeaseInfo, lease_duration_ns: u64) -> RenewOutcome {
        if let Err(e) = self.check_not_poisoned() {
            return RenewOutcome::NotCommitted(e);
        }

        let min_dur = 1_000_000_000u64;
        let max_dur = 7 * 24 * 60 * 60 * 1_000_000_000u64;
        if lease_duration_ns < min_dur || lease_duration_ns > max_dur {
            return RenewOutcome::NotCommitted(Error::InvalidInput(
                "lease duration must be 1s to 7d".into(),
            ));
        }

        let boottime_now = fs::clock_boottime_ns().unwrap_or(0);
        let wall_now = fs::clock_realtime_ns().unwrap_or(0);
        let new_boottime_dl = boottime_now.saturating_add(lease_duration_ns);
        let new_wall_dl = wall_now.saturating_add(lease_duration_ns);
        let new_gen = lease.generation.wrapping_add(1);

        let lease_bucket =
            spoolq_math::lease_bucket(new_boottime_dl, self.format.lease_bucket_width_ns);
        let bucket_str = bucket_hex(lease_bucket);
        let shard = compute_shard(
            &self.format.queue_id,
            &lease.job_id,
            self.format.shard_count,
        );
        let shard_str = shard_hex(shard);

        let common = CommonFields {
            job_id: lease.job_id,
            generation: new_gen,
            attempt: lease.attempt,
            maximum_attempts: lease.maximum_attempts,
        };

        let base = format!(
            "{}.g{:016x}.a{:08x}.m{:08x}.b{:016x}.w{:016x}.t{}",
            spoolq_names::hex_encode(&lease.job_id),
            new_gen,
            lease.attempt,
            lease.maximum_attempts,
            new_boottime_dl,
            new_wall_dl,
            spoolq_names::hex_encode(&lease.token),
        );
        let ctx = spoolq_names::leased_context(&self.boot_id, &bucket_str, &shard_str, &base);
        let tag = compute_name_tag(&self.format.queue_id, &ctx);
        let fname = spoolq_names::leased_filename(
            &common,
            new_boottime_dl,
            new_wall_dl,
            &lease.token,
            &tag,
        );
        let dest_dir = format!("leased/{}/{}/{}", self.boot_id, bucket_str, shard_str);

        match self.move_leased_renew(lease, &dest_dir, &fname) {
            TransitionOutcome::Committed => RenewOutcome::Renewed(LeaseInfo {
                generation: new_gen,
                expires_boottime_ns: new_boottime_dl,
                expires_wall_ns: new_wall_dl,
                exact_source_path: format!("{}/{}", dest_dir, fname),
                ..lease.clone()
            }),
            TransitionOutcome::LeaseLost => RenewOutcome::LeaseLost,
            TransitionOutcome::NotCommitted(e) => RenewOutcome::NotCommitted(e),
            TransitionOutcome::OutcomeUnknown(t) => RenewOutcome::OutcomeUnknown(t),
        }
    }

    /// Internal: move a leased object to a new state directory.
    fn move_leased(
        &mut self,
        lease: &LeaseInfo,
        dest_dir: &str,
        dest_name: &str,
    ) -> TransitionOutcome {
        if let Err(e) = self.ensure_dir(dest_dir) {
            return TransitionOutcome::NotCommitted(Error::IoFailure(e.to_string()));
        }

        let dest_dir_fd = match open_relative(self.root_fd.as_raw_fd(), dest_dir) {
            Ok(fd) => fd,
            Err(e) => return TransitionOutcome::NotCommitted(Error::IoFailure(e.to_string())),
        };

        // Parse source path
        let src_parts: Vec<&str> = lease.exact_source_path.split('/').collect();
        let src_name = match src_parts.last() {
            Some(n) => *n,
            None => {
                return TransitionOutcome::NotCommitted(Error::InvalidInput(
                    "bad source path".into(),
                ))
            }
        };
        let src_dir = src_parts[..src_parts.len() - 1].join("/");
        let src_dir_fd = match open_relative(self.root_fd.as_raw_fd(), &src_dir) {
            Ok(fd) => fd,
            Err(_) => return TransitionOutcome::LeaseLost,
        };

        match fs::renameat2_noreplace(
            src_dir_fd.as_raw_fd(),
            src_name,
            dest_dir_fd.as_raw_fd(),
            dest_name,
        ) {
            Ok(()) => {
                let src_same = src_dir == dest_dir;
                if src_same {
                    if fs::fsync_dir_fd(dest_dir_fd.as_raw_fd()).is_err() {
                        self.poison();
                        return TransitionOutcome::OutcomeUnknown(
                            self.transition_ticket(lease, dest_dir, dest_name),
                        );
                    }
                } else {
                    if fs::fsync_dir_fd(dest_dir_fd.as_raw_fd()).is_err() {
                        self.poison();
                        return TransitionOutcome::OutcomeUnknown(
                            self.transition_ticket(lease, dest_dir, dest_name),
                        );
                    }
                    if fs::fsync_dir_fd(src_dir_fd.as_raw_fd()).is_err() {
                        self.poison();
                        return TransitionOutcome::OutcomeUnknown(
                            self.transition_ticket(lease, dest_dir, dest_name),
                        );
                    }
                }
                TransitionOutcome::Committed
            }
            Err(e) if e.raw_os_error() == Some(libc::ENOENT) => TransitionOutcome::LeaseLost,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                TransitionOutcome::NotCommitted(Error::QueueCorrupt("destination exists".into()))
            }
            Err(e) => TransitionOutcome::NotCommitted(Error::IoFailure(e.to_string())),
        }
    }

    /// Same as move_leased but for renewal (same token, same attempt).
    fn move_leased_renew(
        &mut self,
        lease: &LeaseInfo,
        dest_dir: &str,
        dest_name: &str,
    ) -> TransitionOutcome {
        self.move_leased(lease, dest_dir, dest_name)
    }

    fn transition_ticket(
        &self,
        lease: &LeaseInfo,
        dest_dir: &str,
        dest_name: &str,
    ) -> TransitionTicket {
        TransitionTicket {
            job_id: lease.job_id,
            source_state: "leased".into(),
            source_generation: lease.generation,
            source_attempt: lease.attempt,
            source_relative_path: lease.exact_source_path.clone(),
            attempted_destination_state: dest_dir.split('/').next().unwrap_or("").into(),
            attempted_destination_relative_path: format!("{}/{}", dest_dir, dest_name),
            lease_token: Some(lease.token),
            envelope_digest: lease.envelope_digest,
        }
    }

    /// Move a ready object to dead (for exhausted attempts cleanup).
    fn move_to_dead(
        &mut self,
        ready_dir: &str,
        ready_name: &str,
        common: &CommonFields,
        reason: DeadReason,
    ) -> Result<(), io::Error> {
        let shard_str = ready_dir.rsplit('/').next().unwrap_or("0000");
        let wall_now = fs::clock_realtime_ns().unwrap_or(0);
        let terminal_bucket =
            spoolq_math::bucket_number(wall_now, self.format.terminal_bucket_width_ns);
        let bucket_str = bucket_hex(terminal_bucket);

        let new_gen = common.generation.wrapping_add(1);
        let dead_common = CommonFields {
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
        let ctx = spoolq_names::terminal_context(
            spoolq_names::State::Dead,
            &bucket_str,
            shard_str,
            &base,
        );
        let tag = compute_name_tag(&self.format.queue_id, &ctx);
        let dead_name = spoolq_names::dead_filename(&dead_common, reason as u16, &tag);
        let dead_dir = format!("dead/{}/{}", bucket_str, shard_str);

        let _ = self.ensure_dir(&dead_dir);
        let dead_dir_fd = open_relative(self.root_fd.as_raw_fd(), &dead_dir)?;
        let ready_dir_fd = open_relative(self.root_fd.as_raw_fd(), ready_dir)?;

        fs::renameat2_noreplace(
            ready_dir_fd.as_raw_fd(),
            ready_name,
            dead_dir_fd.as_raw_fd(),
            &dead_name,
        )?;
        fs::fsync_dir_fd(dead_dir_fd.as_raw_fd())?;
        fs::fsync_dir_fd(ready_dir_fd.as_raw_fd())?;
        Ok(())
    }
    /// Read and verify the payload of a leased job.
    /// Returns Ok(()) if the digest matches, Err with PayloadCorrupt otherwise.
    pub fn verify_lease_payload(&self, lease: &LeaseInfo) -> Result<(), Error> {
        let parts: Vec<&str> = lease.exact_source_path.split('/').collect();
        if parts.is_empty() {
            return Err(Error::InvalidInput("bad source path".into()));
        }
        let src_name = parts.last().unwrap();
        let src_dir = parts[..parts.len() - 1].join("/");

        let src_dir_fd = open_relative(self.root_fd.as_raw_fd(), &src_dir)
            .map_err(|e| Error::IoFailure(e.to_string()))?;

        let file_fd = fs::openat(src_dir_fd.as_raw_fd(), src_name, 0o0, 0)
            .map_err(|e| Error::IoFailure(e.to_string()))?;

        // Skip header (128 bytes) and extension
        let header_size = 128usize;

        // Read the fixed header to get extension_header_length
        let mut header_buf = [0u8; 128];
        let n = fs::pread(file_fd.as_raw_fd(), &mut header_buf, 0)
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        if n != 128 {
            return Err(Error::QueueCorrupt("could not read header".into()));
        }

        let header =
            FixedHeader::decode(&header_buf).map_err(|e| Error::QueueCorrupt(e.to_string()))?;
        let data_offset = header_size + header.extension_header_length as usize;

        // Read and hash the payload
        let mut hasher = sha2::Sha256::new();
        let mut offset = data_offset as u64;
        let mut remaining = header.payload_length as usize;
        let mut buf = vec![0u8; 65536];

        while remaining > 0 {
            let to_read = remaining.min(buf.len());
            let n = fs::pread(file_fd.as_raw_fd(), &mut buf[..to_read], offset)
                .map_err(|e| Error::IoFailure(e.to_string()))?;
            if n == 0 {
                return Err(Error::QueueCorrupt("unexpected EOF".into()));
            }
            hasher.update(&buf[..n]);
            offset += n as u64;
            remaining -= n;
        }

        let computed: [u8; 32] = hasher.finalize().into();
        if computed != header.payload_digest {
            return Err(Error::PayloadCorrupt);
        }

        Ok(())
    }
    /// Diagnostic lookup: find all states for a job_id.
    /// Scans active and terminal states for the computed shard.
    pub fn inspect(&self, job_id: &[u8; 16]) -> Vec<Snapshot> {
        let mut results = Vec::new();
        let shard = compute_shard(&self.format.queue_id, job_id, self.format.shard_count);
        let shard_str = shard_hex(shard);

        // Check ready
        let ready_dir = format!("ready/{}", shard_str);
        if let Ok(dir_fd) = open_relative(self.root_fd.as_raw_fd(), &ready_dir) {
            if let Ok(entries) = fs::read_dir_entries_owned(dir_fd.as_raw_fd()) {
                for entry in entries {
                    if let Ok(parsed) = spoolq_names::parse_ready(&entry) {
                        if parsed.common.job_id == *job_id {
                            results.push(Snapshot {
                                job_id: *job_id,
                                state: "ready".into(),
                                generation: parsed.common.generation,
                                attempt: parsed.common.attempt,
                                maximum_attempts: parsed.common.maximum_attempts,
                                shard,
                                relative_path: format!("{}/{}", ready_dir, entry),
                                size: 0,
                            });
                        }
                    }
                }
            }
        }

        // Check leased (scan boot dirs)
        if let Ok(leased_root) = fs::open_directory(self.root_fd.as_raw_fd(), "leased") {
            if let Ok(boot_dirs) = fs::read_dir_entries_owned(leased_root.as_raw_fd()) {
                for boot_dir in boot_dirs {
                    let boot_path = format!("leased/{}", boot_dir);
                    if let Ok(boot_fd) = open_relative(self.root_fd.as_raw_fd(), &boot_path) {
                        if let Ok(bucket_dirs) = fs::read_dir_entries_owned(boot_fd.as_raw_fd()) {
                            for bucket_dir in bucket_dirs {
                                let shard_path =
                                    format!("{}/{}/{}", boot_path, bucket_dir, shard_str);
                                if let Ok(shard_fd) =
                                    open_relative(self.root_fd.as_raw_fd(), &shard_path)
                                {
                                    if let Ok(entries) =
                                        fs::read_dir_entries_owned(shard_fd.as_raw_fd())
                                    {
                                        for entry in entries {
                                            if let Ok(parsed) = spoolq_names::parse_leased(&entry) {
                                                if parsed.common.job_id == *job_id {
                                                    results.push(Snapshot {
                                                        job_id: *job_id,
                                                        state: "leased".into(),
                                                        generation: parsed.common.generation,
                                                        attempt: parsed.common.attempt,
                                                        maximum_attempts: parsed
                                                            .common
                                                            .maximum_attempts,
                                                        shard,
                                                        relative_path: format!(
                                                            "{}/{}",
                                                            shard_path, entry
                                                        ),
                                                        size: 0,
                                                    });
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Check delayed
        if let Ok(delayed_root) = fs::open_directory(self.root_fd.as_raw_fd(), "delayed") {
            if let Ok(bucket_dirs) = fs::read_dir_entries_owned(delayed_root.as_raw_fd()) {
                for bucket_dir in bucket_dirs {
                    let shard_path = format!("delayed/{}/{}", bucket_dir, shard_str);
                    if let Ok(shard_fd) = open_relative(self.root_fd.as_raw_fd(), &shard_path) {
                        if let Ok(entries) = fs::read_dir_entries_owned(shard_fd.as_raw_fd()) {
                            for entry in entries {
                                if let Ok(parsed) = spoolq_names::parse_delayed(&entry) {
                                    if parsed.common.job_id == *job_id {
                                        results.push(Snapshot {
                                            job_id: *job_id,
                                            state: "delayed".into(),
                                            generation: parsed.common.generation,
                                            attempt: parsed.common.attempt,
                                            maximum_attempts: parsed.common.maximum_attempts,
                                            shard,
                                            relative_path: format!("{}/{}", shard_path, entry),
                                            size: 0,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Check dead
        if let Ok(dead_root) = fs::open_directory(self.root_fd.as_raw_fd(), "dead") {
            if let Ok(bucket_dirs) = fs::read_dir_entries_owned(dead_root.as_raw_fd()) {
                for bucket_dir in bucket_dirs {
                    let shard_path = format!("dead/{}/{}", bucket_dir, shard_str);
                    if let Ok(shard_fd) = open_relative(self.root_fd.as_raw_fd(), &shard_path) {
                        if let Ok(entries) = fs::read_dir_entries_owned(shard_fd.as_raw_fd()) {
                            for entry in entries {
                                if let Ok(parsed) = spoolq_names::parse_dead(&entry) {
                                    if parsed.common.job_id == *job_id {
                                        results.push(Snapshot {
                                            job_id: *job_id,
                                            state: "dead".into(),
                                            generation: parsed.common.generation,
                                            attempt: parsed.common.attempt,
                                            maximum_attempts: parsed.common.maximum_attempts,
                                            shard,
                                            relative_path: format!("{}/{}", shard_path, entry),
                                            size: 0,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Check receipts
        if let Ok(receipts_root) = fs::open_directory(self.root_fd.as_raw_fd(), "receipts") {
            if let Ok(bucket_dirs) = fs::read_dir_entries_owned(receipts_root.as_raw_fd()) {
                for bucket_dir in bucket_dirs {
                    let shard_path = format!("receipts/{}/{}", bucket_dir, shard_str);
                    if let Ok(shard_fd) = open_relative(self.root_fd.as_raw_fd(), &shard_path) {
                        if let Ok(entries) = fs::read_dir_entries_owned(shard_fd.as_raw_fd()) {
                            for entry in entries {
                                if let Ok(parsed) = spoolq_names::parse_receipt(&entry) {
                                    if parsed.common.job_id == *job_id {
                                        results.push(Snapshot {
                                            job_id: *job_id,
                                            state: "receipt".into(),
                                            generation: parsed.common.generation,
                                            attempt: parsed.common.attempt,
                                            maximum_attempts: parsed.common.maximum_attempts,
                                            shard,
                                            relative_path: format!("{}/{}", shard_path, entry),
                                            size: 0,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        results
    }

    /// Duplicate acknowledgment probe: check if a receipt exists for this lease.
    /// Probes exact receipt filenames across retained terminal buckets.
    pub fn check_duplicate_ack(&self, lease: &LeaseInfo) -> AckOutcome {
        let shard = compute_shard(
            &self.format.queue_id,
            &lease.job_id,
            self.format.shard_count,
        );
        let shard_str = shard_hex(shard);

        // Scan receipt buckets
        if let Ok(receipts_root) = fs::open_directory(self.root_fd.as_raw_fd(), "receipts") {
            if let Ok(bucket_dirs) = fs::read_dir_entries_owned(receipts_root.as_raw_fd()) {
                for bucket_dir in bucket_dirs {
                    let shard_path = format!("receipts/{}/{}", bucket_dir, shard_str);
                    if let Ok(shard_fd) = open_relative(self.root_fd.as_raw_fd(), &shard_path) {
                        if let Ok(entries) = fs::read_dir_entries_owned(shard_fd.as_raw_fd()) {
                            for entry in entries {
                                if let Ok(parsed) = spoolq_names::parse_receipt(&entry) {
                                    if parsed.common.job_id == lease.job_id
                                        && parsed.token == lease.token
                                        && parsed.common.generation
                                            == lease.generation.wrapping_add(1)
                                    {
                                        return AckOutcome::AlreadyAcked;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        AckOutcome::LeaseLost
    }
}

/// Open a relative path from a directory fd.
pub(crate) fn open_relative(root_fd: RawFd, relative: &str) -> io::Result<OwnedFd> {
    let components: Vec<&str> = relative.split('/').filter(|s| !s.is_empty()).collect();
    let mut current_fd = root_fd;
    let mut opened = Vec::new();
    for comp in &components {
        let fd = fs::open_directory(current_fd, comp)?;
        current_fd = fd.as_raw_fd();
        opened.push(fd);
    }
    // Return the last opened fd, forgetting the intermediates
    match opened.into_iter().last() {
        Some(fd) => Ok(fd),
        None => {
            // Re-open root
            let root_path = format!("/proc/self/fd/{}", root_fd);
            fs::open_dir_absolute(std::path::Path::new(&root_path))
        }
    }
}

/// Input for an enqueue operation.
#[derive(Clone, Debug, Default)]
pub struct EnqueueInput {
    pub maximum_attempts: u32,
    pub content_type: String,
    pub metadata: std::collections::BTreeMap<String, spoolq_format::cbor::MetadataValue>,
    pub producer_id: Option<String>,
    pub trace_context: Option<Vec<u8>>,
    pub initial_not_before: Option<u64>,
    pub payload: Vec<u8>,
}

/// Internal error type for publication.
enum PublishError {
    NotCommitted(Error),
    OutcomeUnknown(Error),
}

impl PublishError {
    fn classify_write(e: io::Error) -> Self {
        match e.raw_os_error() {
            Some(libc::ENOSPC) | Some(libc::EDQUOT) => {
                PublishError::NotCommitted(Error::ResourceExhausted)
            }
            Some(libc::EIO) | Some(libc::ESTALE) => {
                PublishError::NotCommitted(Error::IoFailure(e.to_string()))
            }
            _ => PublishError::NotCommitted(Error::IoFailure(e.to_string())),
        }
    }

    fn classify_post_fsync(e: io::Error) -> Self {
        // After fsync failure, outcome is unknown if the linearization may have occurred.
        // For file fsync before publication, it's NotCommitted.
        // For directory fsync after publication, it's OutcomeUnknown.
        // This is simplified; callers determine context.
        PublishError::OutcomeUnknown(Error::IoFailure(e.to_string()))
    }
}

fn nb_to_u64(opt: Option<u64>) -> u64 {
    opt.unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_queue() -> (TempDir, Queue) {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path();
        Queue::init(path, &CreateOptions::default()).unwrap();
        let queue = Queue::open(
            path,
            &OpenOptions {
                create: false,
                allow_unsupported_fs: true,
                ..Default::default()
            },
        )
        .unwrap();
        (tmp, queue)
    }

    #[test]
    fn init_and_open() {
        let (_tmp, queue) = create_test_queue();
        assert_eq!(queue.format().shard_count, 64);
    }

    #[test]
    fn enqueue_basic() {
        let (_tmp, mut queue) = create_test_queue();
        let input = EnqueueInput {
            maximum_attempts: 3,
            content_type: "text/plain".to_string(),
            payload: b"hello world".to_vec(),
            ..Default::default()
        };
        let outcome = queue.enqueue(input);
        match outcome {
            EnqueueOutcome::Committed(ticket) => {
                assert!(!ticket.expected_relative_path.is_empty());
                assert!(ticket.expected_relative_path.starts_with("ready/"));
            }
            _ => panic!("expected committed, got {:?}", outcome),
        }
    }

    #[test]
    fn enqueue_delayed() {
        let (_tmp, mut queue) = create_test_queue();
        let future = fs::clock_realtime_ns().unwrap() + 60_000_000_000; // 60s in future
        let input = EnqueueInput {
            maximum_attempts: 1,
            content_type: "application/octet-stream".to_string(),
            initial_not_before: Some(future),
            payload: vec![0x42; 100],
            ..Default::default()
        };
        let outcome = queue.enqueue(input);
        match outcome {
            EnqueueOutcome::Committed(ticket) => {
                assert!(ticket.expected_relative_path.starts_with("delayed/"));
            }
            _ => panic!("expected committed, got {:?}", outcome),
        }
    }

    #[test]
    fn enqueue_zero_attempts_rejected() {
        let (_tmp, mut queue) = create_test_queue();
        let input = EnqueueInput {
            maximum_attempts: 0,
            content_type: "x".to_string(),
            payload: vec![1],
            ..Default::default()
        };
        let outcome = queue.enqueue(input);
        assert!(matches!(outcome, EnqueueOutcome::NotCommitted(_, _)));
    }

    #[test]
    fn format_file_exists_after_init() {
        let tmp = TempDir::new().unwrap();
        Queue::init(tmp.path(), &CreateOptions::default()).unwrap();
        assert!(tmp.path().join("FORMAT").exists());
        assert!(tmp.path().join("control").exists());
        assert!(tmp.path().join("control/maintenance.lock").exists());
        assert!(tmp.path().join("control/wall-watermark").exists());
        assert!(tmp.path().join("ready").exists());
        // Check shard dirs
        assert!(tmp.path().join("ready/0000").exists());
        assert!(tmp.path().join("ready/003f").exists());
    }

    #[test]
    fn full_lifecycle() {
        let (_tmp, mut queue) = create_test_queue();

        // Enqueue
        let input = EnqueueInput {
            maximum_attempts: 3,
            content_type: "text/plain".to_string(),
            payload: b"hello world".to_vec(),
            ..Default::default()
        };
        let ticket = match queue.enqueue(input) {
            EnqueueOutcome::Committed(t) => t,
            other => panic!("enqueue failed: {:?}", other),
        };

        // Lease
        let lease = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            other => panic!("lease failed: {:?}", other),
        };
        assert_eq!(lease.job_id, ticket.job_id);
        assert_eq!(lease.attempt, 1);
        assert_eq!(lease.generation, 1);

        // Ack
        let ack_result = queue.ack(&lease);
        assert!(matches!(ack_result, AckOutcome::Acked));
    }

    #[test]
    fn lease_empty_queue() {
        let (_tmp, mut queue) = create_test_queue();
        let result = queue.lease(0, 30_000_000_000);
        assert!(matches!(result, LeaseOutcome::Empty));
    }

    #[test]
    fn retry_after_lease() {
        let (_tmp, mut queue) = create_test_queue();
        let input = EnqueueInput {
            maximum_attempts: 3,
            content_type: "x".to_string(),
            payload: b"data".to_vec(),
            ..Default::default()
        };
        let _ = queue.enqueue(input);

        let lease = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            other => panic!("lease failed: {:?}", other),
        };

        // Retry now -> back to ready
        let result = queue.retry_now(&lease);
        assert!(matches!(result, TransitionOutcome::Committed));

        // Should be able to lease again
        let lease2 = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            other => panic!("second lease failed: {:?}", other),
        };
        assert_eq!(lease2.attempt, 2);
    }

    #[test]
    fn bury_after_lease() {
        let (_tmp, mut queue) = create_test_queue();
        let input = EnqueueInput {
            maximum_attempts: 3,
            content_type: "x".to_string(),
            payload: b"data".to_vec(),
            ..Default::default()
        };
        let _ = queue.enqueue(input);

        let lease = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            other => panic!("lease failed: {:?}", other),
        };

        let result = queue.bury(&lease, DeadReason::ConsumerRejected);
        assert!(matches!(result, TransitionOutcome::Committed));

        // Queue should be empty now
        let result2 = queue.lease(0, 30_000_000_000);
        assert!(matches!(result2, LeaseOutcome::Empty));
    }

    #[test]
    fn retry_exhausted_goes_to_dead() {
        let (_tmp, mut queue) = create_test_queue();
        let input = EnqueueInput {
            maximum_attempts: 1,
            content_type: "x".to_string(),
            payload: b"data".to_vec(),
            ..Default::default()
        };
        let _ = queue.enqueue(input);

        let lease = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            other => panic!("lease failed: {:?}", other),
        };
        assert_eq!(lease.maximum_attempts, 1);
        assert_eq!(lease.attempt, 1);

        // Attempt >= maximum_attempts, retry should go to dead
        let result = queue.retry_now(&lease);
        assert!(matches!(result, TransitionOutcome::Committed));

        // Queue should be empty
        let result2 = queue.lease(0, 30_000_000_000);
        assert!(matches!(result2, LeaseOutcome::Empty));
    }

    #[test]
    fn renew_extends_lease() {
        let (_tmp, mut queue) = create_test_queue();
        let input = EnqueueInput {
            maximum_attempts: 3,
            content_type: "x".to_string(),
            payload: b"data".to_vec(),
            ..Default::default()
        };
        let _ = queue.enqueue(input);

        let lease = match queue.lease(0, 10_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            other => panic!("lease failed: {:?}", other),
        };

        let renewed = match queue.renew(&lease, 60_000_000_000) {
            RenewOutcome::Renewed(l) => l,
            other => panic!("renew failed: {:?}", other),
        };
        assert!(renewed.expires_boottime_ns > lease.expires_boottime_ns);
        assert_eq!(renewed.attempt, lease.attempt);
        assert_eq!(renewed.token, lease.token);
    }

    #[test]
    fn ack_already_lost_returns_lease_lost() {
        let (_tmp, mut queue) = create_test_queue();
        let input = EnqueueInput {
            maximum_attempts: 3,
            content_type: "x".to_string(),
            payload: b"data".to_vec(),
            ..Default::default()
        };
        let _ = queue.enqueue(input);

        let lease = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            other => panic!("lease failed: {:?}", other),
        };

        // Ack once
        assert!(matches!(queue.ack(&lease), AckOutcome::Acked));

        // Ack again with the same lease should return LeaseLost (source gone)
        let result = queue.ack(&lease);
        assert!(matches!(result, AckOutcome::LeaseLost));
    }

    #[test]
    fn lease_duration_validation() {
        let (_tmp, mut queue) = create_test_queue();
        // Too short
        assert!(matches!(
            queue.lease(0, 500_000_000),
            LeaseOutcome::NotCommitted(_)
        ));
        // Too long (more than 7 days)
        assert!(matches!(
            queue.lease(0, 8 * 24 * 60 * 60 * 1_000_000_000),
            LeaseOutcome::NotCommitted(_)
        ));
    }
    #[test]
    fn payload_verification() {
        let (_tmp, mut queue) = create_test_queue();
        let payload = b"verify me please";
        let input = EnqueueInput {
            maximum_attempts: 3,
            content_type: "text/plain".to_string(),
            payload: payload.to_vec(),
            ..Default::default()
        };
        queue.enqueue(input);

        let lease = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            _ => panic!("lease failed"),
        };

        assert!(queue.verify_lease_payload(&lease).is_ok());
    }
    #[test]
    fn retry_with_policy_works() {
        let (_tmp, mut queue) = create_test_queue();
        queue.enqueue(EnqueueInput {
            maximum_attempts: 5,
            content_type: "x".to_string(),
            payload: b"data".to_vec(),
            ..Default::default()
        });
        let lease = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            _ => panic!("lease failed"),
        };
        let policy = spoolq_math::RetryPolicy {
            base_ms: 1000,
            cap_ms: 300_000,
            use_jitter: false,
            max_delay_ms: None,
        };
        let result = queue.retry_with_policy(&lease, &policy);
        assert!(matches!(result, TransitionOutcome::Committed));
    }
    #[test]
    fn inspect_finds_ready_job() {
        let (_tmp, mut queue) = create_test_queue();
        let outcome = queue.enqueue(EnqueueInput {
            maximum_attempts: 3,
            content_type: "x".to_string(),
            payload: b"data".to_vec(),
            ..Default::default()
        });
        let ticket = match outcome {
            EnqueueOutcome::Committed(t) => t,
            _ => panic!("enqueue failed"),
        };

        let snapshots = queue.inspect(&ticket.job_id);
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].state, "ready");
        assert_eq!(snapshots[0].generation, 0);
    }

    #[test]
    fn inspect_finds_leased_job() {
        let (_tmp, mut queue) = create_test_queue();
        queue.enqueue(EnqueueInput {
            maximum_attempts: 3,
            content_type: "x".to_string(),
            payload: b"data".to_vec(),
            ..Default::default()
        });
        let lease = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            _ => panic!("lease failed"),
        };

        let snapshots = queue.inspect(&lease.job_id);
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].state, "leased");
    }

    #[test]
    fn duplicate_ack_returns_already_acked() {
        let (_tmp, mut queue) = create_test_queue();
        queue.enqueue(EnqueueInput {
            maximum_attempts: 3,
            content_type: "x".to_string(),
            payload: b"data".to_vec(),
            ..Default::default()
        });
        let lease = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            _ => panic!("lease failed"),
        };

        // First ack succeeds
        assert!(matches!(queue.ack(&lease), AckOutcome::Acked));

        // Source is gone, so check_duplicate_ack should find the receipt
        let result = queue.check_duplicate_ack(&lease);
        assert!(matches!(result, AckOutcome::AlreadyAcked));
    }

    #[test]
    fn inspect_returns_empty_for_unknown() {
        let (_tmp, queue) = create_test_queue();
        let unknown_id = [0xFF; 16];
        let snapshots = queue.inspect(&unknown_id);
        assert!(snapshots.is_empty());
    }
    #[test]
    fn concurrent_producers_consumers() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::thread;

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        Queue::init(&path, &CreateOptions::default()).unwrap();

        let num_producers = 4;
        let num_consumers = 4;
        let jobs_per_producer = 25;
        let total_jobs = num_producers * jobs_per_producer;
        let leased_count = Arc::new(AtomicUsize::new(0));
        let acked_count = Arc::new(AtomicUsize::new(0));

        // Producers
        let mut producer_handles = Vec::new();
        for _ in 0..num_producers {
            let p = path.clone();
            let handle = thread::spawn(move || {
                let queue = Queue::open(
                    &p,
                    &OpenOptions {
                        allow_unsupported_fs: true,
                        ..Default::default()
                    },
                )
                .unwrap();
                let mut queue = queue;
                for _ in 0..jobs_per_producer {
                    let payload =
                        format!("payload-{}", spoolq_fs_linux::random_128bit().unwrap()[0]);
                    queue.enqueue(EnqueueInput {
                        maximum_attempts: 3,
                        content_type: "text/plain".to_string(),
                        payload: payload.into_bytes(),
                        ..Default::default()
                    });
                }
            });
            producer_handles.push(handle);
        }
        for h in producer_handles {
            h.join().unwrap();
        }

        // Consumers
        let mut consumer_handles = Vec::new();
        for _ in 0..num_consumers {
            let p = path.clone();
            let lc = leased_count.clone();
            let ac = acked_count.clone();
            let handle = thread::spawn(move || {
                let queue = Queue::open(
                    &p,
                    &OpenOptions {
                        allow_unsupported_fs: true,
                        ..Default::default()
                    },
                )
                .unwrap();
                let mut queue = queue;
                loop {
                    match queue.lease(0, 30_000_000_000) {
                        LeaseOutcome::Leased(lease) => {
                            lc.fetch_add(1, Ordering::SeqCst);
                            if queue.ack(&lease) == AckOutcome::Acked {
                                ac.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                        LeaseOutcome::Empty => break,
                        _ => {}
                    }
                }
            });
            consumer_handles.push(handle);
        }
        for h in consumer_handles {
            h.join().unwrap();
        }

        assert_eq!(
            leased_count.load(Ordering::SeqCst),
            total_jobs,
            "expected {} leased, got {}",
            total_jobs,
            leased_count.load(Ordering::SeqCst)
        );
        assert_eq!(
            acked_count.load(Ordering::SeqCst),
            total_jobs,
            "expected {} acked, got {}",
            total_jobs,
            acked_count.load(Ordering::SeqCst)
        );
    }

    #[test]
    fn concurrent_lease_uniqueness() {
        // 8 consumers race for 1 job: exactly one should win
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::thread;

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        Queue::init(&path, &CreateOptions::default()).unwrap();

        // Enqueue exactly one job
        {
            let mut queue = Queue::open(
                &path,
                &OpenOptions {
                    allow_unsupported_fs: true,
                    ..Default::default()
                },
            )
            .unwrap();
            queue.enqueue(EnqueueInput {
                maximum_attempts: 3,
                content_type: "x".to_string(),
                payload: b"race".to_vec(),
                ..Default::default()
            });
        }

        let success_count = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        for _ in 0..32 {
            let p = path.clone();
            let sc = success_count.clone();
            handles.push(thread::spawn(move || {
                let queue = Queue::open(
                    &p,
                    &OpenOptions {
                        allow_unsupported_fs: true,
                        ..Default::default()
                    },
                )
                .unwrap();
                let mut queue = queue;
                if let LeaseOutcome::Leased(_) = queue.lease(0, 30_000_000_000) {
                    sc.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            success_count.load(Ordering::SeqCst),
            1,
            "exactly one consumer should win the race"
        );
    }

    #[test]
    fn enqueue_survives_reopen() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path();
        Queue::init(path, &CreateOptions::default()).unwrap();

        // Enqueue
        let ticket = {
            let mut queue = Queue::open(
                path,
                &OpenOptions {
                    allow_unsupported_fs: true,
                    ..Default::default()
                },
            )
            .unwrap();
            match queue.enqueue(EnqueueInput {
                maximum_attempts: 3,
                content_type: "text/plain".to_string(),
                payload: b"survive reopen".to_vec(),
                ..Default::default()
            }) {
                EnqueueOutcome::Committed(t) => t,
                _ => panic!("enqueue failed"),
            }
        };

        // Reopen and verify the job is visible
        let queue2 = Queue::open(
            path,
            &OpenOptions {
                allow_unsupported_fs: true,
                ..Default::default()
            },
        )
        .unwrap();
        let snapshots = queue2.inspect(&ticket.job_id);
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].state, "ready");
    }

    #[test]
    fn enqueue_zero_payload() {
        let (_tmp, mut queue) = create_test_queue();
        let outcome = queue.enqueue(EnqueueInput {
            maximum_attempts: 1,
            content_type: "empty".to_string(),
            payload: vec![],
            ..Default::default()
        });
        match outcome {
            EnqueueOutcome::Committed(ticket) => {
                // Verify it can be leased
                let lease = match queue.lease(0, 30_000_000_000) {
                    LeaseOutcome::Leased(l) => l,
                    _ => panic!("lease failed"),
                };
                assert_eq!(lease.job_id, ticket.job_id);
            }
            _ => panic!("zero-payload enqueue should succeed"),
        }
    }

    #[test]
    fn enqueue_large_payload() {
        let (_tmp, mut queue) = create_test_queue();
        let payload = vec![0x42; 1_000_000]; // 1 MB
        let outcome = queue.enqueue(EnqueueInput {
            maximum_attempts: 1,
            content_type: "large".to_string(),
            payload,
            ..Default::default()
        });
        assert!(matches!(outcome, EnqueueOutcome::Committed(_)));
    }
}
