// SpoolQ/1 queue initialization, open, and enqueue operations.

use std::io;
use std::os::unix::io::{AsRawFd, OwnedFd, RawFd};
use std::path::{Path, PathBuf};

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
    root_fd: OwnedFd,
    root_path: PathBuf,
    format: FormatRecord,
    boot_id: String,
    boot_id_bytes: [u8; 16],
    poisoned: bool,
    worker_nonce: [u8; 16],
    options: OpenOptions,
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
        fs::fsync_dir_fd(root_fd.as_raw_fd())?;

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
        let now_wall = fs::clock_realtime_ns().unwrap_or(0);
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
    fn ensure_dir(&self, relative: &str) -> io::Result<()> {
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
}

/// Open a relative path from a directory fd.
fn open_relative(root_fd: RawFd, relative: &str) -> io::Result<OwnedFd> {
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
}
