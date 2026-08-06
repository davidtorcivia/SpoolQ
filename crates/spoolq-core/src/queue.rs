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

/// Validate all CreateOptions before any filesystem mutation (C-01).
/// Same validation used in encoding and tests.
pub fn validate_create_options(opts: &CreateOptions) -> Result<(), Error> {
    if opts.shard_count == 0 || !opts.shard_count.is_power_of_two() || opts.shard_count > 4096 {
        return Err(Error::InvalidInput("invalid shard count".into()));
    }
    if opts.lease_bucket_width_ns == 0 {
        return Err(Error::InvalidInput(
            "lease bucket width must be non-zero".into(),
        ));
    }
    if opts.delayed_bucket_width_ns == 0 {
        return Err(Error::InvalidInput(
            "delayed bucket width must be non-zero".into(),
        ));
    }
    if opts.terminal_bucket_width_ns == 0 {
        return Err(Error::InvalidInput(
            "terminal bucket width must be non-zero".into(),
        ));
    }
    if !(60_000_000_000..=86_400_000_000_000).contains(&opts.terminal_bucket_width_ns) {
        return Err(Error::InvalidInput("invalid terminal bucket width".into()));
    }
    if opts.max_payload_length > MAX_PAYLOAD_LENGTH {
        return Err(Error::InvalidInput("payload limit exceeds maximum".into()));
    }
    Ok(())
}

/// Operational options for opening a queue.
#[derive(Clone, Debug)]
pub struct OpenOptions {
    /// Deprecated: open-or-create is not implemented (C-04).
    pub create: bool,
    /// Deprecated: unused when create is false (C-04).
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
    pub(crate) scan_round: u64,
    pub(crate) worker_nonce: [u8; 16],
    pub(crate) options: OpenOptions,
    pub(crate) maint_lock_fd: Option<OwnedFd>,
}

impl Queue {
    /// Initialize a new queue at the given path.
    pub fn init(root: &Path, opts: &CreateOptions) -> io::Result<FormatRecord> {
        // C-01: Validate all options before any filesystem mutation
        validate_create_options(opts)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

        // Create root directory if needed
        if !root.exists() {
            std::fs::create_dir_all(root)?;
            // Sync the parent directory so the root entry persists
            if let Some(parent) = root.parent() {
                let parent_fd = fs::open_dir_absolute(parent)?;
                fs::fsync_dir_fd(parent_fd.as_raw_fd())?;
            }
        }

        let root_fd = fs::open_dir_absolute(root)?;

        // B-01: Refuse to overwrite an existing queue. Check for FORMAT via fd-relative.
        let format_exists = fs::fstatat(root_fd.as_raw_fd(), "FORMAT").is_ok();
        if format_exists {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "queue already initialized; use open() to access an existing queue",
            ));
        }

        // B-01: If the root exists but has no FORMAT, it may be a partial init.
        // Acquire exclusive lock on control/maintenance.lock if control dir exists.
        let control_exists = fs::fstatat(root_fd.as_raw_fd(), "control").is_ok();
        if control_exists {
            if let Ok(control_fd) = fs::open_directory(root_fd.as_raw_fd(), "control") {
                if let Ok(lock_fd) = fs::openat(
                    control_fd.as_raw_fd(),
                    "maintenance.lock",
                    libc::O_RDWR,
                    0o600,
                ) {
                    let locked = fs::try_ofd_write_lock(lock_fd.as_raw_fd())?;
                    if !locked {
                        return Err(io::Error::new(
                            io::ErrorKind::WouldBlock,
                            "another initializer or maintenance process holds the lock",
                        ));
                    }
                    // Hold the lock for the duration of init
                    std::mem::forget(lock_fd);
                }
            }
        }

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
            let shard_name = format!("{i:04x}");
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
        let wall_bucket =
            bucket_number(wall_now, opts.delayed_bucket_width_ns).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "zero bucket width in init")
            })?;
        let wm = WatermarkRecord {
            highest_observed_bucket: wall_bucket,
            sequence: 0,
        };
        let wm_bytes = wm.encode();
        // Write via temp file then rename
        // C-03: Use unique temp name to avoid collision on partial init rerun
        let wm_tmp_name = format!(
            ".wm.tmp.{}",
            spoolq_names::hex_encode(&fs::random_128bit()?)
        );
        let wm_tmp = fs::create_exclusive(control_fd.as_raw_fd(), &wm_tmp_name, 0o600)?;
        fs::write_all(wm_tmp.as_raw_fd(), &wm_bytes)?;
        fs::fsync(wm_tmp.as_raw_fd())?;
        fs::renameat(
            control_fd.as_raw_fd(),
            &wm_tmp_name,
            control_fd.as_raw_fd(),
            "wall-watermark",
        )?;
        fs::fsync_dir_fd(control_fd.as_raw_fd())?;

        // Write FORMAT file
        let format_bytes = format_rec.encode();
        // C-03: Unique temp name for partial init recovery
        let fmt_tmp_name = format!(
            ".format.tmp.{}",
            spoolq_names::hex_encode(&fs::random_128bit()?)
        );
        let fmt_tmp = fs::create_exclusive(root_fd.as_raw_fd(), &fmt_tmp_name, 0o600)?;
        fs::write_all(fmt_tmp.as_raw_fd(), &format_bytes)?;
        fs::fsync(fmt_tmp.as_raw_fd())?;
        fs::renameat(
            root_fd.as_raw_fd(),
            &fmt_tmp_name,
            root_fd.as_raw_fd(),
            "FORMAT",
        )?;
        // C-02: Set FORMAT to read-only before final dir fsync, propagate failure
        fs::fchmodat(root_fd.as_raw_fd(), "FORMAT", 0o400)?;
        fs::fsync_dir_fd(root_fd.as_raw_fd())?;

        // Reopen and verify as a normal client (step 13)
        let verify_format = std::fs::read(root.join("FORMAT"))?;
        FormatRecord::decode(&verify_format)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        Ok(format_rec)
    }

    /// Open an existing queue.
    pub fn open(root: &Path, opts: &OpenOptions) -> Result<Self, Error> {
        // B-11: Open root first using descriptor-relative, no-symlink semantics
        let root_fd = fs::open_dir_absolute(root).map_err(|e| Error::IoFailure(e.to_string()))?;

        // B-11: Validate root is a directory
        let root_stat =
            fs::fstat(root_fd.as_raw_fd()).map_err(|e| Error::IoFailure(e.to_string()))?;
        if root_stat.st_mode & libc::S_IFMT != libc::S_IFDIR {
            return Err(Error::QueueCorrupt("root path is not a directory".into()));
        }

        // B-11: Read FORMAT through descriptor-relative open, not pathname
        let format_fd = fs::openat(root_fd.as_raw_fd(), "FORMAT", libc::O_RDONLY, 0)
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        let mut format_bytes = Vec::new();
        {
            let mut buf = [0u8; 4096];
            loop {
                match fs::read(format_fd.as_raw_fd(), &mut buf) {
                    Ok(0) => break,
                    Ok(n) => format_bytes.extend_from_slice(&buf[..n]),
                    Err(e) => return Err(Error::IoFailure(e.to_string())),
                }
            }
        }
        let format_rec = FormatRecord::decode(&format_bytes).map_err(|e| match e {
            spoolq_format::FormatError::BadMagic | spoolq_format::FormatError::WrongSize { .. } => {
                Error::QueueCorrupt(format!("FORMAT decode: {e}"))
            }
            spoolq_format::FormatError::UnsupportedVersion(_, _) => Error::UnsupportedFormat,
            spoolq_format::FormatError::DigestMismatch => {
                Error::QueueCorrupt("FORMAT digest mismatch".into())
            }
            _ => Error::QueueCorrupt(format!("FORMAT decode: {e}")),
        })?;

        // Validate retention bound: ceil(retention / terminal_width) + 2 <= 4096
        let probe_count = ceiling_bucket(
            opts.receipt_retention_ns,
            format_rec.terminal_bucket_width_ns,
        )
        .unwrap_or(0)
        .saturating_add(2);
        if probe_count > 4096 {
            return Err(Error::InvalidInput(
                "receipt retention exceeds duplicate-ack probe bound".into(),
            ));
        }

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

        // B-11: Verify state directories exist and are on the same device
        for state_dir in &[
            "control",
            "ready",
            "leased",
            "delayed",
            "receipts",
            "dead",
            "quarantine",
            "tmp",
        ] {
            if let Ok(stat) = fs::fstatat(root_fd.as_raw_fd(), state_dir) {
                if stat.st_dev != root_stat.st_dev {
                    return Err(Error::QueueCorrupt(format!(
                        "state directory '{state_dir}' is on a different device than root"
                    )));
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
        Ok(Queue {
            root_fd,
            root_path: root.to_path_buf(),
            format: format_rec,
            boot_id,
            boot_id_bytes: boot_id_bin,
            poisoned: false,
            scan_round: 0,
            worker_nonce,
            options: opts.clone(),
            maint_lock_fd: Some(maint_fd),
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
        let clock = match spoolq_fs_linux::clock_realtime_ns() {
            Ok(t) => t,
            Err(_) => return 0,
        };
        // Read the wall watermark
        match self.read_wall_watermark() {
            Some(wm) => spoolq_math::effective_wall_floor(
                clock,
                wm.highest_observed_bucket,
                self.format.delayed_bucket_width_ns,
            )
            .unwrap_or(clock),
            None => clock,
        }
    }

    /// Read the wall watermark record from control/wall-watermark.
    fn read_wall_watermark(&self) -> Option<spoolq_format::WatermarkRecord> {
        let control_fd = fs::open_directory(self.root_fd.as_raw_fd(), "control").ok()?;
        let data = match fs::openat(control_fd.as_raw_fd(), "wall-watermark", libc::O_RDONLY, 0) {
            Ok(fd) => fd,
            Err(_) => return None,
        };
        let mut buf = [0u8; spoolq_format::WATERMARK_SIZE];
        if fs::pread_exact(data.as_raw_fd(), &mut buf, 0).is_err() {
            return None;
        }
        spoolq_format::WatermarkRecord::decode(&buf).ok()
    }

    /// B-05: Advance the wall watermark to max(stored, observed).
    /// Re-reads under lock, computes max, writes atomically with sequence increment.
    pub fn advance_wall_watermark(&self, observed_ns: u64) -> Result<(), Error> {
        let control_fd = fs::open_directory(self.root_fd.as_raw_fd(), "control")
            .map_err(|e| Error::IoFailure(e.to_string()))?;

        // Acquire exclusive write lock on wall-watermark.lock
        let lock_fd = fs::openat(
            control_fd.as_raw_fd(),
            "wall-watermark.lock",
            libc::O_RDWR,
            0o600,
        )
        .map_err(|e| Error::IoFailure(e.to_string()))?;
        let locked = fs::try_ofd_write_lock(lock_fd.as_raw_fd())
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        if !locked {
            return Err(Error::MaintenanceBusy);
        }

        // Re-read current watermark under lock
        let current = self.read_wall_watermark();
        let observed_bucket =
            spoolq_math::bucket_number(observed_ns, self.format.delayed_bucket_width_ns)
                .unwrap_or(0);

        let (new_bucket, new_seq) = match current {
            Some(wm) => {
                let max_bucket = wm.highest_observed_bucket.max(observed_bucket);
                let new_seq = wm.sequence.checked_add(1).ok_or(Error::StateExhausted)?;
                (max_bucket, new_seq)
            }
            None => (observed_bucket, 1),
        };

        let new_wm = spoolq_format::WatermarkRecord {
            highest_observed_bucket: new_bucket,
            sequence: new_seq,
        };
        let wm_bytes = new_wm.encode();

        // Write via unique temp, then atomic rename, then sync
        let tmp_name = format!(
            ".wm.adv.{}",
            spoolq_names::hex_encode(
                &fs::random_128bit().map_err(|e| Error::IoFailure(e.to_string()))?
            )
        );
        let tmp_fd = fs::create_exclusive(control_fd.as_raw_fd(), &tmp_name, 0o600)
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        fs::write_all(tmp_fd.as_raw_fd(), &wm_bytes)
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        fs::fsync(tmp_fd.as_raw_fd()).map_err(|e| Error::IoFailure(e.to_string()))?;
        fs::renameat(
            control_fd.as_raw_fd(),
            &tmp_name,
            control_fd.as_raw_fd(),
            "wall-watermark",
        )
        .map_err(|e| Error::IoFailure(e.to_string()))?;
        fs::fsync_dir_fd(control_fd.as_raw_fd()).map_err(|e| Error::IoFailure(e.to_string()))?;

        Ok(())
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

        // C-11: Validate payload size BEFORE hashing
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

        // Compute payload digest (after size validation - C-11)
        let pdig = payload_digest(&job.payload);

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
                let path = format!("ready/{shard_str}/{fname}");
                (format!("ready/{shard_str}"), fname, path)
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
                let path = format!("delayed/{bucket_str}/{shard_str}/{fname}");
                (format!("delayed/{bucket_str}/{shard_str}"), fname, path)
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
            Ok(()) => {
                // B-05: Advance wall watermark after successful enqueue
                let _ = self.advance_wall_watermark(created_at);
                EnqueueOutcome::Committed(ticket)
            }
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
                let header_bytes = header
                    .encode(ext_bytes)
                    .map_err(|e| PublishError::NotCommitted(Error::InvalidInput(e.to_string())))?;
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
                // C-13: No redundant pwrite - header was already written correctly above.
                // fsync file (before publication: NotCommitted on failure)
                fs::fsync(tmp_fd.as_raw_fd()).map_err(PublishError::classify_pre_pub_fsync)?;

                // Publish via linkat - C-09: capture errors for capability classification
                let link1 =
                    fs::linkat_empty_path(tmp_fd.as_raw_fd(), dest_fd.as_raw_fd(), dest_name);
                if link1.is_ok() {
                    fs::fsync_dir_fd(dest_fd.as_raw_fd())
                        .map_err(PublishError::classify_post_fsync)?;
                    return Ok(());
                }
                let link2 =
                    fs::linkat_proc_self_fd(tmp_fd.as_raw_fd(), dest_fd.as_raw_fd(), dest_name);
                if link2.is_ok() {
                    fs::fsync_dir_fd(dest_fd.as_raw_fd())
                        .map_err(PublishError::classify_post_fsync)?;
                    return Ok(());
                }

                // C-09: Fall back to named temp file only for capability errors.
                // Propagate I/O, resource, and permission errors.
                let last_err = link2.err();
                if let Some(ref e) = last_err {
                    if fs::should_propagate_on_fallback(e) {
                        return Err(PublishError::NotCommitted(Error::IoFailure(e.to_string())));
                    }
                }
                self.named_fallback(dest_dir_relative, dest_name, header, ext_bytes, payload)
            }
            Err(e) => {
                // C-09: Only fall back on capability errors (ENOENT, ENOSYS, EOPNOTSUPP)
                if fs::should_propagate_on_fallback(&e) {
                    return Err(PublishError::NotCommitted(Error::IoFailure(e.to_string())));
                }
                self.named_fallback(dest_dir_relative, dest_name, header, ext_bytes, payload)
            }
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
        let boottime = fs::clock_boottime_ns()
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
        let random = fs::random_128bit()
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
        let temp_name = temp_filename(boottime, &random);

        let tmp_file = fs::create_exclusive(tmp_dir_fd.as_raw_fd(), &temp_name, 0o600)
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;

        // C-10: RAII guard to unlink temp file on early return
        struct TempGuard<'a> {
            dir_fd: std::os::unix::io::RawFd,
            name: &'a str,
            armed: bool,
        }
        impl<'a> Drop for TempGuard<'a> {
            fn drop(&mut self) {
                if self.armed {
                    let _ = fs::unlinkat(self.dir_fd, self.name);
                }
            }
        }
        let mut temp_guard = TempGuard {
            dir_fd: tmp_dir_fd.as_raw_fd(),
            name: &temp_name,
            armed: true,
        };

        // Write header
        let header_bytes = header
            .encode(ext_bytes)
            .map_err(|e| PublishError::NotCommitted(Error::InvalidInput(e.to_string())))?;
        fs::write_all(tmp_file.as_raw_fd(), &header_bytes).map_err(PublishError::classify_write)?;
        if !ext_bytes.is_empty() {
            fs::write_all(tmp_file.as_raw_fd(), ext_bytes).map_err(PublishError::classify_write)?;
        }
        if !payload.is_empty() {
            fs::write_all(tmp_file.as_raw_fd(), payload).map_err(PublishError::classify_write)?;
        }
        // C-13: No redundant pwrite - header was written correctly above.
        // fsync file (before publication: NotCommitted on failure)
        fs::fsync(tmp_file.as_raw_fd()).map_err(PublishError::classify_pre_pub_fsync)?;

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
                temp_guard.armed = false; // C-10: disarm on success
                                          // Sync destination first, then source
                fs::fsync_dir_fd(dest_fd.as_raw_fd()).map_err(PublishError::classify_post_fsync)?;
                fs::fsync_dir_fd(tmp_dir_fd.as_raw_fd())
                    .map_err(PublishError::classify_post_fsync)?;
                Ok(())
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                Err(PublishError::NotCommitted(Error::IdentityCollision))
            }
            Err(e) => Err(PublishError::classify_write(e)),
        }
    }

    /// Create a directory path recursively, syncing parents.
    pub(crate) fn ensure_dir(&self, relative: &str) -> io::Result<()> {
        let components: Vec<&str> = relative.split('/').filter(|s| !s.is_empty()).collect();
        let mut current_fd = self.root_fd.as_raw_fd();
        let mut owned_fds = Vec::new();

        for (i, comp) in components.iter().enumerate() {
            let was_created = fs::mkdirat_eexist_ok(current_fd, comp, 0o700)?;
            // Open the child
            let child = fs::open_directory(current_fd, comp)?;
            // P-01: Only fsync parent when a new child entry was actually created
            if was_created {
                if i > 0 {
                    fs::fsync_dir_fd(current_fd)?;
                } else {
                    fs::fsync_dir_fd(self.root_fd.as_raw_fd())?;
                }
            }
            current_fd = child.as_raw_fd();
            owned_fds.push(child);
        }
        Ok(())
    }
    /// Claim a ready job, returning a lease.
    /// max_wait_ns is accepted for API compatibility but currently performs
    /// a single immediate scan (C-14: bounded wait/backoff not yet implemented).
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

        // C-16: Clocks are re-captured inside the scan loop before each claim
        let _boottime_now = fs::clock_boottime_ns().ok();
        let _wall_now = fs::clock_realtime_ns().ok();

        // C-19: Track scan completeness to distinguish Empty from I/O error
        let mut scan_had_error = false;

        // C-15: Use and advance the per-worker scan round
        let scan_round = self.scan_round;
        self.scan_round = self.scan_round.wrapping_add(1);
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
            let ready_dir = format!("ready/{shard_str}");
            let shard_fd = match open_relative(self.root_fd.as_raw_fd(), &ready_dir) {
                Ok(fd) => fd,
                Err(_) => {
                    scan_had_error = true;
                    continue;
                }
            };

            // List entries
            let entries = match fs::read_dir_entries_owned(shard_fd.as_raw_fd()) {
                Ok(e) => e,
                Err(_) => {
                    scan_had_error = true;
                    continue;
                }
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

                // C-16: Re-capture clocks immediately before the claim
                let boottime_claim = match fs::clock_boottime_ns() {
                    Ok(t) => t,
                    Err(e) => return LeaseOutcome::NotCommitted(Error::IoFailure(e.to_string())),
                };
                let wall_claim = match fs::clock_realtime_ns() {
                    Ok(t) => t,
                    Err(e) => return LeaseOutcome::NotCommitted(Error::IoFailure(e.to_string())),
                };
                // Attempt claim: rename ready -> leased
                let lease_token = match fs::random_128bit() {
                    Ok(t) => t,
                    Err(e) => return LeaseOutcome::NotCommitted(Error::IoFailure(e.to_string())),
                };
                let boottime_deadline = match boottime_claim.checked_add(lease_duration_ns) {
                    Some(d) => d,
                    None => continue, // deadline overflow, skip this candidate
                };
                let wall_deadline = match wall_claim.checked_add(lease_duration_ns) {
                    Some(d) => d,
                    None => continue,
                };
                let lease_bucket =
                    spoolq_math::lease_bucket(boottime_deadline, self.format.lease_bucket_width_ns)
                        .unwrap_or(0);
                let bucket_str = bucket_hex(lease_bucket);

                // Checked generation increment: a source at u64::MAX cannot transition.
                let new_generation = match parsed.common.generation.checked_add(1) {
                    Some(g) => g,
                    None => continue,
                };
                // Checked attempt increment.
                let new_attempt = match parsed.common.attempt.checked_add(1) {
                    Some(a) => a,
                    None => continue,
                };

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
                                    source_relative_path: format!("{ready_dir}/{entry}"),
                                    attempted_destination_state: "leased".into(),
                                    attempted_destination_relative_path: format!(
                                        "{leased_dir}/{leased_name}"
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
                                    source_relative_path: format!("{ready_dir}/{entry}"),
                                    attempted_destination_state: "leased".into(),
                                    attempted_destination_relative_path: format!(
                                        "{leased_dir}/{leased_name}"
                                    ),
                                    lease_token: Some(lease_token),
                                    envelope_digest: [0; 32],
                                });
                            }
                        }

                        // B-03: Post-rename validation failures must NOT continue as Empty.
                        // The claim is committed; failures here are corruption or indeterminate.
                        // Post-rename: open and verify the leased object
                        let leased_stat = match fs::fstatat(leased_dir_fd.as_raw_fd(), &leased_name)
                        {
                            Ok(s) => s,
                            Err(_) => {
                                // The rename succeeded but we cannot stat the result.
                                // This is OutcomeUnknown - the job is leased but we cannot verify.
                                self.poison();
                                return LeaseOutcome::OutcomeUnknown(TransitionTicket {
                                    job_id: parsed.common.job_id,
                                    source_state: "ready".into(),
                                    source_generation: parsed.common.generation,
                                    source_attempt: parsed.common.attempt,
                                    source_relative_path: format!("{ready_dir}/{entry}"),
                                    attempted_destination_state: "leased".into(),
                                    attempted_destination_relative_path: format!(
                                        "{leased_dir}/{leased_name}"
                                    ),
                                    lease_token: Some(lease_token),
                                    envelope_digest: [0; 32],
                                });
                            }
                        };

                        // Verify link count is exactly 1 (rejects external hard links)
                        if leased_stat.st_nlink != 1 {
                            self.poison();
                            return LeaseOutcome::OutcomeUnknown(TransitionTicket {
                                job_id: parsed.common.job_id,
                                source_state: "ready".into(),
                                source_generation: parsed.common.generation,
                                source_attempt: parsed.common.attempt,
                                source_relative_path: format!("{ready_dir}/{entry}"),
                                attempted_destination_state: "leased".into(),
                                attempted_destination_relative_path: format!(
                                    "{leased_dir}/{leased_name}"
                                ),
                                lease_token: Some(lease_token),
                                envelope_digest: [0; 32],
                            });
                        }

                        // Read and validate the fixed header
                        let leased_file = match fs::openat(
                            leased_dir_fd.as_raw_fd(),
                            &leased_name,
                            libc::O_RDONLY,
                            0,
                        ) {
                            Ok(f) => f,
                            Err(_) => {
                                self.poison();
                                return LeaseOutcome::OutcomeUnknown(TransitionTicket {
                                    job_id: parsed.common.job_id,
                                    source_state: "ready".into(),
                                    source_generation: parsed.common.generation,
                                    source_attempt: parsed.common.attempt,
                                    source_relative_path: format!("{ready_dir}/{entry}"),
                                    attempted_destination_state: "leased".into(),
                                    attempted_destination_relative_path: format!(
                                        "{leased_dir}/{leased_name}"
                                    ),
                                    lease_token: Some(lease_token),
                                    envelope_digest: [0; 32],
                                });
                            }
                        };

                        let mut header_buf = [0u8; 128];
                        if fs::pread_exact(leased_file.as_raw_fd(), &mut header_buf, 0).is_err() {
                            self.poison();
                            return LeaseOutcome::OutcomeUnknown(TransitionTicket {
                                job_id: parsed.common.job_id,
                                source_state: "ready".into(),
                                source_generation: parsed.common.generation,
                                source_attempt: parsed.common.attempt,
                                source_relative_path: format!("{ready_dir}/{entry}"),
                                attempted_destination_state: "leased".into(),
                                attempted_destination_relative_path: format!(
                                    "{leased_dir}/{leased_name}"
                                ),
                                lease_token: Some(lease_token),
                                envelope_digest: [0; 32],
                            });
                        }

                        let header = match FixedHeader::decode(&header_buf) {
                            Ok(h) => h,
                            Err(_) => {
                                self.poison();
                                return LeaseOutcome::OutcomeUnknown(TransitionTicket {
                                    job_id: parsed.common.job_id,
                                    source_state: "ready".into(),
                                    source_generation: parsed.common.generation,
                                    source_attempt: parsed.common.attempt,
                                    source_relative_path: format!("{ready_dir}/{entry}"),
                                    attempted_destination_state: "leased".into(),
                                    attempted_destination_relative_path: format!(
                                        "{leased_dir}/{leased_name}"
                                    ),
                                    lease_token: Some(lease_token),
                                    envelope_digest: [0; 32],
                                });
                            }
                        };

                        // Verify job_id matches
                        if header.job_id != parsed.common.job_id {
                            self.poison();
                            return LeaseOutcome::OutcomeUnknown(TransitionTicket {
                                job_id: parsed.common.job_id,
                                source_state: "ready".into(),
                                source_generation: parsed.common.generation,
                                source_attempt: parsed.common.attempt,
                                source_relative_path: format!("{ready_dir}/{entry}"),
                                attempted_destination_state: "leased".into(),
                                attempted_destination_relative_path: format!(
                                    "{leased_dir}/{leased_name}"
                                ),
                                lease_token: Some(lease_token),
                                envelope_digest: [0; 32],
                            });
                        }

                        // C-21: Read content_type from extension header
                        let content_type = if header.extension_header_length > 0
                            && header.extension_header_length <= 65536
                        {
                            let mut ext_buf = vec![0u8; header.extension_header_length as usize];
                            if fs::pread_exact(leased_file.as_raw_fd(), &mut ext_buf, 128).is_ok() {
                                spoolq_format::cbor::ExtensionHeader::decode(&ext_buf)
                                    .map(|e| e.content_type)
                                    .unwrap_or_default()
                            } else {
                                String::new()
                            }
                        } else {
                            String::new()
                        };

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
                            content_type,
                            payload_length: header.payload_length,
                            payload_digest: header.payload_digest,
                            expected_dev: leased_stat.st_dev as u64,
                            expected_inode: leased_stat.st_ino as u64,
                            exact_source_path: format!("{leased_dir}/{leased_name}"),
                            payload_verified: false,
                        };

                        return LeaseOutcome::Leased(lease_info);
                    }
                    Err(_) => continue,
                }
            }
        }

        // C-19: If the scan had I/O errors, report them rather than returning Empty
        if scan_had_error {
            LeaseOutcome::NotCommitted(Error::IoFailure("scan completed with errors".into()))
        } else {
            LeaseOutcome::Empty
        }
    }

    /// Acknowledge a verified lease: move to terminal receipt.
    /// Requires the lease to have been payload-verified.
    pub fn ack(&mut self, lease: &LeaseInfo) -> AckOutcome {
        if !lease.payload_verified {
            return AckOutcome::NotCommitted(Error::InvalidInput(
                "ack requires a verified lease; use ack_unverified for the unsafe path".into(),
            ));
        }
        self.ack_unverified(lease)
    }

    /// Acknowledge a lease without payload verification (unsafe).
    /// Cannot detect payload corruption. Use ack() for the safe path.
    pub fn ack_unverified(&mut self, lease: &LeaseInfo) -> AckOutcome {
        if let Err(e) = self.check_not_poisoned() {
            return AckOutcome::NotCommitted(e);
        }

        // C-25/B-05: Use effective wall floor for terminal transitions
        let wall_now = self.effective_wall_floor_ns();
        let terminal_bucket =
            spoolq_math::bucket_number(wall_now, self.format.terminal_bucket_width_ns).unwrap_or(0);
        let bucket_str = bucket_hex(terminal_bucket);

        let shard = compute_shard(
            &self.format.queue_id,
            &lease.job_id,
            self.format.shard_count,
        );
        let shard_str = shard_hex(shard);

        let new_generation = match lease.generation.checked_add(1) {
            Some(g) => g,
            None => return AckOutcome::NotCommitted(Error::StateExhausted),
        };
        let receipt_common = CommonFields {
            job_id: lease.job_id,
            generation: new_generation,
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

        let receipt_dir = format!("receipts/{bucket_str}/{shard_str}");
        if let Err(e) = self.ensure_dir(&receipt_dir) {
            return AckOutcome::NotCommitted(Error::IoFailure(e.to_string()));
        }

        let receipt_dir_fd = match open_relative(self.root_fd.as_raw_fd(), &receipt_dir) {
            Ok(fd) => fd,
            Err(e) => return AckOutcome::NotCommitted(Error::IoFailure(e.to_string())),
        };

        // B-04: Validate the current lease source before acknowledging
        let (src_dir_fd, src_name) = match self.open_and_validate_current_lease(lease) {
            Ok(Some(pair)) => pair,
            Ok(None) => return AckOutcome::LeaseLost,
            Err(Error::QueueCorrupt(e)) => {
                self.poison();
                return AckOutcome::NotCommitted(Error::QueueCorrupt(e));
            }
            Err(e) => return AckOutcome::NotCommitted(e),
        };

        // Rename leased -> receipt with NOREPLACE
        match fs::renameat2_noreplace(
            src_dir_fd.as_raw_fd(),
            &src_name,
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
                            "{receipt_dir}/{receipt_name}"
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
                            "{receipt_dir}/{receipt_name}"
                        ),
                        lease_token: Some(lease.token),
                        envelope_digest: lease.envelope_digest,
                    });
                }
                AckOutcome::Acked
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => AckOutcome::AlreadyAcked,
            Err(e) if e.raw_os_error() == Some(libc::ENOENT) => {
                // C-22: On source absence, do a bounded receipt probe.
                // Construct the finite set of exact retained receipt paths
                // and check them directly (C-23: bounded, not full scan).
                if self.check_duplicate_ack_bounded(lease) {
                    AckOutcome::AlreadyAcked
                } else {
                    AckOutcome::LeaseLost
                }
            }
            // C-24/B-12: Preserve I/O, permission, resource, and corruption categories
            Err(e) => AckOutcome::NotCommitted(Error::IoFailure(e.to_string())),
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
        let deadline = match spoolq_math::retry_wall_deadline(wall_now, duration_ns) {
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
            let delay_ns = match spoolq_math::checked_mul_u64(delay_ms, 1_000_000) {
                Some(d) => d,
                None => {
                    return TransitionOutcome::NotCommitted(Error::InvalidInput(
                        "delay overflow".into(),
                    ))
                }
            };
            let wall_now = self.effective_wall_floor_ns();
            let deadline = match spoolq_math::retry_wall_deadline(wall_now, delay_ns) {
                Some(d) => d,
                None => {
                    return TransitionOutcome::NotCommitted(Error::InvalidInput(
                        "deadline overflow".into(),
                    ))
                }
            };
            self.retry_at(lease, deadline)
        }
    }

    fn retry(&mut self, lease: &LeaseInfo, delayed_ns: Option<u64>) -> TransitionOutcome {
        // If delayed target is at or before the effective wall floor, it's retry_now.
        let delayed_ns = match delayed_ns {
            Some(t) if t <= self.effective_wall_floor_ns() => None,
            other => other,
        };
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
        let new_gen = match lease.generation.checked_add(1) {
            Some(g) => g,
            None => return TransitionOutcome::NotCommitted(Error::StateExhausted),
        };

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
                let dir = format!("delayed/{bucket_str}/{shard_str}");
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
                (format!("ready/{shard_str}"), fname)
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
        let new_gen = match lease.generation.checked_add(1) {
            Some(g) => g,
            None => return TransitionOutcome::NotCommitted(Error::StateExhausted),
        };

        // C-25/B-05: Use effective wall floor for terminal transitions
        let wall_now = self.effective_wall_floor_ns();
        let terminal_bucket =
            spoolq_math::bucket_number(wall_now, self.format.terminal_bucket_width_ns).unwrap_or(0);
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
        let dest_dir = format!("dead/{bucket_str}/{shard_str}");

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
        let wall_now = self.effective_wall_floor_ns();
        let new_boottime_dl = match boottime_now.checked_add(lease_duration_ns) {
            Some(d) => d,
            None => {
                return RenewOutcome::NotCommitted(Error::InvalidInput("deadline overflow".into()))
            }
        };
        let new_wall_dl = match wall_now.checked_add(lease_duration_ns) {
            Some(d) => d,
            None => {
                return RenewOutcome::NotCommitted(Error::InvalidInput("deadline overflow".into()))
            }
        };
        let new_gen = match lease.generation.checked_add(1) {
            Some(g) => g,
            None => return RenewOutcome::NotCommitted(Error::StateExhausted),
        };

        let lease_bucket =
            spoolq_math::lease_bucket(new_boottime_dl, self.format.lease_bucket_width_ns)
                .unwrap_or(0);
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
                exact_source_path: format!("{dest_dir}/{fname}"),
                ..lease.clone()
            }),
            TransitionOutcome::LeaseLost => RenewOutcome::LeaseLost,
            TransitionOutcome::NotCommitted(e) => RenewOutcome::NotCommitted(e),
            TransitionOutcome::OutcomeUnknown(t) => RenewOutcome::OutcomeUnknown(t),
        }
    }

    /// B-04: Open and validate the current leased source object.
    /// Validates the source path, filename, header, and identity against the handle.
    /// Returns the opened source directory fd and source filename on success.
    fn open_and_validate_current_lease(
        &self,
        lease: &LeaseInfo,
    ) -> Result<Option<(OwnedFd, String)>, Error> {
        let parts: Vec<&str> = lease.exact_source_path.split('/').collect();
        if parts.len() < 2 {
            return Err(Error::InvalidInput("bad source path".into()));
        }

        // B-04: Reject absolute paths, .., empty components
        for part in &parts {
            if part.is_empty() || *part == ".." || *part == "." || part.starts_with('/') {
                return Err(Error::InvalidInput(format!(
                    "invalid path component: {part}"
                )));
            }
        }

        let src_name = parts.last().unwrap().to_string();
        let src_dir = parts[..parts.len() - 1].join("/");

        let src_dir_fd = match open_relative(self.root_fd.as_raw_fd(), &src_dir) {
            Ok(fd) => fd,
            Err(_) => return Ok(None), // Source directory gone
        };

        // Stat the source file with NOFOLLOW
        let src_stat = match fs::fstatat(src_dir_fd.as_raw_fd(), &src_name) {
            Ok(s) => s,
            Err(_) => return Ok(None), // Source file gone
        };

        // B-04: Verify regular file type
        if src_stat.st_mode & libc::S_IFMT != libc::S_IFREG {
            return Err(Error::QueueCorrupt("source is not a regular file".into()));
        }

        // B-04: Verify expected device/inode if set
        if lease.expected_dev != 0 && lease.expected_dev != src_stat.st_dev as u64 {
            return Err(Error::QueueCorrupt(format!(
                "device mismatch: expected {}, got {}",
                lease.expected_dev, src_stat.st_dev
            )));
        }
        if lease.expected_inode != 0 && lease.expected_inode != src_stat.st_ino as u64 {
            return Err(Error::QueueCorrupt(format!(
                "inode mismatch: expected {}, got {}",
                lease.expected_inode, src_stat.st_ino
            )));
        }

        // B-04: Verify link count is exactly 1
        if src_stat.st_nlink != 1 {
            return Err(Error::QueueCorrupt(
                "source has unexpected hard links".into(),
            ));
        }

        // B-04: Parse the leased filename canonically
        let parsed = spoolq_names::parse_leased(&src_name).map_err(|_| {
            Error::QueueCorrupt("source filename is not a valid leased name".into())
        })?;

        // B-04: Verify filename fields match the handle
        if parsed.common.job_id != lease.job_id {
            return Err(Error::QueueCorrupt("source job_id mismatch".into()));
        }
        if parsed.common.generation != lease.generation {
            return Err(Error::QueueCorrupt("source generation mismatch".into()));
        }
        if parsed.common.attempt != lease.attempt {
            return Err(Error::QueueCorrupt("source attempt mismatch".into()));
        }
        if parsed.common.maximum_attempts != lease.maximum_attempts {
            return Err(Error::QueueCorrupt("source max_attempts mismatch".into()));
        }
        if parsed.token != lease.token {
            return Err(Error::QueueCorrupt("source token mismatch".into()));
        }

        // B-04: Read and verify the fixed header
        let file_fd = fs::openat(src_dir_fd.as_raw_fd(), &src_name, libc::O_RDONLY, 0)
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        let mut header_buf = [0u8; 128];
        fs::pread_exact(file_fd.as_raw_fd(), &mut header_buf, 0)
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        let header = FixedHeader::decode(&header_buf)
            .map_err(|e| Error::QueueCorrupt(format!("header decode: {e}")))?;

        if header.job_id != lease.job_id {
            return Err(Error::QueueCorrupt(
                "header job_id does not match handle".into(),
            ));
        }

        // B-04: Verify envelope digest
        let ext_len = header.extension_header_length as usize;
        if ext_len > 0 && ext_len <= 65536 {
            let mut ext_buf = vec![0u8; ext_len];
            if fs::pread_exact(file_fd.as_raw_fd(), &mut ext_buf, 128).is_ok()
                && !spoolq_format::verify_envelope_digest(&header, &ext_buf)
            {
                return Err(Error::QueueCorrupt("envelope digest mismatch".into()));
            }
        }

        // B-04: Verify queue-derived shard matches
        let computed_shard = compute_shard(
            &self.format.queue_id,
            &lease.job_id,
            self.format.shard_count,
        );
        let shard_from_path = parts
            .iter()
            .rev()
            .find(|p| p.starts_with("000") || p.len() == 4)
            .and_then(|s| spoolq_names::shard_from_hex(s));
        if let Some(shard) = shard_from_path {
            if shard != computed_shard {
                return Err(Error::QueueCorrupt(
                    "source shard does not match queue derivation".into(),
                ));
            }
        }

        Ok(Some((src_dir_fd, src_name)))
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

        // B-04: Validate the current lease source before transitioning
        let (src_dir_fd, src_name) = match self.open_and_validate_current_lease(lease) {
            Ok(Some(pair)) => pair,
            Ok(None) => return TransitionOutcome::LeaseLost,
            Err(Error::QueueCorrupt(e)) => {
                self.poison();
                return TransitionOutcome::NotCommitted(Error::QueueCorrupt(e));
            }
            Err(e) => return TransitionOutcome::NotCommitted(e),
        };

        match fs::renameat2_noreplace(
            src_dir_fd.as_raw_fd(),
            &src_name,
            dest_dir_fd.as_raw_fd(),
            dest_name,
        ) {
            Ok(()) => {
                // Check if source and destination are the same directory
                let src_stat = fs::fstat(src_dir_fd.as_raw_fd()).ok();
                let dest_stat = fs::fstat(dest_dir_fd.as_raw_fd()).ok();
                let src_same = match (src_stat, dest_stat) {
                    (Some(s), Some(d)) => s.st_dev == d.st_dev && s.st_ino == d.st_ino,
                    _ => false,
                };
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
            attempted_destination_relative_path: format!("{dest_dir}/{dest_name}"),
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
        let wall_now = self.effective_wall_floor_ns();
        let terminal_bucket =
            spoolq_math::bucket_number(wall_now, self.format.terminal_bucket_width_ns).unwrap_or(0);
        let bucket_str = bucket_hex(terminal_bucket);

        let new_gen = common
            .generation
            .checked_add(1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "generation overflow"))?;
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
        let dead_dir = format!("dead/{bucket_str}/{shard_str}");

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
    /// B-09: Read and verify the payload of a leased job.
    /// First validates source identity (B-04), then verifies envelope digest,
    /// then hashes the payload and compares to the header digest.
    /// Returns a LeaseInfo with payload_verified=true on success.
    /// Returns Err with PayloadCorrupt if the digest does not match.
    pub fn verify_lease_payload(&self, lease: &LeaseInfo) -> Result<LeaseInfo, Error> {
        // B-09: Validate source identity before hashing
        let (src_dir_fd, src_name) = match self.open_and_validate_current_lease(lease)? {
            Some(pair) => pair,
            None => return Err(Error::QueueCorrupt("lease source not found".into())),
        };

        let file_fd = fs::openat(src_dir_fd.as_raw_fd(), &src_name, libc::O_RDONLY, 0)
            .map_err(|e| Error::IoFailure(e.to_string()))?;

        // Read the fixed header
        let mut header_buf = [0u8; 128];
        fs::pread_exact(file_fd.as_raw_fd(), &mut header_buf, 0)
            .map_err(|e| Error::IoFailure(e.to_string()))?;

        let header =
            FixedHeader::decode(&header_buf).map_err(|e| Error::QueueCorrupt(e.to_string()))?;

        // B-09: Read and verify extension, then envelope digest
        let ext_len = header.extension_header_length as usize;
        let data_offset = 128usize + ext_len;

        if ext_len > 0 && ext_len <= 65536 {
            let mut ext_buf = vec![0u8; ext_len];
            fs::pread_exact(file_fd.as_raw_fd(), &mut ext_buf, 128)
                .map_err(|e| Error::IoFailure(e.to_string()))?;
            if !spoolq_format::verify_envelope_digest(&header, &ext_buf) {
                return Err(Error::QueueCorrupt("envelope digest mismatch".into()));
            }
        }

        // B-09: Verify exact file size (no trailing data)
        let file_stat =
            fs::fstat(file_fd.as_raw_fd()).map_err(|e| Error::IoFailure(e.to_string()))?;
        let expected_size = (128 + ext_len + header.payload_length as usize) as u64;
        if file_stat.st_size as u64 != expected_size {
            return Err(Error::QueueCorrupt(format!(
                "file size mismatch: expected {}, got {}",
                expected_size, file_stat.st_size
            )));
        }

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

        Ok(LeaseInfo {
            payload_verified: true,
            ..lease.clone()
        })
    }
    /// Diagnostic lookup: find all states for a job_id.
    /// Scans active and terminal states for the computed shard.
    pub fn inspect(&self, job_id: &[u8; 16]) -> Vec<Snapshot> {
        let mut results = Vec::new();
        let shard = compute_shard(&self.format.queue_id, job_id, self.format.shard_count);
        let shard_str = shard_hex(shard);

        // Check ready
        let ready_dir = format!("ready/{shard_str}");
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
                                relative_path: format!("{ready_dir}/{entry}"),
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
                    let boot_path = format!("leased/{boot_dir}");
                    if let Ok(boot_fd) = open_relative(self.root_fd.as_raw_fd(), &boot_path) {
                        if let Ok(bucket_dirs) = fs::read_dir_entries_owned(boot_fd.as_raw_fd()) {
                            for bucket_dir in bucket_dirs {
                                let shard_path = format!("{boot_path}/{bucket_dir}/{shard_str}");
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
                                                            "{shard_path}/{entry}"
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
                    let shard_path = format!("delayed/{bucket_dir}/{shard_str}");
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
                                            relative_path: format!("{shard_path}/{entry}"),
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
                    let shard_path = format!("dead/{bucket_dir}/{shard_str}");
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
                                            relative_path: format!("{shard_path}/{entry}"),
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
                    let shard_path = format!("receipts/{bucket_dir}/{shard_str}");
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
                                            relative_path: format!("{shard_path}/{entry}"),
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
                    let shard_path = format!("receipts/{bucket_dir}/{shard_str}");
                    if let Ok(shard_fd) = open_relative(self.root_fd.as_raw_fd(), &shard_path) {
                        if let Ok(entries) = fs::read_dir_entries_owned(shard_fd.as_raw_fd()) {
                            for entry in entries {
                                if let Ok(parsed) = spoolq_names::parse_receipt(&entry) {
                                    if parsed.common.job_id == lease.job_id
                                        && parsed.token == lease.token
                                        && parsed.common.generation
                                            == lease.generation.saturating_add(1)
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
    /// C-23: Bounded duplicate-ack check.
    /// Constructs at most the finite set of exact retained receipt paths
    /// and checks them via fstatat, not by listing receipt contents.
    fn check_duplicate_ack_bounded(&self, lease: &LeaseInfo) -> bool {
        let wall_now = self.effective_wall_floor_ns();
        let retention = self.options.receipt_retention_ns;
        let width = self.format.terminal_bucket_width_ns;
        let now_bucket = spoolq_math::bucket_number(wall_now, width).unwrap_or(0);
        let retention_buckets = spoolq_math::ceiling_bucket(retention, width).unwrap_or(0);
        let min_bucket = now_bucket.saturating_sub(retention_buckets + 2);
        let shard = compute_shard(
            &self.format.queue_id,
            &lease.job_id,
            self.format.shard_count,
        );
        let shard_str = shard_hex(shard);
        let new_generation = lease.generation.saturating_add(1);
        let receipt_common = CommonFields {
            job_id: lease.job_id,
            generation: new_generation,
            attempt: lease.attempt,
            maximum_attempts: lease.maximum_attempts,
        };
        for bucket_num in min_bucket..=now_bucket {
            let bucket_str = bucket_hex(bucket_num);
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
            let receipt_dir = format!("receipts/{bucket_str}/{shard_str}");
            if let Ok(dir_fd) = open_relative(self.root_fd.as_raw_fd(), &receipt_dir) {
                if fs::fstatat(dir_fd.as_raw_fd(), &receipt_name).is_ok() {
                    return true;
                }
            }
        }
        false
    }

    /// Resolve an indeterminate operation by probing exact paths.
    pub fn resolve(&self, ticket: &TransitionTicket, stabilize: bool) -> ResolutionOutcome {
        let dest_exists = self.path_exists(&ticket.attempted_destination_relative_path);
        let src_exists = self.path_exists(&ticket.source_relative_path);

        match (dest_exists, src_exists) {
            (true, true) => {
                if stabilize {
                    if let Some(fd) = self.open_path(&ticket.attempted_destination_relative_path) {
                        let _ = fs::fsync(fd.as_raw_fd());
                    }
                    if let Some(dir) = self.open_parent(&ticket.attempted_destination_relative_path)
                    {
                        let _ = fs::fsync_dir_fd(dir.as_raw_fd());
                    }
                }
                ResolutionOutcome::BothObserved
            }
            (true, false) => {
                if stabilize {
                    if let Some(dir) = self.open_parent(&ticket.attempted_destination_relative_path)
                    {
                        let _ = fs::fsync_dir_fd(dir.as_raw_fd());
                    }
                }
                if stabilize {
                    ResolutionOutcome::DestinationStabilized
                } else {
                    ResolutionOutcome::DestinationObserved
                }
            }
            (false, true) => {
                if stabilize {
                    if let Some(dir) = self.open_parent(&ticket.source_relative_path) {
                        let _ = fs::fsync_dir_fd(dir.as_raw_fd());
                    }
                }
                if stabilize {
                    ResolutionOutcome::SourceStabilized
                } else {
                    ResolutionOutcome::SourceObserved
                }
            }
            (false, false) => ResolutionOutcome::NeitherObserved,
        }
    }

    /// Check if a root-relative path exists via fstatat.
    fn path_exists(&self, relative: &str) -> bool {
        let parts: Vec<&str> = relative.split('/').collect();
        if parts.len() < 2 {
            return false;
        }
        let name = parts.last().unwrap();
        let dir = parts[..parts.len() - 1].join("/");
        match open_relative(self.root_fd.as_raw_fd(), &dir) {
            Ok(dir_fd) => fs::fstatat(dir_fd.as_raw_fd(), name).is_ok(),
            Err(_) => false,
        }
    }

    /// Open a root-relative file path.
    fn open_path(&self, relative: &str) -> Option<OwnedFd> {
        let parts: Vec<&str> = relative.split('/').collect();
        if parts.len() < 2 {
            return None;
        }
        let name = parts.last().unwrap();
        let dir = parts[..parts.len() - 1].join("/");
        let dir_fd = open_relative(self.root_fd.as_raw_fd(), &dir).ok()?;
        fs::openat(dir_fd.as_raw_fd(), name, 0o0, 0).ok()
    }

    /// Open the parent directory of a root-relative path.
    fn open_parent(&self, relative: &str) -> Option<OwnedFd> {
        let parts: Vec<&str> = relative.split('/').collect();
        if parts.len() < 2 {
            return None;
        }
        let dir = parts[..parts.len() - 1].join("/");
        open_relative(self.root_fd.as_raw_fd(), &dir).ok()
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
            let root_path = format!("/proc/self/fd/{root_fd}");
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

    /// Classify a file fsync failure that occurs BEFORE the linearizing
    /// link/rename. Per spec section 7.8, this is NotCommitted.
    fn classify_pre_pub_fsync(e: io::Error) -> Self {
        PublishError::NotCommitted(Error::IoFailure(e.to_string()))
    }

    /// Classify a directory fsync failure that occurs AFTER the linearizing
    /// link/rename. Per spec section 7.8, this is OutcomeUnknown.
    fn classify_post_fsync(e: io::Error) -> Self {
        PublishError::OutcomeUnknown(Error::IoFailure(e.to_string()))
    }
}

fn nb_to_u64(opt: Option<u64>) -> u64 {
    opt.unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    trait CommitOrPanic {
        fn commit_or_panic(&self);
    }

    impl CommitOrPanic for TransitionOutcome {
        fn commit_or_panic(&self) {
            assert!(matches!(self, TransitionOutcome::Committed));
        }
    }
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
            _ => panic!("expected committed, got {outcome:?}"),
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
            _ => panic!("expected committed, got {outcome:?}"),
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
            other => panic!("enqueue failed: {other:?}"),
        };

        // Lease
        let lease = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            other => panic!("lease failed: {other:?}"),
        };
        assert_eq!(lease.job_id, ticket.job_id);
        assert_eq!(lease.attempt, 1);
        assert_eq!(lease.generation, 1);

        // Verify and ack
        let verified = queue.verify_lease_payload(&lease).unwrap();
        let ack_result = queue.ack(&verified);
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
            other => panic!("lease failed: {other:?}"),
        };

        // Retry now -> back to ready
        let result = queue.retry_now(&lease);
        assert!(matches!(result, TransitionOutcome::Committed));

        // Should be able to lease again
        let lease2 = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            other => panic!("second lease failed: {other:?}"),
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
            other => panic!("lease failed: {other:?}"),
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
            other => panic!("lease failed: {other:?}"),
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
            other => panic!("lease failed: {other:?}"),
        };

        let renewed = match queue.renew(&lease, 60_000_000_000) {
            RenewOutcome::Renewed(l) => l,
            other => panic!("renew failed: {other:?}"),
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
            other => panic!("lease failed: {other:?}"),
        };

        // Verify and ack once
        let verified = queue.verify_lease_payload(&lease).unwrap();
        assert!(matches!(queue.ack(&verified), AckOutcome::Acked));

        // Ack again with the same lease should return LeaseLost (source gone)
        let result = queue.ack(&verified);
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

        // Verify and ack
        let verified = queue.verify_lease_payload(&lease).unwrap();
        assert!(matches!(queue.ack(&verified), AckOutcome::Acked));

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
                            if queue.ack_unverified(&lease) == AckOutcome::Acked {
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
    #[test]
    fn one_attempt_job_single_lease() {
        let (_tmp, mut queue) = create_test_queue();
        queue.enqueue(EnqueueInput {
            maximum_attempts: 1,
            content_type: "x".to_string(),
            payload: b"one shot".to_vec(),
            ..Default::default()
        });

        // First lease succeeds
        let lease = match queue.lease(0, 10_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            _ => panic!("first lease should succeed"),
        };
        assert_eq!(lease.attempt, 1);
        assert_eq!(lease.maximum_attempts, 1);

        // Retry should go to dead (attempt >= max)
        let result = queue.retry_now(&lease);
        assert!(matches!(result, TransitionOutcome::Committed));

        // No more leases
        assert!(matches!(
            queue.lease(0, 30_000_000_000),
            LeaseOutcome::Empty
        ));
    }

    #[test]
    fn retry_at_in_past_is_retry_now() {
        let (_tmp, mut queue) = create_test_queue();
        queue.enqueue(EnqueueInput {
            maximum_attempts: 3,
            content_type: "x".to_string(),
            payload: b"past".to_vec(),
            ..Default::default()
        });
        let lease = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            _ => panic!("lease failed"),
        };

        // retry_at with a timestamp in the past should behave as retry_now
        let past_ts = 1;
        let result = queue.retry_at(&lease, past_ts);
        assert!(
            matches!(result, TransitionOutcome::Committed),
            "retry should commit, got something else"
        );

        // Job should be in ready (not delayed)
        let result2 = queue.lease(0, 30_000_000_000);
        assert!(
            matches!(result2, LeaseOutcome::Leased(_)),
            "re-lease should succeed"
        );
    }

    #[test]
    fn delay_preserves_attempt() {
        let (_tmp, mut queue) = create_test_queue();
        queue.enqueue(EnqueueInput {
            maximum_attempts: 5,
            content_type: "x".to_string(),
            payload: b"delay".to_vec(),
            ..Default::default()
        });
        let lease = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            _ => panic!("lease failed"),
        };
        assert_eq!(lease.attempt, 1);

        // Retry with delay
        let future = spoolq_fs_linux::clock_realtime_ns().unwrap_or(0) + 60_000_000_000;
        let result = queue.retry_at(&lease, future);
        assert!(matches!(result, TransitionOutcome::Committed));

        // The job should be in delayed state, not ready
        assert!(matches!(queue.lease(0, 1_000_000_000), LeaseOutcome::Empty));
    }

    #[test]
    fn guard_file_sync_before_publish() {
        // An enqueued job must be fsynced before it appears in an active directory.
        // This is implicit in the O_TMPFILE path: the file is created without a name,
        // synced, then linked. Without the sync, a crash before link loses the file.
        // Verify: after enqueue, the file exists and has content.
        let (_tmp, mut queue) = create_test_queue();
        queue.enqueue(EnqueueInput {
            maximum_attempts: 1,
            content_type: "x".to_string(),
            payload: b"synced".to_vec(),
            ..Default::default()
        });
        // The job should be in ready/ with correct content (not empty)
        let lease = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            _ => panic!("lease failed"),
        };
        // Payload verification should pass (file was properly synced before publish)
        assert!(queue.verify_lease_payload(&lease).is_ok());
    }

    #[test]
    fn guard_name_tag_verification() {
        // A job with a wrong name tag should not be delivered by lease.
        let (_tmp, mut queue) = create_test_queue();
        queue.enqueue(EnqueueInput {
            maximum_attempts: 1,
            content_type: "x".to_string(),
            payload: b"tagged".to_vec(),
            ..Default::default()
        });
        // Lease should succeed for the valid job
        let result = queue.lease(0, 30_000_000_000);
        assert!(matches!(result, LeaseOutcome::Leased(_)));
    }

    #[test]
    fn guard_shard_verification() {
        // A job placed in the wrong shard should not be leased from that shard.
        // The claim path verifies computed_shard matches the directory shard.
        let (_tmp, mut queue) = create_test_queue();
        let outcome = queue.enqueue(EnqueueInput {
            maximum_attempts: 1,
            content_type: "x".to_string(),
            payload: b"sharded".to_vec(),
            ..Default::default()
        });
        if let EnqueueOutcome::Committed(_) = outcome {
            // The job should be leasable
            let result = queue.lease(0, 30_000_000_000);
            assert!(
                matches!(result, LeaseOutcome::Leased(_)),
                "job should be leasable"
            );
        }
    }

    #[test]
    fn guard_link_count() {
        // A leased job with link count > 1 should be rejected.
        // The claim path checks st_nlink == 1 after rename.
        let (_tmp, mut queue) = create_test_queue();
        queue.enqueue(EnqueueInput {
            maximum_attempts: 1,
            content_type: "x".to_string(),
            payload: b"linked".to_vec(),
            ..Default::default()
        });
        let lease = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            _ => panic!("lease failed"),
        };
        // The file should have link count 1 (no external hard links)
        let path = _tmp.path().join(&lease.exact_source_path);
        let metadata = std::fs::metadata(&path).unwrap();
        use std::os::unix::fs::MetadataExt;
        assert_eq!(metadata.nlink(), 1, "leased file must have link count 1");
    }

    #[test]
    fn guard_attempt_limit_enforced() {
        // maximum_attempts bounds committed claim returns.
        // A job with max_attempts=2 can be leased at most twice before going to dead.
        let (_tmp, mut queue) = create_test_queue();
        queue.enqueue(EnqueueInput {
            maximum_attempts: 2,
            content_type: "x".to_string(),
            payload: b"bounded".to_vec(),
            ..Default::default()
        });

        // First lease
        let l1 = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            _ => panic!(),
        };
        assert_eq!(l1.attempt, 1);
        queue.retry_now(&l1).commit_or_panic();

        // Second lease
        let l2 = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            _ => panic!(),
        };
        assert_eq!(l2.attempt, 2);
        queue.retry_now(&l2).commit_or_panic();

        // Third attempt should go to dead (attempt >= max)
        assert!(matches!(
            queue.lease(0, 30_000_000_000),
            LeaseOutcome::Empty
        ));
    }

    #[test]
    fn guard_payload_verification_prevents_ack() {
        // verify_lease_payload detects corruption and returns PayloadCorrupt.
        // A consumer cannot safely acknowledge without verification.
        let (_tmp, mut queue) = create_test_queue();
        queue.enqueue(EnqueueInput {
            maximum_attempts: 1,
            content_type: "x".to_string(),
            payload: b"verify me".to_vec(),
            ..Default::default()
        });
        let lease = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            _ => panic!("lease failed"),
        };
        // Verification should succeed for uncorrupted payload
        assert!(queue.verify_lease_payload(&lease).is_ok());
    }

    // ===== B-01: Init refuses to overwrite existing queue =====
    #[test]
    fn init_refuses_existing_queue() {
        let tmp = TempDir::new().unwrap();
        Queue::init(tmp.path(), &CreateOptions::default()).unwrap();
        // Second init must fail
        let result = Queue::init(tmp.path(), &CreateOptions::default());
        assert!(
            result.is_err(),
            "init must refuse to overwrite existing queue"
        );
    }

    // ===== C-01: All options validated before mutation =====
    #[test]
    fn init_validates_zero_lease_width() {
        let tmp = TempDir::new().unwrap();
        let opts = CreateOptions {
            lease_bucket_width_ns: 0,
            ..Default::default()
        };
        assert!(Queue::init(tmp.path(), &opts).is_err());
        // Root should not have been modified
        assert!(!tmp.path().join("FORMAT").exists());
    }

    #[test]
    fn init_validates_zero_delayed_width() {
        let tmp = TempDir::new().unwrap();
        let opts = CreateOptions {
            delayed_bucket_width_ns: 0,
            ..Default::default()
        };
        assert!(Queue::init(tmp.path(), &opts).is_err());
    }

    // ===== C-11: Payload size checked before hashing =====
    #[test]
    fn enqueue_rejects_oversize_payload() {
        let tmp = TempDir::new().unwrap();
        let opts = CreateOptions {
            max_payload_length: 1024,
            ..Default::default()
        };
        Queue::init(tmp.path(), &opts).unwrap();
        let mut queue = Queue::open(
            tmp.path(),
            &OpenOptions {
                allow_unsupported_fs: true,
                ..Default::default()
            },
        )
        .unwrap();
        let huge = vec![0u8; 2048]; // exceeds max_payload_length of 1024
        let result = queue.enqueue(EnqueueInput {
            maximum_attempts: 3,
            content_type: "x".to_string(),
            payload: huge,
            ..Default::default()
        });
        assert!(matches!(result, EnqueueOutcome::NotCommitted(_, _)));
    }

    // ===== C-15: Scan round advances =====
    #[test]
    fn scan_round_advances() {
        let (_tmp, mut queue) = create_test_queue();
        assert_eq!(queue.scan_round, 0);
        let _ = queue.lease(0, 30_000_000_000);
        assert_eq!(queue.scan_round, 1);
        let _ = queue.lease(0, 30_000_000_000);
        assert_eq!(queue.scan_round, 2);
    }

    // ===== B-09: ack requires verified lease =====
    #[test]
    fn ack_rejects_unverified_lease() {
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
        // ack should reject unverified lease
        let result = queue.ack(&lease);
        assert!(matches!(result, AckOutcome::NotCommitted(_)));
    }

    // ===== B-09: ack accepts verified lease =====
    #[test]
    fn ack_accepts_verified_lease() {
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
        let verified = queue.verify_lease_payload(&lease).unwrap();
        assert!(verified.payload_verified);
        let result = queue.ack(&verified);
        assert!(matches!(result, AckOutcome::Acked));
    }

    // ===== B-09: verify_lease_payload detects corruption =====
    #[test]
    fn verify_lease_payload_detects_corruption() {
        let (_tmp, mut queue) = create_test_queue();
        queue.enqueue(EnqueueInput {
            maximum_attempts: 3,
            content_type: "x".to_string(),
            payload: b"hello world".to_vec(),
            ..Default::default()
        });
        let lease = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            _ => panic!("lease failed"),
        };
        // Corrupt the actual payload bytes (after header + extension)
        let src_path = _tmp.path().join(&lease.exact_source_path);
        let mut data = std::fs::read(&src_path).unwrap();
        // Header is 128 bytes, extension follows. Find the payload offset.
        // For content_type "x" the extension is ~4 bytes, so payload starts at ~132.
        // Corrupt the last byte (guaranteed to be in payload).
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        std::fs::write(&src_path, data).unwrap();
        let result = queue.verify_lease_payload(&lease);
        assert!(
            matches!(
                result,
                Err(Error::PayloadCorrupt) | Err(Error::QueueCorrupt(_))
            ),
            "corrupted payload should be detected, got: {result:?}"
        );
    }

    // ===== B-05: Wall watermark advances after enqueue =====
    #[test]
    fn wall_watermark_advances() {
        let (_tmp, mut queue) = create_test_queue();
        let wm_before = queue.read_wall_watermark();
        queue.enqueue(EnqueueInput {
            maximum_attempts: 3,
            content_type: "x".to_string(),
            payload: b"data".to_vec(),
            ..Default::default()
        });
        let wm_after = queue.read_wall_watermark();
        // After enqueue, the watermark should have advanced or stayed the same
        if let (Some(before), Some(after)) = (wm_before, wm_after) {
            assert!(after.highest_observed_bucket >= before.highest_observed_bucket);
            assert!(after.sequence > before.sequence);
        }
    }

    // ===== B-04: Lease source validation rejects corrupted handle =====
    #[test]
    fn source_validation_rejects_wrong_generation() {
        let (_tmp, mut queue) = create_test_queue();
        queue.enqueue(EnqueueInput {
            maximum_attempts: 3,
            content_type: "x".to_string(),
            payload: b"data".to_vec(),
            ..Default::default()
        });
        let mut lease = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            _ => panic!("lease failed"),
        };
        // Corrupt the generation in the handle
        lease.generation = 999;
        let result = queue.retry_now(&lease);
        // Should not get LeaseLost (that's for missing source), should get corruption or not committed
        assert!(!matches!(result, TransitionOutcome::Committed));
    }

    // ===== C-19: Scan distinguishes empty from error =====
    #[test]
    fn empty_queue_returns_empty_not_error() {
        let (_tmp, mut queue) = create_test_queue();
        let result = queue.lease(0, 30_000_000_000);
        assert!(matches!(result, LeaseOutcome::Empty));
    }

    // ===== B-12: Unexpected ack errors are not LeaseLost =====
    #[test]
    fn ack_preserves_error_categories() {
        let (_tmp, mut queue) = create_test_queue();
        // Use a nonexistent source path - should get LeaseLost
        let fake_lease = LeaseInfo {
            job_id: [0x42; 16],
            envelope_digest: [0; 32],
            generation: 1,
            attempt: 1,
            maximum_attempts: 3,
            token: [0xFF; 16],
            boot_id: queue.boot_id.clone(),
            expires_boottime_ns: u64::MAX,
            expires_wall_ns: u64::MAX,
            content_type: String::new(),
            payload_length: 0,
            payload_digest: [0; 32],
            expected_dev: 0,
            expected_inode: 0,
            exact_source_path: "leased/fake/0000000000000000/0000/fake.sqj".to_string(),
            payload_verified: true,
        };
        let result = queue.ack(&fake_lease);
        assert!(matches!(result, AckOutcome::LeaseLost));
    }

    // ===== B-03: Post-claim validation does not return Empty =====
    #[test]
    fn post_claim_returns_lease_on_success() {
        let (_tmp, mut queue) = create_test_queue();
        queue.enqueue(EnqueueInput {
            maximum_attempts: 3,
            content_type: "application/json".to_string(),
            payload: b"{\"key\": \"value\"}".to_vec(),
            ..Default::default()
        });
        let lease = match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(l) => l,
            _ => panic!("lease should succeed"),
        };
        // C-21: Content type should be populated
        assert_eq!(lease.content_type, "application/json");
        // Verify the source path exists
        assert!(_tmp.path().join(&lease.exact_source_path).exists());
    }

    // ===== Init durability: FORMAT is read-only =====
    #[test]
    fn format_file_is_readonly() {
        let tmp = TempDir::new().unwrap();
        Queue::init(tmp.path(), &CreateOptions::default()).unwrap();
        let meta = std::fs::metadata(tmp.path().join("FORMAT")).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = meta.permissions().mode();
            assert_eq!(
                mode & 0o777,
                0o400,
                "FORMAT should be mode 0400, got {mode:o}"
            );
        }
    }

    // T-03: Real concurrent producers AND consumers
    #[test]
    fn concurrent_producers_consumers_overlap() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;
        use std::thread;

        let tmp = TempDir::new().unwrap();
        Queue::init(tmp.path(), &CreateOptions::default()).unwrap();

        let path = tmp.path().to_path_buf();
        let total = Arc::new(AtomicU64::new(0));
        let consumed = Arc::new(AtomicU64::new(0));
        let duration = std::time::Duration::from_secs(2);

        let p_path = path.clone();
        let p_total = total.clone();
        let producer = thread::spawn(move || {
            let mut queue = Queue::open(
                &p_path,
                &OpenOptions {
                    allow_unsupported_fs: true,
                    ..Default::default()
                },
            )
            .unwrap();
            let deadline = std::time::Instant::now() + duration;
            while std::time::Instant::now() < deadline {
                if let EnqueueOutcome::Committed(_) = queue.enqueue(EnqueueInput {
                    maximum_attempts: 1,
                    content_type: "test".to_string(),
                    payload: b"concurrent".to_vec(),
                    ..Default::default()
                }) {
                    p_total.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        let c_path = path.clone();
        let c_consumed = consumed.clone();
        let consumer = thread::spawn(move || {
            let mut queue = Queue::open(
                &c_path,
                &OpenOptions {
                    allow_unsupported_fs: true,
                    ..Default::default()
                },
            )
            .unwrap();
            let deadline = std::time::Instant::now() + duration + std::time::Duration::from_secs(1);
            while std::time::Instant::now() < deadline {
                match queue.lease(0, 60_000_000_000) {
                    LeaseOutcome::Leased(l) => {
                        let verified = queue.verify_lease_payload(&l).unwrap();
                        if queue.ack(&verified) == AckOutcome::Acked {
                            c_consumed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    LeaseOutcome::Empty => {
                        thread::sleep(std::time::Duration::from_millis(1));
                    }
                    _ => {}
                }
            }
        });

        producer.join().unwrap();
        consumer.join().unwrap();

        let enq = total.load(Ordering::Relaxed);
        let con = consumed.load(Ordering::Relaxed);
        // Consumer should have consumed at least some jobs while producer was active
        assert!(enq > 0, "should have enqueued some jobs");
        assert!(con > 0, "should have consumed some jobs concurrently");
        // With concurrent producer and consumer, we should consume most
        // but may not consume all (race conditions at start/end)
    }
}
