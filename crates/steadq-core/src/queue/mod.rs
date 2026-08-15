// SteadQ/1 queue initialization, open, and enqueue operations.

mod batch;
mod cursors;
pub mod engine;
pub mod layout;
mod options;
mod resolve;
pub mod verified;
mod watermark;

pub use batch::*;
pub(crate) use cursors::*;
pub use options::*;
pub use watermark::*;

use std::io;
use std::os::unix::io::{AsFd, BorrowedFd, OwnedFd};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use steadq_format::cbor::ExtensionHeader;

use steadq_format::{
    envelope_digest, payload_digest, FixedHeader, FormatRecord, WatermarkRecord,
    DIGEST_ALGORITHM_SHA256, FORMAT_MINOR, MAX_PAYLOAD_LENGTH,
};
use steadq_fs_linux as fs;
use steadq_math::{self, bucket_number, ceiling_bucket, eligibility_bucket_and_ns};
use steadq_names::{self, bucket_hex, compute_shard, shard_hex, temp_filename, CommonFields};

use crate::errors::*;
use crate::state_machine::ObjectKind;

pub struct Queue {
    pub(crate) root_fd: OwnedFd,
    #[allow(dead_code)]
    pub(crate) root_path: PathBuf,
    pub(crate) format: FormatRecord,
    pub(crate) boot_id: String,
    pub(crate) boot_id_bytes: [u8; 16],
    pub(crate) poisoned: bool,
    pub(crate) scan_round: u64,
    pub(crate) ready_shard_hint: Option<u32>,
    pub(crate) worker_nonce: [u8; 16],
    pub(crate) options: OpenOptions,
    #[allow(dead_code)]
    pub(crate) maint_lock_fd: Option<OwnedFd>,
    pub(crate) recovery_cursor: RecoveryCursor,
    pub(crate) cached_wall_floor: Option<WallFloor>,
    pub(crate) known_dirs: std::cell::RefCell<std::collections::HashSet<String>>,
    pub(crate) cached_dest_fd: Option<(String, std::os::fd::OwnedFd)>,
    pub(crate) publication_mode: Option<fs::PublicationMode>,
    pub(crate) deferred_dir_sync: bool,
    pub(crate) dirty: std::cell::RefCell<engine::DirtySet>,
}

struct ClaimSourceWitness {
    file_fd: OwnedFd,
    device: u64,
    inode: u64,
    evidence: TicketEvidence,
}

struct LeasedSourceWitness {
    directory_fd: OwnedFd,
    name: String,
    file_fd: OwnedFd,
    device: u64,
    inode: u64,
}

/// A reader for a verified lease payload that does not re-hash on each read.
///
/// The payload is verified once at construction. Subsequent `read_at` calls
/// perform direct pread on the held fd, avoiding the O(n^2) cost of calling
/// `read_lease_payload_chunk` repeatedly.
pub struct VerifiedPayloadReader {
    file_fd: OwnedFd,
    payload_start: u64,
    payload_len: u64,
}

impl VerifiedPayloadReader {
    /// Read payload bytes at the given offset into buf.
    /// Returns the number of bytes read (0 at EOF).
    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize, Error> {
        if offset >= self.payload_len {
            return Ok(0);
        }
        let to_read = (buf.len() as u64).min(self.payload_len - offset) as usize;
        let abs_offset = self.payload_start + offset;
        let n = fs::pread(self.file_fd.as_fd(), &mut buf[..to_read], abs_offset)
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        Ok(n)
    }

    /// Total payload length in bytes.
    pub fn payload_len(&self) -> u64 {
        self.payload_len
    }
}

#[derive(Debug)]
enum LeasedMoveOutcome {
    Committed,
    OutcomeUnknown(TransitionPhase),
    SourceGone,
    SourceChanged,
    Collision,
    Failed(Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WitnessPathObservation {
    Match,
    Gone,
    Mismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseDirectoryOpenFailure {
    Gone,
    InvalidDirectory,
    Io,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PresenceFailure {
    Absent,
    Io,
}

fn observe_witness_path(
    directory_fd: BorrowedFd<'_>,
    name: &str,
    device: u64,
    inode: u64,
) -> Result<WitnessPathObservation, Error> {
    match fs::fstatat(directory_fd, name) {
        Ok(stat) if stat_matches_witness(&stat, device, inode) => Ok(WitnessPathObservation::Match),
        Ok(_) => Ok(WitnessPathObservation::Mismatch),
        Err(error) if error.raw_os_error() == Some(libc::ENOENT) => {
            Ok(WitnessPathObservation::Gone)
        }
        Err(error) => Err(Error::IoFailure(error.to_string())),
    }
}

fn is_singly_linked_regular(mode: libc::mode_t, link_count: libc::nlink_t) -> bool {
    mode & libc::S_IFMT == libc::S_IFREG && link_count == 1
}

fn stat_matches_witness(stat: &libc::stat, device: u64, inode: u64) -> bool {
    is_singly_linked_regular(stat.st_mode, stat.st_nlink)
        && identity_matches(stat.st_dev, stat.st_ino, device, inode)
}

fn resolver_file_open_flags() -> i32 {
    libc::O_NOFOLLOW
        .checked_add(libc::O_CLOEXEC)
        .and_then(|flags| flags.checked_add(libc::O_NONBLOCK))
        .expect("Linux open flags fit i32")
}

fn classify_lease_directory_open_failure(error: &io::Error) -> LeaseDirectoryOpenFailure {
    match error.raw_os_error() {
        Some(libc::ENOENT) => LeaseDirectoryOpenFailure::Gone,
        Some(libc::ENOTDIR) => LeaseDirectoryOpenFailure::InvalidDirectory,
        _ => LeaseDirectoryOpenFailure::Io,
    }
}

fn classify_presence_failure(error: &io::Error) -> PresenceFailure {
    match error.raw_os_error() {
        Some(libc::ENOENT) => PresenceFailure::Absent,
        _ => PresenceFailure::Io,
    }
}

fn ticket_phase_for_move_outcome_unknown(phase: engine::MovePhase) -> TransitionPhase {
    match phase {
        engine::MovePhase::SourceFsync => TransitionPhase::DestinationDirectoryDurable,
        engine::MovePhase::EnsureDest
        | engine::MovePhase::PreRename
        | engine::MovePhase::Rename
        | engine::MovePhase::DestinationIdentity
        | engine::MovePhase::PostLinearization
        | engine::MovePhase::DestFsync => TransitionPhase::Linearized,
    }
}

fn identity_matches(device: u64, inode: u64, expected_device: u64, expected_inode: u64) -> bool {
    device == expected_device && inode == expected_inode
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetryTiming {
    Immediate,
    Delayed {
        not_before_ns: u64,
        wall_floor: WallFloor,
    },
}

fn preferred_publication_mode(filesystem_type: i64) -> Option<fs::PublicationMode> {
    // OpenZFS can stall O_TMPFILE link publication behind transaction-group
    // work; its ordinary named-temp rename path avoids that slow path while
    // retaining the same no-overwrite and durability checks.
    (filesystem_type == fs::ZFS_SUPER_MAGIC).then_some(fs::PublicationMode::NamedFallback)
}

fn classify_filesystem_type(
    observation: io::Result<i64>,
    allow_unsupported: bool,
) -> Result<Option<i64>, Error> {
    match observation {
        Ok(filesystem_type)
            if allow_unsupported
                || matches!(
                    filesystem_type,
                    fs::EXT4_SUPER_MAGIC
                        | fs::XFS_SUPER_MAGIC
                        | fs::BTRFS_SUPER_MAGIC
                        | fs::F2FS_SUPER_MAGIC
                        | fs::ZFS_SUPER_MAGIC
                ) =>
        {
            Ok(Some(filesystem_type))
        }
        Ok(_) => Err(Error::UnsupportedFilesystem),
        Err(_) if allow_unsupported => Ok(None),
        Err(error) => Err(Error::IoFailure(error.to_string())),
    }
}

/// Active path context for tag authentication.
#[derive(Clone, Debug)]
pub enum ActivePathContext {
    Ready {
        shard: String,
    },
    Leased {
        boot_id: String,
        bucket: String,
        shard: String,
    },
    Delayed {
        bucket: String,
        shard: String,
    },
}

impl Queue {
    /// Initialize a new queue at the given path.
    pub fn init(root: &Path, opts: &CreateOptions) -> io::Result<FormatRecord> {
        // C-01: Validate all options before any filesystem mutation
        validate_create_options(opts)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

        // P1-23: Preflight filesystem check before any mutation.
        // If the root already exists, check its filesystem. If creating,
        // check the parent's filesystem.
        let check_path = if root.exists() {
            root
        } else {
            root.parent().unwrap_or(root)
        };
        let magic = fs::statfs(check_path).map_err(|e| io::Error::other(format!("statfs: {e}")))?;
        let ft = magic.f_type as i64;
        match ft {
            fs::EXT4_SUPER_MAGIC
            | fs::XFS_SUPER_MAGIC
            | fs::BTRFS_SUPER_MAGIC
            | fs::F2FS_SUPER_MAGIC
            | fs::ZFS_SUPER_MAGIC => {}
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!(
                        "filesystem type not supported for queue (observed magic {ft:#x}; requires ext4, xfs, btrfs, or f2fs)"
                    ),
                ));
            }
        }

        // Create root directory if needed
        if !root.exists() {
            std::fs::create_dir_all(root)?;
            // Sync the parent directory so the root entry persists
            if let Some(parent) = root.parent() {
                let parent_fd = fs::open_dir_absolute(parent)?;
                fs::fsync_dir_fd(parent_fd.as_fd())?;
            }
        }

        let root_fd = fs::open_dir_absolute(root)?;

        // R2-B01: Refuse to overwrite an existing queue.
        let format_exists = fs::fstatat(root_fd.as_fd(), "FORMAT").is_ok();
        if format_exists {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "queue already initialized; use open() to access an existing queue",
            ));
        }

        // R2-B01/P1-08: Create an exclusive initialization marker BEFORE any other state.
        // P1-08: If .initializing already exists but FORMAT is absent, the previous
        // init was interrupted by a crash. Safe to clean up and retry since no FORMAT
        // means no queue identity was committed.
        let _init_marker = match fs::create_exclusive(root_fd.as_fd(), ".initializing", 0o600) {
            Ok(fd) => fd,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                // .initializing exists. If FORMAT is absent, this is a stale marker
                // from a crashed init. Safe to remove and retry.
                fs::unlinkat(root_fd.as_fd(), ".initializing")?;
                fs::create_exclusive(root_fd.as_fd(), ".initializing", 0o600).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "could not acquire init lock after cleaning stale marker",
                    )
                })?
            }
            Err(e) => return Err(e),
        };

        // R2-B01: Use RAII guard to clean up the init marker on any failure.
        struct InitGuard<'fd> {
            root_fd: BorrowedFd<'fd>,
            armed: bool,
        }
        impl Drop for InitGuard<'_> {
            fn drop(&mut self) {
                if self.armed {
                    // Remove the marker so a failed init can be retried
                    let _ = fs::unlinkat(self.root_fd, ".initializing");
                }
            }
        }
        let mut init_guard = InitGuard {
            root_fd: root_fd.as_fd(),
            armed: true,
        };

        // R2-B01: Create control/ early so we can hold the maintenance lock
        // with RAII (no mem::forget leak).
        fs::mkdirat_eexist_ok(root_fd.as_fd(), "control", 0o700)?;
        let control_fd = fs::open_directory(root_fd.as_fd(), "control")?;
        let lock_fd =
            fs::create_exclusive(control_fd.as_fd(), "maintenance.lock", 0o600).or_else(|e| {
                if e.kind() == io::ErrorKind::AlreadyExists {
                    fs::openat(control_fd.as_fd(), "maintenance.lock", libc::O_RDWR, 0o600)
                } else {
                    Err(e)
                }
            })?;
        let locked = fs::try_ofd_write_lock(lock_fd.as_fd())?;
        if !locked {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "another initializer or maintenance process holds the lock",
            ));
        }
        // H1: Hold the maintenance lock for the duration of init by binding it.
        // It will be released when _init_lock goes out of scope at function end.
        let _init_lock = lock_fd;

        // Generate queue ID
        let queue_id = fs::random_128bit()?;
        let created_at = fs::clock_realtime_ns()?;

        let format_rec = FormatRecord::new(
            queue_id,
            created_at,
            opts.shard_count,
            opts.lease_bucket_width_ns,
            opts.delayed_bucket_width_ns,
            opts.terminal_bucket_width_ns,
            opts.max_payload_length,
        )
        .map_err(|e| io::Error::other(e.to_string()))?;

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
            fs::mkdirat_eexist_ok(root_fd.as_fd(), dir, 0o700)?;
        }
        // Sync root after directory creation
        fs::fsync_dir_fd(root_fd.as_fd())?;

        // Create static shard directories under ready/
        let ready_fd = fs::open_directory(root_fd.as_fd(), "ready")?;
        for i in 0..opts.shard_count {
            let shard_name = format!("{i:04x}");
            fs::mkdirat_eexist_ok(ready_fd.as_fd(), &shard_name, 0o700)?;
        }
        // Sync ready/ after shard creation
        fs::fsync_dir_fd(ready_fd.as_fd())?;
        // Sync root
        fs::fsync_dir_fd(root_fd.as_fd())?;

        // Create control lock files
        let control_fd = fs::open_directory(root_fd.as_fd(), "control")?;
        for lock_file in ["maintenance.lock", "wall-watermark.lock", "recovery.lock"] {
            let fd = fs::create_exclusive(control_fd.as_fd(), lock_file, 0o600).or_else(|e| {
                if e.kind() == io::ErrorKind::AlreadyExists {
                    fs::openat(control_fd.as_fd(), lock_file, 0o2, 0o600)
                } else {
                    Err(e)
                }
            })?;
            fs::fsync(fd.as_fd())?;
        }
        fs::fsync_dir_fd(control_fd.as_fd())?;
        fs::fsync_dir_fd(root_fd.as_fd())?;

        // Write initial wall watermark
        let wall_now = created_at;
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
            steadq_names::hex_encode(&fs::random_128bit()?)
        );
        let wm_tmp = fs::create_exclusive(control_fd.as_fd(), &wm_tmp_name, 0o600)?;
        fs::write_all(wm_tmp.as_fd(), &wm_bytes)?;
        fs::fsync(wm_tmp.as_fd())?;
        fs::renameat(
            control_fd.as_fd(),
            &wm_tmp_name,
            control_fd.as_fd(),
            "wall-watermark",
        )?;
        fs::fsync_dir_fd(control_fd.as_fd())?;

        // Write FORMAT file
        let format_bytes = format_rec.encode();
        // C-03: Unique temp name for partial init recovery
        let fmt_tmp_name = format!(
            ".format.tmp.{}",
            steadq_names::hex_encode(&fs::random_128bit()?)
        );
        let fmt_tmp = fs::create_exclusive(root_fd.as_fd(), &fmt_tmp_name, 0o600)?;
        fs::write_all(fmt_tmp.as_fd(), &format_bytes)?;
        fs::fsync(fmt_tmp.as_fd())?;
        // C-02: Set FORMAT temp file to read-only before publication so the
        // published FORMAT is read-only even if the post-rename chmod is
        // skipped by an OutcomeUnknown return.
        fs::fchmodat(root_fd.as_fd(), &fmt_tmp_name, 0o400)?;
        // Publish FORMAT through the phase-aware executor so post-linearization
        // failures are classified correctly.
        match engine::move_verified_noreplace(
            root_fd.as_fd(),
            &fmt_tmp_name,
            root_fd.as_fd(),
            "FORMAT",
            engine::MoveActor::Producer,
        ) {
            Ok(()) => {}
            Err(engine::MoveFailure::AlreadyExists) => {
                let _ = fs::unlinkat(root_fd.as_fd(), &fmt_tmp_name);
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "another initializer published FORMAT first",
                ));
            }
            Err(engine::MoveFailure::NotCommitted { phase, source }) => {
                let _ = fs::unlinkat(root_fd.as_fd(), &fmt_tmp_name);
                return Err(io::Error::other(format!(
                    "FORMAT publication failed at {phase:?}: {source}"
                )));
            }
            Err(engine::MoveFailure::OutcomeUnknown { phase, source }) => {
                // FORMAT may or may not be durable. The init marker stays so
                // a reopening process can detect the indeterminate state.
                return Err(io::Error::other(format!(
                    "FORMAT publication indeterminate at {phase:?}: {source}"
                )));
            }
            Err(engine::MoveFailure::SourceMissing) => {
                return Err(io::Error::other(
                    "FORMAT temp file vanished during publication",
                ));
            }
        }

        // FORMAT is now the linearization point. The executor has synced the
        // root directory. Remove the init marker and sync once more. These are
        // post-commit operations: FORMAT is published and the queue is usable,
        // so cleanup failures do not change the init outcome.
        init_guard.armed = false;
        let _ = fs::unlinkat(root_fd.as_fd(), ".initializing");
        let _ = fs::fsync_dir_fd(root_fd.as_fd());

        Ok(format_rec)
    }

    /// Open an existing queue.
    pub fn open(root: &Path, opts: &OpenOptions) -> Result<Self, Error> {
        // B-11: Open root first using descriptor-relative, no-symlink semantics
        let root_fd = fs::open_dir_absolute(root).map_err(|e| Error::IoFailure(e.to_string()))?;

        // B-11: Validate root is a directory
        let root_stat = fs::fstat(root_fd.as_fd()).map_err(|e| Error::IoFailure(e.to_string()))?;
        if root_stat.st_mode & libc::S_IFMT != libc::S_IFDIR {
            return Err(Error::QueueCorrupt("root path is not a directory".into()));
        }

        // B-11: Read FORMAT through descriptor-relative open, not pathname.
        // If FORMAT is absent, check whether an initialization was interrupted.
        let format_fd = match fs::openat(root_fd.as_fd(), "FORMAT", libc::O_RDONLY, 0) {
            Ok(fd) => fd,
            Err(e) if e.raw_os_error() == Some(libc::ENOENT) => {
                if fs::fstatat(root_fd.as_fd(), ".initializing").is_ok() {
                    return Err(Error::QueueCorrupt(
                        "queue initialization was interrupted; remove .initializing and retry init"
                            .into(),
                    ));
                }
                return Err(Error::QueueCorrupt("FORMAT file is missing".into()));
            }
            Err(e) => return Err(Error::IoFailure(e.to_string())),
        };
        let mut format_bytes = Vec::new();
        {
            let mut buf = [0u8; 4096];
            loop {
                match fs::read(format_fd.as_fd(), &mut buf) {
                    Ok(0) => break,
                    Ok(n) => format_bytes.extend_from_slice(&buf[..n]),
                    Err(e) => return Err(Error::IoFailure(e.to_string())),
                }
            }
        }
        let format_rec = FormatRecord::decode(&format_bytes).map_err(|e| match e {
            steadq_format::FormatError::UnsupportedVersion(_, _) => Error::UnsupportedFormat,
            _ => Error::QueueCorrupt(format!("FORMAT decode: {e}")),
        })?;

        // Validate retention bound: ceil(retention / terminal_width) + 2 <= 4096
        let probe_count = ceiling_bucket(
            opts.receipt_retention_ns,
            format_rec.terminal_bucket_width_ns(),
        )
        .ok_or_else(|| Error::QueueCorrupt("invalid terminal bucket width".into()))?
        .saturating_add(2);
        if probe_count > 4096 {
            return Err(Error::InvalidInput(
                "receipt retention exceeds duplicate-ack probe bound".into(),
            ));
        }

        // Check filesystem type. Keep the observation even when validation is
        // relaxed because publication performance differs materially by backend.
        let filesystem_type = classify_filesystem_type(
            fs::statfs(root).map(|stat| stat.f_type),
            opts.allow_unsupported_fs,
        )?;

        // B-11: Require all state directories to exist and be on the same device.
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
            match fs::fstatat(root_fd.as_fd(), state_dir) {
                Ok(stat) => {
                    if stat.st_dev != root_stat.st_dev {
                        return Err(Error::QueueCorrupt(format!(
                            "state directory '{state_dir}' is on a different device than root"
                        )));
                    }
                    if stat.st_mode & libc::S_IFMT != libc::S_IFDIR {
                        return Err(Error::QueueCorrupt(format!(
                            "state path '{state_dir}' is not a directory"
                        )));
                    }
                }
                Err(_) => {
                    return Err(Error::QueueCorrupt(format!(
                        "required state directory '{state_dir}' is missing"
                    )));
                }
            }
        }

        // Read boot ID
        let boot_id = fs::read_boot_id().map_err(|e| Error::IoFailure(e.to_string()))?;
        let boot_id_bin = steadq_names::boot_id_bytes(&boot_id)
            .ok_or_else(|| Error::InvalidInput("invalid boot_id format".into()))?;

        // Generate worker nonce
        let worker_nonce = fs::random_128bit().map_err(|e| Error::IoFailure(e.to_string()))?;

        // Acquire shared maintenance lock
        let maint_fd = fs::openat(root_fd.as_fd(), "control/maintenance.lock", 0o0, 0o600)
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        let locked =
            fs::try_ofd_read_lock(maint_fd.as_fd()).map_err(|e| Error::IoFailure(e.to_string()))?;
        if !locked {
            return Err(Error::MaintenanceBusy);
        }
        let recovery_cursor =
            crate::recovery::load_recovery_cursor(root_fd.as_fd(), format_rec.queue_id())?;
        let publication_mode = filesystem_type.and_then(preferred_publication_mode);
        Ok(Queue {
            root_fd,
            root_path: root.to_path_buf(),
            format: format_rec,
            boot_id,
            boot_id_bytes: boot_id_bin,
            poisoned: false,
            scan_round: 0,
            ready_shard_hint: None,
            worker_nonce,
            options: opts.clone(),
            maint_lock_fd: Some(maint_fd),
            recovery_cursor,
            cached_wall_floor: None,
            known_dirs: std::cell::RefCell::new(std::collections::HashSet::new()),
            cached_dest_fd: None,
            publication_mode,
            deferred_dir_sync: opts.deferred_dir_sync,
            dirty: std::cell::RefCell::new(engine::DirtySet::new()),
        })
    }

    pub fn format(&self) -> &FormatRecord {
        &self.format
    }

    pub fn queue_id(&self) -> &[u8; 16] {
        self.format.queue_id()
    }

    pub fn boot_id(&self) -> &str {
        &self.boot_id
    }

    pub fn root_fd(&self) -> BorrowedFd<'_> {
        self.root_fd.as_fd()
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

    #[allow(dead_code)]
    pub(crate) fn layout(&self) -> layout::Layout<'_> {
        layout::Layout::new(
            self.format.queue_id(),
            self.format.shard_count(),
            self.format.lease_bucket_width_ns(),
            self.format.delayed_bucket_width_ns(),
            self.format.terminal_bucket_width_ns(),
            &self.boot_id,
        )
    }
    /// Enqueue a job with the given payload and metadata.
    pub fn enqueue(&mut self, job: EnqueueInput) -> EnqueueOutcome {
        if self.deferred_dir_sync {
            let mut tmp = self.dirty.replace(engine::DirtySet::new());
            let outcome = self.enqueue_with_dirty(job, Some(&mut tmp));
            let prev = self.dirty.replace(tmp);
            drop(prev);
            return match outcome {
                EnqueueOutcome::Committed(ticket) => EnqueueOutcome::Deferred(ticket),
                outcome => outcome,
            };
        }
        self.enqueue_inner(job)
    }

    fn enqueue_inner(&mut self, job: EnqueueInput) -> EnqueueOutcome {
        self.enqueue_with_dirty(job, None)
    }

    fn enqueue_batched(
        &mut self,
        job: EnqueueInput,
        dirty: &mut engine::DirtySet,
    ) -> EnqueueOutcome {
        self.enqueue_with_dirty(job, Some(dirty))
    }

    fn prepare_enqueue(
        &mut self,
        job: EnqueueInput,
    ) -> Result<PreparedEnqueue, (EnqueueTicket, Error)> {
        if let Err(e) = self.check_not_poisoned() {
            let ticket = EnqueueTicket {
                job_id: [0; 16],
                envelope_digest: [0; 32],
                expected_initial_state: InitialState::Ready,
                expected_relative_path: String::new(),
            };
            return Err((ticket, e));
        }
        let job_id = match fs::random_128bit() {
            Ok(id) => id,
            Err(e) => {
                let ticket = EnqueueTicket {
                    job_id: [0; 16],
                    envelope_digest: [0; 32],
                    expected_initial_state: InitialState::Ready,
                    expected_relative_path: String::new(),
                };
                return Err((ticket, Error::IoFailure(e.to_string())));
            }
        };
        let wall_floor = match self.wall_floor_for_mutation() {
            Ok(floor) => floor,
            Err(error) => {
                let ticket = EnqueueTicket {
                    job_id,
                    envelope_digest: [0; 32],
                    expected_initial_state: InitialState::Ready,
                    expected_relative_path: String::new(),
                };
                return Err((ticket, error));
            }
        };
        let created_at = wall_floor.unix_ns();
        if job.maximum_attempts == 0 {
            let ticket = EnqueueTicket {
                job_id,
                envelope_digest: [0; 32],
                expected_initial_state: InitialState::Ready,
                expected_relative_path: String::new(),
            };
            return Err((
                ticket,
                Error::InvalidInput("maximum_attempts must be >= 1".into()),
            ));
        }
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
                return Err((ticket, Error::InvalidInput(e.to_string())));
            }
        };
        if job.payload.len() as u64 > self.format.max_payload_length().min(MAX_PAYLOAD_LENGTH) {
            let ticket = EnqueueTicket {
                job_id,
                envelope_digest: [0; 32],
                expected_initial_state: InitialState::Ready,
                expected_relative_path: String::new(),
            };
            return Err((ticket, Error::InvalidInput("payload exceeds limit".into())));
        }
        let pdig = payload_digest(&job.payload);
        let mut header = FixedHeader {
            format_minor: FORMAT_MINOR,
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
        let env_dig = match envelope_digest(&header, &ext_bytes) {
            Some(d) => d,
            None => {
                let ticket = EnqueueTicket {
                    job_id,
                    envelope_digest: [0; 32],
                    expected_initial_state: InitialState::Ready,
                    expected_relative_path: String::new(),
                };
                return Err((
                    ticket,
                    Error::InvalidInput("extension length mismatch".into()),
                ));
            }
        };
        header.envelope_digest = env_dig;
        let now_wall = wall_floor.unix_ns();
        let (initial_state, _) = match job.initial_not_before {
            Some(nb) if nb > now_wall => {
                let (eb, _) =
                    match eligibility_bucket_and_ns(nb, self.format.delayed_bucket_width_ns()) {
                        Some(v) => v,
                        None => {
                            let ticket = EnqueueTicket {
                                job_id,
                                envelope_digest: header.envelope_digest,
                                expected_initial_state: InitialState::Ready,
                                expected_relative_path: String::new(),
                            };
                            return Err((
                                ticket,
                                Error::InvalidInput("eligibility overflow".into()),
                            ));
                        }
                    };
                (InitialState::Delayed, eb)
            }
            _ => (InitialState::Ready, 0),
        };
        let common = CommonFields {
            job_id,
            generation: 0,
            attempt: 0,
            maximum_attempts: job.maximum_attempts,
        };
        let (dest_dir_relative, filename, expected_path) = match initial_state {
            InitialState::Ready => {
                let target = self.layout().ready(&common);
                let path = target.relative_path();
                (target.directory(), target.filename, path)
            }
            InitialState::Delayed => {
                let Some(not_before_ns) = job.initial_not_before else {
                    return Err((
                        EnqueueTicket {
                            job_id,
                            envelope_digest: header.envelope_digest,
                            expected_initial_state: initial_state,
                            expected_relative_path: String::new(),
                        },
                        Error::QueueCorrupt("delayed enqueue lost its deadline".into()),
                    ));
                };
                let target = match self.layout().delayed(&common, not_before_ns) {
                    Ok(target) => target,
                    Err(error) => {
                        return Err((
                            EnqueueTicket {
                                job_id,
                                envelope_digest: header.envelope_digest,
                                expected_initial_state: initial_state,
                                expected_relative_path: String::new(),
                            },
                            error,
                        ));
                    }
                };
                let path = target.relative_path();
                (target.directory(), target.filename, path)
            }
        };
        let ticket = EnqueueTicket {
            job_id,
            envelope_digest: header.envelope_digest,
            expected_initial_state: initial_state,
            expected_relative_path: expected_path.clone(),
        };
        let ready_shard_hint = match initial_state {
            InitialState::Ready => Some(compute_shard(
                self.format.queue_id(),
                &job_id,
                self.format.shard_count(),
            )),
            InitialState::Delayed => None,
        };
        Ok(PreparedEnqueue {
            ticket,
            header,
            ext_bytes,
            payload: job.payload,
            dest_dir: dest_dir_relative,
            filename,
            ready_shard_hint,
        })
    }

    fn enqueue_with_dirty(
        &mut self,
        job: EnqueueInput,
        dirty: Option<&mut engine::DirtySet>,
    ) -> EnqueueOutcome {
        let prepared = match self.prepare_enqueue(job) {
            Ok(p) => p,
            Err((ticket, err)) => return EnqueueOutcome::NotCommitted(ticket, err),
        };
        let result = if let Some(d) = dirty {
            self.write_and_publish_with_dirty(
                &prepared.dest_dir,
                &prepared.filename,
                &prepared.header,
                &prepared.ext_bytes,
                &prepared.payload,
                Some(d),
            )
        } else {
            self.write_and_publish_with_dirty(
                &prepared.dest_dir,
                &prepared.filename,
                &prepared.header,
                &prepared.ext_bytes,
                &prepared.payload,
                None,
            )
        };
        match result {
            Ok(()) => {
                if let Some(shard) = prepared.ready_shard_hint {
                    self.ready_shard_hint = Some(shard);
                }
                EnqueueOutcome::Committed(prepared.ticket)
            }
            Err(PublishError::NotCommitted(e)) => EnqueueOutcome::NotCommitted(prepared.ticket, e),
            Err(PublishError::OutcomeUnknown(e)) => {
                self.poison();
                EnqueueOutcome::OutcomeUnknown(prepared.ticket, e)
            }
        }
    }

    fn lease_batched(
        &mut self,
        max_wait_ns: u64,
        lease_duration_ns: u64,
        dirty: &mut engine::DirtySet,
    ) -> LeaseOutcome {
        self.lease_inner_with_dirty(max_wait_ns, lease_duration_ns, Some(dirty))
    }

    fn ack_batched(&mut self, lease: &LeaseInfo, dirty: &mut engine::DirtySet) -> AckOutcome {
        self.ack_inner_with_dirty(lease, Some(dirty))
    }

    fn open_or_cache_dir(&mut self, relative: &str) -> io::Result<std::os::fd::OwnedFd> {
        if let Some((ref cached_path, ref cached_fd)) = self.cached_dest_fd {
            if cached_path == relative {
                // Re-open the cached fd to get a fresh OwnedFd (the caller needs its own).
                // dup is one syscall, cheaper than 2 openat calls.
                return cached_fd
                    .as_fd()
                    .try_clone_to_owned()
                    .map_err(|e| io::Error::other(e.to_string()));
            }
        }
        let fd = open_relative(self.root_fd.as_fd(), relative)?;
        self.cached_dest_fd = Some((
            relative.to_string(),
            fd.as_fd()
                .try_clone_to_owned()
                .map_err(|e| io::Error::other(e.to_string()))?,
        ));
        Ok(fd)
    }

    /// Flush all deferred directory fsync operations. Call this after a batch
    /// of operations when using deferred_dir_sync mode. This fsyncs the exact
    /// dirty directories that were recorded, deduplicated by device and inode.
    pub fn sync(&self) -> io::Result<()> {
        if self.deferred_dir_sync {
            let result = {
                let dirty = self.dirty.borrow();
                if dirty.is_empty() {
                    return Ok(());
                }
                dirty.sync_all()
            };
            if result.is_ok() {
                self.dirty.borrow_mut().clear();
            }
            return result;
        }
        for dir in [
            "ready",
            "leased",
            "delayed",
            "dead",
            "receipts",
            "quarantine",
            "control",
        ] {
            if let Ok(fd) = open_relative(self.root_fd.as_fd(), dir) {
                fs::fsync_dir_fd(fd.as_fd())?;
            }
        }
        fs::fsync_dir_fd(self.root_fd.as_fd())?;
        Ok(())
    }

    /// Strict group-commit batch. Operations are Pending until `commit` fsyncs
    /// every exact dirty directory once. If the barrier fails, post-linearization
    /// operations are OutcomeUnknown.
    pub fn batch(&mut self) -> Batch<'_> {
        Batch::new(self)
    }

    /// Write the job envelope to a temp file and publish via rename.
    fn write_and_publish_with_dirty(
        &mut self,
        dest_dir_relative: &str,
        dest_name: &str,
        header: &FixedHeader,
        ext_bytes: &[u8],
        payload: &[u8],
        mut dirty: Option<&mut engine::DirtySet>,
    ) -> Result<(), PublishError> {
        // Ensure destination directory exists
        if let Some(d) = dirty.as_deref_mut() {
            self.ensure_dir_with_dirty(dest_dir_relative, Some(d))
                .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
        } else {
            self.ensure_dir(dest_dir_relative)
                .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
        }

        let dest_fd = self
            .open_or_cache_dir(dest_dir_relative)
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;

        if self.publication_mode == Some(fs::PublicationMode::NamedFallback) {
            return self.named_fallback_with_dirty(
                dest_dir_relative,
                dest_fd.as_fd(),
                dest_name,
                header,
                ext_bytes,
                payload,
                dirty,
            );
        }

        match fs::open_tmpfile(dest_fd.as_fd()) {
            Ok(tmp_fd) => {
                let header_bytes = header
                    .encode(ext_bytes)
                    .map_err(|e| PublishError::NotCommitted(Error::InvalidInput(e.to_string())))?;
                fs::writev_all(tmp_fd.as_fd(), &[&header_bytes, ext_bytes, payload])
                    .map_err(PublishError::classify_write)?;
                let publish_outcome = if dirty.is_some() {
                    engine::publish_tmpfile_noreplace_deferred_with_mode(
                        tmp_fd.as_fd(),
                        dest_fd.as_fd(),
                        dest_name,
                        self.publication_mode,
                    )
                } else {
                    engine::publish_tmpfile_noreplace_with_mode(
                        tmp_fd.as_fd(),
                        dest_fd.as_fd(),
                        dest_name,
                        self.publication_mode,
                    )
                };
                match publish_outcome {
                    Ok(engine::TmpfilePublishOutcome::Published(mode)) => {
                        self.publication_mode = Some(mode);
                        if let Some(d) = dirty {
                            d.record(dest_fd.as_fd()).map_err(|e| {
                                PublishError::OutcomeUnknown(Error::IoFailure(e.to_string()))
                            })?;
                        }
                        Ok(())
                    }
                    Ok(engine::TmpfilePublishOutcome::Unsupported) => {
                        self.publication_mode = Some(fs::PublicationMode::NamedFallback);
                        self.named_fallback_with_dirty(
                            dest_dir_relative,
                            dest_fd.as_fd(),
                            dest_name,
                            header,
                            ext_bytes,
                            payload,
                            dirty,
                        )
                    }
                    Err(failure) => Err(PublishError::classify_tmpfile(failure)),
                }
            }
            Err(e) => {
                if engine::is_tmpfile_open_unsupported(&e) {
                    self.publication_mode = Some(fs::PublicationMode::NamedFallback);
                    self.named_fallback_with_dirty(
                        dest_dir_relative,
                        dest_fd.as_fd(),
                        dest_name,
                        header,
                        ext_bytes,
                        payload,
                        dirty,
                    )
                } else {
                    Err(PublishError::classify_write(e))
                }
            }
        }
    }

    /// Named temporary file fallback for enqueue.
    #[allow(clippy::too_many_arguments)]
    fn named_fallback_with_dirty(
        &self,
        dest_dir_relative: &str,
        dest_fd: BorrowedFd<'_>,
        dest_name: &str,
        header: &FixedHeader,
        ext_bytes: &[u8],
        payload: &[u8],
        mut dirty: Option<&mut engine::DirtySet>,
    ) -> Result<(), PublishError> {
        let shard_part = dest_dir_relative.rsplit('/').next().unwrap_or("0000");
        let tmp_dir = format!("tmp/{}/{}", self.boot_id, shard_part);
        if let Some(d) = dirty.as_deref_mut() {
            self.ensure_dir_with_dirty(&tmp_dir, Some(d))
                .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
        } else {
            self.ensure_dir(&tmp_dir)
                .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
        }
        let tmp_dir_fd = open_relative(self.root_fd.as_fd(), &tmp_dir)
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
        let boottime = fs::clock_boottime_ns()
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
        let random = fs::random_128bit()
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
        let temp_name = temp_filename(boottime, &random);
        let tmp_file = fs::create_exclusive(tmp_dir_fd.as_fd(), &temp_name, 0o600)
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
        struct TempGuard<'a, 'fd> {
            dir_fd: BorrowedFd<'fd>,
            name: &'a str,
            armed: bool,
        }
        impl Drop for TempGuard<'_, '_> {
            fn drop(&mut self) {
                if self.armed {
                    let _ = fs::unlinkat(self.dir_fd, self.name);
                }
            }
        }
        let mut temp_guard = TempGuard {
            dir_fd: tmp_dir_fd.as_fd(),
            name: &temp_name,
            armed: true,
        };
        let header_bytes = header
            .encode(ext_bytes)
            .map_err(|e| PublishError::NotCommitted(Error::InvalidInput(e.to_string())))?;
        fs::write_all(tmp_file.as_fd(), &header_bytes).map_err(PublishError::classify_write)?;
        fs::write_all(tmp_file.as_fd(), ext_bytes).map_err(PublishError::classify_write)?;
        fs::write_all(tmp_file.as_fd(), payload).map_err(PublishError::classify_write)?;
        fs::fsync(tmp_file.as_fd()).map_err(PublishError::classify_pre_pub_fsync)?;
        let temp_stat = fs::fstat(tmp_file.as_fd()).map_err(PublishError::classify_write)?;
        if let Some(d) = dirty {
            match engine::move_witnessed_noreplace_deferred(
                tmp_dir_fd.as_fd(),
                &temp_name,
                dest_fd,
                dest_name,
                engine::MoveIdentity::new(temp_stat.st_dev, temp_stat.st_ino),
                |_moved| Ok(()),
            ) {
                Ok(_) => {
                    temp_guard.armed = false;
                    d.record(tmp_dir_fd.as_fd()).map_err(|e| {
                        PublishError::OutcomeUnknown(Error::IoFailure(e.to_string()))
                    })?;
                    d.record(dest_fd).map_err(|e| {
                        PublishError::OutcomeUnknown(Error::IoFailure(e.to_string()))
                    })?;
                    Ok(())
                }
                Err(failure) => {
                    if failure.is_outcome_unknown() {
                        temp_guard.armed = false;
                    }
                    let mapped = match failure {
                        engine::MoveFailure::AlreadyExists => {
                            PublishError::NotCommitted(Error::IdentityCollision)
                        }
                        engine::MoveFailure::SourceMissing => PublishError::NotCommitted(
                            Error::IoFailure("temporary publication source missing".into()),
                        ),
                        engine::MoveFailure::NotCommitted { source, .. } => {
                            PublishError::NotCommitted(Error::IoFailure(source.to_string()))
                        }
                        engine::MoveFailure::OutcomeUnknown { source, .. } => {
                            PublishError::OutcomeUnknown(Error::IoFailure(source.to_string()))
                        }
                    };
                    Err(mapped)
                }
            }
        } else {
            match engine::move_witnessed_noreplace_io(
                tmp_dir_fd.as_fd(),
                &temp_name,
                dest_fd,
                dest_name,
                engine::MoveIdentity::new(temp_stat.st_dev, temp_stat.st_ino),
                engine::MoveActor::Producer,
            ) {
                Ok(()) => {
                    temp_guard.armed = false;
                    Ok(())
                }
                Err(failure) => {
                    if failure.is_outcome_unknown() {
                        temp_guard.armed = false;
                    }
                    Err(PublishError::classify_move(failure))
                }
            }
        }
    }

    /// Create a directory path recursively, syncing parents.
    pub(crate) fn ensure_dir(&self, relative: &str) -> io::Result<()> {
        if self.known_dirs.borrow().contains(relative) {
            return Ok(());
        }
        if self.deferred_dir_sync {
            let mut dirty = self.dirty.borrow_mut();
            return self.ensure_dir_with_dirty(relative, Some(&mut dirty));
        }
        self.ensure_dir_with_dirty(relative, None)
    }

    pub(crate) fn ensure_dir_with_dirty(
        &self,
        relative: &str,
        mut dirty: Option<&mut engine::DirtySet>,
    ) -> io::Result<()> {
        if self.known_dirs.borrow().contains(relative) {
            return Ok(());
        }
        let components: Vec<&str> = relative.split('/').filter(|s| !s.is_empty()).collect();
        let mut current = None::<OwnedFd>;

        for comp in components {
            let parent = current
                .as_ref()
                .map_or(self.root_fd.as_fd(), |directory| directory.as_fd());
            let was_created = fs::mkdirat_eexist_ok(parent, comp, 0o700)?;
            let child = fs::open_directory(parent, comp)?;
            if was_created {
                match &mut dirty {
                    Some(set) => set.record(parent)?,
                    None => fs::fsync_dir_fd(parent)?,
                }
            }
            current = Some(child);
        }
        self.known_dirs.borrow_mut().insert(relative.to_string());
        Ok(())
    }

    /// Enqueue a job from a streaming payload source. The payload is written
    /// to the temp file in 64 KiB chunks without buffering the full payload
    /// in memory. The header (including payload digest) is computed from the
    /// streamed bytes using a placeholder-then-pwrite strategy.
    #[allow(clippy::too_many_arguments)]
    pub fn enqueue_streaming(
        &mut self,
        maximum_attempts: u32,
        content_type: String,
        metadata: std::collections::BTreeMap<String, steadq_format::cbor::MetadataValue>,
        producer_id: Option<String>,
        trace_context: Option<Vec<u8>>,
        initial_not_before: Option<u64>,
        mut reader: impl std::io::Read,
    ) -> EnqueueOutcome {
        if let Err(e) = self.check_not_poisoned() {
            return EnqueueOutcome::NotCommitted(
                EnqueueTicket {
                    job_id: [0; 16],
                    envelope_digest: [0; 32],
                    expected_initial_state: InitialState::Ready,
                    expected_relative_path: String::new(),
                },
                e,
            );
        }

        if maximum_attempts == 0 {
            return EnqueueOutcome::NotCommitted(
                EnqueueTicket {
                    job_id: [0; 16],
                    envelope_digest: [0; 32],
                    expected_initial_state: InitialState::Ready,
                    expected_relative_path: String::new(),
                },
                Error::InvalidInput("maximum_attempts must be >= 1".into()),
            );
        }

        let wall_floor = match self.wall_floor_for_mutation() {
            Ok(floor) => floor,
            Err(error) => {
                return EnqueueOutcome::NotCommitted(
                    EnqueueTicket {
                        job_id: [0; 16],
                        envelope_digest: [0; 32],
                        expected_initial_state: InitialState::Ready,
                        expected_relative_path: String::new(),
                    },
                    error,
                )
            }
        };
        let created_at = wall_floor.unix_ns();
        let job_id = fs::random_128bit().unwrap_or([0; 16]);

        let ext = ExtensionHeader {
            initial_not_before_unix_ns: initial_not_before,
            content_type,
            metadata,
            producer_id,
            trace_context,
        };
        let ext_bytes = match ext.encode() {
            Ok(b) => b,
            Err(e) => {
                return EnqueueOutcome::NotCommitted(
                    EnqueueTicket {
                        job_id,
                        envelope_digest: [0; 32],
                        expected_initial_state: InitialState::Ready,
                        expected_relative_path: String::new(),
                    },
                    Error::InvalidInput(e.to_string()),
                )
            }
        };

        // Write placeholder header, extension, then stream payload while hashing.
        // After streaming, pwrite the real header at offset 0.
        let now_wall = wall_floor.unix_ns();
        let (expected_initial_state, _) = match initial_not_before {
            Some(nb) if nb > now_wall => (InitialState::Delayed, 0u64),
            _ => (InitialState::Ready, 0),
        };

        let common = CommonFields {
            job_id,
            generation: 0,
            attempt: 0,
            maximum_attempts,
        };
        let (dest_dir_relative, filename, expected_path) = match expected_initial_state {
            InitialState::Ready => {
                let target = self.layout().ready(&common);
                {
                    let d = target.directory();
                    let p = target.relative_path();
                    (d, target.filename, p)
                }
            }
            InitialState::Delayed => {
                let Some(not_before_ns) = initial_not_before else {
                    return EnqueueOutcome::NotCommitted(
                        EnqueueTicket {
                            job_id,
                            envelope_digest: [0; 32],
                            expected_initial_state: InitialState::Ready,
                            expected_relative_path: String::new(),
                        },
                        Error::QueueCorrupt("delayed enqueue lost its deadline".into()),
                    );
                };
                let target = match self.layout().delayed(&common, not_before_ns) {
                    Ok(t) => t,
                    Err(e) => {
                        return EnqueueOutcome::NotCommitted(
                            EnqueueTicket {
                                job_id,
                                envelope_digest: [0; 32],
                                expected_initial_state: InitialState::Ready,
                                expected_relative_path: String::new(),
                            },
                            e,
                        )
                    }
                };
                {
                    let d = target.directory();
                    let p = target.relative_path();
                    (d, target.filename, p)
                }
            }
        };

        let result = self.stream_and_publish(
            &dest_dir_relative,
            &filename,
            job_id,
            maximum_attempts,
            created_at,
            &ext_bytes,
            &mut reader,
        );

        let env_dig = match &result {
            Ok(d) => *d,
            Err(PublishError::NotCommitted(e)) => {
                return EnqueueOutcome::NotCommitted(
                    EnqueueTicket {
                        job_id,
                        envelope_digest: [0; 32],
                        expected_initial_state,
                        expected_relative_path: expected_path,
                    },
                    e.clone(),
                )
            }
            Err(PublishError::OutcomeUnknown(e)) => {
                self.poison();
                return EnqueueOutcome::OutcomeUnknown(
                    EnqueueTicket {
                        job_id,
                        envelope_digest: [0; 32],
                        expected_initial_state,
                        expected_relative_path: expected_path,
                    },
                    e.clone(),
                );
            }
        };

        let ticket = EnqueueTicket {
            job_id,
            envelope_digest: env_dig,
            expected_initial_state,
            expected_relative_path: expected_path,
        };
        if self.deferred_dir_sync {
            EnqueueOutcome::Deferred(ticket)
        } else {
            EnqueueOutcome::Committed(ticket)
        }
    }

    /// Stream payload to a temp file while computing the digest, then publish.
    /// Returns the envelope digest on success.
    #[allow(clippy::too_many_arguments)]
    fn stream_and_publish(
        &mut self,
        dest_dir_relative: &str,
        dest_name: &str,
        job_id: [u8; 16],
        maximum_attempts: u32,
        created_at: u64,
        ext_bytes: &[u8],
        reader: &mut dyn std::io::Read,
    ) -> Result<[u8; 32], PublishError> {
        self.ensure_dir(dest_dir_relative)
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
        let dest_fd = open_relative(self.root_fd.as_fd(), dest_dir_relative)
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;

        if self.publication_mode == Some(fs::PublicationMode::NamedFallback) {
            return self.named_fallback_streaming_init(
                dest_dir_relative,
                dest_fd.as_fd(),
                dest_name,
                job_id,
                maximum_attempts,
                created_at,
                ext_bytes,
                reader,
            );
        }

        match fs::open_tmpfile(dest_fd.as_fd()) {
            Ok(tmp_fd) => {
                let (payload_len, payload_digest) =
                    Self::stream_payload_to_fd(tmp_fd.as_fd(), ext_bytes, reader)?;

                // Validate payload size.
                if payload_len > self.format.max_payload_length().min(MAX_PAYLOAD_LENGTH) {
                    return Err(PublishError::NotCommitted(Error::InvalidInput(
                        "payload exceeds limit".into(),
                    )));
                }

                // Construct and pwrite the real header.
                let mut header = FixedHeader {
                    format_minor: FORMAT_MINOR,
                    extension_header_length: ext_bytes.len() as u32,
                    payload_length: payload_len,
                    flags: 0,
                    digest_algorithm: DIGEST_ALGORITHM_SHA256,
                    job_id,
                    maximum_attempts,
                    created_at_unix_ns: created_at,
                    payload_digest,
                    envelope_digest: [0; 32],
                };
                let env_dig = envelope_digest(&header, ext_bytes).ok_or_else(|| {
                    PublishError::NotCommitted(Error::InvalidInput(
                        "extension length mismatch".into(),
                    ))
                })?;
                header.envelope_digest = env_dig;
                let header_bytes = header
                    .encode(ext_bytes)
                    .map_err(|e| PublishError::NotCommitted(Error::InvalidInput(e.to_string())))?;
                fs::pwrite_all(tmp_fd.as_fd(), &header_bytes, 0)
                    .map_err(PublishError::classify_write)?;

                match engine::publish_tmpfile_noreplace_with_mode(
                    tmp_fd.as_fd(),
                    dest_fd.as_fd(),
                    dest_name,
                    self.publication_mode,
                ) {
                    Ok(engine::TmpfilePublishOutcome::Published(mode)) => {
                        self.publication_mode = Some(mode);
                    }
                    Ok(engine::TmpfilePublishOutcome::Unsupported) => {
                        self.publication_mode = Some(fs::PublicationMode::NamedFallback);
                        // O_TMPFILE not supported: need named fallback.
                        // Read back from the tmpfile and use the standard path.
                        return self.named_fallback_streaming(
                            dest_dir_relative,
                            dest_fd.as_fd(),
                            dest_name,
                            &header,
                            ext_bytes,
                            tmp_fd.as_fd(),
                            payload_len,
                        );
                    }
                    Err(failure) => return Err(PublishError::classify_tmpfile(failure)),
                }
                fs::fsync_dir_fd(dest_fd.as_fd())
                    .map_err(|e| PublishError::OutcomeUnknown(Error::IoFailure(e.to_string())))?;
                Ok(env_dig)
            }
            Err(e) => {
                if engine::is_tmpfile_open_unsupported(&e) {
                    self.publication_mode = Some(fs::PublicationMode::NamedFallback);
                    // O_TMPFILE not supported: stream to a named temp file instead.
                    self.named_fallback_streaming_init(
                        dest_dir_relative,
                        dest_fd.as_fd(),
                        dest_name,
                        job_id,
                        maximum_attempts,
                        created_at,
                        ext_bytes,
                        reader,
                    )
                } else {
                    Err(PublishError::classify_write(e))
                }
            }
        }
    }

    /// Stream payload to a temp fd: write placeholder header, extension, then
    /// payload chunks while hashing. Returns (payload_length, payload_digest).
    fn stream_payload_to_fd(
        fd: BorrowedFd<'_>,
        ext_bytes: &[u8],
        reader: &mut dyn std::io::Read,
    ) -> Result<(u64, [u8; 32]), PublishError> {
        // Placeholder header: 128 zero bytes.
        let placeholder = [0u8; 128];
        fs::write_all(fd, &placeholder).map_err(PublishError::classify_write)?;
        fs::write_all(fd, ext_bytes).map_err(PublishError::classify_write)?;

        let mut hasher = Sha256::new();
        let mut total: u64 = 0;
        let mut buf = vec![0u8; 65536];
        loop {
            let n = reader
                .read(&mut buf)
                .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            fs::write_all(fd, &buf[..n]).map_err(PublishError::classify_write)?;
            total = total.checked_add(n as u64).ok_or_else(|| {
                PublishError::NotCommitted(Error::InvalidInput("payload length overflow".into()))
            })?;
        }
        Ok((total, hasher.finalize().into()))
    }

    /// Named fallback for streaming when O_TMPFILE is unsupported.
    /// Reads back from the O_TMPFILE fd and writes to a named temp file.
    #[allow(clippy::too_many_arguments)]
    fn named_fallback_streaming(
        &mut self,
        dest_dir_relative: &str,
        dest_fd: BorrowedFd<'_>,
        dest_name: &str,
        header: &FixedHeader,
        ext_bytes: &[u8],
        tmpfile_fd: BorrowedFd<'_>,
        payload_len: u64,
    ) -> Result<[u8; 32], PublishError> {
        // The tmpfile already has the full content (placeholder header + ext + payload).
        // We need a named temp file that we can publish via rename.
        let tmp_dir = format!("tmp/{}", self.boot_id);
        let shard_part = dest_dir_relative.rsplit('/').next().unwrap_or("0000");
        let tmp_shard_dir = format!("{tmp_dir}/{shard_part}");
        self.ensure_dir(&tmp_shard_dir)
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
        let tmp_dir_fd = open_relative(self.root_fd.as_fd(), &tmp_shard_dir)
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
        let boottime = fs::clock_boottime_ns()
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
        let random = fs::random_128bit()
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
        let temp_name = temp_filename(boottime, &random);

        let tmp_file = fs::create_exclusive(tmp_dir_fd.as_fd(), &temp_name, 0o600)
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;

        // Write the real header to the named temp.
        let header_bytes = header
            .encode(ext_bytes)
            .map_err(|e| PublishError::NotCommitted(Error::InvalidInput(e.to_string())))?;
        fs::write_all(tmp_file.as_fd(), &header_bytes).map_err(PublishError::classify_write)?;
        fs::write_all(tmp_file.as_fd(), ext_bytes).map_err(PublishError::classify_write)?;

        // Copy payload from tmpfile_fd (offset 128 + ext_len) to named temp.
        let data_offset = (128 + ext_bytes.len()) as u64;
        let mut copied: u64 = 0;
        let mut buf = vec![0u8; 65536];
        while copied < payload_len {
            let to_read = (buf.len() as u64).min(payload_len - copied) as usize;
            let n = fs::pread(tmpfile_fd, &mut buf[..to_read], data_offset + copied)
                .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
            if n == 0 {
                break;
            }
            fs::write_all(tmp_file.as_fd(), &buf[..n]).map_err(PublishError::classify_write)?;
            copied += n as u64;
        }

        fs::fsync(tmp_file.as_fd()).map_err(PublishError::classify_pre_pub_fsync)?;

        let temp_stat = fs::fstat(tmp_file.as_fd()).map_err(PublishError::classify_write)?;
        match engine::move_witnessed_noreplace_io(
            tmp_dir_fd.as_fd(),
            &temp_name,
            dest_fd,
            dest_name,
            engine::MoveIdentity::new(temp_stat.st_dev, temp_stat.st_ino),
            engine::MoveActor::Producer,
        ) {
            Ok(()) => {}
            Err(failure) => {
                if failure.is_outcome_unknown() {
                    let _ = fs::unlinkat(tmp_dir_fd.as_fd(), &temp_name);
                }
                return Err(PublishError::classify_move(failure));
            }
        }
        Ok(header.envelope_digest)
    }

    /// Named fallback for streaming when O_TMPFILE open fails entirely.
    #[allow(clippy::too_many_arguments)]
    fn named_fallback_streaming_init(
        &mut self,
        dest_dir_relative: &str,
        dest_fd: BorrowedFd<'_>,
        dest_name: &str,
        job_id: [u8; 16],
        maximum_attempts: u32,
        created_at: u64,
        ext_bytes: &[u8],
        reader: &mut dyn std::io::Read,
    ) -> Result<[u8; 32], PublishError> {
        let tmp_dir = format!("tmp/{}", self.boot_id);
        let shard_part = dest_dir_relative.rsplit('/').next().unwrap_or("0000");
        let tmp_shard_dir = format!("{tmp_dir}/{shard_part}");
        self.ensure_dir(&tmp_shard_dir)
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
        let tmp_dir_fd = open_relative(self.root_fd.as_fd(), &tmp_shard_dir)
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
        let boottime = fs::clock_boottime_ns()
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
        let random = fs::random_128bit()
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;
        let temp_name = temp_filename(boottime, &random);

        let tmp_file = fs::create_exclusive(tmp_dir_fd.as_fd(), &temp_name, 0o600)
            .map_err(|e| PublishError::NotCommitted(Error::IoFailure(e.to_string())))?;

        // Stream payload to named temp while hashing.
        // Write placeholder header first, then extension, then payload.
        // After streaming, pwrite real header.
        let (payload_len, payload_digest) =
            Self::stream_payload_to_fd(tmp_file.as_fd(), ext_bytes, reader)?;

        if payload_len > self.format.max_payload_length().min(MAX_PAYLOAD_LENGTH) {
            let _ = fs::unlinkat(tmp_dir_fd.as_fd(), &temp_name);
            return Err(PublishError::NotCommitted(Error::InvalidInput(
                "payload exceeds limit".into(),
            )));
        }

        let mut header = FixedHeader {
            format_minor: FORMAT_MINOR,
            extension_header_length: ext_bytes.len() as u32,
            payload_length: payload_len,
            flags: 0,
            digest_algorithm: DIGEST_ALGORITHM_SHA256,
            job_id,
            maximum_attempts,
            created_at_unix_ns: created_at,
            payload_digest,
            envelope_digest: [0; 32],
        };
        let env_dig = envelope_digest(&header, ext_bytes).ok_or_else(|| {
            PublishError::NotCommitted(Error::InvalidInput("extension length mismatch".into()))
        })?;
        header.envelope_digest = env_dig;
        let header_bytes = header
            .encode(ext_bytes)
            .map_err(|e| PublishError::NotCommitted(Error::InvalidInput(e.to_string())))?;
        fs::pwrite_all(tmp_file.as_fd(), &header_bytes, 0).map_err(PublishError::classify_write)?;
        fs::fsync(tmp_file.as_fd()).map_err(PublishError::classify_pre_pub_fsync)?;

        let temp_stat = fs::fstat(tmp_file.as_fd()).map_err(PublishError::classify_write)?;
        match engine::move_witnessed_noreplace_io(
            tmp_dir_fd.as_fd(),
            &temp_name,
            dest_fd,
            dest_name,
            engine::MoveIdentity::new(temp_stat.st_dev, temp_stat.st_ino),
            engine::MoveActor::Producer,
        ) {
            Ok(()) => {}
            Err(failure) => {
                if failure.is_outcome_unknown() {
                    let _ = fs::unlinkat(tmp_dir_fd.as_fd(), &temp_name);
                }
                return Err(PublishError::classify_move(failure));
            }
        }
        Ok(env_dig)
    }

    /// Claim a ready job, returning a lease. Empty scans and transient watermark
    /// lock contention retry with bounded exponential backoff until `max_wait_ns`.
    pub fn lease(&mut self, max_wait_ns: u64, lease_duration_ns: u64) -> LeaseOutcome {
        self.lease_inner_with_dirty(max_wait_ns, lease_duration_ns, None)
    }

    fn lease_inner_with_dirty(
        &mut self,
        max_wait_ns: u64,
        lease_duration_ns: u64,
        mut dirty: Option<&mut engine::DirtySet>,
    ) -> LeaseOutcome {
        let started = std::time::Instant::now();
        let wait = std::time::Duration::from_nanos(max_wait_ns);
        let mut backoff = std::time::Duration::from_micros(50);
        let max_backoff = std::time::Duration::from_millis(10);

        loop {
            let outcome = self.lease_once_with_dirty(lease_duration_ns, dirty.as_deref_mut());
            let retryable = matches!(
                outcome,
                LeaseOutcome::Empty | LeaseOutcome::NotCommitted(Error::MaintenanceBusy)
            );
            if max_wait_ns == 0 || !retryable {
                return outcome;
            }

            let elapsed = started.elapsed();
            if elapsed >= wait {
                return outcome;
            }
            std::thread::sleep(backoff.min(wait.saturating_sub(elapsed)));
            backoff = backoff.saturating_mul(2).min(max_backoff);
        }
    }

    fn lease_once_with_dirty(
        &mut self,
        lease_duration_ns: u64,
        mut _dirty: Option<&mut engine::DirtySet>,
    ) -> LeaseOutcome {
        if let Err(e) = self.check_not_poisoned() {
            return LeaseOutcome::NotCommitted(e);
        }

        // Validate lease duration: 1s to 7d
        if !lease_duration_is_valid(lease_duration_ns) {
            return LeaseOutcome::NotCommitted(Error::InvalidInput(
                "lease duration must be 1s to 7d".into(),
            ));
        }

        // C-16: Clocks are re-captured inside the scan loop before each claim
        let _boottime_now = fs::clock_boottime_ns().ok();

        // C-19: Track scan completeness to distinguish Empty from I/O error
        let mut scan_had_error = false;
        let mut wall_floor = None;

        // C-15: Use and advance the per-worker scan round
        let scan_round = self.scan_round;
        self.scan_round = self.scan_round.wrapping_add(1);
        let (scheduled_start, stride) = steadq_names::shard_scan_params(
            self.format.queue_id(),
            &self.boot_id_bytes,
            &self.worker_nonce,
            scan_round,
            self.format.shard_count(),
        );
        let start = self.ready_shard_hint.take().unwrap_or(scheduled_start);

        for i in 0..self.format.shard_count() {
            let shard = steadq_names::shard_at(start, stride, i, self.format.shard_count());
            let shard_str = shard_hex(shard);

            // Open the ready shard directory
            let ready_dir = self.layout().ready_shard_dir(shard);
            let shard_fd = match open_relative(self.root_fd.as_fd(), &ready_dir) {
                Ok(fd) => fd,
                Err(_) => {
                    scan_had_error = true;
                    continue;
                }
            };

            // List entries
            let entries = match fs::read_dir_entries(shard_fd.as_fd()) {
                Ok(e) => e,
                Err(_) => {
                    scan_had_error = true;
                    continue;
                }
            };

            for entry in &entries {
                let Some(entry) = entry.as_ascii_str() else {
                    scan_had_error = true;
                    continue;
                };
                if !entry.ends_with(".sqj") {
                    continue;
                }

                // Parse and verify the ready filename
                let parsed = match steadq_names::parse_ready(entry) {
                    Ok(p) => p,
                    Err(_) => continue,
                };

                if !parsed.authenticate_tag(self.format.queue_id(), &shard_str) {
                    continue;
                }

                // Verify shard matches job_id
                let computed_shard = compute_shard(
                    self.format.queue_id(),
                    &parsed.common.job_id,
                    self.format.shard_count(),
                );
                if computed_shard != shard {
                    continue;
                }

                // Check attempt limit
                if parsed.common.attempt >= parsed.common.maximum_attempts {
                    let operation_wall_floor = match wall_floor {
                        Some(floor) => floor,
                        None => match self.wall_floor_for_mutation() {
                            Ok(floor) => {
                                wall_floor = Some(floor);
                                floor
                            }
                            Err(error) => return LeaseOutcome::NotCommitted(error),
                        },
                    };
                    // Move to dead
                    match self.move_to_dead(
                        &ready_dir,
                        entry,
                        &parsed.common,
                        DeadReason::AttemptsExhausted,
                        operation_wall_floor,
                    ) {
                        Ok(()) => continue,
                        Err(_) => {
                            scan_had_error = true;
                            self.poison();
                            continue;
                        }
                    }
                }

                // C-16: Re-capture clocks immediately before the claim
                let boottime_claim = match fs::clock_boottime_ns() {
                    Ok(t) => t,
                    Err(e) => return LeaseOutcome::NotCommitted(Error::IoFailure(e.to_string())),
                };
                let wall_claim = match wall_floor {
                    Some(floor) => floor.unix_ns(),
                    None => match self.wall_floor_for_mutation() {
                        Ok(floor) => {
                            wall_floor = Some(floor);
                            floor.unix_ns()
                        }
                        Err(error) => return LeaseOutcome::NotCommitted(error),
                    },
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

                let lease_target = match self.layout().leased_for_boot(
                    &leased_common,
                    &self.boot_id,
                    boottime_deadline,
                    wall_deadline,
                    &lease_token,
                ) {
                    Ok(target) => target,
                    Err(_) => {
                        scan_had_error = true;
                        continue;
                    }
                };
                let leased_dir = lease_target.directory();
                if let Err(e) = self.ensure_dir_with_dirty(&leased_dir, _dirty.as_deref_mut()) {
                    // R4-B04: Propagate real errors, don't mask as scan miss
                    scan_had_error = true;
                    let _ = e;
                    continue;
                }

                let leased_dir_fd = match open_relative(self.root_fd.as_fd(), &leased_dir) {
                    Ok(fd) => fd,
                    Err(error) => match classify_lease_directory_open_failure(&error) {
                        LeaseDirectoryOpenFailure::Gone => continue,
                        LeaseDirectoryOpenFailure::InvalidDirectory
                        | LeaseDirectoryOpenFailure::Io => {
                            scan_had_error = true;
                            continue;
                        }
                    },
                };

                let claim_source = match Self::open_claim_source(
                    shard_fd.as_fd(),
                    entry,
                    &parsed.common.job_id,
                    parsed.common.maximum_attempts,
                ) {
                    Ok(Some(source)) => source,
                    Ok(None) => continue,
                    Err(Error::IoFailure(_)) => {
                        scan_had_error = true;
                        continue;
                    }
                    Err(error) => return LeaseOutcome::NotCommitted(error),
                };
                let mut claim_ticket = match self.claim_transition_ticket(
                    &parsed.common,
                    lease_token,
                    claim_source.evidence.clone(),
                    boottime_deadline,
                    wall_deadline,
                ) {
                    Ok(ticket) => ticket,
                    Err(error) => return LeaseOutcome::NotCommitted(error),
                };

                match observe_witness_path(
                    shard_fd.as_fd(),
                    entry,
                    claim_source.device,
                    claim_source.inode,
                ) {
                    Ok(WitnessPathObservation::Match) => {}
                    Ok(WitnessPathObservation::Gone) => continue,
                    Ok(WitnessPathObservation::Mismatch) => {
                        return LeaseOutcome::NotCommitted(Error::QueueCorrupt(
                            "ready source identity changed before claim".into(),
                        ));
                    }
                    Err(_) => {
                        scan_had_error = true;
                        continue;
                    }
                }

                let move_result = if _dirty.is_some() {
                    let result = engine::move_witnessed_noreplace_deferred(
                        shard_fd.as_fd(),
                        entry,
                        leased_dir_fd.as_fd(),
                        &lease_target.filename,
                        engine::MoveIdentity::new(claim_source.device, claim_source.inode),
                        |_| {
                            let refreshed_evidence = Self::read_claim_ticket_evidence(
                                claim_source.file_fd.as_fd(),
                                &parsed.common.job_id,
                                parsed.common.maximum_attempts,
                            )
                            .map_err(|error| std::io::Error::other(error.to_string()))?;
                            claim_ticket = self
                                .claim_transition_ticket(
                                    &parsed.common,
                                    lease_token,
                                    refreshed_evidence,
                                    boottime_deadline,
                                    wall_deadline,
                                )
                                .map_err(|error| std::io::Error::other(error.to_string()))?;
                            Ok(())
                        },
                    );
                    // Record both source and destination directories for durability.
                    if let Some(d) = _dirty.as_deref_mut() {
                        let _ = d.record(shard_fd.as_fd());
                        let _ = d.record(leased_dir_fd.as_fd());
                    }
                    result
                } else {
                    engine::move_witnessed_noreplace_with(
                        shard_fd.as_fd(),
                        entry,
                        leased_dir_fd.as_fd(),
                        &lease_target.filename,
                        engine::MoveIdentity::new(claim_source.device, claim_source.inode),
                        engine::MoveActor::Consumer,
                        |_| {
                            let refreshed_evidence = Self::read_claim_ticket_evidence(
                                claim_source.file_fd.as_fd(),
                                &parsed.common.job_id,
                                parsed.common.maximum_attempts,
                            )
                            .map_err(|error| std::io::Error::other(error.to_string()))?;
                            claim_ticket = self
                                .claim_transition_ticket(
                                    &parsed.common,
                                    lease_token,
                                    refreshed_evidence,
                                    boottime_deadline,
                                    wall_deadline,
                                )
                                .map_err(|error| std::io::Error::other(error.to_string()))?;
                            Ok(())
                        },
                    )
                };
                match move_result {
                    Ok((leased_object, ())) => {
                        // B-03: Post-rename validation failures must NOT continue as Empty.
                        // The claim is committed; failures here are corruption or indeterminate.
                        let leased_file = claim_source.file_fd;

                        let mut header_buf = [0u8; 128];
                        if fs::pread_exact(leased_file.as_fd(), &mut header_buf, 0).is_err() {
                            self.poison();
                            return LeaseOutcome::OutcomeUnknown(
                                claim_ticket.with_phase(TransitionPhase::SourceDirectoryDurable),
                            );
                        }

                        let header = match FixedHeader::decode(&header_buf) {
                            Ok(h) => h,
                            Err(_) => {
                                self.poison();
                                return LeaseOutcome::OutcomeUnknown(
                                    claim_ticket
                                        .with_phase(TransitionPhase::SourceDirectoryDurable),
                                );
                            }
                        };

                        // Verify job_id matches
                        if header.job_id != parsed.common.job_id {
                            self.poison();
                            return LeaseOutcome::OutcomeUnknown(
                                claim_ticket.with_phase(TransitionPhase::SourceDirectoryDurable),
                            );
                        }

                        // R4-B05: Full structural validation of the claimed object before return.
                        // Verify envelope digest, exact size, and payload limit.
                        {
                            let ext_len_h = header.extension_header_length as usize;
                            if verified::is_extension_too_large(ext_len_h) {
                                self.poison();
                                return LeaseOutcome::OutcomeUnknown(
                                    claim_ticket
                                        .with_phase(TransitionPhase::SourceDirectoryDurable),
                                );
                            }
                            let mut ext_buf_claim = vec![0u8; ext_len_h];
                            if fs::pread_exact(leased_file.as_fd(), &mut ext_buf_claim, 128)
                                .is_err()
                            {
                                self.poison();
                                return LeaseOutcome::OutcomeUnknown(
                                    claim_ticket
                                        .with_phase(TransitionPhase::SourceDirectoryDurable),
                                );
                            }
                            if !steadq_format::verify_envelope_digest(&header, &ext_buf_claim) {
                                self.poison();
                                return LeaseOutcome::OutcomeUnknown(
                                    claim_ticket
                                        .with_phase(TransitionPhase::SourceDirectoryDurable),
                                );
                            }
                            // Verify exact file size
                            let expected_claim_size =
                                (128 + ext_len_h + header.payload_length as usize) as u64;
                            if leased_object.size() != expected_claim_size {
                                self.poison();
                                return LeaseOutcome::OutcomeUnknown(
                                    claim_ticket
                                        .with_phase(TransitionPhase::SourceDirectoryDurable),
                                );
                            }
                            // Verify payload limit
                            if !payload_length_is_valid(
                                header.payload_length,
                                self.format.max_payload_length(),
                            ) {
                                self.poison();
                                return LeaseOutcome::OutcomeUnknown(
                                    claim_ticket
                                        .with_phase(TransitionPhase::SourceDirectoryDurable),
                                );
                            }
                            // Verify header max_attempts matches filename
                            if header.maximum_attempts != parsed.common.maximum_attempts {
                                self.poison();
                                return LeaseOutcome::OutcomeUnknown(
                                    claim_ticket
                                        .with_phase(TransitionPhase::SourceDirectoryDurable),
                                );
                            }
                        }

                        // B2: Extension read/decode failure after claim is a post-linearization
                        // corruption. Do not return an ordinary lease with empty content_type.
                        let content_type = if verified::is_extension_present(
                            header.extension_header_length as usize,
                        ) {
                            let mut ext_buf = vec![0u8; header.extension_header_length as usize];
                            match fs::pread_exact(leased_file.as_fd(), &mut ext_buf, 128) {
                                Ok(()) => {
                                    match steadq_format::cbor::ExtensionHeader::decode(&ext_buf) {
                                        Ok(e) => e.content_type,
                                        Err(_) => {
                                            self.poison();
                                            return LeaseOutcome::OutcomeUnknown(
                                                claim_ticket.with_phase(
                                                    TransitionPhase::SourceDirectoryDurable,
                                                ),
                                            );
                                        }
                                    }
                                }
                                Err(_) => {
                                    self.poison();
                                    return LeaseOutcome::OutcomeUnknown(
                                        claim_ticket
                                            .with_phase(TransitionPhase::SourceDirectoryDurable),
                                    );
                                }
                            }
                        } else {
                            String::new()
                        };

                        // P0-01: Verify payload digest on held fd before delivery.
                        // Deterministic PayloadCorrupt is quarantined, not delivered.
                        // Indeterminate I/O poisons and yields OutcomeUnknown.
                        if let Err(e) = self.verify_payload_on_fd(leased_file.as_fd()) {
                            match e {
                                Error::PayloadCorrupt => {
                                    if let Err(failure) = self.quarantine_corrupt_lease(
                                        leased_dir_fd.as_fd(),
                                        &lease_target.filename,
                                        leased_file.as_fd(),
                                    ) {
                                        if failure.is_outcome_unknown() {
                                            self.poison();
                                            return LeaseOutcome::OutcomeUnknown(
                                                claim_ticket.with_phase(
                                                    ticket_phase_for_move_outcome_unknown(
                                                        failure.phase().unwrap(),
                                                    ),
                                                ),
                                            );
                                        }
                                    }
                                    return LeaseOutcome::NotCommitted(Error::PayloadCorrupt);
                                }
                                _ => {
                                    self.poison();
                                    return LeaseOutcome::OutcomeUnknown(
                                        claim_ticket
                                            .with_phase(TransitionPhase::SourceDirectoryDurable),
                                    );
                                }
                            }
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
                            content_type,
                            payload_length: header.payload_length,
                            payload_digest: header.payload_digest,
                            expected_dev: leased_object.device(),
                            expected_inode: leased_object.inode(),
                            exact_source_path: format!("{leased_dir}/{}", lease_target.filename),
                        };

                        return LeaseOutcome::Leased(lease_info);
                    }
                    Err(engine::MoveFailure::SourceMissing) => continue,
                    Err(engine::MoveFailure::OutcomeUnknown { phase, .. }) => {
                        self.poison();
                        return LeaseOutcome::OutcomeUnknown(
                            claim_ticket.with_phase(ticket_phase_for_move_outcome_unknown(phase)),
                        );
                    }
                    Err(
                        engine::MoveFailure::AlreadyExists
                        | engine::MoveFailure::NotCommitted { .. },
                    ) => {
                        scan_had_error = true;
                        continue;
                    }
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

    /// Acknowledge a lease: strictly verify its payload, then move it to a
    /// terminal receipt.
    ///
    /// The payload is re-hashed at acknowledgment time to close the TOCTOU
    /// window between lease delivery and terminal publication. SteadQ/1 has no
    /// public unverified acknowledgment path.
    pub fn ack(&mut self, lease: &LeaseInfo) -> AckOutcome {
        self.ack_inner_with_dirty(lease, None)
    }

    fn ack_inner_with_dirty(
        &mut self,
        lease: &LeaseInfo,
        mut dirty: Option<&mut engine::DirtySet>,
    ) -> AckOutcome {
        if let Err(e) = self.check_not_poisoned() {
            return AckOutcome::NotCommitted(e);
        }

        // C-25/B-05: Use effective wall floor for terminal transitions
        let wall_floor = match self.wall_floor_for_mutation() {
            Ok(floor) => floor,
            Err(e) => return AckOutcome::NotCommitted(e),
        };
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

        let terminal_bucket =
            match bucket_number(wall_floor.unix_ns(), self.format.terminal_bucket_width_ns()) {
                Some(bucket) => bucket,
                None => return AckOutcome::NotCommitted(Error::StateExhausted),
            };
        let target =
            self.layout()
                .receipt_in_bucket(&receipt_common, &lease.token, terminal_bucket);
        let receipt_dir = target.directory();
        let receipt_name = target.filename;
        let transition_ticket = match self.transition_ticket_for_lease(
            lease,
            TransitionOperation::Acknowledge,
            TicketDestination::Receipt { terminal_bucket },
        ) {
            Ok(ticket) => ticket,
            Err(error) => return AckOutcome::NotCommitted(error),
        };
        if let Err(e) = self.ensure_dir_with_dirty(&receipt_dir, dirty.as_deref_mut()) {
            return AckOutcome::NotCommitted(Error::IoFailure(e.to_string()));
        }

        let receipt_dir_fd = match open_relative(self.root_fd.as_fd(), &receipt_dir) {
            Ok(fd) => fd,
            Err(e) => return AckOutcome::NotCommitted(Error::IoFailure(e.to_string())),
        };

        // B-04: Validate the current lease source before acknowledging
        let source = match self.open_and_validate_current_lease(lease) {
            Ok(Some(source)) => source,
            Ok(None) => {
                // R2-H01: Source is gone. Before returning LeaseLost,
                // check if this was a duplicate ack by probing receipts.
                if self.check_duplicate_ack_bounded(lease, wall_floor) {
                    return AckOutcome::AlreadyAcked;
                }
                return AckOutcome::LeaseLost;
            }
            Err(Error::QueueCorrupt(e)) => {
                self.poison();
                return AckOutcome::NotCommitted(Error::QueueCorrupt(e));
            }
            Err(e) => return AckOutcome::NotCommitted(e),
        };

        if let Err(e) = self.verify_payload_on_fd(source.file_fd.as_fd()) {
            self.poison();
            return AckOutcome::NotCommitted(e);
        }

        match Self::execute_leased_move_with_dirty(
            &source,
            receipt_dir_fd.as_fd(),
            &receipt_name,
            dirty,
        ) {
            LeasedMoveOutcome::Committed => AckOutcome::Acked,
            LeasedMoveOutcome::OutcomeUnknown(phase) => {
                self.poison();
                AckOutcome::OutcomeUnknown(transition_ticket.with_phase(phase))
            }
            LeasedMoveOutcome::Collision => {
                // P0-04: Authenticate the existing receipt instead of blindly
                // reporting AlreadyAcked. A conflicting object at the
                // deterministic path must not be treated as idempotent success.
                if self.receipt_is_authentic(lease, &receipt_dir, &receipt_name) {
                    // Source exists and receipt is authentic: both observed.
                    // The lease is still live. Report as corruption rather
                    // than collapsing into idempotent success.
                    self.poison();
                    AckOutcome::NotCommitted(Error::QueueCorrupt(
                        "source lease and receipt both exist".into(),
                    ))
                } else {
                    self.poison();
                    AckOutcome::NotCommitted(Error::QueueCorrupt(
                        "conflicting object at receipt path".into(),
                    ))
                }
            }
            LeasedMoveOutcome::SourceGone => {
                // C-22: On source absence, do a bounded receipt probe.
                // Construct the finite set of exact retained receipt paths
                // and check them directly (C-23: bounded, not full scan).
                if self.check_duplicate_ack_bounded(lease, wall_floor) {
                    AckOutcome::AlreadyAcked
                } else {
                    AckOutcome::LeaseLost
                }
            }
            LeasedMoveOutcome::SourceChanged => {
                self.poison();
                AckOutcome::NotCommitted(Error::QueueCorrupt(
                    "leased source identity changed before acknowledgment".into(),
                ))
            }
            LeasedMoveOutcome::Failed(error) => AckOutcome::NotCommitted(error),
        }
    }

    /// Retry a lease immediately (move to ready).
    pub fn retry_now(&mut self, lease: &LeaseInfo) -> TransitionOutcome {
        self.retry(lease, RetryTiming::Immediate)
    }

    /// Retry a lease at a future time (move to delayed).
    pub fn retry_at(&mut self, lease: &LeaseInfo, not_before_ns: u64) -> TransitionOutcome {
        let wall_floor = match self.wall_floor_for_mutation() {
            Ok(floor) => floor,
            Err(error) => return TransitionOutcome::NotCommitted(error),
        };
        self.retry(
            lease,
            RetryTiming::Delayed {
                not_before_ns,
                wall_floor,
            },
        )
    }

    /// Retry a lease after a duration.
    pub fn retry_after(&mut self, lease: &LeaseInfo, duration_ns: u64) -> TransitionOutcome {
        let wall_floor = match self.wall_floor_for_mutation() {
            Ok(floor) => floor,
            Err(e) => return TransitionOutcome::NotCommitted(e),
        };
        let deadline = match steadq_math::retry_wall_deadline(wall_floor.unix_ns(), duration_ns) {
            Some(d) => d,
            None => {
                return TransitionOutcome::NotCommitted(Error::InvalidInput(
                    "deadline overflow".into(),
                ))
            }
        };
        self.retry(
            lease,
            RetryTiming::Delayed {
                not_before_ns: deadline,
                wall_floor,
            },
        )
    }

    /// Retry with a policy (computes delay from attempt and policy).
    pub fn retry_with_policy(
        &mut self,
        lease: &LeaseInfo,
        policy: &steadq_math::RetryPolicy,
    ) -> TransitionOutcome {
        if let Err(e) = policy.validate() {
            return TransitionOutcome::NotCommitted(Error::InvalidInput(e.to_string()));
        }

        let delay_ms = match steadq_math::retry_delay_ms(
            self.format.queue_id(),
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
            let delay_ns = match steadq_math::checked_mul_u64(delay_ms, 1_000_000) {
                Some(d) => d,
                None => {
                    return TransitionOutcome::NotCommitted(Error::InvalidInput(
                        "delay overflow".into(),
                    ))
                }
            };
            let wall_floor = match self.wall_floor_for_mutation() {
                Ok(floor) => floor,
                Err(e) => return TransitionOutcome::NotCommitted(e),
            };
            let deadline = match steadq_math::retry_wall_deadline(wall_floor.unix_ns(), delay_ns) {
                Some(d) => d,
                None => {
                    return TransitionOutcome::NotCommitted(Error::InvalidInput(
                        "deadline overflow".into(),
                    ))
                }
            };
            self.retry(
                lease,
                RetryTiming::Delayed {
                    not_before_ns: deadline,
                    wall_floor,
                },
            )
        }
    }

    fn retry(&mut self, lease: &LeaseInfo, timing: RetryTiming) -> TransitionOutcome {
        if let Err(e) = self.check_not_poisoned() {
            return TransitionOutcome::NotCommitted(e);
        }
        // If delayed target is at or before the effective wall floor, it's retry_now.
        let (delayed_ns, wall_floor) = match timing {
            RetryTiming::Immediate => (None, None),
            RetryTiming::Delayed {
                not_before_ns,
                wall_floor,
            } if not_before_ns <= wall_floor.unix_ns() => (None, Some(wall_floor)),
            RetryTiming::Delayed {
                not_before_ns,
                wall_floor,
            } => (Some(not_before_ns), Some(wall_floor)),
        };

        // Check attempt limit for retry
        if lease.attempt >= lease.maximum_attempts {
            let wall_floor = match wall_floor {
                Some(floor) => floor,
                None => match self.wall_floor_for_mutation() {
                    Ok(floor) => floor,
                    Err(error) => return TransitionOutcome::NotCommitted(error),
                },
            };
            // Move to dead with attempts_exhausted
            return match self.bury_with_wall_floor(lease, DeadReason::AttemptsExhausted, wall_floor)
            {
                TransitionOutcome::Committed => TransitionOutcome::Committed,
                other => other,
            };
        }

        let new_gen = match lease.generation.checked_add(1) {
            Some(g) => g,
            None => return TransitionOutcome::NotCommitted(Error::StateExhausted),
        };

        let (dest_dir, dest_name, operation, destination) = match delayed_ns {
            Some(nb) => {
                let common = CommonFields {
                    job_id: lease.job_id,
                    generation: new_gen,
                    attempt: lease.attempt,
                    maximum_attempts: lease.maximum_attempts,
                };
                let target = match self.layout().delayed(&common, nb) {
                    Ok(target) => target,
                    Err(error) => return TransitionOutcome::NotCommitted(error),
                };
                (
                    target.directory(),
                    target.filename,
                    TransitionOperation::RetryLater,
                    TicketDestination::Delayed { not_before_ns: nb },
                )
            }
            None => {
                let common = CommonFields {
                    job_id: lease.job_id,
                    generation: new_gen,
                    attempt: lease.attempt,
                    maximum_attempts: lease.maximum_attempts,
                };
                let target = self.layout().ready(&common);
                (
                    target.directory(),
                    target.filename,
                    TransitionOperation::RetryNow,
                    TicketDestination::Ready {},
                )
            }
        };

        let ticket = match self.transition_ticket_for_lease(lease, operation, destination) {
            Ok(ticket) => ticket,
            Err(error) => return TransitionOutcome::NotCommitted(error),
        };
        self.move_leased(lease, &dest_dir, &dest_name, &ticket)
    }

    /// Bury a lease (move to dead).
    pub fn bury(&mut self, lease: &LeaseInfo, reason: DeadReason) -> TransitionOutcome {
        if let Err(e) = self.check_not_poisoned() {
            return TransitionOutcome::NotCommitted(e);
        }
        self.bury_internal(lease, reason)
    }

    fn bury_internal(&mut self, lease: &LeaseInfo, reason: DeadReason) -> TransitionOutcome {
        let wall_floor = match self.wall_floor_for_mutation() {
            Ok(floor) => floor,
            Err(error) => return TransitionOutcome::NotCommitted(error),
        };
        self.bury_with_wall_floor(lease, reason, wall_floor)
    }

    fn bury_with_wall_floor(
        &mut self,
        lease: &LeaseInfo,
        reason: DeadReason,
        wall_floor: WallFloor,
    ) -> TransitionOutcome {
        let new_gen = match lease.generation.checked_add(1) {
            Some(g) => g,
            None => return TransitionOutcome::NotCommitted(Error::StateExhausted),
        };

        let common = CommonFields {
            job_id: lease.job_id,
            generation: new_gen,
            attempt: lease.attempt,
            maximum_attempts: lease.maximum_attempts,
        };

        let terminal_bucket =
            match bucket_number(wall_floor.unix_ns(), self.format.terminal_bucket_width_ns()) {
                Some(bucket) => bucket,
                None => return TransitionOutcome::NotCommitted(Error::StateExhausted),
            };
        let target = self
            .layout()
            .dead_in_bucket(&common, reason as u16, terminal_bucket);
        let dest_dir = target.directory();
        let fname = target.filename;
        let ticket = match self.transition_ticket_for_lease(
            lease,
            TransitionOperation::Bury,
            TicketDestination::Dead {
                terminal_bucket,
                reason: reason as u16,
            },
        ) {
            Ok(ticket) => ticket,
            Err(error) => return TransitionOutcome::NotCommitted(error),
        };

        self.move_leased(lease, &dest_dir, &fname, &ticket)
    }

    /// Renew a lease with a new deadline.
    pub fn renew(&mut self, lease: &LeaseInfo, lease_duration_ns: u64) -> RenewOutcome {
        if let Err(e) = self.check_not_poisoned() {
            return RenewOutcome::NotCommitted(e);
        }

        if !lease_duration_is_valid(lease_duration_ns) {
            return RenewOutcome::NotCommitted(Error::InvalidInput(
                "lease duration must be 1s to 7d".into(),
            ));
        }

        let boottime_now = match fs::clock_boottime_ns() {
            Ok(t) => t,
            Err(e) => return RenewOutcome::NotCommitted(Error::IoFailure(e.to_string())),
        };
        let wall_now = match self.wall_floor_for_mutation() {
            Ok(floor) => floor.unix_ns(),
            Err(e) => return RenewOutcome::NotCommitted(e),
        };
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

        let common = CommonFields {
            job_id: lease.job_id,
            generation: new_gen,
            attempt: lease.attempt,
            maximum_attempts: lease.maximum_attempts,
        };

        let target = self
            .layout()
            .leased(&common, new_boottime_dl, new_wall_dl, &lease.token)
            .unwrap();
        let dest_dir = target.directory();
        let fname = target.filename;

        let ticket = match self.transition_ticket_for_lease(
            lease,
            TransitionOperation::Renew,
            TicketDestination::Leased {
                boot_id: self.boot_id.clone(),
                boottime_deadline_ns: new_boottime_dl,
                wall_deadline_ns: new_wall_dl,
            },
        ) {
            Ok(ticket) => ticket,
            Err(error) => return RenewOutcome::NotCommitted(error),
        };

        match self.move_leased(lease, &dest_dir, &fname, &ticket) {
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
    fn is_expected_dev_zero(dev: u64) -> bool {
        dev == 0
    }

    fn is_expected_inode_zero(ino: u64) -> bool {
        ino == 0
    }

    fn shard_matches(path: u32, computed: u32) -> bool {
        path == computed
    }

    /// Returns a retained source descriptor and exact path identity on success.
    fn open_and_validate_current_lease(
        &self,
        lease: &LeaseInfo,
    ) -> Result<Option<LeasedSourceWitness>, Error> {
        if Self::is_expected_dev_zero(lease.expected_dev) {
            return Err(Error::QueueCorrupt(
                "expected_dev is zero (forgeable handle)".into(),
            ));
        }
        if Self::is_expected_inode_zero(lease.expected_inode) {
            return Err(Error::QueueCorrupt(
                "expected_inode is zero (forgeable handle)".into(),
            ));
        }

        let (loc, src_name) = self.layout().parse_leased_path(&lease.exact_source_path)?;
        let (boot_id, path_bucket, path_shard) = match &loc {
            layout::Location::Leased {
                boot_id,
                bucket,
                shard,
            } => (boot_id.clone(), *bucket, *shard),
            _ => unreachable!("parse_leased_path always returns Leased"),
        };

        if boot_id != self.boot_id {
            return Err(Error::InvalidInput(format!(
                "source boot_id '{}' does not match queue boot_id '{}'",
                boot_id, self.boot_id
            )));
        }
        if boot_id != lease.boot_id {
            return Err(Error::QueueCorrupt(
                "source boot_id does not match lease handle".into(),
            ));
        }

        let computed_shard = compute_shard(
            self.format.queue_id(),
            &lease.job_id,
            self.format.shard_count(),
        );
        if !Self::shard_matches(path_shard, computed_shard) {
            return Err(Error::QueueCorrupt(format!(
                "source shard {path_shard} does not match queue-derived shard {computed_shard}"
            )));
        }

        let src_dir = match loc {
            layout::Location::Leased {
                boot_id,
                bucket,
                shard,
            } => {
                format!(
                    "leased/{}/{}/{}",
                    boot_id,
                    bucket_hex(bucket),
                    shard_hex(shard)
                )
            }
            _ => unreachable!(),
        };

        // R2-H02: Only ENOENT means "source gone". Other errors are real failures.
        let src_dir_fd = match open_relative(self.root_fd.as_fd(), &src_dir) {
            Ok(fd) => fd,
            Err(error) => match classify_lease_directory_open_failure(&error) {
                LeaseDirectoryOpenFailure::Gone => return Ok(None),
                LeaseDirectoryOpenFailure::InvalidDirectory => {
                    return Err(Error::QueueCorrupt(
                        "intermediate lease path component is not a directory".into(),
                    ));
                }
                LeaseDirectoryOpenFailure::Io => {
                    return Err(Error::IoFailure(error.to_string()));
                }
            },
        };

        let src_stat = match fs::fstatat(src_dir_fd.as_fd(), &src_name) {
            Ok(s) => s,
            Err(error) => match classify_presence_failure(&error) {
                PresenceFailure::Absent => return Ok(None),
                PresenceFailure::Io => return Err(Error::IoFailure(error.to_string())),
            },
        };

        if !is_singly_linked_regular(src_stat.st_mode, src_stat.st_nlink) {
            return Err(Error::QueueCorrupt(
                "source is not a singly-linked regular file".into(),
            ));
        }

        let parsed = steadq_names::parse_leased(&src_name).map_err(|_| {
            Error::QueueCorrupt("source filename is not a valid leased name".into())
        })?;

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
        if parsed.boottime_deadline_ns != lease.expires_boottime_ns {
            return Err(Error::QueueCorrupt(
                "source boottime deadline mismatch".into(),
            ));
        }
        if parsed.wall_deadline_ns != lease.expires_wall_ns {
            return Err(Error::QueueCorrupt("source wall deadline mismatch".into()));
        }
        let expected_bucket = steadq_math::lease_bucket(
            parsed.boottime_deadline_ns,
            self.format.lease_bucket_width_ns(),
        )
        .ok_or(Error::StateExhausted)?;
        if path_bucket != expected_bucket {
            return Err(Error::QueueCorrupt("source lease bucket mismatch".into()));
        }
        if !parsed.authenticate_tag(
            self.format.queue_id(),
            &boot_id,
            &bucket_hex(path_bucket),
            &shard_hex(path_shard),
        ) {
            return Err(Error::QueueCorrupt("source name tag mismatch".into()));
        }

        let file_fd = fs::openat(src_dir_fd.as_fd(), &src_name, resolver_file_open_flags(), 0)
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        let opened_stat =
            fs::fstat(file_fd.as_fd()).map_err(|error| Error::IoFailure(error.to_string()))?;
        if !stat_matches_witness(&opened_stat, lease.expected_dev, lease.expected_inode) {
            return Err(Error::QueueCorrupt(
                "opened source identity does not match lease handle".into(),
            ));
        }
        if !stat_matches_witness(
            &src_stat,
            opened_stat.st_dev as u64,
            opened_stat.st_ino as u64,
        ) {
            return Err(Error::QueueCorrupt(
                "source path changed while opening lease".into(),
            ));
        }
        let mut header_buf = [0u8; 128];
        fs::pread_exact(file_fd.as_fd(), &mut header_buf, 0)
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        let header = FixedHeader::decode(&header_buf)
            .map_err(|e| Error::QueueCorrupt(format!("header decode: {e}")))?;

        if header.job_id != lease.job_id {
            return Err(Error::QueueCorrupt(
                "header job_id does not match handle".into(),
            ));
        }

        // H5: Verify header maximum_attempts matches filename/handle
        if header.maximum_attempts != lease.maximum_attempts {
            return Err(Error::QueueCorrupt(format!(
                "header maximum_attempts {} does not match handle {}",
                header.maximum_attempts, lease.maximum_attempts
            )));
        }

        // R4-H04: Verify envelope digest matches the handle
        if header.envelope_digest != lease.envelope_digest {
            return Err(Error::QueueCorrupt(
                "envelope digest does not match handle".into(),
            ));
        }
        if header.payload_length != lease.payload_length {
            return Err(Error::QueueCorrupt(
                "payload length does not match handle".into(),
            ));
        }
        if header.payload_digest != lease.payload_digest {
            return Err(Error::QueueCorrupt(
                "payload digest does not match handle".into(),
            ));
        }

        // R2-H03: Extension read failure is a real error, not a silent pass.
        let ext_len = header.extension_header_length as usize;
        if verified::is_extension_too_large(ext_len) {
            return Err(Error::QueueCorrupt("extension header too large".into()));
        }
        // R4-H05: Always verify envelope digest (even when extension is empty).
        let mut ext_buf = vec![0u8; ext_len];
        if verified::is_extension_present(ext_len) {
            fs::pread_exact(file_fd.as_fd(), &mut ext_buf, 128)
                .map_err(|e| Error::IoFailure(e.to_string()))?;
        }
        if !steadq_format::verify_envelope_digest(&header, &ext_buf) {
            return Err(Error::QueueCorrupt("envelope digest mismatch".into()));
        }

        // R2-B02: Verify exact file size (no trailing data)
        if opened_stat.st_size < 0 {
            return Err(Error::QueueCorrupt("negative file size".into()));
        }
        let expected_size = 128u64
            .checked_add(ext_len as u64)
            .and_then(|s| s.checked_add(header.payload_length))
            .ok_or_else(|| Error::QueueCorrupt("size overflow".into()))?;
        if opened_stat.st_size as u64 != expected_size {
            return Err(Error::QueueCorrupt(format!(
                "source file size mismatch: expected {}, got {}",
                expected_size, opened_stat.st_size
            )));
        }

        Ok(Some(LeasedSourceWitness {
            directory_fd: src_dir_fd,
            name: src_name,
            file_fd,
            device: opened_stat.st_dev as u64,
            inode: opened_stat.st_ino as u64,
        }))
    }

    fn observe_leased_source_path(
        source: &LeasedSourceWitness,
    ) -> Result<WitnessPathObservation, Error> {
        observe_witness_path(
            source.directory_fd.as_fd(),
            &source.name,
            source.device,
            source.inode,
        )
    }

    fn execute_leased_move(
        source: &LeasedSourceWitness,
        destination_directory_fd: BorrowedFd<'_>,
        destination_name: &str,
    ) -> LeasedMoveOutcome {
        Self::execute_leased_move_with_dirty(
            source,
            destination_directory_fd,
            destination_name,
            None,
        )
    }

    fn execute_leased_move_with_dirty(
        source: &LeasedSourceWitness,
        destination_directory_fd: BorrowedFd<'_>,
        destination_name: &str,
        dirty: Option<&mut engine::DirtySet>,
    ) -> LeasedMoveOutcome {
        match Self::observe_leased_source_path(source) {
            Ok(WitnessPathObservation::Match) => {}
            Ok(WitnessPathObservation::Gone) => return LeasedMoveOutcome::SourceGone,
            Ok(WitnessPathObservation::Mismatch) => {
                return LeasedMoveOutcome::SourceChanged;
            }
            Err(error) => return LeasedMoveOutcome::Failed(error),
        }

        let result = match &dirty {
            Some(_) => engine::move_witnessed_noreplace_deferred(
                source.directory_fd.as_fd(),
                &source.name,
                destination_directory_fd,
                destination_name,
                engine::MoveIdentity::new(source.device, source.inode),
                |_| Ok(()),
            )
            .map(|_| ()),
            None => engine::move_witnessed_noreplace(
                source.directory_fd.as_fd(),
                &source.name,
                destination_directory_fd,
                destination_name,
                engine::MoveIdentity::new(source.device, source.inode),
                engine::MoveActor::Consumer,
            ),
        };
        let outcome = match result {
            Ok(()) => LeasedMoveOutcome::Committed,
            Err(engine::MoveFailure::SourceMissing) => LeasedMoveOutcome::SourceGone,
            Err(engine::MoveFailure::AlreadyExists) => LeasedMoveOutcome::Collision,
            Err(engine::MoveFailure::NotCommitted { source, .. }) => {
                LeasedMoveOutcome::Failed(Error::IoFailure(source.to_string()))
            }
            Err(engine::MoveFailure::OutcomeUnknown { phase, .. }) => {
                LeasedMoveOutcome::OutcomeUnknown(ticket_phase_for_move_outcome_unknown(phase))
            }
        };
        if matches!(outcome, LeasedMoveOutcome::Committed) {
            if let Some(d) = dirty {
                match d
                    .record(source.directory_fd.as_fd())
                    .and_then(|()| d.record(destination_directory_fd))
                {
                    Ok(()) => {}
                    Err(_) => {
                        return LeasedMoveOutcome::OutcomeUnknown(
                            TransitionPhase::DestinationDirectoryDurable,
                        )
                    }
                }
            }
        }
        outcome
    }

    /// Internal: move a leased object to a new state directory.
    fn move_leased(
        &mut self,
        lease: &LeaseInfo,
        dest_dir: &str,
        dest_name: &str,
        ticket: &TransitionTicket,
    ) -> TransitionOutcome {
        if let Err(e) = self.ensure_dir(dest_dir) {
            return TransitionOutcome::NotCommitted(Error::IoFailure(e.to_string()));
        }

        let dest_dir_fd = match open_relative(self.root_fd.as_fd(), dest_dir) {
            Ok(fd) => fd,
            Err(e) => return TransitionOutcome::NotCommitted(Error::IoFailure(e.to_string())),
        };

        // B-04: Validate the current lease source before transitioning
        let source = match self.open_and_validate_current_lease(lease) {
            Ok(Some(source)) => source,
            Ok(None) => return TransitionOutcome::LeaseLost,
            Err(Error::QueueCorrupt(e)) => {
                self.poison();
                return TransitionOutcome::NotCommitted(Error::QueueCorrupt(e));
            }
            Err(e) => return TransitionOutcome::NotCommitted(e),
        };

        match Self::execute_leased_move(&source, dest_dir_fd.as_fd(), dest_name) {
            LeasedMoveOutcome::Committed => TransitionOutcome::Committed,
            LeasedMoveOutcome::OutcomeUnknown(phase) => {
                self.poison();
                TransitionOutcome::OutcomeUnknown(ticket.with_phase(phase))
            }
            LeasedMoveOutcome::SourceGone => TransitionOutcome::LeaseLost,
            LeasedMoveOutcome::SourceChanged => {
                self.poison();
                TransitionOutcome::NotCommitted(Error::QueueCorrupt(
                    "leased source identity changed before transition".into(),
                ))
            }
            LeasedMoveOutcome::Collision => {
                TransitionOutcome::NotCommitted(Error::QueueCorrupt("destination exists".into()))
            }
            LeasedMoveOutcome::Failed(error) => TransitionOutcome::NotCommitted(error),
        }
    }

    fn transition_ticket_for_lease(
        &self,
        lease: &LeaseInfo,
        operation: TransitionOperation,
        destination: TicketDestination,
    ) -> Result<TransitionTicket, Error> {
        TransitionTicket::new(
            *self.format.queue_id(),
            operation,
            TransitionPhase::Linearized,
            TicketIdentity::new(
                lease.job_id,
                lease.generation,
                lease.attempt,
                lease.maximum_attempts,
                lease.token,
                TicketEvidence::new(lease.envelope_digest, lease.payload_length),
            ),
            TicketSource::Leased {
                boot_id: lease.boot_id.clone(),
                boottime_deadline_ns: lease.expires_boottime_ns,
                wall_deadline_ns: lease.expires_wall_ns,
            },
            destination,
        )
    }

    fn claim_transition_ticket(
        &self,
        source: &CommonFields,
        lease_token: [u8; 16],
        evidence: TicketEvidence,
        boottime_deadline_ns: u64,
        wall_deadline_ns: u64,
    ) -> Result<TransitionTicket, Error> {
        TransitionTicket::new(
            *self.format.queue_id(),
            TransitionOperation::Claim,
            TransitionPhase::Linearized,
            TicketIdentity::new(
                source.job_id,
                source.generation,
                source.attempt,
                source.maximum_attempts,
                lease_token,
                evidence,
            ),
            TicketSource::Ready {},
            TicketDestination::Leased {
                boot_id: self.boot_id.clone(),
                boottime_deadline_ns,
                wall_deadline_ns,
            },
        )
    }

    fn open_claim_source(
        directory_fd: BorrowedFd<'_>,
        name: &str,
        expected_job_id: &[u8; 16],
        expected_maximum_attempts: u32,
    ) -> Result<Option<ClaimSourceWitness>, Error> {
        let file = match fs::openat(directory_fd, name, resolver_file_open_flags(), 0) {
            Ok(file) => file,
            Err(error) if error.raw_os_error() == Some(libc::ENOENT) => return Ok(None),
            Err(error) => return Err(Error::IoFailure(error.to_string())),
        };
        let stat = fs::fstat(file.as_fd()).map_err(|error| Error::IoFailure(error.to_string()))?;
        if !is_singly_linked_regular(stat.st_mode, stat.st_nlink) {
            return Err(Error::QueueCorrupt(
                "ready source is not a singly-linked regular file".into(),
            ));
        }
        let evidence = Self::read_claim_ticket_evidence(
            file.as_fd(),
            expected_job_id,
            expected_maximum_attempts,
        )?;
        Ok(Some(ClaimSourceWitness {
            file_fd: file,
            device: stat.st_dev as u64,
            inode: stat.st_ino as u64,
            evidence,
        }))
    }

    fn read_claim_ticket_evidence(
        file_fd: BorrowedFd<'_>,
        expected_job_id: &[u8; 16],
        expected_maximum_attempts: u32,
    ) -> Result<TicketEvidence, Error> {
        let verified = verified::verify_envelope_on_fd(file_fd).map_err(Error::from)?;
        let header = verified.header();
        if &header.job_id != expected_job_id {
            return Err(Error::QueueCorrupt("header job_id mismatch".into()));
        }
        if header.maximum_attempts != expected_maximum_attempts {
            return Err(Error::QueueCorrupt(
                "header maximum_attempts mismatch".into(),
            ));
        }
        Ok(TicketEvidence::new(
            header.envelope_digest,
            header.payload_length,
        ))
    }

    /// Move a ready object to dead (for exhausted attempts cleanup).
    fn move_to_dead(
        &mut self,
        ready_dir: &str,
        ready_name: &str,
        common: &CommonFields,
        reason: DeadReason,
        wall_floor: WallFloor,
    ) -> Result<(), Error> {
        let terminal_bucket = match steadq_math::bucket_number(
            wall_floor.unix_ns(),
            self.format.terminal_bucket_width_ns(),
        ) {
            Some(bucket) => bucket,
            None => return Err(Error::StateExhausted),
        };

        let new_gen = common
            .generation
            .checked_add(1)
            .ok_or(Error::StateExhausted)?;
        let dead_common = CommonFields {
            job_id: common.job_id,
            generation: new_gen,
            attempt: common.attempt,
            maximum_attempts: common.maximum_attempts,
        };

        let target = self
            .layout()
            .dead_in_bucket(&dead_common, reason as u16, terminal_bucket);
        let dead_dir = target.directory();

        self.ensure_dir(&dead_dir)
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        let dead_dir_fd = open_relative(self.root_fd.as_fd(), &dead_dir)
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        let ready_dir_fd = open_relative(self.root_fd.as_fd(), ready_dir)
            .map_err(|e| Error::IoFailure(e.to_string()))?;

        match engine::move_verified_noreplace(
            ready_dir_fd.as_fd(),
            ready_name,
            dead_dir_fd.as_fd(),
            &target.filename,
            engine::MoveActor::Consumer,
        ) {
            Ok(()) => Ok(()),
            Err(engine::MoveFailure::SourceMissing) => Ok(()),
            Err(engine::MoveFailure::AlreadyExists) => Err(Error::IdentityCollision),
            Err(engine::MoveFailure::NotCommitted { phase, source }) => Err(Error::IoFailure(
                format!("dead-letter move failed at {phase:?}: {source}"),
            )),
            Err(engine::MoveFailure::OutcomeUnknown { phase, source }) => Err(Error::IoFailure(
                format!("dead-letter move indeterminate at {phase:?}: {source}"),
            )),
        }
    }
    /// B-09: Read and verify the payload of a leased job.
    /// Validates source identity (B-04), then verifies envelope digest,
    /// then hashes the payload and compares to the header digest.
    /// Returns Ok(()) on success, Err(PayloadCorrupt) if the digest does not match.
    pub fn verify_lease_payload(&self, lease: &LeaseInfo) -> Result<(), Error> {
        let source = match self.open_and_validate_current_lease(lease)? {
            Some(source) => source,
            None => return Err(Error::QueueCorrupt("lease source not found".into())),
        };
        self.verify_payload_on_fd(source.file_fd.as_fd())
    }

    /// R4-H22/H23: Verify the payload digest on an already-open file descriptor.
    /// Central verifier is the single source of truth; this wrapper preserves
    /// the existing Error mapping for callers that have not yet adopted
    /// VerificationError directly.
    fn verify_payload_on_fd(&self, fd: BorrowedFd<'_>) -> Result<(), Error> {
        verified::verify_job_on_fd(fd)
            .map(|_| ())
            .map_err(Error::from)
    }

    /// Verify only the envelope and size, without hashing payload bytes.
    /// Used by inspection paths that have not yet delivered payload.
    fn verify_envelope_on_fd(&self, fd: BorrowedFd<'_>) -> Result<verified::VerifiedJob, Error> {
        verified::verify_envelope_on_fd(fd).map_err(Error::from)
    }

    fn quarantine_corrupt_lease(
        &self,
        leased_dir_fd: BorrowedFd<'_>,
        leased_name: &str,
        held_fd: BorrowedFd<'_>,
    ) -> Result<(), engine::MoveFailure> {
        let held_stat = fs::fstat(held_fd).map_err(|source| engine::MoveFailure::NotCommitted {
            phase: engine::MovePhase::PreRename,
            source,
        })?;
        let name_stat = fs::fstatat(leased_dir_fd, leased_name).map_err(|source| {
            engine::MoveFailure::NotCommitted {
                phase: engine::MovePhase::PreRename,
                source,
            }
        })?;
        if held_stat.st_dev != name_stat.st_dev || held_stat.st_ino != name_stat.st_ino {
            return Err(engine::MoveFailure::SourceMissing);
        }
        let source_identity = engine::MoveIdentity::new(held_stat.st_dev, held_stat.st_ino);

        let qid = fs::random_128bit().map_err(|source| engine::MoveFailure::NotCommitted {
            phase: engine::MovePhase::PreRename,
            source,
        })?;
        let q_name =
            steadq_names::quarantine_filename(&qid, QuarantineReason::PayloadCorrupt as u16);
        self.ensure_dir("quarantine")
            .map_err(|source| engine::MoveFailure::NotCommitted {
                phase: engine::MovePhase::PreRename,
                source,
            })?;
        let q_dir_fd = open_relative(self.root_fd.as_fd(), "quarantine").map_err(|source| {
            engine::MoveFailure::NotCommitted {
                phase: engine::MovePhase::PreRename,
                source,
            }
        })?;

        engine::move_witnessed_noreplace(
            leased_dir_fd,
            leased_name,
            q_dir_fd.as_fd(),
            &q_name,
            source_identity,
            engine::MoveActor::Consumer,
        )
    }
    /// R4-PERF: Read a chunk of a leased job's payload at the given offset.
    /// Returns the number of bytes read (0 at EOF).
    /// Validates source identity before reading (B-04).
    pub fn read_lease_payload_chunk(
        &self,
        lease: &LeaseInfo,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<usize, Error> {
        let source = match self.open_and_validate_current_lease(lease)? {
            Some(source) => source,
            None => return Err(Error::QueueCorrupt("lease source not found".into())),
        };
        // P0-01: Verify payload before delivering any bytes.
        if let Err(e) = self.verify_payload_on_fd(source.file_fd.as_fd()) {
            if matches!(e, Error::PayloadCorrupt) {
                if let Err(engine::MoveFailure::OutcomeUnknown {
                    phase,
                    source: detail,
                }) = self.quarantine_corrupt_lease(
                    source.directory_fd.as_fd(),
                    &source.name,
                    source.file_fd.as_fd(),
                ) {
                    return Err(Error::QueueCorrupt(format!(
                        "payload is corrupt and quarantine is indeterminate at {phase:?}: {detail}"
                    )));
                }
            }
            return Err(e);
        }
        let mut header_buf = [0u8; 128];
        fs::pread_exact(source.file_fd.as_fd(), &mut header_buf, 0)
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        let header =
            FixedHeader::decode(&header_buf).map_err(|e| Error::QueueCorrupt(e.to_string()))?;
        let ext_len = header.extension_header_length as usize;
        let payload_start = (128 + ext_len) as u64;
        let payload_len = header.payload_length;
        if offset >= payload_len {
            return Ok(0);
        }
        let remaining = payload_len
            .checked_sub(offset)
            .expect("offset below payload length was checked");
        let to_read = (buf.len() as u64).min(remaining) as usize;
        let abs_offset = payload_start + offset;
        let n = fs::pread(source.file_fd.as_fd(), &mut buf[..to_read], abs_offset)
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        Ok(n)
    }

    /// P1-14: Stream a leased job's payload with O(1) validation/open.
    /// Opens the file once, validates identity once, reads header once,
    /// then performs pread calls on the held fd.
    pub fn stream_lease_payload<F: FnMut(&[u8]) -> Result<(), Error>>(
        &self,
        lease: &LeaseInfo,
        chunk_size: usize,
        mut f: F,
    ) -> Result<(), Error> {
        let source = match self.open_and_validate_current_lease(lease)? {
            Some(source) => source,
            None => return Err(Error::QueueCorrupt("lease source not found".into())),
        };
        // P0-01: Verify payload before streaming any bytes.
        if let Err(e) = self.verify_payload_on_fd(source.file_fd.as_fd()) {
            if matches!(e, Error::PayloadCorrupt) {
                if let Err(engine::MoveFailure::OutcomeUnknown {
                    phase,
                    source: detail,
                }) = self.quarantine_corrupt_lease(
                    source.directory_fd.as_fd(),
                    &source.name,
                    source.file_fd.as_fd(),
                ) {
                    return Err(Error::QueueCorrupt(format!(
                        "payload is corrupt and quarantine is indeterminate at {phase:?}: {detail}"
                    )));
                }
            }
            return Err(e);
        }

        let mut header_buf = [0u8; 128];
        fs::pread_exact(source.file_fd.as_fd(), &mut header_buf, 0)
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        let header =
            FixedHeader::decode(&header_buf).map_err(|e| Error::QueueCorrupt(e.to_string()))?;

        let ext_len = header.extension_header_length as usize;
        let payload_start = (128 + ext_len) as u64;
        let payload_len = header.payload_length;

        let cap = chunk_size.clamp(4096, 1 << 20);
        let mut buf = vec![0u8; cap];
        let mut offset = 0u64;
        while offset < payload_len {
            let remaining = payload_len
                .checked_sub(offset)
                .expect("offset below payload length was checked");
            let to_read = (buf.len() as u64).min(remaining) as usize;
            let n = fs::pread(
                source.file_fd.as_fd(),
                &mut buf[..to_read],
                payload_start + offset,
            )
            .map_err(|e| Error::IoFailure(e.to_string()))?;
            if n == 0 {
                return Err(Error::QueueCorrupt("unexpected EOF during stream".into()));
            }
            f(&buf[..n])?;
            offset = offset
                .checked_add(n as u64)
                .expect("stream offset cannot exceed the verified payload length");
        }
        Ok(())
    }

    /// Open a verified payload reader for a lease. The payload is hashed
    /// once at construction; subsequent `read_at` calls do not re-hash.
    pub fn open_verified_payload_reader(
        &self,
        lease: &LeaseInfo,
    ) -> Result<Option<VerifiedPayloadReader>, Error> {
        let source = match self.open_and_validate_current_lease(lease)? {
            Some(source) => source,
            None => return Ok(None),
        };
        // P0-01: Verify payload before allowing reads.
        if let Err(e) = self.verify_payload_on_fd(source.file_fd.as_fd()) {
            if matches!(e, Error::PayloadCorrupt) {
                if let Err(engine::MoveFailure::OutcomeUnknown {
                    phase,
                    source: detail,
                }) = self.quarantine_corrupt_lease(
                    source.directory_fd.as_fd(),
                    &source.name,
                    source.file_fd.as_fd(),
                ) {
                    return Err(Error::QueueCorrupt(format!(
                        "payload is corrupt and quarantine is indeterminate at {phase:?}: {detail}"
                    )));
                }
            }
            return Err(e);
        }
        let mut header_buf = [0u8; 128];
        fs::pread_exact(source.file_fd.as_fd(), &mut header_buf, 0)
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        let header =
            FixedHeader::decode(&header_buf).map_err(|e| Error::QueueCorrupt(e.to_string()))?;
        let ext_len = header.extension_header_length as usize;
        Ok(Some(VerifiedPayloadReader {
            file_fd: source.file_fd,
            payload_start: (128 + ext_len) as u64,
            payload_len: header.payload_length,
        }))
    }

    /// Diagnostic lookup: find all states for a job_id.
    /// Scans active and terminal states for the computed shard.
    pub fn inspect(&self, job_id: &[u8; 16]) -> Vec<Snapshot> {
        let mut results = Vec::new();
        let shard = compute_shard(self.format.queue_id(), job_id, self.format.shard_count());
        let shard_str = shard_hex(shard);

        // Check ready
        let ready_dir = format!("ready/{shard_str}");
        if let Ok(dir_fd) = open_relative(self.root_fd.as_fd(), &ready_dir) {
            if let Ok(entries) = fs::read_dir_entries(dir_fd.as_fd()) {
                for entry in entries {
                    let Some(entry) = entry.as_ascii_str() else {
                        continue;
                    };
                    if let Ok(parsed) = steadq_names::parse_ready(entry) {
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
        if let Ok(leased_root) = fs::open_directory(self.root_fd.as_fd(), "leased") {
            if let Ok(boot_dirs) = fs::read_dir_entries(leased_root.as_fd()) {
                for boot_dir in boot_dirs {
                    let Some(boot_dir) = boot_dir.as_ascii_str() else {
                        continue;
                    };
                    let boot_path = format!("leased/{boot_dir}");
                    if let Ok(boot_fd) = open_relative(self.root_fd.as_fd(), &boot_path) {
                        if let Ok(bucket_dirs) = fs::read_dir_entries(boot_fd.as_fd()) {
                            for bucket_dir in bucket_dirs {
                                let Some(bucket_dir) = bucket_dir.as_ascii_str() else {
                                    continue;
                                };
                                let shard_path = format!("{boot_path}/{bucket_dir}/{shard_str}");
                                if let Ok(shard_fd) =
                                    open_relative(self.root_fd.as_fd(), &shard_path)
                                {
                                    if let Ok(entries) = fs::read_dir_entries(shard_fd.as_fd()) {
                                        for entry in entries {
                                            let Some(entry) = entry.as_ascii_str() else {
                                                continue;
                                            };
                                            if let Ok(parsed) = steadq_names::parse_leased(entry) {
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
        if let Ok(delayed_root) = fs::open_directory(self.root_fd.as_fd(), "delayed") {
            if let Ok(bucket_dirs) = fs::read_dir_entries(delayed_root.as_fd()) {
                for bucket_dir in bucket_dirs {
                    let Some(bucket_dir) = bucket_dir.as_ascii_str() else {
                        continue;
                    };
                    let shard_path = format!("delayed/{bucket_dir}/{shard_str}");
                    if let Ok(shard_fd) = open_relative(self.root_fd.as_fd(), &shard_path) {
                        if let Ok(entries) = fs::read_dir_entries(shard_fd.as_fd()) {
                            for entry in entries {
                                let Some(entry) = entry.as_ascii_str() else {
                                    continue;
                                };
                                if let Ok(parsed) = steadq_names::parse_delayed(entry) {
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
        if let Ok(dead_root) = fs::open_directory(self.root_fd.as_fd(), "dead") {
            if let Ok(bucket_dirs) = fs::read_dir_entries(dead_root.as_fd()) {
                for bucket_dir in bucket_dirs {
                    let Some(bucket_dir) = bucket_dir.as_ascii_str() else {
                        continue;
                    };
                    let shard_path = format!("dead/{bucket_dir}/{shard_str}");
                    if let Ok(shard_fd) = open_relative(self.root_fd.as_fd(), &shard_path) {
                        if let Ok(entries) = fs::read_dir_entries(shard_fd.as_fd()) {
                            for entry in entries {
                                let Some(entry) = entry.as_ascii_str() else {
                                    continue;
                                };
                                if let Ok(parsed) = steadq_names::parse_dead(entry) {
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
        if let Ok(receipts_root) = fs::open_directory(self.root_fd.as_fd(), "receipts") {
            if let Ok(bucket_dirs) = fs::read_dir_entries(receipts_root.as_fd()) {
                for bucket_dir in bucket_dirs {
                    let Some(bucket_dir) = bucket_dir.as_ascii_str() else {
                        continue;
                    };
                    let shard_path = format!("receipts/{bucket_dir}/{shard_str}");
                    if let Ok(shard_fd) = open_relative(self.root_fd.as_fd(), &shard_path) {
                        if let Ok(entries) = fs::read_dir_entries(shard_fd.as_fd()) {
                            for entry in entries {
                                let Some(entry) = entry.as_ascii_str() else {
                                    continue;
                                };
                                if let Ok(parsed) = steadq_names::parse_receipt(entry) {
                                    if parsed.common.job_id == *job_id {
                                        let file_fd = match fs::openat(
                                            shard_fd.as_fd(),
                                            entry,
                                            verified::receipt_read_open_flags(),
                                            0,
                                        ) {
                                            Ok(file_fd) => file_fd,
                                            Err(_) => continue,
                                        };
                                        if verified::verify_receipt_on_fd(
                                            file_fd.as_fd(),
                                            verified::ReceiptContext {
                                                queue_id: self.format.queue_id(),
                                                shard_count: self.format.shard_count(),
                                                terminal_bucket_width_ns: self
                                                    .format
                                                    .terminal_bucket_width_ns(),
                                                max_payload_length: self
                                                    .format
                                                    .max_payload_length(),
                                                bucket: bucket_dir,
                                                shard: &shard_str,
                                                filename: entry,
                                            },
                                            None,
                                        )
                                        .is_ok()
                                        {
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
        }

        results
    }

    /// Export a dead job's raw bytes to an output file. Opens the job through
    /// the root capability with O_NOFOLLOW, not via a pathname.
    pub fn export_dead(&self, job_id: &[u8; 16], output: &std::path::Path) -> Result<u64, Error> {
        let snapshot = self
            .inspect(job_id)
            .into_iter()
            .find(|s| s.state == "dead")
            .ok_or_else(|| Error::QueueCorrupt("dead job not found".into()))?;

        let (dir_rel, name) = snapshot
            .relative_path
            .rsplit_once('/')
            .ok_or_else(|| Error::QueueCorrupt("invalid dead path".into()))?;

        let dir_fd = open_relative(self.root_fd.as_fd(), dir_rel)
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        let file_fd = fs::openat(
            dir_fd.as_fd(),
            name,
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0,
        )
        .map_err(|e| Error::IoFailure(e.to_string()))?;

        let stat = fs::fstat(file_fd.as_fd()).map_err(|e| Error::IoFailure(e.to_string()))?;
        if stat.st_size < 0 {
            return Err(Error::QueueCorrupt("negative file size".into()));
        }
        let size = stat.st_size as u64;

        let mut out = std::fs::File::create(output).map_err(|e| Error::IoFailure(e.to_string()))?;
        let mut offset = 0u64;
        let mut buf = vec![0u8; 65536];
        while offset < size {
            let n = fs::pread(file_fd.as_fd(), &mut buf, offset)
                .map_err(|e| Error::IoFailure(e.to_string()))?;
            if n == 0 {
                break;
            }
            use std::io::Write;
            out.write_all(&buf[..n])
                .map_err(|e| Error::IoFailure(e.to_string()))?;
            offset += n as u64;
        }
        out.sync_all()
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        Ok(offset)
    }

    /// Remove a dead job through the phase-aware unlink executor.
    pub fn remove_dead(&self, job_id: &[u8; 16]) -> Result<bool, Error> {
        let snapshot = self
            .inspect(job_id)
            .into_iter()
            .find(|s| s.state == "dead")
            .ok_or_else(|| Error::QueueCorrupt("dead job not found".into()))?;

        let (dir_rel, name) = snapshot
            .relative_path
            .rsplit_once('/')
            .ok_or_else(|| Error::QueueCorrupt("invalid dead path".into()))?;

        let dir_fd = open_relative(self.root_fd.as_fd(), dir_rel)
            .map_err(|e| Error::IoFailure(e.to_string()))?;

        match engine::unlink_verified(dir_fd.as_fd(), name, engine::MoveActor::Consumer) {
            Ok(()) => Ok(true),
            Err(engine::UnlinkFailure::SourceMissing) => Ok(false),
            Err(engine::UnlinkFailure::NotCommitted { phase, source }) => Err(Error::IoFailure(
                format!("dead removal failed at {phase:?}: {source}"),
            )),
            Err(engine::UnlinkFailure::OutcomeUnknown { phase, source }) => Err(Error::IoFailure(
                format!("dead removal indeterminate at {phase:?}: {source}"),
            )),
        }
    }

    /// Duplicate acknowledgment probe: check if a receipt exists for this lease.
    /// Probes exact receipt filenames across retained terminal buckets.
    pub fn check_duplicate_ack(&self, lease: &LeaseInfo) -> AckOutcome {
        let wall_floor = match self.authenticated_wall_floor() {
            Ok(wall_floor) => wall_floor,
            Err(error) => return AckOutcome::NotCommitted(error),
        };
        if self.check_duplicate_ack_bounded(lease, wall_floor) {
            AckOutcome::AlreadyAcked
        } else {
            AckOutcome::LeaseLost
        }
    }

    /// B1: Authenticate an active-state object structurally.
    /// Validates: file type, link count, header, envelope digest, file size,
    /// name tag, shard placement, and header/name consistency with typed path context.
    /// Returns the validated header on success.
    pub(crate) fn validate_active_object(
        &self,
        dir_fd: BorrowedFd<'_>,
        name: &str,
        ctx: &ActivePathContext,
    ) -> Result<FixedHeader, Error> {
        // Stat with NOFOLLOW
        let stat = fs::fstatat(dir_fd, name).map_err(|e| Error::IoFailure(e.to_string()))?;

        // Regular file
        if stat.st_mode & libc::S_IFMT != libc::S_IFREG {
            return Err(Error::QueueCorrupt(format!("{name}: not a regular file")));
        }

        // Link count
        if stat.st_nlink != 1 {
            return Err(Error::QueueCorrupt(format!(
                "{name}: unexpected link count {}",
                stat.st_nlink
            )));
        }

        // Use central verifier for header, extension, envelope, and size.
        // stat has already been collected for mode and nlink; verify_envelope_on_fd
        // will re-stat the fd for size, which is fine since the fd is held open.
        let file_fd = fs::openat(dir_fd, name, libc::O_RDONLY, 0)
            .map_err(|e| Error::IoFailure(e.to_string()))?;
        let verified = self.verify_envelope_on_fd(file_fd.as_fd())?;
        let header = verified.header();

        // R4-H06: Check queue-configured payload limit
        if !payload_length_is_valid(header.payload_length, self.format.max_payload_length()) {
            return Err(Error::QueueCorrupt(format!(
                "payload length {} exceeds queue limit {}",
                header.payload_length,
                self.format.max_payload_length()
            )));
        }

        // Parse and verify filename with typed path context and tag authentication.
        let (
            job_id,
            _parsed_gen,
            _parsed_attempt,
            max_att,
            parsed_tag,
            expected_tag,
            path_shard_str,
        ) = match ctx {
            ActivePathContext::Ready { shard } => {
                let p = steadq_names::parse_ready(name)
                    .map_err(|_| Error::QueueCorrupt("invalid ready filename".into()))?;
                if !p.authenticate_tag(self.format.queue_id(), shard) {
                    return Err(Error::QueueCorrupt("name tag mismatch".into()));
                }
                (
                    p.common.job_id,
                    p.common.generation,
                    p.common.attempt,
                    p.common.maximum_attempts,
                    p.tag,
                    p.tag,
                    shard.clone(),
                )
            }
            ActivePathContext::Leased {
                boot_id,
                bucket,
                shard,
            } => {
                let p = steadq_names::parse_leased(name)
                    .map_err(|_| Error::QueueCorrupt("invalid leased filename".into()))?;
                if !p.authenticate_tag(self.format.queue_id(), boot_id, bucket, shard) {
                    return Err(Error::QueueCorrupt("name tag mismatch".into()));
                }
                let expected_bucket = steadq_math::lease_bucket(
                    p.boottime_deadline_ns,
                    self.format.lease_bucket_width_ns(),
                )
                .ok_or_else(|| Error::QueueCorrupt("invalid lease bucket width".into()))?;
                let expected_bucket_str = steadq_names::bucket_hex(expected_bucket);
                if expected_bucket_str != *bucket {
                    return Err(Error::QueueCorrupt(format!(
                        "leased bucket mismatch: path {bucket} != expected {expected_bucket_str}"
                    )));
                }
                (
                    p.common.job_id,
                    p.common.generation,
                    p.common.attempt,
                    p.common.maximum_attempts,
                    p.tag,
                    p.tag,
                    shard.clone(),
                )
            }
            ActivePathContext::Delayed { bucket, shard } => {
                let p = steadq_names::parse_delayed(name)
                    .map_err(|_| Error::QueueCorrupt("invalid delayed filename".into()))?;
                if !p.authenticate_tag(self.format.queue_id(), bucket, shard) {
                    return Err(Error::QueueCorrupt("name tag mismatch".into()));
                }
                let expected_bucket = steadq_math::ceiling_bucket(
                    p.not_before_ns,
                    self.format.delayed_bucket_width_ns(),
                )
                .ok_or_else(|| Error::QueueCorrupt("invalid delayed bucket width".into()))?;
                let expected_bucket_str = steadq_names::bucket_hex(expected_bucket);
                if expected_bucket_str != *bucket {
                    return Err(Error::QueueCorrupt(format!(
                        "delayed bucket mismatch: path {bucket} != expected {expected_bucket_str}"
                    )));
                }
                (
                    p.common.job_id,
                    p.common.generation,
                    p.common.attempt,
                    p.common.maximum_attempts,
                    p.tag,
                    p.tag,
                    shard.clone(),
                )
            }
        };

        // Verify header matches filename
        if header.job_id != job_id {
            return Err(Error::QueueCorrupt(
                "header job_id does not match filename".into(),
            ));
        }
        if header.maximum_attempts != max_att {
            return Err(Error::QueueCorrupt(
                "header maximum_attempts does not match filename".into(),
            ));
        }

        if parsed_tag != expected_tag {
            return Err(Error::QueueCorrupt("name tag mismatch".into()));
        }

        // Verify shard placement
        let computed_shard =
            compute_shard(self.format.queue_id(), &job_id, self.format.shard_count());
        let path_shard = steadq_names::shard_from_hex(&path_shard_str)
            .ok_or_else(|| Error::QueueCorrupt(format!("invalid shard hex: {path_shard_str}")))?;
        if path_shard != computed_shard {
            return Err(Error::QueueCorrupt(format!(
                "shard mismatch: path {path_shard} != computed {computed_shard}"
            )));
        }

        Ok(header.clone())
    }

    /// C-23: Bounded duplicate-ack check.
    /// Constructs at most the finite set of exact retained receipt paths
    /// and checks them via fstatat, not by listing receipt contents.
    /// P0-04: Authenticate a receipt at a specific path.
    fn receipt_is_authentic(&self, lease: &LeaseInfo, dir: &str, name: &str) -> bool {
        let new_generation = match lease.generation.checked_add(1) {
            Some(generation) => generation,
            None => return false,
        };
        let expected = verified::ExpectedReceipt {
            common: CommonFields {
                job_id: lease.job_id,
                generation: new_generation,
                attempt: lease.attempt,
                maximum_attempts: lease.maximum_attempts,
            },
            token: lease.token,
            envelope_digest: lease.envelope_digest,
            payload_length: lease.payload_length,
        };
        let dir_fd = match open_relative(self.root_fd.as_fd(), dir) {
            Ok(fd) => fd,
            Err(_) => return false,
        };
        let parts: Vec<&str> = dir.split('/').collect();
        let (bucket, shard_hex) = match parts.len() {
            3 => (parts[1], parts[2]),
            _ => return false,
        };
        let file_fd = match fs::openat(dir_fd.as_fd(), name, verified::receipt_read_open_flags(), 0)
        {
            Ok(f) => f,
            Err(_) => return false,
        };
        verified::verify_receipt_on_fd(
            file_fd.as_fd(),
            verified::ReceiptContext {
                queue_id: self.format.queue_id(),
                shard_count: self.format.shard_count(),
                terminal_bucket_width_ns: self.format.terminal_bucket_width_ns(),
                max_payload_length: self.format.max_payload_length(),
                bucket,
                shard: shard_hex,
                filename: name,
            },
            Some(&expected),
        )
        .is_ok()
    }

    fn check_duplicate_ack_bounded(&self, lease: &LeaseInfo, wall_floor: WallFloor) -> bool {
        let retention = self.options.receipt_retention_ns;
        let width = self.format.terminal_bucket_width_ns();
        let now_bucket = match steadq_math::bucket_number(wall_floor.unix_ns(), width) {
            Some(bucket) => bucket,
            None => return false,
        };
        let retention_buckets = match steadq_math::ceiling_bucket(retention, width) {
            Some(buckets) => buckets,
            None => return false,
        };
        let min_bucket = now_bucket.saturating_sub(retention_buckets + 2);
        let shard = compute_shard(
            self.format.queue_id(),
            &lease.job_id,
            self.format.shard_count(),
        );
        let shard_str = shard_hex(shard);
        let new_generation = match lease.generation.checked_add(1) {
            Some(generation) => generation,
            None => return false,
        };
        let receipt_common = CommonFields {
            job_id: lease.job_id,
            generation: new_generation,
            attempt: lease.attempt,
            maximum_attempts: lease.maximum_attempts,
        };
        let expected = verified::ExpectedReceipt {
            common: receipt_common.clone(),
            token: lease.token,
            envelope_digest: lease.envelope_digest,
            payload_length: lease.payload_length,
        };
        for bucket_num in min_bucket..=now_bucket {
            let bucket_str = bucket_hex(bucket_num);
            let receipt_name = steadq_names::make_receipt_name(
                self.format.queue_id(),
                &bucket_str,
                &shard_str,
                &receipt_common,
                &lease.token,
            );
            let receipt_dir = format!("receipts/{bucket_str}/{shard_str}");
            if let Ok(dir_fd) = open_relative(self.root_fd.as_fd(), &receipt_dir) {
                if let Ok(file_fd) = fs::openat(
                    dir_fd.as_fd(),
                    &receipt_name,
                    verified::receipt_read_open_flags(),
                    0,
                ) {
                    if verified::verify_receipt_on_fd(
                        file_fd.as_fd(),
                        verified::ReceiptContext {
                            queue_id: self.format.queue_id(),
                            shard_count: self.format.shard_count(),
                            terminal_bucket_width_ns: self.format.terminal_bucket_width_ns(),
                            max_payload_length: self.format.max_payload_length(),
                            bucket: &bucket_str,
                            shard: &shard_str,
                            filename: &receipt_name,
                        },
                        Some(&expected),
                    )
                    .is_ok()
                    {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Resolve an indeterminate operation by probing exact paths.
    /// R2-B03: Resolve an indeterminate operation by authenticating objects.
    /// Validates source/destination by opening them, reading headers, and
    /// comparing job_id and generation against the ticket.
    /// Helper: verify shard placement from a shard hex string.
    fn verify_shard_placement(&self, shard_hex: &str, job_id: &[u8; 16]) -> bool {
        let computed = compute_shard(self.format.queue_id(), job_id, self.format.shard_count());
        match steadq_names::shard_from_hex(shard_hex) {
            Some(s) => s == computed,
            None => false,
        }
    }
}

/// Open a relative path from a directory fd.
pub(crate) fn open_relative(root_fd: BorrowedFd<'_>, relative: &str) -> io::Result<OwnedFd> {
    let relative = fs::ValidatedRelativePath::new(relative)?;
    let mut current = None::<OwnedFd>;
    for component in relative.components() {
        let parent_fd = current
            .as_ref()
            .map_or(root_fd, |directory| directory.as_fd());
        current = Some(fs::open_directory(parent_fd, component)?);
    }
    current.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "empty relative path"))
}

/// Input for an enqueue operation.
#[derive(Clone, Debug, Default)]
pub struct EnqueueInput {
    pub maximum_attempts: u32,
    pub content_type: String,
    pub metadata: std::collections::BTreeMap<String, steadq_format::cbor::MetadataValue>,
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
    fn classify_tmpfile(failure: engine::TmpfilePublishFailure) -> Self {
        match failure {
            engine::TmpfilePublishFailure::AlreadyExists => {
                PublishError::NotCommitted(Error::IdentityCollision)
            }
            engine::TmpfilePublishFailure::NotCommitted { phase, source } => {
                match source.raw_os_error() {
                    Some(libc::ENOSPC) | Some(libc::EDQUOT) => {
                        PublishError::NotCommitted(Error::ResourceExhausted)
                    }
                    _ => PublishError::NotCommitted(Error::IoFailure(format!(
                        "temporary-file publication failed at {phase:?}: {source}"
                    ))),
                }
            }
            engine::TmpfilePublishFailure::OutcomeUnknown { phase, source } => {
                PublishError::OutcomeUnknown(Error::IoFailure(format!(
                    "temporary-file publication failed at {phase:?}: {source}"
                )))
            }
        }
    }

    fn classify_move(failure: engine::MoveFailureWith<io::Error>) -> Self {
        match failure {
            engine::MoveFailureWith::AlreadyExists => {
                PublishError::NotCommitted(Error::IdentityCollision)
            }
            engine::MoveFailureWith::SourceMissing => PublishError::NotCommitted(Error::IoFailure(
                "temporary publication source missing".into(),
            )),
            engine::MoveFailureWith::NotCommitted { source, .. } => Self::classify_write(source),
            engine::MoveFailureWith::OutcomeUnknown { source, .. } => {
                PublishError::OutcomeUnknown(Error::IoFailure(source.to_string()))
            }
        }
    }

    fn classify_write(e: io::Error) -> Self {
        match e.raw_os_error() {
            Some(libc::ENOSPC) | Some(libc::EDQUOT) => {
                PublishError::NotCommitted(Error::ResourceExhausted)
            }
            _ => PublishError::NotCommitted(Error::IoFailure(e.to_string())),
        }
    }

    /// Classify a file fsync failure that occurs BEFORE the linearizing
    /// link/rename. Per spec section 7.8, this is NotCommitted.
    fn classify_pre_pub_fsync(e: io::Error) -> Self {
        PublishError::NotCommitted(Error::IoFailure(e.to_string()))
    }
}

#[cfg(test)]
mod tests;
