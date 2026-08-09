// Phase-aware durable move engine: single source for rename+fsync with actor attribution.
//
// Every state transition linearizes via RENAME_NOREPLACE and then syncs each
// distinct affected directory. Errors before the rename are retryable
// (NotCommitted); later errors are OutcomeUnknown.

use std::os::fd::{AsRawFd, BorrowedFd};

use steadq_fs_linux as fs;

use crate::errors::Error;

/// Actor attribution for poisoning decisions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MoveActor {
    Producer,
    Consumer,
    Recovery,
}

/// Phase where the move failed. Determines whether the effect is known durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MovePhase {
    EnsureDest,
    PreRename,
    Rename,
    DestinationIdentity,
    PostLinearization,
    DestFsync,
    SourceFsync,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MoveIdentity {
    device: u64,
    inode: u64,
}

impl MoveIdentity {
    pub fn new(device: u64, inode: u64) -> Self {
        Self { device, inode }
    }

    fn matches(self, stat: &libc::stat) -> bool {
        stat.st_mode & libc::S_IFMT == libc::S_IFREG
            && stat.st_nlink == 1
            && stat.st_size >= 0
            && stat.st_dev == self.device
            && stat.st_ino == self.inode
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MovedObject {
    device: u64,
    inode: u64,
    size: u64,
}

impl MovedObject {
    pub fn device(self) -> u64 {
        self.device
    }

    pub fn inode(self) -> u64 {
        self.inode
    }

    pub fn size(self) -> u64 {
        self.size
    }
}

#[derive(Debug, Clone)]
pub enum MoveFailure {
    /// The rename did not happen. The destination still needs provisioning
    /// or the source vanished before linearizing.
    NotCommitted { phase: MovePhase, source: String },
    /// The rename happened but the durability barrier failed.
    /// The queue must be poisoned and the caller must surface OutcomeUnknown.
    OutcomeUnknown { phase: MovePhase, source: String },
    /// The rename failed with EEXIST because the destination already exists.
    /// For ack-style exact-source moves this is LeaseLost under verified handles;
    /// for publication it is a retriable not-committed without poison.
    AlreadyExists,
    /// The exact source was missing at rename time.
    SourceMissing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnlinkPhase {
    Unlink,
    DirectoryFsync,
}

#[derive(Clone, Debug)]
pub enum UnlinkFailure {
    NotCommitted { phase: UnlinkPhase, source: String },
    OutcomeUnknown { phase: UnlinkPhase, source: String },
    SourceMissing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoveDirectoryPhase {
    Remove,
    ParentFsync,
}

#[derive(Clone, Debug)]
pub enum RemoveDirectoryFailure {
    NotCommitted {
        phase: RemoveDirectoryPhase,
        source: String,
    },
    OutcomeUnknown {
        phase: RemoveDirectoryPhase,
        source: String,
    },
    SourceMissing,
    NotEmpty,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplacePhase {
    DestinationIdentity,
    Rename,
    DirectoryIdentity,
    DestinationFsync,
    SourceFsync,
}

#[derive(Clone, Debug)]
pub enum ReplaceFailure {
    NotCommitted { phase: ReplacePhase, source: String },
    OutcomeUnknown { phase: ReplacePhase, source: String },
    SourceMissing,
    DestinationChanged,
}

impl ReplaceFailure {
    pub fn phase(&self) -> Option<ReplacePhase> {
        match self {
            Self::NotCommitted { phase, .. } | Self::OutcomeUnknown { phase, .. } => Some(*phase),
            Self::SourceMissing | Self::DestinationChanged => None,
        }
    }

    pub fn is_outcome_unknown(&self) -> bool {
        matches!(self, Self::OutcomeUnknown { .. })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplaceIdentity {
    device: u64,
    inode: u64,
}

impl ReplaceIdentity {
    pub fn new(device: u64, inode: u64) -> Self {
        Self { device, inode }
    }

    fn matches(self, stat: &libc::stat) -> bool {
        stat.st_dev == self.device && stat.st_ino == self.inode
    }
}

impl UnlinkFailure {
    pub fn phase(&self) -> Option<UnlinkPhase> {
        match self {
            Self::NotCommitted { phase, .. } | Self::OutcomeUnknown { phase, .. } => Some(*phase),
            Self::SourceMissing => None,
        }
    }

    pub fn is_outcome_unknown(&self) -> bool {
        matches!(self, Self::OutcomeUnknown { .. })
    }
}

impl RemoveDirectoryFailure {
    pub fn phase(&self) -> Option<RemoveDirectoryPhase> {
        match self {
            Self::NotCommitted { phase, .. } | Self::OutcomeUnknown { phase, .. } => Some(*phase),
            Self::SourceMissing | Self::NotEmpty => None,
        }
    }

    pub fn is_outcome_unknown(&self) -> bool {
        matches!(self, Self::OutcomeUnknown { .. })
    }
}

impl MoveFailure {
    pub fn is_outcome_unknown(&self) -> bool {
        matches!(self, Self::OutcomeUnknown { .. })
    }
    pub fn is_not_committed(&self) -> bool {
        matches!(self, Self::NotCommitted { .. })
    }
    pub fn phase(&self) -> Option<MovePhase> {
        match self {
            Self::NotCommitted { phase, .. } | Self::OutcomeUnknown { phase, .. } => Some(*phase),
            _ => None,
        }
    }
}

/// Durable move via RENAME_NOREPLACE with phase-aware error classification.
/// The caller provides already-opened dir fds for src and dest to avoid TOCTOU.
/// On success both dirs are fsynced before returning.
pub fn is_already_exists_io_kind(kind: std::io::ErrorKind) -> bool {
    kind == std::io::ErrorKind::AlreadyExists
}
pub fn is_not_found_io_kind(kind: std::io::ErrorKind) -> bool {
    kind == std::io::ErrorKind::NotFound
}

pub fn move_verified_noreplace(
    src_dir_fd: BorrowedFd<'_>,
    src_name: &str,
    dest_dir_fd: BorrowedFd<'_>,
    dest_name: &str,
    _actor: MoveActor,
) -> Result<(), MoveFailure> {
    move_noreplace(
        src_dir_fd,
        src_name,
        dest_dir_fd,
        dest_name,
        None,
        false,
        |_| Ok(()),
    )
}

pub fn move_witnessed_noreplace(
    src_dir_fd: BorrowedFd<'_>,
    src_name: &str,
    dest_dir_fd: BorrowedFd<'_>,
    dest_name: &str,
    source_identity: MoveIdentity,
    _actor: MoveActor,
) -> Result<(), MoveFailure> {
    move_witnessed_noreplace_with(
        src_dir_fd,
        src_name,
        dest_dir_fd,
        dest_name,
        source_identity,
        _actor,
        |_| Ok(()),
    )
    .map(|_| ())
}

pub fn move_witnessed_noreplace_with<T>(
    src_dir_fd: BorrowedFd<'_>,
    src_name: &str,
    dest_dir_fd: BorrowedFd<'_>,
    dest_name: &str,
    source_identity: MoveIdentity,
    _actor: MoveActor,
    after_linearization: impl FnOnce(MovedObject) -> Result<T, String>,
) -> Result<(MovedObject, T), MoveFailure> {
    move_noreplace(
        src_dir_fd,
        src_name,
        dest_dir_fd,
        dest_name,
        Some(source_identity),
        true,
        move |moved| {
            let moved = moved.expect("witnessed move authenticates its destination");
            after_linearization(moved).map(|output| (moved, output))
        },
    )
}

fn move_noreplace<T>(
    src_dir_fd: BorrowedFd<'_>,
    src_name: &str,
    dest_dir_fd: BorrowedFd<'_>,
    dest_name: &str,
    source_identity: Option<MoveIdentity>,
    detect_same_directory: bool,
    after_linearization: impl FnOnce(Option<MovedObject>) -> Result<T, String>,
) -> Result<T, MoveFailure> {
    match fs::renameat2_noreplace(src_dir_fd, src_name, dest_dir_fd, dest_name) {
        Ok(()) => {}
        Err(e) if is_already_exists_io_kind(e.kind()) => {
            return Err(MoveFailure::AlreadyExists);
        }
        Err(e) if is_not_found_io_kind(e.kind()) => {
            return Err(MoveFailure::SourceMissing);
        }
        Err(e) => {
            return Err(MoveFailure::NotCommitted {
                phase: MovePhase::Rename,
                source: e.to_string(),
            });
        }
    };

    let moved = if let Some(source_identity) = source_identity {
        match fs::fstatat(dest_dir_fd, dest_name) {
            Ok(stat) if source_identity.matches(&stat) => Some(MovedObject {
                device: stat.st_dev,
                inode: stat.st_ino,
                size: stat.st_size as u64,
            }),
            Ok(_) => {
                return Err(MoveFailure::OutcomeUnknown {
                    phase: MovePhase::DestinationIdentity,
                    source: "destination identity changed after rename".into(),
                });
            }
            Err(error) => {
                return Err(MoveFailure::OutcomeUnknown {
                    phase: MovePhase::DestinationIdentity,
                    source: error.to_string(),
                });
            }
        }
    } else {
        None
    };

    let output = after_linearization(moved).map_err(|source| MoveFailure::OutcomeUnknown {
        phase: MovePhase::PostLinearization,
        source,
    })?;

    if let Err(e) = fs::fsync_dir_fd(dest_dir_fd) {
        return Err(MoveFailure::OutcomeUnknown {
            phase: MovePhase::DestFsync,
            source: e.to_string(),
        });
    }

    if detect_same_directory && same_directory(src_dir_fd, dest_dir_fd) {
        return Ok(output);
    }

    if let Err(e) = fs::fsync_dir_fd(src_dir_fd) {
        return Err(MoveFailure::OutcomeUnknown {
            phase: MovePhase::SourceFsync,
            source: e.to_string(),
        });
    }
    Ok(output)
}

fn same_directory(source: BorrowedFd<'_>, destination: BorrowedFd<'_>) -> bool {
    if source.as_raw_fd() == destination.as_raw_fd() {
        return true;
    }
    match (fs::fstat(source), fs::fstat(destination)) {
        (Ok(source), Ok(destination)) => {
            source.st_dev == destination.st_dev && source.st_ino == destination.st_ino
        }
        _ => false,
    }
}

pub fn unlink_verified(
    directory_fd: BorrowedFd<'_>,
    name: &str,
    _actor: MoveActor,
) -> Result<(), UnlinkFailure> {
    match fs::unlinkat(directory_fd, name) {
        Ok(()) => {}
        Err(error) if is_not_found_io_kind(error.kind()) => {
            return Err(UnlinkFailure::SourceMissing);
        }
        Err(error) => {
            return Err(UnlinkFailure::NotCommitted {
                phase: UnlinkPhase::Unlink,
                source: error.to_string(),
            });
        }
    }
    fs::fsync_dir_fd(directory_fd).map_err(|error| UnlinkFailure::OutcomeUnknown {
        phase: UnlinkPhase::DirectoryFsync,
        source: error.to_string(),
    })
}

fn is_directory_not_empty(error: &std::io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::ENOTEMPTY) | Some(libc::EEXIST)
    )
}

pub fn remove_empty_directory_verified(
    parent_directory_fd: BorrowedFd<'_>,
    name: &str,
    _actor: MoveActor,
) -> Result<(), RemoveDirectoryFailure> {
    match fs::unlinkat_dir(parent_directory_fd, name) {
        Ok(()) => {}
        Err(error) if is_not_found_io_kind(error.kind()) => {
            return Err(RemoveDirectoryFailure::SourceMissing);
        }
        Err(error) if is_directory_not_empty(&error) => {
            return Err(RemoveDirectoryFailure::NotEmpty);
        }
        Err(error) => {
            return Err(RemoveDirectoryFailure::NotCommitted {
                phase: RemoveDirectoryPhase::Remove,
                source: error.to_string(),
            });
        }
    }
    fs::fsync_dir_fd(parent_directory_fd).map_err(|error| RemoveDirectoryFailure::OutcomeUnknown {
        phase: RemoveDirectoryPhase::ParentFsync,
        source: error.to_string(),
    })
}

/// Atomically replace an authenticated destination and durably publish the
/// replacement. The rename is the linearization point, so every later failure
/// is outcome unknown.
pub fn replace_verified(
    src_dir_fd: BorrowedFd<'_>,
    src_name: &str,
    dest_dir_fd: BorrowedFd<'_>,
    dest_name: &str,
    expected_destination: Option<ReplaceIdentity>,
    _actor: MoveActor,
) -> Result<(), ReplaceFailure> {
    if let Some(expected_destination) = expected_destination {
        let destination =
            fs::fstatat(dest_dir_fd, dest_name).map_err(|error| ReplaceFailure::NotCommitted {
                phase: ReplacePhase::DestinationIdentity,
                source: error.to_string(),
            })?;
        if !expected_destination.matches(&destination) {
            return Err(ReplaceFailure::DestinationChanged);
        }
    }

    match fs::renameat(src_dir_fd, src_name, dest_dir_fd, dest_name) {
        Ok(()) => {}
        Err(error) if is_not_found_io_kind(error.kind()) => {
            return Err(ReplaceFailure::SourceMissing);
        }
        Err(error) => {
            return Err(ReplaceFailure::NotCommitted {
                phase: ReplacePhase::Rename,
                source: error.to_string(),
            });
        }
    }

    if src_dir_fd.as_raw_fd() == dest_dir_fd.as_raw_fd() {
        return fs::fsync_dir_fd(dest_dir_fd).map_err(|error| ReplaceFailure::OutcomeUnknown {
            phase: ReplacePhase::DestinationFsync,
            source: error.to_string(),
        });
    }

    let src_stat = fs::fstat(src_dir_fd).map_err(|error| ReplaceFailure::OutcomeUnknown {
        phase: ReplacePhase::DirectoryIdentity,
        source: error.to_string(),
    })?;
    let dest_stat = fs::fstat(dest_dir_fd).map_err(|error| ReplaceFailure::OutcomeUnknown {
        phase: ReplacePhase::DirectoryIdentity,
        source: error.to_string(),
    })?;
    if src_stat.st_dev == dest_stat.st_dev && src_stat.st_ino == dest_stat.st_ino {
        return fs::fsync_dir_fd(dest_dir_fd).map_err(|error| ReplaceFailure::OutcomeUnknown {
            phase: ReplacePhase::DestinationFsync,
            source: error.to_string(),
        });
    }

    fs::fsync_dir_fd(dest_dir_fd).map_err(|error| ReplaceFailure::OutcomeUnknown {
        phase: ReplacePhase::DestinationFsync,
        source: error.to_string(),
    })?;
    fs::fsync_dir_fd(src_dir_fd).map_err(|error| ReplaceFailure::OutcomeUnknown {
        phase: ReplacePhase::SourceFsync,
        source: error.to_string(),
    })
}

/// Convert a MoveFailure into the public Error / poison decision.
/// The caller decides poison; this helper maps phases to Error variants.
pub fn map_move_failure(f: MoveFailure) -> Error {
    match f {
        MoveFailure::AlreadyExists => Error::QueueCorrupt("destination already exists".into()),
        MoveFailure::SourceMissing => Error::QueueCorrupt("source missing".into()),
        MoveFailure::NotCommitted { source, .. } => Error::IoFailure(source),
        MoveFailure::OutcomeUnknown { source, .. } => Error::IoFailure(source),
    }
}

// helpers for mutant killing
pub fn is_already_exists(f: &MoveFailure) -> bool {
    matches!(f, MoveFailure::AlreadyExists)
}
pub fn is_source_missing(f: &MoveFailure) -> bool {
    matches!(f, MoveFailure::SourceMissing)
}
pub fn is_outcome_unknown_phase(phase: MovePhase) -> bool {
    matches!(
        phase,
        MovePhase::DestinationIdentity
            | MovePhase::PostLinearization
            | MovePhase::DestFsync
            | MovePhase::SourceFsync
    )
}
pub fn is_not_committed_phase(phase: MovePhase) -> bool {
    matches!(
        phase,
        MovePhase::EnsureDest | MovePhase::PreRename | MovePhase::Rename
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsFd;

    #[test]
    fn is_already_exists_table() {
        assert!(is_already_exists(&MoveFailure::AlreadyExists));
        assert!(!is_already_exists(&MoveFailure::SourceMissing));
        assert!(!is_already_exists(&MoveFailure::NotCommitted {
            phase: MovePhase::Rename,
            source: "x".into()
        }));
        assert!(!is_already_exists(&MoveFailure::OutcomeUnknown {
            phase: MovePhase::DestFsync,
            source: "y".into()
        }));
    }

    #[test]
    fn is_source_missing_table() {
        assert!(is_source_missing(&MoveFailure::SourceMissing));
        assert!(!is_source_missing(&MoveFailure::AlreadyExists));
        assert!(!is_source_missing(&MoveFailure::NotCommitted {
            phase: MovePhase::PreRename,
            source: "z".into()
        }));
    }

    #[test]
    fn is_outcome_unknown_phase_table() {
        assert!(is_outcome_unknown_phase(MovePhase::DestinationIdentity));
        assert!(is_outcome_unknown_phase(MovePhase::PostLinearization));
        assert!(is_outcome_unknown_phase(MovePhase::DestFsync));
        assert!(is_outcome_unknown_phase(MovePhase::SourceFsync));
        assert!(!is_outcome_unknown_phase(MovePhase::Rename));
        assert!(!is_outcome_unknown_phase(MovePhase::EnsureDest));
        assert!(!is_outcome_unknown_phase(MovePhase::PreRename));
    }

    #[test]
    fn is_not_committed_phase_table() {
        assert!(is_not_committed_phase(MovePhase::EnsureDest));
        assert!(is_not_committed_phase(MovePhase::PreRename));
        assert!(is_not_committed_phase(MovePhase::Rename));
        assert!(!is_not_committed_phase(MovePhase::DestinationIdentity));
        assert!(!is_not_committed_phase(MovePhase::PostLinearization));
        assert!(!is_not_committed_phase(MovePhase::DestFsync));
        assert!(!is_not_committed_phase(MovePhase::SourceFsync));
    }

    #[test]
    fn move_failure_phase_extraction() {
        let f = MoveFailure::NotCommitted {
            phase: MovePhase::Rename,
            source: "a".into(),
        };
        assert_eq!(f.phase(), Some(MovePhase::Rename));
        assert!(f.is_not_committed());
        assert!(!f.is_outcome_unknown());

        let g = MoveFailure::OutcomeUnknown {
            phase: MovePhase::SourceFsync,
            source: "b".into(),
        };
        assert_eq!(g.phase(), Some(MovePhase::SourceFsync));
        assert!(g.is_outcome_unknown());
        assert!(!g.is_not_committed());

        assert_eq!(MoveFailure::AlreadyExists.phase(), None);
        assert_eq!(MoveFailure::SourceMissing.phase(), None);
    }

    #[test]
    fn map_move_failure_covers_variants() {
        let e = map_move_failure(MoveFailure::AlreadyExists);
        assert!(matches!(e, Error::QueueCorrupt(_)));
        let e = map_move_failure(MoveFailure::SourceMissing);
        assert!(matches!(e, Error::QueueCorrupt(_)));
        let e = map_move_failure(MoveFailure::NotCommitted {
            phase: MovePhase::Rename,
            source: "io".into(),
        });
        assert!(matches!(e, Error::IoFailure(_)));
        let e = map_move_failure(MoveFailure::OutcomeUnknown {
            phase: MovePhase::DestFsync,
            source: "fsync".into(),
        });
        assert!(matches!(e, Error::IoFailure(_)));
    }

    #[test]
    fn is_already_exists_io_kind_table() {
        assert!(is_already_exists_io_kind(std::io::ErrorKind::AlreadyExists));
        assert!(!is_already_exists_io_kind(std::io::ErrorKind::NotFound));
        assert!(!is_already_exists_io_kind(
            std::io::ErrorKind::PermissionDenied
        ));
        assert!(!is_already_exists_io_kind(std::io::ErrorKind::Other));
        assert!(!is_already_exists_io_kind(std::io::ErrorKind::Interrupted));
    }

    #[test]
    fn is_not_found_io_kind_table() {
        assert!(is_not_found_io_kind(std::io::ErrorKind::NotFound));
        assert!(!is_not_found_io_kind(std::io::ErrorKind::AlreadyExists));
        assert!(!is_not_found_io_kind(std::io::ErrorKind::PermissionDenied));
        assert!(!is_not_found_io_kind(std::io::ErrorKind::Other));
        assert!(!is_not_found_io_kind(std::io::ErrorKind::Interrupted));
    }

    #[test]
    fn move_verified_noreplace_bad_fd_is_not_committed() {
        // EBADF should map to NotCommitted, not SourceMissing, to kill the
        // match guard mutant that replaces is_not_found_io_kind with true.
        let dir = tempfile::tempdir().unwrap();
        let dest_dir = tempfile::tempdir_in(dir.path()).unwrap();
        let dest_fd = std::fs::OpenOptions::new()
            .read(true)
            .open(dest_dir.path())
            .unwrap();
        fs::fault::reset();
        fs::fault::inject_errno("renameat2_noreplace", 1, libc::EIO);
        let r = move_verified_noreplace(
            dest_fd.as_fd(),
            "nope.raw",
            dest_fd.as_fd(),
            "dest.raw",
            MoveActor::Recovery,
        );
        fs::fault::reset();
        assert!(matches!(
            &r,
            Err(MoveFailure::NotCommitted {
                phase: MovePhase::Rename,
                ..
            })
        ));
        assert!(r.clone().unwrap_err().is_not_committed());
        assert!(!matches!(&r, Ok(())));
        // Ensure it is not misclassified as SourceMissing when guard is true
        assert!(!matches!(&r, Err(MoveFailure::SourceMissing)));
    }

    #[test]
    fn durable_move_round_trip_tmpdir() {
        let dir = tempfile::tempdir().unwrap();
        let src_dir = tempfile::tempdir_in(dir.path()).unwrap();
        let dest_dir = tempfile::tempdir_in(dir.path()).unwrap();
        let src_path = src_dir.path().join("src.raw");
        std::fs::write(&src_path, b"hello").unwrap();
        let src_fd = std::fs::OpenOptions::new()
            .read(true)
            .open(src_dir.path())
            .unwrap();
        let dest_fd = std::fs::OpenOptions::new()
            .read(true)
            .open(dest_dir.path())
            .unwrap();
        let r = move_verified_noreplace(
            src_fd.as_fd(),
            "src.raw",
            dest_fd.as_fd(),
            "dest.raw",
            MoveActor::Recovery,
        );
        assert!(r.is_ok());
        assert!(dest_dir.path().join("dest.raw").exists());
        assert!(!src_dir.path().join("src.raw").exists());

        // second move of same source should be SourceMissing
        let r2 = move_verified_noreplace(
            src_fd.as_fd(),
            "src.raw",
            dest_fd.as_fd(),
            "dest2.raw",
            MoveActor::Recovery,
        );
        assert!(matches!(r2, Err(MoveFailure::SourceMissing)));

        // recreate source and try to overwrite existing dest
        std::fs::write(src_dir.path().join("src.raw"), b"again").unwrap();
        let r3 = move_verified_noreplace(
            src_fd.as_fd(),
            "src.raw",
            dest_fd.as_fd(),
            "dest.raw",
            MoveActor::Recovery,
        );
        assert!(matches!(r3, Err(MoveFailure::AlreadyExists)));
    }

    #[test]
    fn move_identity_requires_exact_singly_linked_regular_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.raw");
        std::fs::write(&path, b"source").unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let stat = fs::fstat(file.as_fd()).unwrap();
        let identity = MoveIdentity::new(stat.st_dev, stat.st_ino);

        assert!(identity.matches(&stat));

        let mut wrong_type = stat;
        wrong_type.st_mode = libc::S_IFDIR | 0o700;
        assert!(!identity.matches(&wrong_type));

        let mut wrong_link_count = stat;
        wrong_link_count.st_nlink = 2;
        assert!(!identity.matches(&wrong_link_count));

        let mut negative_size = stat;
        negative_size.st_size = -1;
        assert!(!identity.matches(&negative_size));

        let mut wrong_device = stat;
        wrong_device.st_dev = wrong_device.st_dev.wrapping_add(1);
        assert!(!identity.matches(&wrong_device));

        let mut wrong_inode = stat;
        wrong_inode.st_ino = wrong_inode.st_ino.wrapping_add(1);
        assert!(!identity.matches(&wrong_inode));
    }

    #[test]
    fn witnessed_move_preserves_every_failure_phase() {
        for (fault, fault_count, expected_phase, source_remains) in [
            ("renameat2_noreplace", 1, MovePhase::Rename, true),
            ("fstatat", 1, MovePhase::DestinationIdentity, false),
            ("fsync_dir_fd", 1, MovePhase::DestFsync, false),
            ("fsync_dir_fd", 2, MovePhase::SourceFsync, false),
        ] {
            let root = tempfile::tempdir().unwrap();
            let source_dir = root.path().join("source");
            let destination_dir = root.path().join("destination");
            std::fs::create_dir(&source_dir).unwrap();
            std::fs::create_dir(&destination_dir).unwrap();
            std::fs::write(source_dir.join("source.raw"), b"source").unwrap();
            let source_fd = std::fs::File::open(&source_dir).unwrap();
            let destination_fd = std::fs::File::open(&destination_dir).unwrap();
            let source_stat = fs::fstatat(source_fd.as_fd(), "source.raw").unwrap();

            fs::fault::reset();
            fs::fault::inject_errno(fault, fault_count, libc::EIO);
            let failure = move_witnessed_noreplace(
                source_fd.as_fd(),
                "source.raw",
                destination_fd.as_fd(),
                "destination.raw",
                MoveIdentity::new(source_stat.st_dev, source_stat.st_ino),
                MoveActor::Consumer,
            )
            .unwrap_err();
            fs::fault::reset();

            assert_eq!(failure.phase(), Some(expected_phase));
            assert_eq!(failure.is_outcome_unknown(), !source_remains);
            assert_eq!(source_dir.join("source.raw").exists(), source_remains);
            assert_eq!(
                destination_dir.join("destination.raw").exists(),
                !source_remains
            );
        }
    }

    #[test]
    fn witnessed_move_rejects_the_wrong_destination_identity() {
        let root = tempfile::tempdir().unwrap();
        let source_dir = root.path().join("source");
        let destination_dir = root.path().join("destination");
        std::fs::create_dir(&source_dir).unwrap();
        std::fs::create_dir(&destination_dir).unwrap();
        std::fs::write(source_dir.join("source.raw"), b"source").unwrap();
        let source_fd = std::fs::File::open(&source_dir).unwrap();
        let destination_fd = std::fs::File::open(&destination_dir).unwrap();
        let source_stat = fs::fstatat(source_fd.as_fd(), "source.raw").unwrap();

        let failure = move_witnessed_noreplace(
            source_fd.as_fd(),
            "source.raw",
            destination_fd.as_fd(),
            "destination.raw",
            MoveIdentity::new(source_stat.st_dev, source_stat.st_ino.wrapping_add(1)),
            MoveActor::Consumer,
        )
        .unwrap_err();

        assert!(matches!(
            failure,
            MoveFailure::OutcomeUnknown {
                phase: MovePhase::DestinationIdentity,
                ..
            }
        ));
        assert!(!source_dir.join("source.raw").exists());
        assert!(destination_dir.join("destination.raw").exists());
    }

    #[test]
    fn witnessed_move_syncs_same_directory_once_across_distinct_fds() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("source.raw"), b"source").unwrap();
        let source_fd = std::fs::File::open(dir.path()).unwrap();
        let destination_fd = std::fs::File::open(dir.path()).unwrap();
        let source_stat = fs::fstatat(source_fd.as_fd(), "source.raw").unwrap();

        fs::fault::reset();
        fs::fault::inject_errno("fsync_dir_fd", 2, libc::EIO);
        move_witnessed_noreplace(
            source_fd.as_fd(),
            "source.raw",
            destination_fd.as_fd(),
            "destination.raw",
            MoveIdentity::new(source_stat.st_dev, source_stat.st_ino),
            MoveActor::Consumer,
        )
        .unwrap();
        assert_eq!(fs::fault::call_count("fsync_dir_fd"), 1);
        fs::fault::reset();
    }

    #[test]
    fn witnessed_move_runs_post_linearization_work_before_barriers() {
        let root = tempfile::tempdir().unwrap();
        let source_dir = root.path().join("source");
        let destination_dir = root.path().join("destination");
        std::fs::create_dir(&source_dir).unwrap();
        std::fs::create_dir(&destination_dir).unwrap();
        let contents = b"source evidence";
        std::fs::write(source_dir.join("source.raw"), contents).unwrap();
        let source_fd = std::fs::File::open(&source_dir).unwrap();
        let destination_fd = std::fs::File::open(&destination_dir).unwrap();
        let source_stat = fs::fstatat(source_fd.as_fd(), "source.raw").unwrap();

        fs::fault::reset();
        let failure = move_witnessed_noreplace_with(
            source_fd.as_fd(),
            "source.raw",
            destination_fd.as_fd(),
            "destination.raw",
            MoveIdentity::new(source_stat.st_dev, source_stat.st_ino),
            MoveActor::Consumer,
            |moved| {
                assert!(!source_dir.join("source.raw").exists());
                assert!(destination_dir.join("destination.raw").exists());
                assert_eq!(moved.device(), source_stat.st_dev);
                assert_eq!(moved.inode(), source_stat.st_ino);
                assert_eq!(moved.size(), contents.len() as u64);
                Err::<(), _>("evidence refresh failed".to_string())
            },
        )
        .unwrap_err();

        assert!(matches!(
            failure,
            MoveFailure::OutcomeUnknown {
                phase: MovePhase::PostLinearization,
                ..
            }
        ));
        assert_eq!(fs::fault::call_count("fsync_dir_fd"), 0);
        fs::fault::reset();
    }

    #[test]
    fn unlink_verified_preserves_linearization_phase() {
        for (fault, expected_phase, outcome_unknown, file_remains) in [
            ("unlinkat", UnlinkPhase::Unlink, false, true),
            ("fsync_dir_fd", UnlinkPhase::DirectoryFsync, true, false),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("object.raw");
            std::fs::write(&path, b"object").unwrap();
            let directory_fd = std::fs::OpenOptions::new()
                .read(true)
                .open(dir.path())
                .unwrap();
            fs::fault::reset();
            fs::fault::inject_errno(fault, 1, libc::EIO);
            let result = unlink_verified(directory_fd.as_fd(), "object.raw", MoveActor::Recovery);
            fs::fault::reset();
            let failure = result.unwrap_err();
            assert_eq!(failure.phase(), Some(expected_phase));
            assert_eq!(failure.is_outcome_unknown(), outcome_unknown);
            assert_eq!(path.exists(), file_remains);
        }
    }

    #[test]
    fn unlink_verified_distinguishes_missing_source_and_io_failure() {
        let dir = tempfile::tempdir().unwrap();
        let directory_fd = std::fs::OpenOptions::new()
            .read(true)
            .open(dir.path())
            .unwrap();
        assert!(matches!(
            unlink_verified(directory_fd.as_fd(), "missing.raw", MoveActor::Recovery),
            Err(UnlinkFailure::SourceMissing)
        ));
        fs::fault::reset();
        fs::fault::inject_errno("unlinkat", 1, libc::EIO);
        assert!(matches!(
            unlink_verified(directory_fd.as_fd(), "missing.raw", MoveActor::Recovery),
            Err(UnlinkFailure::NotCommitted {
                phase: UnlinkPhase::Unlink,
                ..
            })
        ));
        fs::fault::reset();
    }

    #[test]
    fn directory_not_empty_error_classification_is_exact() {
        assert!(is_directory_not_empty(&std::io::Error::from_raw_os_error(
            libc::ENOTEMPTY
        )));
        assert!(is_directory_not_empty(&std::io::Error::from_raw_os_error(
            libc::EEXIST
        )));
        assert!(!is_directory_not_empty(&std::io::Error::from_raw_os_error(
            libc::ENOENT
        )));
        assert!(!is_directory_not_empty(&std::io::Error::from_raw_os_error(
            libc::EIO
        )));
    }

    #[test]
    fn remove_empty_directory_preserves_linearization_phase_and_replays() {
        for (fault, expected_phase, outcome_unknown, directory_remains) in [
            ("unlinkat_dir", RemoveDirectoryPhase::Remove, false, true),
            (
                "fsync_dir_fd",
                RemoveDirectoryPhase::ParentFsync,
                true,
                false,
            ),
        ] {
            let root = tempfile::tempdir().unwrap();
            std::fs::create_dir(root.path().join("empty")).unwrap();
            std::fs::write(root.path().join("sibling"), b"distinct").unwrap();
            let parent = std::fs::OpenOptions::new()
                .read(true)
                .open(root.path())
                .unwrap();
            fs::fault::reset();
            fs::fault::inject_errno(fault, 1, libc::EIO);
            let result =
                remove_empty_directory_verified(parent.as_fd(), "empty", MoveActor::Recovery);
            fs::fault::reset();
            let failure = result.unwrap_err();
            assert_eq!(failure.phase(), Some(expected_phase));
            assert_eq!(failure.is_outcome_unknown(), outcome_unknown);
            assert_eq!(root.path().join("empty").exists(), directory_remains);
            assert_eq!(
                std::fs::read(root.path().join("sibling")).unwrap(),
                b"distinct"
            );

            drop(parent);
            let reopened = std::fs::OpenOptions::new()
                .read(true)
                .open(root.path())
                .unwrap();
            let replay =
                remove_empty_directory_verified(reopened.as_fd(), "empty", MoveActor::Recovery);
            if directory_remains {
                assert!(replay.is_ok());
            } else {
                assert!(matches!(replay, Err(RemoveDirectoryFailure::SourceMissing)));
            }
        }
    }

    #[test]
    fn remove_empty_directory_distinguishes_missing_nonempty_and_io() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("nonempty")).unwrap();
        std::fs::write(root.path().join("nonempty/object"), b"object").unwrap();
        let parent = std::fs::OpenOptions::new()
            .read(true)
            .open(root.path())
            .unwrap();
        assert!(matches!(
            remove_empty_directory_verified(parent.as_fd(), "missing", MoveActor::Recovery),
            Err(RemoveDirectoryFailure::SourceMissing)
        ));
        assert!(matches!(
            remove_empty_directory_verified(parent.as_fd(), "nonempty", MoveActor::Recovery),
            Err(RemoveDirectoryFailure::NotEmpty)
        ));
        fs::fault::reset();
        fs::fault::inject_errno("unlinkat_dir", 1, libc::EIO);
        assert!(matches!(
            remove_empty_directory_verified(parent.as_fd(), "missing", MoveActor::Recovery),
            Err(RemoveDirectoryFailure::NotCommitted {
                phase: RemoveDirectoryPhase::Remove,
                ..
            })
        ));
        fs::fault::reset();
        assert!(root.path().join("nonempty/object").exists());
    }

    #[test]
    fn replace_verified_preserves_linearization_phase() {
        for (fault, expected_phase, outcome_unknown, source_remains) in [
            ("renameat", ReplacePhase::Rename, false, true),
            ("fsync_dir_fd", ReplacePhase::DestinationFsync, true, false),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("replacement.tmp");
            let destination = dir.path().join("receipt.rct");
            std::fs::write(&source, b"new").unwrap();
            std::fs::write(&destination, b"old").unwrap();
            let directory_fd = std::fs::OpenOptions::new()
                .read(true)
                .open(dir.path())
                .unwrap();
            fs::fault::reset();
            fs::fault::inject_errno(fault, 1, libc::EIO);
            let result = replace_verified(
                directory_fd.as_fd(),
                "replacement.tmp",
                directory_fd.as_fd(),
                "receipt.rct",
                None,
                MoveActor::Recovery,
            );
            fs::fault::reset();
            let failure = result.unwrap_err();
            assert_eq!(failure.phase(), Some(expected_phase));
            assert_eq!(failure.is_outcome_unknown(), outcome_unknown);
            assert_eq!(source.exists(), source_remains);
            assert_eq!(
                std::fs::read(destination).unwrap(),
                if source_remains { b"old" } else { b"new" }
            );
        }
    }

    #[test]
    fn replace_verified_distinguishes_missing_source_and_io_failure() {
        let dir = tempfile::tempdir().unwrap();
        let directory_fd = std::fs::OpenOptions::new()
            .read(true)
            .open(dir.path())
            .unwrap();
        assert!(matches!(
            replace_verified(
                directory_fd.as_fd(),
                "missing.tmp",
                directory_fd.as_fd(),
                "receipt.rct",
                None,
                MoveActor::Recovery,
            ),
            Err(ReplaceFailure::SourceMissing)
        ));
        std::fs::write(dir.path().join("source.tmp"), b"new").unwrap();
        fs::fault::reset();
        fs::fault::inject_errno("renameat", 1, libc::EIO);
        assert!(matches!(
            replace_verified(
                directory_fd.as_fd(),
                "source.tmp",
                directory_fd.as_fd(),
                "receipt.rct",
                None,
                MoveActor::Recovery,
            ),
            Err(ReplaceFailure::NotCommitted {
                phase: ReplacePhase::Rename,
                ..
            })
        ));
        fs::fault::reset();
    }

    #[test]
    fn replace_verified_classifies_cross_directory_post_rename_failures() {
        for (fault, fault_count, expected_phase) in [
            ("fstat", 1, ReplacePhase::DirectoryIdentity),
            ("fstat", 2, ReplacePhase::DirectoryIdentity),
            ("fsync_dir_fd", 1, ReplacePhase::DestinationFsync),
            ("fsync_dir_fd", 2, ReplacePhase::SourceFsync),
        ] {
            let root = tempfile::tempdir().unwrap();
            let source_dir = root.path().join("source");
            let destination_dir = root.path().join("destination");
            std::fs::create_dir(&source_dir).unwrap();
            std::fs::create_dir(&destination_dir).unwrap();
            std::fs::write(source_dir.join("replacement.tmp"), b"new").unwrap();
            std::fs::write(destination_dir.join("receipt.rct"), b"old").unwrap();
            let source_fd = std::fs::OpenOptions::new()
                .read(true)
                .open(&source_dir)
                .unwrap();
            let destination_fd = std::fs::OpenOptions::new()
                .read(true)
                .open(&destination_dir)
                .unwrap();

            fs::fault::reset();
            fs::fault::inject_errno(fault, fault_count, libc::EIO);
            let failure = replace_verified(
                source_fd.as_fd(),
                "replacement.tmp",
                destination_fd.as_fd(),
                "receipt.rct",
                None,
                MoveActor::Recovery,
            )
            .unwrap_err();
            fs::fault::reset();

            assert_eq!(failure.phase(), Some(expected_phase));
            assert!(failure.is_outcome_unknown());
            assert!(!source_dir.join("replacement.tmp").exists());
            assert_eq!(
                std::fs::read(destination_dir.join("receipt.rct")).unwrap(),
                b"new"
            );
        }
    }

    #[test]
    fn replace_verified_syncs_one_directory_for_distinct_fds_to_same_directory() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("replacement.tmp"), b"new").unwrap();
        std::fs::write(dir.path().join("receipt.rct"), b"old").unwrap();
        let source_fd = std::fs::OpenOptions::new()
            .read(true)
            .open(dir.path())
            .unwrap();
        let destination_fd = std::fs::OpenOptions::new()
            .read(true)
            .open(dir.path())
            .unwrap();

        fs::fault::reset();
        fs::fault::inject_errno("fsync_dir_fd", 2, libc::EIO);
        replace_verified(
            source_fd.as_fd(),
            "replacement.tmp",
            destination_fd.as_fd(),
            "receipt.rct",
            None,
            MoveActor::Recovery,
        )
        .unwrap();
        assert_eq!(fs::fault::call_count("fsync_dir_fd"), 1);
        fs::fault::reset();
    }

    #[test]
    fn replace_verified_revalidates_destination_immediately_before_rename() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("replacement.tmp");
        let destination = dir.path().join("receipt.rct");
        std::fs::write(&source, b"new").unwrap();
        std::fs::write(&destination, b"old").unwrap();
        let directory_fd = std::fs::OpenOptions::new()
            .read(true)
            .open(dir.path())
            .unwrap();
        let destination_stat = fs::fstatat(directory_fd.as_fd(), "receipt.rct").unwrap();

        let changed = replace_verified(
            directory_fd.as_fd(),
            "replacement.tmp",
            directory_fd.as_fd(),
            "receipt.rct",
            Some(ReplaceIdentity::new(
                destination_stat.st_dev,
                destination_stat.st_ino.wrapping_add(1),
            )),
            MoveActor::Recovery,
        );
        assert!(matches!(changed, Err(ReplaceFailure::DestinationChanged)));
        assert!(source.exists());
        assert_eq!(std::fs::read(&destination).unwrap(), b"old");

        replace_verified(
            directory_fd.as_fd(),
            "replacement.tmp",
            directory_fd.as_fd(),
            "receipt.rct",
            Some(ReplaceIdentity::new(
                destination_stat.st_dev,
                destination_stat.st_ino,
            )),
            MoveActor::Recovery,
        )
        .unwrap();
        assert!(!source.exists());
        assert_eq!(std::fs::read(destination).unwrap(), b"new");
    }
}
