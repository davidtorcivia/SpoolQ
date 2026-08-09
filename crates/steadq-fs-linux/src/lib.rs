// Linux syscall substrate for SteadQ/1.
// Confines all unsafe code to this module.

use std::ffi::CString;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::path::Path;

const MAX_COMPONENT_BYTES: usize = 255;
const MAX_RELATIVE_PATH_BYTES: usize = 4095;
const RESOLVE_NO_MAGICLINKS: u64 = 0x02;
const RESOLVE_NO_SYMLINKS: u64 = 0x04;
const RESOLVE_BENEATH: u64 = 0x08;
const RESOLVER_RESOLVE_FLAGS: u64 = RESOLVE_NO_MAGICLINKS + RESOLVE_NO_SYMLINKS + RESOLVE_BENEATH;

fn resolver_open_flags() -> i32 {
    libc::O_DIRECTORY
        .checked_add(libc::O_CLOEXEC)
        .expect("Linux open flags fit i32")
}

/// A relative path whose components are safe to resolve beneath a directory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidatedRelativePath<'a> {
    path: &'a str,
}

impl<'a> ValidatedRelativePath<'a> {
    pub fn new(path: &'a str) -> io::Result<Self> {
        validate_relative_path(path)
    }

    pub fn as_str(self) -> &'a str {
        self.path
    }

    pub fn components(self) -> impl Iterator<Item = &'a str> {
        self.path.split('/')
    }
}

#[repr(C)]
struct OpenHow {
    flags: u64,
    mode: u64,
    resolve: u64,
}

// ---------- Fault injection (always compiled; idle until armed) ----------
//
// Tests arm faults via fault::inject / inject_errno. Idle threads take a
// TLS path that finds an empty map and returns immediately.

/// Fault injection control for deterministic failure testing.
///
/// State is thread-local so parallel tests do not interfere with each other.
/// Idle threads pay only a TLS lookup that finds an empty map.
pub mod fault {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::io;
    use std::os::fd::RawFd;

    #[derive(Clone, Copy, Debug)]
    struct Fault {
        current: u64,
        target: u64,
        errno: i32,
    }

    struct State {
        faults: HashMap<String, Fault>,
        counts: HashMap<String, u64>,
        fd_identities: HashMap<String, Vec<(u64, u64)>>,
        readdir_rotation: usize,
        readdir_reversed: bool,
    }

    impl State {
        fn new() -> Self {
            State {
                faults: HashMap::new(),
                counts: HashMap::new(),
                fd_identities: HashMap::new(),
                readdir_rotation: 0,
                readdir_reversed: false,
            }
        }
    }

    thread_local! {
        static STATE: RefCell<State> = RefCell::new(State::new());
    }

    /// Clear all pending faults and call counters on this thread.
    pub fn reset() {
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            s.faults.clear();
            s.counts.clear();
            s.fd_identities.clear();
            s.readdir_rotation = 0;
            s.readdir_reversed = false;
        });
    }

    /// Permute complete directory enumerations on this thread.
    pub fn permute_readdir(rotation: usize, reversed: bool) {
        STATE.with(|state| {
            let mut state = state.borrow_mut();
            state.readdir_rotation = rotation;
            state.readdir_reversed = reversed;
        });
    }

    pub(crate) fn permute_directory_entries<T>(entries: &mut [T]) {
        STATE.with(|state| {
            let state = state.borrow();
            if entries.is_empty() {
                return;
            }
            let rotation = state.readdir_rotation % entries.len();
            entries.rotate_left(rotation);
            if state.readdir_reversed {
                entries.reverse();
            }
        });
    }

    /// Fail the Nth (1-indexed) call to `func_name` with EIO.
    pub fn inject(func_name: &str, at_count: u64) {
        inject_errno(func_name, at_count, libc::EIO);
    }

    /// Fail the Nth (1-indexed) call to `func_name` with the given errno.
    pub fn inject_errno(func_name: &str, at_count: u64, errno: i32) {
        assert!(at_count >= 1, "fault inject count is 1-indexed");
        STATE.with(|s| {
            s.borrow_mut().faults.insert(
                func_name.to_string(),
                Fault {
                    current: 0,
                    target: at_count,
                    errno,
                },
            );
        });
    }

    /// Alias used by older call sites / docs.
    pub fn inject_at(func_name: &str, at_count: u64) {
        inject(func_name, at_count);
    }

    /// Number of times `func_name` has been checked since the last reset.
    pub fn call_count(func_name: &str) -> u64 {
        STATE.with(|s| *s.borrow().counts.get(func_name).unwrap_or(&0))
    }

    /// Ordered device/inode identities recorded for fd-bearing fault points.
    pub fn fd_identities(func_name: &str) -> Vec<(u64, u64)> {
        STATE.with(|state| {
            state
                .borrow()
                .fd_identities
                .get(func_name)
                .cloned()
                .unwrap_or_default()
        })
    }

    pub(crate) fn record_fd_identity(func_name: &str, fd: RawFd) -> io::Result<()> {
        STATE.with(|state| {
            if state.borrow().faults.is_empty() {
                return Ok(());
            }

            let mut statbuf: libc::stat = unsafe { std::mem::zeroed() };
            // SAFETY: `statbuf` points to writable storage for one `libc::stat`,
            // and the caller supplies an open descriptor for the duration of
            // this synchronous instrumentation call.
            if unsafe { libc::fstat(fd, &mut statbuf) } < 0 {
                return Err(io::Error::last_os_error());
            }
            state
                .borrow_mut()
                .fd_identities
                .entry(func_name.to_string())
                .or_default()
                .push((statbuf.st_dev as u64, statbuf.st_ino as u64));
            Ok(())
        })
    }

    /// Called by instrumented functions. Returns an error when a fault fires.
    #[inline]
    pub fn check(func_name: &str) -> Option<io::Error> {
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            if s.faults.is_empty() {
                return None;
            }
            *s.counts.entry(func_name.to_string()).or_insert(0) += 1;
            if let Some(entry) = s.faults.get_mut(func_name) {
                entry.current += 1;
                if entry.current == entry.target {
                    let errno = entry.errno;
                    s.faults.remove(func_name);
                    return Some(io::Error::from_raw_os_error(errno));
                }
            }
            None
        })
    }
}

macro_rules! fault_check {
    ($name:expr) => {
        if let Some(e) = $crate::fault::check($name) {
            return Err(e);
        }
    };
}

/// Open or create a file with O_TMPFILE.
pub fn open_tmpfile(dir_fd: RawFd) -> io::Result<OwnedFd> {
    fault_check!("open_tmpfile");
    // Use libc::O_TMPFILE which correctly includes O_DIRECTORY on all arches.
    // Fall back to the defined constant if libc does not expose it.
    let o_tmpfile = libc::O_TMPFILE;
    let dot = CString::new(".").unwrap();
    let fd = unsafe {
        libc::openat(
            dir_fd,
            dot.as_ptr(),
            o_tmpfile | libc::O_RDWR | libc::O_CLOEXEC,
            0o600,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Convert a name string to CString, returning InvalidInput on embedded NUL.
fn cstr_from_name(name: &str) -> io::Result<CString> {
    CString::new(name).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "path component contains NUL byte",
        )
    })
}

/// Convert a byte slice (OsStr on Linux) to CString, returning InvalidInput on embedded NUL.
fn cstr_from_bytes(bytes: &[u8]) -> io::Result<CString> {
    CString::new(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL byte"))
}

/// Open a directory for reading.
pub fn open_directory(dir_fd: RawFd, name: &str) -> io::Result<OwnedFd> {
    fault_check!("open_directory");
    // R2-B06: Use O_NOFOLLOW to prevent symlink traversal on state directories.
    let c_name = cstr_from_name(name)?;
    let fd = unsafe {
        libc::openat(
            dir_fd,
            c_name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Open a directory path while the kernel enforces confinement beneath `root_fd`.
pub fn open_directory_beneath(
    root_fd: RawFd,
    relative: ValidatedRelativePath<'_>,
) -> io::Result<OwnedFd> {
    fault_check!("openat2_beneath");
    let path = cstr_from_name(relative.as_str())?;
    let how = OpenHow {
        flags: resolver_open_flags() as u64,
        mode: 0,
        resolve: RESOLVER_RESOLVE_FLAGS,
    };
    // SAFETY: `path` is NUL-terminated, `how` has the kernel open_how layout,
    // and both pointers remain valid for the duration of the syscall.
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            root_fd,
            path.as_ptr(),
            &how as *const OpenHow,
            std::mem::size_of::<OpenHow>(),
        ) as libc::c_int
    };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a nonnegative openat2 result is a newly owned file descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Open a path relative to a directory fd with given flags.
pub fn openat(dir_fd: RawFd, name: &str, flags: i32, mode: u32) -> io::Result<OwnedFd> {
    fault_check!("openat");
    let c_name = cstr_from_name(name)?;
    let fd = unsafe { libc::openat(dir_fd, c_name.as_ptr(), flags, mode) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Create a directory.
pub fn mkdirat(dir_fd: RawFd, name: &str, mode: u32) -> io::Result<()> {
    fault_check!("mkdirat");
    let c_name = cstr_from_name(name)?;
    let rc = unsafe { libc::mkdirat(dir_fd, c_name.as_ptr(), mode) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Create a directory, treating EEXIST as Ok.
/// Returns true if the directory was newly created, false if it already existed.
pub fn mkdirat_eexist_ok(dir_fd: RawFd, name: &str, mode: u32) -> io::Result<bool> {
    match mkdirat(dir_fd, name, mode) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(false),
        Err(e) => Err(e),
    }
}

/// fsync a file descriptor.
pub fn fsync(fd: RawFd) -> io::Result<()> {
    fault_check!("fsync");
    let rc = unsafe { libc::fsync(fd) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// fsync a directory by opening it read-only and syncing.
pub fn fsync_dir(dir_fd: RawFd, name: &str) -> io::Result<()> {
    fault_check!("fsync_dir");
    let fd = open_directory(dir_fd, name)?;
    fsync(fd.as_raw_fd())
}

/// fsync a directory by its already-open fd.
pub fn fsync_dir_fd(fd: RawFd) -> io::Result<()> {
    fault::record_fd_identity("fsync_dir_fd", fd)?;
    fault_check!("fsync_dir_fd");
    fsync(fd)
}

/// Rename with RENAME_NOREPLACE.
pub fn renameat2_noreplace(
    old_dir_fd: RawFd,
    old_name: &str,
    new_dir_fd: RawFd,
    new_name: &str,
) -> io::Result<()> {
    fault_check!("renameat2_noreplace");
    const RENAME_NOREPLACE: u32 = 1 << 0;
    let c_old = cstr_from_name(old_name)?;
    let c_new = cstr_from_name(new_name)?;
    let rc = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            old_dir_fd,
            c_old.as_ptr(),
            new_dir_fd,
            c_new.as_ptr(),
            RENAME_NOREPLACE,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Plain rename (for receipt compaction).
pub fn renameat(
    old_dir_fd: RawFd,
    old_name: &str,
    new_dir_fd: RawFd,
    new_name: &str,
) -> io::Result<()> {
    fault_check!("renameat");
    let c_old = cstr_from_name(old_name)?;
    let c_new = cstr_from_name(new_name)?;
    let rc = unsafe { libc::renameat(old_dir_fd, c_old.as_ptr(), new_dir_fd, c_new.as_ptr()) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// linkat with AT_EMPTY_PATH for O_TMPFILE publication.
pub fn linkat_empty_path(fd: RawFd, dest_dir_fd: RawFd, dest_name: &str) -> io::Result<()> {
    fault_check!("linkat_empty_path");
    const AT_EMPTY_PATH: i32 = 0x1000;
    let c_dest = cstr_from_name(dest_name)?;
    let empty = CString::new("").unwrap();
    let rc = unsafe {
        libc::syscall(
            libc::SYS_linkat,
            fd,
            empty.as_ptr(),
            dest_dir_fd,
            c_dest.as_ptr(),
            AT_EMPTY_PATH,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// linkat via /proc/self/fd for unprivileged O_TMPFILE publication.
pub fn linkat_proc_self_fd(fd: RawFd, dest_dir_fd: RawFd, dest_name: &str) -> io::Result<()> {
    fault_check!("linkat_proc_self_fd");
    const AT_SYMLINK_FOLLOW: i32 = 0x400;
    #[allow(clippy::manual_c_str_literals)]
    let proc_path = format!("/proc/self/fd/{fd}\0");
    let c_dest = cstr_from_name(dest_name)?;
    let rc = unsafe {
        libc::syscall(
            libc::SYS_linkat,
            libc::AT_FDCWD,
            proc_path.as_ptr() as *const _,
            dest_dir_fd,
            c_dest.as_ptr(),
            AT_SYMLINK_FOLLOW,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// unlinkat - remove a file.
pub fn unlinkat(dir_fd: RawFd, name: &str) -> io::Result<()> {
    fault_check!("unlinkat");
    let c_name = cstr_from_name(name)?;
    let rc = unsafe { libc::unlinkat(dir_fd, c_name.as_ptr(), 0) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Remove a directory (must be empty).
pub fn unlinkat_dir(dir_fd: RawFd, name: &str) -> io::Result<()> {
    fault_check!("unlinkat_dir");
    const AT_REMOVEDIR: i32 = 0x200;
    let c_name = cstr_from_name(name)?;
    let rc = unsafe { libc::unlinkat(dir_fd, c_name.as_ptr(), AT_REMOVEDIR) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// stat a file relative to a directory fd using AT_SYMLINK_NOFOLLOW.
pub fn fstatat(dir_fd: RawFd, name: &str) -> io::Result<libc::stat> {
    fault_check!("fstatat");
    let c_name = cstr_from_name(name)?;
    let mut statbuf: libc::stat = unsafe { std::mem::zeroed() };
    let rc = unsafe {
        libc::fstatat(
            dir_fd,
            c_name.as_ptr(),
            &mut statbuf,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(statbuf)
}

/// fstat on an already-open fd.
pub fn fstat(fd: RawFd) -> io::Result<libc::stat> {
    fault_check!("fstat");
    let mut statbuf: libc::stat = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::fstat(fd, &mut statbuf) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(statbuf)
}

/// Get filesystem stats using OsStrExt for byte-safe paths.
pub fn statfs(path: &Path) -> io::Result<libc::statfs> {
    let c_path = cstr_from_bytes(path.as_os_str().as_bytes())?;
    let mut statbuf: libc::statfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statfs(c_path.as_ptr(), &mut statbuf) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(statbuf)
}

/// Read the boot ID from /proc/sys/kernel/random/boot_id.
pub fn read_boot_id() -> io::Result<String> {
    std::fs::read_to_string("/proc/sys/kernel/random/boot_id").map(|s| s.trim().to_string())
}

/// CLOCK_BOOTTIME in nanoseconds.
pub fn clock_boottime_ns() -> io::Result<u64> {
    fault_check!("clock_boottime_ns");
    let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut ts) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64)
}

/// CLOCK_REALTIME in nanoseconds.
pub fn clock_realtime_ns() -> io::Result<u64> {
    fault_check!("clock_realtime_ns");
    let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_REALTIME, &mut ts) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    // Check for negative tv_sec (before epoch)
    if ts.tv_sec < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "clock before epoch",
        ));
    }
    Ok(ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64)
}

/// CLOCK_MONOTONIC in nanoseconds (for budget enforcement).
pub fn clock_monotonic_ns() -> io::Result<u64> {
    fault_check!("clock_monotonic_ns");
    let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64)
}

/// Generate random bytes from the OS crypto source.
/// Loops until the entire buffer is filled. Handles short reads, EINTR,
/// and EAGAIN. Returns an error (not zero data) on any failure.
pub fn get_random(bytes: usize) -> io::Result<Vec<u8>> {
    fault_check!("get_random");
    if bytes == 0 {
        return Ok(Vec::new());
    }
    let mut buf = vec![0u8; bytes];
    let mut filled = 0usize;
    loop {
        let rc = unsafe {
            libc::syscall(
                libc::SYS_getrandom,
                buf[filled..].as_mut_ptr(),
                bytes - filled,
                0,
            )
        };
        if rc < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue; // EINTR: retry
            }
            // EAGAIN should not happen with flags=0, but handle defensively
            if e.raw_os_error() == Some(libc::EAGAIN) {
                continue;
            }
            return Err(e);
        }
        let n = rc as usize;
        if n == 0 {
            // A zero-byte successful return is anomalous for getrandom
            // with a non-zero length request. Treat as an error.
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "getrandom returned zero bytes",
            ));
        }
        filled += n;
        if filled >= bytes {
            break;
        }
    }
    Ok(buf)
}

/// Generate a random 128-bit value.
pub fn random_128bit() -> io::Result<[u8; 16]> {
    let bytes = get_random(16)?;
    let mut out = [0u8; 16];
    out.copy_from_slice(&bytes);
    Ok(out)
}

/// pwrite to a file descriptor at a given offset.
pub fn pwrite(fd: RawFd, buf: &[u8], offset: u64) -> io::Result<usize> {
    fault_check!("pwrite");
    let rc = unsafe { libc::pwrite(fd, buf.as_ptr() as *const _, buf.len(), offset as i64) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(rc as usize)
}

/// Write all bytes, retrying on partial writes. Rejects zero progress.
/// Returns an error if write returns 0 (no progress) rather than looping forever.
pub fn write_all(fd: RawFd, buf: &[u8]) -> io::Result<()> {
    fault_check!("write_all");
    let mut written = 0;
    while written < buf.len() {
        let rc =
            unsafe { libc::write(fd, buf[written..].as_ptr() as *const _, buf.len() - written) };
        if rc < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        if rc == 0 {
            // Zero progress on a write: this indicates an error condition
            // (e.g., full filesystem returning 0, or broken pipe).
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "write returned zero bytes (no progress)",
            ));
        }
        written += rc as usize;
    }
    Ok(())
}

/// Write all bytes at a given offset using pwrite, retrying on partial writes.
/// Returns an error if pwrite returns 0 (no progress).
pub fn pwrite_all(fd: RawFd, buf: &[u8], offset: u64) -> io::Result<()> {
    let mut written = 0;
    let mut current_offset = offset;
    while written < buf.len() {
        let n = pwrite(fd, &buf[written..], current_offset)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "pwrite returned zero bytes (no progress)",
            ));
        }
        written += n;
        current_offset += n as u64;
    }
    Ok(())
}

/// Read from a file descriptor.
pub fn read(fd: RawFd, buf: &mut [u8]) -> io::Result<usize> {
    loop {
        let rc = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut _, buf.len()) };
        if rc < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        return Ok(rc as usize);
    }
}

/// Read at a specific offset using pread.
pub fn pread(fd: RawFd, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    fault_check!("pread");
    loop {
        let rc = unsafe { libc::pread(fd, buf.as_mut_ptr() as *mut _, buf.len(), offset as i64) };
        if rc < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        return Ok(rc as usize);
    }
}

/// Read exactly `buf.len()` bytes at `offset`, or return an error.
pub fn pread_exact(fd: RawFd, buf: &mut [u8], offset: u64) -> io::Result<()> {
    let mut filled = 0;
    let mut cur = offset;
    while filled < buf.len() {
        let n = pread(fd, &mut buf[filled..], cur)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "pread hit EOF before filling buffer",
            ));
        }
        filled += n;
        cur += n as u64;
    }
    Ok(())
}

/// Open a directory path (absolute) and return an OwnedFd.
/// Uses OsStrExt for byte-safe path handling.
pub fn open_dir_absolute(path: &Path) -> io::Result<OwnedFd> {
    // R2-B06: Use O_NOFOLLOW to prevent the root from being a symlink.
    let c_path = cstr_from_bytes(path.as_os_str().as_bytes())?;
    let fd = unsafe {
        libc::open(
            c_path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Create a file with O_CREAT | O_EXCL | O_NOFOLLOW.
pub fn create_exclusive(dir_fd: RawFd, name: &str, mode: u32) -> io::Result<OwnedFd> {
    openat(
        dir_fd,
        name,
        libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        mode,
    )
}

/// Try a nonblocking exclusive OFD lock on a file.
/// Returns Ok(true) if acquired, Ok(false) if contended.
pub fn try_ofd_write_lock(fd: RawFd) -> io::Result<bool> {
    fault_check!("try_ofd_write_lock");
    let mut flock: libc::flock = unsafe { std::mem::zeroed() };
    flock.l_type = libc::F_WRLCK as i16;
    flock.l_whence = libc::SEEK_SET as i16;
    flock.l_start = 0;
    flock.l_len = 0;
    let rc = unsafe { libc::fcntl(fd, libc::F_OFD_SETLK, &flock) };
    if rc < 0 {
        let e = io::Error::last_os_error();
        if e.kind() == io::ErrorKind::WouldBlock || e.raw_os_error() == Some(libc::EAGAIN) {
            return Ok(false);
        }
        return Err(e);
    }
    Ok(true)
}

/// Try a nonblocking shared OFD lock on a file.
pub fn try_ofd_read_lock(fd: RawFd) -> io::Result<bool> {
    fault_check!("try_ofd_read_lock");
    let mut flock: libc::flock = unsafe { std::mem::zeroed() };
    flock.l_type = libc::F_RDLCK as i16;
    flock.l_whence = libc::SEEK_SET as i16;
    flock.l_start = 0;
    flock.l_len = 0;
    let rc = unsafe { libc::fcntl(fd, libc::F_OFD_SETLK, &flock) };
    if rc < 0 {
        let e = io::Error::last_os_error();
        if e.kind() == io::ErrorKind::WouldBlock || e.raw_os_error() == Some(libc::EAGAIN) {
            return Ok(false);
        }
        return Err(e);
    }
    Ok(true)
}

/// Byte-preserving directory entry name.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct DirEntryName(Vec<u8>);

impl DirEntryName {
    /// Returns the exact bytes supplied by the directory stream.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns the name when it is valid UTF-8.
    pub fn as_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.0).ok()
    }

    /// Returns the name only when it belongs to the protocol's ASCII alphabet.
    pub fn as_ascii_str(&self) -> Option<&str> {
        if self.0.is_ascii() {
            self.as_str()
        } else {
            None
        }
    }
}

impl std::fmt::Debug for DirEntryName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "b\"")?;
        for byte in &self.0 {
            for escaped in std::ascii::escape_default(*byte) {
                write!(formatter, "{}", char::from(escaped))?;
            }
        }
        write!(formatter, "\"")
    }
}

/// Exact protocol-visible work completed by a directory enumeration.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DirectoryEnumerationProgress {
    /// Non-dot entries returned by `readdir`.
    pub entries_read: usize,
    /// Raw name bytes across those entries.
    pub name_bytes_read: usize,
}

/// A complete bounded enumeration and its work accounting.
#[derive(Debug)]
pub struct DirectoryEnumeration {
    pub entries: Vec<DirEntryName>,
    pub progress: DirectoryEnumerationProgress,
}

#[derive(Debug)]
pub enum DirectoryEnumerationError {
    Cancelled,
    CancellationCheck(io::Error),
    Io(io::Error),
}

#[derive(Debug)]
pub enum DirectoryEnumerationProgressError {
    Cancelled(DirectoryEnumerationProgress),
    CancellationCheck {
        error: io::Error,
        progress: DirectoryEnumerationProgress,
    },
    Io {
        error: io::Error,
        progress: DirectoryEnumerationProgress,
    },
}

impl std::fmt::Display for DirectoryEnumerationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled => write!(formatter, "directory enumeration cancelled"),
            Self::CancellationCheck(error) => {
                write!(formatter, "directory cancellation check failed: {error}")
            }
            Self::Io(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for DirectoryEnumerationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Cancelled => None,
            Self::CancellationCheck(error) | Self::Io(error) => Some(error),
        }
    }
}

impl DirectoryEnumerationError {
    fn into_io_error(self) -> io::Error {
        match self {
            Self::Cancelled => io::Error::new(
                io::ErrorKind::Interrupted,
                "directory enumeration cancelled unexpectedly",
            ),
            Self::CancellationCheck(error) | Self::Io(error) => error,
        }
    }
}

impl std::fmt::Display for DirectoryEnumerationProgressError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled(_) => write!(formatter, "directory enumeration cancelled"),
            Self::CancellationCheck { error, .. } => {
                write!(formatter, "directory cancellation check failed: {error}")
            }
            Self::Io { error, .. } => error.fmt(formatter),
        }
    }
}

impl std::error::Error for DirectoryEnumerationProgressError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Cancelled(_) => None,
            Self::CancellationCheck { error, .. } | Self::Io { error, .. } => Some(error),
        }
    }
}

impl DirectoryEnumerationProgressError {
    pub fn progress(&self) -> DirectoryEnumerationProgress {
        match self {
            Self::Cancelled(progress)
            | Self::CancellationCheck { progress, .. }
            | Self::Io { progress, .. } => *progress,
        }
    }

    fn into_legacy(self) -> DirectoryEnumerationError {
        match self {
            Self::Cancelled(_) => DirectoryEnumerationError::Cancelled,
            Self::CancellationCheck { error, .. } => {
                DirectoryEnumerationError::CancellationCheck(error)
            }
            Self::Io { error, .. } => DirectoryEnumerationError::Io(error),
        }
    }
}

/// Read directory entries without losing non-UTF-8 names.
/// Consumes the fd via fdopendir and rejects directories exceeding either
/// bound before retaining an unbounded collection.
fn read_dir_entry_names_impl<F>(
    dir_fd: RawFd,
    max_entries: usize,
    max_name_bytes: usize,
    mut should_stop: F,
) -> Result<DirectoryEnumeration, DirectoryEnumerationProgressError>
where
    F: FnMut() -> io::Result<bool>,
{
    let mut entries = Vec::new();
    let mut name_bytes_read = 0usize;
    let mut progress = DirectoryEnumerationProgress::default();
    let dir = unsafe { libc::fdopendir(dir_fd) };
    if dir.is_null() {
        let error = io::Error::last_os_error();
        // fdopendir did not take ownership when it returned a null pointer.
        unsafe { libc::close(dir_fd) };
        return Err(DirectoryEnumerationProgressError::Io { error, progress });
    }

    loop {
        match should_stop() {
            Ok(true) => {
                unsafe { libc::closedir(dir) };
                return Err(DirectoryEnumerationProgressError::Cancelled(progress));
            }
            Ok(false) => {}
            Err(error) => {
                unsafe { libc::closedir(dir) };
                return Err(DirectoryEnumerationProgressError::CancellationCheck {
                    error,
                    progress,
                });
            }
        }
        // B5: Set errno to 0 before readdir to distinguish EOF from error.
        unsafe { *libc::__errno_location() = 0 };
        let entry = unsafe { libc::readdir(dir) };
        if entry.is_null() {
            // B5: Check errno to distinguish EOF from error.
            let errno = unsafe { *libc::__errno_location() };
            if errno != 0 {
                unsafe { libc::closedir(dir) };
                return Err(DirectoryEnumerationProgressError::Io {
                    error: io::Error::from_raw_os_error(errno),
                    progress,
                });
            }
            break;
        }
        let name_bytes = unsafe {
            let name_ptr = (*entry).d_name.as_ptr();
            let len = libc::strlen(name_ptr);
            std::slice::from_raw_parts(name_ptr as *const u8, len)
        };
        if name_bytes != b"." && name_bytes != b".." {
            progress.entries_read = progress.entries_read.saturating_add(1);
            progress.name_bytes_read = progress.name_bytes_read.saturating_add(name_bytes.len());
            let Some(next_name_bytes) = name_bytes_read.checked_add(name_bytes.len()) else {
                unsafe { libc::closedir(dir) };
                return Err(DirectoryEnumerationProgressError::Io {
                    error: io::Error::new(
                        io::ErrorKind::FileTooLarge,
                        "directory entry byte count overflow",
                    ),
                    progress,
                });
            };
            if entries.len() >= max_entries || next_name_bytes > max_name_bytes {
                unsafe { libc::closedir(dir) };
                return Err(DirectoryEnumerationProgressError::Io {
                    error: io::Error::new(
                        io::ErrorKind::FileTooLarge,
                        "directory exceeds configured recovery scan bound",
                    ),
                    progress,
                });
            }
            name_bytes_read = next_name_bytes;
            entries.push(DirEntryName(name_bytes.to_vec()));
        }
    }

    unsafe { libc::closedir(dir) };

    fault::permute_directory_entries(&mut entries);

    Ok(DirectoryEnumeration { entries, progress })
}

/// Read byte-preserving directory entries.
/// Consumes the fd via fdopendir.
fn read_dir_entries_impl(dir_fd: RawFd) -> io::Result<Vec<DirEntryName>> {
    read_dir_entry_names_impl(dir_fd, usize::MAX, usize::MAX, || Ok(false))
        .map_err(DirectoryEnumerationProgressError::into_legacy)
        .map_err(DirectoryEnumerationError::into_io_error)
        .map(|enumeration| enumeration.entries)
}

/// R4-PERF: Iterate directory entries with a callback, avoiding full
/// materialization. The callback returns true to continue, false to stop.
/// Returns the number of entries processed.
pub fn read_dir_for_each<F: FnMut(&DirEntryName) -> bool>(
    dir_fd: RawFd,
    mut f: F,
) -> io::Result<usize> {
    let dup_fd = unsafe { libc::dup(dir_fd) };
    if dup_fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let dir = unsafe { libc::fdopendir(dup_fd) };
    if dir.is_null() {
        // P1-16: Close the dup'd fd before returning to avoid descriptor leak.
        unsafe { libc::close(dup_fd) };
        return Err(io::Error::last_os_error());
    }
    let mut count = 0usize;
    loop {
        unsafe { *libc::__errno_location() = 0 };
        let entry = unsafe { libc::readdir(dir) };
        if entry.is_null() {
            let errno = unsafe { *libc::__errno_location() };
            if errno != 0 {
                unsafe { libc::closedir(dir) };
                return Err(io::Error::from_raw_os_error(errno));
            }
            break;
        }
        let name_bytes = unsafe {
            let name_ptr = (*entry).d_name.as_ptr();
            let len = libc::strlen(name_ptr);
            std::slice::from_raw_parts(name_ptr as *const u8, len)
        };
        if name_bytes != b"." && name_bytes != b".." {
            let name = DirEntryName(name_bytes.to_vec());
            count += 1;
            if !f(&name) {
                break;
            }
        }
    }
    unsafe { libc::closedir(dir) };
    Ok(count)
}

/// Read directory entries. Consumes the fd (fdopendir takes ownership).
/// Prefer read_dir_entries_owned which dups the fd first.
pub fn read_dir_entries(dir_fd: RawFd) -> io::Result<Vec<DirEntryName>> {
    read_dir_entries_impl(dir_fd)
}

/// Get the filesystem type magic number.
pub fn fs_type_magic(path: &Path) -> io::Result<i64> {
    let stat = statfs(path)?;
    Ok(stat.f_type as i64)
}

/// Known filesystem magic numbers.
pub const EXT4_SUPER_MAGIC: i64 = 0xEF53;
pub const XFS_SUPER_MAGIC: i64 = 0x58465342;
pub const TMPFS_MAGIC: i64 = 0x01021994;
pub const NFS_SUPER_MAGIC: i64 = 0x6969;
pub const OVERLAYFS_SUPER_MAGIC: i64 = 0x794c7630;
pub const FUSE_SUPER_MAGIC: i64 = 0x65735546;

/// Read directory entries without consuming the fd (uses dup first).
pub fn read_dir_entries_owned(dir_fd: RawFd) -> io::Result<Vec<DirEntryName>> {
    // dup the fd so fdopendir doesn't consume the original
    let dup_fd = unsafe { libc::dup(dir_fd) };
    if dup_fd < 0 {
        return Err(io::Error::last_os_error());
    }
    read_dir_entries_impl(dup_fd)
}

/// Read byte-preserving directory entries without consuming the caller's fd.
/// The function returns an error rather than materializing more than the
/// configured entry or aggregate-name-byte bound.
pub fn read_dir_entry_names_bounded_owned(
    dir_fd: RawFd,
    max_entries: usize,
    max_name_bytes: usize,
) -> io::Result<Vec<DirEntryName>> {
    read_dir_entry_names_bounded_owned_until(dir_fd, max_entries, max_name_bytes, || Ok(false))
        .map_err(DirectoryEnumerationError::into_io_error)
}

/// Read bounded byte-preserving directory entries with cooperative cancellation.
pub fn read_dir_entry_names_bounded_owned_until<F>(
    dir_fd: RawFd,
    max_entries: usize,
    max_name_bytes: usize,
    should_stop: F,
) -> Result<Vec<DirEntryName>, DirectoryEnumerationError>
where
    F: FnMut() -> io::Result<bool>,
{
    read_dir_entry_names_bounded_owned_until_with_progress(
        dir_fd,
        max_entries,
        max_name_bytes,
        should_stop,
    )
    .map(|enumeration| enumeration.entries)
    .map_err(DirectoryEnumerationProgressError::into_legacy)
}

/// Read bounded byte-preserving entries and retain exact partial progress.
pub fn read_dir_entry_names_bounded_owned_until_with_progress<F>(
    dir_fd: RawFd,
    max_entries: usize,
    max_name_bytes: usize,
    should_stop: F,
) -> Result<DirectoryEnumeration, DirectoryEnumerationProgressError>
where
    F: FnMut() -> io::Result<bool>,
{
    let reopened =
        open_directory(dir_fd, ".").map_err(|error| DirectoryEnumerationProgressError::Io {
            error,
            progress: DirectoryEnumerationProgress::default(),
        })?;
    read_dir_entry_names_impl(
        reopened.into_raw_fd(),
        max_entries,
        max_name_bytes,
        should_stop,
    )
}

/// Change file mode relative to a directory fd.
pub fn fchmodat(dir_fd: RawFd, name: &str, mode: u32) -> io::Result<()> {
    let c_name = cstr_from_name(name)?;
    let rc = unsafe { libc::fchmodat(dir_fd, c_name.as_ptr(), mode, 0) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Change file mode on an open fd.
pub fn fchmod(fd: RawFd, mode: u32) -> io::Result<()> {
    let rc = unsafe { libc::fchmod(fd, mode) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Durable no-overwrite move: renameat2 with RENAME_NOREPLACE, then sync directories.
/// If source and destination are the same directory, sync once.
pub fn durable_move_noreplace(
    src_dir_fd: RawFd,
    src_name: &str,
    dest_dir_fd: RawFd,
    dest_name: &str,
) -> io::Result<()> {
    fault_check!("durable_move_noreplace");
    renameat2_noreplace(src_dir_fd, src_name, dest_dir_fd, dest_name)?;

    // Check if same directory by comparing device and inode
    let src_stat = fstat(src_dir_fd)?;
    let dest_stat = fstat(dest_dir_fd)?;

    if src_stat.st_dev == dest_stat.st_dev && src_stat.st_ino == dest_stat.st_ino {
        // Same directory: sync once
        fsync_dir_fd(dest_dir_fd)?;
    } else {
        // Different directories: sync destination first, then source
        fsync_dir_fd(dest_dir_fd)?;
        fsync_dir_fd(src_dir_fd)?;
    }
    Ok(())
}

/// Replacing rename: for receipt compaction and wall-watermark replacement only.
/// Performs a standard rename (overwrites destination), then syncs the directory.
pub fn durable_move_replace(
    src_dir_fd: RawFd,
    src_name: &str,
    dest_dir_fd: RawFd,
    dest_name: &str,
) -> io::Result<()> {
    fault_check!("durable_move_replace");
    renameat(src_dir_fd, src_name, dest_dir_fd, dest_name)?;

    let src_stat = fstat(src_dir_fd)?;
    let dest_stat = fstat(dest_dir_fd)?;

    if src_stat.st_dev == dest_stat.st_dev && src_stat.st_ino == dest_stat.st_ino {
        fsync_dir_fd(dest_dir_fd)?;
    } else {
        fsync_dir_fd(dest_dir_fd)?;
        fsync_dir_fd(src_dir_fd)?;
    }
    Ok(())
}

/// Stabilization sync: sync a verified destination and its parent directories.
/// Non-mutating: only performs fsync, no rename or write.
pub fn stabilize(fd: RawFd) -> io::Result<()> {
    fsync(fd)
}

/// Stabilize a directory by its fd.
pub fn stabilize_dir(fd: RawFd) -> io::Result<()> {
    fsync_dir_fd(fd)
}

/// syncfs: sync an entire filesystem. Caller must assert the queue owns the mount.
pub fn syncfs(fd: RawFd) -> io::Result<()> {
    let rc = unsafe { libc::syscall(libc::SYS_syncfs, fd) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Check if an error indicates the source is gone (ENOENT).
pub fn is_source_gone(err: &io::Error) -> bool {
    err.raw_os_error() == Some(libc::ENOENT)
}

/// Check if an error indicates a collision (EEXIST).
pub fn is_collision(err: &io::Error) -> bool {
    err.raw_os_error() == Some(libc::EEXIST)
}

/// Check if an error indicates resource exhaustion (ENOSPC, EDQUOT).
pub fn is_resource_exhausted(err: &io::Error) -> bool {
    matches!(err.raw_os_error(), Some(libc::ENOSPC) | Some(libc::EDQUOT))
}

/// Check if an error is a sync failure that should poison the handle.
pub fn is_sync_failure(err: &io::Error) -> bool {
    matches!(err.raw_os_error(), Some(libc::EIO) | Some(libc::ESTALE))
}

/// Check if an error is a capability/permission error (should not fall back).
pub fn is_capability_error(err: &io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::EPERM) | Some(libc::EACCES) | Some(libc::ENOSYS)
    )
}

/// Check if an error should suppress publication fallback.
pub fn should_propagate_on_fallback(err: &io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::EIO)
            | Some(libc::ENOSPC)
            | Some(libc::EDQUOT)
            | Some(libc::ESTALE)
            | Some(libc::EPERM)
            | Some(libc::EACCES)
    )
}

/// Probe unnamed-file publication modes. Returns which mode is available.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationMode {
    DirectAtEmptyPath,
    ProcSelfFd,
    NamedFallback,
}

/// Probe publication capability by creating a temp file and trying to link it.
pub fn probe_publication_mode(dir_fd: RawFd) -> io::Result<PublicationMode> {
    // Create a temp file via O_TMPFILE
    let tmp = match open_tmpfile(dir_fd) {
        Ok(fd) => fd,
        Err(_) => return Ok(PublicationMode::NamedFallback),
    };

    // Write a byte so it's not empty
    write_all(tmp.as_raw_fd(), b"x")?;

    let rand = random_128bit()?;
    let probe_name = format!(
        ".pubprobe-{}\0",
        rand.iter().map(|x| format!("{x:02x}")).collect::<String>()
    );

    // Try AT_EMPTY_PATH first
    let name = probe_name.trim_end_matches('\0');
    if linkat_empty_path(tmp.as_raw_fd(), dir_fd, name).is_ok() {
        let _ = unlinkat(dir_fd, name);
        return Ok(PublicationMode::DirectAtEmptyPath);
    }

    // Try /proc/self/fd
    if linkat_proc_self_fd(tmp.as_raw_fd(), dir_fd, name).is_ok() {
        let _ = unlinkat(dir_fd, name);
        return Ok(PublicationMode::ProcSelfFd);
    }

    Ok(PublicationMode::NamedFallback)
}

/// Probe no-overwrite rename support.
pub fn probe_rename_noreplace(dir_fd: RawFd) -> io::Result<bool> {
    let rand1 = random_128bit()?;
    let rand2 = random_128bit()?;
    let name1 = format!(
        ".rnprobe1-{}\0",
        rand1.iter().map(|x| format!("{x:02x}")).collect::<String>()
    );
    let name2 = format!(
        ".rnprobe2-{}\0",
        rand2.iter().map(|x| format!("{x:02x}")).collect::<String>()
    );
    let n1 = name1.trim_end_matches('\0');
    let n2 = name2.trim_end_matches('\0');

    // Create two files
    let f1 = create_exclusive(dir_fd, n1, 0o600)?;
    let f2 = create_exclusive(dir_fd, n2, 0o600)?;
    drop(f1);
    drop(f2);

    // Try to rename n1 -> n2 with NOREPLACE (should fail with EEXIST since n2 exists)
    let result = renameat2_noreplace(dir_fd, n1, dir_fd, n2);
    // R2-H15: Only EEXIST proves RENAME_NOREPLACE support.
    let works = result.is_err_and(|e| e.raw_os_error() == Some(libc::EEXIST));

    // Cleanup
    let _ = unlinkat(dir_fd, n1);
    let _ = unlinkat(dir_fd, n2);

    Ok(works)
}

/// Probe directory fsync support.
pub fn probe_dir_fsync(dir_fd: RawFd) -> io::Result<bool> {
    match fsync_dir_fd(dir_fd) {
        Ok(()) => Ok(true),
        Err(_) => Ok(false),
    }
}

/// Check if a path is absolute (starts with '/').
pub fn is_absolute_path(s: &str) -> bool {
    s.starts_with('/')
}

/// Validate a relative path component for safety:
/// rejects slashes, dot components, empty components, NUL, and noncanonical bytes.
pub fn validate_path_component(comp: &str) -> io::Result<()> {
    if comp.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "empty path component",
        ));
    }
    if comp == ".." || comp == "." {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path component is '.' or '..'",
        ));
    }
    if comp.contains('/') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "slash in path component",
        ));
    }
    if comp.contains('\0') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "NUL byte in path component",
        ));
    }
    if !comp.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path component contains noncanonical ASCII",
        ));
    }
    if comp.len() > MAX_COMPONENT_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path component exceeds 255 bytes",
        ));
    }
    Ok(())
}

/// Validate a relative path for safety: rejects absolute paths, '.' and '..'.
pub fn validate_relative_path(path: &str) -> io::Result<ValidatedRelativePath<'_>> {
    if path.starts_with('/') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "absolute path not allowed",
        ));
    }
    if path.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty path"));
    }
    if path.len() > MAX_RELATIVE_PATH_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "relative path exceeds 4095 bytes",
        ));
    }
    for comp in path.split('/') {
        if comp.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "empty path component in relative path",
            ));
        }
        validate_path_component(comp)?;
    }
    Ok(ValidatedRelativePath { path })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_test_dir(label: &str) -> std::path::PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sequence = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "steadq_{label}_{}_{}",
            std::process::id(),
            sequence
        ))
    }

    #[test]
    fn boot_id_available() {
        let boot_id = read_boot_id().unwrap();
        assert_eq!(boot_id.len(), 36);
    }

    #[test]
    fn clock_boottime_positive() {
        let now = clock_boottime_ns().unwrap();
        assert!(now > 0);
    }

    #[test]
    fn clock_realtime_positive() {
        let now = clock_realtime_ns().unwrap();
        assert!(now > 0);
    }

    #[test]
    fn clock_monotonic_positive() {
        let now = clock_monotonic_ns().unwrap();
        assert!(now > 0);
    }

    #[test]
    fn random_128_bit_is_random() {
        let a = random_128bit().unwrap();
        let b = random_128bit().unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn write_all_persists_every_byte() {
        let path = unique_test_dir("write_all");
        std::fs::create_dir(&path).unwrap();
        let dir: OwnedFd = std::fs::File::open(&path).unwrap().into();
        let file = create_exclusive(dir.as_raw_fd(), "data", 0o600).unwrap();
        write_all(file.as_raw_fd(), b"complete").unwrap();
        let mut bytes = [0u8; 8];
        pread_exact(file.as_raw_fd(), &mut bytes, 0).unwrap();
        assert_eq!(&bytes, b"complete");
        drop(file);
        drop(dir);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn ofd_write_lock_reports_acquired_and_contended() {
        let path = unique_test_dir("write_lock");
        std::fs::create_dir(&path).unwrap();
        let dir: OwnedFd = std::fs::File::open(&path).unwrap().into();
        let first = create_exclusive(dir.as_raw_fd(), "lock", 0o600).unwrap();
        let second = openat(dir.as_raw_fd(), "lock", libc::O_RDWR, 0).unwrap();
        assert!(try_ofd_write_lock(first.as_raw_fd()).unwrap());
        assert!(!try_ofd_write_lock(second.as_raw_fd()).unwrap());
        drop(second);
        drop(first);
        drop(dir);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn ofd_read_lock_reports_acquired_and_writer_contention() {
        let path = unique_test_dir("read_lock");
        std::fs::create_dir(&path).unwrap();
        let dir: OwnedFd = std::fs::File::open(&path).unwrap().into();
        let reader = create_exclusive(dir.as_raw_fd(), "lock", 0o600).unwrap();
        assert!(try_ofd_read_lock(reader.as_raw_fd()).unwrap());
        drop(reader);

        let writer = openat(dir.as_raw_fd(), "lock", libc::O_RDWR, 0).unwrap();
        let blocked_reader = openat(dir.as_raw_fd(), "lock", libc::O_RDWR, 0).unwrap();
        assert!(try_ofd_write_lock(writer.as_raw_fd()).unwrap());
        assert!(!try_ofd_read_lock(blocked_reader.as_raw_fd()).unwrap());
        drop(blocked_reader);
        drop(writer);
        drop(dir);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn random_is_not_all_zero() {
        // Verify randomness is never all-zero (regression for B-07)
        for _ in 0..100 {
            let r = random_128bit().unwrap();
            assert_ne!(r, [0u8; 16], "random_128bit returned all zeros");
        }
    }

    #[test]
    fn get_random_fills_buffer() {
        // Verify the full buffer is filled (regression for B-07)
        let buf = get_random(256).unwrap();
        assert_eq!(buf.len(), 256);
        // Extremely unlikely that 256 random bytes are all zero
        assert!(buf.iter().any(|&b| b != 0));
    }

    #[test]
    fn get_random_zero_returns_empty() {
        let buf = get_random(0).unwrap();
        assert!(buf.is_empty());
    }

    #[test]
    fn nul_in_name_returns_error() {
        // C-06: embedded NUL must return error, not panic
        let result = cstr_from_name("hello\0world");
        assert!(result.is_err());
    }

    #[test]
    fn path_validation_rejects_dotdot() {
        assert!(validate_path_component("..").is_err());
        assert!(validate_path_component(".").is_err());
        assert!(validate_path_component("").is_err());
        assert!(validate_path_component("/abs").is_err());
        assert!(validate_path_component("a/b").is_err());
        assert!(validate_path_component("non-ascii-\u{00e9}").is_err());
        assert!(validate_path_component("with space").is_err());
        assert!(validate_path_component("with\ttab").is_err());
        assert!(validate_path_component("with\nnewline").is_err());
        assert!(validate_path_component(&"a".repeat(256)).is_err());
        assert!(validate_path_component(&"a".repeat(255)).is_ok());
        assert!(validate_path_component("ok").is_ok());
    }

    #[test]
    fn validate_relative_path_rejects_absolute_and_empty() {
        let path_with_len = |length: usize| {
            assert_eq!(length % 2, 1);
            "a/".repeat(length / 2) + "a"
        };
        assert!(validate_relative_path("/etc/passwd").is_err());
        assert!(validate_relative_path("").is_err());
        assert!(validate_relative_path("a//b").is_err());
        assert!(validate_relative_path("a/b/").is_err());
        assert!(validate_relative_path("a/./b").is_err());
        assert!(validate_relative_path("a/../b").is_err());
        assert!(validate_relative_path("a/b\0c").is_err());
        assert!(validate_relative_path(&path_with_len(4095)).is_ok());
        assert!(validate_relative_path(&path_with_len(4097)).is_err());
        assert_eq!(validate_relative_path("a/b").unwrap().as_str(), "a/b");
        assert_eq!(
            validate_relative_path("a/b/c")
                .unwrap()
                .components()
                .collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
    }

    #[test]
    fn open_directory_beneath_opens_nested_directory() {
        let base = unique_test_dir("openat2_nested");
        std::fs::create_dir_all(base.join("root/a/b")).unwrap();
        let root = std::fs::File::open(base.join("root")).unwrap();
        let path = ValidatedRelativePath::new("a/b").unwrap();
        let opened = open_directory_beneath(root.as_raw_fd(), path).unwrap();
        let stat = fstat(opened.as_raw_fd()).unwrap();
        assert_eq!(stat.st_mode & libc::S_IFMT, libc::S_IFDIR);
        // SAFETY: `opened` owns a valid descriptor for the duration of the call.
        let descriptor_flags = unsafe { libc::fcntl(opened.as_raw_fd(), libc::F_GETFD) };
        assert_ne!(descriptor_flags, -1);
        assert_ne!(descriptor_flags & libc::FD_CLOEXEC, 0);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn open_directory_beneath_rejects_symlink_escape() {
        use std::os::unix::fs::symlink;

        let base = unique_test_dir("openat2_symlink");
        std::fs::create_dir_all(base.join("root")).unwrap();
        std::fs::create_dir_all(base.join("outside/secret")).unwrap();
        let root = std::fs::File::open(base.join("root")).unwrap();
        let path = ValidatedRelativePath::new("link/secret").unwrap();
        symlink(base.join("outside"), base.join("root/link")).unwrap();
        assert!(open_directory_beneath(root.as_raw_fd(), path).is_err());
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn open_directory_beneath_enforces_kernel_beneath_and_directory_flags() {
        let base = unique_test_dir("openat2_policy");
        std::fs::create_dir_all(base.join("root")).unwrap();
        std::fs::create_dir_all(base.join("outside")).unwrap();
        std::fs::write(base.join("root/file"), b"not a directory").unwrap();
        let root = std::fs::File::open(base.join("root")).unwrap();

        let forged_escape = ValidatedRelativePath { path: "../outside" };
        assert!(open_directory_beneath(root.as_raw_fd(), forged_escape).is_err());

        let file = ValidatedRelativePath::new("file").unwrap();
        assert!(open_directory_beneath(root.as_raw_fd(), file).is_err());

        assert_eq!(RESOLVER_RESOLVE_FLAGS, 0x0e);
        assert_eq!(resolver_open_flags(), libc::O_DIRECTORY | libc::O_CLOEXEC);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn open_directory_beneath_does_not_fallback_on_enosys() {
        let base = unique_test_dir("openat2_enosys");
        std::fs::create_dir_all(base.join("root/a")).unwrap();
        let root = std::fs::File::open(base.join("root")).unwrap();
        let path = ValidatedRelativePath::new("a").unwrap();

        fault::reset();
        fault::inject_errno("openat2_beneath", 1, libc::ENOSYS);
        let error = open_directory_beneath(root.as_raw_fd(), path).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::ENOSYS));
        assert_eq!(fault::call_count("openat2_beneath"), 1);
        assert_eq!(fault::call_count("open_directory"), 0);
        fault::reset();
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn read_dir_for_each_visits_all_entries() {
        let tmp = std::env::temp_dir();
        let dir_name = format!("steadq_rdfe_test_{}", std::process::id());
        let dir_path = tmp.join(&dir_name);
        std::fs::create_dir_all(&dir_path).unwrap();
        std::fs::write(dir_path.join("a.txt"), b"x").unwrap();
        std::fs::write(dir_path.join("b.txt"), b"y").unwrap();

        let fd = std::fs::File::open(&dir_path).unwrap();
        use std::os::unix::io::AsRawFd;
        let mut names = Vec::new();
        let count = read_dir_for_each(fd.as_raw_fd(), |name| {
            names.push(name.as_bytes().to_vec());
            true
        })
        .unwrap();
        assert_eq!(count, 2);
        assert!(names.contains(&b"a.txt".to_vec()));
        assert!(names.contains(&b"b.txt".to_vec()));

        std::fs::remove_dir_all(&dir_path).ok();
    }

    #[test]
    fn read_dir_for_each_stops_early() {
        let tmp = std::env::temp_dir();
        let dir_name = format!("steadq_rdfe_stop_{}", std::process::id());
        let dir_path = tmp.join(&dir_name);
        std::fs::create_dir_all(&dir_path).unwrap();
        std::fs::write(dir_path.join("a.txt"), b"x").unwrap();
        std::fs::write(dir_path.join("b.txt"), b"y").unwrap();
        std::fs::write(dir_path.join("c.txt"), b"z").unwrap();

        let fd = std::fs::File::open(&dir_path).unwrap();
        use std::os::unix::io::AsRawFd;
        let mut count_seen = 0;
        let _count = read_dir_for_each(fd.as_raw_fd(), |_| {
            count_seen += 1;
            false // stop immediately
        })
        .unwrap();
        assert_eq!(count_seen, 1);

        std::fs::remove_dir_all(&dir_path).ok();
    }

    #[test]
    fn bounded_directory_read_preserves_distinct_non_utf8_names() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;

        let dir_path = unique_test_dir("raw-directory-names");
        std::fs::create_dir(&dir_path).unwrap();
        std::fs::write(dir_path.join("plain"), b"plain").unwrap();
        let first = OsStr::from_bytes(b"bad-\x80");
        let second = OsStr::from_bytes(b"bad-\x81");
        std::fs::write(dir_path.join(first), b"a").unwrap();
        std::fs::write(dir_path.join(second), b"b").unwrap();
        let dir = std::fs::File::open(&dir_path).unwrap();

        let mut entries = read_dir_entry_names_bounded_owned(dir.as_raw_fd(), 3, 510).unwrap();
        entries.sort();
        assert_eq!(entries[0].as_bytes(), b"bad-\x80");
        assert_eq!(entries[1].as_bytes(), b"bad-\x81");
        assert_eq!(entries[2].as_bytes(), b"plain");
        assert_eq!(entries[0].as_str(), None);
        assert_eq!(entries[1].as_str(), None);
        assert_eq!(entries[2].as_str(), Some("plain"));
        assert_eq!(entries[2].as_ascii_str(), Some("plain"));
        assert_eq!(format!("{:?}", entries[0]), "b\"bad-\\x80\"");

        let mut owned = read_dir_entries_owned(dir.as_raw_fd()).unwrap();
        owned.sort();
        assert_eq!(owned[0].as_bytes(), b"bad-\x80");
        assert_eq!(owned[1].as_bytes(), b"bad-\x81");

        let callback_dir = std::fs::File::open(&dir_path).unwrap();
        let mut visited = Vec::new();
        read_dir_for_each(callback_dir.as_raw_fd(), |entry| {
            visited.push(entry.as_bytes().to_vec());
            true
        })
        .unwrap();
        visited.sort();
        assert_eq!(visited[0], b"bad-\x80");
        assert_eq!(visited[1], b"bad-\x81");
        std::fs::remove_dir_all(dir_path).unwrap();
    }

    #[test]
    fn protocol_text_rejects_non_ascii_utf8() {
        let name = DirEntryName("café".as_bytes().to_vec());
        assert_eq!(name.as_str(), Some("café"));
        assert_eq!(name.as_ascii_str(), None);
    }

    #[test]
    fn bounded_directory_read_applies_thread_local_permutation() {
        let dir_path = unique_test_dir("permuted-directory-read");
        std::fs::create_dir(&dir_path).unwrap();
        for name in ["a", "b", "c", "d"] {
            std::fs::write(dir_path.join(name), name.as_bytes()).unwrap();
        }
        let dir = std::fs::File::open(&dir_path).unwrap();
        fault::reset();
        let baseline = read_dir_entry_names_bounded_owned(dir.as_raw_fd(), 4, usize::MAX).unwrap();

        for (rotation, reversed) in [(1, false), (3, false), (0, true), (2, true)] {
            let mut expected = baseline.clone();
            let rotation = rotation % expected.len();
            expected.rotate_left(rotation);
            if reversed {
                expected.reverse();
            }
            fault::permute_readdir(rotation, reversed);
            let actual =
                read_dir_entry_names_bounded_owned(dir.as_raw_fd(), 4, usize::MAX).unwrap();
            assert_eq!(actual, expected, "rotation={rotation} reversed={reversed}");
        }

        fault::reset();
        assert_eq!(
            read_dir_entry_names_bounded_owned(dir.as_raw_fd(), 4, usize::MAX).unwrap(),
            baseline
        );
        std::fs::remove_dir_all(dir_path).unwrap();
    }

    #[test]
    fn bounded_directory_read_rejects_entry_and_byte_overflow() {
        let dir_path = unique_test_dir("bounded-directory-read");
        std::fs::create_dir(&dir_path).unwrap();
        std::fs::write(dir_path.join("a"), b"a").unwrap();
        std::fs::write(dir_path.join("bb"), b"b").unwrap();
        let dir = std::fs::File::open(&dir_path).unwrap();

        let entry_error =
            read_dir_entry_names_bounded_owned(dir.as_raw_fd(), 1, usize::MAX).unwrap_err();
        assert_eq!(entry_error.kind(), io::ErrorKind::FileTooLarge);
        let byte_error = read_dir_entry_names_bounded_owned(dir.as_raw_fd(), 2, 2).unwrap_err();
        assert_eq!(byte_error.kind(), io::ErrorKind::FileTooLarge);
        let mut exact_entries = read_dir_entry_names_bounded_owned(dir.as_raw_fd(), 2, 3).unwrap();
        exact_entries.sort();
        assert_eq!(exact_entries[0].as_bytes(), b"a");
        assert_eq!(exact_entries[1].as_bytes(), b"bb");
        std::fs::remove_dir_all(dir_path).unwrap();
    }

    #[test]
    fn owned_directory_read_returns_exact_name_bytes() {
        let dir_path = unique_test_dir("owned-directory-read");
        std::fs::create_dir(&dir_path).unwrap();
        std::fs::write(dir_path.join("alpha"), b"a").unwrap();
        std::fs::write(dir_path.join("beta"), b"b").unwrap();
        let dir = std::fs::File::open(&dir_path).unwrap();

        let mut entries = read_dir_entries_owned(dir.as_raw_fd()).unwrap();
        entries.sort();
        assert_eq!(entries[0].as_bytes(), b"alpha");
        assert_eq!(entries[1].as_bytes(), b"beta");
        std::fs::remove_dir_all(dir_path).unwrap();
    }

    #[test]
    fn bounded_directory_read_stops_at_cooperative_deadline() {
        let dir_path = unique_test_dir("bounded-directory-deadline");
        std::fs::create_dir(&dir_path).unwrap();
        std::fs::write(dir_path.join("alpha"), b"a").unwrap();
        let dir = std::fs::File::open(&dir_path).unwrap();
        let mut checks = 0;

        let error = read_dir_entry_names_bounded_owned_until_with_progress(
            dir.as_raw_fd(),
            usize::MAX,
            usize::MAX,
            || {
                checks += 1;
                Ok(checks == 1)
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            DirectoryEnumerationProgressError::Cancelled(DirectoryEnumerationProgress {
                entries_read: 0,
                name_bytes_read: 0,
            })
        ));
        assert_eq!(checks, 1);
        std::fs::remove_dir_all(dir_path).unwrap();
    }

    #[test]
    fn legacy_cancellable_directory_api_retains_its_result_shape() {
        let dir_path = unique_test_dir("bounded-directory-legacy-result");
        std::fs::create_dir(&dir_path).unwrap();
        std::fs::write(dir_path.join("alpha"), b"a").unwrap();
        let dir = std::fs::File::open(&dir_path).unwrap();

        let entries = read_dir_entry_names_bounded_owned_until(
            dir.as_raw_fd(),
            usize::MAX,
            usize::MAX,
            || Ok(false),
        )
        .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].as_bytes(), b"alpha");

        let error = read_dir_entry_names_bounded_owned_until(
            dir.as_raw_fd(),
            usize::MAX,
            usize::MAX,
            || Ok(true),
        )
        .unwrap_err();
        assert!(matches!(error, DirectoryEnumerationError::Cancelled));
        std::fs::remove_dir_all(dir_path).unwrap();
    }

    #[test]
    fn cancelled_directory_read_reports_partial_progress() {
        let dir_path = unique_test_dir("bounded-directory-partial-progress");
        std::fs::create_dir(&dir_path).unwrap();
        for index in 0..32 {
            std::fs::write(dir_path.join(format!("entry-{index:02}")), b"x").unwrap();
        }
        let dir = std::fs::File::open(&dir_path).unwrap();
        let mut checks = 0;

        let error = read_dir_entry_names_bounded_owned_until_with_progress(
            dir.as_raw_fd(),
            usize::MAX,
            usize::MAX,
            || {
                checks += 1;
                Ok(checks == 10)
            },
        )
        .unwrap_err();

        let progress = error.progress();
        assert!(matches!(
            error,
            DirectoryEnumerationProgressError::Cancelled(_)
        ));
        assert!(progress.entries_read > 0);
        assert!(progress.entries_read < 10);
        assert_eq!(progress.name_bytes_read, progress.entries_read * 8);
        std::fs::remove_dir_all(dir_path).unwrap();
    }

    #[test]
    fn bounded_directory_read_distinguishes_cancellation_check_failure() {
        let dir_path = unique_test_dir("bounded-directory-check-failure");
        std::fs::create_dir(&dir_path).unwrap();
        let dir = std::fs::File::open(&dir_path).unwrap();

        let error = read_dir_entry_names_bounded_owned_until_with_progress(
            dir.as_raw_fd(),
            usize::MAX,
            usize::MAX,
            || Err(io::Error::from_raw_os_error(libc::ETIMEDOUT)),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            DirectoryEnumerationProgressError::CancellationCheck {
                ref error,
                progress: DirectoryEnumerationProgress {
                    entries_read: 0,
                    name_bytes_read: 0,
                },
            } if error.raw_os_error() == Some(libc::ETIMEDOUT)
        ));
        std::fs::remove_dir_all(dir_path).unwrap();
    }

    #[test]
    fn directory_enumeration_error_preserves_category_and_source() {
        use std::error::Error as _;

        let cancelled = DirectoryEnumerationError::Cancelled;
        assert_eq!(cancelled.to_string(), "directory enumeration cancelled");
        assert!(cancelled.source().is_none());

        let check = DirectoryEnumerationError::CancellationCheck(io::Error::from_raw_os_error(
            libc::ETIMEDOUT,
        ));
        assert!(check
            .to_string()
            .starts_with("directory cancellation check failed:"));
        assert_eq!(
            check
                .source()
                .and_then(|source| source.downcast_ref::<io::Error>())
                .and_then(io::Error::raw_os_error),
            Some(libc::ETIMEDOUT)
        );

        let io_error = DirectoryEnumerationError::Io(io::Error::from_raw_os_error(libc::EIO));
        assert_eq!(
            io_error
                .source()
                .and_then(|source| source.downcast_ref::<io::Error>())
                .and_then(io::Error::raw_os_error),
            Some(libc::EIO)
        );

        let progress = DirectoryEnumerationProgress {
            entries_read: 2,
            name_bytes_read: 7,
        };
        let progress_error = DirectoryEnumerationProgressError::Io {
            error: io::Error::from_raw_os_error(libc::EIO),
            progress,
        };
        assert_eq!(
            progress_error.to_string(),
            io::Error::from_raw_os_error(libc::EIO).to_string()
        );
        assert_eq!(
            progress_error
                .source()
                .and_then(|source| source.downcast_ref::<io::Error>())
                .and_then(io::Error::raw_os_error),
            Some(libc::EIO)
        );
        assert_eq!(progress_error.progress(), progress);
    }

    #[test]
    fn bounded_directory_read_reports_the_overflow_sentinel() {
        let dir_path = unique_test_dir("bounded-directory-progress");
        std::fs::create_dir(&dir_path).unwrap();
        std::fs::write(dir_path.join("a"), b"a").unwrap();
        std::fs::write(dir_path.join("bb"), b"b").unwrap();
        let dir = std::fs::File::open(&dir_path).unwrap();

        let error = read_dir_entry_names_bounded_owned_until_with_progress(
            dir.as_raw_fd(),
            1,
            usize::MAX,
            || Ok(false),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            DirectoryEnumerationProgressError::Io {
                ref error,
                progress: DirectoryEnumerationProgress {
                    entries_read: 2,
                    name_bytes_read: 3,
                },
            } if error.kind() == io::ErrorKind::FileTooLarge
        ));
        std::fs::remove_dir_all(dir_path).unwrap();
    }

    #[test]
    fn fault_inject_fsync_fires_once() {
        fault::reset();
        fault::inject("fsync", 1);
        // Use a real fd (stdout). The fault fires before the syscall.
        let err = fsync(1).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EIO));
        // Second call is not faulted.
        let _ = fsync(1);
        fault::reset();
    }

    #[test]
    fn fault_inject_nth_call() {
        fault::reset();
        fault::inject("renameat2_noreplace", 2);
        let dir = tempfile_dir("nth");
        let fd = open_dir_absolute(&dir).unwrap();
        std::fs::write(dir.join("src1"), b"1").unwrap();
        std::fs::write(dir.join("src2"), b"2").unwrap();
        let r1 = renameat2_noreplace(fd.as_raw_fd(), "src1", fd.as_raw_fd(), "dst1");
        assert!(r1.is_ok(), "first call should succeed: {r1:?}");
        let r2 = renameat2_noreplace(fd.as_raw_fd(), "src2", fd.as_raw_fd(), "dst2");
        assert!(r2.is_err(), "second call should fault: {r2:?}");
        assert_eq!(r2.unwrap_err().raw_os_error(), Some(libc::EIO));
        fault::reset();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fault_inject_errno_enotdir() {
        fault::reset();
        fault::inject_errno("fstatat", 1, libc::ENOTDIR);
        let dir = tempfile_dir("enotdir");
        let fd = open_dir_absolute(&dir).unwrap();
        std::fs::write(dir.join("f"), b"x").unwrap();
        let err = fstatat(fd.as_raw_fd(), "f").unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ENOTDIR));
        fault::reset();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fault_idle_has_no_effect() {
        fault::reset();
        assert_eq!(fault::call_count("fsync"), 0);
        let dir = tempfile_dir("idle");
        let fd = open_dir_absolute(&dir).unwrap();
        fsync(fd.as_raw_fd()).unwrap();
        // Idle threads do not count checks when no faults are armed.
        assert_eq!(fault::call_count("fsync"), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn tempfile_dir(tag: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "steadq-fs-fault-{}-{}-{}",
            std::process::id(),
            tag,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn mkdirat_and_unlinkat_dir_round_trip() {
        let dir = tempfile_dir("mkdir");
        let fd = open_dir_absolute(&dir).unwrap();
        mkdirat(fd.as_raw_fd(), "child", 0o700).unwrap();
        // Open proves the directory was created (Ok(()) no-op mutant fails here).
        let child = open_directory(fd.as_raw_fd(), "child").unwrap();
        drop(child);
        unlinkat_dir(fd.as_raw_fd(), "child").unwrap();
        assert!(open_directory(fd.as_raw_fd(), "child").is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn unlinkat_removes_file() {
        let dir = tempfile_dir("unlink");
        let fd = open_dir_absolute(&dir).unwrap();
        std::fs::write(dir.join("f"), b"x").unwrap();
        fstatat(fd.as_raw_fd(), "f").unwrap();
        unlinkat(fd.as_raw_fd(), "f").unwrap();
        assert!(fstatat(fd.as_raw_fd(), "f").is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn renameat_moves_file() {
        let dir = tempfile_dir("renameat");
        let fd = open_dir_absolute(&dir).unwrap();
        std::fs::write(dir.join("a"), b"z").unwrap();
        renameat(fd.as_raw_fd(), "a", fd.as_raw_fd(), "b").unwrap();
        assert!(fstatat(fd.as_raw_fd(), "a").is_err());
        assert!(fstatat(fd.as_raw_fd(), "b").is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fsync_dir_and_fd_succeed() {
        let dir = tempfile_dir("fsyncdir");
        let fd = open_dir_absolute(&dir).unwrap();
        std::fs::write(dir.join("x"), b"1").unwrap();
        fsync_dir_fd(fd.as_raw_fd()).unwrap();
        // Nested child dir
        mkdirat(fd.as_raw_fd(), "nested", 0o700).unwrap();
        fsync_dir(fd.as_raw_fd(), "nested").unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fsync_dir_fd_honors_fault_injection() {
        // Whole-function Ok(()) mutants skip fault_check and would pass a bare
        // success test. Arming a fault requires the real function body.
        fault::reset();
        let dir = tempfile_dir("fsyncdir-fault");
        let fd = open_dir_absolute(&dir).unwrap();
        fault::inject("fsync_dir_fd", 1);
        let err = fsync_dir_fd(fd.as_raw_fd()).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EIO));
        fault::reset();
        fault::inject("fsync_dir", 1);
        mkdirat(fd.as_raw_fd(), "nested", 0o700).unwrap();
        let err = fsync_dir(fd.as_raw_fd(), "nested").unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EIO));
        fault::reset();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fsync_dir_fd_records_ordered_directory_identities() {
        fault::reset();
        let first_dir = tempfile_dir("fsyncdir-record-first");
        let second_dir = tempfile_dir("fsyncdir-record-second");
        let first_fd = open_dir_absolute(&first_dir).unwrap();
        let second_fd = open_dir_absolute(&second_dir).unwrap();
        let first_stat = fstat(first_fd.as_raw_fd()).unwrap();
        let second_stat = fstat(second_fd.as_raw_fd()).unwrap();
        let expected = [
            (first_stat.st_dev as u64, first_stat.st_ino as u64),
            (second_stat.st_dev as u64, second_stat.st_ino as u64),
        ];

        fault::inject("fsync_dir_fd", u64::MAX);
        fsync_dir_fd(first_fd.as_raw_fd()).unwrap();
        fsync_dir_fd(second_fd.as_raw_fd()).unwrap();
        assert_eq!(fault::fd_identities("fsync_dir_fd"), expected);
        assert_eq!(fault::call_count("fsync_dir_fd"), 2);

        let error = fault::record_fd_identity("invalid", -1).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EBADF));

        fault::reset();
        assert!(fault::fd_identities("fsync_dir_fd").is_empty());
        let _ = std::fs::remove_dir_all(&first_dir);
        let _ = std::fs::remove_dir_all(&second_dir);
    }

    #[test]
    fn clocks_return_plausible_values() {
        // Kill Ok(1) whole-function mutants: real clocks are far above 1.
        let boot = clock_boottime_ns().unwrap();
        let mono = clock_monotonic_ns().unwrap();
        let real = clock_realtime_ns().unwrap();
        assert!(boot > 1_000_000, "boottime too small: {boot}");
        assert!(mono > 1_000_000, "monotonic too small: {mono}");
        // Realtime after 2020-01-01 in nanoseconds.
        assert!(
            real > 1_577_836_800_000_000_000,
            "realtime too small: {real}"
        );
    }

    #[test]
    fn pwrite_pread_round_trip() {
        let dir = tempfile_dir("pwrite");
        let fd = open_dir_absolute(&dir).unwrap();
        let file = openat(
            fd.as_raw_fd(),
            "blob",
            libc::O_CREAT | libc::O_RDWR | libc::O_CLOEXEC,
            0o600,
        )
        .unwrap();
        let data = b"hello-steadq";
        let n = pwrite(file.as_raw_fd(), data, 0).unwrap();
        assert_eq!(n, data.len());
        fsync(file.as_raw_fd()).unwrap();
        let mut buf = vec![0u8; data.len()];
        let r = pread(file.as_raw_fd(), &mut buf, 0).unwrap();
        assert_eq!(r, data.len());
        assert_eq!(&buf, data);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn durable_move_noreplace_moves_and_syncs() {
        let dir = tempfile_dir("dmnr");
        let fd = open_dir_absolute(&dir).unwrap();
        mkdirat(fd.as_raw_fd(), "src", 0o700).unwrap();
        mkdirat(fd.as_raw_fd(), "dst", 0o700).unwrap();
        let src = open_directory(fd.as_raw_fd(), "src").unwrap();
        let dst = open_directory(fd.as_raw_fd(), "dst").unwrap();
        std::fs::write(dir.join("src/f"), b"payload").unwrap();
        durable_move_noreplace(src.as_raw_fd(), "f", dst.as_raw_fd(), "f").unwrap();
        assert!(fstatat(src.as_raw_fd(), "f").is_err());
        assert!(fstatat(dst.as_raw_fd(), "f").is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn durable_move_replace_overwrites() {
        let dir = tempfile_dir("dmr");
        let fd = open_dir_absolute(&dir).unwrap();
        mkdirat(fd.as_raw_fd(), "src", 0o700).unwrap();
        mkdirat(fd.as_raw_fd(), "dst", 0o700).unwrap();
        let src = open_directory(fd.as_raw_fd(), "src").unwrap();
        let dst = open_directory(fd.as_raw_fd(), "dst").unwrap();
        std::fs::write(dir.join("src/f"), b"new").unwrap();
        std::fs::write(dir.join("dst/f"), b"old").unwrap();
        durable_move_replace(src.as_raw_fd(), "f", dst.as_raw_fd(), "f").unwrap();
        assert!(fstatat(src.as_raw_fd(), "f").is_err());
        let st = fstatat(dst.as_raw_fd(), "f").unwrap();
        assert_eq!(st.st_size as usize, 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn linkat_tmpfile_publication_paths() {
        let dir = tempfile_dir("tmpfile");
        let fd = open_dir_absolute(&dir).unwrap();
        // O_TMPFILE may be unsupported on some filesystems; skip if so.
        let tmp = match open_tmpfile(fd.as_raw_fd()) {
            Ok(t) => t,
            Err(_) => {
                let _ = std::fs::remove_dir_all(&dir);
                return;
            }
        };
        write_all(tmp.as_raw_fd(), b"tmp").unwrap();
        // Prefer empty_path; fall back to proc path.
        let linked = linkat_empty_path(tmp.as_raw_fd(), fd.as_raw_fd(), "pub1")
            .or_else(|_| linkat_proc_self_fd(tmp.as_raw_fd(), fd.as_raw_fd(), "pub1"));
        assert!(linked.is_ok(), "tmpfile link failed: {linked:?}");
        assert!(fstatat(fd.as_raw_fd(), "pub1").is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn linkat_proc_self_fd_honors_fault_injection() {
        // empty_path may succeed first in the publication test and leave
        // linkat_proc_self_fd unexercised. Arm a fault so the real body runs.
        fault::reset();
        let dir = tempfile_dir("linkat-proc-fault");
        let fd = open_dir_absolute(&dir).unwrap();
        let tmp = match open_tmpfile(fd.as_raw_fd()) {
            Ok(t) => t,
            Err(_) => {
                let _ = std::fs::remove_dir_all(&dir);
                return;
            }
        };
        fault::inject("linkat_proc_self_fd", 1);
        let err = linkat_proc_self_fd(tmp.as_raw_fd(), fd.as_raw_fd(), "pub-fault").unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EIO));
        fault::reset();
        // Real publication via proc path when empty_path is not used.
        linkat_proc_self_fd(tmp.as_raw_fd(), fd.as_raw_fd(), "pub-proc").unwrap();
        assert!(fstatat(fd.as_raw_fd(), "pub-proc").is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
