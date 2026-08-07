// Linux syscall substrate for SpoolQ/1.
// Confines all unsafe code to this module.

use std::ffi::CString;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::Path;

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

    #[derive(Clone, Copy, Debug)]
    struct Fault {
        current: u64,
        target: u64,
        errno: i32,
    }

    struct State {
        faults: HashMap<String, Fault>,
        counts: HashMap<String, u64>,
    }

    impl State {
        fn new() -> Self {
            State {
                faults: HashMap::new(),
                counts: HashMap::new(),
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

/// Read directory entries.
/// Consumes the fd via fdopendir.
fn read_dir_entries_impl(dir_fd: RawFd) -> io::Result<Vec<String>> {
    let mut entries = Vec::new();
    let dir = unsafe { libc::fdopendir(dir_fd) };
    if dir.is_null() {
        return Err(io::Error::last_os_error());
    }

    loop {
        // B5: Set errno to 0 before readdir to distinguish EOF from error.
        unsafe { *libc::__errno_location() = 0 };
        let entry = unsafe { libc::readdir(dir) };
        if entry.is_null() {
            // B5: Check errno to distinguish EOF from error.
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
        let name = String::from_utf8_lossy(name_bytes).to_string();
        if name != "." && name != ".." {
            entries.push(name);
        }
    }

    unsafe { libc::closedir(dir) };

    Ok(entries)
}

/// R4-PERF: Iterate directory entries with a callback, avoiding full
/// materialization. The callback returns true to continue, false to stop.
/// Returns the number of entries processed.
pub fn read_dir_for_each<F: FnMut(&str) -> bool>(dir_fd: RawFd, mut f: F) -> io::Result<usize> {
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
        let name = String::from_utf8_lossy(name_bytes);
        if name != "." && name != ".." {
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
pub fn read_dir_entries(dir_fd: RawFd) -> io::Result<Vec<String>> {
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
pub fn read_dir_entries_owned(dir_fd: RawFd) -> io::Result<Vec<String>> {
    // dup the fd so fdopendir doesn't consume the original
    let dup_fd = unsafe { libc::dup(dir_fd) };
    if dup_fd < 0 {
        return Err(io::Error::last_os_error());
    }
    read_dir_entries_impl(dup_fd)
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
/// rejects absolute paths, '..', empty components, and NUL.
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
    if comp.starts_with('/') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "absolute path component in relative path",
        ));
    }
    if comp.contains('\0') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "NUL byte in path component",
        ));
    }
    Ok(())
}

/// Validate a relative path for safety: split into components and validate each.
pub fn validate_relative_path(path: &str) -> io::Result<Vec<&str>> {
    let components: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    for comp in &components {
        validate_path_component(comp)?;
    }
    Ok(components)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(validate_path_component("ok").is_ok());
    }

    #[test]
    fn read_dir_for_each_visits_all_entries() {
        let tmp = std::env::temp_dir();
        let dir_name = format!("spoolq_rdfe_test_{}", std::process::id());
        let dir_path = tmp.join(&dir_name);
        std::fs::create_dir_all(&dir_path).unwrap();
        std::fs::write(dir_path.join("a.txt"), b"x").unwrap();
        std::fs::write(dir_path.join("b.txt"), b"y").unwrap();

        let fd = std::fs::File::open(&dir_path).unwrap();
        use std::os::unix::io::AsRawFd;
        let mut names = Vec::new();
        let count = read_dir_for_each(fd.as_raw_fd(), |name| {
            names.push(name.to_string());
            true
        })
        .unwrap();
        assert_eq!(count, 2);
        assert!(names.contains(&"a.txt".to_string()));
        assert!(names.contains(&"b.txt".to_string()));

        std::fs::remove_dir_all(&dir_path).ok();
    }

    #[test]
    fn read_dir_for_each_stops_early() {
        let tmp = std::env::temp_dir();
        let dir_name = format!("spoolq_rdfe_stop_{}", std::process::id());
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
            "spoolq-fs-fault-{}-{}-{}",
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
        let data = b"hello-spoolq";
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
}
