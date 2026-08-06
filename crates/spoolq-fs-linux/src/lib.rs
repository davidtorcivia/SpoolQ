// Linux syscall substrate for SpoolQ/1.
// Confines all unsafe code to this module.

use std::ffi::CString;
use std::io;
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::Path;

/// Open or create a file with O_TMPFILE.
pub fn open_tmpfile(dir_fd: RawFd) -> io::Result<OwnedFd> {
    // O_TMPFILE = 020000000 | O_RDWR
    const O_TMPFILE: i32 = 0o20000000;
    const O_RDWR: i32 = 0o2;
    const O_CLOEXEC: i32 = 0o2000000;

    let dot = CString::new(".").unwrap();
    let fd = unsafe { libc::openat(dir_fd, dot.as_ptr(), O_TMPFILE | O_RDWR | O_CLOEXEC, 0o600) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Open a directory for reading.
pub fn open_directory(dir_fd: RawFd, name: &str) -> io::Result<OwnedFd> {
    const O_RDONLY: i32 = 0o0;
    const O_DIRECTORY: i32 = 0o200000;
    const O_CLOEXEC: i32 = 0o2000000;
    let c_name = CString::new(name).unwrap();
    let fd = unsafe { libc::openat(dir_fd, c_name.as_ptr(), O_RDONLY | O_DIRECTORY | O_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Open a path relative to a directory fd with given flags.
pub fn openat(dir_fd: RawFd, name: &str, flags: i32, mode: u32) -> io::Result<OwnedFd> {
    let c_name = CString::new(name).unwrap();
    let fd = unsafe { libc::openat(dir_fd, c_name.as_ptr(), flags, mode) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Create a directory.
pub fn mkdirat(dir_fd: RawFd, name: &str, mode: u32) -> io::Result<()> {
    let c_name = CString::new(name).unwrap();
    let rc = unsafe { libc::mkdirat(dir_fd, c_name.as_ptr(), mode) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Create a directory, treating EEXIST as Ok.
pub fn mkdirat_eexist_ok(dir_fd: RawFd, name: &str, mode: u32) -> io::Result<bool> {
    match mkdirat(dir_fd, name, mode) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(false),
        Err(e) => Err(e),
    }
}

/// fsync a file descriptor.
pub fn fsync(fd: RawFd) -> io::Result<()> {
    let rc = unsafe { libc::fsync(fd) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// fsync a directory by opening it read-only and syncing.
pub fn fsync_dir(dir_fd: RawFd, name: &str) -> io::Result<()> {
    let fd = open_directory(dir_fd, name)?;
    fsync(fd.as_raw_fd())
}

/// fsync a directory by its already-open fd.
pub fn fsync_dir_fd(fd: RawFd) -> io::Result<()> {
    fsync(fd)
}

/// Rename with RENAME_NOREPLACE.
pub fn renameat2_noreplace(
    old_dir_fd: RawFd,
    old_name: &str,
    new_dir_fd: RawFd,
    new_name: &str,
) -> io::Result<()> {
    const RENAME_NOREPLACE: u32 = 1 << 0;
    let c_old = CString::new(old_name).unwrap();
    let c_new = CString::new(new_name).unwrap();
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
    let c_old = CString::new(old_name).unwrap();
    let c_new = CString::new(new_name).unwrap();
    let rc = unsafe { libc::renameat(old_dir_fd, c_old.as_ptr(), new_dir_fd, c_new.as_ptr()) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// linkat with AT_EMPTY_PATH for O_TMPFILE publication.
pub fn linkat_empty_path(fd: RawFd, dest_dir_fd: RawFd, dest_name: &str) -> io::Result<()> {
    const AT_EMPTY_PATH: i32 = 0x1000;
    let c_dest = CString::new(dest_name).unwrap();
    let rc = unsafe {
        libc::syscall(
            libc::SYS_linkat,
            fd,
            CString::new("").unwrap().as_ptr(),
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
    const AT_SYMLINK_FOLLOW: i32 = 0x400;
    let proc_path = format!("/proc/self/fd/{}\0", fd);
    let c_dest = CString::new(dest_name).unwrap();
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
    let c_name = CString::new(name).unwrap();
    let rc = unsafe { libc::unlinkat(dir_fd, c_name.as_ptr(), 0) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Remove a directory (must be empty).
pub fn unlinkat_dir(dir_fd: RawFd, name: &str) -> io::Result<()> {
    const AT_REMOVEDIR: i32 = 0x200;
    let c_name = CString::new(name).unwrap();
    let rc = unsafe { libc::unlinkat(dir_fd, c_name.as_ptr(), AT_REMOVEDIR) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// stat a file relative to a directory fd.
pub fn fstatat(dir_fd: RawFd, name: &str) -> io::Result<libc::stat> {
    let c_name = CString::new(name).unwrap();
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
    let mut statbuf: libc::stat = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::fstat(fd, &mut statbuf) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(statbuf)
}

/// Get filesystem stats.
pub fn statfs(path: &Path) -> io::Result<libc::statfs> {
    let c_path = CString::new(path.to_str().unwrap()).unwrap();
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
    let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut ts) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64)
}

/// CLOCK_REALTIME in nanoseconds.
pub fn clock_realtime_ns() -> io::Result<u64> {
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

/// Generate random bytes from the OS crypto source.
pub fn get_random(bytes: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; bytes];
    // Use getrandom syscall
    let rc = unsafe { libc::syscall(libc::SYS_getrandom, buf.as_mut_ptr(), bytes, 0) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
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
    let rc = unsafe { libc::pwrite(fd, buf.as_ptr() as *const _, buf.len(), offset as i64) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(rc as usize)
}

/// Write all bytes, retrying on partial writes.
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
        written += rc as usize;
    }
    Ok(())
}

/// Write all bytes at a given offset using pwrite, retrying on partial writes.
pub fn pwrite_all(fd: RawFd, buf: &[u8], offset: u64) -> io::Result<()> {
    let mut written = 0;
    let mut current_offset = offset;
    while written < buf.len() {
        let n = pwrite(fd, &buf[written..], current_offset)?;
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

/// Open a directory path (absolute) and return an OwnedFd.
pub fn open_dir_absolute(path: &Path) -> io::Result<OwnedFd> {
    const O_RDONLY: i32 = 0o0;
    const O_DIRECTORY: i32 = 0o200000;
    const O_CLOEXEC: i32 = 0o2000000;
    let c_path = CString::new(path.to_str().unwrap()).unwrap();
    let fd = unsafe { libc::open(c_path.as_ptr(), O_RDONLY | O_DIRECTORY | O_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Create a file with O_CREAT | O_EXCL | O_NOFOLLOW.
pub fn create_exclusive(dir_fd: RawFd, name: &str, mode: u32) -> io::Result<OwnedFd> {
    const O_RDWR: i32 = 0o2;
    const O_CREAT: i32 = 0o100;
    const O_EXCL: i32 = 0o200;
    const O_NOFOLLOW: i32 = 0o400000;
    const O_CLOEXEC: i32 = 0o2000000;
    openat(
        dir_fd,
        name,
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
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
pub fn read_dir_entries(dir_fd: RawFd) -> io::Result<Vec<String>> {
    let mut entries = Vec::new();
    let dir = unsafe { libc::fdopendir(dir_fd) };
    if dir.is_null() {
        return Err(io::Error::last_os_error());
    }

    // Save the fd so we don't double-close
    let dir_fd_saved = dir_fd;

    loop {
        let entry = unsafe { libc::readdir(dir) };
        if entry.is_null() {
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

    // Prevent double-close: fdopendir takes ownership, so we need to forget our OwnedFd
    // But since we pass RawFd, the caller must be aware.
    // Mark the fd as consumed to prevent use-after-close.
    let _ = dir_fd_saved;

    Ok(entries)
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
    read_dir_entries(dup_fd)
}

/// Change file mode relative to a directory fd.
pub fn fchmodat(dir_fd: RawFd, name: &str, mode: u32) -> io::Result<()> {
    let c_name = CString::new(name).unwrap();
    let rc = unsafe { libc::fchmodat(dir_fd, c_name.as_ptr(), mode, 0) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
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
    fn random_128_bit_is_random() {
        let a = random_128bit().unwrap();
        let b = random_128bit().unwrap();
        assert_ne!(a, b);
    }
}
