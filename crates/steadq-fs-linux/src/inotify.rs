// inotify wake hints for the lease scan loop. Watches are advisory only:
// the scan remains the sole source of truth. Every failure path falls back
// to the plain timed sleep, so a watch can never affect correctness.

use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::path::Path;
use std::time::Duration;

use crate::cstr_from_bytes;

/// Create a nonblocking close-on-exec inotify descriptor.
pub fn init() -> io::Result<OwnedFd> {
    fault_check!("inotify_init");
    // SAFETY: plain descriptor creation; the flags are integer constants.
    let fd = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a nonnegative inotify_init1 result is a newly owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Watch `path` for renames into it (IN_MOVED_TO). Events also fire for
/// moves within the directory, such as colocated lease claims; the scan
/// sorts truth from noise, so no name filtering is done here.
pub fn add_moved_to_watch(fd: BorrowedFd<'_>, path: &Path) -> io::Result<()> {
    fault_check!("inotify_add_watch");
    use std::os::unix::ffi::OsStrExt;
    let c_path = cstr_from_bytes(path.as_os_str().as_bytes())?;
    // SAFETY: `c_path` is NUL-terminated and lives across the call.
    let wd = unsafe {
        libc::inotify_add_watch(
            fd.as_raw_fd(),
            c_path.as_ptr(),
            libc::IN_MOVED_TO | libc::IN_Q_OVERFLOW,
        )
    };
    if wd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Wait up to `timeout` for watch events. Returns Ok(true) when events
/// fired; the events are drained. EINTR reports a timeout so the caller's
/// backoff schedule is unaffected by signals.
pub fn wait_readable(fd: BorrowedFd<'_>, timeout: Duration) -> io::Result<bool> {
    fault_check!("inotify_wait");
    let mut pfd = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let ts = libc::timespec {
        tv_sec: timeout.as_secs() as i64,
        tv_nsec: timeout.subsec_nanos() as i64,
    };
    // SAFETY: `pfd` is writable for one pollfd and `ts` is readable; ppoll
    // retains neither pointer after returning.
    let rc = unsafe { libc::ppoll(&mut pfd, 1, &ts, std::ptr::null()) };
    if rc < 0 {
        let e = io::Error::last_os_error();
        if e.kind() == io::ErrorKind::Interrupted {
            return Ok(false);
        }
        return Err(e);
    }
    if rc == 0 || pfd.revents & libc::POLLIN == 0 {
        return Ok(false);
    }
    drain(fd);
    Ok(true)
}

/// Read and discard pending events so the descriptor stops reading ready.
fn drain(fd: BorrowedFd<'_>) {
    let mut buf = [0u8; 4096];
    loop {
        // SAFETY: `buf` is writable for its full length for the duration of
        // the call, which never retains the pointer.
        let n = unsafe { libc::read(fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
        if n <= 0 {
            // EAGAIN once the queue is empty; on other errors the hint is
            // simply dropped: unread events only cause a spurious wake.
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsFd;

    #[test]
    fn moved_to_event_wakes_wait() {
        let dir = tempfile::tempdir().unwrap();
        let fd = init().unwrap();
        add_moved_to_watch(fd.as_fd(), dir.path()).unwrap();

        // No events: a short wait times out.
        assert!(!wait_readable(fd.as_fd(), Duration::from_millis(10)).unwrap());

        // A rename into the watched directory wakes the next wait.
        std::fs::write(dir.path().join("a"), b"x").unwrap();
        std::fs::rename(dir.path().join("a"), dir.path().join("b")).unwrap();
        assert!(wait_readable(fd.as_fd(), Duration::from_millis(1000)).unwrap());
        // Drained: the next wait times out again.
        assert!(!wait_readable(fd.as_fd(), Duration::from_millis(10)).unwrap());
    }

    #[test]
    fn add_watch_missing_dir_fails() {
        let dir = tempfile::tempdir().unwrap();
        let fd = init().unwrap();
        assert!(add_moved_to_watch(fd.as_fd(), &dir.path().join("nope")).is_err());
    }
}
