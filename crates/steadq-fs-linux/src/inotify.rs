// inotify wake hints for the lease scan loop. Watches are advisory only:
// the scan remains the sole source of truth. Every failure path falls back
// to the plain timed sleep, so a watch can never affect correctness.

use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::path::Path;
use std::time::Duration;

use crate::cstr_from_bytes;

/// True when ppoll reports a readable event on the watched descriptor.
/// Pure so its operator logic is exhaustively table-tested.
fn ppoll_woke(rc: i32, revents: i16) -> bool {
    rc > 0 && revents & libc::POLLIN != 0
}

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

/// Watch `path` for objects appearing in it: IN_CREATE covers linkat
/// publication (the primary path on tmpfile-capable filesystems),
/// IN_MOVED_TO covers rename publication (named fallback, ZFS) and
/// delayed-to-ready promotion. The scan sorts truth from noise, so no
/// name filtering is done here.
pub fn add_appear_watch(fd: BorrowedFd<'_>, path: &Path) -> io::Result<()> {
    fault_check!("inotify_add_watch");
    use std::os::unix::ffi::OsStrExt;
    let c_path = cstr_from_bytes(path.as_os_str().as_bytes())?;
    // SAFETY: `c_path` is NUL-terminated and lives across the call.
    let wd = unsafe {
        libc::inotify_add_watch(
            fd.as_raw_fd(),
            c_path.as_ptr(),
            libc::IN_MOVED_TO | libc::IN_CREATE | libc::IN_Q_OVERFLOW,
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
        if is_interrupted(&io::Error::last_os_error()) {
            return Ok(false);
        }
        return Err(io::Error::last_os_error());
    }
    if !ppoll_woke(rc, pfd.revents) {
        return Ok(false);
    }
    drain(fd);
    Ok(true)
}

fn is_interrupted(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::Interrupted
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
    fn ppoll_woke_table() {
        let pollin: i16 = libc::POLLIN;
        let pollerr: i16 = libc::POLLERR;
        // Timeout: no descriptors ready.
        assert!(!ppoll_woke(0, 0));
        // Spurious or error-only wake without POLLIN.
        assert!(!ppoll_woke(1, 0));
        assert!(!ppoll_woke(1, pollerr));
        // Real wake.
        assert!(ppoll_woke(1, pollin));
        assert!(ppoll_woke(1, pollin | pollerr));
        // Error return never reports a wake.
        assert!(!ppoll_woke(-1, pollin));
        // A timeout with stale revents never reports a wake.
        assert!(!ppoll_woke(0, pollin));
    }

    #[test]
    fn link_create_event_wakes_wait() {
        let dir = tempfile::tempdir().unwrap();
        let fd = init().unwrap();
        add_appear_watch(fd.as_fd(), dir.path()).unwrap();

        // A plain file creation (the linkat-publication signal) wakes the
        // wait without any rename.
        std::fs::write(dir.path().join("linked"), b"x").unwrap();
        assert!(wait_readable(fd.as_fd(), Duration::from_millis(1000)).unwrap());
        assert!(!wait_readable(fd.as_fd(), Duration::from_millis(10)).unwrap());
    }

    #[test]
    fn moved_to_event_wakes_wait() {
        let dir = tempfile::tempdir().unwrap();
        let fd = init().unwrap();
        add_appear_watch(fd.as_fd(), dir.path()).unwrap();

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
    fn drain_spans_multiple_reads() {
        let dir = tempfile::tempdir().unwrap();
        let fd = init().unwrap();
        add_appear_watch(fd.as_fd(), dir.path()).unwrap();

        // Enough renames that the pending events exceed one 4096-byte read.
        for i in 0..300u32 {
            let name = format!("f{i}");
            std::fs::write(dir.path().join(&name), b"x").unwrap();
            std::fs::rename(dir.path().join(&name), dir.path().join(format!("g{i}"))).unwrap();
        }
        assert!(wait_readable(fd.as_fd(), Duration::from_millis(1000)).unwrap());
        // One wait must drain every buffered event, not just the first read.
        assert!(
            !wait_readable(fd.as_fd(), Duration::from_millis(50)).unwrap(),
            "events survived the drain"
        );
    }

    #[test]
    fn is_interrupted_table() {
        assert!(is_interrupted(&io::Error::new(
            io::ErrorKind::Interrupted,
            "eintr",
        )));
        assert!(is_interrupted(&io::Error::from_raw_os_error(libc::EINTR)));
        assert!(!is_interrupted(&io::Error::from_raw_os_error(libc::EBADF)));
        assert!(!is_interrupted(&io::Error::from_raw_os_error(libc::EIO)));
    }

    #[test]
    fn add_watch_missing_dir_fails() {
        let dir = tempfile::tempdir().unwrap();
        let fd = init().unwrap();
        assert!(add_appear_watch(fd.as_fd(), &dir.path().join("nope")).is_err());
    }
}
