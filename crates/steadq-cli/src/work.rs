// Worker loop: lease a job, feed its payload to a command on stdin, renew
// the lease while the command runs, ack on exit 0, requeue on nonzero.
// A crash mid-job is covered by lease expiry: recovery reaps the lease and
// the job re-runs (at-least-once).

use std::io::Write;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use steadq_core::{
    AckOutcome, LeaseInfo, LeaseOutcome, OpenOptions, Queue, RenewOutcome, TransitionOutcome,
    VerifiedPayloadReader,
};

const CHUNK: usize = 64 * 1024;
const POLL: Duration = Duration::from_millis(50);
// Bounded wait between scans in loop mode; the lease() backoff paces inside.
const SCAN_WAIT_NS: u64 = 1_000_000_000;

pub fn run(
    path: &Path,
    concurrency: u32,
    lease_duration_ns: u64,
    once: bool,
    command: &[String],
) -> u8 {
    let mut handles = Vec::new();
    for _ in 0..concurrency {
        let path = path.to_path_buf();
        let command = command.to_vec();
        handles.push(std::thread::spawn(move || {
            worker(path, lease_duration_ns, once, command)
        }));
    }
    let mut code = 0;
    for handle in handles {
        match handle.join() {
            Ok(c) => code = code.max(c),
            Err(_) => {
                eprintln!("worker thread panicked");
                code = code.max(1);
            }
        }
    }
    code
}

fn worker(
    path: std::path::PathBuf,
    lease_duration_ns: u64,
    once: bool,
    command: Vec<String>,
) -> u8 {
    let mut queue = match Queue::open(&path, &OpenOptions::default()) {
        Ok(q) => q,
        Err(e) => {
            eprintln!("open failed: {e}");
            return crate::core_exit_code(&e);
        }
    };
    loop {
        let wait_ns = if once { 0 } else { SCAN_WAIT_NS };
        let lease = match queue.lease(wait_ns, lease_duration_ns) {
            LeaseOutcome::Leased(lease) => lease,
            LeaseOutcome::Empty if once => return 0,
            LeaseOutcome::Empty => continue,
            LeaseOutcome::NotCommitted(e) => {
                eprintln!("lease failed: {e}");
                return crate::core_exit_code(&e);
            }
            LeaseOutcome::OutcomeUnknown(ticket) => {
                eprintln!(
                    "lease outcome unknown: job {}",
                    steadq_names::hex_encode(&ticket.job_id())
                );
                return 2;
            }
        };
        let code = run_one(&mut queue, lease, lease_duration_ns, &command);
        if once {
            return code;
        }
    }
}

fn run_one(
    queue: &mut Queue,
    mut lease: LeaseInfo,
    lease_duration_ns: u64,
    command: &[String],
) -> u8 {
    let reader = match queue.open_verified_payload_reader(&lease) {
        Ok(Some(reader)) => reader,
        Ok(None) => {
            eprintln!("lease source vanished");
            return 1;
        }
        Err(e) => {
            eprintln!("payload verification failed: {e}");
            return crate::core_exit_code(&e);
        }
    };

    let mut child = match Command::new(&command[0])
        .args(&command[1..])
        .stdin(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => {
            eprintln!("spawn {} failed: {e}", command[0]);
            // 127: command not found. Requeue; attempts cap eventually buries
            // a permanently missing command.
            return requeue(queue, &lease).max(127);
        }
    };

    let stdin = child.stdin.take();
    let feeder = std::thread::spawn(move || feed(reader, stdin));
    let status = babysit(queue, &mut lease, &mut child, lease_duration_ns);
    let _ = feeder.join();

    match status {
        Ok(status) if status.success() => finish(queue, &lease),
        Ok(status) => {
            let code = status.code().unwrap_or(1).max(1) as u8;
            requeue(queue, &lease).max(code)
        }
        Err(e) => {
            eprintln!("wait failed: {e}");
            6
        }
    }
}

/// Stream the verified payload into the child's stdin. A write error means
/// the child stopped reading (EPIPE after early exit); drop the rest.
fn feed(reader: VerifiedPayloadReader, stdin: Option<std::process::ChildStdin>) {
    let Some(mut stdin) = stdin else { return };
    let mut buf = vec![0u8; CHUNK];
    let mut offset = 0u64;
    loop {
        match reader.read_at(&mut buf, offset) {
            Ok(0) => break,
            Ok(n) => {
                if stdin.write_all(&buf[..n]).is_err() {
                    break;
                }
                offset += n as u64;
            }
            Err(_) => break,
        }
    }
}

/// Wait for the child while renewing the lease at half its duration.
/// When renewal stops (lost or unknown), the child still finishes; the
/// closing ack reports the loss.
fn babysit(
    queue: &mut Queue,
    lease: &mut LeaseInfo,
    child: &mut Child,
    lease_duration_ns: u64,
) -> std::io::Result<ExitStatus> {
    let renew_every = Duration::from_nanos(lease_duration_ns / 2);
    let mut next_renew = Instant::now() + renew_every;
    let mut renewing = true;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if renewing && Instant::now() >= next_renew {
            match queue.renew(lease, lease_duration_ns) {
                RenewOutcome::Renewed(fresh) => {
                    *lease = fresh;
                    next_renew = Instant::now() + renew_every;
                }
                RenewOutcome::LeaseLost => {
                    eprintln!("lease lost; letting the job finish without renewal");
                    renewing = false;
                }
                RenewOutcome::NotCommitted(e) => {
                    eprintln!("renew failed: {e}; retrying next interval");
                    next_renew = Instant::now() + renew_every;
                }
                RenewOutcome::OutcomeUnknown(ticket) => {
                    eprintln!(
                        "renew outcome unknown: job {}",
                        steadq_names::hex_encode(&ticket.job_id())
                    );
                    renewing = false;
                }
            }
        }
        std::thread::sleep(POLL);
    }
}

/// Acknowledge a finished job. Exit 0 on ack; the ack's own outcome decides
/// everything else.
fn finish(queue: &mut Queue, lease: &LeaseInfo) -> u8 {
    match queue.ack(lease) {
        AckOutcome::Acked | AckOutcome::AlreadyAcked => 0,
        AckOutcome::LeaseLost => {
            eprintln!("lease lost");
            1
        }
        AckOutcome::NotCommitted(e) => {
            eprintln!("ack not committed: {e}");
            crate::core_exit_code(&e)
        }
        AckOutcome::OutcomeUnknown(ticket) => {
            eprintln!(
                "ack outcome unknown: job {}",
                steadq_names::hex_encode(&ticket.job_id())
            );
            2
        }
    }
}

/// Return a failed job to ready; the library routes exhausted attempts to
/// dead.
fn requeue(queue: &mut Queue, lease: &LeaseInfo) -> u8 {
    match queue.retry_now(lease) {
        TransitionOutcome::Committed => 0,
        TransitionOutcome::LeaseLost => {
            eprintln!("lease lost");
            1
        }
        TransitionOutcome::NotCommitted(e) => {
            eprintln!("retry not committed: {e}");
            crate::core_exit_code(&e)
        }
        TransitionOutcome::OutcomeUnknown(ticket) => {
            eprintln!(
                "retry outcome unknown: job {}",
                steadq_names::hex_encode(&ticket.job_id())
            );
            2
        }
    }
}
