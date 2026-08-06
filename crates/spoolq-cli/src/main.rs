// SpoolQ command-line interface.

use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use spoolq_core::{CreateOptions, EnqueueOutcome, LeaseOutcome, OpenOptions, Queue};

#[derive(Parser)]
#[command(name = "spoolq", about = "Crash-safe filesystem queue")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new queue
    Init {
        path: PathBuf,
        #[arg(long, default_value = "64")]
        shards: u32,
        #[arg(long, default_value = "3600000000000")]
        terminal_bucket_width_ns: u64,
    },
    /// Enqueue a job
    Put {
        path: PathBuf,
        /// Input file, or - for stdin
        file: Option<String>,
        #[arg(long, default_value = "application/octet-stream")]
        content_type: String,
        #[arg(long, default_value = "3")]
        max_attempts: u32,
        #[arg(long)]
        not_before: Option<u64>,
        #[arg(long)]
        producer_id: Option<String>,
    },
    /// Lease a job
    Lease {
        path: PathBuf,
        #[arg(long, default_value = "30")]
        duration_seconds: u64,
        #[arg(long)]
        handle_file: Option<PathBuf>,
    },
    /// Stats
    Stats { path: PathBuf },
    /// Doctor: check environment
    Doctor {
        path: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Acknowledge a lease
    Ack {
        path: PathBuf,
        #[arg(long)]
        handle_file: PathBuf,
    },
    /// Retry a lease
    Retry {
        path: PathBuf,
        #[arg(long)]
        handle_file: PathBuf,
        #[arg(long)]
        after_seconds: Option<u64>,
    },
    /// Bury a lease
    Bury {
        path: PathBuf,
        #[arg(long)]
        handle_file: PathBuf,
        #[arg(long, default_value = "0")]
        reason: u16,
    },
    /// Run a recovery pass
    Recover {
        path: PathBuf,
        #[arg(long)]
        watch: bool,
        #[arg(long, default_value = "1000")]
        budget_ops: u32,
        #[arg(long, default_value = "100")]
        budget_ms: u64,
    },
    /// Inspect a job by ID
    Inspect { path: PathBuf, job_id: String },
    /// Verify a job or receipt file
    Verify {
        file: PathBuf,
        #[arg(long)]
        deep: bool,
    },
    /// Dump format info for a file
    FormatDump { file: PathBuf },
}

fn parse_duration_seconds(s: u64) -> u64 {
    s * 1_000_000_000
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init {
            path,
            shards,
            terminal_bucket_width_ns,
        } => {
            let opts = CreateOptions {
                shard_count: shards,
                terminal_bucket_width_ns,
                ..Default::default()
            };
            match Queue::init(&path, &opts) {
                Ok(format) => {
                    eprintln!("initialized queue at {}", path.display());
                    eprintln!("queue_id: {}", spoolq_names::hex_encode(&format.queue_id));
                    eprintln!("shards: {}", format.shard_count);
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("init failed: {}", e);
                    ExitCode::FAILURE
                }
            }
        }

        Commands::Put {
            path,
            file,
            content_type,
            max_attempts,
            not_before,
            producer_id,
        } => {
            let payload = match file.as_deref() {
                Some("-") | None => {
                    use std::io::Read;
                    let mut buf = Vec::new();
                    std::io::stdin().read_to_end(&mut buf).unwrap_or(0);
                    buf
                }
                Some(f) => std::fs::read(f).unwrap_or_default(),
            };

            let queue = match Queue::open(&path, &OpenOptions::default()) {
                Ok(q) => q,
                Err(e) => {
                    eprintln!("open failed: {}", e);
                    return ExitCode::FAILURE;
                }
            };
            let mut queue = queue;

            let input = spoolq_core::EnqueueInput {
                maximum_attempts: max_attempts,
                content_type,
                payload,
                initial_not_before: not_before,
                producer_id,
                ..Default::default()
            };

            match queue.enqueue(input) {
                EnqueueOutcome::Committed(ticket) => {
                    println!("job_id: {}", spoolq_names::hex_encode(&ticket.job_id));
                    println!("path: {}", ticket.expected_relative_path);
                    ExitCode::SUCCESS
                }
                EnqueueOutcome::NotCommitted(ticket, err) => {
                    eprintln!("not committed: {}", err);
                    if ticket.job_id != [0; 16] {
                        eprintln!("job_id: {}", spoolq_names::hex_encode(&ticket.job_id));
                    }
                    ExitCode::FAILURE
                }
                EnqueueOutcome::OutcomeUnknown(ticket, err) => {
                    eprintln!("outcome unknown: {}", err);
                    eprintln!("job_id: {}", spoolq_names::hex_encode(&ticket.job_id));
                    eprintln!("path: {}", ticket.expected_relative_path);
                    ExitCode::from(2)
                }
            }
        }

        Commands::Lease {
            path,
            duration_seconds,
            handle_file,
        } => {
            let queue = match Queue::open(&path, &OpenOptions::default()) {
                Ok(q) => q,
                Err(e) => {
                    eprintln!("open failed: {}", e);
                    return ExitCode::FAILURE;
                }
            };
            let mut queue = queue;

            let duration_ns = parse_duration_seconds(duration_seconds);
            match queue.lease(0, duration_ns) {
                LeaseOutcome::Leased(lease) => {
                    if let Some(ref hf) = handle_file {
                        if let Err(e) = save_handle_to_file(&path, hf, &lease) {
                            eprintln!("warning: failed to write handle file: {}", e);
                        }
                    }
                    println!("job_id: {}", spoolq_names::hex_encode(&lease.job_id));
                    println!("generation: {}", lease.generation);
                    println!("attempt: {}/{}", lease.attempt, lease.maximum_attempts);
                    ExitCode::SUCCESS
                }
                LeaseOutcome::Empty => {
                    eprintln!("no jobs available");
                    ExitCode::FAILURE
                }
                LeaseOutcome::NotCommitted(e) => {
                    eprintln!("lease failed: {}", e);
                    ExitCode::FAILURE
                }
                LeaseOutcome::OutcomeUnknown(ticket) => {
                    eprintln!("outcome unknown");
                    eprintln!("job_id: {}", spoolq_names::hex_encode(&ticket.job_id));
                    ExitCode::from(2)
                }
            }
        }

        Commands::Stats { path } => {
            match Queue::open(&path, &OpenOptions::default()) {
                Ok(_queue) => {
                    // Basic stats: count files in each state
                    let root = &path;
                    for state in [
                        "ready",
                        "leased",
                        "delayed",
                        "receipts",
                        "dead",
                        "quarantine",
                    ] {
                        let state_path = root.join(state);
                        if state_path.exists() {
                            let count = count_files_recursive(&state_path);
                            println!("{}: {}", state, count);
                        }
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("open failed: {}", e);
                    ExitCode::FAILURE
                }
            }
        }

        Commands::Doctor { path, json } => {
            let mut results: Vec<(&str, String, bool)> = Vec::new();

            // boot_id
            match spoolq_fs_linux::read_boot_id() {
                Ok(id) => results.push(("boot_id", id, true)),
                Err(e) => results.push(("boot_id", e.to_string(), false)),
            }
            // clock_boottime
            match spoolq_fs_linux::clock_boottime_ns() {
                Ok(ns) => results.push(("clock_boottime", format!("{} ns", ns), true)),
                Err(e) => results.push(("clock_boottime", e.to_string(), false)),
            }
            // clock_realtime
            match spoolq_fs_linux::clock_realtime_ns() {
                Ok(ns) => results.push(("clock_realtime", format!("{} ns", ns), true)),
                Err(e) => results.push(("clock_realtime", e.to_string(), false)),
            }
            // getrandom
            match spoolq_fs_linux::random_128bit() {
                Ok(_) => results.push(("getrandom", "OK".to_string(), true)),
                Err(e) => results.push(("getrandom", e.to_string(), false)),
            }

            if path.exists() {
                // filesystem type
                match spoolq_fs_linux::statfs(&path) {
                    Ok(stat) => {
                        let ft = stat.f_type;
                        let fs_name = if ft == spoolq_fs_linux::EXT4_SUPER_MAGIC {
                            "ext4"
                        } else if ft == spoolq_fs_linux::XFS_SUPER_MAGIC {
                            "xfs"
                        } else if ft == spoolq_fs_linux::TMPFS_MAGIC {
                            "tmpfs_not_certified"
                        } else if ft == spoolq_fs_linux::NFS_SUPER_MAGIC {
                            "nfs_refused"
                        } else if ft == spoolq_fs_linux::FUSE_SUPER_MAGIC {
                            "fuse_refused"
                        } else if ft == spoolq_fs_linux::OVERLAYFS_SUPER_MAGIC {
                            "overlay_refused"
                        } else {
                            "unknown_refused"
                        };
                        results.push((
                            "filesystem",
                            format!("{} (magic {:#x})", fs_name, ft),
                            true,
                        ));
                    }
                    Err(e) => results.push(("filesystem", e.to_string(), false)),
                }

                // Publication mode probe under tmp/
                let probe_dir = path.join("tmp");
                if probe_dir.exists() {
                    match spoolq_fs_linux::open_dir_absolute(&probe_dir) {
                        Ok(dir_fd) => {
                            match spoolq_fs_linux::probe_publication_mode(dir_fd.as_raw_fd()) {
                                Ok(mode) => {
                                    let mode_str = match mode {
                                        spoolq_fs_linux::PublicationMode::DirectAtEmptyPath => {
                                            "direct-at-empty-path"
                                        }
                                        spoolq_fs_linux::PublicationMode::ProcSelfFd => {
                                            "proc-self-fd"
                                        }
                                        spoolq_fs_linux::PublicationMode::NamedFallback => {
                                            "named-fallback"
                                        }
                                    };
                                    results.push(("publication_mode", mode_str.to_string(), true));
                                }
                                Err(e) => results.push(("publication_mode", e.to_string(), false)),
                            }
                            // rename probe
                            match spoolq_fs_linux::probe_rename_noreplace(dir_fd.as_raw_fd()) {
                                Ok(supported) => results.push((
                                    "rename_noreplace",
                                    if supported {
                                        "supported".into()
                                    } else {
                                        "unsupported".into()
                                    },
                                    supported,
                                )),
                                Err(e) => results.push(("rename_noreplace", e.to_string(), false)),
                            }
                            // dir fsync probe
                            match spoolq_fs_linux::probe_dir_fsync(dir_fd.as_raw_fd()) {
                                Ok(supported) => results.push((
                                    "dir_fsync",
                                    if supported {
                                        "supported".into()
                                    } else {
                                        "unsupported".into()
                                    },
                                    supported,
                                )),
                                Err(e) => results.push(("dir_fsync", e.to_string(), false)),
                            }
                        }
                        Err(e) => {
                            results.push(("publication_mode", format!("open failed: {}", e), false))
                        }
                    }
                }
            }

            if json {
                let map: std::collections::BTreeMap<&str, serde_json::Value> = results
                    .iter()
                    .map(|(k, v, ok)| (*k, serde_json::json!({"value": v, "ok": ok})))
                    .collect();
                println!("{}", serde_json::to_string_pretty(&map).unwrap());
            } else {
                eprintln!("spoolq doctor {}", path.display());
                for (k, v, ok) in &results {
                    eprintln!("  {}: {}{}", k, v, if *ok { "" } else { " [FAIL]" });
                }
            }
            ExitCode::SUCCESS
        }

        Commands::Ack { path, handle_file } => {
            let lease = match load_handle(&handle_file) {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("handle load failed: {}", e);
                    return ExitCode::FAILURE;
                }
            };
            let mut queue = match Queue::open(&path, &OpenOptions::default()) {
                Ok(q) => q,
                Err(e) => {
                    eprintln!("open failed: {}", e);
                    return ExitCode::FAILURE;
                }
            };
            match queue.ack(&lease) {
                spoolq_core::AckOutcome::Acked => {
                    eprintln!("acked");
                    ExitCode::SUCCESS
                }
                spoolq_core::AckOutcome::AlreadyAcked => {
                    eprintln!("already acked");
                    ExitCode::FAILURE
                }
                spoolq_core::AckOutcome::LeaseLost => {
                    eprintln!("lease lost");
                    ExitCode::FAILURE
                }
                spoolq_core::AckOutcome::NotCommitted(e) => {
                    eprintln!("not committed: {}", e);
                    ExitCode::FAILURE
                }
                spoolq_core::AckOutcome::OutcomeUnknown(_) => {
                    eprintln!("outcome unknown");
                    ExitCode::from(2)
                }
            }
        }

        Commands::Retry {
            path,
            handle_file,
            after_seconds,
        } => {
            let lease = match load_handle(&handle_file) {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("handle load failed: {}", e);
                    return ExitCode::FAILURE;
                }
            };
            let mut queue = match Queue::open(&path, &OpenOptions::default()) {
                Ok(q) => q,
                Err(e) => {
                    eprintln!("open failed: {}", e);
                    return ExitCode::FAILURE;
                }
            };
            let outcome = match after_seconds {
                Some(s) => queue.retry_at(
                    &lease,
                    spoolq_fs_linux::clock_realtime_ns().unwrap_or(0) + s * 1_000_000_000,
                ),
                None => queue.retry_now(&lease),
            };
            match outcome {
                spoolq_core::TransitionOutcome::Committed => {
                    eprintln!("retried");
                    ExitCode::SUCCESS
                }
                spoolq_core::TransitionOutcome::LeaseLost => {
                    eprintln!("lease lost");
                    ExitCode::FAILURE
                }
                spoolq_core::TransitionOutcome::NotCommitted(e) => {
                    eprintln!("not committed: {}", e);
                    ExitCode::FAILURE
                }
                spoolq_core::TransitionOutcome::OutcomeUnknown(_) => {
                    eprintln!("outcome unknown");
                    ExitCode::from(2)
                }
            }
        }

        Commands::Bury {
            path,
            handle_file,
            reason,
        } => {
            let lease = match load_handle(&handle_file) {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("handle load failed: {}", e);
                    return ExitCode::FAILURE;
                }
            };
            let mut queue = match Queue::open(&path, &OpenOptions::default()) {
                Ok(q) => q,
                Err(e) => {
                    eprintln!("open failed: {}", e);
                    return ExitCode::FAILURE;
                }
            };
            let reason = spoolq_core::DeadReason::from_u16(reason)
                .unwrap_or(spoolq_core::DeadReason::Unspecified);
            match queue.bury(&lease, reason) {
                spoolq_core::TransitionOutcome::Committed => {
                    eprintln!("buried");
                    ExitCode::SUCCESS
                }
                spoolq_core::TransitionOutcome::LeaseLost => {
                    eprintln!("lease lost");
                    ExitCode::FAILURE
                }
                spoolq_core::TransitionOutcome::NotCommitted(e) => {
                    eprintln!("not committed: {}", e);
                    ExitCode::FAILURE
                }
                spoolq_core::TransitionOutcome::OutcomeUnknown(_) => {
                    eprintln!("outcome unknown");
                    ExitCode::from(2)
                }
            }
        }

        Commands::Inspect { path, job_id } => {
            let job_id_bytes = match spoolq_names::hex_decode_16(&job_id) {
                Some(b) => b,
                None => {
                    eprintln!("invalid job_id: expected 32 lowercase hex chars");
                    return ExitCode::FAILURE;
                }
            };
            match Queue::open(&path, &OpenOptions::default()) {
                Ok(queue) => {
                    let snapshots = queue.inspect(&job_id_bytes);
                    if snapshots.is_empty() {
                        eprintln!("not found");
                        return ExitCode::FAILURE;
                    }
                    for s in &snapshots {
                        println!(
                            "{} gen={} attempt={}/{} {}",
                            s.state, s.generation, s.attempt, s.maximum_attempts, s.relative_path
                        );
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("open failed: {}", e);
                    ExitCode::FAILURE
                }
            }
        }

        Commands::Verify { file, deep } => {
            let data = match std::fs::read(&file) {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("read failed: {}", e);
                    return ExitCode::FAILURE;
                }
            };
            if data.len() >= 128 && &data[0..8] == b"SPQJOB1\0" {
                match spoolq_format::FixedHeader::decode(&data[0..128]) {
                    Ok(header) => {
                        eprintln!("job_id: {}", spoolq_names::hex_encode(&header.job_id));
                        eprintln!("payload_length: {}", header.payload_length);
                        eprintln!("maximum_attempts: {}", header.maximum_attempts);
                        let expected_size = 128
                            + header.extension_header_length as usize
                            + header.payload_length as usize;
                        if data.len() != expected_size {
                            eprintln!(
                                "CORRUPT: expected {} bytes, got {}",
                                expected_size,
                                data.len()
                            );
                            return ExitCode::from(3);
                        }
                        if deep {
                            let payload = &data[128 + header.extension_header_length as usize..];
                            let computed = spoolq_format::payload_digest(payload);
                            if computed != header.payload_digest {
                                eprintln!("CORRUPT: payload digest mismatch");
                                return ExitCode::from(3);
                            }
                            eprintln!("payload_digest: verified");
                        }
                        eprintln!("valid");
                        ExitCode::SUCCESS
                    }
                    Err(e) => {
                        eprintln!("CORRUPT: {}", e);
                        ExitCode::from(3)
                    }
                }
            } else if data.len() == 160 && &data[0..8] == b"SPQFMT1\0" {
                match spoolq_format::FormatRecord::decode(&data) {
                    Ok(fmt) => {
                        eprintln!("queue_id: {}", spoolq_names::hex_encode(&fmt.queue_id));
                        eprintln!("shard_count: {}", fmt.shard_count);
                        eprintln!("valid");
                        ExitCode::SUCCESS
                    }
                    Err(e) => {
                        eprintln!("CORRUPT: {}", e);
                        ExitCode::from(3)
                    }
                }
            } else {
                eprintln!("unknown format");
                ExitCode::FAILURE
            }
        }

        Commands::FormatDump { file } => {
            let data = match std::fs::read(&file) {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("read failed: {}", e);
                    return ExitCode::FAILURE;
                }
            };
            if data.len() >= 128 && &data[0..8] == b"SPQJOB1\0" {
                match spoolq_format::FixedHeader::decode(&data[0..128]) {
                    Ok(h) => {
                        println!("type: job");
                        println!("job_id: {}", spoolq_names::hex_encode(&h.job_id));
                        println!("payload_length: {}", h.payload_length);
                        println!("extension_header_length: {}", h.extension_header_length);
                        println!("maximum_attempts: {}", h.maximum_attempts);
                        println!(
                            "payload_digest: {}",
                            spoolq_names::hex_encode(&h.payload_digest)
                        );
                        println!(
                            "envelope_digest: {}",
                            spoolq_names::hex_encode(&h.envelope_digest)
                        );
                        ExitCode::SUCCESS
                    }
                    Err(e) => {
                        eprintln!("parse error: {}", e);
                        ExitCode::FAILURE
                    }
                }
            } else if data.len() == 160 && &data[0..8] == b"SPQFMT1\0" {
                match spoolq_format::FormatRecord::decode(&data) {
                    Ok(f) => {
                        println!("type: format");
                        println!("queue_id: {}", spoolq_names::hex_encode(&f.queue_id));
                        println!("shard_count: {}", f.shard_count);
                        println!("lease_bucket_width_ns: {}", f.lease_bucket_width_ns);
                        println!("delayed_bucket_width_ns: {}", f.delayed_bucket_width_ns);
                        println!("terminal_bucket_width_ns: {}", f.terminal_bucket_width_ns);
                        println!("max_payload_length: {}", f.max_payload_length);
                        ExitCode::SUCCESS
                    }
                    Err(e) => {
                        eprintln!("parse error: {}", e);
                        ExitCode::FAILURE
                    }
                }
            } else {
                eprintln!("unrecognized format");
                ExitCode::FAILURE
            }
        }

        Commands::Recover {
            path,
            watch,
            budget_ops,
            budget_ms,
        } => {
            let budget = spoolq_core::WorkBudget {
                max_operations: budget_ops,
                max_duration_ms: budget_ms,
            };
            loop {
                let mut queue = match Queue::open(&path, &OpenOptions::default()) {
                    Ok(q) => q,
                    Err(e) => {
                        eprintln!("open failed: {}", e);
                        return ExitCode::FAILURE;
                    }
                };
                let stats = queue.recover(&budget);
                eprintln!(
                    "reaped:{} promoted:{} temp_deleted:{} dead:{} ops:{}{}",
                    stats.leases_reaped,
                    stats.delayed_promoted,
                    stats.temp_files_deleted,
                    stats.leases_to_dead,
                    stats.operations_attempted,
                    if stats.budget_exhausted {
                        " (budget exhausted)"
                    } else {
                        ""
                    },
                );
                if !watch {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_secs(5));
            }
            ExitCode::SUCCESS
        }
    }
}

fn count_files_recursive(path: &std::path::Path) -> usize {
    let mut count = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                count += count_files_recursive(&p);
            } else {
                count += 1;
            }
        }
    }
    count
}

#[derive(serde::Serialize, serde::Deserialize)]
struct HandleFile {
    queue_root: String,
    job_id: String,
    generation: u64,
    attempt: u32,
    maximum_attempts: u32,
    token: String,
    boot_id: String,
    expires_boottime_ns: u64,
    expires_wall_ns: u64,
    expected_dev: u64,
    expected_inode: u64,
    exact_source_path: String,
    envelope_digest: String,
    payload_verified: bool,
}

fn save_handle_to_file(
    queue_root: &std::path::Path,
    handle_path: &std::path::Path,
    lease: &spoolq_core::LeaseInfo,
) -> std::io::Result<()> {
    let handle = HandleFile {
        queue_root: queue_root.display().to_string(),
        job_id: spoolq_names::hex_encode(&lease.job_id),
        generation: lease.generation,
        attempt: lease.attempt,
        maximum_attempts: lease.maximum_attempts,
        token: spoolq_names::hex_encode(&lease.token),
        boot_id: lease.boot_id.clone(),
        expires_boottime_ns: lease.expires_boottime_ns,
        expires_wall_ns: lease.expires_wall_ns,
        expected_dev: lease.expected_dev,
        expected_inode: lease.expected_inode,
        exact_source_path: lease.exact_source_path.clone(),
        envelope_digest: spoolq_names::hex_encode(&lease.envelope_digest),
        payload_verified: lease.payload_verified,
    };
    let json = serde_json::to_string_pretty(&handle)?;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    use std::io::Write;
    let mut file = opts.open(handle_path)?;
    file.write_all(json.as_bytes())?;
    Ok(())
}

fn load_handle(path: &std::path::Path) -> std::io::Result<spoolq_core::LeaseInfo> {
    let data = std::fs::read(path)?;
    let handle: HandleFile = serde_json::from_slice(&data)?;
    let job_id = spoolq_names::hex_decode_16(&handle.job_id)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad job_id"))?;
    let token = spoolq_names::hex_decode_16(&handle.token)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad token"))?;
    let envelope_digest = spoolq_names::hex_decode_16(&handle.envelope_digest)
        .map(|b| {
            let mut d = [0u8; 32];
            d.copy_from_slice(&b);
            d
        })
        .or_else(|| {
            // hex_decode_16 returns [u8;16] but envelope_digest is 32 bytes
            // Try decoding 32 bytes manually
            if handle.envelope_digest.len() == 64 {
                let mut d = [0u8; 32];
                for (i, chunk) in handle.envelope_digest.as_bytes().chunks(2).enumerate() {
                    d[i] = u8::from_str_radix(std::str::from_utf8(chunk).ok()?, 16).ok()?;
                }
                Some(d)
            } else {
                None
            }
        })
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "bad envelope_digest")
        })?;
    Ok(spoolq_core::LeaseInfo {
        job_id,
        envelope_digest,
        generation: handle.generation,
        attempt: handle.attempt,
        maximum_attempts: handle.maximum_attempts,
        token,
        boot_id: handle.boot_id,
        expires_boottime_ns: handle.expires_boottime_ns,
        expires_wall_ns: handle.expires_wall_ns,
        content_type: String::new(),
        payload_length: 0,
        payload_digest: [0; 32],
        expected_dev: handle.expected_dev,
        expected_inode: handle.expected_inode,
        exact_source_path: handle.exact_source_path,
        payload_verified: handle.payload_verified,
    })
}
