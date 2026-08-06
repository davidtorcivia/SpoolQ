// SpoolQ command-line interface.

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
    },
    /// Stats
    Stats { path: PathBuf },
    /// Doctor: check environment
    Doctor { path: PathBuf },
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
                    println!("job_id: {}", spoolq_names::hex_encode(&lease.job_id));
                    println!("generation: {}", lease.generation);
                    println!("attempt: {}/{}", lease.attempt, lease.maximum_attempts);
                    println!("token: {}", spoolq_names::hex_encode(&lease.token));
                    println!("source: {}", lease.exact_source_path);
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

        Commands::Doctor { path } => {
            eprintln!("spoolq doctor {}", path.display());
            // Check basic capabilities
            match spoolq_fs_linux::read_boot_id() {
                Ok(id) => eprintln!("boot_id: {}", id),
                Err(e) => eprintln!("boot_id: FAILED ({})", e),
            }
            match spoolq_fs_linux::clock_boottime_ns() {
                Ok(ns) => eprintln!("clock_boottime: {} ns", ns),
                Err(e) => eprintln!("clock_boottime: FAILED ({})", e),
            }
            match spoolq_fs_linux::random_128bit() {
                Ok(_) => eprintln!("getrandom: OK"),
                Err(e) => eprintln!("getrandom: FAILED ({})", e),
            }
            if path.exists() {
                match spoolq_fs_linux::statfs(&path) {
                    Ok(stat) => {
                        let ft = stat.f_type;
                        let fs_name = if ft == spoolq_fs_linux::EXT4_SUPER_MAGIC {
                            "ext4"
                        } else if ft == spoolq_fs_linux::XFS_SUPER_MAGIC {
                            "xfs"
                        } else if ft == spoolq_fs_linux::TMPFS_MAGIC {
                            "tmpfs (not certified)"
                        } else if ft == spoolq_fs_linux::NFS_SUPER_MAGIC {
                            "nfs (refused)"
                        } else {
                            "unknown"
                        };
                        eprintln!("filesystem: {} (magic {:#x})", fs_name, ft);
                    }
                    Err(e) => eprintln!("statfs: FAILED ({})", e),
                }
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
