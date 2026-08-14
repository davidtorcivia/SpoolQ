// Crash-lab workload: runs a seeded operation sequence against a real queue
// and appends one JSONL line per completed operation to the op log.
//
// The op log is the crash oracle input: a written line is a completed fact.
// The runner (cargo xtask crashlab tier0) SIGKILLs this process after
// observing a target number of lines, so the surviving prefix is the cut.
//
// Usage: crashlab-workload --queue DIR --oplog FILE --seed N --ops N

use std::io::Write as _;
use steadq_testkit::driver::ProductionDriver;

struct Args {
    queue: String,
    oplog: String,
    seed: u64,
    ops: u64,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        queue: String::new(),
        oplog: String::new(),
        seed: 1,
        ops: 24,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let value = it
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--queue" => args.queue = value,
            "--oplog" => args.oplog = value,
            "--seed" => args.seed = value.parse().map_err(|_| "bad --seed")?,
            "--ops" => args.ops = value.parse().map_err(|_| "bad --ops")?,
            _ => return Err(format!("unknown flag {flag}")),
        }
    }
    if args.queue.is_empty() || args.oplog.is_empty() {
        return Err(
            "usage: crashlab-workload --queue DIR --oplog FILE [--seed N] [--ops N]".into(),
        );
    }
    Ok(args)
}

/// xorshift64*: deterministic, dependency-free.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn payload_for(seed: u64, seq: u64) -> Vec<u8> {
    // Mostly small payloads, occasionally a large one to cross block boundaries.
    let len = match seq % 16 {
        0 => 96 * 1024 + (seed % 512) as usize,
        1 => 4096,
        2 => 0,
        _ => 32 + ((seed + seq * 37) % 2048) as usize,
    };
    let mut data = Vec::with_capacity(len);
    let mut rng = Rng(seed ^ (seq.wrapping_mul(0x9E3779B97F4A7C15)));
    for _ in 0..len {
        data.push((rng.next() & 0xff) as u8);
    }
    data
}

fn result_of(err: &steadq_core::Error) -> String {
    format!("err:{}", short_variant(err))
}

fn short_variant(err: &steadq_core::Error) -> String {
    let rendered = format!("{err:?}");
    rendered
        .split('(')
        .next()
        .unwrap_or(&rendered)
        .trim()
        .to_string()
}

fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("crashlab-workload: {e}");
            std::process::exit(2);
        }
    };

    let mut driver = match ProductionDriver::new(std::path::Path::new(&args.queue)) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("crashlab-workload: init/open failed: {e}");
            std::process::exit(2);
        }
    };

    let mut log = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&args.oplog)
        .unwrap_or_else(|e| {
            eprintln!("crashlab-workload: cannot open oplog {}: {e}", args.oplog);
            std::process::exit(2);
        });

    let mut rng = Rng(args.seed);
    let mut committed: Vec<[u8; 16]> = Vec::new();
    let mut leased: Vec<[u8; 16]> = Vec::new();

    for seq in 1..=args.ops {
        let pick = rng.next() % 100;
        let (op, job, result): (String, Option<[u8; 16]>, String) = if pick < 30 {
            let payload = payload_for(args.seed, seq);
            let max_attempts = 2 + (rng.next() % 2) as u32;
            match driver.enqueue(&payload, max_attempts) {
                Ok(job_id) => {
                    committed.push(job_id);
                    ("enqueue".into(), Some(job_id), "committed".into())
                }
                Err(e) => ("enqueue".into(), None, result_of(&e)),
            }
        } else if pick < 55 {
            match driver.lease(30_000_000_000) {
                Ok(Some(job_id)) => {
                    leased.push(job_id);
                    ("lease".into(), Some(job_id), "leased".into())
                }
                Ok(None) => ("lease".into(), None, "empty".into()),
                Err(e) => ("lease".into(), None, result_of(&e)),
            }
        } else if pick < 70 && !leased.is_empty() {
            let job_id = leased[0];
            match driver.ack(&job_id) {
                Ok(()) => {
                    leased.remove(0);
                    ("ack".into(), Some(job_id), "acked".into())
                }
                Err(e) => ("ack".into(), Some(job_id), result_of(&e)),
            }
        } else if pick < 80 && !leased.is_empty() {
            let job_id = leased[0];
            match driver.retry_now(&job_id) {
                Ok(()) => {
                    leased.remove(0);
                    ("retry".into(), Some(job_id), "retried".into())
                }
                Err(e) => ("retry".into(), Some(job_id), result_of(&e)),
            }
        } else if pick < 85 && !leased.is_empty() {
            let job_id = leased[0];
            match driver.bury(&job_id) {
                Ok(()) => {
                    leased.remove(0);
                    ("bury".into(), Some(job_id), "buried".into())
                }
                Err(e) => ("bury".into(), Some(job_id), result_of(&e)),
            }
        } else if pick < 90 {
            match driver.queue().sync() {
                Ok(()) => ("sync".into(), None, "ok".into()),
                Err(e) => ("sync".into(), None, format!("err:io:{e}")),
            }
        } else {
            let errors = driver.verify_consistency();
            if errors.is_empty() {
                ("verify".into(), None, "ok".into())
            } else {
                let description = &errors[0].description;
                ("verify".into(), None, format!("divergence:{description}"))
            }
        };

        let job_hex = job.map(|j| hex(&j)).unwrap_or_default();
        let line = format!(
            "{{\"seq\":{seq},\"op\":\"{op}\",\"job\":\"{job_hex}\",\"result\":\"{result}\"}}\n"
        );
        if let Err(e) = log.write_all(line.as_bytes()) {
            eprintln!("crashlab-workload: oplog write failed: {e}");
            std::process::exit(2);
        }
        let _ = log.flush();
        let _ = log.sync_data();

        if result.starts_with("divergence:") {
            eprintln!("crashlab-workload: pre-crash divergence at seq {seq}: {result}");
            std::process::exit(2);
        }
    }
}
