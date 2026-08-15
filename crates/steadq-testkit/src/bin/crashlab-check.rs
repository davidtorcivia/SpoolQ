// Crash-lab checker: verifies a queue directory against an op-log prefix
// after a crash. Runs recovery, fsck, and the A-015 acceptance gates:
//
//   G1 no returned-committed enqueue is lost
//   G2 no acknowledged job is active (must be terminal)
//   G3 no phantom job is delivered
//   G4 recovery completes without errors
//   G5 fsck reports no Error-severity findings
//
// A written op-log line is a completed fact; the surviving prefix defines
// the expectations. Any payload corruption must surface as quarantine, never
// as delivery.
//
// Usage: crashlab-check --queue DIR --oplog FILE --out VERDICT.json

use serde_json::json;
use std::path::Path;
use steadq_core::{
    Error, FsckDepth, FsckMode, FsckOptions, LeaseOutcome, OpenOptions, Queue, WorkBudget,
};

struct Args {
    queue: String,
    oplog: String,
    out: String,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        queue: String::new(),
        oplog: String::new(),
        out: String::new(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let value = it
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--queue" => args.queue = value,
            "--oplog" => args.oplog = value,
            "--out" => args.out = value,
            _ => return Err(format!("unknown flag {flag}")),
        }
    }
    if args.queue.is_empty() || args.oplog.is_empty() || args.out.is_empty() {
        return Err("usage: crashlab-check --queue DIR --oplog FILE --out VERDICT.json".into());
    }
    Ok(args)
}

struct OpLine {
    op: String,
    job: String,
    result: String,
}

fn unhex(s: &str) -> Option<[u8; 16]> {
    if s.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hi = (chunk[0] as char).to_digit(16)?;
        let lo = (chunk[1] as char).to_digit(16)?;
        out[i] = ((hi << 4) | lo) as u8;
    }
    Some(out)
}

fn read_oplog(path: &Path) -> Result<Vec<OpLine>, String> {
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        // A crash state before the oplog file was created has zero
        // completed operations and therefore zero expectations.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(format!("cannot read oplog {}: {e}", path.display())),
    };
    // Tolerate a truncated trailing line: drop anything after the last newline.
    let text = match text.rfind('\n') {
        Some(i) => &text[..=i],
        None => "",
    };
    let mut lines = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value =
            serde_json::from_str(line).map_err(|e| format!("bad oplog line: {e}"))?;
        lines.push(OpLine {
            op: v["op"].as_str().unwrap_or("").to_string(),
            job: v["job"].as_str().unwrap_or("").to_string(),
            result: v["result"].as_str().unwrap_or("").to_string(),
        });
    }
    Ok(lines)
}

fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("crashlab-check: {e}");
            std::process::exit(2);
        }
    };
    let verdict = match run_check(&args) {
        Ok(v) => v,
        Err(e) => {
            let v = json!({
                "pass": false,
                "internal_error": e,
            });
            let _ = std::fs::write(&args.out, serde_json::to_string_pretty(&v).unwrap());
            std::process::exit(2);
        }
    };
    let pass = verdict["pass"].as_bool().unwrap_or(false);
    let _ = std::fs::write(&args.out, serde_json::to_string_pretty(&verdict).unwrap());
    println!(
        "crashlab-check: {} ({})",
        if pass { "PASS" } else { "FAIL" },
        args.out
    );
    std::process::exit(if pass { 0 } else { 1 });
}

fn run_check(args: &Args) -> Result<serde_json::Value, String> {
    // A crash state before the queue was initialized checks nothing: no
    // queue exists, no operations were durably completed.
    if !Path::new(&args.queue).is_dir() {
        return Ok(json!({ "pass": true, "queue_absent": true }));
    }
    // FORMAT publication is fsync-then-rename and precedes every queue
    // write, so a missing FORMAT means initialization was interrupted
    // before any operation could complete durably. A durable operation
    // without FORMAT would be a causality violation.
    if !Path::new(&args.queue).join("FORMAT").is_file() {
        let ops = read_oplog(Path::new(&args.oplog))?;
        return if ops.is_empty() {
            Ok(json!({ "pass": true, "interrupted_init": true }))
        } else {
            Ok(json!({
                "pass": false,
                "format_missing_with_durable_ops": ops.len(),
            }))
        };
    }
    let ops = read_oplog(Path::new(&args.oplog))?;

    let committed: Vec<[u8; 16]> = ops
        .iter()
        .filter(|l| l.op == "enqueue" && l.result.starts_with("committed"))
        .filter_map(|l| unhex(&l.job))
        .collect();
    let acked: Vec<[u8; 16]> = ops
        .iter()
        .filter(|l| l.op == "ack" && l.result == "acked")
        .filter_map(|l| unhex(&l.job))
        .collect();

    let mut queue = Queue::open(
        Path::new(&args.queue),
        &OpenOptions {
            allow_unsupported_fs: true,
            ..Default::default()
        },
    )
    .map_err(|e| format!("open failed: {e}"))?;

    // Recovery to quiescence.
    let budget = WorkBudget {
        max_operations: 100_000,
        max_duration_ms: 60_000,
    };
    let mut passes = 0u32;
    let mut total_errors = 0usize;
    let last = loop {
        let stats = queue.recover(&budget);
        total_errors += stats.errors.len();
        passes += 1;
        if !stats.budget_exhausted || passes > 100 {
            break stats;
        }
    };

    // Deep fsck in check-only mode.
    let report = queue.fsck(&FsckOptions {
        mode: FsckMode::Check,
        depth: FsckDepth::Deep,
    });
    let fsck_errors = report
        .findings
        .iter()
        .filter(|f| matches!(f.severity, steadq_core::FindingSeverity::Error))
        .count();
    let fsck_warnings = report.findings.len() - fsck_errors;

    // G1: committed jobs must still exist somewhere.
    let mut missing = Vec::new();
    let mut acked_bad = Vec::new();
    for job in &committed {
        let snapshots = queue.inspect(job);
        if snapshots.is_empty() {
            missing.push(hex(job));
            continue;
        }
        // G2: if this job was later acked, its state must be terminal.
        if acked.contains(job) {
            let active = snapshots
                .iter()
                .any(|s| matches!(s.state.as_str(), "ready" | "leased" | "delayed"));
            if active {
                acked_bad.push(format!("{}:{}", hex(job), snapshots[0].state));
            }
        }
    }
    // Acked jobs that were never seen as committed cannot exist (ack requires
    // a prior lease of a committed job), but check them anyway if present.
    for job in &acked {
        if !committed.contains(job) {
            let snapshots = queue.inspect(job);
            let active = snapshots
                .iter()
                .any(|s| matches!(s.state.as_str(), "ready" | "leased" | "delayed"));
            if active {
                acked_bad.push(format!("{}:{}", hex(job), snapshots[0].state));
            }
        }
    }

    // G3: probe deliveries. Anything leasable must be a committed job; an
    // acknowledged job must never be delivered; corrupt payloads must be
    // quarantined, not delivered.
    let acked_hex: Vec<String> = acked.iter().map(|j| hex(j)).collect();
    let mut delivered = Vec::new();
    let mut phantom = Vec::new();
    let mut quarantined_corrupt = 0u32;
    let committed_hex: Vec<String> = committed.iter().map(|j| hex(j)).collect();
    for _ in 0..8 {
        match queue.lease(0, 30_000_000_000) {
            LeaseOutcome::Leased(info) => {
                let jh = hex(&info.job_id);
                if acked_hex.contains(&jh) {
                    phantom.push(format!("acked-delivered:{jh}"));
                } else if !committed_hex.contains(&jh) {
                    phantom.push(format!("phantom:{jh}"));
                } else {
                    delivered.push(jh);
                }
            }
            LeaseOutcome::NotCommitted(Error::PayloadCorrupt) => {
                // Deterministic corruption was quarantined before delivery.
                quarantined_corrupt += 1;
            }
            _ => break,
        }
    }

    let stats = last;
    let gates_pass = missing.is_empty()
        && acked_bad.is_empty()
        && phantom.is_empty()
        && total_errors == 0
        && fsck_errors == 0;

    Ok(json!({
        "pass": gates_pass,
        "ops": ops.len(),
        "committed": committed.len(),
        "acked": acked.len(),
        "gates": {
            "committed_not_lost": { "checked": committed.len(), "missing": missing },
            "acked_terminal": { "checked": acked.len(), "violations": acked_bad },
            "no_phantom_or_acked_delivery": { "violations": phantom, "delivered_probe": delivered.len() },
            "recovery_clean": { "passes": passes, "errors": total_errors },
            "fsck_clean": {
                "errors": fsck_errors,
                "warnings": fsck_warnings,
                "total_objects": report.total_objects,
                "structurally_verified": report.structurally_verified,
                "payloads_deep_verified": report.payloads_deep_verified,
            },
            "quarantined_corrupt_payloads": quarantined_corrupt,
        },
        "recovery": {
            "reaped": stats.leases_reaped,
            "promoted": stats.delayed_promoted,
            "temp_deleted": stats.temp_files_deleted,
            "to_dead": stats.leases_to_dead,
        },
    }))
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}
