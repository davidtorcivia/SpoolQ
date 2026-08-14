// Tier 0: SIGKILL crash lane.
//
// Runs the workload against a real queue in a tempdir, kills it with SIGKILL
// after a target number of completed operations (observed via the op log),
// then verifies the surviving prefix with the checker. This is process-crash
// evidence only, not power-loss evidence; it validates the checker plumbing
// and executor phase classification cheaply, including in CI.

use super::{ensure_bins, now_iso, write_json};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub fn run(root: &Path, args: &[String]) -> Result<(), String> {
    let mut runs = 20;
    let mut ops = 24;
    let mut seed = 1u64;
    let mut store = root.join("target/crashlab/tier0");
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let value = it
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--runs" => runs = value.parse().map_err(|_| "bad --runs")?,
            "--ops" => ops = value.parse().map_err(|_| "bad --ops")?,
            "--seed" => seed = value.parse().map_err(|_| "bad --seed")?,
            "--store" => store = PathBuf::from(value),
            _ => return Err(format!("unknown flag {flag}")),
        }
    }
    std::fs::create_dir_all(&store).map_err(|e| format!("store: {e}"))?;

    let (workload, check) = ensure_bins(root)?;

    let mut passed = 0;
    let mut failed = 0;
    let mut verdicts = Vec::new();
    let id_base = format!("t0-{}", std::process::id());

    for run_idx in 0..runs {
        let cut = (run_idx % ops as usize) + 1;
        let qdir = std::env::temp_dir().join(format!("crashlab-{id_base}-{run_idx}"));
        let _ = std::fs::remove_dir_all(&qdir);
        std::fs::create_dir_all(&qdir).map_err(|e| format!("queue dir: {e}"))?;
        let oplog = store.join(format!("{id_base}-{run_idx}.oplog"));
        let verdict_path = store.join(format!("{id_base}-{run_idx}.verdict.json"));

        let qdir_str = qdir.to_str().ok_or("queue path not utf-8")?.to_string();
        let oplog_str = oplog.to_str().ok_or("oplog path not utf-8")?.to_string();
        let verdict_str = verdict_path
            .to_str()
            .ok_or("verdict path not utf-8")?
            .to_string();
        let mut child = std::process::Command::new(&workload)
            .args([
                "--queue",
                &qdir_str,
                "--oplog",
                &oplog_str,
                "--seed",
                &seed.to_string(),
                "--ops",
                &ops.to_string(),
            ])
            .spawn()
            .map_err(|e| format!("spawn workload: {e}"))?;

        // Poll the op log and SIGKILL once the target prefix survives.
        let started = Instant::now();
        let mut observed = 0usize;
        loop {
            if let Ok(text) = std::fs::read_to_string(&oplog) {
                observed = text.matches('\n').count();
            }
            if observed >= cut {
                child.kill().map_err(|e| format!("kill: {e}"))?;
                break;
            }
            if started.elapsed() > Duration::from_secs(120) {
                let _ = child.kill();
                return Err(format!("workload timeout at cut {cut}"));
            }
            std::thread::sleep(Duration::from_micros(500));
        }
        let status = child.wait().map_err(|e| format!("wait workload: {e}"))?;
        // Re-read after wait: kill may land one op late, that is fine, the
        // surviving prefix defines the expectations.
        if let Ok(text) = std::fs::read_to_string(&oplog) {
            observed = text.matches('\n').count();
        }

        let check_status = std::process::Command::new(&check)
            .args([
                "--queue",
                &qdir_str,
                "--oplog",
                &oplog_str,
                "--out",
                &verdict_str,
            ])
            .status()
            .map_err(|e| format!("spawn checker: {e}"))?;

        let pass = check_status.success();
        if pass {
            passed += 1;
        } else {
            failed += 1;
        }
        verdicts.push(json!({
            "run": run_idx,
            "cut_target": cut,
            "ops_survived": observed,
            "workload_signal": format!("{status}"),
            "verdict": if verdict_path.exists() {
                serde_json::from_str::<serde_json::Value>(&std::fs::read_to_string(&verdict_path).map_err(|e| e.to_string())?)
                    .unwrap_or(json!({"pass": false, "parse_error": true}))
            } else {
                json!({"pass": false, "missing_verdict": true})
            },
        }));
        eprintln!(
            "tier0 run {}/{} cut={} survived={} verdict={}",
            run_idx + 1,
            runs,
            cut,
            observed,
            if pass { "PASS" } else { "FAIL" }
        );
        let _ = std::fs::remove_dir_all(&qdir);
    }

    let summary = json!({
        "tier": 0,
        "started": now_iso(),
        "runs": runs,
        "ops": ops,
        "seed": seed,
        "passed": passed,
        "failed": failed,
        "verdicts": verdicts,
    });
    write_json(&store.join(format!("{id_base}.summary.json")), &summary)?;
    eprintln!(
        "tier0 summary: {passed} passed, {failed} failed -> {}",
        store.display()
    );
    if failed > 0 {
        Err(format!("tier0: {failed} failing runs"))
    } else {
        Ok(())
    }
}
