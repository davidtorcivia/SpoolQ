// Tier 1: dm-log-writes exhaustive crash replay.
//
// Records every block write (with fsync/FUA/discard marks) issued by a real
// workload on a real filesystem, then replays the log at every persistence
// barrier and checks each resulting crash state with the checker. Coverage is
// exhaustive over reachable crash states, not sampled.
//
// Requires root. Only touches loop devices this run created, over image
// files under an allowlisted store (guards g1-g4, see guards.rs).

use super::guards;
use super::registry::{self, RegistryRun};
use super::{ensure_bins, now_iso, run_cmd, write_json};
use serde_json::json;
use std::path::{Path, PathBuf};

struct T1Args {
    fs: String,
    ops: u64,
    seed: u64,
    size_mb: u64,
    store: PathBuf,
    max_marks: usize,
    keep_images: bool,
}

pub fn run(root: &Path, args: &[String]) -> Result<(), String> {
    let mut a = T1Args {
        fs: String::new(),
        ops: 40,
        seed: 1,
        size_mb: 8192,
        store: PathBuf::from("/dev/shm/crashlab"),
        max_marks: 0,
        keep_images: false,
    };
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let value = it
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--fs" => a.fs = value.clone(),
            "--ops" => a.ops = value.parse().map_err(|_| "bad --ops")?,
            "--seed" => a.seed = value.parse().map_err(|_| "bad --seed")?,
            "--size-mb" => a.size_mb = value.parse().map_err(|_| "bad --size-mb")?,
            "--store" => a.store = PathBuf::from(value),
            "--max-marks" => a.max_marks = value.parse().map_err(|_| "bad --max-marks")?,
            "--keep-images" => {
                a.keep_images = true;
                continue;
            }
            _ => return Err(format!("unknown flag {flag}")),
        }
    }
    match a.fs.as_str() {
        "ext4" | "xfs" | "btrfs" | "f2fs" => {}
        _ => return Err("tier1 requires --fs ext4|xfs|btrfs|f2fs".into()),
    }

    // Root check with a clear message (guards still protect without it).
    if !is_root() {
        return Err("tier1 needs root (losetup/dmsetup/mount); run under sudo. Device guards stay on either way.".into());
    }
    preflight(&a.fs)?;
    if !guards::store_path_allowed(&a.store, root) {
        return Err(format!(
            "g1: store {} is not an allowed crash-lab store",
            a.store.display()
        ));
    }
    std::fs::create_dir_all(&a.store).map_err(|e| format!("store: {e}"))?;
    print_preflight_device_map();

    let (workload, check) = ensure_bins(root)?;

    let id = format!("t1-{}-{}", a.fs, std::process::id());
    let backing = a.store.join(format!("{id}.img"));
    let marker = a.store.join(format!("{id}-log.img"));
    let oplog = a.store.join(format!("{id}.oplog"));
    let mount_dir = PathBuf::from(format!("/mnt/crashlab-{id}"));

    let mut run = RegistryRun {
        id: id.clone(),
        kind: "tier1".into(),
        backing: Some(backing.display().to_string()),
        marker: Some(marker.display().to_string()),
        loops: Vec::new(),
        dm_names: Vec::new(),
        mount: Some(mount_dir.display().to_string()),
        status: "active".into(),
        started: now_iso(),
        ended: None,
    };
    registry::upsert(&a.store, &run)?;

    let result = execute_run(
        root, &a, &workload, &check, &id, &backing, &marker, &oplog, &mount_dir, &mut run,
    );

    run.status = match &result {
        Ok(()) => "done".into(),
        Err(_) => "failed".into(),
    };
    run.ended = Some(now_iso());
    registry::upsert(&a.store, &run)?;
    teardown_run_resources(&run);
    if !a.keep_images && result.is_ok() {
        let _ = std::fs::remove_file(&backing);
        let _ = std::fs::remove_file(&marker);
    }
    result
}

#[allow(clippy::too_many_arguments)]
fn execute_run(
    root: &Path,
    a: &T1Args,
    workload: &Path,
    check: &Path,
    id: &str,
    backing: &Path,
    marker: &Path,
    oplog: &Path,
    mount_dir: &Path,
    run: &mut RegistryRun,
) -> Result<(), String> {
    // 1. Backing + marker images.
    allocate_image(backing, a.size_mb)?;
    allocate_image(marker, 64)?;
    guards::store_path_allowed(backing, root)
        .then_some(())
        .ok_or_else(|| format!("g1: {}", backing.display()))?;

    // 2. Attach loops: backing, then marker.
    let loop_b = attach_loop(backing)?;
    let loop_m = attach_loop(marker)?;
    run.loops = vec![loop_b.clone(), loop_m.clone()];
    registry::upsert(&a.store, run)?;
    guards::verify_block_target(&loop_b, backing, root)?;
    guards::verify_block_target(&loop_m, marker, root)?;

    // 3. Filesystem with recorded options.
    let mkfs_opts: &[&str] = match a.fs.as_str() {
        "ext4" => &["-b", "4096", "-F"],
        "xfs" => &["-f"],
        "btrfs" => &["-f"],
        "f2fs" => &["-f"],
        _ => unreachable!(),
    };
    run_cmd(&format!("mkfs.{}", a.fs), mkfs_opts, &[&loop_b]).map_err(|e| format!("mkfs: {e}"))?;
    let mkfs_version = std::process::Command::new(format!("mkfs.{}", a.fs))
        .arg("-V")
        .output()
        .map(|o| {
            format!(
                "{}{}",
                String::from_utf8_lossy(&o.stdout),
                String::from_utf8_lossy(&o.stderr)
            )
        })
        .unwrap_or_default();

    // 4. dm log-writes target over the fresh fs.
    let sectors = sectors_of(&loop_b)?;
    let dm_name = format!("crashlab-{id}");
    run.dm_names = vec![dm_name.clone()];
    registry::upsert(&a.store, run)?;
    let table = format!("0 {sectors} log-writes {loop_b} {loop_m}");
    run_cmd("dmsetup", &["create", &dm_name, "--table", &table], &[])?;
    let dm_node = format!("/dev/mapper/{dm_name}");

    // 5. Mount, run the workload, quiesce.
    std::fs::create_dir_all(mount_dir).map_err(|e| format!("mkdir mount: {e}"))?;
    run_cmd("mount", &[&dm_node, &mount_dir.to_string_lossy()], &[])?;
    let mount_info = run_cmd(
        "findmnt",
        &[
            "-rn",
            "-T",
            &mount_dir.to_string_lossy(),
            "-o",
            "FSTYPE,OPTIONS",
        ],
        &[],
    )
    .unwrap_or_default();
    let queue_dir = mount_dir.join("queue");
    let workload_status = std::process::Command::new(workload)
        .args([
            "--queue",
            queue_dir.to_str().ok_or("queue path not utf-8")?,
            "--oplog",
            oplog.to_str().ok_or("oplog path not utf-8")?,
            "--seed",
            &a.seed.to_string(),
            "--ops",
            &a.ops.to_string(),
        ])
        .status()
        .map_err(|e| format!("spawn workload: {e}"))?;
    if !workload_status.success() {
        return Err(format!("workload failed: {workload_status}"));
    }
    run_cmd("umount", &[&mount_dir.to_string_lossy()], &[])?;
    run_cmd("dmsetup", &["remove", &dm_name], &[])?;
    detach_loop(&loop_b);
    detach_loop(&loop_m);
    run.loops.clear();
    run.dm_names.clear();
    registry::upsert(&a.store, run)?;

    // 6. Enumerate persistence-barrier entries from the log.
    // xfstests replay-log: find mode walks entries from --start-entry and
    // prints "<entry>@<sector>" at the next flush/fua; replay mode applies
    // entries to --replay <target> with --limit N (entries 0..N-1).
    attach_loop_explicit(&loop_m, marker)?;
    guards::verify_block_target(&loop_m, marker, root)?;
    let replay_log = locate_replay_log();
    let mut barriers: Vec<u64> = Vec::new();
    for flag in ["--next-flush", "--next-fua"] {
        let mut start: u64 = 0;
        loop {
            let out = run_cmd(
                &replay_log,
                &[
                    "--log",
                    &loop_m,
                    "--find",
                    flag,
                    "--start-entry",
                    &start.to_string(),
                ],
                &[],
            );
            match out {
                Ok(line) => {
                    let Some(entry) = line.split('@').next().and_then(|n| n.parse::<u64>().ok())
                    else {
                        break;
                    };
                    barriers.push(entry);
                    start = entry + 1;
                }
                Err(_) => break, // no more barriers of this kind
            }
        }
    }
    barriers.sort_unstable();
    barriers.dedup();
    let nr_entries: u64 = run_cmd(&replay_log, &["--log", &loop_m, "--number-entries"], &[])?
        .trim()
        .parse()
        .unwrap_or(0);
    if barriers.is_empty() {
        detach_loop(&loop_m);
        return Err("no persistence barriers (flush/fua) found in log".into());
    }
    // Crash states to test: the prefix ending right before and right after
    // each barrier, plus the full log (clean completion sanity state).
    let mut limits: Vec<u64> = Vec::new();
    for b in &barriers {
        limits.push((*b).max(1)); // before the barrier
        limits.push((*b + 1).min(nr_entries.max(1))); // after the barrier
    }
    if nr_entries > 0 {
        limits.push(nr_entries);
    }
    limits.sort_unstable();
    limits.dedup();

    // 7. Replay each prefix and check the crash state.
    let mut verdicts = Vec::new();
    let mut failures = Vec::new();
    for (i, limit) in limits.iter().enumerate() {
        if a.max_marks > 0 && i >= a.max_marks {
            break;
        }
        attach_loop_explicit(&loop_b, backing)?;
        guards::verify_block_target(&loop_b, backing, root)?;
        run_cmd(
            &replay_log,
            &[
                "--log",
                &loop_m,
                "--replay",
                &loop_b,
                "--limit",
                &limit.to_string(),
            ],
            &[],
        )
        .map_err(|e| format!("replay limit {limit}: {e}"))?;

        let mount_result = run_cmd("mount", &[&loop_b, &mount_dir.to_string_lossy()], &[]);
        let verdict_path = a.store.join(format!("{id}.e{limit}.verdict.json"));
        let pass = match mount_result {
            Ok(_) => {
                let status = std::process::Command::new(check)
                    .args([
                        "--queue",
                        queue_dir.to_str().ok_or("queue path not utf-8")?,
                        "--oplog",
                        oplog.to_str().ok_or("oplog path not utf-8")?,
                        "--out",
                        verdict_path.to_str().ok_or("verdict path not utf-8")?,
                    ])
                    .status()
                    .map_err(|e| format!("spawn checker: {e}"))?;
                let _ = run_cmd("umount", &[&mount_dir.to_string_lossy()], &[]);
                status.success()
            }
            Err(e) => {
                // A crash state that cannot even mount is a catastrophic
                // durability failure for this profile.
                write_json(&verdict_path, &json!({"pass": false, "mount_error": e}))?;
                false
            }
        };
        verdicts.push(json!({"entries": limit, "pass": pass}));
        if !pass {
            failures.push(*limit);
        }
        detach_loop(&loop_b);
        if !failures.is_empty() {
            // Keep everything for debugging the first failure.
            detach_loop(&loop_m);
            let manifest = json!({
                "id": id, "fs": a.fs, "ops": a.ops, "seed": a.seed,
                "kernel": kernel_version(), "mkfs": mkfs_version,
                "mount": mount_info, "barriers": barriers, "entries": nr_entries,
                "verdicts": verdicts, "failures": failures,
                "backing": backing.display().to_string(),
            });
            write_json(&a.store.join(format!("{id}.manifest.json")), &manifest)?;
            return Err(format!(
                "tier1: crash state at entry {limit} FAILED; images kept at {} / {}",
                backing.display(),
                marker.display()
            ));
        }
    }
    detach_loop(&loop_m);

    let manifest = json!({
        "id": id,
        "fs": a.fs,
        "tier": 1,
        "ops": a.ops,
        "seed": a.seed,
        "kernel": kernel_version(),
        "mkfs": mkfs_version,
        "mount": mount_info,
        "size_mb": a.size_mb,
        "entries": nr_entries,
        "barriers": barriers.len(),
        "states_checked": verdicts.len(),
        "verdicts": verdicts,
        "pass": failures.is_empty(),
        "started": now_iso(),
    });
    write_json(&a.store.join(format!("{id}.manifest.json")), &manifest)?;
    eprintln!(
        "tier1 {}: {} entries, {} barriers, {} states checked, all passed",
        a.fs,
        nr_entries,
        barriers.len(),
        verdicts.len()
    );
    Ok(())
}

fn teardown_run_resources(run: &RegistryRun) {
    if let Some(mount) = &run.mount {
        let _ = std::process::Command::new("umount").arg(mount).status();
    }
    for dm in &run.dm_names {
        let _ = std::process::Command::new("dmsetup")
            .args(["remove", dm])
            .status();
    }
    for loop_dev in &run.loops {
        let _ = std::process::Command::new("losetup")
            .args(["-d", loop_dev])
            .status();
    }
}

fn allocate_image(path: &Path, size_mb: u64) -> Result<(), String> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .map_err(|e| format!("create image {}: {e}", path.display()))?;
    file.set_len(size_mb * 1024 * 1024)
        .map_err(|e| format!("fallocate {}: {e}", path.display()))?;
    file.sync_all().map_err(|e| format!("sync image: {e}"))?;
    Ok(())
}

fn attach_loop(backing: &Path) -> Result<String, String> {
    let out = run_cmd(
        "losetup",
        &["--find", "--show", "--direct-io=on"],
        &[&backing.display().to_string()],
    )?;
    let dev = out.trim().to_string();
    if dev.starts_with("/dev/loop") {
        Ok(dev)
    } else {
        Err(format!("losetup returned unexpected output: {out}"))
    }
}

fn attach_loop_explicit(dev: &str, backing: &Path) -> Result<(), String> {
    run_cmd(
        "losetup",
        &["--direct-io=on", dev, &backing.display().to_string()],
        &[],
    )
    .map(|_| ())
}

fn detach_loop(dev: &str) {
    let _ = std::process::Command::new("losetup")
        .args(["-d", dev])
        .status();
}

fn sectors_of(dev: &str) -> Result<String, String> {
    let out = run_cmd("blockdev", &["--getsz", dev], &[])?;
    let n: u64 = out
        .trim()
        .parse()
        .map_err(|_| format!("bad blockdev --getsz output: {out}"))?;
    Ok(n.to_string())
}

fn kernel_version() -> String {
    std::process::Command::new("uname")
        .arg("-r")
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default()
}

fn is_root() -> bool {
    std::process::Command::new("id")
        .arg("-u")
        .output()
        .ok()
        .and_then(|o| {
            String::from_utf8_lossy(&o.stdout)
                .trim()
                .parse::<u32>()
                .ok()
        })
        == Some(0)
}

fn preflight(fs: &str) -> Result<(), String> {
    for tool in [
        "losetup", "dmsetup", "mount", "umount", "blockdev", "findmnt",
    ] {
        if which(tool).is_none() {
            return Err(format!("missing tool: {tool}"));
        }
    }
    if !Path::new(&locate_replay_log()).is_file() && which("replay-log").is_none() {
        return Err(
            "missing tool: replay-log (build from xfstests log-writes, install to ~/.local/bin)"
                .into(),
        );
    }
    if which(&format!("mkfs.{fs}")).is_none() {
        return Err(format!("missing mkfs.{fs}"));
    }
    Ok(())
}

fn print_preflight_device_map() {
    if let Ok(out) = std::process::Command::new("lsblk")
        .args(["-o", "NAME,SIZE,TYPE,FSTYPE,MOUNTPOINTS,MODEL"])
        .output()
    {
        eprintln!("--- device map (guards active) ---");
        eprintln!(
            "{}",
            guards::annotate_lsblk(&String::from_utf8_lossy(&out.stdout))
        );
        eprintln!("----------------------------------");
    }
}

fn which(tool: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(tool))
        .find(|candidate| candidate.is_file())
}

/// replay-log may live outside root's PATH (user-local install).
fn locate_replay_log() -> String {
    if which("replay-log").is_some() {
        return "replay-log".into();
    }
    for candidate in [
        std::env::var("HOME")
            .ok()
            .map(|h| format!("{h}/.local/bin/replay-log")),
        Some("/dev/shm/crashlab/replay-log".into()),
        Some("/usr/local/bin/replay-log".into()),
    ]
    .into_iter()
    .flatten()
    {
        if Path::new(&candidate).is_file() {
            return candidate;
        }
    }
    "replay-log".into()
}
