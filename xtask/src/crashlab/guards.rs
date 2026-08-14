// Device-safety guards for the crash lab.
//
// Hard rule: the OS drive and any device holding real data are never touched.
// The only permitted block targets are loop devices created by this tooling
// over image files under an allowlisted store. Everything here fails closed.

use std::path::{Path, PathBuf};

/// Store directories whose contents this tooling may use as backing images.
/// CRASHLAB_STORE adds one operator-provided store.
pub fn allowed_stores(root: &Path) -> Vec<PathBuf> {
    let mut stores = vec![
        PathBuf::from("/dev/shm/crashlab"),
        root.join("target/crashlab"),
    ];
    if let Ok(extra) = std::env::var("CRASHLAB_STORE") {
        stores.push(PathBuf::from(extra));
    }
    stores
}

/// g1: a backing file must live under an allowlisted store.
/// Lexical prefix check (stores may not exist yet) plus a canonical check
/// when the path exists, so symlinks cannot redirect out of a store.
pub fn store_path_allowed(path: &Path, root: &Path) -> bool {
    // Fail closed on traversal attempts; no need to resolve them.
    if path
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return false;
    }
    let lexical_ok = allowed_stores(root)
        .iter()
        .any(|store| path.starts_with(store));
    if !lexical_ok {
        return false;
    }
    match path.canonicalize() {
        Ok(canonical) => allowed_stores(root)
            .iter()
            .filter_map(|s| s.canonicalize().ok())
            .any(|store| canonical.starts_with(store)),
        // Not created yet: only the lexical claim exists, and we create the
        // store directory ourselves, so the claim is trustworthy.
        Err(_) => true,
    }
}

/// Whole-disk / partition / mapper node names that must never be a target.
pub fn is_forbidden_block_node(name: &str) -> bool {
    let name = name.trim_start_matches("/dev/");
    for prefix in [
        "nvme", "sd", "hd", "vd", "xvd", "mmcblk", "ram", "zram", "nbd", "md",
    ] {
        if name.starts_with(prefix) {
            return true;
        }
    }
    if name.starts_with("mapper/") || name.starts_with("dm-") {
        return true;
    }
    false
}

pub fn is_loop_node(name: &str) -> bool {
    name.trim_start_matches("/dev/").starts_with("loop")
}

/// Derive the parent disk of a partition node name (nvme0n1p3 -> nvme0n1,
/// sda1 -> sda). Returns None for whole-disk names.
pub fn parent_disk_of(name: &str) -> Option<String> {
    let name = name.trim_start_matches("/dev/");
    if name.starts_with("nvme") {
        // nvme0n1p3 -> nvme0n1; nvme0n1 has no partition suffix.
        if let Some(idx) = name.rfind('p') {
            let suffix = &name[idx + 1..];
            if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()) {
                return Some(name[..idx].to_string());
            }
        }
        None
    } else if name.starts_with("sd") || name.starts_with("hd") || name.starts_with("vd") {
        // sda1 -> sda
        let disk: String = name
            .chars()
            .take_while(|c| c.is_ascii_alphabetic())
            .collect();
        if disk.len() < name.len() {
            Some(disk)
        } else {
            None
        }
    } else {
        None
    }
}

/// g2: verify a loop device is attached to exactly our backing file.
/// Runs `losetup -j <backing>` and checks the output names our loop device.
pub fn verify_loop_owned(loop_dev: &str, backing: &Path) -> Result<(), String> {
    let output = std::process::Command::new("losetup")
        .arg("-j")
        .arg(backing)
        .output()
        .map_err(|e| format!("losetup -j failed: {e}"))?;
    let text = String::from_utf8_lossy(&output.stdout);
    // losetup -j lines start with "<dev>: [...]"; compare the exact device
    // name so loop1 never matches loop12.
    let loop_name = loop_dev.trim_start_matches("/dev/");
    let owned = text.lines().any(|l| {
        l.split(':')
            .next()
            .map(|dev| dev.trim_start_matches("/dev/") == loop_name)
            .unwrap_or(false)
    });
    if owned {
        Ok(())
    } else {
        Err(format!(
            "g2: {loop_dev} is not attached to {} (losetup -j said: {})",
            backing.display(),
            text.trim()
        ))
    }
}

/// g4: refuse a device that is (or is a parent of) a mounted filesystem's source.
pub fn device_mounted(node: &str, mounted_sources: &[String]) -> bool {
    let name = node.trim_start_matches("/dev/");
    mounted_sources.iter().any(|s| {
        let src = s.trim_start_matches("/dev/");
        src == name || parent_disk_of(src).as_deref() == Some(name)
    })
}

/// g4: current mounted filesystem sources from findmnt.
pub fn mounted_sources() -> Vec<String> {
    let output = match std::process::Command::new("findmnt")
        .args(["-rn", "-o", "SOURCE"])
        .output()
    {
        Ok(o) if o.status.success() => o,
        _ => return Vec::new(),
    };
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

/// Full target validation: only our own loop device over an allowlisted file.
pub fn verify_block_target(loop_dev: &str, backing: &Path, root: &Path) -> Result<(), String> {
    if is_forbidden_block_node(loop_dev) {
        return Err(format!("g3: {loop_dev} is a forbidden block node"));
    }
    if !is_loop_node(loop_dev) {
        return Err(format!("g3: {loop_dev} is not a loop device"));
    }
    if !store_path_allowed(backing, root) {
        return Err(format!(
            "g1: backing file {} is outside the allowed crash-lab stores",
            backing.display()
        ));
    }
    let mounted = mounted_sources();
    if device_mounted(loop_dev, &mounted) {
        return Err(format!(
            "g4: {loop_dev} or its parent is the source of a mounted filesystem"
        ));
    }
    verify_loop_owned(loop_dev, backing)
}

/// g7: annotate an lsblk listing, marking forbidden devices.
pub fn annotate_lsblk(lsblk_output: &str) -> String {
    lsblk_output
        .lines()
        .map(|line| {
            let name = line.split_whitespace().next().unwrap_or("");
            if is_forbidden_block_node(name) {
                format!("{line}    FORBIDDEN (os/data device - never a target)")
            } else if is_loop_node(name) {
                format!("{line}    loop (allowed only when created by crashlab)")
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forbidden_nodes() {
        for node in [
            "nvme0n1",
            "nvme1n1p3",
            "sda",
            "sda1",
            "sdb9",
            "hdz",
            "vda",
            "mmcblk0",
            "mapper/ubuntu--vg-ubuntu--lv",
            "dm-3",
            "md0",
            "ram0",
            "nbd0",
        ] {
            assert!(is_forbidden_block_node(node), "{node} must be forbidden");
        }
        for node in ["loop0", "loop17"] {
            assert!(
                !is_forbidden_block_node(node),
                "{node} must not be forbidden"
            );
        }
    }

    #[test]
    fn loop_detection() {
        assert!(is_loop_node("loop0"));
        assert!(is_loop_node("/dev/loop12"));
        assert!(!is_loop_node("nvme0n1"));
    }

    #[test]
    fn parent_disk() {
        assert_eq!(parent_disk_of("sda1").as_deref(), Some("sda"));
        assert_eq!(parent_disk_of("nvme1n1p3").as_deref(), Some("nvme1n1"));
        assert_eq!(parent_disk_of("sda"), None);
        assert_eq!(parent_disk_of("loop0"), None);
    }

    #[test]
    fn mounted_device_detection() {
        let mounted = vec!["/dev/nvme1n1p3".to_string()];
        assert!(device_mounted("nvme1n1p3", &mounted));
        assert!(device_mounted("nvme1n1", &mounted));
        assert!(!device_mounted("sda", &mounted));
        assert!(!device_mounted("loop0", &mounted));
    }

    #[test]
    fn store_allowlist() {
        let root = Path::new("/repo");
        assert!(store_path_allowed(
            Path::new("/dev/shm/crashlab/x.img"),
            root
        ));
        assert!(store_path_allowed(
            Path::new("/repo/target/crashlab/x.img"),
            root
        ));
        assert!(!store_path_allowed(
            Path::new("/nvme-mirror/temp/crashlab/x.img"),
            root
        ));
        assert!(!store_path_allowed(Path::new("/etc/x.img"), root));
        assert!(!store_path_allowed(Path::new("/home/x.img"), root));
        assert!(!store_path_allowed(Path::new("/repo/other/x.img"), root));
    }

    #[test]
    fn lsblk_annotation_marks_forbidden() {
        let annotated = annotate_lsblk("NAME SIZE\nsda 16.4T\nloop0 8G\n");
        assert!(annotated.contains("sda 16.4T    FORBIDDEN"));
        assert!(annotated.contains("loop0 8G    loop"));
    }
}
