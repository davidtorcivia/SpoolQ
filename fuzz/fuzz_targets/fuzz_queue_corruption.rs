// Fuzz target: queue directory corruption.
// Property: no panic on open, inspect, fsck, or recover after corruption.
//
// Input: bytes used to select corruption operations on a valid queue.

#![no_main]
use libfuzzer_sys::fuzz_target;
use steadq_core::{Queue, CreateOptions, OpenOptions};

fuzz_target!(|data: &[u8]| {
    let tmp = match tempfile::tempdir() {
        Ok(t) => t,
        Err(_) => return,
    };

    if Queue::init(tmp.path(), &CreateOptions::default()).is_err() {
        return;
    }

    // Enqueue a few jobs to create state files.
    {
        let mut queue = match Queue::open(
            tmp.path(),
            &OpenOptions {
                allow_unsupported_fs: true,
                ..Default::default()
            },
        ) {
            Ok(q) => q,
            Err(_) => return,
        };
        for i in 0..5u8 {
            let _ = queue.enqueue(steadq_core::EnqueueInput {
                maximum_attempts: 3,
                content_type: "text/plain".into(),
                payload: vec![i; 32],
                ..Default::default()
            });
        }
    }

    // Apply corruptions from fuzz input.
    for chunk in data.chunks(4) {
        if chunk.len() < 2 {
            continue;
        }
        let subdir = match chunk[0] % 6 {
            0 => "ready",
            1 => "dead",
            2 => "receipts",
            3 => "quarantine",
            4 => "control",
            _ => "tmp",
        };
        let action = chunk[1] % 4;
        let base = tmp.path().join(subdir);
        match action {
            0 => {
                // Create unexpected file
                let name = format!("fuzz_{}", chunk[0].wrapping_mul(chunk[1]));
                let _ = std::fs::write(base.join(&name), chunk);
            }
            1 => {
                // Create unexpected directory
                let name = format!("fuzz_dir_{}", chunk[0]);
                let _ = std::fs::create_dir(base.join(&name));
            }
            2 => {
                // Truncate or corrupt an existing file
                if let Ok(entries) = std::fs::read_dir(&base) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.is_file() && chunk.len() >= 3 {
                            let _ = std::fs::write(&path, &chunk[2..]);
                            break;
                        }
                    }
                }
            }
            3 => {
                // Remove a random file or directory
                if let Ok(entries) = std::fs::read_dir(&base) {
                    for entry in entries.flatten() {
                        let _ = std::fs::remove_file(entry.path());
                        break;
                    }
                }
            }
            _ => {}
        }
    }

    // Open the corrupted queue and verify no panic on operations.
    let queue = match Queue::open(
        tmp.path(),
        &OpenOptions {
            allow_unsupported_fs: true,
            ..Default::default()
        },
    ) {
        Ok(q) => q,
        Err(_) => return,
    };

    // fsck should not panic
    let report = queue.fsck(&Default::default());
    // Just accessing the report should not panic
    let _ = report.findings.len();

    // inspect should not panic
    let _ = queue.inspect(&[0xFF; 16]);
});
