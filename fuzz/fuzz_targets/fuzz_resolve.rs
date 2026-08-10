// Fuzz target: ticket resolution.
// Property: no panic when resolving arbitrary ticket JSON.
//
// Input: arbitrary bytes interpreted as ticket JSON.

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

    let ticket = match steadq_core::TransitionTicket::from_json(data) {
        Ok(t) => t,
        Err(_) => return,
    };

    // Resolve with and without stabilization. Should never panic.
    let _ = queue.resolve(&ticket, false);
    let _ = queue.resolve(&ticket, true);
});
