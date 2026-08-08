// Fuzz target: transition-ticket state and path validation.
// Property: arbitrary input exercises both path validators without panicking.

#![no_main]

use libfuzzer_sys::fuzz_target;
use steadq_core::TransitionTicket;

fuzz_target!(|data: &[u8]| {
    let mut fields = data.splitn(4, |byte| *byte == b'\n');
    let Some(source_state) = fields.next().and_then(|field| std::str::from_utf8(field).ok()) else {
        return;
    };
    let Some(source_path) = fields.next().and_then(|field| std::str::from_utf8(field).ok()) else {
        return;
    };
    let Some(destination_state) = fields.next().and_then(|field| std::str::from_utf8(field).ok())
    else {
        return;
    };
    let Some(destination_path) = fields.next().and_then(|field| std::str::from_utf8(field).ok())
    else {
        return;
    };

    let ticket = TransitionTicket {
        job_id: [0; 16],
        source_state: source_state.into(),
        source_generation: 0,
        source_attempt: 0,
        source_relative_path: source_path.into(),
        attempted_destination_state: destination_state.into(),
        attempted_destination_relative_path: destination_path.into(),
        lease_token: None,
        envelope_digest: [0; 32],
    };
    let _ = ticket.validate_paths();
});
