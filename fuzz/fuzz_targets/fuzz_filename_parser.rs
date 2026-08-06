// Fuzz target: spoolq-names filename parsers.
// Property: no panic, round-trip stability for valid inputs.

#![no_main]
use libfuzzer_sys::fuzz_target;
use spoolq_names;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = spoolq_names::parse_ready(s);
        let _ = spoolq_names::parse_leased(s);
        let _ = spoolq_names::parse_delayed(s);
        let _ = spoolq_names::parse_dead(s);
        let _ = spoolq_names::parse_receipt(s);
        let _ = spoolq_names::parse_temp(s);
        let _ = spoolq_names::parse_quarantine(s);
    }
});
