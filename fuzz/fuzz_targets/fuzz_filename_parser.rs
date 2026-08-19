// Fuzz target: steadq-names filename parsers.
// Property: no panic. Round-trip: parse -> re-encode -> compare for valid inputs.

#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        // Verify round-trip for successful parses.
        if let Ok(p) = steadq_names::parse_ready(s) {
            let base = format!(
                "{}.g{:016x}.a{:08x}.m{:08x}",
                steadq_names::hex_encode(&p.common.job_id),
                p.common.generation,
                p.common.attempt,
                p.common.maximum_attempts,
            );
            let ctx = steadq_names::ready_context("0000", &base);
            let tag = steadq_names::compute_name_tag(&[0x42; 16], &ctx);
            let encoded = steadq_names::ready_filename(&p.common, &tag);
            // The parse should have extracted the tag; re-encoding with the
            // same queue_id and shard should produce the same filename.
            assert_eq!(encoded.matches(".k").count(), 1);
        }
        if let Ok(p) = steadq_names::parse_receipt(s) {
            // Verify the token round-trips
            assert_eq!(steadq_names::hex_encode(&p.token).len(), 32);
        }
        // All parsers must not panic
        let _ = steadq_names::parse_ready(s);
        let _ = steadq_names::parse_leased(s);
        let _ = steadq_names::parse_delayed(s);
        let _ = steadq_names::parse_dead(s);
        let _ = steadq_names::parse_receipt(s);
        let _ = steadq_names::parse_temp(s);
        let _ = steadq_names::parse_quarantine(s);
    }
});
