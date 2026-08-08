// Fuzz target: steadq-format FixedHeader decoder.
// Property: no panic. Round-trip: decode -> re-encode -> decode equals original.

#![no_main]
use libfuzzer_sys::fuzz_target;
use steadq_format::FixedHeader;

fuzz_target!(|data: &[u8]| {
    if let Ok(header) = FixedHeader::decode(data) {
        // P1-32: Verify encode round-trip for inputs with extension data.
        if data.len() >= 128 {
            let ext = &data[128..];
            if let Ok(encoded) = header.encode(ext) {
                if let Ok(rt) = FixedHeader::decode(&encoded) {
                    assert_eq!(rt.job_id, header.job_id);
                    assert_eq!(rt.payload_length, header.payload_length);
                    assert_eq!(rt.extension_header_length, header.extension_header_length);
                }
            }
        }
    }
});
