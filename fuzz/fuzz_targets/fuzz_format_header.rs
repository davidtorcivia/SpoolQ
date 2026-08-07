// Fuzz target: spoolq-format FixedHeader decoder.
// Property: no panic, no unbounded allocation on any input.

#![no_main]
use libfuzzer_sys::fuzz_target;
use spoolq_format::FixedHeader;

fuzz_target!(|data: &[u8]| {
    let _ = FixedHeader::decode(data);
});
    }
