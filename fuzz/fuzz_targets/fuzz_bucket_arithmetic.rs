// Fuzz target: steadq-math bucket arithmetic.
// Property: no panic, no overflow on any u64 input.

#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() >= 16 {
        let timestamp = u64::from_be_bytes(data[0..8].try_into().unwrap());
        let width = u64::from_be_bytes(data[8..16].try_into().unwrap());
        if width > 0 {
            let _ = steadq_math::bucket_number(timestamp, width);
            let _ = steadq_math::ceiling_bucket(timestamp, width);
            let _ = steadq_math::eligibility_bucket_and_ns(timestamp, width);
            let _ = steadq_math::bucket_end_ns(timestamp, width);
        }
    }
});
