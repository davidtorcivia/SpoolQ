// Fuzz target: spoolq-math bucket arithmetic.
// Property: no panic, no overflow on any u64 input.

#![no_main]
use libfuzzer_sys::fuzz_target;
use spoolq_math;

fuzz_target!(|(timestamp, width): (u64, u64)| {
    if width > 0 {
        let _ = spoolq_math::bucket_number(timestamp, width);
        let _ = spoolq_math::ceiling_bucket(timestamp, width);
        let _ = spoolq_math::eligibility_bucket_and_ns(timestamp, width);
        let _ = spoolq_math::bucket_end_ns(timestamp, width);
    }
});
