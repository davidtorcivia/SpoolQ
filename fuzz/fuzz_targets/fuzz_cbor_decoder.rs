// Fuzz target: spoolq-format CBOR extension header decoder.
// Property: no panic, no unbounded allocation, canonical rejection.

#![no_main]
use libfuzzer_sys::fuzz_target;
use spoolq_format::cbor::ExtensionHeader;

fuzz_target!(|data: &[u8]| {
    let _ = ExtensionHeader::decode(data);
});
