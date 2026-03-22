#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(line) = std::str::from_utf8(data) {
        // Single-line parser — must not panic on any input.
        let _ = exchange_net::ilp::parser::parse_ilp_line(line);

        // Batch parser — must not panic on any input.
        let _ = exchange_net::ilp::parser::parse_ilp_batch(line);
    }
});
