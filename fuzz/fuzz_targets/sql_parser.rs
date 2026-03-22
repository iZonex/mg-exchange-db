#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Only feed valid UTF-8 to the SQL parser.
    if let Ok(sql) = std::str::from_utf8(data) {
        // plan_query may return Ok or Err — both are fine.
        // We only care that it does not panic or crash.
        let _ = exchange_query::planner::plan_query(sql);
    }
});
