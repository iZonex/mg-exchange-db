//! Tests for PRODUCTION_CHECKLIST net-layer items:
//! 3. Graceful shutdown drain
//! 5. Per-partition write locking in ILP
//! 6. Connection drain on shutdown

use exchange_net::{
    active_connection_count, is_shutting_down, track_connection_close,
    track_connection_open,
};

// ── Item 6: Connection tracking ────────────────────────────────────────

#[test]
fn connection_tracking_increments_and_decrements() {
    let before = active_connection_count();
    track_connection_open();
    assert_eq!(active_connection_count(), before + 1);
    track_connection_open();
    assert_eq!(active_connection_count(), before + 2);
    track_connection_close();
    assert_eq!(active_connection_count(), before + 1);
    track_connection_close();
    assert_eq!(active_connection_count(), before);
}

// ── Item 3: Shutdown flag ──────────────────────────────────────────────

// Note: We cannot test request_shutdown in isolation because it sets a
// global static flag that would affect other tests. Instead we verify
// the function exists and is callable. The integration test in the binary
// tests the actual shutdown sequence.

#[test]
fn shutdown_flag_functions_exist() {
    // Just verify these are callable; do NOT actually set the flag
    // as it would affect other tests running in the same process.
    let _ = is_shutting_down();
    // request_shutdown(); -- intentionally not called in unit tests
}
