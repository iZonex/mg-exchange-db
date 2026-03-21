//! Diagnostics endpoint for ExchangeDB.
//!
//! `GET /api/v1/diagnostics` returns comprehensive system information
//! including version, OS, memory usage, storage stats, and configuration.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::response::IntoResponse;
use serde::Serialize;

use super::handlers::AppState;

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct DiagnosticsResponse {
    pub version: &'static str,
    pub rust_version: &'static str,
    pub os: &'static str,
    pub arch: &'static str,
    pub pid: u32,
    pub uptime_secs: f64,
    pub memory: MemoryInfo,
    pub storage: StorageInfo,
    pub connections: ConnectionInfo,
    pub wal: WalInfo,
    pub config: ConfigInfo,
}

#[derive(Debug, Serialize)]
pub struct MemoryInfo {
    pub rss_bytes: u64,
    pub heap_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct StorageInfo {
    pub data_dir: String,
    pub disk_free_bytes: u64,
    pub tables: usize,
    pub total_rows: u64,
}

#[derive(Debug, Serialize)]
pub struct ConnectionInfo {
    pub http: u64,
    pub pgwire: u64,
    pub ilp: u64,
}

#[derive(Debug, Serialize)]
pub struct WalInfo {
    pub pending_segments: u64,
    pub applied_segments: u64,
}

#[derive(Debug, Serialize)]
pub struct ConfigInfo {
    pub http_port: u16,
    pub pg_port: u16,
    pub ilp_port: u16,
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `GET /api/v1/diagnostics` -- return comprehensive system diagnostics.
pub async fn diagnostics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let uptime = state.start_time.elapsed().as_secs_f64();
    let pid = std::process::id();

    // Count tables and rows.
    let (table_count, total_rows) = count_tables_and_rows(&state.db_root);

    // Disk free space (best-effort).
    let disk_free = disk_free_bytes(&state.db_root);

    // RSS (best-effort, platform-specific).
    let rss = get_rss_bytes();

    let resp = DiagnosticsResponse {
        version: env!("CARGO_PKG_VERSION"),
        rust_version: built_rust_version(),
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        pid,
        uptime_secs: uptime,
        memory: MemoryInfo {
            rss_bytes: rss,
            heap_bytes: 0, // Not easily available without a custom allocator.
        },
        storage: StorageInfo {
            data_dir: state.db_root.display().to_string(),
            disk_free_bytes: disk_free,
            tables: table_count,
            total_rows,
        },
        connections: ConnectionInfo {
            http: state
                .metrics
                .connections_http
                .load(std::sync::atomic::Ordering::Relaxed) as u64,
            pgwire: state
                .metrics
                .connections_pg
                .load(std::sync::atomic::Ordering::Relaxed) as u64,
            ilp: state
                .metrics
                .connections_ilp
                .load(std::sync::atomic::Ordering::Relaxed) as u64,
        },
        wal: WalInfo {
            pending_segments: state
                .metrics
                .wal_segments_total
                .load(std::sync::atomic::Ordering::Relaxed)
                .saturating_sub(
                    state
                        .metrics
                        .wal_segments_applied
                        .load(std::sync::atomic::Ordering::Relaxed),
                ),
            applied_segments: state
                .metrics
                .wal_segments_applied
                .load(std::sync::atomic::Ordering::Relaxed),
        },
        config: ConfigInfo {
            http_port: 9000,
            pg_port: 8812,
            ilp_port: 9009,
        },
    };

    Json(resp)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn built_rust_version() -> &'static str {
    // The rustc version used to compile. We use a compile-time env var set
    // by cargo. Falls back to a static string.
    option_env!("RUSTC_VERSION").unwrap_or("unknown")
}

fn count_tables_and_rows(db_root: &std::path::Path) -> (usize, u64) {
    let Ok(entries) = std::fs::read_dir(db_root) else {
        return (0, 0);
    };

    let mut table_count = 0usize;
    let mut total_rows = 0u64;

    for entry in entries.flatten() {
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let meta_path = entry.path().join("_meta");
        if !meta_path.exists() {
            continue;
        }
        table_count += 1;

        // Estimate row count from first fixed-width column file sizes.
        if let Ok(meta) = exchange_core::table::TableMeta::load(&meta_path) {
            let first_fixed = meta.columns.iter().find(|c| {
                let ct: exchange_common::types::ColumnType = c.col_type.into();
                ct.fixed_size().is_some()
            });
            if let Some(col) = first_fixed {
                let ct: exchange_common::types::ColumnType = col.col_type.into();
                let elem_size = ct.fixed_size().unwrap() as u64;
                if let Ok(partitions) = std::fs::read_dir(entry.path()) {
                    for part in partitions.flatten() {
                        if part.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                            let col_file = part.path().join(format!("{}.d", col.name));
                            if let Ok(m) = std::fs::metadata(&col_file) {
                                total_rows += m.len() / elem_size;
                            }
                        }
                    }
                }
            }
        }
    }

    (table_count, total_rows)
}

/// Best-effort free disk space for the given path.
#[cfg(unix)]
fn disk_free_bytes(path: &std::path::Path) -> u64 {
    use std::ffi::CString;
    use std::mem::MaybeUninit;

    let path_str = path.to_string_lossy();
    let c_path = match CString::new(path_str.as_bytes()) {
        Ok(p) => p,
        Err(_) => return 0,
    };

    unsafe {
        let mut stat = MaybeUninit::<libc::statvfs>::uninit();
        if libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) == 0 {
            let stat = stat.assume_init();
            stat.f_bavail as u64 * stat.f_frsize
        } else {
            0
        }
    }
}

#[cfg(not(unix))]
fn disk_free_bytes(_path: &std::path::Path) -> u64 {
    0
}

/// Best-effort RSS (resident set size) in bytes.
#[cfg(target_os = "macos")]
fn get_rss_bytes() -> u64 {
    use std::mem::MaybeUninit;

    unsafe {
        let mut info = MaybeUninit::<libc::rusage>::uninit();
        if libc::getrusage(libc::RUSAGE_SELF, info.as_mut_ptr()) == 0 {
            let info = info.assume_init();
            // On macOS ru_maxrss is in bytes.
            info.ru_maxrss as u64
        } else {
            0
        }
    }
}

#[cfg(target_os = "linux")]
fn get_rss_bytes() -> u64 {
    // Read from /proc/self/statm -- field 1 is RSS in pages.
    if let Ok(contents) = std::fs::read_to_string("/proc/self/statm") {
        let fields: Vec<&str> = contents.split_whitespace().collect();
        if let Some(rss_pages) = fields.get(1) {
            if let Ok(pages) = rss_pages.parse::<u64>() {
                return pages * 4096;
            }
        }
    }
    0
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn get_rss_bytes() -> u64 {
    0
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostics_response_serializes_to_json() {
        let resp = DiagnosticsResponse {
            version: "0.1.0",
            rust_version: "1.85",
            os: "darwin",
            arch: "aarch64",
            pid: 12345,
            uptime_secs: 3600.0,
            memory: MemoryInfo {
                rss_bytes: 100_000_000,
                heap_bytes: 50_000_000,
            },
            storage: StorageInfo {
                data_dir: "./data".to_string(),
                disk_free_bytes: 500_000_000,
                tables: 5,
                total_rows: 1_000_000,
            },
            connections: ConnectionInfo {
                http: 10,
                pgwire: 3,
                ilp: 2,
            },
            wal: WalInfo {
                pending_segments: 0,
                applied_segments: 15,
            },
            config: ConfigInfo {
                http_port: 9000,
                pg_port: 8812,
                ilp_port: 9009,
            },
        };

        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["version"], "0.1.0");
        assert_eq!(json["os"], "darwin");
        assert_eq!(json["arch"], "aarch64");
        assert_eq!(json["pid"], 12345);
        assert_eq!(json["uptime_secs"], 3600.0);
        assert_eq!(json["memory"]["rss_bytes"], 100_000_000);
        assert_eq!(json["memory"]["heap_bytes"], 50_000_000);
        assert_eq!(json["storage"]["data_dir"], "./data");
        assert_eq!(json["storage"]["disk_free_bytes"], 500_000_000);
        assert_eq!(json["storage"]["tables"], 5);
        assert_eq!(json["storage"]["total_rows"], 1_000_000);
        assert_eq!(json["connections"]["http"], 10);
        assert_eq!(json["connections"]["pgwire"], 3);
        assert_eq!(json["connections"]["ilp"], 2);
        assert_eq!(json["wal"]["pending_segments"], 0);
        assert_eq!(json["wal"]["applied_segments"], 15);
        assert_eq!(json["config"]["http_port"], 9000);
        assert_eq!(json["config"]["pg_port"], 8812);
        assert_eq!(json["config"]["ilp_port"], 9009);
    }
}
