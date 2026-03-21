//! Comprehensive tenant and metering tests (40 tests).
//!
//! Covers tenant management (create, get, list, delete), usage tracking,
//! storage isolation, and metering (record, persist, load).

use std::fs;
use std::path::PathBuf;

use exchange_core::metering::{CounterSnapshot, UsageMeter};
use exchange_core::tenant::{Tenant, TenantManager};
use tempfile::TempDir;

fn make_tenant(id: &str) -> Tenant {
    Tenant {
        id: id.to_string(),
        name: format!("Tenant {id}"),
        namespace: id.to_string(),
        storage_quota: 1_000_000,
        query_quota: 10,
        created_at: 1_700_000_000,
    }
}

// ---------------------------------------------------------------------------
// mod tenant_management
// ---------------------------------------------------------------------------

mod tenant_management {
    use super::*;

    #[test]
    fn create_tenant() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        mgr.create_tenant(&make_tenant("t1")).unwrap();
    }

    #[test]
    fn get_tenant() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        mgr.create_tenant(&make_tenant("t1")).unwrap();
        let t = mgr.get_tenant("t1").unwrap().unwrap();
        assert_eq!(t.id, "t1");
        assert_eq!(t.name, "Tenant t1");
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        assert!(mgr.get_tenant("nope").unwrap().is_none());
    }

    #[test]
    fn list_tenants_empty() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        assert!(mgr.list_tenants().unwrap().is_empty());
    }

    #[test]
    fn list_tenants_sorted() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        mgr.create_tenant(&make_tenant("t3")).unwrap();
        mgr.create_tenant(&make_tenant("t1")).unwrap();
        mgr.create_tenant(&make_tenant("t2")).unwrap();
        let tenants = mgr.list_tenants().unwrap();
        assert_eq!(tenants.len(), 3);
        assert_eq!(tenants[0].id, "t1");
        assert_eq!(tenants[1].id, "t2");
        assert_eq!(tenants[2].id, "t3");
    }

    #[test]
    fn delete_tenant() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        mgr.create_tenant(&make_tenant("t1")).unwrap();
        mgr.delete_tenant("t1").unwrap();
        assert!(mgr.get_tenant("t1").unwrap().is_none());
    }

    #[test]
    fn delete_nonexistent_fails() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        assert!(mgr.delete_tenant("nope").is_err());
    }

    #[test]
    fn duplicate_tenant_fails() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        mgr.create_tenant(&make_tenant("t1")).unwrap();
        assert!(mgr.create_tenant(&make_tenant("t1")).is_err());
    }

    #[test]
    fn create_provisions_data_dir() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        mgr.create_tenant(&make_tenant("t1")).unwrap();
        assert!(dir.path().join("t1").exists());
    }

    #[test]
    fn delete_removes_data_dir() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        mgr.create_tenant(&make_tenant("t1")).unwrap();
        assert!(dir.path().join("t1").exists());
        mgr.delete_tenant("t1").unwrap();
        assert!(!dir.path().join("t1").exists());
    }

    #[test]
    fn storage_isolation_directories() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        mgr.create_tenant(&make_tenant("t1")).unwrap();
        mgr.create_tenant(&make_tenant("t2")).unwrap();
        assert!(dir.path().join("t1").exists());
        assert!(dir.path().join("t2").exists());
    }

    #[test]
    fn usage_tracking_empty_tenant() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        mgr.create_tenant(&make_tenant("t1")).unwrap();
        let usage = mgr.get_usage("t1").unwrap();
        assert_eq!(usage.storage_bytes, 0);
        assert_eq!(usage.table_count, 0);
    }

    #[test]
    fn usage_tracking_with_data() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        mgr.create_tenant(&make_tenant("t1")).unwrap();

        // Simulate a table
        let table_dir = dir.path().join("t1").join("trades");
        fs::create_dir_all(&table_dir).unwrap();
        fs::write(table_dir.join("data.bin"), vec![0u8; 1024]).unwrap();

        let usage = mgr.get_usage("t1").unwrap();
        assert_eq!(usage.table_count, 1);
        assert!(usage.storage_bytes >= 1024);
    }

    #[test]
    fn usage_tracking_isolation() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        mgr.create_tenant(&make_tenant("t1")).unwrap();
        mgr.create_tenant(&make_tenant("t2")).unwrap();

        // Write data only in t1
        let table_dir = dir.path().join("t1").join("data");
        fs::create_dir_all(&table_dir).unwrap();
        fs::write(table_dir.join("col.d"), vec![0u8; 512]).unwrap();

        let u1 = mgr.get_usage("t1").unwrap();
        let u2 = mgr.get_usage("t2").unwrap();
        assert!(u1.storage_bytes >= 512);
        assert_eq!(u2.storage_bytes, 0);
    }

    #[test]
    fn usage_nonexistent_tenant_fails() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        assert!(mgr.get_usage("nope").is_err());
    }

    #[test]
    fn tenant_quota_fields_preserved() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        let mut t = make_tenant("q1");
        t.storage_quota = 5_000_000;
        t.query_quota = 50;
        mgr.create_tenant(&t).unwrap();
        let loaded = mgr.get_tenant("q1").unwrap().unwrap();
        assert_eq!(loaded.storage_quota, 5_000_000);
        assert_eq!(loaded.query_quota, 50);
    }

    #[test]
    fn tenant_namespace_preserved() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        let mut t = make_tenant("ns1");
        t.namespace = "custom_ns".into();
        mgr.create_tenant(&t).unwrap();
        let loaded = mgr.get_tenant("ns1").unwrap().unwrap();
        assert_eq!(loaded.namespace, "custom_ns");
    }

    #[test]
    fn create_delete_create_works() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        mgr.create_tenant(&make_tenant("t1")).unwrap();
        mgr.delete_tenant("t1").unwrap();
        mgr.create_tenant(&make_tenant("t1")).unwrap();
        assert!(mgr.get_tenant("t1").unwrap().is_some());
    }

    #[test]
    fn ten_tenants() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        for i in 0..10 {
            mgr.create_tenant(&make_tenant(&format!("tenant{i:02}"))).unwrap();
        }
        assert_eq!(mgr.list_tenants().unwrap().len(), 10);
    }
}

// ---------------------------------------------------------------------------
// mod metering
// ---------------------------------------------------------------------------

mod metering {
    use super::*;

    #[test]
    fn record_query_increments() {
        let dir = TempDir::new().unwrap();
        let meter = UsageMeter::new(dir.path().to_path_buf());
        meter.record_query("t1", 100, 4096);
        meter.record_query("t1", 50, 2048);
        let usage = meter.get_usage("t1");
        assert_eq!(usage.queries, 2);
        assert_eq!(usage.rows_read, 150);
        assert_eq!(usage.bytes_scanned, 6144);
    }

    #[test]
    fn record_write_increments() {
        let dir = TempDir::new().unwrap();
        let meter = UsageMeter::new(dir.path().to_path_buf());
        meter.record_write("t1", 500);
        meter.record_write("t1", 300);
        let usage = meter.get_usage("t1");
        assert_eq!(usage.rows_written, 800);
    }

    #[test]
    fn unknown_tenant_returns_zeros() {
        let dir = TempDir::new().unwrap();
        let meter = UsageMeter::new(dir.path().to_path_buf());
        let usage = meter.get_usage("nobody");
        assert_eq!(usage.queries, 0);
        assert_eq!(usage.rows_read, 0);
        assert_eq!(usage.rows_written, 0);
    }

    #[test]
    fn get_all_usage() {
        let dir = TempDir::new().unwrap();
        let meter = UsageMeter::new(dir.path().to_path_buf());
        meter.record_query("t1", 10, 100);
        meter.record_query("t2", 20, 200);
        let all = meter.get_all_usage();
        assert_eq!(all.len(), 2);
        assert_eq!(all["t1"].queries, 1);
        assert_eq!(all["t2"].queries, 1);
    }

    #[test]
    fn persist_creates_file() {
        let dir = TempDir::new().unwrap();
        let meter = UsageMeter::new(dir.path().to_path_buf());
        meter.record_query("t1", 100, 4096);
        meter.persist().unwrap();
        assert!(dir.path().join("_metering/usage.json").exists());
    }

    #[test]
    fn persist_and_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        {
            let meter = UsageMeter::new(dir.path().to_path_buf());
            meter.record_query("t1", 100, 4096);
            meter.record_write("t1", 50);
            meter.record_query("t2", 200, 8192);
            meter.persist().unwrap();
        }
        {
            let mut meter = UsageMeter::new(dir.path().to_path_buf());
            meter.load().unwrap();
            let t1 = meter.get_usage("t1");
            assert_eq!(t1.queries, 1);
            assert_eq!(t1.rows_read, 100);
            assert_eq!(t1.bytes_scanned, 4096);
            assert_eq!(t1.rows_written, 50);
        }
    }

    #[test]
    fn load_nonexistent_is_ok() {
        let dir = TempDir::new().unwrap();
        let mut meter = UsageMeter::new(dir.path().to_path_buf());
        meter.load().unwrap();
    }

    #[test]
    fn per_tenant_isolation() {
        let dir = TempDir::new().unwrap();
        let meter = UsageMeter::new(dir.path().to_path_buf());
        meter.record_query("t1", 100, 1000);
        meter.record_query("t2", 200, 2000);
        let t1 = meter.get_usage("t1");
        let t2 = meter.get_usage("t2");
        assert_eq!(t1.rows_read, 100);
        assert_eq!(t2.rows_read, 200);
    }

    #[test]
    fn multiple_persists_overwrites() {
        let dir = TempDir::new().unwrap();
        let meter = UsageMeter::new(dir.path().to_path_buf());
        meter.record_query("t1", 100, 1000);
        meter.persist().unwrap();
        meter.record_query("t1", 200, 2000);
        meter.persist().unwrap();

        let mut meter2 = UsageMeter::new(dir.path().to_path_buf());
        meter2.load().unwrap();
        let t1 = meter2.get_usage("t1");
        assert_eq!(t1.queries, 2);
        assert_eq!(t1.rows_read, 300);
    }

    #[test]
    fn counter_snapshot_serialization() {
        let snap = CounterSnapshot {
            queries: 10,
            rows_read: 1000,
            rows_written: 500,
            bytes_scanned: 65536,
            bytes_stored: 32768,
        };
        let json = serde_json::to_string(&snap).unwrap();
        let restored: CounterSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, snap);
    }

    #[test]
    fn many_tenants_metering() {
        let dir = TempDir::new().unwrap();
        let meter = UsageMeter::new(dir.path().to_path_buf());
        for i in 0..100 {
            meter.record_query(&format!("t{i}"), i as u64, i as u64 * 100);
        }
        let all = meter.get_all_usage();
        assert_eq!(all.len(), 100);
    }

    #[test]
    fn bytes_stored_field() {
        let dir = TempDir::new().unwrap();
        let meter = UsageMeter::new(dir.path().to_path_buf());
        // bytes_stored is not directly incremented by record_query/write
        // but we can verify it starts at 0
        let usage = meter.get_usage("t1");
        assert_eq!(usage.bytes_stored, 0);
    }
}
