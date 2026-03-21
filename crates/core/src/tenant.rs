//! Multi-tenancy support for ExchangeDB.
//!
//! Each tenant gets an isolated namespace with its own storage directory,
//! storage quota, and query concurrency limit.

use std::fs;
use std::path::PathBuf;

use exchange_common::error::{ExchangeDbError, Result};

/// Represents a tenant in a multi-tenant deployment.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Tenant {
    /// Unique identifier.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Namespace prefix for tables (e.g., `tenant1.trades`).
    pub namespace: String,
    /// Maximum storage in bytes.
    pub storage_quota: u64,
    /// Maximum concurrent queries.
    pub query_quota: u32,
    /// Unix timestamp of creation.
    pub created_at: i64,
}

/// Current resource usage for a tenant.
#[derive(Debug, Clone)]
pub struct TenantUsage {
    /// Total bytes stored on disk.
    pub storage_bytes: u64,
    /// Number of tables owned by this tenant.
    pub table_count: u32,
    /// Number of currently active queries (placeholder).
    pub active_queries: u32,
}

/// Manages tenant lifecycle and storage isolation.
pub struct TenantManager {
    db_root: PathBuf,
}

impl TenantManager {
    /// Create a new tenant manager rooted at `db_root`.
    pub fn new(db_root: PathBuf) -> Self {
        Self { db_root }
    }

    /// Directory where tenant metadata is stored.
    fn tenants_dir(&self) -> PathBuf {
        self.db_root.join("_tenants")
    }

    /// Directory for a specific tenant's data.
    fn tenant_data_dir(&self, id: &str) -> PathBuf {
        self.db_root.join(id)
    }

    /// Path to the tenant's metadata JSON file.
    fn tenant_meta_path(&self, id: &str) -> PathBuf {
        self.tenants_dir().join(format!("{id}.json"))
    }

    /// Create a new tenant, provisioning its storage directory and metadata.
    pub fn create_tenant(&self, tenant: &Tenant) -> Result<()> {
        let meta_dir = self.tenants_dir();
        fs::create_dir_all(&meta_dir)?;

        let meta_path = self.tenant_meta_path(&tenant.id);
        if meta_path.exists() {
            return Err(ExchangeDbError::TableAlreadyExists(format!(
                "tenant '{}' already exists",
                tenant.id
            )));
        }

        // Create the tenant's isolated data directory.
        let data_dir = self.tenant_data_dir(&tenant.id);
        fs::create_dir_all(&data_dir)?;

        // Persist metadata.
        let json = serde_json::to_string_pretty(tenant)
            .map_err(|e| ExchangeDbError::Corruption(format!("serialize tenant: {e}")))?;
        fs::write(&meta_path, json)?;

        Ok(())
    }

    /// Retrieve a tenant by id, or `None` if it does not exist.
    pub fn get_tenant(&self, id: &str) -> Result<Option<Tenant>> {
        let meta_path = self.tenant_meta_path(id);
        if !meta_path.exists() {
            return Ok(None);
        }
        let data = fs::read_to_string(&meta_path)?;
        let tenant: Tenant = serde_json::from_str(&data)
            .map_err(|e| ExchangeDbError::Corruption(format!("deserialize tenant: {e}")))?;
        Ok(Some(tenant))
    }

    /// List all registered tenants.
    pub fn list_tenants(&self) -> Result<Vec<Tenant>> {
        let meta_dir = self.tenants_dir();
        if !meta_dir.exists() {
            return Ok(Vec::new());
        }

        let mut tenants = Vec::new();
        for entry in fs::read_dir(&meta_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("json") {
                let data = fs::read_to_string(&path)?;
                let tenant: Tenant = serde_json::from_str(&data).map_err(|e| {
                    ExchangeDbError::Corruption(format!("deserialize tenant: {e}"))
                })?;
                tenants.push(tenant);
            }
        }

        tenants.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(tenants)
    }

    /// Delete a tenant, removing its metadata and data directory.
    pub fn delete_tenant(&self, id: &str) -> Result<()> {
        let meta_path = self.tenant_meta_path(id);
        if !meta_path.exists() {
            return Err(ExchangeDbError::TableNotFound(format!(
                "tenant '{id}' not found"
            )));
        }

        fs::remove_file(&meta_path)?;

        let data_dir = self.tenant_data_dir(id);
        if data_dir.exists() {
            fs::remove_dir_all(&data_dir)?;
        }

        Ok(())
    }

    /// Get the current resource usage for a tenant.
    pub fn get_usage(&self, id: &str) -> Result<TenantUsage> {
        let data_dir = self.tenant_data_dir(id);
        if !data_dir.exists() {
            return Err(ExchangeDbError::TableNotFound(format!(
                "tenant '{id}' not found"
            )));
        }

        let mut storage_bytes: u64 = 0;
        let mut table_count: u32 = 0;

        if let Ok(entries) = fs::read_dir(&data_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    table_count += 1;
                    storage_bytes += dir_size(&path);
                } else if path.is_file() {
                    storage_bytes += entry.metadata().map(|m| m.len()).unwrap_or(0);
                }
            }
        }

        Ok(TenantUsage {
            storage_bytes,
            table_count,
            active_queries: 0,
        })
    }
}

/// Recursively calculate directory size.
fn dir_size(path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                total += dir_size(&p);
            } else {
                total += entry.metadata().map(|m| m.len()).unwrap_or(0);
            }
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_tenant(id: &str) -> Tenant {
        Tenant {
            id: id.to_string(),
            name: format!("Tenant {id}"),
            namespace: format!("{id}"),
            storage_quota: 1_000_000,
            query_quota: 10,
            created_at: 1700000000,
        }
    }

    #[test]
    fn create_and_get_tenant() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());

        let tenant = make_tenant("t1");
        mgr.create_tenant(&tenant).unwrap();

        let fetched = mgr.get_tenant("t1").unwrap().unwrap();
        assert_eq!(fetched.id, "t1");
        assert_eq!(fetched.name, "Tenant t1");
    }

    #[test]
    fn get_nonexistent_tenant_returns_none() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        assert!(mgr.get_tenant("nope").unwrap().is_none());
    }

    #[test]
    fn create_duplicate_tenant_fails() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());

        let tenant = make_tenant("t1");
        mgr.create_tenant(&tenant).unwrap();
        assert!(mgr.create_tenant(&tenant).is_err());
    }

    #[test]
    fn list_tenants() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());

        mgr.create_tenant(&make_tenant("t2")).unwrap();
        mgr.create_tenant(&make_tenant("t1")).unwrap();
        mgr.create_tenant(&make_tenant("t3")).unwrap();

        let tenants = mgr.list_tenants().unwrap();
        assert_eq!(tenants.len(), 3);
        // Should be sorted by id.
        assert_eq!(tenants[0].id, "t1");
        assert_eq!(tenants[1].id, "t2");
        assert_eq!(tenants[2].id, "t3");
    }

    #[test]
    fn delete_tenant() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());

        mgr.create_tenant(&make_tenant("t1")).unwrap();
        assert!(mgr.get_tenant("t1").unwrap().is_some());

        mgr.delete_tenant("t1").unwrap();
        assert!(mgr.get_tenant("t1").unwrap().is_none());
    }

    #[test]
    fn delete_nonexistent_tenant_fails() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        assert!(mgr.delete_tenant("nope").is_err());
    }

    #[test]
    fn tenant_storage_isolation() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());

        mgr.create_tenant(&make_tenant("t1")).unwrap();
        mgr.create_tenant(&make_tenant("t2")).unwrap();

        // Each tenant has its own directory.
        let t1_dir = dir.path().join("t1");
        let t2_dir = dir.path().join("t2");
        assert!(t1_dir.exists());
        assert!(t2_dir.exists());

        // Write a file in t1's directory (simulating a table).
        let table_dir = t1_dir.join("trades");
        fs::create_dir_all(&table_dir).unwrap();
        fs::write(table_dir.join("data.bin"), vec![0u8; 1024]).unwrap();

        // t1 usage should reflect the data.
        let usage = mgr.get_usage("t1").unwrap();
        assert_eq!(usage.table_count, 1);
        assert!(usage.storage_bytes >= 1024);

        // t2 usage should be empty.
        let usage2 = mgr.get_usage("t2").unwrap();
        assert_eq!(usage2.table_count, 0);
        assert_eq!(usage2.storage_bytes, 0);
    }

    #[test]
    fn tenant_usage_nonexistent_fails() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        assert!(mgr.get_usage("nope").is_err());
    }
}
