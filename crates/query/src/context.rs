//! Execution context for query processing with RBAC and resource limits.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use exchange_common::error::{ExchangeDbError, Result};
use exchange_core::audit::AuditLog;
use exchange_core::mvcc::{MvccManager, SnapshotGuard};
use exchange_core::rbac::SecurityContext;
use exchange_core::resource::{QueryToken, ResourceManager};
use exchange_core::rls::RlsManager;

use crate::memory::QueryMemoryTracker;
use crate::timeout::QueryDeadline;

// ── Cancellation Token ─────────────────────────────────────────────────

/// A cooperative cancellation token.
///
/// A running query periodically calls [`CancellationToken::check`] to see if
/// cancellation has been requested. Cancellation is requested by calling
/// [`CancellationToken::cancel`] (typically from the HTTP cancel endpoint).
#[derive(Debug, Clone)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    /// Create a new, non-cancelled token.
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Request cancellation.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// Check if cancellation has been requested.
    /// Returns `Err` if the query should be aborted.
    pub fn check(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Relaxed) {
            Err(ExchangeDbError::Query(
                "query cancelled by user".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    /// Returns true if cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

// ── Query Registry ─────────────────────────────────────────────────────

/// Tracks active queries and their cancellation tokens.
///
/// Each query is assigned a unique ID. The registry allows external callers
/// (e.g., the HTTP cancel endpoint) to cancel a running query by ID.
pub struct QueryRegistry {
    next_id: AtomicU64,
    active: Mutex<HashMap<u64, CancellationToken>>,
}

impl QueryRegistry {
    /// Create a new, empty query registry.
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            active: Mutex::new(HashMap::new()),
        }
    }

    /// Register a new query and return its (id, cancellation_token).
    pub fn register(&self) -> (u64, CancellationToken) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let token = CancellationToken::new();
        self.active.lock().unwrap().insert(id, token.clone());
        (id, token)
    }

    /// Remove a query from the registry (called when query completes).
    pub fn deregister(&self, id: u64) {
        self.active.lock().unwrap().remove(&id);
    }

    /// Cancel a query by ID. Returns true if the query was found and cancelled.
    pub fn cancel(&self, id: u64) -> bool {
        if let Some(token) = self.active.lock().unwrap().get(&id) {
            token.cancel();
            true
        } else {
            false
        }
    }

    /// List currently active query IDs.
    pub fn active_query_ids(&self) -> Vec<u64> {
        self.active.lock().unwrap().keys().copied().collect()
    }

    /// Number of currently active queries.
    pub fn active_count(&self) -> usize {
        self.active.lock().unwrap().len()
    }
}

impl Default for QueryRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Context for query execution, carrying security and resource information.
pub struct ExecutionContext {
    /// Root directory of the database.
    pub db_root: PathBuf,
    /// Authenticated security context, if any.
    pub security: Option<SecurityContext>,
    /// Resource manager for admission control and limits.
    pub resource_mgr: Option<Arc<ResourceManager>>,
    /// Unique identifier for this query.
    pub query_id: u64,
    /// When this context was created.
    pub start_time: Instant,
    /// Whether INSERT operations should use WAL for durability.
    pub use_wal: bool,
    /// Per-query memory tracker to prevent OOM.
    pub memory_tracker: Option<Arc<QueryMemoryTracker>>,
    /// Per-query deadline for timeout enforcement.
    pub deadline: Option<QueryDeadline>,
    /// When true, route queries through the cursor-based execution engine.
    pub use_cursor_engine: bool,
    /// MVCC manager for snapshot isolation, if configured.
    pub mvcc: Option<Arc<MvccManager>>,
    /// Row-level security manager, if configured.
    pub rls: Option<Arc<RlsManager>>,
    /// The current user name (for RLS policy lookup).
    pub current_user: Option<String>,
    /// The original SQL text (for improved error messages).
    pub sql_text: Option<String>,
    /// Audit logger for recording security-relevant events.
    pub audit_log: Option<Arc<AuditLog>>,
    /// Optional replication manager for shipping WAL segments after SQL INSERT.
    pub replication_manager: Option<Arc<exchange_core::replication::ReplicationManager>>,
    /// Cancellation token for cooperative query cancellation.
    ///
    /// Long-running scan operations should periodically call `check_cancelled()`
    /// to honour cancel requests from the `/api/v1/query/cancel/{id}` endpoint.
    pub cancellation_token: Option<CancellationToken>,
}

impl ExecutionContext {
    /// Create a minimal context with no security or resource limits.
    pub fn anonymous(db_root: PathBuf) -> Self {
        Self {
            db_root,
            security: None,
            resource_mgr: None,
            query_id: 0,
            start_time: Instant::now(),
            use_wal: false,
            memory_tracker: None,
            deadline: None,
            use_cursor_engine: false,
            mvcc: None,
            rls: None,
            current_user: None,
            sql_text: None,
            audit_log: None,
            replication_manager: None,
            cancellation_token: None,
        }
    }

    /// Begin an MVCC snapshot if an MVCC manager is configured.
    /// The snapshot is automatically released when the guard is dropped.
    pub fn begin_snapshot(&self) -> Option<SnapshotGuard> {
        self.mvcc
            .as_ref()
            .map(|mgr| SnapshotGuard::new(Arc::clone(mgr)))
    }

    /// Get the RLS filter for the current user on a table, if any.
    pub fn get_rls_filter(&self, table: &str) -> Option<crate::plan::Filter> {
        let rls = self.rls.as_ref()?;
        let user = self.current_user.as_ref()?;
        let rls_filter = rls.get_filter(user, table)?;
        // Convert RLS filter to plan::Filter::In
        let values: Vec<crate::plan::Value> = rls_filter
            .allowed_values
            .iter()
            .map(|v| crate::plan::Value::Str(v.clone()))
            .collect();
        if values.is_empty() {
            // No allowed values = deny all: use a filter that matches nothing.
            Some(crate::plan::Filter::Eq(
                rls_filter.column,
                crate::plan::Value::Str("__rls_deny_all__".to_string()),
            ))
        } else {
            Some(crate::plan::Filter::In(rls_filter.column, values))
        }
    }

    /// Check that the current user has read access to the given table.
    ///
    /// If no security context is set (anonymous mode), access is allowed
    /// for backwards compatibility.
    pub fn check_read(&self, table: &str) -> Result<()> {
        if let Some(ref sec) = self.security
            && !sec.can_read_table(table)
        {
            return Err(ExchangeDbError::PermissionDenied(format!(
                "user '{}' does not have READ permission on table '{}'",
                sec.user, table
            )));
        }
        Ok(())
    }

    /// Check that the current user has write access to the given table.
    pub fn check_write(&self, table: &str) -> Result<()> {
        if let Some(ref sec) = self.security
            && !sec.can_write_table(table)
        {
            return Err(ExchangeDbError::PermissionDenied(format!(
                "user '{}' does not have WRITE permission on table '{}'",
                sec.user, table
            )));
        }
        Ok(())
    }

    /// Check that the current user has DDL privileges (CREATE/ALTER/DROP TABLE).
    pub fn check_ddl(&self) -> Result<()> {
        if let Some(ref sec) = self.security
            && !sec.can_ddl()
        {
            return Err(ExchangeDbError::PermissionDenied(format!(
                "user '{}' does not have DDL permission",
                sec.user
            )));
        }
        Ok(())
    }

    /// Check that the current user has admin privileges.
    pub fn check_admin(&self) -> Result<()> {
        if let Some(ref sec) = self.security
            && !sec.can_admin()
        {
            return Err(ExchangeDbError::PermissionDenied(format!(
                "user '{}' does not have ADMIN permission",
                sec.user
            )));
        }
        Ok(())
    }

    /// Check whether the query has been cancelled via its cancellation token.
    ///
    /// Returns `Ok(())` if no token is set or if cancellation has not been
    /// requested. Returns an error if the query should be aborted.
    pub fn check_cancelled(&self) -> Result<()> {
        if let Some(ref token) = self.cancellation_token {
            token.check()
        } else {
            Ok(())
        }
    }

    /// Check whether the query deadline has been exceeded.
    ///
    /// Returns `Ok(())` if no deadline is set or if the deadline has not
    /// yet been reached. Returns an error if the query has timed out.
    pub fn check_timeout(&self) -> Result<()> {
        if let Some(ref deadline) = self.deadline {
            deadline.check()
        } else {
            Ok(())
        }
    }

    /// Try to allocate `bytes` of memory against the per-query budget.
    ///
    /// Returns `Ok(())` if no memory tracker is configured or if the
    /// allocation succeeded. Returns an error if the limit is exceeded.
    pub fn try_allocate_memory(&self, bytes: u64) -> Result<()> {
        if let Some(ref tracker) = self.memory_tracker {
            tracker.try_allocate(bytes)
        } else {
            Ok(())
        }
    }

    /// Release `bytes` of memory from the per-query budget.
    pub fn release_memory(&self, bytes: u64) {
        if let Some(ref tracker) = self.memory_tracker {
            tracker.release(bytes);
        }
    }

    /// Try to admit this query through the resource manager.
    ///
    /// Returns `Some(QueryToken)` if a resource manager is configured and
    /// the query was admitted, `None` if no resource manager is configured.
    /// Returns an error if the resource limit has been reached.
    pub fn admit_query(&self) -> Result<Option<QueryToken>> {
        match self.resource_mgr {
            Some(ref mgr) => {
                let token = mgr
                    .try_admit()
                    .map_err(|e| ExchangeDbError::ResourceExhausted(e.to_string()))?;
                Ok(Some(token))
            }
            None => Ok(None),
        }
    }

    /// Release a query token back to the resource manager.
    pub fn release_query(&self, token: QueryToken) {
        if let Some(ref mgr) = self.resource_mgr {
            mgr.release(token);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exchange_core::rbac::{Permission, SecurityContext};
    use exchange_core::resource::ResourceLimits;
    use std::time::Duration;

    fn ctx_with_security(permissions: Vec<Permission>) -> ExecutionContext {
        ExecutionContext {
            db_root: PathBuf::from("/tmp/test"),
            security: Some(SecurityContext {
                user: "testuser".to_string(),
                roles: vec!["testrole".to_string()],
                permissions,
            }),
            resource_mgr: None,
            query_id: 1,
            start_time: Instant::now(),
            use_wal: false,
            memory_tracker: None,
            deadline: None,
            use_cursor_engine: false,
            mvcc: None,
            rls: None,
            current_user: None,
            sql_text: None,
            audit_log: None,
            replication_manager: None,
            cancellation_token: None,
        }
    }

    fn anonymous_ctx() -> ExecutionContext {
        ExecutionContext::anonymous(PathBuf::from("/tmp/test"))
    }

    // ── Anonymous (no security) tests ────────────────────────────────

    #[test]
    fn anonymous_allows_read() {
        let ctx = anonymous_ctx();
        assert!(ctx.check_read("trades").is_ok());
    }

    #[test]
    fn anonymous_allows_write() {
        let ctx = anonymous_ctx();
        assert!(ctx.check_write("trades").is_ok());
    }

    #[test]
    fn anonymous_allows_ddl() {
        let ctx = anonymous_ctx();
        assert!(ctx.check_ddl().is_ok());
    }

    #[test]
    fn anonymous_allows_admin() {
        let ctx = anonymous_ctx();
        assert!(ctx.check_admin().is_ok());
    }

    // ── Permission granted tests ─────────────────────────────────────

    #[test]
    fn select_with_read_permission_succeeds() {
        let ctx = ctx_with_security(vec![Permission::Read { table: None }]);
        assert!(ctx.check_read("trades").is_ok());
    }

    #[test]
    fn insert_with_write_permission_succeeds() {
        let ctx = ctx_with_security(vec![Permission::Write {
            table: Some("trades".to_string()),
        }]);
        assert!(ctx.check_write("trades").is_ok());
    }

    #[test]
    fn ddl_with_ddl_permission_succeeds() {
        let ctx = ctx_with_security(vec![Permission::DDL]);
        assert!(ctx.check_ddl().is_ok());
    }

    #[test]
    fn admin_with_admin_permission_succeeds() {
        let ctx = ctx_with_security(vec![Permission::Admin]);
        assert!(ctx.check_admin().is_ok());
    }

    // ── Permission denied tests ──────────────────────────────────────

    #[test]
    fn select_without_read_permission_denied() {
        let ctx = ctx_with_security(vec![Permission::Write { table: None }]);
        let err = ctx.check_read("trades").unwrap_err();
        assert!(matches!(err, ExchangeDbError::PermissionDenied(_)));
        assert!(err.to_string().contains("READ"));
    }

    #[test]
    fn insert_without_write_permission_denied() {
        let ctx = ctx_with_security(vec![Permission::Read { table: None }]);
        let err = ctx.check_write("trades").unwrap_err();
        assert!(matches!(err, ExchangeDbError::PermissionDenied(_)));
        assert!(err.to_string().contains("WRITE"));
    }

    #[test]
    fn ddl_without_ddl_permission_denied() {
        let ctx = ctx_with_security(vec![Permission::Read { table: None }]);
        let err = ctx.check_ddl().unwrap_err();
        assert!(matches!(err, ExchangeDbError::PermissionDenied(_)));
        assert!(err.to_string().contains("DDL"));
    }

    #[test]
    fn admin_without_admin_permission_denied() {
        let ctx = ctx_with_security(vec![Permission::DDL]);
        let err = ctx.check_admin().unwrap_err();
        assert!(matches!(err, ExchangeDbError::PermissionDenied(_)));
        assert!(err.to_string().contains("ADMIN"));
    }

    #[test]
    fn no_permissions_denies_everything() {
        let ctx = ctx_with_security(vec![]);
        assert!(ctx.check_read("trades").is_err());
        assert!(ctx.check_write("trades").is_err());
        assert!(ctx.check_ddl().is_err());
        assert!(ctx.check_admin().is_err());
    }

    // ── Resource limit tests ─────────────────────────────────────────

    #[test]
    fn admit_query_without_resource_manager() {
        let ctx = anonymous_ctx();
        let token = ctx.admit_query().unwrap();
        assert!(token.is_none());
    }

    #[test]
    fn admit_query_succeeds_within_limit() {
        let mgr = Arc::new(ResourceManager::new(ResourceLimits {
            max_concurrent_queries: 2,
            ..ResourceLimits::default()
        }));
        let ctx = ExecutionContext {
            db_root: PathBuf::from("/tmp/test"),
            security: None,
            resource_mgr: Some(mgr),
            query_id: 1,
            start_time: Instant::now(),
            use_wal: false,
            memory_tracker: None,
            deadline: None,
            use_cursor_engine: false,
            mvcc: None,
            rls: None,
            current_user: None,
            sql_text: None,
            audit_log: None,
            replication_manager: None,
            cancellation_token: None,
        };
        let token = ctx.admit_query().unwrap();
        assert!(token.is_some());
        ctx.release_query(token.unwrap());
    }

    #[test]
    fn admit_query_resource_exhausted() {
        let mgr = Arc::new(ResourceManager::new(ResourceLimits {
            max_concurrent_queries: 1,
            max_memory_bytes: 1024,
            max_query_time: Duration::from_secs(10),
            max_result_rows: 100,
            max_scan_bytes: 4096,
        }));

        // Admit the first query to fill the limit.
        let _token = mgr.try_admit().unwrap();

        let ctx = ExecutionContext {
            db_root: PathBuf::from("/tmp/test"),
            security: None,
            resource_mgr: Some(mgr),
            query_id: 2,
            start_time: Instant::now(),
            use_wal: false,
            memory_tracker: None,
            deadline: None,
            use_cursor_engine: false,
            mvcc: None,
            rls: None,
            current_user: None,
            sql_text: None,
            audit_log: None,
            replication_manager: None,
            cancellation_token: None,
        };
        let result = ctx.admit_query();
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            ExchangeDbError::ResourceExhausted(_)
        ));
    }
}
