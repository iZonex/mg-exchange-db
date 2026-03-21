//! RBAC data model: users, roles, permissions, and security contexts.

use serde::{Deserialize, Serialize};

/// A database user with credentials and role assignments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub username: String,
    /// Password hash (argon2id PHC string, or legacy SHA-256 hex).
    pub password_hash: String,
    pub roles: Vec<String>,
    pub enabled: bool,
    /// Unix timestamp (seconds) when the user was created.
    pub created_at: i64,
}

/// A named role containing a set of permissions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    pub name: String,
    pub permissions: Vec<Permission>,
}

/// A discrete permission that can be granted to a role.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Permission {
    /// Full admin access.
    Admin,
    /// Read from a specific table (`None` = all tables).
    Read { table: Option<String> },
    /// Write to a specific table (`None` = all tables).
    Write { table: Option<String> },
    /// Create / drop / alter tables.
    DDL,
    /// Read only specific columns from a table.
    ColumnRead {
        table: String,
        columns: Vec<String>,
    },
    /// System operations (VACUUM, snapshot, replication, etc.).
    System,
}

/// The resolved security context for an authenticated session.
#[derive(Debug, Clone)]
pub struct SecurityContext {
    pub user: String,
    pub roles: Vec<String>,
    pub permissions: Vec<Permission>,
}

impl SecurityContext {
    /// Returns `true` if the context allows reading the given table.
    pub fn can_read_table(&self, table: &str) -> bool {
        self.permissions.iter().any(|p| match p {
            Permission::Admin => true,
            Permission::Read { table: None } => true,
            Permission::Read {
                table: Some(t),
            } => t == table,
            Permission::ColumnRead { table: t, .. } => t == table,
            _ => false,
        })
    }

    /// Returns `true` if the context allows writing to the given table.
    pub fn can_write_table(&self, table: &str) -> bool {
        self.permissions.iter().any(|p| match p {
            Permission::Admin => true,
            Permission::Write { table: None } => true,
            Permission::Write {
                table: Some(t),
            } => t == table,
            _ => false,
        })
    }

    /// Returns `true` if the context allows reading a specific column in a table.
    ///
    /// Full table-level `Read` or `Admin` grants implicitly allow all columns.
    /// A `ColumnRead` grant restricts access to the listed columns only.
    pub fn can_read_column(&self, table: &str, column: &str) -> bool {
        let mut has_column_read_for_table = false;

        for p in &self.permissions {
            match p {
                Permission::Admin => return true,
                Permission::Read { table: None } => return true,
                Permission::Read {
                    table: Some(t),
                } if t == table => return true,
                Permission::ColumnRead {
                    table: t,
                    columns,
                } if t == table => {
                    has_column_read_for_table = true;
                    if columns.iter().any(|c| c == column) {
                        return true;
                    }
                }
                _ => {}
            }
        }

        // If there is a ColumnRead for this table but the column is not listed,
        // access is denied. If there is no grant at all, access is also denied.
        !has_column_read_for_table && false
    }

    /// Returns `true` if DDL operations (CREATE/DROP/ALTER TABLE) are allowed.
    pub fn can_ddl(&self) -> bool {
        self.permissions
            .iter()
            .any(|p| matches!(p, Permission::Admin | Permission::DDL))
    }

    /// Returns `true` if the context has admin privileges.
    pub fn can_admin(&self) -> bool {
        self.permissions
            .iter()
            .any(|p| matches!(p, Permission::Admin))
    }

    /// Returns `true` if this is the built-in superuser (admin role).
    pub fn is_superuser(&self) -> bool {
        self.can_admin()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx_with(permissions: Vec<Permission>) -> SecurityContext {
        SecurityContext {
            user: "test".to_string(),
            roles: vec!["testrole".to_string()],
            permissions,
        }
    }

    #[test]
    fn admin_can_do_everything() {
        let ctx = ctx_with(vec![Permission::Admin]);
        assert!(ctx.can_read_table("trades"));
        assert!(ctx.can_write_table("trades"));
        assert!(ctx.can_read_column("trades", "price"));
        assert!(ctx.can_ddl());
        assert!(ctx.can_admin());
        assert!(ctx.is_superuser());
    }

    #[test]
    fn read_all_tables() {
        let ctx = ctx_with(vec![Permission::Read { table: None }]);
        assert!(ctx.can_read_table("trades"));
        assert!(ctx.can_read_table("orders"));
        assert!(!ctx.can_write_table("trades"));
        assert!(!ctx.can_ddl());
    }

    #[test]
    fn read_specific_table() {
        let ctx = ctx_with(vec![Permission::Read {
            table: Some("trades".to_string()),
        }]);
        assert!(ctx.can_read_table("trades"));
        assert!(!ctx.can_read_table("orders"));
        assert!(ctx.can_read_column("trades", "price"));
        assert!(!ctx.can_read_column("orders", "price"));
    }

    #[test]
    fn write_specific_table() {
        let ctx = ctx_with(vec![Permission::Write {
            table: Some("trades".to_string()),
        }]);
        assert!(ctx.can_write_table("trades"));
        assert!(!ctx.can_write_table("orders"));
    }

    #[test]
    fn column_read_allows_only_listed_columns() {
        let ctx = ctx_with(vec![Permission::ColumnRead {
            table: "trades".to_string(),
            columns: vec!["price".to_string(), "volume".to_string()],
        }]);
        assert!(ctx.can_read_table("trades"));
        assert!(ctx.can_read_column("trades", "price"));
        assert!(ctx.can_read_column("trades", "volume"));
        assert!(!ctx.can_read_column("trades", "symbol"));
        assert!(!ctx.can_read_table("orders"));
    }

    #[test]
    fn ddl_permission() {
        let ctx = ctx_with(vec![Permission::DDL]);
        assert!(ctx.can_ddl());
        assert!(!ctx.can_admin());
        assert!(!ctx.can_read_table("trades"));
    }

    #[test]
    fn no_permissions() {
        let ctx = ctx_with(vec![]);
        assert!(!ctx.can_read_table("trades"));
        assert!(!ctx.can_write_table("trades"));
        assert!(!ctx.can_read_column("trades", "price"));
        assert!(!ctx.can_ddl());
        assert!(!ctx.can_admin());
        assert!(!ctx.is_superuser());
    }
}
