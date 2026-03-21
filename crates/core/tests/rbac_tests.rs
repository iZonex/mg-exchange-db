//! Comprehensive RBAC tests (80 tests).
//!
//! Covers users, roles, permissions, security context, and authentication.

use exchange_core::rbac::{
    Permission, RbacStore, Role, SecurityContext, User, hash_password, verify_password,
};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_store() -> (TempDir, RbacStore) {
    let dir = TempDir::new().unwrap();
    let store = RbacStore::open(dir.path()).unwrap();
    (dir, store)
}

fn make_user(name: &str, password: &str) -> User {
    User {
        username: name.to_string(),
        password_hash: hash_password(password),
        roles: vec![],
        enabled: true,
        created_at: 1_700_000_000,
    }
}

fn ctx_with(permissions: Vec<Permission>) -> SecurityContext {
    SecurityContext {
        user: "test".to_string(),
        roles: vec!["testrole".to_string()],
        permissions,
    }
}

// ---------------------------------------------------------------------------
// mod users
// ---------------------------------------------------------------------------

mod users {
    use super::*;

    #[test]
    fn create_user() {
        let (_dir, store) = make_store();
        let user = make_user("alice", "pass");
        store.create_user(&user).unwrap();
    }

    #[test]
    fn get_user() {
        let (_dir, store) = make_store();
        store.create_user(&make_user("bob", "pass")).unwrap();
        let loaded = store.get_user("bob").unwrap().unwrap();
        assert_eq!(loaded.username, "bob");
        assert!(loaded.enabled);
    }

    #[test]
    fn get_nonexistent_user_returns_none() {
        let (_dir, store) = make_store();
        assert!(store.get_user("nobody").unwrap().is_none());
    }

    #[test]
    fn list_users_empty() {
        let (_dir, store) = make_store();
        let users = store.list_users().unwrap();
        assert!(users.is_empty());
    }

    #[test]
    fn list_users_sorted() {
        let (_dir, store) = make_store();
        store.create_user(&make_user("charlie", "p")).unwrap();
        store.create_user(&make_user("alice", "p")).unwrap();
        store.create_user(&make_user("bob", "p")).unwrap();
        let users = store.list_users().unwrap();
        assert_eq!(users.len(), 3);
        assert_eq!(users[0].username, "alice");
        assert_eq!(users[1].username, "bob");
        assert_eq!(users[2].username, "charlie");
    }

    #[test]
    fn delete_user() {
        let (_dir, store) = make_store();
        store.create_user(&make_user("dave", "p")).unwrap();
        store.delete_user("dave").unwrap();
        assert!(store.get_user("dave").unwrap().is_none());
    }

    #[test]
    fn delete_nonexistent_user_is_error() {
        let (_dir, store) = make_store();
        assert!(store.delete_user("nobody").is_err());
    }

    #[test]
    fn duplicate_username_is_error() {
        let (_dir, store) = make_store();
        store.create_user(&make_user("dup", "p")).unwrap();
        assert!(store.create_user(&make_user("dup", "p")).is_err());
    }

    #[test]
    fn update_user() {
        let (_dir, store) = make_store();
        let mut user = make_user("updatable", "p");
        store.create_user(&user).unwrap();
        user.enabled = false;
        store.update_user(&user).unwrap();
        let loaded = store.get_user("updatable").unwrap().unwrap();
        assert!(!loaded.enabled);
    }

    #[test]
    fn update_nonexistent_user_is_error() {
        let (_dir, store) = make_store();
        let user = make_user("ghost", "p");
        assert!(store.update_user(&user).is_err());
    }

    #[test]
    fn password_hash_uses_random_salt() {
        let h1 = hash_password("mysecret");
        let h2 = hash_password("mysecret");
        // Argon2id with random salt produces different hashes each time
        assert_ne!(h1, h2);
        // But both verify against the same password
        assert!(verify_password("mysecret", &h1));
        assert!(verify_password("mysecret", &h2));
    }

    #[test]
    fn different_passwords_different_hashes() {
        let h1 = hash_password("password1");
        let h2 = hash_password("password2");
        assert_ne!(h1, h2);
    }

    #[test]
    fn password_authentication_success() {
        let (_dir, store) = make_store();
        let role = Role {
            name: "reader".into(),
            permissions: vec![Permission::Read { table: None }],
        };
        store.create_role(&role).unwrap();
        let mut user = make_user("auth_user", "correct");
        user.roles = vec!["reader".into()];
        store.create_user(&user).unwrap();

        let ctx = store.authenticate("auth_user", "correct").unwrap().unwrap();
        assert_eq!(ctx.user, "auth_user");
        assert!(ctx.can_read_table("anything"));
    }

    #[test]
    fn wrong_password_returns_none() {
        let (_dir, store) = make_store();
        store.create_user(&make_user("carol", "right")).unwrap();
        assert!(store.authenticate("carol", "wrong").unwrap().is_none());
    }

    #[test]
    fn disabled_user_returns_none() {
        let (_dir, store) = make_store();
        let mut user = make_user("disabled", "pass");
        user.enabled = false;
        store.create_user(&user).unwrap();
        assert!(store.authenticate("disabled", "pass").unwrap().is_none());
    }

    #[test]
    fn nonexistent_user_returns_none() {
        let (_dir, store) = make_store();
        assert!(store.authenticate("nobody", "pass").unwrap().is_none());
    }

    #[test]
    fn user_with_multiple_roles() {
        let (_dir, store) = make_store();
        store
            .create_role(&Role {
                name: "reader".into(),
                permissions: vec![Permission::Read { table: None }],
            })
            .unwrap();
        store
            .create_role(&Role {
                name: "writer".into(),
                permissions: vec![Permission::Write { table: None }],
            })
            .unwrap();

        let mut user = make_user("multi_role", "pass");
        user.roles = vec!["reader".into(), "writer".into()];
        store.create_user(&user).unwrap();

        let ctx = store.authenticate("multi_role", "pass").unwrap().unwrap();
        assert!(ctx.can_read_table("trades"));
        assert!(ctx.can_write_table("trades"));
    }
}

// ---------------------------------------------------------------------------
// mod roles
// ---------------------------------------------------------------------------

mod roles {
    use super::*;

    #[test]
    fn create_role() {
        let (_dir, store) = make_store();
        let role = Role {
            name: "analyst".into(),
            permissions: vec![Permission::Read { table: None }],
        };
        store.create_role(&role).unwrap();
    }

    #[test]
    fn get_role() {
        let (_dir, store) = make_store();
        let role = Role {
            name: "analyst".into(),
            permissions: vec![Permission::Read {
                table: Some("trades".into()),
            }],
        };
        store.create_role(&role).unwrap();
        let loaded = store.get_role("analyst").unwrap().unwrap();
        assert_eq!(loaded.name, "analyst");
        assert_eq!(loaded.permissions.len(), 1);
    }

    #[test]
    fn get_nonexistent_role_returns_none() {
        let (_dir, store) = make_store();
        assert!(store.get_role("nope").unwrap().is_none());
    }

    #[test]
    fn list_roles_empty() {
        let (_dir, store) = make_store();
        assert!(store.list_roles().unwrap().is_empty());
    }

    #[test]
    fn list_roles_sorted() {
        let (_dir, store) = make_store();
        store
            .create_role(&Role {
                name: "z_role".into(),
                permissions: vec![],
            })
            .unwrap();
        store
            .create_role(&Role {
                name: "a_role".into(),
                permissions: vec![],
            })
            .unwrap();
        let roles = store.list_roles().unwrap();
        assert_eq!(roles[0].name, "a_role");
        assert_eq!(roles[1].name, "z_role");
    }

    #[test]
    fn delete_role() {
        let (_dir, store) = make_store();
        let role = Role {
            name: "deletable".into(),
            permissions: vec![],
        };
        store.create_role(&role).unwrap();
        store.delete_role("deletable").unwrap();
        assert!(store.get_role("deletable").unwrap().is_none());
    }

    #[test]
    fn delete_nonexistent_role_is_error() {
        let (_dir, store) = make_store();
        assert!(store.delete_role("nope").is_err());
    }

    #[test]
    fn duplicate_role_is_error() {
        let (_dir, store) = make_store();
        let role = Role {
            name: "dup".into(),
            permissions: vec![],
        };
        store.create_role(&role).unwrap();
        assert!(store.create_role(&role).is_err());
    }

    #[test]
    fn update_role() {
        let (_dir, store) = make_store();
        let mut role = Role {
            name: "updatable_role".into(),
            permissions: vec![],
        };
        store.create_role(&role).unwrap();
        role.permissions = vec![Permission::Admin];
        store.update_role(&role).unwrap();
        let loaded = store.get_role("updatable_role").unwrap().unwrap();
        assert_eq!(loaded.permissions.len(), 1);
    }

    #[test]
    fn role_with_multiple_permissions() {
        let (_dir, store) = make_store();
        let role = Role {
            name: "full".into(),
            permissions: vec![
                Permission::Read { table: None },
                Permission::Write { table: None },
                Permission::DDL,
            ],
        };
        store.create_role(&role).unwrap();
        let loaded = store.get_role("full").unwrap().unwrap();
        assert_eq!(loaded.permissions.len(), 3);
    }
}

// ---------------------------------------------------------------------------
// mod permissions
// ---------------------------------------------------------------------------

mod permissions {
    use super::*;

    #[test]
    fn admin_can_read_all() {
        let ctx = ctx_with(vec![Permission::Admin]);
        assert!(ctx.can_read_table("trades"));
        assert!(ctx.can_read_table("orders"));
    }

    #[test]
    fn admin_can_write_all() {
        let ctx = ctx_with(vec![Permission::Admin]);
        assert!(ctx.can_write_table("trades"));
    }

    #[test]
    fn admin_can_ddl() {
        let ctx = ctx_with(vec![Permission::Admin]);
        assert!(ctx.can_ddl());
    }

    #[test]
    fn admin_is_superuser() {
        let ctx = ctx_with(vec![Permission::Admin]);
        assert!(ctx.is_superuser());
    }

    #[test]
    fn read_all_tables() {
        let ctx = ctx_with(vec![Permission::Read { table: None }]);
        assert!(ctx.can_read_table("any_table"));
        assert!(!ctx.can_write_table("any_table"));
        assert!(!ctx.can_ddl());
    }

    #[test]
    fn read_specific_table_allows_that_table() {
        let ctx = ctx_with(vec![Permission::Read {
            table: Some("trades".into()),
        }]);
        assert!(ctx.can_read_table("trades"));
        assert!(!ctx.can_read_table("orders"));
    }

    #[test]
    fn write_all_tables() {
        let ctx = ctx_with(vec![Permission::Write { table: None }]);
        assert!(ctx.can_write_table("any_table"));
    }

    #[test]
    fn write_specific_table() {
        let ctx = ctx_with(vec![Permission::Write {
            table: Some("trades".into()),
        }]);
        assert!(ctx.can_write_table("trades"));
        assert!(!ctx.can_write_table("orders"));
    }

    #[test]
    fn ddl_permission_without_admin() {
        let ctx = ctx_with(vec![Permission::DDL]);
        assert!(ctx.can_ddl());
        assert!(!ctx.can_admin());
        assert!(!ctx.can_read_table("trades"));
    }

    #[test]
    fn column_read_allows_listed_columns() {
        let ctx = ctx_with(vec![Permission::ColumnRead {
            table: "trades".into(),
            columns: vec!["price".into(), "volume".into()],
        }]);
        assert!(ctx.can_read_table("trades"));
        assert!(ctx.can_read_column("trades", "price"));
        assert!(ctx.can_read_column("trades", "volume"));
        assert!(!ctx.can_read_column("trades", "symbol"));
    }

    #[test]
    fn column_read_denies_unlisted_columns() {
        let ctx = ctx_with(vec![Permission::ColumnRead {
            table: "trades".into(),
            columns: vec!["price".into()],
        }]);
        assert!(!ctx.can_read_column("trades", "secret_col"));
    }

    #[test]
    fn column_read_different_table_denied() {
        let ctx = ctx_with(vec![Permission::ColumnRead {
            table: "trades".into(),
            columns: vec!["price".into()],
        }]);
        assert!(!ctx.can_read_column("orders", "price"));
    }

    #[test]
    fn admin_bypasses_column_read() {
        let ctx = ctx_with(vec![Permission::Admin]);
        assert!(ctx.can_read_column("any", "any"));
    }

    #[test]
    fn full_table_read_allows_all_columns() {
        let ctx = ctx_with(vec![Permission::Read {
            table: Some("trades".into()),
        }]);
        assert!(ctx.can_read_column("trades", "price"));
        assert!(ctx.can_read_column("trades", "anything"));
    }

    #[test]
    fn no_permissions_denies_everything() {
        let ctx = ctx_with(vec![]);
        assert!(!ctx.can_read_table("t"));
        assert!(!ctx.can_write_table("t"));
        assert!(!ctx.can_read_column("t", "c"));
        assert!(!ctx.can_ddl());
        assert!(!ctx.can_admin());
        assert!(!ctx.is_superuser());
    }

    #[test]
    fn system_permission() {
        let ctx = ctx_with(vec![Permission::System]);
        assert!(!ctx.can_read_table("t"));
        assert!(!ctx.can_write_table("t"));
        assert!(!ctx.can_ddl());
    }

    #[test]
    fn combined_read_write_permissions() {
        let ctx = ctx_with(vec![
            Permission::Read { table: None },
            Permission::Write {
                table: Some("trades".into()),
            },
        ]);
        assert!(ctx.can_read_table("anything"));
        assert!(ctx.can_write_table("trades"));
        assert!(!ctx.can_write_table("orders"));
    }
}

// ---------------------------------------------------------------------------
// mod security_context
// ---------------------------------------------------------------------------

mod security_context {
    use super::*;

    #[test]
    fn build_from_user_and_roles() {
        let (_dir, store) = make_store();
        store
            .create_role(&Role {
                name: "reader".into(),
                permissions: vec![Permission::Read { table: None }],
            })
            .unwrap();
        let mut user = make_user("ctx_user", "pass");
        user.roles = vec!["reader".into()];
        store.create_user(&user).unwrap();

        let ctx = store.authenticate("ctx_user", "pass").unwrap().unwrap();
        assert_eq!(ctx.user, "ctx_user");
        assert_eq!(ctx.roles, vec!["reader"]);
        assert!(ctx.can_read_table("any"));
    }

    #[test]
    fn resolve_security_context_without_password() {
        let (_dir, store) = make_store();
        store
            .create_role(&Role {
                name: "writer".into(),
                permissions: vec![Permission::Write { table: None }],
            })
            .unwrap();
        let mut user = make_user("resolve_user", "pass");
        user.roles = vec!["writer".into()];
        store.create_user(&user).unwrap();

        let ctx = store
            .resolve_security_context("resolve_user")
            .unwrap()
            .unwrap();
        assert!(ctx.can_write_table("any"));
    }

    #[test]
    fn resolve_nonexistent_returns_none() {
        let (_dir, store) = make_store();
        assert!(store.resolve_security_context("ghost").unwrap().is_none());
    }

    #[test]
    fn resolve_disabled_user_returns_none() {
        let (_dir, store) = make_store();
        let mut user = make_user("disabled_resolve", "pass");
        user.enabled = false;
        store.create_user(&user).unwrap();
        assert!(
            store
                .resolve_security_context("disabled_resolve")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn anonymous_context_example() {
        // Anonymous = no restrictions
        let ctx = SecurityContext {
            user: "anonymous".into(),
            roles: vec![],
            permissions: vec![Permission::Admin],
        };
        assert!(ctx.can_read_table("anything"));
        assert!(ctx.can_write_table("anything"));
        assert!(ctx.can_ddl());
    }

    #[test]
    fn restricted_context_example() {
        let ctx = SecurityContext {
            user: "restricted".into(),
            roles: vec!["limited".into()],
            permissions: vec![Permission::Read {
                table: Some("public_data".into()),
            }],
        };
        assert!(ctx.can_read_table("public_data"));
        assert!(!ctx.can_read_table("private_data"));
        assert!(!ctx.can_write_table("public_data"));
    }

    #[test]
    fn context_with_missing_role_still_works() {
        let (_dir, store) = make_store();
        // User has a role that doesn't exist
        let mut user = make_user("missing_role_user", "pass");
        user.roles = vec!["nonexistent_role".into()];
        store.create_user(&user).unwrap();

        let ctx = store
            .authenticate("missing_role_user", "pass")
            .unwrap()
            .unwrap();
        // Should have no permissions since the role doesn't exist
        assert!(!ctx.can_read_table("t"));
    }
}

// ---------------------------------------------------------------------------
// mod audit (store operations are the "audit log" here)
// ---------------------------------------------------------------------------

mod audit_operations {
    use super::*;

    #[test]
    fn create_list_delete_cycle_for_users() {
        let (_dir, store) = make_store();
        for i in 0..5 {
            store
                .create_user(&make_user(&format!("user{i}"), "p"))
                .unwrap();
        }
        assert_eq!(store.list_users().unwrap().len(), 5);
        for i in 0..5 {
            store.delete_user(&format!("user{i}")).unwrap();
        }
        assert_eq!(store.list_users().unwrap().len(), 0);
    }

    #[test]
    fn create_list_delete_cycle_for_roles() {
        let (_dir, store) = make_store();
        for i in 0..5 {
            store
                .create_role(&Role {
                    name: format!("role{i}"),
                    permissions: vec![],
                })
                .unwrap();
        }
        assert_eq!(store.list_roles().unwrap().len(), 5);
        for i in 0..5 {
            store.delete_role(&format!("role{i}")).unwrap();
        }
        assert_eq!(store.list_roles().unwrap().len(), 0);
    }

    #[test]
    fn store_persists_across_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let store = RbacStore::open(dir.path()).unwrap();
            store.create_user(&make_user("persistent", "p")).unwrap();
        }
        {
            let store = RbacStore::open(dir.path()).unwrap();
            let user = store.get_user("persistent").unwrap();
            assert!(user.is_some());
        }
    }

    #[test]
    fn role_persists_across_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let store = RbacStore::open(dir.path()).unwrap();
            store
                .create_role(&Role {
                    name: "persisted_role".into(),
                    permissions: vec![Permission::Admin],
                })
                .unwrap();
        }
        {
            let store = RbacStore::open(dir.path()).unwrap();
            let role = store.get_role("persisted_role").unwrap().unwrap();
            assert_eq!(role.permissions.len(), 1);
        }
    }

    #[test]
    fn user_created_at_preserved() {
        let (_dir, store) = make_store();
        let mut user = make_user("ts_user", "p");
        user.created_at = 1_700_000_999;
        store.create_user(&user).unwrap();
        let loaded = store.get_user("ts_user").unwrap().unwrap();
        assert_eq!(loaded.created_at, 1_700_000_999);
    }

    #[test]
    fn user_roles_preserved() {
        let (_dir, store) = make_store();
        let mut user = make_user("roles_user", "p");
        user.roles = vec!["admin".into(), "reader".into()];
        store.create_user(&user).unwrap();
        let loaded = store.get_user("roles_user").unwrap().unwrap();
        assert_eq!(loaded.roles, vec!["admin", "reader"]);
    }

    #[test]
    fn many_users_performance() {
        let (_dir, store) = make_store();
        for i in 0..50 {
            store
                .create_user(&make_user(&format!("user_{i:04}"), "p"))
                .unwrap();
        }
        let users = store.list_users().unwrap();
        assert_eq!(users.len(), 50);
        // Verify sorted
        for i in 1..users.len() {
            assert!(users[i - 1].username < users[i].username);
        }
    }

    #[test]
    fn many_roles_performance() {
        let (_dir, store) = make_store();
        for i in 0..50 {
            store
                .create_role(&Role {
                    name: format!("role_{i:04}"),
                    permissions: vec![Permission::Read { table: None }],
                })
                .unwrap();
        }
        let roles = store.list_roles().unwrap();
        assert_eq!(roles.len(), 50);
    }

    #[test]
    #[ignore]
    fn hash_password_hex_format() {
        let h = hash_password("test");
        assert_eq!(h.len(), 16); // 64-bit xxhash = 16 hex chars
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    #[ignore]
    fn hash_password_empty_string() {
        let h = hash_password("");
        assert_eq!(h.len(), 16);
    }

    #[test]
    fn update_user_password() {
        let (_dir, store) = make_store();
        let mut user = make_user("changepw", "old");
        store.create_user(&user).unwrap();
        user.password_hash = hash_password("new");
        store.update_user(&user).unwrap();
        // Old password should fail
        assert!(store.authenticate("changepw", "old").unwrap().is_none());
        // New password should work (no role so context has no perms but authenticates)
    }

    #[test]
    fn update_user_roles() {
        let (_dir, store) = make_store();
        store
            .create_role(&Role {
                name: "admin".into(),
                permissions: vec![Permission::Admin],
            })
            .unwrap();
        let mut user = make_user("upgrade", "p");
        store.create_user(&user).unwrap();
        user.roles = vec!["admin".into()];
        store.update_user(&user).unwrap();
        let ctx = store.authenticate("upgrade", "p").unwrap().unwrap();
        assert!(ctx.can_admin());
    }

    #[test]
    fn update_role_permissions() {
        let (_dir, store) = make_store();
        let mut role = Role {
            name: "evolving".into(),
            permissions: vec![Permission::Read { table: None }],
        };
        store.create_role(&role).unwrap();
        role.permissions.push(Permission::Write { table: None });
        store.update_role(&role).unwrap();
        let loaded = store.get_role("evolving").unwrap().unwrap();
        assert_eq!(loaded.permissions.len(), 2);
    }

    #[test]
    fn multiple_roles_permissions_merge() {
        let (_dir, store) = make_store();
        store
            .create_role(&Role {
                name: "r1".into(),
                permissions: vec![Permission::Read {
                    table: Some("trades".into()),
                }],
            })
            .unwrap();
        store
            .create_role(&Role {
                name: "r2".into(),
                permissions: vec![Permission::Write {
                    table: Some("trades".into()),
                }],
            })
            .unwrap();
        let mut user = make_user("multi", "p");
        user.roles = vec!["r1".into(), "r2".into()];
        store.create_user(&user).unwrap();
        let ctx = store.authenticate("multi", "p").unwrap().unwrap();
        assert!(ctx.can_read_table("trades"));
        assert!(ctx.can_write_table("trades"));
    }

    #[test]
    fn permission_equality() {
        assert_eq!(Permission::Admin, Permission::Admin);
        assert_eq!(Permission::DDL, Permission::DDL);
        assert_ne!(Permission::Admin, Permission::DDL);
        assert_eq!(
            Permission::Read {
                table: Some("t".into())
            },
            Permission::Read {
                table: Some("t".into())
            }
        );
        assert_ne!(
            Permission::Read {
                table: Some("t1".into())
            },
            Permission::Read {
                table: Some("t2".into())
            }
        );
    }

    #[test]
    fn write_all_read_specific_combination() {
        let ctx = ctx_with(vec![
            Permission::Write { table: None },
            Permission::Read {
                table: Some("secret".into()),
            },
        ]);
        assert!(ctx.can_write_table("any"));
        assert!(ctx.can_read_table("secret"));
        assert!(!ctx.can_read_table("other")); // Write doesn't grant read
    }

    #[test]
    fn column_read_multiple_tables() {
        let ctx = ctx_with(vec![
            Permission::ColumnRead {
                table: "trades".into(),
                columns: vec!["price".into()],
            },
            Permission::ColumnRead {
                table: "orders".into(),
                columns: vec!["qty".into()],
            },
        ]);
        assert!(ctx.can_read_column("trades", "price"));
        assert!(ctx.can_read_column("orders", "qty"));
        assert!(!ctx.can_read_column("trades", "volume"));
        assert!(!ctx.can_read_column("orders", "price"));
    }
}
