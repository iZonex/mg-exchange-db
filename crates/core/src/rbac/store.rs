//! Persistent storage for RBAC users and roles.
//!
//! Data is stored as JSON files under `<db_root>/_security/`:
//! - `_security/users/<username>.json` — one file per user
//! - `_security/roles/<rolename>.json` — one file per role

use std::path::{Path, PathBuf};

use exchange_common::error::{ExchangeDbError, Result};

use super::model::{Role, SecurityContext, User};
#[cfg(test)]
use super::model::Permission;

/// Manages on-disk persistence of users and roles.
pub struct RbacStore {
    security_dir: PathBuf,
}

impl RbacStore {
    /// Open (or initialize) the RBAC store under `<db_root>/_security/`.
    pub fn open(db_root: &Path) -> Result<Self> {
        let security_dir = db_root.join("_security");
        std::fs::create_dir_all(security_dir.join("users"))?;
        std::fs::create_dir_all(security_dir.join("roles"))?;
        Ok(Self { security_dir })
    }

    // ── Users ────────────────────────────────────────────────────────

    /// Persist a new user. Returns an error if the username already exists.
    pub fn create_user(&self, user: &User) -> Result<()> {
        let path = self.user_path(&user.username);
        if path.exists() {
            return Err(ExchangeDbError::Query(format!(
                "user '{}' already exists",
                user.username
            )));
        }
        let json = serde_json::to_string_pretty(user)
            .map_err(|e| ExchangeDbError::Query(e.to_string()))?;
        std::fs::write(&path, json)?;
        Ok(())
    }

    /// Load a user by username. Returns `None` if the user does not exist.
    pub fn get_user(&self, username: &str) -> Result<Option<User>> {
        let path = self.user_path(username);
        if !path.exists() {
            return Ok(None);
        }
        let data = std::fs::read_to_string(&path)?;
        let user: User =
            serde_json::from_str(&data).map_err(|e| ExchangeDbError::Query(e.to_string()))?;
        Ok(Some(user))
    }

    /// List all users.
    pub fn list_users(&self) -> Result<Vec<User>> {
        let dir = self.security_dir.join("users");
        let mut users = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("json") {
                let data = std::fs::read_to_string(&path)?;
                let user: User = serde_json::from_str(&data)
                    .map_err(|e| ExchangeDbError::Query(e.to_string()))?;
                users.push(user);
            }
        }
        users.sort_by(|a, b| a.username.cmp(&b.username));
        Ok(users)
    }

    /// Delete a user by username.
    pub fn delete_user(&self, username: &str) -> Result<()> {
        let path = self.user_path(username);
        if !path.exists() {
            return Err(ExchangeDbError::Query(format!(
                "user '{username}' not found"
            )));
        }
        std::fs::remove_file(&path)?;
        Ok(())
    }

    /// Update a user (overwrites the existing file).
    pub fn update_user(&self, user: &User) -> Result<()> {
        let path = self.user_path(&user.username);
        if !path.exists() {
            return Err(ExchangeDbError::Query(format!(
                "user '{}' not found",
                user.username
            )));
        }
        let json = serde_json::to_string_pretty(user)
            .map_err(|e| ExchangeDbError::Query(e.to_string()))?;
        std::fs::write(&path, json)?;
        Ok(())
    }

    // ── Roles ────────────────────────────────────────────────────────

    /// Persist a new role. Returns an error if the role name already exists.
    pub fn create_role(&self, role: &Role) -> Result<()> {
        let path = self.role_path(&role.name);
        if path.exists() {
            return Err(ExchangeDbError::Query(format!(
                "role '{}' already exists",
                role.name
            )));
        }
        let json = serde_json::to_string_pretty(role)
            .map_err(|e| ExchangeDbError::Query(e.to_string()))?;
        std::fs::write(&path, json)?;
        Ok(())
    }

    /// Load a role by name.
    pub fn get_role(&self, name: &str) -> Result<Option<Role>> {
        let path = self.role_path(name);
        if !path.exists() {
            return Ok(None);
        }
        let data = std::fs::read_to_string(&path)?;
        let role: Role =
            serde_json::from_str(&data).map_err(|e| ExchangeDbError::Query(e.to_string()))?;
        Ok(Some(role))
    }

    /// List all roles.
    pub fn list_roles(&self) -> Result<Vec<Role>> {
        let dir = self.security_dir.join("roles");
        let mut roles = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("json") {
                let data = std::fs::read_to_string(&path)?;
                let role: Role = serde_json::from_str(&data)
                    .map_err(|e| ExchangeDbError::Query(e.to_string()))?;
                roles.push(role);
            }
        }
        roles.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(roles)
    }

    /// Delete a role by name.
    pub fn delete_role(&self, name: &str) -> Result<()> {
        let path = self.role_path(name);
        if !path.exists() {
            return Err(ExchangeDbError::Query(format!(
                "role '{name}' not found"
            )));
        }
        std::fs::remove_file(&path)?;
        Ok(())
    }

    /// Update a role (overwrites the existing file).
    pub fn update_role(&self, role: &Role) -> Result<()> {
        let path = self.role_path(&role.name);
        if !path.exists() {
            return Err(ExchangeDbError::Query(format!(
                "role '{}' not found",
                role.name
            )));
        }
        let json = serde_json::to_string_pretty(role)
            .map_err(|e| ExchangeDbError::Query(e.to_string()))?;
        std::fs::write(&path, json)?;
        Ok(())
    }

    // ── Authentication ───────────────────────────────────────────────

    /// Authenticate a user by username and plain-text password.
    ///
    /// Returns the resolved `SecurityContext` on success, or `None` if
    /// the credentials are invalid or the user is disabled.
    pub fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> Result<Option<SecurityContext>> {
        let user = match self.get_user(username)? {
            Some(u) => u,
            None => return Ok(None),
        };

        if !user.enabled {
            return Ok(None);
        }

        if !verify_password(password, &user.password_hash) {
            return Ok(None);
        }

        // Auto-upgrade legacy hashes to argon2id on successful login.
        if needs_rehash(&user.password_hash) {
            let mut upgraded = user.clone();
            upgraded.password_hash = hash_password(password);
            // Best-effort upgrade; don't fail the login if write fails.
            let _ = self.update_user(&upgraded);
        }

        // Resolve all permissions from the user's roles.
        let mut permissions = Vec::new();
        for role_name in &user.roles {
            if let Some(role) = self.get_role(role_name)? {
                permissions.extend(role.permissions.iter().cloned());
            }
        }

        Ok(Some(SecurityContext {
            user: user.username,
            roles: user.roles,
            permissions,
        }))
    }

    // ── Security context ─────────────────────────────────────────────

    /// Resolve a `SecurityContext` for a user by username (without
    /// password verification). This is used when the user has already
    /// been authenticated via an external auth method (OAuth, service
    /// accounts, etc.) and we just need their RBAC permissions.
    ///
    /// Returns `None` if the user does not exist or is disabled.
    pub fn resolve_security_context(
        &self,
        username: &str,
    ) -> Result<Option<SecurityContext>> {
        let user = match self.get_user(username)? {
            Some(u) => u,
            None => return Ok(None),
        };

        if !user.enabled {
            return Ok(None);
        }

        let mut permissions = Vec::new();
        for role_name in &user.roles {
            if let Some(role) = self.get_role(role_name)? {
                permissions.extend(role.permissions.iter().cloned());
            }
        }

        Ok(Some(SecurityContext {
            user: user.username,
            roles: user.roles,
            permissions,
        }))
    }

    // ── Helpers ──────────────────────────────────────────────────────

    fn user_path(&self, username: &str) -> PathBuf {
        self.security_dir.join("users").join(format!("{username}.json"))
    }

    fn role_path(&self, name: &str) -> PathBuf {
        self.security_dir.join("roles").join(format!("{name}.json"))
    }
}

/// Hash a plain-text password using Argon2id with a random 16-byte salt.
///
/// Returns a PHC-formatted string (e.g. `$argon2id$v=19$m=19456,t=2,p=1$...`).
/// Argon2id is the recommended algorithm for password hashing (OWASP 2024).
pub fn hash_password(password: &str) -> String {
    use argon2::password_hash::SaltString;
    use argon2::{Argon2, PasswordHasher};
    use rand::rngs::OsRng;

    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default(); // argon2id v19, m=19456, t=2, p=1
    argon2
        .hash_password(password.as_bytes(), &salt)
        .expect("argon2 hash failed")
        .to_string()
}

/// Verify a password against a stored hash.
///
/// Supports both:
/// - New argon2id PHC strings (`$argon2id$...`)
/// - Legacy SHA-256 hex hashes (64 hex chars) — auto-detected for migration
pub fn verify_password(password: &str, stored_hash: &str) -> bool {
    if stored_hash.starts_with("$argon2") {
        // Modern argon2id hash
        use argon2::password_hash::PasswordHash;
        use argon2::{Argon2, PasswordVerifier};

        let parsed = match PasswordHash::new(stored_hash) {
            Ok(h) => h,
            Err(_) => return false,
        };
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok()
    } else if stored_hash.len() == 64 && stored_hash.chars().all(|c| c.is_ascii_hexdigit()) {
        // Legacy PBKDF2-style SHA-256 hash — verify using old algorithm
        legacy_hash_password(password) == stored_hash
    } else {
        false
    }
}

/// Legacy password hashing (PBKDF2-style SHA-256). Used only for verifying
/// existing hashes during migration. Do NOT use for new passwords.
fn legacy_hash_password(password: &str) -> String {
    use sha2::{Digest, Sha256};

    let salt = format!("exchangedb_salt_{}", password.len());
    let mut hasher = Sha256::new();
    hasher.update(format!("{}:{}", salt, password).as_bytes());

    for _ in 0..10_000 {
        let result = hasher.finalize_reset();
        hasher.update(result);
    }

    format!("{:x}", hasher.finalize())
}

/// Check if a stored hash needs upgrading to argon2id.
pub fn needs_rehash(stored_hash: &str) -> bool {
    !stored_hash.starts_with("$argon2")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_store(tmp: &Path) -> RbacStore {
        RbacStore::open(tmp).unwrap()
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

    #[test]
    fn user_crud() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        // Create
        let user = make_user("alice", "secret123");
        store.create_user(&user).unwrap();

        // Read
        let loaded = store.get_user("alice").unwrap().unwrap();
        assert_eq!(loaded.username, "alice");
        assert!(loaded.enabled);

        // List
        let users = store.list_users().unwrap();
        assert_eq!(users.len(), 1);

        // Duplicate
        assert!(store.create_user(&user).is_err());

        // Delete
        store.delete_user("alice").unwrap();
        assert!(store.get_user("alice").unwrap().is_none());
        assert!(store.delete_user("alice").is_err());
    }

    #[test]
    fn role_crud() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let role = Role {
            name: "analyst".to_string(),
            permissions: vec![Permission::Read {
                table: Some("trades".to_string()),
            }],
        };
        store.create_role(&role).unwrap();

        let loaded = store.get_role("analyst").unwrap().unwrap();
        assert_eq!(loaded.permissions.len(), 1);

        let roles = store.list_roles().unwrap();
        assert_eq!(roles.len(), 1);

        assert!(store.create_role(&role).is_err());

        store.delete_role("analyst").unwrap();
        assert!(store.get_role("analyst").unwrap().is_none());
    }

    #[test]
    fn authenticate_success() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let role = Role {
            name: "reader".to_string(),
            permissions: vec![Permission::Read { table: None }],
        };
        store.create_role(&role).unwrap();

        let mut user = make_user("bob", "pass");
        user.roles = vec!["reader".to_string()];
        store.create_user(&user).unwrap();

        let ctx = store.authenticate("bob", "pass").unwrap().unwrap();
        assert_eq!(ctx.user, "bob");
        assert!(ctx.can_read_table("trades"));
    }

    #[test]
    fn authenticate_wrong_password() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let user = make_user("carol", "right");
        store.create_user(&user).unwrap();

        assert!(store.authenticate("carol", "wrong").unwrap().is_none());
    }

    #[test]
    fn authenticate_disabled_user() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let mut user = make_user("dave", "pass");
        user.enabled = false;
        store.create_user(&user).unwrap();

        assert!(store.authenticate("dave", "pass").unwrap().is_none());
    }

    #[test]
    fn authenticate_nonexistent_user() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        assert!(store.authenticate("nobody", "pass").unwrap().is_none());
    }

    #[test]
    fn column_level_access_via_roles() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let role = Role {
            name: "limited".to_string(),
            permissions: vec![Permission::ColumnRead {
                table: "trades".to_string(),
                columns: vec!["price".to_string(), "volume".to_string()],
            }],
        };
        store.create_role(&role).unwrap();

        let mut user = make_user("eve", "pass");
        user.roles = vec!["limited".to_string()];
        store.create_user(&user).unwrap();

        let ctx = store.authenticate("eve", "pass").unwrap().unwrap();
        assert!(ctx.can_read_table("trades"));
        assert!(ctx.can_read_column("trades", "price"));
        assert!(ctx.can_read_column("trades", "volume"));
        assert!(!ctx.can_read_column("trades", "symbol"));
        assert!(!ctx.can_write_table("trades"));
    }

    // ── Password hashing security tests (argon2id) ──────────────────────

    #[test]
    fn hash_password_produces_argon2id_phc_string() {
        let h = hash_password("my_secret");
        assert!(
            h.starts_with("$argon2id$"),
            "hash must be argon2id PHC format, got: {h}"
        );
    }

    #[test]
    fn hash_password_random_salt_gives_different_hashes() {
        let h1 = hash_password("same_password");
        let h2 = hash_password("same_password");
        // Same password, different salts → different PHC strings
        assert_ne!(h1, h2, "random salt must produce unique hashes");
    }

    #[test]
    fn verify_password_correct() {
        let hash = hash_password("correct_horse");
        assert!(verify_password("correct_horse", &hash));
    }

    #[test]
    fn verify_password_wrong() {
        let hash = hash_password("correct_horse");
        assert!(!verify_password("wrong_horse", &hash));
    }

    #[test]
    fn verify_password_legacy_sha256() {
        // Simulate a legacy hash created by the old algorithm
        let legacy = legacy_hash_password("old_password");
        assert_eq!(legacy.len(), 64);
        assert!(verify_password("old_password", &legacy));
        assert!(!verify_password("wrong", &legacy));
    }

    #[test]
    fn needs_rehash_detects_legacy() {
        let legacy = legacy_hash_password("test");
        assert!(needs_rehash(&legacy));
        let modern = hash_password("test");
        assert!(!needs_rehash(&modern));
    }

    #[test]
    fn authenticate_upgrades_legacy_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        // Create user with legacy hash
        let user = User {
            username: "legacy_user".to_string(),
            password_hash: legacy_hash_password("mypass"),
            roles: vec![],
            enabled: true,
            created_at: 1_700_000_000,
        };
        store.create_user(&user).unwrap();

        // Verify the hash is legacy
        let before = store.get_user("legacy_user").unwrap().unwrap();
        assert!(!before.password_hash.starts_with("$argon2"));

        // Authenticate — should succeed and upgrade
        let ctx = store.authenticate("legacy_user", "mypass").unwrap();
        assert!(ctx.is_some());

        // Verify the hash was upgraded to argon2id
        let after = store.get_user("legacy_user").unwrap().unwrap();
        assert!(
            after.password_hash.starts_with("$argon2id$"),
            "hash should be upgraded to argon2id, got: {}",
            after.password_hash
        );
    }
}
