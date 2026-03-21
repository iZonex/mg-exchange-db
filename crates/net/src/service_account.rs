use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use sha2::{Digest, Sha256};

/// Errors that can occur during service account operations.
#[derive(Debug, thiserror::Error)]
pub enum ServiceAccountError {
    #[error("service account not found: {0}")]
    NotFound(String),
    #[error("service account is disabled: {0}")]
    Disabled(String),
    #[error("invalid credentials")]
    InvalidCredentials,
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// A service account for machine-to-machine authentication.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServiceAccount {
    /// Unique identifier for the service account.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// The API key (public identifier).
    pub api_key: String,
    /// SHA-256 hash of the secret (never store plaintext).
    pub secret_hash: String,
    /// Roles assigned to this service account.
    pub roles: Vec<String>,
    /// Whether this service account is enabled.
    pub enabled: bool,
    /// Unix timestamp when the account was created.
    pub created_at: i64,
    /// Unix timestamp of the last authentication (if any).
    pub last_used: Option<i64>,
}

/// Credentials returned when a service account is created or its secret rotated.
/// The plaintext secret is only available at this point and must be stored by the caller.
#[derive(Debug, Clone)]
pub struct ServiceAccountCredentials {
    /// The API key (public identifier).
    pub api_key: String,
    /// The secret in plaintext. Shown only once.
    pub secret: String,
}

/// Persistent store for service accounts, backed by a JSON file on disk.
pub struct ServiceAccountStore {
    store_path: PathBuf,
}

impl std::fmt::Debug for ServiceAccountStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceAccountStore")
            .field("store_path", &self.store_path)
            .finish()
    }
}

/// Internal on-disk representation.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct StoreData {
    accounts: Vec<ServiceAccount>,
}

impl ServiceAccountStore {
    /// Open or create a service account store at `db_root/_service_accounts.json`.
    pub fn open(db_root: &Path) -> Result<Self, ServiceAccountError> {
        let store_path = db_root.join("_service_accounts.json");
        if !store_path.exists() {
            let data = StoreData {
                accounts: Vec::new(),
            };
            let json = serde_json::to_string_pretty(&data)?;
            fs::write(&store_path, json)?;
        }
        Ok(Self { store_path })
    }

    /// Create a new service account with the given name and roles.
    ///
    /// Returns the credentials (API key + plaintext secret). The secret is only
    /// available at creation time and cannot be retrieved later.
    pub fn create(
        &self,
        name: &str,
        roles: Vec<String>,
    ) -> Result<ServiceAccountCredentials, ServiceAccountError> {
        let mut data = self.load()?;

        let id = uuid::Uuid::new_v4().to_string();
        let api_key = generate_api_key();
        let secret = generate_secret();
        let secret_hash = hash_secret(&secret);
        let now = current_timestamp();

        let account = ServiceAccount {
            id,
            name: name.to_string(),
            api_key: api_key.clone(),
            secret_hash,
            roles,
            enabled: true,
            created_at: now,
            last_used: None,
        };

        data.accounts.push(account);
        self.save(&data)?;

        Ok(ServiceAccountCredentials {
            api_key,
            secret,
        })
    }

    /// Authenticate a service account using an API key and plaintext secret.
    ///
    /// Returns the service account if credentials are valid and the account is enabled.
    pub fn authenticate(
        &self,
        api_key: &str,
        secret: &str,
    ) -> Result<Option<ServiceAccount>, ServiceAccountError> {
        let mut data = self.load()?;

        let account = data
            .accounts
            .iter_mut()
            .find(|a| constant_time_eq(&a.api_key, api_key));

        match account {
            Some(account) => {
                if !account.enabled {
                    return Err(ServiceAccountError::Disabled(account.id.clone()));
                }
                if !verify_secret(secret, &account.secret_hash) {
                    return Err(ServiceAccountError::InvalidCredentials);
                }
                // Update last_used timestamp.
                account.last_used = Some(current_timestamp());
                let result = account.clone();
                self.save(&data)?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    /// List all service accounts (secrets are never included in plaintext).
    pub fn list(&self) -> Result<Vec<ServiceAccount>, ServiceAccountError> {
        let data = self.load()?;
        Ok(data.accounts)
    }

    /// Revoke (disable) a service account by ID.
    pub fn revoke(&self, id: &str) -> Result<(), ServiceAccountError> {
        let mut data = self.load()?;
        let account = data
            .accounts
            .iter_mut()
            .find(|a| a.id == id)
            .ok_or_else(|| ServiceAccountError::NotFound(id.to_string()))?;

        account.enabled = false;
        self.save(&data)?;
        Ok(())
    }

    /// Rotate the secret for a service account, returning new credentials.
    ///
    /// The old secret is immediately invalidated. The new plaintext secret
    /// is only available at this point.
    pub fn rotate_secret(
        &self,
        id: &str,
    ) -> Result<ServiceAccountCredentials, ServiceAccountError> {
        let mut data = self.load()?;
        let account = data
            .accounts
            .iter_mut()
            .find(|a| a.id == id)
            .ok_or_else(|| ServiceAccountError::NotFound(id.to_string()))?;

        let new_secret = generate_secret();
        account.secret_hash = hash_secret(&new_secret);

        let credentials = ServiceAccountCredentials {
            api_key: account.api_key.clone(),
            secret: new_secret,
        };

        self.save(&data)?;
        Ok(credentials)
    }

    /// Delete a service account entirely by ID.
    pub fn delete(&self, id: &str) -> Result<(), ServiceAccountError> {
        let mut data = self.load()?;
        let initial_len = data.accounts.len();
        data.accounts.retain(|a| a.id != id);
        if data.accounts.len() == initial_len {
            return Err(ServiceAccountError::NotFound(id.to_string()));
        }
        self.save(&data)?;
        Ok(())
    }

    fn load(&self) -> Result<StoreData, ServiceAccountError> {
        let content = fs::read_to_string(&self.store_path)?;
        let data: StoreData = serde_json::from_str(&content)?;
        Ok(data)
    }

    fn save(&self, data: &StoreData) -> Result<(), ServiceAccountError> {
        let json = serde_json::to_string_pretty(data)?;
        fs::write(&self.store_path, json)?;
        Ok(())
    }
}

/// Generate a random API key (prefix + random bytes).
fn generate_api_key() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut bytes = [0u8; 24];
    rng.fill(&mut bytes);
    format!("exdb_{}", URL_SAFE_NO_PAD.encode(bytes))
}

/// Generate a random secret (high-entropy token).
fn generate_secret() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Hash a service account secret using Argon2id.
///
/// Returns a PHC-formatted hash string (includes salt, params, and hash).
fn hash_secret(secret: &str) -> String {
    use argon2::{password_hash::SaltString, Argon2, PasswordHasher};
    let salt = SaltString::generate(&mut rand::thread_rng());
    Argon2::default()
        .hash_password(secret.as_bytes(), &salt)
        .expect("argon2 hashing should not fail")
        .to_string()
}

/// Verify a plaintext secret against an Argon2id hash.
///
/// Falls back to legacy SHA-256 hex comparison for existing accounts
/// that were hashed before the upgrade to Argon2id.
fn verify_secret(secret: &str, stored_hash: &str) -> bool {
    // Argon2id PHC hashes start with "$argon2id$"
    if stored_hash.starts_with("$argon2") {
        use argon2::{password_hash::PasswordHash, Argon2, PasswordVerifier};
        match PasswordHash::new(stored_hash) {
            Ok(parsed) => Argon2::default()
                .verify_password(secret.as_bytes(), &parsed)
                .is_ok(),
            Err(_) => false,
        }
    } else {
        // Legacy SHA-256 hex hash — constant-time compare.
        let mut hasher = Sha256::new();
        hasher.update(secret.as_bytes());
        let result = hasher.finalize();
        let computed = hex_encode(&result);
        constant_time_eq(&computed, stored_hash)
    }
}

/// Encode bytes as a lowercase hex string.
fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// Constant-time string comparison to prevent timing attacks.
fn constant_time_eq(a: &str, b: &str) -> bool {
    use subtle::ConstantTimeEq;
    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();
    a_bytes.len() == b_bytes.len() && a_bytes.ct_eq(b_bytes).into()
}

/// Get current Unix timestamp in seconds.
fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Parse service account credentials from HTTP headers.
///
/// Supports two authentication schemes:
/// 1. `X-API-Key` + `X-API-Secret` headers
/// 2. `Authorization: Basic base64(api_key:secret)`
pub fn extract_credentials(headers: &axum::http::HeaderMap) -> Option<(String, String)> {
    // Try X-API-Key + X-API-Secret headers.
    if let (Some(key), Some(secret)) = (
        headers.get("x-api-key").and_then(|v| v.to_str().ok()),
        headers.get("x-api-secret").and_then(|v| v.to_str().ok()),
    ) {
        return Some((key.to_string(), secret.to_string()));
    }

    // Try Authorization: Basic.
    if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        if let Some(encoded) = auth.strip_prefix("Basic ") {
            if let Ok(decoded) = URL_SAFE_NO_PAD
                .decode(encoded)
                .or_else(|_| base64::engine::general_purpose::STANDARD.decode(encoded))
            {
                if let Ok(cred_str) = String::from_utf8(decoded) {
                    if let Some((key, secret)) = cred_str.split_once(':') {
                        return Some((key.to_string(), secret.to_string()));
                    }
                }
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_store() -> (TempDir, ServiceAccountStore) {
        let dir = TempDir::new().unwrap();
        let store = ServiceAccountStore::open(dir.path()).unwrap();
        (dir, store)
    }

    #[test]
    fn test_create_and_authenticate() {
        let (_dir, store) = setup_store();

        let creds = store
            .create("test-service", vec!["reader".to_string()])
            .unwrap();

        assert!(creds.api_key.starts_with("exdb_"));
        assert!(!creds.secret.is_empty());

        // Authenticate with correct credentials.
        let account = store
            .authenticate(&creds.api_key, &creds.secret)
            .unwrap()
            .unwrap();
        assert_eq!(account.name, "test-service");
        assert_eq!(account.roles, vec!["reader"]);
        assert!(account.enabled);
        assert!(account.last_used.is_some());
    }

    #[test]
    fn test_authenticate_wrong_secret() {
        let (_dir, store) = setup_store();
        let creds = store.create("svc", vec![]).unwrap();

        let result = store.authenticate(&creds.api_key, "wrong-secret");
        assert!(matches!(
            result.unwrap_err(),
            ServiceAccountError::InvalidCredentials
        ));
    }

    #[test]
    fn test_authenticate_unknown_key() {
        let (_dir, store) = setup_store();

        let result = store
            .authenticate("unknown-key", "some-secret")
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_list_accounts() {
        let (_dir, store) = setup_store();

        store.create("svc-a", vec!["admin".to_string()]).unwrap();
        store.create("svc-b", vec!["reader".to_string()]).unwrap();

        let accounts = store.list().unwrap();
        assert_eq!(accounts.len(), 2);
        assert!(accounts.iter().any(|a| a.name == "svc-a"));
        assert!(accounts.iter().any(|a| a.name == "svc-b"));
    }

    #[test]
    fn test_revoke_account() {
        let (_dir, store) = setup_store();

        let creds = store.create("svc", vec![]).unwrap();

        // Get the account ID.
        let accounts = store.list().unwrap();
        let id = &accounts[0].id;

        // Revoke it.
        store.revoke(id).unwrap();

        // Try to authenticate — should fail because disabled.
        let result = store.authenticate(&creds.api_key, &creds.secret);
        assert!(matches!(
            result.unwrap_err(),
            ServiceAccountError::Disabled(_)
        ));
    }

    #[test]
    fn test_revoke_nonexistent() {
        let (_dir, store) = setup_store();
        let result = store.revoke("nonexistent-id");
        assert!(matches!(
            result.unwrap_err(),
            ServiceAccountError::NotFound(_)
        ));
    }

    #[test]
    fn test_rotate_secret() {
        let (_dir, store) = setup_store();

        let creds = store.create("svc", vec![]).unwrap();
        let accounts = store.list().unwrap();
        let id = &accounts[0].id;

        // Rotate the secret.
        let new_creds = store.rotate_secret(id).unwrap();

        // Old secret should no longer work.
        let result = store.authenticate(&creds.api_key, &creds.secret);
        assert!(matches!(
            result.unwrap_err(),
            ServiceAccountError::InvalidCredentials
        ));

        // New secret should work.
        let account = store
            .authenticate(&new_creds.api_key, &new_creds.secret)
            .unwrap()
            .unwrap();
        assert_eq!(account.name, "svc");

        // API key should remain the same.
        assert_eq!(creds.api_key, new_creds.api_key);
    }

    #[test]
    fn test_secret_hashing_argon2() {
        let hash1 = hash_secret("my-secret");
        let hash2 = hash_secret("my-secret");

        // Argon2 uses random salts, so hashes differ each time.
        assert_ne!(hash1, hash2);
        // But both verify against the same password.
        assert!(verify_secret("my-secret", &hash1));
        assert!(verify_secret("my-secret", &hash2));
        // Wrong password doesn't verify.
        assert!(!verify_secret("wrong-secret", &hash1));
        // Hash starts with "$argon2" (PHC format).
        assert!(hash1.starts_with("$argon2"));
    }

    #[test]
    fn test_legacy_sha256_verification() {
        // Legacy hashes (pre-upgrade) should still verify.
        let mut hasher = Sha256::new();
        hasher.update(b"old-secret");
        let legacy_hash = hex_encode(&hasher.finalize());
        assert!(verify_secret("old-secret", &legacy_hash));
        assert!(!verify_secret("wrong", &legacy_hash));
    }

    #[test]
    fn test_extract_credentials_api_key_headers() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-api-key", "my-key".parse().unwrap());
        headers.insert("x-api-secret", "my-secret".parse().unwrap());

        let (key, secret) = extract_credentials(&headers).unwrap();
        assert_eq!(key, "my-key");
        assert_eq!(secret, "my-secret");
    }

    #[test]
    fn test_extract_credentials_basic_auth() {
        use base64::engine::general_purpose::STANDARD;
        let encoded = STANDARD.encode("my-key:my-secret");
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Basic {}", encoded).parse().unwrap(),
        );

        let (key, secret) = extract_credentials(&headers).unwrap();
        assert_eq!(key, "my-key");
        assert_eq!(secret, "my-secret");
    }

    #[test]
    fn test_extract_credentials_none() {
        let headers = axum::http::HeaderMap::new();
        assert!(extract_credentials(&headers).is_none());
    }

    #[test]
    fn test_delete_account() {
        let (_dir, store) = setup_store();

        store.create("svc", vec![]).unwrap();
        let accounts = store.list().unwrap();
        assert_eq!(accounts.len(), 1);

        let id = accounts[0].id.clone();
        store.delete(&id).unwrap();

        let accounts = store.list().unwrap();
        assert_eq!(accounts.len(), 0);
    }
}
