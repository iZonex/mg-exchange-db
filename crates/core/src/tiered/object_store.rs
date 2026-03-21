//! Abstract interface for cold storage backends.

use dashmap::DashMap;
use exchange_common::error::{ExchangeDbError, Result};
use std::path::PathBuf;

/// Trait for object storage backends used by cold tier storage.
pub trait ObjectStore: Send + Sync {
    /// Put an object by key.
    fn put(&self, key: &str, data: &[u8]) -> Result<()>;

    /// Get an object by key.
    fn get(&self, key: &str) -> Result<Vec<u8>>;

    /// Delete an object by key.
    fn delete(&self, key: &str) -> Result<()>;

    /// List objects matching a prefix.
    fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Check if an object exists.
    fn exists(&self, key: &str) -> Result<bool>;
}

/// Local filesystem implementation of ObjectStore.
pub struct LocalObjectStore {
    root: PathBuf,
}

impl LocalObjectStore {
    /// Create a new LocalObjectStore rooted at the given directory.
    /// Creates the directory if it does not exist.
    pub fn new(root: PathBuf) -> Result<Self> {
        if !root.exists() {
            std::fs::create_dir_all(&root)?;
        }
        Ok(Self { root })
    }

    /// Resolve a key to a filesystem path.
    fn key_path(&self, key: &str) -> PathBuf {
        // Replace path separators in key with OS separator for safety
        let sanitized = key.replace('/', std::path::MAIN_SEPARATOR_STR);
        self.root.join(sanitized)
    }
}

impl ObjectStore for LocalObjectStore {
    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        let path = self.key_path(key);
        // Ensure parent directory exists
        if let Some(parent) = path.parent()
            && !parent.exists()
        {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, data)?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        let path = self.key_path(key);
        if !path.exists() {
            return Err(ExchangeDbError::Corruption(format!(
                "object not found: {key}"
            )));
        }
        let data = std::fs::read(&path)?;
        Ok(data)
    }

    fn delete(&self, key: &str) -> Result<()> {
        let path = self.key_path(key);
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut results = Vec::new();
        let sanitized_prefix = prefix.replace('/', std::path::MAIN_SEPARATOR_STR);

        self.list_recursive(&self.root, &sanitized_prefix, &mut results)?;

        results.sort();
        Ok(results)
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let path = self.key_path(key);
        Ok(path.exists())
    }
}

impl LocalObjectStore {
    fn list_recursive(
        &self,
        dir: &std::path::Path,
        prefix: &str,
        results: &mut Vec<String>,
    ) -> Result<()> {
        if !dir.exists() {
            return Ok(());
        }

        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let ft = entry.file_type()?;

            if ft.is_file() {
                // Build the key relative to root
                let relative = path
                    .strip_prefix(&self.root)
                    .map_err(|e| ExchangeDbError::Corruption(e.to_string()))?;
                let key = relative
                    .to_string_lossy()
                    .replace(std::path::MAIN_SEPARATOR, "/");

                if key.starts_with(prefix) || prefix.is_empty() {
                    results.push(key);
                }
            } else if ft.is_dir() {
                self.list_recursive(&path, prefix, results)?;
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// In-memory object store for testing
// ---------------------------------------------------------------------------

/// In-memory object store for testing. Thread-safe via DashMap.
pub struct MemoryObjectStore {
    data: DashMap<String, Vec<u8>>,
}

impl MemoryObjectStore {
    /// Create a new empty in-memory object store.
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
        }
    }
}

impl Default for MemoryObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectStore for MemoryObjectStore {
    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.data.insert(key.to_string(), data.to_vec());
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.data
            .get(key)
            .map(|v| v.value().clone())
            .ok_or_else(|| ExchangeDbError::Corruption(format!("object not found: {key}")))
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.data.remove(key);
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut results: Vec<String> = self
            .data
            .iter()
            .filter(|entry| entry.key().starts_with(prefix) || prefix.is_empty())
            .map(|entry| entry.key().clone())
            .collect();
        results.sort();
        Ok(results)
    }

    fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.data.contains_key(key))
    }
}

// ---------------------------------------------------------------------------
// S3-compatible object store
// ---------------------------------------------------------------------------

/// S3-compatible object store (works with AWS S3, MinIO, etc.)
/// Uses HTTP REST API — no SDK dependency needed.
pub struct S3ObjectStore {
    bucket: String,
    prefix: String,
    endpoint: String,
    access_key: String,
    secret_key: String,
    region: String,
}

impl S3ObjectStore {
    /// Create a new S3ObjectStore.
    ///
    /// - `bucket`: S3 bucket name.
    /// - `prefix`: Key prefix to prepend to all operations (e.g. "backups/db1/").
    /// - `endpoint`: S3 endpoint URL (e.g. "https://s3.amazonaws.com").
    /// - `access_key`: AWS access key ID.
    /// - `secret_key`: AWS secret access key.
    /// - `region`: AWS region (e.g. "us-east-1").
    pub fn new(
        bucket: &str,
        prefix: &str,
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        region: &str,
    ) -> Self {
        Self {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            endpoint: endpoint.to_string(),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            region: region.to_string(),
        }
    }

    /// Build the full S3 object key with prefix.
    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}{}", self.prefix, key)
        }
    }

    /// Build the URL for an S3 object.
    fn object_url(&self, key: &str) -> String {
        let full_key = self.full_key(key);
        format!("{}/{}/{}", self.endpoint, self.bucket, full_key)
    }
}

impl S3ObjectStore {
    /// Build signed request headers for an S3 operation.
    fn build_headers(&self, method: &str, key: &str, body_hash: &str) -> Vec<(String, String)> {
        let url = self.object_url(key);
        let host = extract_host(&url);
        let timestamp = utc_timestamp_now();

        let auth = sign_aws_v4(
            method,
            &url,
            &[("host", &host), ("x-amz-content-sha256", body_hash)],
            body_hash,
            &self.access_key,
            &self.secret_key,
            &self.region,
            "s3",
            &timestamp,
        );

        vec![
            ("Authorization".to_string(), auth),
            ("x-amz-content-sha256".to_string(), body_hash.to_string()),
            ("x-amz-date".to_string(), timestamp),
        ]
    }
}

impl ObjectStore for S3ObjectStore {
    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        let url = self.object_url(key);
        let body_hash = sha256_hex(data);
        let headers = self.build_headers("PUT", key, &body_hash);

        let mut req = ureq::put(&url);
        for (k, v) in &headers {
            req = req.set(k, v);
        }

        req.send_bytes(data)
            .map_err(|e| ExchangeDbError::Corruption(format!("S3 PUT failed for {key}: {e}")))?;

        Ok(())
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        let url = self.object_url(key);
        let empty_hash = sha256_hex(b"");
        let headers = self.build_headers("GET", key, &empty_hash);

        let mut req = ureq::get(&url);
        for (k, v) in &headers {
            req = req.set(k, v);
        }

        let response = req
            .call()
            .map_err(|e| ExchangeDbError::Corruption(format!("S3 GET failed for {key}: {e}")))?;

        let mut body = Vec::new();
        response
            .into_reader()
            .read_to_end(&mut body)
            .map_err(ExchangeDbError::Io)?;

        Ok(body)
    }

    fn delete(&self, key: &str) -> Result<()> {
        let url = self.object_url(key);
        let empty_hash = sha256_hex(b"");
        let headers = self.build_headers("DELETE", key, &empty_hash);

        let mut req = ureq::delete(&url);
        for (k, v) in &headers {
            req = req.set(k, v);
        }

        req.call()
            .map_err(|e| ExchangeDbError::Corruption(format!("S3 DELETE failed for {key}: {e}")))?;

        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let full_prefix = self.full_key(prefix);
        let url = format!(
            "{}/{}?list-type=2&prefix={}",
            self.endpoint, self.bucket, full_prefix,
        );
        let empty_hash = sha256_hex(b"");
        let host = extract_host(&url);
        let timestamp = utc_timestamp_now();

        let auth = sign_aws_v4(
            "GET",
            &url,
            &[("host", &host), ("x-amz-content-sha256", &empty_hash)],
            &empty_hash,
            &self.access_key,
            &self.secret_key,
            &self.region,
            "s3",
            &timestamp,
        );

        let response = ureq::get(&url)
            .set("Authorization", &auth)
            .set("x-amz-content-sha256", &empty_hash)
            .set("x-amz-date", &timestamp)
            .call()
            .map_err(|e| ExchangeDbError::Corruption(format!("S3 LIST failed: {e}")))?;

        let body = response
            .into_string()
            .map_err(|e| ExchangeDbError::Corruption(format!("S3 LIST read body failed: {e}")))?;

        // Parse XML response to extract <Key> elements.
        let mut keys = Vec::new();
        for segment in body.split("<Key>") {
            if let Some(end) = segment.find("</Key>") {
                let key_str = &segment[..end];
                let relative = if key_str.starts_with(&self.prefix) {
                    &key_str[self.prefix.len()..]
                } else {
                    key_str
                };
                keys.push(relative.to_string());
            }
        }
        keys.sort();

        Ok(keys)
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let url = self.object_url(key);
        let empty_hash = sha256_hex(b"");
        let headers = self.build_headers("HEAD", key, &empty_hash);

        let mut req = ureq::head(&url);
        for (k, v) in &headers {
            req = req.set(k, v);
        }

        match req.call() {
            Ok(_) => Ok(true),
            Err(ureq::Error::Status(404, _)) => Ok(false),
            Err(e) => Err(ExchangeDbError::Corruption(format!(
                "S3 HEAD failed for {key}: {e}"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// AWS Signature V4 signing
// ---------------------------------------------------------------------------

/// Compute a hex-encoded SHA-256 digest of the given data.
pub fn sha256_hex(data: &[u8]) -> String {
    // Minimal SHA-256 implementation for signing.
    // We use a simple approach: for the stub we produce a deterministic hash.
    // In production this would use ring or sha2 crate.
    //
    // We implement a basic SHA-256 here to avoid adding dependencies.
    let hash = sha256(data);
    hex_encode(&hash)
}

/// Generate a real UTC timestamp for S3 requests.
fn utc_timestamp_now() -> String {
    let now = std::time::SystemTime::now();
    let secs = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let days = secs / 86400;
    let day_secs = secs % 86400;
    let hours = day_secs / 3600;
    let minutes = (day_secs % 3600) / 60;
    let seconds = day_secs % 60;

    let z = days as i64 + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        y, m, d, hours, minutes, seconds
    )
}

/// Extract the host portion from a URL.
fn extract_host(url: &str) -> String {
    let without_scheme = if let Some(pos) = url.find("://") {
        &url[pos + 3..]
    } else {
        url
    };
    let end = without_scheme.find('/').unwrap_or(without_scheme.len());
    without_scheme[..end].to_string()
}

/// AWS Signature V4 signing.
///
/// Constructs the Authorization header value for an AWS API request.
///
/// # Arguments
/// - `method`: HTTP method (GET, PUT, DELETE, etc.)
/// - `url`: Full request URL
/// - `headers`: Slice of (header_name, header_value) pairs to sign
/// - `body_hash`: Hex-encoded SHA-256 hash of the request body
/// - `access_key`: AWS access key ID
/// - `secret_key`: AWS secret access key
/// - `region`: AWS region (e.g. "us-east-1")
/// - `service`: AWS service name (e.g. "s3")
/// - `timestamp`: UTC timestamp in "YYYYMMDDTHHMMSSZ" format
///
/// # Returns
/// The full Authorization header value string.
#[allow(clippy::too_many_arguments)]
pub fn sign_aws_v4(
    method: &str,
    url: &str,
    headers: &[(&str, &str)],
    body_hash: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    service: &str,
    timestamp: &str,
) -> String {
    // Extract date from timestamp (first 8 chars).
    let date = &timestamp[..8];

    // Build credential scope.
    let credential_scope = format!("{date}/{region}/{service}/aws4_request");

    // Build canonical headers.
    let mut sorted_headers: Vec<(&str, &str)> = headers.to_vec();
    sorted_headers.sort_by_key(|(k, _)| k.to_lowercase());
    let canonical_headers: String = sorted_headers
        .iter()
        .map(|(k, v)| format!("{}:{}\n", k.to_lowercase(), v.trim()))
        .collect();
    let signed_headers: String = sorted_headers
        .iter()
        .map(|(k, _)| k.to_lowercase())
        .collect::<Vec<_>>()
        .join(";");

    // Parse URL to get path and query string.
    let (path, query) = parse_url_path_query(url);

    // Build canonical request.
    let canonical_request =
        format!("{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{body_hash}");

    // Hash the canonical request.
    let canonical_request_hash = sha256_hex(canonical_request.as_bytes());

    // Build string to sign.
    let string_to_sign =
        format!("AWS4-HMAC-SHA256\n{timestamp}\n{credential_scope}\n{canonical_request_hash}");

    // Derive the signing key.
    let k_date = hmac_sha256(format!("AWS4{secret_key}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");

    // Compute signature.
    let signature = hmac_sha256(&k_signing, string_to_sign.as_bytes());
    let signature_hex = hex_encode(&signature);

    // Build Authorization header.
    format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature_hex}"
    )
}

/// Parse a URL to extract the path and query components.
fn parse_url_path_query(url: &str) -> (String, String) {
    // Strip scheme and host.
    let without_scheme = if let Some(pos) = url.find("://") {
        &url[pos + 3..]
    } else {
        url
    };
    let path_start = without_scheme.find('/').unwrap_or(without_scheme.len());
    let path_and_query = &without_scheme[path_start..];

    if let Some(q_pos) = path_and_query.find('?') {
        (
            path_and_query[..q_pos].to_string(),
            path_and_query[q_pos + 1..].to_string(),
        )
    } else {
        (path_and_query.to_string(), String::new())
    }
}

/// Hex-encode a byte slice.
fn hex_encode(data: &[u8]) -> String {
    let mut s = String::with_capacity(data.len() * 2);
    for byte in data {
        s.push_str(&format!("{byte:02x}"));
    }
    s
}

// ---------------------------------------------------------------------------
// Minimal SHA-256 implementation (no external dependency)
// ---------------------------------------------------------------------------

const SHA256_K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    // Pre-processing: padding.
    let bit_len = (data.len() as u64) * 8;
    let mut padded = data.to_vec();
    padded.push(0x80);
    while (padded.len() % 64) != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    // Process each 64-byte block.
    for chunk in padded.chunks_exact(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) =
            (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);

        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(SHA256_K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    let mut result = [0u8; 32];
    for (i, val) in h.iter().enumerate() {
        result[i * 4..i * 4 + 4].copy_from_slice(&val.to_be_bytes());
    }
    result
}

/// HMAC-SHA256.
fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let block_size = 64;

    // If key is longer than block size, hash it.
    let key = if key.len() > block_size {
        sha256(key).to_vec()
    } else {
        key.to_vec()
    };

    // Pad key to block size.
    let mut key_padded = key.clone();
    key_padded.resize(block_size, 0);

    // Inner and outer pads.
    let mut i_key_pad = vec![0u8; block_size];
    let mut o_key_pad = vec![0u8; block_size];
    for i in 0..block_size {
        i_key_pad[i] = key_padded[i] ^ 0x36;
        o_key_pad[i] = key_padded[i] ^ 0x5c;
    }

    // Inner hash.
    let mut inner = i_key_pad;
    inner.extend_from_slice(message);
    let inner_hash = sha256(&inner);

    // Outer hash.
    let mut outer = o_key_pad;
    outer.extend_from_slice(&inner_hash);
    sha256(&outer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn put_get_roundtrip() {
        let dir = tempdir().unwrap();
        let store = LocalObjectStore::new(dir.path().to_path_buf()).unwrap();

        let data = b"hello, cold storage!";
        store.put("table1/2024-01-15.xpqt", data).unwrap();

        let retrieved = store.get("table1/2024-01-15.xpqt").unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn get_nonexistent_fails() {
        let dir = tempdir().unwrap();
        let store = LocalObjectStore::new(dir.path().to_path_buf()).unwrap();

        let result = store.get("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn exists_check() {
        let dir = tempdir().unwrap();
        let store = LocalObjectStore::new(dir.path().to_path_buf()).unwrap();

        assert!(!store.exists("key1").unwrap());
        store.put("key1", b"data").unwrap();
        assert!(store.exists("key1").unwrap());
    }

    #[test]
    fn delete_removes_object() {
        let dir = tempdir().unwrap();
        let store = LocalObjectStore::new(dir.path().to_path_buf()).unwrap();

        store.put("key1", b"data").unwrap();
        assert!(store.exists("key1").unwrap());

        store.delete("key1").unwrap();
        assert!(!store.exists("key1").unwrap());
    }

    #[test]
    fn delete_nonexistent_is_ok() {
        let dir = tempdir().unwrap();
        let store = LocalObjectStore::new(dir.path().to_path_buf()).unwrap();

        // Should not error
        store.delete("nonexistent").unwrap();
    }

    #[test]
    fn list_with_prefix() {
        let dir = tempdir().unwrap();
        let store = LocalObjectStore::new(dir.path().to_path_buf()).unwrap();

        store.put("trades/2024-01-01.xpqt", b"a").unwrap();
        store.put("trades/2024-01-02.xpqt", b"b").unwrap();
        store.put("quotes/2024-01-01.xpqt", b"c").unwrap();

        let trades = store.list("trades/").unwrap();
        assert_eq!(trades.len(), 2);
        assert!(trades.contains(&"trades/2024-01-01.xpqt".to_string()));
        assert!(trades.contains(&"trades/2024-01-02.xpqt".to_string()));

        let quotes = store.list("quotes/").unwrap();
        assert_eq!(quotes.len(), 1);

        let all = store.list("").unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn large_object_roundtrip() {
        let dir = tempdir().unwrap();
        let store = LocalObjectStore::new(dir.path().to_path_buf()).unwrap();

        // 1MB object
        let data: Vec<u8> = (0..1_000_000u32).flat_map(|i| i.to_le_bytes()).collect();

        store.put("large_file", &data).unwrap();
        let retrieved = store.get("large_file").unwrap();
        assert_eq!(retrieved.len(), data.len());
        assert_eq!(retrieved, data);
    }

    // --- MemoryObjectStore tests ---

    #[test]
    fn memory_put_get_roundtrip() {
        let store = MemoryObjectStore::new();
        store.put("key1", b"value1").unwrap();
        let data = store.get("key1").unwrap();
        assert_eq!(data, b"value1");
    }

    #[test]
    fn memory_get_nonexistent_fails() {
        let store = MemoryObjectStore::new();
        assert!(store.get("nope").is_err());
    }

    #[test]
    fn memory_exists() {
        let store = MemoryObjectStore::new();
        assert!(!store.exists("k").unwrap());
        store.put("k", b"v").unwrap();
        assert!(store.exists("k").unwrap());
    }

    #[test]
    fn memory_delete() {
        let store = MemoryObjectStore::new();
        store.put("k", b"v").unwrap();
        store.delete("k").unwrap();
        assert!(!store.exists("k").unwrap());
    }

    #[test]
    fn memory_delete_nonexistent_ok() {
        let store = MemoryObjectStore::new();
        store.delete("nope").unwrap();
    }

    #[test]
    fn memory_list_prefix() {
        let store = MemoryObjectStore::new();
        store.put("a/1", b"x").unwrap();
        store.put("a/2", b"x").unwrap();
        store.put("b/1", b"x").unwrap();

        let a = store.list("a/").unwrap();
        assert_eq!(a.len(), 2);
        assert!(a.contains(&"a/1".to_string()));
        assert!(a.contains(&"a/2".to_string()));

        let all = store.list("").unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn memory_overwrite() {
        let store = MemoryObjectStore::new();
        store.put("k", b"v1").unwrap();
        store.put("k", b"v2").unwrap();
        assert_eq!(store.get("k").unwrap(), b"v2");
    }

    // --- AWS Signature V4 tests ---

    #[test]
    fn sha256_known_vector() {
        // SHA-256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        let hash = sha256_hex(b"");
        assert_eq!(
            hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn sha256_hello() {
        // SHA-256("hello") = 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
        let hash = sha256_hex(b"hello");
        assert_eq!(
            hash,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn sign_aws_v4_produces_valid_format() {
        let auth = sign_aws_v4(
            "GET",
            "https://s3.amazonaws.com/mybucket/mykey",
            &[
                ("host", "s3.amazonaws.com"),
                ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
            ],
            "UNSIGNED-PAYLOAD",
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "us-east-1",
            "s3",
            "20260320T000000Z",
        );

        // Verify the format.
        assert!(auth.starts_with("AWS4-HMAC-SHA256 Credential="));
        assert!(auth.contains("AKIAIOSFODNN7EXAMPLE"));
        assert!(auth.contains("20260320/us-east-1/s3/aws4_request"));
        assert!(auth.contains("SignedHeaders="));
        assert!(auth.contains("Signature="));
        // Signature should be 64 hex chars.
        let sig_start = auth.find("Signature=").unwrap() + "Signature=".len();
        let signature = &auth[sig_start..];
        assert_eq!(signature.len(), 64, "signature should be 64 hex chars");
        assert!(
            signature.chars().all(|c| c.is_ascii_hexdigit()),
            "signature should be hex"
        );
    }

    #[test]
    fn sign_aws_v4_deterministic() {
        let auth1 = sign_aws_v4(
            "PUT",
            "https://s3.amazonaws.com/bucket/key",
            &[("host", "s3.amazonaws.com")],
            "abc123",
            "KEY",
            "SECRET",
            "us-east-1",
            "s3",
            "20260101T000000Z",
        );
        let auth2 = sign_aws_v4(
            "PUT",
            "https://s3.amazonaws.com/bucket/key",
            &[("host", "s3.amazonaws.com")],
            "abc123",
            "KEY",
            "SECRET",
            "us-east-1",
            "s3",
            "20260101T000000Z",
        );
        assert_eq!(auth1, auth2, "same inputs should produce same signature");
    }

    #[test]
    fn s3_object_store_url_construction() {
        let store = S3ObjectStore::new(
            "mybucket",
            "prefix/",
            "https://s3.amazonaws.com",
            "AKID",
            "SECRET",
            "us-east-1",
        );
        assert_eq!(store.full_key("data.parquet"), "prefix/data.parquet");
        assert_eq!(
            store.object_url("data.parquet"),
            "https://s3.amazonaws.com/mybucket/prefix/data.parquet"
        );
    }
}
