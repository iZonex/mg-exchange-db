//! TLS/SSL configuration for ExchangeDB HTTP server.
//!
//! When TLS is enabled, the HTTP server uses `axum_server` with rustls
//! instead of a plain `tokio::net::TcpListener`. Only TLS 1.2+ is allowed,
//! and only modern cipher suites (AEAD-based) are used.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use serde::Deserialize;

/// TLS configuration for the server.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TlsConfig {
    /// Whether TLS is enabled.
    pub enabled: bool,
    /// Path to the PEM-encoded certificate file.
    pub cert_path: String,
    /// Path to the PEM-encoded private key file.
    pub key_path: String,
    /// Minimum TLS version: "1.2" or "1.3". Default: "1.2".
    pub min_version: String,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_path: "cert.pem".into(),
            key_path: "key.pem".into(),
            min_version: "1.2".into(),
        }
    }
}

/// Build a hardened rustls `ServerConfig` with explicit protocol versions
/// and modern AEAD cipher suites only.
fn build_rustls_server_config(
    config: &TlsConfig,
) -> io::Result<rustls::ServerConfig> {
    use rustls::crypto::ring::default_provider;
    use rustls::pki_types::CertificateDer;

    // Select protocol versions based on config
    let versions: Vec<&'static rustls::SupportedProtocolVersion> = match config.min_version.as_str()
    {
        "1.3" => vec![&rustls::version::TLS13],
        _ => vec![&rustls::version::TLS12, &rustls::version::TLS13],
    };

    // Use the default ring crypto provider with all its cipher suites
    // (rustls only includes AEAD ciphers — no CBC, no RC4, no 3DES)
    let provider = default_provider();

    let builder = rustls::ServerConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(&versions)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
        .with_no_client_auth();

    // Load certificate chain
    let cert_pem = std::fs::read(&config.cert_path)
        .map_err(|e| io::Error::new(io::ErrorKind::NotFound, format!("cert: {e}")))?;
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut &cert_pem[..])
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("cert parse: {e}")))?;

    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "no certificates found in PEM file",
        ));
    }

    // Load private key
    let key_pem = std::fs::read(&config.key_path)
        .map_err(|e| io::Error::new(io::ErrorKind::NotFound, format!("key: {e}")))?;
    let key = rustls_pemfile::private_key(&mut &key_pem[..])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("key parse: {e}")))?
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "no private key found in PEM file")
        })?;

    builder
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("TLS config: {e}")))
}

/// Load a rustls configuration from the given [`TlsConfig`].
///
/// Enforces TLS 1.2+ with modern AEAD cipher suites only (no CBC, RC4, 3DES).
pub async fn load_tls_config(
    config: &TlsConfig,
) -> io::Result<axum_server::tls_rustls::RustlsConfig> {
    let tls_config = config.clone();
    let server_config = tokio::task::spawn_blocking(move || build_rustls_server_config(&tls_config))
        .await
        .map_err(io::Error::other)??;

    Ok(axum_server::tls_rustls::RustlsConfig::from_config(
        Arc::new(server_config),
    ))
}

/// Start the HTTP server with TLS on the given address.
pub async fn serve_tls(
    addr: SocketAddr,
    router: axum::Router,
    tls_config: axum_server::tls_rustls::RustlsConfig,
) -> io::Result<()> {
    tracing::info!(addr = %addr, "starting HTTPS server with TLS");
    axum_server::bind_rustls(addr, tls_config)
        .serve(router.into_make_service())
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_defaults() {
        let config = TlsConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.cert_path, "cert.pem");
        assert_eq!(config.key_path, "key.pem");
        assert_eq!(config.min_version, "1.2");
    }

    #[test]
    fn test_tls_config_deserialization() {
        let toml_str = r#"
enabled = true
cert_path = "/etc/ssl/server.crt"
key_path = "/etc/ssl/server.key"
min_version = "1.3"
"#;
        let config: TlsConfig = toml::from_str(toml_str).unwrap();
        assert!(config.enabled);
        assert_eq!(config.cert_path, "/etc/ssl/server.crt");
        assert_eq!(config.key_path, "/etc/ssl/server.key");
        assert_eq!(config.min_version, "1.3");
    }

    #[test]
    fn test_tls_config_deserialization_defaults_min_version() {
        let toml_str = r#"
enabled = true
cert_path = "cert.pem"
key_path = "key.pem"
"#;
        let config: TlsConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.min_version, "1.2");
    }

    #[tokio::test]
    async fn test_load_tls_config_missing_files() {
        let config = TlsConfig {
            enabled: true,
            cert_path: "/nonexistent/cert.pem".into(),
            key_path: "/nonexistent/key.pem".into(),
            min_version: "1.2".into(),
        };
        let result = load_tls_config(&config).await;
        assert!(result.is_err(), "should fail with missing cert/key files");
    }
}
