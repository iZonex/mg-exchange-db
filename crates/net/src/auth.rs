use std::sync::Arc;

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::oauth::OAuthProvider;
use crate::service_account::{ServiceAccountStore, extract_credentials};

/// Configuration for token-based authentication.
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    /// Whether authentication is enabled.
    pub enabled: bool,
    /// List of valid API tokens.
    pub tokens: Vec<String>,
}

impl AuthConfig {
    /// Create a new auth config with the given tokens. Enables auth automatically
    /// if at least one token is provided.
    pub fn new(tokens: Vec<String>) -> Self {
        let enabled = !tokens.is_empty();
        Self { enabled, tokens }
    }

    /// Check whether a given token is valid (constant-time comparison).
    pub fn is_valid_token(&self, token: &str) -> bool {
        use subtle::ConstantTimeEq;
        self.tokens.iter().any(|t| {
            // Constant-time comparison to prevent timing attacks.
            // We still leak the *number* of tokens (via iteration count),
            // but not which token matched or how many bytes matched.
            let t_bytes = t.as_bytes();
            let token_bytes = token.as_bytes();
            t_bytes.len() == token_bytes.len() && t_bytes.ct_eq(token_bytes).into()
        })
    }
}

/// Represents the different authentication methods supported by the system.
///
/// Multiple methods can be combined using `Multi`, which tries each method
/// in order until one succeeds.
pub enum AuthMethod {
    /// No authentication required (standalone mode).
    None,
    /// Simple bearer token authentication.
    Token(AuthConfig),
    /// OAuth 2.0 / OIDC authentication via JWT bearer tokens.
    OAuth(Arc<OAuthProvider>),
    /// Service account authentication via API key + secret.
    ServiceAccount(Arc<ServiceAccountStore>),
    /// Try multiple authentication methods in order.
    Multi(Vec<AuthMethod>),
}

impl std::fmt::Debug for AuthMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthMethod::None => write!(f, "AuthMethod::None"),
            AuthMethod::Token(_) => write!(f, "AuthMethod::Token(...)"),
            AuthMethod::OAuth(_) => write!(f, "AuthMethod::OAuth(...)"),
            AuthMethod::ServiceAccount(_) => write!(f, "AuthMethod::ServiceAccount(...)"),
            AuthMethod::Multi(methods) => {
                write!(f, "AuthMethod::Multi({} methods)", methods.len())
            }
        }
    }
}

/// Result of an authentication attempt.
#[derive(Debug, Clone)]
pub enum AuthResult {
    /// Authentication succeeded.
    Authenticated {
        /// Identity of the authenticated user/service.
        identity: String,
        /// Method that was used to authenticate.
        method: String,
        /// Roles assigned to the authenticated entity.
        roles: Vec<String>,
    },
    /// No credentials were provided.
    NoCredentials,
    /// Credentials were provided but are invalid.
    InvalidCredentials(String),
}

/// Paths that are always public (no auth required).
fn is_public_path(path: &str) -> bool {
    path == "/" || path == "/api/v1/health" || path == "/metrics" || path.starts_with("/auth/")
}

/// Try to authenticate a request using the given auth method.
pub fn try_authenticate(method: &AuthMethod, headers: &axum::http::HeaderMap) -> AuthResult {
    match method {
        AuthMethod::None => AuthResult::Authenticated {
            identity: "anonymous".to_string(),
            method: "none".to_string(),
            roles: vec![],
        },

        AuthMethod::Token(config) => {
            if !config.enabled {
                return AuthResult::Authenticated {
                    identity: "anonymous".to_string(),
                    method: "token-disabled".to_string(),
                    roles: vec![],
                };
            }

            let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

            match auth_header {
                Some(header) if header.starts_with("Bearer ") => {
                    let token = &header[7..];
                    if config.is_valid_token(token) {
                        AuthResult::Authenticated {
                            identity: "token-user".to_string(),
                            method: "bearer-token".to_string(),
                            roles: vec![],
                        }
                    } else {
                        AuthResult::InvalidCredentials("invalid token".to_string())
                    }
                }
                _ => AuthResult::NoCredentials,
            }
        }

        AuthMethod::OAuth(provider) => {
            let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

            match auth_header {
                Some(header) if header.starts_with("Bearer ") => {
                    let token = &header[7..];
                    match provider.verify_token(token) {
                        Ok(claims) => AuthResult::Authenticated {
                            identity: claims.email.unwrap_or(claims.sub),
                            method: "oauth".to_string(),
                            roles: claims.roles,
                        },
                        Err(e) => AuthResult::InvalidCredentials(format!("OAuth: {e}")),
                    }
                }
                _ => AuthResult::NoCredentials,
            }
        }

        AuthMethod::ServiceAccount(store) => match extract_credentials(headers) {
            Some((api_key, secret)) => match store.authenticate(&api_key, &secret) {
                Ok(Some(account)) => AuthResult::Authenticated {
                    identity: format!("service:{}", account.name),
                    method: "service-account".to_string(),
                    roles: account.roles,
                },
                Ok(None) => AuthResult::NoCredentials,
                Err(e) => AuthResult::InvalidCredentials(format!("Service account: {e}")),
            },
            None => AuthResult::NoCredentials,
        },

        AuthMethod::Multi(methods) => {
            let mut last_error = None;
            for m in methods {
                match try_authenticate(m, headers) {
                    AuthResult::Authenticated {
                        identity,
                        method,
                        roles,
                    } => {
                        return AuthResult::Authenticated {
                            identity,
                            method,
                            roles,
                        };
                    }
                    AuthResult::InvalidCredentials(msg) => {
                        last_error = Some(msg);
                    }
                    AuthResult::NoCredentials => {
                        // Try next method.
                    }
                }
            }
            match last_error {
                Some(msg) => AuthResult::InvalidCredentials(msg),
                None => AuthResult::NoCredentials,
            }
        }
    }
}

/// Axum middleware function for unified authentication.
///
/// This middleware supports multiple authentication methods: bearer tokens,
/// OAuth 2.0 JWT tokens, and service account credentials. It checks each
/// configured method in order until one succeeds.
///
/// Usage:
/// ```ignore
/// let app = Router::new()
///     .route("/api/v1/query", post(query))
///     .layer(axum::middleware::from_fn_with_state(
///         state.clone(),
///         auth_middleware,
///     ));
/// ```
pub async fn auth_middleware(
    state: axum::extract::State<Arc<crate::http::handlers::AppState>>,
    request: Request,
    next: Next,
) -> Response {
    // Public endpoints are always accessible.
    let path = request.uri().path().to_string();
    if is_public_path(&path) {
        return next.run(request).await;
    }

    // Try unified auth if configured.
    if let Some(ref auth_method) = state.auth_method {
        match try_authenticate(auth_method, request.headers()) {
            AuthResult::Authenticated { .. } => {
                return next.run(request).await;
            }
            AuthResult::InvalidCredentials(msg) => {
                let body = serde_json::json!({"error": msg, "code": 401});
                return (
                    StatusCode::UNAUTHORIZED,
                    [("content-type", "application/json")],
                    body.to_string(),
                )
                    .into_response();
            }
            AuthResult::NoCredentials => {
                // Fall through to legacy token check.
            }
        }
    }

    // Legacy token-based authentication.
    let auth_config = &state.auth_config;

    if !auth_config.enabled {
        return next.run(request).await;
    }

    let auth_header = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok());

    match auth_header {
        Some(header) if header.starts_with("Bearer ") => {
            let token = &header[7..];
            if auth_config.is_valid_token(token) {
                next.run(request).await
            } else {
                (
                    StatusCode::UNAUTHORIZED,
                    [("content-type", "application/json")],
                    r#"{"error":"invalid token","code":401}"#,
                )
                    .into_response()
            }
        }
        _ => (
            StatusCode::UNAUTHORIZED,
            [("content-type", "application/json")],
            r#"{"error":"missing or invalid Authorization header","code":401}"#,
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oauth::{OAuthConfig, OAuthProvider, TokenClaims};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_auth_config_default_disabled() {
        let config = AuthConfig::default();
        assert!(!config.enabled);
        assert!(config.tokens.is_empty());
    }

    #[test]
    fn test_auth_config_new_with_tokens() {
        let config = AuthConfig::new(vec!["token1".to_string(), "token2".to_string()]);
        assert!(config.enabled);
        assert!(config.is_valid_token("token1"));
        assert!(config.is_valid_token("token2"));
        assert!(!config.is_valid_token("token3"));
    }

    #[test]
    fn test_auth_config_new_empty() {
        let config = AuthConfig::new(vec![]);
        assert!(!config.enabled);
    }

    #[test]
    fn test_public_paths() {
        assert!(is_public_path("/api/v1/health"));
        assert!(is_public_path("/metrics"));
        assert!(is_public_path("/auth/login"));
        assert!(is_public_path("/auth/callback"));
        assert!(!is_public_path("/api/v1/query"));
        assert!(!is_public_path("/api/v1/write"));
        assert!(!is_public_path("/api/v1/tables"));
    }

    #[test]
    fn test_auth_method_none() {
        let method = AuthMethod::None;
        let headers = axum::http::HeaderMap::new();
        let result = try_authenticate(&method, &headers);
        assert!(matches!(result, AuthResult::Authenticated { .. }));
    }

    #[test]
    fn test_auth_method_token_valid() {
        let config = AuthConfig::new(vec!["valid-token".to_string()]);
        let method = AuthMethod::Token(config);

        let mut headers = axum::http::HeaderMap::new();
        headers.insert("authorization", "Bearer valid-token".parse().unwrap());

        let result = try_authenticate(&method, &headers);
        match result {
            AuthResult::Authenticated { method, .. } => assert_eq!(method, "bearer-token"),
            _ => panic!("expected Authenticated"),
        }
    }

    #[test]
    fn test_auth_method_token_invalid() {
        let config = AuthConfig::new(vec!["valid-token".to_string()]);
        let method = AuthMethod::Token(config);

        let mut headers = axum::http::HeaderMap::new();
        headers.insert("authorization", "Bearer wrong-token".parse().unwrap());

        let result = try_authenticate(&method, &headers);
        assert!(matches!(result, AuthResult::InvalidCredentials(_)));
    }

    #[test]
    fn test_auth_method_token_missing() {
        let config = AuthConfig::new(vec!["valid-token".to_string()]);
        let method = AuthMethod::Token(config);
        let headers = axum::http::HeaderMap::new();

        let result = try_authenticate(&method, &headers);
        assert!(matches!(result, AuthResult::NoCredentials));
    }

    #[test]
    fn test_auth_method_oauth() {
        let provider = OAuthProvider::new(OAuthConfig {
            enabled: true,
            issuer_url: "https://auth.test.com".to_string(),
            client_id: "client-id".to_string(),
            hmac_secret: Some("test-secret".to_string()),
            ..OAuthConfig::default()
        });

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let claims = TokenClaims {
            sub: "user-1".to_string(),
            email: Some("user@test.com".to_string()),
            name: Some("Test User".to_string()),
            roles: vec!["admin".to_string()],
            exp: now + 3600,
            iss: "https://auth.test.com".to_string(),
            aud: Some("client-id".to_string()),
        };

        let token = provider.create_token(&claims).unwrap();
        let method = AuthMethod::OAuth(Arc::new(provider));

        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );

        let result = try_authenticate(&method, &headers);
        match result {
            AuthResult::Authenticated {
                identity,
                method: m,
                roles,
            } => {
                assert_eq!(identity, "user@test.com");
                assert_eq!(m, "oauth");
                assert_eq!(roles, vec!["admin"]);
            }
            other => panic!("expected Authenticated, got {:?}", other),
        }
    }

    #[test]
    fn test_auth_method_service_account() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = ServiceAccountStore::open(dir.path()).unwrap();
        let creds = store
            .create("test-svc", vec!["reader".to_string()])
            .unwrap();

        let method = AuthMethod::ServiceAccount(Arc::new(store));

        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-api-key", creds.api_key.parse().unwrap());
        headers.insert("x-api-secret", creds.secret.parse().unwrap());

        let result = try_authenticate(&method, &headers);
        match result {
            AuthResult::Authenticated {
                identity,
                method: m,
                roles,
            } => {
                assert_eq!(identity, "service:test-svc");
                assert_eq!(m, "service-account");
                assert_eq!(roles, vec!["reader"]);
            }
            other => panic!("expected Authenticated, got {:?}", other),
        }
    }

    #[test]
    fn test_auth_method_multi_first_succeeds() {
        let config = AuthConfig::new(vec!["my-token".to_string()]);
        let method = AuthMethod::Multi(vec![AuthMethod::Token(config), AuthMethod::None]);

        let mut headers = axum::http::HeaderMap::new();
        headers.insert("authorization", "Bearer my-token".parse().unwrap());

        let result = try_authenticate(&method, &headers);
        match result {
            AuthResult::Authenticated { method: m, .. } => assert_eq!(m, "bearer-token"),
            _ => panic!("expected Authenticated"),
        }
    }

    #[test]
    fn test_auth_method_multi_fallback() {
        let config = AuthConfig::new(vec!["my-token".to_string()]);
        let method = AuthMethod::Multi(vec![
            AuthMethod::Token(config),
            AuthMethod::None, // Fallback
        ]);

        // No credentials provided, Token returns NoCredentials, None returns Authenticated.
        let headers = axum::http::HeaderMap::new();
        let result = try_authenticate(&method, &headers);
        match result {
            AuthResult::Authenticated { method: m, .. } => assert_eq!(m, "none"),
            _ => panic!("expected Authenticated via fallback"),
        }
    }

    #[test]
    fn test_auth_method_multi_all_fail() {
        let config1 = AuthConfig::new(vec!["token-a".to_string()]);
        let config2 = AuthConfig::new(vec!["token-b".to_string()]);
        let method =
            AuthMethod::Multi(vec![AuthMethod::Token(config1), AuthMethod::Token(config2)]);

        let mut headers = axum::http::HeaderMap::new();
        headers.insert("authorization", "Bearer wrong".parse().unwrap());

        let result = try_authenticate(&method, &headers);
        assert!(matches!(result, AuthResult::InvalidCredentials(_)));
    }
}
