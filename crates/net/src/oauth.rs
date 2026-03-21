use std::sync::RwLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

/// Errors that can occur during OAuth/JWT operations.
#[derive(Debug, thiserror::Error)]
pub enum OAuthError {
    #[error("OAuth is not enabled")]
    Disabled,
    #[error("invalid JWT structure: {0}")]
    InvalidJwt(String),
    #[error("JWT has expired")]
    TokenExpired,
    #[error("invalid issuer: expected {expected}, got {actual}")]
    InvalidIssuer { expected: String, actual: String },
    #[error("invalid audience: expected {expected}, got {actual}")]
    InvalidAudience { expected: String, actual: String },
    #[error("email domain '{domain}' is not in allowed domains")]
    DomainNotAllowed { domain: String },
    #[error("invalid signature")]
    InvalidSignature,
    #[error("missing required claim: {0}")]
    MissingClaim(String),
    #[error("base64 decode error: {0}")]
    Base64Error(String),
    #[error("JSON parse error: {0}")]
    JsonError(String),
}

/// Configuration for OAuth 2.0 / OIDC authentication.
#[derive(Debug, Clone)]
pub struct OAuthConfig {
    /// Whether OAuth authentication is enabled.
    pub enabled: bool,
    /// The issuer URL, e.g., `https://accounts.google.com`.
    pub issuer_url: String,
    /// The OAuth client ID.
    pub client_id: String,
    /// The OAuth client secret (optional for public clients).
    pub client_secret: Option<String>,
    /// The redirect URI for the OAuth callback, e.g., `http://localhost:9000/auth/callback`.
    pub redirect_uri: String,
    /// Requested OAuth scopes, e.g., `["openid", "profile", "email"]`.
    pub scopes: Vec<String>,
    /// JSON Web Key Set URL for token verification (optional; defaults to `{issuer_url}/.well-known/jwks.json`).
    pub jwks_url: Option<String>,
    /// List of allowed email domains, e.g., `["mycompany.com"]`.
    /// If empty, all domains are allowed.
    pub allowed_domains: Vec<String>,
    /// HMAC secret for development token signing/verification.
    /// In production, RS256 with JWKS keys would be used instead.
    pub hmac_secret: Option<String>,
}

impl Default for OAuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            issuer_url: String::new(),
            client_id: String::new(),
            client_secret: None,
            redirect_uri: String::new(),
            scopes: vec!["openid".to_string(), "profile".to_string(), "email".to_string()],
            jwks_url: None,
            allowed_domains: Vec::new(),
            hmac_secret: None,
        }
    }
}

/// Represents a cached JWKS key set.
struct JwksCache {
    keys: Vec<JwkKey>,
    fetched_at: Instant,
}

/// A single JWK RSA public key.
#[derive(Debug, Clone)]
pub struct JwkKey {
    /// Key ID.
    pub kid: String,
    /// RSA modulus (base64url encoded).
    pub n: String,
    /// RSA exponent (base64url encoded).
    pub e: String,
}

/// Claims extracted from a verified JWT token.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TokenClaims {
    /// Subject (user ID from the identity provider).
    pub sub: String,
    /// User's email address.
    pub email: Option<String>,
    /// User's display name.
    pub name: Option<String>,
    /// Roles assigned to the user (from custom claims).
    pub roles: Vec<String>,
    /// Token expiry as a Unix timestamp (seconds).
    pub exp: i64,
    /// Token issuer.
    pub iss: String,
    /// Audience (client ID).
    #[serde(default)]
    pub aud: Option<String>,
}

/// JWT header.
#[derive(Debug, serde::Deserialize)]
struct JwtHeader {
    #[allow(dead_code)]
    alg: String,
    #[allow(dead_code)]
    typ: Option<String>,
}

/// The OAuth provider handles token generation and verification.
pub struct OAuthProvider {
    config: OAuthConfig,
    /// Cached JWKS keys for token verification.
    jwks_cache: RwLock<Option<JwksCache>>,
}

impl std::fmt::Debug for OAuthProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthProvider")
            .field("config", &self.config)
            .finish()
    }
}

impl OAuthProvider {
    /// Create a new OAuth provider with the given configuration.
    pub fn new(config: OAuthConfig) -> Self {
        Self {
            config,
            jwks_cache: RwLock::new(None),
        }
    }

    /// Get a reference to the provider's configuration.
    pub fn config(&self) -> &OAuthConfig {
        &self.config
    }

    /// Generate the authorization URL for redirecting users to the OAuth provider.
    ///
    /// The `state` parameter is an opaque value used to prevent CSRF attacks.
    pub fn authorization_url(&self, state: &str) -> String {
        let scopes = self.config.scopes.join("+");
        format!(
            "{}/authorize?response_type=code&client_id={}&redirect_uri={}&scope={}&state={}",
            self.config.issuer_url,
            url_encode(&self.config.client_id),
            url_encode(&self.config.redirect_uri),
            scopes,
            url_encode(state),
        )
    }

    /// Verify a JWT token (from an `Authorization: Bearer` header).
    ///
    /// This performs full verification including HMAC-SHA256 signature validation
    /// (for development). In production, RS256 verification with JWKS keys would
    /// be used instead.
    ///
    /// Returns the validated claims on success.
    pub fn verify_token(&self, token: &str) -> Result<TokenClaims, OAuthError> {
        if !self.config.enabled {
            return Err(OAuthError::Disabled);
        }

        // First validate structure and claims.
        let claims = self.validate_jwt_claims(token)?;

        // Then verify the HMAC-SHA256 signature if we have a secret.
        if let Some(ref secret) = self.config.hmac_secret {
            self.verify_hmac_signature(token, secret)?;
        }

        Ok(claims)
    }

    /// Extract and validate JWT claims without full cryptographic verification.
    ///
    /// Validates the JWT structure, expiry, issuer, and audience. Also checks
    /// domain restrictions if configured.
    pub fn validate_jwt_claims(&self, token: &str) -> Result<TokenClaims, OAuthError> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err(OAuthError::InvalidJwt(format!(
                "expected 3 parts, got {}",
                parts.len()
            )));
        }

        // Decode and parse header (we validate it exists but don't enforce alg here).
        let _header: JwtHeader = decode_jwt_part(parts[0])?;

        // Decode and parse payload.
        let payload_json = decode_jwt_segment(parts[1])?;
        let payload: serde_json::Value = serde_json::from_str(&payload_json)
            .map_err(|e| OAuthError::JsonError(e.to_string()))?;

        // Extract required claims.
        let sub = payload
            .get("sub")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OAuthError::MissingClaim("sub".to_string()))?
            .to_string();

        let exp = payload
            .get("exp")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| OAuthError::MissingClaim("exp".to_string()))?;

        let iss = payload
            .get("iss")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let aud = payload
            .get("aud")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let email = payload
            .get("email")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let name = payload
            .get("name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Extract roles from custom claim.
        let roles = payload
            .get("roles")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let claims = TokenClaims {
            sub,
            email,
            name,
            roles,
            exp,
            iss,
            aud,
        };

        // Validate expiry.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        if claims.exp < now {
            return Err(OAuthError::TokenExpired);
        }

        // Validate issuer if configured.
        if !self.config.issuer_url.is_empty() && claims.iss != self.config.issuer_url {
            return Err(OAuthError::InvalidIssuer {
                expected: self.config.issuer_url.clone(),
                actual: claims.iss.clone(),
            });
        }

        // Validate audience if configured.
        if !self.config.client_id.is_empty() {
            if let Some(ref aud) = claims.aud {
                if aud != &self.config.client_id {
                    return Err(OAuthError::InvalidAudience {
                        expected: self.config.client_id.clone(),
                        actual: aud.clone(),
                    });
                }
            }
        }

        // Validate email domain restriction.
        if !self.config.allowed_domains.is_empty() {
            if let Some(ref email) = claims.email {
                let domain = email
                    .rsplit('@')
                    .next()
                    .unwrap_or("")
                    .to_lowercase();
                if !self.config.allowed_domains.iter().any(|d| d.to_lowercase() == domain) {
                    return Err(OAuthError::DomainNotAllowed { domain });
                }
            }
        }

        Ok(claims)
    }

    /// Verify the HMAC-SHA256 signature of a JWT.
    fn verify_hmac_signature(&self, token: &str, secret: &str) -> Result<(), OAuthError> {
        let last_dot = token
            .rfind('.')
            .ok_or_else(|| OAuthError::InvalidJwt("missing signature".to_string()))?;
        let signing_input = &token[..last_dot];
        let signature_b64 = &token[last_dot + 1..];

        let signature = URL_SAFE_NO_PAD
            .decode(signature_b64)
            .map_err(|e| OAuthError::Base64Error(e.to_string()))?;

        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
            .map_err(|_| OAuthError::InvalidSignature)?;
        mac.update(signing_input.as_bytes());

        mac.verify_slice(&signature)
            .map_err(|_| OAuthError::InvalidSignature)?;

        Ok(())
    }

    /// Get cached JWKS keys, if available and not expired.
    pub fn get_cached_jwks(&self) -> Option<Vec<JwkKey>> {
        let cache = self.jwks_cache.read().ok()?;
        let cache = cache.as_ref()?;
        // Keys are valid for 1 hour.
        if cache.fetched_at.elapsed() < Duration::from_secs(3600) {
            Some(cache.keys.clone())
        } else {
            None
        }
    }

    /// Update the JWKS key cache.
    pub fn set_jwks_cache(&self, keys: Vec<JwkKey>) {
        if let Ok(mut cache) = self.jwks_cache.write() {
            *cache = Some(JwksCache {
                keys,
                fetched_at: Instant::now(),
            });
        }
    }

    /// Create a signed JWT for the given claims using HMAC-SHA256.
    ///
    /// This is primarily used for development and testing. In production,
    /// tokens would be issued by the identity provider.
    pub fn create_token(&self, claims: &TokenClaims) -> Result<String, OAuthError> {
        let secret = self
            .config
            .hmac_secret
            .as_ref()
            .ok_or_else(|| OAuthError::InvalidJwt("no HMAC secret configured".to_string()))?;

        let header = serde_json::json!({"alg": "HS256", "typ": "JWT"});
        let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
        let payload_b64 = URL_SAFE_NO_PAD.encode(
            serde_json::to_string(claims)
                .map_err(|e| OAuthError::JsonError(e.to_string()))?
                .as_bytes(),
        );

        let signing_input = format!("{}.{}", header_b64, payload_b64);

        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
            .map_err(|_| OAuthError::InvalidSignature)?;
        mac.update(signing_input.as_bytes());
        let signature = mac.finalize().into_bytes();
        let sig_b64 = URL_SAFE_NO_PAD.encode(&signature);

        Ok(format!("{}.{}", signing_input, sig_b64))
    }
}

/// Decode a base64url-encoded JWT segment to a string.
fn decode_jwt_segment(segment: &str) -> Result<String, OAuthError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(segment)
        .or_else(|_| {
            // Try with standard base64 padding as some providers use it.
            STANDARD.decode(segment)
        })
        .or_else(|_| {
            // Try adding padding.
            let padded = match segment.len() % 4 {
                2 => format!("{}==", segment),
                3 => format!("{}=", segment),
                _ => segment.to_string(),
            };
            URL_SAFE_NO_PAD.decode(&padded).or_else(|_| STANDARD.decode(&padded))
        })
        .map_err(|e| OAuthError::Base64Error(e.to_string()))?;

    String::from_utf8(bytes).map_err(|e| OAuthError::Base64Error(e.to_string()))
}

/// Decode a JWT part (header or payload) from base64url JSON.
fn decode_jwt_part<T: serde::de::DeserializeOwned>(segment: &str) -> Result<T, OAuthError> {
    let json_str = decode_jwt_segment(segment)?;
    serde_json::from_str(&json_str).map_err(|e| OAuthError::JsonError(e.to_string()))
}

/// Minimal URL encoding for query parameter values.
fn url_encode(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    for c in input.chars() {
        match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => result.push(c),
            ' ' => result.push_str("%20"),
            ':' => result.push_str("%3A"),
            '/' => result.push_str("%2F"),
            '?' => result.push_str("%3F"),
            '#' => result.push_str("%23"),
            '&' => result.push_str("%26"),
            '=' => result.push_str("%3D"),
            '@' => result.push_str("%40"),
            _ => {
                for byte in c.to_string().as_bytes() {
                    result.push_str(&format!("%{:02X}", byte));
                }
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_provider() -> OAuthProvider {
        OAuthProvider::new(OAuthConfig {
            enabled: true,
            issuer_url: "https://auth.example.com".to_string(),
            client_id: "test-client-id".to_string(),
            client_secret: Some("test-client-secret".to_string()),
            redirect_uri: "http://localhost:9000/auth/callback".to_string(),
            scopes: vec!["openid".to_string(), "profile".to_string(), "email".to_string()],
            jwks_url: None,
            allowed_domains: vec!["example.com".to_string()],
            hmac_secret: Some("super-secret-key-for-testing".to_string()),
        })
    }

    fn make_claims(exp_offset_secs: i64) -> TokenClaims {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        TokenClaims {
            sub: "user-123".to_string(),
            email: Some("alice@example.com".to_string()),
            name: Some("Alice".to_string()),
            roles: vec!["admin".to_string(), "reader".to_string()],
            exp: now + exp_offset_secs,
            iss: "https://auth.example.com".to_string(),
            aud: Some("test-client-id".to_string()),
        }
    }

    #[test]
    fn test_jwt_roundtrip() {
        let provider = test_provider();
        let claims = make_claims(3600);

        let token = provider.create_token(&claims).unwrap();
        let verified = provider.verify_token(&token).unwrap();

        assert_eq!(verified.sub, "user-123");
        assert_eq!(verified.email.as_deref(), Some("alice@example.com"));
        assert_eq!(verified.name.as_deref(), Some("Alice"));
        assert_eq!(verified.roles, vec!["admin", "reader"]);
        assert_eq!(verified.iss, "https://auth.example.com");
    }

    #[test]
    fn test_jwt_parsing_and_claim_validation() {
        let provider = test_provider();
        let claims = make_claims(3600);
        let token = provider.create_token(&claims).unwrap();

        // Validate claims without full crypto verification.
        let parsed = provider.validate_jwt_claims(&token).unwrap();
        assert_eq!(parsed.sub, "user-123");
        assert_eq!(parsed.email.as_deref(), Some("alice@example.com"));
    }

    #[test]
    fn test_token_expiry_detection() {
        let provider = test_provider();
        // Create a token that expired 1 hour ago.
        let claims = make_claims(-3600);
        let token = provider.create_token(&claims).unwrap();

        let result = provider.verify_token(&token);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OAuthError::TokenExpired));
    }

    #[test]
    fn test_invalid_jwt_structure() {
        let provider = test_provider();

        let result = provider.validate_jwt_claims("not-a-jwt");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OAuthError::InvalidJwt(_)));

        let result = provider.validate_jwt_claims("a.b");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), OAuthError::InvalidJwt(_)));
    }

    #[test]
    fn test_invalid_signature() {
        let provider = test_provider();
        let claims = make_claims(3600);
        let token = provider.create_token(&claims).unwrap();

        // Tamper with the signature.
        let parts: Vec<&str> = token.split('.').collect();
        let tampered = format!("{}.{}.{}", parts[0], parts[1], "invalid-signature");

        let result = provider.verify_token(&tampered);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_issuer() {
        let provider = test_provider();
        let mut claims = make_claims(3600);
        claims.iss = "https://evil.example.com".to_string();
        let token = provider.create_token(&claims).unwrap();

        let result = provider.verify_token(&token);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            OAuthError::InvalidIssuer { .. }
        ));
    }

    #[test]
    fn test_invalid_audience() {
        let provider = test_provider();
        let mut claims = make_claims(3600);
        claims.aud = Some("wrong-client-id".to_string());
        let token = provider.create_token(&claims).unwrap();

        let result = provider.verify_token(&token);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            OAuthError::InvalidAudience { .. }
        ));
    }

    #[test]
    fn test_domain_restriction_allowed() {
        let provider = test_provider();
        let claims = make_claims(3600); // email is alice@example.com
        let token = provider.create_token(&claims).unwrap();

        let result = provider.verify_token(&token);
        assert!(result.is_ok());
    }

    #[test]
    fn test_domain_restriction_denied() {
        let provider = test_provider();
        let mut claims = make_claims(3600);
        claims.email = Some("alice@evil.com".to_string());
        let token = provider.create_token(&claims).unwrap();

        let result = provider.verify_token(&token);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            OAuthError::DomainNotAllowed { .. }
        ));
    }

    #[test]
    fn test_domain_restriction_empty_allows_all() {
        let config = OAuthConfig {
            enabled: true,
            issuer_url: "https://auth.example.com".to_string(),
            client_id: "test-client-id".to_string(),
            hmac_secret: Some("secret".to_string()),
            allowed_domains: vec![], // No restriction
            ..OAuthConfig::default()
        };
        let provider = OAuthProvider::new(config);

        let mut claims = make_claims(3600);
        claims.email = Some("alice@anywhere.com".to_string());
        let token = provider.create_token(&claims).unwrap();

        let result = provider.verify_token(&token);
        assert!(result.is_ok());
    }

    #[test]
    fn test_authorization_url() {
        let provider = test_provider();
        let url = provider.authorization_url("random-state-123");

        assert!(url.starts_with("https://auth.example.com/authorize?"));
        assert!(url.contains("response_type=code"));
        assert!(url.contains("client_id=test-client-id"));
        assert!(url.contains("state=random-state-123"));
        assert!(url.contains("scope=openid+profile+email"));
    }

    #[test]
    fn test_jwks_cache() {
        let provider = test_provider();

        // Initially empty.
        assert!(provider.get_cached_jwks().is_none());

        // Set keys.
        let keys = vec![JwkKey {
            kid: "key-1".to_string(),
            n: "modulus".to_string(),
            e: "AQAB".to_string(),
        }];
        provider.set_jwks_cache(keys.clone());

        // Should be cached now.
        let cached = provider.get_cached_jwks().unwrap();
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].kid, "key-1");
    }

    #[test]
    fn test_disabled_provider() {
        let provider = OAuthProvider::new(OAuthConfig::default());
        let result = provider.verify_token("some.jwt.token");
        assert!(matches!(result.unwrap_err(), OAuthError::Disabled));
    }
}
