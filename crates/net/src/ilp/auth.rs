//! Challenge-response authentication for ILP TCP connections.
//!
//! After TCP accept, the server sends a challenge nonce. The client must
//! respond with its key-id (kid) and an HMAC-SHA256 signature of the
//! challenge using the shared secret for that kid.

use std::collections::HashMap;
use std::io;

use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

type HmacSha256 = Hmac<Sha256>;

/// Configuration for ILP TCP authentication.
#[derive(Debug, Clone, Default)]
pub struct IlpAuthConfig {
    /// Whether authentication is enabled.
    pub enabled: bool,
    /// Map of key-id to shared secret (base64 encoded).
    pub auth_keys: HashMap<String, String>,
}

/// Authenticator that manages challenge-response handshakes.
pub struct IlpAuthenticator {
    config: IlpAuthConfig,
}

impl IlpAuthenticator {
    /// Create a new authenticator with the given configuration.
    pub fn new(config: IlpAuthConfig) -> Self {
        Self { config }
    }

    /// Generate a random challenge nonce (32 bytes).
    pub fn generate_challenge() -> Vec<u8> {
        use rand::RngCore;
        let mut nonce = vec![0u8; 32];
        rand::thread_rng().fill_bytes(&mut nonce);
        nonce
    }

    /// Verify a signed challenge response.
    ///
    /// The client signs the challenge bytes with HMAC-SHA256 using the
    /// shared secret associated with `kid`.
    ///
    /// Returns `true` if the signature is valid.
    pub fn verify_response(&self, kid: &str, challenge: &[u8], signature: &[u8]) -> bool {
        let secret = match self.config.auth_keys.get(kid) {
            Some(s) => s,
            None => return false,
        };

        // Decode the base64-encoded secret.
        use base64::Engine as _;
        let secret_bytes = match base64::engine::general_purpose::STANDARD.decode(secret) {
            Ok(b) => b,
            Err(_) => return false,
        };

        let mut mac = match HmacSha256::new_from_slice(&secret_bytes) {
            Ok(m) => m,
            Err(_) => return false,
        };
        mac.update(challenge);

        mac.verify_slice(signature).is_ok()
    }

    /// Perform the authentication handshake on a TCP stream.
    ///
    /// Protocol:
    /// 1. Server sends the challenge as a base64 line: `<challenge_b64>\n`
    /// 2. Client responds with: `<kid> <signature_b64>\n`
    /// 3. If valid, server sends `ok\n` and returns the stream.
    ///    If invalid, server sends `error: ...\n` and returns an error.
    pub async fn handshake(&self, mut stream: TcpStream) -> io::Result<TcpStream> {
        use base64::Engine as _;
        let b64 = base64::engine::general_purpose::STANDARD;

        // 1. Send challenge
        let challenge = Self::generate_challenge();
        let challenge_b64 = b64.encode(&challenge);
        stream
            .write_all(format!("{challenge_b64}\n").as_bytes())
            .await?;
        stream.flush().await?;

        // 2. Read response: "kid signature_b64\n"
        let mut reader = BufReader::new(stream);
        let mut response_line = String::with_capacity(256);
        let n = reader.read_line(&mut response_line).await?;
        // Reject oversized auth responses to prevent OOM attacks.
        if n > 4096 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "auth response too large (>4KB)",
            ));
        }
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "client disconnected during auth handshake",
            ));
        }

        let response_line = response_line.trim();
        let parts: Vec<&str> = response_line.splitn(2, ' ').collect();
        if parts.len() != 2 {
            let mut stream = reader.into_inner();
            stream
                .write_all(b"error: invalid auth response format\n")
                .await?;
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "invalid auth response format",
            ));
        }

        let kid = parts[0];
        let sig_bytes = match b64.decode(parts[1]) {
            Ok(b) => b,
            Err(_) => {
                let mut stream = reader.into_inner();
                stream
                    .write_all(b"error: invalid signature encoding\n")
                    .await?;
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "invalid signature encoding",
                ));
            }
        };

        // 3. Verify
        if self.verify_response(kid, &challenge, &sig_bytes) {
            let mut stream = reader.into_inner();
            stream.write_all(b"ok\n").await?;
            stream.flush().await?;
            tracing::info!(kid = kid, "ILP client authenticated");
            Ok(stream)
        } else {
            let mut stream = reader.into_inner();
            stream.write_all(b"error: authentication failed\n").await?;
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("authentication failed for kid={kid}"),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;

    fn make_test_config() -> IlpAuthConfig {
        let secret = base64::engine::general_purpose::STANDARD.encode(b"test-secret-key!");
        let mut keys = HashMap::new();
        keys.insert("testkey".to_string(), secret);
        IlpAuthConfig {
            enabled: true,
            auth_keys: keys,
        }
    }

    #[test]
    fn test_generate_challenge() {
        let c1 = IlpAuthenticator::generate_challenge();
        let c2 = IlpAuthenticator::generate_challenge();
        assert_eq!(c1.len(), 32);
        assert_eq!(c2.len(), 32);
        assert_ne!(c1, c2); // extremely unlikely to collide
    }

    #[test]
    fn test_verify_valid_response() {
        let config = make_test_config();
        let auth = IlpAuthenticator::new(config.clone());

        let challenge = IlpAuthenticator::generate_challenge();

        // Compute the expected HMAC-SHA256 signature.
        let secret_bytes = base64::engine::general_purpose::STANDARD
            .decode(config.auth_keys.get("testkey").unwrap())
            .unwrap();
        let mut mac = HmacSha256::new_from_slice(&secret_bytes).unwrap();
        mac.update(&challenge);
        let signature = mac.finalize().into_bytes().to_vec();

        assert!(auth.verify_response("testkey", &challenge, &signature));
    }

    #[test]
    fn test_verify_invalid_kid() {
        let config = make_test_config();
        let auth = IlpAuthenticator::new(config);

        let challenge = IlpAuthenticator::generate_challenge();
        let fake_sig = vec![0u8; 32];

        assert!(!auth.verify_response("unknown_kid", &challenge, &fake_sig));
    }

    #[test]
    fn test_verify_wrong_signature() {
        let config = make_test_config();
        let auth = IlpAuthenticator::new(config);

        let challenge = IlpAuthenticator::generate_challenge();
        let wrong_sig = vec![0u8; 32];

        assert!(!auth.verify_response("testkey", &challenge, &wrong_sig));
    }

    #[test]
    fn test_verify_wrong_challenge() {
        let config = make_test_config();
        let auth = IlpAuthenticator::new(config.clone());

        let challenge = IlpAuthenticator::generate_challenge();
        let other_challenge = IlpAuthenticator::generate_challenge();

        // Sign the original challenge
        let secret_bytes = base64::engine::general_purpose::STANDARD
            .decode(config.auth_keys.get("testkey").unwrap())
            .unwrap();
        let mut mac = HmacSha256::new_from_slice(&secret_bytes).unwrap();
        mac.update(&challenge);
        let signature = mac.finalize().into_bytes().to_vec();

        // Verify against a different challenge -- should fail
        assert!(!auth.verify_response("testkey", &other_challenge, &signature));
    }
}
