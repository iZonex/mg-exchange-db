//! HTTP handlers for OAuth authentication routes.
//!
//! These routes handle the OAuth 2.0 authorization code flow:
//! - `GET /auth/login` — Redirect the user to the OAuth provider
//! - `GET /auth/callback` — Handle the OAuth callback after user authorization
//! - `GET /auth/token` — Return information about the current session token
//! - `POST /auth/logout` — Invalidate the current session

use std::sync::Arc;

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use serde::Deserialize;

use crate::auth::AuthMethod;
use crate::http::handlers::AppState;
use crate::oauth::OAuthProvider;

/// Query parameters for the OAuth callback.
#[derive(Debug, Deserialize)]
pub struct CallbackParams {
    /// The authorization code from the OAuth provider.
    pub code: Option<String>,
    /// The state parameter for CSRF protection.
    pub state: Option<String>,
    /// Error returned by the OAuth provider (if any).
    pub error: Option<String>,
    /// Human-readable error description.
    pub error_description: Option<String>,
}

/// Query parameters for the login redirect.
#[derive(Debug, Deserialize)]
pub struct LoginParams {
    /// Optional redirect URL after authentication.
    pub redirect_to: Option<String>,
}

/// `GET /auth/login`
///
/// Redirects the user to the OAuth provider's authorization endpoint.
/// If OAuth is not configured, returns a 404.
pub async fn login(
    State(state): State<Arc<AppState>>,
    Query(params): Query<LoginParams>,
) -> Response {
    let provider = match find_oauth_provider(&state) {
        Some(p) => p,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": "OAuth is not configured",
                    "code": 404,
                })),
            )
                .into_response();
        }
    };

    // Use the redirect_to as state for CSRF + post-login redirect.
    let state_value = params.redirect_to.unwrap_or_else(|| "/".to_string());
    let auth_url = provider.authorization_url(&state_value);

    Redirect::temporary(&auth_url).into_response()
}

/// `GET /auth/callback`
///
/// Handles the OAuth callback. In a full implementation, this would exchange
/// the authorization code for tokens. For now, it returns the code and state
/// as JSON (the token exchange requires an HTTP client to call the token endpoint).
pub async fn callback(
    State(state): State<Arc<AppState>>,
    Query(params): Query<CallbackParams>,
) -> Response {
    // Check for OAuth errors from the provider.
    if let Some(error) = params.error {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": error,
                "error_description": params.error_description.unwrap_or_default(),
                "code": 400,
            })),
        )
            .into_response();
    }

    let provider = match find_oauth_provider(&state) {
        Some(p) => p,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": "OAuth is not configured",
                    "code": 404,
                })),
            )
                .into_response();
        }
    };

    let code = match params.code {
        Some(c) => c,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "missing authorization code",
                    "code": 400,
                })),
            )
                .into_response();
        }
    };

    // In a production implementation, we would:
    // 1. Exchange the authorization code for an access token via HTTP POST to the token endpoint
    // 2. Validate the ID token
    // 3. Create a session
    //
    // For now, return the code and indicate where to exchange it.
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "authorization_code_received",
            "code": code,
            "state": params.state,
            "token_endpoint": format!("{}/token", provider.config().issuer_url),
            "message": "Exchange this code at the token endpoint to obtain an access token",
        })),
    )
        .into_response()
}

/// `GET /auth/token`
///
/// Returns information about the current bearer token. If a valid OAuth JWT
/// is provided in the Authorization header, returns the decoded claims.
pub async fn token_info(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Response {
    let provider = match find_oauth_provider(&state) {
        Some(p) => p,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": "OAuth is not configured",
                    "code": 404,
                })),
            )
                .into_response();
        }
    };

    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());

    let token = match auth_header {
        Some(header) if header.starts_with("Bearer ") => &header[7..],
        _ => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({
                    "error": "missing Bearer token",
                    "code": 401,
                })),
            )
                .into_response();
        }
    };

    match provider.verify_token(token) {
        Ok(claims) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "valid": true,
                "sub": claims.sub,
                "email": claims.email,
                "name": claims.name,
                "roles": claims.roles,
                "exp": claims.exp,
                "iss": claims.iss,
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "valid": false,
                "error": e.to_string(),
                "code": 401,
            })),
        )
            .into_response(),
    }
}

/// `POST /auth/logout`
///
/// Invalidates the current session. In a stateless JWT setup, the client
/// simply discards the token. This endpoint exists for session-based
/// implementations and to signal the client to clear credentials.
pub async fn logout() -> Response {
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "logged_out",
            "message": "Token should be discarded by the client",
        })),
    )
        .into_response()
}

/// Find the OAuth provider from the application's auth configuration.
fn find_oauth_provider(state: &AppState) -> Option<&OAuthProvider> {
    match &state.auth_method {
        Some(AuthMethod::OAuth(provider)) => Some(provider.as_ref()),
        Some(AuthMethod::Multi(methods)) => {
            for method in methods {
                if let AuthMethod::OAuth(provider) = method {
                    return Some(provider.as_ref());
                }
            }
            None
        }
        _ => None,
    }
}
