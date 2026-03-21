pub mod admin;
pub mod diagnostics;
pub mod export;
pub mod grafana;
pub mod handlers;
pub mod rate_limit;
pub mod response;

use std::net::IpAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{header, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use tower_http::cors::CorsLayer;

use handlers::AppState;

/// Default maximum concurrent connections when no explicit limit is configured.
const DEFAULT_MAX_CONNECTIONS: u32 = 1024;

/// Rate-limiting middleware that delegates to the `RateLimiter` stored in `AppState`.
///
/// Extracts the client IP from the request extensions and checks against
/// the per-IP rate limit. Returns 429 Too Many Requests when exceeded.
async fn app_rate_limit_middleware(
    State(state): State<Arc<AppState>>,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    // Try to get the client IP from ConnectInfo if available,
    // otherwise fall back to localhost.
    let ip = request
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|ci| ci.0.ip())
        .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));

    if !state.rate_limiter.check(ip) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "Rate limit exceeded. Try again later.",
        )
            .into_response();
    }

    next.run(request).await
}

/// Build the axum [`Router`] with all API routes.
///
/// Applies a connection-pool middleware that rejects requests with HTTP 503
/// when the server has reached `DEFAULT_MAX_CONNECTIONS` concurrent connections.
pub fn router(state: Arc<AppState>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION]);

    let connection_pool = crate::pool::ConnectionPool::new(DEFAULT_MAX_CONNECTIONS);

    let api = Router::new()
        .route("/health", get(handlers::health))
        .route("/query", post(handlers::query))
        .route("/query/cancel/{id}", post(handlers::cancel_query))
        .route("/queries/active", get(handlers::active_queries))
        .route("/write", post(handlers::write))
        .route("/tables", get(handlers::list_tables))
        .route("/tables/{name}", get(handlers::table_info))
        .route("/query/stream", get(handlers::query_stream))
        .route("/export", get(export::export_csv))
        .route("/import", post(export::import_csv))
        .route("/ws", get(crate::ws::ws_handler))
        .route("/diagnostics", get(diagnostics::diagnostics));

    // Auth routes (always public — handled by is_public_path in auth middleware).
    let auth = Router::new()
        .route("/login", get(crate::auth_routes::login))
        .route("/callback", get(crate::auth_routes::callback))
        .route("/token", get(crate::auth_routes::token_info))
        .route("/logout", post(crate::auth_routes::logout));

    Router::new()
        .route("/", get(|| async { crate::console::console_handler() }))
        .nest("/api/v1", api)
        .nest("/api/v1/grafana", grafana::grafana_router())
        .nest("/auth", auth)
        .nest("/admin", admin::admin_router())
        .route("/metrics", get(crate::metrics::metrics_handler))
        .layer(cors)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            app_rate_limit_middleware,
        ))
        .layer(axum::middleware::from_fn(move |req, next| {
            let pool = connection_pool.clone();
            crate::pool::connection_limit_middleware(pool, req, next)
        }))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::auth::auth_middleware,
        ))
        .with_state(state)
}

/// Build the axum [`Router`] with a custom connection limit.
///
/// Same as [`router`] but allows callers to specify the maximum number of
/// concurrent connections.
pub fn router_with_pool(state: Arc<AppState>, max_connections: u32) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION]);

    let connection_pool = crate::pool::ConnectionPool::new(max_connections);

    let api = Router::new()
        .route("/health", get(handlers::health))
        .route("/query", post(handlers::query))
        .route("/query/cancel/{id}", post(handlers::cancel_query))
        .route("/queries/active", get(handlers::active_queries))
        .route("/write", post(handlers::write))
        .route("/tables", get(handlers::list_tables))
        .route("/tables/{name}", get(handlers::table_info))
        .route("/query/stream", get(handlers::query_stream))
        .route("/export", get(export::export_csv))
        .route("/import", post(export::import_csv))
        .route("/ws", get(crate::ws::ws_handler))
        .route("/diagnostics", get(diagnostics::diagnostics));

    let auth = Router::new()
        .route("/login", get(crate::auth_routes::login))
        .route("/callback", get(crate::auth_routes::callback))
        .route("/token", get(crate::auth_routes::token_info))
        .route("/logout", post(crate::auth_routes::logout));

    Router::new()
        .route("/", get(|| async { crate::console::console_handler() }))
        .nest("/api/v1", api)
        .nest("/api/v1/grafana", grafana::grafana_router())
        .nest("/auth", auth)
        .nest("/admin", admin::admin_router())
        .route("/metrics", get(crate::metrics::metrics_handler))
        .layer(cors)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            app_rate_limit_middleware,
        ))
        .layer(axum::middleware::from_fn(move |req, next| {
            let pool = connection_pool.clone();
            crate::pool::connection_limit_middleware(pool, req, next)
        }))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::auth::auth_middleware,
        ))
        .with_state(state)
}
