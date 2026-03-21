//! PostgreSQL wire protocol support for ExchangeDB.
//!
//! Allows users to connect with `psql`, DBeaver, or any PostgreSQL-compatible
//! client and execute SQL queries against the database.

pub mod copy;
pub mod extended;
pub mod handler;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::{NoopErrorHandler, PgWireServerHandlers};

use exchange_core::replication::ReplicationManager;

use self::copy::ExchangeDbCopyHandler;
use self::extended::ExchangeDbExtendedHandler;
use self::handler::ExchangeDbHandler;

/// Default port for the PostgreSQL wire protocol server.
pub const DEFAULT_PG_PORT: u16 = 8812;

/// Server handler configuration that wires together all pgwire trait
/// implementations needed to serve connections.
struct ExchangeDbServerHandlers {
    startup_handler: Arc<ExchangeDbStartupHandler>,
    simple_query_handler: Arc<ExchangeDbHandler>,
    extended_query_handler: Arc<ExchangeDbExtendedHandler>,
    copy_handler: Arc<ExchangeDbCopyHandler>,
    error_handler: Arc<NoopErrorHandler>,
}

/// A noop startup handler that accepts all connections without authentication.
#[derive(Debug)]
struct ExchangeDbStartupHandler;

impl NoopStartupHandler for ExchangeDbStartupHandler {}

impl PgWireServerHandlers for ExchangeDbServerHandlers {
    type StartupHandler = ExchangeDbStartupHandler;
    type SimpleQueryHandler = ExchangeDbHandler;
    type ExtendedQueryHandler = ExchangeDbExtendedHandler;
    type CopyHandler = ExchangeDbCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.simple_query_handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.extended_query_handler.clone()
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.startup_handler.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        self.copy_handler.clone()
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        self.error_handler.clone()
    }
}

/// Start the PostgreSQL wire protocol server on the given address.
///
/// The server accepts connections and handles SQL queries by delegating to
/// the ExchangeDB query engine. It runs until the process is terminated.
pub async fn start_pg_server(
    addr: SocketAddr,
    db_root: impl Into<PathBuf>,
    replication_manager: Option<Arc<ReplicationManager>>,
) -> std::io::Result<()> {
    let db_root = db_root.into();
    let handler = Arc::new(ExchangeDbHandler::new(db_root.clone(), replication_manager.clone()));
    let extended_handler = Arc::new(ExchangeDbExtendedHandler::new(db_root.clone(), replication_manager));
    let copy_handler = Arc::new(ExchangeDbCopyHandler::new(db_root));

    let handlers = Arc::new(ExchangeDbServerHandlers {
        startup_handler: Arc::new(ExchangeDbStartupHandler),
        simple_query_handler: handler,
        extended_query_handler: extended_handler,
        copy_handler,
        error_handler: Arc::new(NoopErrorHandler),
    });

    tracing::info!(addr = %addr, "starting PostgreSQL wire protocol server");

    let listener = tokio::net::TcpListener::bind(addr).await?;

    loop {
        let (tcp_stream, _peer_addr) = listener.accept().await?;
        let handlers_ref = handlers.clone();

        tokio::spawn(async move {
            if let Err(e) = pgwire::tokio::process_socket(tcp_stream, None, handlers_ref).await {
                tracing::error!(error = %e, "error processing PostgreSQL connection");
            }
        });
    }
}
