//! Dedicated TCP server for high-throughput ILP (InfluxDB Line Protocol)
//! ingestion.
//!
//! Clients connect over plain TCP and send newline-delimited ILP text.
//! The server parses each line with the existing ILP parser, batches
//! writes, and flushes to disk.

use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpListener;

use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::partition::partition_dir;
use exchange_core::partition_lock::PartitionLockManager;
use exchange_core::table::{ColumnValue, TableBuilder, TableMeta, TableWriter, WriteMode};
use exchange_core::wal::row_codec::OwnedColumnValue;
use exchange_core::wal_writer::{WalTableWriter, WalTableWriterConfig};

use super::auth::{IlpAuthConfig, IlpAuthenticator};
use super::parser::{parse_ilp_line, IlpLine, IlpValue};

use std::collections::HashMap;

/// Global per-table partition lock managers for fine-grained concurrent ILP writes.
///
/// Multiple ILP writers can write to DIFFERENT partitions of the same table
/// simultaneously, improving throughput for high-volume ingestion with diverse
/// timestamps. Writers to the SAME partition are serialized.
static PARTITION_LOCK_MANAGERS: std::sync::LazyLock<
    dashmap::DashMap<String, Arc<PartitionLockManager>>,
> = std::sync::LazyLock::new(dashmap::DashMap::new);

/// Get or create the partition lock manager for a given table.
fn get_partition_lock_manager(table_name: &str) -> Arc<PartitionLockManager> {
    PARTITION_LOCK_MANAGERS
        .entry(table_name.to_string())
        .or_insert_with(|| Arc::new(PartitionLockManager::new()))
        .clone()
}

/// Default port for the ILP TCP server.
pub const DEFAULT_ILP_PORT: u16 = 9009;

/// Default number of lines to accumulate before flushing to disk.
const DEFAULT_BATCH_SIZE: usize = 1000;

/// Configuration for the ILP TCP server.
#[derive(Debug, Clone)]
pub struct IlpServerConfig {
    /// Address to listen on.
    pub addr: SocketAddr,
    /// Database root directory.
    pub db_root: PathBuf,
    /// Number of ILP lines to batch before flushing.
    pub batch_size: usize,
    /// Controls whether writes go through WAL for durability.
    pub write_mode: WriteMode,
    /// Optional authentication configuration.
    pub auth: Option<IlpAuthConfig>,
}

impl IlpServerConfig {
    pub fn new(addr: SocketAddr, db_root: PathBuf) -> Self {
        Self {
            addr,
            db_root,
            batch_size: DEFAULT_BATCH_SIZE,
            write_mode: WriteMode::default(),
            auth: None,
        }
    }
}

/// Start the ILP TCP server.
///
/// This function listens on `addr` and spawns a task for each incoming
/// connection. It runs forever (or until the runtime is shut down).
pub async fn start_ilp_server(addr: SocketAddr, db_root: PathBuf) -> io::Result<()> {
    let config = IlpServerConfig::new(addr, db_root);
    start_ilp_server_with_config(config).await
}

/// Start the ILP TCP server with full configuration.
pub async fn start_ilp_server_with_config(config: IlpServerConfig) -> io::Result<()> {
    let listener = TcpListener::bind(config.addr).await?;
    let config = Arc::new(config);

    tracing::info!(addr = %config.addr, "ILP TCP server listening");

    loop {
        // Stop accepting new connections if shutdown has been requested.
        if crate::is_shutting_down() {
            tracing::info!("ILP server: shutdown requested, stopping accept loop");
            break;
        }

        let (stream, peer) = listener.accept().await?;
        let cfg = Arc::clone(&config);

        crate::track_connection_open();
        tokio::spawn(async move {
            tracing::debug!(peer = %peer, "ILP connection accepted");
            if let Err(e) = handle_connection(stream, &cfg).await {
                tracing::warn!(peer = %peer, error = %e, "ILP connection error");
            }
            tracing::debug!(peer = %peer, "ILP connection closed");
            crate::track_connection_close();
        });
    }

    Ok(())
}

/// Handle a single TCP connection: optionally authenticate, then read lines, parse, batch, write.
async fn handle_connection(
    stream: tokio::net::TcpStream,
    config: &IlpServerConfig,
) -> io::Result<()> {
    // Perform authentication handshake if configured.
    let stream = if let Some(ref auth_config) = config.auth {
        if auth_config.enabled {
            let authenticator = IlpAuthenticator::new(auth_config.clone());
            authenticator.handshake(stream).await?
        } else {
            stream
        }
    } else {
        stream
    };

    let reader = BufReader::new(stream);
    let mut lines_iter = reader.lines();
    let mut batch: Vec<IlpLine> = Vec::with_capacity(config.batch_size);

    while let Some(line) = lines_iter.next_line().await? {
        let trimmed = line.trim().to_string();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        match parse_ilp_line(&trimmed) {
            Ok(parsed) => {
                batch.push(parsed);
                if batch.len() >= config.batch_size {
                    let to_flush = std::mem::replace(
                        &mut batch,
                        Vec::with_capacity(config.batch_size),
                    );
                    let db_root = config.db_root.clone();
                    let wm = config.write_mode;
                    tokio::task::spawn_blocking(move || flush_batch(&db_root, to_flush, wm))
                        .await
                        .map_err(io::Error::other)??;
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, line = %trimmed, "ILP parse error, skipping line");
            }
        }
    }

    // Flush remaining lines.
    if !batch.is_empty() {
        let db_root = config.db_root.clone();
        let wm = config.write_mode;
        tokio::task::spawn_blocking(move || flush_batch(&db_root, batch, wm))
            .await
            .map_err(io::Error::other)??;
    }

    Ok(())
}

/// Write a batch of parsed ILP lines to disk (public for UDP server).
pub fn flush_batch_public(db_root: &Path, lines: Vec<IlpLine>, write_mode: WriteMode) -> io::Result<()> {
    flush_batch(db_root, lines, write_mode)
}

/// Write a batch of parsed ILP lines to disk. Groups by measurement
/// (table), auto-creates tables if needed.
fn flush_batch(db_root: &Path, lines: Vec<IlpLine>, write_mode: WriteMode) -> io::Result<()> {
    // Group by measurement.
    let mut by_table: std::collections::BTreeMap<String, Vec<&IlpLine>> =
        std::collections::BTreeMap::new();
    for line in &lines {
        by_table
            .entry(line.measurement.clone())
            .or_default()
            .push(line);
    }

    for (table_name, table_lines) in &by_table {
        // Validate measurement name to prevent path traversal.
        if let Err(e) = exchange_common::validation::validate_measurement_name(table_name) {
            tracing::warn!(measurement = %table_name, error = %e, "rejecting invalid measurement name");
            continue;
        }

        let table_dir = db_root.join(table_name);
        let meta_path = table_dir.join("_meta");

        // Auto-create the table if it does not exist.
        if !meta_path.exists() {
            let first = table_lines[0];
            auto_create_table(db_root, table_name, first).map_err(|e| {
                io::Error::other(format!("auto-create table: {e}"))
            })?;
        }

        match write_mode {
            WriteMode::Wal => {
                flush_table_wal(db_root, table_name, table_lines)?;
            }
            WriteMode::Direct => {
                flush_table_direct(db_root, table_name, table_lines)?;
            }
        }
    }

    tracing::debug!(lines = lines.len(), "ILP batch flushed");
    Ok(())
}

/// Write ILP lines for a single table using WAL for durability.
///
/// Uses per-partition locking so that concurrent ILP writers writing to
/// DIFFERENT time partitions (e.g., different days) can proceed in parallel.
/// Writers to the SAME partition are serialized.
///
/// Within each partition batch, all rows are written to WAL as a single
/// event before a single commit, amortizing the fsync cost.
fn flush_table_wal(db_root: &Path, table_name: &str, table_lines: &[&IlpLine]) -> io::Result<()> {
    let table_dir = db_root.join(table_name);

    // Load metadata to determine partition scheme.
    let meta = TableMeta::load(&table_dir.join("_meta")).map_err(|e| {
        io::Error::other(format!("load meta: {e}"))
    })?;
    let partition_by: PartitionBy = meta.partition_by.into();

    // Group lines by partition.
    let mut by_partition: HashMap<String, Vec<&IlpLine>> = HashMap::new();
    for line in table_lines {
        let ts = line.timestamp.unwrap_or_else(Timestamp::now);
        let part_name = partition_dir(ts, partition_by);
        by_partition.entry(part_name).or_default().push(line);
    }

    let partition_mgr = get_partition_lock_manager(table_name);

    for (part_name, part_lines) in &by_partition {
        // Acquire per-partition lock (other partitions can write concurrently).
        let _lock = partition_mgr.lock_partition(part_name);

        let config = WalTableWriterConfig {
            buffer_capacity: part_lines.len().max(1),
            ..WalTableWriterConfig::default()
        };

        let mut writer = WalTableWriter::open(db_root, table_name, config).map_err(|e| {
            io::Error::other(format!("open WAL writer: {e}"))
        })?;

        for line in part_lines {
            let ts = line.timestamp.unwrap_or_else(Timestamp::now);

            let owned_values: Vec<OwnedColumnValue> = meta
                .columns
                .iter()
                .enumerate()
                .map(|(i, col_def)| {
                    if i == meta.timestamp_column {
                        return OwnedColumnValue::Timestamp(ts.as_nanos());
                    }
                    if let Some(tag_val) = line.tags.get(&col_def.name) {
                        return OwnedColumnValue::Varchar(tag_val.clone());
                    }
                    if let Some(field_val) = line.fields.get(&col_def.name) {
                        return ilp_value_to_owned(field_val);
                    }
                    OwnedColumnValue::Null
                })
                .collect();

            writer.write_row(ts, owned_values).map_err(|e| {
                io::Error::other(format!("WAL write row: {e}"))
            })?;
        }

        writer.commit().map_err(|e| {
            io::Error::other(format!("WAL commit: {e}"))
        })?;
    }

    Ok(())
}

/// Write ILP lines for a single table using direct column file writes (no WAL).
///
/// Uses per-partition locking so that concurrent ILP writers writing to
/// DIFFERENT time partitions can proceed in parallel. Writers to the SAME
/// partition are serialized.
fn flush_table_direct(db_root: &Path, table_name: &str, table_lines: &[&IlpLine]) -> io::Result<()> {
    let table_dir = db_root.join(table_name);

    let meta = TableMeta::load(&table_dir.join("_meta")).map_err(|e| {
        io::Error::other(format!("load meta: {e}"))
    })?;
    let partition_by: PartitionBy = meta.partition_by.into();

    // Group lines by partition.
    let mut by_partition: HashMap<String, Vec<&IlpLine>> = HashMap::new();
    for line in table_lines {
        let ts = line.timestamp.unwrap_or_else(Timestamp::now);
        let part_name = partition_dir(ts, partition_by);
        by_partition.entry(part_name).or_default().push(line);
    }

    let partition_mgr = get_partition_lock_manager(table_name);

    for (part_name, part_lines) in &by_partition {
        let _lock = partition_mgr.lock_partition(part_name);

        let mut writer = TableWriter::open(db_root, table_name).map_err(|e| {
            io::Error::other(format!("open writer: {e}"))
        })?;

        for line in part_lines {
            let ts = line.timestamp.unwrap_or_else(Timestamp::now);

            let col_values: Vec<ColumnValue<'_>> = meta
                .columns
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != meta.timestamp_column)
                .map(|(_, col_def)| {
                    if let Some(tag_val) = line.tags.get(&col_def.name) {
                        return ColumnValue::Str(tag_val.as_str());
                    }
                    if let Some(field_val) = line.fields.get(&col_def.name) {
                        return ilp_value_to_column_value(field_val);
                    }
                    ColumnValue::Null
                })
                .collect();

            writer.write_row(ts, &col_values).map_err(|e| {
                io::Error::other(format!("write row: {e}"))
            })?;
        }

        writer.flush().map_err(|e| {
            io::Error::other(format!("flush: {e}"))
        })?;
    }

    Ok(())
}

/// Auto-create a table from the first ILP line, inferring the schema.
/// Uses the same logic as the HTTP write handler.
fn auto_create_table(
    db_root: &Path,
    table_name: &str,
    first_line: &IlpLine,
) -> Result<(), exchange_common::error::ExchangeDbError> {
    let mut builder = TableBuilder::new(table_name);

    // Always add a timestamp column first.
    builder = builder.column("timestamp", ColumnType::Timestamp);

    // Add tag columns as VARCHAR (not Symbol, since Symbol requires an
    // integer symbol-map ID and ILP sends raw strings).
    for tag_name in first_line.tags.keys() {
        builder = builder.column(tag_name, ColumnType::Varchar);
    }

    // Add field columns with types inferred from the first line.
    for (field_name, field_value) in &first_line.fields {
        let col_type = match field_value {
            IlpValue::Integer(_) => ColumnType::I64,
            IlpValue::Float(_) => ColumnType::F64,
            IlpValue::String(_) => ColumnType::Varchar,
            IlpValue::Boolean(_) => ColumnType::Boolean,
            IlpValue::Timestamp(_) => ColumnType::Timestamp,
            IlpValue::Symbol(_) => ColumnType::Symbol,
            IlpValue::Long256(_) => ColumnType::Long256,
        };
        builder = builder.column(field_name, col_type);
    }

    builder = builder.timestamp("timestamp");
    builder.build(db_root)?;
    Ok(())
}

/// Convert an ILP field value to a `ColumnValue`.
fn ilp_value_to_column_value(v: &IlpValue) -> ColumnValue<'_> {
    match v {
        IlpValue::Integer(n) => ColumnValue::I64(*n),
        IlpValue::Float(n) => ColumnValue::F64(*n),
        IlpValue::String(s) => ColumnValue::Str(s.as_str()),
        IlpValue::Boolean(b) => ColumnValue::I64(if *b { 1 } else { 0 }),
        IlpValue::Timestamp(n) => ColumnValue::I64(*n),
        IlpValue::Symbol(s) => ColumnValue::Str(s.as_str()),
        IlpValue::Long256(s) => ColumnValue::Str(s.as_str()),
    }
}

/// Convert an ILP field value to an `OwnedColumnValue` for WAL writing.
fn ilp_value_to_owned(v: &IlpValue) -> OwnedColumnValue {
    match v {
        IlpValue::Integer(n) => OwnedColumnValue::I64(*n),
        IlpValue::Float(n) => OwnedColumnValue::F64(*n),
        IlpValue::String(s) => OwnedColumnValue::Varchar(s.clone()),
        IlpValue::Boolean(b) => OwnedColumnValue::I64(if *b { 1 } else { 0 }),
        IlpValue::Timestamp(n) => OwnedColumnValue::Timestamp(*n),
        IlpValue::Symbol(s) => OwnedColumnValue::Varchar(s.clone()),
        IlpValue::Long256(s) => OwnedColumnValue::Varchar(s.clone()),
    }
}
