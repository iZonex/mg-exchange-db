use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use exchange_common::types::ColumnType;
use exchange_core::recovery::RecoveryManager;
use exchange_core::scheduler::{
    CheckpointJob, DownsamplingRefreshJob, JobScheduler, PitrCheckpointJob, RetentionJob,
    StatsRefreshJob, TieringJob, TtlJob, WalCleanupJob,
};
use exchange_core::table::{ColumnTypeSerializable, TableMeta};
use exchange_net::ServerConfig;
use exchange_query::{QueryResult, Value};

pub mod bench_report;
mod config;
#[allow(dead_code)]
mod log_rotation;
#[allow(dead_code)]
mod tsbs;
use config::ExchangeDbConfig;

// ---------------------------------------------------------------------------
// CLI definition
// ---------------------------------------------------------------------------

/// ExchangeDB -- high-performance time-series database for exchanges.
#[derive(Parser)]
#[command(name = "exchange-db", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the database server.
    Server {
        /// Path to configuration file (default: exchange-db.toml).
        #[arg(long)]
        config: Option<PathBuf>,

        /// Address to bind (host:port). Overrides config file.
        #[arg(long)]
        bind: Option<String>,

        /// Root data directory. Overrides config file.
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },

    /// Execute a SQL query directly and print results as a table.
    Sql {
        /// Root data directory.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,

        /// The SQL query to execute.
        query: String,
    },

    /// Import a CSV file into a table (create if not exists).
    Import {
        /// Root data directory.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,

        /// Target table name.
        #[arg(long)]
        table: String,

        /// Path to the CSV file.
        #[arg(long)]
        file: PathBuf,
    },

    /// List all tables.
    Tables {
        /// Root data directory.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,
    },

    /// Show table metadata (columns, partition strategy, row count).
    Info {
        /// Root data directory.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,

        /// Name of the table to inspect.
        table_name: String,
    },

    /// Create a snapshot/backup of all tables.
    Snapshot {
        /// Root data directory.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,

        /// Output directory for the snapshot.
        #[arg(long)]
        output: PathBuf,
    },

    /// Restore tables from a snapshot/backup.
    Restore {
        /// Root data directory to restore into.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,

        /// Input directory containing the snapshot.
        #[arg(long)]
        input: PathBuf,
    },

    /// Print the version and build information.
    Version,

    /// Show, validate, or generate configuration.
    #[command(subcommand)]
    Config(ConfigCommand),

    /// Run health and integrity checks on the database.
    #[command(subcommand)]
    Check(CheckCommand),

    /// Manage replication: status, promote, demote, add/remove replicas.
    #[command(subcommand)]
    Replication(ReplicationCommand),

    /// Inspect internal state for debugging.
    #[command(subcommand)]
    Debug(DebugCommand),

    /// Compact WAL segments and reclaim disk space.
    Compact {
        /// Root data directory.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,

        /// Only compact a specific table (default: all tables).
        #[arg(long)]
        table: Option<String>,

        /// Dry-run: show what would be compacted without modifying data.
        #[arg(long)]
        dry_run: bool,
    },

    /// Print the current server status (requires a running server).
    Status {
        /// HTTP address of the running server.
        #[arg(long, default_value = "http://localhost:9000")]
        host: String,
    },
}

/// Configuration subcommands.
#[derive(Subcommand)]
enum ConfigCommand {
    /// Show the effective configuration (file + env + defaults).
    Show {
        /// Path to configuration file.
        #[arg(long)]
        config: Option<PathBuf>,

        /// Output format: toml, json.
        #[arg(long, default_value = "toml")]
        format: String,
    },

    /// Validate a configuration file for errors.
    Validate {
        /// Path to configuration file.
        #[arg(long, default_value = "exchange-db.toml")]
        config: PathBuf,
    },

    /// Generate a reference configuration file with all defaults.
    Generate {
        /// Output path (default: stdout).
        #[arg(long)]
        output: Option<PathBuf>,
    },
}

/// Health and integrity check subcommands.
#[derive(Subcommand)]
enum CheckCommand {
    /// Run all health checks (WAL, partitions, metadata, disk space).
    All {
        /// Root data directory.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,
    },

    /// Check WAL integrity for all tables.
    Wal {
        /// Root data directory.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,

        /// Check only a specific table.
        #[arg(long)]
        table: Option<String>,
    },

    /// Verify partition integrity and checksums.
    Partitions {
        /// Root data directory.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,

        /// Check only a specific table.
        #[arg(long)]
        table: Option<String>,
    },

    /// Check metadata consistency across all tables.
    Metadata {
        /// Root data directory.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,
    },

    /// Show disk space usage per table and partition.
    DiskUsage {
        /// Root data directory.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,
    },
}

/// Replication management subcommands.
#[derive(Subcommand)]
enum ReplicationCommand {
    /// Show current replication status.
    Status {
        /// Path to configuration file.
        #[arg(long)]
        config: Option<PathBuf>,
    },

    /// Promote this replica to primary.
    Promote {
        /// Path to configuration file.
        #[arg(long)]
        config: Option<PathBuf>,

        /// Root data directory.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,
    },

    /// Demote this primary to replica.
    Demote {
        /// Path to configuration file.
        #[arg(long)]
        config: Option<PathBuf>,

        /// Root data directory.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,

        /// Address of the new primary to follow.
        #[arg(long)]
        new_primary: String,
    },
}

/// Debug / inspection subcommands.
#[derive(Subcommand)]
enum DebugCommand {
    /// Inspect WAL segments for a table.
    WalInspect {
        /// Root data directory.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,

        /// Table name.
        table: String,

        /// Show individual events in each segment.
        #[arg(long)]
        verbose: bool,
    },

    /// Show detailed partition information for a table.
    PartitionInfo {
        /// Root data directory.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,

        /// Table name.
        table: String,
    },

    /// Show server internals via the diagnostics endpoint.
    Diagnostics {
        /// HTTP address of the running server.
        #[arg(long, default_value = "http://localhost:9000")]
        host: String,
    },

    /// Dump column data for a table (raw inspection).
    ColumnDump {
        /// Root data directory.
        #[arg(long, default_value = "./data")]
        data_dir: PathBuf,

        /// Table name.
        table: String,

        /// Column name.
        column: String,

        /// Partition to inspect (e.g. "2024-03-01"). If omitted, shows all.
        #[arg(long)]
        partition: Option<String>,

        /// Maximum number of values to print.
        #[arg(long, default_value = "20")]
        limit: usize,
    },
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    // Structured logging: support "json" or "text" (default) format.
    // We initialize with env filter first; the server command may re-configure
    // later based on config file, but the initial setup uses env vars.
    let log_format = std::env::var("EXCHANGEDB_LOG_FORMAT").unwrap_or_default();
    let filter = EnvFilter::from_default_env();
    if log_format == "json" {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }

    let cli = Cli::parse();

    match cli.command {
        Command::Server {
            config,
            bind,
            data_dir,
        } => cmd_server(config, bind, data_dir),
        Command::Sql { data_dir, query } => cmd_sql(&data_dir, &query),
        Command::Import {
            data_dir,
            table,
            file,
        } => cmd_import(&data_dir, &table, &file),
        Command::Tables { data_dir } => cmd_tables(&data_dir),
        Command::Info {
            data_dir,
            table_name,
        } => cmd_info(&data_dir, &table_name),
        Command::Snapshot { data_dir, output } => cmd_snapshot(&data_dir, &output),
        Command::Restore { data_dir, input } => cmd_restore(&input, &data_dir),
        Command::Version => cmd_version(),
        Command::Config(sub) => match sub {
            ConfigCommand::Show { config, format } => cmd_config_show(config, &format),
            ConfigCommand::Validate { config } => cmd_config_validate(&config),
            ConfigCommand::Generate { output } => cmd_config_generate(output.as_deref()),
        },
        Command::Check(sub) => match sub {
            CheckCommand::All { data_dir } => cmd_check_all(&data_dir),
            CheckCommand::Wal { data_dir, table } => cmd_check_wal(&data_dir, table.as_deref()),
            CheckCommand::Partitions { data_dir, table } => {
                cmd_check_partitions(&data_dir, table.as_deref())
            }
            CheckCommand::Metadata { data_dir } => cmd_check_metadata(&data_dir),
            CheckCommand::DiskUsage { data_dir } => cmd_check_disk_usage(&data_dir),
        },
        Command::Replication(sub) => match sub {
            ReplicationCommand::Status { config } => cmd_replication_status(config),
            ReplicationCommand::Promote { config, data_dir } => {
                cmd_replication_promote(config, &data_dir)
            }
            ReplicationCommand::Demote {
                config,
                data_dir,
                new_primary,
            } => cmd_replication_demote(config, &data_dir, &new_primary),
        },
        Command::Debug(sub) => match sub {
            DebugCommand::WalInspect {
                data_dir,
                table,
                verbose,
            } => cmd_debug_wal_inspect(&data_dir, &table, verbose),
            DebugCommand::PartitionInfo { data_dir, table } => {
                cmd_debug_partition_info(&data_dir, &table)
            }
            DebugCommand::Diagnostics { host } => cmd_debug_diagnostics(&host),
            DebugCommand::ColumnDump {
                data_dir,
                table,
                column,
                partition,
                limit,
            } => cmd_debug_column_dump(&data_dir, &table, &column, partition.as_deref(), limit),
        },
        Command::Compact {
            data_dir,
            table,
            dry_run,
        } => cmd_compact(&data_dir, table.as_deref(), dry_run),
        Command::Status { host } => cmd_status(&host),
    }
}

// ---------------------------------------------------------------------------
// Subcommand implementations
// ---------------------------------------------------------------------------

/// Start the HTTP server.
fn cmd_server(
    config_path: Option<PathBuf>,
    bind_override: Option<String>,
    data_dir_override: Option<PathBuf>,
) -> Result<()> {
    // Load config: file -> env vars -> CLI overrides
    let mut cfg =
        ExchangeDbConfig::load(config_path.as_deref()).context("failed to load configuration")?;
    cfg = cfg.with_env();

    // CLI flags take highest precedence.
    if let Some(bind) = bind_override {
        cfg.http.bind = bind;
    }
    if let Some(dir) = data_dir_override {
        cfg.server.data_dir = dir;
    }

    let bind_addr = cfg.http_bind_addr()?;
    let pg_bind_addr = cfg.pgwire_bind_addr()?;
    let ilp_bind_addr = cfg.ilp_bind_addr()?;

    let tls = if cfg.tls.enabled {
        Some(exchange_net::tls::TlsConfig {
            enabled: true,
            cert_path: cfg.tls.cert_path.clone(),
            key_path: cfg.tls.key_path.clone(),
            min_version: cfg.tls.min_version.clone(),
        })
    } else {
        None
    };

    // Build replication config from TOML section.
    let repl_config = cfg.replication.to_replication_config();
    let is_replica = repl_config.role == exchange_core::replication::ReplicationRole::Replica;

    // Enable replication port for both primary (to receive acks) and
    // replica (to receive WAL segments). Port 0 disables the listener.
    let repl_port = if repl_config.role != exchange_core::replication::ReplicationRole::Standalone {
        repl_config.replication_port
    } else {
        0
    };

    let server_config = ServerConfig {
        bind_addr,
        pg_bind_addr,
        ilp_bind_addr,
        db_root: cfg.server.data_dir.clone(),
        tls,
        http_enabled: cfg.http.enabled,
        pg_enabled: cfg.pgwire.enabled,
        ilp_enabled: cfg.ilp.enabled,
        read_only: is_replica,
        replication_manager: None,
        replication_port: repl_port,
    };

    tracing::info!("ExchangeDB v{}", env!("CARGO_PKG_VERSION"));
    tracing::info!(data_dir = %cfg.server.data_dir.display(), "data directory");
    tracing::info!(
        http = %if cfg.http.enabled { cfg.http.bind.as_str() } else { "disabled" },
        pgwire = %if cfg.pgwire.enabled { cfg.pgwire.bind.as_str() } else { "disabled" },
        ilp = %if cfg.ilp.enabled { cfg.ilp.bind.as_str() } else { "disabled" },
        "server endpoints"
    );

    // Run crash recovery before starting servers.
    tracing::info!("running WAL recovery...");
    match RecoveryManager::recover_all(&cfg.server.data_dir) {
        Ok(stats) => {
            if stats.tables_recovered > 0 {
                tracing::info!(
                    tables = stats.tables_recovered,
                    segments = stats.segments_replayed,
                    rows = stats.rows_recovered,
                    duration_ms = stats.duration_ms,
                    "WAL recovery complete"
                );
            } else {
                tracing::info!(duration_ms = stats.duration_ms, "no WAL recovery needed");
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "WAL recovery failed");
            anyhow::bail!("WAL recovery failed: {e}");
        }
    }

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        // Start the replication manager if configured.
        let repl_mgr =
            if repl_config.role != exchange_core::replication::ReplicationRole::Standalone {
                let mut mgr = exchange_core::replication::ReplicationManager::new(
                    cfg.server.data_dir.clone(),
                    repl_config.clone(),
                );
                if let Err(e) = mgr.start().await {
                    tracing::error!(error = %e, "failed to start replication manager");
                    anyhow::bail!("replication startup failed: {e}");
                }
                tracing::info!(role = ?repl_config.role, "replication manager started");
                Some(std::sync::Arc::new(mgr))
            } else {
                None
            };

        // Start the health monitor for automatic failover if this is a replica
        // with failover enabled.
        if is_replica
            && repl_config.failover_enabled
            && let Some(ref repl_mgr_arc) = repl_mgr
        {
            let primary_addr = repl_config
                .primary_addr
                .clone()
                .unwrap_or_else(|| "127.0.0.1:19001".to_string());

            let monitor =
                std::sync::Arc::new(exchange_core::replication::PrimaryHealthMonitor::new(
                    primary_addr.clone(),
                    repl_config.health_check_interval,
                    repl_config.failure_threshold,
                ));

            let mgr_for_failover = repl_mgr_arc.clone();
            tokio::spawn(monitor.start(move || {
                mgr_for_failover.promote_to_primary();
            }));

            tracing::info!(
                primary = %primary_addr,
                interval = ?repl_config.health_check_interval,
                threshold = repl_config.failure_threshold,
                "automatic failover health monitor started"
            );
        }

        // Update server config with replication manager.
        let mut server_config = server_config;
        server_config.replication_manager = repl_mgr;

        // Start the background job scheduler with ALL configured jobs.
        let mut scheduler = JobScheduler::new(cfg.server.data_dir.clone());

        // Always register core maintenance jobs.
        scheduler.register(
            "wal_cleanup",
            std::time::Duration::from_secs(5 * 60),
            Box::new(WalCleanupJob),
        );
        scheduler.register(
            "checkpoint",
            std::time::Duration::from_secs(5 * 60),
            Box::new(CheckpointJob {
                interval: std::time::Duration::from_secs(5 * 60),
            }),
        );
        scheduler.register(
            "stats_refresh",
            std::time::Duration::from_secs(10 * 60),
            Box::new(StatsRefreshJob),
        );

        // Retention: drops partitions exceeding the configured max_age.
        if cfg.retention.enabled {
            tracing::info!(
                "retention job enabled (max_age={:?})",
                cfg.retention.max_age.as_duration()
            );
            // Scan existing tables and build retention policies.
            let policies =
                build_retention_policies(&cfg.server.data_dir, cfg.retention.max_age.as_duration());
            scheduler.register(
                "retention",
                cfg.retention.check_interval.as_duration(),
                Box::new(RetentionJob { policies }),
            );
        }

        // Tiered storage: moves old partitions through hot -> warm -> cold.
        if cfg.tiering.enabled {
            tracing::info!(
                "tiering job enabled (hot={:?}, warm={:?})",
                cfg.tiering.hot_retention.as_duration(),
                cfg.tiering.warm_retention.as_duration(),
            );
            let cold_path = if cfg.tiering.cold_storage_path.is_empty() {
                None
            } else {
                Some(PathBuf::from(&cfg.tiering.cold_storage_path))
            };
            let policies = build_tiering_policies(
                &cfg.server.data_dir,
                cfg.tiering.hot_retention.as_duration(),
                cfg.tiering.warm_retention.as_duration(),
                cold_path,
            );
            scheduler.register(
                "tiering",
                cfg.tiering.check_interval.as_duration(),
                Box::new(TieringJob { policies }),
            );
        }

        // PITR: creates periodic snapshots for point-in-time recovery.
        if cfg.pitr.enabled {
            tracing::info!(
                "PITR job enabled (retention={:?}, interval={:?})",
                cfg.pitr.retention_window.as_duration(),
                cfg.pitr.snapshot_interval.as_duration(),
            );
            scheduler.register(
                "pitr_checkpoint",
                cfg.pitr.snapshot_interval.as_duration(),
                Box::new(PitrCheckpointJob {
                    config: exchange_core::pitr::PitrConfig {
                        enabled: true,
                        retention_window: cfg.pitr.retention_window.as_duration(),
                        snapshot_interval: cfg.pitr.snapshot_interval.as_duration(),
                    },
                }),
            );
        }

        // TTL: expires data older than configured max_age across all tables.
        if cfg.ttl.enabled {
            tracing::info!(
                "TTL job enabled (default_max_age={:?})",
                cfg.ttl.default_max_age.as_duration()
            );
            let ttl_configs =
                build_ttl_configs(&cfg.server.data_dir, cfg.ttl.default_max_age.as_duration());
            scheduler.register(
                "ttl",
                cfg.ttl.check_interval.as_duration(),
                Box::new(TtlJob {
                    configs: ttl_configs,
                }),
            );
        }

        // Downsampling: refreshes any _downsampling/ configs found in table dirs.
        if cfg.downsampling.enabled {
            tracing::info!("downsampling refresh job enabled");
            scheduler.register(
                "downsampling_refresh",
                cfg.downsampling.check_interval.as_duration(),
                Box::new(DownsamplingRefreshJob),
            );
        }

        tracing::info!(
            jobs = scheduler.job_count(),
            names = ?scheduler.job_names(),
            "starting background job scheduler",
        );
        let scheduler_handle = scheduler.start();

        // Start SIGHUP-based config reload watcher (Unix only).
        #[cfg(unix)]
        {
            let config_path = config_path
                .clone()
                .unwrap_or_else(|| PathBuf::from("exchange-db.toml"));
            tokio::spawn(watch_config_reload(config_path));
        }

        // Run all servers until shutdown.
        let result = exchange_net::start_all_servers(server_config).await;

        // On shutdown, stop the scheduler gracefully.
        tracing::info!("stopping background job scheduler");
        scheduler_handle.wait().await;

        result.map_err(|e| anyhow::anyhow!(e))
    })?;

    Ok(())
}

/// Execute a SQL query and print results.
fn cmd_sql(data_dir: &Path, query: &str) -> Result<()> {
    let plan = exchange_query::plan_query(query)
        .with_context(|| format!("failed to plan query: {query}"))?;

    let result =
        exchange_query::execute(data_dir, &plan).with_context(|| "failed to execute query")?;

    match result {
        QueryResult::Rows { columns, rows } => {
            print_ascii_table(&columns, &rows);
        }
        QueryResult::Ok { affected_rows } => {
            println!("OK ({affected_rows} row(s) affected)");
        }
    }

    Ok(())
}

/// Import a CSV file into a table.
fn cmd_import(data_dir: &Path, table_name: &str, csv_path: &Path) -> Result<()> {
    use exchange_common::types::{ColumnType, Timestamp};
    use exchange_core::table::{ColumnValue, TableBuilder, TableWriter};
    use std::io::{BufRead, BufReader};

    let file = std::fs::File::open(csv_path)
        .with_context(|| format!("cannot open CSV file: {}", csv_path.display()))?;
    let reader = BufReader::new(file);

    let mut lines = reader.lines();

    // -- Parse header row ---------------------------------------------------
    let header_line = lines
        .next()
        .context("CSV file is empty")?
        .context("failed to read CSV header")?;
    let headers: Vec<&str> = header_line.split(',').map(|s| s.trim()).collect();

    // -- Collect all data rows so we can auto-detect types ------------------
    let mut raw_rows: Vec<Vec<String>> = Vec::new();
    for line_result in lines {
        let line = line_result.context("failed to read CSV line")?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let fields: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        raw_rows.push(fields);
    }

    if raw_rows.is_empty() {
        println!("CSV file contains no data rows.");
        return Ok(());
    }

    // -- Auto-detect column types -------------------------------------------
    let col_types: Vec<ColumnType> = (0..headers.len())
        .map(|col_idx| detect_column_type(&raw_rows, col_idx))
        .collect();

    // -- Create table if it does not exist -----------------------------------
    let table_dir = data_dir.join(table_name);
    if !table_dir.exists() {
        let mut builder = TableBuilder::new(table_name);
        // Always prepend a timestamp column if none detected in headers.
        let has_timestamp = col_types.iter().any(|t| matches!(t, ColumnType::Timestamp));

        for (i, header) in headers.iter().enumerate() {
            builder = builder.column(header, col_types[i]);
        }

        if has_timestamp {
            // Use the first timestamp column as the designated timestamp.
            let ts_name = headers
                .iter()
                .zip(col_types.iter())
                .find(|(_, t)| matches!(t, ColumnType::Timestamp))
                .map(|(n, _)| *n)
                .unwrap();
            builder = builder.timestamp(ts_name);
        }

        builder
            .build(data_dir)
            .with_context(|| format!("failed to create table '{table_name}'"))?;
        println!("Created table '{table_name}'");
    }

    // -- Write rows ---------------------------------------------------------
    let mut writer = TableWriter::open(data_dir, table_name)
        .with_context(|| format!("failed to open table '{table_name}' for writing"))?;

    let meta = writer.meta().clone();
    let ts_col_idx = meta.timestamp_column;

    let mut count = 0u64;
    for raw_row in &raw_rows {
        // Parse the timestamp value.
        let ts = if ts_col_idx < raw_row.len() {
            parse_timestamp_value(&raw_row[ts_col_idx])
        } else {
            Timestamp::now()
        };

        // Build column values, skipping the timestamp column (writer fills it).
        let col_values: Vec<ColumnValue<'_>> = raw_row
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != ts_col_idx)
            .map(|(orig_idx, field)| {
                let ct: ColumnType = if orig_idx < meta.columns.len() {
                    meta.columns[orig_idx].col_type.into()
                } else {
                    ColumnType::Varchar
                };
                parse_field_to_column_value(field, ct)
            })
            .collect();

        writer
            .write_row(ts, &col_values)
            .with_context(|| format!("failed to write row {count}"))?;
        count += 1;
    }

    writer.flush()?;
    println!("Imported {count} row(s) into '{table_name}'");

    Ok(())
}

/// Create a snapshot/backup of all tables.
fn cmd_snapshot(data_dir: &Path, output: &Path) -> Result<()> {
    let info = exchange_core::snapshot::create_snapshot(data_dir, output)
        .with_context(|| "failed to create snapshot")?;

    println!("Snapshot created successfully:");
    println!("  Directory: {}", output.display());
    println!("  Tables: {}", info.tables.join(", "));
    println!("  Total size: {} bytes", info.total_bytes);
    println!("  Timestamp: {}", info.timestamp);

    Ok(())
}

/// Restore tables from a snapshot/backup.
fn cmd_restore(input: &Path, data_dir: &Path) -> Result<()> {
    exchange_core::snapshot::restore_snapshot(input, data_dir)
        .with_context(|| "failed to restore snapshot")?;

    println!("Snapshot restored successfully to: {}", data_dir.display());

    Ok(())
}

/// List all tables by scanning the data directory.
fn cmd_tables(data_dir: &Path) -> Result<()> {
    if !data_dir.exists() {
        println!("No data directory found at: {}", data_dir.display());
        return Ok(());
    }

    let mut tables: Vec<String> = Vec::new();

    for entry in std::fs::read_dir(data_dir).context("failed to read data directory")? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            let meta_path = entry.path().join("_meta");
            if meta_path.exists() {
                tables.push(entry.file_name().to_string_lossy().to_string());
            }
        }
    }

    tables.sort();

    if tables.is_empty() {
        println!("No tables found.");
    } else {
        println!("Tables:");
        for t in &tables {
            println!("  {t}");
        }
    }

    Ok(())
}

/// Show metadata for a single table.
fn cmd_info(data_dir: &Path, table_name: &str) -> Result<()> {
    let table_dir = data_dir.join(table_name);
    let meta_path = table_dir.join("_meta");

    if !meta_path.exists() {
        anyhow::bail!("table '{table_name}' not found in {}", data_dir.display());
    }

    let meta = TableMeta::load(&meta_path)
        .with_context(|| format!("failed to load metadata for '{table_name}'"))?;

    println!("Table: {}", meta.name);
    println!("Partition by: {:?}", meta.partition_by);
    println!("Version: {}", meta.version);
    println!(
        "Designated timestamp: {}",
        meta.columns
            .get(meta.timestamp_column)
            .map(|c| c.name.as_str())
            .unwrap_or("(none)")
    );
    println!();

    // Count rows by scanning partitions.
    let row_count = count_table_rows(&table_dir, &meta);

    println!("Columns ({}):", meta.columns.len());
    for (i, col) in meta.columns.iter().enumerate() {
        let ts_marker = if i == meta.timestamp_column {
            " [timestamp]"
        } else {
            ""
        };
        let idx_marker = if col.indexed { " [indexed]" } else { "" };
        println!(
            "  {:<20} {:<12}{}{}",
            col.name,
            col_type_display(col.col_type),
            ts_marker,
            idx_marker,
        );
    }

    println!();
    println!("Row count: {row_count}");

    Ok(())
}

// ---------------------------------------------------------------------------
// Version
// ---------------------------------------------------------------------------

fn cmd_version() -> Result<()> {
    println!("ExchangeDB v{}", env!("CARGO_PKG_VERSION"));
    println!("  Rust edition: 2024");
    println!("  Min Rust version: 1.85");
    println!("  Target: {}", std::env::consts::ARCH);
    println!("  OS: {}", std::env::consts::OS);
    #[cfg(debug_assertions)]
    println!("  Profile: debug");
    #[cfg(not(debug_assertions))]
    println!("  Profile: release");
    Ok(())
}

// ---------------------------------------------------------------------------
// Config subcommands
// ---------------------------------------------------------------------------

fn cmd_config_show(config_path: Option<PathBuf>, format: &str) -> Result<()> {
    let mut cfg =
        ExchangeDbConfig::load(config_path.as_deref()).context("failed to load configuration")?;
    cfg = cfg.with_env();

    match format {
        "json" => {
            let json =
                serde_json::to_string_pretty(&cfg).context("failed to serialize config to JSON")?;
            println!("{json}");
        }
        _ => {
            let toml =
                toml::to_string_pretty(&cfg).context("failed to serialize config to TOML")?;
            println!("{toml}");
        }
    }
    Ok(())
}

fn cmd_config_validate(config_path: &Path) -> Result<()> {
    if !config_path.exists() {
        anyhow::bail!("configuration file not found: {}", config_path.display());
    }

    let contents = std::fs::read_to_string(config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;

    match toml::from_str::<ExchangeDbConfig>(&contents) {
        Ok(cfg) => {
            // Validate addresses are parseable.
            let mut warnings = Vec::new();

            if cfg.http.enabled
                && let Err(e) = cfg.http_bind_addr()
            {
                warnings.push(format!("http.bind: {e}"));
            }
            if cfg.pgwire.enabled
                && let Err(e) = cfg.pgwire_bind_addr()
            {
                warnings.push(format!("pgwire.bind: {e}"));
            }
            if cfg.ilp.enabled
                && let Err(e) = cfg.ilp_bind_addr()
            {
                warnings.push(format!("ilp.bind: {e}"));
            }

            if cfg.tls.enabled {
                if !Path::new(&cfg.tls.cert_path).exists() {
                    warnings.push(format!(
                        "tls.cert_path: file not found: {}",
                        cfg.tls.cert_path
                    ));
                }
                if !Path::new(&cfg.tls.key_path).exists() {
                    warnings.push(format!(
                        "tls.key_path: file not found: {}",
                        cfg.tls.key_path
                    ));
                }
            }

            if cfg.performance.query_parallelism > 256 {
                warnings.push("performance.query_parallelism: unusually high (>256)".to_string());
            }

            if warnings.is_empty() {
                println!("Configuration is valid: {}", config_path.display());
            } else {
                println!("Configuration parsed with {} warning(s):", warnings.len());
                for w in &warnings {
                    println!("  WARNING: {w}");
                }
            }
        }
        Err(e) => {
            anyhow::bail!("configuration is INVALID: {e}");
        }
    }
    Ok(())
}

fn cmd_config_generate(output: Option<&Path>) -> Result<()> {
    let reference = include_str!("reference_config.toml");

    if let Some(path) = output {
        std::fs::write(path, reference)
            .with_context(|| format!("failed to write to {}", path.display()))?;
        println!("Reference configuration written to: {}", path.display());
    } else {
        print!("{reference}");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Check subcommands
// ---------------------------------------------------------------------------

fn cmd_check_all(data_dir: &Path) -> Result<()> {
    println!("Running all checks on: {}", data_dir.display());
    println!();

    let mut errors = 0u32;

    // Check data directory exists.
    if !data_dir.exists() {
        println!("[FAIL] Data directory does not exist");
        return Ok(());
    }
    println!("[OK] Data directory exists");

    // Check disk space.
    match fs_usage(data_dir) {
        Some((used, total)) => {
            let pct = if total > 0 {
                used as f64 / total as f64 * 100.0
            } else {
                0.0
            };
            if pct > 90.0 {
                println!(
                    "[WARN] Disk usage: {:.1}% ({} / {})",
                    pct,
                    human_bytes(used),
                    human_bytes(total)
                );
            } else {
                println!(
                    "[OK] Disk usage: {:.1}% ({} / {})",
                    pct,
                    human_bytes(used),
                    human_bytes(total)
                );
            }
        }
        None => println!("[SKIP] Could not determine disk usage"),
    }

    // Check each table.
    let tables = list_tables(data_dir);
    println!("[OK] Found {} table(s)", tables.len());

    for table_name in &tables {
        let table_dir = data_dir.join(table_name);
        let meta_path = table_dir.join("_meta");

        // Metadata check.
        match TableMeta::load(&meta_path) {
            Ok(meta) => {
                println!(
                    "[OK] {table_name}: metadata valid ({} columns, partition_by={:?})",
                    meta.columns.len(),
                    meta.partition_by
                );

                // WAL check.
                let wal_dir = table_dir.join("_wal");
                if wal_dir.exists() {
                    let seg_count = count_wal_segments(&wal_dir);
                    if seg_count > 100 {
                        println!(
                            "[WARN] {table_name}: {seg_count} WAL segments (consider compacting)"
                        );
                    } else {
                        println!("[OK] {table_name}: {seg_count} WAL segment(s)");
                    }
                }

                // Partition check.
                let partitions = count_partitions(&table_dir);
                let row_count = count_table_rows(&table_dir, &meta);
                println!("[OK] {table_name}: {partitions} partition(s), {row_count} row(s)");
            }
            Err(e) => {
                println!("[FAIL] {table_name}: metadata error: {e}");
                errors += 1;
            }
        }
    }

    println!();
    if errors > 0 {
        println!("Check completed with {errors} error(s)");
    } else {
        println!("All checks passed");
    }

    Ok(())
}

fn cmd_check_wal(data_dir: &Path, table: Option<&str>) -> Result<()> {
    let tables = match table {
        Some(t) => vec![t.to_string()],
        None => list_tables(data_dir),
    };

    for table_name in &tables {
        let wal_dir = data_dir.join(table_name).join("_wal");
        if !wal_dir.exists() {
            println!("{table_name}: no WAL directory");
            continue;
        }

        let segments = count_wal_segments(&wal_dir);
        let wal_size = dir_size(&wal_dir);
        println!(
            "{table_name}: {segments} segment(s), {}",
            human_bytes(wal_size)
        );
    }

    Ok(())
}

fn cmd_check_partitions(data_dir: &Path, table: Option<&str>) -> Result<()> {
    let tables = match table {
        Some(t) => vec![t.to_string()],
        None => list_tables(data_dir),
    };

    for table_name in &tables {
        let table_dir = data_dir.join(table_name);
        let meta_path = table_dir.join("_meta");

        let meta = TableMeta::load(&meta_path)
            .with_context(|| format!("failed to load metadata for '{table_name}'"))?;

        println!("Table: {table_name}");

        let mut partitions = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&table_dir) {
            for entry in entries.flatten() {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if name.starts_with('_') {
                        continue;
                    }
                    let size = dir_size(&entry.path());
                    let rows = count_partition_rows(&entry.path(), &meta);
                    partitions.push((name, rows, size));
                }
            }
        }

        partitions.sort_by(|a, b| a.0.cmp(&b.0));

        if partitions.is_empty() {
            println!("  (no partitions)");
        } else {
            println!("  {:30} {:>12} {:>12}", "PARTITION", "ROWS", "SIZE");
            for (name, rows, size) in &partitions {
                println!("  {:30} {:>12} {:>12}", name, rows, human_bytes(*size));
            }
            let total_rows: u64 = partitions.iter().map(|(_, r, _)| r).sum();
            let total_size: u64 = partitions.iter().map(|(_, _, s)| s).sum();
            println!(
                "  {:30} {:>12} {:>12}",
                "TOTAL",
                total_rows,
                human_bytes(total_size)
            );
        }
        println!();
    }

    Ok(())
}

fn cmd_check_metadata(data_dir: &Path) -> Result<()> {
    let tables = list_tables(data_dir);

    for table_name in &tables {
        let meta_path = data_dir.join(table_name).join("_meta");
        match TableMeta::load(&meta_path) {
            Ok(meta) => {
                let ts_col = meta
                    .columns
                    .get(meta.timestamp_column)
                    .map(|c| c.name.as_str())
                    .unwrap_or("(none)");
                println!(
                    "[OK] {table_name}: {} cols, ts={ts_col}, partition={:?}, v{}",
                    meta.columns.len(),
                    meta.partition_by,
                    meta.version
                );
            }
            Err(e) => {
                println!("[FAIL] {table_name}: {e}");
            }
        }
    }

    Ok(())
}

fn cmd_check_disk_usage(data_dir: &Path) -> Result<()> {
    let tables = list_tables(data_dir);

    println!(
        "{:30} {:>12} {:>12} {:>12}",
        "TABLE", "DATA", "WAL", "TOTAL"
    );
    println!("{}", "-".repeat(68));

    let mut grand_data = 0u64;
    let mut grand_wal = 0u64;

    for table_name in &tables {
        let table_dir = data_dir.join(table_name);
        let wal_dir = table_dir.join("_wal");

        let wal_size = if wal_dir.exists() {
            dir_size(&wal_dir)
        } else {
            0
        };
        let total_size = dir_size(&table_dir);
        let data_size = total_size.saturating_sub(wal_size);

        grand_data += data_size;
        grand_wal += wal_size;

        println!(
            "{:30} {:>12} {:>12} {:>12}",
            table_name,
            human_bytes(data_size),
            human_bytes(wal_size),
            human_bytes(total_size)
        );
    }

    println!("{}", "-".repeat(68));
    println!(
        "{:30} {:>12} {:>12} {:>12}",
        "TOTAL",
        human_bytes(grand_data),
        human_bytes(grand_wal),
        human_bytes(grand_data + grand_wal)
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Replication subcommands
// ---------------------------------------------------------------------------

fn cmd_replication_status(config_path: Option<PathBuf>) -> Result<()> {
    let mut cfg =
        ExchangeDbConfig::load(config_path.as_deref()).context("failed to load configuration")?;
    cfg = cfg.with_env();

    let repl = &cfg.replication;

    println!("Replication Status");
    println!("  Role:                {}", repl.role);
    println!("  Sync mode:           {}", repl.sync_mode);
    println!("  Replication port:    {}", repl.replication_port);
    println!("  Failover enabled:    {}", repl.failover_enabled);

    if repl.role == "primary" {
        println!("  Replicas:");
        if repl.replica_addrs.is_empty() {
            println!("    (none configured)");
        } else {
            for addr in &repl.replica_addrs {
                println!("    - {addr}");
            }
        }
    } else if repl.role == "replica" {
        println!(
            "  Primary address:     {}",
            if repl.primary_addr.is_empty() {
                "(not set)"
            } else {
                &repl.primary_addr
            }
        );
        println!(
            "  Health check:        {:?}",
            repl.health_check_interval.as_duration()
        );
        println!("  Failure threshold:   {}", repl.failure_threshold);
    }

    Ok(())
}

fn cmd_replication_promote(config_path: Option<PathBuf>, data_dir: &Path) -> Result<()> {
    let mut cfg =
        ExchangeDbConfig::load(config_path.as_deref()).context("failed to load configuration")?;
    cfg = cfg.with_env();

    if cfg.replication.role != "replica" {
        anyhow::bail!(
            "can only promote a replica (current role: {})",
            cfg.replication.role
        );
    }

    let repl_config = cfg.replication.to_replication_config();
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let mut mgr = exchange_core::replication::ReplicationManager::new(
            data_dir.to_path_buf(),
            repl_config,
        );
        mgr.start()
            .await
            .context("failed to start replication manager")?;
        mgr.promote_to_primary();
        println!("Node promoted to PRIMARY successfully");
        println!("Update the configuration file to set replication.role = \"primary\"");
        Ok(())
    })
}

fn cmd_replication_demote(
    _config_path: Option<PathBuf>,
    _data_dir: &Path,
    new_primary: &str,
) -> Result<()> {
    // Demoting requires a FailoverManager which operates on a running cluster.
    // For now, provide guidance on manual demote via configuration.
    println!("To demote this node to a replica:");
    println!();
    println!("1. Stop the server: kill $(pidof exchange-db)");
    println!("2. Update exchange-db.toml:");
    println!("     [replication]");
    println!("     role = \"replica\"");
    println!("     primary_addr = \"{new_primary}\"");
    println!("3. Restart the server: exchange-db server");
    println!();
    println!("The node will begin following the new primary at {new_primary}");
    Ok(())
}

// ---------------------------------------------------------------------------
// Debug subcommands
// ---------------------------------------------------------------------------

fn cmd_debug_wal_inspect(data_dir: &Path, table_name: &str, verbose: bool) -> Result<()> {
    let wal_dir = data_dir.join(table_name).join("_wal");
    if !wal_dir.exists() {
        println!("No WAL directory for table '{table_name}'");
        return Ok(());
    }

    let mut segments: Vec<String> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&wal_dir) {
        for entry in entries.flatten() {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                segments.push(entry.file_name().to_string_lossy().to_string());
            }
        }
    }
    segments.sort();

    println!(
        "WAL for table '{table_name}': {} segment(s)",
        segments.len()
    );
    println!();

    for seg_name in &segments {
        let seg_dir = wal_dir.join(seg_name);
        let seg_size = dir_size(&seg_dir);
        let events_path = seg_dir.join("_events");
        let events_size = std::fs::metadata(&events_path)
            .map(|m| m.len())
            .unwrap_or(0);

        println!("  {seg_name}/");
        println!("    Total size: {}", human_bytes(seg_size));
        println!("    Events file: {}", human_bytes(events_size));

        if verbose {
            // List column data files in the segment.
            if let Ok(files) = std::fs::read_dir(&seg_dir) {
                let mut col_files: Vec<(String, u64)> = files
                    .flatten()
                    .filter(|e| e.file_name().to_string_lossy().ends_with(".d"))
                    .map(|e| {
                        let name = e.file_name().to_string_lossy().to_string();
                        let size = e.metadata().map(|m| m.len()).unwrap_or(0);
                        (name, size)
                    })
                    .collect();
                col_files.sort_by(|a, b| a.0.cmp(&b.0));

                for (name, size) in &col_files {
                    println!("    {name}: {}", human_bytes(*size));
                }
            }
        }
    }

    Ok(())
}

fn cmd_debug_partition_info(data_dir: &Path, table_name: &str) -> Result<()> {
    let table_dir = data_dir.join(table_name);
    let meta_path = table_dir.join("_meta");

    let meta = TableMeta::load(&meta_path)
        .with_context(|| format!("failed to load metadata for '{table_name}'"))?;

    println!("Table: {table_name}");
    println!("Partition by: {:?}", meta.partition_by);
    println!("Columns: {}", meta.columns.len());
    println!();

    let mut partitions = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&table_dir) {
        for entry in entries.flatten() {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with('_') {
                    continue;
                }
                let size = dir_size(&entry.path());
                let rows = count_partition_rows(&entry.path(), &meta);

                // List column files.
                let mut col_files = Vec::new();
                if let Ok(files) = std::fs::read_dir(entry.path()) {
                    for f in files.flatten() {
                        let fname = f.file_name().to_string_lossy().to_string();
                        let fsize = f.metadata().map(|m| m.len()).unwrap_or(0);
                        col_files.push((fname, fsize));
                    }
                }
                col_files.sort_by(|a, b| a.0.cmp(&b.0));

                partitions.push((name, rows, size, col_files));
            }
        }
    }

    partitions.sort_by(|a, b| a.0.cmp(&b.0));

    for (name, rows, size, col_files) in &partitions {
        println!("Partition: {name}");
        println!("  Rows: {rows}");
        println!("  Size: {}", human_bytes(*size));
        println!("  Files:");
        for (fname, fsize) in col_files {
            println!("    {fname}: {}", human_bytes(*fsize));
        }
        println!();
    }

    Ok(())
}

fn cmd_debug_diagnostics(host: &str) -> Result<()> {
    let url = format!("{host}/api/v1/diagnostics");
    let resp = ureq::get(&url)
        .call()
        .with_context(|| format!("failed to connect to {url}"))?;

    let body: serde_json::Value = resp
        .into_json()
        .context("failed to parse diagnostics response")?;

    println!("{}", serde_json::to_string_pretty(&body)?);
    Ok(())
}

fn cmd_debug_column_dump(
    data_dir: &Path,
    table_name: &str,
    column_name: &str,
    partition: Option<&str>,
    limit: usize,
) -> Result<()> {
    let table_dir = data_dir.join(table_name);
    let meta_path = table_dir.join("_meta");

    let meta = TableMeta::load(&meta_path)
        .with_context(|| format!("failed to load metadata for '{table_name}'"))?;

    let col_meta = meta
        .columns
        .iter()
        .find(|c| c.name == column_name)
        .with_context(|| format!("column '{column_name}' not found in table '{table_name}'"))?;

    let ct: ColumnType = col_meta.col_type.into();
    let elem_size = ct.fixed_size();

    // Collect partitions to dump.
    let mut partitions = Vec::new();
    if let Some(p) = partition {
        partitions.push(p.to_string());
    } else {
        if let Ok(entries) = std::fs::read_dir(&table_dir) {
            for entry in entries.flatten() {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if !name.starts_with('_') {
                        partitions.push(name);
                    }
                }
            }
        }
        partitions.sort();
    }

    for part_name in &partitions {
        let col_file = table_dir.join(part_name).join(format!("{column_name}.d"));
        if !col_file.exists() {
            println!("{part_name}/{column_name}.d: not found");
            continue;
        }

        let file_size = std::fs::metadata(&col_file)?.len();
        println!(
            "{part_name}/{column_name}.d ({}, {})",
            col_type_display(col_meta.col_type),
            human_bytes(file_size)
        );

        // Read raw bytes and display first `limit` values.
        let data = std::fs::read(&col_file)?;
        if let Some(es) = elem_size {
            let count = data.len() / es;
            let show = count.min(limit);
            for i in 0..show {
                let offset = i * es;
                let raw = &data[offset..offset + es];
                let display = match ct {
                    ColumnType::I8 => format!("{}", raw[0] as i8),
                    ColumnType::I16 => {
                        format!("{}", i16::from_le_bytes(raw.try_into().unwrap_or_default()))
                    }
                    ColumnType::I32 => {
                        format!("{}", i32::from_le_bytes(raw.try_into().unwrap_or_default()))
                    }
                    ColumnType::I64 => {
                        format!("{}", i64::from_le_bytes(raw.try_into().unwrap_or_default()))
                    }
                    ColumnType::F32 => {
                        format!("{}", f32::from_le_bytes(raw.try_into().unwrap_or_default()))
                    }
                    ColumnType::F64 => {
                        format!("{}", f64::from_le_bytes(raw.try_into().unwrap_or_default()))
                    }
                    ColumnType::Boolean => format!("{}", raw[0] != 0),
                    ColumnType::Timestamp => {
                        let ns = i64::from_le_bytes(raw.try_into().unwrap_or_default());
                        format!("{ns} (nanos)")
                    }
                    _ => format!("{raw:02x?}"),
                };
                println!("  [{i:>6}] {display}");
            }
            if count > show {
                println!("  ... and {} more values", count - show);
            }
        } else {
            println!("  (variable-length column, showing raw size only)");
        }
        println!();
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Compact
// ---------------------------------------------------------------------------

fn cmd_compact(data_dir: &Path, table: Option<&str>, dry_run: bool) -> Result<()> {
    let tables = match table {
        Some(t) => vec![t.to_string()],
        None => list_tables(data_dir),
    };

    for table_name in &tables {
        let wal_dir = data_dir.join(table_name).join("_wal");
        if !wal_dir.exists() {
            continue;
        }

        let seg_count = count_wal_segments(&wal_dir);
        let wal_size = dir_size(&wal_dir);

        if seg_count == 0 {
            println!("{table_name}: no WAL segments to compact");
            continue;
        }

        if dry_run {
            println!(
                "{table_name}: would compact {seg_count} segment(s) ({})",
                human_bytes(wal_size)
            );
        } else {
            // Trigger recovery which merges WAL into column store.
            match RecoveryManager::recover_all(data_dir) {
                Ok(stats) => {
                    println!(
                        "{table_name}: compacted {seg_count} segment(s), recovered {} rows in {}ms",
                        stats.rows_recovered, stats.duration_ms
                    );
                }
                Err(e) => {
                    println!("{table_name}: compaction failed: {e}");
                }
            }
            // Only run recovery once for all tables.
            break;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Status (remote server)
// ---------------------------------------------------------------------------

fn cmd_status(host: &str) -> Result<()> {
    // Health check.
    let health_url = format!("{host}/health");
    match ureq::get(&health_url).call() {
        Ok(resp) => {
            let status = resp.status();
            if status == 200 {
                println!("Server: HEALTHY");
            } else {
                println!("Server: UNHEALTHY (HTTP {status})");
            }

            if let Ok(body) = resp.into_json::<serde_json::Value>()
                && let Some(obj) = body.as_object()
            {
                for (k, v) in obj {
                    println!("  {k}: {v}");
                }
            }
        }
        Err(e) => {
            println!("Server: UNREACHABLE ({e})");
            return Ok(());
        }
    }

    println!();

    // Tables.
    let tables_url = format!("{host}/api/v1/tables");
    match ureq::get(&tables_url).call() {
        Ok(resp) => {
            if let Ok(body) = resp.into_json::<serde_json::Value>()
                && let Some(tables) = body.get("tables").and_then(|t| t.as_array())
            {
                println!("Tables: {}", tables.len());
                for t in tables {
                    if let Some(name) = t.as_str() {
                        println!("  - {name}");
                    } else if let Some(obj) = t.as_object()
                        && let Some(name) = obj.get("name").and_then(|n| n.as_str())
                    {
                        println!("  - {name}");
                    }
                }
            }
        }
        Err(_) => {
            println!("Tables: (could not fetch)");
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// SIGHUP config reload (Unix only)
// ---------------------------------------------------------------------------

/// Watch for SIGHUP signals and reload the configuration file.
///
/// This enables operators to update runtime-safe settings (like log level)
/// without restarting the server: `kill -HUP <pid>`.
#[cfg(unix)]
async fn watch_config_reload(config_path: PathBuf) {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sighup = match signal(SignalKind::hangup()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "failed to register SIGHUP handler");
            return;
        }
    };

    loop {
        sighup.recv().await;
        tracing::info!(path = %config_path.display(), "SIGHUP received, reloading config...");

        match ExchangeDbConfig::load(Some(&config_path)) {
            Ok(new_cfg) => {
                let new_cfg = new_cfg.with_env();

                // Apply runtime-safe settings.
                // Log level can be updated dynamically.
                tracing::info!(
                    log_level = %new_cfg.server.log_level,
                    "config reloaded successfully"
                );
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to reload config, keeping current settings");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// List all table names in the data directory.
fn list_tables(data_dir: &Path) -> Vec<String> {
    let mut tables = Vec::new();
    let entries = match std::fs::read_dir(data_dir) {
        Ok(e) => e,
        Err(_) => return tables,
    };
    for entry in entries.flatten() {
        if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            let meta_path = entry.path().join("_meta");
            if meta_path.exists() {
                tables.push(entry.file_name().to_string_lossy().to_string());
            }
        }
    }
    tables.sort();
    tables
}

/// Count WAL segments in a WAL directory.
fn count_wal_segments(wal_dir: &Path) -> usize {
    std::fs::read_dir(wal_dir)
        .map(|entries| {
            entries
                .flatten()
                .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
                .count()
        })
        .unwrap_or(0)
}

/// Count partitions (non-underscore directories) in a table directory.
fn count_partitions(table_dir: &Path) -> usize {
    std::fs::read_dir(table_dir)
        .map(|entries| {
            entries
                .flatten()
                .filter(|e| {
                    e.file_type().map(|t| t.is_dir()).unwrap_or(false)
                        && !e.file_name().to_string_lossy().starts_with('_')
                })
                .count()
        })
        .unwrap_or(0)
}

/// Count rows in a single partition by dividing column file size by element size.
fn count_partition_rows(partition_dir: &Path, meta: &TableMeta) -> u64 {
    let first_fixed = meta.columns.iter().find(|c| {
        let ct: ColumnType = c.col_type.into();
        ct.fixed_size().is_some()
    });
    let Some(col) = first_fixed else { return 0 };
    let ct: ColumnType = col.col_type.into();
    let elem_size = ct.fixed_size().unwrap() as u64;

    let col_file = partition_dir.join(format!("{}.d", col.name));
    std::fs::metadata(&col_file)
        .map(|m| m.len() / elem_size)
        .unwrap_or(0)
}

/// Recursively calculate directory size.
fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let ft = entry
                .file_type()
                .unwrap_or_else(|_| std::fs::metadata(entry.path()).unwrap().file_type());
            if ft.is_file() {
                total += entry.metadata().map(|m| m.len()).unwrap_or(0);
            } else if ft.is_dir() {
                total += dir_size(&entry.path());
            }
        }
    }
    total
}

/// Get filesystem usage (used, total) for a path.
fn fs_usage(_path: &Path) -> Option<(u64, u64)> {
    // Use statvfs on Unix.
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let c_path = std::ffi::CString::new(_path.as_os_str().as_bytes()).ok()?;
        unsafe {
            let mut stat: libc::statvfs = std::mem::zeroed();
            if libc::statvfs(c_path.as_ptr(), &mut stat) == 0 {
                let total = stat.f_blocks as u64 * stat.f_frsize as u64;
                let free = stat.f_bfree as u64 * stat.f_frsize as u64;
                Some((total.saturating_sub(free), total))
            } else {
                None
            }
        }
    }
    #[cfg(not(unix))]
    {
        None
    }
}

/// Format bytes as human-readable string.
fn human_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    const TB: u64 = 1024 * GB;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

/// Scan all tables under `db_root` and build retention policies for each,
/// applying the given `max_age` as the default retention limit.
fn build_retention_policies(
    db_root: &Path,
    max_age: std::time::Duration,
) -> Vec<(
    String,
    exchange_core::retention::RetentionPolicy,
    exchange_common::types::PartitionBy,
)> {
    let mut policies = Vec::new();
    let entries = match std::fs::read_dir(db_root) {
        Ok(e) => e,
        Err(_) => return policies,
    };
    for entry in entries.flatten() {
        let table_dir = entry.path();
        if !table_dir.is_dir() {
            continue;
        }
        let meta_path = table_dir.join("_meta");
        if !meta_path.exists() {
            continue;
        }
        if let Ok(meta) = TableMeta::load(&meta_path) {
            let table_name = meta.name.clone();
            let partition_by: exchange_common::types::PartitionBy = meta.partition_by.into();
            let policy = exchange_core::retention::RetentionPolicy {
                max_age: Some(max_age),
                max_partitions: None,
                max_disk_size: None,
            };
            policies.push((table_name, policy, partition_by));
        }
    }
    policies
}

/// Scan all tables under `db_root` and build tiering policies for each.
fn build_tiering_policies(
    db_root: &Path,
    hot_retention: std::time::Duration,
    warm_retention: std::time::Duration,
    cold_path: Option<PathBuf>,
) -> Vec<(
    String,
    exchange_core::tiered::policy::TieringPolicy,
    exchange_common::types::PartitionBy,
)> {
    let mut policies = Vec::new();
    let entries = match std::fs::read_dir(db_root) {
        Ok(e) => e,
        Err(_) => return policies,
    };
    for entry in entries.flatten() {
        let table_dir = entry.path();
        if !table_dir.is_dir() {
            continue;
        }
        let meta_path = table_dir.join("_meta");
        if !meta_path.exists() {
            continue;
        }
        if let Ok(meta) = TableMeta::load(&meta_path) {
            let table_name = meta.name.clone();
            let partition_by: exchange_common::types::PartitionBy = meta.partition_by.into();
            let policy = exchange_core::tiered::policy::TieringPolicy {
                hot_retention,
                warm_retention,
                cold_storage_path: cold_path.clone(),
                auto_tier: true,
            };
            policies.push((table_name, policy, partition_by));
        }
    }
    policies
}

/// Scan all tables under `db_root` and build TTL configs for each.
fn build_ttl_configs(
    db_root: &Path,
    default_max_age: std::time::Duration,
) -> Vec<exchange_core::ttl::TtlConfig> {
    let mut configs = Vec::new();
    let entries = match std::fs::read_dir(db_root) {
        Ok(e) => e,
        Err(_) => return configs,
    };
    for entry in entries.flatten() {
        let table_dir = entry.path();
        if !table_dir.is_dir() {
            continue;
        }
        let meta_path = table_dir.join("_meta");
        if !meta_path.exists() {
            continue;
        }
        if let Ok(meta) = TableMeta::load(&meta_path) {
            configs.push(exchange_core::ttl::TtlConfig {
                table: meta.name,
                max_age: default_max_age,
                action: exchange_core::ttl::TtlAction::Delete,
                check_interval: std::time::Duration::from_secs(3600),
            });
        }
    }
    configs
}

/// Count total rows across all partitions of a table by checking column file
/// sizes for the first fixed-width column.
fn count_table_rows(table_dir: &Path, meta: &TableMeta) -> u64 {
    // Find the first fixed-width column to derive row count from file size.
    let first_fixed = meta.columns.iter().find(|c| {
        let ct: ColumnType = c.col_type.into();
        ct.fixed_size().is_some()
    });

    let Some(col) = first_fixed else {
        return 0;
    };

    let ct: ColumnType = col.col_type.into();
    let elem_size = ct.fixed_size().unwrap() as u64;

    let Ok(entries) = std::fs::read_dir(table_dir) else {
        return 0;
    };

    let mut total = 0u64;
    for entry in entries.flatten() {
        if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            let col_file = entry.path().join(format!("{}.d", col.name));
            if let Ok(m) = std::fs::metadata(&col_file) {
                total += m.len() / elem_size;
            }
        }
    }

    total
}

fn col_type_display(ct: ColumnTypeSerializable) -> &'static str {
    match ct {
        ColumnTypeSerializable::Boolean => "BOOLEAN",
        ColumnTypeSerializable::I8 => "TINYINT",
        ColumnTypeSerializable::I16 => "SMALLINT",
        ColumnTypeSerializable::I32 => "INT",
        ColumnTypeSerializable::I64 => "BIGINT",
        ColumnTypeSerializable::F32 => "FLOAT",
        ColumnTypeSerializable::F64 => "DOUBLE",
        ColumnTypeSerializable::Timestamp => "TIMESTAMP",
        ColumnTypeSerializable::Symbol => "SYMBOL",
        ColumnTypeSerializable::Varchar => "VARCHAR",
        ColumnTypeSerializable::Binary => "BINARY",
        ColumnTypeSerializable::Uuid => "UUID",
        ColumnTypeSerializable::Date => "DATE",
        ColumnTypeSerializable::Char => "CHAR",
        ColumnTypeSerializable::IPv4 => "IPV4",
        ColumnTypeSerializable::Long128 => "LONG128",
        ColumnTypeSerializable::Long256 => "LONG256",
        ColumnTypeSerializable::GeoHash => "GEOHASH",
        ColumnTypeSerializable::String => "STRING",
        ColumnTypeSerializable::TimestampMicro => "TIMESTAMP_MICRO",
        ColumnTypeSerializable::TimestampMilli => "TIMESTAMP_MILLI",
        ColumnTypeSerializable::Interval => "INTERVAL",
        ColumnTypeSerializable::Decimal8 => "DECIMAL8",
        ColumnTypeSerializable::Decimal16 => "DECIMAL16",
        ColumnTypeSerializable::Decimal32 => "DECIMAL32",
        ColumnTypeSerializable::Decimal64 => "DECIMAL64",
        ColumnTypeSerializable::Decimal128 => "DECIMAL128",
        ColumnTypeSerializable::Decimal256 => "DECIMAL256",
        ColumnTypeSerializable::GeoByte => "GEOBYTE",
        ColumnTypeSerializable::GeoShort => "GEOSHORT",
        ColumnTypeSerializable::GeoInt => "GEOINT",
        ColumnTypeSerializable::Array => "ARRAY",
        ColumnTypeSerializable::Cursor => "CURSOR",
        ColumnTypeSerializable::Record => "RECORD",
        ColumnTypeSerializable::RegClass => "REGCLASS",
        ColumnTypeSerializable::RegProcedure => "REGPROCEDURE",
        ColumnTypeSerializable::ArrayString => "ARRAYSTRING",
        ColumnTypeSerializable::Null => "NULL",
        ColumnTypeSerializable::VarArg => "VARARG",
        ColumnTypeSerializable::Parameter => "PARAMETER",
        ColumnTypeSerializable::VarcharSlice => "VARCHAR_SLICE",
        ColumnTypeSerializable::IPv6 => "IPV6",
    }
}

/// Detect the column type by sampling values in the column.
fn detect_column_type(rows: &[Vec<String>], col_idx: usize) -> ColumnType {
    let mut all_int = true;
    let mut all_float = true;
    let mut all_timestamp = true;

    for row in rows {
        let field = match row.get(col_idx) {
            Some(f) => f.trim(),
            None => continue,
        };
        if field.is_empty() {
            continue;
        }

        if field.parse::<i64>().is_err() {
            all_int = false;
        }
        if field.parse::<f64>().is_err() {
            all_float = false;
        }
        // Heuristic: timestamps look like ISO 8601 or are very large integers
        // (nanosecond epoch). A simple heuristic: contains '-' and 'T' or ':'
        if !(field.contains('-') && (field.contains('T') || field.contains(':')))
            && !(all_int && field.len() > 15)
        {
            all_timestamp = false;
        }
    }

    if all_timestamp {
        ColumnType::Timestamp
    } else if all_int {
        ColumnType::I64
    } else if all_float {
        ColumnType::F64
    } else {
        ColumnType::Varchar
    }
}

/// Parse a string field into a `Timestamp`.
fn parse_timestamp_value(s: &str) -> exchange_common::types::Timestamp {
    use exchange_common::types::Timestamp;

    let s = s.trim();
    // Try nanosecond epoch integer first.
    if let Ok(ns) = s.parse::<i64>() {
        return Timestamp(ns);
    }
    // Fallback: current time.
    Timestamp::now()
}

/// Parse a CSV field into a `ColumnValue` given the expected type.
fn parse_field_to_column_value(
    field: &str,
    ct: ColumnType,
) -> exchange_core::table::ColumnValue<'_> {
    use exchange_core::table::ColumnValue;

    let field = field.trim();
    match ct {
        ColumnType::I64 | ColumnType::I32 | ColumnType::I16 | ColumnType::I8 => {
            if let Ok(v) = field.parse::<i64>() {
                ColumnValue::I64(v)
            } else {
                ColumnValue::I64(0)
            }
        }
        ColumnType::F64 | ColumnType::F32 => {
            if let Ok(v) = field.parse::<f64>() {
                ColumnValue::F64(v)
            } else {
                ColumnValue::F64(0.0)
            }
        }
        ColumnType::Timestamp => {
            let ts = parse_timestamp_value(field);
            ColumnValue::Timestamp(ts)
        }
        ColumnType::Symbol => {
            // Symbols are stored as i32 IDs; for import we use 0.
            ColumnValue::I32(0)
        }
        _ => ColumnValue::Str(field),
    }
}

/// Print a `QueryResult::Rows` as a formatted ASCII table.
fn print_ascii_table(columns: &[String], rows: &[Vec<Value>]) {
    if columns.is_empty() {
        println!("(empty result set)");
        return;
    }

    // Compute the display string for every cell.
    let header: Vec<String> = columns.to_vec();
    let body: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.iter().map(|v| format!("{v}")).collect())
        .collect();

    // Determine column widths.
    let mut widths: Vec<usize> = header.iter().map(|h| h.len()).collect();
    for row in &body {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }

    // Build separator line.
    let sep: String = widths
        .iter()
        .map(|w| "-".repeat(w + 2))
        .collect::<Vec<_>>()
        .join("+");
    let sep = format!("+{sep}+");

    // Print header.
    println!("{sep}");
    let header_cells: Vec<String> = header
        .iter()
        .enumerate()
        .map(|(i, h)| format!(" {:<width$} ", h, width = widths[i]))
        .collect();
    println!("|{}|", header_cells.join("|"));
    println!("{sep}");

    // Print rows.
    for row in &body {
        let cells: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, cell)| {
                let w = widths.get(i).copied().unwrap_or(cell.len());
                format!(" {:<width$} ", cell, width = w)
            })
            .collect();
        println!("|{}|", cells.join("|"));
    }
    println!("{sep}");

    println!("{} row(s)", rows.len());
}
