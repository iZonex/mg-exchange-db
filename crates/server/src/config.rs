//! Configuration system for ExchangeDB.
//!
//! Supports loading from a TOML file (`exchange-db.toml`) with environment
//! variable overrides (`EXCHANGEDB_*`).

use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context, Result};
use serde::de::{self, Visitor};
use serde::Deserialize;

// ---------------------------------------------------------------------------
// Human-readable byte sizes
// ---------------------------------------------------------------------------

/// A byte size parsed from human-readable strings like `"64MB"`, `"4KB"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteSize(pub u64);

impl ByteSize {
    #[allow(dead_code)]
    pub fn bytes(self) -> u64 {
        self.0
    }
}

impl FromStr for ByteSize {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        let (num_part, unit) = split_numeric_suffix(s);
        let value: u64 = num_part
            .parse()
            .with_context(|| format!("invalid byte size number: {num_part}"))?;

        let multiplier = match unit.to_ascii_uppercase().as_str() {
            "" | "B" => 1u64,
            "KB" | "K" => 1024,
            "MB" | "M" => 1024 * 1024,
            "GB" | "G" => 1024 * 1024 * 1024,
            "TB" | "T" => 1024 * 1024 * 1024 * 1024,
            _ => anyhow::bail!("unknown byte size unit: {unit}"),
        };

        Ok(ByteSize(value * multiplier))
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ByteSizeVisitor;

        impl<'de> Visitor<'de> for ByteSizeVisitor {
            type Value = ByteSize;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a byte size string like \"64MB\" or an integer")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<ByteSize, E> {
                v.parse().map_err(de::Error::custom)
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<ByteSize, E> {
                Ok(ByteSize(v))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<ByteSize, E> {
                Ok(ByteSize(v as u64))
            }
        }

        deserializer.deserialize_any(ByteSizeVisitor)
    }
}

impl serde::Serialize for ByteSize {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        const KB: u64 = 1024;
        const MB: u64 = 1024 * KB;
        const GB: u64 = 1024 * MB;
        const TB: u64 = 1024 * GB;

        let s = if self.0 == 0 {
            "0".to_string()
        } else if self.0 % TB == 0 {
            format!("{}TB", self.0 / TB)
        } else if self.0 % GB == 0 {
            format!("{}GB", self.0 / GB)
        } else if self.0 % MB == 0 {
            format!("{}MB", self.0 / MB)
        } else if self.0 % KB == 0 {
            format!("{}KB", self.0 / KB)
        } else {
            self.0.to_string()
        };
        serializer.serialize_str(&s)
    }
}

// ---------------------------------------------------------------------------
// Human-readable durations
// ---------------------------------------------------------------------------

/// A duration parsed from human-readable strings like `"30d"`, `"1h"`, `"5m"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HumanDuration(pub std::time::Duration);

impl HumanDuration {
    #[allow(dead_code)]
    pub fn as_duration(self) -> std::time::Duration {
        self.0
    }
}

impl FromStr for HumanDuration {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        let (num_part, unit) = split_numeric_suffix(s);
        let value: u64 = num_part
            .parse()
            .with_context(|| format!("invalid duration number: {num_part}"))?;

        let secs = match unit.to_ascii_lowercase().as_str() {
            "s" | "sec" | "secs" => value,
            "m" | "min" | "mins" => value * 60,
            "h" | "hr" | "hrs" | "hour" | "hours" => value * 3600,
            "d" | "day" | "days" => value * 86400,
            "w" | "week" | "weeks" => value * 7 * 86400,
            _ => anyhow::bail!("unknown duration unit: {unit}"),
        };

        Ok(HumanDuration(std::time::Duration::from_secs(secs)))
    }
}

impl<'de> Deserialize<'de> for HumanDuration {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct DurationVisitor;

        impl<'de> Visitor<'de> for DurationVisitor {
            type Value = HumanDuration;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a duration string like \"30d\" or \"1h\"")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<HumanDuration, E> {
                v.parse().map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_str(DurationVisitor)
    }
}

impl serde::Serialize for HumanDuration {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let secs = self.0.as_secs();
        let s = if secs == 0 {
            "0s".to_string()
        } else if secs % (7 * 86400) == 0 {
            format!("{}w", secs / (7 * 86400))
        } else if secs % 86400 == 0 {
            format!("{}d", secs / 86400)
        } else if secs % 3600 == 0 {
            format!("{}h", secs / 3600)
        } else if secs % 60 == 0 {
            format!("{}m", secs / 60)
        } else {
            format!("{secs}s")
        };
        serializer.serialize_str(&s)
    }
}

// ---------------------------------------------------------------------------
// Helper: split "64MB" → ("64", "MB")
// ---------------------------------------------------------------------------

fn split_numeric_suffix(s: &str) -> (&str, &str) {
    let idx = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());
    (&s[..idx], &s[idx..])
}

// ---------------------------------------------------------------------------
// Configuration structs
// ---------------------------------------------------------------------------

/// Top-level ExchangeDB configuration.
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct ExchangeDbConfig {
    pub server: ServerSection,
    pub http: HttpSection,
    pub pgwire: PgwireSection,
    pub ilp: IlpSection,
    pub storage: StorageSection,
    pub retention: RetentionSection,
    pub performance: PerformanceSection,
    pub tls: TlsSection,
    pub replication: ReplicationSection,
    pub cairo: CairoSection,
    pub wal: WalSection,
    pub o3: O3Section,
    pub memory: MemorySection,
    pub telemetry: TelemetrySection,
    pub security: SecuritySection,
    pub cluster: ClusterSection,
    pub backup: BackupSection,
    pub tiering: TieringSection,
    pub pitr: PitrSection,
    pub ttl: TtlSection,
    pub downsampling: DownsamplingSection,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct TlsSection {
    pub enabled: bool,
    pub cert_path: String,
    pub key_path: String,
    /// Minimum TLS version: "1.2" (default) or "1.3".
    pub min_version: String,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct ServerSection {
    pub data_dir: PathBuf,
    pub log_level: String,
    /// Log format: "text" (default) or "json".
    pub log_format: String,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct HttpSection {
    pub bind: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct PgwireSection {
    pub bind: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct IlpSection {
    pub bind: String,
    pub enabled: bool,
    pub batch_size: usize,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct StorageSection {
    pub wal_enabled: bool,
    pub wal_max_segment_size: ByteSize,
    pub default_partition_by: String,
    pub mmap_page_size: ByteSize,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct RetentionSection {
    pub enabled: bool,
    pub max_age: HumanDuration,
    pub check_interval: HumanDuration,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct PerformanceSection {
    /// 0 = auto (num_cpus)
    pub query_parallelism: usize,
    /// "sync" or "async"
    pub writer_commit_mode: String,
}

/// Configuration for replication.
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct ReplicationSection {
    /// Role of this node: "standalone", "primary", or "replica".
    pub role: String,
    /// For replicas: address of the primary (e.g. "10.0.0.1:9100").
    pub primary_addr: String,
    /// For primaries: list of replica addresses.
    pub replica_addrs: Vec<String>,
    /// Synchronization mode: "async", "semi-sync", or "sync".
    pub sync_mode: String,
    /// TCP port for the replication listener (replica receives WAL segments here).
    pub replication_port: u16,
    /// Whether automatic failover is enabled (replica only).
    pub failover_enabled: bool,
    /// How often the replica checks primary liveness (e.g. "2s").
    pub health_check_interval: HumanDuration,
    /// Number of consecutive health check failures before auto-promoting.
    pub failure_threshold: u32,
}

/// Cairo storage engine tuning.
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct CairoSection {
    pub max_uncommitted_rows: u64,
    pub commit_lag: HumanDuration,
    pub o3_max_lag: HumanDuration,
    pub writer_data_append_page_size: ByteSize,
    pub reader_pool_max_segments: u32,
    pub spin_lock_timeout: HumanDuration,
    pub character_store_capacity: u32,
    pub character_store_sequence_pool_capacity: u32,
    pub column_pool_capacity: u32,
    pub compact_map_load_factor: f64,
    pub default_map_type: String,
    pub default_symbol_cache_flag: bool,
    pub default_symbol_capacity: u32,
    pub file_operation_retry_count: u32,
    pub inactive_reader_ttl: HumanDuration,
    pub inactive_writer_ttl: HumanDuration,
    pub index_value_block_size: u32,
    pub max_swap_file_count: u32,
    pub mkdir_mode: u32,
    pub parallel_index_threshold: u32,
    pub snapshot_instance_id: String,
    pub sql_copy_buffer_size: ByteSize,
    pub system_table_prefix: String,
    pub volumes: Vec<String>,
}

/// WAL (Write-Ahead Log) configuration.
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct WalSection {
    pub enabled: bool,
    pub max_segment_size: ByteSize,
    pub apply_table_time_quota: HumanDuration,
    pub purge_interval: HumanDuration,
    pub segment_rollover_row_count: u64,
    pub squash_uncommitted_rows_multiplier: f64,
}

/// Out-of-order ingestion settings.
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct O3Section {
    pub partition_split_min_size: ByteSize,
    pub last_partition_max_splits: u32,
    pub column_memory_size: ByteSize,
}

/// Memory limits.
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct MemorySection {
    pub max_per_query: ByteSize,
    pub max_total: ByteSize,
    pub sort_key_max_size: ByteSize,
}

/// Telemetry and metrics settings.
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct TelemetrySection {
    pub enabled: bool,
    pub queue_capacity: u32,
    pub hide_tables: bool,
}

/// Authentication and security settings.
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct SecuritySection {
    pub auth_enabled: bool,
    pub rbac_enabled: bool,
    pub audit_enabled: bool,
    pub password_min_length: u32,
    pub session_timeout: HumanDuration,
    pub max_failed_login_attempts: u32,
    pub lockout_duration: HumanDuration,
    /// Enable encryption at rest for column data files.
    pub encryption_enabled: bool,
    /// Base64-encoded 32-byte encryption key. Required when encryption is enabled.
    pub encryption_key: String,
}

/// Cluster settings for multi-node deployments.
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct ClusterSection {
    pub enabled: bool,
    pub node_id: String,
    pub seed_nodes: Vec<String>,
    pub heartbeat_interval: HumanDuration,
    pub failure_threshold: u32,
}

/// Backup scheduling configuration.
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct BackupSection {
    pub enabled: bool,
    pub schedule: String,
    pub destination: String,
    pub retention_count: u32,
}

/// Tiered storage configuration.
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct TieringSection {
    pub enabled: bool,
    pub hot_retention: HumanDuration,
    pub warm_retention: HumanDuration,
    pub cold_storage_path: String,
    pub check_interval: HumanDuration,
}

/// Point-in-Time Recovery configuration.
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct PitrSection {
    pub enabled: bool,
    pub retention_window: HumanDuration,
    pub snapshot_interval: HumanDuration,
}

/// TTL (Time-To-Live) configuration for automatic data expiration.
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct TtlSection {
    pub enabled: bool,
    pub default_max_age: HumanDuration,
    pub check_interval: HumanDuration,
}

/// Downsampling auto-refresh configuration.
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(default)]
pub struct DownsamplingSection {
    pub enabled: bool,
    pub check_interval: HumanDuration,
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

impl Default for TlsSection {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_path: "cert.pem".into(),
            key_path: "key.pem".into(),
            min_version: "1.2".into(),
        }
    }
}

impl Default for ExchangeDbConfig {
    fn default() -> Self {
        Self {
            server: ServerSection::default(),
            http: HttpSection::default(),
            pgwire: PgwireSection::default(),
            ilp: IlpSection::default(),
            storage: StorageSection::default(),
            retention: RetentionSection::default(),
            performance: PerformanceSection::default(),
            tls: TlsSection::default(),
            replication: ReplicationSection::default(),
            cairo: CairoSection::default(),
            wal: WalSection::default(),
            o3: O3Section::default(),
            memory: MemorySection::default(),
            telemetry: TelemetrySection::default(),
            security: SecuritySection::default(),
            cluster: ClusterSection::default(),
            backup: BackupSection::default(),
            tiering: TieringSection::default(),
            pitr: PitrSection::default(),
            ttl: TtlSection::default(),
            downsampling: DownsamplingSection::default(),
        }
    }
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            log_level: "info".into(),
            log_format: "text".into(),
        }
    }
}

impl Default for HttpSection {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:9000".into(),
            enabled: true,
        }
    }
}

impl Default for PgwireSection {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:8812".into(),
            enabled: true,
        }
    }
}

impl Default for IlpSection {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:9009".into(),
            enabled: true,
            batch_size: 1000,
        }
    }
}

impl Default for StorageSection {
    fn default() -> Self {
        Self {
            wal_enabled: true,
            wal_max_segment_size: ByteSize(64 * 1024 * 1024), // 64MB
            default_partition_by: "day".into(),
            mmap_page_size: ByteSize(4096), // 4KB
        }
    }
}

impl Default for RetentionSection {
    fn default() -> Self {
        Self {
            enabled: false,
            max_age: HumanDuration(std::time::Duration::from_secs(30 * 86400)), // 30d
            check_interval: HumanDuration(std::time::Duration::from_secs(3600)), // 1h
        }
    }
}

impl Default for PerformanceSection {
    fn default() -> Self {
        Self {
            query_parallelism: 0,
            writer_commit_mode: "async".into(),
        }
    }
}

impl Default for ReplicationSection {
    fn default() -> Self {
        Self {
            role: "standalone".into(),
            primary_addr: String::new(),
            replica_addrs: Vec::new(),
            sync_mode: "async".into(),
            replication_port: 19100,
            failover_enabled: false,
            health_check_interval: HumanDuration(std::time::Duration::from_secs(2)),
            failure_threshold: 3,
        }
    }
}

impl Default for CairoSection {
    fn default() -> Self {
        Self {
            max_uncommitted_rows: 500_000,
            commit_lag: HumanDuration(std::time::Duration::from_secs(10)),
            o3_max_lag: HumanDuration(std::time::Duration::from_secs(600)),
            writer_data_append_page_size: ByteSize(16 * 1024 * 1024), // 16MB
            reader_pool_max_segments: 5,
            spin_lock_timeout: HumanDuration(std::time::Duration::from_secs(5)),
            character_store_capacity: 1024,
            character_store_sequence_pool_capacity: 64,
            column_pool_capacity: 4096,
            compact_map_load_factor: 0.7,
            default_map_type: "fast".into(),
            default_symbol_cache_flag: true,
            default_symbol_capacity: 256,
            file_operation_retry_count: 30,
            inactive_reader_ttl: HumanDuration(std::time::Duration::from_secs(120)),
            inactive_writer_ttl: HumanDuration(std::time::Duration::from_secs(120)),
            index_value_block_size: 256,
            max_swap_file_count: 30,
            mkdir_mode: 0o777,
            parallel_index_threshold: 100_000,
            snapshot_instance_id: String::new(),
            sql_copy_buffer_size: ByteSize(4 * 1024 * 1024), // 4MB
            system_table_prefix: "sys.".into(),
            volumes: Vec::new(),
        }
    }
}

impl Default for WalSection {
    fn default() -> Self {
        Self {
            enabled: true,
            max_segment_size: ByteSize(64 * 1024 * 1024), // 64MB
            apply_table_time_quota: HumanDuration(std::time::Duration::from_secs(30)),
            purge_interval: HumanDuration(std::time::Duration::from_secs(30)),
            segment_rollover_row_count: 200_000,
            squash_uncommitted_rows_multiplier: 20.0,
        }
    }
}

impl Default for O3Section {
    fn default() -> Self {
        Self {
            partition_split_min_size: ByteSize(50 * 1024 * 1024), // 50MB
            last_partition_max_splits: 20,
            column_memory_size: ByteSize(8 * 1024 * 1024), // 8MB
        }
    }
}

impl Default for MemorySection {
    fn default() -> Self {
        Self {
            max_per_query: ByteSize(256 * 1024 * 1024), // 256MB
            max_total: ByteSize(0),                      // unlimited
            sort_key_max_size: ByteSize(2 * 1024 * 1024), // 2MB
        }
    }
}

impl Default for TelemetrySection {
    fn default() -> Self {
        Self {
            enabled: true,
            queue_capacity: 512,
            hide_tables: false,
        }
    }
}

impl Default for SecuritySection {
    fn default() -> Self {
        Self {
            auth_enabled: false,
            rbac_enabled: false,
            audit_enabled: false,
            password_min_length: 8,
            session_timeout: HumanDuration(std::time::Duration::from_secs(3600)), // 1h
            max_failed_login_attempts: 5,
            lockout_duration: HumanDuration(std::time::Duration::from_secs(900)), // 15m
            encryption_enabled: false,
            encryption_key: String::new(),
        }
    }
}

impl Default for ClusterSection {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: String::new(),
            seed_nodes: Vec::new(),
            heartbeat_interval: HumanDuration(std::time::Duration::from_secs(5)),
            failure_threshold: 3,
        }
    }
}

impl Default for BackupSection {
    fn default() -> Self {
        Self {
            enabled: false,
            schedule: "0 2 * * *".into(),
            destination: String::new(),
            retention_count: 7,
        }
    }
}

impl Default for TieringSection {
    fn default() -> Self {
        Self {
            enabled: false,
            hot_retention: HumanDuration(std::time::Duration::from_secs(7 * 86400)),  // 7d
            warm_retention: HumanDuration(std::time::Duration::from_secs(30 * 86400)), // 30d
            cold_storage_path: String::new(),
            check_interval: HumanDuration(std::time::Duration::from_secs(1800)), // 30m
        }
    }
}

impl Default for PitrSection {
    fn default() -> Self {
        Self {
            enabled: false,
            retention_window: HumanDuration(std::time::Duration::from_secs(7 * 86400)), // 7d
            snapshot_interval: HumanDuration(std::time::Duration::from_secs(6 * 3600)),  // 6h
        }
    }
}

impl Default for TtlSection {
    fn default() -> Self {
        Self {
            enabled: false,
            default_max_age: HumanDuration(std::time::Duration::from_secs(90 * 86400)), // 90d
            check_interval: HumanDuration(std::time::Duration::from_secs(3600)),          // 1h
        }
    }
}

impl Default for DownsamplingSection {
    fn default() -> Self {
        Self {
            enabled: false,
            check_interval: HumanDuration(std::time::Duration::from_secs(600)), // 10m
        }
    }
}

impl ReplicationSection {
    /// Convert the TOML replication section into the core `ReplicationConfig`.
    pub fn to_replication_config(&self) -> exchange_core::replication::ReplicationConfig {
        use exchange_core::replication::{ReplicationConfig, ReplicationRole, ReplicationSyncMode};

        let role = match self.role.to_lowercase().as_str() {
            "primary" => ReplicationRole::Primary,
            "replica" => ReplicationRole::Replica,
            _ => ReplicationRole::Standalone,
        };

        let sync_mode = match self.sync_mode.to_lowercase().as_str() {
            "sync" => ReplicationSyncMode::Sync,
            "semi-sync" | "semisync" | "semi_sync" => ReplicationSyncMode::SemiSync,
            _ => ReplicationSyncMode::Async,
        };

        let primary_addr = if self.primary_addr.is_empty() {
            None
        } else {
            Some(self.primary_addr.clone())
        };

        ReplicationConfig {
            role,
            primary_addr,
            replica_addrs: self.replica_addrs.clone(),
            sync_mode,
            max_lag_bytes: 256 * 1024 * 1024,
            replication_port: self.replication_port,
            failover_enabled: self.failover_enabled,
            health_check_interval: self.health_check_interval.as_duration(),
            failure_threshold: self.failure_threshold,
        }
    }
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

impl ExchangeDbConfig {
    /// Load configuration from a TOML file. If `path` is `None`, tries
    /// `exchange-db.toml` in the current directory. Falls back to defaults
    /// if the file does not exist.
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let default_path = PathBuf::from("exchange-db.toml");
        let path = path.unwrap_or(&default_path);

        if path.exists() {
            let contents = std::fs::read_to_string(path)
                .with_context(|| format!("failed to read config file: {}", path.display()))?;
            let config: ExchangeDbConfig = toml::from_str(&contents)
                .with_context(|| format!("failed to parse config file: {}", path.display()))?;
            Ok(config)
        } else if path == default_path.as_path() {
            // Default path not found — use defaults silently.
            Ok(Self::default())
        } else {
            anyhow::bail!("config file not found: {}", path.display());
        }
    }

    /// Override configuration values from environment variables.
    ///
    /// Supported variables:
    /// - `EXCHANGEDB_DATA_DIR`
    /// - `EXCHANGEDB_LOG_LEVEL`
    /// - `EXCHANGEDB_HTTP_BIND`
    /// - `EXCHANGEDB_HTTP_ENABLED`
    /// - `EXCHANGEDB_PGWIRE_BIND`
    /// - `EXCHANGEDB_PGWIRE_ENABLED`
    /// - `EXCHANGEDB_ILP_BIND`
    /// - `EXCHANGEDB_ILP_ENABLED`
    /// - `EXCHANGEDB_ILP_BATCH_SIZE`
    /// - `EXCHANGEDB_WAL_ENABLED`
    /// - `EXCHANGEDB_QUERY_PARALLELISM`
    /// - `EXCHANGEDB_WRITER_COMMIT_MODE`
    pub fn from_env(mut self) -> Self {
        if let Ok(v) = std::env::var("EXCHANGEDB_DATA_DIR") {
            self.server.data_dir = PathBuf::from(v);
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_LOG_LEVEL") {
            self.server.log_level = v;
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_LOG_FORMAT") {
            self.server.log_format = v;
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_HTTP_BIND") {
            self.http.bind = v;
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_HTTP_ENABLED") {
            if let Ok(b) = v.parse::<bool>() {
                self.http.enabled = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_PGWIRE_BIND") {
            self.pgwire.bind = v;
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_PGWIRE_ENABLED") {
            if let Ok(b) = v.parse::<bool>() {
                self.pgwire.enabled = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_ILP_BIND") {
            self.ilp.bind = v;
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_ILP_ENABLED") {
            if let Ok(b) = v.parse::<bool>() {
                self.ilp.enabled = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_ILP_BATCH_SIZE") {
            if let Ok(n) = v.parse::<usize>() {
                self.ilp.batch_size = n;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_WAL_ENABLED") {
            if let Ok(b) = v.parse::<bool>() {
                self.storage.wal_enabled = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_QUERY_PARALLELISM") {
            if let Ok(n) = v.parse::<usize>() {
                self.performance.query_parallelism = n;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_WRITER_COMMIT_MODE") {
            self.performance.writer_commit_mode = v;
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_TLS_ENABLED") {
            if let Ok(b) = v.parse::<bool>() {
                self.tls.enabled = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_TLS_CERT_PATH") {
            self.tls.cert_path = v;
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_TLS_KEY_PATH") {
            self.tls.key_path = v;
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_REPLICATION_ROLE") {
            self.replication.role = v;
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_REPLICATION_PRIMARY_ADDR") {
            self.replication.primary_addr = v;
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_REPLICATION_SYNC_MODE") {
            self.replication.sync_mode = v;
        }
        // Cairo section
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_MAX_UNCOMMITTED_ROWS") {
            if let Ok(n) = v.parse::<u64>() {
                self.cairo.max_uncommitted_rows = n;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_COMMIT_LAG") {
            if let Ok(d) = v.parse::<HumanDuration>() {
                self.cairo.commit_lag = d;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_O3_MAX_LAG") {
            if let Ok(d) = v.parse::<HumanDuration>() {
                self.cairo.o3_max_lag = d;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_WRITER_DATA_APPEND_PAGE_SIZE") {
            if let Ok(b) = v.parse::<ByteSize>() {
                self.cairo.writer_data_append_page_size = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_READER_POOL_MAX_SEGMENTS") {
            if let Ok(n) = v.parse::<u32>() {
                self.cairo.reader_pool_max_segments = n;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_SPIN_LOCK_TIMEOUT") {
            if let Ok(d) = v.parse::<HumanDuration>() {
                self.cairo.spin_lock_timeout = d;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_CHARACTER_STORE_CAPACITY") {
            if let Ok(n) = v.parse::<u32>() {
                self.cairo.character_store_capacity = n;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_CHARACTER_STORE_SEQUENCE_POOL_CAPACITY") {
            if let Ok(n) = v.parse::<u32>() {
                self.cairo.character_store_sequence_pool_capacity = n;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_COLUMN_POOL_CAPACITY") {
            if let Ok(n) = v.parse::<u32>() {
                self.cairo.column_pool_capacity = n;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_COMPACT_MAP_LOAD_FACTOR") {
            if let Ok(n) = v.parse::<f64>() {
                self.cairo.compact_map_load_factor = n;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_DEFAULT_MAP_TYPE") {
            self.cairo.default_map_type = v;
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_DEFAULT_SYMBOL_CACHE_FLAG") {
            if let Ok(b) = v.parse::<bool>() {
                self.cairo.default_symbol_cache_flag = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_DEFAULT_SYMBOL_CAPACITY") {
            if let Ok(n) = v.parse::<u32>() {
                self.cairo.default_symbol_capacity = n;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_FILE_OPERATION_RETRY_COUNT") {
            if let Ok(n) = v.parse::<u32>() {
                self.cairo.file_operation_retry_count = n;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_INACTIVE_READER_TTL") {
            if let Ok(d) = v.parse::<HumanDuration>() {
                self.cairo.inactive_reader_ttl = d;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_INACTIVE_WRITER_TTL") {
            if let Ok(d) = v.parse::<HumanDuration>() {
                self.cairo.inactive_writer_ttl = d;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_INDEX_VALUE_BLOCK_SIZE") {
            if let Ok(n) = v.parse::<u32>() {
                self.cairo.index_value_block_size = n;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_MAX_SWAP_FILE_COUNT") {
            if let Ok(n) = v.parse::<u32>() {
                self.cairo.max_swap_file_count = n;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_MKDIR_MODE") {
            if let Ok(n) = u32::from_str_radix(v.trim_start_matches("0o").trim_start_matches("0"), 8) {
                self.cairo.mkdir_mode = n;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_PARALLEL_INDEX_THRESHOLD") {
            if let Ok(n) = v.parse::<u32>() {
                self.cairo.parallel_index_threshold = n;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_SNAPSHOT_INSTANCE_ID") {
            self.cairo.snapshot_instance_id = v;
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_SQL_COPY_BUFFER_SIZE") {
            if let Ok(b) = v.parse::<ByteSize>() {
                self.cairo.sql_copy_buffer_size = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CAIRO_SYSTEM_TABLE_PREFIX") {
            self.cairo.system_table_prefix = v;
        }
        // WAL section
        if let Ok(v) = std::env::var("EXCHANGEDB_WAL_WAL_ENABLED") {
            if let Ok(b) = v.parse::<bool>() {
                self.wal.enabled = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_WAL_MAX_SEGMENT_SIZE") {
            if let Ok(b) = v.parse::<ByteSize>() {
                self.wal.max_segment_size = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_WAL_APPLY_TABLE_TIME_QUOTA") {
            if let Ok(d) = v.parse::<HumanDuration>() {
                self.wal.apply_table_time_quota = d;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_WAL_PURGE_INTERVAL") {
            if let Ok(d) = v.parse::<HumanDuration>() {
                self.wal.purge_interval = d;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_WAL_SEGMENT_ROLLOVER_ROW_COUNT") {
            if let Ok(n) = v.parse::<u64>() {
                self.wal.segment_rollover_row_count = n;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_WAL_SQUASH_UNCOMMITTED_ROWS_MULTIPLIER") {
            if let Ok(n) = v.parse::<f64>() {
                self.wal.squash_uncommitted_rows_multiplier = n;
            }
        }
        // O3 section
        if let Ok(v) = std::env::var("EXCHANGEDB_O3_PARTITION_SPLIT_MIN_SIZE") {
            if let Ok(b) = v.parse::<ByteSize>() {
                self.o3.partition_split_min_size = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_O3_LAST_PARTITION_MAX_SPLITS") {
            if let Ok(n) = v.parse::<u32>() {
                self.o3.last_partition_max_splits = n;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_O3_COLUMN_MEMORY_SIZE") {
            if let Ok(b) = v.parse::<ByteSize>() {
                self.o3.column_memory_size = b;
            }
        }
        // Memory section
        if let Ok(v) = std::env::var("EXCHANGEDB_MEMORY_MAX_PER_QUERY") {
            if let Ok(b) = v.parse::<ByteSize>() {
                self.memory.max_per_query = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_MEMORY_MAX_TOTAL") {
            if let Ok(b) = v.parse::<ByteSize>() {
                self.memory.max_total = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_MEMORY_SORT_KEY_MAX_SIZE") {
            if let Ok(b) = v.parse::<ByteSize>() {
                self.memory.sort_key_max_size = b;
            }
        }
        // Telemetry section
        if let Ok(v) = std::env::var("EXCHANGEDB_TELEMETRY_ENABLED") {
            if let Ok(b) = v.parse::<bool>() {
                self.telemetry.enabled = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_TELEMETRY_QUEUE_CAPACITY") {
            if let Ok(n) = v.parse::<u32>() {
                self.telemetry.queue_capacity = n;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_TELEMETRY_HIDE_TABLES") {
            if let Ok(b) = v.parse::<bool>() {
                self.telemetry.hide_tables = b;
            }
        }
        // Security section
        if let Ok(v) = std::env::var("EXCHANGEDB_SECURITY_AUTH_ENABLED") {
            if let Ok(b) = v.parse::<bool>() {
                self.security.auth_enabled = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_SECURITY_RBAC_ENABLED") {
            if let Ok(b) = v.parse::<bool>() {
                self.security.rbac_enabled = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_SECURITY_AUDIT_ENABLED") {
            if let Ok(b) = v.parse::<bool>() {
                self.security.audit_enabled = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_SECURITY_PASSWORD_MIN_LENGTH") {
            if let Ok(n) = v.parse::<u32>() {
                self.security.password_min_length = n;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_SECURITY_SESSION_TIMEOUT") {
            if let Ok(d) = v.parse::<HumanDuration>() {
                self.security.session_timeout = d;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_SECURITY_MAX_FAILED_LOGIN_ATTEMPTS") {
            if let Ok(n) = v.parse::<u32>() {
                self.security.max_failed_login_attempts = n;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_SECURITY_LOCKOUT_DURATION") {
            if let Ok(d) = v.parse::<HumanDuration>() {
                self.security.lockout_duration = d;
            }
        }
        // Cluster section
        if let Ok(v) = std::env::var("EXCHANGEDB_CLUSTER_ENABLED") {
            if let Ok(b) = v.parse::<bool>() {
                self.cluster.enabled = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CLUSTER_NODE_ID") {
            self.cluster.node_id = v;
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CLUSTER_HEARTBEAT_INTERVAL") {
            if let Ok(d) = v.parse::<HumanDuration>() {
                self.cluster.heartbeat_interval = d;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_CLUSTER_FAILURE_THRESHOLD") {
            if let Ok(n) = v.parse::<u32>() {
                self.cluster.failure_threshold = n;
            }
        }
        // Backup section
        if let Ok(v) = std::env::var("EXCHANGEDB_BACKUP_ENABLED") {
            if let Ok(b) = v.parse::<bool>() {
                self.backup.enabled = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_BACKUP_SCHEDULE") {
            self.backup.schedule = v;
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_BACKUP_DESTINATION") {
            self.backup.destination = v;
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_BACKUP_RETENTION_COUNT") {
            if let Ok(n) = v.parse::<u32>() {
                self.backup.retention_count = n;
            }
        }
        // Tiering section
        if let Ok(v) = std::env::var("EXCHANGEDB_TIERING_ENABLED") {
            if let Ok(b) = v.parse::<bool>() {
                self.tiering.enabled = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_TIERING_HOT_RETENTION") {
            if let Ok(d) = v.parse::<HumanDuration>() {
                self.tiering.hot_retention = d;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_TIERING_WARM_RETENTION") {
            if let Ok(d) = v.parse::<HumanDuration>() {
                self.tiering.warm_retention = d;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_TIERING_COLD_STORAGE_PATH") {
            self.tiering.cold_storage_path = v;
        }
        // PITR section
        if let Ok(v) = std::env::var("EXCHANGEDB_PITR_ENABLED") {
            if let Ok(b) = v.parse::<bool>() {
                self.pitr.enabled = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_PITR_RETENTION_WINDOW") {
            if let Ok(d) = v.parse::<HumanDuration>() {
                self.pitr.retention_window = d;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_PITR_SNAPSHOT_INTERVAL") {
            if let Ok(d) = v.parse::<HumanDuration>() {
                self.pitr.snapshot_interval = d;
            }
        }
        // TTL section
        if let Ok(v) = std::env::var("EXCHANGEDB_TTL_ENABLED") {
            if let Ok(b) = v.parse::<bool>() {
                self.ttl.enabled = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_TTL_DEFAULT_MAX_AGE") {
            if let Ok(d) = v.parse::<HumanDuration>() {
                self.ttl.default_max_age = d;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_TTL_CHECK_INTERVAL") {
            if let Ok(d) = v.parse::<HumanDuration>() {
                self.ttl.check_interval = d;
            }
        }
        // Downsampling section
        if let Ok(v) = std::env::var("EXCHANGEDB_DOWNSAMPLING_ENABLED") {
            if let Ok(b) = v.parse::<bool>() {
                self.downsampling.enabled = b;
            }
        }
        if let Ok(v) = std::env::var("EXCHANGEDB_DOWNSAMPLING_CHECK_INTERVAL") {
            if let Ok(d) = v.parse::<HumanDuration>() {
                self.downsampling.check_interval = d;
            }
        }
        self
    }

    /// Parse the HTTP bind address into a `SocketAddr`.
    pub fn http_bind_addr(&self) -> Result<SocketAddr> {
        self.http
            .bind
            .parse()
            .with_context(|| format!("invalid HTTP bind address: {}", self.http.bind))
    }

    /// Parse the pgwire bind address into a `SocketAddr`.
    pub fn pgwire_bind_addr(&self) -> Result<SocketAddr> {
        self.pgwire
            .bind
            .parse()
            .with_context(|| format!("invalid pgwire bind address: {}", self.pgwire.bind))
    }

    /// Parse the ILP bind address into a `SocketAddr`.
    pub fn ilp_bind_addr(&self) -> Result<SocketAddr> {
        self.ilp
            .bind
            .parse()
            .with_context(|| format!("invalid ILP bind address: {}", self.ilp.bind))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_byte_size_parsing() {
        assert_eq!("4KB".parse::<ByteSize>().unwrap().bytes(), 4096);
        assert_eq!("64MB".parse::<ByteSize>().unwrap().bytes(), 64 * 1024 * 1024);
        assert_eq!("1GB".parse::<ByteSize>().unwrap().bytes(), 1024 * 1024 * 1024);
        assert_eq!("100B".parse::<ByteSize>().unwrap().bytes(), 100);
        assert_eq!("512".parse::<ByteSize>().unwrap().bytes(), 512);
    }

    #[test]
    fn test_human_duration_parsing() {
        assert_eq!(
            "30d".parse::<HumanDuration>().unwrap().as_duration(),
            std::time::Duration::from_secs(30 * 86400)
        );
        assert_eq!(
            "1h".parse::<HumanDuration>().unwrap().as_duration(),
            std::time::Duration::from_secs(3600)
        );
        assert_eq!(
            "5m".parse::<HumanDuration>().unwrap().as_duration(),
            std::time::Duration::from_secs(300)
        );
    }

    #[test]
    fn test_default_config() {
        let config = ExchangeDbConfig::default();
        assert_eq!(config.http.bind, "0.0.0.0:9000");
        assert_eq!(config.pgwire.bind, "0.0.0.0:8812");
        assert_eq!(config.ilp.bind, "0.0.0.0:9009");
        assert!(config.http.enabled);
        assert!(config.storage.wal_enabled);
        assert!(!config.retention.enabled);
        assert!(!config.tls.enabled);
        assert_eq!(config.tls.cert_path, "cert.pem");
        assert_eq!(config.tls.key_path, "key.pem");
    }

    #[test]
    fn test_toml_deserialization() {
        let toml_str = r#"
[server]
data_dir = "/var/lib/exchangedb"
log_level = "debug"

[http]
bind = "127.0.0.1:8080"
enabled = true

[storage]
wal_max_segment_size = "128MB"
mmap_page_size = "2MB"

[retention]
enabled = true
max_age = "7d"
check_interval = "30m"
"#;
        let config: ExchangeDbConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(
            config.server.data_dir,
            PathBuf::from("/var/lib/exchangedb")
        );
        assert_eq!(config.server.log_level, "debug");
        assert_eq!(config.http.bind, "127.0.0.1:8080");
        assert_eq!(
            config.storage.wal_max_segment_size.bytes(),
            128 * 1024 * 1024
        );
        assert_eq!(config.storage.mmap_page_size.bytes(), 2 * 1024 * 1024);
        assert!(config.retention.enabled);
        assert_eq!(
            config.retention.max_age.as_duration(),
            std::time::Duration::from_secs(7 * 86400)
        );
    }

    #[test]
    fn test_cairo_defaults() {
        let config = ExchangeDbConfig::default();
        assert_eq!(config.cairo.max_uncommitted_rows, 500_000);
        assert_eq!(config.cairo.commit_lag.as_duration(), std::time::Duration::from_secs(10));
        assert_eq!(config.cairo.o3_max_lag.as_duration(), std::time::Duration::from_secs(600));
        assert_eq!(config.cairo.writer_data_append_page_size.bytes(), 16 * 1024 * 1024);
        assert_eq!(config.cairo.reader_pool_max_segments, 5);
        assert_eq!(config.cairo.column_pool_capacity, 4096);
        assert_eq!(config.cairo.compact_map_load_factor, 0.7);
        assert_eq!(config.cairo.default_map_type, "fast");
        assert!(config.cairo.default_symbol_cache_flag);
        assert_eq!(config.cairo.default_symbol_capacity, 256);
        assert_eq!(config.cairo.file_operation_retry_count, 30);
        assert_eq!(config.cairo.index_value_block_size, 256);
        assert_eq!(config.cairo.mkdir_mode, 0o777);
        assert_eq!(config.cairo.parallel_index_threshold, 100_000);
        assert_eq!(config.cairo.system_table_prefix, "sys.");
        assert!(config.cairo.volumes.is_empty());
    }

    #[test]
    fn test_wal_defaults() {
        let config = ExchangeDbConfig::default();
        assert!(config.wal.enabled);
        assert_eq!(config.wal.max_segment_size.bytes(), 64 * 1024 * 1024);
        assert_eq!(config.wal.apply_table_time_quota.as_duration(), std::time::Duration::from_secs(30));
        assert_eq!(config.wal.purge_interval.as_duration(), std::time::Duration::from_secs(30));
        assert_eq!(config.wal.segment_rollover_row_count, 200_000);
        assert_eq!(config.wal.squash_uncommitted_rows_multiplier, 20.0);
    }

    #[test]
    fn test_o3_defaults() {
        let config = ExchangeDbConfig::default();
        assert_eq!(config.o3.partition_split_min_size.bytes(), 50 * 1024 * 1024);
        assert_eq!(config.o3.last_partition_max_splits, 20);
        assert_eq!(config.o3.column_memory_size.bytes(), 8 * 1024 * 1024);
    }

    #[test]
    fn test_memory_defaults() {
        let config = ExchangeDbConfig::default();
        assert_eq!(config.memory.max_per_query.bytes(), 256 * 1024 * 1024);
        assert_eq!(config.memory.max_total.bytes(), 0);
        assert_eq!(config.memory.sort_key_max_size.bytes(), 2 * 1024 * 1024);
    }

    #[test]
    fn test_telemetry_defaults() {
        let config = ExchangeDbConfig::default();
        assert!(config.telemetry.enabled);
        assert_eq!(config.telemetry.queue_capacity, 512);
        assert!(!config.telemetry.hide_tables);
    }

    #[test]
    fn test_security_defaults() {
        let config = ExchangeDbConfig::default();
        assert!(!config.security.auth_enabled);
        assert!(!config.security.rbac_enabled);
        assert!(!config.security.audit_enabled);
        assert_eq!(config.security.password_min_length, 8);
        assert_eq!(config.security.session_timeout.as_duration(), std::time::Duration::from_secs(3600));
        assert_eq!(config.security.max_failed_login_attempts, 5);
        assert_eq!(config.security.lockout_duration.as_duration(), std::time::Duration::from_secs(900));
    }

    #[test]
    fn test_cluster_defaults() {
        let config = ExchangeDbConfig::default();
        assert!(!config.cluster.enabled);
        assert!(config.cluster.node_id.is_empty());
        assert!(config.cluster.seed_nodes.is_empty());
        assert_eq!(config.cluster.heartbeat_interval.as_duration(), std::time::Duration::from_secs(5));
        assert_eq!(config.cluster.failure_threshold, 3);
    }

    #[test]
    fn test_backup_defaults() {
        let config = ExchangeDbConfig::default();
        assert!(!config.backup.enabled);
        assert_eq!(config.backup.schedule, "0 2 * * *");
        assert!(config.backup.destination.is_empty());
        assert_eq!(config.backup.retention_count, 7);
    }

    #[test]
    fn test_new_sections_toml_deserialization() {
        let toml_str = r#"
[cairo]
max_uncommitted_rows = 1000000
commit_lag = "20s"
default_map_type = "compact"
default_symbol_cache_flag = false
compact_map_load_factor = 0.5

[wal]
enabled = false
max_segment_size = "128MB"
segment_rollover_row_count = 500000

[o3]
partition_split_min_size = "100MB"
last_partition_max_splits = 10

[memory]
max_per_query = "512MB"
sort_key_max_size = "4MB"

[telemetry]
enabled = false
queue_capacity = 1024

[security]
auth_enabled = true
rbac_enabled = true
password_min_length = 12
session_timeout = "2h"

[cluster]
enabled = true
node_id = "node-1"
seed_nodes = ["10.0.0.1:9100", "10.0.0.2:9100"]
heartbeat_interval = "10s"

[backup]
enabled = true
schedule = "0 3 * * 0"
destination = "/backups/exchangedb"
retention_count = 14
"#;
        let config: ExchangeDbConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.cairo.max_uncommitted_rows, 1_000_000);
        assert_eq!(config.cairo.commit_lag.as_duration(), std::time::Duration::from_secs(20));
        assert_eq!(config.cairo.default_map_type, "compact");
        assert!(!config.cairo.default_symbol_cache_flag);
        assert_eq!(config.cairo.compact_map_load_factor, 0.5);
        assert!(!config.wal.enabled);
        assert_eq!(config.wal.max_segment_size.bytes(), 128 * 1024 * 1024);
        assert_eq!(config.wal.segment_rollover_row_count, 500_000);
        assert_eq!(config.o3.partition_split_min_size.bytes(), 100 * 1024 * 1024);
        assert_eq!(config.o3.last_partition_max_splits, 10);
        assert_eq!(config.memory.max_per_query.bytes(), 512 * 1024 * 1024);
        assert_eq!(config.memory.sort_key_max_size.bytes(), 4 * 1024 * 1024);
        assert!(!config.telemetry.enabled);
        assert_eq!(config.telemetry.queue_capacity, 1024);
        assert!(config.security.auth_enabled);
        assert!(config.security.rbac_enabled);
        assert_eq!(config.security.password_min_length, 12);
        assert_eq!(config.security.session_timeout.as_duration(), std::time::Duration::from_secs(7200));
        assert!(config.cluster.enabled);
        assert_eq!(config.cluster.node_id, "node-1");
        assert_eq!(config.cluster.seed_nodes, vec!["10.0.0.1:9100", "10.0.0.2:9100"]);
        assert_eq!(config.cluster.heartbeat_interval.as_duration(), std::time::Duration::from_secs(10));
        assert!(config.backup.enabled);
        assert_eq!(config.backup.schedule, "0 3 * * 0");
        assert_eq!(config.backup.destination, "/backups/exchangedb");
        assert_eq!(config.backup.retention_count, 14);
    }

    #[test]
    fn test_tls_toml_deserialization() {
        let toml_str = r#"
[tls]
enabled = true
cert_path = "/etc/ssl/server.crt"
key_path = "/etc/ssl/server.key"
"#;
        let config: ExchangeDbConfig = toml::from_str(toml_str).unwrap();
        assert!(config.tls.enabled);
        assert_eq!(config.tls.cert_path, "/etc/ssl/server.crt");
        assert_eq!(config.tls.key_path, "/etc/ssl/server.key");
    }
}
