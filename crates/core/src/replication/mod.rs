//! Primary-replica replication for ExchangeDB Enterprise.
//!
//! This module implements WAL-based replication where the primary ships
//! WAL segments to replicas over TCP. Replicas apply the segments to their
//! local storage to maintain a consistent copy of the data.
//!
//! # Components
//!
//! - **Config** (`config.rs`): Replication configuration (role, sync mode, addresses).
//! - **Protocol** (`protocol.rs`): Binary message format for primary-replica communication.
//! - **WalShipper** (`wal_shipper.rs`): Primary-side logic for shipping WAL segments.
//! - **WalReceiver** (`wal_receiver.rs`): Replica-side logic for receiving and applying segments.
//! - **Failover** (`failover.rs`): Promotion and demotion for failover scenarios.

pub mod auto_failover;
pub mod config;
pub mod failover;
pub mod health_monitor;
pub mod manager;
pub mod protocol;
pub mod s3_shipper;
pub mod wal_receiver;
pub mod wal_shipper;

pub use auto_failover::AutoFailover;
pub use config::{ReplicationConfig, ReplicationRole, ReplicationSyncMode};
pub use failover::FailoverManager;
pub use health_monitor::PrimaryHealthMonitor;
pub use manager::{ReplicationManager, ReplicationStatus};
pub use protocol::ReplicationMessage;
pub use wal_receiver::{ReplicaPosition, WalReceiver};
pub use s3_shipper::{S3WalReceiver, S3WalShipper};
pub use wal_shipper::{ReplicationLag, ShipStats, WalShipper};
