//! Minimal Raft consensus for leader election and DDL replication.
//!
//! Provides leader election with heartbeats, term management, and
//! consensus-based cluster coordination for DDL operations.

pub mod coordinator;
pub mod raft;

pub use coordinator::ClusterCoordinator;
pub use raft::{RaftCommand, RaftLogEntry, RaftMessage, RaftNode, RaftState};
