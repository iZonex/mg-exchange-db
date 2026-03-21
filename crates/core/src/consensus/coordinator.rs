//! Consensus-based cluster coordination for DDL operations.
//!
//! Wraps a [`RaftNode`] to provide a higher-level API for submitting
//! DDL operations through Raft consensus and applying committed entries.

use exchange_common::error::{ExchangeDbError, Result};

use super::raft::{RaftCommand, RaftNode};

/// Coordinates DDL operations across the cluster using Raft consensus.
pub struct ClusterCoordinator {
    raft: RaftNode,
    /// DDL operations pending consensus acknowledgment.
    pending_ddl: Vec<(u64, RaftCommand)>,
}

impl ClusterCoordinator {
    /// Create a new coordinator backed by a Raft node.
    pub fn new(node_id: String, peers: Vec<String>) -> Self {
        Self {
            raft: RaftNode::new(node_id, peers),
            pending_ddl: Vec::new(),
        }
    }

    /// Submit a DDL operation through consensus.
    ///
    /// The command is proposed to the Raft leader. If this node is not
    /// the leader, an error is returned. On success, the command is
    /// added to the pending list and will be returned by
    /// [`apply_committed`] once committed.
    pub fn submit_ddl(&mut self, command: RaftCommand) -> Result<()> {
        if !self.raft.is_leader() {
            return Err(ExchangeDbError::Query(format!(
                "not the leader; forward DDL to leader at {:?}",
                self.raft.leader_addr()
            )));
        }

        let index = self.raft.propose(command.clone())?;
        self.pending_ddl.push((index, command));
        Ok(())
    }

    /// Apply committed Raft entries and return the commands that are
    /// ready to be executed locally.
    pub fn apply_committed(&mut self) -> Vec<RaftCommand> {
        let entries = self.raft.take_committed();
        let mut applied = Vec::new();

        for entry in &entries {
            applied.push(entry.command.clone());
            // Remove from pending if it was submitted by us.
            self.pending_ddl.retain(|(idx, _)| *idx != entry.index);
        }

        applied
    }

    /// Access the underlying Raft node (e.g. to call `tick` or
    /// `handle_message`).
    pub fn raft(&self) -> &RaftNode {
        &self.raft
    }

    /// Mutable access to the underlying Raft node.
    pub fn raft_mut(&mut self) -> &mut RaftNode {
        &mut self.raft
    }

    /// Check whether there are pending DDL operations awaiting consensus.
    pub fn has_pending(&self) -> bool {
        !self.pending_ddl.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn coordinator_submit_and_apply() {
        let mut coord = ClusterCoordinator::new("node-1".into(), vec![]);

        // Become leader (single node).
        coord.raft_mut().election_timeout = Duration::from_millis(1);
        thread::sleep(Duration::from_millis(5));
        coord.raft_mut().tick();
        assert!(coord.raft().is_leader());

        // Submit DDL.
        coord
            .submit_ddl(RaftCommand::CreateTable("orders".into()))
            .unwrap();
        assert!(coord.has_pending());

        // Apply committed entries.
        let applied = coord.apply_committed();
        assert_eq!(applied.len(), 1);
        assert!(!coord.has_pending());
    }

    #[test]
    fn coordinator_rejects_on_follower() {
        let coord = &mut ClusterCoordinator::new("node-1".into(), vec!["node-2".into()]);
        let result = coord.submit_ddl(RaftCommand::CreateTable("t1".into()));
        assert!(result.is_err());
    }
}
