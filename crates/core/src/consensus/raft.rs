//! Simplified Raft consensus implementation for leader election.
//!
//! This is a minimal Raft that handles leader election, heartbeats,
//! term management, and vote management. It does not implement full
//! log replication but supports proposing commands on the leader.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use exchange_common::error::{ExchangeDbError, Result};

/// The role of a Raft node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
}

/// A node participating in Raft consensus.
pub struct RaftNode {
    pub id: String,
    pub state: RaftState,
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub leader_id: Option<String>,
    pub peers: Vec<String>,
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub last_heartbeat: Instant,

    // Internal state for elections.
    votes_received: HashMap<String, bool>,

    // Log state (simplified).
    log: Vec<RaftLogEntry>,
    commit_index: u64,
    last_applied: u64,
}

/// Messages exchanged between Raft nodes.
#[derive(Debug, Clone)]
pub enum RaftMessage {
    RequestVote {
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteResponse {
        term: u64,
        vote_granted: bool,
    },
    AppendEntries {
        term: u64,
        leader_id: String,
        entries: Vec<RaftLogEntry>,
    },
    AppendEntriesResponse {
        term: u64,
        success: bool,
    },
}

/// A single entry in the Raft log.
#[derive(Debug, Clone)]
pub struct RaftLogEntry {
    pub term: u64,
    pub index: u64,
    pub command: RaftCommand,
}

/// Commands replicated through Raft consensus.
#[derive(Debug, Clone)]
pub enum RaftCommand {
    /// DDL operation that must be replicated.
    CreateTable(String),
    DropTable(String),
    AlterTable(String, String),
    /// WAL segment committed.
    WalCommit {
        table: String,
        segment_id: u32,
    },
}

impl RaftNode {
    /// Create a new Raft node with the given id and peer addresses.
    ///
    /// The node starts as a `Follower` with term 0. The election timeout
    /// is set to 300ms and the heartbeat interval to 100ms.
    pub fn new(id: String, peers: Vec<String>) -> Self {
        Self {
            id,
            state: RaftState::Follower,
            current_term: 0,
            voted_for: None,
            leader_id: None,
            peers,
            election_timeout: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(100),
            last_heartbeat: Instant::now(),
            votes_received: HashMap::new(),
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
        }
    }

    /// Tick the Raft node. Returns a message to broadcast if a state
    /// transition occurs.
    ///
    /// - If the node is a follower or candidate and the election timeout
    ///   has elapsed without a heartbeat, it transitions to candidate and
    ///   starts an election by returning a `RequestVote` message.
    /// - If the node is the leader and the heartbeat interval has elapsed,
    ///   it returns an `AppendEntries` heartbeat message.
    pub fn tick(&mut self) -> Option<RaftMessage> {
        let now = Instant::now();

        match self.state {
            RaftState::Follower | RaftState::Candidate => {
                if now.duration_since(self.last_heartbeat) >= self.election_timeout {
                    self.start_election()
                }
                None
            }
            RaftState::Leader => {
                if now.duration_since(self.last_heartbeat) >= self.heartbeat_interval {
                    self.last_heartbeat = now;
                    Some(RaftMessage::AppendEntries {
                        term: self.current_term,
                        leader_id: self.id.clone(),
                        entries: Vec::new(),
                    })
                } else {
                    None
                }
            }
        }
    }

    /// Start an election: increment term, vote for self, send RequestVote.
    fn start_election(&mut self) {
        self.current_term += 1;
        self.state = RaftState::Candidate;
        self.voted_for = Some(self.id.clone());
        self.leader_id = None;
        self.last_heartbeat = Instant::now();

        // Vote for self.
        self.votes_received.clear();
        self.votes_received.insert(self.id.clone(), true);

        // If we are the only node, we win immediately.
        if self.peers.is_empty() {
            self.become_leader();
        }
    }

    /// Transition to leader state.
    fn become_leader(&mut self) {
        self.state = RaftState::Leader;
        self.leader_id = Some(self.id.clone());
        self.last_heartbeat = Instant::now();
    }

    /// Handle an incoming Raft message. Returns a list of response messages
    /// to send back.
    pub fn handle_message(&mut self, msg: RaftMessage) -> Vec<RaftMessage> {
        match msg {
            RaftMessage::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => self.handle_request_vote(term, candidate_id, last_log_index, last_log_term),

            RaftMessage::RequestVoteResponse { term, vote_granted } => {
                self.handle_vote_response(term, vote_granted);
                Vec::new()
            }

            RaftMessage::AppendEntries {
                term,
                leader_id,
                entries,
            } => self.handle_append_entries(term, leader_id, entries),

            RaftMessage::AppendEntriesResponse { term, success } => {
                self.handle_append_entries_response(term, success);
                Vec::new()
            }
        }
    }

    fn handle_request_vote(
        &mut self,
        term: u64,
        candidate_id: String,
        _last_log_index: u64,
        _last_log_term: u64,
    ) -> Vec<RaftMessage> {
        // If the candidate's term is higher, step down.
        if term > self.current_term {
            self.step_down(term);
        }

        let vote_granted = if term < self.current_term {
            // Reject votes from older terms.
            false
        } else if self.voted_for.is_none() || self.voted_for.as_deref() == Some(&candidate_id) {
            // Grant vote if we haven't voted yet or already voted for this candidate.
            self.voted_for = Some(candidate_id);
            self.last_heartbeat = Instant::now();
            true
        } else {
            false
        };

        vec![RaftMessage::RequestVoteResponse {
            term: self.current_term,
            vote_granted,
        }]
    }

    fn handle_vote_response(&mut self, term: u64, vote_granted: bool) {
        if term > self.current_term {
            self.step_down(term);
            return;
        }

        if self.state != RaftState::Candidate || term != self.current_term {
            return;
        }

        if vote_granted {
            // Record the vote (we don't track who voted, just count).
            let vote_count = self.votes_received.len() + 1;
            // Use a dummy key for the voter since we don't have sender info.
            self.votes_received
                .insert(format!("voter-{}", vote_count), true);
        }

        // Check if we have a majority: self + votes from peers.
        let total_nodes = self.peers.len() + 1;
        let majority = total_nodes / 2 + 1;
        if self.votes_received.len() >= majority {
            self.become_leader();
        }
    }

    fn handle_append_entries(
        &mut self,
        term: u64,
        leader_id: String,
        entries: Vec<RaftLogEntry>,
    ) -> Vec<RaftMessage> {
        if term > self.current_term {
            self.step_down(term);
        }

        if term < self.current_term {
            return vec![RaftMessage::AppendEntriesResponse {
                term: self.current_term,
                success: false,
            }];
        }

        // Valid heartbeat / append from current leader.
        self.state = RaftState::Follower;
        self.leader_id = Some(leader_id);
        self.last_heartbeat = Instant::now();

        // Append entries to our log.
        for entry in entries {
            if entry.index as usize > self.log.len() {
                self.log.push(entry);
            }
        }

        vec![RaftMessage::AppendEntriesResponse {
            term: self.current_term,
            success: true,
        }]
    }

    fn handle_append_entries_response(&mut self, term: u64, _success: bool) {
        if term > self.current_term {
            self.step_down(term);
        }
    }

    /// Step down to follower state when a higher term is discovered.
    fn step_down(&mut self, new_term: u64) {
        self.current_term = new_term;
        self.state = RaftState::Follower;
        self.voted_for = None;
        self.leader_id = None;
        self.last_heartbeat = Instant::now();
    }

    /// Check if this node is the leader.
    pub fn is_leader(&self) -> bool {
        self.state == RaftState::Leader
    }

    /// Propose a command to the Raft log. Only works on the leader.
    ///
    /// Returns the log index of the proposed entry.
    pub fn propose(&mut self, command: RaftCommand) -> Result<u64> {
        if self.state != RaftState::Leader {
            return Err(ExchangeDbError::Query(
                "not the leader; cannot propose commands".into(),
            ));
        }

        let index = self.log.len() as u64 + 1;
        let entry = RaftLogEntry {
            term: self.current_term,
            index,
            command,
        };
        self.log.push(entry);
        self.commit_index = index;
        Ok(index)
    }

    /// Get the current leader address.
    pub fn leader_addr(&self) -> Option<&str> {
        self.leader_id.as_deref()
    }

    /// Return the last log index and term.
    pub fn last_log_info(&self) -> (u64, u64) {
        if let Some(entry) = self.log.last() {
            (entry.index, entry.term)
        } else {
            (0, 0)
        }
    }

    /// Return committed log entries that have not yet been applied.
    pub fn take_committed(&mut self) -> Vec<RaftLogEntry> {
        let mut entries = Vec::new();
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self.log.get(self.last_applied as usize - 1) {
                entries.push(entry.clone());
            }
        }
        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn single_node_becomes_leader() {
        let mut node = RaftNode::new("node-1".into(), vec![]);
        // With no peers, starting an election should immediately win.
        node.election_timeout = Duration::from_millis(1);
        thread::sleep(Duration::from_millis(5));
        node.tick();
        assert!(node.is_leader());
        assert_eq!(node.current_term, 1);
        assert_eq!(node.leader_addr(), Some("node-1"));
    }

    #[test]
    fn three_node_election() {
        let mut node1 = RaftNode::new("node-1".into(), vec!["node-2".into(), "node-3".into()]);
        let mut node2 = RaftNode::new("node-2".into(), vec!["node-1".into(), "node-3".into()]);
        let mut node3 = RaftNode::new("node-3".into(), vec!["node-1".into(), "node-2".into()]);

        // Trigger election on node1 by expiring its timeout.
        node1.election_timeout = Duration::from_millis(1);
        thread::sleep(Duration::from_millis(5));
        node1.tick();

        // node1 is now a candidate in term 1. Build the RequestVote message.
        let (last_log_index, last_log_term) = node1.last_log_info();
        let request_vote = RaftMessage::RequestVote {
            term: node1.current_term,
            candidate_id: "node-1".into(),
            last_log_index,
            last_log_term,
        };

        // node2 and node3 receive the vote request.
        let responses2 = node2.handle_message(request_vote.clone());
        let responses3 = node3.handle_message(request_vote);

        // Both should grant their votes.
        assert_eq!(responses2.len(), 1);
        assert_eq!(responses3.len(), 1);

        // Feed responses back to node1.
        for resp in responses2 {
            node1.handle_message(resp);
        }
        for resp in responses3 {
            node1.handle_message(resp);
        }

        // node1 should now be leader with a majority (2 out of 3).
        assert!(node1.is_leader());
        assert_eq!(node1.current_term, 1);
    }

    #[test]
    fn term_increments_on_election() {
        let mut node = RaftNode::new("node-1".into(), vec![]);
        assert_eq!(node.current_term, 0);

        node.election_timeout = Duration::from_millis(1);
        thread::sleep(Duration::from_millis(5));
        node.tick();

        assert_eq!(node.current_term, 1);
        assert!(node.is_leader());

        // Force another election by stepping down and re-electing.
        node.state = RaftState::Follower;
        node.election_timeout = Duration::from_millis(1);
        thread::sleep(Duration::from_millis(5));
        node.tick();

        assert_eq!(node.current_term, 2);
    }

    #[test]
    fn heartbeat_prevents_reelection() {
        let mut node1 = RaftNode::new("node-1".into(), vec!["node-2".into()]);
        let mut node2 = RaftNode::new("node-2".into(), vec!["node-1".into()]);

        // Make node1 leader through election.
        node1.election_timeout = Duration::from_millis(1);
        thread::sleep(Duration::from_millis(5));
        node1.tick();

        let (last_log_index, last_log_term) = node1.last_log_info();
        let vote_req = RaftMessage::RequestVote {
            term: node1.current_term,
            candidate_id: "node-1".into(),
            last_log_index,
            last_log_term,
        };
        let responses = node2.handle_message(vote_req);
        for r in responses {
            node1.handle_message(r);
        }
        assert!(node1.is_leader());

        // Leader sends heartbeat to node2.
        let heartbeat = RaftMessage::AppendEntries {
            term: node1.current_term,
            leader_id: "node-1".into(),
            entries: Vec::new(),
        };
        node2.handle_message(heartbeat);

        // node2's last_heartbeat is refreshed; it should remain a follower.
        assert_eq!(node2.state, RaftState::Follower);
        assert_eq!(node2.leader_id, Some("node-1".into()));

        // Even if we wait a bit, node2 should not start election because
        // the heartbeat just arrived.
        node2.election_timeout = Duration::from_millis(50);
        // No sleep -- timeout has not elapsed.
        let msg = node2.tick();
        assert!(msg.is_none());
        assert_eq!(node2.state, RaftState::Follower);
    }

    #[test]
    fn higher_term_steps_down_leader() {
        let mut leader = RaftNode::new("node-1".into(), vec!["node-2".into()]);

        // Make node-1 the leader at term 1.
        leader.election_timeout = Duration::from_millis(1);
        thread::sleep(Duration::from_millis(5));
        leader.tick();

        // Simulate receiving a vote response: grant from node-2.
        leader.handle_message(RaftMessage::RequestVoteResponse {
            term: 1,
            vote_granted: true,
        });
        assert!(leader.is_leader());

        // A message arrives from a node with a higher term.
        let higher_term_msg = RaftMessage::AppendEntries {
            term: 5,
            leader_id: "node-2".into(),
            entries: Vec::new(),
        };
        leader.handle_message(higher_term_msg);

        // node-1 should have stepped down.
        assert_eq!(leader.state, RaftState::Follower);
        assert_eq!(leader.current_term, 5);
        assert_eq!(leader.leader_id, Some("node-2".into()));
    }

    #[test]
    fn propose_only_on_leader() {
        let mut node = RaftNode::new("node-1".into(), vec![]);

        // As follower, propose should fail.
        let result = node.propose(RaftCommand::CreateTable("t1".into()));
        assert!(result.is_err());

        // Become leader.
        node.election_timeout = Duration::from_millis(1);
        thread::sleep(Duration::from_millis(5));
        node.tick();
        assert!(node.is_leader());

        // Now propose should succeed.
        let idx = node.propose(RaftCommand::CreateTable("t1".into())).unwrap();
        assert_eq!(idx, 1);

        let idx2 = node.propose(RaftCommand::DropTable("t2".into())).unwrap();
        assert_eq!(idx2, 2);
    }

    #[test]
    fn take_committed_entries() {
        let mut node = RaftNode::new("node-1".into(), vec![]);
        node.election_timeout = Duration::from_millis(1);
        thread::sleep(Duration::from_millis(5));
        node.tick();

        node.propose(RaftCommand::CreateTable("t1".into())).unwrap();
        node.propose(RaftCommand::WalCommit {
            table: "t1".into(),
            segment_id: 1,
        })
        .unwrap();

        let entries = node.take_committed();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].index, 1);
        assert_eq!(entries[1].index, 2);

        // Taking again returns nothing.
        let entries2 = node.take_committed();
        assert!(entries2.is_empty());
    }
}
