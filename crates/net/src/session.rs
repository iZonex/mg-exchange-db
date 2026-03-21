//! Session management for ExchangeDB connections.
//!
//! Each client connection gets a session that tracks user identity,
//! session-level settings (`SET key = value`), and transaction state.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use uuid::Uuid;

/// State of a transaction within a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// No active transaction.
    Idle,
    /// Inside a transaction block (after BEGIN).
    InTransaction,
    /// Transaction has encountered an error and must be rolled back.
    Failed,
}

/// A client session with its associated state.
#[derive(Debug, Clone)]
pub struct Session {
    pub id: String,
    pub user: Option<String>,
    pub created_at: Instant,
    pub last_active: Instant,
    pub settings: HashMap<String, String>,
    pub transaction_state: TransactionState,
}

impl Session {
    fn new(id: String) -> Self {
        let now = Instant::now();
        Self {
            id,
            user: None,
            created_at: now,
            last_active: now,
            settings: HashMap::new(),
            transaction_state: TransactionState::Idle,
        }
    }

    fn touch(&mut self) {
        self.last_active = Instant::now();
    }

    fn is_expired(&self, timeout: Duration) -> bool {
        self.last_active.elapsed() >= timeout
    }
}

/// Thread-safe session manager supporting concurrent access.
pub struct SessionManager {
    sessions: DashMap<String, Session>,
    max_sessions: usize,
    session_timeout: Duration,
}

impl SessionManager {
    /// Create a new session manager.
    pub fn new(max_sessions: usize, session_timeout: Duration) -> Self {
        Self {
            sessions: DashMap::new(),
            max_sessions,
            session_timeout,
        }
    }

    /// Create a new session and return its ID.
    ///
    /// Returns an error if the maximum number of sessions has been reached.
    pub fn create_session(&self) -> Result<String, String> {
        if self.sessions.len() >= self.max_sessions {
            return Err(format!(
                "maximum number of sessions ({}) reached",
                self.max_sessions
            ));
        }

        let id = Uuid::new_v4().to_string();
        let session = Session::new(id.clone());
        self.sessions.insert(id.clone(), session);
        Ok(id)
    }

    /// Get a clone of a session by ID, updating its last-active timestamp.
    pub fn get_session(&self, id: &str) -> Option<Session> {
        let mut entry = self.sessions.get_mut(id)?;
        entry.touch();
        Some(entry.clone())
    }

    /// Set a session variable (e.g., from `SET key = value`).
    pub fn set_variable(&self, session_id: &str, key: &str, value: &str) {
        if let Some(mut session) = self.sessions.get_mut(session_id) {
            session.settings.insert(key.to_string(), value.to_string());
            session.touch();
        }
    }

    /// Get a session variable.
    pub fn get_variable(&self, session_id: &str, key: &str) -> Option<String> {
        let session = self.sessions.get(session_id)?;
        session.settings.get(key).cloned()
    }

    /// Remove expired sessions and return the number of sessions cleaned up.
    pub fn cleanup_expired(&self) -> usize {
        let timeout = self.session_timeout;
        let before = self.sessions.len();
        self.sessions
            .retain(|_, session| !session.is_expired(timeout));
        before - self.sessions.len()
    }

    /// Return the number of active sessions.
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Remove a specific session by ID.
    pub fn remove_session(&self, id: &str) -> bool {
        self.sessions.remove(id).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_get_session() {
        let mgr = SessionManager::new(10, Duration::from_secs(3600));
        let id = mgr.create_session().unwrap();
        let session = mgr.get_session(&id).unwrap();
        assert_eq!(session.id, id);
        assert_eq!(session.transaction_state, TransactionState::Idle);
        assert!(session.user.is_none());
    }

    #[test]
    fn get_nonexistent_session() {
        let mgr = SessionManager::new(10, Duration::from_secs(3600));
        assert!(mgr.get_session("no-such-session").is_none());
    }

    #[test]
    fn set_and_get_variable() {
        let mgr = SessionManager::new(10, Duration::from_secs(3600));
        let id = mgr.create_session().unwrap();

        mgr.set_variable(&id, "timezone", "UTC");
        assert_eq!(mgr.get_variable(&id, "timezone"), Some("UTC".into()));
        assert_eq!(mgr.get_variable(&id, "missing"), None);
    }

    #[test]
    fn max_sessions_enforced() {
        let mgr = SessionManager::new(2, Duration::from_secs(3600));
        mgr.create_session().unwrap();
        mgr.create_session().unwrap();
        assert!(mgr.create_session().is_err());
    }

    #[test]
    fn cleanup_expired_sessions() {
        let mgr = SessionManager::new(10, Duration::from_millis(1));
        let _id1 = mgr.create_session().unwrap();
        let _id2 = mgr.create_session().unwrap();

        // Wait for sessions to expire.
        std::thread::sleep(Duration::from_millis(10));

        let cleaned = mgr.cleanup_expired();
        assert_eq!(cleaned, 2);
        assert_eq!(mgr.session_count(), 0);
    }

    #[test]
    fn cleanup_keeps_active_sessions() {
        let mgr = SessionManager::new(10, Duration::from_secs(3600));
        let id = mgr.create_session().unwrap();

        let cleaned = mgr.cleanup_expired();
        assert_eq!(cleaned, 0);
        assert_eq!(mgr.session_count(), 1);

        // Session should still be accessible.
        assert!(mgr.get_session(&id).is_some());
    }

    #[test]
    fn remove_session() {
        let mgr = SessionManager::new(10, Duration::from_secs(3600));
        let id = mgr.create_session().unwrap();

        assert!(mgr.remove_session(&id));
        assert!(!mgr.remove_session(&id)); // already removed
        assert!(mgr.get_session(&id).is_none());
    }
}
