//! Row-Level Security (RLS) for ExchangeDB.
//!
//! Allows defining per-user policies that restrict which rows a user
//! can see based on column values.

use std::collections::HashMap;

/// A row-level security policy that restricts access to rows where
/// a specific column contains one of the allowed values.
#[derive(Debug, Clone)]
pub struct RowLevelPolicy {
    /// The table this policy applies to.
    pub table: String,
    /// The column to check.
    pub column: String,
    /// Values this user is permitted to see.
    pub allowed_values: Vec<String>,
}

/// A filter derived from row-level policies that should be applied
/// when querying a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filter {
    /// Column to filter on.
    pub column: String,
    /// Only rows where the column value is in this set should be returned.
    pub allowed_values: Vec<String>,
}

impl Filter {
    /// Generate a SQL-style WHERE clause for this filter.
    pub fn to_where_clause(&self) -> String {
        if self.allowed_values.is_empty() {
            return "1=0".to_string(); // deny all
        }
        if self.allowed_values.len() == 1 {
            return format!("{} = '{}'", self.column, self.allowed_values[0]);
        }
        let values: Vec<String> = self
            .allowed_values
            .iter()
            .map(|v| format!("'{v}'"))
            .collect();
        format!("{} IN ({})", self.column, values.join(", "))
    }
}

/// Manages row-level security policies for users.
pub struct RlsManager {
    /// Map of user -> list of policies.
    policies: HashMap<String, Vec<RowLevelPolicy>>,
}

impl RlsManager {
    /// Create a new empty RLS manager.
    pub fn new() -> Self {
        Self {
            policies: HashMap::new(),
        }
    }

    /// Add a row-level policy for a user/role.
    pub fn add_policy(&mut self, user: &str, policy: RowLevelPolicy) {
        self.policies
            .entry(user.to_string())
            .or_default()
            .push(policy);
    }

    /// Remove all policies for a user on a specific table.
    pub fn remove_policies(&mut self, user: &str, table: &str) {
        if let Some(policies) = self.policies.get_mut(user) {
            policies.retain(|p| p.table != table);
        }
    }

    /// Get the filter to apply for a user on a table.
    ///
    /// If the user has a policy for this table, returns a `Filter`
    /// restricting the visible rows. If no policy exists, returns `None`
    /// (no restriction).
    pub fn get_filter(&self, user: &str, table: &str) -> Option<Filter> {
        let user_policies = self.policies.get(user)?;
        let matching: Vec<&RowLevelPolicy> =
            user_policies.iter().filter(|p| p.table == table).collect();

        if matching.is_empty() {
            return None;
        }

        // If multiple policies exist for the same table, use the first one.
        // A more sophisticated implementation could merge them.
        let policy = matching[0];
        Some(Filter {
            column: policy.column.clone(),
            allowed_values: policy.allowed_values.clone(),
        })
    }

    /// List all users with policies.
    pub fn users_with_policies(&self) -> Vec<String> {
        let mut users: Vec<String> = self.policies.keys().cloned().collect();
        users.sort();
        users
    }

    /// Check if a specific value is allowed for a user on a table/column.
    pub fn is_value_allowed(&self, user: &str, table: &str, column: &str, value: &str) -> bool {
        let user_policies = match self.policies.get(user) {
            Some(p) => p,
            None => return true, // No policy means no restriction.
        };

        let matching = user_policies
            .iter()
            .find(|p| p.table == table && p.column == column);

        match matching {
            Some(policy) => policy.allowed_values.iter().any(|v| v == value),
            None => true, // No policy for this table/column means allowed.
        }
    }
}

impl Default for RlsManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_policy_returns_none() {
        let mgr = RlsManager::new();
        assert!(mgr.get_filter("alice", "trades").is_none());
    }

    #[test]
    fn add_policy_and_get_filter() {
        let mut mgr = RlsManager::new();
        mgr.add_policy(
            "alice",
            RowLevelPolicy {
                table: "trades".into(),
                column: "exchange".into(),
                allowed_values: vec!["NYSE".into(), "NASDAQ".into()],
            },
        );

        let filter = mgr.get_filter("alice", "trades").unwrap();
        assert_eq!(filter.column, "exchange");
        assert_eq!(filter.allowed_values, vec!["NYSE", "NASDAQ"]);
    }

    #[test]
    fn filter_does_not_apply_to_other_tables() {
        let mut mgr = RlsManager::new();
        mgr.add_policy(
            "alice",
            RowLevelPolicy {
                table: "trades".into(),
                column: "exchange".into(),
                allowed_values: vec!["NYSE".into()],
            },
        );

        assert!(mgr.get_filter("alice", "orders").is_none());
    }

    #[test]
    fn filter_does_not_apply_to_other_users() {
        let mut mgr = RlsManager::new();
        mgr.add_policy(
            "alice",
            RowLevelPolicy {
                table: "trades".into(),
                column: "exchange".into(),
                allowed_values: vec!["NYSE".into()],
            },
        );

        assert!(mgr.get_filter("bob", "trades").is_none());
    }

    #[test]
    fn where_clause_single_value() {
        let filter = Filter {
            column: "exchange".into(),
            allowed_values: vec!["NYSE".into()],
        };
        assert_eq!(filter.to_where_clause(), "exchange = 'NYSE'");
    }

    #[test]
    fn where_clause_multiple_values() {
        let filter = Filter {
            column: "exchange".into(),
            allowed_values: vec!["NYSE".into(), "NASDAQ".into()],
        };
        assert_eq!(filter.to_where_clause(), "exchange IN ('NYSE', 'NASDAQ')");
    }

    #[test]
    fn where_clause_empty_values() {
        let filter = Filter {
            column: "exchange".into(),
            allowed_values: vec![],
        };
        assert_eq!(filter.to_where_clause(), "1=0");
    }

    #[test]
    fn is_value_allowed_with_policy() {
        let mut mgr = RlsManager::new();
        mgr.add_policy(
            "alice",
            RowLevelPolicy {
                table: "trades".into(),
                column: "exchange".into(),
                allowed_values: vec!["NYSE".into()],
            },
        );

        assert!(mgr.is_value_allowed("alice", "trades", "exchange", "NYSE"));
        assert!(!mgr.is_value_allowed("alice", "trades", "exchange", "LSE"));
    }

    #[test]
    fn is_value_allowed_without_policy() {
        let mgr = RlsManager::new();
        // No policy means no restriction.
        assert!(mgr.is_value_allowed("alice", "trades", "exchange", "NYSE"));
    }

    #[test]
    fn remove_policies() {
        let mut mgr = RlsManager::new();
        mgr.add_policy(
            "alice",
            RowLevelPolicy {
                table: "trades".into(),
                column: "exchange".into(),
                allowed_values: vec!["NYSE".into()],
            },
        );
        mgr.add_policy(
            "alice",
            RowLevelPolicy {
                table: "orders".into(),
                column: "region".into(),
                allowed_values: vec!["US".into()],
            },
        );

        mgr.remove_policies("alice", "trades");
        assert!(mgr.get_filter("alice", "trades").is_none());
        // Orders policy should remain.
        assert!(mgr.get_filter("alice", "orders").is_some());
    }

    #[test]
    fn users_with_policies_lists_all() {
        let mut mgr = RlsManager::new();
        mgr.add_policy(
            "bob",
            RowLevelPolicy {
                table: "t".into(),
                column: "c".into(),
                allowed_values: vec![],
            },
        );
        mgr.add_policy(
            "alice",
            RowLevelPolicy {
                table: "t".into(),
                column: "c".into(),
                allowed_values: vec![],
            },
        );

        let users = mgr.users_with_policies();
        assert_eq!(users, vec!["alice", "bob"]);
    }
}
