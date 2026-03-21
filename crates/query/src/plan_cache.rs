//! Query plan cache with LRU eviction and TTL expiry.
//!
//! Caches optimized query plans keyed by SQL string. Plans are evicted
//! when the cache exceeds `max_entries` (least-recently-used first) or
//! when they exceed the configured TTL.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

use exchange_common::hash::xxh3_64;

use crate::plan::QueryPlan;

/// A cached query plan with usage metadata.
#[allow(dead_code)]
struct CachedPlan {
    plan: QueryPlan,
    created_at: Instant,
    last_accessed: Instant,
    hit_count: u64,
    sql_hash: u64,
    /// Original SQL string, kept for table-name invalidation.
    sql: String,
}

/// Cache statistics exposed for Prometheus metrics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

/// Thread-safe query plan cache with LRU eviction and TTL.
pub struct PlanCache {
    cache: RwLock<HashMap<u64, CachedPlan>>,
    max_entries: usize,
    ttl: Duration,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl PlanCache {
    /// Create a new plan cache with the given capacity and TTL.
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            max_entries,
            ttl,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    /// Create a plan cache with default settings (1000 entries, 5 min TTL).
    pub fn default_config() -> Self {
        Self::new(1000, Duration::from_secs(300))
    }

    /// Hash SQL string using xxHash3 for fast lookup.
    fn hash_sql(sql: &str) -> u64 {
        xxh3_64(sql.as_bytes())
    }

    /// Get a cached plan for the given SQL.
    ///
    /// Returns `Some(plan)` if a non-expired entry exists, updating access time.
    /// Returns `None` on miss or expiry.
    pub fn get(&self, sql: &str) -> Option<QueryPlan> {
        let hash = Self::hash_sql(sql);
        let now = Instant::now();

        // Try read lock first for the common case.
        {
            let cache = self.cache.read().unwrap_or_else(|e| e.into_inner());
            if let Some(entry) = cache.get(&hash) {
                if entry.sql != sql {
                    // Hash collision — treat as miss.
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                if now.duration_since(entry.created_at) > self.ttl {
                    // Expired — will be cleaned up later.
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                // Clone the plan before upgrading the lock.
                let plan = entry.plan.clone();
                drop(cache);

                // Upgrade to write lock to update access time.
                if let Ok(mut cache) = self.cache.write()
                    && let Some(entry) = cache.get_mut(&hash) {
                        entry.last_accessed = now;
                        entry.hit_count += 1;
                    }

                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(plan);
            }
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Store a plan for the given SQL.
    ///
    /// If the cache is full, the least-recently-used entry is evicted.
    pub fn put(&self, sql: &str, plan: QueryPlan) {
        let hash = Self::hash_sql(sql);
        let now = Instant::now();

        let mut cache = self.cache.write().unwrap_or_else(|e| e.into_inner());

        // Evict if at capacity and this is a new key.
        if !cache.contains_key(&hash) && cache.len() >= self.max_entries {
            self.evict_lru(&mut cache);
        }

        cache.insert(
            hash,
            CachedPlan {
                plan,
                created_at: now,
                last_accessed: now,
                hit_count: 0,
                sql_hash: hash,
                sql: sql.to_string(),
            },
        );
    }

    /// Invalidate all plans that reference a given table name.
    ///
    /// Called after DDL operations (CREATE/ALTER/DROP TABLE) to ensure
    /// stale plans are not served.
    pub fn invalidate_table(&self, table_name: &str) {
        let mut cache = self.cache.write().unwrap_or_else(|e| e.into_inner());
        let before = cache.len();
        cache.retain(|_, entry| !plan_references_table(&entry.plan, table_name));
        let removed = before - cache.len();
        if removed > 0 {
            self.evictions.fetch_add(removed as u64, Ordering::Relaxed);
        }
    }

    /// Clear the entire cache.
    pub fn clear(&self) {
        let mut cache = self.cache.write().unwrap_or_else(|e| e.into_inner());
        let count = cache.len();
        cache.clear();
        self.evictions.fetch_add(count as u64, Ordering::Relaxed);
    }

    /// Get current cache statistics.
    pub fn stats(&self) -> CacheStats {
        let cache = self.cache.read().unwrap_or_else(|e| e.into_inner());
        CacheStats {
            entries: cache.len(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    /// Evict all entries whose TTL has expired.
    pub fn evict_expired(&self) {
        let now = Instant::now();
        let mut cache = self.cache.write().unwrap_or_else(|e| e.into_inner());
        let before = cache.len();
        cache.retain(|_, entry| now.duration_since(entry.created_at) <= self.ttl);
        let removed = before - cache.len();
        if removed > 0 {
            self.evictions.fetch_add(removed as u64, Ordering::Relaxed);
        }
    }

    /// Evict the least-recently-used entry from the cache.
    fn evict_lru(&self, cache: &mut HashMap<u64, CachedPlan>) {
        if cache.is_empty() {
            return;
        }
        let lru_key = cache
            .iter()
            .min_by_key(|(_, entry)| entry.last_accessed)
            .map(|(key, _)| *key);
        if let Some(key) = lru_key {
            cache.remove(&key);
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Check if a query plan references a given table name.
fn plan_references_table(plan: &QueryPlan, table_name: &str) -> bool {
    match plan {
        QueryPlan::Select { table, .. } => table == table_name,
        QueryPlan::Insert { table, .. } => table == table_name,
        QueryPlan::Delete { table, .. } => table == table_name,
        QueryPlan::Update { table, .. } => table == table_name,
        QueryPlan::Join { left_table, right_table, .. } => {
            left_table == table_name || right_table == table_name
        }
        QueryPlan::MultiJoin { left, right_table, .. } => {
            right_table == table_name || plan_references_table(left, table_name)
        }
        QueryPlan::AsofJoin { left_table, right_table, .. } => {
            left_table == table_name || right_table == table_name
        }
        QueryPlan::Explain { query } => plan_references_table(query, table_name),
        QueryPlan::ExplainAnalyze { query } => plan_references_table(query, table_name),
        QueryPlan::SetOperation { left, right, .. } => {
            plan_references_table(left, table_name) || plan_references_table(right, table_name)
        }
        QueryPlan::WithCte { ctes, body } => {
            ctes.iter().any(|c| plan_references_table(&c.query, table_name))
                || plan_references_table(body, table_name)
        }
        QueryPlan::DerivedScan { subquery, .. } => plan_references_table(subquery, table_name),
        QueryPlan::CreateTable { name, .. } => name == table_name,
        QueryPlan::DropTable { table, .. } => table == table_name,
        QueryPlan::AddColumn { table, .. } => table == table_name,
        QueryPlan::DropColumn { table, .. } => table == table_name,
        QueryPlan::RenameColumn { table, .. } => table == table_name,
        QueryPlan::SetColumnType { table, .. } => table == table_name,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{SelectColumn, Value, Filter, OrderBy};

    fn sample_plan(table: &str) -> QueryPlan {
        QueryPlan::Select {
            table: table.to_string(),
            columns: vec![SelectColumn::Wildcard],
            filter: None,
            order_by: vec![],
            limit: None,
            offset: None,
            sample_by: None,
            latest_on: None,
            group_by: vec![],
            group_by_mode: crate::plan::GroupByMode::Normal,
            having: None,
            distinct: false,
            distinct_on: vec![],
        }
    }

    #[test]
    fn put_and_get_returns_same_plan() {
        let cache = PlanCache::new(100, Duration::from_secs(300));
        let plan = sample_plan("trades");
        let sql = "SELECT * FROM trades";

        cache.put(sql, plan.clone());
        let cached = cache.get(sql);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap(), plan);
    }

    #[test]
    fn get_miss_returns_none() {
        let cache = PlanCache::new(100, Duration::from_secs(300));
        assert!(cache.get("SELECT 1").is_none());

        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 0);
    }

    #[test]
    fn ttl_expiry() {
        // Use a very short TTL.
        let cache = PlanCache::new(100, Duration::from_millis(1));
        let plan = sample_plan("trades");
        let sql = "SELECT * FROM trades";

        cache.put(sql, plan);

        // Wait for TTL to expire.
        std::thread::sleep(Duration::from_millis(5));

        assert!(cache.get(sql).is_none());

        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn ttl_evict_expired() {
        let cache = PlanCache::new(100, Duration::from_millis(1));
        cache.put("SELECT 1", sample_plan("t1"));
        cache.put("SELECT 2", sample_plan("t2"));

        std::thread::sleep(Duration::from_millis(5));

        cache.evict_expired();

        let stats = cache.stats();
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.evictions, 2);
    }

    #[test]
    fn table_invalidation_clears_relevant_entries() {
        let cache = PlanCache::new(100, Duration::from_secs(300));
        cache.put("SELECT * FROM trades", sample_plan("trades"));
        cache.put("SELECT * FROM orders", sample_plan("orders"));
        cache.put("SELECT * FROM quotes", sample_plan("quotes"));

        cache.invalidate_table("trades");

        assert!(cache.get("SELECT * FROM trades").is_none());
        assert!(cache.get("SELECT * FROM orders").is_some());
        assert!(cache.get("SELECT * FROM quotes").is_some());

        let stats = cache.stats();
        assert_eq!(stats.entries, 2);
    }

    #[test]
    fn max_entries_eviction() {
        let cache = PlanCache::new(2, Duration::from_secs(300));

        cache.put("SELECT 1", sample_plan("t1"));
        cache.put("SELECT 2", sample_plan("t2"));

        assert_eq!(cache.stats().entries, 2);

        // Adding a third should evict one.
        cache.put("SELECT 3", sample_plan("t3"));

        assert_eq!(cache.stats().entries, 2);
        assert_eq!(cache.stats().evictions, 1);

        // The newest entry should be present.
        assert!(cache.get("SELECT 3").is_some());
    }

    #[test]
    fn clear_empties_cache() {
        let cache = PlanCache::new(100, Duration::from_secs(300));
        cache.put("SELECT 1", sample_plan("t1"));
        cache.put("SELECT 2", sample_plan("t2"));

        cache.clear();

        assert_eq!(cache.stats().entries, 0);
        assert_eq!(cache.stats().evictions, 2);
    }

    #[test]
    fn hit_counter_increments() {
        let cache = PlanCache::new(100, Duration::from_secs(300));
        cache.put("SELECT 1", sample_plan("t1"));

        cache.get("SELECT 1");
        cache.get("SELECT 1");
        cache.get("SELECT 1");

        let stats = cache.stats();
        assert_eq!(stats.hits, 3);
    }
}
