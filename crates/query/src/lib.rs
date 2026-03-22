//! ExchangeDB SQL Query Engine.
//!
//! Provides SQL parsing, planning, and execution for the ExchangeDB
//! time-series database. Supports standard SQL (CREATE TABLE, INSERT,
//! SELECT with WHERE/ORDER BY/LIMIT) plus a custom `SAMPLE BY` extension
//! for time-bucketed aggregation.

pub mod adaptive;
pub mod asof;
pub mod balance_functions;
pub mod batch;
pub mod casts;
pub mod catalog;
pub mod columnar;
pub mod compiled_filter;
pub mod context;
pub mod cursor_executor;
pub mod cursors;
pub mod exchange_functions;
pub mod executor;
pub mod functions;
pub mod functions_compat;
pub mod functions_extra;
pub mod join;
pub mod latest;
pub mod latest_indexed;
pub mod memory;
pub mod optimizer;
pub mod parallel;
pub mod parallel_groupby;
pub mod parallel_sort;
pub mod parser;
pub mod pipeline;
pub mod plan;
pub mod plan_cache;
pub mod planner;
pub mod profiler;
pub mod record_cursor;
pub mod scalar;
pub mod sequence;
pub mod slow_log;
pub mod spill;
pub mod table_registry;
pub mod timeout;
pub mod value;
pub mod vector_groupby;
pub mod window;

/// Test utilities module. Available for integration tests.
pub mod test_utils;

// Re-export key types for convenience.
pub use context::{CancellationToken, ExecutionContext, QueryRegistry};
pub use cursor_executor::{CursorEngineConfig, execute_via_cursors, execute_with_engine};
pub use executor::{execute, execute_with_context, execute_with_wal};
pub use plan::{QueryPlan, QueryResult, Value};
pub use plan_cache::PlanCache;
pub use planner::plan_query;
pub use profiler::QueryProfiler;
pub use record_cursor::RecordCursor;
pub use slow_log::SlowQueryLog;
