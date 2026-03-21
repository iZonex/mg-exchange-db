//! Cursor implementations for the pull-based execution engine.

pub mod advanced;
pub mod aggregate;
pub mod anti_join;
pub mod approximate_aggregate;
pub mod asof_join;
pub mod async_scan;
pub mod band_join;
pub mod broadcast_join;
pub mod buffer;
pub mod builder;
pub mod cache;
pub mod case_when;
pub mod coalesce;
pub mod column_scan;
pub mod concat;
pub mod constant;
pub mod count_only;
pub mod cross_join;
pub mod cube;
pub mod debug;
pub mod deferred_filter;
pub mod distinct;
pub mod empty;
pub mod except;
pub mod explain;
pub mod expression;
pub mod fill;
pub mod filter;
pub mod filtered_scan;
pub mod generate_series;
pub mod group_by_hash;
pub mod group_by_sorted;
pub mod hash_join;
pub mod having_filter;
pub mod incremental_aggregate;
pub mod index_join;
pub mod index_scan;
pub mod intersect;
pub mod latest_by;
pub mod limit;
pub mod mark_join;
pub mod memory;
pub mod merge_sort;
pub mod nested_loop_join;
pub mod null_scan;
pub mod nullif;
pub mod page_frame;
pub mod parallel_aggregate;
pub mod parallel_scan;
pub mod pivoted_aggregate;
pub mod progress;
pub mod project;
pub mod rate_limit;
pub mod rename;
pub mod reverse_scan;
pub mod rollup;
pub mod row_id;
pub mod sample_by;
pub mod sampled_scan;
pub mod scan;
pub mod semi_join;
pub mod sort;
pub mod sort_merge_join;
pub mod spill;
pub mod stats;
pub mod streaming_aggregate;
pub mod symbol_filter_scan;
pub mod tee;
pub mod timeout;
pub mod timestamp_range_scan;
pub mod topk;
pub mod type_cast;
pub mod union;
pub mod values;
pub mod window;
pub mod window_join;

// Advanced cursors (50 additional strategies)
pub use advanced::{
    // Specialized scan cursors
    PartitionPrunedScanCursor,
    IndexedSymbolScanCursor,
    TopNScanCursor,
    SkipScanCursor,
    ZeroCopyScanCursor,
    CompressedScanCursor,
    TieredScanCursor,
    PredicatePushdownScanCursor,
    ProjectPushdownScanCursor,
    BatchPrefetchScanCursor,
    // Specialized join cursors
    AsofJoinIndexedCursor,
    LookupJoinCursor,
    PartitionWiseJoinCursor,
    AdaptiveJoinCursor,
    ParallelHashJoinCursor,
    GraceHashJoinCursor,
    SkewedJoinCursor,
    SemiHashJoinCursor,
    AntiHashJoinCursor,
    MultiJoinCursor,
    // Specialized aggregate cursors
    PartialAggregateCursor,
    MergeAggregateCursor,
    DistinctAggregateCursor,
    FilteredAggregateCursor,
    OrderedAggregateCursor,
    GroupingSetsAggregateCursor,
    TopKAggregateCursor,
    StreamingCountCursor,
    MinMaxOnlyCursor,
    RunningTotalCursor,
    // Specialized output cursors
    CsvOutputCursor,
    JsonOutputCursor,
    NdjsonOutputCursor,
    ParquetOutputCursor,
    InsertOutputCursor,
    UpdateOutputCursor,
    DeleteOutputCursor,
    CountOutputCursor,
    HashOutputCursor,
    ChecksumOutputCursor,
    // Specialized transform cursors
    PivotCursor,
    UnpivotCursor,
    DeduplicateCursor,
    InterpolateCursor,
    NormalizeCursor,
    ZScoreCursor,
    RankCursor,
    RowHashCursor,
    SplitCursor,
    FlattenCursor,
};

pub use aggregate::AggregateCursor;
pub use anti_join::AntiJoinCursor;
pub use approximate_aggregate::ApproxAggregateCursor;
pub use asof_join::AsofJoinCursor;
pub use async_scan::AsyncScanCursor;
pub use band_join::BandJoinCursor;
pub use broadcast_join::BroadcastJoinCursor;
pub use buffer::BufferCursor;
pub use builder::build_cursor;
pub use cache::CachedCursor;
pub use case_when::{CaseWhenCursor, WhenBranch};
pub use coalesce::CoalesceCursor;
pub use column_scan::ColumnOnlyScanCursor;
pub use concat::ConcatCursor;
pub use constant::ConstantCursor;
pub use count_only::CountOnlyCursor;
pub use cross_join::CrossJoinCursor;
pub use cube::CubeCursor;
pub use debug::DebugCursor;
pub use deferred_filter::DeferredFilterCursor;
pub use distinct::DistinctCursor;
pub use empty::EmptyCursor;
pub use except::ExceptCursor;
pub use explain::ExplainCursor;
pub use expression::{ExprOp, ExpressionCursor};
pub use fill::{FillCursor, FillStrategy};
pub use filter::FilterCursor;
pub use filtered_scan::FilteredScanCursor;
pub use generate_series::GenerateSeriesCursor;
pub use group_by_hash::{HashAggOp, HashGroupByCursor};
pub use group_by_sorted::SortedGroupByCursor;
pub use hash_join::HashJoinCursor;
pub use having_filter::HavingFilterCursor;
pub use incremental_aggregate::IncrementalAggregateCursor;
pub use index_join::IndexJoinCursor;
pub use index_scan::IndexScanCursor;
pub use intersect::IntersectCursor;
pub use latest_by::LatestByCursor;
pub use limit::LimitCursor;
pub use mark_join::MarkJoinCursor;
pub use memory::MemoryCursor;
pub use merge_sort::MergeSortCursor;
pub use nested_loop_join::NestedLoopJoinCursor;
pub use null_scan::NullScanCursor;
pub use nullif::NullIfCursor;
pub use page_frame::PageFrameCursor;
pub use parallel_aggregate::ParallelAggregateCursor;
pub use parallel_scan::ParallelScanCursor;
pub use pivoted_aggregate::PivotedAggregateCursor;
pub use progress::ProgressCursor;
pub use project::ProjectCursor;
pub use rate_limit::RateLimitCursor;
pub use rename::RenameCursor;
pub use reverse_scan::ReverseScanCursor;
pub use rollup::RollupCursor;
pub use row_id::RowIdCursor;
pub use sample_by::SampleByCursor;
pub use sampled_scan::SampledScanCursor;
pub use scan::ScanCursor;
pub use semi_join::SemiJoinCursor;
pub use sort::SortCursor;
pub use sort_merge_join::SortMergeJoinCursor;
pub use spill::SpillCursor;
pub use stats::StatsCursor;
pub use streaming_aggregate::StreamingAggregateCursor;
pub use symbol_filter_scan::SymbolFilterScanCursor;
pub use tee::TeeCursor;
pub use timeout::TimeoutCursor;
pub use timestamp_range_scan::TimestampRangeScanCursor;
pub use topk::TopKCursor;
pub use type_cast::TypeCastCursor;
pub use union::{UnionCursor, UnionDistinctCursor};
pub use values::ValuesCursor;
pub use window::WindowCursor;
pub use window_join::WindowJoinCursor;
