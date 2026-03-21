//! Query plan types for the ExchangeDB query engine.

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

/// A value that can appear in query plans and results.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    Null,
    I64(i64),
    F64(f64),
    Str(String),
    /// Nanosecond timestamp since Unix epoch.
    Timestamp(i64),
}

impl Value {
    /// Compare two values with numeric type coercion (I64 <-> F64).
    ///
    /// The derived `PartialOrd` compares by discriminant first, which
    /// makes cross-type comparisons (e.g. `F64(1.0) > I64(5)`) incorrect.
    /// This method promotes `I64` to `F64` when the other operand is `F64`
    /// so that the comparison is numerically correct.
    #[inline(always)]
    pub fn cmp_coerce(&self, other: &Value) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::I64(a), Value::F64(b)) => (*a as f64).partial_cmp(b),
            (Value::F64(a), Value::I64(b)) => a.partial_cmp(&(*b as f64)),
            (Value::Timestamp(a), Value::I64(b)) => a.partial_cmp(b),
            (Value::I64(a), Value::Timestamp(b)) => a.partial_cmp(b),
            (Value::Timestamp(a), Value::F64(b)) => (*a as f64).partial_cmp(b),
            (Value::F64(a), Value::Timestamp(b)) => a.partial_cmp(&(*b as f64)),
            _ => self.partial_cmp(other),
        }
    }

    /// Equality with numeric type coercion.
    #[inline(always)]
    pub fn eq_coerce(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::I64(a), Value::F64(b)) => (*a as f64) == *b,
            (Value::F64(a), Value::I64(b)) => *a == (*b as f64),
            (Value::Timestamp(a), Value::I64(b)) => a == b,
            (Value::I64(a), Value::Timestamp(b)) => a == b,
            _ => self == other,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::I64(v) => write!(f, "{v}"),
            Value::F64(v) => write!(f, "{v}"),
            Value::Str(s) => write!(f, "'{s}'"),
            Value::Timestamp(ns) => {
                let secs = ns / 1_000_000_000;
                let sub_ns = (ns % 1_000_000_000) as u32;
                if let Some(dt) = chrono_format(secs, sub_ns) {
                    write!(f, "{dt}")
                } else {
                    write!(f, "{ns}")
                }
            }
        }
    }
}

fn chrono_format(_secs: i64, _sub_ns: u32) -> Option<String> {
    // Lightweight display without pulling in chrono; just show nanos.
    None
}

/// An argument to a scalar function call: either a column reference or a literal.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectColumnArg {
    /// A column name reference.
    Column(String),
    /// A literal value.
    Literal(Value),
}

/// Column selection in a SELECT query.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectColumn {
    /// A bare column name.
    Name(String),
    /// Wildcard `*`.
    Wildcard,
    /// An aggregate function call: e.g. `sum(price)`.
    Aggregate {
        function: AggregateKind,
        column: String,
        alias: Option<String>,
        /// Optional FILTER (WHERE ...) clause on this aggregate.
        filter: Option<Box<Filter>>,
        /// Optional WITHIN GROUP (ORDER BY ...) for ordered-set aggregates.
        within_group_order: Option<Vec<OrderBy>>,
        /// Optional expression to evaluate before aggregating (e.g. sum(d * 2.0)).
        arg_expr: Option<PlanExpr>,
    },
    /// A window function call with OVER clause.
    WindowFunction(crate::window::WindowFunction),
    /// A scalar function call: e.g. `upper(symbol)`, `round(price, 2)`.
    ScalarFunction {
        name: String,
        args: Vec<SelectColumnArg>,
    },
    /// A scalar subquery in SELECT: (SELECT count(*) FROM t).
    ScalarSubquery {
        subquery: Box<QueryPlan>,
        alias: Option<String>,
    },
    /// CASE WHEN expression.
    CaseWhen {
        conditions: Vec<(Filter, Value)>,
        else_value: Option<Value>,
        alias: Option<String>,
        /// Extended conditions with PlanExpr results (for cases like CASE WHEN x > 5 THEN y * 2 ...).
        expr_conditions: Option<Vec<(Filter, PlanExpr)>>,
        expr_else: Option<PlanExpr>,
    },
    /// A computed expression: e.g. `price * volume AS notional`.
    Expression {
        expr: PlanExpr,
        alias: Option<String>,
    },
}

/// Supported aggregate function kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregateKind {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    First,
    Last,
    StdDev,
    Variance,
    Median,
    CountDistinct,
    StringAgg,
    PercentileCont,
    PercentileDisc,
    Mode,
    Corr,
    CovarPop,
    CovarSamp,
    RegrSlope,
    RegrIntercept,
    BoolAnd,
    BoolOr,
    ArrayAgg,
    // Financial aggregates
    Vwap,
    Ema,
    Sma,
    Wma,
    Rsi,
    MacdSignal,
    BollingerUpper,
    BollingerLower,
    Atr,
    Drawdown,
    Twap,
    RealizedVol,
    SharpeRatio,
    OrderImbalance,
    TradeFlow,
    PriceImpact,
    Ohlcv,
    // Type-specific / precision aggregates
    SumDouble,
    SumLong,
    AvgDouble,
    MinLong,
    MaxLong,
    Ksum,
    Nsum,
    ApproxCountDistinct,
    // Sample variants
    StdDevSamp,
    VarianceSamp,
    // Per-type aggregate variants (QuestDB compat)
    SumInt,
    SumFloat,
    AvgInt,
    AvgLong,
    AvgFloat,
    MinInt,
    MinFloat,
    MinDouble,
    MinDate,
    MinTimestamp,
    MaxInt,
    MaxFloat,
    MaxDouble,
    MaxDate,
    MaxTimestamp,
    CountInt,
    CountLong,
    CountDouble,
    CountFloat,
    CountStr,
    FirstInt,
    FirstLong,
    FirstFloat,
    FirstDouble,
    FirstStr,
    FirstDate,
    FirstTimestamp,
    LastInt,
    LastLong,
    LastFloat,
    LastDouble,
    LastStr,
    LastDate,
    LastTimestamp,
    SumByte,
    SumShort,
    AvgByte,
    AvgShort,
    MinByte,
    MinShort,
    MaxByte,
    MaxShort,
    StdDevInt,
    StdDevLong,
    StdDevFloat,
    StdDevDouble,
    VarianceInt,
    VarianceLong,
    VarianceFloat,
    VarianceDouble,
    // Median per-type variants
    MedianInt,
    MedianLong,
    MedianFloat,
    MedianDouble,
    // CountDistinct per-type variants
    CountDistinctInt,
    CountDistinctLong,
    CountDistinctDouble,
    CountDistinctStr,
    // StringAgg per-type
    StringAggStr,
    // Ksum/Nsum per-type
    KsumDouble,
    KsumFloat,
    NsumDouble,
    NsumFloat,
    // Window function extras
    NthValue,
    Ntile,
    PercentRank,
    CumeDist,
}

impl AggregateKind {
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "sum" => Some(Self::Sum),
            "avg" => Some(Self::Avg),
            "min" => Some(Self::Min),
            "max" => Some(Self::Max),
            "count" => Some(Self::Count),
            "first" => Some(Self::First),
            "last" => Some(Self::Last),
            "stddev" => Some(Self::StdDev),
            "variance" => Some(Self::Variance),
            "median" => Some(Self::Median),
            "count_distinct" => Some(Self::CountDistinct),
            "string_agg" => Some(Self::StringAgg),
            "percentile_cont" => Some(Self::PercentileCont),
            "percentile_disc" => Some(Self::PercentileDisc),
            "mode" => Some(Self::Mode),
            "corr" => Some(Self::Corr),
            "covar_pop" => Some(Self::CovarPop),
            "covar_samp" => Some(Self::CovarSamp),
            "regr_slope" => Some(Self::RegrSlope),
            "regr_intercept" => Some(Self::RegrIntercept),
            "bool_and" => Some(Self::BoolAnd),
            "bool_or" => Some(Self::BoolOr),
            "array_agg" => Some(Self::ArrayAgg),
            // Financial aggregates
            "vwap" => Some(Self::Vwap),
            "ema" => Some(Self::Ema),
            "sma" => Some(Self::Sma),
            "wma" => Some(Self::Wma),
            "rsi" => Some(Self::Rsi),
            "macd_signal" => Some(Self::MacdSignal),
            "bollinger_upper" => Some(Self::BollingerUpper),
            "bollinger_lower" => Some(Self::BollingerLower),
            "atr" => Some(Self::Atr),
            "drawdown" | "max_drawdown" => Some(Self::Drawdown),
            "twap" => Some(Self::Twap),
            "realized_vol" | "realized_volatility" => Some(Self::RealizedVol),
            "sharpe" | "sharpe_ratio" => Some(Self::SharpeRatio),
            "order_imbalance" => Some(Self::OrderImbalance),
            "trade_flow" | "buy_ratio" => Some(Self::TradeFlow),
            "price_impact" | "slippage" => Some(Self::PriceImpact),
            "ohlcv" => Some(Self::Ohlcv),
            // Type-specific / precision aggregates
            "sum_double" => Some(Self::SumDouble),
            "sum_long" => Some(Self::SumLong),
            "avg_double" => Some(Self::AvgDouble),
            "min_long" => Some(Self::MinLong),
            "max_long" => Some(Self::MaxLong),
            "ksum" => Some(Self::Ksum),
            "nsum" => Some(Self::Nsum),
            "approx_count_distinct" => Some(Self::ApproxCountDistinct),
            // Sample variants
            "stddev_samp" => Some(Self::StdDevSamp),
            "variance_samp" | "var_samp" => Some(Self::VarianceSamp),
            // Per-type sum variants
            "sum_int" => Some(Self::SumInt),
            "sum_float" => Some(Self::SumFloat),
            "sum_byte" => Some(Self::SumByte),
            "sum_short" => Some(Self::SumShort),
            // Per-type avg variants
            "avg_int" => Some(Self::AvgInt),
            "avg_long" => Some(Self::AvgLong),
            "avg_float" => Some(Self::AvgFloat),
            "avg_byte" => Some(Self::AvgByte),
            "avg_short" => Some(Self::AvgShort),
            // Per-type min variants
            "min_int" => Some(Self::MinInt),
            "min_float" => Some(Self::MinFloat),
            "min_double" => Some(Self::MinDouble),
            "min_date" => Some(Self::MinDate),
            "min_timestamp" => Some(Self::MinTimestamp),
            "min_byte" => Some(Self::MinByte),
            "min_short" => Some(Self::MinShort),
            // Per-type max variants
            "max_int" => Some(Self::MaxInt),
            "max_float" => Some(Self::MaxFloat),
            "max_double" => Some(Self::MaxDouble),
            "max_date" => Some(Self::MaxDate),
            "max_timestamp" => Some(Self::MaxTimestamp),
            "max_byte" => Some(Self::MaxByte),
            "max_short" => Some(Self::MaxShort),
            // Per-type count variants
            "count_int" => Some(Self::CountInt),
            "count_long" => Some(Self::CountLong),
            "count_double" => Some(Self::CountDouble),
            "count_float" => Some(Self::CountFloat),
            "count_str" => Some(Self::CountStr),
            // Per-type first variants
            "first_int" => Some(Self::FirstInt),
            "first_long" => Some(Self::FirstLong),
            "first_float" => Some(Self::FirstFloat),
            "first_double" => Some(Self::FirstDouble),
            "first_str" | "first_string" => Some(Self::FirstStr),
            "first_date" => Some(Self::FirstDate),
            "first_timestamp" => Some(Self::FirstTimestamp),
            // Per-type last variants
            "last_int" => Some(Self::LastInt),
            "last_long" => Some(Self::LastLong),
            "last_float" => Some(Self::LastFloat),
            "last_double" => Some(Self::LastDouble),
            "last_str" | "last_string" => Some(Self::LastStr),
            "last_date" => Some(Self::LastDate),
            "last_timestamp" => Some(Self::LastTimestamp),
            // Per-type stddev variants
            "stddev_int" => Some(Self::StdDevInt),
            "stddev_long" => Some(Self::StdDevLong),
            "stddev_float" => Some(Self::StdDevFloat),
            "stddev_double" => Some(Self::StdDevDouble),
            // Per-type variance variants
            "variance_int" | "var_int" => Some(Self::VarianceInt),
            "variance_long" | "var_long" => Some(Self::VarianceLong),
            "variance_float" | "var_float" => Some(Self::VarianceFloat),
            "variance_double" | "var_double" => Some(Self::VarianceDouble),
            // Per-type median variants
            "median_int" => Some(Self::MedianInt),
            "median_long" => Some(Self::MedianLong),
            "median_float" => Some(Self::MedianFloat),
            "median_double" => Some(Self::MedianDouble),
            // Per-type count_distinct variants
            "count_distinct_int" => Some(Self::CountDistinctInt),
            "count_distinct_long" => Some(Self::CountDistinctLong),
            "count_distinct_double" => Some(Self::CountDistinctDouble),
            "count_distinct_str" | "count_distinct_string" => Some(Self::CountDistinctStr),
            // StringAgg per-type
            "string_agg_str" | "string_agg_string" => Some(Self::StringAggStr),
            // Ksum/Nsum per-type
            "ksum_double" => Some(Self::KsumDouble),
            "ksum_float" => Some(Self::KsumFloat),
            "nsum_double" => Some(Self::NsumDouble),
            "nsum_float" => Some(Self::NsumFloat),
            // Window function extras
            "nth_value" => Some(Self::NthValue),
            "ntile" => Some(Self::Ntile),
            "percent_rank" => Some(Self::PercentRank),
            "cume_dist" => Some(Self::CumeDist),
            _ => None,
        }
    }
}

/// Binary arithmetic/string/comparison operator for expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Concat,
    // Comparison operators (return I64 0/1 as boolean)
    Gt,
    Lt,
    Gte,
    Lte,
    Eq,
    NotEq,
    And,
    Or,
}

/// Unary operator for expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
}

/// A general-purpose expression tree used in SELECT and WHERE clauses.
#[derive(Debug, Clone, PartialEq)]
pub enum PlanExpr {
    /// A column reference.
    Column(String),
    /// A literal value.
    Literal(Value),
    /// A binary operation: `left op right`.
    BinaryOp {
        left: Box<PlanExpr>,
        op: BinaryOp,
        right: Box<PlanExpr>,
    },
    /// A unary operation: `op expr`.
    UnaryOp { op: UnaryOp, expr: Box<PlanExpr> },
    /// A function call: `name(args...)`.
    Function { name: String, args: Vec<PlanExpr> },
}

impl PlanExpr {
    /// Collect all column names referenced by this expression.
    pub fn collect_columns(&self, out: &mut Vec<String>) {
        match self {
            PlanExpr::Column(name) => out.push(name.clone()),
            PlanExpr::Literal(_) => {}
            PlanExpr::BinaryOp { left, right, .. } => {
                left.collect_columns(out);
                right.collect_columns(out);
            }
            PlanExpr::UnaryOp { expr, .. } => expr.collect_columns(out),
            PlanExpr::Function { args, .. } => {
                for arg in args {
                    arg.collect_columns(out);
                }
            }
        }
    }
}

/// Comparison operator for scalar subqueries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    NotEq,
    Gt,
    Lt,
    Gte,
    Lte,
}

/// A filter condition in a WHERE clause.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    Eq(String, Value),
    NotEq(String, Value),
    Gt(String, Value),
    Lt(String, Value),
    Gte(String, Value),
    Lte(String, Value),
    Between(String, Value, Value),
    /// BETWEEN SYMMETRIC: auto-swaps low/high if needed.
    BetweenSymmetric(String, Value, Value),
    And(Vec<Filter>),
    Or(Vec<Filter>),
    /// Logical NOT: negates the inner filter.
    Not(Box<Filter>),
    /// IS NULL check.
    IsNull(String),
    /// IS NOT NULL check.
    IsNotNull(String),
    /// IN list check: column IN (val1, val2, ...).
    In(String, Vec<Value>),
    /// NOT IN list check.
    NotIn(String, Vec<Value>),
    /// LIKE pattern match: column LIKE pattern.
    Like(String, String),
    /// NOT LIKE pattern match.
    NotLike(String, String),
    /// Case-insensitive LIKE: column ILIKE pattern.
    ILike(String, String),
    /// Scalar subquery comparison: `column op (SELECT ...)`.
    Subquery {
        column: String,
        op: CompareOp,
        subquery: Box<QueryPlan>,
    },
    /// IN subquery: `column IN (SELECT ...)` or `column NOT IN (SELECT ...)`.
    InSubquery {
        column: String,
        subquery: Box<QueryPlan>,
        negated: bool,
    },
    /// EXISTS subquery: `EXISTS (SELECT ...)` or `NOT EXISTS (SELECT ...)`.
    Exists {
        subquery: Box<QueryPlan>,
        negated: bool,
    },
    /// Expression comparison: `expr op expr` (e.g. `price * volume > 100000`).
    Expression {
        left: PlanExpr,
        op: CompareOp,
        right: PlanExpr,
    },
    /// ALL subquery: `column op ALL (SELECT ...)`.
    All {
        column: String,
        op: CompareOp,
        subquery: Box<QueryPlan>,
    },
    /// ANY subquery: `column op ANY (SELECT ...)`.
    Any {
        column: String,
        op: CompareOp,
        subquery: Box<QueryPlan>,
    },
}

/// ORDER BY specification.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    pub column: String,
    pub descending: bool,
}

/// Fill mode for SAMPLE BY empty intervals.
#[derive(Debug, Clone, PartialEq)]
pub enum FillMode {
    /// Skip empty intervals (default behavior).
    None,
    /// Fill with NULL values.
    Null,
    /// Carry forward last known value.
    Prev,
    /// Fill with a constant value.
    Value(Value),
    /// Linear interpolation between surrounding known values.
    Linear,
}

/// Mode of GROUP BY aggregation.
#[derive(Debug, Clone, PartialEq)]
pub enum GroupByMode {
    /// Normal GROUP BY (one set of grouping expressions).
    Normal,
    /// GROUPING SETS: each inner Vec is one grouping set.
    GroupingSets(Vec<Vec<String>>),
    /// ROLLUP: generates progressively less-detailed grouping sets.
    Rollup(Vec<String>),
    /// CUBE: generates all possible combinations of grouping sets.
    Cube(Vec<String>),
}

/// Alignment mode for SAMPLE BY bucket boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlignMode {
    /// Align to first observation (default).
    FirstObservation,
    /// Align to calendar boundaries (midnight, hour start, etc.).
    Calendar,
}

/// SAMPLE BY specification for time bucketing.
#[derive(Debug, Clone, PartialEq)]
pub struct SampleBy {
    /// The bucketing interval.
    pub interval: Duration,
    /// How to handle empty intervals.
    pub fill: FillMode,
    /// How to align bucket boundaries.
    pub align: AlignMode,
}

/// Type of standard JOIN.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    FullOuter,
    Cross,
    /// LATERAL: for each left row, evaluate the right subquery with correlated references.
    Lateral,
}

/// Column reference in a JOIN select list (may be table-qualified).
#[derive(Debug, Clone, PartialEq)]
pub enum JoinSelectColumn {
    /// A table-qualified column: (table_alias_or_name, column_name).
    Qualified(String, String),
    /// An unqualified column name.
    Unqualified(String),
    /// Wildcard `*` (all columns from all tables).
    Wildcard,
    /// Table-qualified wildcard: `t.*`.
    QualifiedWildcard(String),
    /// A column with an alias: (table, column, alias).
    QualifiedAlias(String, String, String),
    /// An expression in JOIN select: e.g. `a.id + b.value AS total`.
    Expression {
        expr: PlanExpr,
        alias: Option<String>,
    },
    /// CASE WHEN in JOIN select.
    CaseWhen {
        conditions: Vec<(Filter, Value)>,
        else_value: Option<Value>,
        alias: Option<String>,
        expr_conditions: Option<Vec<(Filter, PlanExpr)>>,
        expr_else: Option<PlanExpr>,
    },
    /// An aggregate in JOIN select: e.g. `count(*)`.
    Aggregate {
        function: AggregateKind,
        column: String,
        alias: Option<String>,
        arg_expr: Option<PlanExpr>,
    },
}

/// Column definition used in CREATE TABLE plans.
#[derive(Debug, Clone, PartialEq)]
pub struct PlanColumnDef {
    pub name: String,
    pub type_name: String,
    /// Optional CHECK constraint expression for this column.
    pub check: Option<PlanExpr>,
    /// Whether this column has a UNIQUE constraint.
    pub unique: bool,
    /// Optional REFERENCES (foreign key) constraint: (ref_table, ref_column).
    pub references: Option<(String, String)>,
}

/// A CHECK constraint stored in table metadata.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CheckConstraint {
    pub column: String,
    /// The constraint expression serialized as SQL text.
    pub expr_sql: String,
}

/// A UNIQUE constraint stored in table metadata.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UniqueConstraint {
    pub columns: Vec<String>,
}

/// A FOREIGN KEY constraint stored in table metadata.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ForeignKeyConstraint {
    pub column: String,
    pub ref_table: String,
    pub ref_column: String,
}

/// Object type for COMMENT ON.
#[derive(Debug, Clone, PartialEq)]
pub enum CommentObjectType {
    Table,
    Column,
}

/// LATEST ON specification for returning the most recent row per partition.
#[derive(Debug, Clone, PartialEq)]
pub struct LatestOn {
    /// The timestamp column name.
    pub timestamp_col: String,
    /// The partition column name.
    pub partition_col: String,
}

/// Type of set operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    Union,
    Intersect,
    Except,
}

/// A Common Table Expression definition.
#[derive(Debug, Clone, PartialEq)]
pub struct CteDefinition {
    pub name: String,
    pub query: Box<QueryPlan>,
    /// Whether this CTE is recursive (WITH RECURSIVE).
    pub recursive: bool,
}

/// A PIVOT value specification: the literal value and optional alias.
#[derive(Debug, Clone, PartialEq)]
pub struct PivotValue {
    pub value: Value,
    pub alias: String,
}

/// ON CONFLICT action for INSERT.
#[derive(Debug, Clone, PartialEq)]
pub enum OnConflictAction {
    DoNothing,
    DoUpdate {
        assignments: Vec<(String, PlanExpr)>,
    },
}

/// ON CONFLICT specification for INSERT.
#[derive(Debug, Clone, PartialEq)]
pub struct OnConflictClause {
    pub columns: Vec<String>,
    pub action: OnConflictAction,
}

/// A MERGE WHEN clause.
#[derive(Debug, Clone, PartialEq)]
pub enum MergeWhen {
    /// WHEN MATCHED THEN UPDATE SET col = expr, ...
    MatchedUpdate {
        assignments: Vec<(String, PlanExpr)>,
    },
    /// WHEN MATCHED THEN DELETE
    MatchedDelete,
    /// WHEN NOT MATCHED THEN INSERT VALUES (exprs...)
    NotMatchedInsert { values: Vec<PlanExpr> },
}

/// The top-level query plan produced by the planner.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryPlan {
    CreateTable {
        name: String,
        columns: Vec<PlanColumnDef>,
        partition_by: Option<String>,
        timestamp_col: Option<String>,
        if_not_exists: bool,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<Value>>,
        /// If true, this is an INSERT OR REPLACE (upsert) operation.
        upsert: bool,
    },
    Select {
        table: String,
        columns: Vec<SelectColumn>,
        filter: Option<Filter>,
        order_by: Vec<OrderBy>,
        limit: Option<u64>,
        offset: Option<u64>,
        sample_by: Option<SampleBy>,
        latest_on: Option<LatestOn>,
        group_by: Vec<String>,
        /// Advanced GROUP BY mode (GROUPING SETS / ROLLUP / CUBE).
        group_by_mode: GroupByMode,
        having: Option<Filter>,
        distinct: bool,
        /// DISTINCT ON columns (PostgreSQL extension).
        distinct_on: Vec<String>,
    },
    /// Standard JOIN between two tables.
    Join {
        left_table: String,
        right_table: String,
        left_alias: Option<String>,
        right_alias: Option<String>,
        /// Columns to select (may include table-qualified names).
        columns: Vec<JoinSelectColumn>,
        /// Join type: inner or left.
        join_type: JoinType,
        /// Equality conditions: pairs of (left_col, right_col).
        on_columns: Vec<(String, String)>,
        filter: Option<Filter>,
        order_by: Vec<OrderBy>,
        limit: Option<u64>,
    },
    /// Multi-table JOIN: the left side is a sub-plan (another Join or MultiJoin).
    MultiJoin {
        left: Box<QueryPlan>,
        right_table: String,
        right_alias: Option<String>,
        columns: Vec<JoinSelectColumn>,
        join_type: JoinType,
        on_columns: Vec<(String, String)>,
        filter: Option<Filter>,
        order_by: Vec<OrderBy>,
        limit: Option<u64>,
    },
    /// Temporal ASOF JOIN between two tables.
    AsofJoin {
        left_table: String,
        right_table: String,
        /// Columns to select from the left table.
        left_columns: Vec<SelectColumn>,
        /// Columns to select from the right table.
        right_columns: Vec<SelectColumn>,
        /// Equality columns: pairs of (left_col_name, right_col_name).
        on_columns: Vec<(String, String)>,
        filter: Option<Filter>,
        order_by: Vec<OrderBy>,
        limit: Option<u64>,
    },
    /// Add a column to a table.
    AddColumn {
        table: String,
        column_name: String,
        column_type: String,
    },
    /// Drop a column from a table.
    DropColumn { table: String, column_name: String },
    /// Rename a column in a table.
    RenameColumn {
        table: String,
        old_name: String,
        new_name: String,
    },
    /// Change a column's type.
    SetColumnType {
        table: String,
        column_name: String,
        new_type: String,
    },
    /// Drop a table entirely.
    DropTable { table: String, if_exists: bool },
    /// DELETE rows from a table.
    Delete {
        table: String,
        filter: Option<Filter>,
    },
    /// UPDATE rows in a table.
    Update {
        table: String,
        assignments: Vec<(String, PlanExpr)>,
        filter: Option<Filter>,
    },
    /// Set operation: UNION / INTERSECT / EXCEPT between two queries.
    SetOperation {
        op: SetOp,
        left: Box<QueryPlan>,
        right: Box<QueryPlan>,
        all: bool,
        limit: Option<u64>,
    },
    /// A query with Common Table Expressions (CTEs).
    WithCte {
        ctes: Vec<CteDefinition>,
        body: Box<QueryPlan>,
    },
    /// A derived table (subquery in FROM) – used internally.
    DerivedScan {
        subquery: Box<QueryPlan>,
        alias: String,
        columns: Vec<SelectColumn>,
        filter: Option<Filter>,
        order_by: Vec<OrderBy>,
        limit: Option<u64>,
        group_by: Vec<String>,
        having: Option<Filter>,
        distinct: bool,
    },
    /// COPY table TO file.
    CopyTo {
        table: String,
        path: PathBuf,
        options: CopyOptions,
    },
    /// COPY table FROM file.
    CopyFrom {
        table: String,
        path: PathBuf,
        options: CopyOptions,
    },
    /// EXPLAIN a query plan without executing it.
    Explain { query: Box<QueryPlan> },
    /// EXPLAIN ANALYZE: execute the query with profiling instrumentation
    /// and return timing/row-count data instead of query results.
    ExplainAnalyze { query: Box<QueryPlan> },
    /// VACUUM a table to reclaim space.
    Vacuum { table: String },
    /// Create a materialized view backed by a stored query.
    CreateMatView { name: String, source_sql: String },
    /// Refresh a materialized view by re-executing its defining query.
    RefreshMatView { name: String },
    /// Drop a materialized view and its backing table.
    DropMatView { name: String },
    /// CREATE USER <name> WITH PASSWORD '<password>'
    CreateUser { username: String, password: String },
    /// DROP USER <name>
    DropUser { username: String },
    /// CREATE ROLE <name>
    CreateRole { name: String },
    /// DROP ROLE <name>
    DropRole { name: String },
    /// GRANT <permission> [ON <table>] TO <role_or_user>
    Grant {
        permission: GrantPermission,
        target: String,
    },
    /// REVOKE <permission> [ON <table>] FROM <role_or_user>
    Revoke {
        permission: GrantPermission,
        target: String,
    },
    /// SHOW TABLES
    ShowTables,
    /// SHOW COLUMNS FROM <table> / DESCRIBE <table>
    ShowColumns { table: String },
    /// SHOW CREATE TABLE <table>
    ShowCreateTable { table: String },
    /// SELECT x FROM long_sequence(N) — virtual table generating 1..N.
    LongSequence {
        count: u64,
        columns: Vec<SelectColumn>,
    },
    /// SELECT * FROM generate_series(start, stop, step) with integer or timestamp values.
    GenerateSeries {
        start: i64,
        stop: i64,
        step: i64,
        columns: Vec<SelectColumn>,
        /// When true, values are interpreted as nanosecond timestamps.
        is_timestamp: bool,
    },
    /// SELECT * FROM read_parquet('/path/to/file.parquet')
    ReadParquet {
        path: PathBuf,
        columns: Vec<SelectColumn>,
    },
    /// BEGIN / START TRANSACTION — no-op for client compatibility.
    Begin,
    /// COMMIT — no-op for client compatibility.
    Commit,
    /// ROLLBACK — no-op for client compatibility.
    Rollback,
    /// SET variable = value — no-op for client compatibility.
    Set { name: String, value: String },
    /// SHOW variable — returns the variable value.
    Show { name: String },
    /// INSERT INTO ... SELECT: insert results of a query into a table.
    InsertSelect {
        target_table: String,
        columns: Vec<String>,
        source: Box<QueryPlan>,
    },
    /// TRUNCATE TABLE: delete all rows from a table.
    TruncateTable { table: String },
    /// ALTER TABLE ... DETACH PARTITION '<name>'
    DetachPartition { table: String, partition: String },
    /// ALTER TABLE ... ATTACH PARTITION '<name>'
    AttachPartition { table: String, partition: String },
    /// ALTER TABLE ... SQUASH PARTITIONS '<p1>', '<p2>'
    SquashPartitions {
        table: String,
        partition1: String,
        partition2: String,
    },
    /// PIVOT: rotate rows into columns.
    Pivot {
        source: Box<QueryPlan>,
        aggregate: AggregateKind,
        agg_column: String,
        pivot_col: String,
        values: Vec<PivotValue>,
    },
    /// MERGE INTO target USING source ON condition WHEN ...
    Merge {
        target_table: String,
        source_table: String,
        on_column: (String, String),
        when_clauses: Vec<MergeWhen>,
    },
    /// INSERT with ON CONFLICT clause (PostgreSQL-style upsert).
    InsertOnConflict {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<Value>>,
        on_conflict: OnConflictClause,
    },
    /// A standalone VALUES expression: VALUES (1, 'a'), (2, 'b')
    Values {
        column_names: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    /// CREATE INDEX idx_name ON table (col1, col2, ...)
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<String>,
    },
    /// DROP INDEX idx_name
    DropIndex { name: String },
    /// ALTER TABLE t RENAME TO new_name
    RenameTable { old_name: String, new_name: String },
    /// CREATE SEQUENCE seq_name START n INCREMENT n
    CreateSequence {
        name: String,
        start: i64,
        increment: i64,
    },
    /// DROP SEQUENCE seq_name
    DropSequence { name: String },
    /// SELECT nextval('seq') / currval('seq') / setval('seq', n)
    SequenceOp { op: SequenceOpKind },
    /// CREATE PROCEDURE <name> AS BEGIN <body> END
    CreateProcedure { name: String, body: String },
    /// DROP PROCEDURE <name>
    DropProcedure { name: String },
    /// CALL <procedure_name>()
    CallProcedure { name: String },
    /// CREATE DOWNSAMPLING ON <source> INTERVAL <interval> AS <name> COLUMNS <cols>
    CreateDownsampling {
        source_table: String,
        target_name: String,
        interval_secs: u64,
        columns: Vec<(String, String, String)>, // (agg_fn, source_col, alias)
    },
    /// LATERAL JOIN: for each left row, execute the subquery with correlated refs.
    LateralJoin {
        left_table: String,
        left_alias: Option<String>,
        subquery: Box<QueryPlan>,
        subquery_alias: String,
        columns: Vec<JoinSelectColumn>,
        filter: Option<Filter>,
        order_by: Vec<OrderBy>,
        limit: Option<u64>,
    },
    /// CREATE VIEW <name> AS <select_sql>
    CreateView { name: String, sql: String },
    /// DROP VIEW <name>
    DropView { name: String },
    /// CREATE TRIGGER <name> AFTER INSERT ON <table> FOR EACH ROW EXECUTE PROCEDURE <proc>()
    CreateTrigger {
        name: String,
        table: String,
        procedure: String,
    },
    /// DROP TRIGGER <name> ON <table>
    DropTrigger { name: String, table: String },
    /// COMMENT ON TABLE <table> IS '<comment>'
    CommentOn {
        object_type: CommentObjectType,
        object_name: String,
        /// For COLUMN comments: the table name.
        table_name: Option<String>,
        comment: String,
    },
    /// CREATE TABLE ... AS SELECT: create a table from query results.
    CreateTableAs {
        name: String,
        source: Box<QueryPlan>,
        partition_by: Option<String>,
    },
    /// SELECT * FROM read_csv('/path/to/file.csv')
    ReadCsv {
        path: PathBuf,
        columns: Vec<SelectColumn>,
    },
}

/// Sequence operation kind.
#[derive(Debug, Clone, PartialEq)]
pub enum SequenceOpKind {
    NextVal(String),
    CurrVal(String),
    SetVal(String, i64),
}

/// Describes a permission being granted or revoked.
#[derive(Debug, Clone, PartialEq)]
pub enum GrantPermission {
    /// GRANT READ [ON <table>] TO <target>
    Read { table: Option<String> },
    /// GRANT WRITE [ON <table>] TO <target>
    Write { table: Option<String> },
    /// GRANT DDL TO <target>
    DDL,
    /// GRANT ADMIN TO <target>
    Admin,
    /// GRANT SYSTEM TO <target>
    System,
    /// GRANT COLUMN READ (col1, col2) ON <table> TO <target>
    ColumnRead { table: String, columns: Vec<String> },
    /// GRANT <role_name> TO <user> — role assignment
    Role { role_name: String },
    /// GRANT SELECT ON <table> TO <target> — SQL-standard SELECT privilege (maps to Read)
    Select { table: String },
    /// GRANT INSERT ON <table> TO <target> — SQL-standard INSERT privilege (maps to Write)
    Insert { table: String },
    /// GRANT UPDATE ON <table> TO <target> — SQL-standard UPDATE privilege (maps to Write)
    Update { table: String },
    /// GRANT DELETE ON <table> TO <target> — SQL-standard DELETE privilege (maps to Write)
    Delete { table: String },
    /// GRANT ALL ON <table> TO <target> — SQL-standard ALL PRIVILEGES
    All { table: String },
}

/// Options for COPY TO/FROM commands.
#[derive(Debug, Clone, PartialEq)]
pub struct CopyOptions {
    pub header: bool,
    pub delimiter: char,
    pub format: CopyFormat,
}

impl Default for CopyOptions {
    fn default() -> Self {
        Self {
            header: true,
            delimiter: ',',
            format: CopyFormat::Csv,
        }
    }
}

/// Output format for COPY commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    Csv,
    Tsv,
    Parquet,
}

/// Result of executing a query.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Ok {
        affected_rows: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_display() {
        assert_eq!(format!("{}", Value::Null), "NULL");
        assert_eq!(format!("{}", Value::I64(42)), "42");
        assert_eq!(format!("{}", Value::F64(3.14)), "3.14");
        assert_eq!(format!("{}", Value::Str("hello".into())), "'hello'");
    }

    #[test]
    fn aggregate_kind_from_name() {
        assert_eq!(AggregateKind::from_name("SUM"), Some(AggregateKind::Sum));
        assert_eq!(AggregateKind::from_name("avg"), Some(AggregateKind::Avg));
        assert_eq!(
            AggregateKind::from_name("COUNT"),
            Some(AggregateKind::Count)
        );
        assert_eq!(
            AggregateKind::from_name("first"),
            Some(AggregateKind::First)
        );
        assert_eq!(AggregateKind::from_name("last"), Some(AggregateKind::Last));
        assert_eq!(AggregateKind::from_name("unknown"), None);
    }

    #[test]
    fn sample_by_duration() {
        let sb = SampleBy {
            interval: Duration::from_secs(60),
            fill: FillMode::None,
            align: AlignMode::FirstObservation,
        };
        assert_eq!(sb.interval.as_secs(), 60);
    }
}
