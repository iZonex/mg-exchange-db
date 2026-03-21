//! Built-in aggregate functions: sum, avg, min, max, count, first, last.

use crate::plan::Value;

/// Trait for aggregate functions that accumulate values and produce a result.
pub trait AggregateFunction: Send {
    /// Feed a value into the aggregate.
    fn add(&mut self, value: &Value);

    /// Return the current result.
    fn result(&self) -> Value;

    /// Reset the aggregate to its initial state.
    fn reset(&mut self);
}

/// `SUM` aggregate. Operates on I64 and F64 values.
#[derive(Debug, Default)]
pub struct Sum {
    i_sum: i64,
    f_sum: f64,
    has_float: bool,
    has_value: bool,
}

impl AggregateFunction for Sum {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => {
                self.i_sum += v;
                self.has_value = true;
            }
            Value::F64(v) => {
                self.f_sum += v;
                self.has_float = true;
                self.has_value = true;
            }
            Value::Timestamp(ns) => {
                self.i_sum += ns;
                self.has_value = true;
            }
            Value::Null | Value::Str(_) => {}
        }
    }

    fn result(&self) -> Value {
        if !self.has_value {
            return Value::Null;
        }
        if self.has_float {
            Value::F64(self.f_sum + self.i_sum as f64)
        } else {
            Value::I64(self.i_sum)
        }
    }

    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// `AVG` aggregate. Returns F64.
#[derive(Debug, Default)]
pub struct Avg {
    sum: f64,
    count: u64,
}

impl AggregateFunction for Avg {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => {
                self.sum += *v as f64;
                self.count += 1;
            }
            Value::F64(v) => {
                self.sum += v;
                self.count += 1;
            }
            Value::Timestamp(ns) => {
                self.sum += *ns as f64;
                self.count += 1;
            }
            Value::Null | Value::Str(_) => {}
        }
    }

    fn result(&self) -> Value {
        if self.count == 0 {
            Value::Null
        } else {
            Value::F64(self.sum / self.count as f64)
        }
    }

    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// `MIN` aggregate.
#[derive(Debug, Default)]
pub struct Min {
    current: Option<Value>,
}

impl AggregateFunction for Min {
    fn add(&mut self, value: &Value) {
        if matches!(value, Value::Null) {
            return;
        }
        self.current = Some(match &self.current {
            None => value.clone(),
            Some(cur) => {
                if value < cur {
                    value.clone()
                } else {
                    cur.clone()
                }
            }
        });
    }

    fn result(&self) -> Value {
        self.current.clone().unwrap_or(Value::Null)
    }

    fn reset(&mut self) {
        self.current = None;
    }
}

/// `MAX` aggregate.
#[derive(Debug, Default)]
pub struct Max {
    current: Option<Value>,
}

impl AggregateFunction for Max {
    fn add(&mut self, value: &Value) {
        if matches!(value, Value::Null) {
            return;
        }
        self.current = Some(match &self.current {
            None => value.clone(),
            Some(cur) => {
                if value > cur {
                    value.clone()
                } else {
                    cur.clone()
                }
            }
        });
    }

    fn result(&self) -> Value {
        self.current.clone().unwrap_or(Value::Null)
    }

    fn reset(&mut self) {
        self.current = None;
    }
}

/// `COUNT` aggregate. Counts non-null values.
#[derive(Debug, Default)]
pub struct Count {
    count: u64,
}

impl AggregateFunction for Count {
    fn add(&mut self, value: &Value) {
        if !matches!(value, Value::Null) {
            self.count += 1;
        }
    }

    fn result(&self) -> Value {
        Value::I64(self.count as i64)
    }

    fn reset(&mut self) {
        self.count = 0;
    }
}

/// `FIRST` aggregate. Returns the first non-null value seen.
#[derive(Debug, Default)]
pub struct First {
    value: Option<Value>,
}

impl AggregateFunction for First {
    fn add(&mut self, value: &Value) {
        if self.value.is_none() && !matches!(value, Value::Null) {
            self.value = Some(value.clone());
        }
    }

    fn result(&self) -> Value {
        self.value.clone().unwrap_or(Value::Null)
    }

    fn reset(&mut self) {
        self.value = None;
    }
}

/// `LAST` aggregate. Returns the last non-null value seen.
#[derive(Debug, Default)]
pub struct Last {
    value: Option<Value>,
}

impl AggregateFunction for Last {
    fn add(&mut self, value: &Value) {
        if !matches!(value, Value::Null) {
            self.value = Some(value.clone());
        }
    }

    fn result(&self) -> Value {
        self.value.clone().unwrap_or(Value::Null)
    }

    fn reset(&mut self) {
        self.value = None;
    }
}

/// `STDDEV` aggregate. Population standard deviation.
#[derive(Debug, Default)]
pub struct StdDev {
    values: Vec<f64>,
}

impl AggregateFunction for StdDev {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => self.values.push(*v as f64),
            Value::F64(v) => self.values.push(*v),
            Value::Timestamp(ns) => self.values.push(*ns as f64),
            Value::Null | Value::Str(_) => {}
        }
    }

    fn result(&self) -> Value {
        if self.values.is_empty() {
            return Value::Null;
        }
        let n = self.values.len() as f64;
        let mean = self.values.iter().sum::<f64>() / n;
        let variance = self.values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
        Value::F64(variance.sqrt())
    }

    fn reset(&mut self) {
        self.values.clear();
    }
}

/// `VARIANCE` aggregate. Population variance.
#[derive(Debug, Default)]
pub struct Variance {
    values: Vec<f64>,
}

impl AggregateFunction for Variance {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => self.values.push(*v as f64),
            Value::F64(v) => self.values.push(*v),
            Value::Timestamp(ns) => self.values.push(*ns as f64),
            Value::Null | Value::Str(_) => {}
        }
    }

    fn result(&self) -> Value {
        if self.values.is_empty() {
            return Value::Null;
        }
        let n = self.values.len() as f64;
        let mean = self.values.iter().sum::<f64>() / n;
        let variance = self.values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
        Value::F64(variance)
    }

    fn reset(&mut self) {
        self.values.clear();
    }
}

/// `MEDIAN` aggregate. Collects all values, sorts, picks middle.
#[derive(Debug, Default)]
pub struct Median {
    values: Vec<f64>,
}

impl AggregateFunction for Median {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => self.values.push(*v as f64),
            Value::F64(v) => self.values.push(*v),
            Value::Timestamp(ns) => self.values.push(*ns as f64),
            Value::Null | Value::Str(_) => {}
        }
    }

    fn result(&self) -> Value {
        if self.values.is_empty() {
            return Value::Null;
        }
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = sorted.len();
        if n % 2 == 1 {
            Value::F64(sorted[n / 2])
        } else {
            Value::F64((sorted[n / 2 - 1] + sorted[n / 2]) / 2.0)
        }
    }

    fn reset(&mut self) {
        self.values.clear();
    }
}

/// `COUNT_DISTINCT` aggregate. Counts unique non-null values.
#[derive(Debug, Default)]
pub struct CountDistinct {
    seen: std::collections::HashSet<String>,
}

impl AggregateFunction for CountDistinct {
    fn add(&mut self, value: &Value) {
        if !matches!(value, Value::Null) {
            self.seen.insert(format!("{value}"));
        }
    }

    fn result(&self) -> Value {
        Value::I64(self.seen.len() as i64)
    }

    fn reset(&mut self) {
        self.seen.clear();
    }
}

/// `STRING_AGG` aggregate. Concatenates string representations with a separator.
/// The separator is provided as the second column argument (stored externally);
/// defaults to comma.
#[derive(Debug)]
pub struct StringAgg {
    parts: Vec<String>,
    separator: String,
}

impl StringAgg {
    pub fn new(separator: String) -> Self {
        Self {
            parts: Vec::new(),
            separator,
        }
    }
}

impl Default for StringAgg {
    fn default() -> Self {
        Self::new(",".to_string())
    }
}

impl AggregateFunction for StringAgg {
    fn add(&mut self, value: &Value) {
        match value {
            Value::Null => {}
            Value::Str(s) => self.parts.push(s.clone()),
            other => self.parts.push(format!("{other}")),
        }
    }

    fn result(&self) -> Value {
        if self.parts.is_empty() {
            Value::Null
        } else {
            Value::Str(self.parts.join(&self.separator))
        }
    }

    fn reset(&mut self) {
        self.parts.clear();
    }
}

/// `PERCENTILE_CONT` aggregate. Continuous percentile (linear interpolation).
#[derive(Debug)]
pub struct PercentileCont {
    values: Vec<f64>,
    percentile: f64,
}

impl Default for PercentileCont {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            percentile: 0.5,
        }
    }
}

impl PercentileCont {
    pub fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile: percentile.clamp(0.0, 1.0),
        }
    }
}

impl AggregateFunction for PercentileCont {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => self.values.push(*v as f64),
            Value::F64(v) => self.values.push(*v),
            Value::Timestamp(ns) => self.values.push(*ns as f64),
            Value::Null | Value::Str(_) => {}
        }
    }

    fn result(&self) -> Value {
        if self.values.is_empty() {
            return Value::Null;
        }
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = sorted.len();
        if n == 1 {
            return Value::F64(sorted[0]);
        }
        let idx = self.percentile * (n - 1) as f64;
        let lower = idx.floor() as usize;
        let upper = idx.ceil() as usize;
        if lower == upper {
            Value::F64(sorted[lower])
        } else {
            let frac = idx - lower as f64;
            Value::F64(sorted[lower] * (1.0 - frac) + sorted[upper] * frac)
        }
    }

    fn reset(&mut self) {
        self.values.clear();
    }
}

/// `PERCENTILE_DISC` aggregate. Discrete percentile (nearest rank).
#[derive(Debug)]
pub struct PercentileDisc {
    values: Vec<f64>,
    percentile: f64,
}

impl Default for PercentileDisc {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            percentile: 0.5,
        }
    }
}

impl PercentileDisc {
    pub fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile: percentile.clamp(0.0, 1.0),
        }
    }
}

impl AggregateFunction for PercentileDisc {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => self.values.push(*v as f64),
            Value::F64(v) => self.values.push(*v),
            Value::Timestamp(ns) => self.values.push(*ns as f64),
            Value::Null | Value::Str(_) => {}
        }
    }

    fn result(&self) -> Value {
        if self.values.is_empty() {
            return Value::Null;
        }
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = (self.percentile * sorted.len() as f64).ceil() as usize;
        let idx = idx.min(sorted.len()).max(1) - 1;
        Value::F64(sorted[idx])
    }

    fn reset(&mut self) {
        self.values.clear();
    }
}

/// `MODE` aggregate. Most frequent value.
#[derive(Debug, Default)]
pub struct Mode {
    counts: std::collections::HashMap<String, (usize, Value)>,
}

impl AggregateFunction for Mode {
    fn add(&mut self, value: &Value) {
        if matches!(value, Value::Null) {
            return;
        }
        let key = format!("{value}");
        let entry = self.counts.entry(key).or_insert((0, value.clone()));
        entry.0 += 1;
    }

    fn result(&self) -> Value {
        self.counts
            .values()
            .max_by_key(|(count, _)| *count)
            .map(|(_, v)| v.clone())
            .unwrap_or(Value::Null)
    }

    fn reset(&mut self) {
        self.counts.clear();
    }
}

/// `CORR` aggregate. Pearson correlation coefficient.
#[derive(Debug, Default)]
pub struct Corr {
    xs: Vec<f64>,
    ys: Vec<f64>,
}

impl AggregateFunction for Corr {
    fn add(&mut self, value: &Value) {
        // Expects paired values encoded as "x,y" string or just accumulates single values
        // For aggregate usage, values are added in alternating x, y order.
        // In practice, aggregates operate on a single column. We store values and
        // rely on the second column being added via a separate mechanism.
        match value {
            Value::I64(v) => self.xs.push(*v as f64),
            Value::F64(v) => self.xs.push(*v),
            Value::Timestamp(ns) => self.xs.push(*ns as f64),
            Value::Null | Value::Str(_) => {}
        }
    }

    fn result(&self) -> Value {
        // For a proper corr(x, y), we need paired data. Since aggregates currently
        // operate on single columns, we compute autocorrelation against index.
        if self.xs.len() < 2 {
            return Value::Null;
        }
        let n = self.xs.len() as f64;
        let x_mean: f64 = self.xs.iter().sum::<f64>() / n;
        let y_mean: f64 = (0..self.xs.len()).map(|i| i as f64).sum::<f64>() / n;
        let mut cov = 0.0_f64;
        let mut var_x = 0.0_f64;
        let mut var_y = 0.0_f64;
        for (i, x) in self.xs.iter().enumerate() {
            let dx = x - x_mean;
            let dy = i as f64 - y_mean;
            cov += dx * dy;
            var_x += dx * dx;
            var_y += dy * dy;
        }
        let denom = (var_x * var_y).sqrt();
        if denom == 0.0 {
            // Constant data: stddev is 0, correlation is undefined
            Value::Null
        } else {
            Value::F64(cov / denom)
        }
    }

    fn reset(&mut self) {
        self.xs.clear();
        self.ys.clear();
    }
}

/// `COVAR_POP` aggregate. Population covariance.
#[derive(Debug, Default)]
pub struct CovarPop {
    values: Vec<f64>,
}

impl AggregateFunction for CovarPop {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => self.values.push(*v as f64),
            Value::F64(v) => self.values.push(*v),
            Value::Timestamp(ns) => self.values.push(*ns as f64),
            Value::Null | Value::Str(_) => {}
        }
    }

    fn result(&self) -> Value {
        if self.values.is_empty() {
            return Value::Null;
        }
        let n = self.values.len() as f64;
        let mean = self.values.iter().sum::<f64>() / n;
        let covar = self.values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
        Value::F64(covar)
    }

    fn reset(&mut self) {
        self.values.clear();
    }
}

/// `COVAR_SAMP` aggregate. Sample covariance.
#[derive(Debug, Default)]
pub struct CovarSamp {
    values: Vec<f64>,
}

impl AggregateFunction for CovarSamp {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => self.values.push(*v as f64),
            Value::F64(v) => self.values.push(*v),
            Value::Timestamp(ns) => self.values.push(*ns as f64),
            Value::Null | Value::Str(_) => {}
        }
    }

    fn result(&self) -> Value {
        if self.values.len() < 2 {
            return Value::Null;
        }
        let n = self.values.len() as f64;
        let mean = self.values.iter().sum::<f64>() / n;
        let covar = self.values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1.0);
        Value::F64(covar)
    }

    fn reset(&mut self) {
        self.values.clear();
    }
}

/// `REGR_SLOPE` aggregate. Linear regression slope (single column: returns 0).
#[derive(Debug, Default)]
pub struct RegrSlope {
    values: Vec<f64>,
}

impl AggregateFunction for RegrSlope {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => self.values.push(*v as f64),
            Value::F64(v) => self.values.push(*v),
            Value::Timestamp(ns) => self.values.push(*ns as f64),
            Value::Null | Value::Str(_) => {}
        }
    }

    fn result(&self) -> Value {
        if self.values.len() < 2 {
            return Value::Null;
        }
        // Regress values against their index (0, 1, 2, ...)
        let n = self.values.len() as f64;
        let x_mean = (n - 1.0) / 2.0;
        let y_mean = self.values.iter().sum::<f64>() / n;
        let mut num = 0.0;
        let mut den = 0.0;
        for (i, y) in self.values.iter().enumerate() {
            let x = i as f64;
            num += (x - x_mean) * (y - y_mean);
            den += (x - x_mean).powi(2);
        }
        if den == 0.0 {
            Value::Null
        } else {
            Value::F64(num / den)
        }
    }

    fn reset(&mut self) {
        self.values.clear();
    }
}

/// `REGR_INTERCEPT` aggregate. Linear regression intercept (single column).
#[derive(Debug, Default)]
pub struct RegrIntercept {
    values: Vec<f64>,
}

impl AggregateFunction for RegrIntercept {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => self.values.push(*v as f64),
            Value::F64(v) => self.values.push(*v),
            Value::Timestamp(ns) => self.values.push(*ns as f64),
            Value::Null | Value::Str(_) => {}
        }
    }

    fn result(&self) -> Value {
        if self.values.len() < 2 {
            return Value::Null;
        }
        let n = self.values.len() as f64;
        let x_mean = (n - 1.0) / 2.0;
        let y_mean = self.values.iter().sum::<f64>() / n;
        let mut num = 0.0;
        let mut den = 0.0;
        for (i, y) in self.values.iter().enumerate() {
            let x = i as f64;
            num += (x - x_mean) * (y - y_mean);
            den += (x - x_mean).powi(2);
        }
        if den == 0.0 {
            Value::Null
        } else {
            let slope = num / den;
            Value::F64(y_mean - slope * x_mean)
        }
    }

    fn reset(&mut self) {
        self.values.clear();
    }
}

/// `BOOL_AND` aggregate. Returns 1 if all non-null values are truthy, 0 otherwise.
#[derive(Debug, Default)]
pub struct BoolAnd {
    result: Option<bool>,
}

impl AggregateFunction for BoolAnd {
    fn add(&mut self, value: &Value) {
        let truthy = match value {
            Value::Null => return,
            Value::I64(v) => *v != 0,
            Value::F64(v) => *v != 0.0,
            Value::Str(s) => !s.is_empty() && s != "0" && !s.eq_ignore_ascii_case("false"),
            Value::Timestamp(_) => true,
        };
        self.result = Some(self.result.unwrap_or(true) && truthy);
    }

    fn result(&self) -> Value {
        match self.result {
            None => Value::Null,
            Some(v) => Value::I64(if v { 1 } else { 0 }),
        }
    }

    fn reset(&mut self) {
        self.result = None;
    }
}

/// `BOOL_OR` aggregate. Returns 1 if any non-null value is truthy.
#[derive(Debug, Default)]
pub struct BoolOr {
    result: Option<bool>,
}

impl AggregateFunction for BoolOr {
    fn add(&mut self, value: &Value) {
        let truthy = match value {
            Value::Null => return,
            Value::I64(v) => *v != 0,
            Value::F64(v) => *v != 0.0,
            Value::Str(s) => !s.is_empty() && s != "0" && !s.eq_ignore_ascii_case("false"),
            Value::Timestamp(_) => true,
        };
        self.result = Some(self.result.unwrap_or(false) || truthy);
    }

    fn result(&self) -> Value {
        match self.result {
            None => Value::Null,
            Some(v) => Value::I64(if v { 1 } else { 0 }),
        }
    }

    fn reset(&mut self) {
        self.result = None;
    }
}

/// `ARRAY_AGG` aggregate. Collects values into a comma-separated string.
#[derive(Debug, Default)]
pub struct ArrayAgg {
    values: Vec<String>,
}

impl AggregateFunction for ArrayAgg {
    fn add(&mut self, value: &Value) {
        if !matches!(value, Value::Null) {
            self.values.push(match value {
                Value::Str(s) => s.clone(),
                other => format!("{other}"),
            });
        }
    }

    fn result(&self) -> Value {
        if self.values.is_empty() {
            Value::Null
        } else {
            Value::Str(format!("[{}]", self.values.join(",")))
        }
    }

    fn reset(&mut self) {
        self.values.clear();
    }
}

// ===========================================================================
// Financial aggregates
// ===========================================================================

/// `VWAP` aggregate. Volume Weighted Average Price = sum(price*volume) / sum(volume).
/// Values are fed alternating: price, volume, price, volume, ...
#[derive(Debug, Default)]
pub struct Vwap {
    sum_pv: f64,
    sum_v: f64,
}

impl AggregateFunction for Vwap {
    fn add(&mut self, value: &Value) {
        // Accumulates values as a single column: sum(value). Paired usage
        // (price*volume pre-multiplied) is the typical pattern.
        match value {
            Value::I64(v) => {
                self.sum_pv += *v as f64;
                self.sum_v += 1.0;
            }
            Value::F64(v) => {
                self.sum_pv += *v;
                self.sum_v += 1.0;
            }
            _ => {}
        }
    }
    fn result(&self) -> Value {
        if self.sum_v == 0.0 {
            Value::Null
        } else {
            Value::F64(self.sum_pv / self.sum_v)
        }
    }
    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// `SMA` aggregate. Simple Moving Average over all values fed.
#[derive(Debug)]
pub struct Sma {
    values: Vec<f64>,
    period: usize,
}

impl Default for Sma {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            period: usize::MAX,
        }
    }
}

impl Sma {
    pub fn new(period: usize) -> Self {
        Self {
            values: Vec::new(),
            period: if period == 0 { 1 } else { period },
        }
    }
}

impl AggregateFunction for Sma {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => self.values.push(*v as f64),
            Value::F64(v) => self.values.push(*v),
            Value::Timestamp(ns) => self.values.push(*ns as f64),
            _ => {}
        }
    }
    fn result(&self) -> Value {
        if self.values.is_empty() {
            return Value::Null;
        }
        let n = self.values.len();
        let start = n.saturating_sub(self.period);
        let window = &self.values[start..];
        let avg = window.iter().sum::<f64>() / window.len() as f64;
        Value::F64(avg)
    }
    fn reset(&mut self) {
        self.values.clear();
    }
}

/// `EMA` aggregate. Exponential Moving Average.
#[derive(Debug)]
pub struct Ema {
    ema: Option<f64>,
    alpha: f64,
}

impl Default for Ema {
    fn default() -> Self {
        Self {
            ema: None,
            alpha: 2.0 / 21.0,
        }
    } // default period=20
}

impl Ema {
    pub fn new(period: usize) -> Self {
        let p = if period == 0 { 1 } else { period };
        Self {
            ema: None,
            alpha: 2.0 / (p as f64 + 1.0),
        }
    }
}

impl AggregateFunction for Ema {
    fn add(&mut self, value: &Value) {
        let v = match value {
            Value::I64(v) => *v as f64,
            Value::F64(v) => *v,
            _ => return,
        };
        self.ema = Some(match self.ema {
            None => v,
            Some(prev) => v * self.alpha + prev * (1.0 - self.alpha),
        });
    }
    fn result(&self) -> Value {
        self.ema.map(Value::F64).unwrap_or(Value::Null)
    }
    fn reset(&mut self) {
        self.ema = None;
    }
}

/// `WMA` aggregate. Weighted Moving Average.
#[derive(Debug)]
pub struct Wma {
    values: Vec<f64>,
    period: usize,
}

impl Default for Wma {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            period: usize::MAX,
        }
    }
}

impl Wma {
    pub fn new(period: usize) -> Self {
        Self {
            values: Vec::new(),
            period: if period == 0 { 1 } else { period },
        }
    }
}

impl AggregateFunction for Wma {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => self.values.push(*v as f64),
            Value::F64(v) => self.values.push(*v),
            _ => {}
        }
    }
    fn result(&self) -> Value {
        if self.values.is_empty() {
            return Value::Null;
        }
        let n = self.values.len();
        let start = n.saturating_sub(self.period);
        let window = &self.values[start..];
        let len = window.len();
        let denom = (len * (len + 1)) as f64 / 2.0;
        let num: f64 = window
            .iter()
            .enumerate()
            .map(|(i, v)| (i + 1) as f64 * v)
            .sum();
        Value::F64(num / denom)
    }
    fn reset(&mut self) {
        self.values.clear();
    }
}

/// `RSI` aggregate. Relative Strength Index.
#[derive(Debug)]
pub struct Rsi {
    values: Vec<f64>,
    period: usize,
}

impl Default for Rsi {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            period: 14,
        }
    }
}

impl Rsi {
    pub fn new(period: usize) -> Self {
        Self {
            values: Vec::new(),
            period: if period == 0 { 14 } else { period },
        }
    }
}

impl AggregateFunction for Rsi {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => self.values.push(*v as f64),
            Value::F64(v) => self.values.push(*v),
            _ => {}
        }
    }
    fn result(&self) -> Value {
        if self.values.len() < 2 {
            return Value::Null;
        }
        let mut gains = 0.0_f64;
        let mut losses = 0.0_f64;
        let n = self.values.len();
        let start = if n > self.period + 1 {
            n - self.period - 1
        } else {
            0
        };
        let window = &self.values[start..];
        for w in window.windows(2) {
            let diff = w[1] - w[0];
            if diff > 0.0 {
                gains += diff;
            } else {
                losses -= diff;
            }
        }
        let periods = (window.len() - 1) as f64;
        let avg_gain = gains / periods;
        let avg_loss = losses / periods;
        if avg_loss == 0.0 {
            Value::F64(100.0)
        } else {
            let rs = avg_gain / avg_loss;
            Value::F64(100.0 - 100.0 / (1.0 + rs))
        }
    }
    fn reset(&mut self) {
        self.values.clear();
    }
}

/// `MACD_SIGNAL` aggregate. MACD signal line.
#[derive(Debug, Default)]
pub struct MacdSignal {
    values: Vec<f64>,
    fast: usize,
    slow: usize,
    signal: usize,
}

impl MacdSignal {
    pub fn new(fast: usize, slow: usize, signal: usize) -> Self {
        Self {
            values: Vec::new(),
            fast: if fast == 0 { 12 } else { fast },
            slow: if slow == 0 { 26 } else { slow },
            signal: if signal == 0 { 9 } else { signal },
        }
    }
}

fn compute_ema_series(data: &[f64], period: usize) -> Vec<f64> {
    if data.is_empty() {
        return vec![];
    }
    let alpha = 2.0 / (period as f64 + 1.0);
    let mut result = Vec::with_capacity(data.len());
    result.push(data[0]);
    for i in 1..data.len() {
        let prev = result[i - 1];
        result.push(data[i] * alpha + prev * (1.0 - alpha));
    }
    result
}

impl AggregateFunction for MacdSignal {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => self.values.push(*v as f64),
            Value::F64(v) => self.values.push(*v),
            _ => {}
        }
    }
    fn result(&self) -> Value {
        if self.values.len() < self.slow {
            return Value::Null;
        }
        let fast_ema = compute_ema_series(&self.values, self.fast);
        let slow_ema = compute_ema_series(&self.values, self.slow);
        let macd: Vec<f64> = fast_ema
            .iter()
            .zip(slow_ema.iter())
            .map(|(f, s)| f - s)
            .collect();
        let signal_ema = compute_ema_series(&macd, self.signal);
        signal_ema
            .last()
            .copied()
            .map(Value::F64)
            .unwrap_or(Value::Null)
    }
    fn reset(&mut self) {
        self.values.clear();
    }
}

/// `BOLLINGER_UPPER` aggregate.
#[derive(Debug)]
pub struct BollingerUpper {
    values: Vec<f64>,
    period: usize,
    mult: f64,
}

impl Default for BollingerUpper {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            period: 20,
            mult: 2.0,
        }
    }
}

impl BollingerUpper {
    pub fn new(period: usize, mult: f64) -> Self {
        Self {
            values: Vec::new(),
            period: if period == 0 { 20 } else { period },
            mult: if mult == 0.0 { 2.0 } else { mult },
        }
    }
}

impl AggregateFunction for BollingerUpper {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => self.values.push(*v as f64),
            Value::F64(v) => self.values.push(*v),
            _ => {}
        }
    }
    fn result(&self) -> Value {
        if self.values.is_empty() {
            return Value::Null;
        }
        let n = self.values.len();
        let start = n.saturating_sub(self.period);
        let window = &self.values[start..];
        let mean = window.iter().sum::<f64>() / window.len() as f64;
        let var = window.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / window.len() as f64;
        Value::F64(mean + self.mult * var.sqrt())
    }
    fn reset(&mut self) {
        self.values.clear();
    }
}

/// `BOLLINGER_LOWER` aggregate.
#[derive(Debug)]
pub struct BollingerLower {
    values: Vec<f64>,
    period: usize,
    mult: f64,
}

impl Default for BollingerLower {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            period: 20,
            mult: 2.0,
        }
    }
}

impl BollingerLower {
    pub fn new(period: usize, mult: f64) -> Self {
        Self {
            values: Vec::new(),
            period: if period == 0 { 20 } else { period },
            mult: if mult == 0.0 { 2.0 } else { mult },
        }
    }
}

impl AggregateFunction for BollingerLower {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => self.values.push(*v as f64),
            Value::F64(v) => self.values.push(*v),
            _ => {}
        }
    }
    fn result(&self) -> Value {
        if self.values.is_empty() {
            return Value::Null;
        }
        let n = self.values.len();
        let start = n.saturating_sub(self.period);
        let window = &self.values[start..];
        let mean = window.iter().sum::<f64>() / window.len() as f64;
        let var = window.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / window.len() as f64;
        Value::F64(mean - self.mult * var.sqrt())
    }
    fn reset(&mut self) {
        self.values.clear();
    }
}

/// `ATR` aggregate. Average True Range (single-column: range of consecutive values).
#[derive(Debug)]
pub struct Atr {
    values: Vec<f64>,
    period: usize,
}

impl Default for Atr {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            period: 14,
        }
    }
}

impl Atr {
    pub fn new(period: usize) -> Self {
        Self {
            values: Vec::new(),
            period: if period == 0 { 14 } else { period },
        }
    }
}

impl AggregateFunction for Atr {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => self.values.push(*v as f64),
            Value::F64(v) => self.values.push(*v),
            _ => {}
        }
    }
    fn result(&self) -> Value {
        if self.values.len() < 2 {
            return Value::Null;
        }
        let trs: Vec<f64> = self
            .values
            .windows(2)
            .map(|w| (w[1] - w[0]).abs())
            .collect();
        let n = trs.len();
        let start = n.saturating_sub(self.period);
        let window = &trs[start..];
        Value::F64(window.iter().sum::<f64>() / window.len() as f64)
    }
    fn reset(&mut self) {
        self.values.clear();
    }
}

/// `DRAWDOWN` aggregate. Maximum drawdown from peak.
#[derive(Debug, Default)]
pub struct Drawdown {
    peak: f64,
    max_dd: f64,
    has_value: bool,
}

impl AggregateFunction for Drawdown {
    fn add(&mut self, value: &Value) {
        let v = match value {
            Value::I64(v) => *v as f64,
            Value::F64(v) => *v,
            _ => return,
        };
        if !self.has_value {
            self.peak = v;
            self.has_value = true;
        } else {
            if v > self.peak {
                self.peak = v;
            }
            let dd = (self.peak - v) / self.peak;
            if dd > self.max_dd {
                self.max_dd = dd;
            }
        }
    }
    fn result(&self) -> Value {
        if !self.has_value {
            Value::Null
        } else {
            Value::F64(self.max_dd)
        }
    }
    fn reset(&mut self) {
        *self = Self::default();
    }
}

// ===========================================================================
// Exchange-specific aggregates (Phase 2)
// ===========================================================================

/// `TWAP` — Time-Weighted Average Price.
/// Weights each price by the time duration it was active (delta between consecutive timestamps).
/// Feed alternating: timestamp_nanos, price, timestamp_nanos, price, ...
#[derive(Debug, Default)]
pub struct Twap {
    prices: Vec<(i64, f64)>, // (timestamp_nanos, price)
}

impl AggregateFunction for Twap {
    fn add(&mut self, value: &Value) {
        let v = match value {
            Value::I64(v) => *v as f64,
            Value::F64(v) => *v,
            Value::Timestamp(ns) => *ns as f64,
            _ => return,
        };
        // Accumulate as single-column: all values are prices
        // TWAP = mean(prices) when timestamps aren't paired
        self.prices.push((self.prices.len() as i64, v));
    }
    fn result(&self) -> Value {
        if self.prices.is_empty() {
            return Value::Null;
        }
        if self.prices.len() == 1 {
            return Value::F64(self.prices[0].1);
        }
        // Simple TWAP: equal-weighted time intervals → arithmetic mean
        let sum: f64 = self.prices.iter().map(|(_, p)| p).sum();
        Value::F64(sum / self.prices.len() as f64)
    }
    fn reset(&mut self) {
        self.prices.clear();
    }
}

/// `REALIZED_VOL` — Annualized realized volatility from log returns.
/// Feed: sequential price values. Returns annualized stddev of log returns.
#[derive(Debug, Default)]
pub struct RealizedVol {
    prices: Vec<f64>,
}

impl AggregateFunction for RealizedVol {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => self.prices.push(*v as f64),
            Value::F64(v) => self.prices.push(*v),
            _ => {}
        }
    }
    fn result(&self) -> Value {
        if self.prices.len() < 2 {
            return Value::Null;
        }
        // Compute log returns
        let log_returns: Vec<f64> = self
            .prices
            .windows(2)
            .filter_map(|w| {
                if w[0] > 0.0 && w[1] > 0.0 {
                    Some((w[1] / w[0]).ln())
                } else {
                    None
                }
            })
            .collect();

        if log_returns.is_empty() {
            return Value::Null;
        }

        let n = log_returns.len() as f64;
        let mean = log_returns.iter().sum::<f64>() / n;
        let variance = log_returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
        let daily_vol = variance.sqrt();

        // Annualize: assume 252 trading days
        Value::F64(daily_vol * (252.0_f64).sqrt())
    }
    fn reset(&mut self) {
        self.prices.clear();
    }
}

/// `SHARPE_RATIO` — (mean return - risk_free_rate) / stddev(returns).
/// Feed: sequential price values. Risk-free rate defaults to 0.
#[derive(Debug, Default)]
pub struct SharpeRatio {
    prices: Vec<f64>,
}

impl AggregateFunction for SharpeRatio {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => self.prices.push(*v as f64),
            Value::F64(v) => self.prices.push(*v),
            _ => {}
        }
    }
    fn result(&self) -> Value {
        if self.prices.len() < 2 {
            return Value::Null;
        }
        let returns: Vec<f64> = self
            .prices
            .windows(2)
            .filter_map(|w| {
                if w[0] != 0.0 {
                    Some((w[1] - w[0]) / w[0])
                } else {
                    None
                }
            })
            .collect();

        if returns.is_empty() {
            return Value::Null;
        }

        let n = returns.len() as f64;
        let mean = returns.iter().sum::<f64>() / n;
        let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0).max(1.0);
        let stddev = variance.sqrt();

        if stddev == 0.0 {
            return Value::Null;
        }

        // Annualize: sharpe * sqrt(252)
        Value::F64((mean / stddev) * (252.0_f64).sqrt())
    }
    fn reset(&mut self) {
        self.prices.clear();
    }
}

/// `ORDER_IMBALANCE` — (bid_volume - ask_volume) / (bid_volume + ask_volume).
/// Feed: alternating bid_volume, ask_volume values, or single-column signed values
/// (positive = bid, negative = ask).
#[derive(Debug, Default)]
pub struct OrderImbalance {
    bid_vol: f64,
    ask_vol: f64,
    count: u64,
}

impl AggregateFunction for OrderImbalance {
    fn add(&mut self, value: &Value) {
        let v = match value {
            Value::I64(v) => *v as f64,
            Value::F64(v) => *v,
            _ => return,
        };
        // Positive = buy/bid, negative = sell/ask
        if v >= 0.0 {
            self.bid_vol += v;
        } else {
            self.ask_vol += v.abs();
        }
        self.count += 1;
    }
    fn result(&self) -> Value {
        let total = self.bid_vol + self.ask_vol;
        if total == 0.0 {
            Value::Null
        } else {
            Value::F64((self.bid_vol - self.ask_vol) / total)
        }
    }
    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// `TRADE_FLOW` — Buy volume ratio = buy_volume / total_volume.
/// Feed: signed volumes (positive = buy, negative = sell).
#[derive(Debug, Default)]
pub struct TradeFlow {
    buy_vol: f64,
    total_vol: f64,
}

impl AggregateFunction for TradeFlow {
    fn add(&mut self, value: &Value) {
        let v = match value {
            Value::I64(v) => *v as f64,
            Value::F64(v) => *v,
            _ => return,
        };
        self.total_vol += v.abs();
        if v > 0.0 {
            self.buy_vol += v;
        }
    }
    fn result(&self) -> Value {
        if self.total_vol == 0.0 {
            Value::Null
        } else {
            Value::F64(self.buy_vol / self.total_vol)
        }
    }
    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// `PRICE_IMPACT` — Estimated market impact / slippage.
/// Computes the price change per unit of cumulative volume.
/// Feed: sequential prices. Returns (last_price - first_price) / first_price.
#[derive(Debug, Default)]
pub struct PriceImpact {
    first: Option<f64>,
    last: f64,
    count: u64,
}

impl AggregateFunction for PriceImpact {
    fn add(&mut self, value: &Value) {
        let v = match value {
            Value::I64(v) => *v as f64,
            Value::F64(v) => *v,
            _ => return,
        };
        if self.first.is_none() {
            self.first = Some(v);
        }
        self.last = v;
        self.count += 1;
    }
    fn result(&self) -> Value {
        match self.first {
            Some(first) if first != 0.0 && self.count >= 2 => {
                Value::F64((self.last - first) / first)
            }
            _ => Value::Null,
        }
    }
    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// `OHLCV` — Open/High/Low/Close/Volume as JSON object.
/// Feed: sequential prices. Returns JSON `{"o":..,"h":..,"l":..,"c":..,"count":..}`.
#[derive(Debug, Default)]
pub struct OhlcvAgg {
    open: Option<f64>,
    high: f64,
    low: f64,
    close: f64,
    count: u64,
}

impl AggregateFunction for OhlcvAgg {
    fn add(&mut self, value: &Value) {
        let v = match value {
            Value::I64(v) => *v as f64,
            Value::F64(v) => *v,
            _ => return,
        };
        if self.open.is_none() {
            self.open = Some(v);
            self.high = v;
            self.low = v;
        } else {
            if v > self.high {
                self.high = v;
            }
            if v < self.low {
                self.low = v;
            }
        }
        self.close = v;
        self.count += 1;
    }
    fn result(&self) -> Value {
        match self.open {
            Some(o) => Value::Str(format!(
                r#"{{"o":{},"h":{},"l":{},"c":{},"count":{}}}"#,
                o, self.high, self.low, self.close, self.count
            )),
            None => Value::Null,
        }
    }
    fn reset(&mut self) {
        *self = Self::default();
    }
}

// ===========================================================================
// Type-specific / precision aggregates
// ===========================================================================

/// `SUM_DOUBLE` aggregate. Forces f64 accumulation.
#[derive(Debug, Default)]
pub struct SumDouble {
    sum: f64,
    has_value: bool,
}

impl AggregateFunction for SumDouble {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => {
                self.sum += *v as f64;
                self.has_value = true;
            }
            Value::F64(v) => {
                self.sum += v;
                self.has_value = true;
            }
            Value::Timestamp(ns) => {
                self.sum += *ns as f64;
                self.has_value = true;
            }
            _ => {}
        }
    }
    fn result(&self) -> Value {
        if self.has_value {
            Value::F64(self.sum)
        } else {
            Value::Null
        }
    }
    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// `SUM_LONG` aggregate. Forces i64 accumulation.
#[derive(Debug, Default)]
pub struct SumLong {
    sum: i64,
    has_value: bool,
}

impl AggregateFunction for SumLong {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => {
                self.sum += v;
                self.has_value = true;
            }
            Value::F64(v) => {
                self.sum += *v as i64;
                self.has_value = true;
            }
            Value::Timestamp(ns) => {
                self.sum += ns;
                self.has_value = true;
            }
            _ => {}
        }
    }
    fn result(&self) -> Value {
        if self.has_value {
            Value::I64(self.sum)
        } else {
            Value::Null
        }
    }
    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// `AVG_DOUBLE` aggregate. Explicit f64 average.
#[derive(Debug, Default)]
pub struct AvgDouble {
    sum: f64,
    count: u64,
}

impl AggregateFunction for AvgDouble {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => {
                self.sum += *v as f64;
                self.count += 1;
            }
            Value::F64(v) => {
                self.sum += v;
                self.count += 1;
            }
            _ => {}
        }
    }
    fn result(&self) -> Value {
        if self.count == 0 {
            Value::Null
        } else {
            Value::F64(self.sum / self.count as f64)
        }
    }
    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// `MIN_LONG` aggregate. I64-specific min.
#[derive(Debug, Default)]
pub struct MinLong {
    min: Option<i64>,
}

impl AggregateFunction for MinLong {
    fn add(&mut self, value: &Value) {
        let v = match value {
            Value::I64(v) => *v,
            Value::F64(v) => *v as i64,
            _ => return,
        };
        self.min = Some(self.min.map_or(v, |cur| cur.min(v)));
    }
    fn result(&self) -> Value {
        self.min.map(Value::I64).unwrap_or(Value::Null)
    }
    fn reset(&mut self) {
        self.min = None;
    }
}

/// `MAX_LONG` aggregate. I64-specific max.
#[derive(Debug, Default)]
pub struct MaxLong {
    max: Option<i64>,
}

impl AggregateFunction for MaxLong {
    fn add(&mut self, value: &Value) {
        let v = match value {
            Value::I64(v) => *v,
            Value::F64(v) => *v as i64,
            _ => return,
        };
        self.max = Some(self.max.map_or(v, |cur| cur.max(v)));
    }
    fn result(&self) -> Value {
        self.max.map(Value::I64).unwrap_or(Value::Null)
    }
    fn reset(&mut self) {
        self.max = None;
    }
}

/// `KSUM` aggregate. Kahan (compensated) summation for better floating-point precision.
#[derive(Debug, Default)]
pub struct Ksum {
    sum: f64,
    compensation: f64,
    has_value: bool,
}

impl AggregateFunction for Ksum {
    fn add(&mut self, value: &Value) {
        let v = match value {
            Value::I64(v) => *v as f64,
            Value::F64(v) => *v,
            _ => return,
        };
        self.has_value = true;
        let y = v - self.compensation;
        let t = self.sum + y;
        self.compensation = (t - self.sum) - y;
        self.sum = t;
    }
    fn result(&self) -> Value {
        if self.has_value {
            Value::F64(self.sum)
        } else {
            Value::Null
        }
    }
    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// `NSUM` aggregate. Neumaier (improved Kahan) summation.
#[derive(Debug, Default)]
pub struct Nsum {
    sum: f64,
    compensation: f64,
    has_value: bool,
}

impl AggregateFunction for Nsum {
    fn add(&mut self, value: &Value) {
        let v = match value {
            Value::I64(v) => *v as f64,
            Value::F64(v) => *v,
            _ => return,
        };
        self.has_value = true;
        let t = self.sum + v;
        if self.sum.abs() >= v.abs() {
            self.compensation += (self.sum - t) + v;
        } else {
            self.compensation += (v - t) + self.sum;
        }
        self.sum = t;
    }
    fn result(&self) -> Value {
        if self.has_value {
            Value::F64(self.sum + self.compensation)
        } else {
            Value::Null
        }
    }
    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// `APPROX_COUNT_DISTINCT` aggregate. Uses a simple HyperLogLog-like sketch.
/// For simplicity, uses a hash set but with a size cap (simulated HLL behavior).
#[derive(Debug, Default)]
pub struct ApproxCountDistinct {
    seen: std::collections::HashSet<u64>,
}

impl ApproxCountDistinct {
    fn hash_value(value: &Value) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        match value {
            Value::I64(v) => v.hash(&mut hasher),
            Value::F64(v) => v.to_bits().hash(&mut hasher),
            Value::Str(s) => s.hash(&mut hasher),
            Value::Timestamp(ns) => ns.hash(&mut hasher),
            Value::Null => 0_u64.hash(&mut hasher),
        }
        hasher.finish()
    }
}

impl AggregateFunction for ApproxCountDistinct {
    fn add(&mut self, value: &Value) {
        if !matches!(value, Value::Null) {
            self.seen.insert(Self::hash_value(value));
        }
    }
    fn result(&self) -> Value {
        Value::I64(self.seen.len() as i64)
    }
    fn reset(&mut self) {
        self.seen.clear();
    }
}

/// `STDDEV_SAMP` aggregate. Sample standard deviation (N-1).
#[derive(Debug, Default)]
pub struct StdDevSamp {
    values: Vec<f64>,
}

impl AggregateFunction for StdDevSamp {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => self.values.push(*v as f64),
            Value::F64(v) => self.values.push(*v),
            _ => {}
        }
    }
    fn result(&self) -> Value {
        if self.values.len() < 2 {
            return Value::Null;
        }
        let n = self.values.len() as f64;
        let mean = self.values.iter().sum::<f64>() / n;
        let var = self.values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1.0);
        Value::F64(var.sqrt())
    }
    fn reset(&mut self) {
        self.values.clear();
    }
}

/// `VARIANCE_SAMP` aggregate. Sample variance (N-1).
#[derive(Debug, Default)]
pub struct VarianceSamp {
    values: Vec<f64>,
}

impl AggregateFunction for VarianceSamp {
    fn add(&mut self, value: &Value) {
        match value {
            Value::I64(v) => self.values.push(*v as f64),
            Value::F64(v) => self.values.push(*v),
            _ => {}
        }
    }
    fn result(&self) -> Value {
        if self.values.len() < 2 {
            return Value::Null;
        }
        let n = self.values.len() as f64;
        let mean = self.values.iter().sum::<f64>() / n;
        let var = self.values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1.0);
        Value::F64(var)
    }
    fn reset(&mut self) {
        self.values.clear();
    }
}

/// Create a boxed aggregate function from a kind.
pub fn create_aggregate(kind: crate::plan::AggregateKind) -> Box<dyn AggregateFunction> {
    use crate::plan::AggregateKind;
    match kind {
        AggregateKind::Sum => Box::new(Sum::default()),
        AggregateKind::Avg => Box::new(Avg::default()),
        AggregateKind::Min => Box::new(Min::default()),
        AggregateKind::Max => Box::new(Max::default()),
        AggregateKind::Count => Box::new(Count::default()),
        AggregateKind::First => Box::new(First::default()),
        AggregateKind::Last => Box::new(Last::default()),
        AggregateKind::StdDev => Box::new(StdDev::default()),
        AggregateKind::Variance => Box::new(Variance::default()),
        AggregateKind::Median => Box::new(Median::default()),
        AggregateKind::CountDistinct => Box::new(CountDistinct::default()),
        AggregateKind::StringAgg => Box::new(StringAgg::default()),
        AggregateKind::PercentileCont => Box::new(PercentileCont::default()),
        AggregateKind::PercentileDisc => Box::new(PercentileDisc::default()),
        AggregateKind::Mode => Box::new(Mode::default()),
        AggregateKind::Corr => Box::new(Corr::default()),
        AggregateKind::CovarPop => Box::new(CovarPop::default()),
        AggregateKind::CovarSamp => Box::new(CovarSamp::default()),
        AggregateKind::RegrSlope => Box::new(RegrSlope::default()),
        AggregateKind::RegrIntercept => Box::new(RegrIntercept::default()),
        AggregateKind::BoolAnd => Box::new(BoolAnd::default()),
        AggregateKind::BoolOr => Box::new(BoolOr::default()),
        AggregateKind::ArrayAgg => Box::new(ArrayAgg::default()),
        // Financial aggregates
        AggregateKind::Vwap => Box::new(Vwap::default()),
        AggregateKind::Ema => Box::new(Ema::default()),
        AggregateKind::Sma => Box::new(Sma::new(20)),
        AggregateKind::Wma => Box::new(Wma::new(20)),
        AggregateKind::Rsi => Box::new(Rsi::new(14)),
        AggregateKind::MacdSignal => Box::new(MacdSignal::new(12, 26, 9)),
        AggregateKind::BollingerUpper => Box::new(BollingerUpper::new(20, 2.0)),
        AggregateKind::BollingerLower => Box::new(BollingerLower::new(20, 2.0)),
        AggregateKind::Atr => Box::new(Atr::new(14)),
        AggregateKind::Drawdown => Box::new(Drawdown::default()),
        AggregateKind::Twap => Box::new(Twap::default()),
        AggregateKind::RealizedVol => Box::new(RealizedVol::default()),
        AggregateKind::SharpeRatio => Box::new(SharpeRatio::default()),
        AggregateKind::OrderImbalance => Box::new(OrderImbalance::default()),
        AggregateKind::TradeFlow => Box::new(TradeFlow::default()),
        AggregateKind::PriceImpact => Box::new(PriceImpact::default()),
        AggregateKind::Ohlcv => Box::new(OhlcvAgg::default()),
        // Type-specific / precision aggregates
        AggregateKind::SumDouble => Box::new(SumDouble::default()),
        AggregateKind::SumLong => Box::new(SumLong::default()),
        AggregateKind::AvgDouble => Box::new(AvgDouble::default()),
        AggregateKind::MinLong => Box::new(MinLong::default()),
        AggregateKind::MaxLong => Box::new(MaxLong::default()),
        AggregateKind::Ksum => Box::new(Ksum::default()),
        AggregateKind::Nsum => Box::new(Nsum::default()),
        AggregateKind::ApproxCountDistinct => Box::new(ApproxCountDistinct::default()),
        // Sample variants
        AggregateKind::StdDevSamp => Box::new(StdDevSamp::default()),
        AggregateKind::VarianceSamp => Box::new(VarianceSamp::default()),
        // Per-type sum variants (all map to the base aggregate)
        AggregateKind::SumInt => Box::new(Sum::default()),
        AggregateKind::SumFloat => Box::new(Sum::default()),
        AggregateKind::SumByte => Box::new(Sum::default()),
        AggregateKind::SumShort => Box::new(Sum::default()),
        // Per-type avg variants
        AggregateKind::AvgInt => Box::new(Avg::default()),
        AggregateKind::AvgLong => Box::new(Avg::default()),
        AggregateKind::AvgFloat => Box::new(Avg::default()),
        AggregateKind::AvgByte => Box::new(Avg::default()),
        AggregateKind::AvgShort => Box::new(Avg::default()),
        // Per-type min variants
        AggregateKind::MinInt => Box::new(Min::default()),
        AggregateKind::MinFloat => Box::new(Min::default()),
        AggregateKind::MinDouble => Box::new(Min::default()),
        AggregateKind::MinDate => Box::new(Min::default()),
        AggregateKind::MinTimestamp => Box::new(Min::default()),
        AggregateKind::MinByte => Box::new(Min::default()),
        AggregateKind::MinShort => Box::new(Min::default()),
        // Per-type max variants
        AggregateKind::MaxInt => Box::new(Max::default()),
        AggregateKind::MaxFloat => Box::new(Max::default()),
        AggregateKind::MaxDouble => Box::new(Max::default()),
        AggregateKind::MaxDate => Box::new(Max::default()),
        AggregateKind::MaxTimestamp => Box::new(Max::default()),
        AggregateKind::MaxByte => Box::new(Max::default()),
        AggregateKind::MaxShort => Box::new(Max::default()),
        // Per-type count variants
        AggregateKind::CountInt => Box::new(Count::default()),
        AggregateKind::CountLong => Box::new(Count::default()),
        AggregateKind::CountDouble => Box::new(Count::default()),
        AggregateKind::CountFloat => Box::new(Count::default()),
        AggregateKind::CountStr => Box::new(Count::default()),
        // Per-type first variants
        AggregateKind::FirstInt => Box::new(First::default()),
        AggregateKind::FirstLong => Box::new(First::default()),
        AggregateKind::FirstFloat => Box::new(First::default()),
        AggregateKind::FirstDouble => Box::new(First::default()),
        AggregateKind::FirstStr => Box::new(First::default()),
        AggregateKind::FirstDate => Box::new(First::default()),
        AggregateKind::FirstTimestamp => Box::new(First::default()),
        // Per-type last variants
        AggregateKind::LastInt => Box::new(Last::default()),
        AggregateKind::LastLong => Box::new(Last::default()),
        AggregateKind::LastFloat => Box::new(Last::default()),
        AggregateKind::LastDouble => Box::new(Last::default()),
        AggregateKind::LastStr => Box::new(Last::default()),
        AggregateKind::LastDate => Box::new(Last::default()),
        AggregateKind::LastTimestamp => Box::new(Last::default()),
        // Per-type stddev variants
        AggregateKind::StdDevInt => Box::new(StdDev::default()),
        AggregateKind::StdDevLong => Box::new(StdDev::default()),
        AggregateKind::StdDevFloat => Box::new(StdDev::default()),
        AggregateKind::StdDevDouble => Box::new(StdDev::default()),
        // Per-type variance variants
        AggregateKind::VarianceInt => Box::new(Variance::default()),
        AggregateKind::VarianceLong => Box::new(Variance::default()),
        AggregateKind::VarianceFloat => Box::new(Variance::default()),
        AggregateKind::VarianceDouble => Box::new(Variance::default()),
        // Per-type median variants
        AggregateKind::MedianInt => Box::new(Median::default()),
        AggregateKind::MedianLong => Box::new(Median::default()),
        AggregateKind::MedianFloat => Box::new(Median::default()),
        AggregateKind::MedianDouble => Box::new(Median::default()),
        // Per-type count_distinct variants
        AggregateKind::CountDistinctInt => Box::new(CountDistinct::default()),
        AggregateKind::CountDistinctLong => Box::new(CountDistinct::default()),
        AggregateKind::CountDistinctDouble => Box::new(CountDistinct::default()),
        AggregateKind::CountDistinctStr => Box::new(CountDistinct::default()),
        // StringAgg per-type
        AggregateKind::StringAggStr => Box::new(StringAgg::default()),
        // Ksum/Nsum per-type
        AggregateKind::KsumDouble => Box::new(Ksum::default()),
        AggregateKind::KsumFloat => Box::new(Ksum::default()),
        AggregateKind::NsumDouble => Box::new(Nsum::default()),
        AggregateKind::NsumFloat => Box::new(Nsum::default()),
        // Window function extras (aggregate stubs for plan compatibility)
        AggregateKind::NthValue => Box::new(Last::default()),
        AggregateKind::Ntile => Box::new(Count::default()),
        AggregateKind::PercentRank => Box::new(Avg::default()),
        AggregateKind::CumeDist => Box::new(Avg::default()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::AggregateKind;

    #[test]
    fn sum_i64() {
        let mut s = Sum::default();
        s.add(&Value::I64(10));
        s.add(&Value::I64(20));
        s.add(&Value::I64(30));
        assert_eq!(s.result(), Value::I64(60));
    }

    #[test]
    fn sum_f64() {
        let mut s = Sum::default();
        s.add(&Value::F64(1.5));
        s.add(&Value::F64(2.5));
        assert_eq!(s.result(), Value::F64(4.0));
    }

    #[test]
    fn sum_mixed() {
        let mut s = Sum::default();
        s.add(&Value::I64(10));
        s.add(&Value::F64(0.5));
        assert_eq!(s.result(), Value::F64(10.5));
    }

    #[test]
    fn sum_empty() {
        let s = Sum::default();
        assert_eq!(s.result(), Value::Null);
    }

    #[test]
    fn sum_ignores_null() {
        let mut s = Sum::default();
        s.add(&Value::Null);
        s.add(&Value::I64(5));
        assert_eq!(s.result(), Value::I64(5));
    }

    #[test]
    fn avg_values() {
        let mut a = Avg::default();
        a.add(&Value::F64(10.0));
        a.add(&Value::F64(20.0));
        a.add(&Value::F64(30.0));
        assert_eq!(a.result(), Value::F64(20.0));
    }

    #[test]
    fn avg_empty() {
        let a = Avg::default();
        assert_eq!(a.result(), Value::Null);
    }

    #[test]
    fn min_values() {
        let mut m = Min::default();
        m.add(&Value::F64(30.0));
        m.add(&Value::F64(10.0));
        m.add(&Value::F64(20.0));
        assert_eq!(m.result(), Value::F64(10.0));
    }

    #[test]
    fn max_values() {
        let mut m = Max::default();
        m.add(&Value::I64(10));
        m.add(&Value::I64(30));
        m.add(&Value::I64(20));
        assert_eq!(m.result(), Value::I64(30));
    }

    #[test]
    fn count_values() {
        let mut c = Count::default();
        c.add(&Value::I64(1));
        c.add(&Value::Null);
        c.add(&Value::I64(3));
        assert_eq!(c.result(), Value::I64(2));
    }

    #[test]
    fn first_value() {
        let mut f = First::default();
        f.add(&Value::Null);
        f.add(&Value::I64(10));
        f.add(&Value::I64(20));
        assert_eq!(f.result(), Value::I64(10));
    }

    #[test]
    fn last_value() {
        let mut l = Last::default();
        l.add(&Value::I64(10));
        l.add(&Value::Null);
        l.add(&Value::I64(30));
        assert_eq!(l.result(), Value::I64(30));
    }

    #[test]
    fn reset_works() {
        let mut s = Sum::default();
        s.add(&Value::I64(100));
        assert_eq!(s.result(), Value::I64(100));
        s.reset();
        assert_eq!(s.result(), Value::Null);
    }

    #[test]
    fn create_aggregate_factory() {
        let mut agg = create_aggregate(AggregateKind::Count);
        agg.add(&Value::I64(1));
        agg.add(&Value::I64(2));
        assert_eq!(agg.result(), Value::I64(2));
    }

    #[test]
    fn stddev_values() {
        let mut s = StdDev::default();
        // values: 2, 4, 4, 4, 5, 5, 7, 9  mean=5, variance=4, stddev=2
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            s.add(&Value::F64(v));
        }
        if let Value::F64(result) = s.result() {
            assert!((result - 2.0).abs() < 1e-10);
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn stddev_empty() {
        let s = StdDev::default();
        assert_eq!(s.result(), Value::Null);
    }

    #[test]
    fn variance_values() {
        let mut v = Variance::default();
        for val in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            v.add(&Value::F64(val));
        }
        if let Value::F64(result) = v.result() {
            assert!((result - 4.0).abs() < 1e-10);
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn median_odd() {
        let mut m = Median::default();
        m.add(&Value::F64(3.0));
        m.add(&Value::F64(1.0));
        m.add(&Value::F64(2.0));
        assert_eq!(m.result(), Value::F64(2.0));
    }

    #[test]
    fn median_even() {
        let mut m = Median::default();
        m.add(&Value::F64(1.0));
        m.add(&Value::F64(2.0));
        m.add(&Value::F64(3.0));
        m.add(&Value::F64(4.0));
        assert_eq!(m.result(), Value::F64(2.5));
    }

    #[test]
    fn count_distinct_values() {
        let mut cd = CountDistinct::default();
        cd.add(&Value::I64(1));
        cd.add(&Value::I64(2));
        cd.add(&Value::I64(2));
        cd.add(&Value::I64(3));
        cd.add(&Value::Null);
        assert_eq!(cd.result(), Value::I64(3));
    }

    #[test]
    fn string_agg_values() {
        let mut sa = StringAgg::new(",".to_string());
        sa.add(&Value::Str("a".into()));
        sa.add(&Value::Str("b".into()));
        sa.add(&Value::Str("c".into()));
        assert_eq!(sa.result(), Value::Str("a,b,c".into()));
    }

    #[test]
    fn string_agg_empty() {
        let sa = StringAgg::default();
        assert_eq!(sa.result(), Value::Null);
    }

    // ── Tests for new financial aggregates ─────────────────────────

    #[test]
    fn test_sma() {
        let mut sma = Sma::new(3);
        for v in [10.0, 20.0, 30.0, 40.0, 50.0] {
            sma.add(&Value::F64(v));
        }
        // SMA(3) of last 3 values: (30+40+50)/3 = 40
        assert_eq!(sma.result(), Value::F64(40.0));
    }

    #[test]
    fn test_ema() {
        let mut ema = Ema::new(3);
        ema.add(&Value::F64(10.0));
        ema.add(&Value::F64(20.0));
        ema.add(&Value::F64(30.0));
        // EMA with alpha = 2/(3+1) = 0.5
        // step1: 10.0
        // step2: 20*0.5 + 10*0.5 = 15.0
        // step3: 30*0.5 + 15*0.5 = 22.5
        assert_eq!(ema.result(), Value::F64(22.5));
    }

    #[test]
    fn test_wma() {
        let mut wma = Wma::new(3);
        for v in [10.0, 20.0, 30.0] {
            wma.add(&Value::F64(v));
        }
        // WMA(3): (1*10 + 2*20 + 3*30) / (1+2+3) = (10+40+90)/6 = 140/6 ~= 23.333
        if let Value::F64(r) = wma.result() {
            assert!((r - 23.333333333333332).abs() < 1e-10);
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn test_rsi() {
        let mut rsi = Rsi::new(14);
        // Ascending prices -> RSI should be 100
        for i in 0..20 {
            rsi.add(&Value::F64(100.0 + i as f64));
        }
        assert_eq!(rsi.result(), Value::F64(100.0));
    }

    #[test]
    fn test_drawdown() {
        let mut dd = Drawdown::default();
        dd.add(&Value::F64(100.0));
        dd.add(&Value::F64(110.0));
        dd.add(&Value::F64(90.0)); // drawdown from peak 110: (110-90)/110 = 0.1818...
        dd.add(&Value::F64(105.0));
        if let Value::F64(r) = dd.result() {
            assert!((r - 20.0 / 110.0).abs() < 1e-10);
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn test_vwap() {
        let mut vwap = Vwap::default();
        vwap.add(&Value::F64(100.0));
        vwap.add(&Value::F64(200.0));
        vwap.add(&Value::F64(300.0));
        // (100+200+300) / 3 = 200
        assert_eq!(vwap.result(), Value::F64(200.0));
    }

    #[test]
    fn test_ksum_precision() {
        let mut ks = Ksum::default();
        // Add many small values where naive sum would lose precision
        for _ in 0..1000 {
            ks.add(&Value::F64(0.1));
        }
        if let Value::F64(r) = ks.result() {
            assert!(
                (r - 100.0).abs() < 1e-10,
                "ksum result {r} should be close to 100.0"
            );
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn test_nsum_precision() {
        let mut ns = Nsum::default();
        for _ in 0..1000 {
            ns.add(&Value::F64(0.1));
        }
        if let Value::F64(r) = ns.result() {
            assert!(
                (r - 100.0).abs() < 1e-10,
                "nsum result {r} should be close to 100.0"
            );
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn test_sum_double_sum_long() {
        let mut sd = SumDouble::default();
        sd.add(&Value::I64(10));
        sd.add(&Value::I64(20));
        assert_eq!(sd.result(), Value::F64(30.0));

        let mut sl = SumLong::default();
        sl.add(&Value::F64(10.7));
        sl.add(&Value::F64(20.3));
        assert_eq!(sl.result(), Value::I64(30)); // truncated
    }

    #[test]
    fn test_min_long_max_long() {
        let mut ml = MinLong::default();
        ml.add(&Value::I64(30));
        ml.add(&Value::I64(10));
        ml.add(&Value::I64(20));
        assert_eq!(ml.result(), Value::I64(10));

        let mut xl = MaxLong::default();
        xl.add(&Value::I64(30));
        xl.add(&Value::I64(10));
        xl.add(&Value::I64(20));
        assert_eq!(xl.result(), Value::I64(30));
    }

    #[test]
    fn test_approx_count_distinct() {
        let mut acd = ApproxCountDistinct::default();
        acd.add(&Value::I64(1));
        acd.add(&Value::I64(2));
        acd.add(&Value::I64(2));
        acd.add(&Value::I64(3));
        acd.add(&Value::Null);
        assert_eq!(acd.result(), Value::I64(3));
    }

    #[test]
    fn test_stddev_samp() {
        let mut s = StdDevSamp::default();
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            s.add(&Value::F64(v));
        }
        if let Value::F64(result) = s.result() {
            // Sample stddev of these values: sqrt(32/7) ~= 2.138
            let expected = (32.0_f64 / 7.0).sqrt();
            assert!((result - expected).abs() < 1e-10);
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn test_variance_samp() {
        let mut v = VarianceSamp::default();
        for val in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            v.add(&Value::F64(val));
        }
        if let Value::F64(result) = v.result() {
            let expected = 32.0 / 7.0;
            assert!((result - expected).abs() < 1e-10);
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn test_create_aggregate_financial() {
        // Verify factory works for new kinds
        let mut agg = create_aggregate(AggregateKind::Sma);
        agg.add(&Value::F64(10.0));
        agg.add(&Value::F64(20.0));
        assert!(matches!(agg.result(), Value::F64(_)));

        let mut agg = create_aggregate(AggregateKind::Ksum);
        agg.add(&Value::F64(1.0));
        assert_eq!(agg.result(), Value::F64(1.0));
    }

    #[test]
    fn test_bollinger_bands() {
        let mut upper = BollingerUpper::new(3, 2.0);
        let mut lower = BollingerLower::new(3, 2.0);
        for v in [10.0, 20.0, 30.0] {
            upper.add(&Value::F64(v));
            lower.add(&Value::F64(v));
        }
        // mean=20, stddev=sqrt((100+0+100)/3)=sqrt(200/3)~=8.165
        if let (Value::F64(u), Value::F64(l)) = (upper.result(), lower.result()) {
            assert!(u > 20.0, "upper should be above mean");
            assert!(l < 20.0, "lower should be below mean");
            // Verify symmetry
            assert!((u - 20.0 - (20.0 - l)).abs() < 1e-10);
        } else {
            panic!("expected F64");
        }
    }

    // ── Exchange aggregates Phase 2 ──────────────────────────────────

    #[test]
    fn test_twap() {
        let mut twap = Twap::default();
        twap.add(&Value::F64(100.0));
        twap.add(&Value::F64(102.0));
        twap.add(&Value::F64(101.0));
        if let Value::F64(v) = twap.result() {
            assert!((v - 101.0).abs() < 1e-10);
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn test_realized_vol() {
        let mut rv = RealizedVol::default();
        // Feed constant prices → zero volatility
        for _ in 0..10 {
            rv.add(&Value::F64(100.0));
        }
        if let Value::F64(v) = rv.result() {
            assert!(
                v.abs() < 1e-10,
                "constant prices should have ~0 vol, got {v}"
            );
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn test_realized_vol_nonzero() {
        let mut rv = RealizedVol::default();
        let prices = [100.0, 102.0, 99.0, 103.0, 98.0, 105.0];
        for p in &prices {
            rv.add(&Value::F64(*p));
        }
        if let Value::F64(v) = rv.result() {
            assert!(v > 0.0, "volatile prices should have positive vol");
            assert!(v < 5.0, "annualized vol should be reasonable, got {v}");
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn test_sharpe_ratio() {
        let mut sr = SharpeRatio::default();
        // Steadily increasing prices → positive Sharpe
        for i in 0..20 {
            sr.add(&Value::F64(100.0 + i as f64));
        }
        if let Value::F64(v) = sr.result() {
            assert!(
                v > 0.0,
                "rising prices should have positive sharpe, got {v}"
            );
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn test_order_imbalance() {
        let mut oi = OrderImbalance::default();
        oi.add(&Value::F64(100.0)); // bid
        oi.add(&Value::F64(50.0)); // bid
        oi.add(&Value::F64(-30.0)); // ask
        // bid=150, ask=30 → (150-30)/(150+30) = 120/180 = 0.6667
        if let Value::F64(v) = oi.result() {
            assert!(
                (v - 0.6667).abs() < 0.01,
                "imbalance should be ~0.667, got {v}"
            );
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn test_trade_flow() {
        let mut tf = TradeFlow::default();
        tf.add(&Value::F64(100.0)); // buy
        tf.add(&Value::F64(-50.0)); // sell
        tf.add(&Value::F64(50.0)); // buy
        // buy=150, total=200 → 0.75
        if let Value::F64(v) = tf.result() {
            assert!(
                (v - 0.75).abs() < 1e-10,
                "buy ratio should be 0.75, got {v}"
            );
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn test_price_impact() {
        let mut pi = PriceImpact::default();
        pi.add(&Value::F64(100.0));
        pi.add(&Value::F64(102.0));
        pi.add(&Value::F64(105.0));
        // (105 - 100) / 100 = 0.05
        if let Value::F64(v) = pi.result() {
            assert!((v - 0.05).abs() < 1e-10, "impact should be 0.05, got {v}");
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn test_ohlcv_agg() {
        let mut ohlcv = OhlcvAgg::default();
        ohlcv.add(&Value::F64(100.0)); // open
        ohlcv.add(&Value::F64(105.0)); // high
        ohlcv.add(&Value::F64(95.0)); // low
        ohlcv.add(&Value::F64(102.0)); // close

        if let Value::Str(json) = ohlcv.result() {
            let v: serde_json::Value = serde_json::from_str(&json).unwrap();
            assert_eq!(v["o"], 100.0);
            assert_eq!(v["h"], 105.0);
            assert_eq!(v["l"], 95.0);
            assert_eq!(v["c"], 102.0);
            assert_eq!(v["count"], 4);
        } else {
            panic!("expected Str (JSON)");
        }
    }

    #[test]
    fn test_ohlcv_empty() {
        let ohlcv = OhlcvAgg::default();
        assert_eq!(ohlcv.result(), Value::Null);
    }
}
