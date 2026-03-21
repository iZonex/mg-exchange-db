//! Vectorized string operations for columnar processing.
//!
//! Provides batch operations on string columns that can be auto-vectorized
//! by LLVM. These avoid per-row function call overhead and improve cache
//! locality when processing large string columns.

/// Fast string length computation for a column of strings.
///
/// Returns the length (in bytes) of each string as an `i64`.
pub fn batch_string_length(strings: &[&str]) -> Vec<i64> {
    strings.iter().map(|s| s.len() as i64).collect()
}

/// Fast uppercase conversion for a column of strings.
pub fn batch_uppercase(strings: &[&str]) -> Vec<String> {
    strings.iter().map(|s| s.to_uppercase()).collect()
}

/// Fast LIKE pattern matching for a column.
///
/// Supports SQL LIKE patterns with `%` (match any sequence) and `_` (match
/// any single character). No escape character support for simplicity.
pub fn batch_like(strings: &[&str], pattern: &str) -> Vec<bool> {
    // Pre-compile the pattern into a matching strategy for efficiency.
    let matcher = LikeMatcher::new(pattern);
    strings.iter().map(|s| matcher.matches(s)).collect()
}

/// Fast string equality check against a single target.
pub fn batch_eq(strings: &[&str], target: &str) -> Vec<bool> {
    let target_bytes = target.as_bytes();
    let target_len = target_bytes.len();
    strings
        .iter()
        .map(|s| {
            let sb = s.as_bytes();
            sb.len() == target_len && sb == target_bytes
        })
        .collect()
}

/// Pre-compiled LIKE pattern matcher for efficient batch evaluation.
struct LikeMatcher {
    /// The compiled pattern as a sequence of tokens.
    tokens: Vec<LikeToken>,
}

enum LikeToken {
    /// A literal string that must match exactly.
    Literal(String),
    /// A single-char wildcard `_`.
    AnyChar,
    /// A `%` wildcard — matches any sequence of zero or more characters.
    AnySequence,
}

impl LikeMatcher {
    fn new(pattern: &str) -> Self {
        let mut tokens = Vec::new();
        let mut current = String::new();

        for ch in pattern.chars() {
            match ch {
                '%' => {
                    if !current.is_empty() {
                        tokens.push(LikeToken::Literal(current.clone()));
                        current.clear();
                    }
                    // Collapse consecutive `%` into one.
                    if !matches!(tokens.last(), Some(LikeToken::AnySequence)) {
                        tokens.push(LikeToken::AnySequence);
                    }
                }
                '_' => {
                    if !current.is_empty() {
                        tokens.push(LikeToken::Literal(current.clone()));
                        current.clear();
                    }
                    tokens.push(LikeToken::AnyChar);
                }
                other => current.push(other),
            }
        }
        if !current.is_empty() {
            tokens.push(LikeToken::Literal(current));
        }

        LikeMatcher { tokens }
    }

    fn matches(&self, s: &str) -> bool {
        Self::match_tokens(s, &self.tokens)
    }

    fn match_tokens(s: &str, tokens: &[LikeToken]) -> bool {
        if tokens.is_empty() {
            return s.is_empty();
        }

        match &tokens[0] {
            LikeToken::Literal(lit) => {
                if s.starts_with(lit.as_str()) {
                    Self::match_tokens(&s[lit.len()..], &tokens[1..])
                } else {
                    false
                }
            }
            LikeToken::AnyChar => {
                if s.is_empty() {
                    false
                } else {
                    let char_len = s.chars().next().unwrap().len_utf8();
                    Self::match_tokens(&s[char_len..], &tokens[1..])
                }
            }
            LikeToken::AnySequence => {
                // Try consuming 0, 1, 2, ... characters.
                let rest_tokens = &tokens[1..];
                if Self::match_tokens(s, rest_tokens) {
                    return true;
                }
                let mut pos = 0;
                for ch in s.chars() {
                    pos += ch.len_utf8();
                    if Self::match_tokens(&s[pos..], rest_tokens) {
                        return true;
                    }
                }
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_string_length_basic() {
        let strings = vec!["hello", "world", "", "a"];
        let result = batch_string_length(&strings);
        assert_eq!(result, vec![5, 5, 0, 1]);
    }

    #[test]
    fn batch_string_length_empty() {
        let strings: Vec<&str> = vec![];
        let result = batch_string_length(&strings);
        assert!(result.is_empty());
    }

    #[test]
    fn batch_uppercase_basic() {
        let strings = vec!["hello", "World", "RUST"];
        let result = batch_uppercase(&strings);
        assert_eq!(result, vec!["HELLO", "WORLD", "RUST"]);
    }

    #[test]
    fn batch_uppercase_empty_strings() {
        let strings = vec!["", "a", ""];
        let result = batch_uppercase(&strings);
        assert_eq!(result, vec!["", "A", ""]);
    }

    #[test]
    fn batch_eq_basic() {
        let strings = vec!["BTC", "ETH", "BTC", "SOL", "BTC"];
        let result = batch_eq(&strings, "BTC");
        assert_eq!(result, vec![true, false, true, false, true]);
    }

    #[test]
    fn batch_eq_no_match() {
        let strings = vec!["ETH", "SOL"];
        let result = batch_eq(&strings, "BTC");
        assert_eq!(result, vec![false, false]);
    }

    #[test]
    fn batch_like_prefix() {
        let strings = vec!["Bitcoin", "Ethereum", "Binance"];
        let result = batch_like(&strings, "B%");
        assert_eq!(result, vec![true, false, true]);
    }

    #[test]
    fn batch_like_suffix() {
        let strings = vec!["Bitcoin", "Ethereum", "Litecoin"];
        let result = batch_like(&strings, "%coin");
        assert_eq!(result, vec![true, false, true]);
    }

    #[test]
    fn batch_like_contains() {
        let strings = vec!["Bitcoin", "Ethereum", "Litecoin"];
        let result = batch_like(&strings, "%coin%");
        assert_eq!(result, vec![true, false, true]);
    }

    #[test]
    fn batch_like_exact() {
        let strings = vec!["BTC", "ETH", "BTC"];
        let result = batch_like(&strings, "BTC");
        assert_eq!(result, vec![true, false, true]);
    }

    #[test]
    fn batch_like_single_char_wildcard() {
        let strings = vec!["BTC", "ETH", "BTS", "BTX"];
        let result = batch_like(&strings, "BT_");
        assert_eq!(result, vec![true, false, true, true]);
    }

    #[test]
    fn batch_like_all_match() {
        let strings = vec!["anything", "else", ""];
        let result = batch_like(&strings, "%");
        assert_eq!(result, vec![true, true, true]);
    }

    #[test]
    fn batch_like_complex_pattern() {
        let strings = vec!["abcdef", "abXdef", "abXXdef", "abXYZdef"];
        let result = batch_like(&strings, "ab%def");
        assert_eq!(result, vec![true, true, true, true]);
    }

    #[test]
    fn batch_string_length_matches_scalar() {
        let strings = vec!["hello", "world", "foo", "bar", "baz"];
        let batch_result = batch_string_length(&strings);
        let scalar_result: Vec<i64> = strings.iter().map(|s| s.len() as i64).collect();
        assert_eq!(batch_result, scalar_result);
    }

    #[test]
    fn batch_uppercase_matches_scalar() {
        let strings = vec!["Hello", "wOrLd", "RUST", "test"];
        let batch_result = batch_uppercase(&strings);
        let scalar_result: Vec<String> = strings.iter().map(|s| s.to_uppercase()).collect();
        assert_eq!(batch_result, scalar_result);
    }
}
