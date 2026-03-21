//! Fuzz-like testing for the ExchangeDB SQL parser/planner.
//!
//! Feeds random and malformed SQL input to the planner, verifying that
//! it never panics — only returns errors gracefully.

use exchange_query::planner::plan_query;

// ---------------------------------------------------------------------------
// Simple PRNG (xorshift64)
// ---------------------------------------------------------------------------

struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 1 } else { seed },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn next_usize(&mut self, bound: usize) -> usize {
        if bound == 0 {
            return 0;
        }
        (self.next_u64() % bound as u64) as usize
    }

    fn next_char_ascii(&mut self) -> char {
        // Printable ASCII range 32-126, plus some control chars.
        let c = (self.next_u64() % 128) as u8;
        c as char
    }
}

// ---------------------------------------------------------------------------
// SQL fuzz input generation
// ---------------------------------------------------------------------------

fn generate_random_sql(rng: &mut SimpleRng, count: usize) -> Vec<String> {
    let mut inputs = Vec::with_capacity(count);

    // Category 1: completely random bytes.
    for _ in 0..count / 10 {
        let len = rng.next_usize(200);
        let s: String = (0..len).map(|_| rng.next_char_ascii()).collect();
        inputs.push(s);
    }

    // Category 2: truncated valid SQL.
    let valid_sqls = [
        "SELECT * FROM trades WHERE price > 100",
        "CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)",
        "INSERT INTO test (timestamp, value) VALUES (1000, 42.5)",
        "SELECT count(*), avg(price) FROM trades GROUP BY symbol HAVING count(*) > 5",
        "SELECT * FROM trades ORDER BY timestamp DESC LIMIT 10",
        "DROP TABLE trades",
        "ALTER TABLE trades ADD COLUMN volume DOUBLE",
        "UPDATE trades SET price = 100 WHERE symbol = 'BTC'",
        "DELETE FROM trades WHERE timestamp < 1000",
        "SELECT DISTINCT symbol FROM trades",
    ];
    for _ in 0..count / 10 {
        let sql = valid_sqls[rng.next_usize(valid_sqls.len())];
        let truncate_at = rng.next_usize(sql.len() + 1);
        inputs.push(sql[..truncate_at].to_string());
    }

    // Category 3: SQL injection attempts.
    let injections = [
        "'; DROP TABLE users; --",
        "1; SELECT * FROM passwords",
        "' OR '1'='1",
        "'; EXEC xp_cmdshell('cmd'); --",
        "UNION SELECT * FROM secret_table",
        "1' AND 1=1 UNION SELECT null,null,null--",
        "'; WAITFOR DELAY '00:00:10'; --",
        "SELECT * FROM trades; DELETE FROM trades",
    ];
    for inj in &injections {
        inputs.push(inj.to_string());
    }

    // Category 4: edge cases.
    inputs.push(String::new());
    inputs.push(" ".to_string());
    inputs.push("\n\t\r".to_string());
    inputs.push("SELECT".to_string());
    inputs.push("FROM".to_string());
    inputs.push("WHERE".to_string());
    inputs.push("NULL".to_string());
    inputs.push(";;;".to_string());
    inputs.push("()()()".to_string());
    inputs.push("SELECT ,,, FROM".to_string());
    inputs.push("SELECT * FROM ".to_string());

    // Category 5: very long strings.
    let long_name = "a".repeat(10000);
    inputs.push(format!("SELECT * FROM {long_name}"));
    inputs.push(format!("SELECT {long_name} FROM t"));

    // Category 6: unicode.
    inputs.push("SELECT * FROM \u{1F600}".to_string());
    inputs.push("SELECT '\u{0000}' FROM t".to_string());
    inputs.push("SELECT * FROM t WHERE x = '\u{FEFF}'".to_string());

    // Category 7: deeply nested expressions.
    let mut nested = "SELECT ".to_string();
    for _ in 0..50 {
        nested.push_str("((");
    }
    nested.push('1');
    for _ in 0..50 {
        nested.push_str("))");
    }
    nested.push_str(" FROM t");
    inputs.push(nested);

    // Category 8: valid SQL with random mutations.
    for _ in 0..count / 5 {
        let base = valid_sqls[rng.next_usize(valid_sqls.len())];
        let mut chars: Vec<char> = base.chars().collect();
        let mutations = rng.next_usize(5) + 1;
        for _ in 0..mutations {
            if chars.is_empty() {
                break;
            }
            let op = rng.next_usize(4);
            match op {
                0 => {
                    let idx = rng.next_usize(chars.len());
                    chars.remove(idx);
                }
                1 => {
                    let idx = rng.next_usize(chars.len() + 1);
                    chars.insert(idx, rng.next_char_ascii());
                }
                2 => {
                    let idx = rng.next_usize(chars.len());
                    chars[idx] = rng.next_char_ascii();
                }
                3 => {
                    if chars.len() > 1 {
                        let i = rng.next_usize(chars.len());
                        let j = rng.next_usize(chars.len());
                        chars.swap(i, j);
                    }
                }
                _ => {}
            }
        }
        inputs.push(chars.into_iter().collect());
    }

    // Fill remaining with random SQL-like fragments.
    let keywords = [
        "SELECT",
        "FROM",
        "WHERE",
        "INSERT",
        "INTO",
        "VALUES",
        "CREATE",
        "TABLE",
        "DROP",
        "ALTER",
        "ADD",
        "COLUMN",
        "UPDATE",
        "SET",
        "DELETE",
        "ORDER",
        "BY",
        "GROUP",
        "HAVING",
        "LIMIT",
        "OFFSET",
        "AND",
        "OR",
        "NOT",
        "NULL",
        "AS",
        "JOIN",
        "ON",
        "LEFT",
        "RIGHT",
        "INNER",
        "OUTER",
        "DISTINCT",
        "COUNT",
        "AVG",
        "SUM",
        "MIN",
        "MAX",
        "LIKE",
        "IN",
        "BETWEEN",
        "EXISTS",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "UNION",
        "ALL",
        "DESC",
        "ASC",
        "*",
        ",",
        "(",
        ")",
        "=",
        ">",
        "<",
        ">=",
        "<=",
        "<>",
        "!=",
        "'hello'",
        "42",
        "3.14",
        "timestamp",
        "TIMESTAMP",
        "DOUBLE",
        "VARCHAR",
        "INT",
    ];
    while inputs.len() < count {
        let num_tokens = rng.next_usize(10) + 1;
        let sql: String = (0..num_tokens)
            .map(|_| keywords[rng.next_usize(keywords.len())])
            .collect::<Vec<_>>()
            .join(" ");
        inputs.push(sql);
    }

    inputs
}

/// Feed random/malformed SQL to the planner, verify no panics.
#[test]
fn fuzz_sql_parser_no_panic() {
    let mut rng = SimpleRng::new(12345);
    let inputs = generate_random_sql(&mut rng, 1000);

    let mut ok_count = 0;
    let mut err_count = 0;

    for sql in &inputs {
        // This should never panic, only return Ok or Err.
        match plan_query(sql) {
            Ok(_) => ok_count += 1,
            Err(_) => err_count += 1,
        }
    }

    // Sanity check: we should have some of both.
    assert!(err_count > 0, "expected some errors from random input");
    // Some valid SQL fragments might parse successfully.
    eprintln!(
        "SQL fuzz: {ok_count} ok, {err_count} errors out of {} inputs",
        inputs.len()
    );
}
