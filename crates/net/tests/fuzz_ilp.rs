//! Fuzz-like testing for the ILP (InfluxDB Line Protocol) parser.
//!
//! Feeds random and malformed ILP lines to the parser, verifying that
//! it never panics — only returns errors gracefully.

use exchange_net::ilp::parser::parse_ilp_line;

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
        (self.next_u64() % 128) as u8 as char
    }
}

// ---------------------------------------------------------------------------
// ILP fuzz input generation
// ---------------------------------------------------------------------------

fn generate_random_ilp(rng: &mut SimpleRng, count: usize) -> Vec<String> {
    let mut inputs = Vec::with_capacity(count);

    // Category 1: completely random bytes (valid UTF-8).
    for _ in 0..count / 10 {
        let len = rng.next_usize(200);
        let s: String = (0..len).map(|_| rng.next_char_ascii()).collect();
        inputs.push(s);
    }

    // Category 2: valid ILP lines.
    let valid = [
        "cpu,host=server01 usage=0.64 1609459200000000000",
        "mem,host=h1 total=16384i 1000000000",
        "logs,source=app message=\"hello world\" 1000000000",
        "status alive=true 1000",
        "trades,sym=AAPL price=150.5,volume=100i 999999",
    ];
    for v in &valid {
        inputs.push(v.to_string());
    }

    // Category 3: truncated valid ILP.
    for _ in 0..count / 10 {
        let line = valid[rng.next_usize(valid.len())];
        let truncate_at = rng.next_usize(line.len() + 1);
        inputs.push(line[..truncate_at].to_string());
    }

    // Category 4: edge cases.
    inputs.push(String::new());
    inputs.push(" ".to_string());
    inputs.push(",".to_string());
    inputs.push("=".to_string());
    inputs.push(",,, ===".to_string());
    inputs.push("#comment".to_string());
    inputs.push("measurement_only".to_string());
    inputs.push("m f=".to_string());
    inputs.push("m f=\"".to_string());
    inputs.push("m f=\"\\\"".to_string());
    inputs.push("m, =v".to_string());
    inputs.push(",tag=v f=1".to_string());
    inputs.push("m f=1 not_a_timestamp".to_string());
    inputs.push("m f=1 -999".to_string());
    inputs.push("m f=1 99999999999999999999999".to_string());

    // Category 5: unicode.
    inputs.push("\u{1F600} value=1 1000".to_string());
    inputs.push("m,tag=\u{00E9}\u{00E8} value=1 1000".to_string());
    inputs.push("m value=\"\u{1F4A9}\" 1000".to_string());

    // Category 6: very long strings.
    let long_measurement = "a".repeat(5000);
    inputs.push(format!("{long_measurement} value=1 1000"));
    let long_tag_value = "b".repeat(5000);
    inputs.push(format!("m,tag={long_tag_value} value=1 1000"));

    // Category 7: random ILP-like structures.
    let measurements = ["cpu", "mem", "disk", "net", "trades", "quotes"];
    let tags = ["host", "region", "sym", "id", "type"];
    let fields = ["value", "count", "total", "usage", "temp"];
    while inputs.len() < count {
        let meas = measurements[rng.next_usize(measurements.len())];
        let num_tags = rng.next_usize(4);
        let num_fields = rng.next_usize(4) + 1;

        let mut line = meas.to_string();
        for _ in 0..num_tags {
            let tag = tags[rng.next_usize(tags.len())];
            let val_type = rng.next_usize(3);
            let val = match val_type {
                0 => format!("{}", rng.next_u64() % 100),
                1 => "".to_string(),
                _ => "val".to_string(),
            };
            line.push_str(&format!(",{tag}={val}"));
        }
        line.push(' ');
        for i in 0..num_fields {
            if i > 0 {
                line.push(',');
            }
            let field = fields[rng.next_usize(fields.len())];
            let val_type = rng.next_usize(6);
            let val = match val_type {
                0 => format!("{}i", rng.next_u64() % 10000),
                1 => format!("{:.2}", rng.next_u64() as f64 / 100.0),
                2 => "true".to_string(),
                3 => "\"hello\"".to_string(),
                4 => "".to_string(),
                _ => format!("{}", rng.next_u64()),
            };
            line.push_str(&format!("{field}={val}"));
        }

        if rng.next_usize(3) > 0 {
            line.push_str(&format!(" {}", rng.next_u64()));
        }

        inputs.push(line);
    }

    inputs
}

/// Feed random/malformed ILP lines to the parser, verify no panics.
#[test]
fn fuzz_ilp_parser_no_panic() {
    let mut rng = SimpleRng::new(67890);
    let inputs = generate_random_ilp(&mut rng, 1000);

    let mut ok_count = 0;
    let mut err_count = 0;

    for line in &inputs {
        match parse_ilp_line(line) {
            Ok(_) => ok_count += 1,
            Err(_) => err_count += 1,
        }
    }

    assert!(err_count > 0, "expected some errors from random input");
    assert!(ok_count > 0, "expected some valid parses");
    eprintln!("ILP fuzz: {ok_count} ok, {err_count} errors out of {} inputs", inputs.len());
}
