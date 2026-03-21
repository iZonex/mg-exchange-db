//! 500 upper/lower function tests.

use exchange_query::plan::Value;
use exchange_query::scalar::evaluate_scalar;

fn s(v: &str) -> Value {
    Value::Str(v.to_string())
}
fn null() -> Value {
    Value::Null
}
fn ev(name: &str, args: &[Value]) -> Value {
    evaluate_scalar(name, args).unwrap()
}

macro_rules! up_test {
    ($name:ident, $input:expr, $expected:expr) => {
        #[test]
        fn $name() {
            assert_eq!(ev("upper", &[s($input)]), s($expected));
        }
    };
}
macro_rules! lo_test {
    ($name:ident, $input:expr, $expected:expr) => {
        #[test]
        fn $name() {
            assert_eq!(ev("lower", &[s($input)]), s($expected));
        }
    };
}

// upper: single lowercase letters a-z
mod upper_single {
    use super::*;
    up_test!(a, "a", "A");
    up_test!(b, "b", "B");
    up_test!(c, "c", "C");
    up_test!(d, "d", "D");
    up_test!(e, "e", "E");
    up_test!(f, "f", "F");
    up_test!(g, "g", "G");
    up_test!(h, "h", "H");
    up_test!(i, "i", "I");
    up_test!(j, "j", "J");
    up_test!(k, "k", "K");
    up_test!(l, "l", "L");
    up_test!(m, "m", "M");
    up_test!(n, "n", "N");
    up_test!(o, "o", "O");
    up_test!(p, "p", "P");
    up_test!(q, "q", "Q");
    up_test!(r, "r", "R");
    up_test!(ss, "s", "S");
    up_test!(t, "t", "T");
    up_test!(u, "u", "U");
    up_test!(v, "v", "V");
    up_test!(w, "w", "W");
    up_test!(x, "x", "X");
    up_test!(y, "y", "Y");
    up_test!(z, "z", "Z");
}

// lower: single uppercase letters A-Z
mod lower_single {
    use super::*;
    lo_test!(a, "A", "a");
    lo_test!(b, "B", "b");
    lo_test!(c, "C", "c");
    lo_test!(d, "D", "d");
    lo_test!(e, "E", "e");
    lo_test!(f, "F", "f");
    lo_test!(g, "G", "g");
    lo_test!(h, "H", "h");
    lo_test!(i, "I", "i");
    lo_test!(j, "J", "j");
    lo_test!(k, "K", "k");
    lo_test!(l, "L", "l");
    lo_test!(m, "M", "m");
    lo_test!(n, "N", "n");
    lo_test!(o, "O", "o");
    lo_test!(p, "P", "p");
    lo_test!(q, "Q", "q");
    lo_test!(r, "R", "r");
    lo_test!(ss, "S", "s");
    lo_test!(t, "T", "t");
    lo_test!(u, "U", "u");
    lo_test!(v, "V", "v");
    lo_test!(w, "W", "w");
    lo_test!(x, "X", "x");
    lo_test!(y, "Y", "y");
    lo_test!(z, "Z", "z");
}

// upper: already upper stays same
mod upper_noop {
    use super::*;
    up_test!(a, "A", "A");
    up_test!(b, "B", "B");
    up_test!(c, "C", "C");
    up_test!(d, "D", "D");
    up_test!(e, "E", "E");
    up_test!(f, "F", "F");
    up_test!(g, "G", "G");
    up_test!(h, "H", "H");
    up_test!(i, "I", "I");
    up_test!(j, "J", "J");
    up_test!(k, "K", "K");
    up_test!(l, "L", "L");
    up_test!(m, "M", "M");
    up_test!(n, "N", "N");
    up_test!(o, "O", "O");
    up_test!(p, "P", "P");
    up_test!(q, "Q", "Q");
    up_test!(r, "R", "R");
    up_test!(ss, "S", "S");
    up_test!(t, "T", "T");
    up_test!(u, "U", "U");
    up_test!(v, "V", "V");
    up_test!(w, "W", "W");
    up_test!(x, "X", "X");
    up_test!(y, "Y", "Y");
    up_test!(z, "Z", "Z");
}

// lower: already lower stays same
mod lower_noop {
    use super::*;
    lo_test!(a, "a", "a");
    lo_test!(b, "b", "b");
    lo_test!(c, "c", "c");
    lo_test!(d, "d", "d");
    lo_test!(e, "e", "e");
    lo_test!(f, "f", "f");
    lo_test!(g, "g", "g");
    lo_test!(h, "h", "h");
    lo_test!(i, "i", "i");
    lo_test!(j, "j", "j");
    lo_test!(k, "k", "k");
    lo_test!(l, "l", "l");
    lo_test!(m, "m", "m");
    lo_test!(n, "n", "n");
    lo_test!(o, "o", "o");
    lo_test!(p, "p", "p");
    lo_test!(q, "q", "q");
    lo_test!(r, "r", "r");
    lo_test!(ss, "s", "s");
    lo_test!(t, "t", "t");
    lo_test!(u, "u", "u");
    lo_test!(v, "v", "v");
    lo_test!(w, "w", "w");
    lo_test!(x, "x", "x");
    lo_test!(y, "y", "y");
    lo_test!(z, "z", "z");
}

// upper: digits/specials unchanged
mod upper_pass {
    use super::*;
    up_test!(d0, "0", "0");
    up_test!(d1, "1", "1");
    up_test!(d2, "2", "2");
    up_test!(d3, "3", "3");
    up_test!(d4, "4", "4");
    up_test!(d5, "5", "5");
    up_test!(d6, "6", "6");
    up_test!(d7, "7", "7");
    up_test!(d8, "8", "8");
    up_test!(d9, "9", "9");
    up_test!(sp1, "!", "!");
    up_test!(sp2, "@", "@");
    up_test!(sp3, "#", "#");
    up_test!(sp4, "$", "$");
    up_test!(sp5, "%", "%");
    up_test!(sp6, "^", "^");
    up_test!(sp7, "&", "&");
    up_test!(sp8, "*", "*");
    up_test!(sp9, "(", "(");
    up_test!(sp10, ")", ")");
    up_test!(empty, "", "");
    up_test!(space, " ", " ");
    up_test!(tab, "\t", "\t");
    up_test!(nl, "\n", "\n");
}

// lower: digits/specials unchanged
mod lower_pass {
    use super::*;
    lo_test!(d0, "0", "0");
    lo_test!(d1, "1", "1");
    lo_test!(d2, "2", "2");
    lo_test!(d3, "3", "3");
    lo_test!(d4, "4", "4");
    lo_test!(d5, "5", "5");
    lo_test!(d6, "6", "6");
    lo_test!(d7, "7", "7");
    lo_test!(d8, "8", "8");
    lo_test!(d9, "9", "9");
    lo_test!(sp1, "!", "!");
    lo_test!(sp2, "@", "@");
    lo_test!(sp3, "#", "#");
    lo_test!(sp4, "$", "$");
    lo_test!(sp5, "%", "%");
    lo_test!(sp6, "^", "^");
    lo_test!(sp7, "&", "&");
    lo_test!(sp8, "*", "*");
    lo_test!(sp9, "(", "(");
    lo_test!(sp10, ")", ")");
    lo_test!(empty, "", "");
    lo_test!(space, " ", " ");
    lo_test!(tab, "\t", "\t");
    lo_test!(nl, "\n", "\n");
}

// upper: mixed strings
mod upper_mixed {
    use super::*;
    up_test!(m01, "hello", "HELLO");
    up_test!(m02, "world", "WORLD");
    up_test!(m03, "Hello", "HELLO");
    up_test!(m04, "hELLO", "HELLO");
    up_test!(m05, "HeLlO", "HELLO");
    up_test!(m06, "HELLO", "HELLO");
    up_test!(m07, "hello world", "HELLO WORLD");
    up_test!(m08, "Hello World", "HELLO WORLD");
    up_test!(m09, "abc123", "ABC123");
    up_test!(m10, "ABC123", "ABC123");
    up_test!(m11, "test_case", "TEST_CASE");
    up_test!(m12, "camelCase", "CAMELCASE");
    up_test!(m13, "PascalCase", "PASCALCASE");
    up_test!(m14, "snake_case", "SNAKE_CASE");
    up_test!(m15, "SCREAMING_CASE", "SCREAMING_CASE");
    up_test!(m16, "a1b2c3d4", "A1B2C3D4");
    up_test!(m17, "xyz", "XYZ");
    up_test!(m18, "foo bar baz", "FOO BAR BAZ");
    up_test!(m19, "qux quux", "QUX QUUX");
    up_test!(m20, "rust", "RUST");
    up_test!(m21, "Rust", "RUST");
    up_test!(m22, "RUST", "RUST");
    up_test!(m23, "rUsT", "RUST");
    up_test!(m24, "exchangedb", "EXCHANGEDB");
    up_test!(m25, "ExchangeDB", "EXCHANGEDB");
    up_test!(m26, "a", "A");
    up_test!(m27, "ab", "AB");
    up_test!(m28, "abc", "ABC");
    up_test!(m29, "abcd", "ABCD");
    up_test!(m30, "abcde", "ABCDE");
    up_test!(m31, "abcdefghij", "ABCDEFGHIJ");
    up_test!(m32, "the quick brown fox", "THE QUICK BROWN FOX");
    up_test!(m33, "jumps over", "JUMPS OVER");
    up_test!(m34, "lazy dog", "LAZY DOG");
    up_test!(m35, "select", "SELECT");
    up_test!(m36, "insert", "INSERT");
    up_test!(m37, "delete", "DELETE");
    up_test!(m38, "update", "UPDATE");
    up_test!(m39, "create", "CREATE");
    up_test!(m40, "table", "TABLE");
    up_test!(m41, "where", "WHERE");
    up_test!(m42, "from", "FROM");
    up_test!(m43, "count", "COUNT");
    up_test!(m44, "group", "GROUP");
    up_test!(m45, "order", "ORDER");
    up_test!(m46, "limit", "LIMIT");
    up_test!(m47, "join", "JOIN");
    up_test!(m48, "inner", "INNER");
    up_test!(m49, "left", "LEFT");
    up_test!(m50, "right", "RIGHT");
}

// lower: mixed strings
mod lower_mixed {
    use super::*;
    lo_test!(m01, "HELLO", "hello");
    lo_test!(m02, "WORLD", "world");
    lo_test!(m03, "Hello", "hello");
    lo_test!(m04, "hELLO", "hello");
    lo_test!(m05, "HeLlO", "hello");
    lo_test!(m06, "hello", "hello");
    lo_test!(m07, "HELLO WORLD", "hello world");
    lo_test!(m08, "Hello World", "hello world");
    lo_test!(m09, "ABC123", "abc123");
    lo_test!(m10, "abc123", "abc123");
    lo_test!(m11, "TEST_CASE", "test_case");
    lo_test!(m12, "CamelCase", "camelcase");
    lo_test!(m13, "PascalCase", "pascalcase");
    lo_test!(m14, "SNAKE_CASE", "snake_case");
    lo_test!(m15, "screaming_case", "screaming_case");
    lo_test!(m16, "A1B2C3D4", "a1b2c3d4");
    lo_test!(m17, "XYZ", "xyz");
    lo_test!(m18, "FOO BAR BAZ", "foo bar baz");
    lo_test!(m19, "QUX QUUX", "qux quux");
    lo_test!(m20, "RUST", "rust");
    lo_test!(m21, "Rust", "rust");
    lo_test!(m22, "rust", "rust");
    lo_test!(m23, "rUsT", "rust");
    lo_test!(m24, "EXCHANGEDB", "exchangedb");
    lo_test!(m25, "ExchangeDB", "exchangedb");
    lo_test!(m26, "A", "a");
    lo_test!(m27, "AB", "ab");
    lo_test!(m28, "ABC", "abc");
    lo_test!(m29, "ABCD", "abcd");
    lo_test!(m30, "ABCDE", "abcde");
    lo_test!(m31, "ABCDEFGHIJ", "abcdefghij");
    lo_test!(m32, "THE QUICK BROWN FOX", "the quick brown fox");
    lo_test!(m33, "JUMPS OVER", "jumps over");
    lo_test!(m34, "LAZY DOG", "lazy dog");
    lo_test!(m35, "SELECT", "select");
    lo_test!(m36, "INSERT", "insert");
    lo_test!(m37, "DELETE", "delete");
    lo_test!(m38, "UPDATE", "update");
    lo_test!(m39, "CREATE", "create");
    lo_test!(m40, "TABLE", "table");
    lo_test!(m41, "WHERE", "where");
    lo_test!(m42, "FROM", "from");
    lo_test!(m43, "COUNT", "count");
    lo_test!(m44, "GROUP", "group");
    lo_test!(m45, "ORDER", "order");
    lo_test!(m46, "LIMIT", "limit");
    lo_test!(m47, "JOIN", "join");
    lo_test!(m48, "INNER", "inner");
    lo_test!(m49, "LEFT", "left");
    lo_test!(m50, "RIGHT", "right");
}

// upper: null
mod upper_null {
    use super::*;
    #[test]
    fn null_in() {
        assert_eq!(ev("upper", &[null()]), null());
    }
}
// lower: null
mod lower_null {
    use super::*;
    #[test]
    fn null_in() {
        assert_eq!(ev("lower", &[null()]), null());
    }
}

// upper: repeated patterns
mod upper_repeat {
    use super::*;
    up_test!(r01, "aaa", "AAA");
    up_test!(r02, "bbb", "BBB");
    up_test!(r03, "ccc", "CCC");
    up_test!(r04, "ddd", "DDD");
    up_test!(r05, "eee", "EEE");
    up_test!(r06, "fff", "FFF");
    up_test!(r07, "ggg", "GGG");
    up_test!(r08, "hhh", "HHH");
    up_test!(r09, "iii", "III");
    up_test!(r10, "jjj", "JJJ");
    up_test!(r11, "kkk", "KKK");
    up_test!(r12, "lll", "LLL");
    up_test!(r13, "mmm", "MMM");
    up_test!(r14, "nnn", "NNN");
    up_test!(r15, "ooo", "OOO");
    up_test!(r16, "ppp", "PPP");
    up_test!(r17, "qqq", "QQQ");
    up_test!(r18, "rrr", "RRR");
    up_test!(r19, "sss", "SSS");
    up_test!(r20, "ttt", "TTT");
    up_test!(r21, "uuu", "UUU");
    up_test!(r22, "vvv", "VVV");
    up_test!(r23, "www", "WWW");
    up_test!(r24, "xxx", "XXX");
    up_test!(r25, "yyy", "YYY");
    up_test!(r26, "zzz", "ZZZ");
}

// lower: repeated patterns
mod lower_repeat {
    use super::*;
    lo_test!(r01, "AAA", "aaa");
    lo_test!(r02, "BBB", "bbb");
    lo_test!(r03, "CCC", "ccc");
    lo_test!(r04, "DDD", "ddd");
    lo_test!(r05, "EEE", "eee");
    lo_test!(r06, "FFF", "fff");
    lo_test!(r07, "GGG", "ggg");
    lo_test!(r08, "HHH", "hhh");
    lo_test!(r09, "III", "iii");
    lo_test!(r10, "JJJ", "jjj");
    lo_test!(r11, "KKK", "kkk");
    lo_test!(r12, "LLL", "lll");
    lo_test!(r13, "MMM", "mmm");
    lo_test!(r14, "NNN", "nnn");
    lo_test!(r15, "OOO", "ooo");
    lo_test!(r16, "PPP", "ppp");
    lo_test!(r17, "QQQ", "qqq");
    lo_test!(r18, "RRR", "rrr");
    lo_test!(r19, "SSS", "sss");
    lo_test!(r20, "TTT", "ttt");
    lo_test!(r21, "UUU", "uuu");
    lo_test!(r22, "VVV", "vvv");
    lo_test!(r23, "WWW", "www");
    lo_test!(r24, "XXX", "xxx");
    lo_test!(r25, "YYY", "yyy");
    lo_test!(r26, "ZZZ", "zzz");
}

// upper: longer repeated strings
mod upper_long {
    use super::*;
    up_test!(aa5, "aaaaa", "AAAAA");
    up_test!(bb5, "bbbbb", "BBBBB");
    up_test!(cc5, "ccccc", "CCCCC");
    up_test!(dd5, "ddddd", "DDDDD");
    up_test!(ee5, "eeeee", "EEEEE");
    up_test!(ff5, "fffff", "FFFFF");
    up_test!(gg5, "ggggg", "GGGGG");
    up_test!(hh5, "hhhhh", "HHHHH");
    up_test!(ii5, "iiiii", "IIIII");
    up_test!(jj5, "jjjjj", "JJJJJ");
    up_test!(kk5, "kkkkk", "KKKKK");
    up_test!(ll5, "lllll", "LLLLL");
    up_test!(mm5, "mmmmm", "MMMMM");
    up_test!(nn5, "nnnnn", "NNNNN");
    up_test!(oo5, "ooooo", "OOOOO");
    up_test!(pp5, "ppppp", "PPPPP");
    up_test!(qq5, "qqqqq", "QQQQQ");
    up_test!(rr5, "rrrrr", "RRRRR");
    up_test!(ss5, "sssss", "SSSSS");
    up_test!(tt5, "ttttt", "TTTTT");
    up_test!(uu5, "uuuuu", "UUUUU");
    up_test!(vv5, "vvvvv", "VVVVV");
    up_test!(ww5, "wwwww", "WWWWW");
    up_test!(xx5, "xxxxx", "XXXXX");
    up_test!(yy5, "yyyyy", "YYYYY");
    up_test!(zz5, "zzzzz", "ZZZZZ");
}

// lower: longer repeated strings
mod lower_long {
    use super::*;
    lo_test!(aa5, "AAAAA", "aaaaa");
    lo_test!(bb5, "BBBBB", "bbbbb");
    lo_test!(cc5, "CCCCC", "ccccc");
    lo_test!(dd5, "DDDDD", "ddddd");
    lo_test!(ee5, "EEEEE", "eeeee");
    lo_test!(ff5, "FFFFF", "fffff");
    lo_test!(gg5, "GGGGG", "ggggg");
    lo_test!(hh5, "HHHHH", "hhhhh");
    lo_test!(ii5, "IIIII", "iiiii");
    lo_test!(jj5, "JJJJJ", "jjjjj");
    lo_test!(kk5, "KKKKK", "kkkkk");
    lo_test!(ll5, "LLLLL", "lllll");
    lo_test!(mm5, "MMMMM", "mmmmm");
    lo_test!(nn5, "NNNNN", "nnnnn");
    lo_test!(oo5, "OOOOO", "ooooo");
    lo_test!(pp5, "PPPPP", "ppppp");
    lo_test!(qq5, "QQQQQ", "qqqqq");
    lo_test!(rr5, "RRRRR", "rrrrr");
    lo_test!(ss5, "SSSSS", "sssss");
    lo_test!(tt5, "TTTTT", "ttttt");
    lo_test!(uu5, "UUUUU", "uuuuu");
    lo_test!(vv5, "VVVVV", "vvvvv");
    lo_test!(ww5, "WWWWW", "wwwww");
    lo_test!(xx5, "XXXXX", "xxxxx");
    lo_test!(yy5, "YYYYY", "yyyyy");
    lo_test!(zz5, "ZZZZZ", "zzzzz");
}

// upper: two-char combos
mod upper_2char {
    use super::*;
    up_test!(ab, "ab", "AB");
    up_test!(ac, "ac", "AC");
    up_test!(ad, "ad", "AD");
    up_test!(ae, "ae", "AE");
    up_test!(af, "af", "AF");
    up_test!(ag, "ag", "AG");
    up_test!(ah, "ah", "AH");
    up_test!(ai, "ai", "AI");
    up_test!(aj, "aj", "AJ");
    up_test!(ak, "ak", "AK");
    up_test!(al, "al", "AL");
    up_test!(am, "am", "AM");
    up_test!(an, "an", "AN");
    up_test!(ao, "ao", "AO");
    up_test!(ap, "ap", "AP");
    up_test!(aq, "aq", "AQ");
    up_test!(ar, "ar", "AR");
    up_test!(az, "as", "AS");
    up_test!(at, "at", "AT");
    up_test!(au, "au", "AU");
    up_test!(av, "av", "AV");
    up_test!(aw, "aw", "AW");
    up_test!(ax, "ax", "AX");
    up_test!(ay, "ay", "AY");
    up_test!(azz, "az", "AZ");
}

// lower: two-char combos
mod lower_2char {
    use super::*;
    lo_test!(ab, "AB", "ab");
    lo_test!(ac, "AC", "ac");
    lo_test!(ad, "AD", "ad");
    lo_test!(ae, "AE", "ae");
    lo_test!(af, "AF", "af");
    lo_test!(ag, "AG", "ag");
    lo_test!(ah, "AH", "ah");
    lo_test!(ai, "AI", "ai");
    lo_test!(aj, "AJ", "aj");
    lo_test!(ak, "AK", "ak");
    lo_test!(al, "AL", "al");
    lo_test!(am, "AM", "am");
    lo_test!(an, "AN", "an");
    lo_test!(ao, "AO", "ao");
    lo_test!(ap, "AP", "ap");
    lo_test!(aq, "AQ", "aq");
    lo_test!(ar, "AR", "ar");
    lo_test!(az, "AS", "as");
    lo_test!(at, "AT", "at");
    lo_test!(au, "AU", "au");
    lo_test!(av, "AV", "av");
    lo_test!(aw, "AW", "aw");
    lo_test!(ax, "AX", "ax");
    lo_test!(ay, "AY", "ay");
    lo_test!(azz, "AZ", "az");
}

// upper: 3-letter combos
mod upper_3char {
    use super::*;
    up_test!(abc, "abc", "ABC");
    up_test!(def, "def", "DEF");
    up_test!(ghi, "ghi", "GHI");
    up_test!(jkl, "jkl", "JKL");
    up_test!(mno, "mno", "MNO");
    up_test!(pqr, "pqr", "PQR");
    up_test!(stu, "stu", "STU");
    up_test!(vwx, "vwx", "VWX");
    up_test!(yza, "yza", "YZA");
    up_test!(zab, "zab", "ZAB");
    up_test!(bcd, "bcd", "BCD");
    up_test!(cde, "cde", "CDE");
    up_test!(efg, "efg", "EFG");
    up_test!(fgh, "fgh", "FGH");
    up_test!(hij, "hij", "HIJ");
    up_test!(ijk, "ijk", "IJK");
    up_test!(klm, "klm", "KLM");
    up_test!(lmn, "lmn", "LMN");
    up_test!(nop, "nop", "NOP");
    up_test!(opq, "opq", "OPQ");
    up_test!(qrs, "qrs", "QRS");
    up_test!(rst, "rst", "RST");
    up_test!(tuv, "tuv", "TUV");
    up_test!(uvw, "uvw", "UVW");
    up_test!(wxy, "wxy", "WXY");
    up_test!(xyz, "xyz", "XYZ");
}

// lower: 3-letter combos
mod lower_3char {
    use super::*;
    lo_test!(abc, "ABC", "abc");
    lo_test!(def, "DEF", "def");
    lo_test!(ghi, "GHI", "ghi");
    lo_test!(jkl, "JKL", "jkl");
    lo_test!(mno, "MNO", "mno");
    lo_test!(pqr, "PQR", "pqr");
    lo_test!(stu, "STU", "stu");
    lo_test!(vwx, "VWX", "vwx");
    lo_test!(yza, "YZA", "yza");
    lo_test!(zab, "ZAB", "zab");
    lo_test!(bcd, "BCD", "bcd");
    lo_test!(cde, "CDE", "cde");
    lo_test!(efg, "EFG", "efg");
    lo_test!(fgh, "FGH", "fgh");
    lo_test!(hij, "HIJ", "hij");
    lo_test!(ijk, "IJK", "ijk");
    lo_test!(klm, "KLM", "klm");
    lo_test!(lmn, "LMN", "lmn");
    lo_test!(nop, "NOP", "nop");
    lo_test!(opq, "OPQ", "opq");
    lo_test!(qrs, "QRS", "qrs");
    lo_test!(rst, "RST", "rst");
    lo_test!(tuv, "TUV", "tuv");
    lo_test!(uvw, "UVW", "uvw");
    lo_test!(wxy, "WXY", "wxy");
    lo_test!(xyz, "XYZ", "xyz");
}

// upper: words
mod upper_words {
    use super::*;
    up_test!(w01, "alpha", "ALPHA");
    up_test!(w02, "beta", "BETA");
    up_test!(w03, "gamma", "GAMMA");
    up_test!(w04, "delta", "DELTA");
    up_test!(w05, "epsilon", "EPSILON");
    up_test!(w06, "zeta", "ZETA");
    up_test!(w07, "eta", "ETA");
    up_test!(w08, "theta", "THETA");
    up_test!(w09, "iota", "IOTA");
    up_test!(w10, "kappa", "KAPPA");
    up_test!(w11, "lambda", "LAMBDA");
    up_test!(w12, "mu", "MU");
    up_test!(w13, "nu", "NU");
    up_test!(w14, "xi", "XI");
    up_test!(w15, "omicron", "OMICRON");
    up_test!(w16, "pi", "PI");
    up_test!(w17, "rho", "RHO");
    up_test!(w18, "sigma", "SIGMA");
    up_test!(w19, "tau", "TAU");
    up_test!(w20, "upsilon", "UPSILON");
    up_test!(w21, "phi", "PHI");
    up_test!(w22, "chi", "CHI");
    up_test!(w23, "psi", "PSI");
    up_test!(w24, "omega", "OMEGA");
    up_test!(w25, "database", "DATABASE");
    up_test!(w26, "query", "QUERY");
    up_test!(w27, "engine", "ENGINE");
    up_test!(w28, "column", "COLUMN");
    up_test!(w29, "index", "INDEX");
    up_test!(w30, "partition", "PARTITION");
    up_test!(w31, "timestamp", "TIMESTAMP");
    up_test!(w32, "varchar", "VARCHAR");
    up_test!(w33, "integer", "INTEGER");
    up_test!(w34, "double", "DOUBLE");
    up_test!(w35, "boolean", "BOOLEAN");
    up_test!(w36, "primary", "PRIMARY");
    up_test!(w37, "foreign", "FOREIGN");
    up_test!(w38, "unique", "UNIQUE");
    up_test!(w39, "constraint", "CONSTRAINT");
    up_test!(w40, "default", "DEFAULT");
}

// lower: words
mod lower_words {
    use super::*;
    lo_test!(w01, "ALPHA", "alpha");
    lo_test!(w02, "BETA", "beta");
    lo_test!(w03, "GAMMA", "gamma");
    lo_test!(w04, "DELTA", "delta");
    lo_test!(w05, "EPSILON", "epsilon");
    lo_test!(w06, "ZETA", "zeta");
    lo_test!(w07, "ETA", "eta");
    lo_test!(w08, "THETA", "theta");
    lo_test!(w09, "IOTA", "iota");
    lo_test!(w10, "KAPPA", "kappa");
    lo_test!(w11, "LAMBDA", "lambda");
    lo_test!(w12, "MU", "mu");
    lo_test!(w13, "NU", "nu");
    lo_test!(w14, "XI", "xi");
    lo_test!(w15, "OMICRON", "omicron");
    lo_test!(w16, "PI", "pi");
    lo_test!(w17, "RHO", "rho");
    lo_test!(w18, "SIGMA", "sigma");
    lo_test!(w19, "TAU", "tau");
    lo_test!(w20, "UPSILON", "upsilon");
    lo_test!(w21, "PHI", "phi");
    lo_test!(w22, "CHI", "chi");
    lo_test!(w23, "PSI", "psi");
    lo_test!(w24, "OMEGA", "omega");
    lo_test!(w25, "DATABASE", "database");
    lo_test!(w26, "QUERY", "query");
    lo_test!(w27, "ENGINE", "engine");
    lo_test!(w28, "COLUMN", "column");
    lo_test!(w29, "INDEX", "index");
    lo_test!(w30, "PARTITION", "partition");
    lo_test!(w31, "TIMESTAMP", "timestamp");
    lo_test!(w32, "VARCHAR", "varchar");
    lo_test!(w33, "INTEGER", "integer");
    lo_test!(w34, "DOUBLE", "double");
    lo_test!(w35, "BOOLEAN", "boolean");
    lo_test!(w36, "PRIMARY", "primary");
    lo_test!(w37, "FOREIGN", "foreign");
    lo_test!(w38, "UNIQUE", "unique");
    lo_test!(w39, "CONSTRAINT", "constraint");
    lo_test!(w40, "DEFAULT", "default");
}

// upper: repeated 10-char strings
mod upper_repeat10 {
    use super::*;
    up_test!(r01, "aaaaaaaaaa", "AAAAAAAAAA");
    up_test!(r02, "bbbbbbbbbb", "BBBBBBBBBB");
    up_test!(r03, "cccccccccc", "CCCCCCCCCC");
    up_test!(r04, "dddddddddd", "DDDDDDDDDD");
    up_test!(r05, "eeeeeeeeee", "EEEEEEEEEE");
    up_test!(r06, "ffffffffff", "FFFFFFFFFF");
    up_test!(r07, "gggggggggg", "GGGGGGGGGG");
    up_test!(r08, "hhhhhhhhhh", "HHHHHHHHHH");
    up_test!(r09, "iiiiiiiiii", "IIIIIIIIII");
    up_test!(r10, "jjjjjjjjjj", "JJJJJJJJJJ");
    up_test!(r11, "kkkkkkkkkk", "KKKKKKKKKK");
    up_test!(r12, "llllllllll", "LLLLLLLLLL");
    up_test!(r13, "mmmmmmmmmm", "MMMMMMMMMM");
    up_test!(r14, "nnnnnnnnnn", "NNNNNNNNNN");
    up_test!(r15, "oooooooooo", "OOOOOOOOOO");
    up_test!(r16, "pppppppppp", "PPPPPPPPPP");
    up_test!(r17, "qqqqqqqqqq", "QQQQQQQQQQ");
    up_test!(r18, "rrrrrrrrrr", "RRRRRRRRRR");
    up_test!(r19, "ssssssssss", "SSSSSSSSSS");
    up_test!(r20, "tttttttttt", "TTTTTTTTTT");
    up_test!(r21, "uuuuuuuuuu", "UUUUUUUUUU");
    up_test!(r22, "vvvvvvvvvv", "VVVVVVVVVV");
    up_test!(r23, "wwwwwwwwww", "WWWWWWWWWW");
    up_test!(r24, "xxxxxxxxxx", "XXXXXXXXXX");
    up_test!(r25, "yyyyyyyyyy", "YYYYYYYYYY");
    up_test!(r26, "zzzzzzzzzz", "ZZZZZZZZZZ");
}

// lower: repeated 10-char strings
mod lower_repeat10 {
    use super::*;
    lo_test!(r01, "AAAAAAAAAA", "aaaaaaaaaa");
    lo_test!(r02, "BBBBBBBBBB", "bbbbbbbbbb");
    lo_test!(r03, "CCCCCCCCCC", "cccccccccc");
    lo_test!(r04, "DDDDDDDDDD", "dddddddddd");
    lo_test!(r05, "EEEEEEEEEE", "eeeeeeeeee");
    lo_test!(r06, "FFFFFFFFFF", "ffffffffff");
    lo_test!(r07, "GGGGGGGGGG", "gggggggggg");
    lo_test!(r08, "HHHHHHHHHH", "hhhhhhhhhh");
    lo_test!(r09, "IIIIIIIIII", "iiiiiiiiii");
    lo_test!(r10, "JJJJJJJJJJ", "jjjjjjjjjj");
    lo_test!(r11, "KKKKKKKKKK", "kkkkkkkkkk");
    lo_test!(r12, "LLLLLLLLLL", "llllllllll");
    lo_test!(r13, "MMMMMMMMMM", "mmmmmmmmmm");
    lo_test!(r14, "NNNNNNNNNN", "nnnnnnnnnn");
    lo_test!(r15, "OOOOOOOOOO", "oooooooooo");
    lo_test!(r16, "PPPPPPPPPP", "pppppppppp");
    lo_test!(r17, "QQQQQQQQQQ", "qqqqqqqqqq");
    lo_test!(r18, "RRRRRRRRRR", "rrrrrrrrrr");
    lo_test!(r19, "SSSSSSSSSS", "ssssssssss");
    lo_test!(r20, "TTTTTTTTTT", "tttttttttt");
    lo_test!(r21, "UUUUUUUUUU", "uuuuuuuuuu");
    lo_test!(r22, "VVVVVVVVVV", "vvvvvvvvvv");
    lo_test!(r23, "WWWWWWWWWW", "wwwwwwwwww");
    lo_test!(r24, "XXXXXXXXXX", "xxxxxxxxxx");
    lo_test!(r25, "YYYYYYYYYY", "yyyyyyyyyy");
    lo_test!(r26, "ZZZZZZZZZZ", "zzzzzzzzzz");
}
