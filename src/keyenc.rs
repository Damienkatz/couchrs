//! Order-preserving byte encoding of JSON view keys.
//!
//! RocksDB orders keys bytewise, so view keys are stored in an encoding
//! whose byte order equals CouchDB's view collation (collate::cmp_json):
//! null < false < true < numbers < strings < arrays < objects, strings by
//! the Unicode Collation Algorithm (ICU4X sort keys) or by code point for
//! raw collation. Every encoding is self-delimiting, so a doc id can follow
//! the key and composite keys compare element by element.

use icu_collator::CollatorBorrowed;
use icu_collator::options::CollatorOptions;
use serde_json::Value;
use std::sync::OnceLock;

const NULL: u8 = 0x10;
const FALSE: u8 = 0x20;
const TRUE: u8 = 0x21;
const NUMBER: u8 = 0x30;
const STRING: u8 = 0x40;
const ARRAY: u8 = 0x50;
const OBJECT: u8 = 0x60;
/// Ends an array or object; below every element tag, so a prefix sorts first.
const END: u8 = 0x00;
/// Starts each object member; above END.
const MEMBER: u8 = 0x01;

fn collator() -> &'static CollatorBorrowed<'static> {
    static C: OnceLock<CollatorBorrowed<'static>> = OnceLock::new();
    C.get_or_init(|| CollatorBorrowed::try_new(Default::default(), CollatorOptions::default()).expect("collator"))
}

/// Append bytes so that they sort as themselves and terminate unambiguously:
/// 0x00 becomes 0x00 0xFF, and the end is 0x00 0x00 (below any escaped byte).
fn escaped(bytes: &[u8], out: &mut Vec<u8>) {
    for &b in bytes {
        out.push(b);
        if b == 0 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0, 0]);
}

fn string(s: &str, raw: bool, out: &mut Vec<u8>) {
    out.push(STRING);
    if raw {
        // UTF-8 byte order is code point order.
        escaped(s.as_bytes(), out);
    } else {
        let mut key = Vec::new();
        let Ok(()) = collator().write_sort_key_to(s, &mut key);
        escaped(&key, out);
    }
}

fn number(n: &serde_json::Number, out: &mut Vec<u8>) {
    out.push(NUMBER);
    let f = n.as_f64().unwrap_or(0.0);
    let f = if f == 0.0 { 0.0 } else { f }; // -0 sorts as 0
    let bits = f.to_bits();
    // Flip so that unsigned big-endian order is numeric order.
    let ordered = if bits >> 63 == 1 { !bits } else { bits ^ (1 << 63) };
    out.extend_from_slice(&ordered.to_be_bytes());
}

/// Append the order-preserving encoding of `v`.
pub fn encode(v: &Value, raw: bool, out: &mut Vec<u8>) {
    match v {
        Value::Null => out.push(NULL),
        Value::Bool(false) => out.push(FALSE),
        Value::Bool(true) => out.push(TRUE),
        Value::Number(n) => number(n, out),
        Value::String(s) => string(s, raw, out),
        Value::Array(a) => {
            out.push(ARRAY);
            for e in a {
                encode(e, raw, out);
            }
            out.push(END);
        }
        Value::Object(o) => {
            out.push(OBJECT);
            for (k, e) in o {
                out.push(MEMBER);
                string(k, raw, out);
                encode(e, raw, out);
            }
            out.push(END);
        }
    }
}

/// Encoded view row key: the key, then the doc id (raw bytes).
pub fn row_key(prefix: u8, key: &Value, raw: bool, doc_id: &str) -> Vec<u8> {
    let mut out = vec![prefix];
    encode(key, raw, &mut out);
    escaped(doc_id.as_bytes(), &mut out);
    out
}

/// Lowest row key for `key` (before any doc id).
pub fn key_min(prefix: u8, key: &Value, raw: bool) -> Vec<u8> {
    let mut out = vec![prefix];
    encode(key, raw, &mut out);
    out
}

/// Above every row key for `key`: escaped doc ids never contain 0xFF in
/// their first byte position (UTF-8 never has 0xFF).
pub fn key_max(prefix: u8, key: &Value, raw: bool) -> Vec<u8> {
    let mut out = key_min(prefix, key, raw);
    out.push(0xFF);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collate::{cmp_json, cmp_json_raw};
    use serde_json::json;
    use std::cmp::Ordering;

    fn enc(v: &Value, raw: bool) -> Vec<u8> {
        let mut o = Vec::new();
        encode(v, raw, &mut o);
        o
    }

    /// Small deterministic generator of varied JSON values.
    fn gen_value(x: &mut u64, depth: u32) -> Value {
        *x ^= *x << 13;
        *x ^= *x >> 7;
        *x ^= *x << 17;
        let strs = ["", "a", "A", "aa", "b", "B", "ba", "é", "e", "f", "\u{0}", "a\u{0}", "z", "Z", "ä", "résumé", "resume"];
        match (*x % if depth > 2 { 5 } else { 7 }) as u8 {
            0 => Value::Null,
            1 => json!(*x % 2 == 0),
            2 => json!(((*x >> 8) % 2001) as i64 - 1000),
            3 => json!(((*x >> 8) % 10_000) as f64 / 7.0 - 700.0),
            4 => json!(strs[((*x >> 8) as usize) % strs.len()]),
            5 => Value::Array((0..(*x >> 9) % 4).map(|_| gen_value(x, depth + 1)).collect()),
            _ => {
                let mut m = serde_json::Map::new();
                for _ in 0..(*x >> 9) % 3 {
                    let k = strs[((*x >> 12) as usize) % strs.len()].to_string();
                    let v = gen_value(x, depth + 1);
                    m.insert(k, v);
                }
                Value::Object(m)
            }
        }
    }

    #[test]
    fn byte_order_matches_collation() {
        let mut x = 0x9E3779B97F4A7C15u64;
        let vals: Vec<Value> = (0..600).map(|_| gen_value(&mut x, 0)).collect();
        for raw in [false, true] {
            for a in &vals {
                for b in &vals {
                    let want = if raw { cmp_json_raw(a, b) } else { cmp_json(a, b) };
                    let got = enc(a, raw).cmp(&enc(b, raw));
                    assert_eq!(got, want, "raw={raw}: {a} vs {b}");
                }
            }
        }
    }

    #[test]
    fn doc_id_breaks_ties_and_bounds_bracket_rows() {
        let k = json!(["x", 1]);
        let a = row_key(0, &k, false, "a");
        let b = row_key(0, &k, false, "b");
        assert!(a < b);
        assert!(key_min(0, &k, false) < a && b < key_max(0, &k, false));
        // A longer key with the same prefix sorts after every row of k.
        assert!(key_max(0, &k, false) < key_min(0, &json!(["x", 1, null]), false));
        assert_eq!(enc(&json!(1), false).cmp(&enc(&json!(1.0), false)), Ordering::Equal);
    }
}
