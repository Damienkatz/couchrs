//! View key collation, a port of couch_ejson_compare.
//!
//! null < false < true < numbers < strings < arrays < objects. Strings use
//! the Unicode Collation Algorithm (root locale, tertiary strength) like
//! CouchDB's ICU collation, so "a" < "A" < "aa" < "b".

use icu_collator::options::CollatorOptions;
use icu_collator::CollatorBorrowed;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::cmp::Ordering;
use std::sync::OnceLock;

fn collator() -> &'static CollatorBorrowed<'static> {
    static C: OnceLock<CollatorBorrowed<'static>> = OnceLock::new();
    C.get_or_init(|| CollatorBorrowed::try_new(Default::default(), CollatorOptions::default()).expect("collator"))
}

fn type_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(false) => 1,
        Value::Bool(true) => 2,
        Value::Number(_) => 3,
        Value::String(_) => 4,
        Value::Array(_) => 5,
        Value::Object(_) => 6,
    }
}

pub fn cmp_str(a: &str, b: &str) -> Ordering {
    if a == b {
        return Ordering::Equal;
    }
    // ASCII fast path is not order-equivalent to UCA, so always collate.
    collator().compare(a, b)
}

pub fn cmp_num(a: &serde_json::Number, b: &serde_json::Number) -> Ordering {
    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        return x.cmp(&y);
    }
    let x = a.as_f64().unwrap_or(0.0);
    let y = b.as_f64().unwrap_or(0.0);
    x.partial_cmp(&y).unwrap_or(Ordering::Equal)
}

pub fn cmp_json(a: &Value, b: &Value) -> Ordering {
    let (ra, rb) = (type_rank(a), type_rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => cmp_num(x, y),
        (Value::String(x), Value::String(y)) => cmp_str(x, y),
        (Value::Array(x), Value::Array(y)) => {
            for (p, q) in x.iter().zip(y.iter()) {
                let c = cmp_json(p, q);
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        (Value::Object(x), Value::Object(y)) => {
            for ((ka, va), (kb, vb)) in x.iter().zip(y.iter()) {
                let c = cmp_str(ka, kb);
                if c != Ordering::Equal {
                    return c;
                }
                let c = cmp_json(va, vb);
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        _ => Ordering::Equal,
    }
}

/// Raw collation: type order as above, but strings by code point.
pub fn cmp_json_raw(a: &Value, b: &Value) -> Ordering {
    let (ra, rb) = (type_rank(a), type_rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => cmp_num(x, y),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Array(x), Value::Array(y)) => {
            for (p, q) in x.iter().zip(y.iter()) {
                let c = cmp_json_raw(p, q);
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        (Value::Object(x), Value::Object(y)) => {
            for ((ka, va), (kb, vb)) in x.iter().zip(y.iter()) {
                let c = ka.cmp(kb).then_with(|| cmp_json_raw(va, vb));
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        _ => Ordering::Equal,
    }
}

/// A JSON value stored in btrees as its JSON text (bincode can't encode
/// serde_json::Value directly).
#[derive(Clone, Debug, PartialEq)]
pub struct Json(pub Value);

impl Serialize for Json {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(&serde_json::to_vec(&self.0).map_err(serde::ser::Error::custom)?)
    }
}

impl<'de> Deserialize<'de> for Json {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let b: serde_bytes_compat::Bytes = Deserialize::deserialize(d)?;
        serde_json::from_slice(&b.0).map(Json).map_err(serde::de::Error::custom)
    }
}

mod serde_bytes_compat {
    use serde::de::{Deserialize, Deserializer, Visitor};
    pub struct Bytes(pub Vec<u8>);
    impl<'de> Deserialize<'de> for Bytes {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            struct V;
            impl<'de> Visitor<'de> for V {
                type Value = Bytes;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("bytes")
                }
                fn visit_bytes<E>(self, v: &[u8]) -> Result<Bytes, E> {
                    Ok(Bytes(v.to_vec()))
                }
                fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Bytes, E> {
                    Ok(Bytes(v))
                }
                fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut s: A) -> Result<Bytes, A::Error> {
                    let mut out = Vec::new();
                    while let Some(b) = s.next_element::<u8>()? {
                        out.push(b);
                    }
                    Ok(Bytes(out))
                }
            }
            d.deserialize_byte_buf(V)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn couch_collation_order() {
        // From the CouchDB view collation docs.
        let ordered = vec![
            json!(null),
            json!(false),
            json!(true),
            json!(1),
            json!(2),
            json!(3.0),
            json!(4),
            json!("a"),
            json!("A"),
            json!("aa"),
            json!("b"),
            json!("B"),
            json!("ba"),
            json!("bb"),
            json!(["a"]),
            json!(["b"]),
            json!(["b", "c"]),
            json!(["b", "c", "a"]),
            json!(["b", "d"]),
            json!(["b", "d", "e"]),
            json!({"a": 1}),
            json!({"a": 2}),
            json!({"b": 1}),
            json!({"b": 2}),
            json!({"b": 2, "a": 1}),
            json!({"b": 2, "c": 2}),
        ];
        for w in ordered.windows(2) {
            assert_eq!(cmp_json(&w[0], &w[1]), Ordering::Less, "{} < {}", w[0], w[1]);
        }
        let j = Json(json!({"x": [1, "two"]}));
        let enc = bincode::serialize(&j).unwrap();
        let back: Json = bincode::deserialize(&enc).unwrap();
        assert_eq!(back, j);
    }
}
