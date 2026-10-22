//! JSON as CouchDB writes it (jiffy): compact, with floats in Erlang's
//! shortest form, so 1000.0 is `1.0e3` and 1e21 is `1.0e21`.

use serde::Serialize;
use std::io;

/// A float as Erlang's float_to_list(F, [short]) (and so jiffy, and
/// io_lib's ~w) writes it: the shortest digits that read back the same,
/// in fixed or scientific notation, whichever is shorter (fixed on a tie).
pub fn short_float(f: f64) -> String {
    let e = format!("{:e}", f.abs());
    let (mant, exp) = e.split_once('e').unwrap_or((&e, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let digits = mant.replace('.', "");
    let sign = if f.is_sign_negative() { "-" } else { "" };
    let sci = format!("{sign}{}.{}e{exp}", &digits[..1], if digits.len() > 1 { &digits[1..] } else { "0" });
    let fixed = if exp >= 0 {
        let int_len = exp as usize + 1;
        if digits.len() <= int_len {
            format!("{sign}{digits}{}.0", "0".repeat(int_len - digits.len()))
        } else {
            format!("{sign}{}.{}", &digits[..int_len], &digits[int_len..])
        }
    } else {
        format!("{sign}0.{}{digits}", "0".repeat((-exp - 1) as usize))
    };
    if sci.len() < fixed.len() { sci } else { fixed }
}

struct Jiffy;

impl serde_json::ser::Formatter for Jiffy {
    fn write_f64<W: ?Sized + io::Write>(&mut self, w: &mut W, v: f64) -> io::Result<()> {
        w.write_all(short_float(v).as_bytes())
    }

    fn write_f32<W: ?Sized + io::Write>(&mut self, w: &mut W, v: f32) -> io::Result<()> {
        w.write_all(short_float(v as f64).as_bytes())
    }
}

pub fn to_vec<T: Serialize + ?Sized>(v: &T) -> Vec<u8> {
    let mut out = Vec::with_capacity(128);
    let mut ser = serde_json::Serializer::with_formatter(&mut out, Jiffy);
    // Serializing a Value (or our own types) to memory can't fail.
    let _ = v.serialize(&mut ser);
    out
}

pub fn to_string<T: Serialize + ?Sized>(v: &T) -> String {
    String::from_utf8(to_vec(v)).unwrap_or_default()
}

/// A body as couch_mrview_http and chttpd_db stream them: each of the
/// "rows" (and of /queries' "results", rows and all) on its own line.
pub fn rows_body(v: &serde_json::Value) -> Vec<u8> {
    let mut out = Vec::with_capacity(256);
    write_rows(v, &mut out);
    out.push(b'\n');
    out
}

fn write_rows(v: &serde_json::Value, out: &mut Vec<u8>) {
    let serde_json::Value::Object(o) = v else {
        out.extend(to_vec(v));
        return;
    };
    out.push(b'{');
    for (i, (k, x)) in o.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend(to_vec(k));
        out.push(b':');
        match (k.as_str(), x) {
            ("rows" | "results", serde_json::Value::Array(items)) => {
                out.extend_from_slice(b"[\r\n");
                for (j, item) in items.iter().enumerate() {
                    if j > 0 {
                        out.extend_from_slice(b",\r\n");
                    }
                    if k == "results" { write_rows(item, out) } else { out.extend(to_vec(item)) }
                }
                out.extend_from_slice(b"\r\n]");
            }
            _ => out.extend(to_vec(x)),
        }
    }
    out.push(b'}');
}

/// A normal or longpoll _changes body as chttpd_db sends it: the results
/// between newlines (and one more for a longpoll that timed out), then
/// the rest.
pub fn changes_body(v: &serde_json::Value, timed_out: bool) -> Vec<u8> {
    let mut out = b"{\"results\":[\n".to_vec();
    let empty = Vec::new();
    let rows = v.get("results").and_then(serde_json::Value::as_array).unwrap_or(&empty);
    for (i, r) in rows.iter().enumerate() {
        if i > 0 {
            out.extend_from_slice(b",\r\n");
        }
        out.extend(to_vec(r));
    }
    if timed_out {
        out.push(b'\n');
    }
    out.extend_from_slice(b"\n],\n");
    let rest: Vec<(&String, &serde_json::Value)> = v.as_object().into_iter().flatten().filter(|(k, _)| *k != "results").collect();
    for (i, (k, x)) in rest.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend(to_vec(k));
        out.push(b':');
        out.extend(to_vec(x));
    }
    out.extend_from_slice(b"}\n\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn floats_as_jiffy_writes_them() {
        let v: serde_json::Value = serde_json::from_str(
            "[1000.0,100.0,10.0,1e21,1e22,0.0001,0.00001,123456789.0,1.5e300,5e-324,0.1,1e15,12345678901234567890.0,1.0,-2.5e-7,1234.5,1e16,9007199254740993,120000.0,3.0e5,0.5,2e-3,1.7976931348623157e308,-0.0]",
        )
        .unwrap();
        assert_eq!(
            to_string(&v),
            "[1.0e3,100.0,10.0,1.0e21,1.0e22,0.0001,1.0e-5,123456789.0,1.5e300,5.0e-324,0.1,1.0e15,1.2345678901234567e19,1.0,-2.5e-7,1234.5,1.0e16,9007199254740993,1.2e5,3.0e5,0.5,0.002,1.7976931348623157e308,-0.0]"
        );
        assert_eq!(to_string(&json!({"a": "é\u{1}", "b": [true, null]})), r#"{"a":"é\u0001","b":[true,null]}"#);
    }
}
