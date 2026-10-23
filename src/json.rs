//! JSON as CouchDB reads and writes it (jiffy): integers exact whatever
//! their size (serde_json's arbitrary_precision keeps a number's digits),
//! floats as the nearest double, written compactly in Erlang's shortest
//! form, so 1000.0 is `1.0e3` and 1e21 is `1.0e21`.

use serde::Serialize;
use serde_json::{Number, Value};
use std::cmp::Ordering;
use std::io;

/// Whether a number is an integer, as written (no fraction or exponent).
pub fn is_integer(n: &Number) -> bool {
    !n.as_str().bytes().any(|b| matches!(b, b'.' | b'e' | b'E'))
}

/// An integer's value, if it fits 128 bits.
pub fn int128(n: &Number) -> Option<i128> {
    if is_integer(n) { n.as_str().parse().ok() } else { None }
}

/// An integer from 128 bits.
pub fn from_int128(i: i128) -> Value {
    Value::Number(Number::from_string_unchecked(i.to_string()))
}

/// A number's text as ±0.DIGITS × 10^point, DIGITS being its significant
/// digits (no leading or trailing zeros; none for zero) split where its
/// decimal point was.
struct Decimal<'a> {
    neg: bool,
    digits: (&'a [u8], &'a [u8]),
    point: i64,
}

fn trim_zeros(d: &[u8]) -> &[u8] {
    let n = d.iter().rev().take_while(|&&c| c == b'0').count();
    &d[..d.len() - n]
}

impl<'a> Decimal<'a> {
    fn of(s: &'a str) -> Decimal<'a> {
        let s = s.as_bytes();
        let (neg, s) = match s.split_first() {
            Some((b'-', rest)) => (true, rest),
            _ => (false, s),
        };
        let digits = |from: usize| from + s[from..].iter().take_while(|c| c.is_ascii_digit()).count();
        let i = digits(0);
        let int = &s[..i];
        let (frac, i) = match s.get(i) {
            Some(b'.') => {
                let j = digits(i + 1);
                (&s[i + 1..j], j)
            }
            _ => (&[][..], i),
        };
        let mut exp: i64 = 0;
        if i < s.len() {
            let (eneg, e) = match s.get(i + 1) {
                Some(b'-') => (true, &s[i + 2..]),
                Some(b'+') => (false, &s[i + 2..]),
                _ => (false, &s[i + 1..]),
            };
            exp = e.iter().fold(0i64, |x, d| x.saturating_mul(10).saturating_add((d - b'0') as i64));
            if eneg {
                exp = -exp;
            }
        }
        // JSON integer parts have no leading zeros, but can be 0.
        let (head, tail, point) = if int.iter().all(|&c| c == b'0') {
            let z = frac.iter().take_while(|&&c| c == b'0').count();
            (&frac[z..], &[][..], exp.saturating_sub(z as i64))
        } else {
            (int, frac, exp.saturating_add(int.len() as i64))
        };
        let tail = trim_zeros(tail);
        let head = if tail.is_empty() { trim_zeros(head) } else { head };
        Decimal { neg, digits: (head, tail), point }
    }

    fn sign(&self) -> i8 {
        match (self.digits.0.is_empty(), self.neg) {
            (true, _) => 0,
            (_, true) => -1,
            _ => 1,
        }
    }
}

/// Two split digit strings compared as one string each.
fn cmp_digits<'a>(mut a: (&'a [u8], &'a [u8]), mut b: (&'a [u8], &'a [u8])) -> Ordering {
    loop {
        if a.0.is_empty() {
            if a.1.is_empty() {
                return if b.0.is_empty() && b.1.is_empty() { Ordering::Equal } else { Ordering::Less };
            }
            a = (a.1, &[]);
        }
        if b.0.is_empty() {
            if b.1.is_empty() {
                return Ordering::Greater;
            }
            b = (b.1, &[]);
        }
        let n = a.0.len().min(b.0.len());
        match a.0[..n].cmp(&b.0[..n]) {
            Ordering::Equal => (a.0, b.0) = (&a.0[n..], &b.0[n..]),
            o => return o,
        }
    }
}

/// A number written without an exponent, the common case: compared by
/// sign, then integer digits (JSON gives them no leading zeros, so more
/// digits is bigger), then fraction digits aligned at the point.
struct Plain<'a> {
    neg: bool,
    int: &'a [u8],
    frac: &'a [u8],
}

impl<'a> Plain<'a> {
    fn of(s: &'a str) -> Option<Plain<'a>> {
        let s = s.as_bytes();
        let (neg, s) = match s.split_first() {
            Some((b'-', rest)) => (true, rest),
            _ => (false, s),
        };
        let mut point = s.len();
        for (i, &c) in s.iter().enumerate() {
            match c {
                b'.' => point = i,
                b'e' | b'E' => return None,
                _ => {}
            }
        }
        let (int, frac) = (&s[..point], s.get(point + 1..).unwrap_or(&[]));
        // -0 and -0.0 are zero.
        let neg = neg && !(int == b"0" && frac.iter().all(|&c| c == b'0'));
        Some(Plain { neg, int, frac })
    }

    fn cmp(&self, o: &Plain) -> Ordering {
        if self.neg != o.neg {
            return if self.neg { Ordering::Less } else { Ordering::Greater };
        }
        let mag = self.int.len().cmp(&o.int.len()).then_with(|| self.int.cmp(o.int)).then_with(|| {
            let n = self.frac.len().min(o.frac.len());
            let nonzero = |d: &[u8]| d.iter().any(|&c| c != b'0');
            self.frac[..n].cmp(&o.frac[..n]).then_with(|| {
                if nonzero(&self.frac[n..]) {
                    Ordering::Greater
                } else if nonzero(&o.frac[n..]) {
                    Ordering::Less
                } else {
                    Ordering::Equal
                }
            })
        });
        if self.neg { mag.reverse() } else { mag }
    }
}

/// Numbers compared by value, exactly, as Erlang's term order compares
/// them: integers whatever their size, and an integer with a float by
/// their values (9007199254740993 > 9007199254740992.0), from their
/// digits without parsing them.
pub fn cmp_numbers(a: &Number, b: &Number) -> Ordering {
    if let (Some(x), Some(y)) = (Plain::of(a.as_str()), Plain::of(b.as_str())) {
        return x.cmp(&y);
    }
    cmp_decimal(a.as_str(), b.as_str())
}

/// Two numbers' texts compared exactly, in any form.
fn cmp_decimal(a: &str, b: &str) -> Ordering {
    let (x, y) = (Decimal::of(a), Decimal::of(b));
    let (sx, sy) = (x.sign(), y.sign());
    if sx != sy || sx == 0 {
        return sx.cmp(&sy);
    }
    let mag = x.point.cmp(&y.point).then_with(|| cmp_digits(x.digits, y.digits));
    if sx < 0 { mag.reverse() } else { mag }
}

/// An integer's remainder by `d` (Erlang's rem: the dividend's sign), for
/// integers of any size; None for a zero divisor.
pub fn int_rem(n: &Number, d: i128) -> Option<i128> {
    if d == 0 {
        return None;
    }
    let s = n.as_str();
    let (neg, digits) = s.strip_prefix('-').map(|x| (true, x)).unwrap_or((false, s));
    let m = d.unsigned_abs();
    let mut r: u128 = 0;
    for b in digits.bytes() {
        r = (r * 10 + (b - b'0') as u128) % m;
    }
    let r = r as i128;
    Some(if neg { -r } else { r })
}

/// A request's JSON as jiffy decodes it: integers exactly (-0 as 0),
/// floats as the nearest double (which must be finite), so numbers equal
/// in value are equal as JSON. Err for a float out of range.
pub fn normalize(v: &mut Value) -> Result<(), ()> {
    match v {
        Value::Number(n) if is_integer(n) => {
            if n.as_str() == "-0" {
                *n = Number::from(0);
            }
        }
        Value::Number(n) => {
            let f: f64 = n.as_str().parse().map_err(|_| ())?;
            *n = Number::from_f64(f).ok_or(())?;
        }
        Value::Array(a) => a.iter_mut().try_for_each(normalize)?,
        Value::Object(o) => o.values_mut().try_for_each(normalize)?,
        _ => {}
    }
    Ok(())
}

/// JSON parsed and normalized as jiffy decodes it; None if it isn't
/// JSON (or has a float out of range).
pub fn parse(b: &[u8]) -> Option<Value> {
    let mut v: Value = serde_json::from_slice(b).ok()?;
    normalize(&mut v).ok()?;
    Some(v)
}

/// A float as jiffy writes it: short_float/1, with no negative zero.
pub fn json_float(f: f64) -> String {
    if f == 0.0 { "0.0".into() } else { short_float(f) }
}

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
        w.write_all(json_float(v).as_bytes())
    }

    fn write_f32<W: ?Sized + io::Write>(&mut self, w: &mut W, v: f32) -> io::Result<()> {
        w.write_all(json_float(v as f64).as_bytes())
    }

    /// A Value's number, by its digits: an integer as it is, a float in
    /// the shortest form.
    fn write_number_str<W: ?Sized + io::Write>(&mut self, w: &mut W, v: &str) -> io::Result<()> {
        if v.bytes().any(|b| matches!(b, b'.' | b'e' | b'E')) {
            w.write_all(json_float(v.parse().unwrap_or(0.0)).as_bytes())
        } else {
            w.write_all(v.as_bytes())
        }
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
            "[1.0e3,100.0,10.0,1.0e21,1.0e22,0.0001,1.0e-5,123456789.0,1.5e300,5.0e-324,0.1,1.0e15,1.2345678901234567e19,1.0,-2.5e-7,1234.5,1.0e16,9007199254740993,1.2e5,3.0e5,0.5,0.002,1.7976931348623157e308,0.0]"
        );
        assert_eq!(to_string(&json!({"a": "é\u{1}", "b": [true, null]})), r#"{"a":"é\u0001","b":[true,null]}"#);
    }

    #[test]
    fn integers_exact_floats_normalized() {
        let mut v: Value = serde_json::from_str(
            r#"{"big":123456789012345678901234567890123456789012345,"neg":-170141183460469231731687303715884105729,"z":-0,"zf":-0.0,"e":1E2,"p":1.5e+3,"t":0.10,"l":1.0000000000000001}"#,
        )
        .unwrap();
        normalize(&mut v).unwrap();
        assert_eq!(
            to_string(&v),
            r#"{"big":123456789012345678901234567890123456789012345,"neg":-170141183460469231731687303715884105729,"z":0,"zf":0.0,"e":100.0,"p":1.5e3,"t":0.1,"l":1.0}"#
        );
        let mut huge: Value = serde_json::from_str("[1e400]").unwrap();
        assert!(normalize(&mut huge).is_err());
        let n = |s: &str| match serde_json::from_str::<Value>(s).unwrap() {
            Value::Number(n) => n,
            _ => unreachable!(),
        };
        assert_eq!(cmp_numbers(&n("123456789012345678901234567890"), &n("123456789012345678901234567891")), Ordering::Less);
        assert_eq!(cmp_numbers(&n("-123456789012345678901234567890"), &n("-2")), Ordering::Less);
        assert_eq!(cmp_numbers(&n("10"), &n("9.5")), Ordering::Greater);
        assert_eq!(cmp_numbers(&n("9007199254740993"), &n("9007199254740992.0")), Ordering::Greater);
        assert_eq!(cmp_numbers(&n("1000000000000000000001"), &n("1e21")), Ordering::Greater);
        assert_eq!(cmp_numbers(&n("100"), &n("1.0e2")), Ordering::Equal);
        assert_eq!(cmp_numbers(&n("0"), &n("-0.0")), Ordering::Equal);
        assert_eq!(cmp_numbers(&n("-0.5"), &n("0")), Ordering::Less);
        assert_eq!(cmp_numbers(&n("0.001"), &n("1e-3")), Ordering::Equal);
        assert_eq!(cmp_numbers(&n("0.0012"), &n("1e-3")), Ordering::Greater);
        assert_eq!(cmp_numbers(&n("-1.5e300"), &n("-2")), Ordering::Less);
        assert_eq!(cmp_numbers(&n("1.5e-300"), &n("0.1")), Ordering::Less);
        assert_eq!(cmp_numbers(&n("120"), &n("12")), Ordering::Greater);
        assert_eq!(cmp_numbers(&n("0.30000000000000004"), &n("0.3")), Ordering::Greater);
        assert_eq!(cmp_numbers(&n("-0.0"), &n("0")), Ordering::Equal);
        assert_eq!(cmp_numbers(&n("-0.0"), &n("-1")), Ordering::Greater);
        assert_eq!(cmp_numbers(&n("3"), &n("3.0")), Ordering::Equal);
        assert_eq!(cmp_numbers(&n("3"), &n("3.01")), Ordering::Less);
        assert_eq!(cmp_numbers(&n("-3"), &n("-3.01")), Ordering::Greater);
        assert_eq!(cmp_numbers(&n("0.5"), &n("5")), Ordering::Less);
        assert_eq!(cmp_numbers(&n("12.5"), &n("9.75")), Ordering::Greater);
        // Plain and exponent forms agree.
        let texts = ["0", "-0.0", "1", "-1", "0.001", "1e-3", "2.5e2", "250", "250.5", "-2.5e2", "1e21", "999999999999999999999", "0.0", "7.25", "-7.250"];
        for a in texts {
            for b in texts {
                assert_eq!(cmp_numbers(&n(a), &n(b)), cmp_decimal(a, b), "{a} vs {b}");
            }
        }
        assert_eq!(int_rem(&n("-123456789012345678901234567890"), 7), Some(-((123456789012345678901234567890u128 % 7) as i128)));
    }
}
