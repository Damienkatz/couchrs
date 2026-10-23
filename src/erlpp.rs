//! Erlang's `~p` layout (io_lib_pretty, as OTP 27 has it) for the flat
//! term text we write. CouchDB's `couch_util:to_binary/1` formats
//! non-binary reasons with `~p`, which wraps a term longer than its
//! 80-column line, so the error reasons it reports carry newlines and
//! indentation. Only the layout is computed here: atoms, strings,
//! binaries and numbers keep the text they were written with.

/// Line length, as `~p` uses it.
const LL: i64 = 80;

struct Node {
    /// Characters it takes on one line.
    len: i64,
    kind: Kind,
}

enum Kind {
    /// Atomic: written as is.
    Flat(String),
    /// Tagged: its first element is an atom, and it has others.
    Tuple(bool, Vec<Node>),
    List(Vec<Node>),
}

/// `io_lib:format("~p", [T])` of the term `text` writes, or `text` itself
/// if it isn't one (or needs no breaking).
pub fn pretty(text: &str) -> String {
    let mut p = Parser { s: text.as_bytes(), i: 0 };
    let node = match p.term() {
        Some(n) if p.i == p.s.len() => n,
        _ => return text.to_string(),
    };
    let col = 1;
    let m = node.len;
    if matches!(node.kind, Kind::Flat(_)) || node.len < LL - col {
        return text.to_string();
    }
    // The indentation for tagged tuples: none past the tag if that fits,
    // else 4, else 1.
    let tind = [-1, 4].into_iter().find(|&i| cind(&node, col, m, i, 0, 0).is_ok()).unwrap_or(1);
    let mut out = String::new();
    pp(&node, col, m, tind, (col - 1) as usize, 0, 0, &mut out);
    out
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<u8> {
        self.s.get(self.i).copied()
    }

    fn eat(&mut self, c: u8) -> bool {
        if self.peek() == Some(c) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn term(&mut self) -> Option<Node> {
        let start = self.i;
        match self.peek()? {
            b'{' => self.seq(b'}').map(|es| {
                let tagged = es.len() > 1 && is_atom(&es[0]);
                node(es, |es| Kind::Tuple(tagged, es), "{}")
            }),
            b'[' => self.seq(b']').map(|es| node(es, Kind::List, "[]")),
            b'"' => {
                self.quoted(b'"')?;
                Some(self.flat(start))
            }
            b'\'' => {
                self.quoted(b'\'')?;
                Some(self.flat(start))
            }
            b'<' if self.s.get(self.i + 1) == Some(&b'<') => {
                self.i += 2;
                if self.peek() == Some(b'"') {
                    self.quoted(b'"')?;
                    if self.s[self.i..].starts_with(b"/utf8") {
                        self.i += 5;
                    }
                } else {
                    while self.peek().is_some_and(|c| c.is_ascii_digit() || c == b',' || c == b':') {
                        self.i += 1;
                    }
                }
                (self.eat(b'>') && self.eat(b'>')).then(|| self.flat(start))
            }
            // A pid, or a #Ref<...>, #Port<...> or #Fun<...>.
            b'<' | b'#' => {
                let end = self.s[self.i..].iter().position(|&c| c == b'>')?;
                self.i += end + 1;
                Some(self.flat(start))
            }
            b'a'..=b'z' => {
                while self.peek().is_some_and(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'@') {
                    self.i += 1;
                }
                Some(self.flat(start))
            }
            b'-' | b'0'..=b'9' => {
                self.eat(b'-');
                while self.peek().is_some_and(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'+' | b'-' | b'#')) {
                    self.i += 1;
                }
                Some(self.flat(start))
            }
            _ => None,
        }
    }

    /// Comma-separated terms up to `close`.
    fn seq(&mut self, close: u8) -> Option<Vec<Node>> {
        self.i += 1;
        let mut es = Vec::new();
        if self.eat(close) {
            return Some(es);
        }
        loop {
            es.push(self.term()?);
            if self.eat(close) {
                return Some(es);
            }
            if !self.eat(b',') {
                return None;
            }
        }
    }

    fn quoted(&mut self, q: u8) -> Option<()> {
        self.i += 1;
        loop {
            match self.peek()? {
                b'\\' => self.i += 2,
                c => {
                    self.i += 1;
                    if c == q {
                        return Some(());
                    }
                }
            }
        }
    }

    fn flat(&self, start: usize) -> Node {
        let s = String::from_utf8_lossy(&self.s[start..self.i]).into_owned();
        Node { len: s.chars().count() as i64, kind: Kind::Flat(s) }
    }
}

fn node(es: Vec<Node>, kind: impl FnOnce(Vec<Node>) -> Kind, empty: &str) -> Node {
    if es.is_empty() {
        return Node { len: 2, kind: Kind::Flat(empty.into()) };
    }
    // Brackets, elements and the commas between them.
    let len = 2 + es.iter().map(|e| e.len).sum::<i64>() + es.len() as i64 - 1;
    Node { len, kind: kind(es) }
}

fn is_atom(n: &Node) -> bool {
    matches!(&n.kind, Kind::Flat(s) if s.starts_with(|c: char| c.is_ascii_lowercase() || c == '\''))
}

/// ?ATM: written as a flat string.
fn atm(n: &Node) -> bool {
    matches!(n.kind, Kind::Flat(_))
}

fn write(n: &Node, out: &mut String) {
    let elems = |es: &[Node], open: char, close: char, out: &mut String| {
        out.push(open);
        for (i, e) in es.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            write(e, out);
        }
        out.push(close);
    };
    match &n.kind {
        Kind::Flat(s) => out.push_str(s),
        Kind::Tuple(_, es) => elems(es, '{', '}', out),
        Kind::List(es) => elems(es, '[', ']', out),
    }
}

fn last_depth(rest: &[Node], ld: i64) -> i64 {
    if rest.is_empty() { ld + 1 } else { 0 }
}

/// Fits on the rest of the line, LD closing brackets included.
fn fits(n: &Node, col: i64, m: i64, ld: i64, w: i64) -> bool {
    n.len < LL - col - ld && n.len + w + ld <= m
}

/// The next element can follow on the same line.
fn fits_tail(e: &Node, col: i64, m: i64, ld1: i64, w: i64) -> bool {
    let elen = 1 + e.len;
    atm(e) && if ld1 == 0 { elen + 1 < LL - col && w + elen < m } else { elen < LL - col - ld1 && w + elen + ld1 <= m }
}

#[allow(clippy::too_many_arguments)]
fn pp(n: &Node, col: i64, m: i64, tind: i64, ind: usize, ld: i64, w: i64, out: &mut String) {
    if fits(n, col, m, ld, w) {
        return write(n, out);
    }
    match &n.kind {
        Kind::Flat(s) => out.push_str(s),
        Kind::List(es) => {
            out.push('[');
            pp_list(es, col + 1, m, tind, ind + 1, ld, w + 1, out);
            out.push(']');
        }
        Kind::Tuple(false, es) => {
            out.push('{');
            pp_list(es, col + 1, m, tind, ind + 1, ld, w + 1, out);
            out.push('}');
        }
        Kind::Tuple(true, es) => {
            out.push('{');
            let tlen = es[0].len;
            write(&es[0], out);
            let tag_ind = tlen + 2;
            let tcol = col + tag_ind;
            if tind > 0 && tag_ind > tind {
                pp_tail(&es[1..], col + tind, tcol, m, tind, ind + tind as usize, ld, w + 1 + tlen, out);
            } else {
                out.push(',');
                pp_list(&es[1..], tcol, m, tind, ind + tag_ind as usize, ld, w + 1 + tlen + 1, out);
            }
            out.push('}');
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn pp_list(es: &[Node], col0: i64, m: i64, tind: i64, ind: usize, ld: i64, w: i64, out: &mut String) {
    let we = pp_element(&es[0], col0, m, tind, ind, last_depth(&es[1..], ld), w, out);
    pp_tail(&es[1..], col0, col0 + we, m, tind, ind, ld, w + we, out);
}

#[allow(clippy::too_many_arguments)]
fn pp_tail(es: &[Node], col0: i64, col: i64, m: i64, tind: i64, ind: usize, ld: i64, w: i64, out: &mut String) {
    let Some((e, rest)) = es.split_first() else { return };
    let ld1 = last_depth(rest, ld);
    if fits_tail(e, col, m, ld1, w) {
        out.push(',');
        write(e, out);
        let elen = 1 + e.len;
        pp_tail(rest, col0, col + elen, m, tind, ind, ld, w + elen, out);
    } else {
        out.push_str(",\n");
        out.extend(std::iter::repeat_n(' ', ind));
        let we = pp_element(e, col0, m, tind, ind, ld1, 0, out);
        pp_tail(rest, col0, col0 + we, m, tind, ind, ld, we, out);
    }
}

/// An element; what it takes of the line (all of it if it was broken).
#[allow(clippy::too_many_arguments)]
fn pp_element(e: &Node, col: i64, m: i64, tind: i64, ind: usize, ld: i64, w: i64, out: &mut String) -> i64 {
    if fits(e, col, m, ld, w) && atm(e) {
        write(e, out);
        e.len
    } else {
        pp(e, col, m, tind, ind, ld, w, out);
        LL
    }
}

/// cind/7: whether `ind` works as the tag indentation, which it doesn't
/// when a tag would push the rest past half the line.
fn cind(n: &Node, col: i64, m: i64, ind: i64, ld: i64, w: i64) -> Result<(), ()> {
    if fits(n, col, m, ld, w) {
        return Ok(());
    }
    match &n.kind {
        Kind::Flat(_) => Ok(()),
        Kind::List(es) | Kind::Tuple(false, es) => cind_list(es, col + 1, m, ind, ld, w + 1),
        Kind::Tuple(true, es) => {
            let w = w + 1;
            let tlen = es[0].len;
            let tag_ind = tlen + 2;
            let tcol = col + tag_ind;
            if ind > 0 && tag_ind > ind {
                let col1 = col + ind;
                if m + col1 <= LL || col1 <= LL / 2 { cind_tail(&es[1..], col1, tcol, m, ind, ld, w + tlen) } else { Err(()) }
            } else if m + tcol < LL || tcol < LL / 2 {
                cind_list(&es[1..], tcol, m, ind, ld, w + tlen + 1)
            } else {
                Err(())
            }
        }
    }
}

fn cind_list(es: &[Node], col0: i64, m: i64, ind: i64, ld: i64, w: i64) -> Result<(), ()> {
    let we = cind_element(&es[0], col0, m, ind, last_depth(&es[1..], ld), w)?;
    cind_tail(&es[1..], col0, col0 + we, m, ind, ld, w + we)
}

fn cind_tail(es: &[Node], col0: i64, col: i64, m: i64, ind: i64, ld: i64, w: i64) -> Result<(), ()> {
    let Some((e, rest)) = es.split_first() else { return Ok(()) };
    let ld1 = last_depth(rest, ld);
    if fits_tail(e, col, m, ld1, w) {
        cind_tail(rest, col0, col + 1 + e.len, m, ind, ld, w + 1 + e.len)
    } else {
        let we = cind_element(e, col0, m, ind, ld1, 0)?;
        cind_tail(rest, col0, col0 + we, m, ind, ld, we)
    }
}

fn cind_element(e: &Node, col: i64, m: i64, ind: i64, ld: i64, w: i64) -> Result<i64, ()> {
    if fits(e, col, m, ld, w) && atm(e) {
        return Ok(e.len);
    }
    cind(e, col, m, ind, ld, w)?;
    Ok(LL)
}

#[cfg(test)]
mod tests {
    use super::pretty;

    // Each as OTP 27's io_lib:format("~p", [T]) lays it out.
    #[test]
    fn matches_otp() {
        let cases: &[(&str, &str)] = &[
            (
                r#"{http_request_failed,"GET","http://127.0.0.1:1/x/",{error,{error,{conn_failed,{error,econnrefused}}}}}"#,
                "{http_request_failed,\"GET\",\"http://127.0.0.1:1/x/\",\n                     {error,{error,{conn_failed,{error,econnrefused}}}}}",
            ),
            (
                r#"{http_request_failed,"GET","http://admin:*****@127.0.0.1:15984/some_really_long_database_name_here/_changes?feed=normal&style=all_docs",{error,{code,500}}}"#,
                "{http_request_failed,\"GET\",\n                     \"http://admin:*****@127.0.0.1:15984/some_really_long_database_name_here/_changes?feed=normal&style=all_docs\",\n                     {error,{code,500}}}",
            ),
            (
                "[{alpha,1},{beta,2},{gamma,3},{delta,4},{epsilon,5},{zeta,6},{eta,7},{theta,8},{iota,9}]",
                "[{alpha,1},\n {beta,2},\n {gamma,3},\n {delta,4},\n {epsilon,5},\n {zeta,6},\n {eta,7},\n {theta,8},\n {iota,9}]",
            ),
            (
                r#"{bad_request,<<"a long binary message that goes on and on and on past the end of the line">>}"#,
                r#"{bad_request,<<"a long binary message that goes on and on and on past the end of the line">>}"#,
            ),
            (
                r#"{shutdown,{error,{http_request_failed,"POST","http://127.0.0.1:5984/db/_bulk_docs",{error,req_timedout}}}}"#,
                "{shutdown,{error,{http_request_failed,\"POST\",\n                                      \"http://127.0.0.1:5984/db/_bulk_docs\",\n                                      {error,req_timedout}}}}",
            ),
            (
                "[aaaaaaaaaa,bbbbbbbbbb,cccccccccc,dddddddddd,eeeeeeeeee,ffffffffff,gggggggggg,hhhhhhhhhh,iiiiiiiiii]",
                "[aaaaaaaaaa,bbbbbbbbbb,cccccccccc,dddddddddd,eeeeeeeeee,ffffffffff,gggggggggg,\n hhhhhhhhhh,iiiiiiiiii]",
            ),
            (
                "{{a,b},{cccccccccccccccccccccccccccccc,dddddddddddddddddddddddddddddd},{eeeeeeeeeeeeeeee,ffffffffffffffffff}}",
                "{{a,b},\n {cccccccccccccccccccccccccccccc,dddddddddddddddddddddddddddddd},\n {eeeeeeeeeeeeeeee,ffffffffffffffffff}}",
            ),
            (
                r#"{worker_died,<0.1234.0>,{http_request_failed,"GET","http://127.0.0.1:15984/source_database/_changes",{error,{code,503}}}}"#,
                "{worker_died,<0.1234.0>,\n             {http_request_failed,\"GET\",\n                                  \"http://127.0.0.1:15984/source_database/_changes\",\n                                  {error,{code,503}}}}",
            ),
            (
                r#"{a_very_long_tag_name_that_is_more_than_forty_characters_long_x,{some,nested,"tuple value here"},[1,2,3],"and a string"}"#,
                "{a_very_long_tag_name_that_is_more_than_forty_characters_long_x,\n    {some,nested,\"tuple value here\"},\n    [1,2,3],\n    \"and a string\"}",
            ),
            (
                r#"{error,{[{<<"a">>,1},{<<"bbbbbbbbbbbbbbbbbbbbbbbbbbbbb">>,{[{<<"cccccccccccccccccccccc">>,null}]}}],x}}"#,
                "{error,{[{<<\"a\">>,1},\n         {<<\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbb\">>,\n          {[{<<\"cccccccccccccccccccccc\">>,null}]}}],\n        x}}",
            ),
        ];
        for (flat, want) in cases {
            assert_eq!(pretty(flat), *want, "{flat}");
        }
    }

    #[test]
    fn leaves_other_text() {
        for s in ["db_not_found: could not open http://h/x/", "{conn_failed,error sending request}", "short", "{a,b}"] {
            assert_eq!(pretty(s), s);
        }
    }
}
