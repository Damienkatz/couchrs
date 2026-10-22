//! Lucene's query syntax, as Nouveau's NouveauQueryParser (the flexible
//! StandardQueryParser) reads it, into tantivy queries: terms analyzed by
//! their field's analyzer (several words OR'd, quoted ones a phrase),
//! wildcards, prefixes, fuzzy terms, regular expressions and ranges on
//! the normalized text, and numbers parsed in the request's locale for
//! double fields. AND binds tighter than OR, words side by side are OR'd.

use super::analysis::Analyzer;
use crate::error::{CouchError, Result};
use std::ops::Bound;
use tantivy::query::{
    AllQuery, BooleanQuery, BoostQuery, EmptyQuery, FuzzyTermQuery, InvertedIndexRangeQuery, Occur, PhraseQuery, Query, RangeQuery,
    RegexQuery, TermQuery,
};
use tantivy::schema::{Field, IndexRecordOption};
use tantivy::Term;

#[derive(Debug, Clone, Copy, PartialEq)]
enum Modifier {
    Req,
    Not,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Leaf {
    /// The text as written, escapes and all.
    Term { raw: String, fuzzy: Option<Option<f32>> },
    Phrase { text: String, slop: u32 },
    Regex(String),
    Range { lo: Option<String>, hi: Option<String>, lo_incl: bool, hi_incl: bool },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Q {
    Leaf { field: String, leaf: Leaf },
    MatchAll,
    Bool(Vec<(Occur, Q)>),
    Boost(Box<Q>, f32),
}

/// How a field is indexed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Kind {
    Text,
    /// Indexed whole, without positions (string fields, _id, _partition).
    Keyword,
    Double,
}

pub fn syntax_error(q: &str, detail: &str) -> CouchError {
    CouchError::bad_request(format!("Syntax Error, cannot parse {q}: {detail}"))
}

// ---------------------------------------------------------------- parser

struct Parser<'a> {
    src: &'a str,
    s: Vec<char>,
    at: usize,
}

fn is_ws(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r' | '\u{3000}')
}

fn term_start(c: char) -> bool {
    !is_ws(c) && !"+-!():^<>=[]\"{}~\\/".contains(c)
}

fn term_char(c: char) -> bool {
    term_start(c) || c == '-' || c == '+'
}

pub fn parse(src: &str, default_field: &str) -> Result<Q> {
    let mut p = Parser { src, s: src.chars().collect(), at: 0 };
    p.ws();
    if p.at >= p.s.len() {
        return Err(syntax_error(src, "Encountered \"<EOF>\" at line 1, column 0."));
    }
    let (m, q) = p.query(default_field)?;
    p.ws();
    if p.at < p.s.len() {
        return Err(p.unexpected());
    }
    Ok(combine_one(m, q))
}

/// A lone clause's modifier still applies: -a matches nothing, as a
/// query of only prohibited clauses does.
fn combine_one(m: Option<Modifier>, q: Q) -> Q {
    match m {
        Some(Modifier::Not) => Q::Bool(vec![(Occur::MustNot, q)]),
        _ => q,
    }
}

impl Parser<'_> {
    fn peek(&self) -> Option<char> {
        self.s.get(self.at).copied()
    }

    fn peek_at(&self, n: usize) -> Option<char> {
        self.s.get(self.at + n).copied()
    }

    fn ws(&mut self) {
        while self.peek().is_some_and(is_ws) {
            self.at += 1;
        }
    }

    fn unexpected(&self) -> CouchError {
        let found: String = self.s[self.at.min(self.s.len())..].iter().take(10).collect();
        let what = if found.is_empty() { "<EOF>".to_string() } else { found };
        syntax_error(self.src, &format!("Encountered \"{what}\" at line 1, column {}.", self.at))
    }

    /// The operator keyword (AND, OR, NOT and their symbols) here, if any:
    /// a word is one only standing alone, as the lexer's longest match has it.
    fn keyword(&self) -> Option<(&'static str, usize)> {
        for (kw, name) in [("AND", "AND"), ("&&", "AND"), ("OR", "OR"), ("||", "OR"), ("NOT", "NOT")] {
            let n = kw.chars().count();
            let matches = kw.chars().enumerate().all(|(i, c)| self.peek_at(i) == Some(c));
            if matches && !self.peek_at(n).is_some_and(term_char) {
                return Some((name, n));
            }
        }
        if self.peek() == Some('!') {
            return Some(("NOT", 1));
        }
        None
    }

    fn eat_keyword(&mut self, name: &str) -> bool {
        self.ws();
        match self.keyword() {
            Some((k, n)) if k == name => {
                self.at += n;
                true
            }
            _ => false,
        }
    }

    /// Query := DisjQuery+ (the default operator, OR, between them).
    fn query(&mut self, field: &str) -> Result<(Option<Modifier>, Q)> {
        let mut parts = vec![self.disj(field)?];
        loop {
            self.ws();
            if matches!(self.peek(), None | Some(')')) {
                break;
            }
            parts.push(self.disj(field)?);
        }
        Ok(combine(parts, Occur::Should))
    }

    /// DisjQuery := ConjQuery (OR ConjQuery)*
    fn disj(&mut self, field: &str) -> Result<(Option<Modifier>, Q)> {
        let mut parts = vec![self.conj(field)?];
        while self.eat_keyword("OR") {
            parts.push(self.conj(field)?);
        }
        Ok(combine(parts, Occur::Should))
    }

    /// ConjQuery := ModClause (AND ModClause)*
    fn conj(&mut self, field: &str) -> Result<(Option<Modifier>, Q)> {
        let mut parts = vec![self.mod_clause(field)?];
        while self.eat_keyword("AND") {
            parts.push(self.mod_clause(field)?);
        }
        Ok(combine(parts, Occur::Must))
    }

    fn mod_clause(&mut self, field: &str) -> Result<(Option<Modifier>, Q)> {
        self.ws();
        let m = match self.peek() {
            Some('+') => {
                self.at += 1;
                Some(Modifier::Req)
            }
            Some('-') => {
                self.at += 1;
                Some(Modifier::Not)
            }
            _ if self.eat_keyword("NOT") => Some(Modifier::Not),
            _ => None,
        };
        Ok((m, self.clause(field)?))
    }

    fn clause(&mut self, default_field: &str) -> Result<Q> {
        self.ws();
        // field: or field<op>
        let save = self.at;
        let mut field = default_field.to_string();
        if self.peek().is_some_and(|c| term_start(c) || c == '\\') && self.keyword().is_none() {
            let name = self.raw_term();
            self.ws();
            match (self.peek(), self.peek_at(1)) {
                (Some(':' | '='), _) => {
                    self.at += 1;
                    field = unescape(&name);
                }
                (Some(c @ ('<' | '>')), next) => {
                    self.at += if next == Some('=') { 2 } else { 1 };
                    let incl = next == Some('=');
                    self.ws();
                    let value = if self.peek() == Some('"') { self.quoted()? } else { unescape(&self.raw_term()) };
                    if value.is_empty() {
                        return Err(self.unexpected());
                    }
                    let leaf = if c == '<' {
                        Leaf::Range { lo: None, hi: Some(value), lo_incl: true, hi_incl: incl }
                    } else {
                        Leaf::Range { lo: Some(value), hi: None, lo_incl: incl, hi_incl: true }
                    };
                    return self.boosted(Q::Leaf { field: unescape(&name), leaf });
                }
                _ => self.at = save,
            }
        }
        self.ws();
        match self.peek() {
            Some('(') => {
                self.at += 1;
                let (m, q) = self.query(&field)?;
                self.ws();
                if self.peek() != Some(')') {
                    return Err(self.unexpected());
                }
                self.at += 1;
                self.boosted(combine_one(m, q))
            }
            Some('"') => {
                let text = self.quoted()?;
                let mut slop = 0;
                if self.peek() == Some('~') {
                    self.at += 1;
                    slop = self.number().map(|n| n as u32).unwrap_or(0);
                }
                self.boosted(Q::Leaf { field, leaf: Leaf::Phrase { text, slop } })
            }
            Some('/') => {
                self.at += 1;
                let mut re = String::new();
                loop {
                    match self.peek() {
                        None => return Err(self.unexpected()),
                        Some('/') => break,
                        Some('\\') if self.peek_at(1) == Some('/') => {
                            re.push('/');
                            self.at += 2;
                        }
                        Some(c) => {
                            re.push(c);
                            self.at += 1;
                        }
                    }
                }
                self.at += 1;
                self.boosted(Q::Leaf { field, leaf: Leaf::Regex(re) })
            }
            Some(c @ ('[' | '{')) => {
                self.at += 1;
                let lo = self.range_bound()?;
                self.ws();
                if !(self.peek() == Some('T') && self.peek_at(1) == Some('O') && self.peek_at(2).is_some_and(is_ws)) {
                    return Err(self.unexpected());
                }
                self.at += 2;
                let hi = self.range_bound()?;
                self.ws();
                let end = match self.peek() {
                    Some(e @ (']' | '}')) => e,
                    _ => return Err(self.unexpected()),
                };
                self.at += 1;
                let open = |b: String| if b == "*" { None } else { Some(b) };
                let leaf = Leaf::Range { lo: open(lo), hi: open(hi), lo_incl: c == '[', hi_incl: end == ']' };
                self.boosted(Q::Leaf { field, leaf })
            }
            Some(c) if term_start(c) || c == '\\' => {
                if self.keyword().is_some() {
                    return Err(self.unexpected());
                }
                let raw = self.raw_term();
                if field == "*" && raw == "*" {
                    return self.boosted(Q::MatchAll);
                }
                let mut fuzzy = None;
                if self.peek() == Some('~') {
                    self.at += 1;
                    fuzzy = Some(self.number());
                }
                self.boosted(Q::Leaf { field, leaf: Leaf::Term { raw, fuzzy } })
            }
            _ => Err(self.unexpected()),
        }
    }

    fn boosted(&mut self, q: Q) -> Result<Q> {
        if self.peek() == Some('^') {
            self.at += 1;
            let b = self.number().ok_or_else(|| self.unexpected())?;
            return Ok(Q::Boost(Box::new(q), b));
        }
        Ok(q)
    }

    fn number(&mut self) -> Option<f32> {
        let start = self.at;
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            self.at += 1;
        }
        if self.peek() == Some('.') && self.peek_at(1).is_some_and(|c| c.is_ascii_digit()) {
            self.at += 1;
            while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                self.at += 1;
            }
        }
        if self.at == start {
            return None;
        }
        self.s[start..self.at].iter().collect::<String>().parse().ok()
    }

    /// A term's characters, escapes kept.
    fn raw_term(&mut self) -> String {
        let mut out = String::new();
        let mut first = true;
        while let Some(c) = self.peek() {
            if c == '\\' {
                match self.peek_at(1) {
                    Some(n) => {
                        out.push('\\');
                        out.push(n);
                        self.at += 2;
                    }
                    None => break,
                }
            } else if (first && term_start(c)) || (!first && term_char(c)) {
                out.push(c);
                self.at += 1;
            } else {
                break;
            }
            first = false;
        }
        out
    }

    fn quoted(&mut self) -> Result<String> {
        self.at += 1;
        let mut out = String::new();
        loop {
            match self.peek() {
                None => return Err(self.unexpected()),
                Some('"') => {
                    self.at += 1;
                    return Ok(out);
                }
                Some('\\') => {
                    if let Some(n) = self.peek_at(1) {
                        out.push(n);
                    }
                    self.at += 2;
                }
                Some(c) => {
                    out.push(c);
                    self.at += 1;
                }
            }
        }
    }

    fn range_bound(&mut self) -> Result<String> {
        self.ws();
        if self.peek() == Some('"') {
            return self.quoted();
        }
        let mut out = String::new();
        while let Some(c) = self.peek() {
            if is_ws(c) || c == ']' || c == '}' {
                break;
            }
            out.push(c);
            self.at += 1;
        }
        if out.is_empty() {
            return Err(self.unexpected());
        }
        Ok(unescape(&out))
    }
}

/// Clauses joined by AND (`Must`) or OR (`Should`); a lone one keeps
/// its modifier for the level above.
fn combine(mut parts: Vec<(Option<Modifier>, Q)>, plain: Occur) -> (Option<Modifier>, Q) {
    if parts.len() == 1 {
        return parts.pop().unwrap();
    }
    let clauses = parts
        .into_iter()
        .map(|(m, q)| {
            let occur = match m {
                Some(Modifier::Not) => Occur::MustNot,
                Some(Modifier::Req) => Occur::Must,
                None => plain,
            };
            (occur, q)
        })
        .collect();
    (None, Q::Bool(clauses))
}

fn unescape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut cs = raw.chars();
    while let Some(c) = cs.next() {
        if c == '\\' {
            if let Some(n) = cs.next() {
                out.push(n);
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// A term with unescaped * or ?: its text as a pattern, wildcards apart.
enum Pattern {
    Plain(String),
    /// Text then a single trailing *.
    Prefix(String),
    Wildcard(Vec<PatPart>),
}

#[derive(Clone)]
enum PatPart {
    Lit(char),
    Any,
    One,
}

fn pattern(raw: &str) -> Pattern {
    let mut parts = Vec::new();
    let mut cs = raw.chars();
    while let Some(c) = cs.next() {
        match c {
            '\\' => {
                if let Some(n) = cs.next() {
                    parts.push(PatPart::Lit(n));
                }
            }
            '*' => parts.push(PatPart::Any),
            '?' => parts.push(PatPart::One),
            c => parts.push(PatPart::Lit(c)),
        }
    }
    let wild = parts.iter().filter(|p| !matches!(p, PatPart::Lit(_))).count();
    let lit = |ps: &[PatPart]| ps.iter().map(|p| if let PatPart::Lit(c) = p { *c } else { '*' }).collect::<String>();
    match parts.last() {
        _ if wild == 0 => Pattern::Plain(lit(&parts)),
        Some(PatPart::Any) if wild == 1 => Pattern::Prefix(lit(&parts[..parts.len() - 1])),
        _ => Pattern::Wildcard(parts),
    }
}

// ----------------------------------------------------- to tantivy queries

/// What the conversion needs to know about the index.
pub struct Ctx<'a> {
    /// The tantivy field a name is indexed in, and how.
    pub field: &'a dyn Fn(&str) -> Option<(Kind, Field)>,
    pub analyzer: &'a dyn Fn(&str) -> Analyzer,
    pub locale: Option<&'a str>,
    pub query: &'a str,
}

impl Ctx<'_> {
    pub fn build(&self, q: &Q) -> Result<Box<dyn Query>> {
        Ok(self.convert(q)?.unwrap_or_else(|| Box::new(EmptyQuery)))
    }

    /// None for a clause that analyzed away (a stop word): dropped from
    /// its boolean query, as Lucene drops it.
    fn convert(&self, q: &Q) -> Result<Option<Box<dyn Query>>> {
        Ok(match q {
            Q::MatchAll => Some(Box::new(AllQuery)),
            Q::Boost(inner, b) => self.convert(inner)?.map(|q| Box::new(BoostQuery::new(q, *b)) as Box<dyn Query>),
            Q::Bool(clauses) => {
                let mut out = Vec::new();
                for (occur, c) in clauses {
                    if let Some(q) = self.convert(c)? {
                        out.push((*occur, q));
                    }
                }
                match out.len() {
                    0 => None,
                    1 if out[0].0 != Occur::MustNot => out.pop().map(|(_, q)| q),
                    _ => Some(Box::new(BooleanQuery::new(out))),
                }
            }
            Q::Leaf { field, leaf } => self.leaf(field, leaf)?,
        })
    }

    fn leaf(&self, name: &str, leaf: &Leaf) -> Result<Option<Box<dyn Query>>> {
        let Some((kind, field)) = (self.field)(name) else {
            // Nothing is indexed under that name.
            return Ok(Some(Box::new(EmptyQuery)));
        };
        if kind == Kind::Double {
            return self.double_leaf(name, field, leaf).map(Some);
        }
        let analyzer = (self.analyzer)(name);
        let term = |t: &str| Term::from_field_text(field, t);
        Ok(match leaf {
            Leaf::Term { raw, fuzzy: Some(edits) } => {
                let text = analyzer.normalize(&unescape(raw));
                let len = text.chars().count();
                let d = match edits {
                    None => 2,
                    Some(e) if *e >= 1.0 => e.min(2.0) as u8,
                    Some(e) if *e == 0.0 => 0,
                    Some(e) => (((1.0 - *e as f64) * len as f64) as u8).min(2),
                };
                Some(Box::new(FuzzyTermQuery::new(term(&text), d, true)))
            }
            Leaf::Term { raw, fuzzy: None } => match pattern(raw) {
                Pattern::Plain(text) => self.analyzed(name, &analyzer, field, kind, &text, false, 0)?,
                Pattern::Prefix(_) | Pattern::Wildcard(_) if raw.starts_with('*') || raw.starts_with('?') => {
                    return Err(CouchError::bad_request(format!("Leading wildcard is not allowed: {name}:{raw}")));
                }
                Pattern::Prefix(p) => {
                    let re = format!("{}.*", regex::escape(&analyzer.normalize(&p)));
                    Some(Box::new(regex_query(&re, field)?))
                }
                Pattern::Wildcard(parts) => {
                    let mut re = String::new();
                    let mut lit = String::new();
                    let flush = |lit: &mut String, re: &mut String| {
                        re.push_str(&regex::escape(&analyzer.normalize(lit)));
                        lit.clear();
                    };
                    for p in parts {
                        match p {
                            PatPart::Lit(c) => lit.push(c),
                            PatPart::Any => {
                                flush(&mut lit, &mut re);
                                re.push_str(".*");
                            }
                            PatPart::One => {
                                flush(&mut lit, &mut re);
                                re.push('.');
                            }
                        }
                    }
                    flush(&mut lit, &mut re);
                    Some(Box::new(regex_query(&re, field)?))
                }
            },
            Leaf::Phrase { text, slop } => self.analyzed(name, &analyzer, field, kind, text, true, *slop)?,
            Leaf::Regex(re) => Some(Box::new(regex_query(re, field)?)),
            Leaf::Range { lo, hi, lo_incl, hi_incl } => {
                let bound = |b: &Option<String>, incl: bool| match b {
                    None => Bound::Unbounded,
                    Some(s) if incl => Bound::Included(term(&analyzer.normalize(s))),
                    Some(s) => Bound::Excluded(term(&analyzer.normalize(s))),
                };
                let (mut l, h) = (bound(lo, *lo_incl), bound(hi, *hi_incl));
                if matches!((&l, &h), (Bound::Unbounded, Bound::Unbounded)) {
                    l = Bound::Included(term(""));
                }
                Some(Box::new(InvertedIndexRangeQuery::new(l, h)))
            }
        })
    }

    /// A term or phrase through the field's analyzer: one word a term
    /// query, several a disjunction (unquoted) or phrase (quoted).
    #[allow(clippy::too_many_arguments)]
    fn analyzed(&self, name: &str, analyzer: &Analyzer, field: Field, kind: Kind, text: &str, quoted: bool, slop: u32) -> Result<Option<Box<dyn Query>>> {
        let toks = analyzer.tokens(text);
        let record = if kind == Kind::Text { IndexRecordOption::WithFreqs } else { IndexRecordOption::Basic };
        // Scored as Lucene's BM25 is: without tantivy's (k1 + 1) factor.
        let lucene = |q: Box<dyn Query>| Box::new(BoostQuery::new(q, super::index::LUCENE_SCORE)) as Box<dyn Query>;
        let term_q = |t: &str| lucene(Box::new(TermQuery::new(Term::from_field_text(field, t), record)));
        Ok(match toks.len() {
            0 => None,
            1 => Some(term_q(&toks[0].text)),
            _ if !quoted => {
                // Words at one position (synonyms) are alternatives together.
                let mut groups: Vec<(usize, Vec<Box<dyn Query>>)> = Vec::new();
                for t in &toks {
                    match groups.last_mut() {
                        Some((p, g)) if *p == t.position => g.push(term_q(&t.text)),
                        _ => groups.push((t.position, vec![term_q(&t.text)])),
                    }
                }
                let clauses = groups
                    .into_iter()
                    .map(|(_, mut g)| {
                        let q = if g.len() == 1 { g.pop().unwrap() } else { Box::new(BooleanQuery::union(g)) as Box<dyn Query> };
                        (Occur::Should, q)
                    })
                    .collect();
                Some(Box::new(BooleanQuery::new(clauses)))
            }
            _ => {
                if kind != Kind::Text {
                    return Err(CouchError::bad_request(format!(
                        "field \"{name}\" was indexed without position data; cannot run PhraseQuery (phrase={name}:\"{}\")",
                        toks.iter().map(|t| t.text.as_str()).collect::<Vec<_>>().join(" ")
                    )));
                }
                let first = toks[0].position;
                let terms = toks.iter().map(|t| (t.position - first, Term::from_field_text(field, &t.text))).collect();
                Some(lucene(Box::new(PhraseQuery::new_with_offset_and_slop(terms, slop))))
            }
        })
    }

    fn double_leaf(&self, name: &str, field: Field, leaf: &Leaf) -> Result<Box<dyn Query>> {
        let num = |s: &str| {
            parse_number(s, self.locale).ok_or_else(|| {
                CouchError::bad_request(format!("Could not parse text '{s}' using java.text.DecimalFormat for field {name}"))
            })
        };
        let f = |v: f64| Term::from_field_f64(field, v);
        Ok(match leaf {
            Leaf::Term { raw, .. } => match pattern(raw) {
                Pattern::Plain(t) => {
                    let v = num(&t)?;
                    Box::new(RangeQuery::new(Bound::Included(f(v)), Bound::Included(f(v))))
                }
                // Points have no terms to match patterns against.
                _ => Box::new(EmptyQuery),
            },
            Leaf::Phrase { text, .. } => {
                let v = num(text)?;
                Box::new(RangeQuery::new(Bound::Included(f(v)), Bound::Included(f(v))))
            }
            Leaf::Regex(_) => Box::new(EmptyQuery),
            Leaf::Range { lo, hi, lo_incl, hi_incl } => {
                let lo = match lo {
                    None => f64::NEG_INFINITY,
                    Some(s) => num(s)?,
                };
                let hi = match hi {
                    None => f64::INFINITY,
                    Some(s) => num(s)?,
                };
                let l = if *lo_incl { Bound::Included(f(lo)) } else { Bound::Excluded(f(lo)) };
                let h = if *hi_incl { Bound::Included(f(hi)) } else { Bound::Excluded(f(hi)) };
                Box::new(RangeQuery::new(l, h))
            }
        })
    }
}

fn regex_query(re: &str, field: Field) -> Result<RegexQuery> {
    RegexQuery::from_pattern(re, field).map_err(|e| CouchError::bad_request(format!("invalid regular expression: {e}")))
}

/// The decimal and grouping separators numbers are written with in a
/// (Java) locale, by language, as the JDK's CLDR data has them.
fn separators(locale: Option<&str>) -> (char, Option<char>) {
    let tag = locale.unwrap_or("en").to_ascii_lowercase().replace('-', "_");
    let mut parts = tag.split('_');
    let lang = parts.next().unwrap_or("");
    let region = parts.next().unwrap_or("");
    match lang {
        "de" if region == "ch" || region == "li" => ('.', Some('\u{2019}')),
        "de" | "es" | "it" | "nl" | "pt" | "id" | "tr" | "da" | "el" | "ro" | "hr" | "sl" | "ca" | "gl" | "eu" | "vi" | "is" | "sr" => (',', Some('.')),
        "fr" | "ru" | "pl" | "cs" | "sk" | "sv" | "fi" | "nb" | "no" | "nn" | "uk" | "bg" | "hu" | "lt" | "lv" | "et" | "be" | "kk" => {
            (',', Some('\u{a0}'))
        }
        _ => ('.', Some(',')),
    }
}

/// NumberFormat.getInstance(locale).parse: the longest leading number,
/// grouping separators skipped.
pub fn parse_number(s: &str, locale: Option<&str>) -> Option<f64> {
    let (dec, group) = separators(locale);
    let mut out = String::new();
    let mut cs = s.trim_start().chars().peekable();
    if cs.peek() == Some(&'-') {
        out.push('-');
        cs.next();
    }
    let mut digits = 0;
    let mut seen_dec = false;
    while let Some(&c) = cs.peek() {
        if c.is_ascii_digit() {
            out.push(c);
            digits += 1;
        } else if c == dec && !seen_dec {
            out.push('.');
            seen_dec = true;
        } else if Some(c) == group || (group == Some('\u{a0}') && (c == '\u{202f}' || c == ' ')) {
            if seen_dec {
                break;
            }
        } else if c == 'E' && digits > 0 {
            // DecimalFormat's exponent: E, an optional minus, digits (no
            // plus, no lower case e: the number ends before them).
            let rest: String = cs.clone().skip(1).collect();
            let exp: String = rest.chars().enumerate().take_while(|(i, c)| c.is_ascii_digit() || (*i == 0 && *c == '-')).map(|(_, c)| c).collect();
            if exp.chars().any(|c| c.is_ascii_digit()) {
                out.push('e');
                out.push_str(&exp);
            }
            break;
        } else {
            break;
        }
        cs.next();
    }
    if digits == 0 {
        return None;
    }
    out.trim_end_matches('.').parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn term(f: &str, raw: &str) -> Q {
        Q::Leaf { field: f.into(), leaf: Leaf::Term { raw: raw.into(), fuzzy: None } }
    }

    #[test]
    fn precedence() {
        assert_eq!(parse("a", "default").unwrap(), term("default", "a"));
        assert_eq!(
            parse("a AND b OR c", "default").unwrap(),
            Q::Bool(vec![(Occur::Should, Q::Bool(vec![(Occur::Must, term("default", "a")), (Occur::Must, term("default", "b"))])), (Occur::Should, term("default", "c"))])
        );
        assert_eq!(
            parse("+a -b c", "d").unwrap(),
            Q::Bool(vec![(Occur::Must, term("d", "a")), (Occur::MustNot, term("d", "b")), (Occur::Should, term("d", "c"))])
        );
        assert_eq!(parse("x:(a b)", "d").unwrap(), Q::Bool(vec![(Occur::Should, term("x", "a")), (Occur::Should, term("x", "b"))]));
        assert_eq!(parse("*:*", "d").unwrap(), Q::MatchAll);
        assert_eq!(parse("ANDY", "d").unwrap(), term("d", "ANDY"));
        assert!(parse("a AND", "d").is_err());
        assert!(parse("(a", "d").is_err());
    }

    #[test]
    fn leaves() {
        assert_eq!(
            parse("bar:[1 TO *}", "d").unwrap(),
            Q::Leaf { field: "bar".into(), leaf: Leaf::Range { lo: Some("1".into()), hi: None, lo_incl: true, hi_incl: false } }
        );
        assert_eq!(
            parse("bar>=5", "d").unwrap(),
            Q::Leaf { field: "bar".into(), leaf: Leaf::Range { lo: Some("5".into()), hi: None, lo_incl: true, hi_incl: true } }
        );
        assert_eq!(parse("\"a b\"~2^3", "d").unwrap(), Q::Boost(Box::new(Q::Leaf { field: "d".into(), leaf: Leaf::Phrase { text: "a b".into(), slop: 2 } }), 3.0));
        assert_eq!(parse("look~", "d").unwrap(), Q::Leaf { field: "d".into(), leaf: Leaf::Term { raw: "look".into(), fuzzy: Some(None) } });
        assert_eq!(parse("foo-bar", "d").unwrap(), term("d", "foo-bar"));
    }

    #[test]
    fn locales() {
        assert_eq!(parse_number("10.0", Some("us")), Some(10.0));
        assert_eq!(parse_number("10.0", Some("de")), Some(100.0));
        assert_eq!(parse_number("1,5", Some("de")), Some(1.5));
        assert_eq!(parse_number("1,000.5", None), Some(1000.5));
        assert_eq!(parse_number("abc", None), None);
        assert_eq!(parse_number("-2", None), Some(-2.0));
        assert_eq!(parse_number("1E3", None), Some(1000.0));
        assert_eq!(parse_number("1.20000000000000000000e+01", None), Some(1.2));
    }
}
