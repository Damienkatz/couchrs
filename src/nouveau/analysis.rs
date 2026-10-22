//! Analyzers, after the Lucene ones Nouveau offers (LuceneAnalyzerFactory):
//! text to the tokens indexed and searched for, with Lucene's positions
//! (a removed stop word leaves a gap a phrase must span).
//!
//! standard, simple, simple_asciifolding, whitespace, keyword, email,
//! english and the Snowball-stemmed languages follow Lucene's chains.
//! Where Lucene uses a light stemmer (french, german, italian, portuguese,
//! spanish) the Snowball stemmer stands in; the dictionary-driven ones
//! (chinese, japanese: bigrams as cjk does; thai, polish) are
//! approximations, and bulgarian, galician and latvian go unstemmed.

use std::collections::HashSet;
use std::sync::LazyLock;
use tantivy::tokenizer::{AsciiFoldingFilter, TextAnalyzer, Token, TokenStream, Tokenizer};
use tantivy_stemmers::algorithms as stem;
use unicode_segmentation::UnicodeSegmentation;

/// Lucene's StandardTokenizer.DEFAULT_MAX_TOKEN_LENGTH: longer words are
/// split into pieces this long.
const MAX_TOKEN_LEN: usize = 255;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Analyzer {
    Arabic,
    Armenian,
    Basque,
    Bulgarian,
    Catalan,
    Chinese,
    Cjk,
    Classic,
    Czech,
    Danish,
    Dutch,
    Email,
    English,
    Finnish,
    French,
    Galician,
    German,
    Hindi,
    Hungarian,
    Indonesian,
    Irish,
    Italian,
    Japanese,
    Keyword,
    Latvian,
    Norwegian,
    Persian,
    Polish,
    Portuguese,
    Romanian,
    Russian,
    Simple,
    SimpleAsciiFolding,
    Spanish,
    Standard,
    Swedish,
    Thai,
    Turkish,
    Whitespace,
}

impl Analyzer {
    pub fn from_name(name: &str) -> Option<Analyzer> {
        use Analyzer::*;
        Some(match name {
            "arabic" => Arabic,
            "armenian" => Armenian,
            "basque" => Basque,
            "bulgarian" => Bulgarian,
            "catalan" => Catalan,
            "chinese" => Chinese,
            "cjk" => Cjk,
            "classic" => Classic,
            "czech" => Czech,
            "danish" => Danish,
            "dutch" => Dutch,
            "email" => Email,
            "english" => English,
            "finnish" => Finnish,
            "french" => French,
            "galician" => Galician,
            "german" => German,
            "hindi" => Hindi,
            "hungarian" => Hungarian,
            "indonesian" => Indonesian,
            "irish" => Irish,
            "italian" => Italian,
            "japanese" => Japanese,
            "keyword" => Keyword,
            "latvian" => Latvian,
            "norwegian" => Norwegian,
            "persian" => Persian,
            "polish" => Polish,
            "portuguese" => Portuguese,
            "romanian" => Romanian,
            "russian" => Russian,
            "simple" => Simple,
            "simple_asciifolding" => SimpleAsciiFolding,
            "spanish" => Spanish,
            "standard" => Standard,
            "swedish" => Swedish,
            "thai" => Thai,
            "turkish" => Turkish,
            "whitespace" => Whitespace,
            _ => return None,
        })
    }

    /// The name tantivy knows this analyzer's tokenizer by.
    pub fn tokenizer_name(self) -> String {
        format!("nouveau_{self:?}").to_ascii_lowercase()
    }

    /// The tokens `text` analyzes to.
    pub fn tokens(self, text: &str) -> Vec<Token> {
        use Analyzer::*;
        let snowball = |toks: Vec<Token>, stops: &HashSet<&str>, algo: stem::Algorithm| {
            stemmed(stop(lowercase(toks), stops), algo)
        };
        match self {
            Standard => lowercase(standard(text)),
            Keyword => keyword(text),
            Simple => lowercase(letters(text)),
            SimpleAsciiFolding => ascii_fold(lowercase(letters(text))),
            Whitespace => whitespace(text),
            Email => stop(lowercase(email(text)), &ENGLISH_STOPS),
            Classic => stop(lowercase(classic_filter(email(text))), &ENGLISH_STOPS),
            English => stemmed(stop(lowercase(possessive(standard(text))), &ENGLISH_STOPS), stem::english_porter),
            Cjk | Chinese | Japanese => stop(cjk_bigrams(lowercase(cjk_width(standard(text)))), &CJK_STOPS),
            Arabic => stemmed_with(arabic_normalize(lowercase(standard(text))), arabic_stem),
            Persian => arabic_normalize(lowercase(standard(text))),
            Armenian => stemmed(lowercase(standard(text)), stem::armenian_mkrtchyan),
            Basque => stemmed(lowercase(standard(text)), stem::basque),
            Catalan => stemmed(lowercase(elision(standard(text), &["d", "l", "m", "n", "s", "t"])), stem::catalan),
            Czech => stemmed(lowercase(standard(text)), stem::czech_dolamic_light),
            Danish => snowball(standard(text), &DANISH_STOPS, stem::danish),
            Dutch => snowball(standard(text), &DUTCH_STOPS, stem::dutch),
            Finnish => snowball(standard(text), &FINNISH_STOPS, stem::finnish),
            French => snowball(elision(standard(text), FRENCH_ARTICLES), &FRENCH_STOPS, stem::french),
            German => snowball(standard(text), &GERMAN_STOPS, stem::german),
            Hindi => stemmed(lowercase(standard(text)), stem::hindi_lightweight),
            Hungarian => snowball(standard(text), &HUNGARIAN_STOPS, stem::hungarian),
            Indonesian => stemmed(lowercase(standard(text)), stem::indonesian_tala),
            Irish => stemmed(irish_lowercase(elision(standard(text), &["d", "m", "b"])), stem::irish_gaelic),
            Italian => snowball(elision(standard(text), ITALIAN_ARTICLES), &ITALIAN_STOPS, stem::italian),
            Norwegian => snowball(standard(text), &NORWEGIAN_STOPS, stem::norwegian_bokmal),
            Portuguese => snowball(standard(text), &PORTUGUESE_STOPS, stem::portuguese),
            Romanian => stemmed(lowercase(standard(text)), stem::romanian),
            Russian => snowball(standard(text), &RUSSIAN_STOPS, stem::russian),
            Spanish => snowball(standard(text), &SPANISH_STOPS, stem::spanish),
            Swedish => snowball(standard(text), &SWEDISH_STOPS, stem::swedish),
            Turkish => stemmed(turkish_lowercase(apostrophe(standard(text))), stem::turkish_cilden),
            Bulgarian | Galician | Latvian | Polish | Thai => lowercase(standard(text)),
        }
    }

    pub fn token_texts(self, text: &str) -> Vec<String> {
        self.tokens(text).into_iter().map(|t| t.text).collect()
    }

    /// Analyzer.normalize: what the query parser does to the text of
    /// wildcard, prefix, fuzzy and range terms (the filters that change
    /// characters, none that split or drop words).
    pub fn normalize(self, text: &str) -> String {
        use Analyzer::*;
        let one = vec![tok(text, 0, text.len(), 0)];
        let out = match self {
            Keyword | Whitespace => one,
            SimpleAsciiFolding => ascii_fold(lowercase(one)),
            Turkish => turkish_lowercase(one),
            Irish => irish_lowercase(elision(one, &["d", "m", "b"])),
            Arabic | Persian => arabic_normalize(lowercase(one)),
            French => lowercase(elision(one, FRENCH_ARTICLES)),
            Italian => lowercase(elision(one, ITALIAN_ARTICLES)),
            Catalan => lowercase(elision(one, &["d", "l", "m", "n", "s", "t"])),
            Cjk | Chinese | Japanese => lowercase(cjk_width(one)),
            _ => lowercase(one),
        };
        out.into_iter().next().map(|t| t.text).unwrap_or_default()
    }
}

/// tantivy's view of an analyzer.
#[derive(Clone)]
pub struct NouveauTokenizer(pub Analyzer);

impl Tokenizer for NouveauTokenizer {
    type TokenStream<'a> = VecTokenStream;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> VecTokenStream {
        VecTokenStream::new(self.0.tokens(text))
    }
}

pub struct VecTokenStream {
    tokens: Vec<Token>,
    at: usize,
}

impl VecTokenStream {
    pub fn new(tokens: Vec<Token>) -> VecTokenStream {
        VecTokenStream { tokens, at: 0 }
    }
}

impl TokenStream for VecTokenStream {
    fn advance(&mut self) -> bool {
        self.at += 1;
        self.at <= self.tokens.len()
    }

    fn token(&self) -> &Token {
        &self.tokens[self.at - 1]
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.tokens[self.at - 1]
    }
}

fn tok(text: &str, from: usize, to: usize, position: usize) -> Token {
    Token { offset_from: from, offset_to: to, position, text: text.to_string(), position_length: 1 }
}

// ------------------------------------------------------------ tokenizers

fn is_emoji(c: char) -> bool {
    matches!(c as u32, 0x1F000..=0x1FAFF | 0x2600..=0x27BF | 0x2B00..=0x2BFF | 0x231A..=0x23FF)
}

/// Lucene's StandardTokenizer: Unicode (UAX#29) words; each ideograph and
/// hiragana on its own, runs of Southeast Asian script whole.
fn standard(text: &str) -> Vec<Token> {
    let mut out = Vec::new();
    for (start, w) in text.split_word_bound_indices() {
        if !w.chars().any(|c| c.is_alphanumeric() || is_emoji(c)) {
            continue;
        }
        push_chunked(&mut out, w, start);
    }
    out
}

/// A word as tokens of at most MAX_TOKEN_LEN chars.
fn push_chunked(out: &mut Vec<Token>, w: &str, start: usize) {
    let mut from = 0;
    let mut n = 0;
    for (i, _) in w.char_indices() {
        if n == MAX_TOKEN_LEN {
            out.push(tok(&w[from..i], start + from, start + i, out.len()));
            from = i;
            n = 0;
        }
        n += 1;
    }
    out.push(tok(&w[from..], start + from, start + w.len(), out.len()));
}

/// Runs of characters `keep` accepts.
fn runs(text: &str, keep: impl Fn(char) -> bool) -> Vec<Token> {
    let mut out = Vec::new();
    let mut start = None;
    for (i, c) in text.char_indices() {
        match (keep(c), start) {
            (true, None) => start = Some(i),
            (false, Some(s)) => {
                push_chunked(&mut out, &text[s..i], s);
                start = None;
            }
            _ => {}
        }
    }
    if let Some(s) = start {
        push_chunked(&mut out, &text[s..], s);
    }
    out
}

/// LetterTokenizer.
fn letters(text: &str) -> Vec<Token> {
    runs(text, char::is_alphabetic)
}

/// Java's Character.isWhitespace: no-break spaces are not.
fn java_whitespace(c: char) -> bool {
    c.is_whitespace() && !matches!(c, '\u{a0}' | '\u{2007}' | '\u{202f}') || matches!(c, '\u{1c}'..='\u{1f}')
}

/// WhitespaceTokenizer.
fn whitespace(text: &str) -> Vec<Token> {
    runs(text, |c| !java_whitespace(c))
}

/// KeywordTokenizer: all of it, as one token.
fn keyword(text: &str) -> Vec<Token> {
    vec![tok(text, 0, text.len(), 0)]
}

static URL_OR_EMAIL: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(
        r"(?i)\b(?:[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+|(?:https?|ftp|file)://[^\s<>\x22]+[^\s<>\x22.,;:!?)\]}'])",
    )
    .unwrap()
});

/// UAX29URLEmailTokenizer: the standard tokens, with URLs and email
/// addresses kept whole.
fn email(text: &str) -> Vec<Token> {
    let mut out = Vec::new();
    let mut at = 0;
    let push_standard = |out: &mut Vec<Token>, from: usize, to: usize| {
        for mut t in standard(&text[from..to]) {
            t.offset_from += from;
            t.offset_to += from;
            t.position = out.len();
            out.push(t);
        }
    };
    for m in URL_OR_EMAIL.find_iter(text) {
        push_standard(&mut out, at, m.start());
        let p = out.len();
        out.push(tok(m.as_str(), m.start(), m.end(), p));
        at = m.end();
    }
    push_standard(&mut out, at, text.len());
    out
}

// --------------------------------------------------------------- filters

/// LowerCaseFilter: Java's per-character case mapping.
fn lowercase(mut toks: Vec<Token>) -> Vec<Token> {
    for t in &mut toks {
        if t.text.chars().any(|c| !c.is_lowercase() && c.is_alphabetic()) {
            t.text = t
                .text
                .chars()
                .flat_map(|c| match c {
                    '\u{130}' => 'i'.to_lowercase(),
                    c => c.to_lowercase(),
                })
                .collect();
        }
    }
    toks
}

/// TurkishLowerCaseFilter: dotted and dotless i kept apart.
fn turkish_lowercase(mut toks: Vec<Token>) -> Vec<Token> {
    for t in &mut toks {
        t.text = t
            .text
            .chars()
            .flat_map(|c| match c {
                'I' => 'ı'.to_lowercase(),
                '\u{130}' => 'i'.to_lowercase(),
                c => c.to_lowercase(),
            })
            .collect::<String>()
            .replace("i\u{307}", "i");
    }
    toks
}

/// IrishLowerCaseFilter: an initial n or t before an uppercase vowel
/// ("nAthair") is a mutation, kept apart by a hyphen.
fn irish_lowercase(mut toks: Vec<Token>) -> Vec<Token> {
    for t in &mut toks {
        let mut cs = t.text.chars();
        let (a, b) = (cs.next(), cs.next());
        let mutated = matches!(a, Some('n' | 't')) && b.is_some_and(|b| "AEIOUÁÉÍÓÚ".contains(b));
        let lower: String = t.text.chars().flat_map(char::to_lowercase).collect();
        t.text = if mutated { format!("{}-{}", &lower[..1], &lower[1..]) } else { lower };
    }
    toks
}

/// StopFilter: the words go, their positions stay taken.
fn stop(toks: Vec<Token>, stops: &HashSet<&str>) -> Vec<Token> {
    toks.into_iter().filter(|t| !stops.contains(t.text.as_str())).collect()
}

fn stemmed(toks: Vec<Token>, algo: stem::Algorithm) -> Vec<Token> {
    stemmed_with(toks, |s| algo(s).into_owned())
}

fn stemmed_with(mut toks: Vec<Token>, f: impl Fn(&str) -> String) -> Vec<Token> {
    for t in &mut toks {
        t.text = f(&t.text);
    }
    toks
}

/// EnglishPossessiveFilter: a trailing 's goes.
fn possessive(mut toks: Vec<Token>) -> Vec<Token> {
    for t in &mut toks {
        for suffix in ["'s", "'S", "\u{2019}s", "\u{2019}S", "\u{2bc}s", "\u{2bc}S"] {
            if let Some(s) = t.text.strip_suffix(suffix) {
                t.text = s.to_string();
                break;
            }
        }
    }
    toks
}

/// ClassicFilter: possessives and the dots of acronyms go.
fn classic_filter(toks: Vec<Token>) -> Vec<Token> {
    let mut toks = possessive(toks);
    for t in &mut toks {
        let b = t.text.as_bytes();
        let acronym = b.len() >= 3 && b.chunks(2).all(|p| p[0].is_ascii_alphabetic() && p.get(1).is_none_or(|c| *c == b'.'));
        if acronym {
            t.text.retain(|c| c != '.');
        }
    }
    toks
}

const FRENCH_ARTICLES: &[&str] = &["l", "m", "t", "qu", "n", "s", "j", "d", "c", "jusqu", "quoiqu", "lorsqu", "puisqu"];
const ITALIAN_ARTICLES: &[&str] = &[
    "c", "l", "all", "dall", "dell", "nell", "sull", "coll", "pell", "gl", "agl", "dagl", "degl", "negl", "sugl", "un", "m", "t", "s", "v", "d",
];

/// ElisionFilter: l'avion is avion.
fn elision(mut toks: Vec<Token>, articles: &[&str]) -> Vec<Token> {
    for t in &mut toks {
        if let Some(i) = t.text.find(['\'', '\u{2019}']) {
            let prefix = t.text[..i].to_lowercase();
            if articles.contains(&prefix.as_str()) {
                let apos = t.text[i..].chars().next().map(char::len_utf8).unwrap_or(1);
                t.text = t.text[i + apos..].to_string();
            }
        }
    }
    toks
}

/// ApostropheFilter: what follows an apostrophe (a Turkish suffix) goes.
fn apostrophe(mut toks: Vec<Token>) -> Vec<Token> {
    for t in &mut toks {
        if let Some(i) = t.text.find(['\'', '\u{2019}']) {
            t.text.truncate(i);
        }
    }
    toks
}

/// ASCIIFoldingFilter.
fn ascii_fold(toks: Vec<Token>) -> Vec<Token> {
    #[derive(Clone)]
    struct Given(Vec<Token>);
    impl Tokenizer for Given {
        type TokenStream<'a> = VecTokenStream;
        fn token_stream<'a>(&'a mut self, _: &'a str) -> VecTokenStream {
            VecTokenStream::new(std::mem::take(&mut self.0))
        }
    }
    let mut a = TextAnalyzer::builder(Given(toks)).filter(AsciiFoldingFilter).build();
    let mut s = a.token_stream("");
    let mut out = Vec::new();
    while s.advance() {
        out.push(s.token().clone());
    }
    out
}

fn is_cjk(c: char) -> bool {
    matches!(c as u32,
        0x3040..=0x30FF | 0x31F0..=0x31FF | 0x3400..=0x4DBF | 0x4E00..=0x9FFF | 0xF900..=0xFAFF
        | 0xAC00..=0xD7AF | 0x1100..=0x11FF | 0x3130..=0x318F | 0x20000..=0x2FA1F)
}

/// CJKWidthFilter: full-width ASCII to plain, half-width katakana to full.
fn cjk_width(mut toks: Vec<Token>) -> Vec<Token> {
    const HALF_KATAKANA: &str = "ヲァィゥェォャュョッーアイウエオカキクケコサシスセソタチツテトナニヌネノハヒフヘホマミムメモヤユヨラリルレロワン";
    let half: Vec<char> = HALF_KATAKANA.chars().collect();
    for t in &mut toks {
        if t.text.chars().any(|c| ('\u{ff01}'..='\u{ff9f}').contains(&c)) {
            t.text = t
                .text
                .chars()
                .map(|c| match c as u32 {
                    0xFF01..=0xFF5E => char::from_u32(c as u32 - 0xFEE0).unwrap_or(c),
                    0xFF66..=0xFF9D => half.get((c as u32 - 0xFF66) as usize).copied().unwrap_or(c),
                    _ => c,
                })
                .collect();
        }
    }
    toks
}

/// CJKBigramFilter: CJK runs (adjacent CJK tokens) as overlapping pairs
/// of characters; a lone character as itself.
fn cjk_bigrams(toks: Vec<Token>) -> Vec<Token> {
    let mut out: Vec<Token> = Vec::new();
    let mut run: Vec<(char, usize, usize)> = Vec::new();
    let flush = |run: &mut Vec<(char, usize, usize)>, out: &mut Vec<Token>| {
        if run.len() == 1 {
            let (c, f, t) = run[0];
            let p = out.len();
            out.push(tok(&c.to_string(), f, t, p));
        }
        for w in run.windows(2) {
            let p = out.len();
            out.push(tok(&format!("{}{}", w[0].0, w[1].0), w[0].1, w[1].2, p));
        }
        run.clear();
    };
    for t in toks {
        if t.text.chars().all(is_cjk) {
            if run.last().is_some_and(|l| l.2 != t.offset_from) {
                flush(&mut run, &mut out);
            }
            let mut at = t.offset_from;
            for c in t.text.chars() {
                run.push((c, at, at + c.len_utf8()));
                at += c.len_utf8();
            }
        } else {
            flush(&mut run, &mut out);
            let p = out.len();
            out.push(Token { position: p, ..t });
        }
    }
    flush(&mut run, &mut out);
    out
}

/// ArabicNormalizationFilter.
fn arabic_normalize(mut toks: Vec<Token>) -> Vec<Token> {
    for t in &mut toks {
        t.text = t
            .text
            .chars()
            .filter_map(|c| match c {
                '\u{622}' | '\u{623}' | '\u{625}' => Some('\u{627}'),
                '\u{649}' => Some('\u{64a}'),
                '\u{629}' => Some('\u{647}'),
                '\u{640}' | '\u{64b}'..='\u{652}' => None,
                c => Some(c),
            })
            .collect();
    }
    toks
}

/// ArabicStemmer: Larkey's light stemming (prefixes, then suffixes).
fn arabic_stem(s: &str) -> String {
    const PREFIXES: &[&str] = &["\u{627}\u{644}", "\u{648}\u{627}\u{644}", "\u{628}\u{627}\u{644}", "\u{643}\u{627}\u{644}", "\u{641}\u{627}\u{644}", "\u{644}\u{644}", "\u{648}"];
    const SUFFIXES: &[&str] = &[
        "\u{647}\u{627}", "\u{627}\u{646}", "\u{627}\u{62a}", "\u{648}\u{646}", "\u{64a}\u{646}", "\u{64a}\u{647}", "\u{64a}\u{629}", "\u{647}", "\u{629}", "\u{64a}",
    ];
    let mut cs: Vec<char> = s.chars().collect();
    for p in PREFIXES {
        let pc: Vec<char> = p.chars().collect();
        let min = if pc.len() == 1 { 4 } else { pc.len() + 2 };
        if cs.len() >= min && cs.starts_with(&pc) {
            cs.drain(..pc.len());
            break;
        }
    }
    for sfx in SUFFIXES {
        let sc: Vec<char> = sfx.chars().collect();
        if cs.len() >= sc.len() + 2 && cs.ends_with(&sc) {
            cs.truncate(cs.len() - sc.len());
        }
    }
    cs.into_iter().collect()
}

// ------------------------------------------------------------ stop words

fn set(words: &'static [&'static str]) -> HashSet<&'static str> {
    words.iter().copied().collect()
}

/// EnglishAnalyzer.ENGLISH_STOP_WORDS_SET.
static ENGLISH_STOPS: LazyLock<HashSet<&str>> = LazyLock::new(|| {
    set(&[
        "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that",
        "the", "their", "then", "there", "these", "they", "this", "to", "was", "will", "with",
    ])
});
/// CJKAnalyzer's stopwords.txt.
static CJK_STOPS: LazyLock<HashSet<&str>> = LazyLock::new(|| {
    set(&[
        "a", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "s", "such", "t", "that",
        "the", "their", "then", "there", "these", "they", "this", "to", "was", "will", "with", "www",
    ])
});
static DANISH_STOPS: LazyLock<HashSet<&str>> = LazyLock::new(|| set(super::stopwords::DANISH));
static DUTCH_STOPS: LazyLock<HashSet<&str>> = LazyLock::new(|| set(super::stopwords::DUTCH));
static FINNISH_STOPS: LazyLock<HashSet<&str>> = LazyLock::new(|| set(super::stopwords::FINNISH));
static FRENCH_STOPS: LazyLock<HashSet<&str>> = LazyLock::new(|| set(super::stopwords::FRENCH));
static GERMAN_STOPS: LazyLock<HashSet<&str>> = LazyLock::new(|| set(super::stopwords::GERMAN));
static HUNGARIAN_STOPS: LazyLock<HashSet<&str>> = LazyLock::new(|| set(super::stopwords::HUNGARIAN));
static ITALIAN_STOPS: LazyLock<HashSet<&str>> = LazyLock::new(|| set(super::stopwords::ITALIAN));
static NORWEGIAN_STOPS: LazyLock<HashSet<&str>> = LazyLock::new(|| set(super::stopwords::NORWEGIAN));
static PORTUGUESE_STOPS: LazyLock<HashSet<&str>> = LazyLock::new(|| set(super::stopwords::PORTUGUESE));
static RUSSIAN_STOPS: LazyLock<HashSet<&str>> = LazyLock::new(|| set(super::stopwords::RUSSIAN));
static SPANISH_STOPS: LazyLock<HashSet<&str>> = LazyLock::new(|| set(super::stopwords::SPANISH));
static SWEDISH_STOPS: LazyLock<HashSet<&str>> = LazyLock::new(|| set(super::stopwords::SWEDISH));

#[cfg(test)]
mod tests {
    use super::Analyzer;

    fn a(name: &str, text: &str) -> Vec<String> {
        Analyzer::from_name(name).unwrap().token_texts(text)
    }

    #[test]
    fn lucene_examples() {
        // From CouchDB's nouveau documentation and tests.
        assert_eq!(a("standard", "hello there"), ["hello", "there"]);
        assert_eq!(a("standard", "ablanks@renovations.com"), ["ablanks", "renovations.com"]);
        assert_eq!(a("keyword", "ablanks@renovations.com"), ["ablanks@renovations.com"]);
        assert_eq!(a("email", "Mail ablanks@renovations.com now"), ["mail", "ablanks@renovations.com", "now"]);
        assert_eq!(a("simple", "Foo-bar 42 baz"), ["foo", "bar", "baz"]);
        assert_eq!(a("whitespace", "Foo-bar  42"), ["Foo-bar", "42"]);
        assert_eq!(a("simple_asciifolding", "Crème Brûlée"), ["creme", "brulee"]);
        assert_eq!(a("english", "The dog's running quickly"), ["dog", "run", "quickli"]);
        assert_eq!(a("french", "l'avion"), a("french", "avion"));
        assert_eq!(a("cjk", "我是中国人"), ["我是", "是中", "中国", "国人"]);
        assert_eq!(a("standard", "我是"), ["我", "是"]);
    }

    #[test]
    fn stop_words_leave_gaps() {
        let t = Analyzer::English.tokens("the quick fox");
        assert_eq!(t.iter().map(|t| t.position).collect::<Vec<_>>(), [1, 2]);
    }
}
