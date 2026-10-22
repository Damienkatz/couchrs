//! Server configuration, a port of CouchDB's config application: layered
//! ini files, read in order with later values winning, plus changes made
//! at runtime over the _config API, which are saved to the last file.
//!
//! The chain is the built-in share/default.ini, then each --ini file or
//! directory (a directory contributes its *.ini files in name order), then
//! values given on the command line, which win over every file and are
//! never written to disk.

use crate::error::{CouchError, Result};
use axum::http::StatusCode;
use parking_lot::{Mutex, RwLock};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub const DEFAULT_INI: &str = include_str!("../share/default.ini");

/// Sections that can't be changed over HTTP (couch_util's blacklist).
const BLACKLIST: &[&str] = &[
    "daemons",
    "external",
    "httpd_design_handlers",
    "httpd_db_handlers",
    "httpd_global_handlers",
    "native_query_servers",
    "os_daemons",
    "query_servers",
    "feature_flags",
];

type Key = (String, String);

pub struct Config {
    sources: Vec<PathBuf>,
    /// Runtime changes are saved here: the last ini file in the chain.
    write_file: Option<PathBuf>,
    overrides: RwLock<BTreeMap<Key, String>>,
    values: RwLock<BTreeMap<Key, String>>,
    /// Serializes changes so file rewrites don't interleave.
    write_lock: Mutex<()>,
}

/// Parse ini text the way CouchDB does: `;` starts a comment (inline only
/// after a space or tab), `[section]` headers, `key = value` or `key=value`,
/// lines before the first section and keys without a value ignored.
pub fn parse_ini(text: &str) -> Vec<(Key, String)> {
    let mut out = Vec::new();
    let mut section = String::new();
    for raw in text.split(['\n', '\r', '\u{1a}']) {
        let mut line = raw.trim();
        if line.starts_with(';') {
            continue;
        }
        if let Some(i) = [" ;", "\t;"].iter().filter_map(|p| line.find(p)).min() {
            line = line[..i].trim();
        }
        if line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix('[') {
            match rest.split_once(']') {
                Some((name, "")) if !name.is_empty() => section = name.to_string(),
                _ => {}
            }
            continue;
        }
        if section.is_empty() {
            continue;
        }
        let kv = line.split_once(" = ").or_else(|| line.split_once('='));
        if let Some((k, v)) = kv {
            let (k, v) = (k.trim(), v.trim());
            if !k.is_empty() && !v.is_empty() {
                out.push(((section.clone(), k.to_string()), v.to_string()));
            }
        }
    }
    out
}

/// Rewrite one key in ini text, keeping everything else as it is
/// (config_writer): replace the key's line in its section, or add it at
/// the end of the section, or append the section. `None` removes it.
fn rewrite_ini(text: &str, section: &str, key: &str, value: Option<&str>) -> String {
    let mut lines: Vec<String> = text.split(['\n', '\r', '\u{1a}']).map(String::from).collect();
    while lines.last().is_some_and(|l| l.is_empty()) {
        lines.pop();
    }
    let header = format!("[{section}]");
    let is_key = |l: &str| {
        l.strip_prefix(key)
            .is_some_and(|rest| rest.trim_start().starts_with('='))
    };
    let is_header = |l: &str| {
        l.starts_with('[')
            && l.ends_with(']')
            && l[1..l.len() - 1].chars().all(|c| c.is_ascii_alphanumeric() || "._-".contains(c))
    };
    let new_line = value.map(|v| format!("{key} = {v}"));
    match lines.iter().position(|l| *l == header) {
        None => {
            if let Some(nl) = new_line {
                if !lines.is_empty() {
                    lines.push(String::new());
                }
                lines.push(header);
                lines.push(nl);
            }
        }
        Some(h) => {
            let end = lines[h + 1..].iter().position(|l| is_header(l)).map(|i| h + 1 + i).unwrap_or(lines.len());
            match (h + 1..end).find(|&i| is_key(&lines[i])) {
                Some(i) => match new_line {
                    Some(nl) => lines[i] = nl,
                    None => {
                        lines.remove(i);
                    }
                },
                None => {
                    if let Some(nl) = new_line {
                        // After the section's last non-blank line.
                        let mut at = end;
                        while at > h + 1 && lines[at - 1].trim().is_empty() {
                            at -= 1;
                        }
                        lines.insert(at, nl);
                    }
                }
            }
        }
    }
    let mut s = lines.join("\n");
    s.push('\n');
    s
}

/// The files a source names: itself, or a directory's *.ini in name order.
fn expand(source: &Path) -> Vec<PathBuf> {
    if source.is_dir() {
        let mut files: Vec<PathBuf> = std::fs::read_dir(source)
            .map(|rd| rd.flatten().map(|e| e.path()).filter(|p| p.extension().is_some_and(|x| x == "ini")).collect())
            .unwrap_or_default();
        files.sort();
        files
    } else {
        vec![source.to_path_buf()]
    }
}

/// Section and key may hold visible characters, spaces and tabs but no
/// brackets; values may hold brackets (validate_config_update).
fn validate(section: &str, key: &str, value: &str) -> Result<()> {
    let printable = |s: &str| s.chars().all(|c| c == ' ' || c == '\t' || !(c.is_control() || c.is_whitespace()));
    let bad = |what: &str| Err(CouchError::bad_request(format!("Invalid configuration {what}")));
    if !printable(section) || section.contains(['[', ']']) {
        return bad("section");
    }
    if !printable(key) || key.contains(['[', ']']) {
        return bad("key");
    }
    if !printable(value) {
        return bad("value");
    }
    Ok(())
}

pub fn check_blacklist(section: &str) -> Result<()> {
    if BLACKLIST.contains(&section) {
        return Err(CouchError::new(
            StatusCode::FORBIDDEN,
            "forbidden",
            "Config section blacklisted for modification over HTTP API.",
        ));
    }
    Ok(())
}

impl Config {
    /// Load the chain. Every source file must exist except the write file,
    /// which is created empty (CouchDB's local.ini always exists).
    pub fn load(sources: Vec<PathBuf>, overrides: Vec<(Key, String)>) -> Result<Config> {
        let write_file = sources.iter().flat_map(|s| expand(s)).last();
        if let Some(w) = &write_file
            && !w.exists()
        {
            if let Some(dir) = w.parent() {
                std::fs::create_dir_all(dir)?;
            }
            std::fs::write(w, "")?;
        }
        let c = Config {
            sources,
            write_file,
            overrides: RwLock::new(overrides.into_iter().collect()),
            values: RwLock::new(BTreeMap::new()),
            write_lock: Mutex::new(()),
        };
        *c.values.write() = c.file_values()?;
        Ok(c)
    }

    /// Values from the files and the command line, without runtime changes.
    fn file_values(&self) -> Result<BTreeMap<Key, String>> {
        let mut m: BTreeMap<Key, String> = parse_ini(DEFAULT_INI).into_iter().collect();
        for f in self.sources.iter().flat_map(|s| expand(s)) {
            let text = std::fs::read_to_string(&f).map_err(|e| {
                CouchError::internal(format!("Couldn't read server configuration file {}: {e}", f.display()))
            })?;
            m.extend(parse_ini(&text));
        }
        m.extend(self.overrides.read().iter().map(|(k, v)| (k.clone(), v.clone())));
        Ok(m)
    }

    /// Whether a value came from the command line (and so isn't saved).
    pub fn is_override(&self, section: &str, key: &str) -> bool {
        self.overrides.read().contains_key(&(section.to_string(), key.to_string()))
    }

    pub fn write_file(&self) -> Option<&Path> {
        self.write_file.as_deref()
    }

    pub fn get(&self, section: &str, key: &str) -> Option<String> {
        self.values.read().get(&(section.to_string(), key.to_string())).cloned()
    }

    pub fn get_or(&self, section: &str, key: &str, default: &str) -> String {
        self.get(section, key).unwrap_or_else(|| default.to_string())
    }

    pub fn get_bool(&self, section: &str, key: &str, default: bool) -> bool {
        match self.get(section, key).as_deref() {
            Some("true") => true,
            Some("false") => false,
            _ => default,
        }
    }

    pub fn get_int<T: std::str::FromStr>(&self, section: &str, key: &str, default: T) -> T {
        self.get(section, key).and_then(|v| v.trim().parse().ok()).unwrap_or(default)
    }

    pub fn section(&self, section: &str) -> Vec<(String, String)> {
        self.values
            .read()
            .iter()
            .filter(|((s, _), _)| s == section)
            .map(|((_, k), v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn all(&self) -> BTreeMap<String, BTreeMap<String, String>> {
        let mut out: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        for ((s, k), v) in self.values.read().iter() {
            out.entry(s.clone()).or_default().insert(k.clone(), v.clone());
        }
        out
    }

    /// Set a value, saving it to the write file unless `persist` is false.
    pub fn set(&self, section: &str, key: &str, value: &str, persist: bool) -> Result<()> {
        validate(section, key, value)?;
        let _g = self.write_lock.lock();
        let k = (section.to_string(), key.to_string());
        if persist {
            self.save(section, key, Some(value))?;
        } else if let Some(o) = self.overrides.write().get_mut(&k) {
            // A command-line value changed in memory (a password hashed)
            // stays changed across reloads.
            *o = value.to_string();
        }
        self.values.write().insert(k, value.to_string());
        Ok(())
    }

    /// Delete a value (and from the write file unless `persist` is false);
    /// a value the files still define takes its place.
    pub fn delete(&self, section: &str, key: &str, persist: bool) -> Result<()> {
        let _g = self.write_lock.lock();
        if persist {
            self.save(section, key, None)?;
        }
        let k = (section.to_string(), key.to_string());
        let fallback = self.file_values()?.remove(&k);
        let mut values = self.values.write();
        match fallback {
            Some(v) => values.insert(k, v),
            None => values.remove(&k),
        };
        Ok(())
    }

    /// Re-read the files, dropping changes that weren't saved.
    pub fn reload(&self) -> Result<()> {
        let _g = self.write_lock.lock();
        *self.values.write() = self.file_values()?;
        Ok(())
    }

    fn save(&self, section: &str, key: &str, value: Option<&str>) -> Result<()> {
        let Some(path) = &self.write_file else { return Ok(()) };
        let text = std::fs::read_to_string(path).unwrap_or_default();
        let new = rewrite_ini(&text, section, key, value);
        let tmp = path.with_extension("ini.tmp");
        std::fs::write(&tmp, new)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kv(text: &str) -> Vec<(String, String, String)> {
        parse_ini(text).into_iter().map(|((s, k), v)| (s, k, v)).collect()
    }

    #[test]
    fn parses_like_couchdb() {
        let t = "top = ignored\n[a]\nx = 1\n; comment\ny=2 ; inline\nz = a;b\nempty =\n=v\n[bad\n[b]\nk = v = w\n";
        let want = [("a", "x", "1"), ("a", "y", "2"), ("a", "z", "a;b"), ("b", "k", "v = w")];
        let want: Vec<(String, String, String)> =
            want.iter().map(|(s, k, v)| (s.to_string(), k.to_string(), v.to_string())).collect();
        assert_eq!(kv(t), want);
    }

    #[test]
    fn rewrites_in_place() {
        let t = "; header\n[a]\nx = 1\n\n[b]\ny = 2\n";
        assert_eq!(rewrite_ini(t, "a", "x", Some("9")), "; header\n[a]\nx = 9\n\n[b]\ny = 2\n");
        assert_eq!(rewrite_ini(t, "a", "z", Some("3")), "; header\n[a]\nx = 1\nz = 3\n\n[b]\ny = 2\n");
        assert_eq!(rewrite_ini(t, "c", "k", Some("v")), "; header\n[a]\nx = 1\n\n[b]\ny = 2\n\n[c]\nk = v\n");
        assert_eq!(rewrite_ini(t, "b", "y", None), "; header\n[a]\nx = 1\n\n[b]\n");
        assert_eq!(rewrite_ini("", "a", "x", Some("1")), "[a]\nx = 1\n");
        // A key that only prefixes another isn't matched.
        assert_eq!(rewrite_ini("[a]\nxy = 1\n", "a", "x", Some("2")), "[a]\nxy = 1\nx = 2\n");
    }
}
