//! Admin password hashing and checking, as couch_passwords and
//! couch_auth_cache do it: config [admins] values are plaintext until
//! hashed into `-pbkdf2:PRF-DERIVED,SALT,ITERATIONS` (the salt's hex text
//! is the PBKDF2 salt), with the legacy `-hashed-` (SHA-1) and
//! `-pbkdf2-` (PBKDF2-SHA1) forms still accepted.

use crate::config::Config;
use parking_lot::Mutex;
use sha2::Digest;
use std::collections::HashSet;

pub fn is_hashed(stored: &str) -> bool {
    stored.starts_with("-hashed-") || stored.starts_with("-pbkdf2-") || stored.starts_with("-pbkdf2:")
}

pub fn pbkdf2_hex(prf: &str, password: &[u8], salt: &[u8], iterations: u32) -> Option<String> {
    macro_rules! derive {
        ($h:ty, $len:expr) => {{
            let mut out = [0u8; $len];
            pbkdf2::pbkdf2_hmac::<$h>(password, salt, iterations, &mut out);
            out.to_vec()
        }};
    }
    // CouchDB derives a key as long as the PRF's digest.
    let key = match prf {
        "sha" => derive!(sha1::Sha1, 20),
        "sha224" => derive!(sha2::Sha224, 28),
        "sha256" => derive!(sha2::Sha256, 32),
        "sha384" => derive!(sha2::Sha384, 48),
        "sha512" => derive!(sha2::Sha512, 64),
        _ => return None,
    };
    Some(hex::encode(key))
}

/// Hash a plaintext admin password with [chttpd_auth] password_scheme,
/// pbkdf2_prf and iterations.
pub fn hash_admin_password(config: &Config, plain: &str) -> String {
    let salt = hex::encode(uuid::Uuid::new_v4().as_bytes());
    if config.get_or("chttpd_auth", "password_scheme", "pbkdf2") == "simple" {
        let h = sha1::Sha1::digest([plain.as_bytes(), salt.as_bytes()].concat());
        return format!("-hashed-{},{salt}", hex::encode(h));
    }
    let mut prf = config.get_or("chttpd_auth", "pbkdf2_prf", "sha256");
    let iterations: u32 = config.get_int("chttpd_auth", "iterations", 600_000);
    let derived = pbkdf2_hex(&prf, plain.as_bytes(), salt.as_bytes(), iterations).unwrap_or_else(|| {
        prf = "sha256".into();
        pbkdf2_hex("sha256", plain.as_bytes(), salt.as_bytes(), iterations).unwrap()
    });
    format!("-pbkdf2:{prf}-{derived},{salt},{iterations}")
}

pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    ct_eq(a.as_bytes(), b.as_bytes())
}

/// Successful checks, keyed by stored hash and a digest of the password,
/// so each credential pays for PBKDF2 once (couch_passwords_cache).
static VERIFIED: Mutex<Option<HashSet<(String, [u8; 32])>>> = Mutex::new(None);
const CACHE_MAX: usize = 10_000;

/// Whether `password` matches a stored admin value (hashed or plaintext).
pub fn verify_admin(stored: &str, password: &str) -> bool {
    if !is_hashed(stored) {
        return constant_time_eq(stored, password);
    }
    let key = (stored.to_string(), sha2::Sha256::digest(password.as_bytes()).into());
    if VERIFIED.lock().as_ref().is_some_and(|s| s.contains(&key)) {
        return true;
    }
    let ok = check_hashed(stored, password);
    if ok {
        let mut g = VERIFIED.lock();
        let set = g.get_or_insert_with(HashSet::new);
        if set.len() >= CACHE_MAX {
            set.clear();
        }
        set.insert(key);
    }
    ok
}

fn check_hashed(stored: &str, password: &str) -> bool {
    if let Some(rest) = stored.strip_prefix("-hashed-") {
        let Some((hash, salt)) = rest.split_once(',') else { return false };
        let h = sha1::Sha1::digest([password.as_bytes(), salt.as_bytes()].concat());
        return constant_time_eq(&hex::encode(h), hash);
    }
    let (prf, rest) = if let Some(r) = stored.strip_prefix("-pbkdf2-") {
        ("sha", r)
    } else if let Some(r) = stored.strip_prefix("-pbkdf2:") {
        match r.split_once('-') {
            Some(x) => x,
            None => return false,
        }
    } else {
        return false;
    };
    let parts: Vec<&str> = rest.split(',').collect();
    let [derived, salt, iterations] = parts[..] else { return false };
    let Ok(iterations) = iterations.parse::<u32>() else { return false };
    pbkdf2_hex(prf, password.as_bytes(), salt.as_bytes(), iterations).is_some_and(|d| constant_time_eq(&d, derived))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checks_couchdb_hashes() {
        // Made by CouchDB 3.5.3 for the password "bench".
        let stored = "-pbkdf2:sha256-702f3d8f9f5b9f569f70e5ce15a0fdfa859f6fd19292cdd47aed87d1425010ec,dd9590062b5f2ddb8931307462044d7b,600000";
        assert!(verify_admin(stored, "bench"));
        assert!(!verify_admin(stored, "bench2"));
        let legacy = format!("-hashed-{},abc", hex::encode(sha1::Sha1::digest(b"pwabc")));
        assert!(verify_admin(&legacy, "pw"));
        assert!(verify_admin("plain", "plain") && !verify_admin("plain", "other"));
    }

    #[test]
    fn hashes_round_trip() {
        let cfg = Config::load(vec![], vec![(("chttpd_auth".into(), "iterations".into()), "1000".into())]).unwrap();
        let h = hash_admin_password(&cfg, "s3cret");
        assert!(h.starts_with("-pbkdf2:sha256-") && h.ends_with(",1000"), "{h}");
        assert!(verify_admin(&h, "s3cret") && !verify_admin(&h, "nope"));
    }
}
