//! JWT authentication (couch_httpd_auth's jwt handler and the jwtf
//! library): an `Authorization: Bearer` token signed with a key from the
//! [jwt_keys] config section — `hmac:KID` (base64 secret), `rsa:KID` or
//! `ec:KID` (PEM, "\n" escapes allowed), KID `_default` when the token has
//! none — whose `sub` claim names the user and whose roles come from
//! [jwt_auth] roles_claim_name or roles_claim_path.

use crate::auth::UserCtx;
use crate::config::Config;
use crate::error::{CouchError, Result};
use axum::http::StatusCode;
use base64::Engine;
use serde_json::{Map, Value};

const ALGS: &[&str] = &["RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "HS256", "HS384", "HS512"];

fn bad(reason: &str) -> CouchError {
    CouchError::bad_request(reason)
}

fn b64url_decode(s: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s.trim_end_matches('=')).ok()
}

fn decode_json(part: &str) -> Result<Map<String, Value>> {
    let bytes = b64url_decode(part).ok_or_else(|| bad("Malformed authentication token"))?;
    match serde_json::from_slice::<Value>(&bytes) {
        Ok(Value::Object(m)) => Ok(m),
        Ok(_) => Err(bad("Not an object")),
        Err(_) => Err(bad("Malformed authentication token")),
    }
}

/// [jwt_auth] required_claims: `exp, iat, nbf, kid, typ` names, and
/// `{iss, "value"}` for an exact issuer.
#[derive(Default)]
struct Checks {
    exp: bool,
    iat: bool,
    nbf: bool,
    kid: bool,
    typ: bool,
    iss: Option<String>,
}

fn checks(config: &Config) -> Result<Checks> {
    let spec = config.get_or("jwt_auth", "required_claims", "exp");
    let re = regex::Regex::new(r#"([a-z]+)|\{([a-z]+)\s*,\s*"([^"]+)"\}"#).expect("regex");
    let mut c = Checks::default();
    let mut any = false;
    for m in re.captures_iter(&spec) {
        any = true;
        match (m.get(1).map(|x| x.as_str()), m.get(2).map(|x| x.as_str()), m.get(3)) {
            (Some("exp"), _, _) => c.exp = true,
            (Some("iat"), _, _) => c.iat = true,
            (Some("nbf"), _, _) => c.nbf = true,
            (Some("kid"), _, _) => c.kid = true,
            (Some("typ"), _, _) => c.typ = true,
            (_, Some("iss"), Some(v)) => c.iss = Some(v.as_str().to_string()),
            _ => {}
        }
    }
    if !any && !spec.trim().is_empty() {
        return Err(CouchError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "misconfigured_server",
            "JWT is not configured correctly",
        ));
    }
    Ok(c)
}

fn now() -> i64 {
    crate::http::now_secs() as i64
}

fn validate(header: &Map<String, Value>, payload: &Map<String, Value>, c: &Checks) -> Result<()> {
    if c.typ {
        match header.get("typ").and_then(|t| t.as_str()) {
            None => return Err(bad("Missing typ header parameter")),
            Some("JWT") => {}
            Some(_) => return Err(bad("Invalid typ header parameter")),
        }
    }
    match header.get("alg").and_then(|a| a.as_str()) {
        None => return Err(bad("Missing alg header parameter")),
        Some(a) if !ALGS.contains(&a) => return Err(bad("Invalid alg header parameter")),
        _ => {}
    }
    if let Some(iss) = &c.iss {
        match payload.get("iss").and_then(|v| v.as_str()) {
            None => return Err(bad("Missing iss claim")),
            Some(v) if v == iss => {}
            Some(_) => return Err(bad("Invalid iss claim")),
        }
    }
    if c.iat {
        match payload.get("iat") {
            None => return Err(bad("Missing iat claim")),
            Some(v) if v.is_i64() || v.is_u64() => {}
            Some(_) => return Err(bad("Invalid iat claim")),
        }
    }
    let num = |k: &str| payload.get(k).and_then(|v| v.as_f64());
    if c.nbf {
        match num("nbf") {
            None => return Err(bad("Missing nbf claim")),
            Some(t) if t > now() as f64 => return Err(CouchError::unauthorized("nbf not in past")),
            _ => {}
        }
    }
    if c.exp {
        match num("exp") {
            None => return Err(bad("Missing exp claim")),
            Some(t) if (now() as f64) >= t => return Err(CouchError::unauthorized("exp not in future")),
            _ => {}
        }
    }
    Ok(())
}

/// The configured key for an algorithm and key id, as text.
fn key(config: &Config, alg: &str, kid: Option<&str>) -> Result<String> {
    let kty = match &alg[..2] {
        "HS" => "hmac",
        "RS" => "rsa",
        _ => "ec",
    };
    config
        .get("jwt_keys", &format!("{kty}:{}", kid.unwrap_or("_default")))
        .ok_or_else(|| bad("Unknown kid"))
}

fn verify(alg: &str, message: &[u8], sig_b64: &str, key: &str) -> Result<()> {
    use jsonwebtoken::{Algorithm, DecodingKey};
    let pem = key.replace("\\n", "\n");
    let bad_sig = || bad("Bad signature in authentication token");
    if alg == "ES512" {
        use p521::ecdsa::signature::Verifier;
        use p521::pkcs8::DecodePublicKey;
        let vk = p521::ecdsa::VerifyingKey::from_public_key_pem(&pem).map_err(|_| bad_sig())?;
        let raw = b64url_decode(sig_b64).ok_or_else(bad_sig)?;
        let sig = p521::ecdsa::Signature::from_slice(&raw).map_err(|_| bad_sig())?;
        return vk.verify(message, &sig).map_err(|_| bad_sig());
    }
    let (algorithm, dk) = match alg {
        "HS256" | "HS384" | "HS512" => {
            let secret = crate::doc::B64.decode(key.trim()).map_err(|_| bad("Bad HMAC in authentication token"))?;
            let a = match alg {
                "HS256" => Algorithm::HS256,
                "HS384" => Algorithm::HS384,
                _ => Algorithm::HS512,
            };
            (a, DecodingKey::from_secret(&secret))
        }
        "RS256" | "RS384" | "RS512" => {
            let a = match alg {
                "RS256" => Algorithm::RS256,
                "RS384" => Algorithm::RS384,
                _ => Algorithm::RS512,
            };
            (a, DecodingKey::from_rsa_pem(pem.as_bytes()).map_err(|_| bad_sig())?)
        }
        "ES256" | "ES384" => {
            let a = if alg == "ES256" { Algorithm::ES256 } else { Algorithm::ES384 };
            (a, DecodingKey::from_ec_pem(pem.as_bytes()).map_err(|_| bad_sig())?)
        }
        _ => return Err(bad("Invalid alg header parameter")),
    };
    match jsonwebtoken::crypto::verify(sig_b64, message, &dk, algorithm) {
        Ok(true) => Ok(()),
        _ if alg.starts_with("HS") => Err(bad("Bad HMAC in authentication token")),
        _ => Err(bad_sig()),
    }
}

/// Roles from the claims: [jwt_auth] roles_claim_path (dot-separated,
/// `\.` for a literal dot) or roles_claim_name (default `_couchdb.roles`),
/// a list of strings or a comma-separated string.
fn roles(config: &Config, claims: &Map<String, Value>) -> Result<Vec<String>> {
    let v = match config.get("jwt_auth", "roles_claim_path") {
        Some(path) => {
            let mut cur = Some(Value::Object(claims.clone()));
            let mut seg = String::new();
            let mut segs = Vec::new();
            let mut chars = path.chars().peekable();
            while let Some(ch) = chars.next() {
                match ch {
                    '\\' if chars.peek() == Some(&'.') => {
                        seg.push('.');
                        chars.next();
                    }
                    '.' => segs.push(std::mem::take(&mut seg)),
                    c => seg.push(c),
                }
            }
            segs.push(seg);
            for s in segs {
                cur = cur.and_then(|c| c.get(&s).cloned());
            }
            cur.unwrap_or(Value::Array(vec![]))
        }
        None => {
            let name = config.get_or("jwt_auth", "roles_claim_name", "_couchdb.roles");
            claims.get(&name).cloned().unwrap_or(Value::Array(vec![]))
        }
    };
    match v {
        Value::Array(a) => a
            .into_iter()
            .map(|r| r.as_str().map(String::from).ok_or_else(|| bad("Malformed token")))
            .collect(),
        Value::String(s) => Ok(s.split(',').map(|r| r.trim().to_string()).filter(|r| !r.is_empty()).collect()),
        _ => Err(bad("Malformed token")),
    }
}

/// Authenticate a Bearer token.
pub fn authenticate(config: &Config, token: &str) -> Result<UserCtx> {
    let parts: Vec<&str> = token.trim().split('.').collect();
    let [h, p, s] = parts[..] else { return Err(bad("Malformed authentication token")) };
    let c = checks(config)?;
    let header = decode_json(h)?;
    let payload = decode_json(p)?;
    validate(&header, &payload, &c)?;
    let alg = header.get("alg").and_then(|a| a.as_str()).unwrap_or_default();
    let kid = header.get("kid").and_then(|k| k.as_str());
    if c.kid && kid.is_none() {
        return Err(bad("Missing kid claim"));
    }
    let key = key(config, alg, kid)?;
    verify(alg, format!("{h}.{p}").as_bytes(), s, &key)?;
    let Some(sub) = payload.get("sub").and_then(|v| v.as_str()) else {
        return Err(CouchError::unauthorized("Token missing sub claim."));
    };
    Ok(UserCtx { name: Some(sub.to_string()), roles: roles(config, &payload)? })
}

#[cfg(test)]
mod tests {
    use super::*;
    use hmac::{Hmac, KeyInit, Mac};

    fn token(header: &str, claims: &str, secret: &[u8]) -> String {
        let e = |s: &str| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s);
        let msg = format!("{}.{}", e(header), e(claims));
        let mut m = <Hmac<sha2::Sha256> as KeyInit>::new_from_slice(secret).unwrap();
        m.update(msg.as_bytes());
        format!("{msg}.{}", base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(m.finalize().into_bytes()))
    }

    #[test]
    fn hs256_tokens() {
        let secret = b"s3cret-key";
        let cfg = Config::load(
            vec![],
            vec![(("jwt_keys".into(), "hmac:_default".into()), crate::doc::B64.encode(secret))],
        )
        .unwrap();
        let exp = now() + 60;
        let t = token(r#"{"alg":"HS256"}"#, &format!(r#"{{"sub":"jan","exp":{exp},"_couchdb.roles":["a","b"]}}"#), secret);
        let u = authenticate(&cfg, &t).unwrap();
        assert_eq!((u.name.as_deref(), u.roles), (Some("jan"), vec!["a".to_string(), "b".to_string()]));
        let expired = token(r#"{"alg":"HS256"}"#, &format!(r#"{{"sub":"jan","exp":{}}}"#, now() - 1), secret);
        assert_eq!(authenticate(&cfg, &expired).unwrap_err().reason, "exp not in future");
        let no_exp = token(r#"{"alg":"HS256"}"#, r#"{"sub":"jan"}"#, secret);
        assert_eq!(authenticate(&cfg, &no_exp).unwrap_err().reason, "Missing exp claim");
        let wrong = token(r#"{"alg":"HS256"}"#, &format!(r#"{{"sub":"jan","exp":{exp}}}"#), b"other");
        assert_eq!(authenticate(&cfg, &wrong).unwrap_err().reason, "Bad HMAC in authentication token");
        let unknown = token(r#"{"alg":"HS256","kid":"k2"}"#, &format!(r#"{{"sub":"jan","exp":{exp}}}"#), secret);
        assert_eq!(authenticate(&cfg, &unknown).unwrap_err().reason, "Unknown kid");
        assert_eq!(authenticate(&cfg, "a.b").unwrap_err().reason, "Malformed authentication token");
    }
}
