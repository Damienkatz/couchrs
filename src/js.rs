//! JavaScript query server, replacing couchjs. Functions run in embedded
//! QuickJS. Engines are cached per thread keyed by the hash of the code they
//! were set up with, so the Rust-side handles (`JsFuns`) are plain data and
//! freely `Send`.

use crate::api::blocking;
use crate::btree::{Dir, Range};
use crate::db::{Db, Snapshot};
use crate::doc::{self, DocOpts};
use crate::error::{CouchError, Result};
use crate::http::{Req, with_header};
use axum::body::Body;
use axum::http::{Method, StatusCode, header};
use axum::response::Response;
use rquickjs::{Context, Function, Runtime};
use serde_json::{Value, json};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// CouchDB's mimeparse.js, used by provides() content negotiation.
const MIMEPARSE: &str = include_str!("../share/mimeparse.js");

const PRELUDE: &str = r#"
var __emits = null;
function emit(k, v) {
  if (__emits === null) throw new Error("emit called outside of a map function");
  __emits.push([k === undefined ? null : k, v === undefined ? null : v]);
}
function log(m) { __log.push(typeof m === "string" ? m : JSON.stringify(m)); }
var __log = [];
function sum(a) { var r = 0; for (var i = 0; i < a.length; i++) r += a[i]; return r; }
function toJSON(o) { return JSON.stringify(o); }
function isArray(o) { return Array.isArray(o); }
// Shows, lists and updates run as CouchDB's render.js runs them;
// Mimeparse is CouchDB's mimeparse.js, evaluated before this prelude.
var Mime = (function() {
  var mimesByKey = {};
  var keysByMime = {};
  function registerType() {
    var mimes = [], key = arguments[0];
    for (var i = 1; i < arguments.length; i++) mimes.push(arguments[i]);
    mimesByKey[key] = mimes;
    for (var i = 0; i < mimes.length; i++) keysByMime[mimes[i]] = key;
  }
  registerType("all", "*/*");
  registerType("text", "text/plain; charset=utf-8", "txt");
  registerType("html", "text/html; charset=utf-8");
  registerType("xhtml", "application/xhtml+xml", "xhtml");
  registerType("xml", "application/xml", "text/xml", "application/x-xml");
  registerType("js", "text/javascript", "application/javascript", "application/x-javascript");
  registerType("css", "text/css");
  registerType("ics", "text/calendar");
  registerType("csv", "text/csv");
  registerType("rss", "application/rss+xml");
  registerType("atom", "application/atom+xml");
  registerType("yaml", "application/x-yaml", "text/yaml");
  registerType("multipart_form", "multipart/form-data");
  registerType("url_encoded_form", "application/x-www-form-urlencoded");
  registerType("json", "application/json", "text/x-json");
  var providesUsed = false;
  var mimeFuns = [];
  var responseContentType = null;
  function provides(type, fun) {
    providesUsed = true;
    mimeFuns.push([type, fun]);
  }
  function resetProvides() {
    providesUsed = false;
    mimeFuns = [];
    responseContentType = null;
  }
  function runProvides(req) {
    var supportedMimes = [], bestFun, bestKey = null, accept = req.headers["Accept"];
    if (req.query && req.query.format) {
      bestKey = req.query.format;
      responseContentType = mimesByKey[bestKey][0];
    } else if (accept) {
      mimeFuns.reverse().forEach(function(mimeFun) {
        var mimeKey = mimeFun[0];
        if (mimesByKey[mimeKey]) supportedMimes = supportedMimes.concat(mimesByKey[mimeKey]);
      });
      responseContentType = Mimeparse.bestMatch(supportedMimes, accept);
      bestKey = keysByMime[responseContentType];
    } else {
      bestKey = mimeFuns[0][0];
      responseContentType = mimesByKey[bestKey][0];
    }
    if (bestKey) {
      for (var i = 0; i < mimeFuns.length; i++) {
        if (mimeFuns[i][0] == bestKey) { bestFun = mimeFuns[i][1]; break; }
      }
    }
    if (bestFun) {
      return bestFun.call(null);
    } else {
      var supportedTypes = mimeFuns.map(function(mf) { return mimesByKey[mf[0]].join(', ') || mf[0]; });
      throw(["error", "not_acceptable",
        "Content-Type " + (accept || bestKey) + " not supported, try one of: " + supportedTypes.join(', ')]);
    }
  }
  return {
    registerType: registerType,
    provides: provides,
    resetProvides: resetProvides,
    runProvides: runProvides,
    providesUsed: function() { return providesUsed; },
    responseContentType: function() { return responseContentType; }
  };
})();
function registerType() { return Mime.registerType.apply(null, arguments); }
function provides(type, fun) { Mime.provides(type, fun); }
var __chunks = [], __startResp = {}, __gotRow = false, __lastRow = false;
// A list's rows, and what it has produced: the start response and body.
var __rows = [], __rowAt = 0, __listStart = null, __listBody = [];
function start(resp) { __startResp = resp || {}; }
function send(chunk) { __chunks.push(chunk.toString()); }
function __applyContentType(resp, type) {
  resp["headers"] = resp["headers"] || {};
  if (type) resp["headers"]["Content-Type"] = resp["headers"]["Content-Type"] || type;
  return resp;
}
function __sendStart() {
  __listStart = __applyContentType(__startResp || {}, Mime.responseContentType());
  __listBody = __listBody.concat(__chunks);
  __chunks = [];
  __startResp = {};
}
function __blowChunks() {
  __listBody = __listBody.concat(__chunks);
  __chunks = [];
}
function getRow() {
  if (__lastRow) return null;
  if (!__gotRow) { __gotRow = true; __sendStart(); } else { __blowChunks(); }
  if (__rowAt >= __rows.length) { __lastRow = true; return null; }
  return __rows[__rowAt++];
}
function __resetList() { __gotRow = false; __lastRow = false; __chunks = []; __startResp = {}; }
function __wrapResp(resp) {
  var type = typeof resp;
  return (type == "string" || type == "xml") ? {body: resp} : resp;
}
function __typeOf(value) {
  var s = typeof value;
  if (s === 'object') s = value ? (value instanceof Array ? 'array' : s) : 'null';
  return s;
}
function __renderError(e, errType) {
  if (e && (e.error && e.reason || e[0] == "error" || e[0] == "fatal")) throw e;
  var msg = "function raised error: " + (e && e.toSource ? e.toSource() : String(e)) +
    " \nstacktrace: " + (e === null || e === undefined ? undefined : e.stack);
  log(msg);
  throw(["error", errType || "render_error", msg]);
}
function __freeze(o) {
  if (o && typeof o === "object" && !Object.isFrozen(o)) {
    Object.freeze(o);
    for (var k in o) __freeze(o[k]);
  }
  return o;
}
function __resolve(ddoc, parent, name) {
  var parts = name.split("/");
  var path = (name.charAt(0) === ".") ? parent.slice(0) : [];
  for (var i = 0; i < parts.length; i++) {
    var p = parts[i];
    if (p === "..") { if (path.length === 0) throw ["error", "invalid_require_path", "Object has no parent " + name]; path.pop(); }
    else if (p !== "." && p !== "") path.push(p);
  }
  return path;
}
function __make_require(ddoc, parent) {
  return function(name) {
    if (!ddoc) throw ["error", "invalid_require_path", "require() called without a design document"];
    var path = __resolve(ddoc, parent, name);
    var src = ddoc;
    for (var i = 0; i < path.length; i++) {
      src = src[path[i]];
      if (src === undefined) throw ["error", "invalid_require_path", "Object has no property \"" + path[i] + "\"."];
    }
    if (typeof src !== "string") throw ["error", "invalid_require_path", "Module is not a string: " + name];
    var key = path.join("/");
    if (__modcache[key]) return __modcache[key].exports;
    var module = {id: key, exports: {}};
    __modcache[key] = module;
    (new Function("module", "exports", "require", src))(module, module.exports, __make_require(ddoc, path.slice(0, -1)));
    return module.exports;
  };
}
var __modcache = {};
function __eval_fun(src, ddoc) {
  var f = (new Function("require", "return (" + src + "\n);"))(__make_require(ddoc, []));
  if (typeof f !== "function") throw new Error("Expression does not eval to a function. (" + src + ")");
  return f;
}
function __errjson(e) {
  if (Array.isArray(e) && (e[0] === "error" || e[0] === "fatal")) return {error: e[1], reason: e[2]};
  if (e && typeof e.error === "string" && e.reason !== undefined) return {error: e.error, reason: String(e.reason)};
  if (e && typeof e === "object" && !(e instanceof Error)) return {thrown: e};
  return {error: "render_error", reason: String(e && e.message !== undefined ? e.message : e)};
}
var __funs = [];
var __rfuns = [];
function __map(docjson) {
  var doc = __freeze(JSON.parse(docjson));
  var out = [];
  for (var i = 0; i < __funs.length; i++) {
    __emits = [];
    try { __funs[i](doc); } catch (e) { __emits = []; log("map function raised exception: " + String(e)); }
    out.push(__emits);
  }
  __emits = null;
  return JSON.stringify(out);
}
function __reduce(i, kvjson, rereduce) {
  try {
    var kv = JSON.parse(kvjson);
    var keys = null, values;
    if (rereduce) { values = kv; }
    else { keys = []; values = []; for (var j = 0; j < kv.length; j++) { keys.push(kv[j][0]); values.push(kv[j][1]); } }
    var r = __rfuns[i](keys, values, rereduce);
    return JSON.stringify({ok: r === undefined ? null : r});
  } catch (e) { return JSON.stringify(__errjson(e)); }
}
function __validate(i, nd, od, uc, so) {
  try {
    __funs[i](JSON.parse(nd), JSON.parse(od), JSON.parse(uc), JSON.parse(so));
    return JSON.stringify({ok: true});
  } catch (e) { return JSON.stringify(__errjson(e)); }
}
function __filter(i, dj, rj) {
  try { return JSON.stringify({ok: !!__funs[i](__freeze(JSON.parse(dj)), JSON.parse(rj))}); }
  catch (e) { return JSON.stringify(__errjson(e)); }
}
function __show(i, argsjson) {
  try {
    var args = JSON.parse(argsjson);
    try {
      __resetList();
      Mime.resetProvides();
      var resp = __funs[i].apply(null, args) || {};
      resp = __wrapResp(resp);
      if (__chunks.length && __chunks.length > 0) {
        resp.headers = resp.headers || {};
        for (var header in __startResp) resp.headers[header] = __startResp[header];
        resp.body = __chunks.join("") + (resp.body || "");
        __resetList();
      }
      if (Mime.providesUsed()) {
        var provided = __wrapResp(Mime.runProvides(args[1]) || {});
        resp.body = (resp.body || "") + __chunks.join("");
        resp.body += provided.body || "";
        resp = __applyContentType(resp, Mime.responseContentType());
        __resetList();
      }
      var type = __typeOf(resp);
      if (type == 'object' || type == 'string') return JSON.stringify({ok: __wrapResp(resp)});
      throw(["error", "render_error", "undefined response from show function"]);
    } catch (e) {
      // A missing document named in the path is a 404.
      if (args[0] === null && args[1].path && args[1].path.length > 5) throw(["error", "not_found", "document not found"]);
      __renderError(e);
    }
  } catch (e) { return JSON.stringify(__errjson(e)); }
}
function __list(i, argsjson, rowsjson) {
  try {
    Mime.resetProvides();
    __resetList();
    __rows = JSON.parse(rowsjson); __rowAt = 0; __listStart = null; __listBody = [];
    var args = JSON.parse(argsjson);
    try {
      var tail = __funs[i].apply(null, args);
      if (Mime.providesUsed()) tail = Mime.runProvides(args[1]);
      if (!__gotRow) getRow();
      if (typeof tail != "undefined") __chunks.push(tail);
      __blowChunks();
    } catch (e) { __renderError(e); }
    return JSON.stringify({ok: {start: __listStart, body: __listBody.join("")}});
  } catch (e) { return JSON.stringify(__errjson(e)); }
  finally { __rows = []; }
}
function __rewrite(i, argsjson) {
  try {
    var args = JSON.parse(argsjson);
    var result;
    try { result = __funs[i].apply(null, args); } catch (e) { __renderError(e, "rewrite_error"); }
    return JSON.stringify({ok: result ? result : null});
  } catch (e) { return JSON.stringify(__errjson(e)); }
}
function __update(i, argsjson) {
  try {
    var args = JSON.parse(argsjson);
    try {
      if (args[1].method == "GET") throw(["error", "method_not_allowed", "Update functions do not allow GET"]);
      var result = __funs[i].apply(null, args);
      var resp = result[1];
      var type = __typeOf(resp);
      if (type == 'object' || type == 'string') return JSON.stringify({ok: [result[0], __wrapResp(resp)]});
      throw(["error", "render_error", "undefined response from update function"]);
    } catch (e) { __renderError(e); }
  } catch (e) { return JSON.stringify(__errjson(e)); }
}
"#;

/// A set of compiled functions. Cheap to clone and Send.
#[derive(Clone, Debug)]
pub struct JsFuns {
    key: u64,
    setup: Arc<String>,
}

struct Engine {
    _rt: Runtime,
    ctx: Context,
    deadline: Rc<Cell<Option<Instant>>>,
}

thread_local! {
    static ENGINES: RefCell<HashMap<u64, Rc<Engine>>> = RefCell::new(HashMap::new());
}

const TIMEOUT: Duration = Duration::from_secs(5);

fn js_err(ctx: &rquickjs::Ctx, e: rquickjs::Error) -> CouchError {
    let msg = if matches!(e, rquickjs::Error::Exception) {
        let ex = ctx.catch();
        if let Some(obj) = ex.as_object() {
            let m: Option<String> = obj.get("message").ok();
            m.unwrap_or_else(|| format!("{ex:?}"))
        } else if let Some(s) = ex.as_string() {
            s.to_string().unwrap_or_default()
        } else {
            format!("{ex:?}")
        }
    } else {
        e.to_string()
    };
    CouchError::new(StatusCode::INTERNAL_SERVER_ERROR, "compilation_error", msg)
}

impl JsFuns {
    /// Build from map/validate/filter functions (`funs`) and reduce
    /// functions (`rfuns`), compiled in the context of `ddoc` for require().
    pub fn new(ddoc: &Value, funs: &[String], rfuns: &[String]) -> JsFuns {
        let mut setup = String::new();
        setup.push_str("var __ddoc = ");
        setup.push_str(&serde_json::to_string(ddoc).unwrap());
        setup.push_str(";\n");
        for f in funs {
            setup.push_str("__funs.push(__eval_fun(");
            setup.push_str(&serde_json::to_string(f).unwrap());
            setup.push_str(", __ddoc));\n");
        }
        for f in rfuns {
            setup.push_str("__rfuns.push(__eval_fun(");
            setup.push_str(&serde_json::to_string(f).unwrap());
            setup.push_str(", __ddoc));\n");
        }
        let mut h = std::collections::hash_map::DefaultHasher::new();
        setup.hash(&mut h);
        JsFuns {
            key: h.finish(),
            setup: Arc::new(setup),
        }
    }

    fn engine(&self) -> Result<Rc<Engine>> {
        if let Some(e) = ENGINES.with(|m| m.borrow().get(&self.key).cloned()) {
            return Ok(e);
        }
        let rt = Runtime::new().map_err(|e| CouchError::internal(e.to_string()))?;
        rt.set_memory_limit(512 << 20);
        let deadline: Rc<Cell<Option<Instant>>> = Rc::new(Cell::new(None));
        let d2 = deadline.clone();
        rt.set_interrupt_handler(Some(Box::new(move || {
            d2.get().map(|d| Instant::now() > d).unwrap_or(false)
        })));
        let ctx = Context::full(&rt).map_err(|e| CouchError::internal(e.to_string()))?;
        ctx.with(|c| -> Result<()> {
            c.eval::<(), _>(MIMEPARSE).map_err(|e| js_err(&c, e))?;
            c.eval::<(), _>(PRELUDE).map_err(|e| js_err(&c, e))?;
            c.eval::<(), _>(self.setup.as_str()).map_err(|e| js_err(&c, e))?;
            Ok(())
        })?;
        let eng = Rc::new(Engine {
            _rt: rt,
            ctx,
            deadline,
        });
        ENGINES.with(|m| {
            let mut m = m.borrow_mut();
            if m.len() > 64 {
                m.clear();
            }
            m.insert(self.key, eng.clone());
        });
        Ok(eng)
    }

    /// Call a prelude entry point that returns a JSON string.
    fn call(&self, name: &str, args: CallArgs) -> Result<Value> {
        let eng = self.engine()?;
        eng.deadline.set(Some(Instant::now() + TIMEOUT));
        let out = eng.ctx.with(|c| -> Result<String> {
            let f: Function = c.globals().get(name).map_err(|e| js_err(&c, e))?;
            let r: rquickjs::Result<String> = match args {
                CallArgs::S(a) => f.call((a,)),
                CallArgs::IS(i, a) => f.call((i, a)),
                CallArgs::ISB(i, a, b) => f.call((i, a, b)),
                CallArgs::ISS(i, a, b) => f.call((i, a, b)),
                CallArgs::ISSSS(i, a, b, cc, d) => f.call((i, a, b, cc, d)),
            };
            r.map_err(|e| {
                let err = js_err(&c, e);
                if eng.deadline.get().map(|d| Instant::now() > d).unwrap_or(false) {
                    CouchError::new(StatusCode::INTERNAL_SERVER_ERROR, "os_process_error", "JavaScript function timed out")
                } else {
                    err
                }
            })
        });
        eng.deadline.set(None);
        Ok(serde_json::from_str(&out?)?)
    }

    pub fn map(&self, doc: &Value) -> Result<Vec<Vec<(Value, Value)>>> {
        let out = self.call("__map", CallArgs::S(doc.to_string()))?;
        let mut res = Vec::new();
        for per_fun in out.as_array().cloned().unwrap_or_default() {
            let mut rows = Vec::new();
            for kv in per_fun.as_array().cloned().unwrap_or_default() {
                if let Value::Array(mut a) = kv {
                    let v = a.pop().unwrap_or(Value::Null);
                    let k = a.pop().unwrap_or(Value::Null);
                    rows.push((k, v));
                }
            }
            res.push(rows);
        }
        Ok(res)
    }

    /// `kvs` holds ([key, docid], value) pairs.
    pub fn reduce(&self, i: usize, kvs: &[(Value, Value)]) -> Result<Value> {
        let arr: Vec<Value> = kvs.iter().map(|(k, v)| json!([k, v])).collect();
        unwrap_ok(self.call("__reduce", CallArgs::ISB(i as i32, Value::Array(arr).to_string(), false))?)
    }

    pub fn rereduce(&self, i: usize, values: &[Value]) -> Result<Value> {
        unwrap_ok(self.call("__reduce", CallArgs::ISB(i as i32, Value::Array(values.to_vec()).to_string(), true))?)
    }

    fn validate(&self, i: usize, new: &Value, old: &Value, ctx: &Value, sec: &Value) -> Result<Value> {
        self.call(
            "__validate",
            CallArgs::ISSSS(i as i32, new.to_string(), old.to_string(), ctx.to_string(), sec.to_string()),
        )
    }

    fn filter(&self, i: usize, doc: &Value, req: &Value) -> Result<bool> {
        let r = unwrap_ok(self.call("__filter", CallArgs::ISS(i as i32, doc.to_string(), req.to_string()))?)?;
        Ok(r.as_bool().unwrap_or(false))
    }

    /// Run a show function, with provides()/registerType() negotiation.
    fn show(&self, i: usize, args: &[Value]) -> Result<Value> {
        unwrap_ok(self.call("__show", CallArgs::IS(i as i32, Value::Array(args.to_vec()).to_string()))?)
    }

    /// Run a list function over `rows`: {start: {code, headers}, body}.
    fn list(&self, i: usize, args: &[Value], rows: &[Value]) -> Result<Value> {
        unwrap_ok(self.call(
            "__list",
            CallArgs::ISS(i as i32, Value::Array(args.to_vec()).to_string(), Value::Array(rows.to_vec()).to_string()),
        )?)
    }

    /// Run an update function: [new doc or null, response].
    fn update(&self, i: usize, args: &[Value]) -> Result<Value> {
        unwrap_ok(self.call("__update", CallArgs::IS(i as i32, Value::Array(args.to_vec()).to_string()))?)
    }
}

enum CallArgs {
    S(String),
    IS(i32, String),
    ISB(i32, String, bool),
    ISS(i32, String, String),
    ISSSS(i32, String, String, String, String),
}

fn unwrap_ok(v: Value) -> Result<Value> {
    if let Some(ok) = v.get("ok") {
        return Ok(ok.clone());
    }
    if let Some(t) = v.get("thrown") {
        let (err, reason) = match t.as_object().and_then(|o| o.iter().next()) {
            Some((k, r)) => (k.clone(), r.as_str().map(String::from).unwrap_or_else(|| r.to_string())),
            None => ("error".into(), t.to_string()),
        };
        return Err(CouchError::new(error_status(&err), &err, reason));
    }
    let err = v.get("error").and_then(|e| e.as_str()).unwrap_or("render_error");
    let reason = v.get("reason").and_then(|e| e.as_str()).unwrap_or("").to_string();
    Err(CouchError::new(error_status(err), err, reason))
}

/// HTTP status for a JavaScript error, by name, as chttpd maps them.
/// Show, list and update functions report anything they throw that isn't
/// in CouchDB's error form as a render_error.
fn error_status(err: &str) -> StatusCode {
    match err {
        "bad_request" => StatusCode::BAD_REQUEST,
        "unauthorized" => StatusCode::UNAUTHORIZED,
        "forbidden" => StatusCode::FORBIDDEN,
        "not_found" => StatusCode::NOT_FOUND,
        "method_not_allowed" => StatusCode::METHOD_NOT_ALLOWED,
        "not_acceptable" => StatusCode::NOT_ACCEPTABLE,
        "conflict" => StatusCode::CONFLICT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// ---------------------------------------------------------- validation

struct VduCache {
    ddocs: Vec<(String, String)>,
    funs: Option<JsFuns>,
}

fn ddoc_heads(snap: &Snapshot) -> Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    let range = Range {
        start: Some("_design/".to_string()),
        end: Some("_design0".to_string()),
        inclusive_end: false,
    };
    snap.fold_docs(Dir::Fwd, &range, |fdi| {
        if !fdi.deleted {
            out.push((fdi.id.clone(), fdi.winner().rev()));
        }
        Ok(true)
    })?;
    Ok(out)
}

fn vdu_funs(db: &Db, snap: &Snapshot) -> Result<Option<JsFuns>> {
    let heads = ddoc_heads(snap)?;
    {
        let ext = db.ext.lock();
        if let Some(c) = ext.get("vdu").and_then(|c| c.downcast_ref::<parking_lot::Mutex<VduCache>>()) {
            let c = c.lock();
            if c.ddocs == heads {
                return Ok(c.funs.clone());
            }
        }
    }
    let mut srcs = Vec::new();
    for (id, _) in &heads {
        let d = crate::changes::load_ddoc(snap, id)?;
        if let Some(s) = d.get("validate_doc_update").and_then(|v| v.as_str()) {
            srcs.push(s.to_string());
        }
    }
    let funs = if srcs.is_empty() {
        None
    } else {
        Some(JsFuns::new(&Value::Null, &srcs, &[]))
    };
    let cache = Arc::new(parking_lot::Mutex::new(VduCache {
        ddocs: heads,
        funs: funs.clone(),
    }));
    db.ext.lock().insert("vdu".into(), cache);
    Ok(funs)
}

fn doc_to_vdu_json(d: &doc::Doc) -> Value {
    let mut m = serde_json::Map::new();
    m.insert("_id".into(), json!(d.id));
    if let Some(r) = d.rev() {
        m.insert("_rev".into(), json!(r));
    }
    if d.deleted {
        m.insert("_deleted".into(), json!(true));
    }
    m.extend(d.body.clone());
    if !d.atts.is_empty() {
        let atts: serde_json::Map<String, Value> = d
            .atts
            .iter()
            .map(|a| {
                (
                    a.name.clone(),
                    json!({"content_type": a.content_type, "length": a.length, "stub": true,
                           "digest": format!("md5-{}", base64::Engine::encode(&doc::B64, &a.md5))}),
                )
            })
            .collect();
        m.insert("_attachments".into(), Value::Object(atts));
    }
    Value::Object(m)
}

/// Whether any design doc has a validate_doc_update function (cached).
pub fn has_validators(db: &Db, snap: &Snapshot) -> Result<bool> {
    Ok(vdu_funs(db, snap)?.is_some())
}

/// Run every validate_doc_update function against each doc. Returns a
/// per-doc error (None = valid).
pub fn validate_docs(
    db: &Db,
    snap: &Snapshot,
    docs: &mut [doc::Doc],
    _new_edits: bool,
    user_ctx: &Value,
    skip: &[bool],
) -> Result<Vec<Option<CouchError>>> {
    let mut out = vec![None; docs.len()];
    let Some(funs) = vdu_funs(db, snap)? else {
        return Ok(out);
    };
    let n = funs.setup.matches("__funs.push").count();
    let sec = snap.security();
    for (i, d) in docs.iter().enumerate() {
        if d.is_local() || d.is_design() || skip.get(i).copied().unwrap_or(false) {
            continue;
        }
        let old = match snap.get_fdi(&d.id)? {
            Some(f) => {
                let w = f.winner();
                if w.deleted && d.revs.1.is_empty() {
                    Value::Null
                } else {
                    let target = d
                        .revs
                        .1
                        .first()
                        .and_then(|id| Snapshot::find_rev(&f, d.revs.0, id))
                        .unwrap_or(w);
                    snap.doc_json(&f, &target, &DocOpts::default()).unwrap_or(Value::Null)
                }
            }
            None => Value::Null,
        };
        let new = doc_to_vdu_json(d);
        for fi in 0..n {
            let r = funs.validate(fi, &new, &old, &user_ctx, &sec)?;
            if r.get("ok").is_some() {
                continue;
            }
            let err = if let Some(t) = r.get("thrown") {
                if let Some(m) = t.get("forbidden") {
                    CouchError::forbidden(m.as_str().map(String::from).unwrap_or_else(|| m.to_string()))
                } else if let Some(m) = t.get("unauthorized") {
                    CouchError::unauthorized(m.as_str().map(String::from).unwrap_or_else(|| m.to_string()))
                } else {
                    CouchError::forbidden(t.to_string())
                }
            } else {
                CouchError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    r.get("error").and_then(|e| e.as_str()).unwrap_or("render_error"),
                    r.get("reason").and_then(|e| e.as_str()).unwrap_or("").to_string(),
                )
            };
            out[i] = Some(err);
            break;
        }
    }
    Ok(out)
}

// -------------------------------------------------------------- filters

pub struct ChangesFilter {
    funs: JsFuns,
}

impl ChangesFilter {
    pub fn new(ddoc: &Value, name: &str) -> Result<ChangesFilter> {
        let src = ddoc
            .get("filters")
            .and_then(|f| f.get(name))
            .and_then(|s| s.as_str())
            .ok_or_else(|| CouchError::not_found(format!("missing json key: {name}")))?;
        Ok(ChangesFilter {
            funs: JsFuns::new(ddoc, &[src.to_string()], &[]),
        })
    }
    pub fn filter(&mut self, doc: &Value, req: &Value) -> Result<bool> {
        self.funs.filter(0, doc, req)
    }
}

pub struct ViewFilter {
    funs: JsFuns,
}

impl ViewFilter {
    pub fn new(ddoc: &Value, view: &str) -> Result<ViewFilter> {
        let src = ddoc
            .get("views")
            .and_then(|v| v.get(view))
            .and_then(|v| v.get("map"))
            .and_then(|s| s.as_str())
            .ok_or_else(|| CouchError::not_found("missing_named_view"))?;
        Ok(ViewFilter {
            funs: JsFuns::new(ddoc, &[src.to_string()], &[]),
        })
    }
    pub fn filter(&mut self, doc: &Value) -> Result<bool> {
        Ok(self.funs.map(doc)?.first().map(|r| !r.is_empty()).unwrap_or(false))
    }
}

// ------------------------------------------------------- _show / _update

/// HTTP/1 header names reach us lowercased; CouchDB hands functions the
/// names as clients send them, conventionally "Accept", "Content-Type".
fn title_case(name: &str) -> String {
    name.split('-')
        .map(|w| {
            let mut c = w.chars();
            c.next().map(|f| f.to_ascii_uppercase().to_string() + c.as_str()).unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join("-")
}

fn js_req(req: &Req, db: &Db, id: Option<&str>) -> Value {
    let query: serde_json::Map<String, Value> =
        req.query.iter().map(|(k, v)| (k.clone(), json!(v))).collect();
    let headers: serde_json::Map<String, Value> = req
        .headers
        .iter()
        .map(|(k, v)| (title_case(k.as_str()), json!(v.to_str().unwrap_or(""))))
        .collect();
    let form: serde_json::Map<String, Value> =
        if req.content_type().starts_with("application/x-www-form-urlencoded") {
            form_urlencoded::parse(&req.body).map(|(k, v)| (k.to_string(), json!(v))).collect()
        } else {
            serde_json::Map::new()
        };
    json!({
        "info": db.info(),
        "id": id,
        "uuid": crate::http::new_uuid(),
        "method": req.method.as_str(),
        "requested_path": req.path,
        "path": req.path,
        "raw_path": format!("/{}", req.path.join("/")),
        "query": query,
        "headers": headers,
        "body": if req.body.is_empty() { json!("undefined") } else { json!(String::from_utf8_lossy(&req.body)) },
        "peer": "127.0.0.1",
        "form": form,
        "cookie": {},
        "userCtx": req.auth.user.json(&db.name),
        "secObj": db.snapshot().security(),
    })
}

fn render_response(resp: &Value, default_code: StatusCode) -> Response {
    let (code, headers, body, ct) = match resp {
        Value::String(s) => (default_code, serde_json::Map::new(), s.clone().into_bytes(), "text/html; charset=utf-8"),
        Value::Object(o) => {
            let code = o
                .get("code")
                .and_then(|c| c.as_u64())
                .and_then(|c| StatusCode::from_u16(c as u16).ok())
                .unwrap_or(default_code);
            let headers = o.get("headers").and_then(|h| h.as_object()).cloned().unwrap_or_default();
            if let Some(j) = o.get("json") {
                (code, headers, serde_json::to_vec(j).unwrap(), "application/json")
            } else if let Some(b) = o.get("base64").and_then(|b| b.as_str()) {
                use base64::Engine;
                (code, headers, doc::B64.decode(b).unwrap_or_default(), "application/binary")
            } else {
                let b = o.get("body").and_then(|b| b.as_str()).unwrap_or("").as_bytes().to_vec();
                (code, headers, b, "text/html; charset=utf-8")
            }
        }
        _ => (default_code, serde_json::Map::new(), Vec::new(), "text/html; charset=utf-8"),
    };
    let mut b = Response::builder().status(code);
    // The function's own Content-Type wins over the default for its body kind.
    if !headers.keys().any(|k| k.eq_ignore_ascii_case("content-type")) {
        b = b.header(header::CONTENT_TYPE, ct);
    }
    for (k, v) in headers {
        if let Some(s) = v.as_str() {
            b = b.header(k.as_str(), s);
        }
    }
    b.body(Body::from(body)).unwrap()
}

pub async fn http_ddoc_fun(req: &Req, db: &Arc<Db>, ddoc_id: &str, rest: &[&str]) -> Result<Response> {
    let snap = db.snapshot();
    let ddoc = crate::changes::load_ddoc(&snap, ddoc_id)?;
    match rest {
        ["_show", name, docid @ ..] => {
            let src = ddoc_fun(&ddoc, "shows", name)?;
            let id = if docid.is_empty() { None } else { Some(docid.join("/")) };
            let doc = match &id {
                Some(i) => match snap.get_fdi(i)? {
                    Some(f) if !f.deleted => snap.winner_json(&f, &DocOpts::default())?,
                    _ => Value::Null,
                },
                None => Value::Null,
            };
            let funs = JsFuns::new(&ddoc, &[src.to_string()], &[]);
            let jreq = js_req(req, db, id.as_deref());
            let r = blocking(|| funs.show(0, &[doc, jreq]))?;
            Ok(render_response(&r, StatusCode::OK))
        }
        ["_update", name, docid @ ..] => {
            let src = ddoc_fun(&ddoc, "updates", name)?;
            let id = if docid.is_empty() { None } else { Some(docid.join("/")) };
            let doc = match &id {
                Some(i) => match snap.get_fdi(i)? {
                    Some(f) if !f.deleted => snap.winner_json(&f, &DocOpts::default())?,
                    _ => Value::Null,
                },
                None => Value::Null,
            };
            let funs = JsFuns::new(&ddoc, &[src.to_string()], &[]);
            let jreq = js_req(req, db, id.as_deref());
            let r = blocking(|| funs.update(0, &[doc, jreq]))?;
            let arr = r.as_array().cloned().unwrap_or_default();
            let new_doc = arr.first().cloned().unwrap_or(Value::Null);
            let resp = arr.get(1).cloned().unwrap_or(json!(""));
            if new_doc.is_object() {
                let mut d = doc::from_json(new_doc)?;
                if d.id.is_empty() {
                    d.id = id.clone().ok_or_else(|| CouchError::bad_request("Document id must be specified"))?;
                }
                let rev = crate::api::save_docs(req, db, vec![d.clone()], true).await?.pop().unwrap()?;
                let r = render_response(&resp, StatusCode::CREATED);
                let r = with_header(r, header::HeaderName::from_static("x-couch-update-newrev"), &rev);
                return Ok(with_header(r, header::HeaderName::from_static("x-couch-id"), &d.id));
            }
            Ok(render_response(&resp, StatusCode::OK))
        }
        ["_list", name, source @ ..] => {
            if req.method != Method::GET && req.method != Method::POST && req.method != Method::HEAD {
                return crate::http::not_allowed("GET,POST,HEAD");
            }
            let src = ddoc_fun(&ddoc, "lists", name)?;
            let mut args = crate::api::QueryArgs::from_req(req)?;
            if req.method == Method::POST {
                args = crate::api::QueryArgs::from_obj(&req.json_obj_body()?, args)?;
            }
            // The rows: _all_docs, a view in this design doc, or a view in
            // another one (_list/LIST/DDOC/VIEW).
            let mut result = match source {
                ["_all_docs"] => {
                    let s = db.snapshot();
                    blocking(|| crate::api::run_all_docs(&s, &args, crate::api::AllDocsKind::All))?
                }
                [view] => crate::views::view_json(req, db, ddoc_id, view, &args)?,
                [other, view] => crate::views::view_json(req, db, &format!("_design/{other}"), view, &args)?,
                _ => return Err(CouchError::not_found("missing")),
            };
            let rows = match result.as_object_mut().and_then(|o| o.remove("rows")) {
                Some(Value::Array(r)) => r,
                _ => Vec::new(),
            };
            let funs = JsFuns::new(&ddoc, &[src.to_string()], &[]);
            let jreq = js_req(req, db, None);
            let out = blocking(|| funs.list(0, &[result, jreq], &rows))?;
            let start = out.get("start").cloned().unwrap_or(json!({}));
            let code = start
                .get("code")
                .and_then(|c| c.as_u64())
                .and_then(|c| StatusCode::from_u16(c as u16).ok())
                .unwrap_or(StatusCode::OK);
            let headers = start.get("headers").and_then(|h| h.as_object()).cloned().unwrap_or_default();
            let mut b = Response::builder().status(code);
            if !headers.keys().any(|k| k.eq_ignore_ascii_case("content-type")) {
                b = b.header(header::CONTENT_TYPE, "application/json");
            }
            for (k, v) in headers {
                if let Some(s) = v.as_str() {
                    b = b.header(k.as_str(), s);
                }
            }
            let body = out.get("body").and_then(|b| b.as_str()).unwrap_or("").to_string();
            Ok(b.body(Body::from(body)).unwrap())
        }
        ["_rewrite", ..] => crate::rewrite::handle(req, &ddoc).await,
        _ => Err(CouchError::not_found("missing")),
    }
}

/// Run a design doc's rewrite function on the request: the new path or
/// rewrite object, or null for no match.
pub fn run_rewrite(req: &Req, ddoc: &Value, src: &str) -> Result<Value> {
    let db = req.server.open_db(&req.path[0])?;
    let mut jreq = js_req(req, &db, None);
    if let Some(o) = jreq.as_object_mut() {
        for k in ["info", "form", "uuid", "id"] {
            o.remove(k);
        }
    }
    let funs = JsFuns::new(ddoc, &[src.to_string()], &[]);
    blocking(|| unwrap_ok(funs.call("__rewrite", CallArgs::IS(0, Value::Array(vec![jreq]).to_string()))?))
}

/// A design doc function by kind and name; CouchDB's error when missing.
fn ddoc_fun<'a>(ddoc: &'a Value, kind: &str, name: &str) -> Result<&'a str> {
    ddoc.get(kind)
        .and_then(|s| s.get(name))
        .and_then(|s| s.as_str())
        .ok_or_else(|| CouchError::not_found(format!("missing json key: {name}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_reduce_and_require() {
        let ddoc = json!({"views": {"lib": {"util": "exports.double = function(x) { return x * 2; };"}}});
        let f = JsFuns::new(
            &ddoc,
            &["function(doc) { var u = require('views/lib/util'); emit(doc.k, u.double(doc.v)); }".into()],
            &["function(keys, values, rereduce) { return sum(values); }".into()],
        );
        let rows = f.map(&json!({"_id": "a", "k": "x", "v": 21})).unwrap();
        assert_eq!(rows[0], vec![(json!("x"), json!(42))]);
        let r = f.reduce(0, &[(json!(["x", "a"]), json!(1)), (json!(["y", "b"]), json!(2))]).unwrap();
        assert_eq!(r, json!(3));
        assert_eq!(f.rereduce(0, &[json!(3), json!(4)]).unwrap(), json!(7));
    }

    #[test]
    fn runaway_function_times_out() {
        let f = JsFuns::new(&Value::Null, &["function(doc) { while (true) {} }".into()], &[]);
        let t = Instant::now();
        // Map errors are swallowed per doc, so this returns no rows.
        let rows = f.map(&json!({})).unwrap_or_default();
        assert!(t.elapsed() < Duration::from_secs(10));
        assert!(rows.is_empty() || rows[0].is_empty());
    }
}
