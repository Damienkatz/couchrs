pub mod api;
pub mod auth;
pub mod btree;
pub mod changes;
pub mod collate;
pub mod config;
pub mod db;
pub mod doc;
pub mod error;
pub mod extsort;
pub mod fauxton;
pub mod file;
pub mod http;
pub mod js;
pub mod jwt;
pub mod ken;
pub mod keytree;
pub mod mango;
pub mod passwords;
pub mod replicator;
pub mod rewrite;
pub mod rstore;
pub mod server;
pub mod smoosh;
pub mod stats;
pub mod system;
pub mod tasks;
pub mod tls;
pub mod views;

use std::path::PathBuf;

fn usage() -> ! {
    eprintln!(
        "usage: couchdb-rs [--data-dir DIR] [--ini FILE|DIR]... [--bind ADDR:PORT] [--admin USER:PASS]\n\
         \x20                 [--fsync-mode fsync|full]\n\
         Configuration is read from the built-in defaults, then each --ini file or\n\
         directory in order (default: DATA_DIR/local.ini); changes made over the\n\
         _config API are saved to the last file. --bind and --admin override the\n\
         files without being saved.\n\
         env: COUCHDB_DATA_DIR, COUCHDB_BIND, COUCHDB_USER, COUCHDB_PASSWORD"
    );
    std::process::exit(2)
}

/// Counts each message logged, by level (couch_log's stats).
struct LogStats;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for LogStats {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let m = match *event.metadata().level() {
            tracing::Level::ERROR => stat!("couch_log.level.error"),
            tracing::Level::WARN => stat!("couch_log.level.warning"),
            tracing::Level::INFO => stat!("couch_log.level.info"),
            _ => stat!("couch_log.level.debug"),
        };
        m.inc();
    }
}

fn main() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .with(LogStats)
        .init();
    let mut data_dir = std::env::var("COUCHDB_DATA_DIR").unwrap_or_else(|_| "./data".into());
    let mut bind = std::env::var("COUCHDB_BIND").ok();
    let mut inis: Vec<PathBuf> = Vec::new();
    let mut admin = match (std::env::var("COUCHDB_USER"), std::env::var("COUCHDB_PASSWORD")) {
        (Ok(u), Ok(p)) => Some((u, p)),
        _ => None,
    };
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--data-dir" => data_dir = args.next().unwrap_or_else(|| usage()),
            "--bind" => bind = Some(args.next().unwrap_or_else(|| usage())),
            "--ini" => inis.push(PathBuf::from(args.next().unwrap_or_else(|| usage()))),
            "--fsync-mode" => match args.next().as_deref() {
                Some("fsync") => {}
                Some("full") => file::FULL_FSYNC.store(true, std::sync::atomic::Ordering::Relaxed),
                _ => usage(),
            },
            "--admin" => {
                let s = args.next().unwrap_or_else(|| usage());
                let (u, p) = s.split_once(':').unwrap_or_else(|| usage());
                admin = Some((u.to_string(), p.to_string()));
            }
            _ => usage(),
        }
    }
    let data_dir = PathBuf::from(data_dir);
    if inis.is_empty() {
        inis.push(data_dir.join("local.ini"));
    }
    let mut overrides = Vec::new();
    let mut set = |s: &str, k: &str, v: &str| overrides.push(((s.to_string(), k.to_string()), v.to_string()));
    if let Some(b) = bind {
        let (addr, port) = b.rsplit_once(':').unwrap_or_else(|| usage());
        set("chttpd", "bind_address", addr.trim_start_matches('[').trim_end_matches(']'));
        set("chttpd", "port", port);
    }
    if let Some((u, p)) = admin {
        set("admins", &u, &p);
    }
    let config = config::Config::load(inis, overrides).unwrap_or_else(|e| {
        eprintln!("failed to start: {}", e.reason);
        std::process::exit(1)
    });
    let server = server::Server::new(data_dir, std::sync::Arc::new(config)).unwrap_or_else(|e| {
        eprintln!("failed to start: {}", e.reason);
        std::process::exit(1)
    });
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    if let Err(e) = rt.block_on(http::serve(server)) {
        eprintln!("server error: {e}");
        std::process::exit(1);
    }
}
