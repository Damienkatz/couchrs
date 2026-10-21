pub mod api;
pub mod btree;
pub mod changes;
pub mod collate;
pub mod db;
pub mod doc;
pub mod error;
pub mod file;
pub mod http;
pub mod js;
pub mod keytree;
pub mod mango;
pub mod server;
pub mod views;

use std::path::PathBuf;

fn usage() -> ! {
    eprintln!(
        "usage: couchdb-rs [--data-dir DIR] [--bind ADDR:PORT] [--admin USER:PASS]\n\
         env: COUCHDB_DATA_DIR, COUCHDB_BIND, COUCHDB_USER, COUCHDB_PASSWORD"
    );
    std::process::exit(2)
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let mut data_dir = std::env::var("COUCHDB_DATA_DIR").unwrap_or_else(|_| "./data".into());
    let mut bind = std::env::var("COUCHDB_BIND").unwrap_or_else(|_| "127.0.0.1:5984".into());
    let mut admin = match (std::env::var("COUCHDB_USER"), std::env::var("COUCHDB_PASSWORD")) {
        (Ok(u), Ok(p)) => Some((u, p)),
        _ => None,
    };
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--data-dir" => data_dir = args.next().unwrap_or_else(|| usage()),
            "--bind" => bind = args.next().unwrap_or_else(|| usage()),
            "--admin" => {
                let s = args.next().unwrap_or_else(|| usage());
                let (u, p) = s.split_once(':').unwrap_or_else(|| usage());
                admin = Some((u.to_string(), p.to_string()));
            }
            _ => usage(),
        }
    }
    let config = server::Config {
        data_dir: PathBuf::from(data_dir),
        bind,
        admin,
    };
    let server = server::Server::new(config).unwrap_or_else(|e| {
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
