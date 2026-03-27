pub mod cache_resolver;
pub mod config;
pub mod constants;
pub mod database;
pub mod fetcher;
pub mod redis;
pub mod rpc_server;

use axum::{Router, routing::post};
pub use cache_resolver::*;
pub use config::*;
pub use database::*;
pub use fetcher::*;
pub use rpc_server::*;

use anyhow::{Ok, Result};
use tokio::sync::watch;
use tracing::info;

use crate::{redis::Redis, reqwest_builder::HttpClientBuilder, sqlite::Sqlite};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let cfg = Config::get_config();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rise_indexer=info".into()),
        )
        .init();

    // println!("config: {:?}", cfg);

    let db = Sqlite::get_or_create(&cfg.db_path)?;
    let cache = Redis::create(&cfg.redis_url).await?;
    let client = HttpClientBuilder::new().build()?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let handles = spawn_background_tasks(&db, &client, &cfg, shutdown_rx);

    start_serve(db, cache, client, cfg).await?;

    shutdown(shutdown_tx, handles).await;

    Ok(())
}

fn spawn_background_tasks(
    db: &Sqlite,
    client: &reqwest::Client,
    cfg: &Config,
    shutdown_rx: watch::Receiver<bool>,
) -> (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>) {
    let backfill_handle = {
        let (db, client, rpc_url, rx) = (
            db.clone(),
            client.clone(),
            cfg.rpc_url.clone(),
            shutdown_rx.clone(),
        );
        tokio::spawn(async move {
            fetcher::backfill::run(db, client, rpc_url, rx).await;
        })
    };

    let shreds_handle = {
        let (db, client, ws_url, rpc_url, rx) = (
            db.clone(),
            client.clone(),
            cfg.ws_url.clone(),
            cfg.rpc_url.clone(),
            shutdown_rx,
        );
        tokio::spawn(async move {
            fetcher::sherds_sub::run(db, client, ws_url, rpc_url, rx).await;
        })
    };

    (backfill_handle, shreds_handle)
}

async fn start_serve(db: Sqlite, cache: Redis, client: reqwest::Client, cfg: Config) -> Result<()> {
    let resolver = CacheResolver::new(db, cache, client, cfg.rpc_url);
    let state = AppState { resolver };

    let app = Router::new().route("/", post(handle)).with_state(state);

    let listener = tokio::net::TcpListener::bind(&cfg.server_address).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install Ctrl+C handler");
            info!("signal received, draining in-flight requests...");
        })
        .await?;

    Ok(())
}

async fn shutdown(
    shutdown_tx: watch::Sender<bool>,
    handles: (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>),
) {
    info!("axum drained, stopping background tasks...");
    let _ = shutdown_tx.send(true);

    if tokio::time::timeout(tokio::time::Duration::from_secs(10), async {
        let _ = tokio::join!(handles.0, handles.1);
    })
    .await
    .is_err()
    {
        tracing::warn!("tasks did not finish within timeout, forcing exit");
    }

    info!("shutdown complete");
}
