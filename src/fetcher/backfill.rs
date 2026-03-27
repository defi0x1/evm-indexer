use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::json;
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::{helper::get_and_insert_tx, reqwest_builder::RpcRequestBuilder, sqlite::Sqlite};

const BLOCK_FETCH_ID: u64 = 1;

pub async fn run(db: Sqlite, client: Client, rpc_url: String, shutdown: watch::Receiver<bool>) {
    if let Err(e) = backfill(&db, &client, &rpc_url, shutdown).await {
        error!("backfiller exited with error: {e:#}");
    }
}

async fn backfill(
    db: &Sqlite,
    client: &Client,
    rpc_url: &str,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let latest = get_latest_block_number(client, rpc_url)
        .await
        .context("fetching latest current onchain block number")?;

    let latest_indexed_block_number = db.get_latest_transaction_block_number().unwrap();

    info!(latest_block = latest, "backfiller starting");

    for block_num in (latest_indexed_block_number..=latest).rev() {
        if *shutdown.borrow() {
            tracing::info!(block_num, "backfill interrupted by shutdown");
            return Ok(());
        }

        if let Err(e) = process_block(db, client, rpc_url, block_num).await {
            warn!(block = block_num, "skipping block due to error: {e:#}");
        }
    }

    info!("backfiller finished — all blocks processed");
    Ok(())
}

async fn process_block(db: &Sqlite, client: &Client, rpc_url: &str, block_num: u64) -> Result<()> {
    let txs = get_block_transactions(client, rpc_url, block_num).await?;

    if txs.is_empty() {
        return Ok(());
    }

    info!(block = block_num, txs = txs.len(), "backfilling block");

    for hash in txs {
        if let Err(e) = get_and_insert_tx(db, client, rpc_url, &hash).await {
            warn!(block = block_num, hash, "get_and_insert_tx error: {e:#}");
        }
    }

    Ok(())
}

async fn get_block_transactions(
    client: &Client,
    rpc_url: &str,
    block_num: u64,
) -> Result<Vec<String>> {
    let block = RpcRequestBuilder::new(client, rpc_url)
        .method("eth_getBlockByNumber")
        .params(json!([format!("0x{block_num:x}"), true]))
        .id(BLOCK_FETCH_ID)
        .call_and_extract()
        .await?;

    if block.is_null() {
        return Ok(vec![]);
    }

    let hashes = block["transactions"]
        .as_array()
        .map(|txs| {
            txs.iter()
                .filter_map(|tx| tx["hash"].as_str())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();

    Ok(hashes)
}

async fn get_latest_block_number(client: &Client, rpc_url: &str) -> Result<u64> {
    let result = RpcRequestBuilder::new(client, rpc_url)
        .method("eth_blockNumber")
        .params(json!([]))
        .id(BLOCK_FETCH_ID)
        .call_and_extract()
        .await?;

    let hex = result
        .as_str()
        .context("eth_blockNumber result is not a string")?;

    u64::from_str_radix(hex.trim_start_matches("0x"), 16).context("parsing block number hex")
}
