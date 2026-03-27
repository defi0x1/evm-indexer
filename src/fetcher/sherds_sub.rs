use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::{Value, json};
use tokio::{
    sync::watch,
    time::{Duration, sleep},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use crate::{helper::get_and_insert_tx, sqlite::Sqlite};

const RECONNECT_DELAY: Duration = Duration::from_secs(2);
const SUBSCRIBE_ID: u64 = 1;

pub async fn run(
    db: Sqlite,
    client: Client,
    ws_url: String,
    rpc_url: String,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            result = connect_and_stream(&db, &client, &ws_url, &rpc_url) => {
                if let Err(e) = result {
                    error!("shred listener error: {e:#}");
                }
                // connection dropped, check shutdown trước khi reconnect
                if *shutdown.borrow() {
                    info!("shred listener shutting down");
                    return;
                }
                info!(delay_secs = RECONNECT_DELAY.as_secs(), "shred listener reconnecting");
                sleep(RECONNECT_DELAY).await;
            }
            _ = shutdown.changed() => {
                info!("shred listener shutting down");
                return;
            }
        }
    }
}

async fn connect_and_stream(
    db: &Sqlite,
    client: &Client,
    ws_url: &str,
    rpc_url: &str,
) -> Result<()> {
    info!(ws_url, "shred listener connecting");
    let (mut ws, _) = connect_async(ws_url)
        .await
        .context("connecting to WebSocket")?;
    info!(ws_url, "shred listener connected");
    subscribe_to_shreds(&mut ws).await?;
    event_loop(db, client, rpc_url, &mut ws).await
}

async fn subscribe_to_shreds(
    ws: &mut (
             impl SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error>
             + StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
             + Unpin
         ),
) -> Result<()> {
    let msg = json!({
        "jsonrpc": "2.0",
        "method":  "eth_subscribe",
        "params":  ["shreds"],
        "id":      SUBSCRIBE_ID,
    });

    ws.send(Message::Text(msg.to_string().into()))
        .await
        .context("sending subscribe message")?;

    if let Some(Ok(Message::Text(text))) = ws.next().await {
        if let Ok(parsed) = serde_json::from_str::<Value>(&text) {
            if let Some(sub_id) = parsed["result"].as_str() {
                info!(sub_id, "shred subscription confirmed");
            }
        }
    }

    Ok(())
}

async fn event_loop(
    db: &Sqlite,
    client: &Client,
    rpc_url: &str,
    ws: &mut (
             impl StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
             + SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error>
             + Unpin
         ),
) -> Result<()> {
    while let Some(msg) = ws.next().await {
        match msg? {
            Message::Text(text) => {
                handle_text_message(db, client, rpc_url, &text).await;
            }
            Message::Ping(data) => {
                ws.send(Message::Pong(data)).await.context("sending pong")?;
            }
            Message::Close(_) => {
                warn!("shred WebSocket closed by server");
                break;
            }
            _ => {}
        }
    }
    Ok(())
}

async fn handle_text_message(db: &Sqlite, client: &Client, rpc_url: &str, text: &str) {
    let notification: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            warn!("failed to parse shred notification: {e}");
            return;
        }
    };

    handle_shred_notification(db, client, rpc_url, &notification).await;
}

async fn handle_shred_notification(
    db: &Sqlite,
    client: &Client,
    rpc_url: &str,
    notification: &Value,
) {
    let Some(txs) = extract_shred_transactions(notification) else {
        debug!("no transactions array in shred notification");
        return;
    };

    let block = notification
        .pointer("/params/result/blockNumber")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let shred = notification
        .pointer("/params/result/shredIdx")
        .and_then(Value::as_u64)
        .unwrap_or(0);

    info!(block, shred, txs = txs.len(), "received shred");

    for hash in txs {
        if let Err(e) = get_and_insert_tx(db, client, rpc_url, &hash).await {
            warn!(hash, "get_and_insert_tx failed: {e:#}");
        }
    }
}

fn extract_shred_transactions(notification: &Value) -> Option<Vec<String>> {
    let txs = notification
        .pointer("/params/result/transactions")
        .and_then(Value::as_array)?;

    let hashes: Vec<String> = txs
        .iter()
        .filter_map(|entry| {
            let hash = entry.pointer("/transaction/hash")?.as_str()?;
            if hash.is_empty() {
                warn!("shred transaction entry has empty hash");
                return None;
            }
            Some(hash.to_owned())
        })
        .collect();

    if hashes.is_empty() {
        None
    } else {
        Some(hashes)
    }
}
