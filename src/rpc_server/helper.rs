use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::{debug, warn};

use crate::{
    reqwest_builder::RpcRequestBuilder,
    sqlite::{IndexStatus, Sqlite},
};

#[derive(Default, Clone, Deserialize, Serialize, Debug, PartialEq, Eq, Hash)]
pub struct TxData<'a> {
    pub tx_hash: &'a str,
    pub block_number: u64,
    pub block_hash: Option<&'a str>,
    pub from_address: Option<&'a str>,
    pub to_address: Option<&'a str>,
    pub raw_data: &'a str,
}

impl<'a> TxData<'a> {
    pub fn parsed_from_value(hash: &'a str, v: &'a Value, raw: &'a str) -> Result<Self> {
        Ok(Self {
            tx_hash: hash,
            block_number: v
                .get("blockNumber")
                .and_then(Value::as_str)
                .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
                .ok_or_else(|| anyhow::anyhow!("missing or invalid blockNumber"))?,
            block_hash: v.get("blockHash").and_then(Value::as_str),
            from_address: v.get("from").and_then(Value::as_str),
            to_address: v.get("to").and_then(Value::as_str),
            raw_data: raw,
        })
    }
}

pub async fn get_and_insert_tx(
    db: &Sqlite,
    client: &reqwest::Client,
    url: &str,
    hash: &str,
) -> Result<()> {
    let status = db.index_status(hash);

    if status.is_complete() {
        debug!(hash, "already fully indexed, skipping");
        return Ok(());
    }

    get_raw_tx_and_insert(db, client, url, hash, &status).await
}

async fn get_raw_tx_and_insert(
    db: &Sqlite,
    client: &reqwest::Client,
    url: &str,
    hash: &str,
    status: &IndexStatus,
) -> Result<()> {
    let (tx_result, rc_result) = tokio::join!(
        get_tx_hash(
            client,
            url,
            hash,
            "eth_getTransactionByHash",
            status.has_transaction
        ),
        get_tx_hash(
            client,
            url,
            hash,
            "eth_getTransactionReceipt",
            status.has_receipt
        ),
    );

    insert_data_if_present(db, hash, tx_result?, |db, data| {
        db.insert_transaction(&data)
            .context("inserting transaction")
    })?;

    insert_data_if_present(db, hash, rc_result?, |db, data| {
        db.insert_receipt(data).context("inserting receipt")
    })?;

    debug!(hash, "indexing completed");
    Ok(())
}

async fn get_tx_hash(
    client: &reqwest::Client,
    url: &str,
    hash: &str,
    method: &str,
    already_exists: bool,
) -> Result<Option<Value>> {
    if already_exists {
        return Ok(None);
    }

    let result = RpcRequestBuilder::new(client, url)
        .method(method)
        .params(json!([hash]))
        .call_and_extract()
        .await?;

    Ok(Some(result))
}

fn insert_data_if_present(
    db: &Sqlite,
    hash: &str,
    result: Option<Value>,
    insert: impl Fn(&Sqlite, &TxData) -> Result<()>,
) -> Result<()> {
    match result {
        Some(v) if !v.is_null() => {
            let raw_data = v.to_string();
            let tx_data = TxData::parsed_from_value(hash, &v, &raw_data)
                .context("constructing TxData from RPC response")?;
            insert(db, &tx_data)
        }
        Some(_) => {
            warn!(hash, "upstream returned null");
            Ok(())
        }
        None => Ok(()),
    }
}
