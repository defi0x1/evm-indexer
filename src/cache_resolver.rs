use std::sync::Arc;

use axum::{Json, body::Bytes, extract::State, http::StatusCode, response::IntoResponse};
use reqwest::Client;
use serde_json::{Value, json};
use tracing::{debug, warn};

use crate::{
    helper::get_and_insert_tx,
    redis::Redis,
    reqwest_builder::{DEFAULT_INITIAL_RETRY_DELAY, DEFAULT_MAX_RETRIES, RpcRequestBuilder},
    sqlite::Sqlite,
};

#[derive(Clone)]
pub struct CacheResolver(Arc<ResolverInner>);

struct ResolverInner {
    db: Sqlite,
    cache: Redis,
    client: Client,
    rpc_url: String,
}

impl CacheResolver {
    pub fn new(db: Sqlite, cache: Redis, client: Client, rpc_url: String) -> Self {
        Self(Arc::new(ResolverInner {
            db,
            cache,
            client,
            rpc_url,
        }))
    }

    pub async fn get_transaction(&self, hash: &str) -> anyhow::Result<Option<Value>> {
        let inner = &self.0;
        resolve_cached(
            || inner.cache.get_transaction(hash),
            || inner.db.get_transaction(hash),
            |data| {
                let (data, cache, hash) = (data.clone(), inner.cache.clone(), hash.to_owned());
                async move { cache.set_transaction(&hash, &data).await }
            },
            || async {
                get_and_insert_tx(&inner.db, &inner.client, &inner.rpc_url, hash).await?;
                Ok(inner.db.get_transaction(hash))
            },
        )
        .await
    }

    pub async fn get_receipt(&self, hash: &str) -> anyhow::Result<Option<Value>> {
        let inner = &self.0;
        resolve_cached(
            || inner.cache.get_receipt(hash),
            || inner.db.get_receipt(hash),
            |data| {
                let (data, cache, hash) = (data.clone(), inner.cache.clone(), hash.to_owned());
                async move { cache.set_receipt(&hash, &data).await }
            },
            || async {
                get_and_insert_tx(&inner.db, &inner.client, &inner.rpc_url, hash).await?;
                Ok(inner.db.get_receipt(hash))
            },
        )
        .await
    }

    pub async fn forward(&self, method: &str, body: &Value) -> anyhow::Result<Value> {
        RpcRequestBuilder::new(&self.0.client, &self.0.rpc_url)
            .method(method)
            .params(body["params"].clone())
            .max_retries(DEFAULT_MAX_RETRIES)
            .initial_delay(DEFAULT_INITIAL_RETRY_DELAY)
            .call()
            .await
    }
}

#[derive(Clone)]
pub struct AppState {
    pub resolver: CacheResolver,
}

pub async fn handle(State(state): State<AppState>, body: Bytes) -> impl IntoResponse {
    let body = match serde_json::from_slice::<Value>(&body) {
        Ok(v) => v,
        Err(e) => return rpc_parse_error(e).into_response(),
    };

    let method = body["method"].as_str().unwrap_or("").to_owned();
    let id = body["id"].clone();

    debug!(method, "incoming JSON-RPC request");

    match dispatch(&state, &method, &body, &id).await {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => {
            warn!(method, "handler error: {e:#}");
            (
                StatusCode::OK,
                Json(rpc_error(&id, -32603, &format!("{e:#}"))),
            )
                .into_response()
        }
    }
}

async fn dispatch(
    state: &AppState,
    method: &str,
    body: &Value,
    id: &Value,
) -> anyhow::Result<Value> {
    match method {
        "eth_getTransactionByHash" => {
            let hash = extract_tx_hash(body)?;
            let data = state.resolver.get_transaction(&hash).await?;
            Ok(rpc_ok(id, data.unwrap_or(Value::Null)))
        }
        "eth_getTransactionReceipt" => {
            let hash = extract_tx_hash(body)?;
            let data = state.resolver.get_receipt(&hash).await?;
            Ok(rpc_ok(id, data.unwrap_or(Value::Null)))
        }
        _ => state.resolver.forward(method, body).await,
    }
}

async fn resolve_cached<CacheGet, CacheGetFut, DbGet, CacheSet, CacheSetFut, Fetch, FetchFut>(
    cache_get: CacheGet,
    db_get: DbGet,
    cache_set: CacheSet,
    fetch: Fetch,
) -> anyhow::Result<Option<Value>>
where
    CacheGet: FnOnce() -> CacheGetFut,
    CacheGetFut: std::future::Future<Output = Option<Value>>,
    DbGet: FnOnce() -> Option<Value>,
    CacheSet: Fn(&Value) -> CacheSetFut,
    CacheSetFut: std::future::Future<Output = ()>,
    Fetch: FnOnce() -> FetchFut,
    FetchFut: std::future::Future<Output = anyhow::Result<Option<Value>>>,
{
    if let Some(cached) = cache_get().await {
        return Ok(Some(cached));
    }

    if let Some(stored) = db_get() {
        cache_set(&stored).await;
        return Ok(Some(stored));
    }

    let result = fetch().await?;
    if let Some(ref data) = result {
        cache_set(data).await;
    }
    Ok(result)
}

fn rpc_ok(id: &Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn rpc_error(id: &Value, code: i32, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn rpc_parse_error(e: serde_json::Error) -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(rpc_error(
            &Value::Null,
            -32700,
            &format!("parse error: {e}"),
        )),
    )
}

fn extract_tx_hash(body: &Value) -> anyhow::Result<String> {
    let hash = body["params"]
        .as_array()
        .and_then(|a| a.first())
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing or invalid first param"))?;

    if !hash.starts_with("0x") || hash.len() != 66 {
        anyhow::bail!("invalid tx hash format: {hash}");
    }

    Ok(hash.to_lowercase())
}
