use anyhow::Result;
use redis::{AsyncCommands, Client, aio::ConnectionManager};
use serde_json::Value;
use tracing::warn;

use crate::constants;

enum Key<'a> {
    Transaction(&'a str),
    Receipt(&'a str),
}

impl<'a> Key<'a> {
    fn build(&self) -> String {
        match self {
            Key::Transaction(h) => format!("transaction:{h}"),
            Key::Receipt(h) => format!("receipt:{h}"),
        }
    }
}

#[derive(Clone)]
pub struct Redis {
    conn: ConnectionManager,
}

impl Redis {
    pub async fn create(url: &str) -> Result<Self> {
        let client = Client::open(url)?;
        let conn = ConnectionManager::new(client).await?;
        Ok(Self { conn })
    }

    async fn get(&self, key: Key<'_>) -> Option<Value> {
        let key = key.build();
        let mut conn = self.conn.clone();
        match conn.get::<_, Option<String>>(&key).await {
            Ok(Some(s)) => serde_json::from_str(&s).ok(),
            Ok(None) => None,
            Err(e) => {
                warn!(key, "redis get key: {key} - error: {e}");
                None
            }
        }
    }

    async fn set(&self, key: Key<'_>, value: &Value) {
        let key = key.build();
        let Ok(json) = serde_json::to_string(value) else {
            return;
        };
        let mut conn = self.conn.clone();
        if let Err(e) = conn
            .set_ex::<_, _, ()>(&key, json, constants::REDIS_TTL_SECS)
            .await
        {
            warn!(key, "redis set key: {key} - value: {value} - error: {e}");
        }
    }

    pub async fn get_transaction(&self, hash: &str) -> Option<Value> {
        self.get(Key::Transaction(hash)).await
    }

    pub async fn set_transaction(&self, hash: &str, data: &Value) {
        self.set(Key::Transaction(hash), data).await;
    }

    pub async fn get_receipt(&self, hash: &str) -> Option<Value> {
        self.get(Key::Receipt(hash)).await
    }

    pub async fn set_receipt(&self, hash: &str, data: &Value) {
        self.set(Key::Receipt(hash), data).await;
    }
}
