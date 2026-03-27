use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

use crate::helper::TxData;

const PRAGMAS: &str = "
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous  = NORMAL;
    PRAGMA foreign_keys = ON;
    PRAGMA busy_timeout = 5000;
";

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS transactions (
        tx_hash TEXT PRIMARY KEY,
        block_number INTEGER,
        block_hash TEXT,
        from_address TEXT,
        to_address TEXT,
        raw_data TEXT NOT NULL,
        inserted_at INTEGER NOT NULL DEFAULT (unixepoch())
    );

    CREATE TABLE IF NOT EXISTS receipts (
        tx_hash TEXT PRIMARY KEY,
        block_number INTEGER,
        block_hash TEXT,
        from_address TEXT,
        to_address TEXT,
        raw_data TEXT NOT NULL,
        inserted_at INTEGER NOT NULL DEFAULT (unixepoch())
    );
";

#[derive(Clone, Copy)]
enum Table {
    Transactions,
    Receipts,
}

impl Table {
    fn name(&self) -> &'static str {
        match self {
            Table::Transactions => "transactions",
            Table::Receipts => "receipts",
        }
    }
}

#[derive(Clone)]
pub struct Sqlite(Arc<Mutex<Connection>>);

impl Sqlite {
    pub fn get_or_create(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;

        conn.execute_batch(PRAGMAS)?;
        conn.execute_batch(SCHEMA)?;

        Ok(Self(Arc::new(Mutex::new(conn))))
    }

    fn lock(&'_ self) -> MutexGuard<'_, Connection> {
        self.0.lock().expect("Database already lock poisoned")
    }

    fn exists(&self, table: Table, tx_hash: &str) -> bool {
        let conn = self.lock();
        let query = format!("SELECT 1 FROM {} WHERE tx_hash = ?1", table.name());
        conn.query_row(&query, params![tx_hash], |_| Ok(()))
            .optional()
            .ok()
            .flatten()
            .is_some()
    }

    fn get(&self, table: Table, tx_hash: &str) -> Option<Value> {
        let conn = self.lock();
        let query = format!("SELECT raw_data FROM {} WHERE tx_hash = ?1", table.name());
        conn.query_row(&query, params![tx_hash], |row| row.get::<_, String>(0))
            .optional()
            .ok()
            .flatten()
            .and_then(|s| serde_json::from_str(&s).ok())
    }

    fn set(&self, table: Table, data: &TxData<'_>) -> Result<()> {
        let block_number = data.block_number as i64;
        let conn = self.lock();
        let query = format!(
            "INSERT OR IGNORE INTO {} (tx_hash, block_number, block_hash, from_address, to_address, raw_data)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            table.name()
        );
        conn.execute(
            &query,
            params![
                data.tx_hash,
                block_number,
                data.block_hash,
                data.from_address,
                data.to_address,
                data.raw_data,
            ],
        )?;
        Ok(())
    }

    fn get_latest_block_number(&self, table: Table) -> Option<u64> {
        let conn = self.lock();
        let query = format!("SELECT MAX(block_number) FROM {}", table.name());
        conn.query_row(&query, [], |row| row.get::<_, i64>(0))
            .optional()
            .ok()
            .flatten()
            .map(|n| n as u64)
    }

    pub fn get_latest_transaction_block_number(&self) -> Option<u64> {
        self.get_latest_block_number(Table::Transactions)
    }

    pub fn get_latest_receipt_block_number(&self) -> Option<u64> {
        self.get_latest_block_number(Table::Receipts)
    }

    pub fn get_transaction(&self, tx_hash: &str) -> Option<Value> {
        self.get(Table::Transactions, tx_hash)
    }

    pub fn insert_transaction(&self, data: &TxData<'_>) -> Result<()> {
        self.set(Table::Transactions, data)
    }

    pub fn has_transaction(&self, tx_hash: &str) -> bool {
        self.exists(Table::Transactions, tx_hash)
    }

    pub fn get_receipt(&self, tx_hash: &str) -> Option<Value> {
        self.get(Table::Receipts, tx_hash)
    }

    pub fn insert_receipt(&self, data: &TxData<'_>) -> Result<()> {
        self.set(Table::Receipts, data)
    }

    pub fn has_receipt(&self, tx_hash: &str) -> bool {
        self.exists(Table::Receipts, tx_hash)
    }

    pub fn index_status(&self, tx_hash: &str) -> IndexStatus {
        IndexStatus {
            has_transaction: self.has_transaction(tx_hash),
            has_receipt: self.has_receipt(tx_hash),
        }
    }
}

pub struct IndexStatus {
    pub has_transaction: bool,
    pub has_receipt: bool,
}

impl IndexStatus {
    pub fn is_complete(&self) -> bool {
        self.has_transaction && self.has_receipt
    }

    pub fn needs_transaction(&self) -> bool {
        !self.has_transaction
    }

    pub fn needs_receipt(&self) -> bool {
        !self.has_receipt
    }
}
