// SPDX-License-Identifier: Apache-2.0
//! Best-effort transaction support for writable connectors.
//!
//! Buffers write operations between BEGIN and COMMIT. ROLLBACK discards
//! the buffer. No distributed 2PC — writes are flushed sequentially on
//! COMMIT and partial failures are reported.

use std::collections::HashMap;
use std::sync::Mutex;

use arrow::record_batch::RecordBatch;

/// A pending write operation buffered during a transaction.
#[derive(Debug, Clone)]
pub struct PendingWrite {
    pub datasource: String,
    pub table: String,
    pub batches: Vec<RecordBatch>,
}

/// State of a single transaction.
#[derive(Debug, Default)]
pub struct Transaction {
    pub writes: Vec<PendingWrite>,
}

/// Transaction store keyed by transaction ID.
#[derive(Debug, Default)]
pub struct TransactionStore {
    txns: Mutex<HashMap<String, Transaction>>,
}

impl TransactionStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Begin a new transaction. Returns false if ID already exists.
    pub fn begin(&self, txn_id: &str) -> bool {
        let mut txns = self.txns.lock().unwrap();
        if txns.contains_key(txn_id) {
            return false;
        }
        txns.insert(txn_id.to_string(), Transaction::default());
        true
    }

    /// Buffer a write in an active transaction.
    pub fn add_write(&self, txn_id: &str, write: PendingWrite) -> bool {
        let mut txns = self.txns.lock().unwrap();
        match txns.get_mut(txn_id) {
            Some(txn) => { txn.writes.push(write); true }
            None => false,
        }
    }

    /// Take all pending writes and remove the transaction (for COMMIT).
    pub fn take(&self, txn_id: &str) -> Option<Vec<PendingWrite>> {
        self.txns.lock().unwrap().remove(txn_id).map(|t| t.writes)
    }

    /// Discard a transaction (ROLLBACK).
    pub fn rollback(&self, txn_id: &str) -> bool {
        self.txns.lock().unwrap().remove(txn_id).is_some()
    }

    /// Check if a transaction is active.
    pub fn is_active(&self, txn_id: &str) -> bool {
        self.txns.lock().unwrap().contains_key(txn_id)
    }

    /// Number of active transactions.
    pub fn count(&self) -> usize {
        self.txns.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_batch() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
        vec![RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a"]))]).unwrap()]
    }

    fn pw(ds: &str, tbl: &str) -> PendingWrite {
        PendingWrite { datasource: ds.into(), table: tbl.into(), batches: test_batch() }
    }

    #[test]
    fn test_begin_and_is_active() {
        let store = TransactionStore::new();
        assert!(store.begin("tx1"));
        assert!(store.is_active("tx1"));
        assert!(!store.is_active("tx2"));
    }

    #[test]
    fn test_begin_duplicate_fails() {
        let store = TransactionStore::new();
        assert!(store.begin("tx1"));
        assert!(!store.begin("tx1"));
    }

    #[test]
    fn test_add_write_and_take() {
        let store = TransactionStore::new();
        store.begin("tx1");
        assert!(store.add_write("tx1", pw("ds", "t1")));
        assert!(store.add_write("tx1", pw("ds", "t2")));
        let writes = store.take("tx1").unwrap();
        assert_eq!(writes.len(), 2);
        assert!(!store.is_active("tx1"));
    }

    #[test]
    fn test_add_write_no_txn() {
        let store = TransactionStore::new();
        assert!(!store.add_write("nope", pw("ds", "t")));
    }

    #[test]
    fn test_rollback() {
        let store = TransactionStore::new();
        store.begin("tx1");
        store.add_write("tx1", pw("ds", "t"));
        assert!(store.rollback("tx1"));
        assert!(!store.is_active("tx1"));
        assert!(!store.rollback("tx1"));
    }

    #[test]
    fn test_take_nonexistent() {
        let store = TransactionStore::new();
        assert!(store.take("nope").is_none());
    }

    #[test]
    fn test_count() {
        let store = TransactionStore::new();
        assert_eq!(store.count(), 0);
        store.begin("a");
        store.begin("b");
        assert_eq!(store.count(), 2);
        store.rollback("a");
        assert_eq!(store.count(), 1);
    }
}
