//! MVCC (Multi-Version Concurrency Control) transaction manager for Horizon DB.
//!
//! This module provides snapshot isolation by tracking transaction lifecycles
//! and maintaining version metadata alongside each row. The core idea is that
//! every row carries a "created by" and "deleted by" transaction ID so that
//! each transaction sees a consistent snapshot of the database as of the
//! moment it began.
//!
//! # Key types
//!
//! - [`TransactionManager`]: Allocates transaction IDs, tracks active
//!   transactions, and provides begin / commit / rollback operations.
//! - [`Transaction`]: Represents a single in-flight transaction with its
//!   snapshot of active transactions at start time.
//! - [`RowVersion`]: Per-row version metadata that is serialized into the
//!   B+Tree value alongside the actual row data.
//!
//! # Visibility rules
//!
//! A row version is visible to transaction T if:
//! 1. The row was created by T itself, **or** the creating transaction
//!    committed before T started and was **not** in T's active set.
//! 2. The row has **not** been deleted, **or** the deleting transaction's
//!    writes are not visible to T (by the same rules).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use crate::error::{HorizonError, Result};
use crate::pager::PageId;

/// Transaction isolation level.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IsolationLevel {
    /// Each statement sees the latest committed data at the time the
    /// statement executes.
    ReadCommitted,
    /// The transaction sees a consistent snapshot as of the moment it began.
    Snapshot,
    /// Full serializability — detects and prevents all anomalies.
    Serializable,
}

/// Transaction state.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TxnState {
    /// The transaction is currently executing.
    Active,
    /// The transaction has been successfully committed.
    Committed,
    /// The transaction has been rolled back / aborted.
    Aborted,
}

/// A transaction ID.
pub type TxnId = u64;

/// Represents a single transaction.
#[derive(Debug)]
pub struct Transaction {
    /// Unique, monotonically increasing identifier.
    pub id: TxnId,
    /// Current lifecycle state.
    pub state: TxnState,
    /// Isolation level chosen at begin time.
    pub isolation: IsolationLevel,
    /// Set of transaction IDs that were active when this transaction began.
    /// Used for snapshot isolation — these transactions' writes are invisible.
    pub active_at_start: HashSet<TxnId>,
}

impl Transaction {
    /// Check if a given `writer_txn_id`'s writes are visible to this
    /// transaction.
    ///
    /// A write is visible if:
    /// - The writer is this transaction itself (own writes are always
    ///   visible), **or**
    /// - The writer committed before this transaction started **and** was
    ///   not in the active set when this transaction began.
    pub fn can_see(&self, writer_txn_id: TxnId) -> bool {
        if writer_txn_id == self.id {
            return true; // can always see own writes
        }
        // A write is visible if the writer committed before this transaction started
        // and the writer was NOT in the active set when this transaction started
        !self.active_at_start.contains(&writer_txn_id) && writer_txn_id < self.id
    }
}

/// Row version metadata stored alongside each row in the B+Tree value.
///
/// Every row in the database is wrapped in a `RowVersion` that records which
/// transaction created and (optionally) deleted it. The actual row payload
/// lives in [`data`](Self::data).
#[derive(Debug, Clone)]
pub struct RowVersion {
    /// Transaction that created this version.
    pub created_by: TxnId,
    /// Transaction that deleted this version (`0` means not deleted).
    pub deleted_by: TxnId,
    /// The actual row data (serialized column values).
    pub data: Vec<u8>,
}

impl RowVersion {
    /// Serialize this version into a byte buffer.
    ///
    /// Layout (all integers big-endian):
    /// ```text
    /// [created_by: 8 bytes][deleted_by: 8 bytes][data_len: 4 bytes][data: N bytes]
    /// ```
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16 + self.data.len());
        buf.extend_from_slice(&self.created_by.to_be_bytes());
        buf.extend_from_slice(&self.deleted_by.to_be_bytes());
        buf.extend_from_slice(&(self.data.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.data);
        buf
    }

    /// Deserialize a `RowVersion` from the front of `data`.
    ///
    /// Returns the deserialized version together with the number of bytes
    /// consumed so the caller can advance past it in a contiguous buffer.
    ///
    /// # Errors
    ///
    /// Returns [`HorizonError::CorruptDatabase`] if the buffer is too short
    /// or truncated.
    pub fn deserialize(data: &[u8]) -> Result<(Self, usize)> {
        if data.len() < 20 {
            return Err(HorizonError::CorruptDatabase("row version too short".into()));
        }
        let created_by = u64::from_be_bytes(data[0..8].try_into().unwrap());
        let deleted_by = u64::from_be_bytes(data[8..16].try_into().unwrap());
        let data_len = u32::from_be_bytes(data[16..20].try_into().unwrap()) as usize;
        if data.len() < 20 + data_len {
            return Err(HorizonError::CorruptDatabase("row version data truncated".into()));
        }
        let row_data = data[20..20 + data_len].to_vec();
        Ok((RowVersion { created_by, deleted_by, data: row_data }, 20 + data_len))
    }

    /// Check if this version is visible to the given transaction.
    ///
    /// A version is visible when the creating transaction's writes can be
    /// seen **and** (the version is not deleted, or the deleting
    /// transaction's writes cannot be seen).
    pub fn is_visible_to(&self, txn: &Transaction) -> bool {
        let created_visible = txn.can_see(self.created_by);
        let not_deleted = self.deleted_by == 0 || !txn.can_see(self.deleted_by);
        created_visible && not_deleted
    }
}

/// An undo log entry that records what to do to reverse a mutation.
#[derive(Debug, Clone)]
pub enum UndoEntry {
    /// A key was inserted into a B+Tree — to undo, delete it.
    Insert {
        /// The table name this entry belongs to.
        table: String,
        /// The root page of the B+Tree at the time of the insert.
        root_page: PageId,
        /// The key that was inserted.
        key: Vec<u8>,
    },
    /// A key was deleted from a B+Tree — to undo, re-insert it.
    Delete {
        /// The table name this entry belongs to.
        table: String,
        /// The root page of the B+Tree at the time of the delete.
        root_page: PageId,
        /// The key that was deleted.
        key: Vec<u8>,
        /// The value that was stored before deletion.
        old_value: Vec<u8>,
    },
    /// A key was updated in a B+Tree — to undo, restore old value.
    Update {
        /// The table name this entry belongs to.
        table: String,
        /// The root page of the B+Tree at the time of the update.
        root_page: PageId,
        /// The key that was updated.
        key: Vec<u8>,
        /// The old value before the update.
        old_value: Vec<u8>,
    },
}

/// The transaction manager tracks active transactions and provides MVCC
/// semantics.
///
/// It is the single source of truth for transaction IDs and states. Higher
/// layers consult it to decide whether a particular row version should be
/// visible to a given transaction.
pub struct TransactionManager {
    /// Monotonically increasing counter used to mint new transaction IDs.
    next_txn_id: AtomicU64,
    /// Mapping from transaction ID to its current state.
    active_txns: HashMap<TxnId, TxnState>,
    /// Whether there is an explicit user transaction currently in progress.
    user_txn_active: bool,
    /// Undo log for the current explicit user transaction.
    /// Entries are appended during mutations and replayed in reverse on ROLLBACK.
    undo_log: Vec<UndoEntry>,
}

impl TransactionManager {
    /// Create a new, empty transaction manager.
    ///
    /// The first transaction ID issued will be `1`.
    pub fn new() -> Self {
        TransactionManager {
            next_txn_id: AtomicU64::new(1),
            active_txns: HashMap::new(),
            user_txn_active: false,
            undo_log: Vec::new(),
        }
    }

    /// Begin a new transaction with the default [`IsolationLevel::Snapshot`].
    pub fn begin(&mut self) -> Transaction {
        self.begin_with_isolation(IsolationLevel::Snapshot)
    }

    /// Begin a new transaction with a specific isolation level.
    ///
    /// The returned [`Transaction`] captures a snapshot of all currently
    /// active transaction IDs so that it can later determine visibility.
    pub fn begin_with_isolation(&mut self, isolation: IsolationLevel) -> Transaction {
        let id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let active_at_start: HashSet<TxnId> = self.active_txns
            .iter()
            .filter(|(_, state)| **state == TxnState::Active)
            .map(|(id, _)| *id)
            .collect();
        self.active_txns.insert(id, TxnState::Active);
        Transaction { id, state: TxnState::Active, isolation, active_at_start }
    }

    /// Commit an active transaction.
    ///
    /// # Errors
    ///
    /// Returns [`HorizonError::TransactionError`] if the transaction is not
    /// in the [`TxnState::Active`] state.
    pub fn commit(&mut self, txn: &mut Transaction) -> Result<()> {
        if txn.state != TxnState::Active {
            return Err(HorizonError::TransactionError("transaction not active".into()));
        }
        txn.state = TxnState::Committed;
        self.active_txns.insert(txn.id, TxnState::Committed);
        Ok(())
    }

    /// Roll back (abort) an active transaction.
    ///
    /// # Errors
    ///
    /// Returns [`HorizonError::TransactionError`] if the transaction is not
    /// in the [`TxnState::Active`] state.
    pub fn rollback(&mut self, txn: &mut Transaction) -> Result<()> {
        if txn.state != TxnState::Active {
            return Err(HorizonError::TransactionError("transaction not active".into()));
        }
        txn.state = TxnState::Aborted;
        self.active_txns.insert(txn.id, TxnState::Aborted);
        Ok(())
    }

    /// Check whether the given transaction ID has been committed.
    pub fn is_committed(&self, txn_id: TxnId) -> bool {
        self.active_txns.get(&txn_id) == Some(&TxnState::Committed)
    }

    /// Get the next transaction ID that would be issued, without actually
    /// starting a transaction. Useful for auto-commit operations.
    pub fn next_id(&self) -> TxnId {
        self.next_txn_id.load(Ordering::SeqCst)
    }

    /// Auto-commit: atomically allocate a transaction ID and immediately
    /// mark it as committed. Returns the allocated ID.
    ///
    /// This is a convenience for single-statement implicit transactions
    /// that do not need snapshot isolation.
    pub fn auto_commit(&mut self) -> TxnId {
        let id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        self.active_txns.insert(id, TxnState::Committed);
        id
    }

    /// Begin an explicit user transaction. Returns an error if one is
    /// already active.
    pub fn begin_user_txn(&mut self) -> Result<()> {
        if self.user_txn_active {
            return Err(HorizonError::TransactionError(
                "a transaction is already active".into(),
            ));
        }
        self.user_txn_active = true;
        self.undo_log.clear();
        Ok(())
    }

    /// Commit the current explicit user transaction. The undo log is
    /// discarded because changes are now permanent.
    pub fn commit_user_txn(&mut self) -> Result<()> {
        if !self.user_txn_active {
            return Err(HorizonError::TransactionError(
                "no active transaction to commit".into(),
            ));
        }
        self.user_txn_active = false;
        self.undo_log.clear();
        Ok(())
    }

    /// Roll back the current explicit user transaction. Returns the undo
    /// log entries so the caller can reverse the mutations.
    pub fn rollback_user_txn(&mut self) -> Result<Vec<UndoEntry>> {
        if !self.user_txn_active {
            return Err(HorizonError::TransactionError(
                "no active transaction to rollback".into(),
            ));
        }
        self.user_txn_active = false;
        // Return entries in reverse order for proper undo
        let mut entries = std::mem::take(&mut self.undo_log);
        entries.reverse();
        Ok(entries)
    }

    /// Check whether an explicit user transaction is currently active.
    pub fn is_user_txn_active(&self) -> bool {
        self.user_txn_active
    }

    /// Record an undo entry for the current explicit user transaction.
    /// If no user transaction is active this is a no-op (auto-commit mode).
    pub fn record_undo(&mut self, entry: UndoEntry) {
        if self.user_txn_active {
            self.undo_log.push(entry);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // =====================================================================
    // Transaction visibility tests
    // =====================================================================

    #[test]
    fn transaction_can_see_own_writes() {
        let txn = Transaction {
            id: 5,
            state: TxnState::Active,
            isolation: IsolationLevel::Snapshot,
            active_at_start: HashSet::new(),
        };
        assert!(txn.can_see(5));
    }

    #[test]
    fn transaction_can_see_older_committed_writes() {
        // Transaction 5 started when no other transactions were active.
        // Transactions 1..4 committed before txn 5 began.
        let txn = Transaction {
            id: 5,
            state: TxnState::Active,
            isolation: IsolationLevel::Snapshot,
            active_at_start: HashSet::new(),
        };
        assert!(txn.can_see(1));
        assert!(txn.can_see(4));
    }

    #[test]
    fn transaction_cannot_see_concurrent_active_writes() {
        // Transaction 5 started while transaction 3 was still active.
        let mut active = HashSet::new();
        active.insert(3);
        let txn = Transaction {
            id: 5,
            state: TxnState::Active,
            isolation: IsolationLevel::Snapshot,
            active_at_start: active,
        };
        // Txn 3 was active when txn 5 started — writes are invisible.
        assert!(!txn.can_see(3));
        // Txn 2 was not in the active set and has a lower ID — visible.
        assert!(txn.can_see(2));
    }

    #[test]
    fn transaction_cannot_see_future_writes() {
        let txn = Transaction {
            id: 5,
            state: TxnState::Active,
            isolation: IsolationLevel::Snapshot,
            active_at_start: HashSet::new(),
        };
        // Transaction 6 started after txn 5 — invisible.
        assert!(!txn.can_see(6));
        assert!(!txn.can_see(100));
    }

    #[test]
    fn transaction_cannot_see_write_from_active_set_even_if_lower_id() {
        let mut active = HashSet::new();
        active.insert(2);
        active.insert(4);
        let txn = Transaction {
            id: 5,
            state: TxnState::Active,
            isolation: IsolationLevel::Snapshot,
            active_at_start: active,
        };
        assert!(!txn.can_see(2));
        assert!(txn.can_see(3));
        assert!(!txn.can_see(4));
    }

    #[test]
    fn transaction_visibility_with_empty_active_set() {
        let txn = Transaction {
            id: 10,
            state: TxnState::Active,
            isolation: IsolationLevel::Snapshot,
            active_at_start: HashSet::new(),
        };
        // All transactions with lower IDs are visible.
        for i in 1..10 {
            assert!(txn.can_see(i), "should see txn {}", i);
        }
        // Equal ID (own writes) is visible.
        assert!(txn.can_see(10));
        // Higher IDs are not visible.
        assert!(!txn.can_see(11));
    }

    #[test]
    fn transaction_id_1_sees_only_own_writes() {
        let txn = Transaction {
            id: 1,
            state: TxnState::Active,
            isolation: IsolationLevel::Snapshot,
            active_at_start: HashSet::new(),
        };
        assert!(txn.can_see(1));
        // ID 0 is lower and not in active set, so it is visible per MVCC rules.
        assert!(txn.can_see(0));
        // Higher IDs are not visible.
        assert!(!txn.can_see(2));
    }

    #[test]
    fn can_see_id_zero_if_not_in_active_set() {
        // Edge case: transaction ID 0 is generally unused, but test the
        // logic. For txn 5 with empty active set, 0 < 5 and not in set → visible.
        let txn = Transaction {
            id: 5,
            state: TxnState::Active,
            isolation: IsolationLevel::Snapshot,
            active_at_start: HashSet::new(),
        };
        assert!(txn.can_see(0));
    }

    // =====================================================================
    // RowVersion serialization tests
    // =====================================================================

    #[test]
    fn row_version_serialize_round_trip_empty_data() {
        let rv = RowVersion {
            created_by: 1,
            deleted_by: 0,
            data: vec![],
        };
        let bytes = rv.serialize();
        assert_eq!(bytes.len(), 20); // 8 + 8 + 4 + 0
        let (decoded, consumed) = RowVersion::deserialize(&bytes).unwrap();
        assert_eq!(consumed, 20);
        assert_eq!(decoded.created_by, 1);
        assert_eq!(decoded.deleted_by, 0);
        assert!(decoded.data.is_empty());
    }

    #[test]
    fn row_version_serialize_round_trip_with_data() {
        let rv = RowVersion {
            created_by: 42,
            deleted_by: 99,
            data: vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03],
        };
        let bytes = rv.serialize();
        assert_eq!(bytes.len(), 20 + 7);
        let (decoded, consumed) = RowVersion::deserialize(&bytes).unwrap();
        assert_eq!(consumed, 27);
        assert_eq!(decoded.created_by, 42);
        assert_eq!(decoded.deleted_by, 99);
        assert_eq!(decoded.data, vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03]);
    }

    #[test]
    fn row_version_serialize_round_trip_large_ids() {
        let rv = RowVersion {
            created_by: u64::MAX,
            deleted_by: u64::MAX - 1,
            data: vec![1, 2, 3],
        };
        let bytes = rv.serialize();
        let (decoded, _) = RowVersion::deserialize(&bytes).unwrap();
        assert_eq!(decoded.created_by, u64::MAX);
        assert_eq!(decoded.deleted_by, u64::MAX - 1);
        assert_eq!(decoded.data, vec![1, 2, 3]);
    }

    #[test]
    fn row_version_deserialize_too_short() {
        let data = vec![0u8; 19]; // needs at least 20 bytes
        assert!(RowVersion::deserialize(&data).is_err());
    }

    #[test]
    fn row_version_deserialize_truncated_data() {
        let rv = RowVersion {
            created_by: 1,
            deleted_by: 0,
            data: vec![1, 2, 3, 4, 5],
        };
        let mut bytes = rv.serialize();
        // Truncate the last 2 bytes of payload
        bytes.truncate(bytes.len() - 2);
        assert!(RowVersion::deserialize(&bytes).is_err());
    }

    #[test]
    fn row_version_deserialize_empty_input() {
        assert!(RowVersion::deserialize(&[]).is_err());
    }

    #[test]
    fn row_version_deserialize_consumes_correct_amount() {
        // Put two row versions back to back and verify we consume only the first.
        let rv1 = RowVersion { created_by: 1, deleted_by: 0, data: vec![10, 20] };
        let rv2 = RowVersion { created_by: 2, deleted_by: 0, data: vec![30, 40, 50] };
        let mut buf = rv1.serialize();
        buf.extend_from_slice(&rv2.serialize());

        let (decoded1, consumed1) = RowVersion::deserialize(&buf).unwrap();
        assert_eq!(decoded1.created_by, 1);
        assert_eq!(decoded1.data, vec![10, 20]);

        let (decoded2, consumed2) = RowVersion::deserialize(&buf[consumed1..]).unwrap();
        assert_eq!(decoded2.created_by, 2);
        assert_eq!(decoded2.data, vec![30, 40, 50]);
        assert_eq!(consumed1 + consumed2, buf.len());
    }

    // =====================================================================
    // RowVersion visibility tests
    // =====================================================================

    #[test]
    fn row_version_visible_own_write_not_deleted() {
        let txn = Transaction {
            id: 5,
            state: TxnState::Active,
            isolation: IsolationLevel::Snapshot,
            active_at_start: HashSet::new(),
        };
        let rv = RowVersion { created_by: 5, deleted_by: 0, data: vec![] };
        assert!(rv.is_visible_to(&txn));
    }

    #[test]
    fn row_version_visible_committed_write_not_deleted() {
        let txn = Transaction {
            id: 5,
            state: TxnState::Active,
            isolation: IsolationLevel::Snapshot,
            active_at_start: HashSet::new(),
        };
        let rv = RowVersion { created_by: 3, deleted_by: 0, data: vec![] };
        assert!(rv.is_visible_to(&txn));
    }

    #[test]
    fn row_version_invisible_if_created_by_concurrent() {
        let mut active = HashSet::new();
        active.insert(3);
        let txn = Transaction {
            id: 5,
            state: TxnState::Active,
            isolation: IsolationLevel::Snapshot,
            active_at_start: active,
        };
        let rv = RowVersion { created_by: 3, deleted_by: 0, data: vec![] };
        assert!(!rv.is_visible_to(&txn));
    }

    #[test]
    fn row_version_invisible_if_created_in_future() {
        let txn = Transaction {
            id: 5,
            state: TxnState::Active,
            isolation: IsolationLevel::Snapshot,
            active_at_start: HashSet::new(),
        };
        let rv = RowVersion { created_by: 7, deleted_by: 0, data: vec![] };
        assert!(!rv.is_visible_to(&txn));
    }

    #[test]
    fn row_version_invisible_if_deleted_by_visible_txn() {
        let txn = Transaction {
            id: 5,
            state: TxnState::Active,
            isolation: IsolationLevel::Snapshot,
            active_at_start: HashSet::new(),
        };
        // Created by txn 2 (visible), deleted by txn 3 (also visible).
        let rv = RowVersion { created_by: 2, deleted_by: 3, data: vec![] };
        assert!(!rv.is_visible_to(&txn));
    }

    #[test]
    fn row_version_visible_if_deleted_by_concurrent_txn() {
        let mut active = HashSet::new();
        active.insert(4);
        let txn = Transaction {
            id: 5,
            state: TxnState::Active,
            isolation: IsolationLevel::Snapshot,
            active_at_start: active,
        };
        // Created by txn 2 (visible), deleted by txn 4 (in active set — invisible).
        // So the delete is not visible, meaning the row IS visible.
        let rv = RowVersion { created_by: 2, deleted_by: 4, data: vec![] };
        assert!(rv.is_visible_to(&txn));
    }

    #[test]
    fn row_version_visible_if_deleted_by_future_txn() {
        let txn = Transaction {
            id: 5,
            state: TxnState::Active,
            isolation: IsolationLevel::Snapshot,
            active_at_start: HashSet::new(),
        };
        // Created by txn 2 (visible), deleted by txn 8 (future — invisible).
        let rv = RowVersion { created_by: 2, deleted_by: 8, data: vec![] };
        assert!(rv.is_visible_to(&txn));
    }

    #[test]
    fn row_version_visible_if_deleted_by_self() {
        let txn = Transaction {
            id: 5,
            state: TxnState::Active,
            isolation: IsolationLevel::Snapshot,
            active_at_start: HashSet::new(),
        };
        // Created by txn 2, deleted by txn 5 (self). Self can see own
        // deletes, so row should be INVISIBLE.
        let rv = RowVersion { created_by: 2, deleted_by: 5, data: vec![] };
        assert!(!rv.is_visible_to(&txn));
    }

    #[test]
    fn row_version_own_create_and_own_delete() {
        let txn = Transaction {
            id: 5,
            state: TxnState::Active,
            isolation: IsolationLevel::Snapshot,
            active_at_start: HashSet::new(),
        };
        // Created and deleted by self — invisible (deleted).
        let rv = RowVersion { created_by: 5, deleted_by: 5, data: vec![] };
        assert!(!rv.is_visible_to(&txn));
    }

    // =====================================================================
    // TransactionManager tests
    // =====================================================================

    #[test]
    fn new_manager_starts_at_id_1() {
        let mgr = TransactionManager::new();
        assert_eq!(mgr.next_id(), 1);
    }

    #[test]
    fn begin_allocates_sequential_ids() {
        let mut mgr = TransactionManager::new();
        let t1 = mgr.begin();
        let t2 = mgr.begin();
        let t3 = mgr.begin();
        assert_eq!(t1.id, 1);
        assert_eq!(t2.id, 2);
        assert_eq!(t3.id, 3);
    }

    #[test]
    fn begin_captures_active_set() {
        let mut mgr = TransactionManager::new();
        let _t1 = mgr.begin(); // id = 1, active
        let _t2 = mgr.begin(); // id = 2, active; active_at_start = {1}
        let t3 = mgr.begin();  // id = 3, active; active_at_start = {1, 2}
        assert!(t3.active_at_start.contains(&1));
        assert!(t3.active_at_start.contains(&2));
        assert_eq!(t3.active_at_start.len(), 2);
    }

    #[test]
    fn begin_uses_default_snapshot_isolation() {
        let mut mgr = TransactionManager::new();
        let txn = mgr.begin();
        assert_eq!(txn.isolation, IsolationLevel::Snapshot);
    }

    #[test]
    fn begin_with_isolation_uses_specified_level() {
        let mut mgr = TransactionManager::new();
        let txn = mgr.begin_with_isolation(IsolationLevel::ReadCommitted);
        assert_eq!(txn.isolation, IsolationLevel::ReadCommitted);

        let txn2 = mgr.begin_with_isolation(IsolationLevel::Serializable);
        assert_eq!(txn2.isolation, IsolationLevel::Serializable);
    }

    #[test]
    fn begin_sets_active_state() {
        let mut mgr = TransactionManager::new();
        let txn = mgr.begin();
        assert_eq!(txn.state, TxnState::Active);
    }

    #[test]
    fn commit_changes_state() {
        let mut mgr = TransactionManager::new();
        let mut txn = mgr.begin();
        assert_eq!(txn.state, TxnState::Active);
        mgr.commit(&mut txn).unwrap();
        assert_eq!(txn.state, TxnState::Committed);
    }

    #[test]
    fn commit_marks_as_committed_in_manager() {
        let mut mgr = TransactionManager::new();
        let mut txn = mgr.begin();
        let id = txn.id;
        assert!(!mgr.is_committed(id));
        mgr.commit(&mut txn).unwrap();
        assert!(mgr.is_committed(id));
    }

    #[test]
    fn rollback_changes_state() {
        let mut mgr = TransactionManager::new();
        let mut txn = mgr.begin();
        mgr.rollback(&mut txn).unwrap();
        assert_eq!(txn.state, TxnState::Aborted);
    }

    #[test]
    fn rollback_does_not_mark_committed() {
        let mut mgr = TransactionManager::new();
        let mut txn = mgr.begin();
        let id = txn.id;
        mgr.rollback(&mut txn).unwrap();
        assert!(!mgr.is_committed(id));
    }

    #[test]
    fn commit_already_committed_fails() {
        let mut mgr = TransactionManager::new();
        let mut txn = mgr.begin();
        mgr.commit(&mut txn).unwrap();
        let result = mgr.commit(&mut txn);
        assert!(result.is_err());
    }

    #[test]
    fn commit_aborted_transaction_fails() {
        let mut mgr = TransactionManager::new();
        let mut txn = mgr.begin();
        mgr.rollback(&mut txn).unwrap();
        let result = mgr.commit(&mut txn);
        assert!(result.is_err());
    }

    #[test]
    fn rollback_already_committed_fails() {
        let mut mgr = TransactionManager::new();
        let mut txn = mgr.begin();
        mgr.commit(&mut txn).unwrap();
        let result = mgr.rollback(&mut txn);
        assert!(result.is_err());
    }

    #[test]
    fn rollback_already_aborted_fails() {
        let mut mgr = TransactionManager::new();
        let mut txn = mgr.begin();
        mgr.rollback(&mut txn).unwrap();
        let result = mgr.rollback(&mut txn);
        assert!(result.is_err());
    }

    #[test]
    fn committed_txn_not_in_active_set_of_subsequent() {
        let mut mgr = TransactionManager::new();
        let mut t1 = mgr.begin(); // id = 1
        mgr.commit(&mut t1).unwrap();

        let t2 = mgr.begin(); // id = 2
        // t1 committed, so it should NOT be in t2's active set.
        assert!(!t2.active_at_start.contains(&1));
    }

    #[test]
    fn aborted_txn_not_in_active_set_of_subsequent() {
        let mut mgr = TransactionManager::new();
        let mut t1 = mgr.begin(); // id = 1
        mgr.rollback(&mut t1).unwrap();

        let t2 = mgr.begin(); // id = 2
        // t1 aborted, so it should NOT be in t2's active set.
        assert!(!t2.active_at_start.contains(&1));
    }

    #[test]
    fn auto_commit_returns_incremented_id() {
        let mut mgr = TransactionManager::new();
        let id1 = mgr.auto_commit();
        let id2 = mgr.auto_commit();
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
    }

    #[test]
    fn auto_commit_marks_committed_immediately() {
        let mut mgr = TransactionManager::new();
        let id = mgr.auto_commit();
        assert!(mgr.is_committed(id));
    }

    #[test]
    fn auto_commit_not_in_subsequent_active_set() {
        let mut mgr = TransactionManager::new();
        let _id = mgr.auto_commit(); // id = 1, immediately committed
        let t2 = mgr.begin(); // id = 2
        // Auto-committed txn 1 should not appear in active set.
        assert!(!t2.active_at_start.contains(&1));
    }

    #[test]
    fn is_committed_returns_false_for_unknown_id() {
        let mgr = TransactionManager::new();
        assert!(!mgr.is_committed(999));
    }

    #[test]
    fn next_id_does_not_advance_counter() {
        let mgr = TransactionManager::new();
        let id1 = mgr.next_id();
        let id2 = mgr.next_id();
        assert_eq!(id1, id2);
    }

    // =====================================================================
    // Integration-style tests: manager + visibility
    // =====================================================================

    #[test]
    fn snapshot_isolation_scenario() {
        let mut mgr = TransactionManager::new();

        // T1 begins and inserts a row (created_by = 1).
        let mut t1 = mgr.begin(); // id = 1

        // T2 begins while T1 is still active.
        let t2 = mgr.begin(); // id = 2, active_at_start = {1}

        // T1 commits.
        mgr.commit(&mut t1).unwrap();

        // T2 should NOT see T1's writes (T1 was active when T2 started).
        let rv = RowVersion { created_by: 1, deleted_by: 0, data: vec![1, 2, 3] };
        assert!(!rv.is_visible_to(&t2));

        // T3 begins after T1 committed.
        let t3 = mgr.begin(); // id = 3, active_at_start = {2}
        // T3 SHOULD see T1's writes.
        assert!(rv.is_visible_to(&t3));
        // T3 should NOT see T2's writes (T2 still active).
        let rv2 = RowVersion { created_by: 2, deleted_by: 0, data: vec![4, 5, 6] };
        assert!(!rv2.is_visible_to(&t3));
    }

    #[test]
    fn delete_visibility_scenario() {
        let mut mgr = TransactionManager::new();

        // T1 auto-commits an insert.
        let t1_id = mgr.auto_commit(); // id = 1, committed

        // T2 begins and deletes the row.
        let t2 = mgr.begin(); // id = 2

        // T3 begins while T2 is active.
        let t3 = mgr.begin(); // id = 3, active_at_start = {2}

        // Row created by T1, deleted by T2.
        let rv = RowVersion { created_by: t1_id, deleted_by: t2.id, data: vec![] };

        // T3 should still see the row because T2's delete is invisible to T3.
        assert!(rv.is_visible_to(&t3));

        // T2 itself should NOT see the row (it deleted it).
        assert!(!rv.is_visible_to(&t2));
    }

    #[test]
    fn many_concurrent_transactions() {
        let mut mgr = TransactionManager::new();

        // Start 10 transactions.
        let mut txns: Vec<Transaction> = (0..10).map(|_| mgr.begin()).collect();

        // The last transaction should have all previous 9 in its active set.
        let last = &txns[9];
        assert_eq!(last.active_at_start.len(), 9);
        for i in 0..9 {
            assert!(last.active_at_start.contains(&txns[i].id));
        }

        // Commit all and start a new one — active set should be empty.
        for txn in txns.iter_mut() {
            mgr.commit(txn).unwrap();
        }
        let fresh = mgr.begin();
        assert!(fresh.active_at_start.is_empty());
    }

    #[test]
    fn mixed_commit_and_rollback_active_set() {
        let mut mgr = TransactionManager::new();

        let mut t1 = mgr.begin(); // id = 1
        let mut t2 = mgr.begin(); // id = 2
        let _t3 = mgr.begin();    // id = 3, still active

        mgr.commit(&mut t1).unwrap();
        mgr.rollback(&mut t2).unwrap();

        // T4 starts. T1 committed, T2 aborted, T3 still active.
        let t4 = mgr.begin(); // id = 4
        assert!(!t4.active_at_start.contains(&1)); // committed
        assert!(!t4.active_at_start.contains(&2)); // aborted
        assert!(t4.active_at_start.contains(&3));  // still active
        assert_eq!(t4.active_at_start.len(), 1);
    }

    // =====================================================================
    // Isolation level enum tests
    // =====================================================================

    #[test]
    fn isolation_levels_are_comparable() {
        assert_eq!(IsolationLevel::ReadCommitted, IsolationLevel::ReadCommitted);
        assert_ne!(IsolationLevel::ReadCommitted, IsolationLevel::Snapshot);
        assert_ne!(IsolationLevel::Snapshot, IsolationLevel::Serializable);
    }

    #[test]
    fn isolation_levels_are_cloneable() {
        let level = IsolationLevel::Serializable;
        let cloned = level;
        assert_eq!(level, cloned);
    }

    // =====================================================================
    // TxnState enum tests
    // =====================================================================

    #[test]
    fn txn_states_are_comparable() {
        assert_eq!(TxnState::Active, TxnState::Active);
        assert_ne!(TxnState::Active, TxnState::Committed);
        assert_ne!(TxnState::Committed, TxnState::Aborted);
    }

    #[test]
    fn txn_states_are_debug_printable() {
        let s = format!("{:?}", TxnState::Active);
        assert_eq!(s, "Active");
        let s = format!("{:?}", TxnState::Committed);
        assert_eq!(s, "Committed");
        let s = format!("{:?}", TxnState::Aborted);
        assert_eq!(s, "Aborted");
    }
}
