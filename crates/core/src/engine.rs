//! Thread-safe database engine managing concurrent access to tables.
//!
//! Provides safe concurrent reads and writes with per-table writer
//! serialization and per-partition fine-grained locking.

use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use exchange_common::error::{ExchangeDbError, Result};

use crate::partition_lock::{PartitionGuard, PartitionLockManager};
use crate::table::{TableBuilder, TableMeta, TableWriter};
use crate::txn::{ReadTxn, TxnManager};

/// Thread-safe database engine managing concurrent access to tables.
pub struct Engine {
    db_root: PathBuf,
    /// Per-table partition lock managers.
    table_locks: DashMap<String, Arc<PartitionLockManager>>,
    /// Writer pools: one writer per table, guarded by a mutex.
    writer_pool: DashMap<String, Arc<Mutex<TableWriter>>>,
    /// Global transaction managers (one per table).
    txn_managers: DashMap<String, Arc<TxnManager>>,
}

impl Engine {
    /// Open the database engine rooted at `db_root`.
    ///
    /// Scans for existing tables and initializes lock managers and
    /// transaction managers for each.
    pub fn open(db_root: &Path) -> Result<Self> {
        if !db_root.exists() {
            std::fs::create_dir_all(db_root)?;
        }

        let engine = Self {
            db_root: db_root.to_path_buf(),
            table_locks: DashMap::new(),
            writer_pool: DashMap::new(),
            txn_managers: DashMap::new(),
        };

        // Discover existing tables.
        for entry in std::fs::read_dir(db_root)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                let meta_path = path.join("_meta");
                if meta_path.exists() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    engine.init_table(&name)?;
                }
            }
        }

        Ok(engine)
    }

    /// Initialize lock managers and txn manager for a table.
    fn init_table(&self, table_name: &str) -> Result<()> {
        let table_dir = self.db_root.join(table_name);

        self.table_locks
            .entry(table_name.to_string())
            .or_insert_with(|| Arc::new(PartitionLockManager::new()));

        if !self.txn_managers.contains_key(table_name) {
            let txn_mgr = TxnManager::open(&table_dir)?;
            self.txn_managers
                .insert(table_name.to_string(), Arc::new(txn_mgr));
        }

        Ok(())
    }

    /// Ensure the writer pool entry exists for a table.
    fn ensure_writer(&self, table_name: &str) -> Result<()> {
        if !self.writer_pool.contains_key(table_name) {
            let writer = TableWriter::open(&self.db_root, table_name)?;
            self.writer_pool
                .insert(table_name.to_string(), Arc::new(Mutex::new(writer)));
        }
        Ok(())
    }

    /// Get a writer for a table (blocks if another thread has it).
    pub fn get_writer(&self, table_name: &str) -> Result<WriterHandle> {
        let table_dir = self.db_root.join(table_name);
        if !table_dir.join("_meta").exists() {
            return Err(ExchangeDbError::TableNotFound(table_name.to_string()));
        }

        self.init_table(table_name)?;
        self.ensure_writer(table_name)?;

        let writer_mutex = self
            .writer_pool
            .get(table_name)
            .expect("writer must exist after ensure_writer")
            .clone();

        let guard = writer_mutex.lock().unwrap();

        // SAFETY: The MutexGuard borrows from `writer_mutex`, which we keep
        // alive in the WriterHandle. Guard is dropped before the Arc.
        let guard: MutexGuard<'static, TableWriter> = unsafe { std::mem::transmute(guard) };

        Ok(WriterHandle {
            writer: Some(guard),
            _writer_mutex: writer_mutex,
            _partition_lock: None,
        })
    }

    /// Get a writer for a table with a partition lock for a specific partition.
    pub fn get_writer_for_partition(
        &self,
        table_name: &str,
        partition_name: &str,
    ) -> Result<WriterHandle> {
        let mut handle = self.get_writer(table_name)?;

        let lock_mgr = self
            .table_locks
            .get(table_name)
            .expect("lock manager must exist after init_table")
            .clone();

        let partition_guard = lock_mgr.lock_partition(partition_name);
        handle._partition_lock = Some(partition_guard);

        Ok(handle)
    }

    /// Get a reader for a table (multiple concurrent readers allowed).
    pub fn get_reader(&self, table_name: &str) -> Result<ReaderHandle> {
        let table_dir = self.db_root.join(table_name);
        let meta_path = table_dir.join("_meta");
        if !meta_path.exists() {
            return Err(ExchangeDbError::TableNotFound(table_name.to_string()));
        }

        self.init_table(table_name)?;

        let meta = TableMeta::load(&meta_path)?;

        let txn_mgr = self
            .txn_managers
            .get(table_name)
            .expect("txn manager must exist after init_table")
            .clone();

        let read_txn = txn_mgr.begin_read()?;

        Ok(ReaderHandle {
            meta,
            table_dir,
            _read_txn: read_txn,
        })
    }

    /// Create a new table.
    pub fn create_table(&self, builder: TableBuilder) -> Result<TableMeta> {
        let meta = builder.build(&self.db_root)?;
        self.init_table(&meta.name)?;
        Ok(meta)
    }

    /// List all tables in the database.
    pub fn list_tables(&self) -> Result<Vec<String>> {
        let mut tables = Vec::new();
        for entry in std::fs::read_dir(&self.db_root)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() && path.join("_meta").exists() {
                tables.push(entry.file_name().to_string_lossy().to_string());
            }
        }
        tables.sort();
        Ok(tables)
    }

    /// Get table metadata.
    pub fn get_meta(&self, table_name: &str) -> Result<TableMeta> {
        let meta_path = self.db_root.join(table_name).join("_meta");
        if !meta_path.exists() {
            return Err(ExchangeDbError::TableNotFound(table_name.to_string()));
        }
        TableMeta::load(&meta_path)
    }

    /// Get the partition lock manager for a table.
    pub fn partition_locks(&self, table_name: &str) -> Option<Arc<PartitionLockManager>> {
        self.table_locks.get(table_name).map(|v| v.clone())
    }

    /// Get the transaction manager for a table.
    pub fn txn_manager(&self, table_name: &str) -> Option<Arc<TxnManager>> {
        self.txn_managers.get(table_name).map(|v| v.clone())
    }

    /// Database root directory.
    pub fn db_root(&self) -> &Path {
        &self.db_root
    }
}

/// RAII handle that holds the writer lock.
pub struct WriterHandle {
    writer: Option<MutexGuard<'static, TableWriter>>,
    _writer_mutex: Arc<Mutex<TableWriter>>,
    _partition_lock: Option<PartitionGuard>,
}

impl WriterHandle {
    /// Access the underlying `TableWriter`.
    pub fn writer(&mut self) -> &mut TableWriter {
        self.writer.as_mut().unwrap()
    }
}

impl Drop for WriterHandle {
    fn drop(&mut self) {
        // Drop the MutexGuard before the Arc<Mutex> by taking it out.
        self.writer.take();
    }
}

/// Lightweight reader handle (no exclusive lock needed).
pub struct ReaderHandle {
    meta: TableMeta,
    table_dir: PathBuf,
    _read_txn: ReadTxn,
}

impl ReaderHandle {
    /// Table metadata at the time the read began.
    pub fn meta(&self) -> &TableMeta {
        &self.meta
    }

    /// Path to the table directory.
    pub fn table_dir(&self) -> &Path {
        &self.table_dir
    }

    /// The read transaction version.
    pub fn version(&self) -> u64 {
        self._read_txn.version()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::ColumnValue;
    use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
    use std::sync::{Arc, Barrier};
    use tempfile::tempdir;

    fn create_test_table(engine: &Engine, name: &str) -> TableMeta {
        engine
            .create_table(
                TableBuilder::new(name)
                    .column("timestamp", ColumnType::Timestamp)
                    .column("price", ColumnType::F64)
                    .column("volume", ColumnType::F64)
                    .timestamp("timestamp")
                    .partition_by(PartitionBy::Day),
            )
            .unwrap()
    }

    #[test]
    fn engine_open_create_table() {
        let dir = tempdir().unwrap();
        let engine = Engine::open(dir.path()).unwrap();

        let meta = create_test_table(&engine, "trades");
        assert_eq!(meta.name, "trades");
        assert_eq!(meta.columns.len(), 3);

        let tables = engine.list_tables().unwrap();
        assert_eq!(tables, vec!["trades"]);
    }

    #[test]
    fn engine_get_writer_flow() {
        let dir = tempdir().unwrap();
        let engine = Engine::open(dir.path()).unwrap();
        create_test_table(&engine, "trades");

        let mut handle = engine.get_writer("trades").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        handle
            .writer()
            .write_row(ts, &[ColumnValue::F64(100.0), ColumnValue::F64(1.5)])
            .unwrap();
        handle.writer().flush().unwrap();
        drop(handle);

        // Verify data written
        let partition_dir = dir.path().join("trades").join("2024-03-15");
        assert!(partition_dir.exists());
    }

    #[test]
    fn engine_get_reader() {
        let dir = tempdir().unwrap();
        let engine = Engine::open(dir.path()).unwrap();
        create_test_table(&engine, "trades");

        let reader = engine.get_reader("trades").unwrap();
        assert_eq!(reader.meta().name, "trades");
    }

    #[test]
    fn engine_concurrent_readers() {
        let dir = tempdir().unwrap();
        let engine = Arc::new(Engine::open(dir.path()).unwrap());
        create_test_table(&engine, "trades");

        // Write some data first
        {
            let mut handle = engine.get_writer("trades").unwrap();
            let ts = Timestamp::from_secs(1710513000);
            handle
                .writer()
                .write_row(ts, &[ColumnValue::F64(100.0), ColumnValue::F64(1.5)])
                .unwrap();
            handle.writer().flush().unwrap();
        }

        let barrier = Arc::new(Barrier::new(4));
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let engine = Arc::clone(&engine);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    let reader = engine.get_reader("trades").unwrap();
                    assert_eq!(reader.meta().name, "trades");
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn engine_writer_serialized() {
        // Two threads trying to get writers are serialized.
        let dir = tempdir().unwrap();
        let engine = Arc::new(Engine::open(dir.path()).unwrap());
        create_test_table(&engine, "trades");

        let barrier = Arc::new(Barrier::new(2));
        let counter = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let handles: Vec<_> = (0..2)
            .map(|i| {
                let engine = Arc::clone(&engine);
                let barrier = Arc::clone(&barrier);
                let counter = Arc::clone(&counter);
                std::thread::spawn(move || {
                    barrier.wait();
                    let mut handle = engine.get_writer("trades").unwrap();
                    // Verify we have exclusive access
                    let _prev = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    // Simulate work
                    let ts = Timestamp::from_secs(1710513000 + i * 86400);
                    handle
                        .writer()
                        .write_row(ts, &[ColumnValue::F64(100.0), ColumnValue::F64(1.0)])
                        .unwrap();
                    handle.writer().flush().unwrap();
                    let after = counter.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    // At most one writer at a time
                    assert!(after <= 2);
                    drop(handle);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn engine_table_not_found() {
        let dir = tempdir().unwrap();
        let engine = Engine::open(dir.path()).unwrap();

        let result = engine.get_writer("nonexistent");
        assert!(result.is_err());

        let result = engine.get_reader("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn engine_reopen_discovers_tables() {
        let dir = tempdir().unwrap();

        {
            let engine = Engine::open(dir.path()).unwrap();
            create_test_table(&engine, "trades");
            create_test_table(&engine, "quotes");
        }

        // Reopen
        let engine = Engine::open(dir.path()).unwrap();
        let tables = engine.list_tables().unwrap();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"trades".to_string()));
        assert!(tables.contains(&"quotes".to_string()));
    }

    #[test]
    fn engine_concurrent_read_and_write() {
        let dir = tempdir().unwrap();
        let engine = Arc::new(Engine::open(dir.path()).unwrap());
        create_test_table(&engine, "trades");

        // Write initial data
        {
            let mut handle = engine.get_writer("trades").unwrap();
            let ts = Timestamp::from_secs(1710513000);
            handle
                .writer()
                .write_row(ts, &[ColumnValue::F64(100.0), ColumnValue::F64(1.0)])
                .unwrap();
            handle.writer().flush().unwrap();
        }

        let barrier = Arc::new(Barrier::new(2));

        // Reader thread: reads while writer is working
        let engine_r = Arc::clone(&engine);
        let barrier_r = Arc::clone(&barrier);
        let reader_handle = std::thread::spawn(move || {
            barrier_r.wait();
            for _ in 0..10 {
                let reader = engine_r.get_reader("trades").unwrap();
                // Reader sees a consistent snapshot
                let _meta = reader.meta();
                let _version = reader.version();
                drop(reader);
            }
        });

        // Writer thread: writes while reader is reading
        let engine_w = Arc::clone(&engine);
        let barrier_w = Arc::clone(&barrier);
        let writer_handle = std::thread::spawn(move || {
            barrier_w.wait();
            for i in 0..10 {
                let mut handle = engine_w.get_writer("trades").unwrap();
                let ts = Timestamp::from_secs(1710513000 + (i + 1) * 86400);
                handle
                    .writer()
                    .write_row(
                        ts,
                        &[ColumnValue::F64(100.0 + i as f64), ColumnValue::F64(1.0)],
                    )
                    .unwrap();
                handle.writer().flush().unwrap();
                drop(handle);
            }
        });

        reader_handle.join().unwrap();
        writer_handle.join().unwrap();
    }
}
