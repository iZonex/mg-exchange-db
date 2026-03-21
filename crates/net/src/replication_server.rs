//! TCP replication listener for replica nodes.
//!
//! The replica starts a dedicated TCP server that accepts incoming WAL segment
//! messages from the primary. Each connection reads length-prefixed
//! `ReplicationMessage` frames, writes the segment data to the table's WAL
//! directory, runs a merge to apply the WAL to column files, and sends an
//! Ack back to the primary.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use exchange_core::replication::protocol::{self, ReplicationMessage};
use exchange_core::replication::wal_receiver::WalReceiver;

/// Start the replication TCP server on the given address.
///
/// Accepts connections from the primary and processes incoming WAL segment
/// messages. For each segment received, the data is written to the table's
/// WAL directory, a merge is run, and an acknowledgment is sent back.
pub async fn start_replication_server(
    addr: SocketAddr,
    db_root: PathBuf,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(addr = %addr, "replication server started");

    loop {
        let (stream, peer) = listener.accept().await?;
        let db_root = db_root.clone();
        tokio::spawn(async move {
            tracing::debug!(peer = %peer, "accepted replication connection");
            if let Err(e) = handle_replication_connection(stream, &db_root).await {
                tracing::error!(peer = %peer, error = %e, "replication connection error");
            }
        });
    }
}

/// Handle a single replication connection from the primary.
///
/// Reads length-prefixed frames, decodes them as `ReplicationMessage`,
/// and for `WalSegment` messages: writes the segment, runs merge, and
/// sends an `Ack` back.
async fn handle_replication_connection(
    mut stream: TcpStream,
    db_root: &Path,
) -> std::io::Result<()> {
    // We create a WalReceiver per connection to handle segment application.
    // The receiver manages segment file naming and merge operations.
    let mut receiver = WalReceiver::new(
        db_root.to_path_buf(),
        String::new(), // primary_addr not needed for incoming connections
    );

    loop {
        // Read the 4-byte length prefix.
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Connection closed cleanly.
                tracing::debug!("replication connection closed");
                return Ok(());
            }
            Err(e) => return Err(e),
        }
        let payload_len = u32::from_le_bytes(len_buf) as usize;

        // Sanity check: reject absurdly large frames (>256 MB).
        if payload_len > 256 * 1024 * 1024 {
            tracing::warn!(
                payload_len,
                "replication frame too large, dropping connection"
            );
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "replication frame too large",
            ));
        }

        // Read the payload.
        let mut payload = vec![0u8; payload_len];
        stream.read_exact(&mut payload).await?;

        // Reassemble the full frame for the protocol decoder.
        let mut frame = Vec::with_capacity(4 + payload_len);
        frame.extend_from_slice(&len_buf);
        frame.extend_from_slice(&payload);

        match protocol::decode(&frame) {
            Ok((msg, _)) => {
                match msg {
                    ReplicationMessage::WalSegment {
                        ref table,
                        ref data,
                        txn_range,
                        ..
                    } => {
                        let table_name = table.clone();
                        let high_txn = txn_range.1;

                        // Apply the segment (write to WAL dir + merge).
                        match receiver.apply_segment(&table_name, data) {
                            Ok(bytes) => {
                                tracing::debug!(
                                    table = %table_name,
                                    bytes,
                                    txn_range = ?txn_range,
                                    "applied WAL segment from primary"
                                );

                                // Try to merge the WAL into column files.
                                // This makes the data queryable on the replica.
                                let db_root_clone = db_root.to_path_buf();
                                let tn = table_name.clone();
                                // Run merge in a blocking task to avoid blocking the async runtime.
                                let merge_result = tokio::task::spawn_blocking(move || {
                                    try_merge_table(&db_root_clone, &tn)
                                })
                                .await;

                                match merge_result {
                                    Ok(Ok(())) => {
                                        tracing::debug!(
                                            table = %table_name,
                                            "replica merge completed"
                                        );
                                    }
                                    Ok(Err(e)) => {
                                        tracing::warn!(
                                            table = %table_name,
                                            error = %e,
                                            "replica merge failed (data still in WAL)"
                                        );
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            table = %table_name,
                                            error = %e,
                                            "replica merge task panicked"
                                        );
                                    }
                                }

                                // Send Ack back to primary.
                                let ack = ReplicationMessage::Ack {
                                    replica_id: "replica".to_string(),
                                    table: table_name,
                                    last_txn: high_txn,
                                };
                                if let Ok(ack_bytes) = protocol::encode(&ack) {
                                    let _ = stream.write_all(&ack_bytes).await;
                                    let _ = stream.flush().await;
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    table = %table_name,
                                    error = %e,
                                    "failed to apply WAL segment on replica"
                                );
                            }
                        }
                    }
                    ReplicationMessage::SchemaSync {
                        ref table,
                        ref meta_json,
                        version,
                    } => {
                        let table_name = table.clone();
                        let json = meta_json.clone();

                        let db = db_root.to_path_buf();
                        let sync_result = tokio::task::spawn_blocking(move || {
                            apply_schema_sync(&db, &table_name, &json, version)
                        })
                        .await;

                        match sync_result {
                            Ok(Ok(())) => {
                                tracing::info!(
                                    table = %table,
                                    version,
                                    "schema synced from primary"
                                );
                            }
                            Ok(Err(e)) => {
                                tracing::error!(
                                    table = %table,
                                    version,
                                    error = %e,
                                    "failed to apply schema sync"
                                );
                            }
                            Err(e) => {
                                tracing::error!(
                                    table = %table,
                                    error = %e,
                                    "schema sync task panicked"
                                );
                            }
                        }

                        // Send Ack back to primary.
                        let ack = ReplicationMessage::Ack {
                            replica_id: "replica".to_string(),
                            table: table.clone(),
                            last_txn: 0,
                        };
                        if let Ok(ack_bytes) = protocol::encode(&ack) {
                            let _ = stream.write_all(&ack_bytes).await;
                            let _ = stream.flush().await;
                        }
                    }
                    ReplicationMessage::StatusRequest => {
                        let pos = receiver.current_position();
                        let resp = ReplicationMessage::StatusResponse { position: pos };
                        if let Ok(resp_bytes) = protocol::encode(&resp) {
                            let _ = stream.write_all(&resp_bytes).await;
                            let _ = stream.flush().await;
                        }
                    }
                    other => {
                        tracing::debug!(?other, "ignoring unexpected replication message");
                    }
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to decode replication message");
            }
        }
    }
}

/// Apply a `SchemaSync` message: write the `_meta` file for a table on the
/// replica, creating the table directory if necessary.  Only updates the
/// file when the incoming version is strictly newer than what is already on
/// disk.
fn apply_schema_sync(
    db_root: &Path,
    table: &str,
    meta_json: &str,
    version: u64,
) -> Result<(), String> {
    use exchange_core::table::TableMeta;

    let table_dir = db_root.join(table);
    std::fs::create_dir_all(&table_dir).map_err(|e| e.to_string())?;

    let meta_path = table_dir.join("_meta");

    let should_update = if meta_path.exists() {
        match TableMeta::load(&meta_path) {
            Ok(existing) => version > existing.version,
            Err(_) => true, // corrupt file, overwrite
        }
    } else {
        true
    };

    if should_update {
        std::fs::write(&meta_path, meta_json).map_err(|e| e.to_string())?;

        // Ensure the _txn file exists so future merges can proceed.
        let _ = exchange_core::txn::TxnFile::open(&table_dir);

        tracing::info!(
            table = %table,
            version,
            "wrote _meta from SchemaSync"
        );
    }

    Ok(())
}

/// Try to merge WAL segments into column files for a table on the replica.
///
/// Loads the table metadata and runs `WalMergeJob`. If the metadata doesn't
/// exist yet (the `SchemaSync` message has not arrived), this is a no-op --
/// the merge will succeed on the next WAL delivery after the schema arrives.
fn try_merge_table(db_root: &Path, table: &str) -> Result<(), String> {
    use exchange_core::table::TableMeta;
    use exchange_core::wal::merge::WalMergeJob;

    let table_dir = db_root.join(table);
    let meta_path = table_dir.join("_meta");

    if !meta_path.exists() {
        tracing::debug!(
            table = %table,
            "skipping merge: table metadata not yet received via SchemaSync"
        );
        return Ok(());
    }

    let meta = TableMeta::load(&meta_path).map_err(|e| e.to_string())?;

    // Ensure the _txn file exists (WalMergeJob requires it).
    let txn_path = table_dir.join("_txn");
    if !txn_path.exists() {
        let _txn = exchange_core::txn::TxnFile::open(&table_dir).map_err(|e| e.to_string())?;
    }

    let merge_job = WalMergeJob::new(table_dir, meta);
    match merge_job.run() {
        Ok(stats) => {
            tracing::debug!(
                table = %table,
                rows_merged = stats.rows_merged,
                segments_processed = stats.segments_processed,
                "replica merge completed"
            );
            Ok(())
        }
        Err(e) => Err(e.to_string()),
    }
}
