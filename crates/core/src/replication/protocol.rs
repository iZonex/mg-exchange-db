use exchange_common::error::{ExchangeDbError, Result};
use serde::{Deserialize, Serialize};

use super::wal_receiver::ReplicaPosition;

/// Messages exchanged between primary and replica over the replication channel.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ReplicationMessage {
    /// Primary -> Replica: a WAL segment to apply.
    WalSegment {
        table: String,
        segment_id: u32,
        data: Vec<u8>,
        /// Inclusive range of transaction IDs contained in this segment.
        txn_range: (u64, u64),
    },
    /// Replica -> Primary: acknowledgment that the replica has applied up to
    /// this transaction ID for a given table.
    Ack {
        replica_id: String,
        table: String,
        last_txn: u64,
    },
    /// Primary -> Replica: request the replica's current replication position.
    StatusRequest,
    /// Replica -> Primary: current replication position.
    StatusResponse { position: ReplicaPosition },
    /// Primary -> Replica: the replica is too far behind and must re-sync
    /// from a full snapshot for the given table.
    FullSyncRequired { table: String },
    /// Primary -> Replica: synchronize the table schema (_meta) before
    /// shipping WAL segments.  Sent whenever a table is first seen by a
    /// replica or when the schema version changes (e.g. ALTER TABLE).
    SchemaSync {
        table: String,
        /// Full JSON content of the table's `_meta` file.
        meta_json: String,
        /// Monotonically increasing schema version so replicas can detect
        /// stale or duplicate deliveries.
        version: u64,
    },
}

/// Encode a `ReplicationMessage` into a length-prefixed binary frame.
///
/// Wire format:
/// ```text
/// [4 bytes: payload length (little-endian u32)]
/// [N bytes: JSON-serialized ReplicationMessage]
/// ```
pub fn encode(msg: &ReplicationMessage) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(msg)
        .map_err(|e| ExchangeDbError::Wal(format!("replication encode error: {e}")))?;

    let len = payload.len() as u32;
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&payload);
    Ok(buf)
}

/// Decode a `ReplicationMessage` from a length-prefixed binary frame.
///
/// The input `bytes` must contain the full frame (4-byte length prefix +
/// JSON payload). Returns the decoded message and the number of bytes consumed.
pub fn decode(bytes: &[u8]) -> Result<(ReplicationMessage, usize)> {
    if bytes.len() < 4 {
        return Err(ExchangeDbError::Wal(
            "replication frame too short: need at least 4 bytes for length prefix".into(),
        ));
    }

    let len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let total = 4 + len;

    if bytes.len() < total {
        return Err(ExchangeDbError::Wal(format!(
            "replication frame incomplete: expected {total} bytes, have {}",
            bytes.len()
        )));
    }

    let msg: ReplicationMessage = serde_json::from_slice(&bytes[4..total])
        .map_err(|e| ExchangeDbError::Wal(format!("replication decode error: {e}")))?;

    Ok((msg, total))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn encode_decode_wal_segment() {
        let msg = ReplicationMessage::WalSegment {
            table: "trades".into(),
            segment_id: 42,
            data: vec![0xDE, 0xAD, 0xBE, 0xEF],
            txn_range: (100, 200),
        };

        let encoded = encode(&msg).unwrap();
        let (decoded, consumed) = decode(&encoded).unwrap();

        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_ack() {
        let msg = ReplicationMessage::Ack {
            replica_id: "replica-1".into(),
            table: "orders".into(),
            last_txn: 999,
        };

        let encoded = encode(&msg).unwrap();
        let (decoded, _) = decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_status_request() {
        let msg = ReplicationMessage::StatusRequest;
        let encoded = encode(&msg).unwrap();
        let (decoded, _) = decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_status_response() {
        let mut tables = HashMap::new();
        tables.insert("trades".into(), 50u64);
        tables.insert("orders".into(), 30u64);

        let msg = ReplicationMessage::StatusResponse {
            position: ReplicaPosition {
                last_applied_txn: 50,
                tables,
            },
        };

        let encoded = encode(&msg).unwrap();
        let (decoded, _) = decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_full_sync_required() {
        let msg = ReplicationMessage::FullSyncRequired {
            table: "quotes".into(),
        };

        let encoded = encode(&msg).unwrap();
        let (decoded, _) = decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_schema_sync() {
        let msg = ReplicationMessage::SchemaSync {
            table: "trades".into(),
            meta_json: r#"{"name":"trades","columns":[],"version":3}"#.into(),
            version: 3,
        };

        let encoded = encode(&msg).unwrap();
        let (decoded, consumed) = decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, msg);
    }

    #[test]
    fn decode_too_short() {
        let result = decode(&[0x01, 0x02]);
        assert!(result.is_err());
    }

    #[test]
    fn decode_incomplete_frame() {
        // Length prefix says 100 bytes but we only have 10.
        let mut buf = Vec::new();
        buf.extend_from_slice(&100u32.to_le_bytes());
        buf.extend_from_slice(&[0u8; 6]);

        let result = decode(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn decode_invalid_json() {
        let garbage = b"not valid json";
        let len = garbage.len() as u32;
        let mut buf = Vec::new();
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(garbage);

        let result = decode(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn multiple_messages_in_stream() {
        let msg1 = ReplicationMessage::StatusRequest;
        let msg2 = ReplicationMessage::Ack {
            replica_id: "r1".into(),
            table: "t".into(),
            last_txn: 1,
        };

        let mut stream = encode(&msg1).unwrap();
        stream.extend_from_slice(&encode(&msg2).unwrap());

        let (decoded1, consumed1) = decode(&stream).unwrap();
        assert_eq!(decoded1, msg1);

        let (decoded2, consumed2) = decode(&stream[consumed1..]).unwrap();
        assert_eq!(decoded2, msg2);
        assert_eq!(consumed1 + consumed2, stream.len());
    }
}
