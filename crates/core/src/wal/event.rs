use exchange_common::error::{ExchangeDbError, Result};

/// Event types that can be recorded in the WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EventType {
    /// Row data insert/update.
    Data = 1,
    /// Schema change (DDL): create table, add column, etc.
    Ddl = 2,
    /// Truncate table operation.
    Truncate = 3,
}

impl EventType {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            1 => Ok(Self::Data),
            2 => Ok(Self::Ddl),
            3 => Ok(Self::Truncate),
            other => Err(ExchangeDbError::Corruption(format!(
                "unknown WAL event type: {other}"
            ))),
        }
    }
}

/// On-disk event layout (all little-endian):
///
/// | field        | type | bytes |
/// |--------------|------|-------|
/// | event_type   | u8   | 1     |
/// | txn_id       | u64  | 8     |
/// | timestamp    | i64  | 8     |
/// | payload_len  | u32  | 4     |
/// | payload      | [u8] | var   |
/// | checksum     | u32  | 4     |
///
/// Total header = 21 bytes, total overhead = 25 bytes + payload.
pub const EVENT_HEADER_SIZE: usize = 1 + 8 + 8 + 4; // 21 bytes
pub const EVENT_CHECKSUM_SIZE: usize = 4;
pub const EVENT_OVERHEAD: usize = EVENT_HEADER_SIZE + EVENT_CHECKSUM_SIZE; // 25 bytes

/// A WAL event with its metadata and payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalEvent {
    pub event_type: EventType,
    pub txn_id: u64,
    pub timestamp: i64,
    pub payload: Vec<u8>,
}

impl WalEvent {
    /// Create a new data event.
    pub fn data(txn_id: u64, timestamp: i64, payload: Vec<u8>) -> Self {
        Self {
            event_type: EventType::Data,
            txn_id,
            timestamp,
            payload,
        }
    }

    /// Create a new DDL event.
    pub fn ddl(txn_id: u64, timestamp: i64, payload: Vec<u8>) -> Self {
        Self {
            event_type: EventType::Ddl,
            txn_id,
            timestamp,
            payload,
        }
    }

    /// Create a new truncate event.
    pub fn truncate(txn_id: u64, timestamp: i64, payload: Vec<u8>) -> Self {
        Self {
            event_type: EventType::Truncate,
            txn_id,
            timestamp,
            payload,
        }
    }

    /// Total size of this event on disk (header + payload + checksum).
    pub fn wire_size(&self) -> usize {
        EVENT_OVERHEAD + self.payload.len()
    }

    /// Serialize the event into bytes (header + payload + checksum).
    pub fn serialize(&self) -> Vec<u8> {
        let total = self.wire_size();
        let mut buf = Vec::with_capacity(total);

        buf.push(self.event_type as u8);
        buf.extend_from_slice(&self.txn_id.to_le_bytes());
        buf.extend_from_slice(&self.timestamp.to_le_bytes());
        buf.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.payload);

        // Checksum covers everything before the checksum field itself.
        let checksum = compute_checksum(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());

        buf
    }

    /// Deserialize an event from a byte slice. The slice must contain exactly
    /// one complete event (header + payload + checksum).
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < EVENT_OVERHEAD {
            return Err(ExchangeDbError::Corruption("WAL event too short".into()));
        }

        let event_type = EventType::from_u8(data[0])?;
        let txn_id = u64::from_le_bytes(data[1..9].try_into().unwrap());
        let timestamp = i64::from_le_bytes(data[9..17].try_into().unwrap());
        let payload_len = u32::from_le_bytes(data[17..21].try_into().unwrap()) as usize;

        let expected_total = EVENT_OVERHEAD + payload_len;
        if data.len() < expected_total {
            return Err(ExchangeDbError::Corruption(format!(
                "WAL event truncated: expected {expected_total} bytes, got {}",
                data.len()
            )));
        }

        let payload = data[EVENT_HEADER_SIZE..EVENT_HEADER_SIZE + payload_len].to_vec();

        // Verify checksum.
        let checksum_offset = EVENT_HEADER_SIZE + payload_len;
        let stored_checksum = u32::from_le_bytes(
            data[checksum_offset..checksum_offset + 4]
                .try_into()
                .unwrap(),
        );
        let computed = compute_checksum(&data[..checksum_offset]);

        if stored_checksum != computed {
            return Err(ExchangeDbError::Corruption(format!(
                "WAL checksum mismatch: stored={stored_checksum:#010x}, computed={computed:#010x}"
            )));
        }

        Ok(Self {
            event_type,
            txn_id,
            timestamp,
            payload,
        })
    }
}

/// Compute a 32-bit checksum using xxh3.
fn compute_checksum(data: &[u8]) -> u32 {
    // Use the lower 32 bits of xxh3_64.
    xxhash_rust::xxh3::xxh3_64(data) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_type_roundtrip() {
        for ty in [EventType::Data, EventType::Ddl, EventType::Truncate] {
            let recovered = EventType::from_u8(ty as u8).unwrap();
            assert_eq!(recovered, ty);
        }
    }

    #[test]
    fn event_type_invalid() {
        assert!(EventType::from_u8(0).is_err());
        assert!(EventType::from_u8(255).is_err());
    }

    #[test]
    fn serialize_deserialize_roundtrip() {
        let event = WalEvent::data(42, 1_000_000_000, b"hello world".to_vec());
        let bytes = event.serialize();
        assert_eq!(bytes.len(), event.wire_size());

        let recovered = WalEvent::deserialize(&bytes).unwrap();
        assert_eq!(recovered, event);
    }

    #[test]
    fn serialize_deserialize_empty_payload() {
        let event = WalEvent::truncate(1, 999, vec![]);
        let bytes = event.serialize();
        let recovered = WalEvent::deserialize(&bytes).unwrap();
        assert_eq!(recovered, event);
    }

    #[test]
    fn serialize_deserialize_large_payload() {
        let payload = vec![0xAB; 100_000];
        let event = WalEvent::ddl(99, 12345, payload);
        let bytes = event.serialize();
        let recovered = WalEvent::deserialize(&bytes).unwrap();
        assert_eq!(recovered, event);
    }

    #[test]
    fn checksum_corruption_detected() {
        let event = WalEvent::data(1, 2, b"test".to_vec());
        let mut bytes = event.serialize();
        // Corrupt a payload byte.
        bytes[EVENT_HEADER_SIZE] ^= 0xFF;
        let result = WalEvent::deserialize(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn truncated_data_detected() {
        let event = WalEvent::data(1, 2, b"test".to_vec());
        let bytes = event.serialize();
        // Chop off some bytes.
        let result = WalEvent::deserialize(&bytes[..bytes.len() - 2]);
        assert!(result.is_err());
    }

    #[test]
    fn wire_size_consistency() {
        let event = WalEvent::data(0, 0, vec![1, 2, 3, 4, 5]);
        assert_eq!(event.wire_size(), EVENT_OVERHEAD + 5);
    }
}
