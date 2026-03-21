//! Write-Ahead Log (WAL) module for ExchangeDB.
//!
//! Inspired by QuestDB's WAL system, this module provides crash-safe,
//! append-only logging of all mutations before they are applied to
//! the main column store.
//!
//! # Architecture
//!
//! - **Segments** (`segment.rs`): Append-only files backed by memory-mapped I/O.
//!   Each segment has a header and a sequence of serialized events.
//!   Segments are named `wal-NNNNNN.wal`.
//!
//! - **Events** (`event.rs`): The unit of WAL logging. Each event contains
//!   a type tag, transaction ID, timestamp, variable-length payload, and
//!   an xxh3 checksum for integrity.
//!
//! - **Writer** (`writer.rs`): Appends events to the current segment,
//!   automatically rotating to a new segment when the size limit is reached.
//!   Supports sync and async commit modes.
//!
//! - **Reader** (`reader.rs`): Reads events from one or more segments,
//!   supporting both eager and lazy iteration.
//!
//! - **Sequencer** (`sequencer.rs`): Lock-free, monotonic transaction ID
//!   generator for ordering events.

pub mod event;
pub mod merge;
pub mod reader;
pub mod row_codec;
pub mod segment;
pub mod sequencer;
pub mod writer;

pub use event::{EventType, WalEvent};
pub use merge::{MergeStats, WalMergeJob};
pub use reader::WalReader;
pub use row_codec::{decode_row, encode_row, OwnedColumnValue};
pub use segment::WalSegment;
pub use sequencer::Sequencer;
pub use writer::{CommitMode, WalWriter, WalWriterConfig};
