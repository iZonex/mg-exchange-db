//! PAR1XCHG columnar format — a self-describing, compressed binary columnar
//! format that follows Apache Parquet's conceptual model but uses a simpler
//! serialisation (no Thrift dependency).
//!
//! File layout:
//! ```text
//! [8 bytes]  magic "PAR1XCHG"
//! [2 bytes]  version (u16 LE) — currently 1
//! [2 bytes]  num_columns (u16 LE)
//! [8 bytes]  num_rows (u64 LE)
//! -- per column metadata (repeated num_columns times) --
//!   [2 bytes]  name_len (u16 LE)
//!   [N bytes]  column name (UTF-8)
//!   [1 byte]   ColumnType tag (repr u8)
//!   [8 bytes]  data_offset (u64 LE)
//!   [8 bytes]  data_length (u64 LE)   — uncompressed size
//!   [8 bytes]  compressed_length (u64 LE) — 0 if uncompressed
//!   [1 byte]   encoding: 0=PLAIN, 1=DICTIONARY, 2=RLE
//! -- column data blocks (LZ4-compressed) --
//! [8 bytes]  footer checksum (xxh3 of everything above)
//! [8 bytes]  magic "PAR1XCHG"
//! ```

pub mod apache_writer;
pub mod reader;
pub mod thrift;
pub mod writer;

pub use apache_writer::ApacheParquetWriter;
pub use reader::{ParquetColumnMeta, ParquetMetadata, ParquetReader};
pub use thrift::{
    ColumnChunkMeta, CompressionCodec, ParquetEncoding, ParquetSchemaColumn, PhysicalType,
    Repetition, RowGroupMeta,
};
pub use writer::{ParquetColumn, ParquetType, ParquetWriteStats, ParquetWriter};
