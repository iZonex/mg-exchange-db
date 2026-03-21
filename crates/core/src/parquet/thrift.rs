//! Minimal Thrift compact protocol encoder for Apache Parquet file metadata.
//!
//! This implements just enough of the Thrift compact protocol to encode the
//! `FileMetadata` structure required by the Apache Parquet format spec.
//! The compact protocol is described in the Thrift specification and uses
//! variable-length integers (zigzag + varint encoding).

// Thrift compact protocol type IDs
pub const THRIFT_BOOLEAN_TRUE: u8 = 1;
pub const THRIFT_BOOLEAN_FALSE: u8 = 2;
pub const THRIFT_I8: u8 = 3;
pub const THRIFT_I16: u8 = 4;
pub const THRIFT_I32: u8 = 5;
pub const THRIFT_I64: u8 = 6;
pub const THRIFT_DOUBLE: u8 = 7;
pub const THRIFT_BINARY: u8 = 8;
pub const THRIFT_LIST: u8 = 9;
pub const THRIFT_SET: u8 = 10;
pub const THRIFT_MAP: u8 = 11;
pub const THRIFT_STRUCT: u8 = 12;

/// Minimal Thrift compact protocol encoder for Parquet file metadata.
///
/// The compact protocol encodes field IDs as deltas from the previous field,
/// and uses zigzag + varint encoding for integers.
pub struct ThriftEncoder {
    buf: Vec<u8>,
    /// Stack of last-field-ids for nested structs.
    field_id_stack: Vec<i16>,
    /// Current last field id.
    last_field_id: i16,
}

impl Default for ThriftEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ThriftEncoder {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(256),
            field_id_stack: Vec::new(),
            last_field_id: 0,
        }
    }

    /// Write a field header (field type + field delta).
    ///
    /// If the delta from the previous field ID fits in 4 bits (1..15),
    /// it is packed into the high nibble. Otherwise a full field header
    /// with the field ID as a zigzag varint is written.
    pub fn write_field(&mut self, field_id: i16, field_type: u8) {
        let delta = field_id - self.last_field_id;
        if delta > 0 && delta <= 15 {
            // Short form: high nibble = delta, low nibble = type
            self.buf.push(((delta as u8) << 4) | field_type);
        } else {
            // Long form: type byte, then field id as zigzag varint
            self.buf.push(field_type);
            self.write_zigzag(field_id as i64);
        }
        self.last_field_id = field_id;
    }

    /// Write a varint (unsigned variable-length integer).
    pub fn write_varint(&mut self, mut n: u64) {
        loop {
            let mut byte = (n & 0x7F) as u8;
            n >>= 7;
            if n != 0 {
                byte |= 0x80;
            }
            self.buf.push(byte);
            if n == 0 {
                break;
            }
        }
    }

    /// Write a zigzag-encoded signed integer as a varint.
    ///
    /// Zigzag encoding maps signed integers to unsigned:
    /// 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, ...
    pub fn write_zigzag(&mut self, n: i64) {
        let encoded = ((n << 1) ^ (n >> 63)) as u64;
        self.write_varint(encoded);
    }

    /// Write i32 as zigzag varint (Thrift compact protocol encoding for i32).
    pub fn write_i32(&mut self, n: i32) {
        self.write_zigzag(n as i64);
    }

    /// Write i64 as zigzag varint (Thrift compact protocol encoding for i64).
    pub fn write_i64(&mut self, n: i64) {
        self.write_zigzag(n);
    }

    /// Write a string (length as varint + UTF-8 bytes).
    pub fn write_string(&mut self, s: &str) {
        self.write_varint(s.len() as u64);
        self.buf.extend_from_slice(s.as_bytes());
    }

    /// Write a binary blob (length as varint + raw bytes).
    pub fn write_binary(&mut self, data: &[u8]) {
        self.write_varint(data.len() as u64);
        self.buf.extend_from_slice(data);
    }

    /// Write a list header (element type + count).
    ///
    /// If count <= 14, it is packed into the high nibble with the element type.
    /// Otherwise a 0xF nibble signals that the count follows as a varint.
    pub fn write_list(&mut self, elem_type: u8, count: usize) {
        if count <= 14 {
            self.buf.push(((count as u8) << 4) | elem_type);
        } else {
            self.buf.push(0xF0 | elem_type);
            self.write_varint(count as u64);
        }
    }

    /// Write struct stop marker (0x00).
    pub fn write_stop(&mut self) {
        self.buf.push(0x00);
    }

    /// Begin a nested struct. Saves the current field ID context.
    pub fn begin_struct(&mut self) {
        self.field_id_stack.push(self.last_field_id);
        self.last_field_id = 0;
    }

    /// End a nested struct. Restores the previous field ID context.
    /// Also writes the stop marker.
    pub fn end_struct(&mut self) {
        self.write_stop();
        self.last_field_id = self.field_id_stack.pop().unwrap_or(0);
    }

    /// Get encoded bytes, consuming the encoder.
    pub fn finish(self) -> Vec<u8> {
        self.buf
    }

    /// Get a reference to the current buffer.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }
}

// ---------------------------------------------------------------------------
// Apache Parquet metadata types
// ---------------------------------------------------------------------------

/// Parquet physical type (matches the spec's Type enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PhysicalType {
    Boolean = 0,
    Int32 = 1,
    Int64 = 2,
    Int96 = 3,
    Float = 4,
    Double = 5,
    ByteArray = 6,
    FixedLenByteArray = 7,
}

/// Parquet encoding type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ParquetEncoding {
    Plain = 0,
    // PlainDictionary = 2,
    // Rle = 3,
}

/// Parquet compression codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum CompressionCodec {
    Uncompressed = 0,
    // Snappy = 1,
    // Gzip = 2,
    // Lz4 = 4,
    // Zstd = 6,
    Lz4Raw = 7,
}

/// Repetition type for schema elements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Repetition {
    Required = 0,
    Optional = 1,
    Repeated = 2,
}

/// Column descriptor for Parquet schema encoding.
#[derive(Debug, Clone)]
pub struct ParquetSchemaColumn {
    pub name: String,
    pub physical_type: PhysicalType,
    pub repetition: Repetition,
}

/// Metadata for a single column chunk within a row group.
#[derive(Debug, Clone)]
pub struct ColumnChunkMeta {
    /// Column index in the schema (0-based, after the root element).
    pub schema_idx: usize,
    /// File offset where the column chunk data starts.
    pub file_offset: i64,
    /// Physical type of the column.
    pub physical_type: PhysicalType,
    /// Encoding used.
    pub encodings: Vec<ParquetEncoding>,
    /// Dot-separated path in schema (e.g., ["timestamp"]).
    pub path_in_schema: Vec<String>,
    /// Compression codec.
    pub codec: CompressionCodec,
    /// Number of values in this column chunk.
    pub num_values: i64,
    /// Total uncompressed size in bytes.
    pub total_uncompressed_size: i64,
    /// Total compressed size in bytes.
    pub total_compressed_size: i64,
    /// Offset of the data page within the file.
    pub data_page_offset: i64,
}

/// Metadata for a row group.
#[derive(Debug, Clone)]
pub struct RowGroupMeta {
    pub columns: Vec<ColumnChunkMeta>,
    pub total_byte_size: i64,
    pub num_rows: i64,
}

// ---------------------------------------------------------------------------
// Thrift encoding of Parquet structures
// ---------------------------------------------------------------------------

/// Encode a complete Parquet FileMetadata as Thrift compact protocol bytes.
///
/// The FileMetadata Thrift struct is:
/// ```thrift
/// struct FileMetaData {
///   1: required i32 version
///   2: required list<SchemaElement> schema
///   3: required i64 num_rows
///   4: required list<RowGroup> row_groups
///   5: optional list<KeyValue> key_value_metadata
/// }
/// ```
pub fn encode_parquet_footer(
    schema: &[ParquetSchemaColumn],
    row_groups: &[RowGroupMeta],
    num_rows: i64,
) -> Vec<u8> {
    let mut enc = ThriftEncoder::new();

    // Field 1: version (i32) - Parquet format version 1
    enc.write_field(1, THRIFT_I32);
    enc.write_i32(1);

    // Field 2: schema (list<SchemaElement>)
    // The schema list starts with a root element that has num_children = N,
    // followed by N leaf SchemaElement entries.
    enc.write_field(2, THRIFT_LIST);
    enc.write_list(THRIFT_STRUCT, schema.len() + 1); // +1 for root

    // Root schema element
    encode_schema_root(&mut enc, "schema", schema.len() as i32);

    // Leaf schema elements
    for col in schema {
        encode_schema_element(&mut enc, col);
    }

    // Field 3: num_rows (i64)
    enc.write_field(3, THRIFT_I64);
    enc.write_i64(num_rows);

    // Field 4: row_groups (list<RowGroup>)
    enc.write_field(4, THRIFT_LIST);
    enc.write_list(THRIFT_STRUCT, row_groups.len());
    for rg in row_groups {
        encode_row_group(&mut enc, rg);
    }

    // Field 7: created_by (optional string)
    enc.write_field(5, THRIFT_LIST);
    enc.write_list(THRIFT_STRUCT, 1);
    // Write a single KeyValue: key="created_by", value="exchange-db"
    enc.begin_struct();
    enc.write_field(1, THRIFT_BINARY);
    enc.write_string("created_by");
    enc.write_field(2, THRIFT_BINARY);
    enc.write_string("exchange-db (github.com/exchange-db)");
    enc.end_struct();

    enc.write_stop();
    enc.finish()
}

/// Encode the root SchemaElement (has num_children, no type).
///
/// ```thrift
/// struct SchemaElement {
///   1: optional Type type
///   2: optional i32 type_length
///   3: optional FieldRepetitionType repetition_type
///   4: required string name
///   5: optional i32 num_children
///   6: optional ConvertedType converted_type
///   7: optional i32 scale
///   8: optional i32 precision
///   9: optional i32 field_id
///   10: optional LogicalType logicalType
/// }
/// ```
fn encode_schema_root(enc: &mut ThriftEncoder, name: &str, num_children: i32) {
    enc.begin_struct();
    // Field 4: name
    enc.write_field(4, THRIFT_BINARY);
    enc.write_string(name);
    // Field 5: num_children
    enc.write_field(5, THRIFT_I32);
    enc.write_i32(num_children);
    enc.end_struct();
}

/// Encode a leaf SchemaElement (has type, no num_children).
fn encode_schema_element(enc: &mut ThriftEncoder, col: &ParquetSchemaColumn) {
    enc.begin_struct();
    // Field 1: type (i32 enum)
    enc.write_field(1, THRIFT_I32);
    enc.write_i32(col.physical_type as i32);
    // Field 3: repetition_type
    enc.write_field(3, THRIFT_I32);
    enc.write_i32(col.repetition as i32);
    // Field 4: name
    enc.write_field(4, THRIFT_BINARY);
    enc.write_string(&col.name);
    enc.end_struct();
}

/// Encode a RowGroup.
///
/// ```thrift
/// struct RowGroup {
///   1: required list<ColumnChunk> columns
///   2: required i64 total_byte_size
///   3: required i64 num_rows
/// }
/// ```
fn encode_row_group(enc: &mut ThriftEncoder, rg: &RowGroupMeta) {
    enc.begin_struct();
    // Field 1: columns
    enc.write_field(1, THRIFT_LIST);
    enc.write_list(THRIFT_STRUCT, rg.columns.len());
    for col in &rg.columns {
        encode_column_chunk(enc, col);
    }
    // Field 2: total_byte_size
    enc.write_field(2, THRIFT_I64);
    enc.write_i64(rg.total_byte_size);
    // Field 3: num_rows
    enc.write_field(3, THRIFT_I64);
    enc.write_i64(rg.num_rows);
    enc.end_struct();
}

/// Encode a ColumnChunk.
///
/// ```thrift
/// struct ColumnChunk {
///   1: optional string file_path
///   2: required i64 file_offset
///   3: optional ColumnMetaData meta_data
/// }
/// ```
fn encode_column_chunk(enc: &mut ThriftEncoder, col: &ColumnChunkMeta) {
    enc.begin_struct();
    // Field 2: file_offset
    enc.write_field(2, THRIFT_I64);
    enc.write_i64(col.file_offset);
    // Field 3: meta_data (inline ColumnMetaData struct)
    enc.write_field(3, THRIFT_STRUCT);
    encode_column_metadata(enc, col);
    enc.end_struct();
}

/// Encode ColumnMetaData.
///
/// ```thrift
/// struct ColumnMetaData {
///   1: required Type type
///   2: required list<Encoding> encodings
///   3: required list<string> path_in_schema
///   4: required CompressionCodec codec
///   5: required i64 num_values
///   6: required i64 total_uncompressed_size
///   7: required i64 total_compressed_size
///   9: required i64 data_page_offset
/// }
/// ```
fn encode_column_metadata(enc: &mut ThriftEncoder, col: &ColumnChunkMeta) {
    enc.begin_struct();
    // Field 1: type
    enc.write_field(1, THRIFT_I32);
    enc.write_i32(col.physical_type as i32);
    // Field 2: encodings
    enc.write_field(2, THRIFT_LIST);
    enc.write_list(THRIFT_I32, col.encodings.len());
    for e in &col.encodings {
        enc.write_i32(*e as i32);
    }
    // Field 3: path_in_schema
    enc.write_field(3, THRIFT_LIST);
    enc.write_list(THRIFT_BINARY, col.path_in_schema.len());
    for p in &col.path_in_schema {
        enc.write_string(p);
    }
    // Field 4: codec
    enc.write_field(4, THRIFT_I32);
    enc.write_i32(col.codec as i32);
    // Field 5: num_values
    enc.write_field(5, THRIFT_I64);
    enc.write_i64(col.num_values);
    // Field 6: total_uncompressed_size
    enc.write_field(6, THRIFT_I64);
    enc.write_i64(col.total_uncompressed_size);
    // Field 7: total_compressed_size
    enc.write_field(7, THRIFT_I64);
    enc.write_i64(col.total_compressed_size);
    // Field 9: data_page_offset (skip field 8 = key_value_metadata)
    enc.write_field(9, THRIFT_I64);
    enc.write_i64(col.data_page_offset);
    enc.end_struct();
}

// ---------------------------------------------------------------------------
// Data page header encoding
// ---------------------------------------------------------------------------

/// Page type enum from the Parquet spec.
#[derive(Debug, Clone, Copy)]
#[repr(i32)]
pub enum PageType {
    DataPage = 0,
    // IndexPage = 1,
    DictionaryPage = 2,
    DataPageV2 = 3,
}

/// Encode a Thrift PageHeader for a data page.
///
/// ```thrift
/// struct PageHeader {
///   1: required PageType type
///   2: required i32 uncompressed_page_size
///   3: required i32 compressed_page_size
///   5: optional DataPageHeader data_page_header
/// }
///
/// struct DataPageHeader {
///   1: required i32 num_values
///   2: required Encoding encoding
///   3: required Encoding definition_level_encoding
///   4: required Encoding repetition_level_encoding
/// }
/// ```
pub fn encode_data_page_header(
    uncompressed_size: i32,
    compressed_size: i32,
    num_values: i32,
) -> Vec<u8> {
    let mut enc = ThriftEncoder::new();
    // Field 1: type = DATA_PAGE (0)
    enc.write_field(1, THRIFT_I32);
    enc.write_i32(PageType::DataPage as i32);
    // Field 2: uncompressed_page_size
    enc.write_field(2, THRIFT_I32);
    enc.write_i32(uncompressed_size);
    // Field 3: compressed_page_size
    enc.write_field(3, THRIFT_I32);
    enc.write_i32(compressed_size);
    // Field 5: data_page_header (struct)
    enc.write_field(5, THRIFT_STRUCT);
    enc.begin_struct();
    // DataPageHeader.1: num_values
    enc.write_field(1, THRIFT_I32);
    enc.write_i32(num_values);
    // DataPageHeader.2: encoding = PLAIN (0)
    enc.write_field(2, THRIFT_I32);
    enc.write_i32(ParquetEncoding::Plain as i32);
    // DataPageHeader.3: definition_level_encoding = PLAIN (0)
    enc.write_field(3, THRIFT_I32);
    enc.write_i32(ParquetEncoding::Plain as i32);
    // DataPageHeader.4: repetition_level_encoding = PLAIN (0)
    enc.write_field(4, THRIFT_I32);
    enc.write_i32(ParquetEncoding::Plain as i32);
    enc.end_struct();

    enc.write_stop();
    enc.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zigzag_encoding() {
        // Test zigzag encoding: 0->0, -1->1, 1->2, -2->3, 2->4
        let cases: Vec<(i64, u64)> = vec![
            (0, 0),
            (-1, 1),
            (1, 2),
            (-2, 3),
            (2, 4),
            (100, 200),
            (-100, 199),
        ];
        for (signed, expected_unsigned) in cases {
            let encoded = ((signed << 1) ^ (signed >> 63)) as u64;
            assert_eq!(
                encoded, expected_unsigned,
                "zigzag({signed}) should be {expected_unsigned}"
            );
        }
    }

    #[test]
    fn varint_encoding() {
        let mut enc = ThriftEncoder::new();
        enc.write_varint(0);
        assert_eq!(enc.as_bytes(), &[0x00]);

        let mut enc = ThriftEncoder::new();
        enc.write_varint(1);
        assert_eq!(enc.as_bytes(), &[0x01]);

        let mut enc = ThriftEncoder::new();
        enc.write_varint(127);
        assert_eq!(enc.as_bytes(), &[0x7F]);

        let mut enc = ThriftEncoder::new();
        enc.write_varint(128);
        assert_eq!(enc.as_bytes(), &[0x80, 0x01]);

        let mut enc = ThriftEncoder::new();
        enc.write_varint(300);
        assert_eq!(enc.as_bytes(), &[0xAC, 0x02]);
    }

    #[test]
    fn string_encoding() {
        let mut enc = ThriftEncoder::new();
        enc.write_string("hello");
        let bytes = enc.finish();
        assert_eq!(bytes[0], 5); // length varint
        assert_eq!(&bytes[1..], b"hello");
    }

    #[test]
    fn list_short_form() {
        let mut enc = ThriftEncoder::new();
        enc.write_list(THRIFT_I32, 3);
        let bytes = enc.finish();
        // 3 elements, type I32(5) -> high nibble 3, low nibble 5 = 0x35
        assert_eq!(bytes, &[0x35]);
    }

    #[test]
    fn list_long_form() {
        let mut enc = ThriftEncoder::new();
        enc.write_list(THRIFT_I32, 20);
        let bytes = enc.finish();
        // 20 > 14, so: 0xF5 (0xF0 | type), then varint(20) = 0x14
        assert_eq!(bytes[0], 0xF5);
        assert_eq!(bytes[1], 20);
    }

    #[test]
    fn field_delta_short() {
        let mut enc = ThriftEncoder::new();
        enc.write_field(1, THRIFT_I32);
        let bytes = enc.finish();
        // Delta = 1, type = I32(5) -> high nibble 1, low nibble 5 = 0x15
        assert_eq!(bytes, &[0x15]);
    }

    #[test]
    fn field_delta_long() {
        let mut enc = ThriftEncoder::new();
        // Set last_field_id to 0, write field 20 -> delta = 20 > 15 -> long form
        enc.write_field(20, THRIFT_I32);
        let bytes = enc.finish();
        // Long form: type byte (5), then zigzag(20) = varint(40) = 0x28
        assert_eq!(bytes[0], THRIFT_I32);
        assert_eq!(bytes[1], 40); // zigzag(20) = 40
    }

    #[test]
    fn stop_marker() {
        let mut enc = ThriftEncoder::new();
        enc.write_stop();
        assert_eq!(enc.finish(), &[0x00]);
    }

    #[test]
    fn nested_struct_field_id_tracking() {
        let mut enc = ThriftEncoder::new();
        // Outer struct field 1
        enc.write_field(1, THRIFT_I32);
        enc.write_i32(42);
        // Outer struct field 2 is a nested struct
        enc.write_field(2, THRIFT_STRUCT);
        enc.begin_struct();
        // Inner struct field 1 (last_field_id resets to 0)
        enc.write_field(1, THRIFT_I32);
        enc.write_i32(99);
        enc.end_struct(); // writes stop, restores last_field_id to 2
        // Outer struct field 3 (delta from 2)
        enc.write_field(3, THRIFT_I32);
        enc.write_i32(7);
        enc.write_stop();

        let bytes = enc.finish();
        // Should not panic and should produce valid bytes
        assert!(!bytes.is_empty());
    }

    #[test]
    fn encode_data_page_header_produces_valid_bytes() {
        let header = encode_data_page_header(1000, 800, 100);
        // Should start with field delta + type for field 1
        assert!(!header.is_empty());
        // Last byte should be stop marker (0x00) for outer struct
        assert_eq!(header[header.len() - 1], 0x00);
    }

    #[test]
    fn encode_footer_roundtrip_structure() {
        let schema = vec![
            ParquetSchemaColumn {
                name: "timestamp".to_string(),
                physical_type: PhysicalType::Int64,
                repetition: Repetition::Required,
            },
            ParquetSchemaColumn {
                name: "price".to_string(),
                physical_type: PhysicalType::Double,
                repetition: Repetition::Required,
            },
        ];

        let row_groups = vec![RowGroupMeta {
            columns: vec![
                ColumnChunkMeta {
                    schema_idx: 0,
                    file_offset: 4,
                    physical_type: PhysicalType::Int64,
                    encodings: vec![ParquetEncoding::Plain],
                    path_in_schema: vec!["timestamp".to_string()],
                    codec: CompressionCodec::Uncompressed,
                    num_values: 100,
                    total_uncompressed_size: 800,
                    total_compressed_size: 800,
                    data_page_offset: 4,
                },
                ColumnChunkMeta {
                    schema_idx: 1,
                    file_offset: 804,
                    physical_type: PhysicalType::Double,
                    encodings: vec![ParquetEncoding::Plain],
                    path_in_schema: vec!["price".to_string()],
                    codec: CompressionCodec::Uncompressed,
                    num_values: 100,
                    total_uncompressed_size: 800,
                    total_compressed_size: 800,
                    data_page_offset: 804,
                },
            ],
            total_byte_size: 1600,
            num_rows: 100,
        }];

        let footer = encode_parquet_footer(&schema, &row_groups, 100);
        // Footer should be non-empty and end with stop marker
        assert!(!footer.is_empty());
        assert_eq!(footer[footer.len() - 1], 0x00);

        // Validate it starts with expected field patterns
        // Field 1 (version), delta=1, type=I32(5) -> 0x15
        assert_eq!(footer[0], 0x15);
    }

    #[test]
    fn full_parquet_file_has_valid_magic() {
        use super::super::apache_writer::ApacheParquetWriter;

        let writer = ApacheParquetWriter::new();
        let columns_schema = vec![ParquetSchemaColumn {
            name: "id".to_string(),
            physical_type: PhysicalType::Int64,
            repetition: Repetition::Required,
        }];

        // 10 i64 values
        let data: Vec<u8> = (0..10i64).flat_map(|i| i.to_le_bytes()).collect();
        let column_data = vec![data];

        let file_bytes = writer.write_file(&columns_schema, &column_data, 10);
        // Check PAR1 magic at start and end
        assert_eq!(&file_bytes[0..4], b"PAR1");
        assert_eq!(&file_bytes[file_bytes.len() - 4..], b"PAR1");

        // Check footer length (4 bytes LE i32 before final PAR1)
        let footer_len_pos = file_bytes.len() - 8;
        let footer_len = i32::from_le_bytes(
            file_bytes[footer_len_pos..footer_len_pos + 4]
                .try_into()
                .unwrap(),
        );
        assert!(footer_len > 0);
        assert!((footer_len as usize) < file_bytes.len());
    }
}
