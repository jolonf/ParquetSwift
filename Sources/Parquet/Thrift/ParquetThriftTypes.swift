import Foundation

// MARK: - Thrift Enums

public enum Type: Int32, Codable {
    case boolean = 0
    case int32 = 1
    case int64 = 2
    case int96 = 3
    case float = 4
    case double = 5
    case byteArray = 6
    case fixedLenByteArray = 7
}

public enum ConvertedType: Int32, Codable {
    case utf8 = 0
    case map = 1
    case mapKeyValue = 2
    case list = 3
    // ... others can be added as needed
    case int8 = 6
    case int16 = 7
    case int32 = 8
    case int64 = 9
    case uint8 = 10
    case uint16 = 11
    case uint32 = 12
    case uint64 = 13
}

public enum FieldRepetitionType: Int32, Codable {
    case required = 0
    case optional = 1
    case repeated = 2
}

public enum Encoding: Int32, Codable {
    case plain = 0
    case plainDictionary = 2
    case rle = 3
    case bitPacked = 4
    case deltaBinaryPacked = 5
    case deltaLengthByteArray = 6
    case deltaByteArray = 7
    case rleDictionary = 8
    case byteStreamSplit = 9
}

public enum CompressionCodec: Int32, Codable {
    case uncompressed = 0
    case snappy = 1
    case gzip = 2
    case lzo = 3
    case brotli = 4
    case lz4 = 5
    case zstd = 6
    case lz4Raw = 7
}

public enum PageType: Int32, Codable {
    case dataPage = 0
    case indexPage = 1
    case dictionaryPage = 2
    case dataPageV2 = 3
}

// MARK: - Thrift Structs

public struct Statistics: Codable {
    public var max: Data?
    public var min: Data?
    public var null_count: Int64?
    public var distinct_count: Int64?
}

public struct SchemaElement: Codable {
    public var type: Type?
    public var type_length: Int32?
    public var repetition_type: FieldRepetitionType?
    public var name: String
    public var num_children: Int32?
    public var converted_type: ConvertedType?
    public var scale: Int32?
    public var precision: Int32?
    public var field_id: Int32?
    
    public init(name: String) {
        self.name = name
    }
}

public struct DataPageHeader: Codable {
    public var num_values: Int32
    public var encoding: Encoding
    public var definition_level_encoding: Encoding
    public var repetition_level_encoding: Encoding
    public var statistics: Statistics?
}

public struct IndexPageHeader: Codable {
    // TODO: Fields
}

public struct DictionaryPageHeader: Codable {
    public var num_values: Int32
    public var encoding: Encoding
    public var is_sorted: Bool?
}

public struct DataPageHeaderV2: Codable {
    public var num_values: Int32
    public var num_nulls: Int32
    public var num_rows: Int32
    public var encoding: Encoding
    public var definition_levels_byte_length: Int32
    public var repetition_levels_byte_length: Int32
    public var is_compressed: Bool?
    public var statistics: Statistics?
}

public struct PageHeader: Codable {
    public var type: PageType
    public var uncompressed_page_size: Int32
    public var compressed_page_size: Int32
    public var crc: Int32?
    public var data_page_header: DataPageHeader?
    public var index_page_header: IndexPageHeader?
    public var dictionary_page_header: DictionaryPageHeader?
    public var data_page_header_v2: DataPageHeaderV2?
}

public struct KeyValue: Codable {
    public var key: String
    public var value: String?
}

public struct ColumnMetaData: Codable {
    public var type: Type
    public var encodings: [Encoding]
    public var path_in_schema: [String]
    public var codec: CompressionCodec
    public var num_values: Int64
    public var total_uncompressed_size: Int64
    public var total_compressed_size: Int64
    public var key_value_metadata: [KeyValue]?
    public var data_page_offset: Int64
    public var index_page_offset: Int64?
    public var dictionary_page_offset: Int64?
    public var statistics: Statistics?
    public var encoding_stats: [PageEncodingStats]?
}

public struct PageEncodingStats: Codable {
    public var page_type: PageType
    public var encoding: Encoding
    public var count: Int32
}

public struct ColumnChunk: Codable {
    public var file_path: String?
    public var file_offset: Int64
    public var meta_data: ColumnMetaData?
    public var offset_index_offset: Int64?
    public var offset_index_length: Int32?
    public var column_index_offset: Int64?
    public var column_index_length: Int32?
    // public var crypto_metadata: ColumnCryptoMetaData? // Parsing crypto not required for MVP
}

public struct RowGroup: Codable {
    public var columns: [ColumnChunk]
    public var total_byte_size: Int64
    public var num_rows: Int64
    public var sorting_columns: [SortingColumn]?
    public var file_offset: Int64?
    public var total_compressed_size: Int64?
    public var ordinal: Int16?
}

public struct SortingColumn: Codable {
    public var column_idx: Int32
    public var descending: Bool
    public var nulls_first: Bool
}

public struct FileMetaData: Codable {
    public var version: Int32
    public var schema: [SchemaElement]
    public var num_rows: Int64
    public var row_groups: [RowGroup]
    public var key_value_metadata: [KeyValue]?
    public var created_by: String?
    // public var column_orders: [ColumnOrder]? // Optional for MVP
    // public var encryption_algorithm: EncryptionAlgorithm? // Optional for MVP
    public var footer_signing_key_metadata: Data?
}
