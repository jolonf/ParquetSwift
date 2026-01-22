import Foundation

protocol ThriftReadable {
    init(from thrift: ThriftCompactProtocol) throws
}

extension FileMetaData: ThriftReadable {
    public init(from thrift: ThriftCompactProtocol) throws {
        self.version = 0
        self.schema = []
        self.num_rows = 0
        self.row_groups = []
        
        thrift.readStructBegin()
        while true {
            let (type, id, _) = try thrift.readFieldBegin()
            if type == .stop { break }
            
            switch id {
            case 1: // version: i32
                self.version = try thrift.readI32()
            case 2: // schema: list<SchemaElement>
                let (_, size) = try thrift.readListBegin()
                for _ in 0..<size {
                    self.schema.append(try SchemaElement(from: thrift))
                }
            case 3: // num_rows: i64
                self.num_rows = try thrift.readI64()
            case 4: // row_groups: list<RowGroup>
                let (_, size) = try thrift.readListBegin()
                for _ in 0..<size {
                    self.row_groups.append(try RowGroup(from: thrift))
                }
            case 5: // key_value_metadata: list<KeyValue>
                var kvs: [KeyValue] = []
                let (_, size) = try thrift.readListBegin()
                for _ in 0..<size {
                    kvs.append(try KeyValue(from: thrift))
                }
                self.key_value_metadata = kvs
            case 6: // created_by: string
                self.created_by = try thrift.readString()
            default: 
                try thrift.skip(type)
            }
        }
        thrift.readStructEnd()
    }
}

extension SchemaElement: ThriftReadable {
    public init(from thrift: ThriftCompactProtocol) throws {
        self.name = ""
        thrift.readStructBegin()
        while true {
            let (type, id, _) = try thrift.readFieldBegin()
            if type == .stop { break }
            switch id {
            case 1: if let raw = try? thrift.readI32() { self.type = Type(rawValue: raw) }
            case 2: self.type_length = try thrift.readI32()
            case 3: if let raw = try? thrift.readI32() { self.repetition_type = FieldRepetitionType(rawValue: raw) }
            case 4: self.name = try thrift.readString()
            case 5: self.num_children = try thrift.readI32()
            case 6: if let raw = try? thrift.readI32() { self.converted_type = ConvertedType(rawValue: raw) }
            case 7: self.scale = try thrift.readI32()
            case 8: self.precision = try thrift.readI32()
            case 9: self.field_id = try thrift.readI32()
            default: try thrift.skip(type)
            }
        }
        thrift.readStructEnd()
    }
}

extension RowGroup: ThriftReadable {
    public init(from thrift: ThriftCompactProtocol) throws {
        self.columns = []
        self.total_byte_size = 0
        self.num_rows = 0
        
        thrift.readStructBegin()
        while true {
            let (type, id, _) = try thrift.readFieldBegin()
            if type == .stop { break }
            switch id {
            case 1: 
                let (_, size) = try thrift.readListBegin()
                for _ in 0..<size { columns.append(try ColumnChunk(from: thrift)) }
            case 2: self.total_byte_size = try thrift.readI64()
            case 3: self.num_rows = try thrift.readI64()
            case 4: 
                let (_, size) = try thrift.readListBegin()
                var sc: [SortingColumn] = []
                for _ in 0..<size { sc.append(try SortingColumn(from: thrift)) }
                self.sorting_columns = sc
            case 5: self.file_offset = try thrift.readI64()
            case 6: self.total_compressed_size = try thrift.readI64()
            case 7: self.ordinal = try Int16(thrift.readI16())
            default: try thrift.skip(type)
            }
        }
        thrift.readStructEnd()
    }
}

extension ColumnChunk: ThriftReadable {
    public init(from thrift: ThriftCompactProtocol) throws {
        self.file_offset = 0
        thrift.readStructBegin()
        while true {
            let (type, id, _) = try thrift.readFieldBegin()
            if type == .stop { break }
            switch id {
            case 1: self.file_path = try thrift.readString()
            case 2: self.file_offset = try thrift.readI64()
            case 3: self.meta_data = try ColumnMetaData(from: thrift)
            case 4: self.offset_index_offset = try thrift.readI64()
            case 5: self.offset_index_length = try thrift.readI32()
            case 6: self.column_index_offset = try thrift.readI64()
            case 7: self.column_index_length = try thrift.readI32()
            default: try thrift.skip(type)
            }
        }
        thrift.readStructEnd()
    }
}

extension ColumnMetaData: ThriftReadable {
    public init(from thrift: ThriftCompactProtocol) throws {
        self.type = .boolean // dummy default
        self.encodings = []
        self.path_in_schema = []
        self.codec = .uncompressed
        self.num_values = 0
        self.total_uncompressed_size = 0
        self.total_compressed_size = 0
        self.data_page_offset = 0
        
        thrift.readStructBegin()
        while true {
            let (type, id, _) = try thrift.readFieldBegin()
            if type == .stop { break }
            switch id {
            case 1: if let raw = try? thrift.readI32() { self.type = Type(rawValue: raw) ?? .boolean }
            case 2:
                let (_, size) = try thrift.readListBegin()
                for _ in 0..<size {
                    if let val = Encoding(rawValue: try thrift.readI32()) { encodings.append(val) }
                }
            case 3:
                let (_, size) = try thrift.readListBegin()
                for _ in 0..<size { path_in_schema.append(try thrift.readString()) }
            case 4: if let raw = try? thrift.readI32() { self.codec = CompressionCodec(rawValue: raw) ?? .uncompressed }
            case 5: self.num_values = try thrift.readI64()
            case 6: self.total_uncompressed_size = try thrift.readI64()
            case 7: self.total_compressed_size = try thrift.readI64()
            case 8: 
                 // key value metadata list
                 let (_, size) = try thrift.readListBegin()
                 var kvs: [KeyValue] = []
                 for _ in 0..<size { kvs.append(try KeyValue(from: thrift)) }
                 self.key_value_metadata = kvs
            case 9: self.data_page_offset = try thrift.readI64()
            case 10: self.index_page_offset = try thrift.readI64()
            case 11: self.dictionary_page_offset = try thrift.readI64()
            case 12: self.statistics = try Statistics(from: thrift)
            default: try thrift.skip(type)
            }
        }
        thrift.readStructEnd()
    }
}

extension KeyValue: ThriftReadable {
    public init(from thrift: ThriftCompactProtocol) throws {
        self.key = ""
        thrift.readStructBegin()
        while true {
            let (type, id, _) = try thrift.readFieldBegin()
            if type == .stop { break }
            switch id {
            case 1: self.key = try thrift.readString()
            case 2: self.value = try thrift.readString()
            default: try thrift.skip(type)
            }
        }
        thrift.readStructEnd()
    }
}

extension Statistics: ThriftReadable {
    public init(from thrift: ThriftCompactProtocol) throws {
        thrift.readStructBegin()
        while true {
            let (type, id, _) = try thrift.readFieldBegin()
            if type == .stop { break }
            switch id {
            case 1: self.max = try thrift.readBinary()
            case 2: self.min = try thrift.readBinary()
            case 3: self.null_count = try thrift.readI64()
            case 4: self.distinct_count = try thrift.readI64()
            default: try thrift.skip(type)
            }
        }
        thrift.readStructEnd()
    }
}

extension SortingColumn: ThriftReadable {
    public init(from thrift: ThriftCompactProtocol) throws {
        self.column_idx = 0
        self.descending = false
        self.nulls_first = false
        thrift.readStructBegin()
        while true {
            let (type, id, boolVal) = try thrift.readFieldBegin()
            if type == .stop { break }
            switch id {
            case 1: self.column_idx = try thrift.readI32()
            case 2: 
                if let val = boolVal { self.descending = val }
                else { self.descending = try thrift.readBool() }
            case 3: 
                if let val = boolVal { self.nulls_first = val }
                else { self.nulls_first = try thrift.readBool() }
            default: try thrift.skip(type)
            }
        }
        thrift.readStructEnd()
    }
}

extension PageHeader: ThriftReadable {
    public init(from thrift: ThriftCompactProtocol) throws {
        self.type = .dataPage
        self.uncompressed_page_size = 0
        self.compressed_page_size = 0
        thrift.readStructBegin()
        while true {
            let (type, id, _) = try thrift.readFieldBegin()
            if type == .stop { break }
            switch id {
            case 1: if let raw = try? thrift.readI32() { self.type = PageType(rawValue: raw) ?? .dataPage }
            case 2: self.uncompressed_page_size = try thrift.readI32()
            case 3: self.compressed_page_size = try thrift.readI32()
            case 4: self.crc = try thrift.readI32()
            case 5: self.data_page_header = try DataPageHeader(from: thrift)
            case 6: self.index_page_header = try IndexPageHeader(from: thrift)
            case 7: self.dictionary_page_header = try DictionaryPageHeader(from: thrift)
            case 8: self.data_page_header_v2 = try DataPageHeaderV2(from: thrift)
            default: try thrift.skip(type)
            }
        }
        thrift.readStructEnd()
    }
}

extension DataPageHeader: ThriftReadable {
    public init(from thrift: ThriftCompactProtocol) throws {
        self.num_values = 0
        self.encoding = .plain
        self.definition_level_encoding = .plain
        self.repetition_level_encoding = .plain
        thrift.readStructBegin()
        while true {
            let (type, id, _) = try thrift.readFieldBegin()
            if type == .stop { break }
            switch id {
            case 1: self.num_values = try thrift.readI32()
            case 2: if let raw = try? thrift.readI32() { self.encoding = Encoding(rawValue: raw) ?? .plain }
            case 3: if let raw = try? thrift.readI32() { self.definition_level_encoding = Encoding(rawValue: raw) ?? .plain }
            case 4: if let raw = try? thrift.readI32() { self.repetition_level_encoding = Encoding(rawValue: raw) ?? .plain }
            case 5: self.statistics = try Statistics(from: thrift)
            default: try thrift.skip(type)
            }
        }
        thrift.readStructEnd()
    }
}

extension IndexPageHeader: ThriftReadable {
    public init(from thrift: ThriftCompactProtocol) throws {
         thrift.readStructBegin()
        while true {
            let (type, _, _) = try thrift.readFieldBegin()
            if type == .stop { break }
            try thrift.skip(type)
        }
        thrift.readStructEnd()
    }
}

extension DictionaryPageHeader: ThriftReadable {
    public init(from thrift: ThriftCompactProtocol) throws {
        self.num_values = 0
        self.encoding = .plain
        thrift.readStructBegin()
        while true {
            let (type, id, boolVal) = try thrift.readFieldBegin()
            if type == .stop { break }
            switch id {
            case 1: self.num_values = try thrift.readI32()
            case 2: if let raw = try? thrift.readI32() { self.encoding = Encoding(rawValue: raw) ?? .plain }
            case 3: 
                if let val = boolVal { self.is_sorted = val }
                else { self.is_sorted = try thrift.readBool() }
            default: try thrift.skip(type)
            }
        }
        thrift.readStructEnd()
    }
}

extension DataPageHeaderV2: ThriftReadable {
    public init(from thrift: ThriftCompactProtocol) throws {
        self.num_values = 0
        self.num_nulls = 0
        self.num_rows = 0
        self.encoding = .plain
        self.definition_levels_byte_length = 0
        self.repetition_levels_byte_length = 0
        thrift.readStructBegin()
        while true {
            let (type, id, boolVal) = try thrift.readFieldBegin()
            if type == .stop { break }
            switch id {
            case 1: self.num_values = try thrift.readI32()
            case 2: self.num_nulls = try thrift.readI32()
            case 3: self.num_rows = try thrift.readI32()
            case 4: if let raw = try? thrift.readI32() { self.encoding = Encoding(rawValue: raw) ?? .plain }
            case 5: self.definition_levels_byte_length = try thrift.readI32()
            case 6: self.repetition_levels_byte_length = try thrift.readI32()
            case 7: 
                 if let val = boolVal { self.is_compressed = val }
                 else { self.is_compressed = try thrift.readBool() }
            case 8: self.statistics = try Statistics(from: thrift)
            default: try thrift.skip(type)
            }
        }
        thrift.readStructEnd()
    }
}
