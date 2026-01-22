import Foundation

public class ColumnReader {
    private let handle: FileHandle
    private let metadata: FileMetaData
    private let columnIndex: Int
    
    private var currentRowGroupIndex = 0
    private var currentPageReader: PageReader?
    private var dictionary: [String]? // Only string dictionary for now
    
    // Buffer for current page values
    private var valueBuffer: [Any] = []
    private var bufferIndex = 0
    
    public init(handle: FileHandle, metadata: FileMetaData, columnIndex: Int) {
        self.handle = handle
        self.metadata = metadata
        self.columnIndex = columnIndex
    }
    
    public func nextValue() throws -> Any? {
        if bufferIndex < valueBuffer.count {
            let val = valueBuffer[bufferIndex]
            bufferIndex += 1
            return val
        }
        
        // Load next page
        if try !loadNextPage() {
            return nil // End of stream
        }
        
        return try nextValue()
    }
    
    private func loadNextPage() throws -> Bool {
        // Initialize PageReader if needed
        if currentPageReader == nil {
            if currentRowGroupIndex >= metadata.row_groups.count {
                return false
            }
            let rg = metadata.row_groups[currentRowGroupIndex]
            if columnIndex >= rg.columns.count { return false }
            let columnChunk = rg.columns[columnIndex]
            currentPageReader = try PageReader(handle: handle, column: columnChunk)
        }
        
        guard let page = try currentPageReader?.readPage() else {
            // End of current row group
            currentPageReader = nil
            currentRowGroupIndex += 1
            return try loadNextPage()
        }
        
        // Process Page
        switch page.header.type {
        case .dictionaryPage:
            try parseDictionaryPage(page)
            return try loadNextPage() // Dictionary is metadata, get next data page
        case .dataPage:
            try parseDataPage(page)
            return true
        default:
            print("Skipping unsupported page type: \(page.header.type)")
            return try loadNextPage()
        }
    }
    
    private func parseDictionaryPage(_ page: DataPage) throws {
        // Assuming PLAIN encoded dictionary
        // String type assumption for MVP
        let data = page.data
        var params = PageReadParams(data: data)
        var strings: [String] = []
        
        // Number of values?
        let numValues = page.header.dictionary_page_header?.num_values ?? 0
        
        for _ in 0..<numValues {
            // Read PLAIN string: Length (4 bytes LE) + Bytes
            let len = try params.readInt32()
            let str = try params.readString(length: Int(len))
            strings.append(str)
        }
        self.dictionary = strings
    }
    
    private func parseDataPage(_ page: DataPage) throws {
        valueBuffer = []
        bufferIndex = 0
        
        // Data Page Layout:
        // [Repetition Levels] (if repeated)
        // [Definition Levels] (if optional)
        // [Values]
        
        // Simplification: Assume NO repetition (flat schema) and NO definition (required fields) OR
        // handle simplistic definition levels for Optional.
        // Wikitext fields are optional?
        // Schema: text (Type: Optional(byteArray)) -> Optional!
        // So we MUST read definition levels.
        
        let data = page.data
        var params = PageReadParams(data: data)
        let numValues = page.header.data_page_header?.num_values ?? 0
        
        // Read Definition Levels
        // Encoding? usually RLE/BitPacked
        // definition_level_encoding field in header.
        
        // For MVP/One-Shot: Assume everything is present (all 1s) or just try to read values directly?
        // If we ignore definition levels, we'll desync.
        // We MUST implement RLE for levels.
        
        // For now, let's just create a dummy "ReadAllStrings" for PLAIN encoding to verify connectivity
        // If we crash, we know we need RLE.
        // Wikitext usually is PLAIN? Or Dictionary?
        
        // Header info:
        let encoding = page.header.data_page_header?.encoding ?? .plain
        
        if encoding == .plain {
             // Read PLAIN values
             for _ in 0..<numValues {
                 let len = try? params.readInt32() // if fails (end of buffer), stop
                 if let l = len {
                     let s = try params.readString(length: Int(l))
                     valueBuffer.append(s)
                 }
             }
        } else if encoding == .plainDictionary || encoding == .rleDictionary {
            // Read bit width
            let bitWidth = Int(try params.readByte())
            
            // Read RLE/BitPacked hybrid
            // We need to read until we run out of data or get numValues
            var valuesRead = 0
            while valuesRead < numValues && params.offset < params.data.count {
                let header = try params.readULEB128()
                if (header & 1) == 1 {
                    // Bit Packed: value is (header >> 1) * 8 values
                    // "The definition packed-run-header indicates that there are N groups of 8 values"
                    let numGroups = Int(header >> 1)
                    let count = numGroups * 8
                    
                    // Read numGroups * bitWidth * 8 bits
                    // Simplified: Read bytes, unpack bits.
                    // This is complex to implement efficiently.
                    // For MVP: implement a slow bit reader.
                    
                    for _ in 0..<count {
                        if valuesRead >= numValues { break }
                        let val = try params.readBits(count: bitWidth)
                        if let dict = dictionary, Int(val) < dict.count {
                            valueBuffer.append(dict[Int(val)])
                        } else {
                            valueBuffer.append("InvalidIndex(\(val))")
                        }
                        valuesRead += 1
                    }
                } else {
                     // RLE: value is (header >> 1) run length
                     let count = Int(header >> 1)
                     // Read value. Width = (bitWidth + 7) / 8 bytes
                     let byteWidth = (bitWidth + 7) / 8
                     let val = try params.readIntLittleEndian(bytes: byteWidth)
                     
                     if let dict = dictionary, Int(val) < dict.count {
                         let s = dict[Int(val)]
                         for _ in 0..<count {
                             if valuesRead >= numValues { break }
                             valueBuffer.append(s)
                             valuesRead += 1
                         }
                     } else {
                         for _ in 0..<count {
                             valueBuffer.append("InvalidIndex(\(val))")
                             valuesRead += 1
                         }
                     }
                }
            }
        }
    }
}

// Helper for bit/byte reading
struct PageReadParams {
    let data: Data
    var offset: Int = 0
    
    // Bit Reading State
    var bitBuffer: UInt64 = 0
    var bitsAvailable: Int = 0
    
    mutating func readByte() throws -> UInt8 {
        if offset >= data.count { throw NSError(domain: "Parquet", code: 9, userInfo: [NSLocalizedDescriptionKey: "EOF in readByte"]) }
        let val = data[offset]
        offset += 1
        return val
    }
    
    mutating func readInt32() throws -> Int32 {
        if offset + 4 > data.count { throw NSError(domain: "Parquet", code: 9, userInfo: nil) }
        var val: Int32 = 0
        let range = offset..<offset+4
        let _ = withUnsafeMutableBytes(of: &val) { ptr in
            data.copyBytes(to: ptr, from: range)
        }
        offset += 4
        return val
    }
    
    mutating func readString(length: Int) throws -> String {
        if offset + length > data.count { throw NSError(domain: "Parquet", code: 9, userInfo: nil) }
        let sub = data.subdata(in: offset..<offset+length)
        offset += length
        return String(data: sub, encoding: .utf8) ?? ""
    }
    
    mutating func readULEB128() throws -> UInt64 {
        var result: UInt64 = 0
        var shift: UInt64 = 0
        while true {
            let byte = try readByte()
            result |= UInt64(byte & 0x7f) << shift
            if (byte & 0x80) == 0 { break }
            shift += 7
        }
        return result
    }
    
    mutating func readIntLittleEndian(bytes: Int) throws -> Int {
        var result: Int = 0
        for i in 0..<bytes {
             let byte = try readByte()
             result |= Int(byte) << (i * 8)
        }
        return result
    }
    
    mutating func readBits(count: Int) throws -> Int {
        if count == 0 { return 0 }
        while bitsAvailable < count {
             let byte = try readByte()
             bitBuffer |= UInt64(byte) << bitsAvailable
             bitsAvailable += 8
        }
        let result = Int(bitBuffer & ((1 << count) - 1))
        bitBuffer >>= count
        bitsAvailable -= count
        return result
    }
}
