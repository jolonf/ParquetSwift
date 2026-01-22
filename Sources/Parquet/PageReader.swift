import Foundation

public struct DataPage {
    public let header: PageHeader
    public let data: Data // Decompressed data
}

public class PageReader {
    private let handle: FileHandle
    private let codec: CompressionCodec
    private var currentOffset: UInt64
    private let endOffset: UInt64
    
    public init(handle: FileHandle, column: ColumnChunk) throws {
        self.handle = handle
        guard let meta = column.meta_data else {
            throw NSError(domain: "Parquet", code: 4, userInfo: [NSLocalizedDescriptionKey: "Missing column metadata"])
        }
        self.codec = meta.codec
        // Start at dictionary page if present, else data page
        if let dictOffset = meta.dictionary_page_offset, dictOffset > 0 {
            self.currentOffset = UInt64(dictOffset)
        } else {
            self.currentOffset = UInt64(meta.data_page_offset)
        }
        self.endOffset = self.currentOffset + UInt64(meta.total_compressed_size)
    }
    
    public func readPage() throws -> DataPage? {
        if currentOffset >= endOffset {
            return nil
        }
        
        try handle.seek(toOffset: currentOffset)
        
        // 1. Read Page Header (Thrift)
        // We need to read "enough" bytes to parse the header, but we don't know the size.
        // TCompactProtocol doesn't need to know size if we just parse the struct.
        // But we need to feed it a buffer.
        // Read header. Use larger buffer (64KB) or full remaining to avoid EOF on large headers
        let remaining = endOffset - currentOffset
        let headerReadLen = min(65536, Int(remaining))
        print("DEBUG: Reading page header at \(currentOffset). Remaining in chunk: \(remaining). Reading buf: \(headerReadLen)")
        
        let headerData = handle.readData(ofLength: headerReadLen)
        
        let thriftReader = ThriftCompactProtocol(data: headerData)
        let header = try PageHeader(from: thriftReader)
        // Validates header type is readable
        let headerSize = thriftReader.bytesRead 
        currentOffset += UInt64(headerSize)
        
        // 2. Read Page Data
        let compressedSize = Int(header.compressed_page_size)
        try handle.seek(toOffset: currentOffset) // Seek to exactly after header
        let pageData = handle.readData(ofLength: compressedSize)
        if pageData.count != compressedSize {
             print("ERROR: Read mismatch. Expected \(compressedSize), got \(pageData.count)")
        }
        currentOffset += UInt64(compressedSize)
        
        // 3. Decompress
        let uncompressedData = try Compression.decompress(data: pageData, codec: codec, uncompressedSize: Int(header.uncompressed_page_size))
        
        return DataPage(header: header, data: uncompressedData)
    }
}
