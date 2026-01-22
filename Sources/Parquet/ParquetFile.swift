import Foundation

public class ParquetFile {
    public let path: String
    private let handle: FileHandle
    public var metadata: FileMetaData?
    
    public init(path: String) throws {
        self.path = path
        guard let h = FileHandle(forReadingAtPath: path) else {
            throw NSError(domain: "Parquet", code: 1, userInfo: [NSLocalizedDescriptionKey: "Could not open file at \(path)"])
        }
        self.handle = h
    }
    
    deinit {
        try? handle.close()
    }
    
    public func read() throws {
        // 1. Read Footer Length
        let fileSize = handle.seekToEndOfFile()
        guard fileSize > 8 else { throw NSError(domain: "Parquet", code: 2, userInfo: [NSLocalizedDescriptionKey: "File too small"]) }
        
        // Seek to known footer length position: File Size - 8 bytes (4 for "PAR1" + 4 for FooterLength)
        try handle.seek(toOffset: fileSize - 8)
        let footerLengthData = handle.readData(ofLength: 4)
        let footerLength = footerLengthData.withUnsafeBytes { $0.load(as: Int32.self) }
        
        // 2. Read Magic Bytes (Verified at end)
        let magic = handle.readData(ofLength: 4)
        if String(data: magic, encoding: .ascii) != "PAR1" {
             print("Warning: Magic bytes at end were not PAR1")
        }
        
        // 3. Read Footer
        let footerOffset = fileSize - 8 - UInt64(footerLength)
        try handle.seek(toOffset: footerOffset)
        let footerData = handle.readData(ofLength: Int(footerLength))
        
        // 4. Parse Metadata
        let thriftReader = ThriftCompactProtocol(data: footerData)
        self.metadata = try FileMetaData(from: thriftReader)
        
        // Log basic info
    }
    
    public func columnReader(for name: String) throws -> ColumnReader? {
        guard let metadata = self.metadata else { throw NSError(domain: "Parquet", code: 5, userInfo: [NSLocalizedDescriptionKey: "No metadata loaded"]) }
        
        // Find column index (skipping root)
        var leafIndex = 0
        var found = false
        // schema[0] is root. Start at 1.
        for i in 1..<metadata.schema.count {
            let element = metadata.schema[i]
            if element.name == name {
                found = true
                break
            }
            // Logic for Leaf Nodes: num_children is nil or 0
            if element.num_children == nil || element.num_children == 0 {
                leafIndex += 1
            }
        }
        
        if !found { return nil }
        
        return ColumnReader(handle: handle, metadata: metadata, columnIndex: leafIndex)
    }
}
