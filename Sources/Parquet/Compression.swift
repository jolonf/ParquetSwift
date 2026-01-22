import Foundation
import Snappy

public protocol ParquetDecompressor {
    func decompress(data: Data, uncompressedSize: Int) throws -> Data
}

public struct Compression {
    public static func decompress(data: Data, codec: CompressionCodec, uncompressedSize: Int) throws -> Data {
        switch codec {
        case .uncompressed:
            return data
        case .snappy:
            return try data.uncompressedUsingSnappy()
        default:
            throw NSError(domain: "Parquet", code: 3, userInfo: [NSLocalizedDescriptionKey: "Unsupported codec: \(codec)"])
        }
    }
}
