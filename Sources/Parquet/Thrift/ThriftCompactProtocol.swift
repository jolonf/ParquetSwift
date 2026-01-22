import Foundation

public enum ThriftError: Error {
    case endOfFile
    case invalidVarInt
    case invalidString
    case protocolViolation(String)
}

public class ThriftCompactProtocol {
    private let data: Data
    private var position: Int
    public var bytesRead: Int { return position }
    
    // Field ID delta tracking
    private var lastFieldId: Int32 = 0
    private var fieldIdStack: [Int32] = []
    
    public init(data: Data) {
        self.data = data
        self.position = 0
    }
    
    // MARK: - Low Level Reading
    
    private func readByte() throws -> UInt8 {
        guard position < data.count else { throw ThriftError.endOfFile }
        let byte = data[position]
        position += 1
        return byte
    }
    
    private func readBytes(count: Int) throws -> Data {
        guard position + count <= data.count else { throw ThriftError.endOfFile }
        let subdata = data.subdata(in: position..<position+count)
        position += count
        return subdata
    }
    
    public func readVarInt32() throws -> Int32 {
        var result: Int32 = 0
        var shift: Int32 = 0
        while true {
            let byte = try readByte()
            result |= Int32(byte & 0x7f) << shift
            if (byte & 0x80) == 0 { break }
            shift += 7
            if shift >= 32 { throw ThriftError.invalidVarInt } // simplified check
        }
        return result
    }
    
    public func readVarInt64() throws -> Int64 {
        var result: Int64 = 0
        var shift: Int64 = 0
        while true {
            let byte = try readByte()
            result |= Int64(byte & 0x7f) << shift
            if (byte & 0x80) == 0 { break }
            shift += 7
            if shift >= 64 { throw ThriftError.invalidVarInt }
        }
        return result
    }
    
    private func zigzagToInt32(_ n: Int32) -> Int32 {
        return (n >> 1) ^ -(n & 1)
    }

    private func zigzagToInt64(_ n: Int64) -> Int64 {
        return (n >> 1) ^ -(n & 1)
    }

    public func readI16() throws -> Int16 {
        return Int16(zigzagToInt32(try readVarInt32()))
    }
    
    public func readI32() throws -> Int32 {
        return zigzagToInt32(try readVarInt32())
    }
    
    public func readI64() throws -> Int64 {
        return zigzagToInt64(try readVarInt64())
    }
    
    public func readDouble() throws -> Double {
        let bytes = try readBytes(count: 8)
        let bitPattern = bytes.withUnsafeBytes { $0.load(as: UInt64.self) }
        // Thrift uses little endian for double in Compact Protocol? No, actually it uses explicitly little endian
        // But usually wire format is LE. Let's assume LE for now. 
        // Wait, Compact Protocol specification says double is 8 bytes.
        return Double(bitPattern: UInt64(littleEndian: bitPattern))
    }
    
    public func readBinary() throws -> Data {
        let length = try readVarInt32()
        if length == 0 { return Data() }
        return try readBytes(count: Int(length))
    }
    
    public func readString() throws -> String {
        let data = try readBinary()
        guard let str = String(data: data, encoding: .utf8) else {
            throw ThriftError.invalidString
        }
        return str
    }
    
    public func readBool() throws -> Bool {
        // Boolean values are often encoded in the field header in Compact Protocol
        // Accessing this directly might be wrong if field reading didn't set state
        let byte = try readByte()
        return byte == 1 // Simple boolean_true / boolean_false types map to 1/2.
    }

    // MARK: - Structure Reading
    
    public enum TType: UInt8 {
        case stop = 0
        case boolean_true = 1 // encoded in type
        case boolean_false = 2 // encoded in type
        case byte = 3
        case i16 = 4
        case i32 = 5
        case i64 = 6
        case double = 7
        case binary = 8
        case list = 9
        case set = 10
        case map = 11
        case `struct` = 12
    }
    
    public func readStructBegin() {
        fieldIdStack.append(lastFieldId)
        lastFieldId = 0
    }
    
    public func readStructEnd() {
        lastFieldId = fieldIdStack.popLast() ?? 0
    }
    
    public func readFieldBegin() throws -> (type: TType, fieldId: Int32, modifier: Bool?) {
        let byte = try readByte()
        let typeRaw = byte & 0x0f
        guard let type = TType(rawValue: typeRaw) else {
            throw ThriftError.protocolViolation("Unknown type \(typeRaw)")
        }
        
        if type == .stop {
            return (.stop, 0, nil)
        }
        
        // Compact Protocol: high 4 bits are delta.
        let delta = (byte >> 4) & 0x0f
        var fieldId: Int32
        if delta == 0 {
            fieldId = try readI32() // zigzag int32
        } else {
            fieldId = lastFieldId + Int32(delta)
        }
        
        lastFieldId = fieldId
        
        // Handle immediate bools
        if type == .boolean_true {
            return (type, fieldId, true)
        } else if type == .boolean_false {
            return (type, fieldId, false)
        }
        
        return (type, fieldId, nil)
    }
    
    public func readMapBegin() throws -> (keyType: TType, valueType: TType, size: Int32) {
        let size = try readVarInt32()
        if size == 0 {
            return (.stop, .stop, 0)
        }
        let types = try readByte()
        let keyTypeRaw = (types >> 4) & 0x0f // high 4 bits
        let valTypeRaw = types & 0x0f // low 4 bits
        
        // In Compact Protocol 
        // "The types are encoded with the same values as the field header"
        // But need to be careful about mapping TType.
        
        return (
            TType(rawValue: keyTypeRaw) ?? .stop,
            TType(rawValue: valTypeRaw) ?? .stop,
            size
        )
    }
    
    public func readListBegin() throws -> (elementType: TType, size: Int32) {
        let byte = try readByte()
        var size = Int32((byte >> 4) & 0x0f)
        if size == 0x0f {
            size = try readVarInt32()
        }
        let typeRaw = byte & 0x0f
        return (TType(rawValue: typeRaw) ?? .stop, size)
    }
    
    public func skip(_ type: TType) throws {
        switch type {
        case .stop: break
        case .boolean_true, .boolean_false: break // value encoded in type
        case .byte: _ = try readByte()
        case .i16: _ = try readI16()
        case .i32: _ = try readI32()
        case .i64: _ = try readI64()
        case .double: _ = try readDouble()
        case .binary: _ = try readBinary() // reads length then bytes
        case .struct:
            readStructBegin()
            while true {
                let (fieldType, _, _) = try readFieldBegin()
                if fieldType == .stop { break }
                try skip(fieldType)
            }
            readStructEnd()
        case .map:
            let (keyType, valType, size) = try readMapBegin()
            for _ in 0..<size {
                try skip(keyType)
                try skip(valType)
            }
        case .list:
            let (elemType, size) = try readListBegin()
            for _ in 0..<size {
                try skip(elemType)
            }
        case .set:
            let (elemType, size) = try readListBegin() // sets are same as lists
            for _ in 0..<size {
                try skip(elemType)
            }
        }
    }
}
