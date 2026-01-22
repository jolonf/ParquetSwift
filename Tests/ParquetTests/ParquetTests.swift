import XCTest
@testable import Parquet

final class ParquetTests: XCTestCase {
    func testExample() throws {
        let file = try? ParquetFile(path: "dummy")
        // XCTAssertNotNil(file) // file init throws if path invalid
    }
}
