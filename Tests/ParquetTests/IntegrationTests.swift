import XCTest
@testable import Parquet

final class IntegrationTests: XCTestCase {
    
    let testFileURL = URL(fileURLWithPath: FileManager.default.currentDirectoryPath).appendingPathComponent("test_data.parquet")
    
    override func setUp() async throws {
        // Download test file if needed
        if !FileManager.default.fileExists(atPath: testFileURL.path) {
            print("Downloading test parquet file...")
            let url = URL(string: "https://huggingface.co/datasets/wikitext/resolve/refs%2Fconvert%2Fparquet/wikitext-2-v1/test/0000.parquet")!
            let (data, _) = try await URLSession.shared.data(from: url)
            try data.write(to: testFileURL)
            print("Downloaded to \(testFileURL.path)")
        }
    }
    
    func testReadExternalFile() throws {
        guard FileManager.default.fileExists(atPath: testFileURL.path) else {
            XCTFail("Test file not found")
            return
        }
        
        let file = try ParquetFile(path: testFileURL.path)
        try file.read()
        
        XCTAssertNotNil(file.metadata)
        XCTAssertGreaterThanOrEqual(file.metadata?.version ?? 0, 1)
        // Wikitext test split has ~4k rows or so? Let's just check > 0
        XCTAssertGreaterThan(file.metadata?.num_rows ?? 0, 0)
        
        print("Schema: \(file.metadata?.schema.map { $0.name } ?? [])")
        
        // Read text column
        if let column = try file.columnReader(for: "text") {
             // Read first 5 values
             for i in 0..<5 {
                 if let val = try column.nextValue() {
                     print("Value \(i): \(val)")
                 }
             }
        } else {
             XCTFail("Could not create column reader for 'text'")
        }
    }
}
