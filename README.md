> [!IMPORTANT]
> This project was created using Google's Antigravity. Use at your own risk.

# ParquetSwift

A **pure Swift** library for reading Apache Parquet files.

This library is designed to be a lightweight, dependency-free (apart from compression) way to read Parquet files on Apple platforms, with a specific focus on compatibility with **Hugging Face** datasets (which heavily use Snappy compression and Dictionary encoding).

## Features

- **Pure Swift Implementation**: No heavy C++ wrappers (like Arrow or DuckDB) required.
- **Thrift Metadata Parsing**: Includes a custom, lightweight Thrift Compact Protocol reader to parse file metadata.
- **Compression**: Supports **Snappy** decompression (via `swift-snappy`).
- **Encodings Supported**:
  - `PLAIN`
  - `PLAIN_DICTIONARY`
  - `RLE` (Run Length Encoding) & `BitPacked` (Hybrid)

## Installation

Add this package to your `Package.swift` dependencies:

```swift
dependencies: [
    .package(url: "https://github.com/jolonf/ParquetSwift.git", from: "0.0.1")
]
```

## Usage

### Reading a File

```swift
import Parquet

// 1. Open the Parquet file
let file = try ParquetFile(path: "path/to/data.parquet")

// 2. Parse Metadata (Footer)
try file.read()

print("Number of Rows: \(file.metadata?.num_rows ?? 0)")

// 3. Inspect the Schema
if let schema = file.metadata?.schema {
    for column in schema {
        print("Column: \(column.name)")
    }
}

// 4. Read Column Data
// Get a reader for a specific column
if let reader = try file.columnReader(for: "text") {
    // Iterate over values
    while let value = try reader.nextValue() {
        print(value)
    }
}
```

## Requirements

- Swift 5.5+
- macOS 11.0+ (Required for Snappy compression)
