// swift-tools-version: 5.9
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "ParquetSwift",
    platforms: [
        .macOS(.v11)
    ],
    products: [
        .library(
            name: "Parquet",
            targets: ["Parquet"]),
        .executable(
            name: "ParquetCLI",
            targets: ["ParquetCLI"])
    ],
    dependencies: [
        // Using a pure Swift (or C-bundled) implementation of Snappy
        .package(url: "https://github.com/lovetodream/swift-snappy.git", from: "1.0.0"), 
        // useful for the CLI
        .package(url: "https://github.com/apple/swift-argument-parser", from: "1.2.0"),
    ],
    targets: [
        .target(
            name: "Parquet",
            dependencies: [
                .product(name: "Snappy", package: "swift-snappy")
            ]
        ),
        .executableTarget(
            name: "ParquetCLI",
            dependencies: [
                "Parquet",
                .product(name: "ArgumentParser", package: "swift-argument-parser")
            ]
        ),
        .testTarget(
            name: "ParquetTests",
            dependencies: ["Parquet"]),
    ]
)
