import ArgumentParser
import Parquet
import Foundation

@main
struct ParquetCLI: ParsableCommand {
    @Argument(help: "Path to the parquet file")
    var path: String

    mutating func run() throws {
        let file = try ParquetFile(path: path)
        try file.read()
    }
}
