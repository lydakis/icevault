import CryptoKit
import Foundation
import XCTest
@testable import IceVault

final class FileScannerTests: XCTestCase {
    func testScanIncludesHiddenAndPackageFilesAndComputesSHA256() throws {
        let tempDirectory = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: tempDirectory) }

        let visibleFile = tempDirectory.appendingPathComponent("visible.txt")
        let hiddenFile = tempDirectory.appendingPathComponent(".hidden")
        let hiddenDirectory = tempDirectory.appendingPathComponent(".config", isDirectory: true)
        let hiddenNestedFile = hiddenDirectory.appendingPathComponent("settings.json")
        let packageDirectory = tempDirectory.appendingPathComponent("Sample.app", isDirectory: true)
        let packageNestedFile = packageDirectory.appendingPathComponent("Contents.txt")
        let dsStoreFile = tempDirectory.appendingPathComponent(".DS_Store")

        try FileManager.default.createDirectory(at: hiddenDirectory, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: packageDirectory, withIntermediateDirectories: true)

        try Data("hello".utf8).write(to: visibleFile)
        try Data("secret".utf8).write(to: hiddenFile)
        try Data("{}".utf8).write(to: hiddenNestedFile)
        try Data("package-data".utf8).write(to: packageNestedFile)
        try Data("ignore".utf8).write(to: dsStoreFile)

        var records: [FileRecord] = []
        try FileScanner().scan(sourceRoot: tempDirectory.path) { record in
            records.append(record)
        }
        let relativePaths = Set(records.map(\.relativePath))

        XCTAssertTrue(relativePaths.contains("visible.txt"))
        XCTAssertTrue(relativePaths.contains(".hidden"))
        XCTAssertTrue(relativePaths.contains(".config/settings.json"))
        XCTAssertTrue(relativePaths.contains("Sample.app/Contents.txt"))
        XCTAssertFalse(relativePaths.contains(".DS_Store"))

        let visibleRecord = try XCTUnwrap(records.first(where: { $0.relativePath == "visible.txt" }))
        XCTAssertEqual(visibleRecord.sha256, sha256Hex(of: Data("hello".utf8)))
        XCTAssertEqual(visibleRecord.sha256.count, 64)
    }

    func testStreamingScanPropagatesHandlerError() throws {
        enum TestError: Error {
            case stop
        }

        let tempDirectory = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: tempDirectory) }

        try Data("hello".utf8).write(to: tempDirectory.appendingPathComponent("file.txt"))

        XCTAssertThrowsError(
            try FileScanner().scan(sourceRoot: tempDirectory.path) { _ in
                throw TestError.stop
            }
        ) { error in
            XCTAssertTrue(error is TestError)
        }
    }

    func testScanHashChangesWhenFileContentChangesWithSameSizeAndTimestamp() throws {
        let tempDirectory = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: tempDirectory) }

        let fileURL = tempDirectory.appendingPathComponent("stable.txt")
        let fixedModificationDate = Date(timeIntervalSince1970: 1_700_000_000.25)

        try Data("abc".utf8).write(to: fileURL)
        try FileManager.default.setAttributes([.modificationDate: fixedModificationDate], ofItemAtPath: fileURL.path)
        let firstScan = try FileScanner().scan(sourceRoot: tempDirectory.path)
        let firstRecord = try XCTUnwrap(firstScan.first(where: { $0.relativePath == "stable.txt" }))

        try Data("xyz".utf8).write(to: fileURL)
        try FileManager.default.setAttributes([.modificationDate: fixedModificationDate], ofItemAtPath: fileURL.path)
        let secondScan = try FileScanner().scan(sourceRoot: tempDirectory.path)
        let secondRecord = try XCTUnwrap(secondScan.first(where: { $0.relativePath == "stable.txt" }))

        XCTAssertEqual(firstRecord.fileSize, secondRecord.fileSize)
        XCTAssertEqual(firstRecord.modifiedAt, secondRecord.modifiedAt)
        XCTAssertNotEqual(firstRecord.sha256, secondRecord.sha256)
    }

    func testScanSkipsUnreadableFilesWithoutFailingWholeScan() throws {
        let tempDirectory = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: tempDirectory) }

        let readableFile = tempDirectory.appendingPathComponent("readable.txt")
        let unreadableFile = tempDirectory.appendingPathComponent("unreadable.txt")

        try Data("ok".utf8).write(to: readableFile)
        try Data("nope".utf8).write(to: unreadableFile)
        try FileManager.default.setAttributes([.posixPermissions: 0o000], ofItemAtPath: unreadableFile.path)

        defer {
            try? FileManager.default.setAttributes([.posixPermissions: 0o644], ofItemAtPath: unreadableFile.path)
        }

        if FileManager.default.isReadableFile(atPath: unreadableFile.path) {
            throw XCTSkip("Environment permits reading a chmod 000 file.")
        }

        let records = try FileScanner().scan(sourceRoot: tempDirectory.path)
        let relativePaths = Set(records.map(\.relativePath))

        XCTAssertTrue(relativePaths.contains("readable.txt"))
        XCTAssertFalse(relativePaths.contains("unreadable.txt"))
    }

    func testStreamingScanMatchesArrayScan() throws {
        let tempDirectory = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: tempDirectory) }

        let files: [(String, String)] = [
            ("a.txt", "alpha"),
            ("nested/b.txt", "bravo"),
            (".hidden/c.txt", "charlie")
        ]

        for (relativePath, contents) in files {
            let fileURL = tempDirectory.appendingPathComponent(relativePath)
            try FileManager.default.createDirectory(
                at: fileURL.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
            try Data(contents.utf8).write(to: fileURL)
        }

        let scanner = FileScanner()
        let arrayScan = try scanner.scan(sourceRoot: tempDirectory.path)

        var streamingScan: [FileRecord] = []
        try scanner.scan(sourceRoot: tempDirectory.path) { record in
            streamingScan.append(record)
        }

        let sortedStreaming = streamingScan.sorted {
            $0.relativePath.localizedStandardCompare($1.relativePath) == .orderedAscending
        }

        XCTAssertEqual(arrayScan.map(\.relativePath), sortedStreaming.map(\.relativePath))
        XCTAssertEqual(arrayScan.map(\.sha256), sortedStreaming.map(\.sha256))
    }

    func testInventoryStatsCountsFilesAndBytesWithoutHashes() throws {
        let tempDirectory = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: tempDirectory) }

        let visibleFile = tempDirectory.appendingPathComponent("visible.txt")
        let hiddenFile = tempDirectory.appendingPathComponent(".hidden")
        let hiddenDirectory = tempDirectory.appendingPathComponent(".config", isDirectory: true)
        let hiddenNestedFile = hiddenDirectory.appendingPathComponent("settings.json")
        let packageDirectory = tempDirectory.appendingPathComponent("Sample.app", isDirectory: true)
        let packageNestedFile = packageDirectory.appendingPathComponent("Contents.txt")
        let dsStoreFile = tempDirectory.appendingPathComponent(".DS_Store")

        try FileManager.default.createDirectory(at: hiddenDirectory, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: packageDirectory, withIntermediateDirectories: true)

        try Data("hello".utf8).write(to: visibleFile)
        try Data("secret".utf8).write(to: hiddenFile)
        try Data("{}".utf8).write(to: hiddenNestedFile)
        try Data("package-data".utf8).write(to: packageNestedFile)
        try Data("ignore".utf8).write(to: dsStoreFile)

        let stats = try FileScanner().inventoryStats(sourceRoot: tempDirectory.path)

        XCTAssertEqual(stats.fileCount, 4)
        XCTAssertEqual(
            stats.totalBytes,
            Int64("hello".utf8.count + "secret".utf8.count + "{}".utf8.count + "package-data".utf8.count)
        )
    }

    func testScanMetadataEmitsRecordsWithoutHashes() async throws {
        let tempDirectory = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: tempDirectory) }

        let payload = Data("metadata-only".utf8)
        let fileURL = tempDirectory.appendingPathComponent("file.txt")
        try payload.write(to: fileURL)

        var records: [FileRecord] = []
        try await FileScanner().scanMetadata(sourceRoot: tempDirectory.path) { record in
            records.append(record)
        }

        let scannedRecord = try XCTUnwrap(records.first(where: { $0.relativePath == "file.txt" }))
        XCTAssertEqual(scannedRecord.fileSize, Int64(payload.count))
        XCTAssertEqual(scannedRecord.sha256, "")
    }

    private func makeTempDirectory() throws -> URL {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("IceVaultTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory
    }

    private func sha256Hex(of data: Data) -> String {
        SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    }
}
