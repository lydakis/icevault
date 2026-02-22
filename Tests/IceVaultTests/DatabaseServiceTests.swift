import Foundation
import XCTest
@testable import IceVault

final class DatabaseServiceTests: XCTestCase {
    func testSyncMarksUploadedFilePendingWhenHashChangesWithoutMetadataChanges() throws {
        let database = try makeDatabaseService()
        let sourceRoot = "/tmp/source"
        let fixedDate = Date(timeIntervalSince1970: 1_700_000_100)

        var existing = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "file.txt",
            fileSize: 3,
            modifiedAt: fixedDate,
            sha256: "aaa",
            glacierKey: "old-key",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&existing)

        let scanned = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "file.txt",
            fileSize: 3,
            modifiedAt: fixedDate,
            sha256: "bbb",
            glacierKey: "",
            uploadedAt: nil,
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.syncScannedFiles([scanned], for: sourceRoot)

        let pending = try database.pendingFiles(for: sourceRoot)
        let updated = try XCTUnwrap(pending.first)

        XCTAssertEqual(pending.count, 1)
        XCTAssertEqual(updated.sha256, "bbb")
        XCTAssertEqual(updated.glacierKey, "")
        XCTAssertNil(updated.uploadedAt)
    }

    func testSyncMarksLegacyEmptyHashUploadedFilePendingForRebaseline() throws {
        let database = try makeDatabaseService()
        let sourceRoot = "/tmp/source"
        let fixedDate = Date(timeIntervalSince1970: 1_700_000_200)

        var legacyRecord = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "legacy.txt",
            fileSize: 5,
            modifiedAt: fixedDate,
            sha256: "",
            glacierKey: "legacy-key",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&legacyRecord)

        let scanned = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "legacy.txt",
            fileSize: 5,
            modifiedAt: fixedDate,
            sha256: "hash-after-upgrade",
            glacierKey: "",
            uploadedAt: nil,
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.syncScannedFiles([scanned], for: sourceRoot)

        let pending = try database.pendingFiles(for: sourceRoot)
        let updated = try XCTUnwrap(pending.first)

        XCTAssertEqual(pending.count, 1)
        XCTAssertEqual(updated.sha256, "hash-after-upgrade")
        XCTAssertEqual(updated.glacierKey, "")
        XCTAssertNil(updated.uploadedAt)
    }

    func testSyncDoesNotRequeueUploadedFileWhenOnlyTimestampChanges() throws {
        let database = try makeDatabaseService()
        let sourceRoot = "/tmp/source"
        let originalDate = Date(timeIntervalSince1970: 1_700_000_300)
        let newerDate = Date(timeIntervalSince1970: 1_700_000_300.987)

        var existing = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "same-content.txt",
            fileSize: 4,
            modifiedAt: originalDate,
            sha256: "same-hash",
            glacierKey: "key",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&existing)

        let scanned = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "same-content.txt",
            fileSize: 4,
            modifiedAt: newerDate,
            sha256: "same-hash",
            glacierKey: "",
            uploadedAt: nil,
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.syncScannedFiles([scanned], for: sourceRoot)

        let pending = try database.pendingFiles(for: sourceRoot)
        XCTAssertTrue(pending.isEmpty)
    }

    private func makeDatabaseService() throws -> DatabaseService {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("IceVaultTests-DB-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let databaseURL = directory.appendingPathComponent("icevault.sqlite")
        return try DatabaseService(databaseURL: databaseURL)
    }
}
