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

    func testMetadataOnlySyncDoesNotRequeueUploadedFileWhenSizeAndTimestampMatch() throws {
        let database = try makeDatabaseService()
        let sourceRoot = "/tmp/source"
        let fixedDate = Date(timeIntervalSince1970: 1_700_000_325)

        var existing = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "unchanged.txt",
            fileSize: 6,
            modifiedAt: fixedDate,
            sha256: "existing-hash",
            glacierKey: "unchanged.txt",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&existing)

        let metadataOnlyScan = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "unchanged.txt",
            fileSize: 6,
            modifiedAt: fixedDate,
            sha256: "",
            glacierKey: "",
            uploadedAt: nil,
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.syncScannedFiles([metadataOnlyScan], for: sourceRoot)

        XCTAssertTrue(try database.pendingFiles(for: sourceRoot).isEmpty)
        let stored = try XCTUnwrap(
            database.allFiles().first(where: { $0.sourcePath == sourceRoot && $0.relativePath == "unchanged.txt" })
        )
        XCTAssertEqual(stored.sha256, "existing-hash")
        XCTAssertNotNil(stored.uploadedAt)
    }

    func testMetadataOnlySyncDoesNotRequeueUploadedFileWhenTimestampDiffIsWithinPrecisionTolerance() throws {
        let database = try makeDatabaseService()
        let sourceRoot = "/tmp/source"
        let storedDate = Date(timeIntervalSince1970: 1_700_000_326.123)
        let scannedDate = Date(timeIntervalSince1970: 1_700_000_326.1234)

        var existing = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "precision.txt",
            fileSize: 6,
            modifiedAt: storedDate,
            sha256: "existing-hash",
            glacierKey: "precision.txt",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&existing)

        let metadataOnlyScan = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "precision.txt",
            fileSize: 6,
            modifiedAt: scannedDate,
            sha256: "",
            glacierKey: "",
            uploadedAt: nil,
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.syncScannedFiles([metadataOnlyScan], for: sourceRoot)

        XCTAssertTrue(try database.pendingFiles(for: sourceRoot).isEmpty)
        let stored = try XCTUnwrap(
            database.allFiles().first(where: { $0.sourcePath == sourceRoot && $0.relativePath == "precision.txt" })
        )
        XCTAssertEqual(stored.sha256, "existing-hash")
        XCTAssertNotNil(stored.uploadedAt)
    }

    func testMetadataOnlySyncMarksUploadedFilePendingAndClearsHashWhenMetadataChanges() throws {
        let database = try makeDatabaseService()
        let sourceRoot = "/tmp/source"
        let originalDate = Date(timeIntervalSince1970: 1_700_000_340)
        let newerDate = Date(timeIntervalSince1970: 1_700_000_341)

        var existing = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "candidate.txt",
            fileSize: 10,
            modifiedAt: originalDate,
            sha256: "old-hash",
            glacierKey: "candidate.txt",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&existing)

        let metadataOnlyScan = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "candidate.txt",
            fileSize: 10,
            modifiedAt: newerDate,
            sha256: "",
            glacierKey: "",
            uploadedAt: nil,
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.syncScannedFiles([metadataOnlyScan], for: sourceRoot)

        let pending = try database.pendingFiles(for: sourceRoot)
        let updated = try XCTUnwrap(pending.first)
        XCTAssertEqual(pending.count, 1)
        XCTAssertEqual(updated.modifiedAt, newerDate)
        XCTAssertEqual(updated.sha256, "")
        XCTAssertEqual(updated.glacierKey, "")
        XCTAssertNil(updated.uploadedAt)
    }

    func testMetadataOnlySyncRequeuesUploadedLegacyRecordWhenStoredHashIsEmpty() throws {
        let database = try makeDatabaseService()
        let sourceRoot = "/tmp/source"
        let fixedDate = Date(timeIntervalSince1970: 1_700_000_345)

        var existing = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "legacy-empty-hash.txt",
            fileSize: 9,
            modifiedAt: fixedDate,
            sha256: "",
            glacierKey: "legacy-empty-hash.txt",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&existing)

        let scanToken = try database.beginScan(for: sourceRoot)
        let metadataOnlyScan = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "legacy-empty-hash.txt",
            fileSize: 9,
            modifiedAt: fixedDate,
            sha256: "",
            glacierKey: "",
            uploadedAt: nil,
            storageClass: FileRecord.deepArchiveStorageClass
        )

        let pendingFromBatch = try database.syncScannedFilesBatch(
            [metadataOnlyScan],
            for: sourceRoot,
            scanToken: scanToken
        )

        XCTAssertEqual(pendingFromBatch.map(\.relativePath), ["legacy-empty-hash.txt"])
        let stored = try XCTUnwrap(
            database.allFiles().first(where: {
                $0.sourcePath == sourceRoot && $0.relativePath == "legacy-empty-hash.txt"
            })
        )
        XCTAssertEqual(stored.sha256, "")
        XCTAssertNotNil(stored.uploadedAt)
    }

    func testStreamingSyncPreservesIncrementalDiffBehavior() throws {
        let database = try makeDatabaseService()
        let sourceRoot = "/tmp/source"
        let fixedDate = Date(timeIntervalSince1970: 1_700_000_350)

        var unchangedUploaded = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "unchanged.txt",
            fileSize: 4,
            modifiedAt: fixedDate,
            sha256: "same",
            glacierKey: "unchanged.txt",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        var changedUploaded = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "changed.txt",
            fileSize: 4,
            modifiedAt: fixedDate,
            sha256: "old",
            glacierKey: "changed.txt",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        var staleUploaded = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "stale.txt",
            fileSize: 4,
            modifiedAt: fixedDate,
            sha256: "stale",
            glacierKey: "stale.txt",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&unchangedUploaded)
        try database.insertFile(&changedUploaded)
        try database.insertFile(&staleUploaded)

        let changedScan = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "changed.txt",
            fileSize: 4,
            modifiedAt: fixedDate,
            sha256: "new",
            glacierKey: "",
            uploadedAt: nil,
            storageClass: FileRecord.deepArchiveStorageClass
        )
        let newScan = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "new.txt",
            fileSize: 7,
            modifiedAt: fixedDate,
            sha256: "new-file",
            glacierKey: "",
            uploadedAt: nil,
            storageClass: FileRecord.deepArchiveStorageClass
        )

        try database.syncScannedFiles(for: sourceRoot) { consume in
            try consume(changedScan)
            try consume(newScan)
            try consume(unchangedUploaded)
        }

        let all = try database.allFiles().filter { $0.sourcePath == sourceRoot }
        XCTAssertEqual(Set(all.map(\.relativePath)), ["changed.txt", "new.txt", "unchanged.txt"])

        let changed = try XCTUnwrap(all.first(where: { $0.relativePath == "changed.txt" }))
        XCTAssertEqual(changed.sha256, "new")
        XCTAssertEqual(changed.glacierKey, "")
        XCTAssertNil(changed.uploadedAt)

        let unchanged = try XCTUnwrap(all.first(where: { $0.relativePath == "unchanged.txt" }))
        XCTAssertEqual(unchanged.sha256, "same")
        XCTAssertNotNil(unchanged.uploadedAt)

        let inserted = try XCTUnwrap(all.first(where: { $0.relativePath == "new.txt" }))
        XCTAssertEqual(inserted.sha256, "new-file")
        XCTAssertNil(inserted.uploadedAt)
    }

    func testScanTokenSyncSupportsBatchedUpsertsAndStaleCleanup() throws {
        let database = try makeDatabaseService()
        let sourceRoot = "/tmp/source"
        let fixedDate = Date(timeIntervalSince1970: 1_700_000_375)

        var uploaded = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "uploaded.txt",
            fileSize: 10,
            modifiedAt: fixedDate,
            sha256: "uploaded-hash",
            glacierKey: "uploaded.txt",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        var stale = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "stale.txt",
            fileSize: 10,
            modifiedAt: fixedDate,
            sha256: "stale-hash",
            glacierKey: "stale.txt",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&uploaded)
        try database.insertFile(&stale)

        let scanToken = try database.beginScan(for: sourceRoot)

        let scannedUploaded = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "uploaded.txt",
            fileSize: 10,
            modifiedAt: fixedDate,
            sha256: "uploaded-hash",
            glacierKey: "",
            uploadedAt: nil,
            storageClass: FileRecord.deepArchiveStorageClass
        )
        let scannedNewA = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "new-a.txt",
            fileSize: 5,
            modifiedAt: fixedDate,
            sha256: "new-a",
            glacierKey: "",
            uploadedAt: nil,
            storageClass: FileRecord.deepArchiveStorageClass
        )
        let scannedNewB = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "new-b.txt",
            fileSize: 6,
            modifiedAt: fixedDate,
            sha256: "new-b",
            glacierKey: "",
            uploadedAt: nil,
            storageClass: FileRecord.deepArchiveStorageClass
        )

        let pendingFromBatch = try database.syncScannedFilesBatch(
            [scannedUploaded, scannedNewA, scannedNewB],
            for: sourceRoot,
            scanToken: scanToken
        )

        XCTAssertEqual(
            Set(pendingFromBatch.map(\.relativePath)),
            ["new-a.txt", "new-b.txt"]
        )

        try database.finishScan(for: sourceRoot, scanToken: scanToken)

        let all = try database.allFiles().filter { $0.sourcePath == sourceRoot }
        XCTAssertEqual(Set(all.map(\.relativePath)), ["new-a.txt", "new-b.txt", "uploaded.txt"])
        XCTAssertFalse(all.contains(where: { $0.relativePath == "stale.txt" }))
    }

    func testPendingMultipartUploadPersistsResumeToken() throws {
        let database = try makeDatabaseService()
        let sourceRoot = "/tmp/source"
        let fixedDate = Date(timeIntervalSince1970: 1_700_000_400)

        var file = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "large.bin",
            fileSize: 1_024,
            modifiedAt: fixedDate,
            sha256: "abc123",
            glacierKey: "",
            uploadedAt: nil,
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&file)
        let fileID = try XCTUnwrap(file.id)

        var upload = MultipartUploadRecord(
            fileRecordId: fileID,
            bucket: "bucket",
            key: "archive/large.bin",
            uploadId: "upload-1",
            resumeToken: "resume-token-v1",
            totalParts: 2,
            completedPartsJSON: "[]",
            createdAt: fixedDate,
            lastUpdatedAt: fixedDate
        )
        try database.insertMultipartUpload(&upload)

        let pending = try database.pendingMultipartUpload(bucket: "bucket", key: "archive/large.bin")
        XCTAssertEqual(pending?.uploadId, "upload-1")
        XCTAssertEqual(pending?.resumeToken, "resume-token-v1")
    }

    func testPendingMultipartUploadUsesDefaultResumeTokenWhenUnset() throws {
        let database = try makeDatabaseService()
        let sourceRoot = "/tmp/source"
        let fixedDate = Date(timeIntervalSince1970: 1_700_000_500)

        var file = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "large.bin",
            fileSize: 2_048,
            modifiedAt: fixedDate,
            sha256: "def456",
            glacierKey: "",
            uploadedAt: nil,
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&file)
        let fileID = try XCTUnwrap(file.id)

        var upload = MultipartUploadRecord(
            fileRecordId: fileID,
            bucket: "bucket",
            key: "archive/large-default.bin",
            uploadId: "upload-2",
            totalParts: 2,
            completedPartsJSON: "[]",
            createdAt: fixedDate,
            lastUpdatedAt: fixedDate
        )
        try database.insertMultipartUpload(&upload)

        let pending = try database.pendingMultipartUpload(bucket: "bucket", key: "archive/large-default.bin")
        XCTAssertEqual(pending?.resumeToken, "")
    }

    func testStreamingSyncScannedFilesRemovesStaleRecords() throws {
        let database = try makeDatabaseService()
        let sourceRoot = "/tmp/source"
        let fixedDate = Date(timeIntervalSince1970: 1_700_000_600)

        var staleRecord = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "stale.txt",
            fileSize: 1,
            modifiedAt: fixedDate,
            sha256: "old",
            glacierKey: "old-key",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&staleRecord)

        let freshRecord = FileRecord(
            sourcePath: sourceRoot,
            relativePath: "fresh.txt",
            fileSize: 2,
            modifiedAt: fixedDate,
            sha256: "new",
            glacierKey: "",
            uploadedAt: nil,
            storageClass: FileRecord.deepArchiveStorageClass
        )

        try database.syncScannedFiles(for: sourceRoot) { upsertRecord in
            try upsertRecord(freshRecord)
        }

        let allFiles = try database.allFiles()
        XCTAssertEqual(allFiles.count, 1)
        XCTAssertEqual(allFiles.first?.relativePath, "fresh.txt")
    }

    func testPendingSummaryAndBatchQuery() throws {
        let database = try makeDatabaseService()
        let sourceRoot = "/tmp/source"
        let fixedDate = Date(timeIntervalSince1970: 1_700_000_700)

        for index in 0..<3 {
            var record = FileRecord(
                sourcePath: sourceRoot,
                relativePath: "file-\(index).txt",
                fileSize: Int64(index + 1),
                modifiedAt: fixedDate,
                sha256: "hash-\(index)",
                glacierKey: "",
                uploadedAt: nil,
                storageClass: FileRecord.deepArchiveStorageClass
            )
            try database.insertFile(&record)
        }

        XCTAssertEqual(try database.pendingFileCount(for: sourceRoot), 3)
        XCTAssertEqual(try database.pendingTotalBytes(for: sourceRoot), 6)

        let firstBatch = try database.pendingFiles(for: sourceRoot, limit: 2)
        XCTAssertEqual(firstBatch.count, 2)
        XCTAssertEqual(firstBatch.map(\.relativePath), ["file-0.txt", "file-1.txt"])
    }

    private func makeDatabaseService() throws -> DatabaseService {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("IceVaultTests-DB-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let databaseURL = directory.appendingPathComponent("icevault.sqlite")
        return try DatabaseService(databaseURL: databaseURL)
    }
}
