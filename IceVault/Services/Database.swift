import Foundation
import GRDB

final class DatabaseService: @unchecked Sendable {
    typealias ScannedFileConsumer = (FileRecord) throws -> Void
    typealias ScannedFileStream = (_ consume: ScannedFileConsumer) throws -> Void

    private static let metadataTimestampTolerance: TimeInterval = 0.001

    private let dbQueue: DatabaseQueue

    init(databaseURL: URL? = nil) throws {
        let resolvedDatabaseURL: URL
        if let databaseURL {
            resolvedDatabaseURL = databaseURL
        } else {
            resolvedDatabaseURL = try Self.defaultDatabaseURL()
        }

        try FileManager.default.createDirectory(
            at: resolvedDatabaseURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )

        dbQueue = try DatabaseQueue(path: resolvedDatabaseURL.path)
        try Self.makeMigrator().migrate(dbQueue)
    }

    func insertFile(_ record: inout FileRecord) throws {
        try dbQueue.write { db in
            try record.insert(db)
        }
    }

    func fetchFile(id: Int64) throws -> FileRecord? {
        try dbQueue.read { db in
            try FileRecord.fetchOne(db, key: id)
        }
    }

    func allFiles() throws -> [FileRecord] {
        try dbQueue.read { db in
            try FileRecord
                .order(FileRecord.Columns.sourcePath, FileRecord.Columns.relativePath)
                .fetchAll(db)
        }
    }

    func updateFile(_ record: FileRecord) throws {
        let mutableRecord = record
        try dbQueue.write { db in
            try mutableRecord.update(db)
        }
    }

    func updateFileSHA256(id: Int64, sha256: String) throws {
        let normalizedSHA256 = sha256.trimmingCharacters(in: .whitespacesAndNewlines)
        try dbQueue.write { db in
            try db.execute(
                sql: """
                UPDATE \(FileRecord.databaseTableName)
                SET sha256 = ?
                WHERE id = ?
                """,
                arguments: [normalizedSHA256, id]
            )
        }
    }

    @discardableResult
    func deleteFile(id: Int64) throws -> Bool {
        try dbQueue.write { db in
            try FileRecord.deleteOne(db, key: id)
        }
    }

    func insertMultipartUpload(_ record: inout MultipartUploadRecord) throws {
        try dbQueue.write { db in
            try record.insert(db)
        }
    }

    func updateCompletedParts(
        id: Int64,
        completedPartsJSON: String,
        lastUpdatedAt: Date = Date()
    ) throws {
        try dbQueue.write { db in
            try db.execute(
                sql: """
                UPDATE \(MultipartUploadRecord.databaseTableName)
                SET completedPartsJSON = ?, lastUpdatedAt = ?
                WHERE id = ?
                """,
                arguments: [completedPartsJSON, lastUpdatedAt, id]
            )
        }
    }

    @discardableResult
    func deleteMultipartUpload(id: Int64) throws -> Bool {
        try dbQueue.write { db in
            try MultipartUploadRecord.deleteOne(db, key: id)
        }
    }

    func pendingMultipartUploads() throws -> [MultipartUploadRecord] {
        try dbQueue.read { db in
            try MultipartUploadRecord
                .order(MultipartUploadRecord.Columns.lastUpdatedAt.desc)
                .fetchAll(db)
        }
    }

    func pendingMultipartUpload(bucket: String, key: String) throws -> MultipartUploadRecord? {
        try dbQueue.read { db in
            try MultipartUploadRecord
                .filter(MultipartUploadRecord.Columns.bucket == bucket)
                .filter(MultipartUploadRecord.Columns.key == key)
                .order(MultipartUploadRecord.Columns.lastUpdatedAt.desc)
                .fetchOne(db)
        }
    }

    func pendingFiles(for sourceRoot: String) throws -> [FileRecord] {
        try dbQueue.read { db in
            try FileRecord
                .filter(FileRecord.Columns.sourcePath == sourceRoot)
                .filter(FileRecord.Columns.uploadedAt == nil)
                .order(FileRecord.Columns.relativePath)
                .fetchAll(db)
        }
    }

    func pendingFiles(for sourceRoot: String, limit: Int) throws -> [FileRecord] {
        let normalizedLimit = max(limit, 0)
        guard normalizedLimit > 0 else {
            return []
        }

        return try dbQueue.read { db in
            try FileRecord
                .filter(FileRecord.Columns.sourcePath == sourceRoot)
                .filter(FileRecord.Columns.uploadedAt == nil)
                .order(FileRecord.Columns.relativePath)
                .limit(normalizedLimit)
                .fetchAll(db)
        }
    }

    func pendingFileCount(for sourceRoot: String) throws -> Int {
        try dbQueue.read { db in
            try Int.fetchOne(
                db,
                sql: """
                SELECT COUNT(*)
                FROM \(FileRecord.databaseTableName)
                WHERE sourcePath = ? AND uploadedAt IS NULL
                """,
                arguments: [sourceRoot]
            ) ?? 0
        }
    }

    func pendingTotalBytes(for sourceRoot: String) throws -> Int64 {
        try dbQueue.read { db in
            try Int64.fetchOne(
                db,
                sql: """
                SELECT COALESCE(SUM(fileSize), 0)
                FROM \(FileRecord.databaseTableName)
                WHERE sourcePath = ? AND uploadedAt IS NULL
                """,
                arguments: [sourceRoot]
            ) ?? 0
        }
    }

    func uploadedFiles(for sourceRoot: String) throws -> [FileRecord] {
        try dbQueue.read { db in
            try FileRecord
                .filter(FileRecord.Columns.sourcePath == sourceRoot)
                .filter(FileRecord.Columns.uploadedAt != nil)
                .order(FileRecord.Columns.relativePath)
                .fetchAll(db)
        }
    }

    func uploadedFiles(
        for sourceRoot: String,
        afterRelativePath: String?,
        limit: Int,
        uploadedBefore: Date? = nil
    ) throws -> [FileRecord] {
        try dbQueue.read { db in
            var request = FileRecord
                .filter(FileRecord.Columns.sourcePath == sourceRoot)
                .filter(FileRecord.Columns.uploadedAt != nil)
                .order(FileRecord.Columns.relativePath)
                .limit(max(limit, 0))

            if let afterRelativePath {
                request = request.filter(FileRecord.Columns.relativePath > afterRelativePath)
            }

            if let uploadedBefore {
                request = request.filter(FileRecord.Columns.uploadedAt <= uploadedBefore)
            }

            return try request.fetchAll(db)
        }
    }

    func syncScannedFiles(_ scannedFiles: [FileRecord], for sourceRoot: String) throws {
        try syncScannedFiles(for: sourceRoot) { consume in
            for scanned in scannedFiles {
                try consume(scanned)
            }
        }
    }

    func syncScannedFiles(
        for sourceRoot: String,
        streamScannedFiles: ScannedFileStream
    ) throws {
        let scanToken = try beginScan(for: sourceRoot)
        try streamScannedFiles { scanned in
            _ = try syncScannedFile(scanned, for: sourceRoot, scanToken: scanToken)
        }
        try finishScan(for: sourceRoot, scanToken: scanToken)
    }

    func beginScan(for sourceRoot: String) throws -> String {
        let scanToken = UUID().uuidString
        try dbQueue.write { db in
            try db.execute(
                sql: """
                UPDATE \(FileRecord.databaseTableName)
                SET scanToken = NULL
                WHERE sourcePath = ?
                """,
                arguments: [sourceRoot]
            )
        }
        return scanToken
    }

    @discardableResult
    func syncScannedFile(
        _ scanned: FileRecord,
        for sourceRoot: String,
        scanToken: String
    ) throws -> FileRecord? {
        try syncScannedFilesBatch([scanned], for: sourceRoot, scanToken: scanToken).first
    }

    func syncScannedFilesBatch(
        _ scannedFiles: [FileRecord],
        for sourceRoot: String,
        scanToken: String
    ) throws -> [FileRecord] {
        guard !scannedFiles.isEmpty else {
            return []
        }

        return try dbQueue.write { db in
            var pendingRecords: [FileRecord] = []
            pendingRecords.reserveCapacity(scannedFiles.count)

            for scanned in scannedFiles {
                var normalizedScanned = scanned
                normalizedScanned.sourcePath = sourceRoot
                normalizedScanned.scanToken = scanToken
                normalizedScanned.sha256 = normalizedScanned.sha256.trimmingCharacters(in: .whitespacesAndNewlines)

                if var existing = try FileRecord
                    .filter(FileRecord.Columns.sourcePath == sourceRoot)
                    .filter(FileRecord.Columns.relativePath == normalizedScanned.relativePath)
                    .fetchOne(db) {
                    existing.scanToken = scanToken

                    if normalizedScanned.sha256.isEmpty {
                        if Self.hasLikelyMetadataChanged(existing: existing, comparedTo: normalizedScanned) {
                            existing.fileSize = normalizedScanned.fileSize
                            existing.modifiedAt = normalizedScanned.modifiedAt
                            // Hash becomes unknown until lazy hashing resolves it near upload.
                            existing.sha256 = ""
                            existing.glacierKey = ""
                            existing.uploadedAt = nil
                            existing.storageClass = FileRecord.deepArchiveStorageClass
                            try existing.update(db)
                            pendingRecords.append(existing)
                        } else {
                            try existing.update(db)
                            let hasMissingStoredHash = existing.sha256
                                .trimmingCharacters(in: .whitespacesAndNewlines)
                                .isEmpty
                            if existing.uploadedAt == nil || hasMissingStoredHash {
                                pendingRecords.append(existing)
                            }
                        }
                    } else {
                        if Self.hasContentChanged(existing: existing, comparedTo: normalizedScanned) {
                            existing.fileSize = normalizedScanned.fileSize
                            existing.modifiedAt = normalizedScanned.modifiedAt
                            existing.sha256 = normalizedScanned.sha256
                            existing.glacierKey = ""
                            existing.uploadedAt = nil
                            existing.storageClass = FileRecord.deepArchiveStorageClass
                            try existing.update(db)
                            pendingRecords.append(existing)
                        } else {
                            // Keep metadata in sync even when hash confirms the object is unchanged.
                            existing.fileSize = normalizedScanned.fileSize
                            existing.modifiedAt = normalizedScanned.modifiedAt
                            try existing.update(db)
                            if existing.uploadedAt == nil {
                                pendingRecords.append(existing)
                            }
                        }
                    }

                    continue
                }

                var newRecord = normalizedScanned
                newRecord.sourcePath = sourceRoot
                newRecord.glacierKey = ""
                newRecord.uploadedAt = nil
                newRecord.storageClass = FileRecord.deepArchiveStorageClass
                newRecord.scanToken = scanToken
                try newRecord.insert(db)
                pendingRecords.append(newRecord)
            }

            return pendingRecords
        }
    }

    func finishScan(for sourceRoot: String, scanToken: String) throws {
        try dbQueue.write { db in
            try db.execute(
                sql: """
                DELETE FROM \(FileRecord.databaseTableName)
                WHERE sourcePath = ?
                  AND (scanToken IS NULL OR scanToken != ?)
                """,
                arguments: [sourceRoot, scanToken]
            )
        }
    }

    func markUploaded(id: Int64, glacierKey: String) throws {
        try markUploaded(records: [(id: id, glacierKey: glacierKey)])
    }

    func markUploaded(records: [(id: Int64, glacierKey: String)]) throws {
        guard !records.isEmpty else {
            return
        }

        try dbQueue.write { db in
            let uploadedAt = Date()
            for record in records {
                try db.execute(
                    sql: """
                    UPDATE \(FileRecord.databaseTableName)
                    SET uploadedAt = ?, glacierKey = ?, storageClass = ?
                    WHERE id = ?
                    """,
                    arguments: [uploadedAt, record.glacierKey, FileRecord.deepArchiveStorageClass, record.id]
                )
            }
        }
    }

    func markPending(id: Int64) throws {
        try dbQueue.write { db in
            try db.execute(
                sql: """
                UPDATE \(FileRecord.databaseTableName)
                SET uploadedAt = NULL, storageClass = ?
                WHERE id = ?
                """,
                arguments: [FileRecord.deepArchiveStorageClass, id]
            )
        }
    }

    func fileCount() throws -> Int {
        try dbQueue.read { db in
            try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM \(FileRecord.databaseTableName)") ?? 0
        }
    }

    func uploadedCount() throws -> Int {
        try dbQueue.read { db in
            try Int.fetchOne(
                db,
                sql: "SELECT COUNT(*) FROM \(FileRecord.databaseTableName) WHERE uploadedAt IS NOT NULL"
            ) ?? 0
        }
    }

    private static func defaultDatabaseURL() throws -> URL {
        let fileManager = FileManager.default
        let appSupportURL = try fileManager.url(
            for: .applicationSupportDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )
        let directory = appSupportURL.appendingPathComponent("IceVault", isDirectory: true)
        try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory.appendingPathComponent("icevault.sqlite")
    }

    private static func makeMigrator() -> DatabaseMigrator {
        var migrator = DatabaseMigrator()

        migrator.registerMigration("createFileRecords") { db in
            try db.create(table: FileRecord.databaseTableName) { table in
                table.autoIncrementedPrimaryKey("id")
                table.column("sourcePath", .text).notNull()
                table.column("relativePath", .text).notNull()
                table.column("fileSize", .integer).notNull()
                table.column("modifiedAt", .datetime).notNull()
                table.column("sha256", .text).notNull()
                table.column("glacierKey", .text).notNull().defaults(to: "")
                table.column("uploadedAt", .datetime)
                table.column("storageClass", .text).notNull().defaults(to: FileRecord.deepArchiveStorageClass)
            }

            try db.create(
                index: "idx_file_records_source_relative",
                on: FileRecord.databaseTableName,
                columns: ["sourcePath", "relativePath"],
                unique: true
            )

            try db.create(
                index: "idx_file_records_uploaded_at",
                on: FileRecord.databaseTableName,
                columns: ["uploadedAt"]
            )
        }

        migrator.registerMigration("createMultipartUploadRecords") { db in
            try db.create(table: MultipartUploadRecord.databaseTableName) { table in
                table.autoIncrementedPrimaryKey("id")
                table.column("fileRecordId", .integer)
                    .notNull()
                    .references(FileRecord.databaseTableName, onDelete: .cascade)
                table.column("bucket", .text).notNull()
                table.column("key", .text).notNull()
                table.column("uploadId", .text).notNull()
                table.column("totalParts", .integer).notNull()
                table.column("completedPartsJSON", .text).notNull().defaults(to: "[]")
                table.column("createdAt", .datetime).notNull()
                table.column("lastUpdatedAt", .datetime).notNull()
            }

            try db.create(
                index: "idx_multipart_upload_bucket_key",
                on: MultipartUploadRecord.databaseTableName,
                columns: ["bucket", "key"],
                unique: true
            )

            try db.create(
                index: "idx_multipart_upload_upload_id",
                on: MultipartUploadRecord.databaseTableName,
                columns: ["uploadId"],
                unique: true
            )

            try db.create(
                index: "idx_multipart_upload_last_updated_at",
                on: MultipartUploadRecord.databaseTableName,
                columns: ["lastUpdatedAt"]
            )
        }

        migrator.registerMigration("addMultipartUploadResumeToken") { db in
            try db.alter(table: MultipartUploadRecord.databaseTableName) { table in
                table.add(column: "resumeToken", .text).notNull().defaults(to: "")
            }
        }

        migrator.registerMigration("addFileRecordScanToken") { db in
            try db.alter(table: FileRecord.databaseTableName) { table in
                table.add(column: "scanToken", .text)
            }

            try db.create(
                index: "idx_file_records_source_scan_token_uploaded_at",
                on: FileRecord.databaseTableName,
                columns: ["sourcePath", "scanToken", "uploadedAt"]
            )
        }

        return migrator
    }

    private static func hasContentChanged(existing: FileRecord, comparedTo scanned: FileRecord) -> Bool {
        if existing.fileSize != scanned.fileSize {
            return true
        }

        if existing.sha256.isEmpty || scanned.sha256.isEmpty {
            return true
        }

        if existing.sha256 != scanned.sha256 {
            return true
        }

        return false
    }

    private static func hasLikelyMetadataChanged(existing: FileRecord, comparedTo scanned: FileRecord) -> Bool {
        if existing.fileSize != scanned.fileSize {
            return true
        }

        let modifiedAtDelta = abs(existing.modifiedAt.timeIntervalSince(scanned.modifiedAt))
        if modifiedAtDelta > metadataTimestampTolerance {
            return true
        }

        return false
    }
}
