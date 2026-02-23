import Foundation
import GRDB

final class DatabaseService {
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

    func pendingFiles(for sourceRoot: String) throws -> [FileRecord] {
        try dbQueue.read { db in
            try FileRecord
                .filter(FileRecord.Columns.sourcePath == sourceRoot)
                .filter(FileRecord.Columns.uploadedAt == nil)
                .order(FileRecord.Columns.relativePath)
                .fetchAll(db)
        }
    }

    func syncScannedFiles(_ scannedFiles: [FileRecord], for sourceRoot: String) throws {
        try dbQueue.write { db in
            let existingRecords = try FileRecord
                .filter(FileRecord.Columns.sourcePath == sourceRoot)
                .fetchAll(db)
            var existingByRelativePath = Dictionary(
                uniqueKeysWithValues: existingRecords.map { ($0.relativePath, $0) }
            )

            for scanned in scannedFiles {
                if var existing = existingByRelativePath.removeValue(forKey: scanned.relativePath) {
                    if Self.hasContentChanged(existing: existing, comparedTo: scanned) {
                        existing.fileSize = scanned.fileSize
                        existing.modifiedAt = scanned.modifiedAt
                        existing.sha256 = scanned.sha256
                        existing.glacierKey = ""
                        existing.uploadedAt = nil
                        existing.storageClass = FileRecord.deepArchiveStorageClass
                        try existing.update(db)
                    }
                } else {
                    var newRecord = scanned
                    newRecord.sourcePath = sourceRoot
                    newRecord.glacierKey = ""
                    newRecord.uploadedAt = nil
                    newRecord.storageClass = FileRecord.deepArchiveStorageClass
                    try newRecord.insert(db)
                }
            }

            for staleRecord in existingByRelativePath.values {
                guard let id = staleRecord.id else {
                    continue
                }
                _ = try FileRecord.deleteOne(db, key: id)
            }
        }
    }

    func markUploaded(id: Int64, glacierKey: String) throws {
        try dbQueue.write { db in
            try db.execute(
                sql: """
                UPDATE \(FileRecord.databaseTableName)
                SET uploadedAt = ?, glacierKey = ?, storageClass = ?
                WHERE id = ?
                """,
                arguments: [Date(), glacierKey, FileRecord.deepArchiveStorageClass, id]
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
}
