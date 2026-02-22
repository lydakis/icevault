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
        var mutableRecord = record
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

    func pendingFiles(for sourceRoot: String) throws -> [FileRecord] {
        try dbQueue.read { db in
            try FileRecord
                .filter(FileRecord.Columns.sourcePath == sourceRoot)
                .filter(FileRecord.Columns.uploadedAt == nil)
                .order(FileRecord.Columns.relativePath)
                .fetchAll(db)
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

        return migrator
    }
}
