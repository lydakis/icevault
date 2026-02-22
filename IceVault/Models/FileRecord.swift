import Foundation
import GRDB

struct FileRecord: Codable, FetchableRecord, MutablePersistableRecord, Identifiable {
    static let databaseTableName = "file_records"
    static let deepArchiveStorageClass = "DEEP_ARCHIVE"

    var id: Int64?
    var sourcePath: String
    var relativePath: String
    var fileSize: Int64
    var modifiedAt: Date
    var sha256: String
    var glacierKey: String
    var uploadedAt: Date?
    var storageClass: String

    init(
        id: Int64? = nil,
        sourcePath: String,
        relativePath: String,
        fileSize: Int64,
        modifiedAt: Date,
        sha256: String = "",
        glacierKey: String = "",
        uploadedAt: Date? = nil,
        storageClass: String = FileRecord.deepArchiveStorageClass
    ) {
        self.id = id
        self.sourcePath = sourcePath
        self.relativePath = relativePath
        self.fileSize = fileSize
        self.modifiedAt = modifiedAt
        self.sha256 = sha256
        self.glacierKey = glacierKey
        self.uploadedAt = uploadedAt
        self.storageClass = storageClass
    }

    mutating func didInsert(_ inserted: InsertionSuccess) {
        id = inserted.rowID
    }

    enum Columns: String, ColumnExpression {
        case id
        case sourcePath
        case relativePath
        case fileSize
        case modifiedAt
        case sha256
        case glacierKey
        case uploadedAt
        case storageClass
    }
}
