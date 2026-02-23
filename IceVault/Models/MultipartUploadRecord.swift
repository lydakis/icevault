import Foundation
import GRDB

struct MultipartUploadRecord: Codable, FetchableRecord, MutablePersistableRecord, Identifiable {
    static let databaseTableName = "multipart_upload_records"

    var id: Int64?
    var fileRecordId: Int64
    var bucket: String
    var key: String
    var uploadId: String
    var totalParts: Int
    var completedPartsJSON: String
    var createdAt: Date
    var lastUpdatedAt: Date

    init(
        id: Int64? = nil,
        fileRecordId: Int64,
        bucket: String,
        key: String,
        uploadId: String,
        totalParts: Int,
        completedPartsJSON: String = "[]",
        createdAt: Date = Date(),
        lastUpdatedAt: Date = Date()
    ) {
        self.id = id
        self.fileRecordId = fileRecordId
        self.bucket = bucket
        self.key = key
        self.uploadId = uploadId
        self.totalParts = totalParts
        self.completedPartsJSON = completedPartsJSON
        self.createdAt = createdAt
        self.lastUpdatedAt = lastUpdatedAt
    }

    mutating func didInsert(_ inserted: InsertionSuccess) {
        id = inserted.rowID
    }

    enum Columns: String, ColumnExpression {
        case id
        case fileRecordId
        case bucket
        case key
        case uploadId
        case totalParts
        case completedPartsJSON
        case createdAt
        case lastUpdatedAt
    }
}
