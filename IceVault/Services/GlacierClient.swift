import AWSS3
import Foundation

enum GlacierClientError: Error {
    case notImplemented
}

final class GlacierClient {
    private let bucket: String

    init(bucket: String) {
        self.bucket = bucket
    }

    func upload(fileURL: URL, key: String, storageClass: String = FileRecord.deepArchiveStorageClass) async throws -> String {
        _ = bucket
        _ = fileURL
        _ = key
        _ = storageClass

        // TODO: Implement S3 PutObject upload for files <= 100 MB.
        // TODO: Implement multipart upload workflow for files > 100 MB.
        // TODO: Ensure requests set storage class to Glacier Deep Archive.
        throw GlacierClientError.notImplemented
    }
}
