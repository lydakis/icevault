import AWSS3
import AWSSDKIdentity
import Foundation
import Smithy

enum GlacierClientError: LocalizedError {
    case invalidCredentials
    case invalidRegion(String)
    case invalidBucket
    case invalidObjectKey
    case fileNotFound(String)
    case unreadableFile(String)
    case unsupportedStorageClass(String)
    case missingMultipartUploadID
    case missingMultipartETag(partNumber: Int)
    case incompleteFileRead(expectedBytes: Int64, actualBytes: Int64)
    case s3OperationFailed(operation: String, underlying: Error)

    var errorDescription: String? {
        switch self {
        case .invalidCredentials:
            return "AWS credentials are missing."
        case .invalidRegion(let region):
            return "AWS region is invalid: \(region)"
        case .invalidBucket:
            return "S3 bucket is required."
        case .invalidObjectKey:
            return "S3 object key is required."
        case .fileNotFound(let path):
            return "Local file not found: \(path)"
        case .unreadableFile(let path):
            return "Unable to read local file: \(path)"
        case .unsupportedStorageClass(let value):
            return "Unsupported S3 storage class: \(value)"
        case .missingMultipartUploadID:
            return "S3 did not return a multipart upload ID."
        case .missingMultipartETag(let partNumber):
            return "S3 did not return an ETag for uploaded part \(partNumber)."
        case .incompleteFileRead(let expectedBytes, let actualBytes):
            return "Read \(actualBytes) bytes but expected \(expectedBytes) bytes."
        case .s3OperationFailed(let operation, let underlying):
            return "S3 \(operation) failed: \(underlying.localizedDescription)"
        }
    }
}

final class GlacierClient {
    static let multipartThresholdBytes: Int64 = 100 * 1024 * 1024
    private static let defaultPartSizeBytes: Int = 8 * 1024 * 1024
    private static let minimumPartSizeBytes: Int = 5 * 1024 * 1024

    private let s3Client: S3Client
    private let fileManager: FileManager

    init(
        accessKey: String,
        secretKey: String,
        region: String,
        fileManager: FileManager = .default
    ) throws {
        let trimmedAccessKey = accessKey.trimmingCharacters(in: .whitespacesAndNewlines)
        let trimmedSecretKey = secretKey.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedAccessKey.isEmpty, !trimmedSecretKey.isEmpty else {
            throw GlacierClientError.invalidCredentials
        }

        let trimmedRegion = region.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedRegion.isEmpty else {
            throw GlacierClientError.invalidRegion(region)
        }

        let credentialIdentity = AWSCredentialIdentity(
            accessKey: trimmedAccessKey,
            secret: trimmedSecretKey
        )
        let config = try S3Client.S3ClientConfig(
            awsCredentialIdentityResolver: StaticAWSCredentialIdentityResolver(credentialIdentity),
            region: trimmedRegion
        )

        self.s3Client = S3Client(config: config)
        self.fileManager = fileManager
    }

    @discardableResult
    func uploadFile(
        localPath: String,
        bucket: String,
        key: String,
        storageClass: String = FileRecord.deepArchiveStorageClass,
        onProgress: ((Int64) -> Void)? = nil
    ) async throws -> String {
        let trimmedBucket = bucket.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedBucket.isEmpty else {
            throw GlacierClientError.invalidBucket
        }

        let normalizedKey = Self.normalizedObjectKey(key)
        guard !normalizedKey.isEmpty else {
            throw GlacierClientError.invalidObjectKey
        }

        let fileURL = URL(fileURLWithPath: localPath).standardizedFileURL
        guard fileManager.fileExists(atPath: fileURL.path) else {
            throw GlacierClientError.fileNotFound(fileURL.path)
        }

        let attributes = try fileManager.attributesOfItem(atPath: fileURL.path)
        let fileSize = (attributes[.size] as? NSNumber)?.int64Value ?? 0
        let resolvedStorageClass = try Self.resolveStorageClass(storageClass)

        if fileSize > Self.multipartThresholdBytes {
            try await uploadMultipartFile(
                fileURL: fileURL,
                fileSize: fileSize,
                bucket: trimmedBucket,
                key: normalizedKey,
                storageClass: resolvedStorageClass,
                onProgress: onProgress
            )
        } else {
            try await uploadSinglePartFile(
                fileURL: fileURL,
                bucket: trimmedBucket,
                key: normalizedKey,
                storageClass: resolvedStorageClass,
                onProgress: onProgress
            )
        }

        return normalizedKey
    }

    func verifyBucketAccess(bucket: String) async throws {
        let trimmedBucket = bucket.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedBucket.isEmpty else {
            throw GlacierClientError.invalidBucket
        }

        _ = try await performS3Operation("headBucket") {
            try await s3Client.headBucket(
                input: HeadBucketInput(bucket: trimmedBucket)
            )
        }
    }

    private func uploadSinglePartFile(
        fileURL: URL,
        bucket: String,
        key: String,
        storageClass: S3ClientTypes.StorageClass,
        onProgress: ((Int64) -> Void)?
    ) async throws {
        let data: Data
        do {
            data = try Data(contentsOf: fileURL, options: .mappedIfSafe)
        } catch {
            throw GlacierClientError.unreadableFile(fileURL.path)
        }

        onProgress?(0)
        _ = try await performS3Operation("putObject") {
            try await s3Client.putObject(
                input: PutObjectInput(
                    body: .data(data),
                    bucket: bucket,
                    contentLength: data.count,
                    key: key,
                    storageClass: storageClass
                )
            )
        }
        onProgress?(Int64(data.count))
    }

    private func uploadMultipartFile(
        fileURL: URL,
        fileSize: Int64,
        bucket: String,
        key: String,
        storageClass: S3ClientTypes.StorageClass,
        onProgress: ((Int64) -> Void)?
    ) async throws {
        let createOutput = try await performS3Operation("createMultipartUpload") {
            try await s3Client.createMultipartUpload(
                input: CreateMultipartUploadInput(
                    bucket: bucket,
                    key: key,
                    storageClass: storageClass
                )
            )
        }
        guard let uploadID = createOutput.uploadId, !uploadID.isEmpty else {
            throw GlacierClientError.missingMultipartUploadID
        }

        let partSize = Self.partSize(for: fileSize)
        let fileHandle: FileHandle
        do {
            fileHandle = try FileHandle(forReadingFrom: fileURL)
        } catch {
            throw GlacierClientError.unreadableFile(fileURL.path)
        }

        defer {
            Self.close(fileHandle: fileHandle)
        }

        var uploadedBytes: Int64 = 0
        var partNumber = 1
        var completedParts: [S3ClientTypes.CompletedPart] = []

        onProgress?(0)

        do {
            while uploadedBytes < fileSize {
                try Task.checkCancellation()

                let bytesRemaining = fileSize - uploadedBytes
                let bytesToRead = Int(min(Int64(partSize), bytesRemaining))
                guard let partData = try Self.read(fileHandle: fileHandle, upToCount: bytesToRead), !partData.isEmpty else {
                    throw GlacierClientError.incompleteFileRead(
                        expectedBytes: fileSize,
                        actualBytes: uploadedBytes
                    )
                }

                let partOutput = try await performS3Operation("uploadPart #\(partNumber)") {
                    try await s3Client.uploadPart(
                        input: UploadPartInput(
                            body: .data(partData),
                            bucket: bucket,
                            contentLength: partData.count,
                            key: key,
                            partNumber: partNumber,
                            uploadId: uploadID
                        )
                    )
                }

                guard let eTag = partOutput.eTag else {
                    throw GlacierClientError.missingMultipartETag(partNumber: partNumber)
                }

                completedParts.append(
                    S3ClientTypes.CompletedPart(
                        eTag: eTag,
                        partNumber: partNumber
                    )
                )
                uploadedBytes += Int64(partData.count)
                onProgress?(uploadedBytes)
                partNumber += 1
            }

            if uploadedBytes != fileSize {
                throw GlacierClientError.incompleteFileRead(
                    expectedBytes: fileSize,
                    actualBytes: uploadedBytes
                )
            }

            _ = try await performS3Operation("completeMultipartUpload") {
                try await s3Client.completeMultipartUpload(
                    input: CompleteMultipartUploadInput(
                        bucket: bucket,
                        key: key,
                        multipartUpload: S3ClientTypes.CompletedMultipartUpload(parts: completedParts),
                        uploadId: uploadID
                    )
                )
            }
        } catch {
            _ = try? await s3Client.abortMultipartUpload(
                input: AbortMultipartUploadInput(
                    bucket: bucket,
                    key: key,
                    uploadId: uploadID
                )
            )
            throw error
        }
    }

    private func performS3Operation<T>(
        _ operation: String,
        _ action: () async throws -> T
    ) async throws -> T {
        do {
            return try await action()
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            throw GlacierClientError.s3OperationFailed(operation: operation, underlying: error)
        }
    }

    private static func resolveStorageClass(_ value: String) throws -> S3ClientTypes.StorageClass {
        guard let storageClass = S3ClientTypes.StorageClass(rawValue: value) else {
            throw GlacierClientError.unsupportedStorageClass(value)
        }
        return storageClass
    }

    private static func partSize(for fileSize: Int64) -> Int {
        let maxParts = 10_000.0
        let minRequiredForPartLimit = Int(ceil(Double(fileSize) / maxParts))
        return max(
            minimumPartSizeBytes,
            max(defaultPartSizeBytes, minRequiredForPartLimit)
        )
    }

    private static func normalizedObjectKey(_ key: String) -> String {
        key
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .replacingOccurrences(of: "\\", with: "/")
            .trimmingCharacters(in: CharacterSet(charactersIn: "/"))
    }

    private static func read(fileHandle: FileHandle, upToCount count: Int) throws -> Data? {
        if #available(macOS 11, *) {
            return try fileHandle.read(upToCount: count)
        }
        return fileHandle.readData(ofLength: count)
    }

    private static func close(fileHandle: FileHandle) {
        if #available(macOS 11, *) {
            try? fileHandle.close()
        } else {
            fileHandle.closeFile()
        }
    }
}
