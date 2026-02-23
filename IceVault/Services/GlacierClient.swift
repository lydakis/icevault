import AWSS3
import AWSSDKIdentity
import protocol ClientRuntime.HTTPError
import Foundation
import Smithy

enum CredentialSource: Equatable, Sendable {
    case keychain
    case sso(profileName: String)
    case awsCLI
    case environment

    var displayLabel: String {
        switch self {
        case .keychain:
            return "Keychain"
        case .sso(let profileName):
            return "SSO Profile (\(profileName))"
        case .awsCLI:
            return "~/.aws/credentials"
        case .environment:
            return "Environment variables"
        }
    }

    var settingsDescription: String {
        switch self {
        case .keychain:
            return "Using credentials from Keychain."
        case .sso(let profileName):
            return "Using temporary credentials from SSO profile '\(profileName)'."
        case .awsCLI:
            return "Using credentials from ~/.aws/credentials."
        case .environment:
            return "Using credentials from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY."
        }
    }
}

struct SSOProfileConfiguration: Equatable, Sendable {
    let profileName: String
    let startURL: String
    let accountID: String
    let roleName: String
    let ssoRegion: String
    let region: String?
}

enum SSOTokenStatus: Equatable, Sendable {
    case missingProfile
    case missingToken
    case expired(expiresAt: Date)
    case valid(expiresAt: Date)
}

struct ResolvedCredentials: Equatable, Sendable {
    let credentials: AWSCredentials
    let region: String?
    let credentialSource: CredentialSource
    let expiration: Date?
}

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

protocol GlacierS3Client {
    func createMultipartUpload(input: CreateMultipartUploadInput) async throws -> CreateMultipartUploadOutput
    func uploadPart(input: UploadPartInput) async throws -> UploadPartOutput
    func completeMultipartUpload(input: CompleteMultipartUploadInput) async throws -> CompleteMultipartUploadOutput
    func abortMultipartUpload(input: AbortMultipartUploadInput) async throws -> AbortMultipartUploadOutput
    func listParts(input: ListPartsInput) async throws -> ListPartsOutput
    func putObject(input: PutObjectInput) async throws -> PutObjectOutput
    func headBucket(input: HeadBucketInput) async throws -> HeadBucketOutput
}

extension S3Client: GlacierS3Client {}

final class GlacierClient {
    static let multipartThresholdBytes: Int64 = 100 * 1024 * 1024
    private static let defaultPartSizeBytes: Int = 8 * 1024 * 1024
    private static let minimumPartSizeBytes: Int = 5 * 1024 * 1024
    private static let maxS3Retries = 3
    private static let retryBackoffSeconds: [UInt64] = [2, 8, 32]

    private let s3Client: any GlacierS3Client
    private let fileManager: FileManager
    private let database: DatabaseService?
    private let multipartThreshold: Int64

    init(
        accessKey: String,
        secretKey: String,
        sessionToken: String? = nil,
        region: String,
        fileManager: FileManager = .default,
        database: DatabaseService? = nil,
        multipartThreshold: Int64 = GlacierClient.multipartThresholdBytes
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
            secret: trimmedSecretKey,
            sessionToken: Self.trimmed(sessionToken).isEmpty ? nil : Self.trimmed(sessionToken)
        )
        let config = try S3Client.S3ClientConfig(
            awsCredentialIdentityResolver: StaticAWSCredentialIdentityResolver(credentialIdentity),
            region: trimmedRegion
        )

        self.s3Client = S3Client(config: config)
        self.fileManager = fileManager
        self.database = database
        self.multipartThreshold = multipartThreshold
    }

    init(
        s3Client: any GlacierS3Client,
        fileManager: FileManager = .default,
        database: DatabaseService? = nil,
        multipartThreshold: Int64 = GlacierClient.multipartThresholdBytes
    ) {
        self.s3Client = s3Client
        self.fileManager = fileManager
        self.database = database
        self.multipartThreshold = multipartThreshold
    }

    static func resolveCredentials(
        keychainCredentials: AWSCredentials?,
        authMethod: AppState.Settings.AuthenticationMethod,
        ssoProfileName: String?,
        preferredRegion: String?,
        environment: [String: String] = ProcessInfo.processInfo.environment,
        fileManager: FileManager = .default
    ) -> ResolvedCredentials? {
        if let normalizedKeychainCredentials = normalized(credentials: keychainCredentials) {
            return ResolvedCredentials(
                credentials: normalizedKeychainCredentials,
                region: resolveRegion(
                    preferredRegion: preferredRegion,
                    authMethod: authMethod,
                    ssoProfileName: ssoProfileName,
                    environment: environment,
                    fileManager: fileManager
                ),
                credentialSource: .keychain,
                expiration: normalizedKeychainCredentials.expiration
            )
        }

        if authMethod == .ssoProfile,
           let normalizedProfileName = normalizedProfileName(ssoProfileName),
           let ssoCredentials = credentialsFromSSOProfile(
               profileName: normalizedProfileName,
               environment: environment,
               fileManager: fileManager
           )
        {
            return ResolvedCredentials(
                credentials: ssoCredentials,
                region: resolveRegion(
                    preferredRegion: preferredRegion,
                    authMethod: authMethod,
                    ssoProfileName: normalizedProfileName,
                    environment: environment,
                    fileManager: fileManager
                ),
                credentialSource: .sso(profileName: normalizedProfileName),
                expiration: ssoCredentials.expiration
            )
        }

        if let awsCLICredentials = credentialsFromAWSCLI(fileManager: fileManager) {
            return ResolvedCredentials(
                credentials: awsCLICredentials,
                region: resolveRegion(
                    preferredRegion: preferredRegion,
                    authMethod: authMethod,
                    ssoProfileName: ssoProfileName,
                    environment: environment,
                    fileManager: fileManager
                ),
                credentialSource: .awsCLI,
                expiration: awsCLICredentials.expiration
            )
        }

        if let environmentCredentials = credentialsFromEnvironment(environment) {
            return ResolvedCredentials(
                credentials: environmentCredentials,
                region: resolveRegion(
                    preferredRegion: preferredRegion,
                    authMethod: authMethod,
                    ssoProfileName: ssoProfileName,
                    environment: environment,
                    fileManager: fileManager
                ),
                credentialSource: .environment,
                expiration: environmentCredentials.expiration
            )
        }

        return nil
    }

    static func ssoProfile(named profileName: String, fileManager: FileManager = .default) -> SSOProfileConfiguration? {
        let normalizedProfileName = normalizedProfileName(profileName)
        guard let normalizedProfileName else {
            return nil
        }

        let configURL = awsDirectoryURL(fileManager: fileManager).appendingPathComponent("config")
        let sections = parseINI(from: configURL, fileManager: fileManager)
        guard let profileSection = profileSection(named: normalizedProfileName, in: sections) else {
            return nil
        }

        let accountID = trimmed(profileSection["sso_account_id"])
        let roleName = trimmed(profileSection["sso_role_name"])
        let profileStartURL = trimmed(profileSection["sso_start_url"])
        let profileSSORegion = trimmed(profileSection["sso_region"])
        let linkedSessionName = trimmed(profileSection["sso_session"])
        let linkedSessionSection: [String: String]
        if linkedSessionName.isEmpty {
            linkedSessionSection = [:]
        } else {
            linkedSessionSection = ssoSessionSection(named: linkedSessionName, in: sections) ?? [:]
        }

        let startURL = profileStartURL.isEmpty
            ? trimmed(linkedSessionSection["sso_start_url"])
            : profileStartURL
        let ssoRegion = profileSSORegion.isEmpty
            ? trimmed(linkedSessionSection["sso_region"])
            : profileSSORegion

        guard !startURL.isEmpty, !accountID.isEmpty, !roleName.isEmpty, !ssoRegion.isEmpty else {
            return nil
        }

        let regionValue = trimmed(profileSection["region"])
        return SSOProfileConfiguration(
            profileName: normalizedProfileName,
            startURL: startURL,
            accountID: accountID,
            roleName: roleName,
            ssoRegion: ssoRegion,
            region: regionValue.isEmpty ? nil : regionValue
        )
    }

    static func ssoTokenStatus(
        profileName: String,
        fileManager: FileManager = .default,
        now: Date = Date()
    ) -> SSOTokenStatus {
        guard let profile = ssoProfile(named: profileName, fileManager: fileManager) else {
            return .missingProfile
        }

        guard let token = validTokenCacheEntry(
            matchingStartURL: profile.startURL,
            fileManager: fileManager
        ) else {
            return .missingToken
        }

        if token.expiresAt <= now {
            return .expired(expiresAt: token.expiresAt)
        }

        return .valid(expiresAt: token.expiresAt)
    }

    static func credentialsFromAWSCLI(fileManager: FileManager = .default) -> AWSCredentials? {
        let credentialsURL = awsDirectoryURL(fileManager: fileManager).appendingPathComponent("credentials")
        guard let defaultProfile = parseDefaultProfile(from: credentialsURL, fileManager: fileManager) else {
            return nil
        }

        let accessKey = trimmed(defaultProfile["aws_access_key_id"])
        let secretKey = trimmed(defaultProfile["aws_secret_access_key"])
        guard !accessKey.isEmpty, !secretKey.isEmpty else {
            return nil
        }

        return AWSCredentials(accessKey: accessKey, secretKey: secretKey)
    }

    static func regionFromAWSConfig(fileManager: FileManager = .default) -> String? {
        let configURL = awsDirectoryURL(fileManager: fileManager).appendingPathComponent("config")
        guard let defaultProfile = parseDefaultProfile(from: configURL, fileManager: fileManager) else {
            return nil
        }

        let region = trimmed(defaultProfile["region"])
        return region.isEmpty ? nil : region
    }

    @discardableResult
    func uploadFile(
        localPath: String,
        bucket: String,
        key: String,
        storageClass: String = FileRecord.deepArchiveStorageClass,
        fileRecordId: Int64? = nil,
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
        let fileModifiedAt = (attributes[.modificationDate] as? Date) ?? Date.distantPast
        let multipartResumeToken = Self.multipartResumeToken(
            fileSize: fileSize,
            modifiedAt: fileModifiedAt,
            sha256: multipartSHA256(fileRecordID: fileRecordId)
        )
        let resolvedStorageClass = try Self.resolveStorageClass(storageClass)

        if fileSize > multipartThreshold {
            try await uploadMultipartFile(
                fileURL: fileURL,
                fileSize: fileSize,
                bucket: trimmedBucket,
                key: normalizedKey,
                storageClass: resolvedStorageClass,
                fileRecordID: fileRecordId,
                resumeToken: multipartResumeToken,
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
        fileRecordID: Int64?,
        resumeToken: String,
        onProgress: ((Int64) -> Void)?
    ) async throws {
        let partSize = Self.partSize(for: fileSize)
        let totalParts = max(1, Int((fileSize + Int64(partSize) - 1) / Int64(partSize)))
        var uploadState = try await resumeOrCreateMultipartUpload(
            bucket: bucket,
            key: key,
            storageClass: storageClass,
            fileRecordID: fileRecordID,
            resumeToken: resumeToken,
            totalParts: totalParts
        )

        let fileHandle: FileHandle
        do {
            fileHandle = try FileHandle(forReadingFrom: fileURL)
        } catch {
            throw GlacierClientError.unreadableFile(fileURL.path)
        }

        defer {
            Self.close(fileHandle: fileHandle)
        }

        var uploadedBytes = Self.totalUploadedBytes(
            for: Array(uploadState.completedPartsByNumber.keys),
            totalParts: totalParts,
            fileSize: fileSize,
            partSize: partSize
        )

        onProgress?(uploadedBytes)

        for partNumber in 1...totalParts {
            try Task.checkCancellation()

            if uploadState.completedPartsByNumber[partNumber] != nil {
                continue
            }

            let partOffset = Int64(partNumber - 1) * Int64(partSize)
            let bytesToRead = Int(Self.expectedPartSize(
                partNumber: partNumber,
                totalParts: totalParts,
                fileSize: fileSize,
                partSize: partSize
            ))

            try Self.seek(fileHandle: fileHandle, toOffset: UInt64(partOffset))
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
                        uploadId: uploadState.uploadID
                    )
                )
            }

            guard let eTag = partOutput.eTag else {
                throw GlacierClientError.missingMultipartETag(partNumber: partNumber)
            }

            uploadState.completedPartsByNumber[partNumber] = S3ClientTypes.CompletedPart(
                eTag: eTag,
                partNumber: partNumber
            )
            uploadedBytes += Int64(partData.count)
            onProgress?(uploadedBytes)

            try updateMultipartProgressInDatabase(uploadState: &uploadState)
        }

        if uploadedBytes != fileSize {
            throw GlacierClientError.incompleteFileRead(
                expectedBytes: fileSize,
                actualBytes: uploadedBytes
            )
        }

        let completedParts = uploadState.completedPartsByNumber
            .keys
            .sorted()
            .compactMap { uploadState.completedPartsByNumber[$0] }

        _ = try await performS3Operation("completeMultipartUpload") {
            try await s3Client.completeMultipartUpload(
                input: CompleteMultipartUploadInput(
                    bucket: bucket,
                    key: key,
                    multipartUpload: S3ClientTypes.CompletedMultipartUpload(parts: completedParts),
                    uploadId: uploadState.uploadID
                )
            )
        }

        if let recordID = uploadState.record?.id {
            _ = try? database?.deleteMultipartUpload(id: recordID)
        }
    }

    func abortStaleMultipartUploads(olderThan: TimeInterval) async {
        guard olderThan > 0, let database else {
            return
        }

        let cutoffDate = Date().addingTimeInterval(-olderThan)
        let staleUploads: [MultipartUploadRecord]
        do {
            staleUploads = try database.pendingMultipartUploads()
                .filter { $0.lastUpdatedAt <= cutoffDate }
        } catch {
            print("Failed to query stale multipart uploads: \(error.localizedDescription)")
            return
        }

        for upload in staleUploads {
            guard let recordID = upload.id else {
                continue
            }

            do {
                _ = try await performS3Operation("abortMultipartUpload") {
                    try await s3Client.abortMultipartUpload(
                        input: AbortMultipartUploadInput(
                            bucket: upload.bucket,
                            key: upload.key,
                            uploadId: upload.uploadId
                        )
                    )
                }
                _ = try? database.deleteMultipartUpload(id: recordID)
            } catch {
                let underlyingError = Self.unwrapOperationError(error)
                if Self.httpStatusCode(for: underlyingError) == 404 {
                    _ = try? database.deleteMultipartUpload(id: recordID)
                } else {
                    print(
                        "Failed to abort stale multipart upload \(upload.uploadId): \(underlyingError.localizedDescription)"
                    )
                }
            }
        }
    }

    private func resumeOrCreateMultipartUpload(
        bucket: String,
        key: String,
        storageClass: S3ClientTypes.StorageClass,
        fileRecordID: Int64?,
        resumeToken: String,
        totalParts: Int
    ) async throws -> MultipartUploadState {
        if let existingRecord = try existingMultipartUploadRecord(bucket: bucket, key: key) {
            if existingRecord.resumeToken == resumeToken {
                do {
                    let verifiedCompletedParts = try await verifiedCompletedParts(
                        bucket: bucket,
                        key: key,
                        uploadID: existingRecord.uploadId,
                        totalParts: totalParts
                    )

                    var state = MultipartUploadState(
                        uploadID: existingRecord.uploadId,
                        record: existingRecord,
                        completedPartsByNumber: verifiedCompletedParts
                    )
                    try updateMultipartProgressInDatabase(uploadState: &state)
                    return state
                } catch {
                    let underlyingError = Self.unwrapOperationError(error)
                    if Self.httpStatusCode(for: underlyingError) == 404, let recordID = existingRecord.id {
                        _ = try? database?.deleteMultipartUpload(id: recordID)
                    } else {
                        throw error
                    }
                }
            } else {
                await invalidateMultipartUploadRecord(existingRecord)
            }
        }

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

        var uploadRecord: MultipartUploadRecord?
        if let database, let fileRecordID {
            var newRecord = MultipartUploadRecord(
                fileRecordId: fileRecordID,
                bucket: bucket,
                key: key,
                uploadId: uploadID,
                resumeToken: resumeToken,
                totalParts: totalParts,
                completedPartsJSON: "[]",
                createdAt: Date(),
                lastUpdatedAt: Date()
            )
            try database.insertMultipartUpload(&newRecord)
            uploadRecord = newRecord
        }

        return MultipartUploadState(
            uploadID: uploadID,
            record: uploadRecord,
            completedPartsByNumber: [:]
        )
    }

    private func existingMultipartUploadRecord(bucket: String, key: String) throws -> MultipartUploadRecord? {
        guard let database else {
            return nil
        }

        return try database.pendingMultipartUpload(bucket: bucket, key: key)
    }

    private func invalidateMultipartUploadRecord(_ record: MultipartUploadRecord) async {
        do {
            _ = try await performS3Operation("abortMultipartUpload") {
                try await s3Client.abortMultipartUpload(
                    input: AbortMultipartUploadInput(
                        bucket: record.bucket,
                        key: record.key,
                        uploadId: record.uploadId
                    )
                )
            }
        } catch {
            let underlyingError = Self.unwrapOperationError(error)
            if Self.httpStatusCode(for: underlyingError) != 404 {
                print(
                    "Failed to abort mismatched multipart upload \(record.uploadId): \(underlyingError.localizedDescription)"
                )
            }
        }

        if let recordID = record.id {
            _ = try? database?.deleteMultipartUpload(id: recordID)
        }
    }

    private func multipartSHA256(fileRecordID: Int64?) -> String? {
        guard let fileRecordID, let database else {
            return nil
        }

        do {
            guard let fileRecord = try database.fetchFile(id: fileRecordID) else {
                return nil
            }
            let normalizedHash = fileRecord.sha256.trimmingCharacters(in: .whitespacesAndNewlines)
            return normalizedHash.isEmpty ? nil : normalizedHash
        } catch {
            return nil
        }
    }

    private func verifiedCompletedParts(
        bucket: String,
        key: String,
        uploadID: String,
        totalParts: Int
    ) async throws -> [Int: S3ClientTypes.CompletedPart] {
        let listedParts = try await listUploadedParts(bucket: bucket, key: key, uploadID: uploadID)
        var completedPartsByNumber: [Int: S3ClientTypes.CompletedPart] = [:]

        for part in listedParts {
            guard
                let partNumber = part.partNumber,
                partNumber > 0,
                partNumber <= totalParts,
                let eTag = part.eTag
            else {
                continue
            }
            completedPartsByNumber[partNumber] = S3ClientTypes.CompletedPart(
                eTag: eTag,
                partNumber: partNumber
            )
        }

        return completedPartsByNumber
    }

    private func listUploadedParts(
        bucket: String,
        key: String,
        uploadID: String
    ) async throws -> [S3ClientTypes.Part] {
        var partNumberMarker: String?
        var uploadedParts: [S3ClientTypes.Part] = []

        while true {
            let output = try await performS3Operation("listParts") {
                try await s3Client.listParts(
                    input: ListPartsInput(
                        bucket: bucket,
                        key: key,
                        partNumberMarker: partNumberMarker,
                        uploadId: uploadID
                    )
                )
            }

            uploadedParts.append(contentsOf: output.parts ?? [])

            guard output.isTruncated == true, let nextMarker = output.nextPartNumberMarker, !nextMarker.isEmpty else {
                break
            }
            partNumberMarker = nextMarker
        }

        return uploadedParts
    }

    private func updateMultipartProgressInDatabase(uploadState: inout MultipartUploadState) throws {
        guard let database, var record = uploadState.record, let recordID = record.id else {
            return
        }

        let completedPartsJSON = try Self.completedPartsJSON(from: uploadState.completedPartsByNumber)
        try database.updateCompletedParts(
            id: recordID,
            completedPartsJSON: completedPartsJSON,
            lastUpdatedAt: Date()
        )
        record.completedPartsJSON = completedPartsJSON
        record.lastUpdatedAt = Date()
        uploadState.record = record
    }

    private func performS3Operation<T>(
        _ operation: String,
        _ action: () async throws -> T
    ) async throws -> T {
        var retryIndex = 0

        while true {
            do {
                return try await action()
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                guard Self.shouldRetryOperation(for: error), retryIndex < Self.maxS3Retries else {
                    throw GlacierClientError.s3OperationFailed(operation: operation, underlying: error)
                }

                let delaySeconds = Self.retryBackoffSeconds[retryIndex]
                print(
                    "Retrying S3 \(operation) in \(delaySeconds)s (attempt \(retryIndex + 1)/\(Self.maxS3Retries))"
                )
                try await Task.sleep(nanoseconds: delaySeconds * 1_000_000_000)
                retryIndex += 1
            }
        }
    }

    private static func shouldRetryOperation(for error: Error) -> Bool {
        if let statusCode = httpStatusCode(for: error) {
            if statusCode == 403 || statusCode == 404 {
                return false
            }
            return statusCode == 500 || statusCode == 503
        }

        return isNetworkError(error)
    }

    private static func httpStatusCode(for error: Error) -> Int? {
        let candidateError = unwrapOperationError(error)
        return (candidateError as? any HTTPError)?.httpResponse.statusCode.rawValue
    }

    private static func isNetworkError(_ error: Error) -> Bool {
        let candidateError = unwrapOperationError(error)
        if candidateError is URLError {
            return true
        }

        let nsError = candidateError as NSError
        return nsError.domain == NSURLErrorDomain
    }

    private static func unwrapOperationError(_ error: Error) -> Error {
        guard case .s3OperationFailed(_, let underlying) = error as? GlacierClientError else {
            return error
        }
        return underlying
    }

    private static func resolveStorageClass(_ value: String) throws -> S3ClientTypes.StorageClass {
        guard let storageClass = S3ClientTypes.StorageClass(rawValue: value) else {
            throw GlacierClientError.unsupportedStorageClass(value)
        }

        if case .sdkUnknown = storageClass {
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

    private static func expectedPartSize(
        partNumber: Int,
        totalParts: Int,
        fileSize: Int64,
        partSize: Int
    ) -> Int64 {
        if partNumber < totalParts {
            return Int64(partSize)
        }

        let priorPartsBytes = Int64(max(totalParts - 1, 0)) * Int64(partSize)
        return max(fileSize - priorPartsBytes, 0)
    }

    private static func totalUploadedBytes(
        for partNumbers: [Int],
        totalParts: Int,
        fileSize: Int64,
        partSize: Int
    ) -> Int64 {
        partNumbers.reduce(Int64(0)) { partialResult, partNumber in
            partialResult + expectedPartSize(
                partNumber: partNumber,
                totalParts: totalParts,
                fileSize: fileSize,
                partSize: partSize
            )
        }
    }

    private static func completedPartsJSON(from partsByNumber: [Int: S3ClientTypes.CompletedPart]) throws -> String {
        let orderedParts = partsByNumber.keys.sorted().compactMap { partNumber -> PersistedCompletedPart? in
            guard let eTag = partsByNumber[partNumber]?.eTag else {
                return nil
            }
            return PersistedCompletedPart(partNumber: partNumber, eTag: eTag)
        }

        let encodedParts = try JSONEncoder().encode(orderedParts)
        return String(decoding: encodedParts, as: UTF8.self)
    }

    private static func multipartResumeToken(
        fileSize: Int64,
        modifiedAt: Date,
        sha256: String?
    ) -> String {
        let modifiedAtNanoseconds = Int64((modifiedAt.timeIntervalSince1970 * 1_000_000_000).rounded(.towardZero))
        let normalizedHash = sha256?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        return "\(fileSize):\(modifiedAtNanoseconds):\(normalizedHash)"
    }

    private static func normalizedObjectKey(_ key: String) -> String {
        key
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .replacingOccurrences(of: "\\", with: "/")
            .trimmingCharacters(in: CharacterSet(charactersIn: "/"))
    }

    private static func seek(fileHandle: FileHandle, toOffset offset: UInt64) throws {
        if #available(macOS 11, *) {
            try fileHandle.seek(toOffset: offset)
        } else {
            fileHandle.seek(toFileOffset: offset)
        }
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

    private struct PersistedCompletedPart: Codable {
        let partNumber: Int
        let eTag: String
    }

    private struct MultipartUploadState {
        let uploadID: String
        var record: MultipartUploadRecord?
        var completedPartsByNumber: [Int: S3ClientTypes.CompletedPart]
    }

    private struct SSOCacheToken: Sendable {
        let startURL: String
        let accessToken: String
        let expiresAt: Date
    }

    private struct ExportCredentialsProcessOutput: Decodable {
        let accessKeyID: String
        let secretAccessKey: String
        let sessionToken: String?
        let expiration: String?

        enum CodingKeys: String, CodingKey {
            case accessKeyID = "AccessKeyId"
            case secretAccessKey = "SecretAccessKey"
            case sessionToken = "SessionToken"
            case expiration = "Expiration"
        }
    }

    private static let iso8601DateFormatterWithFractionalSeconds: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()

    private static let iso8601DateFormatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        return formatter
    }()

    private static let awsUTCDateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss'UTC'"
        return formatter
    }()

    private static let awsOffsetDateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ssZ"
        return formatter
    }()

    private static func resolveRegion(
        preferredRegion: String?,
        authMethod: AppState.Settings.AuthenticationMethod,
        ssoProfileName: String?,
        environment: [String: String],
        fileManager: FileManager
    ) -> String? {
        let normalizedPreferredRegion = trimmed(preferredRegion)
        if !normalizedPreferredRegion.isEmpty {
            return normalizedPreferredRegion
        }

        if authMethod == .ssoProfile,
           let normalizedProfileName = normalizedProfileName(ssoProfileName),
           let profile = ssoProfile(named: normalizedProfileName, fileManager: fileManager)
        {
            if let profileRegion = profile.region, !trimmed(profileRegion).isEmpty {
                return profileRegion
            }

            if !trimmed(profile.ssoRegion).isEmpty {
                return profile.ssoRegion
            }
        }

        if let configRegion = regionFromAWSConfig(fileManager: fileManager) {
            return configRegion
        }

        let environmentRegion = trimmed(environment["AWS_REGION"])
        if !environmentRegion.isEmpty {
            return environmentRegion
        }

        let fallbackEnvironmentRegion = trimmed(environment["AWS_DEFAULT_REGION"])
        if !fallbackEnvironmentRegion.isEmpty {
            return fallbackEnvironmentRegion
        }

        return nil
    }

    private static func credentialsFromSSOProfile(
        profileName: String,
        environment: [String: String],
        fileManager: FileManager
    ) -> AWSCredentials? {
        guard case .valid = ssoTokenStatus(profileName: profileName, fileManager: fileManager) else {
            return nil
        }

        return credentialsFromAWSCLIProcess(profileName: profileName, environment: environment)
    }

    private static func credentialsFromAWSCLIProcess(
        profileName: String,
        environment: [String: String]
    ) -> AWSCredentials? {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = [
            "aws",
            "configure",
            "export-credentials",
            "--profile",
            profileName,
            "--format",
            "process"
        ]

        var processEnvironment = environment
        processEnvironment["AWS_PROFILE"] = profileName
        processEnvironment["AWS_SDK_LOAD_CONFIG"] = "1"
        process.environment = processEnvironment

        let outputPipe = Pipe()
        process.standardOutput = outputPipe
        process.standardError = Pipe()

        do {
            try process.run()
        } catch {
            return nil
        }

        process.waitUntilExit()
        guard process.terminationStatus == 0 else {
            return nil
        }

        let outputData = outputPipe.fileHandleForReading.readDataToEndOfFile()
        guard
            !outputData.isEmpty,
            let exportedCredentials = try? JSONDecoder().decode(ExportCredentialsProcessOutput.self, from: outputData)
        else {
            return nil
        }

        let accessKey = trimmed(exportedCredentials.accessKeyID)
        let secretKey = trimmed(exportedCredentials.secretAccessKey)
        guard !accessKey.isEmpty, !secretKey.isEmpty else {
            return nil
        }

        let sessionToken = trimmed(exportedCredentials.sessionToken)
        let expiration = parseDate(exportedCredentials.expiration)
        return AWSCredentials(
            accessKey: accessKey,
            secretKey: secretKey,
            sessionToken: sessionToken.isEmpty ? nil : sessionToken,
            expiration: expiration
        )
    }

    private static func validTokenCacheEntry(
        matchingStartURL startURL: String,
        fileManager: FileManager
    ) -> SSOCacheToken? {
        let cacheDirectoryURL = awsDirectoryURL(fileManager: fileManager)
            .appendingPathComponent("sso", isDirectory: true)
            .appendingPathComponent("cache", isDirectory: true)

        guard let cacheFiles = try? fileManager.contentsOfDirectory(
            at: cacheDirectoryURL,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        ) else {
            return nil
        }

        let normalizedStartURL = trimmed(startURL)
        guard !normalizedStartURL.isEmpty else {
            return nil
        }

        let matchingTokens = cacheFiles
            .filter { $0.pathExtension.caseInsensitiveCompare("json") == .orderedSame }
            .compactMap(parseSSOCacheToken(from:))
            .filter { $0.startURL.caseInsensitiveCompare(normalizedStartURL) == .orderedSame }

        return matchingTokens.max(by: { $0.expiresAt < $1.expiresAt })
    }

    private static func parseSSOCacheToken(from fileURL: URL) -> SSOCacheToken? {
        guard
            let data = try? Data(contentsOf: fileURL),
            let jsonObject = try? JSONSerialization.jsonObject(with: data),
            let json = jsonObject as? [String: Any]
        else {
            return nil
        }

        let startURL = trimmed((json["startUrl"] as? String) ?? (json["start_url"] as? String))
        let accessToken = trimmed((json["accessToken"] as? String) ?? (json["access_token"] as? String))
        let expiresAtRaw = trimmed((json["expiresAt"] as? String) ?? (json["expires_at"] as? String))
        guard
            !startURL.isEmpty,
            !accessToken.isEmpty,
            !expiresAtRaw.isEmpty,
            let expiresAt = parseDate(expiresAtRaw)
        else {
            return nil
        }

        return SSOCacheToken(
            startURL: startURL,
            accessToken: accessToken,
            expiresAt: expiresAt
        )
    }

    private static func parseDate(_ value: String?) -> Date? {
        let normalizedValue = trimmed(value)
        guard !normalizedValue.isEmpty else {
            return nil
        }

        if let date = iso8601DateFormatterWithFractionalSeconds.date(from: normalizedValue) {
            return date
        }

        if let date = iso8601DateFormatter.date(from: normalizedValue) {
            return date
        }

        if let date = awsUTCDateFormatter.date(from: normalizedValue) {
            return date
        }

        return awsOffsetDateFormatter.date(from: normalizedValue)
    }

    private static func normalized(credentials: AWSCredentials?) -> AWSCredentials? {
        guard let credentials else {
            return nil
        }

        let accessKey = trimmed(credentials.accessKey)
        let secretKey = trimmed(credentials.secretKey)
        guard !accessKey.isEmpty, !secretKey.isEmpty else {
            return nil
        }

        let sessionToken = trimmed(credentials.sessionToken)
        return AWSCredentials(
            accessKey: accessKey,
            secretKey: secretKey,
            sessionToken: sessionToken.isEmpty ? nil : sessionToken,
            expiration: credentials.expiration
        )
    }

    private static func credentialsFromEnvironment(_ environment: [String: String]) -> AWSCredentials? {
        let accessKey = trimmed(environment["AWS_ACCESS_KEY_ID"])
        let secretKey = trimmed(environment["AWS_SECRET_ACCESS_KEY"])
        guard !accessKey.isEmpty, !secretKey.isEmpty else {
            return nil
        }

        let sessionToken = trimmed(environment["AWS_SESSION_TOKEN"])
        return AWSCredentials(
            accessKey: accessKey,
            secretKey: secretKey,
            sessionToken: sessionToken.isEmpty ? nil : sessionToken
        )
    }

    private static func parseDefaultProfile(from fileURL: URL, fileManager: FileManager) -> [String: String]? {
        let sections = parseINI(from: fileURL, fileManager: fileManager)
        return profileSection(named: "default", in: sections)
    }

    private static func profileSection(
        named profileName: String,
        in sections: [String: [String: String]]
    ) -> [String: String]? {
        let normalizedProfileName = normalizedProfileName(profileName) ?? "default"
        let candidateSectionNames: [String]
        if normalizedProfileName.caseInsensitiveCompare("default") == .orderedSame {
            candidateSectionNames = ["default", "profile default"]
        } else {
            candidateSectionNames = ["profile \(normalizedProfileName)", normalizedProfileName]
        }

        for sectionName in candidateSectionNames {
            if let section = section(named: sectionName, in: sections) {
                return section
            }
        }
        return nil
    }

    private static func ssoSessionSection(
        named sessionName: String,
        in sections: [String: [String: String]]
    ) -> [String: String]? {
        let normalizedSessionName = trimmed(sessionName)
        guard !normalizedSessionName.isEmpty else {
            return nil
        }

        let candidateSectionNames = [
            "sso-session \(normalizedSessionName)",
            "sso_session \(normalizedSessionName)",
            normalizedSessionName
        ]

        for sectionName in candidateSectionNames {
            if let section = section(named: sectionName, in: sections) {
                return section
            }
        }
        return nil
    }

    private static func section(named expectedName: String, in sections: [String: [String: String]]) -> [String: String]? {
        sections.first(where: { $0.key.caseInsensitiveCompare(expectedName) == .orderedSame })?.value
    }

    private static func parseINI(from fileURL: URL, fileManager: FileManager) -> [String: [String: String]] {
        guard
            fileManager.fileExists(atPath: fileURL.path),
            let data = try? Data(contentsOf: fileURL),
            var contents = String(data: data, encoding: .utf8)
        else {
            return [:]
        }

        contents = contents
            .replacingOccurrences(of: "\r\n", with: "\n")
            .replacingOccurrences(of: "\r", with: "\n")

        var sections: [String: [String: String]] = [:]
        var currentSection = ""

        for rawLine in contents.components(separatedBy: .newlines) {
            var line = rawLine.trimmingCharacters(in: .whitespacesAndNewlines)
            if line.isEmpty {
                continue
            }

            if let firstCharacter = line.first, firstCharacter == "#" || firstCharacter == ";" {
                continue
            }

            if let commentStart = line.firstIndex(where: { $0 == "#" || $0 == ";" }) {
                line = String(line[..<commentStart]).trimmingCharacters(in: .whitespacesAndNewlines)
                if line.isEmpty {
                    continue
                }
            }

            if line.hasPrefix("[") && line.hasSuffix("]") {
                currentSection = String(line.dropFirst().dropLast()).trimmingCharacters(in: .whitespacesAndNewlines)
                continue
            }

            guard !currentSection.isEmpty else {
                continue
            }

            let keyValue = line.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false)
            guard keyValue.count == 2 else {
                continue
            }

            let key = String(keyValue[0])
                .trimmingCharacters(in: .whitespacesAndNewlines)
                .lowercased()
            let value = String(keyValue[1])
                .trimmingCharacters(in: .whitespacesAndNewlines)

            guard !key.isEmpty else {
                continue
            }

            sections[currentSection, default: [:]][key] = value
        }

        return sections
    }

    private static func awsDirectoryURL(fileManager: FileManager) -> URL {
        fileManager.homeDirectoryForCurrentUser
            .appendingPathComponent(".aws", isDirectory: true)
    }

    private static func normalizedProfileName(_ value: String?) -> String? {
        let normalizedValue = trimmed(value)
        return normalizedValue.isEmpty ? nil : normalizedValue
    }

    private static func trimmed(_ value: String?) -> String {
        (value ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
    }
}
