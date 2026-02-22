import AWSS3
import AWSSDKIdentity
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

final class GlacierClient {
    static let multipartThresholdBytes: Int64 = 100 * 1024 * 1024
    private static let defaultPartSizeBytes: Int = 8 * 1024 * 1024
    private static let minimumPartSizeBytes: Int = 5 * 1024 * 1024

    private let s3Client: S3Client
    private let fileManager: FileManager

    init(
        accessKey: String,
        secretKey: String,
        sessionToken: String? = nil,
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
            secret: trimmedSecretKey,
            sessionToken: Self.trimmed(sessionToken).isEmpty ? nil : Self.trimmed(sessionToken)
        )
        let config = try S3Client.S3ClientConfig(
            awsCredentialIdentityResolver: StaticAWSCredentialIdentityResolver(credentialIdentity),
            region: trimmedRegion
        )

        self.s3Client = S3Client(config: config)
        self.fileManager = fileManager
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
