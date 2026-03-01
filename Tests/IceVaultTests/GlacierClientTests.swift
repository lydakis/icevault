import AWSS3
import Foundation
import XCTest
@testable import IceVault

final class GlacierClientTests: XCTestCase {
    func testCredentialSourceProvidesStableUserFacingDescriptions() {
        let cases: [(CredentialSource, String, String)] = [
            (.keychain, "Keychain", "Using credentials from Keychain."),
            (.sso(profileName: "dev"), "SSO Profile (dev)", "Using temporary credentials from SSO profile 'dev'."),
            (.awsCLI, "~/.aws/credentials", "Using credentials from ~/.aws/credentials."),
            (.environment, "Environment variables", "Using credentials from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY.")
        ]

        for (source, expectedLabel, expectedDescription) in cases {
            XCTAssertEqual(source.displayLabel, expectedLabel)
            XCTAssertEqual(source.settingsDescription, expectedDescription)
        }
    }

    func testGlacierClientErrorDescriptionsRemainActionable() {
        XCTAssertEqual(GlacierClientError.invalidCredentials.errorDescription, "AWS credentials are missing.")
        XCTAssertEqual(GlacierClientError.invalidRegion("bad-region").errorDescription, "AWS region is invalid: bad-region")
        XCTAssertEqual(GlacierClientError.invalidBucket.errorDescription, "S3 bucket is required.")
        XCTAssertEqual(GlacierClientError.invalidObjectKey.errorDescription, "S3 object key is required.")
        XCTAssertEqual(GlacierClientError.fileNotFound("/tmp/file").errorDescription, "Local file not found: /tmp/file")
        XCTAssertEqual(GlacierClientError.unreadableFile("/tmp/file").errorDescription, "Unable to read local file: /tmp/file")
        XCTAssertEqual(GlacierClientError.unsupportedStorageClass("BAD").errorDescription, "Unsupported S3 storage class: BAD")
        XCTAssertEqual(GlacierClientError.missingMultipartUploadID.errorDescription, "S3 did not return a multipart upload ID.")
        XCTAssertEqual(
            GlacierClientError.missingMultipartETag(partNumber: 7).errorDescription,
            "S3 did not return an ETag for uploaded part 7."
        )
        XCTAssertEqual(
            GlacierClientError.incompleteFileRead(expectedBytes: 10, actualBytes: 8).errorDescription,
            "Read 8 bytes but expected 10 bytes."
        )
        XCTAssertTrue(
            GlacierClientError.s3OperationFailed(
                operation: "putObject",
                underlying: MockS3Error.syntheticFailure
            )
            .errorDescription?
            .contains("S3 putObject failed") == true
        )
    }

    func testUploadFileMultipartReusesPendingUploadWhenResumeTokenMatches() async throws {
        let database = try makeDatabaseService()
        let workingDirectory = try makeTemporaryDirectory(prefix: "IceVaultTests-Glacier")
        defer { try? FileManager.default.removeItem(at: workingDirectory) }

        let fileURL = workingDirectory.appendingPathComponent("payload.bin")
        let fileData = Data("this-is-a-test-payload".utf8)
        try fileData.write(to: fileURL)
        let modifiedAt = Date(timeIntervalSince1970: 1_700_001_000)
        try FileManager.default.setAttributes([.modificationDate: modifiedAt], ofItemAtPath: fileURL.path)

        var fileRecord = FileRecord(
            sourcePath: workingDirectory.path,
            relativePath: "payload.bin",
            fileSize: Int64(fileData.count),
            modifiedAt: modifiedAt,
            sha256: "sha-matching",
            glacierKey: "",
            uploadedAt: nil,
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&fileRecord)
        let fileRecordID = try XCTUnwrap(fileRecord.id)

        let modifiedAtNanoseconds = Int64((modifiedAt.timeIntervalSince1970 * 1_000_000_000).rounded(.towardZero))
        let resumeToken = "\(Int64(fileData.count)):\(modifiedAtNanoseconds):sha-matching"

        var pendingUpload = MultipartUploadRecord(
            fileRecordId: fileRecordID,
            bucket: "test-bucket",
            key: "archives/payload.bin",
            uploadId: "existing-upload-id",
            resumeToken: resumeToken,
            totalParts: 1,
            completedPartsJSON: "[]",
            createdAt: modifiedAt,
            lastUpdatedAt: modifiedAt
        )
        try database.insertMultipartUpload(&pendingUpload)

        let s3 = MockGlacierS3Client()
        s3.listPartsOutputs = [
            ListPartsOutput(
                isTruncated: false,
                nextPartNumberMarker: nil,
                parts: [S3ClientTypes.Part(eTag: "\"etag-existing\"", partNumber: 1)]
            )
        ]

        let client = GlacierClient(
            s3Client: s3,
            fileManager: .default,
            database: database,
            multipartThreshold: 1
        )

        _ = try await client.uploadFile(
            localPath: fileURL.path,
            bucket: "test-bucket",
            key: "archives/payload.bin",
            fileRecordId: fileRecordID
        )

        XCTAssertEqual(s3.listPartsInputs.count, 1)
        XCTAssertEqual(s3.listPartsInputs.first?.uploadId, "existing-upload-id")
        XCTAssertEqual(s3.createMultipartUploadInputs.count, 0)
        XCTAssertEqual(s3.uploadPartInputs.count, 0)
        XCTAssertEqual(s3.abortMultipartUploadInputs.count, 0)
        XCTAssertEqual(s3.completeMultipartUploadInputs.first?.uploadId, "existing-upload-id")
        XCTAssertTrue(try database.pendingMultipartUploads().isEmpty)
    }

    func testUploadFileMultipartInvalidatesPendingUploadWhenResumeTokenMismatches() async throws {
        let database = try makeDatabaseService()
        let workingDirectory = try makeTemporaryDirectory(prefix: "IceVaultTests-Glacier")
        defer { try? FileManager.default.removeItem(at: workingDirectory) }

        let fileURL = workingDirectory.appendingPathComponent("payload.bin")
        let fileData = Data("changed-payload".utf8)
        try fileData.write(to: fileURL)
        let modifiedAt = Date(timeIntervalSince1970: 1_700_001_100)
        try FileManager.default.setAttributes([.modificationDate: modifiedAt], ofItemAtPath: fileURL.path)

        var fileRecord = FileRecord(
            sourcePath: workingDirectory.path,
            relativePath: "payload.bin",
            fileSize: Int64(fileData.count),
            modifiedAt: modifiedAt,
            sha256: "sha-new",
            glacierKey: "",
            uploadedAt: nil,
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&fileRecord)
        let fileRecordID = try XCTUnwrap(fileRecord.id)

        var pendingUpload = MultipartUploadRecord(
            fileRecordId: fileRecordID,
            bucket: "test-bucket",
            key: "archives/payload.bin",
            uploadId: "stale-upload-id",
            resumeToken: "mismatched-token",
            totalParts: 1,
            completedPartsJSON: "[]",
            createdAt: modifiedAt,
            lastUpdatedAt: modifiedAt
        )
        try database.insertMultipartUpload(&pendingUpload)

        let s3 = MockGlacierS3Client()
        s3.createMultipartUploadOutput = CreateMultipartUploadOutput(uploadId: "fresh-upload-id")
        s3.uploadPartOutput = UploadPartOutput(eTag: "\"etag-fresh\"")

        let client = GlacierClient(
            s3Client: s3,
            fileManager: .default,
            database: database,
            multipartThreshold: 1
        )

        _ = try await client.uploadFile(
            localPath: fileURL.path,
            bucket: "test-bucket",
            key: "archives/payload.bin",
            fileRecordId: fileRecordID
        )

        XCTAssertEqual(s3.abortMultipartUploadInputs.count, 1)
        XCTAssertEqual(s3.abortMultipartUploadInputs.first?.uploadId, "stale-upload-id")
        XCTAssertEqual(s3.createMultipartUploadInputs.count, 1)
        XCTAssertEqual(
            s3.createMultipartUploadInputs.first?.metadata?[GlacierClient.sha256MetadataKey],
            "sha-new"
        )
        XCTAssertEqual(s3.listPartsInputs.count, 0)
        XCTAssertEqual(s3.uploadPartInputs.count, 1)
        XCTAssertEqual(s3.uploadPartInputs.first?.uploadId, "fresh-upload-id")
        XCTAssertEqual(s3.completeMultipartUploadInputs.first?.uploadId, "fresh-upload-id")
        XCTAssertTrue(try database.pendingMultipartUploads().isEmpty)
    }

    func testUploadFileMultipartUsesConfiguredPartConcurrency() async throws {
        let workingDirectory = try makeTemporaryDirectory(prefix: "IceVaultTests-GlacierMultipartConcurrency")
        defer { try? FileManager.default.removeItem(at: workingDirectory) }

        let fileURL = workingDirectory.appendingPathComponent("large.bin")
        let payloadSize = 24 * 1024 * 1024
        try Data(repeating: 0xA5, count: payloadSize).write(to: fileURL)

        let s3 = MultipartConcurrencyTrackingS3Client(uploadPartDelayNanoseconds: 120_000_000)
        let client = GlacierClient(
            s3Client: s3,
            fileManager: .default,
            database: nil,
            multipartThreshold: 1
        )

        _ = try await client.uploadFile(
            localPath: fileURL.path,
            bucket: "test-bucket",
            key: "archives/large.bin",
            fileRecordId: nil,
            multipartPartConcurrency: 3
        )

        let stats = await s3.uploadPartStats()
        XCTAssertGreaterThan(stats.uploadPartCallCount, 1)
        XCTAssertGreaterThanOrEqual(stats.maxInFlightUploadPartCalls, 2)
        XCTAssertLessThanOrEqual(stats.maxInFlightUploadPartCalls, 3)
    }

    func testUploadFileMultipartSharesMemoryBudgetAcrossConcurrentFiles() async throws {
        let workingDirectory = try makeTemporaryDirectory(prefix: "IceVaultTests-GlacierMultipartGlobalMemoryBudget")
        defer { try? FileManager.default.removeItem(at: workingDirectory) }

        let fileCount = 12
        let payloadSize = 16 * 1024 * 1024
        var fileURLs: [URL] = []
        fileURLs.reserveCapacity(fileCount)
        for index in 0..<fileCount {
            let fileURL = workingDirectory.appendingPathComponent("large-\(index).bin")
            try Data(repeating: UInt8(index % 255), count: payloadSize).write(to: fileURL)
            fileURLs.append(fileURL)
        }

        let s3 = MultipartConcurrencyTrackingS3Client(uploadPartDelayNanoseconds: 120_000_000)
        let client = GlacierClient(
            s3Client: s3,
            fileManager: .default,
            database: nil,
            multipartThreshold: 1,
            multipartBufferedBytesBudget: 64 * 1024 * 1024
        )

        try await withThrowingTaskGroup(of: Void.self) { group in
            for (index, fileURL) in fileURLs.enumerated() {
                group.addTask {
                    _ = try await client.uploadFile(
                        localPath: fileURL.path,
                        bucket: "test-bucket",
                        key: "archives/large-\(index).bin",
                        fileRecordId: nil,
                        multipartPartConcurrency: 16
                    )
                }
            }
            try await group.waitForAll()
        }

        let stats = await s3.uploadPartStats()
        XCTAssertGreaterThan(stats.uploadPartCallCount, fileCount)
        XCTAssertGreaterThan(stats.maxInFlightUploadPartCalls, 1)
        // 64 MB total budget / 8 MB default part size = 8 max in-flight parts overall.
        XCTAssertLessThanOrEqual(stats.maxInFlightUploadPartCalls, 8)
    }

    func testUploadFileMultipartCapsConcurrencyWhenPartsAreLarge() async throws {
        let workingDirectory = try makeTemporaryDirectory(prefix: "IceVaultTests-GlacierMultipartMemoryCap")
        defer { try? FileManager.default.removeItem(at: workingDirectory) }

        let fileURL = workingDirectory.appendingPathComponent("sparse-large.bin")
        try createSparseFile(at: fileURL, size: 100 * 1024 * 1024 * 1024)

        let s3 = FailFastMultipartConcurrencyTrackingS3Client(
            failingPartNumber: 2,
            failureDelayNanoseconds: 500_000_000,
            nonFailDelayNanoseconds: 5_000_000_000
        )
        let client = GlacierClient(
            s3Client: s3,
            fileManager: .default,
            database: nil,
            multipartThreshold: 1
        )

        do {
            _ = try await client.uploadFile(
                localPath: fileURL.path,
                bucket: "test-bucket",
                key: "archives/sparse-large.bin",
                fileRecordId: nil,
                multipartPartConcurrency: 128
            )
            XCTFail("Expected multipart upload to fail")
        } catch {}

        let stats = await s3.uploadPartStats()
        XCTAssertGreaterThanOrEqual(stats.maxInFlightUploadPartCalls, 2)
        XCTAssertLessThan(stats.maxInFlightUploadPartCalls, 128)
    }

    func testUploadFileMultipartPersistsCompletedPartsWhenSiblingPartFails() async throws {
        let database = try makeDatabaseService()
        let workingDirectory = try makeTemporaryDirectory(prefix: "IceVaultTests-GlacierMultipartFailure")
        defer { try? FileManager.default.removeItem(at: workingDirectory) }

        let fileURL = workingDirectory.appendingPathComponent("large.bin")
        let payloadSize = 12 * 1024 * 1024
        try Data(repeating: 0x6A, count: payloadSize).write(to: fileURL)

        var fileRecord = FileRecord(
            sourcePath: workingDirectory.path,
            relativePath: "large.bin",
            fileSize: Int64(payloadSize),
            modifiedAt: Date(),
            sha256: "sha-large",
            glacierKey: "",
            uploadedAt: nil,
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&fileRecord)
        let fileRecordID = try XCTUnwrap(fileRecord.id)

        let s3 = DeterministicMultipartFailureS3Client(blockedPartNumber: 1, failingPartNumber: 2)
        let client = GlacierClient(
            s3Client: s3,
            fileManager: .default,
            database: database,
            multipartThreshold: 1
        )

        let uploadTask = Task {
            try await client.uploadFile(
                localPath: fileURL.path,
                bucket: "test-bucket",
                key: "archives/large.bin",
                fileRecordId: fileRecordID,
                multipartPartConcurrency: 2
            )
        }

        await s3.waitUntilFailureTriggered()
        await s3.releaseBlockedPart()

        do {
            _ = try await uploadTask.value
            XCTFail("Expected multipart upload to fail")
        } catch {}

        let pendingUploads = try database.pendingMultipartUploads()
        XCTAssertEqual(pendingUploads.count, 1)
        let pendingUpload = try XCTUnwrap(pendingUploads.first)
        let completedPartNumbers = try completedPartNumbers(from: pendingUpload.completedPartsJSON)
        XCTAssertEqual(completedPartNumbers, [1])
    }

    func testCredentialsFromAWSCLIAndRegionFromConfigParseDefaultProfile() throws {
        let home = try makeTemporaryDirectory(prefix: "IceVaultTests-AWSHome")
        defer { try? FileManager.default.removeItem(at: home) }

        let fileManager = TestHomeFileManager(homeDirectory: home)
        let awsDirectory = home.appendingPathComponent(".aws", isDirectory: true)
        try FileManager.default.createDirectory(at: awsDirectory, withIntermediateDirectories: true)

        let credentialsContents = """
        [default]
        aws_access_key_id =   AKIA_TEST_KEY
        aws_secret_access_key =   test-secret
        """
        try credentialsContents.data(using: .utf8)?
            .write(to: awsDirectory.appendingPathComponent("credentials"))

        let configContents = """
        [default]
        region = us-west-2
        """
        try configContents.data(using: .utf8)?
            .write(to: awsDirectory.appendingPathComponent("config"))

        let credentials = GlacierClient.credentialsFromAWSCLI(fileManager: fileManager)
        XCTAssertEqual(credentials?.accessKey, "AKIA_TEST_KEY")
        XCTAssertEqual(credentials?.secretKey, "test-secret")
        XCTAssertEqual(GlacierClient.regionFromAWSConfig(fileManager: fileManager), "us-west-2")
    }

    func testSSOTokenStatusReportsValidAndExpired() throws {
        let home = try makeTemporaryDirectory(prefix: "IceVaultTests-SSOHome")
        defer { try? FileManager.default.removeItem(at: home) }

        let fileManager = TestHomeFileManager(homeDirectory: home)
        let awsDirectory = home.appendingPathComponent(".aws", isDirectory: true)
        let ssoCacheDirectory = awsDirectory
            .appendingPathComponent("sso", isDirectory: true)
            .appendingPathComponent("cache", isDirectory: true)
        try FileManager.default.createDirectory(at: ssoCacheDirectory, withIntermediateDirectories: true)

        let configContents = """
        [profile dev]
        sso_session = my-session
        sso_account_id = 123456789012
        sso_role_name = Admin
        sso_start_url = https://example.awsapps.com/start
        sso_region = us-east-1
        """
        try configContents.data(using: .utf8)?
            .write(to: awsDirectory.appendingPathComponent("config"))

        let validTokenContents = """
        {
          "startUrl": "https://example.awsapps.com/start",
          "accessToken": "abc",
          "expiresAt": "2099-01-01T00:00:00Z"
        }
        """
        try validTokenContents.data(using: .utf8)?
            .write(to: ssoCacheDirectory.appendingPathComponent("token.json"))

        let validStatus = GlacierClient.ssoTokenStatus(
            profileName: "dev",
            fileManager: fileManager,
            now: Date(timeIntervalSince1970: 1_700_000_000)
        )
        guard case .valid = validStatus else {
            return XCTFail("Expected valid status, got \(validStatus)")
        }

        let expiredStatus = GlacierClient.ssoTokenStatus(
            profileName: "dev",
            fileManager: fileManager,
            now: Date(timeIntervalSince1970: 4_102_444_800)
        )
        guard case .expired = expiredStatus else {
            return XCTFail("Expected expired status, got \(expiredStatus)")
        }
    }

    func testResolveCredentialsFallsBackToEnvironmentAndRegion() throws {
        let home = try makeTemporaryDirectory(prefix: "IceVaultTests-ResolveEnv")
        defer { try? FileManager.default.removeItem(at: home) }

        let resolved = GlacierClient.resolveCredentials(
            keychainCredentials: nil,
            authMethod: .staticKeys,
            ssoProfileName: nil,
            preferredRegion: nil,
            environment: [
                "AWS_ACCESS_KEY_ID": "  ENV_ACCESS  ",
                "AWS_SECRET_ACCESS_KEY": "  ENV_SECRET  ",
                "AWS_SESSION_TOKEN": "  ENV_TOKEN  ",
                "AWS_REGION": " us-east-2 "
            ],
            fileManager: TestHomeFileManager(homeDirectory: home)
        )

        XCTAssertEqual(resolved?.credentials.accessKey, "ENV_ACCESS")
        XCTAssertEqual(resolved?.credentials.secretKey, "ENV_SECRET")
        XCTAssertEqual(resolved?.credentials.sessionToken, "ENV_TOKEN")
        XCTAssertEqual(resolved?.region, "us-east-2")
        XCTAssertEqual(resolved?.credentialSource, .environment)
    }

    func testResolveCredentialsFallsBackToAWSCLIProfileAndConfigRegion() throws {
        let home = try makeTemporaryDirectory(prefix: "IceVaultTests-ResolveCLI")
        defer { try? FileManager.default.removeItem(at: home) }

        let awsDirectory = home.appendingPathComponent(".aws", isDirectory: true)
        try FileManager.default.createDirectory(at: awsDirectory, withIntermediateDirectories: true)
        try """
        [default]
        aws_access_key_id = AKIA_CLI_KEY
        aws_secret_access_key = cli-secret
        """
        .data(using: .utf8)?
        .write(to: awsDirectory.appendingPathComponent("credentials"))
        try """
        [default]
        region = us-west-1
        """
        .data(using: .utf8)?
        .write(to: awsDirectory.appendingPathComponent("config"))

        let resolved = GlacierClient.resolveCredentials(
            keychainCredentials: nil,
            authMethod: .staticKeys,
            ssoProfileName: nil,
            preferredRegion: nil,
            environment: [:],
            fileManager: TestHomeFileManager(homeDirectory: home)
        )

        XCTAssertEqual(resolved?.credentials.accessKey, "AKIA_CLI_KEY")
        XCTAssertEqual(resolved?.credentials.secretKey, "cli-secret")
        XCTAssertEqual(resolved?.region, "us-west-1")
        XCTAssertEqual(resolved?.credentialSource, .awsCLI)
    }

    func testResolveCredentialsUsesSSOProfileAndCLIProcessOutput() throws {
        let home = try makeTemporaryDirectory(prefix: "IceVaultTests-ResolveSSO")
        defer { try? FileManager.default.removeItem(at: home) }

        let fileManager = TestHomeFileManager(homeDirectory: home)
        let awsDirectory = home.appendingPathComponent(".aws", isDirectory: true)
        let ssoCacheDirectory = awsDirectory
            .appendingPathComponent("sso", isDirectory: true)
            .appendingPathComponent("cache", isDirectory: true)
        try FileManager.default.createDirectory(at: ssoCacheDirectory, withIntermediateDirectories: true)

        try """
        [profile dev]
        sso_session = company-session
        sso_account_id = 123456789012
        sso_role_name = Admin
        region = us-east-1

        [sso-session company-session]
        sso_start_url = https://example.awsapps.com/start
        sso_region = us-west-2
        """
        .data(using: .utf8)?
        .write(to: awsDirectory.appendingPathComponent("config"))

        try """
        {
          "startUrl": "https://example.awsapps.com/start",
          "accessToken": "token",
          "expiresAt": "2099-01-01T00:00:00Z"
        }
        """
        .data(using: .utf8)?
        .write(to: ssoCacheDirectory.appendingPathComponent("token.json"))

        let fakeCLIPath = try makeFakeAWSCLI(
            in: home,
            jsonResponse: """
            {"Version":1,"AccessKeyId":"AKIA_SSO_KEY","SecretAccessKey":"sso-secret","SessionToken":"sso-token","Expiration":"2099-01-02T00:00:00Z"}
            """
        )
        let environment = [
            "PATH": "\(fakeCLIPath.deletingLastPathComponent().path):\(ProcessInfo.processInfo.environment["PATH"] ?? "")"
        ]

        let resolved = GlacierClient.resolveCredentials(
            keychainCredentials: nil,
            authMethod: .ssoProfile,
            ssoProfileName: " dev ",
            preferredRegion: nil,
            environment: environment,
            fileManager: fileManager
        )

        XCTAssertEqual(resolved?.credentials.accessKey, "AKIA_SSO_KEY")
        XCTAssertEqual(resolved?.credentials.secretKey, "sso-secret")
        XCTAssertEqual(resolved?.credentials.sessionToken, "sso-token")
        XCTAssertEqual(resolved?.region, "us-east-1")
        XCTAssertEqual(resolved?.credentialSource, .sso(profileName: "dev"))
        XCTAssertNotNil(resolved?.expiration)
    }

    func testResolveCredentialsReturnsNilWhenNoProvidersAreAvailable() throws {
        let home = try makeTemporaryDirectory(prefix: "IceVaultTests-ResolveNone")
        defer { try? FileManager.default.removeItem(at: home) }

        let resolved = GlacierClient.resolveCredentials(
            keychainCredentials: nil,
            authMethod: .staticKeys,
            ssoProfileName: nil,
            preferredRegion: nil,
            environment: [:],
            fileManager: TestHomeFileManager(homeDirectory: home)
        )

        XCTAssertNil(resolved)
    }

    func testVerifyBucketAccessValidatesInputAndCallsHeadBucket() async throws {
        let s3 = MockGlacierS3Client()
        let client = GlacierClient(s3Client: s3)

        do {
            try await client.verifyBucketAccess(bucket: "   ")
            XCTFail("Expected invalid bucket error")
        } catch let error as GlacierClientError {
            guard case .invalidBucket = error else {
                return XCTFail("Unexpected error: \(error)")
            }
        }

        try await client.verifyBucketAccess(bucket: "test-bucket")
        XCTAssertEqual(s3.headBucketInputs.count, 1)
        XCTAssertEqual(s3.headBucketInputs.first?.bucket, "test-bucket")
    }

    func testValidateRemoteObjectReturnsValidForMatchingObject() async throws {
        let s3 = MockGlacierS3Client()
        s3.headObjectOutput = HeadObjectOutput(
            contentLength: 5,
            metadata: [GlacierClient.sha256MetadataKey: "abc123"],
            storageClass: .deepArchive
        )
        let client = GlacierClient(s3Client: s3)

        let result = try await client.validateRemoteObject(
            bucket: "test-bucket",
            key: "path/file.txt",
            expectedSize: 5,
            expectedSHA256: "abc123"
        )

        XCTAssertEqual(result, .valid)
        XCTAssertEqual(s3.headObjectInputs.count, 1)
        XCTAssertEqual(s3.headObjectInputs.first?.bucket, "test-bucket")
        XCTAssertEqual(s3.headObjectInputs.first?.key, "path/file.txt")
    }

    func testValidateRemoteObjectReturnsMismatchWhenChecksumMetadataIsMissing() async throws {
        let s3 = MockGlacierS3Client()
        s3.headObjectOutput = HeadObjectOutput(
            contentLength: 5,
            storageClass: .deepArchive
        )
        let client = GlacierClient(s3Client: s3)

        let result = try await client.validateRemoteObject(
            bucket: "test-bucket",
            key: "path/file.txt",
            expectedSize: 5,
            expectedSHA256: "abc123"
        )

        XCTAssertEqual(result, .mismatch)
        XCTAssertEqual(s3.headObjectInputs.count, 1)
    }

    func testValidateRemoteObjectReturnsMismatchWhenChecksumMetadataDiffers() async throws {
        let s3 = MockGlacierS3Client()
        s3.headObjectOutput = HeadObjectOutput(
            contentLength: 5,
            metadata: [GlacierClient.sha256MetadataKey: "other-hash"],
            storageClass: .deepArchive
        )
        let client = GlacierClient(s3Client: s3)

        let result = try await client.validateRemoteObject(
            bucket: "test-bucket",
            key: "path/file.txt",
            expectedSize: 5,
            expectedSHA256: "abc123"
        )

        XCTAssertEqual(result, .mismatch)
        XCTAssertEqual(s3.headObjectInputs.count, 1)
    }

    func testValidateRemoteObjectReturnsMissingFor404() async throws {
        let s3 = MockGlacierS3Client()
        s3.headObjectError = MockHTTPStatusCodeError(statusCode: .notFound)
        let client = GlacierClient(s3Client: s3)

        let result = try await client.validateRemoteObject(
            bucket: "test-bucket",
            key: "path/file.txt",
            expectedSize: 5,
            expectedSHA256: "abc123"
        )

        XCTAssertEqual(result, .missing)
        XCTAssertEqual(s3.headObjectInputs.count, 1)
    }

    func testValidateRemoteObjectReturnsInaccessibleFor403() async throws {
        let s3 = MockGlacierS3Client()
        s3.headObjectError = MockHTTPStatusCodeError(statusCode: .forbidden)
        let client = GlacierClient(s3Client: s3)

        let result = try await client.validateRemoteObject(
            bucket: "test-bucket",
            key: "path/file.txt",
            expectedSize: 5,
            expectedSHA256: "abc123"
        )

        XCTAssertEqual(result, .inaccessible)
        XCTAssertEqual(s3.headObjectInputs.count, 1)
    }

    func testUploadFileAddsChecksumMetadataForSinglePart() async throws {
        let database = try makeDatabaseService()
        let directory = try makeTemporaryDirectory(prefix: "IceVaultTests-SinglePartMetadata")
        defer { try? FileManager.default.removeItem(at: directory) }

        let fileURL = directory.appendingPathComponent("payload.txt")
        let payload = Data("payload".utf8)
        try payload.write(to: fileURL)

        var fileRecord = FileRecord(
            sourcePath: directory.path,
            relativePath: "payload.txt",
            fileSize: Int64(payload.count),
            modifiedAt: Date(),
            sha256: "sha-single",
            glacierKey: "",
            uploadedAt: nil,
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&fileRecord)
        let fileRecordID = try XCTUnwrap(fileRecord.id)

        let s3 = MockGlacierS3Client()
        let client = GlacierClient(
            s3Client: s3,
            fileManager: .default,
            database: database,
            multipartThreshold: 1_000_000
        )

        _ = try await client.uploadFile(
            localPath: fileURL.path,
            bucket: "bucket",
            key: "payload.txt",
            fileRecordId: fileRecordID
        )

        XCTAssertEqual(s3.putObjectInputs.count, 1)
        XCTAssertEqual(
            s3.putObjectInputs.first?.metadata?[GlacierClient.sha256MetadataKey],
            "sha-single"
        )
    }

    func testUploadFileValidationRejectsInvalidKeyStorageClassAndMissingFile() async throws {
        let s3 = MockGlacierS3Client()
        let client = GlacierClient(s3Client: s3, multipartThreshold: 1_000_000)
        let directory = try makeTemporaryDirectory(prefix: "IceVaultTests-UploadValidation")
        defer { try? FileManager.default.removeItem(at: directory) }

        let fileURL = directory.appendingPathComponent("payload.txt")
        try Data("payload".utf8).write(to: fileURL)

        do {
            _ = try await client.uploadFile(
                localPath: fileURL.path,
                bucket: "bucket",
                key: " / ",
                storageClass: FileRecord.deepArchiveStorageClass
            )
            XCTFail("Expected invalid key error")
        } catch let error as GlacierClientError {
            guard case .invalidObjectKey = error else {
                return XCTFail("Unexpected error: \(error)")
            }
        }

        do {
            _ = try await client.uploadFile(
                localPath: fileURL.path,
                bucket: "bucket",
                key: "file.txt",
                storageClass: "NOT_A_STORAGE_CLASS"
            )
            XCTFail("Expected unsupported storage class error")
        } catch let error as GlacierClientError {
            guard case .unsupportedStorageClass(let value) = error else {
                return XCTFail("Unexpected error: \(error)")
            }
            XCTAssertEqual(value, "NOT_A_STORAGE_CLASS")
        }

        do {
            _ = try await client.uploadFile(
                localPath: directory.appendingPathComponent("missing.bin").path,
                bucket: "bucket",
                key: "missing.bin",
                storageClass: FileRecord.deepArchiveStorageClass
            )
            XCTFail("Expected missing file error")
        } catch let error as GlacierClientError {
            guard case .fileNotFound = error else {
                return XCTFail("Unexpected error: \(error)")
            }
        }
    }

    func testUploadFileWrapsS3ErrorsForSinglePartUploads() async throws {
        let s3 = MockGlacierS3Client()
        s3.putObjectError = MockS3Error.syntheticFailure

        let client = GlacierClient(s3Client: s3, multipartThreshold: 1_000_000)
        let directory = try makeTemporaryDirectory(prefix: "IceVaultTests-UploadFailure")
        defer { try? FileManager.default.removeItem(at: directory) }

        let fileURL = directory.appendingPathComponent("payload.txt")
        try Data("payload".utf8).write(to: fileURL)

        do {
            _ = try await client.uploadFile(
                localPath: fileURL.path,
                bucket: "bucket",
                key: "payload.txt",
                storageClass: FileRecord.deepArchiveStorageClass
            )
            XCTFail("Expected wrapped S3 failure")
        } catch let error as GlacierClientError {
            guard case .s3OperationFailed(let operation, let underlying) = error else {
                return XCTFail("Unexpected error: \(error)")
            }
            XCTAssertEqual(operation, "putObject")
            XCTAssertTrue(underlying is MockS3Error)
        }
    }

    func testAbortStaleMultipartUploadsAbortsOnlyExpiredRecords() async throws {
        let database = try makeDatabaseService()
        let directory = try makeTemporaryDirectory(prefix: "IceVaultTests-AbortStale")
        defer { try? FileManager.default.removeItem(at: directory) }

        var fileRecord = FileRecord(
            sourcePath: directory.path,
            relativePath: "payload.bin",
            fileSize: 10,
            modifiedAt: Date(),
            sha256: "hash",
            glacierKey: "",
            uploadedAt: nil,
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&fileRecord)
        let fileRecordID = try XCTUnwrap(fileRecord.id)

        let staleDate = Date().addingTimeInterval(-8_000)
        let freshDate = Date()

        var stale = MultipartUploadRecord(
            fileRecordId: fileRecordID,
            bucket: "bucket",
            key: "old.bin",
            uploadId: "old-upload-id",
            resumeToken: "old",
            totalParts: 1,
            completedPartsJSON: "[]",
            createdAt: staleDate,
            lastUpdatedAt: staleDate
        )
        var fresh = MultipartUploadRecord(
            fileRecordId: fileRecordID,
            bucket: "bucket",
            key: "new.bin",
            uploadId: "new-upload-id",
            resumeToken: "new",
            totalParts: 1,
            completedPartsJSON: "[]",
            createdAt: freshDate,
            lastUpdatedAt: freshDate
        )
        try database.insertMultipartUpload(&stale)
        try database.insertMultipartUpload(&fresh)

        let s3 = MockGlacierS3Client()
        let client = GlacierClient(s3Client: s3, database: database)
        await client.abortStaleMultipartUploads(olderThan: 3_600)

        XCTAssertEqual(s3.abortMultipartUploadInputs.count, 1)
        XCTAssertEqual(s3.abortMultipartUploadInputs.first?.uploadId, "old-upload-id")

        let remaining = try database.pendingMultipartUploads()
        XCTAssertEqual(remaining.count, 1)
        XCTAssertEqual(remaining.first?.uploadId, "new-upload-id")
    }

    private func makeDatabaseService() throws -> DatabaseService {
        let directory = try makeTemporaryDirectory(prefix: "IceVaultTests-GlacierDB")
        let databaseURL = directory.appendingPathComponent("icevault.sqlite")
        return try DatabaseService(databaseURL: databaseURL)
    }

    private func createSparseFile(at fileURL: URL, size: UInt64) throws {
        guard FileManager.default.createFile(atPath: fileURL.path, contents: nil) else {
            XCTFail("Failed to create sparse file at \(fileURL.path)")
            return
        }

        let handle = try FileHandle(forWritingTo: fileURL)
        defer {
            if #available(macOS 11, *) {
                try? handle.close()
            } else {
                handle.closeFile()
            }
        }

        if #available(macOS 11, *) {
            try handle.truncate(atOffset: size)
        } else {
            handle.truncateFile(atOffset: size)
        }
    }

    private func completedPartNumbers(from completedPartsJSON: String) throws -> [Int] {
        let data = try XCTUnwrap(completedPartsJSON.data(using: .utf8))
        let decoded = try JSONSerialization.jsonObject(with: data)
        guard let items = decoded as? [[String: Any]] else {
            XCTFail("Expected completed parts JSON array")
            return []
        }
        return items.compactMap { $0["partNumber"] as? Int }.sorted()
    }

    private func makeFakeAWSCLI(in directory: URL, jsonResponse: String) throws -> URL {
        let binDirectory = directory.appendingPathComponent("bin", isDirectory: true)
        try FileManager.default.createDirectory(at: binDirectory, withIntermediateDirectories: true)
        let scriptURL = binDirectory.appendingPathComponent("aws")
        let script = """
        #!/bin/sh
        cat <<'JSON'
        \(jsonResponse)
        JSON
        exit 0
        """
        try script.data(using: .utf8)?.write(to: scriptURL)
        try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: scriptURL.path)
        return scriptURL
    }
}
