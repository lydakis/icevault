import AWSS3
import Foundation
import XCTest
@testable import IceVault

final class GlacierClientTests: XCTestCase {
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
        XCTAssertEqual(s3.listPartsInputs.count, 0)
        XCTAssertEqual(s3.uploadPartInputs.count, 1)
        XCTAssertEqual(s3.uploadPartInputs.first?.uploadId, "fresh-upload-id")
        XCTAssertEqual(s3.completeMultipartUploadInputs.first?.uploadId, "fresh-upload-id")
        XCTAssertTrue(try database.pendingMultipartUploads().isEmpty)
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

    private func makeDatabaseService() throws -> DatabaseService {
        let directory = try makeTemporaryDirectory(prefix: "IceVaultTests-GlacierDB")
        let databaseURL = directory.appendingPathComponent("icevault.sqlite")
        return try DatabaseService(databaseURL: databaseURL)
    }
}

private final class MockGlacierS3Client: GlacierS3Client {
    var createMultipartUploadInputs: [CreateMultipartUploadInput] = []
    var uploadPartInputs: [UploadPartInput] = []
    var completeMultipartUploadInputs: [CompleteMultipartUploadInput] = []
    var abortMultipartUploadInputs: [AbortMultipartUploadInput] = []
    var listPartsInputs: [ListPartsInput] = []
    var putObjectInputs: [PutObjectInput] = []
    var headBucketInputs: [HeadBucketInput] = []

    var createMultipartUploadOutput = CreateMultipartUploadOutput(uploadId: "upload-id")
    var uploadPartOutput = UploadPartOutput(eTag: "\"etag-uploaded\"")
    var completeMultipartUploadOutput = CompleteMultipartUploadOutput()
    var abortMultipartUploadOutput = AbortMultipartUploadOutput()
    var putObjectOutput = PutObjectOutput()
    var headBucketOutput = HeadBucketOutput()
    var listPartsOutputs: [ListPartsOutput] = [ListPartsOutput(isTruncated: false, nextPartNumberMarker: nil, parts: [])]

    func createMultipartUpload(input: CreateMultipartUploadInput) async throws -> CreateMultipartUploadOutput {
        createMultipartUploadInputs.append(input)
        return createMultipartUploadOutput
    }

    func uploadPart(input: UploadPartInput) async throws -> UploadPartOutput {
        uploadPartInputs.append(input)
        return uploadPartOutput
    }

    func completeMultipartUpload(input: CompleteMultipartUploadInput) async throws -> CompleteMultipartUploadOutput {
        completeMultipartUploadInputs.append(input)
        return completeMultipartUploadOutput
    }

    func abortMultipartUpload(input: AbortMultipartUploadInput) async throws -> AbortMultipartUploadOutput {
        abortMultipartUploadInputs.append(input)
        return abortMultipartUploadOutput
    }

    func listParts(input: ListPartsInput) async throws -> ListPartsOutput {
        listPartsInputs.append(input)
        if !listPartsOutputs.isEmpty {
            return listPartsOutputs.removeFirst()
        }
        return ListPartsOutput(isTruncated: false, nextPartNumberMarker: nil, parts: [])
    }

    func putObject(input: PutObjectInput) async throws -> PutObjectOutput {
        putObjectInputs.append(input)
        return putObjectOutput
    }

    func headBucket(input: HeadBucketInput) async throws -> HeadBucketOutput {
        headBucketInputs.append(input)
        return headBucketOutput
    }
}
