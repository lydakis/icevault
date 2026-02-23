import Foundation
import AWSS3
import XCTest
@testable import IceVault

final class BackupEngineTests: XCTestCase {
    func testRunFailsWhenDatabaseInitializationFailed() async throws {
        enum TestError: Error {
            case databaseFailed
            case shouldNotCreateGlacierClient
        }

        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let backupEngine = BackupEngine(
            scanner: FileScanner(),
            fileManager: .default,
            database: nil,
            databaseFactory: {
                throw TestError.databaseFailed
            },
            glacierClientFactory: { _, _, _, _, _ in
                throw TestError.shouldNotCreateGlacierClient
            }
        )

        let settings = AppState.Settings(
            awsAccessKey: "test-access-key",
            awsSecretKey: "test-secret-key",
            awsRegion: "us-east-1",
            bucket: "test-bucket",
            sourcePath: sourceRoot.path
        )
        let job = await MainActor.run {
            BackupJob(sourceRoot: sourceRoot.path, bucket: "test-bucket")
        }

        do {
            try await backupEngine.run(job: job, settings: settings)
            XCTFail("Expected database initialization failure")
        } catch let error as BackupEngineError {
            switch error {
            case .databaseUnavailable(let message):
                XCTAssertFalse(message.isEmpty)
            default:
                XCTFail("Unexpected backup engine error: \(error)")
            }
        } catch {
            XCTFail("Unexpected error type: \(error)")
        }

        await MainActor.run {
            XCTAssertEqual(job.status, .failed)
            XCTAssertNotNil(job.error)
        }
    }

    func testRunFailsWhenAccessKeyIsMissing() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let backupEngine = BackupEngine(
            scanner: FileScanner(),
            fileManager: .default,
            database: try makeDatabaseService(),
            glacierClientFactory: { _, _, _, _, _ in
                XCTFail("Glacier client should not be created for invalid settings")
                throw TestError.unexpectedGlacierClientCreation
            }
        )

        let settings = AppState.Settings(
            awsAccessKey: " ",
            awsSecretKey: "secret",
            awsRegion: "us-east-1",
            bucket: "bucket",
            sourcePath: sourceRoot.path
        )
        let job = await MainActor.run {
            BackupJob(sourceRoot: sourceRoot.path, bucket: "bucket")
        }

        do {
            try await backupEngine.run(job: job, settings: settings)
            XCTFail("Expected invalid settings failure")
        } catch let error as BackupEngineError {
            guard case .invalidSettings(let message) = error else {
                return XCTFail("Unexpected error: \(error)")
            }
            XCTAssertTrue(message.contains("AWS access key is required."))
        }
    }

    func testRunCompletesWhenNoPendingFilesExist() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let database = try makeDatabaseService()
        let mockS3 = MockBackupEngineS3Client()
        let backupEngine = BackupEngine(
            scanner: FileScanner(),
            fileManager: .default,
            database: database,
            glacierClientFactory: { _, _, _, _, db in
                GlacierClient(s3Client: mockS3, database: db)
            }
        )

        let settings = AppState.Settings(
            awsAccessKey: "access",
            awsSecretKey: "secret",
            awsRegion: "us-east-1",
            bucket: "bucket",
            sourcePath: sourceRoot.path
        )
        let job = await MainActor.run {
            BackupJob(sourceRoot: sourceRoot.path, bucket: "bucket")
        }

        try await backupEngine.run(job: job, settings: settings)

        await MainActor.run {
            XCTAssertEqual(job.status, .completed)
            XCTAssertEqual(job.filesTotal, 0)
            XCTAssertEqual(job.filesUploaded, 0)
            XCTAssertEqual(job.bytesTotal, 0)
            XCTAssertEqual(job.bytesUploaded, 0)
            XCTAssertNotNil(job.completedAt)
        }

        XCTAssertEqual(mockS3.putObjectInputs.count, 0)
        XCTAssertEqual(try database.fileCount(), 0)
    }

    func testRunUploadsPendingFileAndMarksDatabaseRecordUploaded() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let fileURL = sourceRoot.appendingPathComponent("nested/file.txt")
        try FileManager.default.createDirectory(at: fileURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        let payload = Data("hello-world".utf8)
        try payload.write(to: fileURL)

        let database = try makeDatabaseService()
        let mockS3 = MockBackupEngineS3Client()
        let backupEngine = BackupEngine(
            scanner: FileScanner(),
            fileManager: .default,
            database: database,
            glacierClientFactory: { _, _, _, _, db in
                GlacierClient(s3Client: mockS3, database: db)
            }
        )

        let settings = AppState.Settings(
            awsAccessKey: "access",
            awsSecretKey: "secret",
            awsRegion: "us-east-1",
            bucket: "bucket",
            sourcePath: sourceRoot.path
        )
        let job = await MainActor.run {
            BackupJob(sourceRoot: sourceRoot.path, bucket: "bucket")
        }

        try await backupEngine.run(job: job, settings: settings)

        await MainActor.run {
            XCTAssertEqual(job.status, .completed)
            XCTAssertEqual(job.filesTotal, 1)
            XCTAssertEqual(job.filesUploaded, 1)
            XCTAssertEqual(job.bytesTotal, Int64(payload.count))
            XCTAssertEqual(job.bytesUploaded, Int64(payload.count))
        }

        XCTAssertEqual(mockS3.putObjectInputs.count, 1)
        XCTAssertEqual(mockS3.putObjectInputs.first?.key, "nested/file.txt")
        XCTAssertEqual(try database.pendingFiles(for: sourceRoot.path).count, 0)
        let stored = try database.allFiles()
        XCTAssertEqual(stored.count, 1)
        XCTAssertEqual(stored.first?.glacierKey, "nested/file.txt")
        XCTAssertNotNil(stored.first?.uploadedAt)
    }

    func testRunFailsWhenSourceDirectoryDoesNotExist() async throws {
        let missingPath = "/tmp/icevault-missing-\(UUID().uuidString)"
        let backupEngine = BackupEngine(
            scanner: FileScanner(),
            fileManager: .default,
            database: try makeDatabaseService(),
            glacierClientFactory: { _, _, _, _, _ in
                XCTFail("Glacier client should not be created when source path is invalid")
                throw TestError.unexpectedGlacierClientCreation
            }
        )

        let settings = AppState.Settings(
            awsAccessKey: "access",
            awsSecretKey: "secret",
            awsRegion: "us-east-1",
            bucket: "bucket",
            sourcePath: missingPath
        )
        let job = await MainActor.run {
            BackupJob(sourceRoot: missingPath, bucket: "bucket")
        }

        do {
            try await backupEngine.run(job: job, settings: settings)
            XCTFail("Expected invalid source path failure")
        } catch let error as BackupEngineError {
            guard case .invalidSettings(let message) = error else {
                return XCTFail("Unexpected error: \(error)")
            }
            XCTAssertTrue(message.contains("Source path must be an existing directory"))
        }
    }

    private func makeTempDirectory() throws -> URL {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("IceVaultTests-Engine-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory
    }

    private func makeDatabaseService() throws -> DatabaseService {
        let directory = try makeTempDirectory()
        let databaseURL = directory.appendingPathComponent("icevault.sqlite")
        return try DatabaseService(databaseURL: databaseURL)
    }

    private enum TestError: Error {
        case unexpectedGlacierClientCreation
    }
}

private final class MockBackupEngineS3Client: GlacierS3Client {
    var putObjectInputs: [PutObjectInput] = []

    func createMultipartUpload(input: CreateMultipartUploadInput) async throws -> CreateMultipartUploadOutput {
        CreateMultipartUploadOutput(uploadId: "upload-id")
    }

    func uploadPart(input: UploadPartInput) async throws -> UploadPartOutput {
        UploadPartOutput(eTag: "\"etag\"")
    }

    func completeMultipartUpload(input: CompleteMultipartUploadInput) async throws -> CompleteMultipartUploadOutput {
        CompleteMultipartUploadOutput()
    }

    func abortMultipartUpload(input: AbortMultipartUploadInput) async throws -> AbortMultipartUploadOutput {
        AbortMultipartUploadOutput()
    }

    func listParts(input: ListPartsInput) async throws -> ListPartsOutput {
        ListPartsOutput(isTruncated: false, nextPartNumberMarker: nil, parts: [])
    }

    func putObject(input: PutObjectInput) async throws -> PutObjectOutput {
        putObjectInputs.append(input)
        return PutObjectOutput()
    }

    func headBucket(input: HeadBucketInput) async throws -> HeadBucketOutput {
        HeadBucketOutput()
    }
}
