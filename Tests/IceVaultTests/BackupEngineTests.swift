import Foundation
import AWSS3
import CryptoKit
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

    func testRunSkipsReuploadWhenRemoteObjectAlreadyMatches() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let fileURL = sourceRoot.appendingPathComponent("nested/file.txt")
        try FileManager.default.createDirectory(at: fileURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        let payload = Data("hello-world".utf8)
        try payload.write(to: fileURL)

        let database = try makeDatabaseService()
        let mockS3 = MockBackupEngineS3Client()
        mockS3.headObjectOutputsByKey["nested/file.txt"] = HeadObjectOutput(
            contentLength: payload.count,
            metadata: [GlacierClient.sha256MetadataKey: sha256Hex(of: payload)],
            storageClass: .deepArchive
        )

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

        XCTAssertEqual(mockS3.putObjectInputs.count, 0)
        XCTAssertEqual(mockS3.headObjectInputs.count, 1)
        XCTAssertEqual(mockS3.headObjectInputs.first?.key, "nested/file.txt")
        XCTAssertEqual(try database.pendingFiles(for: sourceRoot.path).count, 0)
        let stored = try database.allFiles()
        XCTAssertEqual(stored.count, 1)
        XCTAssertEqual(stored.first?.glacierKey, "nested/file.txt")
        XCTAssertNotNil(stored.first?.uploadedAt)
    }

    func testRunReuploadsUploadedFileWhenRemoteChecksumMetadataIsMissing() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let fileURL = sourceRoot.appendingPathComponent("nested/file.txt")
        try FileManager.default.createDirectory(at: fileURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        let payload = Data("hello-world".utf8)
        try payload.write(to: fileURL)

        let database = try makeDatabaseService()
        var existingRecord = FileRecord(
            sourcePath: sourceRoot.path,
            relativePath: "nested/file.txt",
            fileSize: Int64(payload.count),
            modifiedAt: Date(),
            sha256: sha256Hex(of: payload),
            glacierKey: "nested/file.txt",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&existingRecord)

        let mockS3 = MockBackupEngineS3Client()
        mockS3.defaultHeadObjectOutput = HeadObjectOutput(
            contentLength: payload.count,
            storageClass: .deepArchive
        )

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

        XCTAssertGreaterThanOrEqual(mockS3.headObjectInputs.count, 2)
        XCTAssertEqual(mockS3.putObjectInputs.count, 1)
        XCTAssertEqual(mockS3.putObjectInputs.first?.key, "nested/file.txt")
        XCTAssertEqual(try database.pendingFiles(for: sourceRoot.path).count, 0)
        let stored = try database.allFiles()
        XCTAssertEqual(stored.count, 1)
        XCTAssertNotNil(stored.first?.uploadedAt)
    }

    func testRunReuploadsPreviouslyUploadedFileWhenRemoteObjectIsMissing() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let fileURL = sourceRoot.appendingPathComponent("nested/file.txt")
        try FileManager.default.createDirectory(at: fileURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        let payload = Data("hello-world".utf8)
        try payload.write(to: fileURL)

        let database = try makeDatabaseService()
        var existingRecord = FileRecord(
            sourcePath: sourceRoot.path,
            relativePath: "nested/file.txt",
            fileSize: Int64(payload.count),
            modifiedAt: Date(),
            sha256: sha256Hex(of: payload),
            glacierKey: "nested/file.txt",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&existingRecord)

        let mockS3 = MockBackupEngineS3Client()
        mockS3.defaultHeadObjectError = MockHTTPStatusCodeError(statusCode: .notFound)

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

        XCTAssertEqual(mockS3.putObjectInputs.count, 1)
        XCTAssertEqual(mockS3.putObjectInputs.first?.key, "nested/file.txt")
        XCTAssertGreaterThanOrEqual(mockS3.headObjectInputs.count, 2)
        XCTAssertEqual(try database.pendingFiles(for: sourceRoot.path).count, 0)
        let stored = try database.allFiles()
        XCTAssertEqual(stored.count, 1)
        XCTAssertNotNil(stored.first?.uploadedAt)
    }

    func testRunUploadsPendingFileWhenRemoteValidationIsInaccessible() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let fileURL = sourceRoot.appendingPathComponent("nested/file.txt")
        try FileManager.default.createDirectory(at: fileURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        let payload = Data("hello-world".utf8)
        try payload.write(to: fileURL)

        let database = try makeDatabaseService()
        let mockS3 = MockBackupEngineS3Client()
        mockS3.defaultHeadObjectError = MockHTTPStatusCodeError(statusCode: .forbidden)

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
        }
        XCTAssertEqual(mockS3.headObjectInputs.count, 1)
        XCTAssertEqual(mockS3.putObjectInputs.count, 1)
        XCTAssertEqual(mockS3.putObjectInputs.first?.key, "nested/file.txt")
    }

    func testRunUploadsPendingFileWhenRemoteValidationHasTransientFailure() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let fileURL = sourceRoot.appendingPathComponent("nested/file.txt")
        try FileManager.default.createDirectory(at: fileURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        let payload = Data("hello-world".utf8)
        try payload.write(to: fileURL)

        let database = try makeDatabaseService()
        let mockS3 = MockBackupEngineS3Client()
        mockS3.headObjectErrorsByKey["nested/file.txt"] = TestError.syntheticHeadValidationFailure

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
        XCTAssertEqual(mockS3.headObjectInputs.count, 1)
        XCTAssertEqual(mockS3.putObjectInputs.count, 1)
        XCTAssertEqual(mockS3.putObjectInputs.first?.key, "nested/file.txt")
    }

    func testRunContinuesWhenUploadedObjectAuditValidationHasTransientFailure() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let existingFileURL = sourceRoot.appendingPathComponent("existing.txt")
        let pendingFileURL = sourceRoot.appendingPathComponent("pending.txt")
        try Data("existing-data".utf8).write(to: existingFileURL)
        try Data("pending-data".utf8).write(to: pendingFileURL)

        let database = try makeDatabaseService()
        var existingRecord = FileRecord(
            sourcePath: sourceRoot.path,
            relativePath: "existing.txt",
            fileSize: Int64(Data("existing-data".utf8).count),
            modifiedAt: Date(),
            sha256: sha256Hex(of: Data("existing-data".utf8)),
            glacierKey: "existing.txt",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&existingRecord)

        let mockS3 = MockBackupEngineS3Client()
        mockS3.headObjectErrorsByKey["existing.txt"] = TestError.syntheticHeadValidationFailure

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
        }

        XCTAssertEqual(mockS3.putObjectInputs.count, 1)
        XCTAssertEqual(mockS3.putObjectInputs.first?.key, "pending.txt")
        XCTAssertEqual(mockS3.headObjectInputs.compactMap(\.key).sorted(), ["existing.txt", "pending.txt"])

        let pending = try database.pendingFiles(for: sourceRoot.path)
        XCTAssertEqual(pending.count, 0)

        let stored = try database.allFiles()
        let uploadedExisting = try XCTUnwrap(stored.first(where: { $0.relativePath == "existing.txt" }))
        let uploadedPending = try XCTUnwrap(stored.first(where: { $0.relativePath == "pending.txt" }))
        XCTAssertNotNil(uploadedExisting.uploadedAt)
        XCTAssertNotNil(uploadedPending.uploadedAt)
    }

    func testRunKeepsUploadedFileWhenRemoteValidationIsInaccessible() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let fileURL = sourceRoot.appendingPathComponent("nested/file.txt")
        try FileManager.default.createDirectory(at: fileURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        let payload = Data("hello-world".utf8)
        try payload.write(to: fileURL)

        let database = try makeDatabaseService()
        var existingRecord = FileRecord(
            sourcePath: sourceRoot.path,
            relativePath: "nested/file.txt",
            fileSize: Int64(payload.count),
            modifiedAt: Date(),
            sha256: sha256Hex(of: payload),
            glacierKey: "nested/file.txt",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&existingRecord)

        let mockS3 = MockBackupEngineS3Client()
        mockS3.defaultHeadObjectError = MockHTTPStatusCodeError(statusCode: .forbidden)

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
        }
        XCTAssertEqual(mockS3.headObjectInputs.count, 1)
        XCTAssertEqual(mockS3.putObjectInputs.count, 0)
        XCTAssertEqual(try database.pendingFiles(for: sourceRoot.path).count, 0)
    }

    func testRunAuditsUploadedFileUsingPersistedGlacierKey() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let fileURL = sourceRoot.appendingPathComponent("nested/file.txt")
        try FileManager.default.createDirectory(at: fileURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        let payload = Data("hello-world".utf8)
        try payload.write(to: fileURL)

        let database = try makeDatabaseService()
        var existingRecord = FileRecord(
            sourcePath: sourceRoot.path,
            relativePath: "nested/file.txt",
            fileSize: Int64(payload.count),
            modifiedAt: Date(),
            sha256: sha256Hex(of: payload),
            glacierKey: "archive-prefix/nested/file.txt",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&existingRecord)

        let mockS3 = MockBackupEngineS3Client()
        mockS3.defaultHeadObjectError = MockHTTPStatusCodeError(statusCode: .notFound)
        mockS3.headObjectOutputsByKey["archive-prefix/nested/file.txt"] = HeadObjectOutput(
            contentLength: payload.count,
            metadata: [GlacierClient.sha256MetadataKey: sha256Hex(of: payload)],
            storageClass: .deepArchive
        )

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
        }

        XCTAssertEqual(mockS3.headObjectInputs.count, 1)
        XCTAssertEqual(mockS3.headObjectInputs.first?.key, "archive-prefix/nested/file.txt")
        XCTAssertEqual(mockS3.putObjectInputs.count, 0)
        XCTAssertEqual(try database.pendingFiles(for: sourceRoot.path).count, 0)
    }

    func testRunSkipsAuditForDeletedUploadedRecordsAfterScanCleanup() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let database = try makeDatabaseService()
        var staleUploadedRecord = FileRecord(
            sourcePath: sourceRoot.path,
            relativePath: "deleted.txt",
            fileSize: 4,
            modifiedAt: Date(timeIntervalSince1970: 1_700_000_000),
            sha256: "deadbeef",
            glacierKey: "deleted.txt",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&staleUploadedRecord)

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
        }

        XCTAssertEqual(mockS3.headObjectInputs.count, 0)
        XCTAssertEqual(try database.fileCount(), 0)
    }

    func testRunReuploadsMissingAuditedFileUsingPersistedGlacierKey() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let fileURL = sourceRoot.appendingPathComponent("nested/file.txt")
        try FileManager.default.createDirectory(at: fileURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        let payload = Data("hello-world".utf8)
        try payload.write(to: fileURL)

        let database = try makeDatabaseService()
        var existingRecord = FileRecord(
            sourcePath: sourceRoot.path,
            relativePath: "nested/file.txt",
            fileSize: Int64(payload.count),
            modifiedAt: Date(),
            sha256: sha256Hex(of: payload),
            glacierKey: "archive-prefix/nested/file.txt",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&existingRecord)

        let mockS3 = MockBackupEngineS3Client()
        mockS3.defaultHeadObjectError = MockHTTPStatusCodeError(statusCode: .notFound)

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
        }

        XCTAssertGreaterThanOrEqual(mockS3.headObjectInputs.count, 2)
        XCTAssertEqual(
            Set(mockS3.headObjectInputs.compactMap(\.key)),
            ["archive-prefix/nested/file.txt"]
        )
        XCTAssertEqual(mockS3.putObjectInputs.count, 1)
        XCTAssertEqual(mockS3.putObjectInputs.first?.key, "archive-prefix/nested/file.txt")

        let stored = try database.allFiles()
        XCTAssertEqual(stored.first?.glacierKey, "archive-prefix/nested/file.txt")
        XCTAssertNotNil(stored.first?.uploadedAt)
        XCTAssertEqual(try database.pendingFiles(for: sourceRoot.path).count, 0)
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

    func testRunUploadsMultipleFilesWithBoundedConcurrency() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let fileCount = 5
        for index in 0..<fileCount {
            let fileURL = sourceRoot.appendingPathComponent("file-\(index).txt")
            try Data("payload-\(index)".utf8).write(to: fileURL)
        }

        let database = try makeDatabaseService()
        let mockS3 = ConcurrencyTrackingBackupEngineS3Client(putObjectDelayNanoseconds: 120_000_000)
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
            sourcePath: sourceRoot.path,
            maxConcurrentFileUploads: 2,
            maxConcurrentMultipartPartUploads: 1
        )
        let job = await MainActor.run {
            BackupJob(sourceRoot: sourceRoot.path, bucket: "bucket")
        }

        try await backupEngine.run(job: job, settings: settings)

        await MainActor.run {
            XCTAssertEqual(job.status, .completed)
            XCTAssertEqual(job.filesTotal, fileCount)
            XCTAssertEqual(job.filesUploaded, fileCount)
        }

        XCTAssertEqual(try database.pendingFiles(for: sourceRoot.path).count, 0)
        let stats = await mockS3.stats()
        XCTAssertEqual(stats.putObjectCallCount, fileCount)
        XCTAssertEqual(stats.maxInFlightPutObjectCalls, 2)
    }

    func testRunRecordsCompletedConcurrentUploadWhenSiblingFails() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        try Data("alpha".utf8).write(to: sourceRoot.appendingPathComponent("alpha.txt"))
        try Data("beta".utf8).write(to: sourceRoot.appendingPathComponent("beta.txt"))

        let database = try makeDatabaseService()
        let mockS3 = DeterministicFailureBackupEngineS3Client(
            blockedKey: "alpha.txt",
            failingKey: "beta.txt"
        )
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
            sourcePath: sourceRoot.path,
            maxConcurrentFileUploads: 2,
            maxConcurrentMultipartPartUploads: 1
        )
        let job = await MainActor.run {
            BackupJob(sourceRoot: sourceRoot.path, bucket: "bucket")
        }

        let runTask = Task {
            try await backupEngine.run(job: job, settings: settings)
        }

        await mockS3.waitUntilFailureTriggered()
        await mockS3.releaseBlockedUpload()

        do {
            try await runTask.value
            XCTFail("Expected backup to fail")
        } catch {}

        let files = try database.allFiles()
        let alphaRecord = try XCTUnwrap(files.first(where: { $0.relativePath == "alpha.txt" }))
        let betaRecord = try XCTUnwrap(files.first(where: { $0.relativePath == "beta.txt" }))
        XCTAssertNotNil(alphaRecord.uploadedAt)
        XCTAssertNil(betaRecord.uploadedAt)
        XCTAssertEqual(try database.pendingFiles(for: sourceRoot.path).map(\.relativePath), ["beta.txt"])

        await MainActor.run {
            XCTAssertEqual(job.status, .failed)
        }
    }

    func testRunUploadsEarlierFileBeforeFailingWhenLaterFileDisappears() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let firstFileURL = sourceRoot.appendingPathComponent("a.txt")
        let missingFileURL = sourceRoot.appendingPathComponent("z.txt")
        try Data("alpha".utf8).write(to: firstFileURL)
        try Data("zeta".utf8).write(to: missingFileURL)

        let missingFileManager = SelectivelyMissingFileManager(missingPath: missingFileURL.path)
        let database = try makeDatabaseService()
        let mockS3 = MockBackupEngineS3Client()
        let backupEngine = BackupEngine(
            scanner: FileScanner(),
            fileManager: missingFileManager,
            database: database,
            glacierClientFactory: { _, _, _, _, db in
                GlacierClient(s3Client: mockS3, fileManager: missingFileManager, database: db)
            }
        )

        let settings = AppState.Settings(
            awsAccessKey: "access",
            awsSecretKey: "secret",
            awsRegion: "us-east-1",
            bucket: "bucket",
            sourcePath: sourceRoot.path,
            maxConcurrentFileUploads: 1,
            maxConcurrentMultipartPartUploads: 1
        )
        let job = await MainActor.run {
            BackupJob(sourceRoot: sourceRoot.path, bucket: "bucket")
        }

        do {
            try await backupEngine.run(job: job, settings: settings)
            XCTFail("Expected backup to fail when one source file is missing")
        } catch {}

        XCTAssertEqual(mockS3.putObjectInputs.count, 1)
        XCTAssertEqual(mockS3.putObjectInputs.first?.key, "a.txt")
        XCTAssertEqual(try database.pendingFiles(for: sourceRoot.path).map(\.relativePath), ["z.txt"])

        let files = try database.allFiles()
        let firstRecord = try XCTUnwrap(files.first(where: { $0.relativePath == "a.txt" }))
        let missingRecord = try XCTUnwrap(files.first(where: { $0.relativePath == "z.txt" }))
        XCTAssertNotNil(firstRecord.uploadedAt)
        XCTAssertNil(missingRecord.uploadedAt)

        await MainActor.run {
            XCTAssertEqual(job.status, .failed)
        }
    }

    func testRunCancellationDuringRemoteValidationWithNoPendingUploadsFailsAsCanceled() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let fileURL = sourceRoot.appendingPathComponent("one.txt")
        let payload = Data("one".utf8)
        try payload.write(to: fileURL)

        let database = try makeDatabaseService()
        var existingRecord = FileRecord(
            sourcePath: sourceRoot.path,
            relativePath: "one.txt",
            fileSize: Int64(payload.count),
            modifiedAt: Date(),
            sha256: sha256Hex(of: payload),
            glacierKey: "one.txt",
            uploadedAt: Date(),
            storageClass: FileRecord.deepArchiveStorageClass
        )
        try database.insertFile(&existingRecord)

        let mockS3 = CancelIgnoringRemoteValidationBackupEngineS3Client(
            output: HeadObjectOutput(
                contentLength: payload.count,
                metadata: [GlacierClient.sha256MetadataKey: sha256Hex(of: payload)],
                storageClass: .deepArchive
            )
        )
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
            sourcePath: sourceRoot.path,
            maxConcurrentFileUploads: 1,
            maxConcurrentMultipartPartUploads: 1
        )
        let job = await MainActor.run {
            BackupJob(sourceRoot: sourceRoot.path, bucket: "bucket")
        }

        let runTask = Task {
            try await backupEngine.run(job: job, settings: settings)
        }

        await mockS3.waitUntilRemoteValidationStarts()
        runTask.cancel()
        await mockS3.releaseRemoteValidation()

        do {
            try await runTask.value
            XCTFail("Expected backup to be canceled")
        } catch is CancellationError {
            // Expected
        } catch {
            XCTFail("Expected CancellationError, got \(error)")
        }

        let headObjectCallCount = await mockS3.headObjectCallCount()
        XCTAssertEqual(headObjectCallCount, 1)
        XCTAssertEqual(try database.pendingFiles(for: sourceRoot.path).count, 0)

        await MainActor.run {
            XCTAssertEqual(job.status, .failed)
            XCTAssertEqual(job.error, "Backup canceled")
        }
    }

    func testRunCancellationDoesNotScheduleAdditionalUploads() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        try Data("one".utf8).write(to: sourceRoot.appendingPathComponent("one.txt"))
        try Data("two".utf8).write(to: sourceRoot.appendingPathComponent("two.txt"))
        try Data("three".utf8).write(to: sourceRoot.appendingPathComponent("three.txt"))

        let database = try makeDatabaseService()
        let mockS3 = CancelIgnoringSequencedBackupEngineS3Client()
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
            sourcePath: sourceRoot.path,
            maxConcurrentFileUploads: 1,
            maxConcurrentMultipartPartUploads: 1
        )
        let job = await MainActor.run {
            BackupJob(sourceRoot: sourceRoot.path, bucket: "bucket")
        }

        let runTask = Task {
            try await backupEngine.run(job: job, settings: settings)
        }

        await mockS3.waitUntilFirstUploadStarts()
        runTask.cancel()
        await mockS3.releaseFirstUpload()

        do {
            try await runTask.value
            XCTFail("Expected backup to be canceled")
        } catch is CancellationError {
            // Expected
        } catch {
            XCTFail("Expected CancellationError, got \(error)")
        }

        let putObjectCallCount = await mockS3.putObjectCallCount()
        XCTAssertEqual(putObjectCallCount, 1)

        await MainActor.run {
            XCTAssertEqual(job.status, .failed)
            XCTAssertEqual(job.error, "Backup canceled")
        }
    }

    func testRunCancellationAfterFinalUploadStillCompletes() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        try Data("one".utf8).write(to: sourceRoot.appendingPathComponent("one.txt"))
        try Data("two".utf8).write(to: sourceRoot.appendingPathComponent("two.txt"))

        let database = try makeDatabaseService()
        let mockS3 = FinalUploadGatedBackupEngineS3Client(gatedKey: "two.txt")
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
            sourcePath: sourceRoot.path,
            maxConcurrentFileUploads: 1,
            maxConcurrentMultipartPartUploads: 1
        )
        let job = await MainActor.run {
            BackupJob(sourceRoot: sourceRoot.path, bucket: "bucket")
        }

        let runTask = Task {
            try await backupEngine.run(job: job, settings: settings)
        }

        await mockS3.waitUntilFinalUploadStarts()
        runTask.cancel()
        await mockS3.releaseFinalUpload()

        do {
            try await runTask.value
        } catch {
            XCTFail("Expected backup to complete after final upload, got \(error)")
        }

        let putObjectCallCount = await mockS3.putObjectCallCount()
        XCTAssertEqual(putObjectCallCount, 2)
        XCTAssertEqual(try database.pendingFiles(for: sourceRoot.path).count, 0)

        await MainActor.run {
            XCTAssertEqual(job.status, .completed)
            XCTAssertEqual(job.filesTotal, 2)
            XCTAssertEqual(job.filesUploaded, 2)
        }
    }

    func testRunPersistsUploadedRecordBeforeRunFinishes() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        try Data("one".utf8).write(to: sourceRoot.appendingPathComponent("one.txt"))
        try Data("two".utf8).write(to: sourceRoot.appendingPathComponent("two.txt"))

        let database = try makeDatabaseService()
        let mockS3 = FinalUploadGatedBackupEngineS3Client(gatedKey: "two.txt")
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
            sourcePath: sourceRoot.path,
            maxConcurrentFileUploads: 1,
            maxConcurrentMultipartPartUploads: 1
        )
        let job = await MainActor.run {
            BackupJob(sourceRoot: sourceRoot.path, bucket: "bucket")
        }

        let runTask = Task {
            try await backupEngine.run(job: job, settings: settings)
        }

        await mockS3.waitUntilFinalUploadStarts()

        do {
            let files = try database.allFiles()
            let firstRecord = try XCTUnwrap(files.first(where: { $0.relativePath == "one.txt" }))
            let secondRecord = try XCTUnwrap(files.first(where: { $0.relativePath == "two.txt" }))

            XCTAssertNotNil(
                firstRecord.uploadedAt,
                "Completed uploads should be persisted before the backup run exits."
            )
            XCTAssertNil(secondRecord.uploadedAt)
        } catch {
            runTask.cancel()
            await mockS3.releaseFinalUpload()
            _ = try? await runTask.value
            throw error
        }

        await mockS3.releaseFinalUpload()
        try await runTask.value
    }

    func testRunUsesStreamingScannerAPI() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let payload = Data("streaming".utf8)
        let fileURL = sourceRoot.appendingPathComponent("streaming.txt")
        try payload.write(to: fileURL)

        let scanner = StreamingOnlyFileScanner(
            records: [
                FileRecord(
                    sourcePath: sourceRoot.path,
                    relativePath: "streaming.txt",
                    fileSize: Int64(payload.count),
                    modifiedAt: Date(timeIntervalSince1970: 1_700_001_000),
                    sha256: sha256Hex(of: payload),
                    glacierKey: "",
                    uploadedAt: nil,
                    storageClass: FileRecord.deepArchiveStorageClass
                )
            ]
        )
        let database = try makeDatabaseService()
        let mockS3 = MockBackupEngineS3Client()
        let backupEngine = BackupEngine(
            scanner: scanner,
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

        XCTAssertEqual(scanner.streamingScanCallCount, 1)
        XCTAssertEqual(scanner.arrayScanCallCount, 0)
        XCTAssertEqual(mockS3.putObjectInputs.count, 1)
        XCTAssertEqual(mockS3.putObjectInputs.first?.key, "streaming.txt")
    }

    func testRunStartsUploadingBeforeScanCompletes() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let firstPayload = Data("first".utf8)
        let secondPayload = Data("second".utf8)
        let firstPath = "one.txt"
        let secondPath = "two.txt"
        try firstPayload.write(to: sourceRoot.appendingPathComponent(firstPath))
        try secondPayload.write(to: sourceRoot.appendingPathComponent(secondPath))

        let scanner = GatedAfterFirstRecordScanner(
            records: [
                FileRecord(
                    sourcePath: sourceRoot.path,
                    relativePath: firstPath,
                    fileSize: Int64(firstPayload.count),
                    modifiedAt: Date(timeIntervalSince1970: 1_700_002_000),
                    sha256: sha256Hex(of: firstPayload),
                    glacierKey: "",
                    uploadedAt: nil,
                    storageClass: FileRecord.deepArchiveStorageClass
                ),
                FileRecord(
                    sourcePath: sourceRoot.path,
                    relativePath: secondPath,
                    fileSize: Int64(secondPayload.count),
                    modifiedAt: Date(timeIntervalSince1970: 1_700_002_001),
                    sha256: sha256Hex(of: secondPayload),
                    glacierKey: "",
                    uploadedAt: nil,
                    storageClass: FileRecord.deepArchiveStorageClass
                )
            ]
        )

        let database = try makeDatabaseService()
        let mockS3 = CancelIgnoringSequencedBackupEngineS3Client()
        let backupEngine = BackupEngine(
            scanner: scanner,
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
            sourcePath: sourceRoot.path,
            maxConcurrentFileUploads: 1,
            maxConcurrentMultipartPartUploads: 1
        )
        let job = await MainActor.run {
            BackupJob(sourceRoot: sourceRoot.path, bucket: "bucket")
        }

        let runTask = Task {
            try await backupEngine.run(job: job, settings: settings)
        }

        guard scanner.waitUntilFirstRecordIsProcessed(timeout: .now() + 2) else {
            runTask.cancel()
            scanner.resumeAfterFirstRecord()
            _ = try? await runTask.value
            XCTFail("Timed out waiting for first scan record.")
            return
        }

        let uploadStartDeadline = Date().addingTimeInterval(2)
        var didStartUpload = false
        while Date() < uploadStartDeadline {
            if await mockS3.putObjectCallCount() > 0 {
                didStartUpload = true
                break
            }
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        guard didStartUpload else {
            runTask.cancel()
            scanner.resumeAfterFirstRecord()
            _ = try? await runTask.value
            XCTFail("Timed out waiting for first upload to start.")
            return
        }

        XCTAssertFalse(scanner.didProcessSecondRecord)

        scanner.resumeAfterFirstRecord()
        await mockS3.releaseFirstUpload()

        try await runTask.value

        let putObjectCallCount = await mockS3.putObjectCallCount()
        XCTAssertEqual(putObjectCallCount, 2)

        await MainActor.run {
            XCTAssertEqual(job.status, .completed)
            XCTAssertEqual(job.filesTotal, 2)
            XCTAssertEqual(job.filesUploaded, 2)
        }
    }

    func testRunAppliesBackpressureWhenUploadsLagBehindScan() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let totalFileCount = 300
        var records: [FileRecord] = []
        records.reserveCapacity(totalFileCount)

        for index in 0..<totalFileCount {
            let relativePath = index == 0 ? "one.txt" : String(format: "file-%03d.txt", index)
            let payload = Data("payload-\(index)".utf8)
            try payload.write(to: sourceRoot.appendingPathComponent(relativePath))
            records.append(
                FileRecord(
                    sourcePath: sourceRoot.path,
                    relativePath: relativePath,
                    fileSize: Int64(payload.count),
                    modifiedAt: Date(timeIntervalSince1970: 1_700_003_000 + TimeInterval(index)),
                    sha256: sha256Hex(of: payload),
                    glacierKey: "",
                    uploadedAt: nil,
                    storageClass: FileRecord.deepArchiveStorageClass
                )
            )
        }

        let scanner = CompletionSignalingStreamingScanner(records: records)
        let database = try makeDatabaseService()
        let mockS3 = CancelIgnoringSequencedBackupEngineS3Client()
        let backupEngine = BackupEngine(
            scanner: scanner,
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
            sourcePath: sourceRoot.path,
            maxConcurrentFileUploads: 1,
            maxConcurrentMultipartPartUploads: 1
        )
        let job = await MainActor.run {
            BackupJob(sourceRoot: sourceRoot.path, bucket: "bucket")
        }

        let runTask = Task {
            try await backupEngine.run(job: job, settings: settings)
        }

        await mockS3.waitUntilFirstUploadStarts()
        XCTAssertFalse(
            scanner.waitUntilScanCompletes(timeout: .now() + 1),
            "Scan should not finish while uploads are blocked and pending queue is full."
        )

        runTask.cancel()
        await mockS3.releaseFirstUpload()

        do {
            try await runTask.value
            XCTFail("Expected backup to be canceled")
        } catch is CancellationError {
            // Expected
        } catch {
            XCTFail("Expected CancellationError, got \(error)")
        }
    }

    func testRunFailsPromptlyWhenUploadLoopFailsUnderBackpressure() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let totalFileCount = 400
        var records: [FileRecord] = []
        records.reserveCapacity(totalFileCount)

        for index in 0..<totalFileCount {
            let relativePath = index == 0 ? "one.txt" : String(format: "file-%03d.txt", index)
            let payload = Data("payload-\(index)".utf8)
            try payload.write(to: sourceRoot.appendingPathComponent(relativePath))
            records.append(
                FileRecord(
                    sourcePath: sourceRoot.path,
                    relativePath: relativePath,
                    fileSize: Int64(payload.count),
                    modifiedAt: Date(timeIntervalSince1970: 1_700_006_000 + TimeInterval(index)),
                    sha256: sha256Hex(of: payload),
                    glacierKey: "",
                    uploadedAt: nil,
                    storageClass: FileRecord.deepArchiveStorageClass
                )
            )
        }

        let scanner = CompletionSignalingStreamingScanner(records: records)
        let database = try makeDatabaseService()
        let mockS3 = DeterministicFailureBackupEngineS3Client(
            blockedKey: "never-blocked.txt",
            failingKey: "one.txt"
        )
        let backupEngine = BackupEngine(
            scanner: scanner,
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
            sourcePath: sourceRoot.path,
            maxConcurrentFileUploads: 1,
            maxConcurrentMultipartPartUploads: 1
        )
        let job = await MainActor.run {
            BackupJob(sourceRoot: sourceRoot.path, bucket: "bucket")
        }

        let runTask = Task {
            try await backupEngine.run(job: job, settings: settings)
        }

        await mockS3.waitUntilFailureTriggered()

        let runOutcome = await waitForRunTaskOutcome(
            runTask,
            timeoutNanoseconds: 2_000_000_000
        )
        switch runOutcome {
        case .failed(let error):
            XCTAssertFalse(
                error is CancellationError,
                "Upload failures should preserve the underlying error instead of becoming cancellation."
            )
        case .succeeded:
            XCTFail("Expected backup to fail after upload error.")
        case .timedOut:
            runTask.cancel()
            _ = try? await runTask.value
            XCTFail("Timed out waiting for backup failure after upload loop error.")
        }

        XCTAssertTrue(
            scanner.waitUntilScanCompletes(timeout: .now() + 2),
            "Scan should stop quickly after upload loop failure."
        )

        await MainActor.run {
            XCTAssertEqual(job.status, .failed)
        }
    }

    func testRunBackpressureCapCanBeTunedViaEnvironmentOverride() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let totalFileCount = 300
        var records: [FileRecord] = []
        records.reserveCapacity(totalFileCount)

        for index in 0..<totalFileCount {
            let relativePath = index == 0 ? "one.txt" : String(format: "file-%03d.txt", index)
            let payload = Data("payload-\(index)".utf8)
            try payload.write(to: sourceRoot.appendingPathComponent(relativePath))
            records.append(
                FileRecord(
                    sourcePath: sourceRoot.path,
                    relativePath: relativePath,
                    fileSize: Int64(payload.count),
                    modifiedAt: Date(timeIntervalSince1970: 1_700_004_000 + TimeInterval(index)),
                    sha256: sha256Hex(of: payload),
                    glacierKey: "",
                    uploadedAt: nil,
                    storageClass: FileRecord.deepArchiveStorageClass
                )
            )
        }

        let scanner = CompletionSignalingStreamingScanner(records: records)
        let database = try makeDatabaseService()
        let mockS3 = CancelIgnoringSequencedBackupEngineS3Client()
        let backupEngine = BackupEngine(
            scanner: scanner,
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
            sourcePath: sourceRoot.path,
            maxConcurrentFileUploads: 1,
            maxConcurrentMultipartPartUploads: 1
        )
        let job = await MainActor.run {
            BackupJob(sourceRoot: sourceRoot.path, bucket: "bucket")
        }

        await withEnvironmentVariable(
            name: "ICEVAULT_MAX_BUFFERED_PENDING_PLANS",
            value: "5000"
        ) {
            let runTask = Task {
                try await backupEngine.run(job: job, settings: settings)
            }

            await mockS3.waitUntilFirstUploadStarts()
            XCTAssertTrue(
                scanner.waitUntilScanCompletes(timeout: .now() + 2),
                "Large override should allow scan to complete even while uploads are blocked."
            )

            runTask.cancel()
            await mockS3.releaseFirstUpload()

            do {
                try await runTask.value
                XCTFail("Expected backup to be canceled")
            } catch is CancellationError {
                // Expected
            } catch {
                XCTFail("Expected CancellationError, got \(error)")
            }
        }
    }

    func testRunBackpressureCapCanBeTunedViaSettings() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let totalFileCount = 300
        var records: [FileRecord] = []
        records.reserveCapacity(totalFileCount)

        for index in 0..<totalFileCount {
            let relativePath = index == 0 ? "one.txt" : String(format: "file-%03d.txt", index)
            let payload = Data("payload-\(index)".utf8)
            try payload.write(to: sourceRoot.appendingPathComponent(relativePath))
            records.append(
                FileRecord(
                    sourcePath: sourceRoot.path,
                    relativePath: relativePath,
                    fileSize: Int64(payload.count),
                    modifiedAt: Date(timeIntervalSince1970: 1_700_005_000 + TimeInterval(index)),
                    sha256: sha256Hex(of: payload),
                    glacierKey: "",
                    uploadedAt: nil,
                    storageClass: FileRecord.deepArchiveStorageClass
                )
            )
        }

        let scanner = CompletionSignalingStreamingScanner(records: records)
        let database = try makeDatabaseService()
        let mockS3 = CancelIgnoringSequencedBackupEngineS3Client()
        let backupEngine = BackupEngine(
            scanner: scanner,
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
            sourcePath: sourceRoot.path,
            maxConcurrentFileUploads: 1,
            maxConcurrentMultipartPartUploads: 1,
            maxBufferedPendingPlans: 5000
        )
        let job = await MainActor.run {
            BackupJob(sourceRoot: sourceRoot.path, bucket: "bucket")
        }

        await withEnvironmentVariable(
            name: "ICEVAULT_MAX_BUFFERED_PENDING_PLANS",
            value: nil
        ) {
            let runTask = Task {
                try await backupEngine.run(job: job, settings: settings)
            }

            await mockS3.waitUntilFirstUploadStarts()
            XCTAssertTrue(
                scanner.waitUntilScanCompletes(timeout: .now() + 2),
                "Configured settings cap should allow scan completion while upload is blocked."
            )

            runTask.cancel()
            await mockS3.releaseFirstUpload()

            do {
                try await runTask.value
                XCTFail("Expected backup to be canceled")
            } catch is CancellationError {
                // Expected
            } catch {
                XCTFail("Expected CancellationError, got \(error)")
            }
        }
    }

    func testRunChunksRemoteAuditPlansWhenBatchExceedsBufferCap() async throws {
        let sourceRoot = try makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: sourceRoot) }

        let totalFileCount = 260
        let uploadedAt = Date(timeIntervalSince1970: 1_700_007_000)
        let database = try makeDatabaseService()

        for index in 0..<totalFileCount {
            let relativePath = String(format: "file-%03d.txt", index)
            let payload = Data("payload-\(index)".utf8)
            let fileURL = sourceRoot.appendingPathComponent(relativePath)
            try payload.write(to: fileURL)

            var uploadedRecord = FileRecord(
                sourcePath: sourceRoot.path,
                relativePath: relativePath,
                fileSize: Int64(payload.count),
                modifiedAt: Date(timeIntervalSince1970: 1_700_007_000 + TimeInterval(index)),
                sha256: sha256Hex(of: payload),
                glacierKey: relativePath,
                uploadedAt: uploadedAt,
                storageClass: FileRecord.deepArchiveStorageClass
            )
            try database.insertFile(&uploadedRecord)
        }

        let s3Client = ConcurrencyTrackingBackupEngineS3Client(putObjectDelayNanoseconds: 0)

        let backupEngine = BackupEngine(
            scanner: FileScanner(),
            fileManager: .default,
            database: database,
            glacierClientFactory: { _, _, _, _, db in
                GlacierClient(s3Client: s3Client, database: db)
            }
        )

        let settings = AppState.Settings(
            awsAccessKey: "access",
            awsSecretKey: "secret",
            awsRegion: "us-east-1",
            bucket: "bucket",
            sourcePath: sourceRoot.path,
            maxConcurrentFileUploads: 1,
            maxConcurrentMultipartPartUploads: 1,
            maxBufferedPendingPlans: AppState.Settings.minimumBufferedPendingPlans
        )
        let job = await MainActor.run {
            BackupJob(sourceRoot: sourceRoot.path, bucket: "bucket")
        }

        let runTask = Task {
            try await backupEngine.run(job: job, settings: settings)
        }

        let runOutcome = await waitForRunTaskOutcome(
            runTask,
            timeoutNanoseconds: 10_000_000_000
        )
        switch runOutcome {
        case .succeeded:
            break
        case .failed(let error):
            XCTFail("Expected backup to complete after chunking remote audit plans, got \(error)")
        case .timedOut:
            runTask.cancel()
            _ = try? await runTask.value
            XCTFail("Backup run timed out while processing remote audit plans larger than buffer cap.")
        }

        await MainActor.run {
            XCTAssertEqual(job.status, .completed)
            XCTAssertEqual(job.filesTotal, totalFileCount)
            XCTAssertEqual(job.filesUploaded, totalFileCount)
        }

        let stats = await s3Client.stats()
        XCTAssertEqual(stats.putObjectCallCount, totalFileCount)
        XCTAssertEqual(try database.pendingFiles(for: sourceRoot.path).count, 0)
    }

    func testApplyMonotonicProgressIgnoresOutOfOrderSnapshots() async {
        let job = await MainActor.run {
            BackupJob(
                sourceRoot: "/tmp/source",
                bucket: "bucket",
                status: .uploading,
                filesTotal: 3,
                filesUploaded: 0,
                bytesTotal: 300,
                bytesUploaded: 0
            )
        }

        await MainActor.run {
            BackupEngine.applyMonotonicProgress(filesUploaded: 2, bytesUploaded: 200, to: job)
            BackupEngine.applyMonotonicProgress(filesUploaded: 1, bytesUploaded: 120, to: job)
            BackupEngine.applyMonotonicProgress(filesUploaded: 4, bytesUploaded: 360, to: job)
        }

        await MainActor.run {
            XCTAssertEqual(job.filesUploaded, 3)
            XCTAssertEqual(job.bytesUploaded, 300)
        }
    }

    private func makeTempDirectory() throws -> URL {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("IceVaultTests-Engine-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory
    }

    private func sha256Hex(of data: Data) -> String {
        SHA256.hash(data: data)
            .map { String(format: "%02x", $0) }
            .joined()
    }

    private func makeDatabaseService() throws -> DatabaseService {
        let directory = try makeTempDirectory()
        let databaseURL = directory.appendingPathComponent("icevault.sqlite")
        return try DatabaseService(databaseURL: databaseURL)
    }

    private func withEnvironmentVariable<T>(
        name: String,
        value: String?,
        operation: () async throws -> T
    ) async rethrows -> T {
        let previousValue = ProcessInfo.processInfo.environment[name]
        if let value {
            setenv(name, value, 1)
        } else {
            unsetenv(name)
        }

        defer {
            if let previousValue {
                setenv(name, previousValue, 1)
            } else {
                unsetenv(name)
            }
        }

        return try await operation()
    }

    private func waitForRunTaskOutcome(
        _ task: Task<Void, Error>,
        timeoutNanoseconds: UInt64
    ) async -> RunTaskOutcome {
        await withTaskGroup(of: RunTaskOutcome.self) { group in
            group.addTask {
                do {
                    try await task.value
                    return .succeeded
                } catch {
                    return .failed(error)
                }
            }

            group.addTask {
                try? await Task.sleep(nanoseconds: timeoutNanoseconds)
                return .timedOut
            }

            let outcome = await group.next() ?? .timedOut
            group.cancelAll()
            return outcome
        }
    }

    private enum TestError: Error {
        case unexpectedGlacierClientCreation
        case syntheticHeadValidationFailure
    }

    private enum RunTaskOutcome {
        case succeeded
        case failed(Error)
        case timedOut
    }
}

private final class SelectivelyMissingFileManager: FileManager {
    private let missingPath: String

    init(missingPath: String) {
        self.missingPath = missingPath
        super.init()
    }

    override func fileExists(atPath path: String) -> Bool {
        if path == missingPath {
            return false
        }
        return super.fileExists(atPath: path)
    }

    override func fileExists(atPath path: String, isDirectory: UnsafeMutablePointer<ObjCBool>?) -> Bool {
        if path == missingPath {
            isDirectory?.pointee = false
            return false
        }
        return super.fileExists(atPath: path, isDirectory: isDirectory)
    }
}

private final class StreamingOnlyFileScanner: FileScanner, @unchecked Sendable {
    private let records: [FileRecord]

    private(set) var streamingScanCallCount = 0
    private(set) var arrayScanCallCount = 0

    init(records: [FileRecord]) {
        self.records = records
        super.init(fileManager: .default)
    }

    override func scan(
        sourceRoot: String,
        onRecord: (FileRecord) throws -> Void
    ) throws {
        streamingScanCallCount += 1
        for record in records {
            try onRecord(record)
        }
    }

    override func scan(
        sourceRoot: String,
        onRecord: @escaping (FileRecord) async throws -> Void
    ) async throws {
        streamingScanCallCount += 1
        for record in records {
            try await onRecord(record)
        }
    }

    override func scan(sourceRoot: String) throws -> [FileRecord] {
        arrayScanCallCount += 1
        XCTFail("Array-based scanner API should not be used by BackupEngine.")
        return []
    }
}

private final class GatedAfterFirstRecordScanner: FileScanner, @unchecked Sendable {
    private let records: [FileRecord]
    private let firstRecordProcessedSemaphore = DispatchSemaphore(value: 0)
    private let continueAfterFirstRecordSemaphore = DispatchSemaphore(value: 0)
    private let stateLock = NSLock()
    private var hasProcessedSecondRecord = false

    init(records: [FileRecord]) {
        self.records = records
        super.init(fileManager: .default)
    }

    var didProcessSecondRecord: Bool {
        stateLock.lock()
        defer { stateLock.unlock() }
        return hasProcessedSecondRecord
    }

    func waitUntilFirstRecordIsProcessed(timeout: DispatchTime) -> Bool {
        firstRecordProcessedSemaphore.wait(timeout: timeout) == .success
    }

    func resumeAfterFirstRecord() {
        continueAfterFirstRecordSemaphore.signal()
    }

    private func waitForResumeAfterFirstRecord() {
        continueAfterFirstRecordSemaphore.wait()
    }

    private func markProcessedSecondRecord() {
        stateLock.lock()
        hasProcessedSecondRecord = true
        stateLock.unlock()
    }

    override func scan(
        sourceRoot: String,
        onRecord: (FileRecord) throws -> Void
    ) throws {
        guard !records.isEmpty else {
            return
        }

        try onRecord(records[0])
        firstRecordProcessedSemaphore.signal()
        waitForResumeAfterFirstRecord()

        for record in records.dropFirst() {
            markProcessedSecondRecord()
            try onRecord(record)
        }
    }

    override func scan(
        sourceRoot: String,
        onRecord: @escaping (FileRecord) async throws -> Void
    ) async throws {
        guard !records.isEmpty else {
            return
        }

        try await onRecord(records[0])
        firstRecordProcessedSemaphore.signal()
        waitForResumeAfterFirstRecord()

        for record in records.dropFirst() {
            markProcessedSecondRecord()
            try await onRecord(record)
        }
    }
}

private final class CompletionSignalingStreamingScanner: FileScanner, @unchecked Sendable {
    private let records: [FileRecord]
    private let scanCompletionSemaphore = DispatchSemaphore(value: 0)

    init(records: [FileRecord]) {
        self.records = records
        super.init(fileManager: .default)
    }

    func waitUntilScanCompletes(timeout: DispatchTime) -> Bool {
        scanCompletionSemaphore.wait(timeout: timeout) == .success
    }

    override func scan(
        sourceRoot: String,
        onRecord: (FileRecord) throws -> Void
    ) throws {
        defer { scanCompletionSemaphore.signal() }
        for record in records {
            try onRecord(record)
        }
    }

    override func scan(
        sourceRoot: String,
        onRecord: @escaping (FileRecord) async throws -> Void
    ) async throws {
        defer { scanCompletionSemaphore.signal() }
        for record in records {
            try await onRecord(record)
        }
    }
}
