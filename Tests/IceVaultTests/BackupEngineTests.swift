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

    private func makeDatabaseService() throws -> DatabaseService {
        let directory = try makeTempDirectory()
        let databaseURL = directory.appendingPathComponent("icevault.sqlite")
        return try DatabaseService(databaseURL: databaseURL)
    }

    private enum TestError: Error {
        case unexpectedGlacierClientCreation
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
